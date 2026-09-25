/**
 * Telegram: a long-polling channel adapter over the Bot API.
 *
 * It normalizes updates into {@link InboundMessage} and sends {@link
 * OutboundMessage} back. Commands, conversation-to-session mapping, streaming
 * and reply policy are implemented above it, from `channel/inbound`. The
 * transport is adapted from `dsh-telegram-channel` (MIT), reduced to the Bot
 * API calls this adapter needs.
 * @module @wowyuarm/dsh-channel-gateway/telegram
 */

import type { Context } from '@deepseek-ai/cordis'
import Schema from '@deepseek-ai/schemastery'
import type {
  Channel,
  ChannelActor,
  ChannelAttachment,
  ChannelCapabilities,
  ChannelInbox,
  ChannelPlace,
  ChannelPlaceKind,
  ChannelSendResult,
  ChannelVisibility,
  InboundMessage,
  OutboundMessage,
} from '../contracts.ts'
import { ChannelGatewayError, errorText } from '../errors.ts'
import type {} from '../gateway.ts'
import { delay, proxyAwareFetch } from './http.ts'
import { silentLog, type AdapterLog } from './log.ts'

/** Telegram's own limit for one message's text. */
export const TELEGRAM_MAX_TEXT_LENGTH = 4096

interface TelegramUser {
  id: number
  is_bot?: boolean
  first_name?: string
  last_name?: string
  username?: string
}

interface TelegramChat {
  id: number
  type: string
  title?: string
}

interface TelegramPhotoSize {
  file_id: string
  file_size?: number
}

interface TelegramFileRef {
  file_id: string
  file_name?: string
  mime_type?: string
  file_size?: number
}

interface TelegramMessage {
  message_id: number
  date: number
  chat: TelegramChat
  from?: TelegramUser
  text?: string
  /** Media captions arrive here, not in `text`. */
  caption?: string
  photo?: TelegramPhotoSize[]
  document?: TelegramFileRef
  audio?: TelegramFileRef
  voice?: TelegramFileRef
  video?: TelegramFileRef
  reply_to_message?: { message_id: number }
}

interface TelegramUpdate {
  update_id: number
  message?: TelegramMessage
}

interface TelegramResponse<T> {
  ok: boolean
  result?: T
  description?: string
}

/** How one telegram channel is built; the plugin row supplies it. */
export interface TelegramChannelOptions {
  readonly token: string
  /** Injected for tests; defaults to a proxy-aware global fetch. */
  readonly fetch?: typeof fetch
  readonly apiBaseUrl?: string
  readonly pollingTimeoutSec?: number
  /** How long to wait after a failed poll before polling again. */
  readonly retryDelayMs?: number
  readonly log?: AdapterLog
}

/** The route scheme this adapter mints and is the only layer to parse. */
const ROUTE_PREFIX = 'chat:'

function routeOf(chatId: number): string {
  return `${ROUTE_PREFIX}${String(chatId)}`
}

function chatIdOf(route: string): number {
  const match = /^chat:(-?\d+)$/.exec(route)
  if (match === null || match[1] === undefined) {
    throw new ChannelGatewayError(`telegram route "${route}" was not minted by the telegram channel`)
  }
  return Number(match[1])
}

/** One message's attachments, as far as the Bot API already carries them. */
function attachmentsOf(message: TelegramMessage): readonly ChannelAttachment[] {
  const found: ChannelAttachment[] = []
  const photo = message.photo?.at(-1)
  if (photo !== undefined) {
    found.push({
      kind: 'image',
      ref: photo.file_id,
      ...(photo.file_size === undefined ? {} : { size: photo.file_size }),
    })
  }
  const files: readonly (readonly [ChannelAttachment['kind'], TelegramFileRef | undefined])[] = [
    ['file', message.document],
    ['audio', message.audio ?? message.voice],
    ['video', message.video],
  ]
  for (const [kind, file] of files) {
    if (file === undefined) continue
    found.push({
      kind,
      ref: file.file_id,
      ...(file.file_name === undefined ? {} : { name: file.file_name }),
      ...(file.mime_type === undefined ? {} : { mimeType: file.mime_type }),
      ...(file.file_size === undefined ? {} : { size: file.file_size }),
    })
  }
  return found
}

/** Split text no chunk of which exceeds `limit`; `''` yields no chunk at all. */
function splitText(text: string, limit: number): string[] {
  if (text === '') return []
  const chunks: string[] = []
  let rest = text
  while (rest.length > limit) {
    const cut = breakAt(rest, limit)
    chunks.push(rest.slice(0, cut))
    rest = rest.slice(cut)
  }
  chunks.push(rest)
  return chunks
}

/** Whether one UTF-16 unit is the first half of a surrogate pair. */
function isHighSurrogate(unit: number): boolean {
  return unit >= 0xd800 && unit <= 0xdbff
}

/** Whether one UTF-16 unit is the second half of a surrogate pair. */
function isLowSurrogate(unit: number): boolean {
  return unit >= 0xdc00 && unit <= 0xdfff
}

/**
 * How much of `text` the first chunk takes: the last line break in range, else
 * the last space, else the limit itself. The result is always positive and never
 * exceeds `limit`, so `splitText` advances, and it never lands between the two
 * halves of a surrogate pair — that would send half a character.
 */
function breakAt(text: string, limit: number): number {
  let end = limit
  if (isHighSurrogate(text.charCodeAt(end - 1)) && isLowSurrogate(text.charCodeAt(end))) end -= 1
  if (end <= 0) return limit
  const window = text.slice(0, end)
  const newline = window.lastIndexOf('\n')
  if (newline > 0) return newline + 1
  const space = window.lastIndexOf(' ')
  if (space > 0) return space + 1
  return end
}

/**
 * Build the telegram channel. The gateway registers it; `start` begins polling
 * and `stop` ends it. Every provider call is one HTTP request, so a test can
 * inject `fetch` and drive the whole adapter without a network.
 */
export function createTelegramChannel(options: TelegramChannelOptions): Channel {
  const { token } = options
  if (token === '') throw new ChannelGatewayError('the telegram channel needs a bot token')
  const fetchImpl = options.fetch ?? proxyAwareFetch()
  const apiBaseUrl = options.apiBaseUrl ?? 'https://api.telegram.org'
  const pollingTimeoutSec = options.pollingTimeoutSec ?? 30
  const retryDelayMs = options.retryDelayMs ?? 3000
  const log = options.log ?? silentLog

  let running = false
  let abort: AbortController | undefined
  let loop: Promise<void> | undefined

  /** Redact the token from anything a failure may quote back. */
  function redact(message: string): string {
    return message.split(token).join('***')
  }

  async function call<T>(method: string, body?: Record<string, unknown>, signal?: AbortSignal): Promise<T> {
    const init: RequestInit = { method: 'POST', headers: { 'content-type': 'application/json' } }
    if (body !== undefined) init.body = JSON.stringify(body)
    if (signal !== undefined) init.signal = signal
    let response: Response
    try {
      response = await fetchImpl(`${apiBaseUrl}/bot${token}/${method}`, init)
    } catch (error: unknown) {
      throw new Error(redact(`telegram ${method} failed: ${errorText(error)}`))
    }
    const payload = (await response.json().catch(() => undefined)) as TelegramResponse<T> | undefined
    if (payload === undefined || payload.ok !== true || payload.result === undefined) {
      const description = payload?.description ?? `HTTP ${String(response.status)}`
      throw new Error(redact(`telegram ${method} was rejected: ${description}`))
    }
    return payload.result
  }

  /** One update as the contract reads it, or nothing when it carries no sender. */
  function normalize(message: TelegramMessage): InboundMessage | undefined {
    const from = message.from
    // Channel posts arrive with no sender, so there is no actor to report and
    // no authorization to ask; the gateway does not invent one.
    if (from === undefined) return undefined

    const displayName = [from.first_name, from.last_name]
      .filter((part): part is string => part !== undefined && part !== '')
      .join(' ')
    const actor: ChannelActor = {
      id: String(from.id),
      ...(displayName === '' ? {} : { displayName }),
      ...(from.is_bot === true ? { isBot: true } : {}),
    }

    const kind: ChannelPlaceKind = message.chat.type === 'private'
      ? 'direct'
      : message.chat.type === 'channel' ? 'channel' : 'group'
    const place: ChannelPlace = {
      route: routeOf(message.chat.id),
      kind,
      ...(message.chat.title === undefined ? {} : { title: message.chat.title }),
    }
    const visibility: ChannelVisibility = kind === 'direct' ? 'private' : 'group'

    const attachments = attachmentsOf(message)
    const replyTo = message.reply_to_message?.message_id
    return {
      channel: 'telegram',
      providerMessageId: String(message.message_id),
      actor,
      place,
      visibility,
      text: message.text ?? message.caption ?? '',
      ...(attachments.length === 0 ? {} : { attachments }),
      timestamp: new Date(message.date * 1000).toISOString(),
      ...(replyTo === undefined ? {} : { replyTo: String(replyTo) }),
      raw: message,
    }
  }

  async function sendAttachment(chatId: number, attachment: ChannelAttachment): Promise<number> {
    const method = attachment.kind === 'image'
      ? 'sendPhoto'
      : attachment.kind === 'video' ? 'sendVideo' : attachment.kind === 'audio' ? 'sendAudio' : 'sendDocument'
    const field = attachment.kind === 'image'
      ? 'photo'
      : attachment.kind === 'video' ? 'video' : attachment.kind === 'audio' ? 'audio' : 'document'
    const handle = attachment.url ?? attachment.ref
    if (handle === undefined) {
      throw new ChannelGatewayError(
        `telegram cannot send a ${attachment.kind} attachment without a url or a ref; local uploads are not part of this adapter`,
      )
    }
    const sent = await call<TelegramMessage>(method, { chat_id: chatId, [field]: handle })
    return sent.message_id
  }

  async function poll(inbox: ChannelInbox, signal: AbortSignal): Promise<void> {
    let offset: number | undefined
    while (running && !signal.aborted) {
      let updates: TelegramUpdate[]
      try {
        updates = await call<TelegramUpdate[]>('getUpdates', {
          timeout: pollingTimeoutSec,
          allowed_updates: ['message'],
          ...(offset === undefined ? {} : { offset }),
        }, signal)
      } catch (error: unknown) {
        if (!running || signal.aborted) return
        log.error(`telegram: getUpdates failed, retrying: ${errorText(error)}`)
        await delay(retryDelayMs, signal)
        continue
      }
      for (const update of updates) {
        // Advance past every update the provider handed over, whether or not it
        // carried a message this adapter can normalize: re-reading it forever
        // would block everything behind it.
        offset = update.update_id + 1
        const message = update.message
        if (message === undefined) continue
        const normalized = normalize(message)
        if (normalized !== undefined) inbox.deliver(normalized)
      }
    }
  }

  const capabilities: ChannelCapabilities = {
    attachments: true,
    buttons: false,
    edit: false,
    replyTo: true,
    maxTextLength: TELEGRAM_MAX_TEXT_LENGTH,
  }

  return {
    name: 'telegram',
    capabilities,
    async start(inbox: ChannelInbox): Promise<void> {
      if (running) return
      const me = await call<TelegramUser>('getMe')
      running = true
      abort = new AbortController()
      log.info(`telegram: receiving as @${me.username ?? String(me.id)}`)
      loop = poll(inbox, abort.signal)
    },
    async stop(): Promise<void> {
      if (!running) return
      running = false
      abort?.abort()
      await loop
      loop = undefined
      abort = undefined
    },
    async send(message: OutboundMessage): Promise<ChannelSendResult> {
      const chatId = chatIdOf(message.route)
      let last: number | undefined
      for (const [index, chunk] of splitText(message.text, TELEGRAM_MAX_TEXT_LENGTH).entries()) {
        const body: Record<string, unknown> = { chat_id: chatId, text: chunk }
        if (index === 0 && message.replyTo !== undefined) body.reply_to_message_id = Number(message.replyTo)
        const sent = await call<TelegramMessage>('sendMessage', body)
        last = sent.message_id
      }
      for (const attachment of message.attachments ?? []) {
        last = await sendAttachment(chatId, attachment)
      }
      if (last === undefined) {
        throw new ChannelGatewayError('telegram send carried neither text nor attachments')
      }
      return { providerMessageId: String(last) }
    },
  }
}

/** The telegram plugin row's own configuration. */
export interface TelegramPluginConfig {
  /** Bot token; falls back to `DSH_TELEGRAM_TOKEN`. */
  readonly token?: string
  readonly apiBaseUrl?: string
  readonly pollingTimeoutSec?: number
}

export const name = 'dsh-channel-gateway-telegram'
export const inject = ['channels']

export const Config: Schema<TelegramPluginConfig> = Schema.object({
  token: Schema.string().default(''),
  apiBaseUrl: Schema.string().default('https://api.telegram.org'),
  pollingTimeoutSec: Schema.number().default(30),
})

export function apply(ctx: Context, config: TelegramPluginConfig): void {
  const configured = config.token ?? ''
  const token = configured !== '' ? configured : (process.env.DSH_TELEGRAM_TOKEN ?? '')
  const logger = ctx.logger('channel-gateway-telegram')
  if (token === '') {
    logger.error('no bot token: set config.token or DSH_TELEGRAM_TOKEN; the telegram channel is not registered')
    return
  }
  const channel = createTelegramChannel({
    token,
    apiBaseUrl: config.apiBaseUrl ?? 'https://api.telegram.org',
    pollingTimeoutSec: config.pollingTimeoutSec ?? 30,
    log: { info: message => logger.info(message), error: message => logger.error(message) },
  })
  ctx.effect(() => {
    const unregister = ctx.channels.register(channel)
    return () => { unregister() }
  }, 'channel-gateway.telegram')
}
