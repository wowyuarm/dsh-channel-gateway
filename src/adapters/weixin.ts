/**
 * Weixin: a long-polling channel adapter over the WeChat iLink bot protocol.
 *
 * It normalizes `getupdates` messages into {@link InboundMessage} and sends
 * `sendmessage` requests back; commands, session mapping and reply policy are
 * implemented above it, from `channel/inbound`. The protocol shape follows the
 * public iLink endpoints as implemented by `openclaw-weixin` (MIT) and
 * `nanobot` (MIT); the code here is this package's own.
 *
 * Two transport facts a consumer needs: a reply carries the inbound message's
 * `context_token`, which WeChat expires server-side after roughly two minutes,
 * and this adapter carries text only — media items are reported inbound but
 * cannot be fetched or sent yet.
 * @module @wowyuarm/dsh-channel-gateway/weixin
 */

import { readFileSync } from 'node:fs'
import { randomBytes, randomInt } from 'node:crypto'
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

/** This adapter's own split threshold; a longer text becomes several messages. */
export const WEIXIN_MAX_TEXT_LENGTH = 2048

/** The public iLink bot endpoint family. */
export const WEIXIN_DEFAULT_BASE_URL = 'https://ilinkai.weixin.qq.com'

/** iLink business codes worth naming; anything else is reported as it came. */
const ERRCODE_CONTEXT_RESTRICTED = -2
const ERRCODE_INVALID_ARGUMENT = -3
const ERRCODE_STALE_TOKEN = -14

/** `message_type` of a message this bot itself produced. */
const MESSAGE_TYPE_BOT = 2
/** `message_state` of a finished message; `item.type` of a text item. */
const MESSAGE_STATE_FINISH = 2
const ITEM_TYPE_TEXT = 1

const ROUTE_PREFIX = 'wx:'

/** One failure the iLink API reported in its own envelope. */
export class WeixinApiError extends Error {
  constructor(
    readonly endpoint: string,
    readonly errcode: number,
    message: string,
  ) {
    super(message)
    this.name = 'WeixinApiError'
  }
}

interface WeixinMedia {
  url?: string
  mid_size?: number
  file_name?: string
  len?: string
}

interface WeixinItem {
  type?: number
  msg_id?: string
  text_item?: { text?: string }
  image_item?: WeixinMedia
  voice_item?: WeixinMedia & { text?: string; playtime?: number }
  file_item?: WeixinMedia
  video_item?: WeixinMedia
}

interface WeixinMessage {
  seq?: number
  message_id?: number
  from_user_id?: string
  to_user_id?: string
  create_time_ms?: number
  group_id?: string
  session_id?: string
  message_type?: number
  item_list?: WeixinItem[]
  context_token?: string
}

interface GetUpdatesResponse {
  ret?: number
  errcode?: number
  errmsg?: string
  msgs?: WeixinMessage[]
  get_updates_buf?: string
  longpolling_timeout_ms?: number
}

/** How one weixin channel is built; the plugin row supplies it. */
export interface WeixinChannelOptions {
  readonly token: string
  /** Injected for tests; defaults to a proxy-aware global fetch. */
  readonly fetch?: typeof fetch
  readonly baseUrl?: string
  /** Long-poll timeout for the first request, before the server suggests one. */
  readonly pollTimeoutSec?: number
  readonly retryDelayMs?: number
  /** The `iLink-App-Id` this account is registered under, when it needs one. */
  readonly appId?: string
  readonly appClientVersion?: number
  /** An optional `SKRouteTag` header. */
  readonly routeTag?: string
  readonly log?: AdapterLog
}

/** This package's own version, for the protocol's `base_info`. */
function bundleVersion(): string {
  try {
    const manifest = JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as { version?: unknown }
    return typeof manifest.version === 'string' ? manifest.version : 'unknown'
  } catch {
    return 'unknown'
  }
}

function encodePart(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64url')
}

/**
 * The route this adapter mints: the peer, plus the context token a reply must
 * quote. Both are opaque to every other layer — a consumer stores the route it
 * saw and hands it back, which is what lets the short-lived token travel
 * without the contract having to grow a field for it.
 */
function routeOf(userId: string, contextToken: string): string {
  return `${ROUTE_PREFIX}${encodePart(userId)}.${encodePart(contextToken)}`
}

function parseRoute(route: string): { userId: string; contextToken: string } {
  const invalid = (): never => {
    throw new ChannelGatewayError(`weixin route "${route}" was not minted by the weixin channel`)
  }
  if (!route.startsWith(ROUTE_PREFIX)) return invalid()
  const [user, token, ...rest] = route.slice(ROUTE_PREFIX.length).split('.')
  if (user === undefined || token === undefined || rest.length > 0) return invalid()
  try {
    return {
      userId: Buffer.from(user, 'base64url').toString('utf8'),
      contextToken: Buffer.from(token, 'base64url').toString('utf8'),
    }
  } catch {
    return invalid()
  }
}

/** Media an inbound item carried: reported, never fetched by this adapter. */
function attachmentsOf(items: readonly WeixinItem[]): readonly ChannelAttachment[] {
  const found: ChannelAttachment[] = []
  for (const item of items) {
    const media: readonly (readonly [ChannelAttachment['kind'], WeixinMedia | undefined])[] = [
      ['image', item.image_item],
      ['audio', item.voice_item],
      ['file', item.file_item],
      ['video', item.video_item],
    ]
    for (const [kind, value] of media) {
      if (value === undefined) continue
      const size = value.mid_size ?? (value.len === undefined ? undefined : Number(value.len))
      found.push({
        kind,
        ...(value.url === undefined ? {} : { url: value.url }),
        ...(value.file_name === undefined ? {} : { name: value.file_name }),
        ...(size === undefined || !Number.isFinite(size) ? {} : { size }),
        ...(item.msg_id === undefined ? {} : { ref: item.msg_id }),
      })
    }
  }
  return found
}

/** The text of one message: its own text items, or the transcript voice carries. */
function textOf(items: readonly WeixinItem[]): string {
  const parts: string[] = []
  for (const item of items) {
    if (item.text_item?.text !== undefined && item.text_item.text !== '') parts.push(item.text_item.text)
    else if (item.voice_item?.text !== undefined && item.voice_item.text !== '') parts.push(item.voice_item.text)
  }
  return parts.join('\n')
}

/** Split text no chunk of which exceeds `limit`; `''` yields no chunk at all. */
function splitText(text: string, limit: number): string[] {
  if (text === '') return []
  const chunks: string[] = []
  let rest = text
  while (rest.length > limit) {
    chunks.push(rest.slice(0, limit))
    rest = rest.slice(limit)
  }
  chunks.push(rest)
  return chunks
}

/**
 * Build the weixin channel. The gateway registers it; `start` begins polling
 * and `stop` ends it. Every provider call is one HTTP request, so a test can
 * inject `fetch` and drive the whole adapter without a network.
 */
export function createWeixinChannel(options: WeixinChannelOptions): Channel {
  const { token } = options
  if (token === '') throw new ChannelGatewayError('the weixin channel needs an iLink bot token')
  const fetchImpl = options.fetch ?? proxyAwareFetch()
  const baseUrl = (options.baseUrl ?? WEIXIN_DEFAULT_BASE_URL).replace(/\/+$/, '')
  const retryDelayMs = options.retryDelayMs ?? 3000
  const log = options.log ?? silentLog
  const version = bundleVersion()
  const baseInfo = { channel_version: version, bot_agent: `dsh-channel-gateway/${version}` }
  const appId = options.appId ?? ''
  const appClientVersion = options.appClientVersion ?? 1
  const routeTag = options.routeTag

  let requestTimeoutMs = (options.pollTimeoutSec ?? 35) * 1000
  let running = false
  let abort: AbortController | undefined
  let loop: Promise<void> | undefined

  function redact(message: string): string {
    return message.split(token).join('***')
  }

  function headers(): Record<string, string> {
    return {
      'content-type': 'application/json',
      'authorization-type': 'ilink_bot_token',
      authorization: `Bearer ${token}`,
      'ilink-app-id': appId,
      'ilink-app-clientversion': String(appClientVersion),
      // The protocol expects a fresh per-request identifier, base64 of a
      // decimal uint32.
      'x-wechat-uin': Buffer.from(String(randomInt(0, 0x1_0000_0000)), 'utf8').toString('base64'),
      ...(routeTag === undefined ? {} : { skroutetag: routeTag }),
    }
  }

  async function request<T>(endpoint: string, body: Record<string, unknown>, signal: AbortSignal | undefined): Promise<T> {
    const timeout = AbortSignal.timeout(requestTimeoutMs)
    const combined = signal === undefined ? timeout : AbortSignal.any([signal, timeout])
    let response: Response
    try {
      response = await fetchImpl(`${baseUrl}/${endpoint}`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ ...body, base_info: baseInfo }),
        signal: combined,
      })
    } catch (error: unknown) {
      throw new Error(redact(`weixin ${endpoint} failed: ${errorText(error)}`))
    }
    const payload = (await response.json().catch(() => undefined)) as (T & { errcode?: number; errmsg?: string }) | undefined
    if (payload === undefined) {
      throw new Error(redact(`weixin ${endpoint} answered nothing (HTTP ${String(response.status)})`))
    }
    // The protocol reports business failures inside a 200 response.
    if (typeof payload.errcode === 'number' && payload.errcode !== 0) {
      throw new WeixinApiError(endpoint, payload.errcode, redact(`weixin ${endpoint} rejected the request: errcode ${String(payload.errcode)}${payload.errmsg === undefined ? '' : ` (${payload.errmsg})`}`))
    }
    return payload
  }

  /** One protocol message as the contract reads it, or nothing to report. */
  function normalize(message: WeixinMessage): InboundMessage | undefined {
    // Our own sends come back through the same stream; they are not inbound.
    if (message.message_type === MESSAGE_TYPE_BOT) return undefined
    const from = message.from_user_id ?? ''
    if (from === '') return undefined
    const items = message.item_list ?? []
    const actor: ChannelActor = { id: from }
    const direct = message.group_id === undefined || message.group_id === ''
    const kind: ChannelPlaceKind = direct ? 'direct' : 'group'
    const place: ChannelPlace = { route: routeOf(from, message.context_token ?? ''), kind }
    const visibility: ChannelVisibility = direct ? 'private' : 'group'
    const attachments = attachmentsOf(items)
    const id = message.message_id === undefined ? message.seq : message.message_id
    return {
      channel: 'weixin',
      providerMessageId: String(id ?? `${from}:${String(message.create_time_ms ?? 0)}`),
      actor,
      place,
      visibility,
      text: textOf(items),
      ...(attachments.length === 0 ? {} : { attachments }),
      timestamp: new Date(message.create_time_ms ?? Date.now()).toISOString(),
      raw: message,
    }
  }

  async function poll(inbox: ChannelInbox, signal: AbortSignal): Promise<void> {
    let cursor = ''
    while (running && !signal.aborted) {
      let page: GetUpdatesResponse
      try {
        page = await request<GetUpdatesResponse>('ilink/bot/getupdates', { get_updates_buf: cursor }, signal)
      } catch (error: unknown) {
        if (!running || signal.aborted) return
        if (error instanceof WeixinApiError && error.errcode === ERRCODE_STALE_TOKEN) {
          running = false
          log.error('weixin: the bot token is stale (errcode -14); polling stops until a fresh token is configured')
          return
        }
        log.error(`weixin: getupdates failed, retrying: ${errorText(error)}`)
        await delay(retryDelayMs, signal)
        continue
      }
      if (typeof page.get_updates_buf === 'string' && page.get_updates_buf !== '') cursor = page.get_updates_buf
      if (typeof page.longpolling_timeout_ms === 'number' && page.longpolling_timeout_ms > 0) {
        // Ask for a little longer than the server said it would hold the call.
        requestTimeoutMs = page.longpolling_timeout_ms + 5000
      }
      for (const message of page.msgs ?? []) {
        const normalized = normalize(message)
        if (normalized !== undefined) inbox.deliver(normalized)
      }
    }
  }

  const capabilities: ChannelCapabilities = {
    attachments: false,
    buttons: false,
    edit: false,
    replyTo: false,
    maxTextLength: WEIXIN_MAX_TEXT_LENGTH,
  }

  return {
    name: 'weixin',
    capabilities,
    async start(inbox: ChannelInbox): Promise<void> {
      if (running) return
      running = true
      abort = new AbortController()
      log.info('weixin: polling for messages')
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
      const target = parseRoute(message.route)
      if ((message.attachments?.length ?? 0) > 0) {
        throw new ChannelGatewayError('weixin cannot send attachments yet: this adapter carries text only')
      }
      const chunks = splitText(message.text, WEIXIN_MAX_TEXT_LENGTH)
      if (chunks.length === 0) {
        throw new ChannelGatewayError('weixin send carried neither text nor attachments')
      }
      // The protocol answers a send with an empty body, so the id the caller
      // gets back is the client id this adapter minted and sent.
      const clientId = `dsh-channel-gateway:${String(Date.now())}-${randomBytes(4).toString('hex')}`
      for (const chunk of chunks) {
        await request('ilink/bot/sendmessage', {
          msg: {
            from_user_id: '',
            to_user_id: target.userId,
            client_id: clientId,
            message_type: MESSAGE_TYPE_BOT,
            message_state: MESSAGE_STATE_FINISH,
            item_list: [{ type: ITEM_TYPE_TEXT, text_item: { text: chunk } }],
            ...(target.contextToken === '' ? {} : { context_token: target.contextToken }),
          },
        }, undefined)
      }
      return { providerMessageId: clientId }
    },
  }
}

/** The weixin plugin row's own configuration. */
export interface WeixinPluginConfig {
  /** iLink bot token; falls back to `DSH_WEIXIN_TOKEN`. */
  token?: string
  baseUrl?: string
  appId?: string
  routeTag?: string
  pollTimeoutSec?: number
}

export const name = 'dsh-channel-gateway-weixin'
export const inject = ['channels']

export const Config: Schema<WeixinPluginConfig> = Schema.object({
  token: Schema.string().default(''),
  baseUrl: Schema.string().default(WEIXIN_DEFAULT_BASE_URL),
  appId: Schema.string().default(''),
  routeTag: Schema.string().default(''),
  pollTimeoutSec: Schema.number().default(35),
})

export function apply(ctx: Context, config: WeixinPluginConfig): void {
  const configured = config.token ?? ''
  const token = configured !== '' ? configured : (process.env.DSH_WEIXIN_TOKEN ?? '')
  const logger = ctx.logger('channel-gateway-weixin')
  if (token === '') {
    logger.error('no iLink bot token: set config.token or DSH_WEIXIN_TOKEN; the weixin channel is not registered')
    return
  }
  const routeTag = config.routeTag ?? ''
  const channel = createWeixinChannel({
    token,
    baseUrl: config.baseUrl ?? WEIXIN_DEFAULT_BASE_URL,
    appId: config.appId ?? '',
    pollTimeoutSec: config.pollTimeoutSec ?? 35,
    ...(routeTag === '' ? {} : { routeTag }),
    log: { info: message => logger.info(message), error: message => logger.error(message) },
  })
  ctx.effect(() => {
    const unregister = ctx.channels.register(channel)
    return () => { unregister() }
  }, 'channel-gateway.weixin')
}
