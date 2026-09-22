import { describe, expect, it } from 'vitest'
import { TELEGRAM_MAX_TEXT_LENGTH, createTelegramChannel } from '../src/adapters/telegram.ts'
import type { ChannelInbox, InboundMessage, OutboundMessage } from '../src/contracts.ts'
import { ChannelGatewayError } from '../src/errors.ts'

interface RecordedCall {
  readonly method: string
  readonly body: Record<string, unknown> | undefined
  readonly signal: AbortSignal | undefined
}

type Responder = (
  method: string,
  body: Record<string, unknown> | undefined,
  signal: AbortSignal | undefined,
) => unknown

/**
 * A Bot API stand-in: every call is recorded, every answer is scripted.
 * A responder returns the `result` value; the envelope is the fake's business.
 */
function fakeTelegram(respond: Responder) {
  const calls: RecordedCall[] = []
  const impl = (async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const url = String(input)
    const method = url.split('/').at(-1) ?? ''
    const body = init?.body === undefined
      ? undefined
      : JSON.parse(String(init.body)) as Record<string, unknown>
    const signal = init?.signal ?? undefined
    calls.push({ method, body, signal: signal ?? undefined })
    const result = await respond(method, body, signal ?? undefined)
    return {
      ok: true,
      status: 200,
      json: () => Promise.resolve({ ok: true, result }),
    } as unknown as Response
  }) as unknown as typeof fetch
  return { impl, calls }
}

/** Poll until `check` holds, or fail the test rather than hanging it. */
async function waitFor(check: () => boolean, timeoutMs = 500): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!check()) {
    if (Date.now() > deadline) throw new Error('timed out waiting for the adapter')
    await new Promise(resolve => { setTimeout(resolve, 1) })
  }
}

/** The first poll answers with these updates; every later poll waits for stop. */
function pollOnce(updates: readonly unknown[], botIdentity: Responder): Responder {
  let polls = 0
  return (method, body, signal) => {
    if (method !== 'getUpdates') return botIdentity(method, body, signal)
    polls += 1
    if (polls === 1) return updates
    return new Promise((_resolve, reject) => {
      signal?.addEventListener('abort', () => { reject(new Error('aborted')) }, { once: true })
    })
  }
}

const privateMessage = {
  update_id: 10,
  message: {
    message_id: 5,
    date: 1_789_000_000,
    chat: { id: 42, type: 'private' },
    from: { id: 42, first_name: 'Yu', last_name: 'Create', is_bot: false },
    text: 'hello there',
  },
}

describe('telegram channel', () => {
  it('normalizes a private message and advances the poll offset', async () => {
    const respond = pollOnce([privateMessage], method => {
      if (method === 'getMe') return { id: 1, username: 'the-bot' }
      throw new Error(`unexpected ${method}`)
    })
    const { impl, calls } = fakeTelegram(respond)
    const channel = createTelegramChannel({ token: 'TESTTOKEN', fetch: impl, retryDelayMs: 1 })
    const seen: InboundMessage[] = []
    const inbox: ChannelInbox = { deliver: message => { seen.push(message) } }

    await channel.start(inbox)
    await waitFor(() => seen.length > 0)
    await channel.stop()

    expect(seen[0]).toEqual({
      channel: 'telegram',
      providerMessageId: '5',
      actor: { id: '42', displayName: 'Yu Create' },
      place: { route: 'chat:42', kind: 'direct' },
      visibility: 'private',
      text: 'hello there',
      timestamp: new Date(1_789_000_000 * 1000).toISOString(),
      raw: privateMessage.message,
    })
    const polls = calls.filter(call => call.method === 'getUpdates')
    expect(polls[0]?.body).toMatchObject({ timeout: 30 })
    expect(polls[1]?.body).toMatchObject({ offset: 11 })
  })

  it('reports a group place, its title, and its attachments', async () => {
    const group = {
      update_id: 3,
      message: {
        message_id: 8,
        date: 1_789_000_001,
        chat: { id: -100, type: 'supergroup', title: 'Team' },
        from: { id: 7, first_name: 'Aster', is_bot: true },
        caption: 'look',
        photo: [{ file_id: 'small', file_size: 1 }, { file_id: 'big', file_size: 9 }],
        document: { file_id: 'doc', file_name: 'plan.pdf', mime_type: 'application/pdf', file_size: 12 },
        reply_to_message: { message_id: 4 },
      },
    }
    const { impl } = fakeTelegram(pollOnce([group], () => ({ id: 1, username: 'the-bot' })))
    const channel = createTelegramChannel({ token: 'TESTTOKEN', fetch: impl, retryDelayMs: 1 })
    const seen: InboundMessage[] = []
    await channel.start({ deliver: message => { seen.push(message) } })
    await waitFor(() => seen.length > 0)
    await channel.stop()

    const message = seen[0]
    expect(message?.place).toEqual({ route: 'chat:-100', kind: 'group', title: 'Team' })
    expect(message?.visibility).toBe('group')
    expect(message?.actor).toEqual({ id: '7', displayName: 'Aster', isBot: true })
    expect(message?.text).toBe('look')
    expect(message?.replyTo).toBe('4')
    expect(message?.attachments).toEqual([
      { kind: 'image', ref: 'big', size: 9 },
      { kind: 'file', ref: 'doc', name: 'plan.pdf', mimeType: 'application/pdf', size: 12 },
    ])
  })

  it('keeps polling past updates it cannot normalize', async () => {
    const updates = [
      { update_id: 20 },
      { update_id: 21, message: { message_id: 9, date: 1, chat: { id: 5, type: 'channel' }, text: 'no sender' } },
      { update_id: 22, message: { ...privateMessage.message, message_id: 10 } },
    ]
    const { impl, calls } = fakeTelegram(pollOnce(updates, () => ({ id: 1, username: 'the-bot' })))
    const channel = createTelegramChannel({ token: 'TESTTOKEN', fetch: impl, retryDelayMs: 1 })
    const seen: InboundMessage[] = []
    await channel.start({ deliver: message => { seen.push(message) } })
    await waitFor(() => seen.length > 0)
    await channel.stop()

    expect(seen.map(message => message.providerMessageId)).toEqual(['10'])
    const polls = calls.filter(call => call.method === 'getUpdates')
    expect(polls[1]?.body).toMatchObject({ offset: 23 })
  })

  it('splits text longer than one message and keeps replyTo on the first chunk', async () => {
    const { impl, calls } = fakeTelegram(() => ({ message_id: 7, date: 0, chat: { id: 42, type: 'private' } }))
    const channel = createTelegramChannel({ token: 'TESTTOKEN', fetch: impl })
    const text = 'x'.repeat(TELEGRAM_MAX_TEXT_LENGTH + 1)

    const result = await channel.send({ channel: 'telegram', route: 'chat:42', text, replyTo: '5' })

    const sends = calls.filter(call => call.method === 'sendMessage')
    expect(sends).toHaveLength(2)
    expect(String(sends[0]?.body?.text)).toHaveLength(TELEGRAM_MAX_TEXT_LENGTH)
    expect(sends[0]?.body).toMatchObject({ chat_id: 42, reply_to_message_id: 5 })
    expect(sends[1]?.body?.reply_to_message_id).toBeUndefined()
    expect(result.providerMessageId).toBe('7')
  })

  it('refuses a route another channel minted', async () => {
    const { impl } = fakeTelegram(() => ({ message_id: 7 }))
    const channel = createTelegramChannel({ token: 'TESTTOKEN', fetch: impl })

    await expect(channel.send({ channel: 'telegram', route: 'weixin:someone', text: 'x' }))
      .rejects.toThrow(ChannelGatewayError)
  })

  it('sends an attachment by url or by provider ref, and refuses neither', async () => {
    const { impl, calls } = fakeTelegram(() => ({ message_id: 11, date: 0, chat: { id: 42, type: 'private' } }))
    const channel = createTelegramChannel({ token: 'TESTTOKEN', fetch: impl })

    await channel.send({ channel: 'telegram', route: 'chat:42', text: '', attachments: [{ kind: 'image', url: 'https://example.test/a.png' }] })
    expect(calls.at(-1)?.method).toBe('sendPhoto')
    expect(calls.at(-1)?.body).toMatchObject({ chat_id: 42, photo: 'https://example.test/a.png' })

    await channel.send({ channel: 'telegram', route: 'chat:42', text: '', attachments: [{ kind: 'file', ref: 'file-id' }] })
    expect(calls.at(-1)?.method).toBe('sendDocument')
    expect(calls.at(-1)?.body).toMatchObject({ document: 'file-id' })

    await expect(channel.send({ channel: 'telegram', route: 'chat:42', text: '', attachments: [{ kind: 'file' }] }))
      .rejects.toThrow(ChannelGatewayError)
    await expect(channel.send({ channel: 'telegram', route: 'chat:42', text: '' }))
      .rejects.toThrow(ChannelGatewayError)
  })

  it('redacts the token from a transport failure', async () => {
    const impl = (() => Promise.reject(new Error('connect ECONNREFUSED via https://api.telegram.org/botSECRET/getMe'))) as unknown as typeof fetch
    const channel = createTelegramChannel({ token: 'SECRET', fetch: impl })

    await expect(channel.start({ deliver: () => {} })).rejects.toThrow(/bot\*\*\*\/getMe/)
  })
})
