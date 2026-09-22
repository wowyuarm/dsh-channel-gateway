# dsh-channel-gateway

A channel gateway plugin for [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness).
A channel adapter normalizes provider traffic into one message contract, the
gateway authorizes it and announces a single `channel/inbound` event, and a
consumer answers through `ctx.channels.send` with the route the inbound message
carried.

The package covers the transport half of a chat integration: it fixes the
message contract and the seam between a provider and a consumer. What a message
means, whether it was seen before, and where it goes next are decided above it.

## Contract

Five surfaces, and they are the public API — keep them small, stable and
versionable. Field-by-field reference in [`docs/architecture.md`](docs/architecture.md).

| Surface | Shape |
| --- | --- |
| Inbound | `InboundMessage { channel, providerMessageId, actor, place, visibility, text, attachments?, timestamp, replyTo?, raw? }` |
| Outbound | `OutboundMessage { channel, route, text, attachments?, replyTo? }` |
| A transport | `Channel { name, capabilities, start(inbox), stop(), send(message) }` |
| The gateway | `ctx.channels.register(channel)` · `ctx.channels.send(message)` · event `channel/inbound` |
| Authorization | `ChannelAuth { authorize(request) }`, defaulting to an allowlist |

Three parts of the contract exist because a consumer needs them and a provider
does not supply them in a usable form:

- **`actor` / `place` / `visibility`** — who spoke, where, and in front of whom.
- **`providerMessageId`** — the provider's stable id, so a consumer can
  deduplicate and accept durably.
- **`place.route`** — an opaque, channel-minted destination. A consumer stores
  the route it saw and hands it back as `OutboundMessage.route`; no layer above
  the adapter composes a provider address.

## Install

The package is a DSH bundle: adding it inserts the gateway service row.

```sh
dsh plugin add @wowyuarm/dsh-channel-gateway --profile <profile>
```

Channel adapters are opt-in rows, because each needs its own credentials. A
profile adds the ones it wants:

```yaml
- insert:
    - id: channel-telegram
      name: '@wowyuarm/dsh-channel-gateway/telegram'
      config: { token: '<bot token>' }
    - id: channel-weixin
      name: '@wowyuarm/dsh-channel-gateway/weixin'
      config: { token: '<iLink bot token>' }
```

Who may speak is the gateway row's own configuration, not the adapter's:

```yaml
- id: channel-gateway
  config:
    allow:
      - telegram:123456789   # one actor of one channel
      - weixin:*             # every actor of one channel
      # - '*'                # everyone
```

An empty `allow` list admits nobody.

## Consuming it

```ts
import type { Context } from '@deepseek-ai/cordis'
import type {} from '@wowyuarm/dsh-channel-gateway'

export const name = 'my-ingress'
export const inject = ['channels']

export function apply(ctx: Context) {
  ctx.on('channel/inbound', (message) => {
    // Deduplicate on (message.channel, message.providerMessageId), accept it
    // durably, route it.
    void handle(message)
  })
}

async function handle(message: InboundMessage) {
  // ... later, answer it:
  await ctx.channels.send({ channel: message.channel, route: message.place.route, text: 'ok' })
}
```

## Adapters

| Adapter | Transport | Entry | Notes |
| --- | --- | --- | --- |
| Telegram | Bot API long poll (`getUpdates`) | `@wowyuarm/dsh-channel-gateway/telegram` | Text, documents, photos, audio, video; attachments send by provider ref or URL, not by local upload. `DSH_TELEGRAM_TOKEN` is the fallback for `config.token`. |
| Weixin | WeChat iLink long poll (`ilink/bot/getupdates`) | `@wowyuarm/dsh-channel-gateway/weixin` | Text only. A reply quotes the inbound `context_token`, which WeChat expires after roughly two minutes; media is reported inbound but not fetched or sent. Starts from a token it is given — the QR login flow is not implemented yet. |

Both adapters honour `HTTPS_PROXY`/`NO_PROXY` (Node's own fetch does not, unless
`NODE_USE_ENV_PROXY` is set).

## Development

```sh
npm install
npm run typecheck     # tsc, strict
npm test              # boundary guard + vitest
npm run build         # emits lib/
```

`src/` may import only its own relative modules, Node builtins, and
`@deepseek-ai/cordis` / `@deepseek-ai/schemastery`; adapters may add `undici`.
`npm run check:boundaries` enforces exactly that, so the neutral seam cannot
quietly acquire a host dependency.

## License

MIT. The Telegram transport is adapted from
[`dsh-telegram-channel`](https://github.com/hi-wenw/dsh-telegram-channel) (MIT);
the WeChat protocol shape follows
[`openclaw-weixin`](https://github.com/tencent-weixin/openclaw-weixin) (MIT) and
[`nanobot`](https://github.com/HKUDS/nanobot) (MIT).
