/**
 * The two transport facts every polling adapter shares: which fetch to use, and
 * how to wait without outliving a stop.
 * @module @wowyuarm/dsh-channel-gateway/adapters/http
 */

import { EnvHttpProxyAgent, fetch as undiciFetch } from 'undici'

/** The proxy variables Node's own fetch does not read on its own. */
function proxyUrl(): string | undefined {
  const raw = process.env.HTTPS_PROXY
    ?? process.env.HTTP_PROXY
    ?? process.env.https_proxy
    ?? process.env.http_proxy
  return raw === undefined || raw === '' ? undefined : raw
}

/**
 * A fetch that honours `HTTPS_PROXY`/`NO_PROXY` and otherwise is the global one.
 *
 * Node's global fetch ignores the proxy variables unless `NODE_USE_ENV_PROXY`
 * is set, and a bot API that is only reachable through a proxy is a transport
 * fact rather than a usage choice — the transport layer is the right place for
 * it, and every layer above keeps calling a plain fetch.
 */
export function proxyAwareFetch(): typeof fetch {
  if (proxyUrl() === undefined) return globalThis.fetch
  const agent = new EnvHttpProxyAgent()
  const proxied = (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) =>
    undiciFetch(input as string | URL, {
      ...(init as Record<string, unknown>),
      dispatcher: agent,
    })
  return proxied as unknown as typeof fetch
}

/** Wait `ms`, or give up as soon as `signal` aborts. Never rejects. */
export function delay(ms: number, signal: AbortSignal | undefined): Promise<void> {
  if (signal === undefined) return new Promise(resolve => setTimeout(resolve, ms))
  if (signal.aborted) return Promise.resolve()
  return new Promise(resolve => {
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    const onAbort = (): void => {
      clearTimeout(timer)
      resolve()
    }
    signal.addEventListener('abort', onAbort, { once: true })
  })
}
