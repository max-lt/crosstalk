import { Peer, type Env } from './peer'
import { ID_RE } from './protocol'

export { Peer }

const LANDING = `crosstalk hub

This hub relays end-to-end encrypted messages between crosstalk peers.
It stores nothing. Connect with the crosstalk plugin for Claude Code.
To use this hub instead of the default one:

  /crosstalk:add-hub https://HOST

Source: https://github.com/max-lt/crosstalk
`

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const id = url.pathname.startsWith('/ws/') ? url.pathname.slice(4) : null
    if (id !== null) {
      if (!ID_RE.test(id)) return new Response('bad peer id', { status: 400 })
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
        return new Response('expected websocket', { status: 426 })
      }
      return env.PEER.get(env.PEER.idFromName(id)).fetch(request)
    }
    if (url.pathname === '/') return new Response(LANDING.replace('HOST', url.host), { headers: { 'content-type': 'text/plain' } })
    return new Response('not found', { status: 404 })
  },
} satisfies ExportedHandler<Env>
