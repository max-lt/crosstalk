// Control socket: one Unix socket per session, so a CLI can drive the channel process
// without a model turn. One JSON request per connection, one text reply, then close.
import { existsSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'

export const CONTROL_SOCKET = 'ctl.sock'

export interface ControlRequest {
  cmd: string
  args: string[]
}

export type ControlHandler = (req: ControlRequest) => Promise<string>

export function controlSocketPath(sessionDir: string): string {
  return join(sessionDir, CONTROL_SOCKET)
}

// Several processes may share one session (Codex respawns its MCP servers without
// killing the old ones). The first live owner keeps the socket; the others skip it.
export async function startControl(
  sessionDir: string,
  handle: ControlHandler,
  log: (line: string) => void,
  mode: 'shared' | 'exclusive',
): Promise<() => void> {
  const path = controlSocketPath(sessionDir)
  if (existsSync(path)) {
    const alive = await controlRequest(path, { cmd: 'ping', args: [] }, 1000).then(r => r === 'pong', () => false)
    if (alive && mode === 'exclusive') throw new Error(`another process owns ${path}`)
    if (alive) {
      log('control socket owned by another process, skipping')
      return () => {}
    }
    unlinkSync(path)
  }
  const server = Bun.listen<{ buf: string }>({
    unix: path,
    socket: {
      open(sock) {
        sock.data = { buf: '' }
      },
      data(sock, chunk) {
        sock.data.buf += chunk.toString()
        const nl = sock.data.buf.indexOf('\n')
        if (nl < 0) return
        let req: ControlRequest
        try {
          req = JSON.parse(sock.data.buf.slice(0, nl))
        } catch {
          sock.end('error: bad request\n')
          return
        }
        handle(req)
          .then(reply => sock.end(`${reply}\n`))
          .catch(err => sock.end(`error: ${(err as Error).message}\n`))
      },
      error(_sock, err) {
        log(`control socket: ${err}`)
      },
    },
  })
  return () => {
    server.stop(true)
    try {
      unlinkSync(path)
    } catch {}
  }
}

// Client side: send one request, return the reply without its trailing newline.
export function controlRequest(path: string, req: ControlRequest, timeoutMs = 60_000): Promise<string> {
  return new Promise((resolve, reject) => {
    let buf = ''
    let conn: { end(): void } | undefined
    const timer = setTimeout(() => {
      conn?.end()
      reject(new Error('control request timed out'))
    }, timeoutMs)
    const fail = (err: unknown) => {
      clearTimeout(timer)
      reject(err)
    }
    Bun.connect({
      unix: path,
      socket: {
        open(sock) {
          conn = sock
          sock.write(`${JSON.stringify(req)}\n`)
        },
        data(_sock, chunk) {
          buf += chunk.toString()
        },
        close() {
          clearTimeout(timer)
          resolve(buf.replace(/\n$/, ''))
        },
        error: (_sock, err) => fail(err),
        connectError: (_sock, err) => fail(err),
      },
    }).catch(fail)
  })
}
