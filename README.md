# webhook-multiplexer

`webhook-multiplexer` fans one incoming HTTP webhook out to every matching local
development server. It gives a tunnel one stable local destination while apps,
services, and worktrees register their changing ports at runtime. It installs a
single binary named `hmux`.

The multiplexer is independent of the tunnel provider and webhook sender. It
does not call ngrok, Cloudflare, or any other provider API.

```text
webhook sender
      |
      v
ngrok, Cloudflare Tunnel, or another HTTP tunnel
      |
      v
hmux serve
      |--------------------|
      v                    v
app on port 3210      app on port 3274
```

## Status

Version 0.1 is intended for local development. The control protocol is
versioned but is not yet a stable public API. See [Architecture](docs/architecture.md)
for the current boundaries.

## Install from this checkout

Rust 1.97.1 or later is required.

```zsh
cargo install --path .
```

## Quick start

Start one multiplexer on the stable port used by your tunnel:

```zsh
hmux serve --listen 127.0.0.1:8080
```

Point a tunnel at that port. For example, the
[ngrok CLI](https://ngrok.com/docs/agent/cli/) accepts a local port and an
optional reserved URL:

```zsh
ngrok http 8080 --url https://your-name.ngrok.dev
```

Or start a
[Cloudflare Quick Tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/trycloudflare/):

```zsh
cloudflared tunnel --url http://localhost:8080
```

Configure the webhook sender to use the tunnel's public URL and the path you
want to route, such as `https://your-name.ngrok.dev/api/webhooks/payments`.

After each local app is listening, keep one registration process running beside
it:

```zsh
hmux register \
  --path /api/webhooks/payments \
  --target http://127.0.0.1:3210/api/webhooks/payments
```

Run the same command with each app's actual port. Every matching app receives
the same method, query string, end-to-end headers, and body bytes. This
preserves the raw body and signature headers used by common webhook verification
schemes. Each target still owns its provider-specific verification.

Use a development process supervisor or app startup script to start the
registration only after the app is ready. Keep the registration process alive
for as long as the app should receive webhooks.

## Registrations and cleanup

`register` creates a lease and renews it every third of its lifetime. A clean
shutdown removes the lease immediately. If the app or registration process is
killed, the lease expires without manual cleanup. The default lease lifetime is
20 seconds.

The registration process also reconnects if the multiplexer restarts. A stale
control descriptor left by an unclean server exit is replaced by the next
`serve` process.

Inspect or remove active leases with:

```zsh
hmux list
hmux list --json
hmux unregister <lease-id>
```

Check a server with `hmux status`, which reports the process, control address,
and active lease count, and exits non-zero when no server is running. Stop a
running server with `hmux stop`. It requests a graceful shutdown through the
control API and waits until in-flight deliveries have drained and the server
has exited, which also works for a server running in the background.

Use `--instance <name>` to run separate groups. Commands in one group must use
the same instance name and state directory. The default state directory is a
per-user directory inside the operating system's temporary directory, and the
server refuses to start if it is not a directory owned by the current user. Set
it explicitly with `--state-directory` or `WEBHOOK_MULTIPLEXER_STATE_DIR` when
process environments do not share the same temporary directory.

## Routing

A registration matches an incoming request by:

- HTTP method, exactly; the default is `POST`.
- URL path, exactly.
- `Host` authority, exactly and case-insensitively, only when `--host` is set.

The incoming query string is copied to the target URL. Target URLs cannot
contain their own query string or fragment, so this behavior is unambiguous.

By default, the outgoing `Host` header is generated from the target URL. Use
`--preserve-host` only when the target needs the public incoming host.

Hop-by-hop headers and headers named by `Connection` are not forwarded.
`Content-Length` is generated for the forwarded body. Redirect responses are
not followed.

## Response policies

The default `all` policy returns success only when every matching target returns
a 2xx response.

| Policy | Multiplexer returns 2xx when |
| --- | --- |
| `all` | Every matching target returns 2xx |
| `any` | At least one matching target returns 2xx |
| `always` | At least one target matched and every delivery was attempted |

When the selected policy is not satisfied, a target timeout produces HTTP 504
and other delivery failures produce HTTP 502. No matching target produces HTTP
503 unless `serve --accept-when-empty` is set. Capacity exhaustion also produces
HTTP 503, and an oversized body produces HTTP 413.

The multiplexer does not retry a target. The webhook sender may retry a non-2xx
response. With the `all` policy, this can deliver the same event again to a
target that succeeded on the first attempt. Webhook handlers must therefore be
idempotent, normally by storing the sender's event ID.

## Safety defaults

- The ingress and control listeners only bind to loopback addresses.
- The control listener uses a random port and a per-process bearer token.
- Control state is private to the current user on Unix.
- Targets must resolve directly to `localhost` or a loopback IP literal unless
  `serve --allow-non-loopback-targets` is set.
- Request bodies are held in memory only. They are not persisted or written to
  logs.
- Body size, active target, ingress concurrency, delivery concurrency, and
  target duration have explicit limits.

Allowing non-loopback targets expands where authenticated local clients can
send webhook data. Use it only when that access is intentional.

This tool does not verify webhook signatures. Each target remains responsible
for authentication, replay protection, idempotency, and provider-specific
behavior.

## Configuration

Run `hmux <command> --help` for every option. Common environment
variables are:

| Variable | Purpose |
| --- | --- |
| `WEBHOOK_MULTIPLEXER_INSTANCE` | Select an independent server group |
| `WEBHOOK_MULTIPLEXER_STATE_DIR` | Set the shared local control-state root |
| `WEBHOOK_MULTIPLEXER_LOG` | Set the tracing filter, such as `info` or `warn` |
| `WEBHOOK_MULTIPLEXER_LOG_FORMAT` | Select `text` or `json` diagnostic logs |

Machine-readable command results go to stdout. Diagnostic logs go to stderr.
With `register --json`, each registration or reconnection is one JSON line.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). The design and internal control protocol
are documented in [docs/architecture.md](docs/architecture.md) and
[docs/control-protocol.md](docs/control-protocol.md).

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
