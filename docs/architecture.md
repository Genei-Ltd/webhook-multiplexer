# Architecture

## Purpose

The multiplexer gives an HTTP tunnel one stable loopback destination and fans
matching requests out to local services whose ports and lifetimes change. The
tunnel provider and webhook sender remain outside the program.

Version 0.1 is designed for one operating-system user on one development
machine. It does not provide a distributed registry, durable delivery, or a
production reverse-proxy security boundary.

## Components

### Ingress server

`serve` binds the configured loopback address. It finds all live registrations
that exactly match the method, path, and optional host, rejects the request
before reading the body when nothing matches, then reads a bounded request body
and dispatches to the matching targets concurrently.

One semaphore bounds complete ingress requests. A second semaphore bounds
outgoing deliveries across all requests. A target timeout includes time spent
waiting for a delivery permit.

The result is reduced through the configured `all`, `any`, or `always` response
policy. The JSON response contains a request ID and aggregate counts. Target
response bodies are not read or exposed.

### Lease registry

The registry is in memory. Each registration has a server-generated UUID and an
expiry time. Create, renew, list, match, and remove operations prune expired
leases. No target survives a server restart.

The registration client renews after one third of the lease lifetime. If a
renewal or control connection fails, it reconnects with bounded exponential
backoff and creates a new lease. A graceful client shutdown attempts to remove
the current lease.

### Control server

Each `serve` process binds a separate random loopback port for control traffic.
Every route requires a bearer token. The address, token, protocol version, and
server process ID are stored in a local descriptor. A shutdown endpoint
requests the same graceful stop as a termination signal. See
[control-protocol.md](control-protocol.md).

The control listener is separate from ingress so a public tunnel cannot reach
registration operations through the ingress port.

### Runtime state

State is namespaced by a validated instance name. The instance directory
contains:

```text
<state root>/<instance>/
  control.json
  instance.lock
```

An exclusive file lock permits one server for each state-root and instance
pair. On Unix, the instance directory is mode `0700`, and created files are mode
`0600`. The descriptor is written through a temporary file and replaced.

The descriptor is removed after a graceful server shutdown. It may remain after
an unclean exit, but it cannot keep a lease alive. A new server holding the
instance lock replaces it.

## Forwarding invariants

- The request body is buffered once as raw bytes and cloned through reference
  counting for each delivery.
- The incoming method and query string are preserved.
- The target URL supplies the outgoing scheme, authority, and base path.
- End-to-end headers are copied without interpreting provider signatures.
- `Connection`, `Content-Length`, `Host`, `Transfer-Encoding`, standard
  hop-by-hop headers, and headers named by `Connection` are removed.
- `Host` is generated from the target unless the registration explicitly asks
  to preserve the incoming value.
- The HTTP client does not use environment proxies and does not follow
  redirects.
- The multiplexer makes one attempt per matching target and does not persist
  requests.

These rules avoid changes to signed bodies while preventing connection-local
metadata from leaking between HTTP hops.

## Trust boundaries

The ingress is intended to receive untrusted public traffic through a tunnel.
It therefore has body and concurrency limits and cannot bind a non-loopback
address.

The control API trusts possession of a token stored in the local descriptor.
The token and control listener are not exposed through ingress. On platforms
without Unix file modes, users must keep the state directory accessible only to
their account.

Targets are restricted to `localhost` and loopback IP literals by default. The
non-loopback opt-in is a server-wide policy because it permits local control
clients to send webhook content to other hosts.

## Deliberate exclusions in version 0.1

- Tunnel lifecycle or provider API integration.
- Provider-specific webhook verification.
- Delivery retries, queues, or body persistence.
- Route patterns, prefixes, or request rewriting.
- Target discovery across machines or users.
- TLS on loopback listeners.
- A stable external control API or compatibility across protocol versions.
