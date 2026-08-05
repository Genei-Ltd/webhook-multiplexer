# Control protocol version 1

The control protocol coordinates local CLI processes. It is versioned so a
client can reject incompatible state, but it is not yet a stable public API.

## Transport and authentication

The server binds a random IPv4 loopback address. Clients discover it from:

```text
<state root>/<instance>/control.json
```

The descriptor shape is:

```json
{
  "protocol_version": 1,
  "address": "127.0.0.1:49152",
  "token": "opaque-random-token",
  "process_id": 12345
}
```

Clients reject non-loopback addresses, incompatible protocol versions, and
malformed tokens. Every request sends:

```http
Authorization: Bearer <token>
```

The comparison is constant-time after checking the value length. Control
request bodies are limited to 16 KiB. The client ignores HTTP proxy environment
variables, does not follow redirects, and limits each control request to two
seconds.

## Data types

Durations are unsigned integer milliseconds. Lease IDs are UUIDs.

Methods must be valid HTTP methods. Paths start with `/` and cannot contain a
query, fragment, or control character. Hosts are valid HTTP authorities. Target
URLs use `http` or `https`, include a host, and do not contain credentials,
queries, or fragments.

## Endpoints

### Status

```http
GET /v1/status
```

Returns HTTP 204 after authentication.

### Create a lease

```http
POST /v1/leases
Content-Type: application/json

{
  "method": "POST",
  "path": "/api/webhooks/payments",
  "host": null,
  "target": "http://127.0.0.1:3210/api/webhooks/payments",
  "preserve_host": false
}
```

Returns HTTP 201:

```json
{
  "lease_id": "d82963a4-ed50-4e6a-9e21-f822b0a05fd5",
  "lease_ttl_ms": 20000,
  "renew_after_ms": 6666
}
```

The server generates the lease ID and controls both durations.

### Renew a lease

```http
PUT /v1/leases/<lease-id>/renew
```

Returns HTTP 200 with the same response shape as creation. An expired lease is
not recoverable; the client creates a new lease.

### Remove a lease

```http
DELETE /v1/leases/<lease-id>
```

Returns HTTP 204.

### List leases

```http
GET /v1/leases
```

Returns HTTP 200:

```json
{
  "leases": [
    {
      "lease_id": "d82963a4-ed50-4e6a-9e21-f822b0a05fd5",
      "method": "POST",
      "path": "/api/webhooks/payments",
      "host": null,
      "target": "http://127.0.0.1:3210/api/webhooks/payments",
      "preserve_host": false,
      "expires_in_ms": 18000
    }
  ]
}
```

Expired leases are omitted.

### Shut down the server

```http
POST /v1/shutdown
```

Returns HTTP 202. The server stops accepting new work, finishes in-flight
deliveries, removes the control descriptor, and exits. This is the same
graceful shutdown as a termination signal.

## Errors

Application errors use a non-2xx status and this JSON shape:

```json
{
  "error": "machine-readable-code",
  "message": "human-readable explanation"
}
```

Defined errors include invalid authorization, invalid lease IDs, missing or
expired leases, non-loopback targets, and target-capacity exhaustion. Invalid
JSON or invalid domain values are rejected by the HTTP boundary before registry
operations run.
