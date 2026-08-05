# Security policy

## Supported versions

Until the first release, security fixes are made on the `main` branch. After
releases begin, the latest minor release will receive security fixes.

## Reporting a vulnerability

Do not open a public issue or include real webhook payloads, tokens, or secrets
in a report.

Use GitHub's private vulnerability reporting feature in the repository's
Security tab when it is available. If it is not available, contact the
maintainer privately through the contact details on the repository owner's
profile.

Include:

- The affected version or commit.
- The security boundary that is crossed.
- Minimal reproduction steps using synthetic data.
- The expected and observed result.
- Any known mitigation.

You should receive an acknowledgement within seven days. Details will remain
private until a fix and disclosure plan are ready.

## Security model

Version 0.1 is a local development tool, not a hardened production reverse
proxy. Its public-facing ingress is expected to be exposed only through an HTTP
tunnel. Its control API and targets are local by default.

The following are security requirements:

- Control traffic remains on a separate loopback listener and requires the
  descriptor bearer token.
- The state directory is private to the current user.
- Target URLs are loopback-only unless the server operator opts out.
- Request bodies are bounded, are not logged, and are not persisted.
- Redirects and environment-configured HTTP proxies are disabled for outgoing
  target requests.

Users remain responsible for protecting tunnel URLs, verifying webhook
signatures at every target, handling replay, and keeping webhook handlers
idempotent.
