# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Tunnel-neutral HTTP webhook fan-out with exact method, path, and optional host
  matching.
- Dynamic target leases with heartbeats, expiry, clean removal, and client
  reconnection.
- Separate loopback ingress and authenticated control listeners.
- `serve`, `register`, `list`, `unregister`, `stop`, and `status` commands.
- Configurable aggregate response policies and resource limits.
- Loopback-only target policy with an explicit non-loopback opt-in.
- Text and JSON diagnostic logging and machine-readable command output.
- Unit, HTTP integration, control, and complete CLI process tests.
