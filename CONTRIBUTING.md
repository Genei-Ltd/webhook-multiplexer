# Contributing

Thank you for improving `webhook-multiplexer`.

## Development setup

Install Rust through rustup, then clone the repository. Rust 1.97.1 or later is
required. The checked-in `rust-toolchain.toml` selects the toolchain and
required components.

Run these checks before submitting a change:

```zsh
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --locked
cargo build --release --locked
cargo package --locked --allow-dirty
```

Dependency policy is checked with `cargo-deny` in CI. If it is installed
locally, run:

```zsh
cargo deny check
```

## Change standards

- Keep the tunnel and webhook provider boundaries generic.
- Preserve raw webhook body bytes and end-to-end headers.
- Parse untrusted values at HTTP, CLI, and state-file boundaries.
- Keep ingress and control listeners on loopback.
- Add focused tests for app-owned behavior. Prefer real loopback servers and
  process boundaries over mocks.
- Do not add request body logging or persistence without an explicit security
  design.
- Update the README, architecture, protocol document, and changelog when their
  contracts change.

Use `cargo fmt`; do not hand-format Rust code. Clippy warnings are treated as
errors.

## Tests

Unit tests cover domain invariants and lease expiry. Integration tests use real
HTTP listeners to cover body and header forwarding, aggregate response policy,
timeouts, authenticated control operations, and the complete CLI lifecycle.

Keep each new test tied to a distinct behavior owned by this project. Do not
duplicate dependency behavior or add tests only to increase coverage.

## Security reports

Do not disclose a suspected vulnerability in a public issue. Follow
[SECURITY.md](SECURITY.md).

## License

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project is licensed under the Apache-2.0 and
MIT licenses, at your option, without additional terms or conditions.
