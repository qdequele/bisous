# CLAUDE.md

meili-connect (working name) is a change-data-capture service that keeps
Meilisearch indexes in sync with Postgres, Supabase, MySQL, MongoDB, Kafka and
DynamoDB. 100% Rust (edition 2024, Rust 1.94+). It runs standalone and as the
**Connect** data plane of the Meilisearch Lab (`../meilisearch-lab`).

Read `docs/ARCHITECTURE.md` before changing anything: it defines the delivery
guarantees and the crate dependency rule.

## Rules

- `crates/core` holds contracts only. Changing a trait or a type there
  affects every crate: do it deliberately and update `docs/ARCHITECTURE.md`.
- `sources/*`, `sink`, `engine`, `store` depend on `core`, never on each other.
- Pick error variants by the reaction the engine must have (`core::error`).
- Secrets use `core::Secret`, show as `<redacted>`, and are never logged.
  Compare tokens in constant time (`subtle`).
- No `unwrap`/`expect` outside tests. No blocking calls on the async runtime.
  Every loop that waits honours its `CancellationToken`.
- Channels are bounded; never buffer a stream in memory without a cap.
- Every behaviour gets a test. Tests that need a live service read its URL
  from an env var and skip (pass with a notice) when it is unset.
- Conventional commits. No AI co-author trailers.

## Before pushing

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
