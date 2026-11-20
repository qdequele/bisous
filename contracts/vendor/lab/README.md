# Vendored Lab contracts

`lab-events.schema.json` is a byte copy of `contracts/lab-events.schema.json` in
`meilisearch/lab`. The Lab owns it.

- Never edit it here. Change it in the Lab (backward compatible, deployed
  first), then copy the file:
  `curl -fsSL -H "Authorization: Bearer $LAB_REPO_TOKEN" https://raw.githubusercontent.com/meilisearch/lab/main/contracts/lab-events.schema.json -o contracts/vendor/lab/lab-events.schema.json`
- The tests validate the events the engine builds (`usage.recorded`,
  `sync.state_changed`) against this copy (`crates/server/src/sync_events.rs`).
- CI (`lab-contract-drift` in `.github/workflows/ci.yml`) diffs the copy against the
  Lab's `main` and fails on drift. It needs the `LAB_REPO_TOKEN` repository secret
  (read access to `meilisearch/lab`); without it the job is skipped with a notice.
