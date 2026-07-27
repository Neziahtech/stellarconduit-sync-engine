# Add property-based and fuzz testing for conflict detection and settlement

Closes #25.

## Summary

Every test in this crate was example-based — specific inputs, specific expected outputs. That's not enough for the two most safety-critical pieces of code here: `conflict::detector`/`conflict::resolver` (double-spend detection) and `settlement::tracker`'s state machine (an invalid transition could report a payment settled when it isn't, or vice versa). This PR adds property-based and fuzz testing for both, plus a fuzz target for the untrusted-deserialization path in `storage::db`.

- Adds `proptest` as a dev-dependency (justified over `quickcheck` in `Cargo.toml` and `README.md`: built-in shrinking and a richer combinator API for composing the `QueuedSlot` generators these tests need).
- `conflict::detector::detect_conflicts`: adds `proptest_detect_conflicts_matches_naive_reference`, which cross-checks the function against a naive O(n²) reference implementation over several thousand randomly generated `QueuedSlot` batches.
  - Writing this test surfaced a real bug: `detect_conflicts` grouped colliding slots, sorted their message IDs, and only paired up *adjacent* IDs (`ids.windows(2)`). With three or more distinct envelopes colliding on the same `(account, sequence)` slot, the pair at the ends of the sorted list (e.g. the 1st and 3rd) was never reported as a `Conflict`, even though it's a genuine double-spend pair. Fixed by enumerating all pairwise combinations within a colliding group. Added `test_three_way_collision_reports_every_pair` as a direct regression test.
- `settlement::tracker::SettlementStatus::can_transition_to`: adds `test_settlement_transition_matrix_is_exhaustive`, checking all 25 `(from, to)` pairs across the 5 `SettlementStatus` variants against an independently hand-written reachability list (not derived from the function's own match arms, so it can actually catch a divergence).
- Adds a `cargo-fuzz` target at `fuzz/fuzz_targets/deserialize_envelope.rs` fuzzing `rmp_serde::from_slice::<TransactionEnvelope>` — the deserialization path `SyncEngineDb` uses to read queued envelopes back out of SQLite. The fuzz crate is its own detached workspace (`fuzz/Cargo.toml` has an empty `[workspace]` table) so it doesn't affect `cargo build`/`test`/`clippy` at the repo root and isn't required in normal CI. Includes a seed corpus entry (a validly-encoded envelope) under `fuzz/corpus/deserialize_envelope/`.
- Documents how to run the fuzz target locally and as a bounded CI-friendly smoke run in `README.md`.

## Why

- `conflict::detector` and `settlement::tracker` are the two places where getting logic wrong has direct funds-safety consequences (a missed double-spend pair, or a payment reported settled when it isn't). Example-based tests can't cover the input space needed to build confidence here.
- `SyncEngineDb` deserializes envelope bytes it reads back from its own storage. That's safe today, but once encryption-at-rest and database export/import for device migration land, those bytes may come from a removable or previously-exported file — i.e., no longer fully trusted. A fuzz target on the deserialization path builds a safety net ahead of that.

## Test plan

- [x] `cargo fmt --all` — clean
- [x] `cargo clippy --all-targets --all-features -- -D warnings` — clean
- [x] `cargo test` — all tests pass, including:
  - [x] `proptest_detect_conflicts_matches_naive_reference` (4096 generated cases)
  - [x] `test_settlement_transition_matrix_is_exhaustive` (all 25 pairs)
  - [x] `test_three_way_collision_reports_every_pair` (regression for the bug found above)
- [x] Fuzz harness compiles cleanly (`cargo +nightly build` from `fuzz/`) and a manual smoke run against the seed corpus (`./target/debug/deserialize_envelope -max_total_time=5 corpus/deserialize_envelope`) completed ~2.7M executions with zero crashes (not required in normal CI; see README for the documented `cargo fuzz run` invocations)
