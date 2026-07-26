//! Throughput benchmarks for [`detect_conflicts`].
//!
//! ## Batch-size rationale
//!
//! Three batch sizes are benchmarked:
//!
//! * **16 ("small")** — a typical single-peer gossip burst arriving on a device
//!   that has been isolated briefly. `detect_conflicts` is called on every
//!   incoming batch, so this is the hottest real-world path.
//! * **512 ("medium")** — a device reconnecting after a longer outage and
//!   receiving a bulk catch-up batch from its first available relay.
//! * **8 192 ("large")** — a worst-case full-mesh reconciliation dump. The
//!   HashMap-based grouping in `detect_conflicts` is O(n), so the 8 192-entry
//!   run should be ≈ 512× the 16-entry run if the implementation is correct.
//!   Any superlinear regression (e.g. an accidental O(n²) re-scan) would
//!   produce a disproportionate jump here.
//!
//! Conflict density is varied across three scenarios per batch size:
//!
//! * **no conflicts** — every slot has a unique (account, sequence) pair;
//!   represents normal operation with no double-spend attempts.
//! * **sparse conflicts** — 10 % of slots share an (account, sequence) pair
//!   with another slot; represents occasional relay mis-ordering.
//! * **dense conflicts** — every even slot clashes with its successor; a
//!   pathological case that stresses the conflict accumulation path.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use stellarconduit_sync_engine::conflict::{detect_conflicts, QueuedSlot};

fn make_slot(account: &str, sequence: i64, id: u8) -> QueuedSlot {
    QueuedSlot {
        source_account: account.to_string(),
        sequence,
        message_id: [id; 32],
    }
}

/// Build a batch with no conflicting (account, sequence) pairs.
fn no_conflict_batch(n: usize) -> Vec<QueuedSlot> {
    (0..n)
        .map(|i| make_slot("GBENCH", i as i64, (i % 255) as u8))
        .collect()
}

/// Build a batch where ~10 % of slots share a sequence number (sparse conflicts).
fn sparse_conflict_batch(n: usize) -> Vec<QueuedSlot> {
    let mut slots = Vec::with_capacity(n);
    for i in 0..n {
        // Every 10th slot reuses the previous sequence, creating a conflict.
        let sequence = if i % 10 == 9 {
            (i - 1) as i64
        } else {
            i as i64
        };
        slots.push(make_slot("GBENCH", sequence, (i % 255) as u8));
    }
    slots
}

/// Build a batch where every even slot conflicts with the odd slot after it.
fn dense_conflict_batch(n: usize) -> Vec<QueuedSlot> {
    let mut slots = Vec::with_capacity(n);
    for i in 0..n {
        // Pair consecutive slots onto the same sequence number.
        let sequence = (i / 2) as i64;
        slots.push(make_slot("GBENCH", sequence, (i % 255) as u8));
    }
    slots
}

fn bench_no_conflicts(c: &mut Criterion) {
    let mut group = c.benchmark_group("detect_conflicts/no_conflicts");
    for batch_size in [16usize, 512, 8192] {
        let slots = no_conflict_batch(batch_size);
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &slots,
            |b, slots| {
                b.iter(|| {
                    let conflicts = detect_conflicts(slots);
                    assert!(conflicts.is_empty());
                });
            },
        );
    }
    group.finish();
}

fn bench_sparse_conflicts(c: &mut Criterion) {
    let mut group = c.benchmark_group("detect_conflicts/sparse_conflicts");
    for batch_size in [16usize, 512, 8192] {
        let slots = sparse_conflict_batch(batch_size);
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &slots,
            |b, slots| {
                b.iter(|| {
                    let _ = detect_conflicts(slots);
                });
            },
        );
    }
    group.finish();
}

fn bench_dense_conflicts(c: &mut Criterion) {
    let mut group = c.benchmark_group("detect_conflicts/dense_conflicts");
    for batch_size in [16usize, 512, 8192] {
        let slots = dense_conflict_batch(batch_size);
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &slots,
            |b, slots| {
                b.iter(|| {
                    let _ = detect_conflicts(slots);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_no_conflicts,
    bench_sparse_conflicts,
    bench_dense_conflicts
);
criterion_main!(benches);
