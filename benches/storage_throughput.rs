//! Throughput benchmarks for [`SyncEngineDb`] storage operations.
//!
//! ## Row-count rationale
//!
//! Three representative counts are used:
//!
//! * **10 ("small")** — a freshly started device with a handful of queued
//!   payments. Measures per-call SQLite overhead with minimal table size.
//! * **500 ("hundreds")** — a device that has been offline for an extended
//!   period, or a merchant terminal processing high throughput. At this scale
//!   a missing index causes a measurable but not catastrophic full-table scan.
//! * **5 000 ("thousands")** — stress-test scale. A full-table scan at 5 000
//!   rows should be visibly ~10× slower than at 500 rows (linear), while a
//!   properly indexed query remains roughly flat, making index regressions
//!   immediately detectable.
//!
//! All benchmarks use an in-memory SQLite database (`:memory:`) to eliminate
//! disk-I/O variance from the measurements.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use stellarconduit_core::message::types::TransactionEnvelope;
use stellarconduit_sync_engine::queue::TxPriority;
use stellarconduit_sync_engine::storage::SyncEngineDb;
use tokio::runtime::Runtime;

fn mock_envelope(id: u32) -> TransactionEnvelope {
    let mut message_id = [0u8; 32];
    message_id[0..4].copy_from_slice(&id.to_le_bytes());
    TransactionEnvelope {
        message_id,
        origin_pubkey: [2u8; 32],
        tx_xdr: format!("bench_xdr_{id}"),
        ttl_hops: 10,
        timestamp: 1_700_000_000 + id as u64,
        signature: [0u8; 64],
    }
}

/// Benchmark: enqueue_envelope — one insert per iteration on a pre-populated DB.
fn bench_enqueue(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("SyncEngineDb/enqueue_envelope");

    for row_count in [10usize, 500, 5000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(row_count),
            &row_count,
            |b, &n| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let db = SyncEngineDb::init(":memory:").await.unwrap();
                            // Pre-populate the table so the index has realistic size.
                            for i in 0..n as u32 {
                                db.enqueue_envelope(
                                    &mock_envelope(i),
                                    "GBENCH",
                                    (100 + i) as i64,
                                    TxPriority::Normal,
                                    1_700_000_000 + i as u64,
                                )
                                .await
                                .unwrap();
                            }
                            db
                        })
                    },
                    |db| {
                        rt.block_on(async {
                            // Insert one additional row; `n` is used as a unique ID.
                            db.enqueue_envelope(
                                &mock_envelope(n as u32),
                                "GBENCH",
                                (100 + n) as i64,
                                TxPriority::Normal,
                                1_800_000_000,
                            )
                            .await
                            .unwrap();
                        });
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Benchmark: list_queued_envelopes — full table scan at realistic row counts.
fn bench_list(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("SyncEngineDb/list_queued_envelopes");

    for row_count in [10usize, 500, 5000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(row_count),
            &row_count,
            |b, &n| {
                // Build the DB once per row_count outside the timed loop;
                // only the SELECT is measured.
                let db = rt.block_on(async {
                    let db = SyncEngineDb::init(":memory:").await.unwrap();
                    for i in 0..n as u32 {
                        db.enqueue_envelope(
                            &mock_envelope(i),
                            "GBENCH",
                            (100 + i) as i64,
                            TxPriority::Normal,
                            1_700_000_000 + i as u64,
                        )
                        .await
                        .unwrap();
                    }
                    db
                });

                b.iter(|| {
                    rt.block_on(async {
                        let rows = db.list_queued_envelopes().await.unwrap();
                        assert_eq!(rows.len(), n);
                    });
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_enqueue, bench_list);
criterion_main!(benches);
