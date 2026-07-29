//! Durable, on-device storage for queued envelopes, sequence reservations,
//! settlement status, and detected conflicts — so none of it is lost if the
//! device restarts before settlement completes. Mirrors the SQLite-via-
//! `tokio-rusqlite` pattern used in `stellarconduit_core::persistence::db`.

use std::path::Path;

use tokio_rusqlite::Connection;

use stellarconduit_core::message::types::TransactionEnvelope;

use crate::conflict::Conflict;
use crate::errors::SyncEngineError;
use crate::queue::TxPriority;
use crate::settlement::SettlementStatus;

pub struct SyncEngineDb {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueuedEnvelopeRecord {
    pub envelope: TransactionEnvelope,
    pub source_account: String,
    pub sequence: i64,
    pub priority: TxPriority,
    pub enqueued_at: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistoryEntry {
    pub message_id: [u8; 32],
    pub from_status: String,
    pub to_status: String,
    pub timestamp: u64,
}

impl SyncEngineDb {
    /// Initialize the embedded SQLite database, creating tables if needed.
    /// Pass `":memory:"` for an ephemeral, test-only database.
    pub async fn init(db_path: &str) -> Result<Self, SyncEngineError> {
        let conn = if db_path == ":memory:" {
            Connection::open_in_memory().await?
        } else {
            Connection::open(Path::new(db_path)).await?
        };

        conn.call(|conn| {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS queued_envelopes (
                    message_id      BLOB PRIMARY KEY,
                    source_account  TEXT NOT NULL,
                    sequence        INTEGER NOT NULL,
                    priority        INTEGER NOT NULL,
                    enqueued_at     INTEGER NOT NULL,
                    envelope_bytes  BLOB NOT NULL
                );

                CREATE TABLE IF NOT EXISTS settlement_status (
                    message_id  BLOB PRIMARY KEY,
                    status      TEXT NOT NULL,
                    updated_at  INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sequence_reservations (
                    source_account  TEXT PRIMARY KEY,
                    last_reserved   INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS conflicts (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_account  TEXT NOT NULL,
                    sequence        INTEGER NOT NULL,
                    envelope_a      BLOB NOT NULL,
                    envelope_b      BLOB NOT NULL,
                    detected_at     INTEGER NOT NULL,
                    resolved        INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS settlement_history (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id  BLOB NOT NULL,
                    from_status TEXT NOT NULL,
                    to_status   TEXT NOT NULL,
                    timestamp   INTEGER NOT NULL
                );",
            )?;
            Ok(())
        })
        .await?;

        Ok(Self { conn })
    }

    pub async fn enqueue_envelope(
        &self,
        envelope: &TransactionEnvelope,
        source_account: &str,
        sequence: i64,
        priority: TxPriority,
        enqueued_at: u64,
    ) -> Result<(), SyncEngineError> {
        let message_id = envelope.message_id.to_vec();
        let envelope_bytes = rmp_serde::to_vec(envelope)?;
        let source_account = source_account.to_string();
        let priority: i64 = priority.into();

        self.conn
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO queued_envelopes
                        (message_id, source_account, sequence, priority, enqueued_at, envelope_bytes)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        message_id,
                        source_account,
                        sequence,
                        priority,
                        enqueued_at as i64,
                        envelope_bytes
                    ],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Atomically persist a newly-signed payment in a single SQLite
    /// transaction: its sequence reservation, the queued envelope, and its
    /// initial `Queued` settlement status.
    ///
    /// All three rows share one transaction so that a crash at any instant
    /// leaves the database either fully updated (the call completed) or
    /// entirely unchanged (as if the call never happened) — never a
    /// half-written state. This is what lets [`crate::engine::SyncEngine`] be
    /// restart-safe: a Stellar sequence number cannot be skipped on-chain, so
    /// writing the reservation without the envelope (or the envelope without
    /// the reservation) would either burn a gap or invite a reuse, both of
    /// which are double-spend hazards against the user's own account.
    pub async fn enqueue_transaction(
        &self,
        envelope: &TransactionEnvelope,
        source_account: &str,
        sequence: i64,
        priority: TxPriority,
        enqueued_at: u64,
    ) -> Result<(), SyncEngineError> {
        let message_id = envelope.message_id.to_vec();
        let envelope_bytes = rmp_serde::to_vec(envelope)?;
        let source_account = source_account.to_string();
        let priority: i64 = priority.into();
        let status = SettlementStatus::Queued.as_str().to_string();

        self.conn
            .call(move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT OR REPLACE INTO sequence_reservations
                        (source_account, last_reserved) VALUES (?1, ?2)",
                    rusqlite::params![source_account, sequence],
                )?;
                tx.execute(
                    "INSERT OR REPLACE INTO queued_envelopes
                        (message_id, source_account, sequence, priority, enqueued_at, envelope_bytes)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![
                        message_id,
                        source_account,
                        sequence,
                        priority,
                        enqueued_at as i64,
                        envelope_bytes
                    ],
                )?;
                tx.execute(
                    "INSERT OR REPLACE INTO settlement_status (message_id, status, updated_at)
                     VALUES (?1, ?2, ?3)",
                    rusqlite::params![message_id, status, enqueued_at as i64],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn get_queued_envelope(
        &self,
        message_id: [u8; 32],
    ) -> Result<Option<QueuedEnvelopeRecord>, SyncEngineError> {
        let id = message_id.to_vec();
        let row = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT source_account, sequence, priority, enqueued_at, envelope_bytes
                     FROM queued_envelopes WHERE message_id = ?1",
                )?;
                let mut rows = stmt.query(rusqlite::params![id])?;
                if let Some(row) = rows.next()? {
                    let source_account: String = row.get(0)?;
                    let sequence: i64 = row.get(1)?;
                    let priority: i64 = row.get(2)?;
                    let enqueued_at: i64 = row.get(3)?;
                    let envelope_bytes: Vec<u8> = row.get(4)?;
                    Ok(Some((
                        source_account,
                        sequence,
                        priority,
                        enqueued_at,
                        envelope_bytes,
                    )))
                } else {
                    Ok(None)
                }
            })
            .await?;

        match row {
            None => Ok(None),
            Some((source_account, sequence, priority, enqueued_at, envelope_bytes)) => {
                let envelope: TransactionEnvelope = rmp_serde::from_slice(&envelope_bytes)?;
                Ok(Some(QueuedEnvelopeRecord {
                    envelope,
                    source_account,
                    sequence,
                    priority: TxPriority::try_from(priority)?,
                    enqueued_at: enqueued_at as u64,
                }))
            }
        }
    }

    pub async fn list_queued_envelopes(
        &self,
    ) -> Result<Vec<QueuedEnvelopeRecord>, SyncEngineError> {
        let rows = self
            .conn
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT source_account, sequence, priority, enqueued_at, envelope_bytes
                     FROM queued_envelopes",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        let source_account: String = row.get(0)?;
                        let sequence: i64 = row.get(1)?;
                        let priority: i64 = row.get(2)?;
                        let enqueued_at: i64 = row.get(3)?;
                        let envelope_bytes: Vec<u8> = row.get(4)?;
                        Ok((
                            source_account,
                            sequence,
                            priority,
                            enqueued_at,
                            envelope_bytes,
                        ))
                    })?
                    .collect::<Result<Vec<_>, rusqlite::Error>>()?;
                Ok(rows)
            })
            .await?;

        rows.into_iter()
            .map(
                |(source_account, sequence, priority, enqueued_at, envelope_bytes)| {
                    let envelope: TransactionEnvelope = rmp_serde::from_slice(&envelope_bytes)?;
                    Ok(QueuedEnvelopeRecord {
                        envelope,
                        source_account,
                        sequence,
                        priority: TxPriority::try_from(priority)?,
                        enqueued_at: enqueued_at as u64,
                    })
                },
            )
            .collect()
    }

    pub async fn remove_queued_envelope(
        &self,
        message_id: [u8; 32],
    ) -> Result<(), SyncEngineError> {
        let id = message_id.to_vec();
        self.conn
            .call(move |conn| {
                conn.execute("DELETE FROM queued_envelopes WHERE message_id = ?1", [id])?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn set_settlement_status(
        &self,
        message_id: [u8; 32],
        status: SettlementStatus,
        updated_at: u64,
    ) -> Result<(), SyncEngineError> {
        let id = message_id.to_vec();
        let status_str = status.as_str().to_string();
        self.conn
            .call(move |conn| {
                let from_status: String = conn
                    .query_row(
                        "SELECT status FROM settlement_status WHERE message_id = ?1",
                        rusqlite::params![id],
                        |row| row.get(0),
                    )
                    .unwrap_or_default();

                conn.execute(
                    "INSERT OR REPLACE INTO settlement_status (message_id, status, updated_at)
                     VALUES (?1, ?2, ?3)",
                    rusqlite::params![id, status_str, updated_at as i64],
                )?;

                conn.execute(
                    "INSERT INTO settlement_history (message_id, from_status, to_status, timestamp)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![id, from_status, status_str, updated_at as i64],
                )?;

                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn get_settlement_status(
        &self,
        message_id: [u8; 32],
    ) -> Result<Option<SettlementStatus>, SyncEngineError> {
        let id = message_id.to_vec();
        let status: Option<String> = self
            .conn
            .call(move |conn| {
                let mut stmt =
                    conn.prepare("SELECT status FROM settlement_status WHERE message_id = ?1")?;
                let mut rows = stmt.query(rusqlite::params![id])?;
                if let Some(row) = rows.next()? {
                    Ok(Some(row.get::<_, String>(0)?))
                } else {
                    Ok(None)
                }
            })
            .await?;

        status.map(|s| s.parse()).transpose()
    }

    pub async fn history_for(
        &self,
        message_id: [u8; 32],
    ) -> Result<Vec<HistoryEntry>, SyncEngineError> {
        let id = message_id.to_vec();
        let rows = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT message_id, from_status, to_status, timestamp
                     FROM settlement_history
                     WHERE message_id = ?1
                     ORDER BY id",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![id], |row| {
                        let message_id: Vec<u8> = row.get(0)?;
                        let from_status: String = row.get(1)?;
                        let to_status: String = row.get(2)?;
                        let timestamp: i64 = row.get(3)?;
                        Ok(HistoryEntry {
                            message_id: message_id.try_into().unwrap_or([0u8; 32]),
                            from_status,
                            to_status,
                            timestamp: timestamp as u64,
                        })
                    })?
                    .collect::<Result<Vec<_>, rusqlite::Error>>()?;
                Ok(rows)
            })
            .await?;
        Ok(rows)
    }

    pub async fn save_sequence_reservation(
        &self,
        source_account: &str,
        last_reserved: i64,
    ) -> Result<(), SyncEngineError> {
        let source_account = source_account.to_string();
        self.conn
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO sequence_reservations (source_account, last_reserved)
                     VALUES (?1, ?2)",
                    rusqlite::params![source_account, last_reserved],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn load_sequence_reservation(
        &self,
        source_account: &str,
    ) -> Result<Option<i64>, SyncEngineError> {
        let source_account = source_account.to_string();
        let value = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT last_reserved FROM sequence_reservations WHERE source_account = ?1",
                )?;
                let mut rows = stmt.query(rusqlite::params![source_account])?;
                if let Some(row) = rows.next()? {
                    Ok(Some(row.get::<_, i64>(0)?))
                } else {
                    Ok(None)
                }
            })
            .await?;
        Ok(value)
    }

    /// Load every known account's last-reserved sequence number. Used by
    /// [`crate::engine::SyncEngine::open`] to rehydrate all in-memory sequence
    /// state in one pass after a restart; the per-account
    /// [`Self::load_sequence_reservation`] is for single-account lookups.
    pub async fn list_all_sequence_reservations(
        &self,
    ) -> Result<std::collections::HashMap<String, i64>, SyncEngineError> {
        let rows = self
            .conn
            .call(|conn| {
                let mut stmt = conn
                    .prepare("SELECT source_account, last_reserved FROM sequence_reservations")?;
                let rows = stmt
                    .query_map([], |row| {
                        let source_account: String = row.get(0)?;
                        let last_reserved: i64 = row.get(1)?;
                        Ok((source_account, last_reserved))
                    })?
                    .collect::<Result<std::collections::HashMap<_, _>, rusqlite::Error>>()?;
                Ok(rows)
            })
            .await?;
        Ok(rows)
    }

    pub async fn record_conflict(
        &self,
        conflict: &Conflict,
        detected_at: u64,
    ) -> Result<(), SyncEngineError> {
        let source_account = conflict.source_account.clone();
        let sequence = conflict.sequence;
        let envelope_a = conflict.envelope_a.to_vec();
        let envelope_b = conflict.envelope_b.to_vec();
        self.conn
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO conflicts (source_account, sequence, envelope_a, envelope_b, detected_at, resolved)
                     VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                    rusqlite::params![source_account, sequence, envelope_a, envelope_b, detected_at as i64],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn list_unresolved_conflicts(&self) -> Result<Vec<Conflict>, SyncEngineError> {
        let rows = self
            .conn
            .call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT source_account, sequence, envelope_a, envelope_b
                     FROM conflicts WHERE resolved = 0",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        let source_account: String = row.get(0)?;
                        let sequence: i64 = row.get(1)?;
                        let envelope_a: Vec<u8> = row.get(2)?;
                        let envelope_b: Vec<u8> = row.get(3)?;
                        Ok((source_account, sequence, envelope_a, envelope_b))
                    })?
                    .collect::<Result<Vec<_>, rusqlite::Error>>()?;
                Ok(rows)
            })
            .await?;

        Ok(rows
            .into_iter()
            .map(
                |(source_account, sequence, envelope_a, envelope_b)| Conflict {
                    source_account,
                    sequence,
                    envelope_a: envelope_a.try_into().unwrap_or([0u8; 32]),
                    envelope_b: envelope_b.try_into().unwrap_or([0u8; 32]),
                },
            )
            .collect())
    }

    pub async fn sweep_stale_envelopes(
        &self,
        max_age_secs: u64,
        now: u64,
    ) -> Result<Vec<[u8; 32]>, SyncEngineError> {
        let cutoff = now.saturating_sub(max_age_secs) as i64;
        let stale_id_bytes: Vec<Vec<u8>> = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT q.message_id
                     FROM queued_envelopes q
                     JOIN settlement_status s ON s.message_id = q.message_id
                     WHERE q.enqueued_at < ?1
                       AND s.status IN ('queued', 'propagating')",
                )?;
                let rows = stmt
                    .query_map(rusqlite::params![cutoff], |row| {
                        let message_id: Vec<u8> = row.get(0)?;
                        Ok(message_id)
                    })?
                    .collect::<Result<Vec<_>, rusqlite::Error>>()?;
                Ok(rows)
            })
            .await?;

        let stale_ids: Vec<[u8; 32]> = stale_id_bytes
            .into_iter()
            .filter_map(|v| v.try_into().ok())
            .collect();

        for &mid in &stale_ids {
            self.set_settlement_status(mid, SettlementStatus::Failed, now)
                .await?;
        }

        Ok(stale_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_envelope(message_id: u8) -> TransactionEnvelope {
        TransactionEnvelope {
            message_id: [message_id; 32],
            origin_pubkey: [1u8; 32],
            tx_xdr: "mock_xdr".to_string(),
            ttl_hops: 10,
            timestamp: 1_700_000_000,
            signature: [0u8; 64],
        }
    }

    #[tokio::test]
    async fn test_enqueue_and_get_queued_envelope() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let envelope = mock_envelope(1);
        db.enqueue_envelope(&envelope, "GABC", 101, TxPriority::Emergency, 1000)
            .await
            .unwrap();

        let record = db.get_queued_envelope([1u8; 32]).await.unwrap().unwrap();
        assert_eq!(record.envelope, envelope);
        assert_eq!(record.source_account, "GABC");
        assert_eq!(record.sequence, 101);
        assert_eq!(record.priority, TxPriority::Emergency);
        assert_eq!(record.enqueued_at, 1000);
    }

    #[tokio::test]
    async fn test_get_missing_envelope_returns_none() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        assert!(db.get_queued_envelope([9u8; 32]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_queued_envelopes() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        db.enqueue_envelope(&mock_envelope(1), "GABC", 101, TxPriority::Normal, 1000)
            .await
            .unwrap();
        db.enqueue_envelope(&mock_envelope(2), "GABC", 102, TxPriority::Low, 1001)
            .await
            .unwrap();

        let records = db.list_queued_envelopes().await.unwrap();
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn test_remove_queued_envelope() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        db.enqueue_envelope(&mock_envelope(1), "GABC", 101, TxPriority::Normal, 1000)
            .await
            .unwrap();
        db.remove_queued_envelope([1u8; 32]).await.unwrap();
        assert!(db.get_queued_envelope([1u8; 32]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_settlement_status_roundtrip() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        db.set_settlement_status([1u8; 32], SettlementStatus::Propagating, 2000)
            .await
            .unwrap();
        let status = db.get_settlement_status([1u8; 32]).await.unwrap();
        assert_eq!(status, Some(SettlementStatus::Propagating));
    }

    #[tokio::test]
    async fn test_sequence_reservation_roundtrip() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        assert_eq!(db.load_sequence_reservation("GABC").await.unwrap(), None);
        db.save_sequence_reservation("GABC", 101).await.unwrap();
        assert_eq!(
            db.load_sequence_reservation("GABC").await.unwrap(),
            Some(101)
        );
    }

    #[tokio::test]
    async fn test_conflict_record_and_list_unresolved() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let conflict = Conflict {
            source_account: "GABC".to_string(),
            sequence: 101,
            envelope_a: [1u8; 32],
            envelope_b: [2u8; 32],
        };
        db.record_conflict(&conflict, 3000).await.unwrap();

        let unresolved = db.list_unresolved_conflicts().await.unwrap();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0], conflict);
    }

    // ── Issue 2: settlement_history tests ──

    #[tokio::test]
    async fn test_successful_transitions_are_recorded_in_order() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let id = [1u8; 32];

        db.set_settlement_status(id, SettlementStatus::Queued, 1000)
            .await
            .unwrap();
        db.set_settlement_status(id, SettlementStatus::Propagating, 1001)
            .await
            .unwrap();
        db.set_settlement_status(id, SettlementStatus::Settled, 1002)
            .await
            .unwrap();

        let history = db.history_for(id).await.unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].from_status, "");
        assert_eq!(history[0].to_status, "queued");
        assert_eq!(history[0].timestamp, 1000);
        assert_eq!(history[1].from_status, "queued");
        assert_eq!(history[1].to_status, "propagating");
        assert_eq!(history[1].timestamp, 1001);
        assert_eq!(history[2].from_status, "propagating");
        assert_eq!(history[2].to_status, "settled");
        assert_eq!(history[2].timestamp, 1002);
    }

    #[tokio::test]
    async fn test_history_for_unknown_envelope_is_empty() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let history = db.history_for([99u8; 32]).await.unwrap();
        assert!(history.is_empty());
    }

    #[tokio::test]
    async fn test_full_lifecycle_produces_complete_history() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let id = [2u8; 32];

        db.set_settlement_status(id, SettlementStatus::Queued, 1000)
            .await
            .unwrap();
        db.set_settlement_status(id, SettlementStatus::Propagating, 1001)
            .await
            .unwrap();
        db.set_settlement_status(id, SettlementStatus::Disputed, 1002)
            .await
            .unwrap();
        db.set_settlement_status(id, SettlementStatus::Settled, 1003)
            .await
            .unwrap();

        let history = db.history_for(id).await.unwrap();
        assert_eq!(history.len(), 4);
        assert_eq!(history[0].to_status, "queued");
        assert_eq!(history[1].to_status, "propagating");
        assert_eq!(history[2].to_status, "disputed");
        assert_eq!(history[3].to_status, "settled");
    }

    // ── Issue 3: staleness sweep tests ──

    #[tokio::test]
    async fn test_sweep_identifies_stale_queued_envelopes() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let stale_id = [1u8; 32];
        let fresh_id = [2u8; 32];

        db.enqueue_envelope(&mock_envelope(1), "GABC", 101, TxPriority::Normal, 100)
            .await
            .unwrap();
        db.set_settlement_status(stale_id, SettlementStatus::Queued, 100)
            .await
            .unwrap();

        db.enqueue_envelope(&mock_envelope(2), "GABC", 102, TxPriority::Normal, 900)
            .await
            .unwrap();
        db.set_settlement_status(fresh_id, SettlementStatus::Queued, 900)
            .await
            .unwrap();

        // now=1000, max_age=500 => cutoff=500; stale (enqueued_at=100) is past, fresh (900) is not
        let stale = db.sweep_stale_envelopes(500, 1000).await.unwrap();
        assert_eq!(stale, vec![stale_id]);
    }

    #[tokio::test]
    async fn test_sweep_ignores_fresh_envelopes() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let id = [1u8; 32];

        db.enqueue_envelope(&mock_envelope(1), "GABC", 101, TxPriority::Normal, 800)
            .await
            .unwrap();
        db.set_settlement_status(id, SettlementStatus::Queued, 800)
            .await
            .unwrap();

        let stale = db.sweep_stale_envelopes(500, 1000).await.unwrap();
        assert!(stale.is_empty());
    }

    #[tokio::test]
    async fn test_sweep_ignores_already_terminal_envelopes() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let settled_id = [1u8; 32];
        let failed_id = [2u8; 32];

        db.enqueue_envelope(&mock_envelope(1), "GABC", 101, TxPriority::Normal, 100)
            .await
            .unwrap();
        db.set_settlement_status(settled_id, SettlementStatus::Settled, 100)
            .await
            .unwrap();

        db.enqueue_envelope(&mock_envelope(2), "GABC", 102, TxPriority::Normal, 100)
            .await
            .unwrap();
        db.set_settlement_status(failed_id, SettlementStatus::Failed, 100)
            .await
            .unwrap();

        let stale = db.sweep_stale_envelopes(500, 1000).await.unwrap();
        assert!(stale.is_empty());
    }

    #[tokio::test]
    async fn test_sweep_is_idempotent() {
        let db = SyncEngineDb::init(":memory:").await.unwrap();
        let id = [1u8; 32];

        db.enqueue_envelope(&mock_envelope(1), "GABC", 101, TxPriority::Normal, 100)
            .await
            .unwrap();
        db.set_settlement_status(id, SettlementStatus::Queued, 100)
            .await
            .unwrap();

        let first = db.sweep_stale_envelopes(500, 1000).await.unwrap();
        assert!(!first.is_empty());

        let second = db.sweep_stale_envelopes(500, 1000).await.unwrap();
        assert!(second.is_empty());
    }
}
