#![no_main]

//! Fuzzes `rmp_serde::from_slice::<TransactionEnvelope>`, the exact call
//! `SyncEngineDb` (see `src/storage/db.rs`) uses to deserialize envelope
//! bytes read back from its own SQLite storage. Those bytes are meant to be
//! ones this crate wrote itself, but per the eventual encryption-at-rest and
//! database export/import work, they may end up read from a removable or
//! previously-exported file — corrupted or adversarial input should be
//! rejected with an error, never cause a panic or memory-safety issue.

use libfuzzer_sys::fuzz_target;
use stellarconduit_core::message::types::TransactionEnvelope;

fuzz_target!(|data: &[u8]| {
    let _ = rmp_serde::from_slice::<TransactionEnvelope>(data);
});
