//! Encrypted NVMe Storage Layer
//!
//! Persistent storage for bi-temporal records and snapshots.
//! dm-crypt encrypted NVMe volume with append-only WAL.
//!
//! Features:
//! - Append-only WAL with checksums
//! - WAL replay on startup
//! - Snapshot persistence
//! - Periodic compaction
//! - dm-crypt volume management

pub mod encrypted_store;
pub mod wal;

pub use encrypted_store::EncryptedStore;
pub use wal::WalEntry;
