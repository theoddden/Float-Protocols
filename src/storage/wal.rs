//! Write-Ahead Log (WAL) for Persistent Bi-Temporal Storage
//!
//! Append-only WAL with checksums for reliable bi-temporal record storage.
//! Ensures records survive power cycles and can be replayed on startup.
//!
//! Format:
//! - Header: 16 bytes (magic, version, sequence number)
//! - Entry: 4-byte length + data + 4-byte CRC32
//! - Footer: 8-byte magic for integrity check

use crate::protocol::Message;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

/// WAL file magic number
const WAL_MAGIC: &[u8; 8] = b"FLOATWAL";

/// WAL version
const WAL_VERSION: u16 = 1;

/// WAL header size
const WAL_HEADER_SIZE: usize = 16;

/// WAL entry header size (length field)
const WAL_ENTRY_HEADER_SIZE: usize = 4;

/// WAL entry footer size (CRC32)
const WAL_ENTRY_FOOTER_SIZE: usize = 4;

/// WAL file path
const WAL_PATH: &str = "/mnt/encrypted_storage/float_wal.bin";

/// WAL header
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalHeader {
    magic: [u8; 8],
    version: u16,
    sequence: u64,
}

impl WalHeader {
    fn new(sequence: u64) -> Self {
        let mut magic = [0u8; 8];
        magic.copy_from_slice(WAL_MAGIC);
        Self {
            magic,
            version: WAL_VERSION,
            sequence,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(WAL_HEADER_SIZE);
        bytes.extend_from_slice(&self.magic);
        bytes.extend_from_slice(&self.version.to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < WAL_HEADER_SIZE {
            return None;
        }

        let magic = bytes[0..8].try_into().ok()?;
        if magic != *WAL_MAGIC {
            return None;
        }

        let version = u16::from_be_bytes([bytes[8], bytes[9]]);
        let sequence = u64::from_be_bytes([
            bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17],
        ]);

        Some(Self {
            magic,
            version,
            sequence,
        })
    }
}

/// WAL entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub message: Message,
    pub timestamp_ms: u64,
}

impl WalEntry {
    fn new(sequence: u64, message: Message) -> Self {
        Self {
            sequence,
            message,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, WalError> {
        let data = bincode::serialize(self).map_err(|e| WalError::Serialization(e.to_string()))?;
        Ok(data)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, WalError> {
        bincode::deserialize(bytes).map_err(|e| WalError::Deserialization(e.to_string()))
    }
}

/// Write-Ahead Log
pub struct Wal {
    file: Arc<Mutex<File>>,
    sequence: Arc<Mutex<u64>>,
    path: String,
}

impl Wal {
    /// Open or create WAL file
    pub fn open(path: &str) -> Result<Self, WalError> {
        let path = path.to_string();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| WalError::FileOpen(e.to_string()))?;

        let mut wal = Self {
            file: Arc::new(Mutex::new(file)),
            sequence: Arc::new(Mutex::new(0)),
            path,
        };

        // Initialize header if new file
        if wal.is_new_file()? {
            wal.write_header()?;
        }

        // Load sequence number from existing file
        wal.load_sequence()?;

        tracing::info!("WAL opened at {}", path);
        Ok(wal)
    }

    /// Check if file is new (empty or no valid header)
    fn is_new_file(&self) -> Result<bool, WalError> {
        let file = self.file.lock().await;
        let metadata = file
            .get_ref()
            .metadata()
            .map_err(|e| WalError::FileRead(e.to_string()))?;
        Ok(metadata.len() == 0)
    }

    /// Write WAL header
    async fn write_header(&self) -> Result<(), WalError> {
        let header = WalHeader::new(0);
        let header_bytes = header.to_bytes();

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| WalError::FileSeek(e.to_string()))?;
        file.write_all(&header_bytes)
            .map_err(|e| WalError::FileWrite(e.to_string()))?;
        file.flush()
            .map_err(|e| WalError::FileWrite(e.to_string()))?;

        Ok(())
    }

    /// Load sequence number from WAL
    async fn load_sequence(&self) -> Result<(), WalError> {
        let file = self.file.lock().await;
        let mut reader = BufReader::new(file.get_ref());

        // Read header
        let mut header_bytes = vec![0u8; WAL_HEADER_SIZE];
        reader
            .read_exact(&mut header_bytes)
            .map_err(|e| WalError::FileRead(e.to_string()))?;

        let header = WalHeader::from_bytes(&header_bytes).ok_or(WalError::InvalidHeader)?;

        *self.sequence.lock().await = header.sequence;

        // Scan entries to find highest sequence
        loop {
            let mut len_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_be_bytes(len_bytes) as usize;
                    let mut data = vec![0u8; len];
                    reader
                        .read_exact(&mut data)
                        .map_err(|e| WalError::FileRead(e.to_string()))?;

                    let mut crc_bytes = [0u8; WAL_ENTRY_FOOTER_SIZE];
                    reader
                        .read_exact(&mut crc_bytes)
                        .map_err(|e| WalError::FileRead(e.to_string()))?;

                    // Verify CRC
                    let computed_crc = compute_crc32(&data);
                    let stored_crc = u32::from_be_bytes(crc_bytes);
                    if computed_crc != stored_crc {
                        return Err(WalError::CrcMismatch);
                    }

                    // Parse entry to get sequence
                    if let Ok(entry) = WalEntry::from_bytes(&data) {
                        *self.sequence.lock().await = entry.sequence;
                    }
                }
                Err(_) => break, // EOF or error
            }
        }

        Ok(())
    }

    /// Append entry to WAL
    pub async fn append(&self, message: Message) -> Result<u64, WalError> {
        let mut sequence = self.sequence.lock().await;
        *sequence += 1;
        let seq = *sequence;
        drop(sequence);

        let entry = WalEntry::new(seq, message);
        let data = entry.to_bytes()?;

        // Compute CRC
        let crc = compute_crc32(&data);

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::End(0))
            .map_err(|e| WalError::FileSeek(e.to_string()))?;

        // Write length
        file.write_all(&(data.len() as u32).to_be_bytes())
            .map_err(|e| WalError::FileWrite(e.to_string()))?;

        // Write data
        file.write_all(&data).map_err(|e| WalError::FileWrite(e.to_string()))?;

        // Write CRC
        file.write_all(&crc.to_be_bytes()).map_err(|e| WalError::FileWrite(e.to_string()))?;

        // Sync to disk
        file.flush()
            .map_err(|e| WalError::FileWrite(e.to_string()))?;
        file.get_ref()
            .sync_all()
            .map_err(|e| WalError::FileWrite(e.to_string()))?;

        Ok(seq)
    }

    /// Replay WAL from beginning to end
    pub async fn replay(&self) -> Result<Vec<Message>, WalError> {
        let file = self.file.lock().await;
        let mut reader = BufReader::new(file.get_ref());

        // Skip header
        let mut header_bytes = vec![0u8; WAL_HEADER_SIZE];
        reader.read_exact(&mut header_bytes).map_err(|e| WalError::FileRead(e.to_string()))?;

        let mut messages = Vec::new();

        loop {
            let mut len_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {
                    let len = u32::from_be_bytes(len_bytes) as usize;
                    let mut data = vec![0u8; len];
                    reader
                        .read_exact(&mut data)
                        .map_err(|e| WalError::FileRead(e.to_string()))?;

                    let mut crc_bytes = [0u8; WAL_ENTRY_FOOTER_SIZE];
                    reader
                        .read_exact(&mut crc_bytes)
                        .map_err(|e| WalError::FileRead(e.to_string()))?;

                    // Verify CRC
                    let computed_crc = compute_crc32(&data);
                    let stored_crc = u32::from_be_bytes(crc_bytes);
                    if computed_crc != stored_crc {
                        tracing::warn!("CRC mismatch in WAL entry, skipping");
                        continue;
                    }

                    // Parse entry
                    match WalEntry::from_bytes(&data) {
                        Ok(entry) => messages.push(entry.message),
                        Err(e) => {
                            tracing::warn!("Failed to parse WAL entry: {}", e);
                            continue;
                        }
                    }
                }
                Err(_) => break, // EOF
            }
        }

        tracing::info!("WAL replayed {} messages", messages.len());
        Ok(messages)
    }

    /// Compact WAL (remove old entries)
    pub async fn compact(&self, retain_ms: u64) -> Result<(), WalError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let cutoff = now - retain_ms;

        // Replay and filter
        let all_messages = self.replay().await?;
        let retained: Vec<WalEntry> = all_messages
            .into_iter()
            .filter(|m| m.timestamp_ms >= cutoff)
            .enumerate()
            .map(|(i, m)| WalEntry::new(i as u64 + 1, m))
            .collect();

        // Truncate file
        let mut file = self.file.lock().await;
        file.set_len(0)
            .map_err(|e| WalError::FileWrite(e.to_string()))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| WalError::FileSeek(e.to_string()))?;

        // Write header
        let header = WalHeader::new(retained.len() as u64);
        file.write_all(&header.to_bytes())
            .map_err(|e| WalError::FileWrite(e.to_string()))?;

        // Write retained entries
        for entry in retained {
            let data = entry.to_bytes()?;
            let crc = compute_crc32(&data);

            file.write_all(&(data.len() as u32).to_be_bytes())
                .map_err(|e| WalError::FileWrite(e.to_string()))?;
            file.write_all(&data)
                .map_err(|e| WalError::FileWrite(e.to_string()))?;
            file.write_all(&crc.to_be_bytes())
                .map_err(|e| WalError::FileWrite(e.to_string()))?;
        }

        file.flush()
            .map_err(|e| WalError::FileWrite(e.to_string()))?;
        file.get_ref()
            .sync_all()
            .map_err(|e| WalError::FileWrite(e.to_string()))?;

        // Update sequence
        *self.sequence.lock().await = retained.len() as u64;

        tracing::info!("WAL compacted, retained {} entries", retained.len());
        Ok(())
    }

    /// Get current sequence number
    pub async fn sequence(&self) -> u64 {
        *self.sequence.lock().await
    }
}

/// Compute CRC32 checksum
fn compute_crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[derive(Debug)]
pub enum WalError {
    FileOpen(String),
    FileRead(String),
    FileWrite(String),
    FileSeek(String),
    Serialization(String),
    Deserialization(String),
    InvalidHeader,
    CrcMismatch,
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::FileOpen(e) => write!(f, "Failed to open WAL file: {}", e),
            WalError::FileRead(e) => write!(f, "Failed to read WAL file: {}", e),
            WalError::FileWrite(e) => write!(f, "Failed to write WAL file: {}", e),
            WalError::FileSeek(e) => write!(f, "Failed to seek WAL file: {}", e),
            WalError::Serialization(e) => write!(f, "Serialization error: {}", e),
            WalError::Deserialization(e) => write!(f, "Deserialization error: {}", e),
            WalError::InvalidHeader => write!(f, "Invalid WAL header"),
            WalError::CrcMismatch => write!(f, "CRC mismatch in WAL entry"),
        }
    }
}

impl std::error::Error for WalError {}
