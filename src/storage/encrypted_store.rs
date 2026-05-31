//! Encrypted NVMe Store with dm-crypt Integration
//!
//! Manages dm-crypt encrypted NVMe volume for persistent bi-temporal storage.
//! Handles volume open/close, WAL management, and snapshot persistence.
//!
//! dm-crypt volume: /dev/mapper/encrypted_storage
//! Raw device: /dev/nvme0n1 (WD SN740 256GB)
//! Config partition: /dev/mapper/encrypted_config

use crate::bitemporal::BiTemporalStore;
use crate::protocol::Message;
use crate::snapshot::Snapshot;
use crate::storage::wal::Wal;
use std::path::Path;
use std::process::Command;
use tokio::sync::Arc;

/// dm-crypt mapper name
const CRYPT_MAPPER: &str = "encrypted_storage";

/// Raw NVMe device
const NVME_DEVICE: &str = "/dev/nvme0n1";

/// Encrypted storage mount point
const STORAGE_MOUNT: &str = "/mnt/encrypted_storage";

/// Config partition mapper
const CONFIG_MAPPER: &str = "encrypted_config";

/// Config partition offset (last 1GB of NVMe)
const CONFIG_OFFSET_SECTORS: u64 = 488384000; // 256GB - 1GB in 512-byte sectors

/// Encrypted store
pub struct EncryptedStore {
    wal: Arc<Wal>,
    bitemporal: Arc<BiTemporalStore>,
    mount_point: String,
}

impl EncryptedStore {
    /// Open encrypted store (opens dm-crypt volume if needed)
    pub async fn open() -> Result<Self, StoreError> {
        // Ensure dm-crypt volume is open
        Self::open_crypt_volume()?;

        // Ensure mount point exists
        Self::ensure_mount_point()?;

        // Mount if not mounted
        Self::mount_volume()?;

        // Open WAL
        let wal_path = format!("{}/float_wal.bin", STORAGE_MOUNT);
        let wal = Arc::new(Wal::open(&wal_path).await?);

        // Replay WAL into bi-temporal store
        let messages = wal.replay().await?;
        let bitemporal = Arc::new(BiTemporalStore::new(10000));

        for msg in messages {
            bitemporal.store(msg).await;
        }

        tracing::info!("Encrypted store opened at {}", STORAGE_MOUNT);
        tracing::info!("Replayed {} messages from WAL", messages.len());

        Ok(Self {
            wal,
            bitemporal,
            mount_point: STORAGE_MOUNT.to_string(),
        })
    }

    /// Open dm-crypt volume (cryptsetup luksOpen)
    fn open_crypt_volume() -> Result<(), StoreError> {
        // Check if already open
        if Path::new(&format!("/dev/mapper/{}", CRYPT_MAPPER)).exists() {
            return Ok(());
        }

        // TODO: Get passphrase from ATECC608B or secure boot
        // For now, use environment variable (production: derive from ATECC608B)
        let passphrase = std::env::var("FLOAT_CRYPT_PASSPHRASE")
            .unwrap_or_else(|_| "default_passphrase".to_string());

        let output = Command::new("cryptsetup")
            .args(["luksOpen", NVME_DEVICE, CRYPT_MAPPER])
            .env("CRYPTTAB_KEY", &passphrase)
            .output()
            .map_err(|e| StoreError::CryptSetup(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(StoreError::CryptSetup(stderr.to_string()));
        }

        tracing::info!("dm-crypt volume opened: {}", CRYPT_MAPPER);
        Ok(())
    }

    /// Ensure mount point directory exists
    fn ensure_mount_point() -> Result<(), StoreError> {
        if !Path::new(STORAGE_MOUNT).exists() {
            std::fs::create_dir_all(STORAGE_MOUNT)
                .map_err(|e| StoreError::MountPoint(e.to_string()))?;
        }
        Ok(())
    }

    /// Mount encrypted volume
    fn mount_volume() -> Result<(), StoreError> {
        // Check if already mounted
        let output = Command::new("mount")
            .arg("--show")
            .arg(format!("/dev/mapper/{}", CRYPT_MAPPER))
            .output()
            .map_err(|e| StoreError::Mount(e.to_string()))?;

        if output.status.success() {
            return Ok(());
        }

        // Mount ext4 filesystem
        let output = Command::new("mount")
            .args([
                format!("/dev/mapper/{}", CRYPT_MAPPER).as_str(),
                STORAGE_MOUNT,
            ])
            .output()
            .map_err(|e| StoreError::Mount(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(StoreError::Mount(stderr.to_string()));
        }

        tracing::info!("Encrypted volume mounted at {}", STORAGE_MOUNT);
        Ok(())
    }

    /// Store message with persistence to WAL
    pub async fn store(&self, message: Message) -> Result<(), StoreError> {
        // Append to WAL
        self.wal.append(message.clone()).await?;

        // Store in in-memory bi-temporal store
        self.bitemporal.store(message).await;

        Ok(())
    }

    /// Store batch of messages with single WAL write
    pub async fn store_batch(&self, messages: &[Message]) -> Result<(), StoreError> {
        for msg in messages {
            self.store(msg.clone()).await?;
        }
        Ok(())
    }

    /// Persist snapshot to disk
    pub async fn persist_snapshot(&self, snapshot: &Snapshot) -> Result<(), StoreError> {
        let snapshot_path = format!(
            "{}/snapshots/{}.bin",
            self.mount_point, snapshot.id
        );

        // Ensure snapshots directory exists
        let snapshots_dir = format!("{}/snapshots", self.mount_point);
        if !Path::new(&snapshots_dir).exists() {
            std::fs::create_dir_all(&snapshots_dir)
                .map_err(|e| StoreError::FileWrite(e.to_string()))?;
        }

        // Serialize snapshot
        let data = bincode::serialize(snapshot)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;

        // Write to file
        std::fs::write(&snapshot_path, data)
            .map_err(|e| StoreError::FileWrite(e.to_string()))?;

        // Sync to disk
        Command::new("sync")
            .status()
            .map_err(|e| StoreError::Sync(e.to_string()))?;

        Ok(())
    }

    /// Load snapshot from disk
    pub async fn load_snapshot(&self, snapshot_id: &str) -> Result<Option<Snapshot>, StoreError> {
        let snapshot_path = format!(
            "{}/snapshots/{}.bin",
            self.mount_point, snapshot_id
        );

        if !Path::new(&snapshot_path).exists() {
            return Ok(None);
        }

        let data = std::fs::read(&snapshot_path)
            .map_err(|e| StoreError::FileRead(e.to_string()))?;

        let snapshot = bincode::deserialize(&data)
            .map_err(|e| StoreError::Deserialization(e.to_string()))?;

        Ok(Some(snapshot))
    }

    /// Get bi-temporal store reference
    pub fn bitemporal(&self) -> Arc<BiTemporalStore> {
        self.bitemporal.clone()
    }

    /// Compact WAL (remove old entries)
    pub async fn compact_wal(&self, retain_ms: u64) -> Result<(), StoreError> {
        self.wal.compact(retain_ms).await?;
        Ok(())
    }

    /// Close encrypted store (unmount and close dm-crypt)
    pub async fn close(&self) -> Result<(), StoreError> {
        // Unmount
        let output = Command::new("umount")
            .arg(STORAGE_MOUNT)
            .output()
            .map_err(|e| StoreError::Unmount(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!("Unmount warning: {}", stderr);
        }

        // Close dm-crypt
        let output = Command::new("cryptsetup")
            .args(["luksClose", CRYPT_MAPPER])
            .output()
            .map_err(|e| StoreError::CryptSetup(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(StoreError::CryptSetup(stderr.to_string()));
        }

        tracing::info!("Encrypted store closed");
        Ok(())
    }

    /// Open config partition (separate dm-crypt mapper for config)
    pub fn open_config_partition() -> Result<(), StoreError> {
        if Path::new(&format!("/dev/mapper/{}", CONFIG_MAPPER)).exists() {
            return Ok(());
        }

        let passphrase = std::env::var("FLOAT_CONFIG_PASSPHRASE")
            .unwrap_or_else(|_| "config_passphrase".to_string());

        let output = Command::new("cryptsetup")
            .args([
                "open",
                "--type",
                "plain",
                "--key-file",
                "-",
                NVME_DEVICE,
                CONFIG_MAPPER,
            ])
            .arg(format!("--offset={}", CONFIG_OFFSET_SECTORS))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| StoreError::CryptSetup(e.to_string()))?
            .stdin
            .unwrap()
            .write_all(passphrase.as_bytes())
            .map_err(|e| StoreError::CryptSetup(e.to_string()))?;

        tracing::info!("Config partition opened: {}", CONFIG_MAPPER);
        Ok(())
    }
}

#[derive(Debug)]
pub enum StoreError {
    CryptSetup(String),
    MountPoint(String),
    Mount(String),
    Unmount(String),
    FileRead(String),
    FileWrite(String),
    Serialization(String),
    Deserialization(String),
    Sync(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::CryptSetup(e) => write!(f, "cryptsetup error: {}", e),
            StoreError::MountPoint(e) => write!(f, "Mount point error: {}", e),
            StoreError::Mount(e) => write!(f, "Mount error: {}", e),
            StoreError::Unmount(e) => write!(f, "Unmount error: {}", e),
            StoreError::FileRead(e) => write!(f, "File read error: {}", e),
            StoreError::FileWrite(e) => write!(f, "File write error: {}", e),
            StoreError::Serialization(e) => write!(f, "Serialization error: {}", e),
            StoreError::Deserialization(e) => write!(f, "Deserialization error: {}", e),
            StoreError::Sync(e) => write!(f, "Sync error: {}", e),
        }
    }
}

impl std::error::Error for StoreError {}
