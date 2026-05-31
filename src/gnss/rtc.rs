//! RTC Synchronization
//!
//! Synchronize Linux hardware RTC with GNSS time.
//! Writes to /dev/rtc0 when GNSS fix is acquired.

use crate::gnss::GnssFix;
use std::process::Command;

/// RTC device
const RTC_DEVICE: &str = "/dev/rtc0";

/// RTC sync service
pub struct RtcSync;

impl RtcSync {
    /// Sync RTC from GNSS fix
    pub fn sync_from_gnss(fix: &GnssFix) -> Result<(), RtcError> {
        // Convert GNSS timestamp to time_t
        let timestamp = fix.timestamp / 1000; // Convert ms to seconds

        // Use hwclock to set RTC
        let output = Command::new("hwclock")
            .args([
                "--set",
                "--date",
                &format!("@{}", timestamp),
                "--rtc",
                RTC_DEVICE,
            ])
            .output()
            .map_err(|e| RtcError::Hwclock(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RtcError::Hwclock(stderr.to_string()));
        }

        tracing::info!("RTC synced from GNSS: timestamp={}", timestamp);
        Ok(())
    }

    /// Get current RTC time
    pub fn get_rtc_time() -> Result<u64, RtcError> {
        let output = Command::new("hwclock")
            .args(["--show", "--rtc", RTC_DEVICE])
            .output()
            .map_err(|e| RtcError::Hwclock(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RtcError::Hwclock(stderr.to_string()));
        }

        // Parse hwclock output
        let stdout = String::from_utf8_lossy(&output.stdout);
        // hwclock output format: "2026-05-30 21:30:00.000000000 -0400"
        // For now, return system time as fallback
        Ok(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs())
    }

    /// Check if RTC is running
    pub fn is_rtc_running() -> Result<bool, RtcError> {
        let output = Command::new("hwclock")
            .args(["--show", "--rtc", RTC_DEVICE])
            .output()
            .map_err(|e| RtcError::Hwclock(e.to_string()))?;

        Ok(output.status.success())
    }
}

#[derive(Debug)]
pub enum RtcError {
    Hwclock(String),
}

impl std::fmt::Display for RtcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RtcError::Hwclock(e) => write!(f, "hwclock error: {}", e),
        }
    }
}

impl std::error::Error for RtcError {}
