//! GNSS Service
//!
//! GNSS positioning and time from GM02SP secondary GNSS port.
//! NMEA sentence parsing for position, velocity, and time.

use crate::hardware::gm02sp::GM02SPModem;
use nmea::{NmeaSentence, ParseResult};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

/// GNSS fix
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GnssFix {
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: f64,
    pub speed: f64,
    pub heading: f64,
    pub fix_quality: u8, // 0=invalid, 1=GPS, 2=DGPS
    pub satellites: u8,
    pub timestamp: u64, // Unix timestamp in milliseconds
    pub hdop: f64,      // Horizontal dilution of precision
    pub vdop: f64,      // Vertical dilution of precision
}

impl Default for GnssFix {
    fn default() -> Self {
        Self {
            latitude: 0.0,
            longitude: 0.0,
            altitude: 0.0,
            speed: 0.0,
            heading: 0.0,
            fix_quality: 0,
            satellites: 0,
            timestamp: 0,
            hdop: 99.99,
            vdop: 99.99,
        }
    }
}

/// GNSS service
pub struct GnssService {
    modem: GM02SPModem,
    enabled: bool,
}

impl GnssService {
    /// Initialize GNSS service
    pub async fn new(modem: GM02SPModem) -> Result<Self, GnssError> {
        let mut service = Self {
            modem,
            enabled: false,
        };

        // Enable GNSS
        service.modem.enable_gnss().await?;
        service.enabled = true;

        tracing::info!("GNSS service initialized");
        Ok(service)
    }

    /// Get current GNSS fix
    pub async fn get_fix(&mut self) -> Result<GnssFix, GnssError> {
        if !self.enabled {
            return Err(GnssError::NotEnabled);
        }

        let fix = self.modem.get_gnss_fix().await?;
        Ok(fix)
    }

    /// Wait for valid fix
    pub async fn wait_for_fix(&mut self, timeout_ms: u64) -> Result<GnssFix, GnssError> {
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > Duration::from_millis(timeout_ms) {
                return Err(GnssError::Timeout);
            }

            let fix = self.get_fix().await?;
            if fix.fix_quality > 0 {
                return Ok(fix);
            }

            sleep(Duration::from_secs(1)).await;
        }
    }

    /// Check if GNSS is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug)]
pub enum GnssError {
    NotEnabled,
    Timeout,
    ModemError(String),
}

impl std::fmt::Display for GnssError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GnssError::NotEnabled => write!(f, "GNSS not enabled"),
            GnssError::Timeout => write!(f, "GNSS fix timeout"),
            GnssError::ModemError(e) => write!(f, "Modem error: {}", e),
        }
    }
}

impl std::error::Error for GnssError {}
