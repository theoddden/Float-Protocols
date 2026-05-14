//! Clock reconciliation algorithm for device clock drift
//!
//! Solves the problem where device internal clocks drift from ASTS network time
//! during dead zone bursts. Tracks offset and applies corrections.

use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct ClockOffset {
    pub device_id: u64,
    pub offset_ms: i64,    // Device time minus network time
    pub last_updated: u64, // Unix timestamp in milliseconds
    pub confidence: f64,   // 0.0 to 1.0
}

impl ClockOffset {
    pub fn new(device_id: u64, offset_ms: i64, confidence: f64) -> Self {
        Self {
            device_id,
            offset_ms,
            last_updated: Self::now_ms(),
            confidence,
        }
    }

    #[cfg(feature = "std")]
    fn now_ms() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[cfg(not(feature = "std"))]
    fn now_ms() -> u64 {
        0
    }

    pub fn is_stale(&self, max_age: Duration) -> bool {
        let age_ms = Self::now_ms() - self.last_updated;
        age_ms > max_age.as_millis() as u64
    }
}

pub struct ClockReconciler {
    offsets: Vec<ClockOffset>,
    max_offsets: usize,
    max_age: Duration,
    network_time_source: NetworkTimeSource,
}

#[derive(Debug, Clone, Copy)]
pub enum NetworkTimeSource {
    ASTS,       // AST SpaceMobile network time
    NTP,        // Network Time Protocol
    GPS,        // GPS time
    LocalClock, // Local system clock
}

impl ClockReconciler {
    pub fn new(
        max_offsets: usize,
        max_age: Duration,
        network_time_source: NetworkTimeSource,
    ) -> Self {
        Self {
            offsets: Vec::with_capacity(max_offsets),
            max_offsets,
            max_age,
            network_time_source,
        }
    }

    /// Register a device clock offset measurement
    pub fn register_offset(&mut self, offset: ClockOffset) {
        // Remove existing offset for this device
        self.offsets.retain(|o| o.device_id != offset.device_id);

        // Add new offset
        self.offsets.push(offset);

        // Evict oldest if at capacity
        if self.offsets.len() > self.max_offsets {
            self.offsets.remove(0);
        }
    }

    /// Get clock offset for a device
    pub fn get_offset(&self, device_id: u64) -> Option<ClockOffset> {
        self.offsets
            .iter()
            .find(|o| o.device_id == device_id)
            .filter(|o| !o.is_stale(self.max_age))
            .copied()
    }

    /// Reconcile device timestamp to network time
    pub fn reconcile(&self, device_id: u64, device_timestamp_ms: u64) -> Option<u64> {
        let offset = self.get_offset(device_id)?;

        // Apply offset: network_time = device_time - offset
        let network_time_ms = device_timestamp_ms as i64 - offset.offset_ms;

        if network_time_ms < 0 {
            return None; // Timestamp would be negative
        }

        Some(network_time_ms as u64)
    }

    /// Reconcile with confidence filtering
    pub fn reconcile_with_confidence(
        &self,
        device_id: u64,
        device_timestamp_ms: u64,
        min_confidence: f64,
    ) -> Option<u64> {
        let offset = self.get_offset(device_id)?;

        if offset.confidence < min_confidence {
            return None;
        }

        let network_time_ms = device_timestamp_ms as i64 - offset.offset_ms;

        if network_time_ms < 0 {
            return None;
        }

        Some(network_time_ms as u64)
    }

    /// Calculate clock offset from device timestamp and network timestamp
    pub fn calculate_offset(
        _device_id: u64,
        device_timestamp_ms: u64,
        network_timestamp_ms: u64,
    ) -> i64 {
        device_timestamp_ms as i64 - network_timestamp_ms as i64
    }

    /// Get network time from configured source
    pub fn get_network_time(&self) -> u64 {
        match self.network_time_source {
            NetworkTimeSource::ASTS => self.get_asts_time(),
            NetworkTimeSource::NTP => self.get_ntp_time(),
            NetworkTimeSource::GPS => self.get_gps_time(),
            NetworkTimeSource::LocalClock => Self::now_ms(),
        }
    }

    #[cfg(feature = "std")]
    fn get_asts_time(&self) -> u64 {
        // TODO: Implement actual ASTS network time query
        Self::now_ms()
    }

    #[cfg(not(feature = "std"))]
    fn get_asts_time(&self) -> u64 {
        Self::now_ms()
    }

    #[cfg(feature = "std")]
    fn get_ntp_time(&self) -> u64 {
        // TODO: Implement NTP time query
        Self::now_ms()
    }

    #[cfg(not(feature = "std"))]
    fn get_ntp_time(&self) -> u64 {
        Self::now_ms()
    }

    #[cfg(feature = "std")]
    fn get_gps_time(&self) -> u64 {
        // TODO: Implement GPS time query
        Self::now_ms()
    }

    #[cfg(not(feature = "std"))]
    fn get_gps_time(&self) -> u64 {
        Self::now_ms()
    }

    /// Update all offsets (periodic maintenance)
    pub fn update_offsets(&mut self) {
        // Remove stale offsets
        self.offsets.retain(|o| !o.is_stale(self.max_age));
    }

    /// Get statistics about clock offsets
    pub fn stats(&self) -> ClockStats {
        let total_offsets = self.offsets.len();
        let stale_offsets = self
            .offsets
            .iter()
            .filter(|o| o.is_stale(self.max_age))
            .count();
        let avg_offset = if self.offsets.is_empty() {
            0
        } else {
            let sum: i64 = self.offsets.iter().map(|o| o.offset_ms).sum();
            sum / self.offsets.len() as i64
        };

        ClockStats {
            total_offsets,
            stale_offsets,
            avg_offset_ms: avg_offset,
        }
    }

    fn now_ms() -> u64 {
        #[cfg(feature = "std")]
        {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        }

        #[cfg(not(feature = "std"))]
        {
            0
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClockStats {
    pub total_offsets: usize,
    pub stale_offsets: usize,
    pub avg_offset_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_reconciliation() {
        let mut reconciler =
            ClockReconciler::new(10, Duration::from_secs(300), NetworkTimeSource::LocalClock);

        // Device clock is 1000ms ahead of network time
        let offset = ClockOffset::new(1, 1000, 0.9);
        reconciler.register_offset(offset);

        // Reconcile device timestamp
        let device_time = 10000;
        let network_time = reconciler.reconcile(1, device_time).unwrap();

        // network_time = device_time - offset = 10000 - 1000 = 9000
        assert_eq!(network_time, 9000);
    }

    #[test]
    fn test_confidence_filtering() {
        let mut reconciler =
            ClockReconciler::new(10, Duration::from_secs(300), NetworkTimeSource::LocalClock);

        let offset = ClockOffset::new(1, 1000, 0.5); // Low confidence
        reconciler.register_offset(offset);

        let device_time = 10000;
        let result = reconciler.reconcile_with_confidence(1, device_time, 0.8);

        // Should return None due to low confidence
        assert!(result.is_none());
    }
}
