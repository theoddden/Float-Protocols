//! Protocol definitions for dead zone communication systems
//!
//! Supports: Iridium SBD, Inmarsat C, VSAT, HF/VHF, RockBLOCK

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Protocol {
    IridiumSBD,
    InmarsatC,
    VSAT,
    HFVHF,
    RockBLOCK,
    Samsara,
    NIDD,
    ASTSpaceMobile,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Priority {
    Emergency = 0,
    SafetyCritical = 1,
    Operational = 2,
    Diagnostic = 3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub protocol: Protocol,
    pub data: Bytes,
    pub priority: Priority,
    pub timestamp: u64, // Unix timestamp in milliseconds (legacy, for compatibility)
    // Bi-temporal timestamps
    pub t_event: u64,  // Valid Time: When sensor recorded event in physical world
    pub t_system: u64, // Transaction Time: When system first learned about event
}

impl Message {
    pub fn new(protocol: Protocol, data: Bytes, priority: Priority) -> Self {
        // Packet size guard: prevent OOM from oversized packets
        let max_size = protocol.max_message_size();
        if data.len() > max_size {
            tracing::warn!(
                protocol = %protocol,
                data_size = data.len(),
                max_size = max_size,
                "Packet exceeds maximum size, truncating"
            );
        }

        let now = Self::now_ms();
        Self {
            protocol,
            data,
            priority,
            timestamp: now,
            t_event: now,  // Default: assume event happened now
            t_system: now, // Default: system learned about it now
        }
    }

    /// Create message with explicit bi-temporal timestamps
    pub fn new_with_temporal(
        protocol: Protocol,
        data: Bytes,
        priority: Priority,
        t_event: u64,
        t_system: u64,
    ) -> Self {
        Self {
            protocol,
            data,
            priority,
            timestamp: t_system, // Legacy field uses system time
            t_event,
            t_system,
        }
    }

    /// Calculate spread between event time and system time (in milliseconds)
    /// This is a deterministic mark for insurance underwriting and trade compliance
    pub fn spread_ms(&self) -> i64 {
        self.t_system as i64 - self.t_event as i64
    }

    /// Get spread in seconds
    pub fn spread_seconds(&self) -> f64 {
        self.spread_ms() as f64 / 1000.0
    }

    /// Check if this message was delayed (positive spread)
    pub fn is_delayed(&self) -> bool {
        self.spread_ms() > 0
    }

    /// Check if this message was from the future (negative spread)
    pub fn is_future(&self) -> bool {
        self.spread_ms() < 0
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
        // For no-std targets, would need external time source
        0
    }

    pub fn is_emergency(&self) -> bool {
        self.priority == Priority::Emergency
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Protocol::IridiumSBD => write!(f, "IridiumSBD"),
            Protocol::InmarsatC => write!(f, "InmarsatC"),
            Protocol::VSAT => write!(f, "VSAT"),
            Protocol::HFVHF => write!(f, "HFVHF"),
            Protocol::RockBLOCK => write!(f, "RockBLOCK"),
            Protocol::Samsara => write!(f, "Samsara"),
            Protocol::NIDD => write!(f, "NIDD"),
            Protocol::ASTSpaceMobile => write!(f, "ASTSpaceMobile"),
        }
    }
}

// Protocol-specific constraints
impl Protocol {
    pub fn max_message_size(&self) -> usize {
        match self {
            Protocol::IridiumSBD => 340,           // Iridium SBD max
            Protocol::InmarsatC => 128,            // Inmarsat C max
            Protocol::VSAT => 65536,               // VSAT variable (64KB typical)
            Protocol::HFVHF => 1024,               // HF/VHF typical
            Protocol::RockBLOCK => 340,            // RockBLOCK same as Iridium SBD
            Protocol::Samsara => 1048576,          // Samsara cellular broadband (1MB typical)
            Protocol::NIDD => 1600,               // 3GPP TS 24.582 NIDD max payload
            Protocol::ASTSpaceMobile => 120000000, // 120 Mbps max theoretical
        }
    }

    pub fn requires_compression(&self) -> bool {
        matches!(self, Protocol::VSAT | Protocol::HFVHF | Protocol::NIDD)
    }
}
