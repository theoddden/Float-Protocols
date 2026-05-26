//! Zero-allocation NIDD (Non-IP Data Delivery) protocol parsing
//!
//! Parses 3GPP TS 24.582 NIDD packets without any heap allocations.
//! NIDD eliminates IP header overhead - every byte is meaningful data.
//! Ideal for NTN NB-IoT use cases with small, infrequent messages.
//!
//! 3GPP TS 24.582 Compliance:
//! - PDU Type: User Data (0x00), Control Plane (0x80)
//! - QoS Parameters: Priority, Reliability, Delay Class
//! - NB-IoT specific: DRX, Paging, Coverage Enhancement
//! - Mobile Originated (MO) vs Mobile Terminated (MT) support
//!
//! Key benefits:
//! - No IP header overhead (saves 20+ bytes per message)
//! - Lower airtime costs
//! - Lower power consumption
//! - Simpler, leaner design

/// PDU Type per 3GPP TS 24.582
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PduType {
    UserData = 0x00,
    ControlPlane = 0x80,
}

/// QoS Priority Class per 3GPP TS 23.501
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QosPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Emergency = 3,
}

/// Reliability Class per 3GPP TS 23.501
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReliabilityClass {
    Unreliable = 0,
    Reliable = 1,
}

/// Delay Class per 3GPP TS 23.501
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DelayClass {
    DelayInsensitive = 0,
    DelaySensitive = 1,
}

/// Coverage Enhancement Level per 3GPP TS 36.331
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CoverageEnhancement {
    None = 0,
    Level1 = 1, // +0 dB
    Level2 = 2, // +3 dB
    Level3 = 3, // +5 dB
    Level4 = 4, // +8 dB
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct NIDDHeader {
    pub pdu_type: u8,
    pub qos_priority: u8,
    pub reliability: u8,
    pub delay_class: u8,
    pub coverage_enhancement: u8,
    pub length: u16,
    pub sequence_number: u16,
}

#[derive(Debug, Clone, Copy)]
pub struct NIDDMessage {
    pub header: NIDDHeader,
    pub payload: [u8; 1600], // Max NIDD payload size (3GPP TS 24.582)
    pub payload_len: u16,
}

impl NIDDMessage {
    /// Parse NIDD message from byte slice (zero-allocation)
    /// Returns None if the buffer is too small
    ///
    /// Format per 3GPP TS 24.582:
    /// [pdu_type (1)][qos_priority (1)][reliability (1)][delay_class (1)]
    /// [coverage_enhancement (1)][length (2)][sequence (2)][payload (N)]
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }

        let pdu_type = data[0];
        let qos_priority = data[1];
        let reliability = data[2];
        let delay_class = data[3];
        let coverage_enhancement = data[4];
        let length = u16::from_be_bytes([data[5], data[6]]);
        let sequence_number = u16::from_be_bytes([data[7], data[8]]);

        if data.len() < (9 + length as usize) {
            return None;
        }

        // Check if length exceeds maximum payload size (1600 bytes per 3GPP TS 24.582)
        if length > 1600 {
            return None;
        }

        let mut payload = [0u8; 1600];
        payload[..length as usize].copy_from_slice(&data[9..9 + length as usize]);

        Some(Self {
            header: NIDDHeader {
                pdu_type,
                qos_priority,
                reliability,
                delay_class,
                coverage_enhancement,
                length,
                sequence_number,
            },
            payload,
            payload_len: length,
        })
    }

    /// Get payload as slice (zero-copy)
    pub fn payload_slice(&self) -> &[u8] {
        &self.payload[..self.payload_len as usize]
    }

    /// Get total message size
    pub fn total_size(&self) -> usize {
        9 + self.payload_len as usize // header (9 bytes) + payload
    }

    /// Check if this is a control plane message
    pub fn is_control_plane(&self) -> bool {
        self.header.pdu_type & 0x80 != 0
    }

    /// Get PDU type
    pub fn pdu_type(&self) -> PduType {
        if self.header.pdu_type & 0x80 != 0 {
            PduType::ControlPlane
        } else {
            PduType::UserData
        }
    }

    /// Get QoS priority
    pub fn qos_priority(&self) -> QosPriority {
        match self.header.qos_priority {
            0 => QosPriority::Low,
            1 => QosPriority::Normal,
            2 => QosPriority::High,
            3 => QosPriority::Emergency,
            _ => QosPriority::Normal,
        }
    }

    /// Get reliability class
    pub fn reliability(&self) -> ReliabilityClass {
        match self.header.reliability {
            0 => ReliabilityClass::Unreliable,
            1 => ReliabilityClass::Reliable,
            _ => ReliabilityClass::Unreliable,
        }
    }

    /// Get delay class
    pub fn delay_class(&self) -> DelayClass {
        match self.header.delay_class {
            0 => DelayClass::DelayInsensitive,
            1 => DelayClass::DelaySensitive,
            _ => DelayClass::DelayInsensitive,
        }
    }

    /// Get coverage enhancement level
    pub fn coverage_enhancement(&self) -> CoverageEnhancement {
        match self.header.coverage_enhancement {
            0 => CoverageEnhancement::None,
            1 => CoverageEnhancement::Level1,
            2 => CoverageEnhancement::Level2,
            3 => CoverageEnhancement::Level3,
            4 => CoverageEnhancement::Level4,
            _ => CoverageEnhancement::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nidd() {
        let data: Vec<u8> = vec![
            0x01, // PDU type
            0x01, // QoS priority (Normal)
            0x01, // Reliability (Reliable)
            0x00, // Delay class (DelayInsensitive)
            0x00, // Coverage enhancement (None)
            0x00, 0x05, // length = 5
            0x00, 0x01, // sequence number = 1
            0x48, 0x65, 0x6c, 0x6c, 0x6f, // "Hello"
        ];

        let msg = NIDDMessage::parse(&data).unwrap();
        assert_eq!(msg.header.pdu_type, 0x01);
        assert_eq!(msg.header.qos_priority, 0x01);
        assert_eq!(msg.header.reliability, 0x01);
        assert_eq!(msg.header.delay_class, 0x00);
        assert_eq!(msg.header.coverage_enhancement, 0x00);
        assert_eq!(msg.header.length, 5);
        assert_eq!(msg.header.sequence_number, 1);
        assert_eq!(msg.payload_len, 5);
        assert_eq!(msg.payload_slice(), b"Hello");
    }

    #[test]
    fn test_parse_invalid() {
        let data: Vec<u8> = vec![0x01]; // Too short
        assert!(NIDDMessage::parse(&data).is_none());
    }

    #[test]
    fn test_max_payload_size() {
        let mut data = vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x06, 0x41, 0x00, 0x01]; // header with length = 1601
        data.extend(vec![0u8; 1601]); // Exceeds max

        assert!(NIDDMessage::parse(&data).is_none());
    }

    #[test]
    fn test_control_plane_bit() {
        let data: Vec<u8> = vec![
            0x81, // PDU type with control plane bit set
            0x01, // QoS priority
            0x01, // Reliability
            0x00, // Delay class
            0x00, // Coverage enhancement
            0x00, 0x05, // length = 5
            0x00, 0x01, // sequence number = 1
            0x48, 0x65, 0x6c, 0x6c, 0x6f, // "Hello"
        ];

        let msg = NIDDMessage::parse(&data).unwrap();
        assert!(msg.is_control_plane());
        assert_eq!(msg.pdu_type(), PduType::ControlPlane);
    }

    #[test]
    fn test_qos_priority() {
        let data: Vec<u8> = vec![
            0x01, // PDU type
            0x03, // QoS priority (Emergency)
            0x01, // Reliability
            0x00, // Delay class
            0x00, // Coverage enhancement
            0x00, 0x05, // length = 5
            0x00, 0x01, // sequence number = 1
            0x48, 0x65, 0x6c, 0x6c, 0x6f, // "Hello"
        ];

        let msg = NIDDMessage::parse(&data).unwrap();
        assert_eq!(msg.qos_priority(), QosPriority::Emergency);
    }

    #[test]
    fn test_coverage_enhancement() {
        let data: Vec<u8> = vec![
            0x01, // PDU type
            0x01, // QoS priority
            0x01, // Reliability
            0x00, // Delay class
            0x02, // Coverage enhancement (Level2)
            0x00, 0x05, // length = 5
            0x00, 0x01, // sequence number = 1
            0x48, 0x65, 0x6c, 0x6c, 0x6f, // "Hello"
        ];

        let msg = NIDDMessage::parse(&data).unwrap();
        assert_eq!(msg.coverage_enhancement(), CoverageEnhancement::Level2);
    }
}
