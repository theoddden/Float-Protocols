//! Compact Span implementation for OTel-over-Satellite architecture
//!
//! This module provides ultra-lightweight span generation for satellite telemetry,
//! optimized for bandwidth-constrained links. Spans are 50-100 bytes vs 1-2KB for standard OTLP.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::time::{SystemTime, UNIX_EPOCH};

/// Compact span status (1 byte)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SpanStatus {
    Ok = 0,
    Error = 1,
}

/// Compact span - optimized for satellite transmission
/// Total size: ~50-100 bytes depending on attributes
#[derive(Debug, Clone)]
pub struct CompactSpan {
    /// 16-byte trace ID
    pub trace_id: [u8; 16],
    /// 8-byte span ID
    pub span_id: [u8; 8],
    /// 8-byte parent span ID (optional, all zeros if none)
    pub parent_span_id: [u8; 8],
    /// Span name (compact, e.g., "sensor.read")
    pub name: String,
    /// Start time (unix nanos)
    pub start_time_unix_nano: u64,
    /// End time (unix nanos)
    pub end_time_unix_nano: u64,
    /// Valid time (t_event) - when event actually occurred
    pub t_event: i64,
    /// Transaction time (t_system) - when span was created
    pub t_system: i64,
    /// Sensor/device ID
    pub sensor_id: String,
    /// Additional attributes (key-value pairs)
    pub attributes: Vec<(String, String)>,
    /// Span status
    pub status: SpanStatus,
}

impl CompactSpan {
    /// Create a new compact span with current system time
    pub fn new(
        trace_id: [u8; 16],
        span_id: [u8; 8],
        name: impl Into<String>,
        sensor_id: impl Into<String>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Self {
            trace_id,
            span_id,
            parent_span_id: [0u8; 8],
            name: name.into(),
            start_time_unix_nano: now,
            end_time_unix_nano: now,
            t_event: now_ms,
            t_system: now_ms,
            sensor_id: sensor_id.into(),
            attributes: Vec::new(),
            status: SpanStatus::Ok,
        }
    }

    /// Set parent span ID
    pub fn with_parent(mut self, parent_id: [u8; 8]) -> Self {
        self.parent_span_id = parent_id;
        self
    }

    /// Set custom valid time (t_event)
    pub fn with_t_event(mut self, t_event: i64) -> Self {
        self.t_event = t_event;
        self
    }

    /// Set custom transaction time (t_system)
    pub fn with_t_system(mut self, t_system: i64) -> Self {
        self.t_system = t_system;
        self
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.push((key.into(), value.into()));
        self
    }

    /// Set span status
    pub fn with_status(mut self, status: SpanStatus) -> Self {
        self.status = status;
        self
    }

    /// Mark span as completed with current time
    pub fn complete(&mut self) {
        self.end_time_unix_nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
    }

    /// Calculate spread between t_event and t_system
    /// Positive: delayed, Negative: from future, Zero: real-time
    pub fn spread_ms(&self) -> i64 {
        self.t_system - self.t_event
    }

    /// Encode compact span to binary format
    /// Format: [trace_id:16][span_id:8][parent_id:8][name_len:1][name:var][start:8][end:8][t_event:8][t_system:8][sensor_len:1][sensor:var][attr_count:1][attrs:var][status:1]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Trace ID (16 bytes)
        buf.put_slice(&self.trace_id);

        // Span ID (8 bytes)
        buf.put_slice(&self.span_id);

        // Parent span ID (8 bytes)
        buf.put_slice(&self.parent_span_id);

        // Name (1 byte length + var bytes)
        let name_bytes = self.name.as_bytes();
        buf.put_u8(name_bytes.len() as u8);
        buf.put_slice(name_bytes);

        // Start time (8 bytes)
        buf.put_u64(self.start_time_unix_nano);

        // End time (8 bytes)
        buf.put_u64(self.end_time_unix_nano);

        // t_event (8 bytes, signed)
        buf.put_i64(self.t_event);

        // t_system (8 bytes, signed)
        buf.put_i64(self.t_system);

        // Sensor ID (1 byte length + var bytes)
        let sensor_bytes = self.sensor_id.as_bytes();
        buf.put_u8(sensor_bytes.len() as u8);
        buf.put_slice(sensor_bytes);

        // Attributes (1 byte count + var)
        buf.put_u8(self.attributes.len() as u8);
        for (key, value) in &self.attributes {
            let key_bytes = key.as_bytes();
            let val_bytes = value.as_bytes();
            buf.put_u8(key_bytes.len() as u8);
            buf.put_slice(key_bytes);
            buf.put_u8(val_bytes.len() as u8);
            buf.put_slice(val_bytes);
        }

        // Status (1 byte)
        buf.put_u8(self.status as u8);

        buf.freeze()
    }

    /// Decode compact span from binary format
    pub fn decode(data: &[u8]) -> Result<Self, &'static str> {
        let mut buf = Bytes::copy_from_slice(data);

        if buf.remaining() < 16 + 8 + 8 + 1 + 8 + 8 + 8 + 8 + 1 + 1 + 1 {
            return Err("Insufficient data for compact span");
        }

        // Trace ID
        let mut trace_id = [0u8; 16];
        buf.copy_to_slice(&mut trace_id);

        // Span ID
        let mut span_id = [0u8; 8];
        buf.copy_to_slice(&mut span_id);

        // Parent span ID
        let mut parent_span_id = [0u8; 8];
        buf.copy_to_slice(&mut parent_span_id);

        // Name
        let name_len = buf.get_u8() as usize;
        if buf.remaining() < name_len {
            return Err("Insufficient data for name");
        }
        let name = String::from_utf8(buf.split_to(name_len).to_vec())
            .map_err(|_| "Invalid UTF-8 in name")?;

        // Start time
        let start_time_unix_nano = buf.get_u64();

        // End time
        let end_time_unix_nano = buf.get_u64();

        // t_event
        let t_event = buf.get_i64();

        // t_system
        let t_system = buf.get_i64();

        // Sensor ID
        let sensor_len = buf.get_u8() as usize;
        if buf.remaining() < sensor_len {
            return Err("Insufficient data for sensor_id");
        }
        let sensor_id = String::from_utf8(buf.split_to(sensor_len).to_vec())
            .map_err(|_| "Invalid UTF-8 in sensor_id")?;

        // Attributes
        let attr_count = buf.get_u8() as usize;
        let mut attributes = Vec::with_capacity(attr_count);
        for _ in 0..attr_count {
            if buf.remaining() < 2 {
                return Err("Insufficient data for attribute");
            }
            let key_len = buf.get_u8() as usize;
            let val_len = buf.get_u8() as usize;
            if buf.remaining() < key_len + val_len {
                return Err("Insufficient data for attribute value");
            }
            let key = String::from_utf8(buf.split_to(key_len).to_vec())
                .map_err(|_| "Invalid UTF-8 in attribute key")?;
            let value = String::from_utf8(buf.split_to(val_len).to_vec())
                .map_err(|_| "Invalid UTF-8 in attribute value")?;
            attributes.push((key, value));
        }

        // Status
        let status_byte = buf.get_u8();
        let status = match status_byte {
            0 => SpanStatus::Ok,
            1 => SpanStatus::Error,
            _ => return Err("Invalid status byte"),
        };

        Ok(Self {
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_time_unix_nano,
            end_time_unix_nano,
            t_event,
            t_system,
            sensor_id,
            attributes,
            status,
        })
    }

    /// Estimate encoded size in bytes
    pub fn encoded_size(&self) -> usize {
        16 + 8 + 8 + // trace_id, span_id, parent_id
        1 + self.name.len() + // name
        8 + 8 + // start, end
        8 + 8 + // t_event, t_system
        1 + self.sensor_id.len() + // sensor_id
        1 + // attr_count
        self.attributes.iter().map(|(k, v)| 1 + k.len() + 1 + v.len()).sum::<usize>() + // attrs
        1 // status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let span = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001")
            .with_t_event(1000)
            .with_t_system(1500)
            .with_attribute("temperature", "25.5")
            .with_status(SpanStatus::Ok);

        let encoded = span.encode();
        let decoded = CompactSpan::decode(&encoded).unwrap();

        assert_eq!(decoded.trace_id, span.trace_id);
        assert_eq!(decoded.span_id, span.span_id);
        assert_eq!(decoded.name, span.name);
        assert_eq!(decoded.t_event, span.t_event);
        assert_eq!(decoded.t_system, span.t_system);
        assert_eq!(decoded.sensor_id, span.sensor_id);
        assert_eq!(decoded.attributes, span.attributes);
        assert_eq!(decoded.status, span.status);
    }

    #[test]
    fn test_spread_calculation() {
        let span = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001")
            .with_t_event(1000)
            .with_t_system(1500);

        assert_eq!(span.spread_ms(), 500);
    }
}
