//! OTel Bundle implementation for ASTS transport
//! 
//! This module bundles compact spans into ASTS protobuf format for satellite transmission.
//! Supports compression (zstd) to minimize bandwidth usage.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use zstd;
use crate::otel_compact_span::CompactSpan;

/// Compression type for bundle
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Zstd = 1,
    Gzip = 2,
}

/// Telemetry bundle for satellite transmission
/// Bundles multiple compact spans with optional compression
#[derive(Debug, Clone)]
pub struct TelemetryBundle {
    /// Sequence number for ordering
    pub sequence_number: u64,
    /// Batch timestamp (unix millis)
    pub batch_timestamp: i64,
    /// Compression type
    pub compression_type: CompressionType,
    /// Raw spans (before compression)
    pub spans: Vec<CompactSpan>,
    /// Compressed data (if compression enabled)
    pub compressed_data: Option<Bytes>,
}

impl TelemetryBundle {
    /// Create a new telemetry bundle
    pub fn new(sequence_number: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Self {
            sequence_number,
            batch_timestamp: now,
            compression_type: CompressionType::None,
            spans: Vec::new(),
            compressed_data: None,
        }
    }

    /// Add a span to the bundle
    pub fn add_span(&mut self, span: CompactSpan) {
        self.spans.push(span);
    }

    /// Set compression type
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression_type = compression;
        self
    }

    /// Compress the bundle data
    pub fn compress(&mut self, level: i32) -> Result<(), Box<dyn std::error::Error>> {
        if self.spans.is_empty() {
            return Ok(());
        }

        match self.compression_type {
            CompressionType::None => {
                self.compressed_data = None;
            }
            CompressionType::Zstd => {
                // Encode all spans to bytes first
                let mut raw_data = BytesMut::new();
                for span in &self.spans {
                    let encoded = span.encode();
                    raw_data.put_u32(encoded.len() as u32);
                    raw_data.put_slice(&encoded);
                }

                // Compress with zstd
                let compressed = zstd::encode_all(&raw_data[..], level)?;
                self.compressed_data = Some(Bytes::from(compressed));
            }
            CompressionType::Gzip => {
                // Encode all spans to bytes first
                let mut raw_data = BytesMut::new();
                for span in &self.spans {
                    let encoded = span.encode();
                    raw_data.put_u32(encoded.len() as u32);
                    raw_data.put_slice(&encoded);
                }

                // Compress with flate2
                use flate2::write::GzEncoder;
                use flate2::Compression;
                use std::io::Write;
                
                let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level as u32));
                encoder.write_all(&raw_data)?;
                let compressed = encoder.finish()?;
                self.compressed_data = Some(Bytes::from(compressed));
            }
        }

        Ok(())
    }

    /// Encode bundle to binary format
    /// Format: [seq:8][batch_ts:8][comp_type:1][has_data:1][data_len:4][data:var]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        
        // Sequence number (8 bytes)
        buf.put_u64(self.sequence_number);
        
        // Batch timestamp (8 bytes, signed)
        buf.put_i64(self.batch_timestamp);
        
        // Compression type (1 byte)
        buf.put_u8(self.compression_type as u8);
        
        // Has compressed data flag (1 byte)
        let has_data = self.compressed_data.is_some();
        buf.put_u8(if has_data { 1 } else { 0 });
        
        if let Some(ref data) = self.compressed_data {
            // Data length (4 bytes)
            buf.put_u32(data.len() as u32);
            // Data
            buf.put_slice(data);
        } else if !self.spans.is_empty() {
            // Encode spans directly if no compression
            let mut raw_data = BytesMut::new();
            for span in &self.spans {
                let encoded = span.encode();
                raw_data.put_u32(encoded.len() as u32);
                raw_data.put_slice(&encoded);
            }
            buf.put_u32(raw_data.len() as u32);
            buf.put_slice(&raw_data);
        } else {
            // No data
            buf.put_u32(0);
        }
        
        buf.freeze()
    }

    /// Decode bundle from binary format
    pub fn decode(data: &[u8]) -> Result<Self, &'static str> {
        let mut buf = Bytes::copy_from_slice(data);
        
        if buf.remaining() < 8 + 8 + 1 + 1 + 4 {
            return Err("Insufficient data for bundle header");
        }
        
        // Sequence number
        let sequence_number = buf.get_u64();
        
        // Batch timestamp
        let batch_timestamp = buf.get_i64();
        
        // Compression type
        let compression_type_byte = buf.get_u8();
        let compression_type = match compression_type_byte {
            0 => CompressionType::None,
            1 => CompressionType::Zstd,
            2 => CompressionType::Gzip,
            _ => return Err("Invalid compression type"),
        };
        
        // Has compressed data flag
        let has_data = buf.get_u8() == 1;
        
        // Data length
        let data_len = buf.get_u32() as usize;
        
        if buf.remaining() < data_len {
            return Err("Insufficient data for bundle payload");
        }
        
        let mut bundle = Self {
            sequence_number,
            batch_timestamp,
            compression_type,
            spans: Vec::new(),
            compressed_data: None,
        };
        
        if has_data {
            let data = buf.split_to(data_len);
            
            match compression_type {
                CompressionType::None => {
                    // Decode spans directly
                    let mut span_buf = data;
                    while span_buf.remaining() >= 4 {
                        let span_len = span_buf.get_u32() as usize;
                        if span_buf.remaining() < span_len {
                            return Err("Insufficient data for span");
                        }
                        let span_data = span_buf.split_to(span_len);
                        let span = CompactSpan::decode(&span_data)?;
                        bundle.spans.push(span);
                    }
                }
                CompressionType::Zstd => {
                    // Decompress with zstd
                    let decompressed = zstd::decode_all(&data[..])
                        .map_err(|_| "Zstd decompression failed")?;
                    let mut span_buf = Bytes::from(decompressed);
                    while span_buf.remaining() >= 4 {
                        let span_len = span_buf.get_u32() as usize;
                        if span_buf.remaining() < span_len {
                            return Err("Insufficient data for span");
                        }
                        let span_data = span_buf.split_to(span_len);
                        let span = CompactSpan::decode(&span_data)?;
                        bundle.spans.push(span);
                    }
                }
                CompressionType::Gzip => {
                    // Decompress with flate2
                    use flate2::read::GzDecoder;
                    use std::io::Read;
                    
                    let mut decoder = GzDecoder::new(&data[..]);
                    let mut decompressed = Vec::new();
                    decoder.read_to_end(&mut decompressed)
                        .map_err(|_| "Gzip decompression failed")?;
                    
                    let mut span_buf = Bytes::from(decompressed);
                    while span_buf.remaining() >= 4 {
                        let span_len = span_buf.get_u32() as usize;
                        if span_buf.remaining() < span_len {
                            return Err("Insufficient data for span");
                        }
                        let span_data = span_buf.split_to(span_len);
                        let span = CompactSpan::decode(&span_data)?;
                        bundle.spans.push(span);
                    }
                }
            }
        }
        
        Ok(bundle)
    }

    /// Get total span count
    pub fn span_count(&self) -> usize {
        self.spans.len()
    }

    /// Estimate encoded size
    pub fn encoded_size(&self) -> usize {
        8 + 8 + 1 + 1 + 4 + // header
        self.compressed_data.as_ref().map(|d| d.len()).unwrap_or_else(|| {
            self.spans.iter().map(|s| 4 + s.encoded_size()).sum()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundle_encode_decode() {
        let mut bundle = TelemetryBundle::new(1);
        
        let span1 = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001");
        let span2 = CompactSpan::new([3u8; 16], [4u8; 8], "sensor.read", "sensor-002");
        
        bundle.add_span(span1);
        bundle.add_span(span2);
        
        let encoded = bundle.encode();
        let decoded = TelemetryBundle::decode(&encoded).unwrap();
        
        assert_eq!(decoded.sequence_number, bundle.sequence_number);
        assert_eq!(decoded.span_count(), 2);
    }

    #[test]
    fn test_bundle_zstd_compression() {
        let mut bundle = TelemetryBundle::new(1).with_compression(CompressionType::Zstd);
        
        for i in 0..10 {
            let span = CompactSpan::new([i as u8; 16], [i as u8; 8], "sensor.read", "sensor-001")
                .with_attribute("temperature", &format!("{}", i * 10));
            bundle.add_span(span);
        }
        
        bundle.compress(3).unwrap();
        
        let encoded = bundle.encode();
        let decoded = TelemetryBundle::decode(&encoded).unwrap();
        
        assert_eq!(decoded.span_count(), 10);
        assert!(decoded.compressed_data.is_some());
    }
}
