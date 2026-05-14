//! OTLP Converter for Mandala Collector
//! 
//! This module converts compact spans to OTLP format for data warehouse ingestion.
//! Preserves bi-temporal attributes (t_event, t_system) for reconciliation.

use bytes::Bytes;
use crate::otel_compact_span::{CompactSpan, SpanStatus};
use crate::otel_bundle::TelemetryBundle;

/// OTLP span representation
#[derive(Debug, Clone)]
pub struct OtlpSpan {
    /// Trace ID (16 bytes hex string)
    pub trace_id: String,
    /// Span ID (8 bytes hex string)
    pub span_id: String,
    /// Parent span ID (8 bytes hex string, optional)
    pub parent_span_id: Option<String>,
    /// Span name
    pub name: String,
    /// Start time (unix nanos)
    pub start_time_unix_nano: u64,
    /// End time (unix nanos)
    pub end_time_unix_nano: u64,
    /// Attributes (key-value pairs)
    pub attributes: Vec<(String, String)>,
    /// Status
    pub status: OtlpStatus,
}

/// OTLP status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OtlpStatus {
    Unset,
    Ok,
    Error,
}

/// OTLP trace export request
#[derive(Debug, Clone)]
pub struct OtlpExportRequest {
    /// Resource attributes (applied to all spans)
    pub resource_attributes: Vec<(String, String)>,
    /// Spans to export
    pub spans: Vec<OtlpSpan>,
}

impl OtlpSpan {
    /// Convert from compact span
    pub fn from_compact(span: &CompactSpan) -> Self {
        // Convert bytes to hex strings
        let trace_id = hex::encode(span.trace_id);
        let span_id = hex::encode(span.span_id);
        let parent_span_id = if span.parent_span_id != [0u8; 8] {
            Some(hex::encode(span.parent_span_id))
        } else {
            None
        };

        // Build attributes with bi-temporal timestamps
        let mut attributes = Vec::with_capacity(span.attributes.len() + 4);
        attributes.push(("t_event".to_string(), span.t_event.to_string()));
        attributes.push(("t_system".to_string(), span.t_system.to_string()));
        attributes.push(("sensor_id".to_string(), span.sensor_id.clone()));
        
        // Calculate spread
        let spread_ms = span.t_system - span.t_event;
        attributes.push(("spread_ms".to_string(), spread_ms.to_string()));
        
        // Add custom attributes
        for (key, value) in &span.attributes {
            attributes.push((key.clone(), value.clone()));
        }

        // Convert status
        let status = match span.status {
            SpanStatus::Ok => OtlpStatus::Ok,
            SpanStatus::Error => OtlpStatus::Error,
        };

        Self {
            trace_id,
            span_id,
            parent_span_id,
            name: span.name.clone(),
            start_time_unix_nano: span.start_time_unix_nano,
            end_time_unix_nano: span.end_time_unix_nano,
            attributes,
            status,
        }
    }

    /// Convert to JSON representation (for downstream OTLP exporters)
    pub fn to_json(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        
        obj.insert("traceId".to_string(), serde_json::Value::String(self.trace_id.clone()));
        obj.insert("spanId".to_string(), serde_json::Value::String(self.span_id.clone()));
        
        if let Some(ref parent) = self.parent_span_id {
            obj.insert("parentSpanId".to_string(), serde_json::Value::String(parent.clone()));
        }
        
        obj.insert("name".to_string(), serde_json::Value::String(self.name.clone()));
        obj.insert("startTimeUnixNano".to_string(), serde_json::Value::Number(self.start_time_unix_nano.into()));
        obj.insert("endTimeUnixNano".to_string(), serde_json::Value::Number(self.end_time_unix_nano.into()));
        
        let status_str = match self.status {
            OtlpStatus::Unset => "STATUS_UNSET",
            OtlpStatus::Ok => "STATUS_OK",
            OtlpStatus::Error => "STATUS_ERROR",
        };
        obj.insert("status".to_string(), serde_json::Value::String(status_str.to_string()));
        
        // Attributes
        let mut attrs_map = serde_json::Map::new();
        for (key, value) in &self.attributes {
            attrs_map.insert(key.clone(), serde_json::Value::String(value.clone()));
        }
        obj.insert("attributes".to_string(), serde_json::Value::Object(attrs_map));
        
        serde_json::Value::Object(obj)
    }
}

impl OtlpExportRequest {
    /// Create new export request
    pub fn new() -> Self {
        Self {
            resource_attributes: Vec::new(),
            spans: Vec::new(),
        }
    }

    /// Add resource attribute
    pub fn with_resource_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.resource_attributes.push((key.into(), value.into()));
        self
    }

    /// Add a span
    pub fn with_span(mut self, span: OtlpSpan) -> Self {
        self.spans.push(span);
        self
    }

    /// Convert from telemetry bundle
    pub fn from_bundle(bundle: &TelemetryBundle) -> Self {
        let mut request = Self::new();
        
        // Add bundle-level resource attributes
        request = request
            .with_resource_attribute("bundle.sequence_number", bundle.sequence_number.to_string())
            .with_resource_attribute("bundle.timestamp", bundle.batch_timestamp.to_string())
            .with_resource_attribute("bundle.compression", format!("{:?}", bundle.compression_type));
        
        // Convert all spans
        for span in &bundle.spans {
            request = request.with_span(OtlpSpan::from_compact(span));
        }
        
        request
    }

    /// Convert to JSON representation
    pub fn to_json(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        
        // Resource attributes
        let mut resource_attrs = serde_json::Map::new();
        for (key, value) in &self.resource_attributes {
            resource_attrs.insert(key.clone(), serde_json::Value::String(value.clone()));
        }
        obj.insert("resource".to_string(), serde_json::Value::Object(resource_attrs.clone()));
        
        // Spans
        let spans_array: Vec<serde_json::Value> = self.spans.iter().map(|s| s.to_json()).collect();
        obj.insert("resourceSpans".to_string(), serde_json::Value::Array(vec![
            serde_json::json!({
                "resource": resource_attrs,
                "scopeSpans": [{
                    "spans": spans_array
                }]
            })
        ]));
        
        serde_json::Value::Object(obj)
    }

    /// Convert to OTLP binary format (simplified protobuf-like encoding)
    pub fn to_binary(&self) -> Bytes {
        use bytes::BufMut;
        
        let mut buf = bytes::BytesMut::new();
        
        // Resource attributes count
        buf.put_u32(self.resource_attributes.len() as u32);
        
        for (key, value) in &self.resource_attributes {
            buf.put_u32(key.len() as u32);
            buf.put_slice(key.as_bytes());
            buf.put_u32(value.len() as u32);
            buf.put_slice(value.as_bytes());
        }
        
        // Spans count
        buf.put_u32(self.spans.len() as u32);
        
        for span in &self.spans {
            // Trace ID (16 bytes)
            let trace_id_bytes = hex::decode(&span.trace_id).unwrap_or_default();
            buf.put_slice(&trace_id_bytes);
            
            // Span ID (8 bytes)
            let span_id_bytes = hex::decode(&span.span_id).unwrap_or_default();
            buf.put_slice(&span_id_bytes);
            
            // Parent span ID (8 bytes or none)
            if let Some(ref parent) = span.parent_span_id {
                buf.put_u8(1);
                let parent_bytes = hex::decode(parent).unwrap_or_default();
                buf.put_slice(&parent_bytes);
            } else {
                buf.put_u8(0);
            }
            
            // Name
            buf.put_u32(span.name.len() as u32);
            buf.put_slice(span.name.as_bytes());
            
            // Times
            buf.put_u64(span.start_time_unix_nano);
            buf.put_u64(span.end_time_unix_nano);
            
            // Status
            buf.put_u8(match span.status {
                OtlpStatus::Unset => 0,
                OtlpStatus::Ok => 1,
                OtlpStatus::Error => 2,
            });
            
            // Attributes
            buf.put_u32(span.attributes.len() as u32);
            for (key, value) in &span.attributes {
                buf.put_u32(key.len() as u32);
                buf.put_slice(key.as_bytes());
                buf.put_u32(value.len() as u32);
                buf.put_slice(value.as_bytes());
            }
        }
        
        buf.freeze()
    }
}

impl Default for OtlpExportRequest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_to_otlp_conversion() {
        let compact = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001")
            .with_t_event(1000)
            .with_t_system(1500)
            .with_attribute("temperature", "25.5");
        
        let otlp = OtlpSpan::from_compact(&compact);
        
        assert_eq!(otlp.trace_id, hex::encode([1u8; 16]));
        assert_eq!(otlp.span_id, hex::encode([2u8; 8]));
        assert_eq!(otlp.name, "sensor.read");
        
        // Check bi-temporal attributes
        let attrs: std::collections::HashMap<_, _> = otlp.attributes.iter().cloned().collect();
        assert_eq!(attrs.get("t_event"), Some(&"1000".to_string()));
        assert_eq!(attrs.get("t_system"), Some(&"1500".to_string()));
        assert_eq!(attrs.get("spread_ms"), Some(&"500".to_string()));
    }

    #[test]
    fn test_bundle_to_export_request() {
        let mut bundle = TelemetryBundle::new(1);
        bundle.add_span(CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001"));
        
        let request = OtlpExportRequest::from_bundle(&bundle);
        
        assert_eq!(request.spans.len(), 1);
        assert!(request.resource_attributes.iter().any(|(k, _)| k == "bundle.sequence_number"));
    }

    #[test]
    fn test_otlp_json_serialization() {
        let compact = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001");
        let otlp = OtlpSpan::from_compact(&compact);
        
        let json = otlp.to_json();
        assert!(json.is_object());
        assert_eq!(json["name"], "sensor.read");
    }
}
