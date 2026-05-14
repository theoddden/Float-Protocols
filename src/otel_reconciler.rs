//! Bi-Temporal Reconciliation for Mandala Collector
//!
//! This module reconciles bi-temporal data (t_event, t_system) for audit trails
//! and compliance reporting. Enables point-in-time queries and spread analysis.

use crate::otel_converter::OtlpSpan;
use std::collections::HashMap;

/// Bi-temporal record
#[derive(Debug, Clone)]
pub struct BitemporalRecord {
    /// Entity ID (trace ID + span ID)
    pub entity_id: String,
    /// Valid time (t_event) - when event actually occurred
    pub t_event: i64,
    /// Transaction time (t_system) - when recorded
    pub t_system: i64,
    /// Span data
    pub span: OtlpSpan,
}

/// Reconciliation statistics
#[derive(Debug, Clone, Default)]
pub struct ReconciliationStats {
    /// Total records processed
    pub total_records: usize,
    /// Records with positive spread (delayed)
    pub positive_spread: usize,
    /// Records with negative spread (from future)
    pub negative_spread: usize,
    /// Records with zero spread (real-time)
    pub zero_spread: usize,
    /// Average spread in milliseconds
    pub avg_spread_ms: f64,
    /// Maximum spread in milliseconds
    pub max_spread_ms: i64,
    /// Minimum spread in milliseconds
    pub min_spread_ms: i64,
}

/// Bi-temporal reconciler
pub struct BitemporalReconciler {
    /// Records indexed by entity_id
    records: HashMap<String, Vec<BitemporalRecord>>,
}

impl BitemporalReconciler {
    /// Create new reconciler
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    /// Ingest a span for reconciliation
    pub fn ingest(&mut self, span: OtlpSpan) {
        // Extract bi-temporal attributes
        let t_event = self.extract_t_event(&span);
        let t_system = self.extract_t_system(&span);

        let entity_id = format!("{}:{}", span.trace_id, span.span_id);

        let record = BitemporalRecord {
            entity_id: entity_id.clone(),
            t_event,
            t_system,
            span,
        };

        self.records
            .entry(entity_id)
            .or_insert_with(Vec::new)
            .push(record);
    }

    /// Extract t_event from span attributes
    fn extract_t_event(&self, span: &OtlpSpan) -> i64 {
        span.attributes
            .iter()
            .find(|(k, _)| k == "t_event")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0)
    }

    /// Extract t_system from span attributes
    fn extract_t_system(&self, span: &OtlpSpan) -> i64 {
        span.attributes
            .iter()
            .find(|(k, _)| k == "t_system")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0)
    }

    /// Query by valid time (what actually happened)
    pub fn query_valid_time(&self, start_ms: i64, end_ms: i64) -> Vec<&OtlpSpan> {
        let mut results = Vec::new();

        for records in self.records.values() {
            for record in records {
                if record.t_event >= start_ms && record.t_event <= end_ms {
                    results.push(&record.span);
                }
            }
        }

        results
    }

    /// Query by transaction time (what system believed)
    pub fn query_transaction_time(&self, start_ms: i64, end_ms: i64) -> Vec<&OtlpSpan> {
        let mut results = Vec::new();

        for records in self.records.values() {
            for record in records {
                if record.t_system >= start_ms && record.t_system <= end_ms {
                    results.push(&record.span);
                }
            }
        }

        results
    }

    /// Get system belief at specific transaction time
    pub fn system_belief_at(&self, transaction_time_ms: i64) -> Vec<&OtlpSpan> {
        let mut results = Vec::new();

        for records in self.records.values() {
            // Find the latest record for this entity at or before transaction_time
            let latest = records
                .iter()
                .filter(|r| r.t_system <= transaction_time_ms)
                .max_by_key(|r| r.t_system);

            if let Some(record) = latest {
                results.push(&record.span);
            }
        }

        results
    }

    /// Get actual state at specific valid time
    pub fn actual_state_at(&self, valid_time_ms: i64) -> Vec<&OtlpSpan> {
        let mut results = Vec::new();

        for records in self.records.values() {
            for record in records {
                if record.t_event == valid_time_ms {
                    results.push(&record.span);
                }
            }
        }

        results
    }

    /// Calculate spread statistics
    pub fn spread_stats(&self, start_ms: i64, end_ms: i64) -> ReconciliationStats {
        let mut stats = ReconciliationStats::default();
        let mut spreads = Vec::new();

        for records in self.records.values() {
            for record in records {
                if record.t_system >= start_ms && record.t_system <= end_ms {
                    stats.total_records += 1;
                    let spread = record.t_system - record.t_event;
                    spreads.push(spread);

                    if spread > 0 {
                        stats.positive_spread += 1;
                    } else if spread < 0 {
                        stats.negative_spread += 1;
                    } else {
                        stats.zero_spread += 1;
                    }
                }
            }
        }

        if !spreads.is_empty() {
            stats.max_spread_ms = *spreads.iter().max().unwrap_or(&0);
            stats.min_spread_ms = *spreads.iter().min().unwrap_or(&0);
            stats.avg_spread_ms = spreads.iter().sum::<i64>() as f64 / spreads.len() as f64;
        }

        stats
    }

    /// Get all records for an entity
    pub fn entity_history(&self, trace_id: &str, span_id: &str) -> Vec<&BitemporalRecord> {
        let entity_id = format!("{}:{}", trace_id, span_id);
        self.records
            .get(&entity_id)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    /// Retroactive correction: insert a record with updated valid time
    pub fn retroactive_correction(&mut self, span: OtlpSpan, new_t_event: i64) {
        let t_system = self.extract_t_system(&span);
        let entity_id = format!("{}:{}", span.trace_id, span.span_id);

        let record = BitemporalRecord {
            entity_id: entity_id.clone(),
            t_event: new_t_event,
            t_system,
            span,
        };

        self.records
            .entry(entity_id)
            .or_insert_with(Vec::new)
            .push(record);
    }

    /// Clear all records
    pub fn clear(&mut self) {
        self.records.clear();
    }

    /// Get total record count
    pub fn record_count(&self) -> usize {
        self.records.values().map(|v| v.len()).sum()
    }
}

impl Default for BitemporalReconciler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otel_compact_span::CompactSpan;

    #[test]
    fn test_ingest_and_query() {
        let mut reconciler = BitemporalReconciler::new();

        let compact = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001")
            .with_t_event(1000)
            .with_t_system(1500);

        let otlp = crate::otel_converter::OtlpSpan::from_compact(&compact);
        reconciler.ingest(otlp);

        assert_eq!(reconciler.record_count(), 1);

        let results = reconciler.query_valid_time(500, 2000);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_spread_stats() {
        let mut reconciler = BitemporalReconciler::new();

        for i in 0..10 {
            let compact =
                CompactSpan::new([i as u8; 16], [i as u8; 8], "sensor.read", "sensor-001")
                    .with_t_event(i * 100)
                    .with_t_system(i * 100 + 500);

            let otlp = crate::otel_converter::OtlpSpan::from_compact(&compact);
            reconciler.ingest(otlp);
        }

        let stats = reconciler.spread_stats(0, 10000);
        assert_eq!(stats.total_records, 10);
        assert_eq!(stats.positive_spread, 10);
        assert_eq!(stats.avg_spread_ms, 500.0);
    }

    #[test]
    fn test_system_belief_at() {
        let mut reconciler = BitemporalReconciler::new();

        let compact1 = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001")
            .with_t_event(1000)
            .with_t_system(1500);

        let compact2 = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001")
            .with_t_event(1000) // Same event
            .with_t_system(2000); // Later correction

        let otlp1 = crate::otel_converter::OtlpSpan::from_compact(&compact1);
        let otlp2 = crate::otel_converter::OtlpSpan::from_compact(&compact2);

        reconciler.ingest(otlp1);
        reconciler.ingest(otlp2);

        // At time 1800, we should believe the first version
        let belief = reconciler.system_belief_at(1800);
        assert_eq!(belief.len(), 1);
    }
}
