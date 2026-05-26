//! Bi-temporal query capabilities for insurance underwriting and trade compliance
//!
//! Enables queries like:
//! - "What did we believe the state of the fleet was at 3 PM yesterday?" (transaction time)
//! - "What actually happened at 3 PM yesterday?" (valid time)
//!
//! Maintains two timestamps per message:
//! - t_event (Valid Time): When sensor recorded event in physical world
//! - t_system (Transaction Time): When system first learned about event

use crate::protocol::{Message, Protocol};
use std::collections::VecDeque;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTime {
    ValidTime,       // Query based on when events actually happened (t_event)
    TransactionTime, // Query based on when system learned about events (t_system)
}

#[derive(Debug, Clone)]
pub struct BiTemporalQuery {
    pub query_time: QueryTime,
    pub start_ms: u64,
    pub end_ms: u64,
    pub protocol_filter: Option<Protocol>,
    pub max_spread_ms: Option<i64>, // Filter by max delay spread
}

impl BiTemporalQuery {
    pub fn new(query_time: QueryTime, start_ms: u64, end_ms: u64) -> Self {
        Self {
            query_time,
            start_ms,
            end_ms,
            protocol_filter: None,
            max_spread_ms: None,
        }
    }

    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol_filter = Some(protocol);
        self
    }

    pub fn with_max_spread(mut self, max_spread_ms: i64) -> Self {
        self.max_spread_ms = Some(max_spread_ms);
        self
    }

    /// Check if a message matches this bi-temporal query
    pub fn matches(&self, message: &Message) -> bool {
        let timestamp = match self.query_time {
            QueryTime::ValidTime => message.t_event,
            QueryTime::TransactionTime => message.t_system,
        };

        // Check time range
        if timestamp < self.start_ms || timestamp > self.end_ms {
            return false;
        }

        // Check protocol filter
        if let Some(protocol) = self.protocol_filter {
            if message.protocol != protocol {
                return false;
            }
        }

        // Check spread filter
        if let Some(max_spread) = self.max_spread_ms {
            let spread = message.spread_ms();
            if spread.abs() > max_spread {
                return false;
            }
        }

        true
    }
}

pub struct BiTemporalStore {
    messages: RwLock<VecDeque<Message>>,
    max_messages: usize,
}

impl BiTemporalStore {
    pub fn new(max_messages: usize) -> Self {
        Self {
            messages: RwLock::new(VecDeque::with_capacity(max_messages)),
            max_messages,
        }
    }

    /// Store a message with bi-temporal timestamps
    pub async fn store(&self, message: Message) {
        let mut messages = self.messages.write().await;

        // Evict oldest if at capacity (O(1) with VecDeque)
        if messages.len() >= self.max_messages {
            messages.pop_front();
        }

        messages.push_back(message);
    }

    /// Store a batch of messages with a single write lock acquisition.
    /// Replaces N `store()` calls (N lock acquires) with 1, amortizing
    /// the async lock overhead across the entire incoming batch.
    pub async fn store_batch(&self, batch: &[Message]) {
        if batch.is_empty() {
            return;
        }
        let mut messages = self.messages.write().await;
        for message in batch {
            if messages.len() >= self.max_messages {
                messages.pop_front();
            }
            messages.push_back(message.clone());
        }
    }

    /// Query messages by valid time (what actually happened)
    pub async fn query_valid_time(&self, start_ms: u64, end_ms: u64) -> Vec<Message> {
        self.query(BiTemporalQuery::new(QueryTime::ValidTime, start_ms, end_ms))
            .await
    }

    /// Query messages by transaction time (what system believed)
    pub async fn query_transaction_time(&self, start_ms: u64, end_ms: u64) -> Vec<Message> {
        self.query(BiTemporalQuery::new(
            QueryTime::TransactionTime,
            start_ms,
            end_ms,
        ))
        .await
    }

    /// Execute a custom bi-temporal query
    pub async fn query(&self, query: BiTemporalQuery) -> Vec<Message> {
        let messages = self.messages.read().await;
        messages
            .iter()
            .filter(|m| query.matches(m))
            .cloned()
            .collect()
    }

    /// Get spread statistics for a time range
    pub async fn spread_stats(&self, start_ms: u64, end_ms: u64) -> SpreadStats {
        let messages = self.messages.read().await;
        let relevant: Vec<_> = messages
            .iter()
            .filter(|m| m.t_system >= start_ms && m.t_system <= end_ms)
            .collect();

        if relevant.is_empty() {
            return SpreadStats::default();
        }

        let spreads: Vec<i64> = relevant.iter().map(|m| m.spread_ms()).collect();
        let total_spread: i64 = spreads.iter().sum();
        let avg_spread = total_spread / spreads.len() as i64;
        let min_spread = *spreads.iter().min().unwrap();
        let max_spread = *spreads.iter().max().unwrap();

        SpreadStats {
            message_count: relevant.len(),
            avg_spread_ms: avg_spread,
            min_spread_ms: min_spread,
            max_spread_ms: max_spread,
            total_spread_ms: total_spread,
        }
    }

    /// Get state at a specific point in time (what system believed)
    pub async fn system_belief_at(&self, timestamp_ms: u64) -> Vec<Message> {
        let messages = self.messages.read().await;
        messages
            .iter()
            .filter(|m| m.t_system <= timestamp_ms)
            .cloned()
            .collect()
    }

    /// Get actual state at a specific point in time (what actually happened)
    pub async fn actual_state_at(&self, timestamp_ms: u64) -> Vec<Message> {
        let messages = self.messages.read().await;
        messages
            .iter()
            .filter(|m| m.t_event <= timestamp_ms)
            .cloned()
            .collect()
    }

    pub async fn stats(&self) -> BiTemporalStats {
        let messages = self.messages.read().await;
        BiTemporalStats {
            total_messages: messages.len(),
            max_messages: self.max_messages,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SpreadStats {
    pub message_count: usize,
    pub avg_spread_ms: i64,
    pub min_spread_ms: i64,
    pub max_spread_ms: i64,
    pub total_spread_ms: i64,
}

impl SpreadStats {
    pub fn avg_spread_seconds(&self) -> f64 {
        self.avg_spread_ms as f64 / 1000.0
    }
}

#[derive(Debug, Clone)]
pub struct BiTemporalStats {
    pub total_messages: usize,
    pub max_messages: usize,
}

impl BiTemporalStats {
    pub fn utilization(&self) -> f64 {
        if self.max_messages == 0 {
            0.0
        } else {
            (self.total_messages as f64) / (self.max_messages as f64)
        }
    }
}
