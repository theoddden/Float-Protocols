//! Cadence translation layer to solve Semantic Drift Between Protocols
//!
//! Iridium is high-latency/low-bandwidth (heartbeat once per hour)
//! ASTS is low-latency/high-bandwidth (heartbeat every 5 seconds)
//! This layer manages differing cadences in a single bridge.

use crate::protocol::{Priority, Protocol};
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageCadence {
    OncePerHour,
    OncePerMinute,
    OncePerSecond,
    Every5Seconds,
    Every10Seconds,
    Every30Seconds,
    Custom(Duration),
}

impl MessageCadence {
    pub fn to_duration(&self) -> Duration {
        match self {
            MessageCadence::OncePerHour => Duration::from_secs(3600),
            MessageCadence::OncePerMinute => Duration::from_secs(60),
            MessageCadence::OncePerSecond => Duration::from_secs(1),
            MessageCadence::Every5Seconds => Duration::from_secs(5),
            MessageCadence::Every10Seconds => Duration::from_secs(10),
            MessageCadence::Every30Seconds => Duration::from_secs(30),
            MessageCadence::Custom(d) => *d,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CadenceRule {
    pub source_protocol: Protocol,
    pub source_cadence: MessageCadence,
    pub target_protocol: Protocol,
    pub target_cadence: MessageCadence,
    pub message_type: String, // e.g., "heartbeat", "location", "telemetry"
    pub transformation: CadenceTransformation,
}

#[derive(Debug, Clone)]
pub enum CadenceTransformation {
    /// No transformation - send as-is
    Passthrough,
    /// Drop messages that are too frequent (rate limiting)
    RateLimit { max_per_second: f64 },
    /// Buffer and aggregate messages
    Aggregate { window_ms: u64 },
    /// Duplicate to meet target cadence
    Duplicate { max_duplicates: usize },
    /// Throttle to target cadence
    Throttle { target_interval_ms: u64 },
}

pub struct CadenceTranslator {
    rules: Vec<CadenceRule>,
    _message_tracker: HashMap<String, Instant>,
    rate_limiters: HashMap<String, RateLimiter>,
}

#[derive(Debug, Clone)]
struct RateLimiter {
    last_sent: Instant,
    min_interval: Duration,
    messages_since_last: usize,
}

impl RateLimiter {
    fn new(min_interval: Duration) -> Self {
        Self {
            last_sent: Instant::now() - min_interval, // Allow first message immediately
            min_interval,
            messages_since_last: 0,
        }
    }

    fn should_send(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_sent) >= self.min_interval {
            self.last_sent = now;
            self.messages_since_last = 0;
            true
        } else {
            self.messages_since_last += 1;
            false
        }
    }
}

impl Default for CadenceTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl CadenceTranslator {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            _message_tracker: HashMap::new(),
            rate_limiters: HashMap::new(),
        }
    }

    /// Add a cadence translation rule
    pub fn add_rule(&mut self, rule: CadenceRule) {
        self.rules.push(rule);
    }

    /// Get default rules for Iridium -> ASTS translation
    pub fn default_iridium_to_asts_rules() -> Vec<CadenceRule> {
        vec![
            // Heartbeat: Iridium (1/hour) -> ASTS (5 seconds)
            CadenceRule {
                source_protocol: Protocol::IridiumSBD,
                source_cadence: MessageCadence::OncePerHour,
                target_protocol: Protocol::ASTSpaceMobile,
                target_cadence: MessageCadence::Every5Seconds,
                message_type: "heartbeat".to_string(),
                transformation: CadenceTransformation::Duplicate {
                    max_duplicates: 720,
                }, // 3600/5 = 720
            },
            // Location: Iridium (1/minute) -> ASTS (10 seconds)
            CadenceRule {
                source_protocol: Protocol::IridiumSBD,
                source_cadence: MessageCadence::OncePerMinute,
                target_protocol: Protocol::ASTSpaceMobile,
                target_cadence: MessageCadence::Every10Seconds,
                message_type: "location".to_string(),
                transformation: CadenceTransformation::Throttle {
                    target_interval_ms: 10000,
                },
            },
            // Telemetry: Iridium (1/minute) -> ASTS (1/second)
            CadenceRule {
                source_protocol: Protocol::IridiumSBD,
                source_cadence: MessageCadence::OncePerMinute,
                target_protocol: Protocol::ASTSpaceMobile,
                target_cadence: MessageCadence::OncePerSecond,
                message_type: "telemetry".to_string(),
                transformation: CadenceTransformation::Duplicate { max_duplicates: 60 },
            },
            // Emergency: Passthrough regardless of cadence
            CadenceRule {
                source_protocol: Protocol::IridiumSBD,
                source_cadence: MessageCadence::OncePerMinute,
                target_protocol: Protocol::ASTSpaceMobile,
                target_cadence: MessageCadence::OncePerMinute,
                message_type: "emergency".to_string(),
                transformation: CadenceTransformation::Passthrough,
            },
        ]
    }

    /// Translate message based on cadence rules
    pub fn translate_message(
        &mut self,
        message_type: &str,
        source_protocol: Protocol,
        priority: Priority,
    ) -> TranslationAction {
        // Emergency messages always pass through
        if priority == Priority::Emergency {
            return TranslationAction::Send;
        }

        // Find matching rule
        let rule = self
            .rules
            .iter()
            .find(|r| r.message_type == message_type && r.source_protocol == source_protocol);

        if let Some(rule) = rule {
            match &rule.transformation {
                CadenceTransformation::Passthrough => TranslationAction::Send,
                CadenceTransformation::RateLimit { max_per_second } => {
                    let key = format!("{}:{}", rule.message_type, source_protocol);
                    let limiter = self.rate_limiters.entry(key.clone()).or_insert_with(|| {
                        RateLimiter::new(Duration::from_secs_f64(1.0 / max_per_second))
                    });

                    if limiter.should_send() {
                        TranslationAction::Send
                    } else {
                        TranslationAction::Drop
                    }
                }
                CadenceTransformation::Aggregate { window_ms } => {
                    let _key = format!("{}:{}", rule.message_type, source_protocol);
                    // TODO: Implement aggregation logic
                    TranslationAction::Buffer {
                        window_ms: *window_ms,
                    }
                }
                CadenceTransformation::Duplicate { max_duplicates } => {
                    TranslationAction::Duplicate {
                        count: *max_duplicates,
                    }
                }
                CadenceTransformation::Throttle { target_interval_ms } => {
                    let key = format!("{}:{}", rule.message_type, source_protocol);
                    let limiter = self.rate_limiters.entry(key.clone()).or_insert_with(|| {
                        RateLimiter::new(Duration::from_millis(*target_interval_ms))
                    });

                    if limiter.should_send() {
                        TranslationAction::Send
                    } else {
                        TranslationAction::Drop
                    }
                }
            }
        } else {
            // No rule found, default to passthrough
            TranslationAction::Send
        }
    }

    /// Get recommended target cadence for a message type
    pub fn get_target_cadence(
        &self,
        message_type: &str,
        source_protocol: Protocol,
    ) -> Option<MessageCadence> {
        self.rules
            .iter()
            .find(|r| r.message_type == message_type && r.source_protocol == source_protocol)
            .map(|r| r.target_cadence)
    }

    /// Get statistics about cadence translation
    pub fn stats(&self) -> CadenceStats {
        let total_rules = self.rules.len();
        let active_rate_limiters = self.rate_limiters.len();

        CadenceStats {
            total_rules,
            active_rate_limiters,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TranslationAction {
    Send,
    Drop,
    Duplicate { count: usize },
    Buffer { window_ms: u64 },
}

#[derive(Debug, Clone)]
pub struct CadenceStats {
    pub total_rules: usize,
    pub active_rate_limiters: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cadence_translation() {
        let mut translator = CadenceTranslator::new();
        translator.add_rule(CadenceRule {
            source_protocol: Protocol::IridiumSBD,
            source_cadence: MessageCadence::OncePerHour,
            target_protocol: Protocol::ASTSpaceMobile,
            target_cadence: MessageCadence::Every5Seconds,
            message_type: "heartbeat".to_string(),
            transformation: CadenceTransformation::Throttle {
                target_interval_ms: 5000,
            },
        });

        let action =
            translator.translate_message("heartbeat", Protocol::IridiumSBD, Priority::Operational);
        assert!(matches!(action, TranslationAction::Send));
    }

    #[test]
    fn test_emergency_passthrough() {
        let mut translator = CadenceTranslator::new();

        let action =
            translator.translate_message("heartbeat", Protocol::IridiumSBD, Priority::Emergency);
        assert!(matches!(action, TranslationAction::Send));
    }

    #[test]
    fn test_default_rules() {
        let rules = CadenceTranslator::default_iridium_to_asts_rules();
        assert_eq!(rules.len(), 4);
    }
}
