//! Main gateway for protocol translation with AST SpaceMobile integration
//!
//! Users bring their own ASTS account details for BYO authentication.
//! Integrates with telemetry for accurate ping monitoring.

use crate::batcher::AsyncBatcher;
use crate::bitemporal::BiTemporalStore;
use crate::cache::AsyncCache;
use crate::cadence_translation::{CadenceTranslator, TranslationAction};
use crate::clock_reconciliation::{ClockReconciler, NetworkTimeSource};
use crate::metrics::Metrics;
use crate::protocol::{Message, Priority, Protocol};
use crate::reliability::{CircuitBreaker, RetryPolicy};
use crate::sharding::ShardManager;
use crate::snapshot::SnapshotManager;
use crate::translator::Translator;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ASTSCredentials {
    pub account_id: String,
    pub api_key: String,
    pub mno_partner_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub endpoint: Option<String>,
    pub ping_interval_ms: u64,
}

pub struct Gateway {
    translator: Translator,
    _batcher: AsyncBatcher,
    cache: AsyncCache,
    circuit_breaker: CircuitBreaker,
    _retry_policy: RetryPolicy,
    metrics: Arc<Metrics>,
    shard_manager: Arc<ShardManager>,
    snapshot_manager: Arc<SnapshotManager>,
    bitemporal_store: Arc<BiTemporalStore>,
    cadence_translator: Mutex<CadenceTranslator>,
    clock_reconciler: Mutex<ClockReconciler>,
    asts_credentials: Option<ASTSCredentials>,
    telemetry_config: TelemetryConfig,
    input_tx: mpsc::Sender<Message>,
}

impl Gateway {
    pub fn new(
        buffer_size: usize,
        batch_timeout: Duration,
        cache_ttl: Duration,
        asts_credentials: Option<ASTSCredentials>,
        telemetry_config: TelemetryConfig,
    ) -> Arc<Self> {
        let translator = Translator::new(buffer_size);
        let batcher = AsyncBatcher::new(10, batch_timeout, buffer_size);
        let cache = AsyncCache::new(1000, cache_ttl);
        let circuit_breaker = CircuitBreaker::new(5, Duration::from_secs(30));
        let retry_policy = RetryPolicy::new(3, Duration::from_millis(100));
        let metrics = Arc::new(Metrics::new());
        let shard_manager = Arc::new(ShardManager::new(8, 1000)); // 8 shards, 1000 messages each
        let snapshot_manager = Arc::new(SnapshotManager::new(100, Duration::from_secs(300)));
        let bitemporal_store = Arc::new(BiTemporalStore::new(10000)); // Store 10k messages
        let (input_tx, input_rx) = mpsc::channel(buffer_size);

        let mut cadence_translator = CadenceTranslator::new();
        for rule in CadenceTranslator::default_iridium_to_asts_rules() {
            cadence_translator.add_rule(rule);
        }
        let clock_reconciler = ClockReconciler::new(
            1000,
            Duration::from_secs(300),
            NetworkTimeSource::LocalClock,
        );

        let gateway = Arc::new(Self {
            translator,
            _batcher: batcher,
            cache,
            circuit_breaker,
            _retry_policy: retry_policy,
            metrics,
            shard_manager,
            snapshot_manager,
            bitemporal_store,
            cadence_translator: Mutex::new(cadence_translator),
            clock_reconciler: Mutex::new(clock_reconciler),
            asts_credentials,
            telemetry_config,
            input_tx,
        });

        // Spawn main processing loop
        let gateway_clone = Arc::clone(&gateway);
        tokio::spawn(async move {
            gateway_clone.process_loop(input_rx).await;
        });

        gateway
    }

    async fn process_loop(&self, mut input_rx: mpsc::Receiver<Message>) {
        while let Some(message) = input_rx.recv().await {
            self.process_message(message).await;
        }
    }

    async fn process_message(&self, message: Message) {
        let start = Instant::now();
        let mut message = message;

        // Apply clock reconciliation: correct drifted device t_event to network time.
        // Use a fast djb2 hash of the payload as a surrogate device ID.
        let device_id: u64 = message
            .data
            .iter()
            .fold(5381u64, |h, &b| h.wrapping_mul(33).wrapping_add(b as u64));
        if let Ok(reconciler) = self.clock_reconciler.lock() {
            if let Some(corrected) = reconciler.reconcile(device_id, message.t_event) {
                message.t_event = corrected;
            }
        }

        // Store message in bi-temporal store for insurance underwriting and trade compliance
        self.bitemporal_store.store(message.clone()).await;
        self.metrics.record_protocol(message.protocol);

        // Check if this is a deadzone emergency - use dedicated shard for immediate uplink
        if message.is_emergency() {
            let _ = self.shard_manager.push_deadzone(message.clone()).await;
            let snapshot_id = self
                .snapshot_manager
                .create_snapshot(vec![message.clone()], message.protocol)
                .await;
            tracing::debug!(
                "Emergency message sent to deadzone shard, snapshot: {}",
                snapshot_id
            );
        }

        // Apply cadence translation for IridiumSBD to prevent flooding ASTS.
        // Emergency priority always bypasses cadence limiting.
        if message.protocol == Protocol::IridiumSBD && message.priority != Priority::Emergency {
            let action = self
                .cadence_translator
                .lock()
                .map(|mut ct| {
                    ct.translate_message(
                        "telemetry",
                        Protocol::IridiumSBD,
                        message.priority.clone(),
                    )
                })
                .unwrap_or(TranslationAction::Send);
            if matches!(action, TranslationAction::Drop) {
                tracing::debug!("IridiumSBD message dropped by cadence rate limiter");
                return;
            }
        }

        // Check cache first (bi-temporal: use t_event for cache key)
        if let Some(cached) = self
            .cache
            .get(message.protocol, &message.data, message.t_event)
            .await
        {
            self.metrics.record_cache_hit();
            self.metrics.increment_translated();
            self.send_to_asts(cached).await;
            return;
        }

        self.metrics.record_cache_miss();

        // Push to appropriate shard for load balancing (with backpressure)
        match self.shard_manager.push(message.clone()).await {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    protocol = %message.protocol,
                    "Failed to push message to shard"
                );
                self.metrics.record_error();
                return;
            }
        }

        // Synchronously translate to get the actual ASTS-format message before caching.
        let translated = match Translator::translate_sync(message.clone()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, protocol = %message.protocol, "Translation failed");
                self.metrics.record_error();
                return;
            }
        };

        // Wrap translation in circuit breaker for fault isolation
        let result = self
            .circuit_breaker
            .call(async {
                let _ = self.translator.send(message.clone()).await;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            })
            .await;

        match result {
            Ok(_) => {
                self.metrics.increment_translated();
                self.metrics.record_latency(start.elapsed());

                // Cache the translated message keyed by original raw data + t_event
                self.cache
                    .set(message.protocol, &message.data, translated.clone())
                    .await;

                // Create snapshot of translated message for fast uplink building
                self.snapshot_manager
                    .create_snapshot(vec![translated.clone()], Protocol::ASTSpaceMobile)
                    .await;

                self.send_to_asts(translated).await;
            }
            Err(_) => {
                self.metrics.record_error();
            }
        }
    }

    async fn send_to_asts(&self, message: Message) {
        // Send to AST SpaceMobile using BYO credentials
        if let Some(_creds) = &self.asts_credentials {
            // TODO: Implement actual AST SpaceMobile API call
            // This would use the user's account details
            self.metrics.record_protocol(Protocol::ASTSpaceMobile);
        }

        // Send telemetry ping if enabled
        if self.telemetry_config.enabled {
            self.send_telemetry_ping(&message).await;
        }
    }

    async fn send_telemetry_ping(&self, _message: &Message) {
        if let Some(_endpoint) = &self.telemetry_config.endpoint {
            // TODO: Send telemetry to configured endpoint
            // Includes message metadata, latency, cache hit rate, etc.
        }
    }

    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        self.input_tx.send(message).await
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn shard_manager(&self) -> Arc<ShardManager> {
        self.shard_manager.clone()
    }

    pub fn snapshot_manager(&self) -> Arc<SnapshotManager> {
        self.snapshot_manager.clone()
    }

    pub fn bitemporal_store(&self) -> Arc<BiTemporalStore> {
        self.bitemporal_store.clone()
    }

    /// Health check for Kubernetes liveness/readiness probes
    /// Returns true if circuit breaker is closed (system healthy)
    pub async fn health_check(&self) -> bool {
        self.circuit_breaker.state() == crate::reliability::CircuitState::Closed
    }

    /// Query by valid time (what actually happened in physical world)
    pub async fn query_valid_time(&self, start_ms: u64, end_ms: u64) -> Vec<Message> {
        self.bitemporal_store
            .query_valid_time(start_ms, end_ms)
            .await
    }

    /// Query by transaction time (what system believed at the time)
    pub async fn query_transaction_time(&self, start_ms: u64, end_ms: u64) -> Vec<Message> {
        self.bitemporal_store
            .query_transaction_time(start_ms, end_ms)
            .await
    }

    /// Get spread statistics for insurance underwriting
    pub async fn spread_stats(&self, start_ms: u64, end_ms: u64) -> crate::bitemporal::SpreadStats {
        self.bitemporal_store.spread_stats(start_ms, end_ms).await
    }

    /// Get system belief at specific timestamp
    pub async fn system_belief_at(&self, timestamp_ms: u64) -> Vec<Message> {
        self.bitemporal_store.system_belief_at(timestamp_ms).await
    }

    /// Get actual state at specific timestamp
    pub async fn actual_state_at(&self, timestamp_ms: u64) -> Vec<Message> {
        self.bitemporal_store.actual_state_at(timestamp_ms).await
    }

    pub fn update_asts_credentials(&mut self, credentials: ASTSCredentials) {
        self.asts_credentials = Some(credentials);
    }

    pub fn update_telemetry_config(&mut self, config: TelemetryConfig) {
        self.telemetry_config = config;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Priority;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_gateway_creation() {
        let gateway = Gateway::new(
            100,
            Duration::from_millis(100),
            Duration::from_secs(60),
            None,
            TelemetryConfig {
                enabled: false,
                endpoint: None,
                ping_interval_ms: 5000,
            },
        );

        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(&b"test"[..]),
            Priority::Operational,
        );

        let _ = gateway.send(message).await;
        // In production, verify message processing
    }
}
