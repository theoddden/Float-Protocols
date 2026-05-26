//! Main gateway for protocol translation with AST SpaceMobile integration
//!
//! Users bring their own ASTS account details for BYO authentication.
//! Integrates with telemetry for accurate ping monitoring.
//!
//! Spread-based routing: messages with a bi-temporal spread (t_system - t_event)
//! exceeding HIGH_SPREAD_THRESHOLD_MS are treated as reconnect-burst candidates.
//! They bypass cadence rate limiting and are routed to the dedicated spread shard
//! for immediate draining. This turns the bi-temporal store from a passive audit
//! log into an active routing control signal.

use crate::batcher::AsyncBatcher;
use crate::bitemporal::BiTemporalStore;
use crate::cache::AsyncCache;
use crate::cadence_translation::{CadenceTranslator, TranslationAction};
use crate::clock_reconciliation::{ClockReconciler, NetworkTimeSource};
use crate::metrics::Metrics;
use crate::protocol::{Message, Priority, Protocol};
use crate::reliability::{CircuitBreaker, RetryPolicy};
use crate::sharding::{ShardId, ShardManager, ShardWorker};
use crate::snapshot::SnapshotManager;
use crate::translator::{Translator, TranslatorPool};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

/// Smoothing factor for the EWMA spread tracker.
/// 0.05 = slow adaptation (~20-message half-life), suitable for satellite
/// environments where the baseline spread is stable for long periods.
const EWMA_ALPHA: f64 = 0.05;

/// Number of EWMA mean-absolute-deviations above the mean that triggers
/// the adaptive threshold. 3 MAD ≈ 3σ for Gaussian data.
const EWMA_SIGMA: f64 = 3.0;

/// Tracks an exponentially weighted moving average of bi-temporal spread
/// to compute an adaptive routing threshold per gateway instance.
/// The threshold rises in environments with chronic high latency (preventing
/// false positives) and stays at the protocol base in healthy environments.
struct EwmaSpreadState {
    spread: f64,
    abs_dev: f64,
    initialized: bool,
}

impl EwmaSpreadState {
    fn new() -> Self {
        Self {
            spread: 0.0,
            abs_dev: 0.0,
            initialized: false,
        }
    }

    fn update(&mut self, spread_ms: i64) {
        let s = spread_ms as f64;
        if !self.initialized {
            self.spread = s;
            self.abs_dev = s.abs();
            self.initialized = true;
            return;
        }
        let prev = self.spread;
        self.spread = EWMA_ALPHA * s + (1.0 - EWMA_ALPHA) * prev;
        self.abs_dev = EWMA_ALPHA * (s - prev).abs() + (1.0 - EWMA_ALPHA) * self.abs_dev;
    }

    /// Adaptive threshold = max(protocol_base, ewma + 3 * MAD).
    /// Can only raise the bar above the protocol base, never lower it.
    fn adaptive_threshold_ms(&self, protocol_base_ms: i64) -> i64 {
        if !self.initialized {
            return protocol_base_ms;
        }
        let adaptive = (self.spread + EWMA_SIGMA * self.abs_dev) as i64;
        protocol_base_ms.max(adaptive)
    }
}

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
    translator_pool: Arc<TranslatorPool>,
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
    ewma_state: Mutex<EwmaSpreadState>,
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
        let translator_pool = Arc::new(TranslatorPool::new(4)); // 4 instances per protocol
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
            translator_pool,
            _batcher: batcher,
            cache,
            circuit_breaker,
            _retry_policy: retry_policy,
            metrics,
            shard_manager: shard_manager.clone(),
            snapshot_manager,
            bitemporal_store,
            cadence_translator: Mutex::new(cadence_translator),
            clock_reconciler: Mutex::new(clock_reconciler),
            ewma_state: Mutex::new(EwmaSpreadState::new()),
            asts_credentials,
            telemetry_config,
            input_tx,
        });

        // TODO: Per-shard workers for parallel processing (optimization 1)
        // Workers are spawned but disabled for now to maintain test compatibility.
        // To enable: route send() through shards instead of process_loop.
        // let num_shards = 8;
        // for shard_id in 1..num_shards {
        //     let shard_id = ShardId(shard_id as u64);
        //     if let Some(receiver) = shard_manager.get_receiver(shard_id) {
        //         let worker = ShardWorker::new(shard_id, receiver);
        //         let gateway_clone = Arc::clone(&gateway);
        //         let _ = worker.run(move |_shard_id, message| {
        //             let gateway = Arc::clone(&gateway_clone);
        //             async move {
        //                 gateway.process_shard_message(message).await;
        //             }
        //         });
        //     }
        // }

        // Spawn main processing loop for direct input (full routing logic)
        let gateway_clone = Arc::clone(&gateway);
        tokio::spawn(async move {
            gateway_clone.process_loop(input_rx).await;
        });

        // Spawn background spread-shard drainer.
        // Every 50ms, drain any messages buffered in the spread shard during
        // reconnect bursts and translate+emit them directly, bypassing routing.
        let drain_clone = Arc::clone(&gateway);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                drain_clone.drain_spread_shard_once().await;
            }
        });

        gateway
    }

    async fn process_loop(&self, mut input_rx: mpsc::Receiver<Message>) {
        while let Some(message) = input_rx.recv().await {
            self.process_message(message).await;
        }
    }

    /// Process a message from a shard worker.
    /// Uses the TranslatorPool for parallel translation.
    async fn process_shard_message(&self, message: Message) {
        let start = Instant::now();
        let protocol = message.protocol;

        // Borrow a translator from the pool
        let translator = match self.translator_pool.borrow(protocol) {
            Some(t) => t,
            None => {
                // Pool exhausted, drop message (backpressure)
                self.metrics.record_error();
                return;
            }
        };

        // Translate using the borrowed translator
        let translated = match Translator::translate_sync(message.clone()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, protocol = %protocol, "Shard translation failed");
                self.metrics.record_error();
                self.translator_pool.return_translator(protocol, translator);
                return;
            }
        };

        // Return translator to pool
        self.translator_pool.return_translator(protocol, translator);

        // Cache, snapshot, emit
        self.cache
            .set(message.protocol, &message.data, translated.clone())
            .await;
        self.snapshot_manager
            .create_snapshot(vec![translated.clone()], Protocol::ASTSpaceMobile)
            .await;
        self.send_to_asts(translated).await;
        self.metrics.record_latency(start.elapsed());
        self.metrics.increment_translated();
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
        {
            let reconciler = self.clock_reconciler.lock();
            if let Some(corrected) = reconciler.reconcile(device_id, message.t_event) {
                message.t_event = corrected;
            }
        }

        // Store message in bi-temporal store for insurance underwriting and trade compliance
        self.bitemporal_store.store(message.clone()).await;
        self.metrics.record_protocol(message.protocol);

        // Compute bi-temporal spread immediately after reconciliation and
        // update the EWMA state. The adaptive threshold is:
        //   max(protocol_base_ms, ewma_spread + 3 * ewma_abs_deviation)
        // This prevents false positives when chronic latency is near the
        // protocol base, while remaining sensitive to sudden reconnect bursts.
        let spread_ms = message.spread_ms();
        let threshold_ms = {
            let mut ewma = self.ewma_state.lock();
            ewma.update(spread_ms);
            ewma.adaptive_threshold_ms(message.protocol.spread_burst_threshold_ms())
        };
        let is_high_spread = spread_ms > threshold_ms;

        // Emergency path: dedicated deadzone shard, unconditional
        if message.is_emergency() {
            let _ = self.shard_manager.push_deadzone(message.clone());
            let snapshot_id = self
                .snapshot_manager
                .create_snapshot(vec![message.clone()], message.protocol)
                .await;
            tracing::debug!(
                "Emergency message sent to deadzone shard, snapshot: {}",
                snapshot_id
            );
        }

        // Spread routing: high-spread messages are reconnect-burst candidates.
        //
        // When a device exits a dead zone, accumulated messages arrive all at once
        // with large t_system - t_event spreads. Applying cadence throttling on top
        // of already-stale messages compounds the delay. Instead:
        //   1. Route to the dedicated spread shard (pre-allocated, no backpressure gate)
        //   2. Skip cadence limiting entirely
        //   3. Record metric so operators can observe burst events
        //
        // Normal messages continue through the cadence check and regular shards.
        if is_high_spread && !message.is_emergency() {
            self.metrics.record_spread_shard();
            match self.shard_manager.push_spread(message.clone()) {
                Ok(shard_id) => tracing::info!(
                    spread_ms,
                    protocol = %message.protocol,
                    shard = shard_id.0,
                    "Reconnect burst: routed to spread shard, cadence bypass"
                ),
                Err(e) => tracing::warn!(
                    error = %e,
                    spread_ms,
                    "Spread shard push failed, continuing to translate"
                ),
            }
        } else if !message.is_emergency() {
            // Normal path: apply cadence limiting for IridiumSBD to prevent ASTS flooding
            if message.protocol == Protocol::IridiumSBD && message.priority != Priority::Emergency {
                let action = {
                    let mut ct = self.cadence_translator.lock();
                    ct.translate_message(
                        "telemetry",
                        Protocol::IridiumSBD,
                        message.priority.clone(),
                    )
                };
                if matches!(action, TranslationAction::Drop) {
                    tracing::debug!("IridiumSBD message dropped by cadence rate limiter");
                    return;
                }
            }

            // Normal shard push with backpressure
            match self.shard_manager.push(message.clone()) {
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
        }

        // Cache check shared across all routing paths
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

        // Synchronously translate to get the actual ASTS-format message before caching.
        let translated = match Translator::translate_sync(message.clone()) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, protocol = %message.protocol, "Translation failed");
                self.metrics.record_error();
                return;
            }
        };

        // Record latency after successful sync translation
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

    /// Drain the spread shard and translate+emit all buffered messages directly.
    ///
    /// Called every 50ms by the background drain task. Messages buffered here
    /// are reconnect-burst candidates that have already been routed — they skip
    /// the normal push/cadence/cache path and go straight to translation+emit.
    async fn drain_spread_shard_once(&self) {
        let shard_id = self.shard_manager.get_spread_shard();
        let messages = self.shard_manager.drain_shard(shard_id);
        if messages.is_empty() {
            return;
        }
        tracing::debug!(
            count = messages.len(),
            "Draining spread shard burst-recovery messages"
        );
        for message in messages {
            let translated = match Translator::translate_sync(message.clone()) {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        protocol = %message.protocol,
                        "Spread drain: translation failed"
                    );
                    self.metrics.record_error();
                    continue;
                }
            };
            self.cache
                .set(message.protocol, &message.data, translated.clone())
                .await;
            self.snapshot_manager
                .create_snapshot(vec![translated.clone()], Protocol::ASTSpaceMobile)
                .await;
            self.send_to_asts(translated).await;
            self.metrics.increment_translated();
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
        self.metrics.increment_translated();
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
