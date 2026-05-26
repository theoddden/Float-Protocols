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
    translator: Translator, // Fallback for single-message paths (emergency, spread drain)
    translator_pool: Arc<TranslatorPool>, // Pool for batched translation in shard workers
    batcher: AsyncBatcher,
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
    // input_tx removed: send() routes through batcher directly
}

impl Gateway {
    pub fn new(
        buffer_size: usize,
        batch_timeout: Duration,
        cache_ttl: Duration,
        asts_credentials: Option<ASTSCredentials>,
        telemetry_config: TelemetryConfig,
    ) -> Arc<Self> {
        let translator = Translator::new(100);
        let translator_pool = Arc::new(TranslatorPool::new(4));
        // Extract batch_rx BEFORE moving batcher into the struct.
        let mut batcher = AsyncBatcher::new(10, batch_timeout, buffer_size);
        let batch_rx = batcher
            .take_batch_receiver()
            .expect("batch receiver taken before construction");
        let cache = AsyncCache::new(1000, cache_ttl);
        let circuit_breaker = CircuitBreaker::new(5, Duration::from_secs(30));
        let retry_policy = RetryPolicy::new(3, Duration::from_millis(100));
        let metrics = Arc::new(Metrics::new());
        let shard_manager = Arc::new(ShardManager::new(8, 1000));
        let snapshot_manager = Arc::new(SnapshotManager::new(100, Duration::from_secs(300)));
        let bitemporal_store = Arc::new(BiTemporalStore::new(10000));

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
            translator_pool,
            batcher,
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
        });

        // Spawn per-shard workers for regular shards (1 through num_shards-2).
        // Each worker parks on spawn_blocking until a message arrives, then
        // greedily drains up to batch_size=10 and calls process_translated_batch.
        // ShardId(0) = deadzone  (emergency only, never consumed by a worker)
        // ShardId(8) = spread    (drained by background 50ms task)
        let num_regular_shards = 8usize;
        for shard_idx in 1..num_regular_shards {
            let shard_id = ShardId(shard_idx as u64);
            if let Some(receiver) = shard_manager.get_receiver(shard_id) {
                let worker = ShardWorker::new(shard_id, receiver);
                let gw = Arc::clone(&gateway);
                worker.run_batched(10, move |_sid, messages| {
                    let gw = Arc::clone(&gw);
                    async move { gw.process_translated_batch(messages).await }
                });
            }
        }

        // Spawn batcher consumer: drains the batcher output channel and routes
        // each batch through process_incoming_batch (clock reconcile, bitemporal
        // store, EWMA, cadence filter, cache check, shard push).
        let gw = Arc::clone(&gateway);
        tokio::spawn(async move {
            let mut rx = batch_rx;
            while let Some(batch) = rx.recv().await {
                gw.process_incoming_batch(batch).await;
            }
        });

        // Spawn background spread-shard drainer (every 50ms).
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

    /// djb2 hash of message payload used as a surrogate device ID.
    fn djb2_hash(data: &bytes::Bytes) -> u64 {
        data.iter()
            .fold(5381u64, |h, &b| h.wrapping_mul(33).wrapping_add(b as u64))
    }

    /// Emergency fast path: bypasses the batcher entirely.
    /// Pushes to the dedicated deadzone shard, snapshots, translates, and emits.
    /// This path MUST complete in <2ms to satisfy the emergency latency SLA.
    async fn process_emergency(&self, message: Message) {
        let start = Instant::now();
        let mut message = message;
        let device_id = Self::djb2_hash(&message.data);
        {
            let reconciler = self.clock_reconciler.lock();
            if let Some(corrected) = reconciler.reconcile(device_id, message.t_event) {
                message.t_event = corrected;
            }
        }
        self.bitemporal_store.store(message.clone()).await;
        self.metrics.record_protocol(message.protocol);
        let _ = self.shard_manager.push_deadzone(message.clone());
        self.snapshot_manager
            .create_snapshot(vec![message.clone()], message.protocol, device_id)
            .await;
        if let Ok(translated) = self.translator.translate_sync(message.clone()) {
            self.cache
                .set(message.protocol, &message.data, translated.clone())
                .await;
            self.send_to_asts(translated).await;
        } else {
            self.metrics.record_error();
        }
        self.metrics.record_latency(start.elapsed());
    }

    /// Process a batch of normal (non-emergency) messages from the batcher output.
    ///
    /// Steps (each with a single lock acquisition per step for the whole batch):
    ///   1. Clock-reconcile all messages (one parking_lot lock for batch)
    ///   2. Bi-temporal store batch (one tokio RwLock write for batch)
    ///   3. EWMA update + spread partition + cadence filter (one parking_lot lock each)
    ///   4. Batch cache lookup (one parking_lot read lock for batch)
    ///   5. Cache hits → emit directly; cache misses → push to shard workers
    async fn process_incoming_batch(&self, batch: Vec<Message>) {
        if batch.is_empty() {
            return;
        }

        // 1. Clock reconcile — acquire once for entire batch
        let reconciled: Vec<Message> = {
            let reconciler = self.clock_reconciler.lock();
            batch
                .into_iter()
                .map(|mut msg| {
                    let device_id = Self::djb2_hash(&msg.data);
                    if let Some(corrected) = reconciler.reconcile(device_id, msg.t_event) {
                        msg.t_event = corrected;
                    }
                    self.metrics.record_protocol(msg.protocol);
                    msg
                })
                .collect()
        };

        // 2. Bi-temporal store — one write lock for entire batch
        self.bitemporal_store.store_batch(&reconciled).await;

        // 3. EWMA + spread partition + cadence filter — one lock each
        let (spread_msgs, normal_msgs): (Vec<Message>, Vec<Message>) = {
            let mut ewma = self.ewma_state.lock();
            let mut ct = self.cadence_translator.lock();
            let mut spread = Vec::new();
            let mut normal = Vec::new();
            for msg in reconciled {
                ewma.update(msg.spread_ms());
                let threshold =
                    ewma.adaptive_threshold_ms(msg.protocol.spread_burst_threshold_ms());
                if msg.spread_ms() > threshold {
                    spread.push(msg);
                } else {
                    // Apply cadence limiting for IridiumSBD
                    if msg.protocol == Protocol::IridiumSBD
                        && msg.priority != Priority::Emergency
                        && matches!(
                            ct.translate_message(
                                "telemetry",
                                Protocol::IridiumSBD,
                                msg.priority.clone()
                            ),
                            TranslationAction::Drop
                        )
                    {
                        tracing::debug!("IridiumSBD message dropped by cadence rate limiter");
                        continue;
                    }
                    normal.push(msg);
                }
            }
            (spread, normal)
        };

        // Route spread messages to spread shard (background drainer handles them)
        for msg in spread_msgs {
            self.metrics.record_spread_shard();
            if let Err(e) = self.shard_manager.push_spread(msg) {
                tracing::warn!(error = %e, "Spread shard push failed");
            }
        }

        if normal_msgs.is_empty() {
            return;
        }

        // 4. Batch cache lookup — one read lock for entire batch
        let cache_results = self.cache.get_batch(
            &normal_msgs
                .iter()
                .map(|m| (m.protocol, &m.data))
                .collect::<Vec<_>>(),
        );

        // 5. Route: hits → emit now; misses → push to shard workers
        for (msg, cached) in normal_msgs.into_iter().zip(cache_results) {
            match cached {
                Some(translated) => {
                    self.metrics.record_cache_hit();
                    self.send_to_asts(translated).await;
                }
                None => {
                    self.metrics.record_cache_miss();
                    if let Err(e) = self.shard_manager.push(msg) {
                        tracing::warn!(error = %e, "Shard push failed after cache miss");
                        self.metrics.record_error();
                    }
                }
            }
        }
    }

    /// Translate, cache, snapshot, and emit a batch of messages from a shard worker.
    ///
    /// Acquires cache write lock and snapshot write lock once each for the batch,
    /// amortizing lock overhead across all N messages.
    /// Uses TranslatorPool for parallel translation across protocols.
    async fn process_translated_batch(&self, messages: Vec<Message>) {
        if messages.is_empty() {
            return;
        }
        let start = Instant::now();

        // Translate all messages using TranslatorPool; collect (original, translated) pairs
        let mut pairs: Vec<(Message, Message)> = Vec::with_capacity(messages.len());
        for msg in messages {
            let protocol = msg.protocol;
            // Borrow translator from pool for this protocol
            if let Some(translator) = self.translator_pool.borrow(protocol) {
                match translator.translate_sync(msg.clone()) {
                    Ok(translated) => pairs.push((msg, translated)),
                    Err(e) => {
                        tracing::warn!(error = %e, protocol = %protocol, "Translation failed");
                        self.metrics.record_error();
                    }
                }
                // Return translator to pool
                self.translator_pool.return_translator(protocol, translator);
            } else {
                // Pool exhausted — fall back to static translation
                tracing::warn!(protocol = %protocol, "Translator pool exhausted, using static fallback");
                match self.translator.translate_sync(msg.clone()) {
                    Ok(translated) => pairs.push((msg, translated)),
                    Err(e) => {
                        tracing::warn!(error = %e, protocol = %protocol, "Static translation failed");
                        self.metrics.record_error();
                    }
                }
            }
        }
        if pairs.is_empty() {
            return;
        }

        // Batch cache set — one write lock
        let cache_entries: Vec<(Protocol, bytes::Bytes, Message)> = pairs
            .iter()
            .map(|(orig, t)| (orig.protocol, orig.data.clone(), t.clone()))
            .collect();
        self.cache.set_batch(cache_entries).await;

        // Batch snapshot — one write lock; use first message's device_id
        let device_id = Self::djb2_hash(&pairs[0].0.data);
        let translated_msgs: Vec<Message> = pairs.into_iter().map(|(_, t)| t).collect();
        self.snapshot_manager
            .create_batch_snapshot(translated_msgs.clone(), Protocol::ASTSpaceMobile, device_id)
            .await;

        // Emit all
        for translated in translated_msgs {
            self.send_to_asts(translated).await;
        }
        self.metrics.record_latency(start.elapsed());
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
            let translated = match self.translator.translate_sync(message.clone()) {
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
            let device_id = Self::djb2_hash(&message.data);
            self.snapshot_manager
                .create_snapshot(
                    vec![translated.clone()],
                    Protocol::ASTSpaceMobile,
                    device_id,
                )
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
        if message.is_emergency() {
            self.process_emergency(message).await;
            Ok(())
        } else {
            self.batcher.send(message).await
        }
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
