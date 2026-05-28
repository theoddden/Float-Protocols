//! Transmission-layer batcher for satellite uplink efficiency
//!
//! Batches *translated* messages immediately before the ASTS HTTP call,
//! reducing per-message HTTP overhead and improving satellite link utilization.
//!
//! ## Why this reduces latency
//!
//! The mechanism is analogous to vLLM continuous batching:
//! - A constrained resource (GPU / satellite uplink) has fixed overhead per
//!   scheduling unit (token-generation step / HTTP round-trip).
//! - Without batching the resource is underutilized; requests queue up waiting
//!   for individual round-trips to complete.
//! - Filling each scheduling unit to capacity raises utilization → shorter
//!   queue times → lower end-to-end latency.
//!
//! ## What this layer does NOT do
//!
//! It does not touch translation, sharding, clock reconciliation, EWMA, or
//! cadence filtering — those happen upstream.  This layer sits between the
//! last shard worker and `send_batch_to_asts`, acting purely as a
//! transmission coalescer.
//!
//! ## Emergency bypass
//!
//! Emergency messages never reach this layer: `Gateway::send()` routes them
//! directly through `process_emergency()` → `send_to_asts()`.

use crate::protocol::Message;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

pub struct TransmissionBatcher {
    input_tx: mpsc::Sender<Message>,
    batch_rx: Option<mpsc::Receiver<Vec<Message>>>,
    max_batch_size: usize,
    batch_timeout: Duration,
}

impl TransmissionBatcher {
    /// Create a new transmission batcher.
    ///
    /// * `max_batch_size` — flush immediately when this many messages are queued.
    /// * `batch_timeout` — flush after this duration even if the batch is not full.
    /// * `buffer_size`   — depth of the input channel (backpressure limit).
    pub fn new(max_batch_size: usize, batch_timeout: Duration, buffer_size: usize) -> Self {
        let (input_tx, mut input_rx) = mpsc::channel::<Message>(buffer_size);
        let (output_tx, output_rx) = mpsc::channel::<Vec<Message>>(buffer_size);

        tokio::spawn(async move {
            let mut buffer: Vec<Message> = Vec::with_capacity(max_batch_size);
            let mut last_flush = Instant::now();

            loop {
                tokio::select! {
                    maybe_msg = input_rx.recv() => {
                        match maybe_msg {
                            Some(msg) => {
                                buffer.push(msg);
                                if buffer.len() >= max_batch_size
                                    || last_flush.elapsed() >= batch_timeout
                                {
                                    let batch = std::mem::take(&mut buffer);
                                    let _ = output_tx.send(batch).await;
                                    last_flush = Instant::now();
                                }
                            }
                            None => {
                                // Channel closed — flush remaining and exit.
                                if !buffer.is_empty() {
                                    let _ = output_tx.send(buffer).await;
                                }
                                break;
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(last_flush + batch_timeout) => {
                        if !buffer.is_empty() {
                            let batch = std::mem::take(&mut buffer);
                            let _ = output_tx.send(batch).await;
                            last_flush = Instant::now();
                        }
                    }
                }
            }
        });

        Self {
            input_tx,
            batch_rx: Some(output_rx),
            max_batch_size,
            batch_timeout,
        }
    }

    /// Enqueue a translated message for batched transmission.
    /// Returns an error only if the internal channel is closed (fatal).
    pub async fn enqueue(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        self.input_tx.send(message).await
    }

    /// Take the batch receiver.  Must be called exactly once during
    /// Gateway construction before the batcher is moved into the struct.
    pub fn take_batch_receiver(&mut self) -> Option<mpsc::Receiver<Vec<Message>>> {
        self.batch_rx.take()
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub fn batch_timeout(&self) -> Duration {
        self.batch_timeout
    }
}
