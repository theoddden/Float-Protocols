//! Async memory-efficient batcher inspired by vLLM's batching patterns
//!
//! Groups messages for efficient processing while maintaining low latency
//! for emergency messages. Uses fixed-size buffers and backpressure.

use crate::protocol::{Message, Priority};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

/// Messages with bi-temporal spread exceeding this threshold (30 seconds) are
/// considered reconnect-burst candidates and trigger an immediate batch flush.
pub const DEFAULT_SPREAD_FLUSH_THRESHOLD_MS: i64 = 30_000;

pub struct AsyncBatcher {
    _buffer: Vec<Message>,
    _max_batch_size: usize,
    _batch_timeout: Duration,
    input_tx: mpsc::Sender<Message>,
    batch_rx: Option<mpsc::Receiver<Vec<Message>>>,
}

impl AsyncBatcher {
    pub fn new(max_batch_size: usize, batch_timeout: Duration, buffer_size: usize) -> Self {
        Self::new_with_spread_threshold(
            max_batch_size,
            batch_timeout,
            buffer_size,
            DEFAULT_SPREAD_FLUSH_THRESHOLD_MS,
        )
    }

    /// Create a batcher with an explicit spread flush threshold.
    /// When any buffered message's bi-temporal spread exceeds `spread_threshold_ms`,
    /// the batch is flushed immediately — reconnect-burst messages should not sit
    /// in the buffer waiting for the normal time window.
    pub fn new_with_spread_threshold(
        max_batch_size: usize,
        batch_timeout: Duration,
        buffer_size: usize,
        spread_threshold_ms: i64,
    ) -> Self {
        let (input_tx, mut input_rx) = mpsc::channel::<Message>(buffer_size);
        let (output_tx, output_rx) = mpsc::channel::<Vec<Message>>(buffer_size);

        // Spawn async batching task
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut last_flush = Instant::now();

            loop {
                tokio::select! {
                    // Receive new message
                    maybe_msg = input_rx.recv() => {
                        match maybe_msg {
                            Some(msg) => {
                                // Emergency messages bypass batching entirely
                                if msg.is_emergency() {
                                    let emergency_batch = vec![msg];
                                    let _ = output_tx.send(emergency_batch).await;
                                } else {
                                    buffer.push(msg);

                                    // Flush if buffer full OR timeout OR priority/spread trigger
                                    if buffer.len() >= max_batch_size
                                        || last_flush.elapsed() >= batch_timeout
                                        || Self::should_flush(&buffer, spread_threshold_ms)
                                    {
                                        let batch = std::mem::take(&mut buffer);
                                        if !batch.is_empty() {
                                            let _ = output_tx.send(batch).await;
                                        }
                                        last_flush = Instant::now();
                                    }
                                }
                            }
                            None => break, // Channel closed
                        }
                    }

                    // Timeout flush
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
            _buffer: Vec::new(),
            _max_batch_size: max_batch_size,
            _batch_timeout: batch_timeout,
            input_tx,
            batch_rx: Some(output_rx),
        }
    }

    /// vLLM-inspired heuristic: flush if high-priority or high-spread messages accumulate.
    ///
    /// Two triggers:
    /// 1. 5+ SafetyCritical messages in the buffer
    /// 2. Any message with bi-temporal spread > spread_threshold_ms — these are
    ///    reconnect-burst messages that are already stale and must exit the buffer
    ///    immediately, not after the normal batch window.
    fn should_flush(buffer: &[Message], spread_threshold_ms: i64) -> bool {
        let safety_critical_count = buffer
            .iter()
            .filter(|m| m.priority == Priority::SafetyCritical)
            .count();

        if safety_critical_count >= 5 {
            return true;
        }

        // Flush immediately if any buffered message is a reconnect-burst candidate
        buffer.iter().any(|m| m.spread_ms() > spread_threshold_ms)
    }

    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        self.input_tx.send(message).await
    }

    /// Take the batch receiver to consume translated batches.
    /// Can only be called once — subsequent calls return None.
    pub fn take_batch_receiver(&mut self) -> Option<mpsc::Receiver<Vec<Message>>> {
        self.batch_rx.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_emergency_bypass() {
        let batcher = AsyncBatcher::new(10, Duration::from_millis(100), 100);

        let emergency_msg = Message::new(
            crate::protocol::Protocol::IridiumSBD,
            bytes::Bytes::from(&b"emergency"[..]),
            crate::protocol::Priority::Emergency,
        );

        // Emergency messages should be sent immediately
        let _ = batcher.send(emergency_msg).await;
        // In production, verify receiver gets single-message batch
    }
}
