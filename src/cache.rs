//! Async cache inspired by LMCache's distributed caching patterns
//!
//! Provides TTL-based caching with async invalidation for protocol
//! translations, reducing redundant computation over expensive satellite links.
//!
//! Cache key is (protocol, payload_hash) — t_event is intentionally excluded.
//! Translation is payload-deterministic: the same raw bytes always produce the
//! same translated output regardless of when the event occurred. Including
//! t_event would give every unique telemetry reading its own cache entry,
//! making the cache a no-op for real satellite traffic.

use crate::protocol::{Message, Protocol};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use tokio::time::{Duration, Instant};

pub struct AsyncCache {
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,
    eviction_order: Mutex<VecDeque<CacheKey>>,
    max_entries: usize,
    default_ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    protocol: Protocol,
    data_hash: u64,
    // t_event removed: translation is payload-deterministic.
    // Caching by (protocol, hash(payload)) enables real hits for
    // retransmitted or burst-duplicate messages from satellite devices.
}

struct CacheEntry {
    message: Message,
    t_event: u64, // retained for time-range invalidation queries
    timestamp: Instant,
    ttl: Duration,
}

impl AsyncCache {
    pub fn new(max_entries: usize, default_ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::with_capacity(max_entries)),
            eviction_order: Mutex::new(VecDeque::with_capacity(max_entries)),
            max_entries,
            default_ttl,
        }
    }

    /// Get cached translation. parking_lot is synchronous so this completes
    /// without yielding — kept async for API compat with callers.
    pub async fn get(&self, protocol: Protocol, data: &Bytes) -> Option<Message> {
        let key = CacheKey {
            protocol,
            data_hash: Self::hash_data(data),
        };
        let entries = self.entries.read();
        entries.get(&key).and_then(|e| {
            if e.timestamp.elapsed() < e.ttl {
                Some(e.message.clone())
            } else {
                None
            }
        })
    }

    /// Cache a translation result with O(1) eviction via insertion-order queue.
    pub async fn set(&self, protocol: Protocol, data: &Bytes, message: Message) {
        let t_event = message.t_event;
        let key = CacheKey {
            protocol,
            data_hash: Self::hash_data(data),
        };
        let entry = CacheEntry {
            message,
            t_event,
            timestamp: Instant::now(),
            ttl: self.default_ttl,
        };
        let mut entries = self.entries.write();
        let mut order = self.eviction_order.lock();
        if entries.len() >= self.max_entries {
            if let Some(oldest) = order.pop_front() {
                entries.remove(&oldest);
            }
        }
        if entries.insert(key.clone(), entry).is_none() {
            order.push_back(key);
        }
    }

    /// Batch get: acquires the read lock once for all N queries.
    /// Returns results in the same order as `queries`.
    pub fn get_batch(&self, queries: &[(Protocol, &Bytes)]) -> Vec<Option<Message>> {
        let entries = self.entries.read();
        queries
            .iter()
            .map(|(protocol, data)| {
                let key = CacheKey {
                    protocol: *protocol,
                    data_hash: Self::hash_data(data),
                };
                entries.get(&key).and_then(|e| {
                    if e.timestamp.elapsed() < e.ttl {
                        Some(e.message.clone())
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    /// Batch set: acquires the write lock once for all N entries.
    pub async fn set_batch(&self, batch: Vec<(Protocol, Bytes, Message)>) {
        if batch.is_empty() {
            return;
        }
        let mut entries = self.entries.write();
        let mut order = self.eviction_order.lock();
        for (protocol, data, message) in batch {
            let t_event = message.t_event;
            let key = CacheKey {
                protocol,
                data_hash: Self::hash_data(&data),
            };
            let entry = CacheEntry {
                message,
                t_event,
                timestamp: Instant::now(),
                ttl: self.default_ttl,
            };
            if entries.len() >= self.max_entries {
                if let Some(oldest) = order.pop_front() {
                    entries.remove(&oldest);
                }
            }
            if entries.insert(key.clone(), entry).is_none() {
                order.push_back(key);
            }
        }
    }

    /// Invalidate cache entries for a specific protocol and time range.
    /// Uses t_event stored in CacheEntry (not CacheKey) for filtering.
    pub async fn invalidate_protocol_time_range(
        &self,
        protocol: Protocol,
        start_ms: u64,
        end_ms: u64,
    ) {
        let mut entries = self.entries.write();
        entries.retain(|key, entry| {
            key.protocol != protocol || !(entry.t_event >= start_ms && entry.t_event <= end_ms)
        });
    }

    /// Invalidate all cache entries for a specific protocol.
    pub async fn invalidate_protocol(&self, protocol: Protocol) {
        let mut entries = self.entries.write();
        entries.retain(|key, _| key.protocol != protocol);
    }

    /// Clear all cache entries.
    pub async fn clear(&self) {
        let mut entries = self.entries.write();
        let mut order = self.eviction_order.lock();
        entries.clear();
        order.clear();
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> CacheStats {
        let entries = self.entries.read();
        let valid_count = entries
            .values()
            .filter(|e| e.timestamp.elapsed() < e.ttl)
            .count();
        CacheStats {
            total_entries: entries.len(),
            valid_entries: valid_count,
            max_entries: self.max_entries,
        }
    }

    fn hash_data(data: &Bytes) -> u64 {
        // djb2 — deterministic, fast for short binary satellite payloads
        data.iter()
            .fold(5381u64, |h, &b| h.wrapping_mul(33).wrapping_add(b as u64))
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub total_entries: usize,
    pub valid_entries: usize,
    pub max_entries: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        if self.total_entries == 0 {
            0.0
        } else {
            self.valid_entries as f64 / self.total_entries as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Priority;

    #[tokio::test]
    async fn test_cache_get_set() {
        let cache = AsyncCache::new(100, Duration::from_secs(60));
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(&b"test data"[..]),
            Priority::Operational,
        );
        cache
            .set(
                Protocol::IridiumSBD,
                &Bytes::from(&b"test data"[..]),
                message.clone(),
            )
            .await;
        // No t_event in get — cache key is (protocol, payload_hash) only
        let cached = cache
            .get(Protocol::IridiumSBD, &Bytes::from(&b"test data"[..]))
            .await;
        assert!(cached.is_some());
    }

    #[tokio::test]
    async fn test_cache_ttl() {
        let cache = AsyncCache::new(100, Duration::from_millis(100));
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(&b"test data"[..]),
            Priority::Operational,
        );
        cache
            .set(
                Protocol::IridiumSBD,
                &Bytes::from(&b"test data"[..]),
                message,
            )
            .await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let cached = cache
            .get(Protocol::IridiumSBD, &Bytes::from(&b"test data"[..]))
            .await;
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_get_batch_hit_and_miss() {
        let cache = AsyncCache::new(100, Duration::from_secs(60));
        let msg = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(&b"batchkey"[..]),
            Priority::Operational,
        );
        cache
            .set(Protocol::IridiumSBD, &Bytes::from(&b"batchkey"[..]), msg)
            .await;
        let results = cache.get_batch(&[(Protocol::IridiumSBD, &Bytes::from(&b"batchkey"[..]))]);
        assert!(results[0].is_some(), "should hit");
        let miss = cache.get_batch(&[(Protocol::InmarsatC, &Bytes::from(&b"batchkey"[..]))]);
        assert!(miss[0].is_none(), "different protocol is a miss");
    }

    #[tokio::test]
    async fn test_eviction_order_o1() {
        let cache = AsyncCache::new(2, Duration::from_secs(60));
        let mk = |n: u8| Bytes::from(vec![n]);
        let msg = |n: u8| Message::new(Protocol::IridiumSBD, mk(n), Priority::Operational);
        cache.set(Protocol::IridiumSBD, &mk(1), msg(1)).await;
        cache.set(Protocol::IridiumSBD, &mk(2), msg(2)).await;
        cache.set(Protocol::IridiumSBD, &mk(3), msg(3)).await; // evicts key 1
        assert!(
            cache.get(Protocol::IridiumSBD, &mk(1)).await.is_none(),
            "key 1 evicted"
        );
        assert!(
            cache.get(Protocol::IridiumSBD, &mk(3)).await.is_some(),
            "key 3 present"
        );
    }
}
