//! Async cache inspired by LMCache's distributed caching patterns
//!
//! Provides TTL-based caching with async invalidation for protocol
//! translations, reducing redundant computation over expensive satellite links.

use crate::protocol::{Message, Priority, Protocol};
use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

pub struct AsyncCache {
    entries: RwLock<HashMap<CacheKey, CacheEntry>>,
    max_entries: usize,
    default_ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    protocol: Protocol,
    data_hash: u64,
    t_event: u64, // Include valid time in cache key
}

struct CacheEntry {
    message: Message,
    timestamp: Instant,
    ttl: Duration,
}

impl AsyncCache {
    pub fn new(max_entries: usize, default_ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_entries,
            default_ttl,
        }
    }

    /// Get cached translation result
    pub async fn get(&self, protocol: Protocol, data: &Bytes, t_event: u64) -> Option<Message> {
        let data_hash = Self::hash_data(data);
        let key = CacheKey {
            protocol,
            data_hash,
            t_event,
        };

        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(&key) {
            // Check if entry is still valid (not expired)
            if entry.timestamp.elapsed() < entry.ttl {
                return Some(entry.message.clone());
            }
        }
        None
    }

    /// Cache a translation result
    pub async fn set(&self, protocol: Protocol, data: &Bytes, message: Message) {
        let data_hash = Self::hash_data(data);
        let key = CacheKey {
            protocol,
            data_hash,
            t_event: message.t_event,
        };

        let mut entries = self.entries.write().await;

        // Evict oldest if at capacity
        if entries.len() >= self.max_entries {
            Self::evict_oldest(&mut entries);
        }

        entries.insert(
            key,
            CacheEntry {
                message,
                timestamp: Instant::now(),
                ttl: self.default_ttl,
            },
        );
    }

    /// Invalidate cache entries for a specific protocol and time range
    pub async fn invalidate_protocol_time_range(
        &self,
        protocol: Protocol,
        start_ms: u64,
        end_ms: u64,
    ) {
        let mut entries = self.entries.write().await;
        entries.retain(|key, _| {
            key.protocol != protocol || !(key.t_event >= start_ms && key.t_event <= end_ms)
        });
    }

    /// Invalidate cache entries for a specific protocol
    pub async fn invalidate_protocol(&self, protocol: Protocol) {
        let mut entries = self.entries.write().await;
        entries.retain(|key, _| key.protocol != protocol);
    }

    /// Clear all cache entries
    pub async fn clear(&self) {
        let mut entries = self.entries.write().await;
        entries.clear();
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let entries = self.entries.read().await;
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
        // Simple hash for cache key - in production use proper hash function
        let mut hash: u64 = 5381;
        for byte in data.iter() {
            hash = hash.wrapping_mul(33).wrapping_add(*byte as u64);
        }
        hash
    }

    fn evict_oldest(entries: &mut HashMap<CacheKey, CacheEntry>) {
        if let Some(oldest_key) = entries
            .iter()
            .min_by_key(|(_, entry)| entry.timestamp)
            .map(|(key, _)| key.clone())
        {
            entries.remove(&oldest_key);
        }
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

    #[tokio::test]
    async fn test_cache_get_set() {
        let cache = AsyncCache::new(100, Duration::from_secs(60));

        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(&b"test data"[..]),
            Priority::Operational,
        );

        let t_event = message.t_event;
        cache
            .set(
                Protocol::IridiumSBD,
                &Bytes::from(&b"test data"[..]),
                message.clone(),
            )
            .await;

        let cached = cache
            .get(Protocol::IridiumSBD, &Bytes::from(&b"test data"[..]), t_event)
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

        let t_event = message.t_event;
        cache
            .set(Protocol::IridiumSBD, &Bytes::from(&b"test data"[..]), message)
            .await;

        tokio::time::sleep(Duration::from_millis(150)).await;

        let cached = cache
            .get(Protocol::IridiumSBD, &Bytes::from(&b"test data"[..]), t_event)
            .await;
        assert!(cached.is_none()); // Should be expired
    }
}
