//! Stress tests for race conditions and concurrent access
//!
//! Tests memory sharding under high load and ASTS uplink toggling.

use bytes::Bytes;
use float_protocols::protocol::{Message, Priority, Protocol};
use float_protocols::sharding::{ShardId, ShardManager};
use tokio::time::{interval, Duration};

#[tokio::test]
async fn test_asts_toggle_stress() {
    let shard_manager = std::sync::Arc::new(ShardManager::new(8, 1000));
    shard_manager.reset_leak_stats();

    // Simulate ASTS uplink toggling every 50ms
    let shard_manager_clone = std::sync::Arc::clone(&shard_manager);
    let toggle_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(50));
        let mut uplink_active = true;

        for _ in 0..200 {
            // 200 toggle cycles = 10 seconds
            interval.tick().await;
            uplink_active = !uplink_active;

            if uplink_active {
                // Uplink active: drain deadzone shard
                shard_manager_clone.drain_shard(ShardId(0));
            } else {
                // Uplink down: push to deadzone shard
                let msg = Message::new(
                    Protocol::InmarsatC,
                    Bytes::from(&b"deadzone"[..]),
                    Priority::Emergency,
                );
                let _ = shard_manager_clone.push_deadzone(msg).await;
            }
        }
    });

    // Run toggle task
    let _ = toggle_task.await;

    // Check leak stats
    let leak_stats = shard_manager.leak_stats();
    assert_eq!(leak_stats.leaked, 0, "Memory leaked during ASTS toggle");
}

#[tokio::test]
async fn test_5000_concurrent_writes() {
    let shard_manager = std::sync::Arc::new(ShardManager::new(8, 1000));
    shard_manager.reset_leak_stats();

    // Spawn 5,000 concurrent tasks
    let mut handles = Vec::new();
    for i in 0..5000 {
        let shard_manager_clone = std::sync::Arc::clone(&shard_manager);
        let handle = tokio::spawn(async move {
            let msg = Message::new(
                Protocol::InmarsatC,
                Bytes::from(format!("packet_{}", i)),
                Priority::Operational,
            );
            shard_manager_clone.push(msg).await
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }

    // Validate no leaks
    let leak_stats = shard_manager.leak_stats();
    assert_eq!(
        leak_stats.leaked, 0,
        "Memory leaked during concurrent writes"
    );

    // Validate all messages accounted for
    let stats = shard_manager.stats();
    let expected_messages = leak_stats.allocated - leak_stats.dropped;
    assert_eq!(stats.total_messages as u64, expected_messages);
}

#[tokio::test]
async fn test_backpressure_under_load() {
    let shard_manager = std::sync::Arc::new(ShardManager::new(2, 100)); // Small shards to trigger backpressure
    shard_manager.reset_leak_stats();

    // Fill shards to >80% utilization
    for i in 0..200 {
        let shard_manager_clone = std::sync::Arc::clone(&shard_manager);
        let msg = Message::new(
            Protocol::InmarsatC,
            Bytes::from(format!("fill_{}", i)),
            Priority::Operational,
        );
        let _ = shard_manager_clone.push(msg).await;
    }

    // Now try to push more - should hit backpressure
    let mut backpressure_count = 0;
    for i in 0..100 {
        let shard_manager_clone = std::sync::Arc::clone(&shard_manager);
        let msg = Message::new(
            Protocol::InmarsatC,
            Bytes::from(format!("overflow_{}", i)),
            Priority::Operational,
        );
        if shard_manager_clone.push(msg).await.is_err() {
            backpressure_count += 1;
        }
    }

    // Some pushes should be rejected due to backpressure
    assert!(
        backpressure_count > 0,
        "Backpressure not triggered under load"
    );

    let leak_stats = shard_manager.leak_stats();
    assert!(
        leak_stats.dropped > 0,
        "Messages should have been dropped due to backpressure"
    );
}
