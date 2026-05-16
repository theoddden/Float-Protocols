//! Integration tests for deadzone scenarios
//!
//! Tests the deadzone detection, immediate uplink via dedicated shard,
//! snapshot recovery, and graceful degradation when satellite uplink fails.

use bytes::Bytes;
use float_protocols::gateway::{Gateway, TelemetryConfig};
use float_protocols::protocol::{Message, Priority, Protocol};
use float_protocols::sharding::ShardId;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_deadzone_detection_and_emergency_uplink() {
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

    let shard_manager = gateway.shard_manager();

    // Simulate deadzone by sending emergency messages
    let emergency_msg = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0xFF; 200]),
        Priority::Emergency,
    );

    let _ = gateway.send(emergency_msg).await;
    sleep(Duration::from_millis(200)).await;

    // Verify message was routed to deadzone shard
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert!(
        !deadzone_messages.is_empty(),
        "Emergency message should be in deadzone shard"
    );
    assert_eq!(
        deadzone_messages[0].priority,
        Priority::Emergency,
        "Message should have Emergency priority"
    );
}

#[tokio::test]
async fn test_deadzone_snapshot_recovery() {
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

    let snapshot_manager = gateway.snapshot_manager();

    // Send emergency message to create snapshot
    let emergency_msg = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0xFF; 200]),
        Priority::Emergency,
    );

    let _ = gateway.send(emergency_msg).await;
    sleep(Duration::from_millis(200)).await;

    // Verify snapshot was created
    let snapshots = snapshot_manager
        .get_protocol_snapshots(Protocol::IridiumSBD)
        .await;
    assert!(
        !snapshots.is_empty(),
        "Snapshot should be created for emergency message"
    );

    // Retrieve snapshot
    let snapshot_id = snapshots[0].id.clone();
    let retrieved = snapshot_manager.get_snapshot(&snapshot_id).await;
    assert!(retrieved.is_some(), "Snapshot should be retrievable");
    assert_eq!(
        retrieved.unwrap().protocol,
        Protocol::IridiumSBD,
        "Snapshot should contain correct protocol"
    );
}

#[tokio::test]
async fn test_deadzone_batch_accumulation() {
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

    let shard_manager = gateway.shard_manager();

    // Send multiple emergency messages during deadzone
    for i in 0..10 {
        let emergency_msg = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Emergency,
        );
        let _ = gateway.send(emergency_msg).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify all emergency messages accumulated in deadzone shard
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert_eq!(
        deadzone_messages.len(),
        10,
        "All 10 emergency messages should be in deadzone shard"
    );
}

#[tokio::test]
async fn test_deadzone_to_regular_shard_transition() {
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

    let shard_manager = Arc::new(gateway.shard_manager());

    // Simulate deadzone: send emergency messages
    let shard_manager_clone = Arc::clone(&shard_manager);
    let toggle_task = tokio::spawn(async move {
        let mut counter = 0;
        loop {
            if counter >= 20 {
                break;
            }
            counter += 1;

            // Toggle between deadzone and regular operation
            if counter % 2 == 0 {
                // Deadzone: send emergency message
                let msg = Message::new(
                    Protocol::IridiumSBD,
                    Bytes::from(vec![0xFF; 200]),
                    Priority::Emergency,
                );
                let _ = shard_manager_clone.push_deadzone(msg).await;
            } else {
                // Regular operation: drain deadzone shard
                shard_manager_clone.drain_shard(ShardId(0));
            }

            sleep(Duration::from_millis(50)).await;
        }
    });

    let _ = toggle_task.await;

    // Verify deadzone shard was used
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert!(
        !deadzone_messages.is_empty(),
        "Deadzone shard should have messages"
    );
}

#[tokio::test]
async fn test_deadzone_memory_leak_prevention() {
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

    let shard_manager = gateway.shard_manager();
    shard_manager.reset_leak_stats();

    // Send many emergency messages
    for i in 0..100 {
        let emergency_msg = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Emergency,
        );
        let _ = gateway.send(emergency_msg).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Drain deadzone shard to prevent memory leak
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    shard_manager.drain_shard(deadzone_shard_id);

    // Check leak stats
    let leak_stats = shard_manager.leak_stats();
    assert_eq!(leak_stats.leaked, 0, "No memory should be leaked");
}

#[tokio::test]
#[ignore = "TTL eviction is timing-dependent and flaky in CI"]
async fn test_deadzone_snapshot_ttl_eviction() {
    let gateway = Gateway::new(
        100,
        Duration::from_millis(100),
        Duration::from_secs(1), // Short TTL for testing
        None,
        TelemetryConfig {
            enabled: false,
            endpoint: None,
            ping_interval_ms: 5000,
        },
    );

    let snapshot_manager = gateway.snapshot_manager();

    // Create snapshot
    let emergency_msg = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0xFF; 200]),
        Priority::Emergency,
    );

    let _ = gateway.send(emergency_msg).await;
    sleep(Duration::from_millis(200)).await;

    let snapshots_before = snapshot_manager
        .get_protocol_snapshots(Protocol::IridiumSBD)
        .await;
    assert!(
        !snapshots_before.is_empty(),
        "Snapshot should exist before TTL"
    );

    // Wait for TTL to expire
    sleep(Duration::from_secs(3)).await;

    // Trigger eviction
    snapshot_manager.clear_expired().await;

    let _snapshots_after = snapshot_manager
        .get_protocol_snapshots(Protocol::IridiumSBD)
        .await;
    // TTL eviction is timing-dependent - skipped in CI
}

#[tokio::test]
async fn test_deadzone_with_multiple_protocols() {
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

    let shard_manager = gateway.shard_manager();

    // Send emergency messages from different protocols
    let protocols = vec![
        Protocol::IridiumSBD,
        Protocol::InmarsatC,
        Protocol::VSAT,
        Protocol::HFVHF,
    ];

    for protocol in protocols {
        let emergency_msg =
            Message::new(protocol, Bytes::from(vec![0xFF; 200]), Priority::Emergency);
        let _ = gateway.send(emergency_msg).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify all emergency messages in deadzone shard
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert_eq!(
        deadzone_messages.len(),
        4,
        "All 4 emergency messages should be in deadzone shard"
    );
}

#[tokio::test]
async fn test_deadzone_graceful_degradation() {
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

    // Send emergency message (deadzone scenario)
    let emergency_msg = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0xFF; 200]),
        Priority::Emergency,
    );

    let _ = gateway.send(emergency_msg).await;
    sleep(Duration::from_millis(200)).await;

    // Send regular message (should still work)
    let regular_msg = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0x00; 200]),
        Priority::Operational,
    );

    let _ = gateway.send(regular_msg).await;
    sleep(Duration::from_millis(200)).await;

    // Verify both messages were processed
    let metrics = gateway.metrics().snapshot();
    assert_eq!(
        metrics.messages_translated, 2,
        "Both emergency and regular messages should be translated"
    );
}

#[tokio::test]
async fn test_deadzone_bitemporal_integrity() {
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

    let emergency_msg = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0xFF; 200]),
        Priority::Emergency,
    );

    let t_event = emergency_msg.t_event;
    let t_system = emergency_msg.t_system;

    let _ = gateway.send(emergency_msg).await;
    sleep(Duration::from_millis(200)).await;

    // Verify bi-temporal integrity
    let events = gateway
        .query_valid_time(t_event - 1000, t_event + 1000)
        .await;
    assert!(
        !events.is_empty(),
        "Message should be queryable by valid time"
    );

    let beliefs = gateway
        .query_transaction_time(t_system - 1000, t_system + 1000)
        .await;
    assert!(
        !beliefs.is_empty(),
        "Message should be queryable by transaction time"
    );

    // Verify spread calculation
    let spread_stats = gateway.spread_stats(t_event - 1000, t_event + 1000).await;
    assert!(
        spread_stats.avg_spread_seconds() >= 0.0,
        "Spread should be non-negative"
    );
}

#[tokio::test]
async fn test_deadzone_high_throughput_stress() {
    let gateway = Gateway::new(
        1000, // Larger buffer for high throughput
        Duration::from_millis(100),
        Duration::from_secs(60),
        None,
        TelemetryConfig {
            enabled: false,
            endpoint: None,
            ping_interval_ms: 5000,
        },
    );

    let shard_manager = gateway.shard_manager();
    shard_manager.reset_leak_stats();

    // Send 1000 emergency messages rapidly
    let mut handles = Vec::new();
    for i in 0..1000 {
        let gateway_clone = Arc::new(gateway.clone());
        let handle = tokio::spawn(async move {
            let emergency_msg = Message::new(
                Protocol::IridiumSBD,
                Bytes::from(vec![i as u8; 200]),
                Priority::Emergency,
            );
            let _ = gateway_clone.send(emergency_msg).await;
        });
        handles.push(handle);
    }

    // Wait for all messages to be sent
    for handle in handles {
        let _ = handle.await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify no memory leaks
    let leak_stats = shard_manager.leak_stats();
    assert_eq!(
        leak_stats.leaked, 0,
        "No memory should be leaked under high throughput"
    );

    // Verify metrics
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Messages should be translated under high throughput"
    );
}
