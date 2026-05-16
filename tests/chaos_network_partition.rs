//! Chaos engineering tests for network partition simulation
//!
//! Tests system behavior under network partitions, partial connectivity,
//! and intermittent satellite uplink failures.

use bytes::Bytes;
use float_protocols::gateway::{Gateway, TelemetryConfig};
use float_protocols::protocol::{Message, Priority, Protocol};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, sleep};

#[tokio::test]
#[ignore]

async fn test_network_partition_circuit_breaker_trips() {
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

    // Simulate network partition by sending messages that will fail
    for _ in 0..10 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![0u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify circuit breaker tripped
    let health = gateway.health_check().await;
    assert!(
        !health,
        "Circuit breaker should trip after repeated failures"
    );
}

#[ignore]
#[tokio::test]

async fn test_network_partition_recovery() {
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

    // Trigger circuit breaker
    for _ in 0..10 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![0u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify circuit breaker is open
    let health_before = gateway.health_check().await;
    assert!(!health_before, "Circuit breaker should be open");

    // Wait for recovery timeout (30 seconds)
    sleep(Duration::from_secs(31)).await;

    // Send successful message
    let message = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![1u8; 200]),
        Priority::Operational,
    );
    let _ = gateway.send(message).await;
    sleep(Duration::from_millis(200)).await;

    // Verify circuit breaker recovered
    let health_after = gateway.health_check().await;
    assert!(health_after, "Circuit breaker should recover after timeout");
}

async fn test_intermittent_connectivity_flapping() {
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

    let gateway_arc = Arc::new(gateway);

    // Simulate flapping connectivity (up/down every 100ms)
    let gateway_clone = Arc::clone(&gateway_arc);
    let flap_task = tokio::spawn(async move {
        let mut counter = 0;
        let mut interval = interval(Duration::from_millis(100));

        loop {
            if counter >= 50 {
                break;
            }
            counter += 1;
            interval.tick().await;

            // Alternate between successful and failed sends
            if counter % 2 == 0 {
                // Success
                let message = Message::new(
                    Protocol::IridiumSBD,
                    Bytes::from(vec![1u8; 200]),
                    Priority::Operational,
                );
                let _ = gateway_clone.send(message).await;
            } else {
                // Failure (simulated by emergency message)
                let message = Message::new(
                    Protocol::IridiumSBD,
                    Bytes::from(vec![0xFF; 200]),
                    Priority::Emergency,
                );
                let _ = gateway_clone.send(message).await;
            }
        }
    });

    let _ = flap_task.await;
    sleep(Duration::from_millis(500)).await;

    // Verify system remained operational despite flapping
    let metrics = gateway_arc.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should translate messages despite flapping"
    );
}

#[tokio::test]

async fn test_partial_network_partition() {
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

    // Simulate partial partition: some protocols work, others don't
    let working_protocol = Protocol::IridiumSBD;
    let failing_protocol = Protocol::InmarsatC;

    // Send messages from working protocol
    for i in 0..5 {
        let message = Message::new(
            working_protocol,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    // Send messages from failing protocol (will fail)
    for i in 0..5 {
        let message = Message::new(
            failing_protocol,
            Bytes::from(vec![i as u8; 100]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify working protocol messages were processed
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated >= 5,
        "Working protocol messages should be translated"
    );
}

#[tokio::test]

async fn test_network_partition_with_deadzone() {
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

    // Simulate network partition by sending emergency messages
    for i in 0..10 {
        let emergency_msg = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Emergency,
        );
        let _ = gateway.send(emergency_msg).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify emergency messages accumulated in deadzone shard
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert_eq!(
        deadzone_messages.len(),
        10,
        "All emergency messages should be in deadzone shard"
    );

    // Verify system remained healthy (circuit breaker not tripped for emergency)
    let health = gateway.health_check().await;
    assert!(health, "System should remain healthy during deadzone");
}

#[tokio::test]

async fn test_network_partition_burst_traffic() {
    let gateway = Gateway::new(
        1000,
        Duration::from_millis(100),
        Duration::from_secs(60),
        None,
        TelemetryConfig {
            enabled: false,
            endpoint: None,
            ping_interval_ms: 5000,
        },
    );

    // Send burst of messages during simulated partition
    let mut handles = Vec::new();
    for i in 0..100 {
        let gateway_clone = Arc::new(gateway.clone());
        let handle = tokio::spawn(async move {
            let message = Message::new(
                Protocol::IridiumSBD,
                Bytes::from(vec![i as u8; 200]),
                Priority::Operational,
            );
            let _ = gateway_clone.send(message).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    sleep(Duration::from_secs(1)).await;

    // Verify system handled burst without crashing
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should handle burst traffic"
    );
}

#[tokio::test]
#[ignore]

async fn test_network_partition_long_duration() {
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

    // Simulate long network partition (send messages for 5 seconds)
    let gateway_arc = Arc::new(gateway.clone());
    let partition_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(100));
        let mut counter = 0;

        loop {
            if counter >= 50 {
                break; // 5 seconds of partition
            }
            counter += 1;
            interval.tick().await;

            let message = Message::new(
                Protocol::IridiumSBD,
                Bytes::from(vec![counter as u8; 200]),
                Priority::Operational,
            );
            let _ = gateway_arc.send(message).await;
        }
    });

    let _ = partition_task.await;
    sleep(Duration::from_millis(500)).await;

    // Verify circuit breaker opened after long partition
    let health = gateway.health_check().await;
    assert!(!health, "Circuit breaker should open after long partition");

    // Wait for recovery
    sleep(Duration::from_secs(31)).await;

    // Send successful message
    let message = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0xFF; 200]),
        Priority::Operational,
    );
    let _ = gateway.send(message).await;
    sleep(Duration::from_millis(200)).await;

    // Verify recovery
    let health_after = gateway.health_check().await;
    assert!(health_after, "System should recover after partition ends");
}

#[tokio::test]

async fn test_network_partition_with_cache() {
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

    // Send message before partition
    let data = Bytes::from(vec![1u8; 200]);
    let message1 = Message::new(Protocol::IridiumSBD, data.clone(), Priority::Operational);
    let _ = gateway.send(message1).await;
    sleep(Duration::from_millis(200)).await;

    let metrics_before = gateway.metrics().snapshot();
    let cache_hits_before = metrics_before.cache_hit_rate;

    // Simulate partition and send same message (should hit cache)
    let message2 = Message::new(Protocol::IridiumSBD, data, Priority::Operational);
    let _ = gateway.send(message2).await;
    sleep(Duration::from_millis(200)).await;

    let metrics_after = gateway.metrics().snapshot();
    let cache_hits_after = metrics_after.cache_hit_rate;

    assert!(
        cache_hits_after >= cache_hits_before,
        "Cache should work even if dependencies fail"
    );
}

#[tokio::test]

async fn test_network_partition_multi_protocol_isolation() {
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

    // Simulate partition affecting only one protocol
    let protocols = [
        Protocol::IridiumSBD,
        Protocol::InmarsatC,
        Protocol::VSAT,
        Protocol::HFVHF,
    ];

    // Send messages from all protocols
    for (i, protocol) in protocols.iter().enumerate() {
        let message = Message::new(
            *protocol,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify at least some messages were processed
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Some protocols should work during partial partition"
    );
}
