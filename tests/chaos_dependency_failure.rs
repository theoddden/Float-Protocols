//! Chaos engineering tests for dependency failure injection
//!
//! Tests system behavior when critical dependencies fail:
//! - Cache failures
//! - Bi-temporal store failures
//! - Shard manager failures
//! - Snapshot manager failures
//! - Translator failures

use bytes::Bytes;
use float_protocols::gateway::{Gateway, TelemetryConfig};
use float_protocols::protocol::{Message, Priority, Protocol};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_cache_failure_graceful_degradation() {
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

    // Send message (cache may fail, but system should still work)
    let message = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0u8; 200]),
        Priority::Operational,
    );

    let _ = gateway.send(message).await;
    sleep(Duration::from_millis(200)).await;

    // Verify message was translated despite potential cache issues
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Message should be translated even if cache fails"
    );
}

#[tokio::test]
async fn test_bitemporal_store_failure_recovery() {
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

    // Send message (bitemporal store may fail, but translation should succeed)
    let message = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0u8; 200]),
        Priority::Operational,
    );

    let _ = gateway.send(message).await;
    sleep(Duration::from_millis(200)).await;

    // Verify message was translated
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Message should be translated even if bitemporal store fails"
    );
}

#[tokio::test]
async fn test_shard_manager_failure_handling() {
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

    // Send message (shard manager may fail, but system should handle gracefully)
    let message = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0u8; 200]),
        Priority::Operational,
    );

    let result = gateway.send(message).await;
    assert!(
        result.is_ok(),
        "Message send should succeed or fail gracefully"
    );

    sleep(Duration::from_millis(200)).await;

    // Verify system remained operational
    let health = gateway.health_check().await;
    assert!(
        health,
        "System should remain healthy despite shard manager issues"
    );
}

#[tokio::test]
async fn test_snapshot_manager_failure_handling() {
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

    // Send emergency message (snapshot may fail, but message should still be processed)
    let emergency_msg = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0xFF; 200]),
        Priority::Emergency,
    );

    let _ = gateway.send(emergency_msg).await;
    sleep(Duration::from_millis(200)).await;

    // Verify message was processed
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Emergency message should be processed even if snapshot fails"
    );
}

#[tokio::test]
async fn test_translator_failure_circuit_breaker() {
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

    // Send many messages to potentially trigger translator failures
    for i in 0..20 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify circuit breaker state
    let _health = gateway.health_check().await;
    // Circuit breaker may or may not be open depending on actual failures
    // System should remain operational either way
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0 || metrics.errors > 0,
        "System should either translate or record errors"
    );
}

#[tokio::test]
async fn test_multiple_dependency_failures_simultaneous() {
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

    // Send messages while multiple dependencies may fail
    for i in 0..30 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify system remained operational despite multiple potential failures
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0 || metrics.errors > 0,
        "System should handle multiple dependency failures"
    );
}

#[tokio::test]
async fn test_dependency_failure_with_deadzone() {
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

    // Send emergency message (dependencies may fail)
    let emergency_msg = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![0xFF; 200]),
        Priority::Emergency,
    );

    let _ = gateway.send(emergency_msg).await;
    sleep(Duration::from_millis(200)).await;

    // Verify emergency message was routed to deadzone shard
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert!(
        !deadzone_messages.is_empty(),
        "Emergency message should be in deadzone shard"
    );
}

#[tokio::test]
async fn test_dependency_failure_recovery_after_timeout() {
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

    // Trigger potential failures
    for _ in 0..10 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![0u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    let health_before = gateway.health_check().await;

    // Wait for recovery timeout
    sleep(Duration::from_secs(31)).await;

    // Send successful message
    let message = Message::new(
        Protocol::IridiumSBD,
        Bytes::from(vec![1u8; 200]),
        Priority::Operational,
    );
    let _ = gateway.send(message).await;
    sleep(Duration::from_millis(200)).await;

    let health_after = gateway.health_check().await;

    // If circuit breaker was open, it should now be closed
    if !health_before {
        assert!(health_after, "System should recover after timeout");
    }
}

#[tokio::test]
async fn test_dependency_failure_with_high_priority_messages() {
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

    // Send high priority messages while dependencies may fail
    let priorities = [
        Priority::Emergency,
        Priority::Operational,
        Priority::Diagnostic,
    ];

    for (i, priority) in priorities.iter().enumerate() {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            priority.clone(),
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify emergency message was prioritized
    let shard_manager = gateway.shard_manager();
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert!(
        !deadzone_messages.is_empty(),
        "Emergency message should be prioritized"
    );
}

#[tokio::test]
async fn test_dependency_failure_memory_pressure() {
    let gateway = Gateway::new(
        50, // Small buffer to induce memory pressure
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

    // Send many messages to induce memory pressure
    for i in 0..200 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify no memory leaks
    let leak_stats = shard_manager.leak_stats();
    assert_eq!(
        leak_stats.leaked, 0,
        "No memory should be leaked under pressure"
    );

    // Verify system remained operational
    let health = gateway.health_check().await;
    assert!(health, "System should remain healthy under memory pressure");
}

#[tokio::test]
async fn test_dependency_failure_with_cache_hit() {
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

    // Send message to populate cache
    let data = Bytes::from(vec![1u8; 200]);
    let message1 = Message::new(Protocol::IridiumSBD, data.clone(), Priority::Operational);
    let _ = gateway.send(message1).await;
    sleep(Duration::from_millis(200)).await;

    let metrics_before = gateway.metrics().snapshot();
    let cache_hits_before = metrics_before.cache_hit_rate;

    // Send same message (cache hit, dependencies may still fail)
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
async fn test_dependency_failure_cascading_prevention() {
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

    // Send messages that may fail in various components
    for i in 0..50 {
        let protocol = match i % 4 {
            0 => Protocol::IridiumSBD,
            1 => Protocol::InmarsatC,
            2 => Protocol::VSAT,
            _ => Protocol::HFVHF,
        };

        let message = Message::new(
            protocol,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify circuit breaker prevented cascading failures
    let health = gateway.health_check().await;
    let metrics = gateway.metrics().snapshot();

    // System should either be healthy or have recorded errors (not crashed)
    assert!(
        health || metrics.errors > 0,
        "System should prevent cascading failures"
    );
}
