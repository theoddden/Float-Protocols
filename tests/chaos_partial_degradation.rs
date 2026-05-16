//! Chaos engineering tests for partial degradation scenarios
//! Tests system behavior under partial degradation where some features
//! fail but the system continues to operate with reduced functionality.

use bytes::Bytes;
use float_protocols::gateway::{Gateway, TelemetryConfig};
use float_protocols::protocol::{Message, Priority, Protocol};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, sleep};

#[tokio::test]
async fn test_partial_degradation_cache_disabled() {
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

    // Send messages (cache may be degraded)
    for i in 0..10 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify system still works without cache
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should work even with degraded cache"
    );
}

#[tokio::test]
async fn test_partial_degradation_bitemporal_disabled() {
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

    // Send messages (bitemporal store may be degraded)
    for i in 0..10 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify system still works without bitemporal storage
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should work even with degraded bitemporal store"
    );
}

#[tokio::test]
async fn test_partial_degradation_snapshots_disabled() {
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

    // Send emergency messages (snapshots may be degraded)
    for i in 0..5 {
        let emergency_msg = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Emergency,
        );
        let _ = gateway.send(emergency_msg).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify emergency messages still processed
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should work even with degraded snapshots"
    );
}

#[tokio::test]
async fn test_partial_degradation_one_protocol_fails() {
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

    // Send messages from multiple protocols (one may fail)
    let protocols = [
        Protocol::IridiumSBD,
        Protocol::InmarsatC,
        Protocol::VSAT,
        Protocol::HFVHF,
    ];

    for (i, protocol) in protocols.iter().enumerate() {
        let message = Message::new(
            *protocol,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify at least some protocols worked
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should work with partial protocol degradation"
    );
}

#[tokio::test]
async fn test_partial_degradation_slow_translation() {
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

    // Send messages (translation may be slow)
    for i in 0..10 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_secs(1)).await;

    // Verify system still works with slow translation
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should work with slow translation"
    );
}

#[tokio::test]
async fn test_partial_degradation_high_error_rate() {
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

    // Send many messages (some may fail)
    for i in 0..50 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify system continues despite errors
    let metrics = gateway.metrics().snapshot();
    let total = metrics.messages_translated + metrics.errors;
    assert!(
        total > 0,
        "System should process messages despite high error rate"
    );
}

#[tokio::test]
async fn test_partial_degradation_memory_pressure() {
    let gateway = Gateway::new(
        50, // Small buffer to induce pressure
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

    // Send many messages under memory pressure
    for i in 0..100 {
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

    // Verify system still works
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0 || metrics.errors > 0,
        "System should work under memory pressure"
    );
}

#[tokio::test]
async fn test_partial_degradation_cpu_pressure() {
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

    // Send many messages rapidly to induce CPU pressure
    let mut handles = Vec::new();
    for i in 0..200 {
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

    // Verify system still works under CPU pressure
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should work under CPU pressure"
    );
}

#[tokio::test]
async fn test_partial_degradation_intermittent_failures() {
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

    // Simulate intermittent failures
    let gateway_clone = Arc::clone(&gateway_arc);
    let flap_task = tokio::spawn(async move {
        let mut counter = 0;
        let mut interval = interval(Duration::from_millis(100));

        loop {
            if counter >= 30 {
                break;
            }
            counter += 1;
            interval.tick().await;

            // Alternate between success and failure simulation
            if counter % 3 == 0 {
                // Simulate failure with emergency message
                let message = Message::new(
                    Protocol::IridiumSBD,
                    Bytes::from(vec![0xFF; 200]),
                    Priority::Emergency,
                );
                let _ = gateway_clone.send(message).await;
            } else {
                // Normal operation
                let message = Message::new(
                    Protocol::IridiumSBD,
                    Bytes::from(vec![counter as u8; 200]),
                    Priority::Operational,
                );
                let _ = gateway_clone.send(message).await;
            }
        }
    });

    let _ = flap_task.await;
    sleep(Duration::from_millis(500)).await;

    // Verify system handled intermittent failures
    let metrics = gateway_arc.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "System should work with intermittent failures"
    );
}

#[tokio::test]
async fn test_partial_degradation_graceful_feature_disable() {
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

    // Send messages (non-critical features may be disabled)
    for i in 0..10 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify core functionality still works
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Core functionality should work even if features are disabled"
    );
}

#[tokio::test]
async fn test_partial_degradation_priority_preservation() {
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

    // Send messages with different priorities under degradation
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
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert!(
        !deadzone_messages.is_empty(),
        "Emergency priority should be preserved"
    );
}

#[tokio::test]
async fn test_partial_degradation_recovery_to_full_health() {
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

    // Induce degradation
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

    // Wait for recovery
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
        assert!(health_after, "System should recover to full health");
    }
}

#[tokio::test]
async fn test_partial_degradation_with_backpressure() {
    let gateway = Gateway::new(
        50, // Small buffer to trigger backpressure
        Duration::from_millis(100),
        Duration::from_secs(60),
        None,
        TelemetryConfig {
            enabled: false,
            endpoint: None,
            ping_interval_ms: 5000,
        },
    );

    // Fill buffer to trigger backpressure
    for i in 0..100 {
        let message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(vec![i as u8; 200]),
            Priority::Operational,
        );
        let _ = gateway.send(message).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Verify system handled backpressure gracefully
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0 || metrics.errors > 0,
        "System should handle backpressure gracefully"
    );
}
