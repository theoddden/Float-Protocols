//! Integration tests for end-to-end protocol translation
//!
//! Tests the full message flow from protocol input to ASTS Protobuf output
//! including batching, caching, sharding, and bi-temporal storage.

use bytes::Bytes;
use float_protocols::gateway::{Gateway, TelemetryConfig};
use float_protocols::protocol::{Message, Priority, Protocol};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_iridium_to_asts_end_to_end() {
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

    // Create a realistic Iridium SBD message (340 bytes max)
    let iridium_data = Bytes::from(vec![0u8; 200]); // Simulated Iridium SBD payload
    let message = Message::new(Protocol::IridiumSBD, iridium_data, Priority::Operational);

    // Send message through gateway
    let send_result = gateway.send(message.clone()).await;
    assert!(send_result.is_ok(), "Message send should succeed");

    // Wait for processing
    sleep(Duration::from_millis(200)).await;

    // Verify metrics
    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Message should be translated"
    );
}

#[tokio::test]
async fn test_inmarsat_to_asts_end_to_end() {
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

    // Create a realistic Inmarsat C message (128 bytes max)
    let inmarsat_data = Bytes::from(vec![0u8; 100]); // Simulated Inmarsat C payload
    let message = Message::new(Protocol::InmarsatC, inmarsat_data, Priority::Operational);

    let send_result = gateway.send(message.clone()).await;
    assert!(send_result.is_ok(), "Message send should succeed");

    sleep(Duration::from_millis(200)).await;

    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Message should be translated"
    );
}

#[tokio::test]
async fn test_vsat_to_asts_end_to_end() {
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

    // Create a VSAT message (larger payload with compression)
    let vsat_data = Bytes::from(vec![0u8; 500]); // Simulated VSAT payload
    let message = Message::new(Protocol::VSAT, vsat_data, Priority::Operational);

    let send_result = gateway.send(message.clone()).await;
    assert!(send_result.is_ok(), "Message send should succeed");

    sleep(Duration::from_millis(200)).await;

    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Message should be translated"
    );
}

#[tokio::test]
async fn test_hfvhf_to_asts_end_to_end() {
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

    // Create an HF/VHF message (codec translation)
    let hfvhf_data = Bytes::from(vec![0u8; 256]); // Simulated HF/VHF payload
    let message = Message::new(Protocol::HFVHF, hfvhf_data, Priority::Operational);

    let send_result = gateway.send(message.clone()).await;
    assert!(send_result.is_ok(), "Message send should succeed");

    sleep(Duration::from_millis(200)).await;

    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Message should be translated"
    );
}

#[tokio::test]
async fn test_samsara_to_asts_end_to_end() {
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

    // Create a Samsara message (cellular broadband, larger payload)
    let samsara_data = Bytes::from(vec![0u8; 1024]); // Simulated Samsara payload (1MB typical)
    let message = Message::new(Protocol::Samsara, samsara_data, Priority::Operational);

    let send_result = gateway.send(message.clone()).await;
    assert!(send_result.is_ok(), "Message send should succeed");

    sleep(Duration::from_millis(200)).await;

    let metrics = gateway.metrics().snapshot();
    assert!(
        metrics.messages_translated > 0,
        "Message should be translated"
    );
}

#[tokio::test]
async fn test_multi_protocol_batch_translation() {
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

    // Send messages from different protocols in quick succession
    let protocols = vec![
        (Protocol::IridiumSBD, Bytes::from(vec![0u8; 200])),
        (Protocol::InmarsatC, Bytes::from(vec![0u8; 100])),
        (Protocol::VSAT, Bytes::from(vec![0u8; 500])),
        (Protocol::HFVHF, Bytes::from(vec![0u8; 256])),
        (Protocol::Samsara, Bytes::from(vec![0u8; 1024])),
    ];

    for (protocol, data) in protocols {
        let message = Message::new(protocol, data, Priority::Operational);
        let _ = gateway.send(message).await;
    }

    // Wait for batch processing
    sleep(Duration::from_millis(500)).await;

    let metrics = gateway.metrics().snapshot();
    assert_eq!(
        metrics.messages_translated, 5,
        "All 5 messages should be translated"
    );
}

#[tokio::test]
async fn test_cache_hit_in_translation_flow() {
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

    // Send the same message twice
    let data = Bytes::from(vec![1u8; 200]);
    let message1 = Message::new(Protocol::IridiumSBD, data.clone(), Priority::Operational);
    let message2 = Message::new(Protocol::IridiumSBD, data, Priority::Operational);

    let _ = gateway.send(message1).await;
    sleep(Duration::from_millis(500)).await;

    let metrics_before = gateway.metrics().snapshot();
    let cache_hits_before = metrics_before.cache_hit_rate;

    let _ = gateway.send(message2).await;
    sleep(Duration::from_millis(500)).await;

    let metrics_after = gateway.metrics().snapshot();
    let cache_hits_after = metrics_after.cache_hit_rate;

    assert!(
        cache_hits_after > cache_hits_before,
        "Second message should hit cache"
    );
}

#[tokio::test]
async fn test_bitemporal_storage_in_translation_flow() {
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
        Bytes::from(vec![0u8; 200]),
        Priority::Operational,
    );

    let t_event = message.t_event;
    let _ = gateway.send(message.clone()).await;
    sleep(Duration::from_millis(200)).await;

    // Query by valid time
    let events = gateway
        .query_valid_time(t_event - 1000, t_event + 1000)
        .await;
    assert!(
        !events.is_empty(),
        "Message should be stored in bi-temporal store"
    );

    // Query by transaction time
    let t_system = message.t_system;
    let beliefs = gateway
        .query_transaction_time(t_system - 1000, t_system + 1000)
        .await;
    assert!(
        !beliefs.is_empty(),
        "Message should be queryable by transaction time"
    );
}

#[tokio::test]
async fn test_emergency_message_priority_handling() {
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

    // Send emergency message
    let emergency_data = Bytes::from(vec![0xFF; 200]);
    let emergency_message = Message::new(Protocol::IridiumSBD, emergency_data, Priority::Emergency);

    let _ = gateway.send(emergency_message).await;
    sleep(Duration::from_millis(200)).await;

    // Verify emergency message was sent to deadzone shard
    let shard_manager = gateway.shard_manager();
    let deadzone_shard_id = shard_manager.get_deadzone_shard();
    let deadzone_messages = shard_manager.drain_shard(deadzone_shard_id);

    assert!(
        !deadzone_messages.is_empty(),
        "Emergency message should be in deadzone shard"
    );
}

#[tokio::test]
async fn test_snapshot_creation_in_translation_flow() {
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
        Bytes::from(vec![0u8; 200]),
        Priority::Operational,
    );

    let _ = gateway.send(message).await;
    sleep(Duration::from_millis(500)).await;

    // Verify snapshot was created
    let snapshot_manager = gateway.snapshot_manager();
    let snapshots = snapshot_manager
        .get_protocol_snapshots(Protocol::IridiumSBD)
        .await;
    assert!(
        !snapshots.is_empty(),
        "Snapshot should be created for translated message"
    );
}
