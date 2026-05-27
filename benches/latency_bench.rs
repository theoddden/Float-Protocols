//! Latency benchmarks for Float Protocols
//!
//! Measures the critical latency paths:
//! - parse-to-queue time
//! - queue-to-translate time
//! - cache-hit translation time
//! - emergency path latency
//! - reconnect-burst drain time

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use float_protocols::gateway::Gateway;
use float_protocols::protocol::{Message, Priority, Protocol};
use float_protocols::translator::Translator;
use std::time::Duration;
use tokio::runtime::Runtime;

fn create_iridium_sbd_message() -> Message {
    // Valid Iridium SBD message: [protocol (1)][length (2)][payload (N)][checksum (2)]
    let mut data = vec![0x01]; // protocol
    let payload = b"test_payload_data";
    data.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    data.extend_from_slice(payload);
    // Compute CRC-16-CCITT for header + payload
    let header_data = [
        0x01,
        (payload.len() as u16 >> 8) as u8,
        (payload.len() as u16 & 0xFF) as u8,
    ];
    let crc = compute_crc16_ccitt(&header_data, payload);
    data.extend_from_slice(&crc.to_be_bytes());

    Message::new(Protocol::IridiumSBD, data.into(), Priority::Operational)
}

fn compute_crc16_ccitt(header: &[u8], payload: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for &byte in header.iter().chain(payload.iter()) {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Benchmark: parse-to-queue time
/// Measures the time from receiving raw bytes to pushing into the batcher queue
fn bench_parse_to_queue(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let gateway = rt.block_on(async {
        Gateway::new(
            1000,
            Duration::from_millis(10),
            Duration::from_secs(300),
            None,
            float_protocols::gateway::TelemetryConfig {
                enabled: false,
                endpoint: None,
                ping_interval_ms: 60000,
            },
        )
    });

    let mut group = c.benchmark_group("parse_to_queue");

    for size in [16, 64, 256, 340].iter() {
        let mut data = vec![0x01u8];
        let payload = vec![0u8; *size];
        data.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        data.extend_from_slice(&payload);
        let header_data = [
            0x01,
            (payload.len() as u16 >> 8) as u8,
            (payload.len() as u16 & 0xFF) as u8,
        ];
        let crc = compute_crc16_ccitt(&header_data, &payload);
        data.extend_from_slice(&crc.to_be_bytes());

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let msg = Message::new(
                    Protocol::IridiumSBD,
                    data.clone().into(),
                    Priority::Operational,
                );
                rt.block_on(async {
                    let _ = gateway.send(msg).await;
                });
            });
        });
    }
    group.finish();
}

/// Benchmark: queue-to-translate time
/// Measures the time from batcher flush to translation completion
fn bench_queue_to_translate(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    let translator = Translator::new(100);

    let mut group = c.benchmark_group("queue_to_translate");

    for size in [16, 64, 256, 340].iter() {
        let mut data = vec![0x01u8];
        let payload = vec![0u8; *size];
        data.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        data.extend_from_slice(&payload);
        let header_data = [
            0x01,
            (payload.len() as u16 >> 8) as u8,
            (payload.len() as u16 & 0xFF) as u8,
        ];
        let crc = compute_crc16_ccitt(&header_data, &payload);
        data.extend_from_slice(&crc.to_be_bytes());

        let msg = Message::new(Protocol::IridiumSBD, data.into(), Priority::Operational);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let msg_clone = msg.clone();
                black_box(translator.translate_sync(msg_clone).unwrap())
            });
        });
    }
    group.finish();
}

/// Benchmark: cache-hit translation time
/// Measures the time when the translated message is already cached
fn bench_cache_hit_translation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let gateway = rt.block_on(async {
        Gateway::new(
            1000,
            Duration::from_millis(10),
            Duration::from_secs(300),
            None,
            float_protocols::gateway::TelemetryConfig {
                enabled: false,
                endpoint: None,
                ping_interval_ms: 60000,
            },
        )
    });

    let msg = create_iridium_sbd_message();

    // Warm the cache
    rt.block_on(async {
        let _ = gateway.send(msg.clone()).await;
        // Wait for batcher to flush and process
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    c.bench_function("cache_hit_translation", |b| {
        b.iter(|| {
            let msg_clone = msg.clone();
            rt.block_on(async {
                let _ = gateway.send(msg_clone).await;
            });
        });
    });
}

/// Benchmark: emergency path latency
/// Measures the time from receiving an emergency message to ASTS uplink
fn bench_emergency_path(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let gateway = rt.block_on(async {
        Gateway::new(
            1000,
            Duration::from_millis(10),
            Duration::from_secs(300),
            None,
            float_protocols::gateway::TelemetryConfig {
                enabled: false,
                endpoint: None,
                ping_interval_ms: 60000,
            },
        )
    });

    let mut group = c.benchmark_group("emergency_path");

    for size in [16, 64, 256, 340].iter() {
        let mut data = vec![0x01u8];
        let payload = vec![0u8; *size];
        data.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        data.extend_from_slice(&payload);
        let header_data = [
            0x01,
            (payload.len() as u16 >> 8) as u8,
            (payload.len() as u16 & 0xFF) as u8,
        ];
        let crc = compute_crc16_ccitt(&header_data, &payload);
        data.extend_from_slice(&crc.to_be_bytes());

        let msg = Message::new(Protocol::IridiumSBD, data.into(), Priority::Emergency);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let msg_clone = msg.clone();
                rt.block_on(async {
                    let _ = gateway.send(msg_clone).await;
                });
            });
        });
    }
    group.finish();
}

/// Benchmark: reconnect-burst drain time
/// Measures the time to drain a spread shard with multiple messages
fn bench_reconnect_burst_drain(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let gateway = rt.block_on(async {
        Gateway::new(
            1000,
            Duration::from_millis(10),
            Duration::from_secs(300),
            None,
            float_protocols::gateway::TelemetryConfig {
                enabled: false,
                endpoint: None,
                ping_interval_ms: 60000,
            },
        )
    });

    let mut group = c.benchmark_group("reconnect_burst_drain");

    for count in [10, 50, 100, 500].iter() {
        let messages: Vec<Message> = (0..*count).map(|_| create_iridium_sbd_message()).collect();

        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, _| {
            b.iter(|| {
                let gateway_clone = gateway.clone();
                let msgs_clone = messages.clone();
                rt.block_on(async {
                    // Push all messages to spread shard
                    for msg in msgs_clone {
                        let _ = gateway_clone.send(msg).await;
                    }
                    // Wait for drain
                    tokio::time::sleep(Duration::from_millis(100)).await;
                });
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_parse_to_queue,
    bench_queue_to_translate,
    bench_cache_hit_translation,
    bench_emergency_path,
    bench_reconnect_burst_drain
);
criterion_main!(benches);
