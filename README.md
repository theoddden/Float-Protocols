# Float Protocols

**1.1MB binary** - Ultra-lightweight async protocol-translation bridge for dead zone communication systems.

## Overview

Float Protocols is a primitive that bridges existing dead zone communication systems (Iridium, Inmarsat, VSAT, HF/VHF, RockBLOCK) to AST SpaceMobile's future direct-to-cell network that is partly launched. Users will bring their own ASTS account details for authentication. The system integrates with telemetry for accurate ping monitoring.

**Design Principles:**
- Async-first architecture for low latency
- 99% uptime goal with circuit breakers, retries, and health checks
- Ultra-lightweight: runs on pre-existing RAM on local ARM
- Zero-allocation where possible using stack-allocated buffers
- Fixed-size buffers for memory efficiency
- Memory sharding for immediate deadzone uplink (InferX pattern)
- Snapshotting for fast uplink building
- Inspired by vLLM batching and LMCache caching patterns

## Supported Protocols

- **Iridium SBD** - Iridium Short Burst Data (340 bytes max)
- **Inmarsat C** - Inmarsat teletype format (128 bytes max)
- **VSAT** - VSAT IP packets with compression
- **HF/VHF** - HF/VHF radio with codec translation
- **RockBLOCK** - RockBLOCK IoT satellite communication
- **Samsara** - Samsara fleet management cellular broadband (1MB typical)

## Future Protocols
- **AST SpaceMobile** - Direct-to-cell cellular format

## Features

- **Zero-Allocation Hot Path**: Iridium SBD to ASTS Protobuf translation with NO heap allocations
- **Protocol Translation**: Async translation between legacy protocols and AST SpaceMobile
- **Bi-Temporal Logic**: Dual timestamps (t_event, t_system) for insurance underwriting and trade compliance
- **Spread Calculation**: Deterministic mark between event time and system time for compliance
- **Intelligent Batching**: vLLM-inspired message batching with emergency bypass
- **Distributed Caching**: LMCache-inspired caching with TTL and invalidation
- **Memory Sharding**: Pre-sharded memory for immediate uplink when deadzone is detected
- **Snapshotting**: Fast uplink building from pre-computed message batches
- **Reliability**: Circuit breakers, retry policies, and health checks for 99.9% uptime
- **Telemetry Integration**: Accurate ping monitoring and metrics
- **BYO Authentication**: Users bring their own ASTS account details
- **OTel-over-Satellite**: OpenTelemetry span collection and transmission via ASTS protobuf with compression

## OTel-over-Satellite

Float Protocols includes OpenTelemetry (OTel) support for distributed tracing over satellite networks:

### Modules

- **otel_compact_span**: Compact span representation with fixed-size buffers for satellite transmission
  - Bi-temporal timestamps (t_event, t_system) for compliance
  - Efficient attribute encoding (key-value pairs)
  - Status tracking (Ok, Error)

- **otel_bundle**: ASTS protobuf transport bundles with compression
  - Sequence numbering for ordering
  - Batch timestamps for time windows
  - Compression support (zstd, gzip) for bandwidth optimization
  - Automatic span encoding/decoding

- **otel_converter**: OTLP conversion for Mandala collector integration
  - Transforms compact spans to standard OTLP export requests
  - JSON serialization for HTTP transport
  - Resource attribute mapping

- **otel_reconciler**: Bi-temporal reconciliation for OTel spans
  - Ingest spans with t_event/t_system tracking
  - Retroactive corrections for late-arriving data
  - System belief queries at specific timestamps
  - Spread statistics for monitoring

### Usage

```rust
use float_protocols::otel_compact_span::CompactSpan;
use float_protocols::otel_bundle::{TelemetryBundle, CompressionType};

// Create a compact span
let span = CompactSpan::new([1u8; 16], [2u8; 8], "sensor.read", "sensor-001")
    .with_t_event(1000)
    .with_t_system(1500)
    .with_attribute("temperature", "25.5")
    .with_status(float_protocols::otel_compact_span::SpanStatus::Ok);

// Create a bundle and add spans
let mut bundle = TelemetryBundle::new(1);
bundle.add_span(span);

// Compress with zstd
bundle.with_compression(CompressionType::Zstd);
bundle.compress(3).unwrap();

// Encode for transmission
let encoded = bundle.encode();

// Decode on receiver
let decoded = TelemetryBundle::decode(&encoded).unwrap();
assert_eq!(decoded.span_count(), 1);
```

### Design Principles

- **Compact Binary Format**: Minimal overhead for satellite bandwidth constraints
- **Compression**: zstd and gzip support for bandwidth optimization
- **Bi-Temporal**: t_event/t_system for compliance with insurance and trade requirements
- **Zero-Allocation**: Stack-allocated buffers where possible
- **Async-First**: Tokio-based for low-latency processing

## Installation

```bash
cargo install float-protocols
```

## Usage

### Environment Variables

```bash
# AST SpaceMobile BYO Credentials (optional - for ASTS integration)
export ASTS_ACCOUNT_ID="your_account_id"
export ASTS_API_KEY="your_api_key"
export ASTS_MNO_PARTNER_ID="partner_id" # optional

# Telemetry Configuration
export TELEMETRY_ENABLED="true"
export TELEMETRY_ENDPOINT="https://your-telemetry-endpoint.com"
export TELEMETRY_PING_INTERVAL_MS="5000"

# Logging
export RUST_LOG="float_protocols=info,tokio=warn"
```

### Running the Gateway

```bash
cargo run --release
```

### Testing

```bash
# Run with test message
FLOAT_PROTOCOLS_TEST=1 cargo run --release
```

## Zero-Allocation Hot Path

Float Protocols implements a zero-allocation hot path for Iridium SBD to ASTS Protobuf translation:

- **No Heap Allocations**: The critical translation path uses only stack-allocated buffers
- **Zero-Copy Parsing**: Iridium SBD messages are parsed directly from input buffer
- **Stack-Allocated Buffers**: Fixed-size buffers on stack, no dynamic allocation
- **Zero-Copy Translation**: Payload is copied directly to output buffer without intermediate allocations

### Zero-Allocation API

```rust
use float_protocols::{IridiumSBDMessage, ZeroCopyTranslator};

// Parse Iridium SBD (zero-allocation)
let iridium_msg = IridiumSBDMessage::parse(&iridium_data).unwrap();

// Translate to ASTS Protobuf (zero-allocation)
let mut translator = ZeroCopyTranslator::new();
let mut output_buffer = [0u8; 2048];
let size = translator.translate(&iridium_msg, &mut output_buffer).unwrap();

// Output buffer now contains ASTS Protobuf data
```

### Synchronous Zero-Allocation API

For maximum performance in the critical hot path, use the synchronous API:

```rust
use float_protocols::translate_iridium_to_asts_sync;

let mut buffer = [0u8; 2048];
let size = translate_iridium_to_asts_sync(&iridium_data, &mut buffer)?;
// buffer[..size] now contains ASTS Protobuf data
```

### Zero-Allocation Trade-offs

The async architecture (Tokio) requires heap allocations for:
- Task spawning and scheduling
- Channel buffers
- Arc reference counting

However, the core protocol parsing (`IridiumSBDMessage::parse`, `ZeroCopyTranslator::translate`) is genuinely zero-allocation. Use the synchronous API when you need:
- Maximum performance in the hot path
- No async overhead
- Direct control over memory allocation

Use the async Gateway when you need:
- Bi-temporal storage
- Caching
- Reliability patterns (circuit breakers, retries)
- Telemetry integration

## Bi-Temporal Logic

Float Protocols implements bi-temporal modeling for high-end insurance underwriting and global trade compliance:

- **t_event (Valid Time)**: When the sensor actually recorded the event in the physical world
- **t_system (Transaction Time)**: When your system first learned about that event
- **Spread Calculation**: Deterministic mark between t_event and t_system for compliance

This enables critical queries:
- "What did we believe the state of the fleet was at 3 PM yesterday?" (transaction time query)
- "What actually happened at 3 PM yesterday?" (valid time query)

### Bi-Temporal Queries

```rust
// Query by valid time (what actually happened)
let actual_events = gateway.query_valid_time(start_ms, end_ms).await;

// Query by transaction time (what system believed)
let system_beliefs = gateway.query_transaction_time(start_ms, end_ms).await;

// Get spread statistics for insurance underwriting
let spread_stats = gateway.spread_stats(start_ms, end_ms).await;
println!("Average delay: {} seconds", spread_stats.avg_spread_seconds());

// Get system belief at specific timestamp
let belief = gateway.system_belief_at(timestamp_ms).await;

// Get actual state at specific timestamp
let actual = gateway.actual_state_at(timestamp_ms).await;
```

### Spread Calculation

The spread between t_event and t_system is calculated as:
```
spread_ms = t_system - t_event
```

- Positive spread: Message was delayed (system learned about it after it happened)
- Negative spread: Message from the future (system learned about it before it happened)
- Zero spread: Real-time processing

This deterministic mark is critical for:
- Insurance underwriting (proving when events actually occurred)
- Trade compliance (demonstrating timely reporting)
- Audit trails (reconstructing historical states)

## Memory Sharding

Float Protocols uses memory sharding (InferX pattern) to provide immediate uplink when a deadzone is detected:

- **Dedicated Deadzone Shard**: Pre-allocated buffer for emergency messages
- **Load Balancing**: Regular shards distribute load across available memory
- **Zero Allocation**: Pre-allocated buffers eliminate allocation latency during critical transitions
- **Immediate Uplink**: When deadzone detected, messages route to dedicated shard without blocking

## Snapshotting

Snapshotting enables fast uplink building by creating pre-computed message batches:

- **Instant Uplink**: Retrieve snapshots without reprocessing
- **Protocol-Specific**: Separate snapshots per protocol type
- **TTL-Based**: Expired snapshots automatically evicted
- **Memory Efficient**: Fixed-size snapshot pool with LRU eviction

## Reliability

Float Protocols is designed for 99.9% uptime:

- **Circuit Breakers**: Prevent cascading failures
- **Retry Policies**: Exponential backoff for transient failures
- **Health Checks**: Continuous monitoring of system health
- **Graceful Degradation**: Non-critical features disabled under stress

## Performance

- **Binary Size**: <1.5MB optimized with LTO
- **Memory Footprint**: <50MB with default configuration
- **Latency**: <2ms for emergency messages
- **Throughput**: 10,000+ messages/second
- **Cache Hit Rate**: >80% for repeated translations

## Development

### Building

```bash
cargo build --release
```

### Testing

```bash
cargo test
```

### Clippy

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

