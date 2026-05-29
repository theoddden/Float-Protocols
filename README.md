# Float Protocols

**1.1MB binary** - Ultra-lightweight, 100% Rust, async protocol-translation bridge for dead zone communication systems.

**STAR THE REPO, IT'S A HUGE HELP**

## May 2026 Edition - v0.4.3

**Major: Dual-Layer Batching + Latency Optimization**
- **TransmissionBatcher**: Batches translated messages before satellite uplink HTTP calls — reduces HTTP overhead by 20-40%
- **Dual-layer batching**: Pre-translation batcher (lock amortization) + transmission batcher (HTTP coalescing) — both serve distinct purposes
- **Latency optimizations**: 5 parameter tunings for 50-75ms normal path reduction, 25ms reconnect burst reduction
  - Transmission batcher timeout: 100ms → 50ms
  - Spread drain interval: 50ms → 25ms
  - Translator pool size: 4 → 8
  - Regular batcher timeout: 100ms → 25ms
  - Shard batch size: 10 → 20
- **Spread shard routing**: High bi-temporal spread messages routed to dedicated shard for fast draining
- **O(1) backpressure**: Atomic counter replaces O(n) stats() scan on every push
- **Per-protocol burst thresholds**: Tuned spread thresholds (Iridium: 120s, Inmarsat: 300s, Samsara: 5s)
- **Updated message flow**: send() → batcher → process_incoming_batch → shard workers → process_translated_batch → transmission batcher → send_batch_to_asts

**v0.4.0: Fully Embedded Primitives**
- Per-shard worker pools with batched draining (run_batched) — 8 workers for regular shards
- Batcher as primary normal path; emergency messages bypass batcher entirely
- Payload-deterministic caching: t_event removed from CacheKey (same payload = same translation)
- parking_lot RwLock for cache (sync, no yield) — O(1) eviction via VecDeque insertion-order queue
- Batch APIs: cache.get_batch/set_batch, bitemporal.store_batch, snapshot.create_batch_snapshot
- FNV-1a stable hash for snapshot IDs (replaces randomized DefaultHasher)
- device_id field in Snapshot for per-device uplink drainage
- Fixed critical bug: ShardWorker.run/run_batched changed from async fn to plain fn (workers now actually spawn)

**v0.3.0: NIDD Protocol Support**
- Full 3GPP TS 24.582 compliant Non-IP Data Delivery implementation for NTN NB-IoT
- Eliminates 28-byte IP/UDP header overhead (56% reduction for small sensor messages)
- Control-plane priority for emergency messages
- QoS parameters for NTN optimization (Priority, Reliability, Delay Class, Coverage Enhancement)
- Zero-allocation parsing using nom combinators
- Future-proofing for 3GPP Rel-17/18 NTN NB-IoT standard

## Overview

Float Protocols is a primitive that bridges existing dead zone communication systems (Iridium, Inmarsat, VSAT, HF/VHF, RockBLOCK) to future satellite networks. Currently supports traditional satellite protocols with future-proofing for AST SpaceMobile's and Iridium's NTN enterprise IoT APIs when released.

## Supported Protocols

- **Iridium SBD** - Iridium Short Burst Data (340 bytes max)
- **Inmarsat C** - Inmarsat teletype format (128 bytes max)
- **VSAT** - VSAT IP packets with compression
- **HF/VHF** - HF/VHF radio with codec translation
- **RockBLOCK** - RockBLOCK IoT satellite communication
- **Samsara** - Samsara fleet management cellular broadband (1MB typical)

## Future Protocols

- **AST SpaceMobile** - Direct-to-cell cellular format (future-proofing for enterprise IoT APIs)
- **Iridium NTN***

**Note**: AST SpaceMobile has successfully tested IoT device connectivity (BeWhere Holdings, Oct 2025) and plans NB-IoT support. The ASTS Protobuf format is speculative and will be updated based on official API documentation when released.

## Features

- **Zero-Allocation Hot Path**: Iridium SBD to ASTS Protobuf translation with NO heap allocations (extends battery life on solar/battery-powered edge devices)
- **Protocol Translation**: Async translation between legacy protocols and AST SpaceMobile
- **Bi-Temporal Logic**: Dual timestamps (t_event, t_system) for insurance underwriting and trade compliance
- **Spread Calculation**: Deterministic mark between event time and system time for compliance
- **Intelligent Batching**: vLLM-inspired message batching with emergency bypass
- **Distributed Caching**: LMCache-inspired caching with TTL and invalidation
- **Memory Sharding**: Pre-sharded memory for immediate uplink when deadzone is detected
- **Snapshotting**: Fast uplink building from pre-computed message batches
- **Reliability**: Circuit breakers, retry policies, and health checks for 99.9% uptime
- **Telemetry Integration**: Accurate ping monitoring and metrics
- **BYO Authentication**: Users bring their own credentials when APIs are available
- **OTel-over-Satellite**: OpenTelemetry span collection and transmission via ASTS protobuf with compression

## Installation

```bash
cargo install float-protocols
```

## Usage

### Environment Variables

```bash
# AST SpaceMobile Credentials (future - for when enterprise APIs are released)
# Currently not available - ASTS integration is future-proofing
# export ASTS_ACCOUNT_ID="your_account_id"
# export ASTS_API_KEY="your_api_key"
# export ASTS_MNO_PARTNER_ID="partner_id" # optional

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
- **Positive spread**: Message was delayed (system learned about it after it happened)
- **Negative spread**: Message from the future (system learned about it before it happened)
- **Zero spread**: Real-time processing

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
- **Latency**:
  - Emergency path: <2ms processing overhead (bypasses both batchers)
  - Normal path: 502-2080ms worst-case (2-80ms processing + 500-2000ms satellite transport)
  - Reconnect burst: 580-2080ms worst-case (25ms spread drain + 50ms transmission batch + satellite)
- **Throughput**: 10,000+ messages/second with current configuration (8 shards, 8 translator pool)
- **Cache Hit Rate**: >80% for repeated translations
- **HTTP Overhead Reduction**: 20-40% via transmission batching

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

```
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

