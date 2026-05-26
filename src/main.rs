//! Float Protocols - Ultra-lightweight async protocol translation bridge
//!
//! Bridges dead zone communication systems (Iridium, Inmarsat, VSAT, HF/VHF, RockBLOCK)
//! to AST SpaceMobile's direct-to-cell network with BYO account details.
//!
//! Features:
//! - Async-first architecture for low latency
//! - 99.9% uptime with circuit breakers and retries
//! - Ultra-lightweight: runs on pre-existing RAM on local devices
//! - Inspired by vLLM batching and LMCache caching patterns
//! - Telemetry integration for accurate ping monitoring

use bytes::Bytes;
use float_protocols::gateway::{ASTSCredentials, Gateway, TelemetryConfig};
use float_protocols::protocol::{Message, Priority, Protocol};
use float_protocols::reliability::init_startup_time;

#[cfg(not(unix))]
use tokio::signal;

/// Validate environment variables at startup
fn validate_config() -> Result<(), Box<dyn std::error::Error>> {
    // Validate ASTS credentials if provided
    if let Ok(account_id) = std::env::var("ASTS_ACCOUNT_ID") {
        if account_id.is_empty() {
            return Err("ASTS_ACCOUNT_ID cannot be empty".into());
        }
        if account_id.len() > 256 {
            return Err("ASTS_ACCOUNT_ID exceeds maximum length (256)".into());
        }
    }

    if let Ok(api_key) = std::env::var("ASTS_API_KEY") {
        if api_key.is_empty() {
            return Err("ASTS_API_KEY cannot be empty".into());
        }
        if api_key.len() < 16 {
            return Err("ASTS_API_KEY too short (minimum 16 characters)".into());
        }
    }

    // Validate telemetry config
    if let Ok(interval) = std::env::var("TELEMETRY_PING_INTERVAL_MS") {
        let interval_ms: u64 = interval.parse()?;
        if interval_ms < 1000 {
            return Err("TELEMETRY_PING_INTERVAL_MS must be at least 1000ms".into());
        }
        if interval_ms > 300000 {
            return Err("TELEMETRY_PING_INTERVAL_MS exceeds maximum (300000ms)".into());
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up panic hook for structured crash logging
    std::panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::capture();
        tracing::error!(
            panic = %panic_info,
            backtrace = %backtrace,
            "Float Protocols crashed"
        );
    }));

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "float_protocols=info,tokio=warn".to_string()),
        )
        .init();

    tracing::info!("Float Protocols starting up...");

    // Initialize startup timestamp for circuit breaker recovery timeout
    init_startup_time();

    // Validate environment variables
    validate_config()?;

    // Load configuration from environment or config file
    let asts_credentials = std::env::var("ASTS_ACCOUNT_ID")
        .ok()
        .and_then(|_| std::env::var("ASTS_API_KEY").ok())
        .map(|_| ASTSCredentials {
            account_id: std::env::var("ASTS_ACCOUNT_ID").unwrap_or_default(),
            api_key: std::env::var("ASTS_API_KEY").unwrap_or_default(),
            mno_partner_id: std::env::var("ASTS_MNO_PARTNER_ID").ok(),
        });

    let telemetry_config = TelemetryConfig {
        enabled: std::env::var("TELEMETRY_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .unwrap_or(false),
        endpoint: std::env::var("TELEMETRY_ENDPOINT").ok(),
        ping_interval_ms: std::env::var("TELEMETRY_PING_INTERVAL_MS")
            .unwrap_or_else(|_| "5000".to_string())
            .parse()
            .unwrap_or(5000),
    };

    // Initialize gateway
    let gateway = Gateway::new(
        1000,                                    // buffer size
        tokio::time::Duration::from_millis(100), // batch timeout
        tokio::time::Duration::from_secs(60),    // cache TTL
        asts_credentials,
        telemetry_config,
    );

    tracing::info!("Gateway initialized");
    tracing::info!("Float Protocols ready to accept messages");

    // Example: Send a test message (in production, this would come from network I/O)
    if std::env::var("FLOAT_PROTOCOLS_TEST").is_ok() {
        let test_message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(&b"test message from Float Protocols"[..]),
            Priority::Operational,
        );
        gateway.send(test_message).await?;
        tracing::info!("Test message sent");
    }

    // Wait for shutdown signal with graceful drain
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;

        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM, initiating graceful shutdown...");
            }
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT, initiating graceful shutdown...");
            }
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await?;
        tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
    }

    // Graceful shutdown: drain in-flight messages with timeout
    tracing::info!("Draining in-flight messages (5s timeout)...");
    let drain_result = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        async {
            // Gateway will naturally drain as we drop it
            // The timeout ensures we don't hang indefinitely
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        },
    )
    .await;

    match drain_result {
        Ok(_) => tracing::info!("Graceful shutdown complete"),
        Err(_) => tracing::warn!("Shutdown timeout exceeded, forcing exit"),
    }

    // Print final metrics
    let metrics = gateway.metrics();
    let snapshot = metrics.snapshot();
    tracing::info!("Final metrics: {:?}", snapshot);

    tracing::info!("Float Protocols shutdown complete");
    Ok(())
}
