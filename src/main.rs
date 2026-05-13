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

use float_protocols::gateway::{Gateway, ASTSCredentials, TelemetryConfig};
use float_protocols::protocol::{Message, Protocol, Priority};
use bytes::Bytes;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "float_protocols=info,tokio=warn".to_string()),
        )
        .init();

    tracing::info!("Float Protocols starting up...");

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
        1000, // buffer size
        tokio::time::Duration::from_millis(100), // batch timeout
        tokio::time::Duration::from_secs(60), // cache TTL
        asts_credentials,
        telemetry_config,
    );

    tracing::info!("Gateway initialized");
    tracing::info!("Float Protocols ready to accept messages");

    // Example: Send a test message (in production, this would come from network I/O)
    if std::env::var("FLOAT_PROTOCOLS_TEST").is_ok() {
        let test_message = Message::new(
            Protocol::IridiumSBD,
            Bytes::from(b"test message from Float Protocols"),
            Priority::Operational,
        );
        gateway.send(test_message).await?;
        tracing::info!("Test message sent");
    }

    // Wait for shutdown signal
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;

        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM, shutting down...");
            }
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT, shutting down...");
            }
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await?;
        tracing::info!("Received Ctrl+C, shutting down...");
    }

    // Print final metrics
    let metrics = gateway.metrics();
    let snapshot = metrics.snapshot();
    tracing::info!("Final metrics: {:?}", snapshot);

    tracing::info!("Float Protocols shutdown complete");
    Ok(())
}
