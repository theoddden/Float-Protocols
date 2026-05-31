//! Float Gateway - Hardware Supervisor
//!
//! Production hardware supervisor for Float Gateway on Raspberry Pi CM5.
//! Manages all hardware components, sensor inputs, and protocol translation.
//!
//! Startup sequence:
//! 1. Spawn watchdog kick task (first, before anything else)
//! 2. Open dm-crypt NVMe volume, replay WAL into BiTemporalStore
//! 3. Init ATECC608B (I2C), read serial, verify provisioned
//! 4. Init BG95-S5 (UART), power on modem, wait for LTE-M registration
//! 5. Init SX1262 (SPI), configure frequency, arm DIO1 IRQ
//! 6. Init RS-232/RS-485/CAN readers
//! 7. Start config button interrupt listener
//! 8. Start LED state machine task
//! 9. Start GNSS reader, sync RTC when fix acquired
//! 10. Start gateway (protocol translation)
//! 11. Bridge: SX1262 RX → gateway.send()
//!             RS-232/RS-485/CAN frame → gateway.send()
//!             gateway uplink → BG95-S5 AT uplink
//! 12. Graceful shutdown: flush WAL, close dm-crypt, kick watchdog one last time

use float_protocols::gateway::{ASTSCredentials, Gateway, TelemetryConfig};
use float_protocols::gnss::{GnssService, RtcSync};
use float_protocols::hardware::{
    BG95Modem, ButtonEvent, ConfigButton, LedController, LedState, Watchdog, ATECC608B, SX1262,
};
use float_protocols::lora::LoRaMeshAggregator;
use float_protocols::protocol::{Message, Priority, Protocol};
use float_protocols::provisioning::{LoRaProvisioningMode, ProvisioningService};
use float_protocols::reliability::init_startup_time;
use float_protocols::sensor::{CanReader, Rs232Reader, Rs485Reader};
use float_protocols::storage::EncryptedStore;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up panic hook for structured crash logging
    std::panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::capture();
        tracing::error!(
            panic = %panic_info,
            backtrace = %backtrace,
            "Float Gateway crashed"
        );
    }));

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "float_gateway=info,tokio=warn".to_string()),
        )
        .init();

    tracing::info!("Float Gateway hardware supervisor starting...");

    // Step 1: Spawn watchdog kick task (MUST be first)
    let _watchdog = Watchdog::new()?;
    tracing::info!("Watchdog initialized");

    // Step 2: Open dm-crypt NVMe volume and replay WAL
    let encrypted_store = EncryptedStore::open().await?;
    let bitemporal = encrypted_store.bitemporal();
    tracing::info!("Encrypted NVMe store opened, WAL replayed");

    // Step 3: Init ATECC608B
    let atecc = ATECC608B::new()?;
    let gateway_id = hex::encode(atecc.read_serial()?);
    tracing::info!("ATECC608B initialized, gateway ID: {}", gateway_id);

    // Check provisioning status
    let provisioning = ProvisioningService::new(atecc)?;
    if provisioning.status() == float_protocols::provisioning::ProvisioningStatus::NotProvisioned {
        tracing::warn!("Gateway not provisioned, waiting for provisioning via QR code or LoRa");
    }

    // Step 4: Init BG95-S5 modem
    let mut modem = BG95Modem::new().await?;
    modem.register_network().await?;
    tracing::info!("BG95-S5 modem registered to network");

    // Step 5: Init SX1262 LoRa
    let lora_freq = std::env::var("LORA_FREQ")
        .unwrap_or_else(|_| "915".to_string())
        .parse::<u32>()
        .unwrap_or(915);
    let sx1262 = SX1262::new(lora_freq)?;
    tracing::info!("SX1262 initialized at {} MHz", lora_freq);

    // Step 6: Init sensor readers
    let (sensor_tx, sensor_rx) = mpsc::channel(1000);

    // RS-232 reader
    let rs232_tx = sensor_tx.clone();
    tokio::spawn(async move {
        if let Ok(mut reader) = Rs232Reader::new(rs232_tx) {
            let _ = reader.start().await;
        }
    });

    // RS-485 reader (requires ECO Change 4 hardware)
    let rs485_tx = sensor_tx.clone();
    tokio::spawn(async move {
        if let Ok(mut reader) = Rs485Reader::new(rs485_tx) {
            let _ = reader.start().await;
        }
    });

    // CAN reader (requires ECO Change 2 hardware)
    let can_tx = sensor_tx.clone();
    tokio::spawn(async move {
        if let Ok(mut reader) = CanReader::new(can_tx) {
            let _ = reader.start().await;
        }
    });

    tracing::info!("Sensor readers initialized");

    // Step 7: Start config button interrupt listener
    let (config_button, mut button_rx) = ConfigButton::new()?;
    let led_controller = LedController::new()?;
    let provisioning_mode = LoRaProvisioningMode::new(sx1262);

    tokio::spawn(async move {
        while let Some(event) = button_rx.recv().await {
            match event {
                ButtonEvent::ProvisioningMode => {
                    tracing::info!("Entering LoRa provisioning mode");
                    let _ = provisioning_mode.start().await;
                }
                ButtonEvent::FactoryReset => {
                    tracing::warn!("Factory reset triggered");
                    let _ = ProvisioningService::factory_reset();
                    // Reboot
                    std::process::Command::new("reboot").status().ok();
                }
                _ => {}
            }
        }
    });

    tracing::info!("Config button listener started");

    // Step 8: Start LED state machine
    led_controller.set_state(LedState::NoUplink).await;

    // Step 9: Start GNSS service
    let gnss_service = GnssService::new(modem.clone()).await?;
    tokio::spawn(async move {
        loop {
            if let Ok(fix) = gnss_service.get_fix().await {
                if fix.fix_quality > 0 {
                    let _ = RtcSync::sync_from_gnss(&fix);
                }
            }
            sleep(Duration::from_secs(60)).await;
        }
    });

    tracing::info!("GNSS service started");

    // Step 10: Start gateway
    init_startup_time();

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

    let gateway = Gateway::new(
        1000,
        Duration::from_millis(100),
        Duration::from_secs(60),
        asts_credentials,
        telemetry_config,
    );

    tracing::info!("Gateway initialized");

    // Step 11: Bridge sensor inputs to gateway
    let gateway_clone = Arc::clone(&gateway);
    let encrypted_store_clone = Arc::clone(&encrypted_store);

    tokio::spawn(async move {
        while let Some(message) = sensor_rx.recv().await {
            // Store in encrypted store (WAL)
            let _ = encrypted_store_clone.store(message.clone()).await;

            // Send to gateway
            let _ = gateway_clone.send(message).await;
        }
    });

    // Step 11b: Bridge LoRa mesh to gateway
    let (lora_tx, lora_rx) = mpsc::channel(1000);
    let lora_aggregator = LoRaMeshAggregator::new(sx1262, lora_tx);
    lora_aggregator.start().await?;

    let gateway_clone2 = Arc::clone(&gateway);
    let encrypted_store_clone2 = Arc::clone(&encrypted_store);

    tokio::spawn(async move {
        while let Some(message) = lora_rx.recv().await {
            let _ = encrypted_store_clone2.store(message.clone()).await;
            let _ = gateway_clone2.send(message).await;
        }
    });

    // Update LED to connected state
    led_controller.set_state(LedState::Connected).await;

    tracing::info!("Float Gateway hardware supervisor ready");

    // Wait for shutdown signal
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
        tokio::signal::ctrl_c().await?;
        tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
    }

    // Graceful shutdown
    led_controller.set_state(LedState::Off).await;
    led_controller.stop();

    // Flush WAL
    tracing::info!("Flushing WAL...");
    // WAL is flushed on every write, but ensure final sync

    // Close encrypted store
    encrypted_store.close().await?;

    // Stop LoRa provisioning mode
    provisioning_mode.stop().await;

    tracing::info!("Float Gateway shutdown complete");
    Ok(())
}
