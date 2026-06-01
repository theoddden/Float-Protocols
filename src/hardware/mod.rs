//! Hardware drivers for Float Gateway
//!
//! Drivers for all physical components on the carrier board:
//! - ATECC608B: I2C ECC root of trust
//! - GM02SP: LTE-M/NTN modem (AT commands over UART) - v0.1.0
//! - LR1121: LoRa transceiver (SPI) - v0.1.0
//! - TPS3813: Hardware watchdog timer
//! - RGB LED: Status indicator
//! - Config button: Provisioning mode trigger
//!
//! Legacy drivers (v0.0.4):
//! - BG95-S5: LTE-M/NTN modem (replaced by GM02SP)
//! - SX1262: LoRa transceiver (replaced by LR1121)

pub mod atecc608b;
pub mod config_button;
pub mod gm02sp;
pub mod led;
pub mod lr1121;
pub mod watchdog;

// Legacy drivers - kept for reference, not used in v0.1.0
#[cfg(feature = "legacy_hardware")]
pub mod bg95;
#[cfg(feature = "legacy_hardware")]
pub mod sx1262;

pub use atecc608b::ATECC608B;
pub use config_button::ConfigButton;
pub use gm02sp::GM02SPModem;
pub use led::LedController;
pub use lr1121::LR1121;
pub use watchdog::Watchdog;

// Legacy exports - only available with legacy_hardware feature
#[cfg(feature = "legacy_hardware")]
pub use bg95::BG95Modem;
#[cfg(feature = "legacy_hardware")]
pub use sx1262::SX1262;
