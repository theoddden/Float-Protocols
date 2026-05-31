//! Hardware drivers for Float Gateway
//!
//! Drivers for all physical components on the carrier board:
//! - ATECC608B: I2C ECC root of trust
//! - BG95-S5: LTE-M/NTN modem (AT commands over UART)
//! - SX1262: LoRa transceiver (SPI)
//! - TPS3813: Hardware watchdog timer
//! - RGB LED: Status indicator
//! - Config button: Provisioning mode trigger

pub mod atecc608b;
pub mod bg95;
pub mod config_button;
pub mod led;
pub mod sx1262;
pub mod watchdog;

pub use atecc608b::ATECC608B;
pub use bg95::BG95Modem;
pub use config_button::ConfigButton;
pub use led::LedController;
pub use sx1262::SX1262;
pub use watchdog::Watchdog;
