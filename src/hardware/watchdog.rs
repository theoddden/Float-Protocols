//! TPS3813 Window Watchdog Timer Driver
//!
//! TI TPS3813K33DBVR window watchdog timer.
//! GPIO kick every 30s to prevent CM5 reset.
//!
//! GPIO assignment:
//! - WDI (Watchdog Input): GPIO26 (open-drain, active low)
//!
//! Window watchdog: must kick within 0.8s to 1.6s window after 30s timeout.
//! Kick sequence: LOW → HIGH → LOW (open-drain pull-up)
//!
//! If firmware hangs, watchdog expires and resets CM5 via RUN pin.

use rppal::gpio::{Gpio, Level, OutputPin};
use std::time::Duration;
use tokio::time::sleep;

/// Watchdog kick interval (must be < 30s)
const WATCHDOG_KICK_INTERVAL_MS: u64 = 25000;

/// GPIO pin for watchdog kick
const WATCHDOG_GPIO: u8 = 26;

/// TPS3813 watchdog driver
pub struct Watchdog {
    pin: OutputPin,
}

impl Watchdog {
    /// Initialize watchdog and start kick task
    pub fn new() -> Result<Self, WatchdogError> {
        let gpio = Gpio::new().map_err(|e| WatchdogError::GpioInit(e.to_string()))?;

        let pin = gpio
            .get(WATCHDOG_GPIO)
            .map_err(|e| WatchdogError::GpioInit(e.to_string()))?
            .into_output();

        let watchdog = Self { pin };

        // Start watchdog kick task
        let mut watchdog_clone = watchdog.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(WATCHDOG_KICK_INTERVAL_MS)).await;
                if let Err(e) = watchdog_clone.kick() {
                    tracing::error!("Watchdog kick failed: {}", e);
                }
            }
        });

        tracing::info!(
            "TPS3813 watchdog initialized, kicking every {}ms",
            WATCHDOG_KICK_INTERVAL_MS
        );
        Ok(watchdog)
    }

    /// Kick the watchdog (LOW → HIGH → LOW sequence)
    pub fn kick(&mut self) -> Result<(), WatchdogError> {
        // Open-drain: set LOW to pull down
        self.pin.set_low();
        std::thread::sleep(Duration::from_millis(10));

        // Release to HIGH (pull-up)
        self.pin.set_high();
        std::thread::sleep(Duration::from_millis(10));

        // Set LOW again to complete kick
        self.pin.set_low();

        Ok(())
    }

    /// Clone for use in async task
    fn clone(&self) -> Self {
        let gpio = Gpio::new().unwrap();
        let pin = gpio.get(WATCHDOG_GPIO).unwrap().into_output();
        Self { pin }
    }
}

#[derive(Debug)]
pub enum WatchdogError {
    GpioInit(String),
    KickFailed,
}

impl std::fmt::Display for WatchdogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WatchdogError::GpioInit(e) => write!(f, "GPIO init failed: {}", e),
            WatchdogError::KickFailed => write!(f, "Watchdog kick failed"),
        }
    }
}

impl std::error::Error for WatchdogError {}
