//! RGB LED Status Indicator Driver
//!
//! RGB LED with light pipe for gateway status indication.
//!
//! GPIO assignments:
//! - Red: GPIO17
//! - Green: GPIO27
//! - Blue: GPIO6 (reassigned from GPIO22 to avoid SX1262 DIO1 conflict)
//!
//! LED states:
//! - Solid green: Connected (LTE-M/NTN up, no queue)
//! - Slow pulse green: No uplink (BG95 not registered)
//! - Fast amber: Burst in progress (drain_spread_shard)
//! - Solid red: Fault (circuit breaker open / ATECC error)

use rppal::gpio::{Gpio, Level, OutputPin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

/// GPIO pins for RGB LED
const LED_RED_PIN: u8 = 17;
const LED_GREEN_PIN: u8 = 27;
const LED_BLUE_PIN: u8 = 6; // Reassigned from GPIO22 to avoid SX1262 DIO1 conflict

/// LED state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedState {
    /// Solid green: Connected (LTE-M/NTN up, no queue)
    Connected,
    /// Slow pulse green: No uplink (BG95 not registered)
    NoUplink,
    /// Fast amber: Burst in progress
    BurstInProgress,
    /// Solid red: Fault
    Fault,
    /// Off
    Off,
}

/// LED controller
pub struct LedController {
    red: OutputPin,
    green: OutputPin,
    blue: OutputPin,
    state: Arc<Mutex<LedState>>,
    running: Arc<AtomicBool>,
}

impl LedController {
    /// Initialize LED controller
    pub fn new() -> Result<Self, LedError> {
        let gpio = Gpio::new().map_err(|e| LedError::GpioInit(e.to_string()))?;

        let red = gpio
            .get(LED_RED_PIN)
            .map_err(|e| LedError::GpioInit(e.to_string()))?
            .into_output();

        let green = gpio
            .get(LED_GREEN_PIN)
            .map_err(|e| LedError::GpioInit(e.to_string()))?
            .into_output();

        let blue = gpio
            .get(LED_BLUE_PIN)
            .map_err(|e| LedError::GpioInit(e.to_string()))?
            .into_output();

        let state = Arc::new(Mutex::new(LedState::Off));
        let running = Arc::new(AtomicBool::new(true));

        let controller = Self {
            red,
            green,
            blue,
            state: state.clone(),
            running: running.clone(),
        };

        // Start LED update task
        tokio::spawn(async move {
            let mut last_state = LedState::Off;
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                let current_state = *state.lock().await;

                // Only update if state changed
                if current_state != last_state {
                    match current_state {
                        LedState::Connected => {
                            // Solid green
                            controller.set_color(Level::Low, Level::High, Level::Low);
                        }
                        LedState::NoUplink => {
                            // Slow pulse green (1Hz)
                            controller.pulse_green_slow().await;
                        }
                        LedState::BurstInProgress => {
                            // Fast amber (5Hz)
                            controller.pulse_amber_fast().await;
                        }
                        LedState::Fault => {
                            // Solid red
                            controller.set_color(Level::High, Level::Low, Level::Low);
                        }
                        LedState::Off => {
                            // Off
                            controller.set_color(Level::High, Level::High, Level::High);
                        }
                    }
                    last_state = current_state;
                }

                sleep(Duration::from_millis(100)).await;
            }
        });

        tracing::info!("RGB LED controller initialized");
        Ok(controller)
    }

    /// Set LED state
    pub async fn set_state(&self, state: LedState) {
        *self.state.lock().await = state;
    }

    /// Set RGB color (LOW = on, HIGH = off for common anode)
    fn set_color(&self, red: Level, green: Level, blue: Level) {
        self.red.set_level(red);
        self.green.set_level(green);
        self.blue.set_level(blue);
    }

    /// Slow pulse green (1Hz)
    async fn pulse_green_slow(&self) {
        for _ in 0..5 {
            self.set_color(Level::Low, Level::High, Level::Low);
            sleep(Duration::from_millis(500)).await;
            self.set_color(Level::High, Level::High, Level::High);
            sleep(Duration::from_millis(500)).await;
        }
    }

    /// Fast amber pulse (5Hz)
    async fn pulse_amber_fast(&self) {
        for _ in 0..10 {
            self.set_color(Level::Low, Level::Low, Level::High); // Red + Green = Amber
            sleep(Duration::from_millis(100)).await;
            self.set_color(Level::High, Level::High, Level::High);
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop LED controller
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub enum LedError {
    GpioInit(String),
}

impl std::fmt::Display for LedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedError::GpioInit(e) => write!(f, "GPIO init failed: {}", e),
        }
    }
}

impl std::error::Error for LedError {}
