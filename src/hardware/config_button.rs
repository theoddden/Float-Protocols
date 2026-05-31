//! Config Button Interrupt Handler
//!
//! Recessed tactile button for provisioning mode and factory reset.
//!
//! GPIO assignment:
//! - Config button: GPIO18 (active low, 10kΩ pull-up)
//!
//! Button actions:
//! - Hold 3s: Enter LoRa provisioning mode
//! - Hold 10s: Factory reset (wipe NVMe config, reboot)
//!
//! Debounce: 50ms

use rppal::gpio::{Gpio, Level, Trigger};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};

/// GPIO pin for config button
const CONFIG_BUTTON_PIN: u8 = 18;

/// Debounce time
const DEBOUNCE_MS: u64 = 50;

/// Hold durations
const PROVISIONING_HOLD_MS: u64 = 3000;
const FACTORY_RESET_HOLD_MS: u64 = 10000;

/// Button event
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ButtonEvent {
    /// Button pressed (transition to low)
    Pressed,
    /// Button released (transition to high)
    Released,
    /// Provisioning mode triggered (3s hold)
    ProvisioningMode,
    /// Factory reset triggered (10s hold)
    FactoryReset,
}

/// Config button handler
pub struct ConfigButton {
    pressed: Arc<AtomicBool>,
    press_start: Arc<Mutex<Option<Instant>>>,
    event_tx: tokio::sync::mpsc::Sender<ButtonEvent>,
}

impl ConfigButton {
    /// Initialize config button and start interrupt handler
    pub fn new() -> Result<(Self, tokio::sync::mpsc::Receiver<ButtonEvent>), ButtonError> {
        let gpio = Gpio::new().map_err(|e| ButtonError::GpioInit(e.to_string()))?;

        let pin = gpio
            .get(CONFIG_BUTTON_PIN)
            .map_err(|e| ButtonError::GpioInit(e.to_string()))?
            .into_input_pullup();

        let pressed = Arc::new(AtomicBool::new(false));
        let press_start = Arc::new(Mutex::new(None));
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(10);

        let button = Self {
            pressed: pressed.clone(),
            press_start: press_start.clone(),
            event_tx,
        };

        // Set up interrupt on falling edge (button press)
        let mut pin_clone = pin;
        let pressed_clone = pressed.clone();
        let press_start_clone = press_start.clone();
        let event_tx_clone = button.event_tx.clone();

        std::thread::spawn(move || {
            // Use polling for now (rppal doesn't have async interrupt support)
            // In production, use gpio-cdev for edge-triggered interrupts
            let mut last_level = pin_clone.read();
            loop {
                let current_level = pin_clone.read();

                if current_level != last_level {
                    // State change detected
                    sleep(Duration::from_millis(DEBOUNCE_MS));

                    // Debounce check
                    let debounced_level = pin_clone.read();
                    if debounced_level == current_level {
                        // Valid state change
                        if current_level == Level::Low {
                            // Button pressed
                            pressed_clone.store(true, Ordering::Relaxed);
                            *press_start_clone.blocking_lock() = Some(Instant::now());
                            let _ = event_tx_clone.blocking_send(ButtonEvent::Pressed);
                        } else {
                            // Button released
                            pressed_clone.store(false, Ordering::Relaxed);
                            let duration = press_start_clone
                                .blocking_lock()
                                .take()
                                .map(|start| start.elapsed());

                            if let Some(d) = duration {
                                if d >= Duration::from_millis(FACTORY_RESET_HOLD_MS) {
                                    let _ = event_tx_clone.blocking_send(ButtonEvent::FactoryReset);
                                } else if d >= Duration::from_millis(PROVISIONING_HOLD_MS) {
                                    let _ =
                                        event_tx_clone.blocking_send(ButtonEvent::ProvisioningMode);
                                }
                            }

                            let _ = event_tx_clone.blocking_send(ButtonEvent::Released);
                        }
                    }
                    last_level = debounced_level;
                }

                // Check for long-press while button is held
                if pressed_clone.load(Ordering::Relaxed) {
                    if let Some(start) = *press_start_clone.blocking_lock() {
                        let elapsed = start.elapsed();
                        if elapsed >= Duration::from_millis(FACTORY_RESET_HOLD_MS) {
                            // Factory reset threshold reached
                            pressed_clone.store(false, Ordering::Relaxed);
                            *press_start_clone.blocking_lock() = None;
                            let _ = event_tx_clone.blocking_send(ButtonEvent::FactoryReset);
                        } else if elapsed >= Duration::from_millis(PROVISIONING_HOLD_MS) {
                            // Provisioning mode threshold reached
                            let _ = event_tx_clone.blocking_send(ButtonEvent::ProvisioningMode);
                        }
                    }
                }

                sleep(Duration::from_millis(10));
            }
        });

        tracing::info!("Config button initialized on GPIO {}", CONFIG_BUTTON_PIN);
        Ok((button, event_rx))
    }

    /// Check if button is currently pressed
    pub fn is_pressed(&self) -> bool {
        self.pressed.load(Ordering::Relaxed)
    }

    /// Get current press duration
    pub async fn press_duration(&self) -> Option<Duration> {
        let start = *self.press_start.lock().await;
        start.map(|s| s.elapsed())
    }
}

#[derive(Debug)]
pub enum ButtonError {
    GpioInit(String),
}

impl std::fmt::Display for ButtonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ButtonError::GpioInit(e) => write!(f, "GPIO init failed: {}", e),
        }
    }
}

impl std::error::Error for ButtonError {}
