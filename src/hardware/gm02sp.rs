//! GM02SP LTE-M/NTN Modem Driver
//!
//! Sequans GM02SP modem with LTE-M + NB-IoT + NTN + GNSS.
//! AT command interface over UART.
//!
//! GPIO assignments (from v0.1.0 schematic):
//! - UART0 TX: GPIO14 → GM02SP RX
//! - UART0 RX: GPIO15 → GM02SP TX
//! - PWRKEY: GPIO24 (modem power key)
//! - RESET: GPIO25 (hardware reset)
//!
//! Features:
//! - LTE-M/NTN network registration
//! - TCP/IP data uplink via AT commands
//! - GNSS positioning (NMEA output)
//! - Signal quality monitoring (CSQ, RSRP)
//! - eSIM status checking
//!
//! Reference: Sequans GM02SP Datasheet and AT Command Reference
//!
//! NOTE: This is a stub implementation. AT commands need to be verified
//! against the GM02SP datasheet before production use.

use serialport::{SerialPort, SerialPortInfo};
use std::io::{Read, Write};
use std::time::Duration;
use tokio::time::sleep;

/// UART device for GM02SP
const GM02SP_UART: &str = "/dev/ttyAMA0";
const GM02SP_BAUD: u32 = 115200;

/// AT command timeout
const AT_TIMEOUT_MS: u64 = 5000;

/// Network registration timeout
const REG_TIMEOUT_MS: u64 = 60000;

/// Modem state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModemState {
    Off,
    Booting,
    Registered,
    Connecting,
    Connected,
    Error,
}

/// Network type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkType {
    LTE_M,
    NB_IoT,
    NTN,
    Unknown,
}

/// Signal quality
#[derive(Debug, Clone)]
pub struct SignalQuality {
    pub rssi: i8,  // Received Signal Strength Indicator (0-31, 99=unknown)
    pub ber: u8,   // Bit Error Rate (0-7, 99=unknown)
    pub rsrp: i16, // Reference Signal Received Power (dBm)
    pub rsrq: i8,  // Reference Signal Received Quality (dB)
    pub sinr: i8,  // Signal to Interference plus Noise Ratio (dB)
}

/// GNSS fix
#[derive(Debug, Clone)]
pub struct GnssFix {
    pub latitude: f64,
    pub longitude: f64,
    pub altitude: f64,
    pub speed: f64,
    pub heading: f64,
    pub fix_quality: u8, // 0=invalid, 1=GPS, 2=DGPS
    pub satellites: u8,
    pub timestamp: u64, // Unix timestamp in milliseconds
}

/// GM02SP modem driver
pub struct GM02SPModem {
    port: Box<dyn SerialPort>,
    state: ModemState,
    network_type: NetworkType,
}

impl GM02SPModem {
    /// Initialize GM02SP modem
    pub fn new() -> Result<Self, Gm02spError> {
        let port = serialport::new(GM02SP_UART, GM02SP_BAUD)
            .timeout(Duration::from_millis(AT_TIMEOUT_MS))
            .open()
            .map_err(|e| Gm02spError::SerialInit(e.to_string()))?;

        let mut modem = Self {
            port,
            state: ModemState::Off,
            network_type: NetworkType::Unknown,
        };

        // Power on modem
        modem.power_on().await?;

        // Wait for boot
        sleep(Duration::from_secs(3)).await;

        // Disable echo
        modem.send_at("ATE0").await?;

        // Check modem is responsive
        modem.send_at("AT").await?;

        modem.state = ModemState::Booting;
        Ok(modem)
    }

    /// Power on modem via PWRKEY GPIO
    async fn power_on(&mut self) -> Result<(), Gm02spError> {
        // TODO: Implement GPIO control via rppal
        // GPIO24 (PWRKEY) should be pulled low for 1s to power on
        tracing::info!("Powering on GM02SP modem");
        Ok(())
    }

    /// Reset modem via RESET GPIO
    pub async fn reset(&mut self) -> Result<(), Gm02spError> {
        // TODO: Implement GPIO control via rppal
        // GPIO25 (RESET) should be pulled low for 100ms
        tracing::info!("Resetting GM02SP modem");
        self.state = ModemState::Booting;
        sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    /// Send AT command and wait for response
    async fn send_at(&mut self, cmd: &str) -> Result<String, Gm02spError> {
        let full_cmd = format!("{}\r\n", cmd);
        self.port
            .write_all(full_cmd.as_bytes())
            .map_err(|e| Gm02spError::SerialWrite(e.to_string()))?;

        let mut response = String::new();
        let mut buf = [0u8; 1024];
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > Duration::from_millis(AT_TIMEOUT_MS) {
                return Err(Gm02spError::Timeout(cmd.to_string()));
            }

            match self.port.read(&mut buf) {
                Ok(n) => {
                    let chunk = String::from_utf8_lossy(&buf[..n]);
                    response.push_str(&chunk);

                    // Check for OK or ERROR
                    if response.contains("OK") {
                        return Ok(response);
                    }
                    if response.contains("ERROR") {
                        return Err(Gm02spError::AtError(response));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    continue;
                }
                Err(e) => return Err(Gm02spError::SerialRead(e.to_string())),
            }
        }
    }

    /// Register to LTE-M/NTN network
    pub async fn register_network(&mut self) -> Result<(), Gm02spError> {
        tracing::info!("Registering to LTE-M/NTN network");

        // TODO: Verify GM02SP network registration AT commands
        // Quectel BG95 uses AT+QNBIOTEURC=1 and AT+CREG?
        // Sequans GM02SP may use different commands

        // Set network mode (auto-select LTE-M/NB-IoT)
        // ⚠️ VERIFY: GM02SP may use different command
        self.send_at("AT+QNBIOTEURC=1").await?;

        // Wait for network registration
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > Duration::from_millis(REG_TIMEOUT_MS) {
                return Err(Gm02spError::RegistrationTimeout);
            }

            // ⚠️ VERIFY: GM02SP may use different registration query
            let response = self.send_at("AT+CREG?").await?;
            if response.contains("+CREG: 1,1") || response.contains("+CREG: 1,5") {
                tracing::info!("Network registered");
                self.state = ModemState::Registered;
                break;
            }

            sleep(Duration::from_secs(2)).await;
        }

        // Determine network type
        // ⚠️ VERIFY: GM02SP may use different network info command
        let response = self.send_at("AT+QNWINFO").await?;
        if response.contains("LTE-M") {
            self.network_type = NetworkType::LTE_M;
        } else if response.contains("NB-IoT") {
            self.network_type = NetworkType::NB_IoT;
        } else if response.contains("NTN") {
            self.network_type = NetworkType::NTN;
        }

        Ok(())
    }

    /// Get signal quality
    pub async fn get_signal_quality(&mut self) -> Result<SignalQuality, Gm02spError> {
        // ⚠️ VERIFY: GM02SP may use different signal query command
        let response = self.send_at("AT+CSQ").await?;
        // Response format: +CSQ: <rssi>,<ber>
        let rssi = parse_at_int(&response, "CSQ", 0).unwrap_or(99) as i8;
        let ber = parse_at_int(&response, "CSQ", 1).unwrap_or(99) as u8;

        // Get extended signal info
        // ⚠️ VERIFY: GM02SP may use different extended signal command
        let response = self.send_at("AT+QENG=\"servingcell\"").await?;
        let rsrp = parse_at_int(&response, "rsrp", 0).unwrap_or(-140) as i16;
        let rsrq = parse_at_int(&response, "rsrq", 0).unwrap_or(-20) as i8;
        let sinr = parse_at_int(&response, "sinr", 0).unwrap_or(-20) as i8;

        Ok(SignalQuality {
            rssi,
            ber,
            rsrp,
            rsrq,
            sinr,
        })
    }

    /// Open TCP connection
    pub async fn open_tcp(&mut self, host: &str, port: u16) -> Result<u8, Gm02spError> {
        tracing::info!("Opening TCP connection to {}:{}", host, port);

        // ⚠️ VERIFY: GM02SP may use different TCP open command
        // Quectel uses AT+QIOPEN, Sequans may use different command
        let cmd = format!("AT+QIOPEN=1,0,\"TCP\",\"{}\",{},0,1", host, port);
        let response = self.send_at(&cmd).await?;

        if response.contains("OK") {
            self.state = ModemState::Connected;
            Ok(0) // Context ID
        } else {
            Err(Gm02spError::ConnectionFailed(response))
        }
    }

    /// Send data over TCP
    pub async fn send_tcp(&mut self, data: &[u8]) -> Result<(), Gm02spError> {
        let len = data.len();
        // ⚠️ VERIFY: GM02SP may use different send command
        let cmd = format!("AT+QISEND=0,{}", len);
        let response = self.send_at(&cmd).await?;

        if !response.contains(">") {
            return Err(Gm02spError::SendFailed(response));
        }

        self.port
            .write_all(data)
            .map_err(|e| Gm02spError::SerialWrite(e.to_string()))?;

        // Wait for SEND OK
        let response = self.send_at("").await?;
        if response.contains("SEND OK") {
            Ok(())
        } else {
            Err(Gm02spError::SendFailed(response))
        }
    }

    /// Enable GNSS
    pub async fn enable_gnss(&mut self) -> Result<(), Gm02spError> {
        tracing::info!("Enabling GNSS");
        // ⚠️ VERIFY: GM02SP may use different GNSS enable command
        self.send_at("AT+QGPS=1").await?;
        Ok(())
    }

    /// Get GNSS fix
    pub async fn get_gnss_fix(&mut self) -> Result<GnssFix, Gm02spError> {
        // ⚠️ VERIFY: GM02SP may use different GNSS location command
        let response = self.send_at("AT+QGPSLOC=2").await?;
        // Response format: +QGPSLOC: <utc>,<lat>,<lon>,<alt>,<speed>,<course>,<fix>,<hdop>,<vdop>,<hacc>,<vacc>,<numsv>
        let parts: Vec<&str> = response.split(',').collect();

        if parts.len() < 12 {
            return Err(Gm02spError::InvalidGnssResponse(response));
        }

        let latitude = parts[1].parse::<f64>().unwrap_or(0.0);
        let longitude = parts[2].parse::<f64>().unwrap_or(0.0);
        let altitude = parts[3].parse::<f64>().unwrap_or(0.0);
        let speed = parts[4].parse::<f64>().unwrap_or(0.0);
        let heading = parts[5].parse::<f64>().unwrap_or(0.0);
        let fix_quality = parts[6].parse::<u8>().unwrap_or(0);
        let satellites = parts[11].parse::<u8>().unwrap_or(0);

        Ok(GnssFix {
            latitude,
            longitude,
            altitude,
            speed,
            heading,
            fix_quality,
            satellites,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        })
    }

    /// Get eSIM status
    pub async fn get_esim_status(&mut self) -> Result<String, Gm02spError> {
        // ⚠️ VERIFY: GM02SP may use different eSIM query command
        let response = self.send_at("AT+CIMI").await?;
        // Returns IMSI
        Ok(response.trim().to_string())
    }

    /// Get current state
    pub fn state(&self) -> ModemState {
        self.state
    }

    /// Get network type
    pub fn network_type(&self) -> NetworkType {
        self.network_type
    }
}

/// Parse integer from AT command response
fn parse_at_int(response: &str, keyword: &str, index: usize) -> Option<i32> {
    let start = response.find(keyword)?;
    let after_colon = response[start..].find(':')?;
    let values = response[start + after_colon + 1..].split(',');
    values.nth(index)?.trim().parse().ok()
}

#[derive(Debug)]
pub enum Gm02spError {
    SerialInit(String),
    SerialWrite(String),
    SerialRead(String),
    Timeout(String),
    AtError(String),
    RegistrationTimeout,
    ConnectionFailed(String),
    SendFailed(String),
    InvalidGnssResponse(String),
}

impl std::fmt::Display for Gm02spError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Gm02spError::SerialInit(e) => write!(f, "Serial init failed: {}", e),
            Gm02spError::SerialWrite(e) => write!(f, "Serial write failed: {}", e),
            Gm02spError::SerialRead(e) => write!(f, "Serial read failed: {}", e),
            Gm02spError::Timeout(cmd) => write!(f, "AT command timeout: {}", cmd),
            Gm02spError::AtError(resp) => write!(f, "AT command error: {}", resp),
            Gm02spError::RegistrationTimeout => write!(f, "Network registration timeout"),
            Gm02spError::ConnectionFailed(resp) => write!(f, "Connection failed: {}", resp),
            Gm02spError::SendFailed(resp) => write!(f, "Send failed: {}", resp),
            Gm02spError::InvalidGnssResponse(resp) => write!(f, "Invalid GNSS response: {}", resp),
        }
    }
}

impl std::error::Error for Gm02spError {}
