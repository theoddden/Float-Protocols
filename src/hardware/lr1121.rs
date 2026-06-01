//! LR1121 LoRa Transceiver Driver
//!
//! Semtech LR1121 LoRa transceiver for 915/868 MHz + 2.4 GHz operation.
//! SPI interface with GPIO for CS, DIO1 (IRQ), and RESET.
//!
//! GPIO assignments (from v0.1.0 schematic):
//! - SPI0: MOSI (GPIO10), MISO (GPIO9), SCK (GPIO11), CS (GPIO8)
//! - DIO1 (packet RX/TX done): GPIO22
//! - RESET: GPIO23
//!
//! Features:
//! - LoRa modulation (SF7-SF12, BW125-BW500)
//! - 2.4 GHz band support
//! - CAD (Channel Activity Detection)
//! - TX power up to +22 dBm
//! - Low power sleep mode
//!
//! Reference: Semtech LR1121 Datasheet (DSLR1121)
//!
//! NOTE: This is a stub implementation. Register map and opcodes need to be
//! verified against the LR1121 datasheet before production use.

use rppal::gpio::{Gpio, InputPin, Level, OutputPin, Pin};
use rppal::spi::{Bus, Mode, SlaveSelect, Spi};
use std::time::Duration;
use tokio::sync::mpsc;

/// SPI bus for LR1121
const LR1121_SPI_BUS: u8 = 0;
const LR1121_SPI_CS: u8 = 0;

/// GPIO pins (same as SX1262)
const LR1121_CS_PIN: u8 = 8;
const LR1121_DIO1_PIN: u8 = 22;
const LR1121_RESET_PIN: u8 = 23;

/// LR1121 register addresses
/// TODO: Verify these against LR1121 datasheet
#[repr(u8)]
enum Register {
    RegFifoTxPtr = 0x0D,
    RegFifoTxBase = 0x0E,
    RegFifoRxPtr = 0x0F,
    RegFifoRxBase = 0x10,
    RegIrqFlags = 0x12,
    RegIrqFlagsMask = 0x13,
    RegFifoAddrPtr = 0x0C,
    RegPacketParams = 0x1A,
    RegModemParams = 0x1B,
    RegRfFreq = 0x06,
    RegPaConfig = 0x09,
    RegPaDutyCycle = 0x0A,
}

/// LR1121 opcodes
/// TODO: Verify these against LR1121 datasheet
#[repr(u8)]
enum Opcode {
    WriteRegister = 0x0D,
    ReadRegister = 0x1D,
    WriteBuffer = 0x0E,
    ReadBuffer = 0x1E,
    SetSleep = 0x84,
    SetStandby = 0x80,
    SetFs = 0xC1,
    SetTx = 0x83,
    SetRx = 0x82,
    StopTimerOnPreamble = 0x9F,
    SetRxDutyCycle = 0x94,
    SetCad = 0xC5,
    SetTxContinuousWave = 0xD1,
    SetTxContinuousPreamble = 0x8B,
    SetPacketType = 0x8A,
    GetIrqStatus = 0x12,
    ClearIrqStatus = 0x02,
    SetRfFrequency = 0x86,
    SetTxParams = 0x8E,
    SetCadParams = 0x87,
    SetBufferBaseAddress = 0x8F,
    SetModulationParams = 0x8D,
    SetPacketParams = 0x8C,
    GetStatus = 0xC0,
    GetPacketType = 0x11,
    GetRxBufferGain = 0x18,
    SetLoraSymbTimeout = 0xA0,
}

/// LoRa spreading factor
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SpreadingFactor {
    SF5 = 0x05,
    SF6 = 0x06,
    SF7 = 0x07,
    SF8 = 0x08,
    SF9 = 0x09,
    SF10 = 0x0A,
    SF11 = 0x0B,
    SF12 = 0x0C,
}

/// LoRa bandwidth
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Bandwidth {
    BW125 = 0x00,
    BW250 = 0x01,
    BW500 = 0x02,
}

/// LoRa coding rate
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CodingRate {
    CR4_5 = 0x01,
    CR4_6 = 0x02,
    CR4_7 = 0x03,
    CR4_8 = 0x04,
}

/// LoRa frame header type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HeaderType {
    Explicit = 0x00,
    Implicit = 0x01,
}

/// LoRa CRC
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CrcMode {
    Enabled = 0x01,
    Disabled = 0x00,
}

/// LoRa IQ mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IqMode {
    Standard = 0x00,
    Inverted = 0x01,
}

/// LoRa configuration
#[derive(Debug, Clone)]
pub struct LoRaConfig {
    pub frequency: u32, // Hz (e.g., 915000000 for 915 MHz)
    pub bandwidth: Bandwidth,
    pub spreading_factor: SpreadingFactor,
    pub coding_rate: CodingRate,
    pub header_type: HeaderType,
    pub crc: CrcMode,
    pub iq_mode: IqMode,
    pub preamble_length: u16,
    pub tx_power: i8, // dBm (-17 to +22)
}

impl Default for LoRaConfig {
    fn default() -> Self {
        Self {
            frequency: 915000000,
            bandwidth: Bandwidth::BW125,
            spreading_factor: SpreadingFactor::SF7,
            coding_rate: CodingRate::CR4_5,
            header_type: HeaderType::Explicit,
            crc: CrcMode::Enabled,
            iq_mode: IqMode::Standard,
            preamble_length: 8,
            tx_power: 14,
        }
    }
}

/// LoRa frame
#[derive(Debug, Clone)]
pub struct LoRaFrame {
    pub data: Vec<u8>,
    pub rssi: i16,
    pub snr: i8,
    pub frequency_error: i32,
}

/// LR1121 driver
pub struct LR1121 {
    spi: Spi,
    cs: OutputPin,
    dio1: InputPin,
    reset: OutputPin,
    config: LoRaConfig,
    tx_channel: mpsc::Sender<LoRaFrame>,
    rx_channel: mpsc::Receiver<LoRaFrame>,
}

impl LR1121 {
    /// Initialize LR1121 with default LoRa config
    pub fn new(frequency_mhz: u32) -> Result<Self, Lr1121Error> {
        let spi = Spi::new(Bus::Spi0, SlaveSelect::Ss0, 1_000_000, Mode::Mode0)
            .map_err(|e| Lr1121Error::SpiInit(e.to_string()))?;

        let gpio = Gpio::new().map_err(|e| Lr1121Error::GpioInit(e.to_string()))?;

        let cs = gpio
            .get(LR1121_CS_PIN)
            .map_err(|e| Lr1121Error::GpioInit(e.to_string()))?
            .into_output();

        let dio1 = gpio
            .get(LR1121_DIO1_PIN)
            .map_err(|e| Lr1121Error::GpioInit(e.to_string()))?
            .into_input_pullup();

        let reset = gpio
            .get(LR1121_RESET_PIN)
            .map_err(|e| Lr1121Error::GpioInit(e.to_string()))?
            .into_output();

        let (tx_channel, rx_channel) = mpsc::channel(100);

        let mut lr1121 = Self {
            spi,
            cs,
            dio1,
            reset,
            config: LoRaConfig::default(),
            tx_channel,
            rx_channel,
        };

        // Set frequency
        lr1121.config.frequency = frequency_mhz * 1_000_000;

        // Reset chip
        lr1121.reset()?;

        // Wake up from sleep
        lr1121.wake_up()?;

        // Set packet type to LoRa
        lr1121.set_packet_type(PacketType::LoRa)?;

        // Configure LoRa parameters
        lr1121.configure_lora()?;

        // Set buffer base addresses
        lr1121.set_buffer_base_address()?;

        // Clear IRQ flags
        lr1121.clear_irq()?;

        Ok(lr1121)
    }

    /// Reset LR1121
    fn reset(&mut self) -> Result<(), Lr1121Error> {
        self.reset.set_low();
        std::thread::sleep(Duration::from_millis(10));
        self.reset.set_high();
        std::thread::sleep(Duration::from_millis(10));
        Ok(())
    }

    /// Wake up from sleep mode
    fn wake_up(&mut self) -> Result<(), Lr1121Error> {
        self.cs.set_low();
        let cmd = [Opcode::SetStandby as u8, 0x01]; // STDBY_RC
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        std::thread::sleep(Duration::from_millis(1));
        Ok(())
    }

    /// Set packet type
    fn set_packet_type(&mut self, pkt_type: PacketType) -> Result<(), Lr1121Error> {
        self.cs.set_low();
        let cmd = [Opcode::SetPacketType as u8, pkt_type as u8];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        Ok(())
    }

    /// Configure LoRa parameters
    fn configure_lora(&mut self) -> Result<(), Lr1121Error> {
        // Set RF frequency
        self.set_rf_frequency(self.config.frequency)?;

        // Set modulation parameters
        self.set_modulation_params()?;

        // Set packet parameters
        self.set_packet_params()?;

        // Set TX power
        self.set_tx_params(self.config.tx_power)?;

        Ok(())
    }

    /// Set RF frequency
    fn set_rf_frequency(&mut self, freq_hz: u32) -> Result<(), Lr1121Error> {
        // TODO: Verify crystal frequency for LR1121
        // SX1262 uses 32 MHz, LR1121 may differ
        let fxtal = 32_000_000u64; // ⚠️ VERIFY FOR LR1121
        let rf_reg = (freq_hz as u64 * (1u64 << 25) / fxtal) as u32;

        self.cs.set_low();
        let cmd = [
            Opcode::SetRfFrequency as u8,
            (rf_reg >> 24) as u8,
            (rf_reg >> 16) as u8,
            (rf_reg >> 8) as u8,
            rf_reg as u8,
        ];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        Ok(())
    }

    /// Set modulation parameters
    fn set_modulation_params(&mut self) -> Result<(), Lr1121Error> {
        self.cs.set_low();
        let cmd = [
            Opcode::SetModulationParams as u8,
            self.config.spreading_factor as u8,
            self.config.bandwidth as u8,
            self.config.coding_rate as u8,
            0x01, // LDRO (Low Data Rate Optimization)
            self.config.header_type as u8,
            self.config.crc as u8,
            self.config.iq_mode as u8,
        ];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        Ok(())
    }

    /// Set packet parameters
    fn set_packet_params(&mut self) -> Result<(), Lr1121Error> {
        self.cs.set_low();
        let cmd = [
            Opcode::SetPacketParams as u8,
            (self.config.preamble_length >> 8) as u8,
            self.config.preamble_length as u8,
            0x00, // Header length (auto)
            self.config.crc as u8,
            0x00, // IQ inversion
        ];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        Ok(())
    }

    /// Set TX power
    fn set_tx_params(&mut self, power_dbm: i8) -> Result<(), Lr1121Error> {
        self.cs.set_low();
        let cmd = [
            Opcode::SetTxParams as u8,
            power_dbm as u8,
            0x04, // Ramp time (200us)
        ];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        Ok(())
    }

    /// Set buffer base addresses
    fn set_buffer_base_address(&mut self) -> Result<(), Lr1121Error> {
        self.cs.set_low();
        let cmd = [
            Opcode::SetBufferBaseAddress as u8,
            0x00, // TX base address
            0x00, // RX base address
        ];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        Ok(())
    }

    /// Clear IRQ flags
    fn clear_irq(&mut self) -> Result<(), Lr1121Error> {
        self.cs.set_low();
        let cmd = [
            Opcode::ClearIrqStatus as u8,
            0xFF, // Clear all IRQs
            0xFF,
        ];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        Ok(())
    }

    /// Transmit LoRa frame
    pub async fn tx_packet(&mut self, data: &[u8]) -> Result<(), Lr1121Error> {
        // Write data to FIFO
        self.write_buffer(data)?;

        // Set TX mode
        self.cs.set_low();
        let cmd = [Opcode::SetTx as u8, 0x00]; // Timeout 0 (no timeout)
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();

        // Wait for TX done IRQ
        self.wait_for_irq(Irq::TxDone).await?;

        // Clear IRQ
        self.clear_irq()?;

        Ok(())
    }

    /// Receive LoRa frame (continuous RX)
    pub async fn rx_packet(&mut self) -> Result<LoRaFrame, Lr1121Error> {
        // Set RX mode
        self.cs.set_low();
        let cmd = [Opcode::SetRx as u8, 0x00]; // Timeout 0 (continuous)
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();

        // Wait for RX done IRQ
        self.wait_for_irq(Irq::RxDone).await?;

        // Read packet from FIFO
        let data = self.read_buffer()?;

        // Get RSSI and SNR
        let (rssi, snr) = self.get_rssi_snr()?;

        // Clear IRQ
        self.clear_irq()?;

        Ok(LoRaFrame {
            data,
            rssi,
            snr,
            frequency_error: 0,
        })
    }

    /// Write data to TX FIFO
    fn write_buffer(&mut self, data: &[u8]) -> Result<(), Lr1121Error> {
        self.cs.set_low();
        let mut cmd = vec![Opcode::WriteBuffer as u8, 0x00]; // Offset 0
        cmd.extend(data);
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        self.cs.set_high();
        Ok(())
    }

    /// Read data from RX FIFO
    fn read_buffer(&mut self) -> Result<Vec<u8>, Lr1121Error> {
        // Get RX buffer size
        let rx_byte = self.read_register(Register::RegRxNbBytes)?;
        let len = rx_byte as usize;

        self.cs.set_low();
        let mut cmd = vec![Opcode::ReadBuffer as u8, 0x00]; // Offset 0
        cmd.extend(vec![0u8; len]);
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        let mut response = vec![0u8; len + 2];
        self.spi
            .read(&mut response)
            .map_err(|e| Lr1121Error::SpiRead(e.to_string()))?;
        self.cs.set_high();

        Ok(response[2..].to_vec())
    }

    /// Read register
    fn read_register(&mut self, reg: Register) -> Result<u8, Lr1121Error> {
        self.cs.set_low();
        let cmd = [Opcode::ReadRegister as u8, reg as u8, 0x00];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        let mut response = [0u8; 2];
        self.spi
            .read(&mut response)
            .map_err(|e| Lr1121Error::SpiRead(e.to_string()))?;
        self.cs.set_high();
        Ok(response[1])
    }

    /// Get RSSI and SNR
    fn get_rssi_snr(&mut self) -> Result<(i16, i8), Lr1121Error> {
        let pkt_rssi = self.read_register(Register::RegPktRssiValue)? as i16;
        let pkt_snr = self.read_register(Register::RegPktSnrValue)? as i8;
        Ok((pkt_rssi - 137, pkt_snr)) // Convert to dBm
    }

    /// Wait for specific IRQ
    async fn wait_for_irq(&mut self, irq: Irq) -> Result<(), Lr1121Error> {
        loop {
            if self.dio1.read() == Level::High {
                let irq_status = self.get_irq_status()?;
                if irq_status & (irq as u16) != 0 {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Get IRQ status
    fn get_irq_status(&mut self) -> Result<u16, Lr1121Error> {
        self.cs.set_low();
        let cmd = [Opcode::GetIrqStatus as u8, 0x00, 0x00];
        self.spi
            .write(&cmd)
            .map_err(|e| Lr1121Error::SpiWrite(e.to_string()))?;
        let mut response = [0u8; 3];
        self.spi
            .read(&mut response)
            .map_err(|e| Lr1121Error::SpiRead(e.to_string()))?;
        self.cs.set_high();
        Ok(u16::from_be_bytes([response[1], response[2]]))
    }
}

/// Packet type
#[repr(u8)]
enum PacketType {
    LoRa = 0x01,
}

/// IRQ flags
#[repr(u16)]
enum Irq {
    TxDone = 0x0001,
    RxDone = 0x0002,
    PreambleDetected = 0x0004,
    SyncWordValid = 0x0008,
    HeaderValid = 0x0010,
    HeaderError = 0x0020,
    CrcError = 0x0040,
    CadDone = 0x0080,
    CadDetected = 0x0100,
    Timeout = 0x0200,
}

/// Additional registers
#[repr(u8)]
enum Register {
    RegRxNbBytes = 0x13,
    RegPktRssiValue = 0x1A,
    RegPktSnrValue = 0x19,
}

#[derive(Debug)]
pub enum Lr1121Error {
    SpiInit(String),
    SpiWrite(String),
    SpiRead(String),
    GpioInit(String),
    Timeout,
    InvalidResponse,
}

impl std::fmt::Display for Lr1121Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Lr1121Error::SpiInit(e) => write!(f, "SPI init failed: {}", e),
            Lr1121Error::SpiWrite(e) => write!(f, "SPI write failed: {}", e),
            Lr1121Error::SpiRead(e) => write!(f, "SPI read failed: {}", e),
            Lr1121Error::GpioInit(e) => write!(f, "GPIO init failed: {}", e),
            Lr1121Error::Timeout => write!(f, "Operation timeout"),
            Lr1121Error::InvalidResponse => write!(f, "Invalid response from LR1121"),
        }
    }
}

impl std::error::Error for Lr1121Error {}
