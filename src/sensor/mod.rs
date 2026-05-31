//! Sensor Input Layer
//!
//! Direct sensor interfaces via M12 A-coded connector:
//! - RS-232: Carrier Micro-Link, Thermo King, Modbus RTU
//! - RS-485: Modbus RTU
//! - CAN Bus: J1939, CANopen

pub mod rs232;
pub mod rs485;
pub mod can;

pub use rs232::Rs232Reader;
pub use rs485::Rs485Reader;
pub use can::CanReader;
