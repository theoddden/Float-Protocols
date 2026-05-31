//! Sensor Input Layer
//!
//! Direct sensor interfaces via M12 A-coded connector:
//! - RS-232: Carrier Micro-Link, Thermo King, Modbus RTU
//! - RS-485: Modbus RTU
//! - CAN Bus: J1939, CANopen

pub mod can;
pub mod rs232;
pub mod rs485;

pub use can::CanReader;
pub use rs232::Rs232Reader;
pub use rs485::Rs485Reader;
