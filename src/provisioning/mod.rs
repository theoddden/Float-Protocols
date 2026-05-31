//! Device Provisioning Subsystem
//!
//! Gateway provisioning via QR code scan or LoRa.
//! ATECC608B serial number → gateway ID.
//! X.509 CSR generation and certificate signing.

pub mod provision;
pub mod lora_provisioning;

pub use provision::{ProvisioningService, ProvisioningConfig};
pub use lora_provisioning::LoRaProvisioningMode;
