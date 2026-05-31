//! Device Provisioning Subsystem
//!
//! Gateway provisioning via QR code scan or LoRa.
//! ATECC608B serial number → gateway ID.
//! X.509 CSR generation and certificate signing.

pub mod lora_provisioning;
pub mod provision;

pub use lora_provisioning::LoRaProvisioningMode;
pub use provision::{ProvisioningConfig, ProvisioningService};
