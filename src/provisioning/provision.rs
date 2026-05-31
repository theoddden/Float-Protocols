//! Device Provisioning Service
//!
//! Gateway provisioning via QR code scan.
//! Reads ATECC608B serial, generates X.509 CSR, requests signed cert from Float CA.

use crate::hardware::atecc608b::ATECC608B;
use rcgen::{CertificateParams, DistinguishedName, KeyPair};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Provisioning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisioningConfig {
    pub gateway_id: String,
    pub site_id: String,
    pub lora_freq: u32,  // 915 or 868 MHz
    pub ca_endpoint: String,
}

impl Default for ProvisioningConfig {
    fn default() -> Self {
        Self {
            gateway_id: String::new(),
            site_id: "SITE-001".to_string(),
            lora_freq: 915,
            ca_endpoint: "https://api.floatgateway.com/v1/provision".to_string(),
        }
    }
}

/// Provisioning status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProvisioningStatus {
    NotProvisioned,
    Provisioning,
    Provisioned,
    Failed(String),
}

/// Provisioning service
pub struct ProvisioningService {
    atecc: ATECC608B,
    config: ProvisioningConfig,
    status: ProvisioningStatus,
}

impl ProvisioningService {
    /// Initialize provisioning service
    pub fn new(atecc: ATECC608B) -> Result<Self, ProvisioningError> {
        let serial = atecc.read_serial()?;
        let gateway_id = hex::encode(serial);

        let config = ProvisioningConfig {
            gateway_id,
            ..Default::default()
        };

        // Check if already provisioned
        let status = if Self::is_provisioned() {
            ProvisioningStatus::Provisioned
        } else {
            ProvisioningStatus::NotProvisioned
        };

        Ok(Self {
            atecc,
            config,
            status,
        })
    }

    /// Get gateway ID from ATECC608B serial
    pub fn gateway_id(&self) -> &str {
        &self.config.gateway_id
    }

    /// Generate X.509 CSR
    pub fn generate_csr(&self) -> Result<String, ProvisioningError> {
        let mut params = CertificateParams::default();

        // Set distinguished name
        let mut dn = DistinguishedName::new();
        dn.push(rcgen::DnType::CommonName, &self.config.gateway_id);
        dn.push(rcgen::DnType::OrganizationName, "Float Gateway");
        dn.push(rcgen::DnType::OrganizationalUnitName, "IoT Gateway");
        params.distinguished_name = dn;

        // Generate key pair
        let key_pair = KeyPair::generate()?;
        params.key_pair = Some(key_pair);

        // Generate CSR
        let csr = params.serialize_request()?;
        let csr_pem = csr.pem();

        Ok(csr_pem.to_string())
    }

    /// Send CSR to Float CA and receive signed certificate
    pub async fn request_certificate(&self, csr: &str) -> Result<String, ProvisioningError> {
        // TODO: Implement HTTP request to Float CA endpoint
        // For now, return a placeholder
        tracing::info!("Sending CSR to Float CA: {}", self.config.ca_endpoint);

        // In production:
        // let client = reqwest::Client::new();
        // let response = client
        //     .post(&self.config.ca_endpoint)
        //     .json(&serde_json::json!({
        //         "gateway_id": self.config.gateway_id,
        //         "csr": csr,
        //     }))
        //     .send()
        //     .await
        //     .map_err(|e| ProvisioningError::CaRequest(e.to_string()))?;

        // let cert: String = response
        //     .json()
        //     .await
        //     .map_err(|e| ProvisioningError::CaResponse(e.to_string()))?;

        Ok("PLACEHOLDER_CERTIFICATE".to_string())
    }

    /// Store signed certificate on NVMe config partition
    pub fn store_certificate(&self, cert: &str) -> Result<(), ProvisioningError> {
        let cert_path = "/mnt/encrypted_config/gateway_cert.pem";

        // Ensure config partition is mounted
        if !Path::new("/mnt/encrypted_config").exists() {
            return Err(ProvisioningError::ConfigNotMounted);
        }

        fs::write(cert_path, cert)
            .map_err(|e| ProvisioningError::FileWrite(e.to_string()))?;

        tracing::info!("Certificate stored at {}", cert_path);
        Ok(())
    }

    /// Store provisioning config
    pub fn store_config(&self) -> Result<(), ProvisioningError> {
        let config_path = "/mnt/encrypted_config/provisioning.json";

        if !Path::new("/mnt/encrypted_config").exists() {
            return Err(ProvisioningError::ConfigNotMounted);
        }

        let config_json = serde_json::to_string_pretty(&self.config)
            .map_err(|e| ProvisioningError::Serialization(e.to_string()))?;

        fs::write(config_path, config_json)
            .map_err(|e| ProvisioningError::FileWrite(e.to_string()))?;

        tracing::info!("Provisioning config stored at {}", config_path);
        Ok(())
    }

    /// Load provisioning config
    pub fn load_config() -> Result<ProvisioningConfig, ProvisioningError> {
        let config_path = "/mnt/encrypted_config/provisioning.json";

        if !Path::new(config_path).exists() {
            return Err(ProvisioningError::NotProvisioned);
        }

        let config_json = fs::read_to_string(config_path)
            .map_err(|e| ProvisioningError::FileRead(e.to_string()))?;

        let config: ProvisioningConfig = serde_json::from_str(&config_json)
            .map_err(|e| ProvisioningError::Deserialization(e.to_string()))?;

        Ok(config)
    }

    /// Check if gateway is provisioned
    fn is_provisioned() -> bool {
        Path::new("/mnt/encrypted_config/provisioning.json").exists()
            && Path::new("/mnt/encrypted_config/gateway_cert.pem").exists()
    }

    /// Get current status
    pub fn status(&self) -> ProvisioningStatus {
        self.status.clone()
    }

    /// Full provisioning flow
    pub async fn provision(&mut self) -> Result<(), ProvisioningError> {
        self.status = ProvisioningStatus::Provisioning;

        // Generate CSR
        let csr = self.generate_csr()?;

        // Request certificate from CA
        let cert = self.request_certificate(&csr).await?;

        // Store certificate
        self.store_certificate(&cert)?;

        // Store config
        self.store_config()?;

        self.status = ProvisioningStatus::Provisioned;
        tracing::info!("Gateway provisioned successfully");
        Ok(())
    }

    /// Factory reset (wipe provisioning data)
    pub fn factory_reset() -> Result<(), ProvisioningError> {
        let config_path = "/mnt/encrypted_config/provisioning.json";
        let cert_path = "/mnt/encrypted_config/gateway_cert.pem";

        if Path::new(config_path).exists() {
            fs::remove_file(config_path)
                .map_err(|e| ProvisioningError::FileDelete(e.to_string()))?;
        }

        if Path::new(cert_path).exists() {
            fs::remove_file(cert_path)
                .map_err(|e| ProvisioningError::FileDelete(e.to_string()))?;
        }

        tracing::info!("Factory reset complete");
        Ok(())
    }
}

#[derive(Debug)]
pub enum ProvisioningError {
    AteccError(String),
    Serialization(String),
    Deserialization(String),
    CaRequest(String),
    CaResponse(String),
    FileWrite(String),
    FileRead(String),
    FileDelete(String),
    ConfigNotMounted,
    NotProvisioned,
}

impl std::fmt::Display for ProvisioningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProvisioningError::AteccError(e) => write!(f, "ATECC608B error: {}", e),
            ProvisioningError::Serialization(e) => write!(f, "Serialization error: {}", e),
            ProvisioningError::Deserialization(e) => write!(f, "Deserialization error: {}", e),
            ProvisioningError::CaRequest(e) => write!(f, "CA request error: {}", e),
            ProvisioningError::CaResponse(e) => write!(f, "CA response error: {}", e),
            ProvisioningError::FileWrite(e) => write!(f, "File write error: {}", e),
            ProvisioningError::FileRead(e) => write!(f, "File read error: {}", e),
            ProvisioningError::FileDelete(e) => write!(f, "File delete error: {}", e),
            ProvisioningError::ConfigNotMounted => write!(f, "Config partition not mounted"),
            ProvisioningError::NotProvisioned => write!(f, "Gateway not provisioned"),
        }
    }
}

impl std::error::Error for ProvisioningError {}
