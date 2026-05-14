//! HF/VHF protocol parsing and encoding with codec translation
//!
//! HF/VHF radio with codec translation for digital cellular transmission.

#[derive(Debug, Clone)]
pub struct HFVHFMessage {
    pub frequency_khz: u32,
    pub modulation: ModulationType,
    pub audio_samples: Vec<i16>, // PCM audio samples
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModulationType {
    AM,  // Amplitude Modulation
    FM,  // Frequency Modulation
    SSB, // Single Sideband
    CW,  // Continuous Wave (Morse code)
}

impl HFVHFMessage {
    /// Parse HF/VHF message from bytes (audio codec format)
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }

        // HF/VHF format (simplified):
        // Bytes 0-3: Frequency in kHz (big-endian)
        // Byte 4: Modulation type (0=AM, 1=FM, 2=SSB, 3=CW)
        // Bytes 5-1023: PCM audio samples (16-bit, little-endian)

        let frequency_khz = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let modulation = match data[4] {
            0 => ModulationType::AM,
            1 => ModulationType::FM,
            2 => ModulationType::SSB,
            3 => ModulationType::CW,
            _ => return None,
        };

        // Parse PCM audio samples (16-bit, little-endian)
        let mut audio_samples = Vec::new();
        for chunk in data[5..].chunks(2) {
            if chunk.len() == 2 {
                let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
                audio_samples.push(sample);
            }
        }

        Some(Self {
            frequency_khz,
            modulation,
            audio_samples,
        })
    }

    /// Encode HF/VHF message to bytes (digital codec format)
    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; 5 + self.audio_samples.len() * 2];

        // Frequency (bytes 0-3)
        buffer[0..4].copy_from_slice(&self.frequency_khz.to_be_bytes());

        // Modulation type (byte 4)
        buffer[4] = match self.modulation {
            ModulationType::AM => 0,
            ModulationType::FM => 1,
            ModulationType::SSB => 2,
            ModulationType::CW => 3,
        };

        // Audio samples (bytes 5+)
        for (i, sample) in self.audio_samples.iter().enumerate() {
            let offset = 5 + i * 2;
            buffer[offset..offset + 2].copy_from_slice(&sample.to_le_bytes());
        }

        buffer
    }

    /// Translate audio codec to digital format (simple PCM to digital conversion)
    pub fn codec_translate_to_digital(&self) -> Vec<u8> {
        // For HF/VHF, we convert analog audio samples to digital format
        // This is a simplified codec translation
        self.encode()
    }

    /// Resample audio to different sample rate (simplified)
    pub fn resample(&self, target_rate: u32, original_rate: u32) -> Self {
        let ratio = target_rate as f64 / original_rate as f64;
        let new_len = (self.audio_samples.len() as f64 * ratio) as usize;

        let mut new_samples = Vec::with_capacity(new_len);
        for i in 0..new_len {
            let src_idx = (i as f64 / ratio) as usize;
            if src_idx < self.audio_samples.len() {
                new_samples.push(self.audio_samples[src_idx]);
            }
        }

        Self {
            frequency_khz: self.frequency_khz,
            modulation: self.modulation,
            audio_samples: new_samples,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hfvhf_parse() {
        let mut data = vec![0u8; 20];
        data[0..4].copy_from_slice(&145800u32.to_be_bytes()); // 145.8 MHz
        data[4] = 1; // FM
        data[5..7].copy_from_slice(&1000i16.to_le_bytes());
        data[7..9].copy_from_slice(&2000i16.to_le_bytes());

        let msg = HFVHFMessage::parse(&data);
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(msg.frequency_khz, 145800);
        assert_eq!(msg.modulation, ModulationType::FM);
    }

    #[test]
    fn test_hfvhf_parse_too_short() {
        let data = b"short";
        let msg = HFVHFMessage::parse(data);
        assert!(msg.is_none());
    }

    #[test]
    fn test_hfvhf_encode() {
        let msg = HFVHFMessage {
            frequency_khz: 145800,
            modulation: ModulationType::FM,
            audio_samples: vec![1000, 2000, 3000],
        };

        let encoded = msg.encode();
        assert!(encoded.len() >= 5);
        assert_eq!(encoded[4], 1); // FM modulation
    }

    #[test]
    fn test_hfvhf_codec_translate() {
        let msg = HFVHFMessage {
            frequency_khz: 145800,
            modulation: ModulationType::FM,
            audio_samples: vec![1000, 2000, 3000],
        };

        let digital = msg.codec_translate_to_digital();
        assert!(digital.len() >= 5);
    }

    #[test]
    fn test_hfvhf_resample() {
        let msg = HFVHFMessage {
            frequency_khz: 145800,
            modulation: ModulationType::FM,
            audio_samples: vec![1000, 2000, 3000, 4000, 5000],
        };

        let resampled = msg.resample(16000, 8000);
        assert_eq!(resampled.audio_samples.len(), 10); // 2x sample rate
    }
}
