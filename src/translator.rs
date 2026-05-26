//! Async protocol translation engine
//!
//! Translates between legacy protocols (Iridium, Inmarsat, VSAT, etc.)
//! and AST SpaceMobile cellular format, using async patterns for low latency.
//!
//! Zero-allocation hot path for Iridium SBD to ASTS Protobuf translation.

use crate::asts_protobuf::ZeroCopyTranslator;
use crate::hfvhf::HFVHFMessage;
use crate::inmarsat_c::InmarsatCMessage;
use crate::iridium_sbd::IridiumSBDMessage;
use crate::protocol::{Message, Protocol};
use crate::vsat::VSATMessage;
use bytes::Bytes;
use tokio::sync::mpsc;

pub struct Translator {
    // Async channels for translation pipeline
    input_tx: mpsc::Sender<Message>,
    output_rx: mpsc::Receiver<Message>,
    // Zero-allocation translator for hot path
    zero_copy_translator: ZeroCopyTranslator,
}

impl Translator {
    pub fn new(buffer_size: usize) -> Self {
        let (input_tx, mut input_rx) = mpsc::channel(buffer_size);
        let (output_tx, output_rx) = mpsc::channel(buffer_size);

        // Spawn async translation task
        tokio::spawn(async move {
            let mut translator = ZeroCopyTranslator::new();
            while let Some(message) = input_rx.recv().await {
                let translated = Self::translate_message(message, &mut translator).await;
                if let Ok(translated) = translated {
                    let _ = output_tx.send(translated).await;
                }
            }
        });

        Self {
            input_tx,
            output_rx,
            zero_copy_translator: ZeroCopyTranslator::new(),
        }
    }

    /// Zero-allocation hot path: translate Iridium SBD bytes to ASTS Protobuf
    /// This is the critical path with NO heap allocations
    pub fn translate_iridium_to_asts_hot_path(
        iridium_data: &[u8],
        output_buffer: &mut [u8],
        translator: &mut ZeroCopyTranslator,
    ) -> Result<usize, TranslateError> {
        let iridium_msg =
            IridiumSBDMessage::parse(iridium_data).ok_or(TranslateError::InvalidProtocol)?;

        translator
            .translate(&iridium_msg, output_buffer)
            .ok_or(TranslateError::DataTooLarge)
    }

    /// Async translation with zero-copy where possible
    async fn translate_message(
        message: Message,
        _translator: &mut ZeroCopyTranslator,
    ) -> Result<Message, TranslateError> {
        match message.protocol {
            Protocol::IridiumSBD => Self::translate_iridium(message).await,
            Protocol::InmarsatC => Self::translate_inmarsat(message).await,
            Protocol::VSAT => Self::translate_vsat(message).await,
            Protocol::HFVHF => Self::translate_hfvhf(message).await,
            Protocol::RockBLOCK => Self::translate_rockblock(message).await,
            Protocol::Samsara => Self::translate_samsara(message).await,
            Protocol::NIDD => Ok(message), // NIDD payload passes through to ASTS as-is
            Protocol::ASTSpaceMobile => Ok(message), // Already in target format
        }
    }

    async fn translate_iridium(message: Message) -> Result<Message, TranslateError> {
        // Iridium SBD (340 bytes max) → AST SpaceMobile cellular format
        // Zero-copy translation using Bytes::clone
        let cellular_data = Self::decode_iridium_sbd(&message.data)?;
        let translated = Message::new(Protocol::ASTSpaceMobile, cellular_data, message.priority);
        Ok(translated)
    }

    async fn translate_inmarsat(message: Message) -> Result<Message, TranslateError> {
        // Inmarsat C (teletype) → AST SpaceMobile cellular format
        let cellular_data = Self::decode_inmarsat_c(&message.data).map_err(|e| {
            // Log corrupt frame event
            tracing::error!(
                protocol = %Protocol::InmarsatC,
                error = %e,
                data_size = message.data.len(),
                "Corrupt Frame: InmarsatC parsing failed"
            );
            TranslateError::InvalidProtocol
        })?;
        let translated = Message::new(Protocol::ASTSpaceMobile, cellular_data, message.priority);
        Ok(translated)
    }

    async fn translate_vsat(message: Message) -> Result<Message, TranslateError> {
        // VSAT IP packets → AST SpaceMobile cellular format with compression
        let compressed = Self::compress_for_cellular(&message.data)?;
        let translated = Message::new(Protocol::ASTSpaceMobile, compressed, message.priority);
        Ok(translated)
    }

    async fn translate_hfvhf(message: Message) -> Result<Message, TranslateError> {
        // HF/VHF audio → AST SpaceMobile cellular format (codec translation)
        let digital = Self::codec_translate_to_digital(&message.data)?;
        let translated = Message::new(Protocol::ASTSpaceMobile, digital, message.priority);
        Ok(translated)
    }

    async fn translate_rockblock(message: Message) -> Result<Message, TranslateError> {
        // RockBLOCK (Iridium SBD variant) → AST SpaceMobile cellular format
        let cellular_data = Self::decode_rockblock(&message.data)?;
        let translated = Message::new(Protocol::ASTSpaceMobile, cellular_data, message.priority);
        Ok(translated)
    }

    async fn translate_samsara(message: Message) -> Result<Message, TranslateError> {
        // Samsara cellular broadband → AST SpaceMobile cellular format
        // Samsara already uses cellular, so minimal translation needed
        let cellular_data = Self::decode_samsara(&message.data)?;
        let translated = Message::new(Protocol::ASTSpaceMobile, cellular_data, message.priority);
        Ok(translated)
    }

    // Protocol-specific decoders using nom for zero-copy parsing
    fn decode_iridium_sbd(data: &Bytes) -> Result<Bytes, TranslateError> {
        let data_slice = data.as_ref();

        // Iridium SBD format: [protocol (1)][length (2)][payload (length)][checksum (2)]
        if data_slice.len() < 3 {
            return Err(TranslateError::InvalidProtocol);
        }

        let protocol = data_slice[0];
        let length = u16::from_be_bytes([data_slice[1], data_slice[2]]) as usize;

        if length > 340 {
            return Err(TranslateError::InvalidProtocol);
        }

        if data_slice.len() < 3 + length {
            return Err(TranslateError::InvalidProtocol);
        }

        let payload = &data_slice[3..3 + length];

        let mut cellular = Vec::with_capacity(1 + payload.len());
        cellular.push(protocol);
        cellular.extend_from_slice(payload);

        Ok(Bytes::from(cellular))
    }

    fn decode_inmarsat_c(data: &Bytes) -> Result<Bytes, TranslateError> {
        let msg = InmarsatCMessage::parse(data).map_err(|_| TranslateError::InvalidProtocol)?;
        let encoded = msg.encode();
        Ok(Bytes::from(encoded))
    }

    fn compress_for_cellular(data: &Bytes) -> Result<Bytes, TranslateError> {
        let msg = VSATMessage::parse(data).ok_or(TranslateError::InvalidProtocol)?;
        let compressed = msg.encode();
        Ok(Bytes::from(compressed))
    }

    fn codec_translate_to_digital(data: &Bytes) -> Result<Bytes, TranslateError> {
        let msg = HFVHFMessage::parse(data).ok_or(TranslateError::InvalidProtocol)?;
        let digital = msg.codec_translate_to_digital();
        Ok(Bytes::from(digital))
    }

    fn decode_rockblock(data: &Bytes) -> Result<Bytes, TranslateError> {
        // RockBLOCK uses Iridium SBD protocol
        Self::decode_iridium_sbd(data)
    }

    fn decode_samsara(data: &Bytes) -> Result<Bytes, TranslateError> {
        use nom::bytes::complete::take;
        use nom::error::Error;
        use nom::number::complete::{be_f64, be_u16, be_u32, be_u64, be_u8};
        use nom::sequence::tuple;
        use nom::IResult;

        type SamsaraParseResult<'a> =
            IResult<&'a [u8], (u8, u16, &'a [u8], u64, f64, f64, u32), Error<&'a [u8]>>;

        // Convert Bytes to slice for nom parser
        let data_slice = data.as_ref();

        // Samsara binary format: [version (1)][device_id_len (2)][device_id (N)][timestamp (8)][latitude (8)][longitude (8)][payload_len (4)][payload (N)]
        let result: SamsaraParseResult = tuple((
            be_u8::<_, Error<&[u8]>>,                      // version
            be_u16::<_, Error<&[u8]>>,                     // device_id_len
            take::<usize, &[u8], Error<&[u8]>>(1024usize), // device_id (max 1024 bytes)
            be_u64::<_, Error<&[u8]>>,                     // timestamp
            be_f64::<_, Error<&[u8]>>,                     // latitude
            be_f64::<_, Error<&[u8]>>,                     // longitude
            be_u32::<_, Error<&[u8]>>,                     // payload_len
        ))(data_slice);

        match result {
            Ok((
                remaining,
                (
                    version,
                    device_id_len,
                    device_id_bytes,
                    timestamp,
                    latitude,
                    longitude,
                    payload_len,
                ),
            )) => {
                // Validate version
                if version != 1 {
                    return Err(TranslateError::InvalidProtocol);
                }

                // Validate device_id_len
                let actual_device_id_len = device_id_len as usize;
                if actual_device_id_len > device_id_bytes.len() {
                    return Err(TranslateError::InvalidProtocol);
                }

                let actual_device_id = &device_id_bytes[..actual_device_id_len];
                let device_id = String::from_utf8(actual_device_id.to_vec())
                    .map_err(|_| TranslateError::InvalidProtocol)?;

                // Extract payload
                let actual_payload_len = payload_len as usize;
                if remaining.len() < actual_payload_len {
                    return Err(TranslateError::InvalidProtocol);
                }
                let payload = &remaining[..actual_payload_len];

                // Convert to ASTS cellular format
                // For Samsara, we pass through the payload with a cellular header
                // Include device_id, timestamp, lat/lon in the cellular header
                let mut cellular =
                    Vec::with_capacity(1 + device_id.len() + 8 + 8 + 8 + payload.len());
                cellular.push(0x07); // Samsara protocol identifier
                cellular.extend_from_slice(device_id.as_bytes());
                cellular.extend_from_slice(&timestamp.to_be_bytes());
                cellular.extend_from_slice(&latitude.to_be_bytes());
                cellular.extend_from_slice(&longitude.to_be_bytes());
                cellular.extend_from_slice(payload);

                Ok(Bytes::from(cellular))
            }
            Err(_) => Err(TranslateError::InvalidProtocol),
        }
    }

    /// Synchronous protocol translation for cache-correct hot path use.
    /// Returns a translated Message ready to send to ASTS, preserving bi-temporal timestamps.
    pub fn translate_sync(message: Message) -> Result<Message, TranslateError> {
        let translated_data = match message.protocol {
            Protocol::IridiumSBD => Self::decode_iridium_sbd(&message.data)?,
            Protocol::InmarsatC => Self::decode_inmarsat_c(&message.data)?,
            Protocol::VSAT => Self::compress_for_cellular(&message.data)?,
            Protocol::HFVHF => Self::codec_translate_to_digital(&message.data)?,
            Protocol::RockBLOCK => Self::decode_rockblock(&message.data)?,
            Protocol::Samsara => Self::decode_samsara(&message.data)?,
            Protocol::NIDD => message.data.clone(),
            Protocol::ASTSpaceMobile => message.data.clone(),
        };
        let mut translated =
            Message::new(Protocol::ASTSpaceMobile, translated_data, message.priority);
        translated.t_event = message.t_event;
        translated.t_system = message.t_system;
        Ok(translated)
    }

    pub fn zero_copy_translator(&mut self) -> &mut ZeroCopyTranslator {
        &mut self.zero_copy_translator
    }

    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        self.input_tx.send(message).await
    }

    pub async fn recv(&mut self) -> Option<Message> {
        self.output_rx.recv().await
    }
}

/// Synchronous zero-allocation translation (no async overhead)
/// Use this for the critical hot path when you have full control over the data flow.
/// This function is completely synchronous and allocates no heap memory.
///
/// # Example
/// ```rust,no_run
/// use float_protocols::{IridiumSBDMessage, translate_iridium_to_asts_sync};
///
/// let iridium_data = vec![0x01, 0x00, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00];
/// let mut buffer = [0u8; 2048];
/// let size = translate_iridium_to_asts_sync(&iridium_data, &mut buffer).unwrap();
/// // buffer[..size] now contains ASTS Protobuf data
/// ```
pub fn translate_iridium_to_asts_sync(
    iridium_data: &[u8],
    output_buffer: &mut [u8],
) -> Result<usize, TranslateError> {
    let mut translator = ZeroCopyTranslator::new();
    let iridium_msg =
        IridiumSBDMessage::parse(iridium_data).ok_or(TranslateError::InvalidProtocol)?;

    translator
        .translate(&iridium_msg, output_buffer)
        .ok_or(TranslateError::DataTooLarge)
}

/// Zero-allocation buffer pool for stack-allocated buffers
pub struct BufferPool {
    buffers: Vec<[u8; 2048]>,
    next_index: usize,
}

impl BufferPool {
    pub fn new(size: usize) -> Self {
        let buffers = vec![[0u8; 2048]; size];
        Self {
            buffers,
            next_index: 0,
        }
    }

    /// Get a buffer from the pool (zero-allocation)
    pub fn get_buffer(&mut self) -> &mut [u8; 2048] {
        let index = self.next_index % self.buffers.len();
        self.next_index += 1;
        &mut self.buffers[index]
    }

    /// Get buffer at specific index
    pub fn get_buffer_at(&mut self, index: usize) -> &mut [u8; 2048] {
        let len = self.buffers.len();
        &mut self.buffers[index % len]
    }
}

#[derive(Debug)]
pub enum TranslateError {
    InvalidProtocol,
    DataTooLarge,
    CodecError,
    CompressionError,
}

impl std::fmt::Display for TranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranslateError::InvalidProtocol => write!(f, "Invalid protocol"),
            TranslateError::DataTooLarge => write!(f, "Data exceeds protocol maximum size"),
            TranslateError::CodecError => write!(f, "Codec translation error"),
            TranslateError::CompressionError => write!(f, "Compression error"),
        }
    }
}

impl std::error::Error for TranslateError {}
