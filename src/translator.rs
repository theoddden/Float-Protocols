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

    /// Synchronous zero-allocation translation (no async overhead)
    /// Use this for the critical hot path when you have full control over the data flow.
    /// This function is completely synchronous and allocates no heap memory.
    ///
    /// # Example
    /// ```rust
    /// let mut buffer = [0u8; 2048];
    /// let size = translate_iridium_to_asts_sync(&iridium_data, &mut buffer)?;
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
        let cellular_data = Self::decode_inmarsat_c(&message.data)?;
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

    // Protocol-specific decoders (simplified for now)
    fn decode_iridium_sbd(data: &Bytes) -> Result<Bytes, TranslateError> {
        // TODO: Implement Iridium SBD protocol parsing
        Ok(data.clone())
    }

    fn decode_inmarsat_c(data: &Bytes) -> Result<Bytes, TranslateError> {
        let msg = InmarsatCMessage::parse(data).ok_or(TranslateError::InvalidProtocol)?;
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
        // TODO: Implement Samsara protocol parsing
        // Samsara uses JSON over HTTPS, for now pass through
        Ok(data.clone())
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
