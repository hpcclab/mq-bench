use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Message header with timestamp and sequence number
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    pub seq: u64,
    pub timestamp_ns: u64,
    pub payload_size: usize,
}

impl MessageHeader {
    pub fn new(seq: u64, payload_size: usize) -> Self {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        Self {
            seq,
            timestamp_ns,
            payload_size,
        }
    }

    /// Encode header as 24-byte prefix
    pub fn encode(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.seq.to_le_bytes());
        buf[8..16].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        buf[16..24].copy_from_slice(&self.payload_size.to_le_bytes());
        buf
    }

    /// Decode header from 24-byte prefix
    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < 24 {
            return Err("Buffer too short for header".to_string());
        }

        let seq = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let timestamp_ns = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        let payload_size = usize::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);

        Ok(Self {
            seq,
            timestamp_ns,
            payload_size,
        })
    }
}

/// Generate payload of specified size with header
pub fn generate_payload(seq: u64, size: usize) -> Vec<u8> {
    if size < 24 {
        panic!("Payload size must be at least 24 bytes for header");
    }

    let header = MessageHeader::new(seq, size);
    let header_bytes = header.encode();

    // Create payload with header + pattern fill
    let mut payload = Vec::with_capacity(size);
    payload.extend_from_slice(&header_bytes);

    // Fill remaining bytes with pattern
    let pattern = b"ZENOH_BENCH";
    let remaining = size - 24;
    for i in 0..remaining {
        payload.push(pattern[i % pattern.len()]);
    }

    payload
}

/// Parse header from received payload
pub fn parse_header(payload: &[u8]) -> Result<MessageHeader, String> {
    MessageHeader::decode(payload)
}
