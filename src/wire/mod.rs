//! Simple length-prefixed framing and helpers for TCP baseline.

use bytes::{BufMut, Bytes, BytesMut};

pub fn frame(topic: &str, payload: Bytes) -> Bytes {
    let topic_bytes = topic.as_bytes();
    let total_len = 4 + 2 + topic_bytes.len() + payload.len();
    let mut buf = BytesMut::with_capacity(total_len);
    buf.put_u32_le((2 + topic_bytes.len() + payload.len()) as u32);
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.extend_from_slice(topic_bytes);
    buf.extend_from_slice(&payload);
    buf.freeze()
}

pub fn parse(mut buf: Bytes) -> Option<(String, Bytes)> {
    if buf.len() < 6 { return None; }
    // First 4 bytes were total (excluding the 4), which we don't need here since we already have the buffer.
    let _ = buf.split_to(4); // drop len
    let topic_len_bytes = buf.split_to(2);
    let topic_len = u16::from_le_bytes([topic_len_bytes[0], topic_len_bytes[1]]) as usize;
    if buf.len() < topic_len { return None; }
    let topic_bytes = buf.split_to(topic_len);
    let topic = String::from_utf8(topic_bytes.to_vec()).ok()?;
    Some((topic, buf))
}
