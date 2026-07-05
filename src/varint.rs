//! Protobuf varint + zigzag encode/decode.

use crate::errors::DecodeError;

// Protobuf Varint Encoding / Decoding
// ---------------------------------------------------------------------------

/// Encode a `u64` as a varint into the buffer.
pub fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a varint from the given byte slice, returning `(value, bytes_read)`.
///
/// # Errors
///
/// Returns `DecodeError` if the buffer is empty or the varint is too long.
pub fn decode_varint(buf: &[u8]) -> Result<(u64, usize), DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::BufferTooShort);
    }
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        if shift >= 70 {
            return Err(DecodeError::VarintTooLong);
        }
        value |= u64::from(byte & 0x7F) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(DecodeError::BufferTooShort)
}

/// Encode a signed 32-bit integer using zigzag encoding.
#[must_use]
pub const fn zigzag_encode_i32(value: i32) -> u32 {
    ((value << 1) ^ (value >> 31)).cast_unsigned()
}

/// Decode a zigzag-encoded 32-bit integer.
#[must_use]
pub const fn zigzag_decode_i32(value: u32) -> i32 {
    (value >> 1).cast_signed() ^ (-((value & 1).cast_signed()))
}

/// Encode a signed 64-bit integer using zigzag encoding.
#[must_use]
pub const fn zigzag_encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)).cast_unsigned()
}

/// Decode a zigzag-encoded 64-bit integer.
#[must_use]
pub const fn zigzag_decode_i64(value: u64) -> i64 {
    (value >> 1).cast_signed() ^ (-((value & 1).cast_signed()))
}
