//! Protobuf field values (`FieldValue` union).

use crate::errors::DecodeError;
use crate::varint::{decode_varint, encode_varint};
use crate::wire_type::WireType;

// Protobuf Field Values
// ---------------------------------------------------------------------------

/// A protobuf field value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldValue {
    Varint(u64),
    Fixed32(u32),
    Fixed64(u64),
    Bytes(Vec<u8>),
}

impl FieldValue {
    /// Encode this field value into the buffer.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Varint(v) => encode_varint(*v, buf),
            Self::Fixed32(v) => buf.extend_from_slice(&v.to_le_bytes()),
            Self::Fixed64(v) => buf.extend_from_slice(&v.to_le_bytes()),
            Self::Bytes(data) => {
                encode_varint(data.len() as u64, buf);
                buf.extend_from_slice(data);
            }
        }
    }

    /// Decode a field value given a wire type.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` if decoding fails.
    pub fn decode(wire_type: WireType, buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        match wire_type {
            WireType::Varint => {
                let (v, n) = decode_varint(buf)?;
                Ok((Self::Varint(v), n))
            }
            WireType::Fixed32 => {
                if buf.len() < 4 {
                    return Err(DecodeError::BufferTooShort);
                }
                let v = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
                Ok((Self::Fixed32(v), 4))
            }
            WireType::Fixed64 => {
                if buf.len() < 8 {
                    return Err(DecodeError::BufferTooShort);
                }
                let v = u64::from_le_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]);
                Ok((Self::Fixed64(v), 8))
            }
            WireType::LengthDelimited => {
                let (len, n) = decode_varint(buf)?;
                #[allow(clippy::cast_possible_truncation)]
                let len = len as usize; // length-delimited field size
                if buf.len() < n + len {
                    return Err(DecodeError::BufferTooShort);
                }
                let data = buf[n..n + len].to_vec();
                Ok((Self::Bytes(data), n + len))
            }
            WireType::StartGroup | WireType::EndGroup => Err(DecodeError::UnsupportedWireType),
        }
    }
}
