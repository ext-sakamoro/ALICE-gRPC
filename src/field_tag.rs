//! Protobuf field tag (field number + wire type).

use crate::errors::DecodeError;
use crate::varint::{decode_varint, encode_varint};
use crate::wire_type::WireType;

// Protobuf Field Tag
// ---------------------------------------------------------------------------

/// A protobuf field tag consisting of a field number and wire type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldTag {
    pub field_number: u32,
    pub wire_type: WireType,
}

impl FieldTag {
    /// Create a new field tag.
    #[must_use]
    pub const fn new(field_number: u32, wire_type: WireType) -> Self {
        Self {
            field_number,
            wire_type,
        }
    }

    /// Encode this field tag into the buffer.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let tag = (u64::from(self.field_number) << 3) | (self.wire_type as u64);
        encode_varint(tag, buf);
    }

    /// Decode a field tag from the buffer.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` if decoding fails.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (tag_val, n) = decode_varint(buf)?;
        let wire = (tag_val & 0x07) as u8;
        #[allow(clippy::cast_possible_truncation)]
        let field_number = (tag_val >> 3) as u32; // protobuf field numbers fit in u32
        if field_number == 0 {
            return Err(DecodeError::InvalidFieldNumber);
        }
        let wire_type = WireType::from_u8(wire)?;
        Ok((Self::new(field_number, wire_type), n))
    }
}
