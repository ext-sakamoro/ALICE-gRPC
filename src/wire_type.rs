//! Protobuf wire types.

use crate::errors::DecodeError;

// Protobuf Wire Types
// ---------------------------------------------------------------------------

/// Protobuf wire types as defined in the protocol buffer specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WireType {
    Varint = 0,
    Fixed64 = 1,
    LengthDelimited = 2,
    StartGroup = 3,
    EndGroup = 4,
    Fixed32 = 5,
}

impl WireType {
    /// Create a `WireType` from its numeric value.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` if the value is not a valid wire type.
    pub const fn from_u8(val: u8) -> Result<Self, DecodeError> {
        match val {
            0 => Ok(Self::Varint),
            1 => Ok(Self::Fixed64),
            2 => Ok(Self::LengthDelimited),
            3 => Ok(Self::StartGroup),
            4 => Ok(Self::EndGroup),
            5 => Ok(Self::Fixed32),
            _ => Err(DecodeError::InvalidWireType(val)),
        }
    }
}
