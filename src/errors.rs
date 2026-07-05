//! `DecodeError` — protobuf / gRPC decode errors.

use std::fmt;

// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during protobuf decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    BufferTooShort,
    VarintTooLong,
    InvalidWireType(u8),
    InvalidFieldNumber,
    UnsupportedWireType,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooShort => write!(f, "buffer too short"),
            Self::VarintTooLong => write!(f, "varint too long"),
            Self::InvalidWireType(w) => write!(f, "invalid wire type: {w}"),
            Self::InvalidFieldNumber => write!(f, "invalid field number (0)"),
            Self::UnsupportedWireType => write!(f, "unsupported wire type (group)"),
        }
    }
}

impl std::error::Error for DecodeError {}
