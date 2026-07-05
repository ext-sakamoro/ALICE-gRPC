//! gRPC frame encoding / decoding (per gRPC-over-HTTP/2 spec).

use crate::errors::DecodeError;

// gRPC Frame Encoding / Decoding
// ---------------------------------------------------------------------------

/// Encode data into a gRPC frame (5-byte header: compressed flag + 4-byte length).
#[must_use]
pub fn encode_grpc_frame(data: &[u8], compressed: bool) -> Vec<u8> {
    let mut frame = Vec::with_capacity(5 + data.len());
    frame.push(u8::from(compressed));
    #[allow(clippy::cast_possible_truncation)]
    let len = data.len() as u32; // gRPC frames use u32 length
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(data);
    frame
}

/// Decode a gRPC frame, returning `(compressed, payload, bytes_consumed)`.
///
/// # Errors
///
/// Returns `DecodeError` if the buffer is too short.
pub fn decode_grpc_frame(buf: &[u8]) -> Result<(bool, Vec<u8>, usize), DecodeError> {
    if buf.len() < 5 {
        return Err(DecodeError::BufferTooShort);
    }
    let compressed = buf[0] != 0;
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize; // u32->usize is safe on 32+ bit
    if buf.len() < 5 + len {
        return Err(DecodeError::BufferTooShort);
    }
    let payload = buf[5..5 + len].to_vec();
    Ok((compressed, payload, 5 + len))
}
