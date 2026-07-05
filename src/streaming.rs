//! Streaming types (`MessageStream`).

use crate::errors::DecodeError;
use crate::grpc_frame::{decode_grpc_frame, encode_grpc_frame};

// Streaming Types
// ---------------------------------------------------------------------------

/// A stream of messages (used for streaming RPCs).
#[derive(Debug, Clone, Default)]
pub struct MessageStream {
    messages: Vec<Vec<u8>>,
    position: usize,
    closed: bool,
}

impl MessageStream {
    /// Create a new empty stream.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a message to the stream.
    pub fn push(&mut self, msg: Vec<u8>) {
        self.messages.push(msg);
    }

    /// Read the next message from the stream.
    pub fn next_message(&mut self) -> Option<Vec<u8>> {
        if self.position < self.messages.len() {
            let msg = self.messages[self.position].clone();
            self.position += 1;
            Some(msg)
        } else {
            None
        }
    }

    /// Check if there are more messages.
    #[must_use]
    pub const fn has_next(&self) -> bool {
        self.position < self.messages.len()
    }

    /// Get the number of remaining messages.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.messages.len() - self.position
    }

    /// Get total message count.
    #[must_use]
    pub const fn total(&self) -> usize {
        self.messages.len()
    }

    /// Close the stream.
    pub const fn close(&mut self) {
        self.closed = true;
    }

    /// Check if the stream is closed.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Reset the stream position to the beginning.
    pub const fn reset(&mut self) {
        self.position = 0;
    }

    /// Encode all messages as gRPC frames.
    #[must_use]
    pub fn to_grpc_frames(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for msg in &self.messages {
            buf.extend(encode_grpc_frame(msg, false));
        }
        buf
    }

    /// Decode multiple gRPC frames into a stream.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` if decoding fails.
    pub fn from_grpc_frames(mut buf: &[u8]) -> Result<Self, DecodeError> {
        let mut stream = Self::new();
        while !buf.is_empty() {
            let (_, payload, consumed) = decode_grpc_frame(buf)?;
            stream.push(payload);
            buf = &buf[consumed..];
        }
        Ok(stream)
    }
}
