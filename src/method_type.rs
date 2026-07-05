//! RPC method types (`MethodType`).

use std::fmt;

// RPC Method Types
// ---------------------------------------------------------------------------

/// The type of an RPC method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MethodType {
    Unary,
    ClientStreaming,
    ServerStreaming,
    BidiStreaming,
}

impl MethodType {
    /// Check if the client sends a stream.
    #[must_use]
    pub const fn is_client_streaming(self) -> bool {
        matches!(self, Self::ClientStreaming | Self::BidiStreaming)
    }

    /// Check if the server sends a stream.
    #[must_use]
    pub const fn is_server_streaming(self) -> bool {
        matches!(self, Self::ServerStreaming | Self::BidiStreaming)
    }
}

impl fmt::Display for MethodType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unary => write!(f, "unary"),
            Self::ClientStreaming => write!(f, "client_streaming"),
            Self::ServerStreaming => write!(f, "server_streaming"),
            Self::BidiStreaming => write!(f, "bidi_streaming"),
        }
    }
}
