//! RPC `Request` / `Response` envelope.

use crate::grpc_frame::encode_grpc_frame;
use crate::metadata::Metadata;
use crate::status::GrpcStatus;

// RPC Request / Response
// ---------------------------------------------------------------------------

/// A gRPC request.
#[derive(Debug, Clone)]
pub struct Request {
    pub method_path: String,
    pub metadata: Metadata,
    pub payload: Vec<u8>,
}

impl Request {
    /// Create a new request.
    #[must_use]
    pub fn new(method_path: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            method_path: method_path.into(),
            metadata: Metadata::new(),
            payload,
        }
    }

    /// Set metadata on the request.
    pub fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }

    /// Add a metadata entry.
    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert_text(key, value);
    }

    /// Encode the request to a gRPC frame.
    #[must_use]
    pub fn to_grpc_frame(&self) -> Vec<u8> {
        encode_grpc_frame(&self.payload, false)
    }
}

/// A gRPC response.
#[derive(Debug, Clone)]
pub struct Response {
    pub status: GrpcStatus,
    pub metadata: Metadata,
    pub payload: Vec<u8>,
}

impl Response {
    /// Create a successful response.
    #[must_use]
    pub fn ok(payload: Vec<u8>) -> Self {
        Self {
            status: GrpcStatus::ok(),
            metadata: Metadata::new(),
            payload,
        }
    }

    /// Create an error response.
    #[must_use]
    pub fn error(status: GrpcStatus) -> Self {
        Self {
            status,
            metadata: Metadata::new(),
            payload: Vec::new(),
        }
    }

    /// Check if the response is successful.
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        self.status.is_ok()
    }

    /// Encode the response payload to a gRPC frame.
    #[must_use]
    pub fn to_grpc_frame(&self) -> Vec<u8> {
        encode_grpc_frame(&self.payload, false)
    }
}
