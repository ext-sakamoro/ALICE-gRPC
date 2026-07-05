//! gRPC status (`GrpcStatus`).

use crate::field_value::FieldValue;
use crate::proto_message::ProtoMessage;
use crate::status_code::StatusCode;
use std::fmt;

// gRPC Status
// ---------------------------------------------------------------------------

/// A gRPC status consisting of a code and optional message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcStatus {
    pub code: StatusCode,
    pub message: String,
}

impl GrpcStatus {
    /// Create a new gRPC status.
    #[must_use]
    pub fn new(code: StatusCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Create an OK status.
    #[must_use]
    pub fn ok() -> Self {
        Self::new(StatusCode::Ok, "")
    }

    /// Create a cancelled status.
    #[must_use]
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Cancelled, message)
    }

    /// Create an internal error status.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Internal, message)
    }

    /// Create a not-found status.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NotFound, message)
    }

    /// Create an invalid argument status.
    #[must_use]
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(StatusCode::InvalidArgument, message)
    }

    /// Create an unimplemented status.
    #[must_use]
    pub fn unimplemented(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Unimplemented, message)
    }

    /// Create an unauthenticated status.
    #[must_use]
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Unauthenticated, message)
    }

    /// Create a permission denied status.
    #[must_use]
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(StatusCode::PermissionDenied, message)
    }

    /// Create an unavailable status.
    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Unavailable, message)
    }

    /// Check if the status is OK.
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        self.code.is_ok()
    }

    /// Encode the status to a protobuf message.
    #[must_use]
    pub fn to_proto(&self) -> ProtoMessage {
        let mut msg = ProtoMessage::new();
        msg.add_varint(1, self.code as u64);
        msg.add_string(2, &self.message);
        msg
    }

    /// Decode a status from a protobuf message.
    #[must_use]
    pub fn from_proto(msg: &ProtoMessage) -> Option<Self> {
        let code_vals = msg.get_field(1);
        let msg_vals = msg.get_field(2);
        let code = match code_vals.first() {
            #[allow(clippy::cast_possible_truncation)]
            Some(FieldValue::Varint(v)) => StatusCode::from_u8(*v as u8)?, // status codes fit in u8
            _ => return None,
        };
        let message = match msg_vals.first() {
            Some(FieldValue::Bytes(b)) => String::from_utf8(b.clone()).ok()?,
            _ => String::new(),
        };
        Some(Self { code, message })
    }
}

impl fmt::Display for GrpcStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "{}", self.code)
        } else {
            write!(f, "{}: {}", self.code, self.message)
        }
    }
}
