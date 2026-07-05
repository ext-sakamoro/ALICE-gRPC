//! ALICE-gRPC: Pure Rust gRPC framework
//!
//! Protobuf encoding/decoding, service definition, unary/streaming RPC,
//! metadata, status codes, and channel management.

#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::wildcard_imports,
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::similar_names,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

pub mod channel;
pub mod channel_pool;
pub mod errors;
pub mod field_tag;
pub mod field_value;
pub mod grpc_frame;
pub mod metadata;
pub mod method_type;
pub mod prelude;
pub mod proto_message;
pub mod router;
pub mod rpc;
pub mod service;
pub mod status;
pub mod status_code;
pub mod streaming;
pub mod varint;
pub mod wire_type;

#[cfg(test)]
mod integration_tests;

// Backward-compat re-exports.
pub use crate::channel::*;
pub use crate::channel_pool::*;
pub use crate::errors::*;
pub use crate::field_tag::*;
pub use crate::field_value::*;
pub use crate::grpc_frame::*;
pub use crate::metadata::*;
pub use crate::method_type::*;
pub use crate::proto_message::*;
pub use crate::router::*;
pub use crate::rpc::*;
pub use crate::service::*;
pub use crate::status::*;
pub use crate::status_code::*;
pub use crate::streaming::*;
pub use crate::varint::*;
pub use crate::wire_type::*;
