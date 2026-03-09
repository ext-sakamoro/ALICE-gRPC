#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

//! ALICE-gRPC: Pure Rust gRPC framework
//!
//! Protobuf encoding/decoding, service definition, unary/streaming RPC,
//! metadata, status codes, and channel management.

use std::collections::HashMap;
use std::fmt;

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Protobuf Varint Encoding / Decoding
// ---------------------------------------------------------------------------

/// Encode a `u64` as a varint into the buffer.
pub fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a varint from the given byte slice, returning `(value, bytes_read)`.
///
/// # Errors
///
/// Returns `DecodeError` if the buffer is empty or the varint is too long.
pub fn decode_varint(buf: &[u8]) -> Result<(u64, usize), DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::BufferTooShort);
    }
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        if shift >= 70 {
            return Err(DecodeError::VarintTooLong);
        }
        value |= u64::from(byte & 0x7F) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(DecodeError::BufferTooShort)
}

/// Encode a signed 32-bit integer using zigzag encoding.
#[must_use]
pub const fn zigzag_encode_i32(value: i32) -> u32 {
    ((value << 1) ^ (value >> 31)).cast_unsigned()
}

/// Decode a zigzag-encoded 32-bit integer.
#[must_use]
pub const fn zigzag_decode_i32(value: u32) -> i32 {
    (value >> 1).cast_signed() ^ (-((value & 1).cast_signed()))
}

/// Encode a signed 64-bit integer using zigzag encoding.
#[must_use]
pub const fn zigzag_encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)).cast_unsigned()
}

/// Decode a zigzag-encoded 64-bit integer.
#[must_use]
pub const fn zigzag_decode_i64(value: u64) -> i64 {
    (value >> 1).cast_signed() ^ (-((value & 1).cast_signed()))
}

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Protobuf Message
// ---------------------------------------------------------------------------

/// A generic protobuf message consisting of tagged fields.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProtoMessage {
    fields: Vec<(FieldTag, FieldValue)>,
}

impl ProtoMessage {
    /// Create a new empty message.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a field to the message.
    pub fn add_field(&mut self, tag: FieldTag, value: FieldValue) {
        self.fields.push((tag, value));
    }

    /// Get all values for a given field number.
    #[must_use]
    pub fn get_field(&self, field_number: u32) -> Vec<&FieldValue> {
        self.fields
            .iter()
            .filter(|(tag, _)| tag.field_number == field_number)
            .map(|(_, v)| v)
            .collect()
    }

    /// Get the number of fields.
    #[must_use]
    pub const fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Encode the message to bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for (tag, value) in &self.fields {
            tag.encode(&mut buf);
            value.encode(&mut buf);
        }
        buf
    }

    /// Decode a message from bytes.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` if decoding fails.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let mut msg = Self::new();
        let mut offset = 0;
        while offset < buf.len() {
            let (tag, n) = FieldTag::decode(&buf[offset..])?;
            offset += n;
            let (value, m) = FieldValue::decode(tag.wire_type, &buf[offset..])?;
            offset += m;
            msg.add_field(tag, value);
        }
        Ok(msg)
    }

    /// Add a string field (`field_number`, string value).
    pub fn add_string(&mut self, field_number: u32, value: &str) {
        self.add_field(
            FieldTag::new(field_number, WireType::LengthDelimited),
            FieldValue::Bytes(value.as_bytes().to_vec()),
        );
    }

    /// Add a varint field.
    pub fn add_varint(&mut self, field_number: u32, value: u64) {
        self.add_field(
            FieldTag::new(field_number, WireType::Varint),
            FieldValue::Varint(value),
        );
    }

    /// Add a fixed32 field.
    pub fn add_fixed32(&mut self, field_number: u32, value: u32) {
        self.add_field(
            FieldTag::new(field_number, WireType::Fixed32),
            FieldValue::Fixed32(value),
        );
    }

    /// Add a fixed64 field.
    pub fn add_fixed64(&mut self, field_number: u32, value: u64) {
        self.add_field(
            FieldTag::new(field_number, WireType::Fixed64),
            FieldValue::Fixed64(value),
        );
    }

    /// Add a bytes field.
    pub fn add_bytes(&mut self, field_number: u32, value: Vec<u8>) {
        self.add_field(
            FieldTag::new(field_number, WireType::LengthDelimited),
            FieldValue::Bytes(value),
        );
    }

    /// Add a nested message field.
    pub fn add_message(&mut self, field_number: u32, msg: &Self) {
        self.add_bytes(field_number, msg.encode());
    }

    /// Add a signed int32 field using zigzag encoding.
    pub fn add_sint32(&mut self, field_number: u32, value: i32) {
        self.add_varint(field_number, u64::from(zigzag_encode_i32(value)));
    }

    /// Add a signed int64 field using zigzag encoding.
    pub fn add_sint64(&mut self, field_number: u32, value: i64) {
        self.add_varint(field_number, zigzag_encode_i64(value));
    }

    /// Check if the message has a given field.
    #[must_use]
    pub fn has_field(&self, field_number: u32) -> bool {
        self.fields
            .iter()
            .any(|(tag, _)| tag.field_number == field_number)
    }

    /// Clear all fields.
    pub fn clear(&mut self) {
        self.fields.clear();
    }

    /// Get all field numbers present in the message.
    #[must_use]
    pub fn field_numbers(&self) -> Vec<u32> {
        let mut nums: Vec<u32> = self.fields.iter().map(|(t, _)| t.field_number).collect();
        nums.sort_unstable();
        nums.dedup();
        nums
    }
}

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// gRPC Status Codes
// ---------------------------------------------------------------------------

/// Standard gRPC status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StatusCode {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl StatusCode {
    /// Create a status code from its numeric value.
    ///
    /// # Errors
    ///
    /// Returns `None` if the value is not a valid status code.
    #[must_use]
    pub const fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(Self::Ok),
            1 => Some(Self::Cancelled),
            2 => Some(Self::Unknown),
            3 => Some(Self::InvalidArgument),
            4 => Some(Self::DeadlineExceeded),
            5 => Some(Self::NotFound),
            6 => Some(Self::AlreadyExists),
            7 => Some(Self::PermissionDenied),
            8 => Some(Self::ResourceExhausted),
            9 => Some(Self::FailedPrecondition),
            10 => Some(Self::Aborted),
            11 => Some(Self::OutOfRange),
            12 => Some(Self::Unimplemented),
            13 => Some(Self::Internal),
            14 => Some(Self::Unavailable),
            15 => Some(Self::DataLoss),
            16 => Some(Self::Unauthenticated),
            _ => None,
        }
    }

    /// Get the human-readable name of the status code.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Cancelled => "CANCELLED",
            Self::Unknown => "UNKNOWN",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::Aborted => "ABORTED",
            Self::OutOfRange => "OUT_OF_RANGE",
            Self::Unimplemented => "UNIMPLEMENTED",
            Self::Internal => "INTERNAL",
            Self::Unavailable => "UNAVAILABLE",
            Self::DataLoss => "DATA_LOSS",
            Self::Unauthenticated => "UNAUTHENTICATED",
        }
    }

    /// Check if this status code represents success.
    #[must_use]
    pub const fn is_ok(self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Check if this status code represents an error.
    #[must_use]
    pub const fn is_error(self) -> bool {
        !self.is_ok()
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// gRPC metadata (headers/trailers) — a collection of key-value pairs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Metadata {
    entries: Vec<(String, MetadataValue)>,
}

/// A metadata value which can be text or binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataValue {
    Text(String),
    Binary(Vec<u8>),
}

impl MetadataValue {
    /// Check if this is a text value.
    #[must_use]
    pub const fn is_text(&self) -> bool {
        matches!(self, Self::Text(_))
    }

    /// Check if this is a binary value.
    #[must_use]
    pub const fn is_binary(&self) -> bool {
        matches!(self, Self::Binary(_))
    }

    /// Get the text value, if any.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            Self::Binary(_) => None,
        }
    }

    /// Get the binary value, if any.
    #[must_use]
    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(b) => Some(b),
            Self::Text(_) => None,
        }
    }
}

impl Metadata {
    /// Create empty metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a text metadata entry.
    pub fn insert_text(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.entries
            .push((key.into(), MetadataValue::Text(value.into())));
    }

    /// Insert a binary metadata entry (key must end with "-bin").
    pub fn insert_binary(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.entries
            .push((key.into(), MetadataValue::Binary(value)));
    }

    /// Get the first text value for a key.
    #[must_use]
    pub fn get_text(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, v)| v.as_text())
    }

    /// Get the first binary value for a key.
    #[must_use]
    pub fn get_binary(&self, key: &str) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, v)| v.as_binary())
    }

    /// Get all values for a key.
    #[must_use]
    pub fn get_all(&self, key: &str) -> Vec<&MetadataValue> {
        self.entries
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v)
            .collect()
    }

    /// Remove all entries with the given key.
    pub fn remove(&mut self, key: &str) {
        self.entries.retain(|(k, _)| k != key);
    }

    /// Check if a key exists.
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|(k, _)| k == key)
    }

    /// Get the number of entries.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if metadata is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all keys.
    #[must_use]
    pub fn keys(&self) -> Vec<&str> {
        self.entries.iter().map(|(k, _)| k.as_str()).collect()
    }

    /// Iterate over entries.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &MetadataValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Merge another metadata into this one.
    pub fn merge(&mut self, other: &Self) {
        for (k, v) in &other.entries {
            self.entries.push((k.clone(), v.clone()));
        }
    }

    /// Encode metadata to a protobuf message.
    #[must_use]
    pub fn to_proto(&self) -> ProtoMessage {
        let mut msg = ProtoMessage::new();
        for (key, value) in &self.entries {
            let mut entry = ProtoMessage::new();
            entry.add_string(1, key);
            match value {
                MetadataValue::Text(t) => entry.add_string(2, t),
                MetadataValue::Binary(b) => entry.add_bytes(3, b.clone()),
            }
            msg.add_message(1, &entry);
        }
        msg
    }
}

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Service & Method Definition
// ---------------------------------------------------------------------------

/// A gRPC method descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MethodDescriptor {
    pub name: String,
    pub full_path: String,
    pub method_type: MethodType,
    pub input_type: String,
    pub output_type: String,
}

impl MethodDescriptor {
    /// Create a new method descriptor.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        service_path: &str,
        method_type: MethodType,
        input_type: impl Into<String>,
        output_type: impl Into<String>,
    ) -> Self {
        let name = name.into();
        let full_path = format!("{service_path}/{name}");
        Self {
            name,
            full_path,
            method_type,
            input_type: input_type.into(),
            output_type: output_type.into(),
        }
    }
}

/// A gRPC service descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceDescriptor {
    pub name: String,
    pub package: String,
    pub methods: Vec<MethodDescriptor>,
}

impl ServiceDescriptor {
    /// Create a new service descriptor.
    #[must_use]
    pub fn new(name: impl Into<String>, package: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            package: package.into(),
            methods: Vec::new(),
        }
    }

    /// Get the full service path (e.g. "/package.ServiceName").
    #[must_use]
    pub fn full_path(&self) -> String {
        format!("/{}.{}", self.package, self.name)
    }

    /// Add a method to the service.
    pub fn add_method(
        &mut self,
        name: impl Into<String>,
        method_type: MethodType,
        input_type: impl Into<String>,
        output_type: impl Into<String>,
    ) {
        let method = MethodDescriptor::new(
            name,
            &self.full_path(),
            method_type,
            input_type,
            output_type,
        );
        self.methods.push(method);
    }

    /// Find a method by name.
    #[must_use]
    pub fn find_method(&self, name: &str) -> Option<&MethodDescriptor> {
        self.methods.iter().find(|m| m.name == name)
    }

    /// Get the number of methods.
    #[must_use]
    pub const fn method_count(&self) -> usize {
        self.methods.len()
    }
}

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Channel & Connection
// ---------------------------------------------------------------------------

/// Channel state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelState {
    Idle,
    Connecting,
    Ready,
    TransientFailure,
    Shutdown,
}

impl fmt::Display for ChannelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle => write!(f, "IDLE"),
            Self::Connecting => write!(f, "CONNECTING"),
            Self::Ready => write!(f, "READY"),
            Self::TransientFailure => write!(f, "TRANSIENT_FAILURE"),
            Self::Shutdown => write!(f, "SHUTDOWN"),
        }
    }
}

/// A gRPC channel representing a connection to a server.
#[derive(Debug, Clone)]
pub struct Channel {
    pub target: String,
    pub state: ChannelState,
    pub metadata: Metadata,
    pub authority: String,
    services: HashMap<String, ServiceDescriptor>,
}

impl Channel {
    /// Create a new channel targeting the given address.
    #[must_use]
    pub fn new(target: impl Into<String>) -> Self {
        let target = target.into();
        let authority = target.clone();
        Self {
            target,
            state: ChannelState::Idle,
            metadata: Metadata::new(),
            authority,
            services: HashMap::new(),
        }
    }

    /// Transition to connecting state.
    pub fn connect(&mut self) {
        if self.state != ChannelState::Shutdown {
            self.state = ChannelState::Connecting;
        }
    }

    /// Mark the channel as ready.
    pub fn set_ready(&mut self) {
        if self.state == ChannelState::Connecting {
            self.state = ChannelState::Ready;
        }
    }

    /// Mark a transient failure.
    pub fn set_transient_failure(&mut self) {
        if self.state != ChannelState::Shutdown {
            self.state = ChannelState::TransientFailure;
        }
    }

    /// Shut down the channel.
    pub const fn shutdown(&mut self) {
        self.state = ChannelState::Shutdown;
    }

    /// Check if the channel is ready.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self.state, ChannelState::Ready)
    }

    /// Check if the channel is shut down.
    #[must_use]
    pub const fn is_shutdown(&self) -> bool {
        matches!(self.state, ChannelState::Shutdown)
    }

    /// Register a service on this channel.
    pub fn register_service(&mut self, service: ServiceDescriptor) {
        self.services.insert(service.full_path(), service);
    }

    /// Look up a service by full path.
    #[must_use]
    pub fn find_service(&self, path: &str) -> Option<&ServiceDescriptor> {
        self.services.get(path)
    }

    /// Get the number of registered services.
    #[must_use]
    pub fn service_count(&self) -> usize {
        self.services.len()
    }

    /// Set default metadata for all requests on this channel.
    pub fn set_default_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
}

// ---------------------------------------------------------------------------
// Channel Pool
// ---------------------------------------------------------------------------

/// A pool of channels for load balancing.
#[derive(Debug, Default)]
pub struct ChannelPool {
    channels: Vec<Channel>,
    next_index: usize,
}

impl ChannelPool {
    /// Create a new empty channel pool.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a channel to the pool.
    pub fn add(&mut self, channel: Channel) {
        self.channels.push(channel);
    }

    /// Get the next available (ready) channel using round-robin.
    pub fn next_ready(&mut self) -> Option<&mut Channel> {
        let len = self.channels.len();
        if len == 0 {
            return None;
        }
        for _ in 0..len {
            let idx = self.next_index % len;
            self.next_index = self.next_index.wrapping_add(1);
            if self.channels[idx].is_ready() {
                return Some(&mut self.channels[idx]);
            }
        }
        None
    }

    /// Get the number of channels.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.channels.len()
    }

    /// Check if the pool is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// Get the number of ready channels.
    #[must_use]
    pub fn ready_count(&self) -> usize {
        self.channels.iter().filter(|c| c.is_ready()).count()
    }

    /// Shut down all channels.
    pub fn shutdown_all(&mut self) {
        for ch in &mut self.channels {
            ch.shutdown();
        }
    }
}

// ---------------------------------------------------------------------------
// Service Router
// ---------------------------------------------------------------------------

/// Routes incoming requests to registered method handlers.
#[derive(Debug, Default)]
pub struct ServiceRouter {
    services: Vec<ServiceDescriptor>,
}

impl ServiceRouter {
    /// Create a new empty router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a service.
    pub fn register(&mut self, service: ServiceDescriptor) {
        self.services.push(service);
    }

    /// Look up a method by its full path (e.g. "/package.Service/Method").
    #[must_use]
    pub fn resolve(&self, path: &str) -> Option<(&ServiceDescriptor, &MethodDescriptor)> {
        for svc in &self.services {
            for method in &svc.methods {
                if method.full_path == path {
                    return Some((svc, method));
                }
            }
        }
        None
    }

    /// Get all registered service paths.
    #[must_use]
    pub fn service_paths(&self) -> Vec<String> {
        self.services
            .iter()
            .map(ServiceDescriptor::full_path)
            .collect()
    }

    /// Get the total number of registered methods.
    #[must_use]
    pub fn method_count(&self) -> usize {
        self.services.iter().map(|s| s.methods.len()).sum()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Varint tests --

    #[test]
    fn test_varint_zero() {
        let mut buf = Vec::new();
        encode_varint(0, &mut buf);
        assert_eq!(buf, vec![0]);
        let (val, n) = decode_varint(&buf).unwrap();
        assert_eq!(val, 0);
        assert_eq!(n, 1);
    }

    #[test]
    fn test_varint_one() {
        let mut buf = Vec::new();
        encode_varint(1, &mut buf);
        assert_eq!(buf, vec![1]);
        let (val, n) = decode_varint(&buf).unwrap();
        assert_eq!(val, 1);
        assert_eq!(n, 1);
    }

    #[test]
    fn test_varint_127() {
        let mut buf = Vec::new();
        encode_varint(127, &mut buf);
        assert_eq!(buf, vec![0x7F]);
        let (val, _) = decode_varint(&buf).unwrap();
        assert_eq!(val, 127);
    }

    #[test]
    fn test_varint_128() {
        let mut buf = Vec::new();
        encode_varint(128, &mut buf);
        assert_eq!(buf, vec![0x80, 0x01]);
        let (val, n) = decode_varint(&buf).unwrap();
        assert_eq!(val, 128);
        assert_eq!(n, 2);
    }

    #[test]
    fn test_varint_300() {
        let mut buf = Vec::new();
        encode_varint(300, &mut buf);
        let (val, _) = decode_varint(&buf).unwrap();
        assert_eq!(val, 300);
    }

    #[test]
    fn test_varint_large() {
        let mut buf = Vec::new();
        let large = 0xFFFF_FFFF_FFFF_FFFF_u64;
        encode_varint(large, &mut buf);
        let (val, _) = decode_varint(&buf).unwrap();
        assert_eq!(val, large);
    }

    #[test]
    fn test_varint_max_u32() {
        let mut buf = Vec::new();
        encode_varint(u64::from(u32::MAX), &mut buf);
        let (val, _) = decode_varint(&buf).unwrap();
        assert_eq!(val, u64::from(u32::MAX));
    }

    #[test]
    fn test_varint_empty_buffer() {
        assert_eq!(decode_varint(&[]), Err(DecodeError::BufferTooShort));
    }

    #[test]
    fn test_varint_incomplete() {
        assert_eq!(decode_varint(&[0x80]), Err(DecodeError::BufferTooShort));
    }

    #[test]
    fn test_varint_powers_of_two() {
        for shift in 0..63 {
            let val = 1u64 << shift;
            let mut buf = Vec::new();
            encode_varint(val, &mut buf);
            let (decoded, _) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, val);
        }
    }

    // -- Zigzag tests --

    #[test]
    fn test_zigzag_i32_zero() {
        assert_eq!(zigzag_encode_i32(0), 0);
        assert_eq!(zigzag_decode_i32(0), 0);
    }

    #[test]
    fn test_zigzag_i32_positive() {
        assert_eq!(zigzag_encode_i32(1), 2);
        assert_eq!(zigzag_decode_i32(2), 1);
    }

    #[test]
    fn test_zigzag_i32_negative() {
        assert_eq!(zigzag_encode_i32(-1), 1);
        assert_eq!(zigzag_decode_i32(1), -1);
    }

    #[test]
    fn test_zigzag_i32_various() {
        let values = [0, 1, -1, 2, -2, 100, -100, i32::MAX, i32::MIN];
        for &v in &values {
            assert_eq!(zigzag_decode_i32(zigzag_encode_i32(v)), v);
        }
    }

    #[test]
    fn test_zigzag_i64_zero() {
        assert_eq!(zigzag_encode_i64(0), 0);
        assert_eq!(zigzag_decode_i64(0), 0);
    }

    #[test]
    fn test_zigzag_i64_positive() {
        assert_eq!(zigzag_encode_i64(1), 2);
        assert_eq!(zigzag_decode_i64(2), 1);
    }

    #[test]
    fn test_zigzag_i64_negative() {
        assert_eq!(zigzag_encode_i64(-1), 1);
        assert_eq!(zigzag_decode_i64(1), -1);
    }

    #[test]
    fn test_zigzag_i64_various() {
        let values = [0i64, 1, -1, 2, -2, 1000, -1000, i64::MAX, i64::MIN];
        for &v in &values {
            assert_eq!(zigzag_decode_i64(zigzag_encode_i64(v)), v);
        }
    }

    // -- Wire Type tests --

    #[test]
    fn test_wire_type_from_valid() {
        assert_eq!(WireType::from_u8(0).unwrap(), WireType::Varint);
        assert_eq!(WireType::from_u8(1).unwrap(), WireType::Fixed64);
        assert_eq!(WireType::from_u8(2).unwrap(), WireType::LengthDelimited);
        assert_eq!(WireType::from_u8(5).unwrap(), WireType::Fixed32);
    }

    #[test]
    fn test_wire_type_from_invalid() {
        assert_eq!(WireType::from_u8(6), Err(DecodeError::InvalidWireType(6)));
        assert_eq!(WireType::from_u8(7), Err(DecodeError::InvalidWireType(7)));
    }

    // -- Field Tag tests --

    #[test]
    fn test_field_tag_encode_decode() {
        let tag = FieldTag::new(1, WireType::Varint);
        let mut buf = Vec::new();
        tag.encode(&mut buf);
        let (decoded, _) = FieldTag::decode(&buf).unwrap();
        assert_eq!(decoded, tag);
    }

    #[test]
    fn test_field_tag_large_number() {
        let tag = FieldTag::new(536_870_911, WireType::LengthDelimited);
        let mut buf = Vec::new();
        tag.encode(&mut buf);
        let (decoded, _) = FieldTag::decode(&buf).unwrap();
        assert_eq!(decoded, tag);
    }

    #[test]
    fn test_field_tag_all_wire_types() {
        for wt in [
            WireType::Varint,
            WireType::Fixed64,
            WireType::LengthDelimited,
            WireType::Fixed32,
        ] {
            let tag = FieldTag::new(42, wt);
            let mut buf = Vec::new();
            tag.encode(&mut buf);
            let (decoded, _) = FieldTag::decode(&buf).unwrap();
            assert_eq!(decoded, tag);
        }
    }

    // -- Field Value tests --

    #[test]
    fn test_field_value_varint() {
        let val = FieldValue::Varint(42);
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let (decoded, _) = FieldValue::decode(WireType::Varint, &buf).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_field_value_fixed32() {
        let val = FieldValue::Fixed32(0x1234_5678);
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let (decoded, _) = FieldValue::decode(WireType::Fixed32, &buf).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_field_value_fixed64() {
        let val = FieldValue::Fixed64(0x1234_5678_9ABC_DEF0);
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let (decoded, _) = FieldValue::decode(WireType::Fixed64, &buf).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_field_value_bytes() {
        let val = FieldValue::Bytes(vec![1, 2, 3, 4, 5]);
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let (decoded, _) = FieldValue::decode(WireType::LengthDelimited, &buf).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_field_value_empty_bytes() {
        let val = FieldValue::Bytes(vec![]);
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let (decoded, _) = FieldValue::decode(WireType::LengthDelimited, &buf).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_field_value_group_unsupported() {
        assert_eq!(
            FieldValue::decode(WireType::StartGroup, &[]),
            Err(DecodeError::UnsupportedWireType)
        );
    }

    // -- ProtoMessage tests --

    #[test]
    fn test_proto_message_empty() {
        let msg = ProtoMessage::new();
        let encoded = msg.encode();
        assert!(encoded.is_empty());
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.field_count(), 0);
    }

    #[test]
    fn test_proto_message_single_varint() {
        let mut msg = ProtoMessage::new();
        msg.add_varint(1, 150);
        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.get_field(1), vec![&FieldValue::Varint(150)]);
    }

    #[test]
    fn test_proto_message_string() {
        let mut msg = ProtoMessage::new();
        msg.add_string(2, "hello");
        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        let vals = decoded.get_field(2);
        assert_eq!(vals.len(), 1);
        match vals[0] {
            FieldValue::Bytes(b) => assert_eq!(b, b"hello"),
            _ => panic!("expected bytes"),
        }
    }

    #[test]
    fn test_proto_message_multiple_fields() {
        let mut msg = ProtoMessage::new();
        msg.add_varint(1, 42);
        msg.add_string(2, "world");
        msg.add_fixed32(3, 99);
        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.field_count(), 3);
        assert_eq!(decoded.get_field(1), vec![&FieldValue::Varint(42)]);
    }

    #[test]
    fn test_proto_message_nested() {
        let mut inner = ProtoMessage::new();
        inner.add_varint(1, 100);
        let mut outer = ProtoMessage::new();
        outer.add_message(1, &inner);
        let encoded = outer.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        let inner_bytes = decoded.get_field(1);
        assert_eq!(inner_bytes.len(), 1);
    }

    #[test]
    fn test_proto_message_repeated_field() {
        let mut msg = ProtoMessage::new();
        msg.add_varint(1, 10);
        msg.add_varint(1, 20);
        msg.add_varint(1, 30);
        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        let vals = decoded.get_field(1);
        assert_eq!(vals.len(), 3);
    }

    #[test]
    fn test_proto_message_has_field() {
        let mut msg = ProtoMessage::new();
        msg.add_varint(1, 42);
        assert!(msg.has_field(1));
        assert!(!msg.has_field(2));
    }

    #[test]
    fn test_proto_message_clear() {
        let mut msg = ProtoMessage::new();
        msg.add_varint(1, 42);
        msg.clear();
        assert_eq!(msg.field_count(), 0);
    }

    #[test]
    fn test_proto_message_field_numbers() {
        let mut msg = ProtoMessage::new();
        msg.add_varint(3, 1);
        msg.add_varint(1, 2);
        msg.add_varint(2, 3);
        assert_eq!(msg.field_numbers(), vec![1, 2, 3]);
    }

    #[test]
    fn test_proto_message_sint32() {
        let mut msg = ProtoMessage::new();
        msg.add_sint32(1, -42);
        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        let vals = decoded.get_field(1);
        match vals[0] {
            FieldValue::Varint(v) => {
                assert_eq!(zigzag_decode_i32(*v as u32), -42);
            }
            _ => panic!("expected varint"),
        }
    }

    #[test]
    fn test_proto_message_sint64() {
        let mut msg = ProtoMessage::new();
        msg.add_sint64(1, -1000);
        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        let vals = decoded.get_field(1);
        match vals[0] {
            FieldValue::Varint(v) => {
                assert_eq!(zigzag_decode_i64(*v), -1000);
            }
            _ => panic!("expected varint"),
        }
    }

    #[test]
    fn test_proto_message_fixed64() {
        let mut msg = ProtoMessage::new();
        msg.add_fixed64(1, 0xDEAD_BEEF);
        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        assert_eq!(
            decoded.get_field(1),
            vec![&FieldValue::Fixed64(0xDEAD_BEEF)]
        );
    }

    #[test]
    fn test_proto_message_bytes() {
        let mut msg = ProtoMessage::new();
        msg.add_bytes(1, vec![0xFF, 0x00, 0xAB]);
        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        assert_eq!(
            decoded.get_field(1),
            vec![&FieldValue::Bytes(vec![0xFF, 0x00, 0xAB])]
        );
    }

    // -- Status Code tests --

    #[test]
    fn test_status_code_all_values() {
        for i in 0..=16u8 {
            let code = StatusCode::from_u8(i).unwrap();
            assert_eq!(code as u8, i);
        }
    }

    #[test]
    fn test_status_code_invalid() {
        assert!(StatusCode::from_u8(17).is_none());
        assert!(StatusCode::from_u8(255).is_none());
    }

    #[test]
    fn test_status_code_is_ok() {
        assert!(StatusCode::Ok.is_ok());
        assert!(!StatusCode::Internal.is_ok());
    }

    #[test]
    fn test_status_code_is_error() {
        assert!(!StatusCode::Ok.is_error());
        assert!(StatusCode::NotFound.is_error());
    }

    #[test]
    fn test_status_code_names() {
        assert_eq!(StatusCode::Ok.name(), "OK");
        assert_eq!(StatusCode::NotFound.name(), "NOT_FOUND");
        assert_eq!(StatusCode::Internal.name(), "INTERNAL");
        assert_eq!(StatusCode::Unauthenticated.name(), "UNAUTHENTICATED");
    }

    #[test]
    fn test_status_code_display() {
        assert_eq!(format!("{}", StatusCode::Ok), "OK");
        assert_eq!(format!("{}", StatusCode::Cancelled), "CANCELLED");
    }

    // -- GrpcStatus tests --

    #[test]
    fn test_grpc_status_ok() {
        let status = GrpcStatus::ok();
        assert!(status.is_ok());
        assert_eq!(status.code, StatusCode::Ok);
    }

    #[test]
    fn test_grpc_status_internal() {
        let status = GrpcStatus::internal("something broke");
        assert!(!status.is_ok());
        assert_eq!(status.message, "something broke");
    }

    #[test]
    fn test_grpc_status_not_found() {
        let status = GrpcStatus::not_found("resource missing");
        assert_eq!(status.code, StatusCode::NotFound);
    }

    #[test]
    fn test_grpc_status_invalid_argument() {
        let status = GrpcStatus::invalid_argument("bad input");
        assert_eq!(status.code, StatusCode::InvalidArgument);
    }

    #[test]
    fn test_grpc_status_unimplemented() {
        let status = GrpcStatus::unimplemented("not yet");
        assert_eq!(status.code, StatusCode::Unimplemented);
    }

    #[test]
    fn test_grpc_status_unauthenticated() {
        let status = GrpcStatus::unauthenticated("no token");
        assert_eq!(status.code, StatusCode::Unauthenticated);
    }

    #[test]
    fn test_grpc_status_permission_denied() {
        let status = GrpcStatus::permission_denied("forbidden");
        assert_eq!(status.code, StatusCode::PermissionDenied);
    }

    #[test]
    fn test_grpc_status_unavailable() {
        let status = GrpcStatus::unavailable("try later");
        assert_eq!(status.code, StatusCode::Unavailable);
    }

    #[test]
    fn test_grpc_status_cancelled() {
        let status = GrpcStatus::cancelled("user cancelled");
        assert_eq!(status.code, StatusCode::Cancelled);
    }

    #[test]
    fn test_grpc_status_display_with_message() {
        let status = GrpcStatus::internal("err");
        assert_eq!(format!("{status}"), "INTERNAL: err");
    }

    #[test]
    fn test_grpc_status_display_no_message() {
        let status = GrpcStatus::ok();
        assert_eq!(format!("{status}"), "OK");
    }

    #[test]
    fn test_grpc_status_to_from_proto() {
        let status = GrpcStatus::not_found("missing");
        let proto = status.to_proto();
        let encoded = proto.encode();
        let decoded_proto = ProtoMessage::decode(&encoded).unwrap();
        let decoded_status = GrpcStatus::from_proto(&decoded_proto).unwrap();
        assert_eq!(decoded_status, status);
    }

    // -- Metadata tests --

    #[test]
    fn test_metadata_empty() {
        let md = Metadata::new();
        assert!(md.is_empty());
        assert_eq!(md.len(), 0);
    }

    #[test]
    fn test_metadata_insert_text() {
        let mut md = Metadata::new();
        md.insert_text("key", "value");
        assert_eq!(md.get_text("key"), Some("value"));
    }

    #[test]
    fn test_metadata_insert_binary() {
        let mut md = Metadata::new();
        md.insert_binary("key-bin", vec![1, 2, 3]);
        assert_eq!(md.get_binary("key-bin"), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn test_metadata_contains_key() {
        let mut md = Metadata::new();
        md.insert_text("auth", "token");
        assert!(md.contains_key("auth"));
        assert!(!md.contains_key("missing"));
    }

    #[test]
    fn test_metadata_remove() {
        let mut md = Metadata::new();
        md.insert_text("key", "val");
        md.remove("key");
        assert!(!md.contains_key("key"));
    }

    #[test]
    fn test_metadata_get_all() {
        let mut md = Metadata::new();
        md.insert_text("key", "a");
        md.insert_text("key", "b");
        assert_eq!(md.get_all("key").len(), 2);
    }

    #[test]
    fn test_metadata_keys() {
        let mut md = Metadata::new();
        md.insert_text("a", "1");
        md.insert_text("b", "2");
        let keys = md.keys();
        assert!(keys.contains(&"a"));
        assert!(keys.contains(&"b"));
    }

    #[test]
    fn test_metadata_merge() {
        let mut md1 = Metadata::new();
        md1.insert_text("a", "1");
        let mut md2 = Metadata::new();
        md2.insert_text("b", "2");
        md1.merge(&md2);
        assert_eq!(md1.len(), 2);
    }

    #[test]
    fn test_metadata_iter() {
        let mut md = Metadata::new();
        md.insert_text("x", "y");
        let items: Vec<_> = md.iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, "x");
    }

    #[test]
    fn test_metadata_value_is_text() {
        let v = MetadataValue::Text("hello".into());
        assert!(v.is_text());
        assert!(!v.is_binary());
    }

    #[test]
    fn test_metadata_value_is_binary() {
        let v = MetadataValue::Binary(vec![1]);
        assert!(v.is_binary());
        assert!(!v.is_text());
    }

    #[test]
    fn test_metadata_value_as_text() {
        let v = MetadataValue::Text("hello".into());
        assert_eq!(v.as_text(), Some("hello"));
        let v2 = MetadataValue::Binary(vec![]);
        assert_eq!(v2.as_text(), None);
    }

    #[test]
    fn test_metadata_value_as_binary() {
        let v = MetadataValue::Binary(vec![1, 2]);
        assert_eq!(v.as_binary(), Some(&[1u8, 2][..]));
        let v2 = MetadataValue::Text("x".into());
        assert_eq!(v2.as_binary(), None);
    }

    #[test]
    fn test_metadata_to_proto() {
        let mut md = Metadata::new();
        md.insert_text("key", "val");
        let proto = md.to_proto();
        assert_eq!(proto.field_count(), 1);
    }

    // -- Method Type tests --

    #[test]
    fn test_method_type_unary() {
        assert!(!MethodType::Unary.is_client_streaming());
        assert!(!MethodType::Unary.is_server_streaming());
    }

    #[test]
    fn test_method_type_client_streaming() {
        assert!(MethodType::ClientStreaming.is_client_streaming());
        assert!(!MethodType::ClientStreaming.is_server_streaming());
    }

    #[test]
    fn test_method_type_server_streaming() {
        assert!(!MethodType::ServerStreaming.is_client_streaming());
        assert!(MethodType::ServerStreaming.is_server_streaming());
    }

    #[test]
    fn test_method_type_bidi_streaming() {
        assert!(MethodType::BidiStreaming.is_client_streaming());
        assert!(MethodType::BidiStreaming.is_server_streaming());
    }

    #[test]
    fn test_method_type_display() {
        assert_eq!(format!("{}", MethodType::Unary), "unary");
        assert_eq!(format!("{}", MethodType::BidiStreaming), "bidi_streaming");
    }

    // -- Service Descriptor tests --

    #[test]
    fn test_service_descriptor_new() {
        let svc = ServiceDescriptor::new("Greeter", "helloworld");
        assert_eq!(svc.full_path(), "/helloworld.Greeter");
    }

    #[test]
    fn test_service_descriptor_add_method() {
        let mut svc = ServiceDescriptor::new("Greeter", "helloworld");
        svc.add_method("SayHello", MethodType::Unary, "HelloRequest", "HelloReply");
        assert_eq!(svc.method_count(), 1);
    }

    #[test]
    fn test_service_descriptor_find_method() {
        let mut svc = ServiceDescriptor::new("Greeter", "helloworld");
        svc.add_method("SayHello", MethodType::Unary, "HelloRequest", "HelloReply");
        let method = svc.find_method("SayHello").unwrap();
        assert_eq!(method.method_type, MethodType::Unary);
        assert!(svc.find_method("NotExist").is_none());
    }

    #[test]
    fn test_method_descriptor_full_path() {
        let mut svc = ServiceDescriptor::new("Greeter", "helloworld");
        svc.add_method("SayHello", MethodType::Unary, "HelloRequest", "HelloReply");
        let method = svc.find_method("SayHello").unwrap();
        assert_eq!(method.full_path, "/helloworld.Greeter/SayHello");
    }

    // -- gRPC Frame tests --

    #[test]
    fn test_grpc_frame_encode_decode() {
        let data = b"hello grpc";
        let frame = encode_grpc_frame(data, false);
        let (compressed, payload, consumed) = decode_grpc_frame(&frame).unwrap();
        assert!(!compressed);
        assert_eq!(payload, data);
        assert_eq!(consumed, frame.len());
    }

    #[test]
    fn test_grpc_frame_compressed() {
        let data = b"compressed data";
        let frame = encode_grpc_frame(data, true);
        let (compressed, payload, _) = decode_grpc_frame(&frame).unwrap();
        assert!(compressed);
        assert_eq!(payload, data);
    }

    #[test]
    fn test_grpc_frame_empty() {
        let frame = encode_grpc_frame(&[], false);
        let (compressed, payload, consumed) = decode_grpc_frame(&frame).unwrap();
        assert!(!compressed);
        assert!(payload.is_empty());
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_grpc_frame_too_short() {
        assert_eq!(
            decode_grpc_frame(&[0, 0, 0]),
            Err(DecodeError::BufferTooShort)
        );
    }

    // -- Request / Response tests --

    #[test]
    fn test_request_new() {
        let req = Request::new("/pkg.Svc/Method", vec![1, 2, 3]);
        assert_eq!(req.method_path, "/pkg.Svc/Method");
        assert_eq!(req.payload, vec![1, 2, 3]);
    }

    #[test]
    fn test_request_metadata() {
        let mut req = Request::new("/pkg.Svc/Method", vec![]);
        req.add_metadata("auth", "bearer token");
        assert_eq!(req.metadata.get_text("auth"), Some("bearer token"));
    }

    #[test]
    fn test_request_set_metadata() {
        let mut req = Request::new("/test", vec![]);
        let mut md = Metadata::new();
        md.insert_text("k", "v");
        req.set_metadata(md);
        assert_eq!(req.metadata.get_text("k"), Some("v"));
    }

    #[test]
    fn test_request_to_grpc_frame() {
        let req = Request::new("/test", vec![0xAB]);
        let frame = req.to_grpc_frame();
        let (_, payload, _) = decode_grpc_frame(&frame).unwrap();
        assert_eq!(payload, vec![0xAB]);
    }

    #[test]
    fn test_response_ok() {
        let resp = Response::ok(vec![1, 2, 3]);
        assert!(resp.is_ok());
        assert_eq!(resp.payload, vec![1, 2, 3]);
    }

    #[test]
    fn test_response_error() {
        let resp = Response::error(GrpcStatus::internal("fail"));
        assert!(!resp.is_ok());
        assert!(resp.payload.is_empty());
    }

    #[test]
    fn test_response_to_grpc_frame() {
        let resp = Response::ok(vec![42]);
        let frame = resp.to_grpc_frame();
        let (_, payload, _) = decode_grpc_frame(&frame).unwrap();
        assert_eq!(payload, vec![42]);
    }

    // -- MessageStream tests --

    #[test]
    fn test_stream_new() {
        let stream = MessageStream::new();
        assert_eq!(stream.total(), 0);
        assert!(!stream.has_next());
    }

    #[test]
    fn test_stream_push_next() {
        let mut stream = MessageStream::new();
        stream.push(vec![1]);
        stream.push(vec![2]);
        assert_eq!(stream.total(), 2);
        assert_eq!(stream.remaining(), 2);
        assert_eq!(stream.next_message(), Some(vec![1]));
        assert_eq!(stream.remaining(), 1);
        assert_eq!(stream.next_message(), Some(vec![2]));
        assert_eq!(stream.next_message(), None);
    }

    #[test]
    fn test_stream_close() {
        let mut stream = MessageStream::new();
        assert!(!stream.is_closed());
        stream.close();
        assert!(stream.is_closed());
    }

    #[test]
    fn test_stream_reset() {
        let mut stream = MessageStream::new();
        stream.push(vec![1]);
        stream.next_message();
        assert!(!stream.has_next());
        stream.reset();
        assert!(stream.has_next());
    }

    #[test]
    fn test_stream_grpc_frames_roundtrip() {
        let mut stream = MessageStream::new();
        stream.push(vec![10, 20]);
        stream.push(vec![30, 40, 50]);
        let frames = stream.to_grpc_frames();
        let decoded = MessageStream::from_grpc_frames(&frames).unwrap();
        assert_eq!(decoded.total(), 2);
    }

    // -- Channel tests --

    #[test]
    fn test_channel_new() {
        let ch = Channel::new("localhost:50051");
        assert_eq!(ch.target, "localhost:50051");
        assert_eq!(ch.state, ChannelState::Idle);
    }

    #[test]
    fn test_channel_connect_ready() {
        let mut ch = Channel::new("localhost:50051");
        ch.connect();
        assert_eq!(ch.state, ChannelState::Connecting);
        ch.set_ready();
        assert!(ch.is_ready());
    }

    #[test]
    fn test_channel_transient_failure() {
        let mut ch = Channel::new("localhost:50051");
        ch.set_transient_failure();
        assert_eq!(ch.state, ChannelState::TransientFailure);
    }

    #[test]
    fn test_channel_shutdown() {
        let mut ch = Channel::new("localhost:50051");
        ch.shutdown();
        assert!(ch.is_shutdown());
        ch.connect(); // should not change state
        assert!(ch.is_shutdown());
    }

    #[test]
    fn test_channel_register_service() {
        let mut ch = Channel::new("localhost:50051");
        let svc = ServiceDescriptor::new("Greeter", "pkg");
        ch.register_service(svc);
        assert_eq!(ch.service_count(), 1);
        assert!(ch.find_service("/pkg.Greeter").is_some());
    }

    #[test]
    fn test_channel_default_metadata() {
        let mut ch = Channel::new("localhost:50051");
        let mut md = Metadata::new();
        md.insert_text("auth", "tok");
        ch.set_default_metadata(md);
        assert_eq!(ch.metadata.get_text("auth"), Some("tok"));
    }

    #[test]
    fn test_channel_state_display() {
        assert_eq!(format!("{}", ChannelState::Idle), "IDLE");
        assert_eq!(format!("{}", ChannelState::Ready), "READY");
        assert_eq!(format!("{}", ChannelState::Shutdown), "SHUTDOWN");
    }

    // -- ChannelPool tests --

    #[test]
    fn test_channel_pool_empty() {
        let mut pool = ChannelPool::new();
        assert!(pool.is_empty());
        assert!(pool.next_ready().is_none());
    }

    #[test]
    fn test_channel_pool_add() {
        let mut pool = ChannelPool::new();
        pool.add(Channel::new("a"));
        pool.add(Channel::new("b"));
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn test_channel_pool_next_ready() {
        let mut pool = ChannelPool::new();
        let mut ch1 = Channel::new("a");
        ch1.connect();
        ch1.set_ready();
        pool.add(ch1);
        let ch = pool.next_ready().unwrap();
        assert_eq!(ch.target, "a");
    }

    #[test]
    fn test_channel_pool_no_ready() {
        let mut pool = ChannelPool::new();
        pool.add(Channel::new("a"));
        assert!(pool.next_ready().is_none());
    }

    #[test]
    fn test_channel_pool_ready_count() {
        let mut pool = ChannelPool::new();
        let mut ch = Channel::new("a");
        ch.connect();
        ch.set_ready();
        pool.add(ch);
        pool.add(Channel::new("b"));
        assert_eq!(pool.ready_count(), 1);
    }

    #[test]
    fn test_channel_pool_shutdown_all() {
        let mut pool = ChannelPool::new();
        pool.add(Channel::new("a"));
        pool.add(Channel::new("b"));
        pool.shutdown_all();
        assert_eq!(pool.ready_count(), 0);
    }

    // -- ServiceRouter tests --

    #[test]
    fn test_router_new() {
        let router = ServiceRouter::new();
        assert_eq!(router.method_count(), 0);
    }

    #[test]
    fn test_router_register_resolve() {
        let mut router = ServiceRouter::new();
        let mut svc = ServiceDescriptor::new("Greeter", "helloworld");
        svc.add_method("SayHello", MethodType::Unary, "HelloRequest", "HelloReply");
        router.register(svc);
        let (_, method) = router.resolve("/helloworld.Greeter/SayHello").unwrap();
        assert_eq!(method.name, "SayHello");
    }

    #[test]
    fn test_router_resolve_not_found() {
        let router = ServiceRouter::new();
        assert!(router.resolve("/pkg.Svc/Method").is_none());
    }

    #[test]
    fn test_router_service_paths() {
        let mut router = ServiceRouter::new();
        router.register(ServiceDescriptor::new("A", "pkg"));
        router.register(ServiceDescriptor::new("B", "pkg"));
        let paths = router.service_paths();
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn test_router_method_count() {
        let mut router = ServiceRouter::new();
        let mut svc = ServiceDescriptor::new("Svc", "pkg");
        svc.add_method("M1", MethodType::Unary, "Req", "Res");
        svc.add_method("M2", MethodType::ServerStreaming, "Req", "Res");
        router.register(svc);
        assert_eq!(router.method_count(), 2);
    }

    // -- Decode Error tests --

    #[test]
    fn test_decode_error_display() {
        assert_eq!(
            format!("{}", DecodeError::BufferTooShort),
            "buffer too short"
        );
        assert_eq!(format!("{}", DecodeError::VarintTooLong), "varint too long");
        assert_eq!(
            format!("{}", DecodeError::InvalidWireType(99)),
            "invalid wire type: 99"
        );
        assert_eq!(
            format!("{}", DecodeError::InvalidFieldNumber),
            "invalid field number (0)"
        );
        assert_eq!(
            format!("{}", DecodeError::UnsupportedWireType),
            "unsupported wire type (group)"
        );
    }

    #[test]
    fn test_decode_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(DecodeError::BufferTooShort);
        assert_eq!(err.to_string(), "buffer too short");
    }

    // -- Integration / roundtrip tests --

    #[test]
    fn test_full_message_roundtrip() {
        let mut msg = ProtoMessage::new();
        msg.add_varint(1, 42);
        msg.add_string(2, "alice");
        msg.add_fixed32(3, 0xDEAD);
        msg.add_fixed64(4, 0xBEEF);
        msg.add_bytes(5, vec![0xCA, 0xFE]);
        msg.add_sint32(6, -99);
        msg.add_sint64(7, -999_999);

        let encoded = msg.encode();
        let decoded = ProtoMessage::decode(&encoded).unwrap();
        assert_eq!(decoded.field_count(), 7);
        assert_eq!(decoded.get_field(1), vec![&FieldValue::Varint(42)]);
    }

    #[test]
    fn test_grpc_request_response_roundtrip() {
        let mut inner_msg = ProtoMessage::new();
        inner_msg.add_string(1, "Hello");
        let payload = inner_msg.encode();

        let mut req = Request::new("/helloworld.Greeter/SayHello", payload.clone());
        req.add_metadata("authorization", "Bearer xyz");

        let frame = req.to_grpc_frame();
        let (_, decoded_payload, _) = decode_grpc_frame(&frame).unwrap();
        let decoded_msg = ProtoMessage::decode(&decoded_payload).unwrap();
        assert!(decoded_msg.has_field(1));

        let mut reply = ProtoMessage::new();
        reply.add_string(1, "Hello back!");
        let resp = Response::ok(reply.encode());
        assert!(resp.is_ok());
    }

    #[test]
    fn test_streaming_rpc_simulation() {
        let mut svc = ServiceDescriptor::new("ChatService", "chat");
        svc.add_method(
            "StreamChat",
            MethodType::BidiStreaming,
            "ChatMessage",
            "ChatMessage",
        );
        let method = svc.find_method("StreamChat").unwrap();
        assert!(method.method_type.is_client_streaming());
        assert!(method.method_type.is_server_streaming());

        let mut client_stream = MessageStream::new();
        for i in 0..5 {
            let mut msg = ProtoMessage::new();
            msg.add_string(1, &format!("msg_{i}"));
            client_stream.push(msg.encode());
        }

        let frames = client_stream.to_grpc_frames();
        let decoded = MessageStream::from_grpc_frames(&frames).unwrap();
        assert_eq!(decoded.total(), 5);
    }

    #[test]
    fn test_channel_full_lifecycle() {
        let mut ch = Channel::new("grpc.example.com:443");
        assert_eq!(ch.state, ChannelState::Idle);
        ch.connect();
        assert_eq!(ch.state, ChannelState::Connecting);
        ch.set_ready();
        assert_eq!(ch.state, ChannelState::Ready);

        let mut svc = ServiceDescriptor::new("TestSvc", "test");
        svc.add_method("DoStuff", MethodType::Unary, "Req", "Res");
        ch.register_service(svc);

        assert_eq!(ch.service_count(), 1);
        ch.shutdown();
        assert!(ch.is_shutdown());
    }
}
