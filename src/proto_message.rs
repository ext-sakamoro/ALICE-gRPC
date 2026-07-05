//! Protobuf message container (`ProtoMessage`).

use crate::errors::DecodeError;
use crate::field_tag::FieldTag;
use crate::field_value::FieldValue;
use crate::varint::{zigzag_encode_i32, zigzag_encode_i64};
use crate::wire_type::WireType;

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
