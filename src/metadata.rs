//! gRPC metadata (`Metadata` / `MetadataValue`).

use crate::proto_message::ProtoMessage;

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
