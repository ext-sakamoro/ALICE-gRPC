//! Channel & connection (`ChannelState` / `Channel`).

use crate::metadata::Metadata;
use crate::service::ServiceDescriptor;
use std::collections::HashMap;
use std::fmt;

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
