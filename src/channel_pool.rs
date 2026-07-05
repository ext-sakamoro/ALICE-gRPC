//! Channel pool (`ChannelPool`).

use crate::channel::Channel;

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
