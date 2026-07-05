//! Service router (`ServiceRouter`).

use crate::service::{MethodDescriptor, ServiceDescriptor};

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
