//! Service & method descriptors.

use crate::method_type::MethodType;

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
