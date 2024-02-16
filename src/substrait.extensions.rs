#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimpleExtensionUri {
    /// A surrogate key used in the context of a single plan used to reference the
    /// URI associated with an extension.
    #[prost(uint32, tag = "1")]
    pub extension_uri_anchor: u32,
    /// The URI where this extension YAML can be retrieved. This is the "namespace"
    /// of this extension.
    #[prost(string, tag = "2")]
    pub uri: ::prost::alloc::string::String,
}
/// Describes a mapping between a specific extension entity and the uri where
/// that extension can be found.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimpleExtensionDeclaration {
    #[prost(oneof = "simple_extension_declaration::MappingType", tags = "1, 2, 3")]
    pub mapping_type: ::core::option::Option<simple_extension_declaration::MappingType>,
}
/// Nested message and enum types in `SimpleExtensionDeclaration`.
pub mod simple_extension_declaration {
    /// Describes a Type
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ExtensionType {
        /// references the extension_uri_anchor defined for a specific extension URI.
        #[prost(uint32, tag = "1")]
        pub extension_uri_reference: u32,
        /// A surrogate key used in the context of a single plan to reference a
        /// specific extension type
        #[prost(uint32, tag = "2")]
        pub type_anchor: u32,
        /// the name of the type in the defined extension YAML.
        #[prost(string, tag = "3")]
        pub name: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ExtensionTypeVariation {
        /// references the extension_uri_anchor defined for a specific extension URI.
        #[prost(uint32, tag = "1")]
        pub extension_uri_reference: u32,
        /// A surrogate key used in the context of a single plan to reference a
        /// specific type variation
        #[prost(uint32, tag = "2")]
        pub type_variation_anchor: u32,
        /// the name of the type in the defined extension YAML.
        #[prost(string, tag = "3")]
        pub name: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ExtensionFunction {
        /// references the extension_uri_anchor defined for a specific extension URI.
        #[prost(uint32, tag = "1")]
        pub extension_uri_reference: u32,
        /// A surrogate key used in the context of a single plan to reference a
        /// specific function
        #[prost(uint32, tag = "2")]
        pub function_anchor: u32,
        /// A function signature compound name
        #[prost(string, tag = "3")]
        pub name: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum MappingType {
        #[prost(message, tag = "1")]
        ExtensionType(ExtensionType),
        #[prost(message, tag = "2")]
        ExtensionTypeVariation(ExtensionTypeVariation),
        #[prost(message, tag = "3")]
        ExtensionFunction(ExtensionFunction),
    }
}
/// A generic object that can be used to embed additional extension information
/// into the serialized substrait plan.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AdvancedExtension {
    /// An optimization is helpful information that don't influence semantics. May
    /// be ignored by a consumer.
    #[prost(message, optional, tag = "1")]
    pub optimization: ::core::option::Option<::prost_types::Any>,
    /// An enhancement alter semantics. Cannot be ignored by a consumer.
    #[prost(message, optional, tag = "2")]
    pub enhancement: ::core::option::Option<::prost_types::Any>,
}