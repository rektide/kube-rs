//! Seam between [`k8s-openapi`] apimachinery types and lightweight local stand-ins.
//!
//! Everything in `kube-core` (and `kube-runtime`) that touches generated
//! Kubernetes types does so through this module. With the default
//! `k8s-openapi` feature enabled, these are re-exports of the real
//! [`k8s-openapi`] types and nothing changes. With
//! `--no-default-features`, structurally identical stand-ins are compiled
//! instead, so the generic machinery (traits, reflector caches, schedulers,
//! object references) can be used against non-Kubernetes sources without
//! pulling in the generated API bindings.
//!
//! The stand-ins mirror the field names, optionality, and serde renames of
//! their `k8s-openapi` counterparts (as of `k8s-openapi` 0.28), so code
//! written against this module compiles identically either way. They are
//! **not** wire-compatibility guarantees for talking to a real apiserver —
//! for that, use the `k8s-openapi` feature.
//!
//! [`k8s-openapi`]: https://docs.rs/k8s-openapi

#[cfg(feature = "k8s-openapi")]
pub use k8s_openapi::{
    ClusterResourceScope, NamespaceResourceScope, ResourceScope, SubResourceScope,
    api::core::v1::ObjectReference,
    apimachinery::pkg::apis::meta::v1::{
        FieldsV1, LabelSelector, LabelSelectorRequirement, ListMeta, ManagedFieldsEntry, ObjectMeta,
        OwnerReference, Time,
    },
};

#[cfg(not(feature = "k8s-openapi"))]
pub use shim::{
    ClusterResourceScope, FieldsV1, LabelSelector, LabelSelectorRequirement, ListMeta,
    ManagedFieldsEntry, NamespaceResourceScope, ObjectMeta, ObjectReference, OwnerReference,
    ResourceScope, SubResourceScope, Time,
};

#[cfg(not(feature = "k8s-openapi"))]
mod shim {
    #![allow(missing_docs)]

    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    /// The scope of a [`Resource`](crate::Resource). Mirrors `k8s_openapi::ResourceScope`.
    pub trait ResourceScope {}

    /// Indicates that a [`Resource`](crate::Resource) is cluster-scoped.
    pub struct ClusterResourceScope {}
    impl ResourceScope for ClusterResourceScope {}

    /// Indicates that a [`Resource`](crate::Resource) is namespace-scoped.
    pub struct NamespaceResourceScope {}
    impl ResourceScope for NamespaceResourceScope {}

    /// Indicates that a [`Resource`](crate::Resource) exists only as a subresource of another.
    pub struct SubResourceScope {}
    impl ResourceScope for SubResourceScope {}

    /// RFC 3339 timestamp. Stand-in for `k8s_openapi::apimachinery::pkg::apis::meta::v1::Time`,
    /// which is the same newtype over [`jiff::Timestamp`].
    #[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub struct Time(pub jiff::Timestamp);

    /// Arbitrary field-manager payload. Stand-in for
    /// `k8s_openapi::apimachinery::pkg::apis::meta::v1::FieldsV1`.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    pub struct FieldsV1(pub serde_json::Value);

    /// Stand-in for `k8s_openapi::apimachinery::pkg::apis::meta::v1::ManagedFieldsEntry`.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", default)]
    pub struct ManagedFieldsEntry {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub api_version: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub fields_type: Option<String>,
        #[serde(rename = "fieldsV1", skip_serializing_if = "Option::is_none")]
        pub fields_v1: Option<FieldsV1>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub manager: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub operation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub subresource: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub time: Option<Time>,
    }

    /// Stand-in for `k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference`.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", default)]
    pub struct OwnerReference {
        pub api_version: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub block_owner_deletion: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub controller: Option<bool>,
        pub kind: String,
        pub name: String,
        pub uid: String,
    }

    /// Stand-in for `k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta`,
    /// with the full 0.28 field set.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", default)]
    pub struct ObjectMeta {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub annotations: Option<BTreeMap<String, String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub creation_timestamp: Option<Time>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub deletion_grace_period_seconds: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub deletion_timestamp: Option<Time>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub finalizers: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub generate_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub generation: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub labels: Option<BTreeMap<String, String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub managed_fields: Option<Vec<ManagedFieldsEntry>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub namespace: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub owner_references: Option<Vec<OwnerReference>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub resource_version: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub self_link: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub uid: Option<String>,
    }

    /// Stand-in for `k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta`.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", default)]
    pub struct ListMeta {
        #[serde(rename = "continue", skip_serializing_if = "Option::is_none")]
        pub continue_: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub remaining_item_count: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub resource_version: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub self_link: Option<String>,
    }

    /// Stand-in for `k8s_openapi::api::core::v1::ObjectReference`.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", default)]
    pub struct ObjectReference {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub api_version: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub field_path: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub namespace: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub resource_version: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub uid: Option<String>,
    }

    /// Stand-in for `k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement`.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", default)]
    pub struct LabelSelectorRequirement {
        pub key: String,
        pub operator: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub values: Option<Vec<String>>,
    }

    /// Stand-in for `k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector`.
    #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", default)]
    pub struct LabelSelector {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub match_expressions: Option<Vec<LabelSelectorRequirement>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub match_labels: Option<BTreeMap<String, String>>,
    }
}
