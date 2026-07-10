//! Common components for building Kubernetes operators
//!
//! This crate contains the core building blocks to allow users to build
//! controllers/operators/watchers that need to synchronize/reconcile kubernetes
//! state.
//!
//! Newcomers are recommended to start with the [`Controller`] builder, which gives an
//! opinionated starting point that should be appropriate for simple operators, but all
//! components are designed to be usable á la carte if your operator doesn't quite fit that mold.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
// Triggered by many derive macros (kube-derive, educe)
#![allow(clippy::default_trait_access)]
#![allow(clippy::type_repetition_in_bounds)]
// Triggered by educe derives on enums
#![allow(clippy::used_underscore_binding)]
// Triggered by Tokio macros
#![allow(clippy::semicolon_if_nothing_returned)]
// Triggered by nightly clippy on idiomatic code
#![allow(clippy::let_underscore_untyped)]

pub mod controller;
#[cfg_attr(docsrs, doc(cfg(feature = "events")))]
#[cfg(feature = "events")]
pub mod events;

#[cfg_attr(docsrs, doc(cfg(feature = "finalizer")))]
#[cfg(feature = "finalizer")]
pub mod finalizer;
pub mod reflector;
pub mod scheduler;
pub mod source;
pub mod utils;
#[cfg_attr(docsrs, doc(cfg(feature = "wait")))]
#[cfg(feature = "wait")]
pub mod wait;
pub mod watcher;

pub use controller::{Config, Controller, applier};
#[cfg(feature = "finalizer")] pub use finalizer::finalizer;
pub use reflector::reflector;
pub use scheduler::scheduler;
pub use source::{WatchSource, source_watcher};
pub use utils::WatchStreamExt;
#[cfg(feature = "client")]
#[allow(deprecated)]
pub use watcher::{metadata_watcher, watcher};

pub use utils::{Predicate, PredicateConfig, predicates};
#[cfg(feature = "wait")] pub use wait::conditions;
