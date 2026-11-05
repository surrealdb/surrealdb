//! Host runtime for Surrealism WASM plugins.
//!
//! Runs on the SurrealDB server. Loads `.surli` packages, compiles them once,
//! and runs every invocation on a fresh instance from a pooling allocator, with
//! epoch-based timeouts.

// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

pub mod abi;
pub mod capabilities;
pub mod config;
pub mod controller;
pub mod dns;
pub mod epoch;
pub mod exports;
pub mod host;
mod http;
pub mod manifest;
pub mod net_allow;
pub mod package;
pub mod package_config;
pub mod runtime;
pub mod store;
mod wasi_context;

pub use net_allow::{NetDeny, NetPolicy, NetRule};
pub use surrealism_types::err::{HostCallError, HostCallResult, PrefixErr, SurrealismError};

/// The version of the Surrealism SDK that this runtime was compiled against.
/// Used by the build toolchain to verify that a module's `surrealism`
/// dependency matches before compilation.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");
