//! Host runtime for Surrealism WASM plugins.
//!
//! Runs on the SurrealDB server. Loads `.surli` packages, manages the
//! controller pool, and executes guest functions with epoch-based timeouts.

pub mod capabilities;
pub mod config;
pub mod controller;
pub mod epoch;
pub mod exports;
pub mod host;
pub mod kv;
pub mod net_allow;
pub mod package;
pub mod runtime;
pub mod store;
mod wasi_context;

pub use net_allow::{ResolvedNetAllow, resolve_allow_net};
pub use surrealism_types::err::{PrefixErr, SurrealismError};

/// The version of the Surrealism SDK that this runtime was compiled against.
/// Used by the build toolchain to verify that a module's `surrealism`
/// dependency matches before compilation.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");
