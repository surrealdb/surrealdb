//! # SurrealDB KVS — any backend
//!
//! The single entry point for constructing a SurrealDB key-value store
//! backend: a [`Backends`] registry selects and constructs the right
//! datastore from a connection path string (e.g. `memory`, `surrealkv://path`),
//! exposing it behind the [`TransactionBuilder`] abstraction.
//! Each first-party backend is gated behind a matching `kv-*` cargo feature
//! and pre-registered by [`Backends::community`]; embedders can plug in
//! additional backends by registering their own [`BackendProvider`].
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

mod community;
pub mod provider;
mod registry;

#[cfg(feature = "kv-mem")]
pub use surrealdb_kvs_mem as mem;
#[cfg(feature = "kv-surrealkv")]
pub use surrealdb_kvs_surrealkv as surrealkv;

pub use crate::provider::{BackendProvider, ConnectContext};
pub use crate::registry::Backends;
