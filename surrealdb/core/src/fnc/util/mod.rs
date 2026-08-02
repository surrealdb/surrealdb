//! Helpers for the function families.
//!
//! The pure ones descended with the families they serve; these keep every
//! `fnc::util::math::…` path in core resolving. `http` is the exception that
//! stayed: it builds requests through the engine's client, so it belongs
//! beside the client.

pub use surrealdb_runtime::util::math;

#[cfg(feature = "http")]
pub mod http;
