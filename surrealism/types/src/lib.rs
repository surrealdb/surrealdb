//! # surrealism-types
//!
//! Shared types for the Surrealism WASM plugin system.
//!
//! This crate provides:
//!
//! - Function argument marshalling ([`Args`](args::Args)) for converting between typed tuples and
//!   vectors of [`surrealdb_types::Value`].
//! - Error types and utilities ([`SurrealismError`](err::SurrealismError),
//!   [`PrefixErr`](err::PrefixErr)) used across the runtime and guest SDK.

/// Traits for marshalling function arguments to and from [`surrealdb_types::Value`] vectors.
pub mod args;

/// Error handling utilities for prefixing errors with context.
pub mod err;
