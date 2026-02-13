//! GraphQL support for SurrealDB.
//!
//! This module implements a dynamic GraphQL schema that is generated from the
//! database's table definitions, field types, and access methods. The schema is
//! regenerated whenever the database configuration changes and cached per
//! namespace/database/config combination.
//!
//! ## Architecture
//!
//! The GraphQL subsystem is split into layers:
//!
//! - **Schema generation** ([`schema`]) -- the main entry point that orchestrates building a
//!   complete `async_graphql::dynamic::Schema` from database metadata.
//! - **Table queries** ([`tables`]) -- generates Query root fields and Object types for each
//!   exposed table, including field resolvers, filter/order types, nested objects, and relation
//!   fields.
//! - **Mutations** ([`mutations`]) -- generates Mutation root fields (create, update, upsert,
//!   delete -- single and bulk) with corresponding input types.
//! - **Functions** ([`functions`]) -- exposes user-defined database functions as Query fields.
//! - **Authentication** ([`auth`]) -- generates `signIn` / `signUp` mutations from database access
//!   definitions.
//! - **Relations** ([`relations`]) -- discovers relation tables and provides data structures for
//!   relation field generation.
//! - **Caching** ([`cache`]) -- caches generated schemas keyed by namespace, database, and GraphQL
//!   configuration.
//! - **Error handling** ([`error`]) -- domain error type ([`GqlError`]) with helper constructors.
//! - **Utilities** ([`utils`], [`ext`]) -- shared helpers for value conversion and `async_graphql`
//!   extensions.
//!
//! The HTTP layer lives in `surrealdb-server::gql`, which wraps the schema in an
//! Axum service.
//!
//! ## WASM
//!
//! This module is excluded from WASM targets (`#![cfg(not(target_family = "wasm"))]`)
//! because `async_graphql` and the HTTP serving stack are not compatible with WASM.
#![cfg(not(target_family = "wasm"))]

mod auth;
pub mod cache;
pub mod error;
mod ext;
mod functions;
mod mutations;
mod relations;
pub mod schema;
mod tables;
mod utils;

pub use cache::*;
pub use error::GqlError;
