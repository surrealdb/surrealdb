//! SurrealDB Catalog definitions.
//!
//! The catalog is the collection of definitions (namespaces, databases, tables, fields, indexes,
//! etc) that are used to describe the state of the database.
//!
//! The catalog should be the only structs/enums that are stored physically in the KV Store.
#![warn(missing_docs)]

mod access;
pub(crate) mod aggregation;
mod auth;
mod compiled;
mod database;
mod error;
mod module;
mod namespace;
pub(crate) mod providers;
mod record;
mod schema;
mod subscription;
mod table;
mod task;
mod text;
mod view;

#[cfg(test)]
mod compat;
#[cfg(test)]
mod test;

pub(crate) use access::*;
pub(crate) use compiled::*;
pub(crate) use database::*;
pub(crate) use error::Error;
pub(crate) use module::*;
pub(crate) use namespace::*;
pub(crate) use record::*;
pub use schema::ApiMethod;
pub(crate) use schema::{
	DiskAnnParams, Distance, FullTextParams, HnswParams, Scoring, StoredApiDefinition, VectorType,
	*,
};
pub(crate) use subscription::*;
pub(crate) use table::*;
pub(crate) use task::*;
pub(crate) use text::*;
pub(crate) use view::*;
