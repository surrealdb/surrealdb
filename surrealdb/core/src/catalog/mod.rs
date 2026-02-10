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
mod database;
mod module;
mod namespace;
pub(crate) mod providers;
mod record;
mod schema;
mod subscription;
mod table;
mod view;

#[cfg(test)]
mod compat;
#[cfg(test)]
mod test;

pub(crate) use access::*;
pub(crate) use database::*;
pub(crate) use module::*;
pub(crate) use namespace::*;
pub(crate) use record::*;
pub use schema::ApiMethod;
pub(crate) use schema::{
	ApiDefinition, Distance, FullTextParams, HnswParams, Scoring, VectorType, *,
};
pub(crate) use subscription::*;
pub(crate) use table::*;
pub(crate) use view::*;
