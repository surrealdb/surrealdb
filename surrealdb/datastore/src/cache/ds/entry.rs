use std::sync::Arc;

use anyhow::Result;
#[cfg(feature = "jwks")]
use chrono::{DateTime, Utc};
#[cfg(feature = "jwks")]
use jsonwebtoken::jwk::JwkSet;

use super::catalog::TableSnapshot;
use super::function::FunctionSnapshot;
use crate::catalog;
use crate::index_shapes::TableIndexShapes;

/// A cached JWKS document together with the time it was stored.
#[cfg(feature = "jwks")]
#[derive(Debug)]
pub struct CachedJwks {
	pub jwks: JwkSet,
	pub time: DateTime<Utc>,
}

/// Datastore-level cache entries hold the **compiled** definition forms
/// (stored text already parsed back to ASTs), keyed by the owning table's
/// `cache_*_ts` stamps, so the per-record document pipeline never reparses
/// definition text between schema changes. The stored (text) forms are
/// cached per transaction (see `cache::tx`), not here.
///
/// The planner's entry is a table's definition, fields and indexes together,
/// keyed by the table's catalog version, which one read validates; see
/// [`TableSnapshot`].
#[derive(Clone, Debug)]
pub enum Entry {
	/// A cached JWKS document and the time it was stored
	#[cfg(feature = "jwks")]
	Jwk(Arc<CachedJwks>),
	/// One table's definition, fields and indexes, as of one catalog version.
	Catalog(Arc<TableSnapshot>),
	/// One function's compiled definition and body, under one content hash.
	Function(Arc<FunctionSnapshot>),
	/// The compiled field definitions specified on a table.
	Fds(Arc<[catalog::FieldDefinition]>),
	/// The compiled event definitions specified on a table.
	Evs(Arc<[catalog::EventDefinition]>),
	/// The compiled foreign (view) table definitions specified on a table.
	Fts(Arc<[catalog::TableDefinition]>),
	/// The compiled index definitions specified on a table.
	Ixs(Arc<[catalog::IndexDefinition]>),
	/// The compiled live-query subscriptions specified on a table.
	Lvs(Arc<[catalog::SubscriptionDefinition]>),
	/// The value shapes a table's online B-tree indexes have stored, as of one
	/// catalog version.
	Ish(TableIndexShapes),
}

impl Entry {
	/// Converts this cache entry into a JWKS payload and timestamp.
	/// This panics if called on a cache entry that is not an [`Entry::Jwk`].
	#[cfg(feature = "jwks")]
	pub fn try_into_jwk(self) -> Result<Arc<CachedJwks>> {
		match self {
			Entry::Jwk(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Jwk"),
		}
	}
	/// Converts this cache entry into a [`TableSnapshot`].
	/// Fails with a typed internal error if called on a cache entry that is not an
	/// [`Entry::Catalog`].
	pub fn try_into_catalog(self) -> Result<Arc<TableSnapshot>> {
		match self {
			Entry::Catalog(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Catalog"),
		}
	}
	/// Converts this cache entry into a [`FunctionSnapshot`].
	/// Fails with a typed internal error if called on a cache entry that is not an
	/// [`Entry::Function`].
	pub fn try_into_function(self) -> Result<Arc<FunctionSnapshot>> {
		match self {
			Entry::Function(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Function"),
		}
	}
	/// Converts this cache entry into a slice of [`catalog::FieldDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Fds`].
	pub fn try_into_fds(self) -> Result<Arc<[catalog::FieldDefinition]>> {
		match self {
			Entry::Fds(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Fds"),
		}
	}

	/// Converts this cache entry into a slice of [`catalog::EventDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Evs`].
	pub fn try_into_evs(self) -> Result<Arc<[catalog::EventDefinition]>> {
		match self {
			Entry::Evs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Evs"),
		}
	}

	/// Converts this cache entry into a slice of [`catalog::IndexDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Ixs`].
	pub fn try_into_ixs(self) -> Result<Arc<[catalog::IndexDefinition]>> {
		match self {
			Entry::Ixs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Ixs"),
		}
	}
	/// Converts this cache entry into a slice of [`catalog::TableDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Fts`].
	pub fn try_into_fts(self) -> Result<Arc<[catalog::TableDefinition]>> {
		match self {
			Entry::Fts(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Fts"),
		}
	}
	/// Converts this cache entry into a slice of [`catalog::SubscriptionDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Lvs`].
	pub fn try_into_lvs(self) -> Result<Arc<[catalog::SubscriptionDefinition]>> {
		match self {
			Entry::Lvs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Lvs"),
		}
	}
	/// Converts this cache entry into a [`TableIndexShapes`].
	/// Fails with a typed internal error if called on a cache entry that is not an
	/// [`Entry::Ish`].
	pub fn try_into_ish(self) -> Result<TableIndexShapes> {
		match self {
			Entry::Ish(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Ish"),
		}
	}
}
