use std::sync::Arc;

use anyhow::Result;
#[cfg(feature = "jwks")]
use chrono::{DateTime, Utc};
#[cfg(feature = "jwks")]
use jsonwebtoken::jwk::JwkSet;
use uuid::Uuid;

use crate::catalog::{self};

/// A cached JWKS document together with the time it was stored.
#[cfg(feature = "jwks")]
#[derive(Debug)]
pub(crate) struct CachedJwks {
	pub(crate) jwks: JwkSet,
	pub(crate) time: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(crate) enum Entry {
	/// A cached JWKS document and the time it was stored
	#[cfg(feature = "jwks")]
	Jwk(Arc<CachedJwks>),
	/// A slice of FieldDefinition specified on a table.
	Fds(Arc<[catalog::FieldDefinition]>),
	/// A slice of DefineEventStatement specified on a table.
	Evs(Arc<[catalog::EventDefinition]>),
	/// A slice of TableDefinition specified on a table.
	Fts(Arc<[catalog::TableDefinition]>),
	/// A slice of DefineIndexStatement specified on a table.
	Ixs(Arc<[catalog::IndexDefinition]>),
	/// A slice of LiveStatement specified on a table.
	Lvs(Arc<[catalog::SubscriptionDefinition]>),
	/// An Uuid.
	Lvv(Uuid),
}

impl Entry {
	/// Converts this cache entry into a JWKS payload and timestamp.
	/// This panics if called on a cache entry that is not an [`Entry::Jwk`].
	#[cfg(feature = "jwks")]
	pub(crate) fn try_into_jwk(self) -> Result<Arc<CachedJwks>> {
		match self {
			Entry::Jwk(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Jwk"),
		}
	}
	/// Converts this cache entry into a slice of [`catalog::FieldDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Fds`].
	pub(crate) fn try_into_fds(self) -> Result<Arc<[catalog::FieldDefinition]>> {
		match self {
			Entry::Fds(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Fds"),
		}
	}

	/// Converts this cache entry into a slice of [`catalog::EventDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Evs`].
	pub(crate) fn try_into_evs(self) -> Result<Arc<[catalog::EventDefinition]>> {
		match self {
			Entry::Evs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Evs"),
		}
	}

	/// Converts this cache entry into a slice of [`catalog::IndexDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Ixs`].
	pub(crate) fn try_into_ixs(self) -> Result<Arc<[catalog::IndexDefinition]>> {
		match self {
			Entry::Ixs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Ixs"),
		}
	}
	/// Converts this cache entry into a slice of [`catalog::TableDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Fts`].
	pub(crate) fn try_into_fts(self) -> Result<Arc<[catalog::TableDefinition]>> {
		match self {
			Entry::Fts(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Fts"),
		}
	}
	/// Converts this cache entry into a slice of [`catalog::SubscriptionDefinition`].
	/// This panics if called on a cache entry that is not an [`Entry::Lvs`].
	pub(crate) fn try_into_lvs(self) -> Result<Arc<[catalog::SubscriptionDefinition]>> {
		match self {
			Entry::Lvs(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Lvs"),
		}
	}

	/// Converts this cache entry into a uuid.
	/// This panics if called on a cache entry that is not an [`Entry::Lvv`].
	pub(crate) fn try_info_lvv(self) -> Result<Uuid> {
		match self {
			Entry::Lvv(v) => Ok(v),
			_ => fail!("Unable to convert type into Entry::Lvv"),
		}
	}
}
