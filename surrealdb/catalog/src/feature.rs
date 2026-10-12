//! The store-wide feature level record.
//!
//! The record names the newest release whose features the store may use, and the newest release
//! whose features anything in the store has used. Binaries below a store's level write only what
//! the level's release reads.
//!
//! The record is the stored grammar at every feature level, so a binary reads it whatever the
//! level says, and it is `floor_fixed`: every release reads it, rewrites it and removes it.

use surrealdb_expr::val::stored::schema::{SchemaError, Stored};
use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{EncodeContext, Encoded, FeatureLevel, Release};

use crate::Error;
use crate::stored::{EntryError, StoredType, canonical, decode_entry, encode_stored};

/// The feature level record, stored under the root `!fl` key.
///
/// `FIRST_STORED <= in_use <= level`. A store with no record is at [`FeatureLevel::Legacy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0", floor_fixed, validate)]
pub struct StoredFeatureLevel {
	/// The newest release whose features the store may use.
	#[stored(n = 2)]
	level: Release,
	/// The newest release whose features anything in the store has used. It only rises.
	#[stored(n = 4)]
	in_use: Release,
}

impl StoredFeatureLevel {
	/// A record for a store at `level` whose newest feature in use is from `in_use`, or the
	/// refusal of a pair that breaks `FIRST_STORED <= in_use <= level`.
	pub fn try_new(level: Release, in_use: Release) -> Result<Self, Error> {
		let record = Self {
			level,
			in_use,
		};
		record.validate().map_err(|e| Error::Entry(EntryError::body(Self::SCHEMA.name, e)))?;
		Ok(record)
	}

	/// The newest release whose features the store may use.
	pub const fn level(&self) -> Release {
		self.level
	}

	/// The newest release whose features anything in the store has used.
	pub const fn in_use(&self) -> Release {
		self.in_use
	}

	/// The feature level the record sets.
	pub const fn feature_level(self) -> FeatureLevel {
		FeatureLevel::At(self.level)
	}

	/// Checks `FIRST_STORED <= in_use <= level`.
	#[allow(
		clippy::trivially_copy_pass_by_ref,
		reason = "the stored derive hands a validator the value it built by reference; the lint \
		          fires only where the record is no wider than the threshold the target sets"
	)]
	fn validate(&self) -> Result<(), SchemaError> {
		let ty = <Self as StoredType>::SCHEMA.name;
		if self.level < Release::FIRST_STORED {
			return Err(SchemaError::OutOfRange {
				ty,
				n: 2,
			});
		}
		if self.in_use < Release::FIRST_STORED || self.in_use > self.level {
			return Err(SchemaError::OutOfRange {
				ty,
				n: 4,
			});
		}
		Ok(())
	}
}

/// Written in the stored grammar at every level, never as `revision`.
///
/// `kv_encode_value_at` honours `ctx.prior`, so a raise or a lower keeps what a newer release
/// added to the record, and ignores `ctx.level`: the record does not depend on the level it sets.
impl KVValue for StoredFeatureLevel {
	type KeyContext = ();

	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		Ok(self.kv_encode_value_at(&EncodeContext::CURRENT)?.bytes)
	}

	fn kv_encode_value_at(&self, ctx: &EncodeContext<'_>) -> anyhow::Result<Encoded> {
		self.validate().map_err(|e| Error::Entry(EntryError::body(Self::SCHEMA.name, e)))?;
		let ctx = EncodeContext {
			level: FeatureLevel::At(Release::CURRENT),
			prior: ctx.prior,
		};
		Ok(encode_stored(self, &ctx).map_err(Error::Entry)?)
	}

	fn kv_canonical(&self) -> anyhow::Result<Vec<u8>> {
		self.validate().map_err(|e| Error::Entry(EntryError::body(Self::SCHEMA.name, e)))?;
		Ok(canonical(self).map_err(Error::Entry)?)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(decode_entry::<Self>(bytes).map_err(Error::Entry)?)
	}
}
