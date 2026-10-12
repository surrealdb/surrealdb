//! The store's feature level, as a transaction sees it.
//!
//! The durable record is [`StoredFeatureLevel`] under the root `!fl` key. Every transaction a
//! [`crate::TransactionFactory`] opens shares one [`FeatureLevelCell`], which holds what the
//! datastore last read of that record, so the hot paths (which level to encode at, whether a
//! feature has been used before) cost one atomic load.
//!
//! The cell is filled from durable values only, and it may lag the record in either direction: a
//! raise by another node leaves it lower, and a lower by another node, or a refresh that lands out
//! of order, leaves it higher. Its `in_use` only rises, as the durable one does, so it is never
//! above the record's. Correctness does not depend on the cell being current:
//!
//! - a write is admitted only by [`crate::Transaction::admit_floor`], whose fast path needs the
//!   floor to be at most the cell's `in_use`, which is at most the stored level, and which
//!   otherwise reads the record under a locked read and refuses a floor above the stored level;
//! - lowering the level is refused while `in_use` exceeds the target;
//! - a `Legacy` cell is repaired when a levelled write meets a stored-grammar entry (see
//!   [`crate::Transaction::write_level`]).
//!
//! A write encoded at a level the cell remembers and above the stored one is refused with
//! [`crate::DatastoreError::FeatureNotEnabled`] after the refusal brings the cell down, and the
//! levelled writers encode once more at the refreshed level. The heartbeat refresh only bounds how
//! long the cell lags.
//!
//! The cell also remembers whether the store records the stored-format ledger entry. That flag is
//! set only from a committed observation (see [`crate::Transaction::admit_stored_format`]), never
//! from a write that may still roll back, and it is never cleared because the entry is never
//! removed. A transaction that finds the flag set may skip its own check of the entry.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use surrealdb_catalog::StoredFeatureLevel;
use surrealdb_kvs::{FeatureLevel, Release};

/// What a store has recorded as its feature level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeatureLevelState {
	/// The newest release whose features the store may use. [`FeatureLevel::Legacy`] when the
	/// store records none.
	pub level: FeatureLevel,
	/// The newest release whose features anything in the store has used. `None` when the store
	/// records none.
	pub in_use: Option<Release>,
}

impl FeatureLevelState {
	/// The state of a store that records no feature level.
	pub const LEGACY: Self = Self {
		level: FeatureLevel::Legacy,
		in_use: None,
	};

	/// The state a durable record names.
	pub const fn of(record: &StoredFeatureLevel) -> Self {
		Self {
			level: record.feature_level(),
			in_use: Some(record.in_use()),
		}
	}
}

/// The last feature level a datastore read, shared by every transaction of one store.
///
/// The level is the high 32 bits and `in_use` the low 32 bits of one atomic, each as
/// [`Release::to_u32`]. A real release is never zero (the first stored release is 4.0), so zero
/// means "none recorded": the `Legacy` level, or no `in_use`.
///
/// `in_use` only rises, as the durable value does, so an observation never lowers it. The level
/// moves both ways, because the durable level can be lowered.
///
/// A second flag records that the stored-format ledger entry is committed in the store. It only
/// rises.
#[derive(Debug)]
pub struct FeatureLevelCell {
	/// The packed level and `in_use`.
	packed: AtomicU64,
	/// Whether the stored-format ledger entry is known to be committed.
	stored_format: AtomicBool,
}

impl FeatureLevelCell {
	const IN_USE_MASK: u64 = u32::MAX as u64;

	/// A cell that has observed nothing: `Legacy` with no `in_use`, and no stored-format entry.
	pub const fn unsettled() -> Self {
		Self {
			packed: AtomicU64::new(0),
			stored_format: AtomicBool::new(false),
		}
	}

	const fn pack(level: u32, in_use: u32) -> u64 {
		((level as u64) << 32) | in_use as u64
	}

	/// The level last observed, which is `Legacy` until a record is observed.
	pub fn level(&self) -> FeatureLevel {
		Self::level_of(self.packed.load(Ordering::Acquire))
	}

	/// The newest release last observed in use, or `None` before a record is observed.
	pub fn in_use(&self) -> Option<Release> {
		Self::in_use_of(self.packed.load(Ordering::Acquire))
	}

	/// The level and `in_use` of one load.
	pub fn state(&self) -> FeatureLevelState {
		let packed = self.packed.load(Ordering::Acquire);
		FeatureLevelState {
			level: Self::level_of(packed),
			in_use: Self::in_use_of(packed),
		}
	}

	fn level_of(packed: u64) -> FeatureLevel {
		match (packed >> 32) as u32 {
			0 => FeatureLevel::Legacy,
			level => FeatureLevel::At(Release::from_u32(level)),
		}
	}

	fn in_use_of(packed: u64) -> Option<Release> {
		match (packed & Self::IN_USE_MASK) as u32 {
			0 => None,
			in_use => Some(Release::from_u32(in_use)),
		}
	}

	/// Takes the durable state `stored` names, where `None` is a store with no record (`Legacy`).
	///
	/// The level is set. `in_use` is raised to the record's and never lowered, because the durable
	/// value only rises and an observation may have been read before a later raise.
	pub fn observe(&self, stored: Option<&StoredFeatureLevel>) {
		let (level, in_use) = match stored {
			Some(record) => (record.level().to_u32(), record.in_use().to_u32()),
			None => (0, 0),
		};
		// The closure always returns `Some`, so the update cannot fail.
		let _ = self.packed.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
			let kept = (current & Self::IN_USE_MASK) as u32;
			Some(Self::pack(level, kept.max(in_use)))
		});
	}

	/// Whether a committed transaction observed the stored-format ledger entry in the store.
	pub fn stored_format_recorded(&self) -> bool {
		self.stored_format.load(Ordering::Acquire)
	}

	/// Takes the stored-format ledger entry as committed.
	///
	/// Called only after a transaction that wrote or saw the entry has committed. The flag is
	/// never cleared, because the entry is never removed.
	pub fn observe_stored_format(&self) {
		self.stored_format.store(true, Ordering::Release);
	}

	/// Takes `level` as the store's level and keeps `in_use`.
	pub fn observe_level(&self, level: Release) {
		let level = level.to_u32();
		let _ = self.packed.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
			Some(Self::pack(level, (current & Self::IN_USE_MASK) as u32))
		});
	}
}

impl Default for FeatureLevelCell {
	fn default() -> Self {
		Self::unsettled()
	}
}

/// Every value of a range that this build can read, with the count of entries it could not.
///
/// See [`crate::Transaction::all_readable`].
#[derive(Debug)]
pub struct Readable<V> {
	/// The values this build read, in key order.
	pub values: Vec<V>,
	/// The entries skipped because a newer release wrote them.
	pub refused: usize,
}

/// How a levelled write treats a key that holds a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LevelledMode {
	/// Insert or update the key, as [`crate::Transaction::set`].
	Set,
	/// Insert or replace the key, as [`crate::Transaction::replace_raw`].
	Replace,
}
