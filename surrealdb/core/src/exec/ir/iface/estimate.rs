//! Size estimates of streams, ranges and predicates.
//!
//! Every consumer takes an `Option<Estimate>` and applies its fixed rule on `None`. An
//! [`Estimate`] never enters a program key; a [`SizeBucket`] may enter a premise.
//!
//! Which consumer may read an estimate depends on its [`EstimateSource`]:
//!
//! | source | read by |
//! |---|---|
//! | [`EstimateSource::KvRange`] | run-time decisions only: the placement policy's questions about a range a scan has opened, and the arm a compiled choice takes |
//! | [`EstimateSource::PriorRun`], [`EstimateSource::Statistics`] | the plan-time cost slots (`ClassFacts::estimate`, `PhysTableFacts::estimate`), behind the premise that names the bucket or epoch |
//!
//! So a decision that varies with the data either reads a premise-guarded estimate while
//! planning, or is compiled as alternatives the run chooses between with a `KvRange` estimate.

use super::premise::ShapeDigest;

/// What is estimated of a stream or a predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Estimate {
	pub(crate) rows: Option<u64>,
	pub(crate) bytes: Option<u64>,
	/// Predicate selectivity in parts per million, an integer so the estimate is `Eq` and `Hash`.
	pub(crate) selectivity_ppm: Option<u32>,
	pub(crate) exact: bool,
	pub(crate) source: EstimateSource,
}

impl Estimate {
	/// The storage layer's answer for a key range a scan has opened: the keys it counted, where it
	/// counted any, and the bytes it measured. Read at run time only.
	pub(crate) fn kv_range(kv: surrealdb_kvs::Estimate) -> Self {
		Self {
			rows: kv.keys,
			bytes: Some(kv.bytes),
			selectivity_ppm: None,
			exact: kv.exact,
			source: EstimateSource::KvRange,
		}
	}
}

/// Where an estimate came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum EstimateSource {
	/// The storage layer's range estimate. Read at run time only: reading it while planning
	/// would read data during planning and vary under an unchanged program key.
	KvRange,
	/// Observed for one plan shape and size bucket.
	PriorRun {
		shape: ShapeDigest,
		bucket: SizeBucket,
	},
	/// A versioned statistics epoch of the physical catalog.
	Statistics {
		epoch: u64,
	},
}

/// A row count on a base-4 logarithmic scale, saturating at [`SizeBucket::MAX`].
///
/// Counts of 0 and 1 share bucket 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SizeBucket(u8);

impl SizeBucket {
	/// The largest bucket.
	pub(crate) const MAX: u8 = 31;

	/// The bucket holding `rows`: the floor of its base-4 logarithm.
	pub(crate) const fn of_rows(rows: u64) -> Self {
		if rows == 0 {
			return Self(0);
		}
		let log2 = u64::BITS - 1 - rows.leading_zeros();
		let log4 = log2 / 2;
		Self(if log4 > Self::MAX as u32 {
			Self::MAX
		} else {
			log4 as u8
		})
	}

	/// The bucket numbered `index`, saturating at [`SizeBucket::MAX`].
	pub(crate) const fn new(index: u8) -> Self {
		Self(if index > Self::MAX {
			Self::MAX
		} else {
			index
		})
	}

	/// The bucket's number.
	pub(crate) const fn index(self) -> u8 {
		self.0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_kv_range_answer_keeps_what_storage_measured() {
		let counted = Estimate::kv_range(surrealdb_kvs::Estimate {
			keys: Some(10),
			bytes: 400,
			exact: true,
		});
		assert_eq!((counted.rows, counted.bytes, counted.exact), (Some(10), Some(400), true));
		assert_eq!(counted.source, EstimateSource::KvRange);
		let measured = Estimate::kv_range(surrealdb_kvs::Estimate {
			keys: None,
			bytes: 1 << 30,
			exact: false,
		});
		assert_eq!((measured.rows, measured.bytes), (None, Some(1 << 30)));
	}

	#[test]
	fn buckets_are_base_four_and_saturate() {
		assert_eq!(SizeBucket::of_rows(0).index(), 0);
		assert_eq!(SizeBucket::of_rows(1).index(), 0);
		assert_eq!(SizeBucket::of_rows(3).index(), 0);
		assert_eq!(SizeBucket::of_rows(4).index(), 1);
		assert_eq!(SizeBucket::of_rows(15).index(), 1);
		assert_eq!(SizeBucket::of_rows(16).index(), 2);
		assert_eq!(SizeBucket::of_rows(u64::MAX).index(), SizeBucket::MAX);
		assert_eq!(SizeBucket::new(200).index(), SizeBucket::MAX);
	}
}
