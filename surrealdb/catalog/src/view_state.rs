//! The running statistics of an aggregated view's groups, in the stored grammar.
//!
//! An aggregated view keeps one row per group and, beside it, the state its aggregates are
//! maintained from: a count, a sum, a mean's sum and count, a variance's shifted sums, an extreme,
//! or the values `array::group` accumulates. A group's state is a [`ViewState`], stored at its own
//! key under the view table's root, so the group's row holds the view's output fields and nothing
//! else. A query never returns it.
//!
//! The statistics are positional: entry `i` of [`ViewState::stats`] is the state of the view's
//! `i`-th aggregate, as the view definition's analysis lists them. An aggregate that a later
//! release adds is a new variant of [`StoredStat`], which raises the floor of every state that
//! holds one and the floor of the view's definition, so an earlier release refuses the view
//! instead of maintaining it wrongly.

use anyhow::{Result, anyhow};

use crate::aggregation::AggregationStat;
use crate::impl_kv_value_stored;
use crate::stored::Stored;
use crate::val::{Datetime, Number, Value};

/// The state of one group of an aggregated view, stored at the group's `!as` key.
#[derive(Clone, Debug, PartialEq, Stored)]
#[stored(since = "4.0")]
pub struct ViewState {
	/// The state of each aggregate of the view, in the order the view's analysis lists them.
	#[stored(n = 2)]
	pub stats: Vec<StoredStat>,
}

impl_kv_value_stored!(ViewState);

impl ViewState {
	/// The state a group holds with these running statistics.
	///
	/// Fails for an aggregate whose argument index does not fit the stored integer.
	pub fn of(stats: &[AggregationStat]) -> Result<Self> {
		Ok(Self {
			stats: stats.iter().map(StoredStat::try_from).collect::<Result<_>>()?,
		})
	}

	/// The running statistics this state holds.
	pub fn into_stats(self) -> Vec<AggregationStat> {
		self.stats.into_iter().map(AggregationStat::from).collect()
	}
}

/// The stored twin of [`AggregationStat`]: the same state, with the argument index as a `u32`.
#[derive(Clone, Debug, PartialEq, Stored)]
#[stored(since = "4.0")]
pub enum StoredStat {
	#[stored(n = 1)]
	Count {
		#[stored(n = 2)]
		count: i64,
	},
	#[stored(n = 2)]
	CountValue {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		count: i64,
	},
	#[stored(n = 3)]
	NumberMax {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		max: Number,
	},
	#[stored(n = 4)]
	NumberMin {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		min: Number,
	},
	#[stored(n = 5)]
	Sum {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		sum: Number,
	},
	#[stored(n = 6)]
	Mean {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		sum: Number,
		#[stored(n = 6)]
		count: i64,
	},
	#[stored(n = 7)]
	StdDev {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		shift: Number,
		#[stored(n = 6)]
		sum: Number,
		#[stored(n = 8)]
		sum_of_squares: Number,
		#[stored(n = 10)]
		count: i64,
	},
	#[stored(n = 8)]
	Variance {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		shift: Number,
		#[stored(n = 6)]
		sum: Number,
		#[stored(n = 8)]
		sum_of_squares: Number,
		#[stored(n = 10)]
		count: i64,
	},
	#[stored(n = 9)]
	TimeMax {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		max: Datetime,
	},
	#[stored(n = 10)]
	TimeMin {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		min: Datetime,
	},
	#[stored(n = 11)]
	Accumulate {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		values: Vec<Value>,
	},
	/// A `time::max` over `date`, `time` or `timestamp` values; see
	/// [`AggregationStat::TemporalMax`].
	#[stored(n = 12)]
	TemporalMax {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		max: Value,
	},
	/// A `time::min` over `date`, `time` or `timestamp` values; see
	/// [`AggregationStat::TemporalMin`].
	#[stored(n = 13)]
	TemporalMin {
		#[stored(n = 2)]
		arg: u32,
		#[stored(n = 4)]
		min: Value,
	},
}

impl TryFrom<&AggregationStat> for StoredStat {
	type Error = anyhow::Error;

	fn try_from(stat: &AggregationStat) -> Result<Self> {
		fn index(arg: usize) -> Result<u32> {
			u32::try_from(arg)
				.map_err(|_| anyhow!("an aggregate's argument index does not fit a stored integer"))
		}
		Ok(match stat {
			AggregationStat::Count {
				count,
			} => Self::Count {
				count: *count,
			},
			AggregationStat::CountValue {
				arg,
				count,
			} => Self::CountValue {
				arg: index(*arg)?,
				count: *count,
			},
			AggregationStat::NumberMax {
				arg,
				max,
			} => Self::NumberMax {
				arg: index(*arg)?,
				max: max.clone(),
			},
			AggregationStat::NumberMin {
				arg,
				min,
			} => Self::NumberMin {
				arg: index(*arg)?,
				min: min.clone(),
			},
			AggregationStat::Sum {
				arg,
				sum,
			} => Self::Sum {
				arg: index(*arg)?,
				sum: sum.clone(),
			},
			AggregationStat::Mean {
				arg,
				sum,
				count,
			} => Self::Mean {
				arg: index(*arg)?,
				sum: sum.clone(),
				count: *count,
			},
			AggregationStat::StdDev {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			} => Self::StdDev {
				arg: index(*arg)?,
				shift: shift.clone(),
				sum: sum.clone(),
				sum_of_squares: sum_of_squares.clone(),
				count: *count,
			},
			AggregationStat::Variance {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			} => Self::Variance {
				arg: index(*arg)?,
				shift: shift.clone(),
				sum: sum.clone(),
				sum_of_squares: sum_of_squares.clone(),
				count: *count,
			},
			AggregationStat::TimeMax {
				arg,
				max,
			} => Self::TimeMax {
				arg: index(*arg)?,
				max: *max,
			},
			AggregationStat::TimeMin {
				arg,
				min,
			} => Self::TimeMin {
				arg: index(*arg)?,
				min: *min,
			},
			AggregationStat::Accumulate {
				arg,
				values,
			} => Self::Accumulate {
				arg: index(*arg)?,
				values: values.clone(),
			},
			AggregationStat::TemporalMax {
				arg,
				max,
			} => Self::TemporalMax {
				arg: index(*arg)?,
				max: max.clone(),
			},
			AggregationStat::TemporalMin {
				arg,
				min,
			} => Self::TemporalMin {
				arg: index(*arg)?,
				min: min.clone(),
			},
		})
	}
}

impl From<StoredStat> for AggregationStat {
	fn from(stat: StoredStat) -> Self {
		// A `u32` always fits a `usize` on the targets this crate builds for.
		let index = |arg: u32| arg as usize;
		match stat {
			StoredStat::Count {
				count,
			} => Self::Count {
				count,
			},
			StoredStat::CountValue {
				arg,
				count,
			} => Self::CountValue {
				arg: index(arg),
				count,
			},
			StoredStat::NumberMax {
				arg,
				max,
			} => Self::NumberMax {
				arg: index(arg),
				max,
			},
			StoredStat::NumberMin {
				arg,
				min,
			} => Self::NumberMin {
				arg: index(arg),
				min,
			},
			StoredStat::Sum {
				arg,
				sum,
			} => Self::Sum {
				arg: index(arg),
				sum,
			},
			StoredStat::Mean {
				arg,
				sum,
				count,
			} => Self::Mean {
				arg: index(arg),
				sum,
				count,
			},
			StoredStat::StdDev {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			} => Self::StdDev {
				arg: index(arg),
				shift,
				sum,
				sum_of_squares,
				count,
			},
			StoredStat::Variance {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			} => Self::Variance {
				arg: index(arg),
				shift,
				sum,
				sum_of_squares,
				count,
			},
			StoredStat::TimeMax {
				arg,
				max,
			} => Self::TimeMax {
				arg: index(arg),
				max,
			},
			StoredStat::TimeMin {
				arg,
				min,
			} => Self::TimeMin {
				arg: index(arg),
				min,
			},
			StoredStat::Accumulate {
				arg,
				values,
			} => Self::Accumulate {
				arg: index(arg),
				values,
			},
			StoredStat::TemporalMax {
				arg,
				max,
			} => Self::TemporalMax {
				arg: index(arg),
				max,
			},
			StoredStat::TemporalMin {
				arg,
				min,
			} => Self::TemporalMin {
				arg: index(arg),
				min,
			},
		}
	}
}
