//! Common code related to aggregation calculations.
//!
//! Uses in aggergate views and group by selects.
//!
//! # Basic overview.
//!
//! Aggregation calculation works in 3 steps.
//!
//! - First on view definition, or when issued a select we analyse the group and selector
//!   expressions to find the aggregate functions, the agruments for those functions, the expression
//!   which defines the group of an entry, and the expression to generate a result for that group.
//! - Then we accumulate all the records the aggregation needs to be calculated over, run the
//!   expressions which generate the agruments for the aggregate functions and then manually update
//!   the computation for the aggregation functions.
//! - Finally when all the values have been consumed we construct a document which has all the
//!   values for the computed aggregates as fields and run the expression we generated in the first
//!   step to compute the final result for the aggregation.
//!
//! Example:
//! ```txt
//!    SELECT foo, math::pow(math::mean(v),2), math::max(v), math::min(x + 1) FROM foo GROUP foo.
//!
//!    the aggregate argument expressions are,
//!        0: v,
//!        1: x + 1,
//!
//!    the group expressions are,
//!        0: foo,
//!
//!    The aggregates functions are:
//!        _a0: math::mean operating on expression 0
//!        _a1: math::max operating on expression 0
//!        _a2: math::min operating on expression 1
//!
//!    the final expression to calculate the result is:
//!        _g0, math::pow(_a0,2), _a1, _a2
//!
//!        here `_g0` refers to the group.
//! ```

use std::fmt::Write;
use std::hash::Hash;
use std::mem;

use ahash::HashMap;
use anyhow::{Result, bail, ensure};
use common::fail;
use revision::{DeserializeRevisioned, SkipCheckRevisioned, revisioned};
use surrealdb_collections::bounded;
use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use crate::catalog::Error;
use crate::expr::field::Selector;
use crate::expr::statements::define::DefineConfigStatement;
use crate::expr::statements::{
	CreateStatement, DefineAccessStatement, DefineApiStatement, DefineFieldStatement,
	DefineFunctionStatement, DefineIndexStatement, InsertStatement, RelateStatement,
	UpdateStatement, UpsertStatement,
};
use crate::expr::visit::{MutVisitor, VisitMut};
use crate::expr::{
	Error as ExprError, Expr, Field, Fields, Function, Groups, Idiom, Part, SelectStatement,
};
use crate::val::{
	Array, Datetime, Number, Object, TryAdd as _, TryFloatDiv, TryMul, TrySub as _, Value,
};

/// An expression which will be aggregated over for each group.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Aggregation {
	Count,
	/// The usizes are index into the exprs field on the aggregate collector and represent the
	/// expression which was fed as an argument to the aggregate expression
	CountValue(usize),
	NumberMax(usize),
	NumberMin(usize),
	Sum(usize),
	Mean(usize),
	StdDev(usize),
	Variance(usize),
	DatetimeMax(usize),
	DatetimeMin(usize),
	Accumulate(usize),
}

impl Aggregation {
	pub fn to_stat(&self) -> AggregationStat {
		match *self {
			Aggregation::Count => AggregationStat::Count {
				count: 0,
			},
			Aggregation::CountValue(arg) => AggregationStat::CountValue {
				arg,
				count: 0,
			},
			Aggregation::NumberMax(arg) => AggregationStat::NumberMax {
				arg,
				max: f64::NEG_INFINITY.into(),
			},
			Aggregation::NumberMin(arg) => AggregationStat::NumberMin {
				arg,
				min: f64::INFINITY.into(),
			},
			Aggregation::Sum(arg) => AggregationStat::Sum {
				arg,
				sum: Number::Int(0),
			},
			Aggregation::Mean(arg) => AggregationStat::Mean {
				arg,
				count: 0,
				sum: Number::Int(0),
			},
			Aggregation::StdDev(arg) => AggregationStat::StdDev {
				arg,
				shift: 0.0.into(),
				sum: 0.0.into(),
				sum_of_squares: 0.0.into(),
				count: 0,
			},
			Aggregation::Variance(arg) => AggregationStat::Variance {
				arg,
				shift: 0.0.into(),
				sum: 0.0.into(),
				sum_of_squares: 0.0.into(),
				count: 0,
			},
			Aggregation::DatetimeMax(arg) => AggregationStat::TimeMax {
				arg,
				max: Datetime::MIN_UTC,
			},
			Aggregation::DatetimeMin(arg) => AggregationStat::TimeMin {
				arg,
				min: Datetime::MAX_UTC,
			},
			Aggregation::Accumulate(arg) => AggregationStat::Accumulate {
				arg,
				values: Vec::new(),
			},
		}
	}
}

/// A enum containing the data for an aggregation.
#[revisioned(revision = 2, deserialize = false, skip = true, walk = true)]
#[derive(Clone, Debug, PartialEq)]
pub enum AggregationStat {
	Count {
		count: i64,
	},
	CountValue {
		/// Index into the exprs field on the view definition.
		arg: usize,
		count: i64,
	},
	NumberMax {
		arg: usize,
		max: Number,
	},
	NumberMin {
		arg: usize,
		min: Number,
	},
	Sum {
		arg: usize,
		sum: Number,
	},
	Mean {
		arg: usize,
		sum: Number,
		count: i64,
	},
	StdDev {
		arg: usize,
		/// Assumed mean. `sum` and `sum_of_squares` accumulate `x - shift`
		/// rather than `x`; see [`shifted_accumulate`].
		#[revision(start = 2, default_fn = "default_shift")]
		shift: Number,
		sum: Number,
		sum_of_squares: Number,
		count: i64,
	},
	Variance {
		arg: usize,
		/// Assumed mean. See [`Self::StdDev`].
		#[revision(start = 2, default_fn = "default_shift")]
		shift: Number,
		sum: Number,
		sum_of_squares: Number,
		count: i64,
	},
	TimeMax {
		arg: usize,
		max: Datetime,
	},
	TimeMin {
		arg: usize,
		min: Datetime,
	},
	Accumulate {
		arg: usize,
		values: Vec<Value>,
	},
}

/// Reads exactly what the derived serializer writes: the revision, a `u32` variant, then the
/// variant's fields in declaration order. A stat written at revision 1 has no `shift` on `StdDev`
/// and `Variance`, and takes [`AggregationStat::default_shift`]. `Accumulate`'s values are read
/// through [`bounded::read_vec`], so a claimed length costs nothing beyond the values the input
/// holds.
impl DeserializeRevisioned for AggregationStat {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		fn arg<R: std::io::Read>(reader: &mut R) -> Result<usize, revision::Error> {
			usize::deserialize_revisioned(reader)
		}
		fn count<R: std::io::Read>(reader: &mut R) -> Result<i64, revision::Error> {
			i64::deserialize_revisioned(reader)
		}
		fn number<R: std::io::Read>(reader: &mut R) -> Result<Number, revision::Error> {
			Number::deserialize_revisioned(reader)
		}
		fn shift<R: std::io::Read>(
			reader: &mut R,
			revision: u16,
		) -> Result<Number, revision::Error> {
			if revision >= 2 {
				number(reader)
			} else {
				AggregationStat::default_shift(revision)
			}
		}

		let revision = bounded::read_revision(reader, "AggregationStat", 2)?;
		Ok(match u32::deserialize_revisioned(reader)? {
			0 => Self::Count {
				count: count(reader)?,
			},
			1 => {
				let arg = arg(reader)?;
				Self::CountValue {
					arg,
					count: count(reader)?,
				}
			}
			2 => {
				let arg = arg(reader)?;
				Self::NumberMax {
					arg,
					max: number(reader)?,
				}
			}
			3 => {
				let arg = arg(reader)?;
				Self::NumberMin {
					arg,
					min: number(reader)?,
				}
			}
			4 => {
				let arg = arg(reader)?;
				Self::Sum {
					arg,
					sum: number(reader)?,
				}
			}
			5 => {
				let arg = arg(reader)?;
				let sum = number(reader)?;
				Self::Mean {
					arg,
					sum,
					count: count(reader)?,
				}
			}
			6 => {
				let arg = arg(reader)?;
				let shift = shift(reader, revision)?;
				let sum = number(reader)?;
				let sum_of_squares = number(reader)?;
				Self::StdDev {
					arg,
					shift,
					sum,
					sum_of_squares,
					count: count(reader)?,
				}
			}
			7 => {
				let arg = arg(reader)?;
				let shift = shift(reader, revision)?;
				let sum = number(reader)?;
				let sum_of_squares = number(reader)?;
				Self::Variance {
					arg,
					shift,
					sum,
					sum_of_squares,
					count: count(reader)?,
				}
			}
			8 => {
				let arg = arg(reader)?;
				Self::TimeMax {
					arg,
					max: Datetime::deserialize_revisioned(reader)?,
				}
			}
			9 => {
				let arg = arg(reader)?;
				Self::TimeMin {
					arg,
					min: Datetime::deserialize_revisioned(reader)?,
				}
			}
			10 => {
				let arg = arg(reader)?;
				Self::Accumulate {
					arg,
					values: bounded::read_vec::<Value, _>(reader)?,
				}
			}
			x => {
				return Err(revision::Error::Deserialize(format!(
					"Invalid discriminant `{x}` for enum `AggregationStat` revision `{revision}`"
				)));
			}
		})
	}
}

impl SkipCheckRevisioned for AggregationStat {
	fn skip_check_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		Self::deserialize_revisioned(reader).map(drop)
	}
}

/// Widen an integer difference so that squaring it cannot overflow.
///
/// `Number::Int * Number::Int` is a checked `i64` multiply, so squaring a
/// difference larger than `sqrt(i64::MAX)` (~3.04e9) fails the whole statement:
/// `math::variance` over a group holding `0` and `4_000_000_000` would error
/// rather than return 8e18. Integer inputs therefore accumulate as `f64`, which
/// is what the scalar `math::variance` already does -- it subtracts a `Float`
/// mean, so its differences are never `Int`.
///
/// `Decimal` and `Float` differences are returned untouched, so a `Decimal`
/// group keeps full precision. Nothing is lost by widening: the overflow
/// threshold (~3.04e9) is far below the point where an `f64` stops representing
/// integers exactly (2^53 ≈ 9.01e15).
fn widen_difference(delta: Number) -> Number {
	match delta {
		Number::Int(v) => Number::Float(v as f64),
		other => other,
	}
}

/// Fold one value into shifted `sum` / `sum_of_squares` state.
///
/// Variance is invariant under translation, so the accumulators track
/// `x - shift` instead of `x`. The finaliser is unchanged --
/// `(Σd² - (Σd)²/n) / (n - 1)` -- but with `shift` near the data the two terms
/// are small and their difference no longer cancels. Accumulating raw `x` makes
/// both terms O(n·x²): at x ≈ 1e8 they agree to more digits than an `f64`
/// carries, and the variance is lost entirely.
///
/// `shift` is chosen as the first value folded into an empty group, which needs
/// no second pass and no knowledge of the group's spread. A group decoded from
/// revision 1 has `shift = 0` and keeps accumulating that way, so its existing
/// sums stay consistent; rebuilding the view re-picks a shift.
///
/// The state stays **invertible** -- removing `x` subtracts `x - shift` and
/// `(x - shift)²` -- which is what lets materialized-view maintenance stay
/// incremental. That is the property Welford would have cost.
///
/// Arithmetic goes through `Number`, so a group of `Decimal` inputs accumulates
/// and finalises as `Decimal`.
pub fn shifted_accumulate(
	shift: &mut Number,
	sum: &mut Number,
	sum_of_squares: &mut Number,
	count: &mut i64,
	x: Number,
) -> Result<()> {
	if *count == 0 {
		*shift = x;
	}
	let delta = widen_difference(x.try_sub(*shift)?);
	*sum = (*sum).try_add(delta)?;
	*sum_of_squares = (*sum_of_squares).try_add(delta.try_mul(delta)?)?;
	*count += 1;
	Ok(())
}

/// Re-base shifted state accumulated against `from_shift` onto `to_shift`.
///
/// Two partially-accumulated halves of one group can hold different shifts.
/// With `d = from_shift - to_shift`, substituting
/// `x - to_shift = (x - from_shift) + d` gives exactly
///
/// ```txt
/// Σ(x - to_shift)  = Σ(x - from_shift) + n·d
/// Σ(x - to_shift)² = Σ(x - from_shift)² + 2d·Σ(x - from_shift) + n·d²
/// ```
///
/// Exact algebra, needing none of the original values back. Conditioning is
/// preserved while the two shifts are close, which they are for halves of one
/// group.
pub fn shifted_rebase(
	sum: Number,
	sum_of_squares: Number,
	count: i64,
	from_shift: Number,
	to_shift: Number,
) -> Result<(Number, Number)> {
	let n = Number::from(count);
	let d = widen_difference(from_shift.try_sub(to_shift)?);
	let rebased_sum = sum.try_add(n.try_mul(d)?)?;
	let rebased_sum_of_squares = sum_of_squares
		.try_add(Number::from(2).try_mul(d)?.try_mul(sum)?)?
		.try_add(n.try_mul(d.try_mul(d)?)?)?;
	Ok((rebased_sum, rebased_sum_of_squares))
}

/// Remove one value from shifted state, the exact inverse of
/// [`shifted_accumulate`].
///
/// Emptying a group clears the shift so the next value picks a fresh one.
pub fn shifted_remove(
	shift: &mut Number,
	sum: &mut Number,
	sum_of_squares: &mut Number,
	count: &mut i64,
	x: Number,
) -> Result<()> {
	let delta = widen_difference(x.try_sub(*shift)?);
	*sum = (*sum).try_sub(delta)?;
	*sum_of_squares = (*sum_of_squares).try_sub(delta.try_mul(delta)?)?;
	*count -= 1;
	if *count == 0 {
		*shift = 0.0.into();
		*sum = 0.0.into();
		*sum_of_squares = 0.0.into();
	}
	Ok(())
}

/// Build shifted state for a group that already exists, from its mean, sample
/// variance and count.
///
/// Used when a materialized view is backfilled: the initial `SELECT` reports
/// summary statistics rather than replaying rows, so the state is reconstructed
/// from them -- `Σ(x - mean)` is zero and `Σ(x - mean)²` is `variance * (n - 1)`.
/// Deriving them from `math::sum` and `math::sum(x * x)` instead would
/// reintroduce, in the backfill, the very cancellation [`shifted_accumulate`]
/// exists to avoid.
///
/// Those two identities hold for the *exact* mean. `math::mean` returns a
/// rounded `f64`, so `Σ(x - shift)` is really on the order of `n · ulp(mean)`
/// rather than zero, and `sum` is stored as an approximation. The finalised
/// variance is unaffected at this point -- `sum = 0` collapses the formula to
/// `sum_of_squares / (n - 1)`, which is the reported variance exactly -- but a
/// later incremental insert accumulates only its own delta, so the group carries
/// that residue for as long as it is maintained rather than rebuilt. It enters
/// the finaliser as `sum² / n`, far below the variance itself for any realistic
/// data. For the same reason the shift of a `Decimal` group is `f64`-precise:
/// `math::mean` has no `Decimal` result.
pub fn shifted_state_from_summary(
	mean: Number,
	variance: Number,
	count: i64,
) -> Result<(Number, Number, Number)> {
	if count == 0 {
		return Ok((0.0.into(), 0.0.into(), 0.0.into()));
	}
	if count == 1 {
		// A single-element group has no spread, and its mean is that element.
		return Ok((mean, 0.0.into(), 0.0.into()));
	}
	let sum_of_squares = variance.try_mul(Number::from(count - 1))?;
	Ok((mean, 0.0.into(), sum_of_squares))
}

/// Sample variance from shifted state: NaN for an empty group, 0 for a single
/// element, `(Σd² - (Σd)²/n) / (n - 1)` otherwise.
///
/// Shared by the materialized-view finaliser, the ad-hoc `GROUP BY` finaliser
/// and the streaming aggregator so all three agree bit for bit.
pub fn shifted_sample_variance(sum: Number, sum_of_squares: Number, count: i64) -> Number {
	match count {
		// A group cannot hold fewer than no rows. A negative count means the
		// state was decremented more often than it was incremented, and the
		// divisions below would silently return a plausible-looking number
		// (`sum / -1`, then `/ -2`) from it, so report it as undefined instead.
		i64::MIN..=0 => f64::NAN.into(),
		1 => 0.0.into(),
		_ => {
			// `count` is at least 2 here, so both divisions are defined.
			let mean = sum / Number::from(count);
			(sum_of_squares - (sum * mean)) / Number::from(count - 1)
		}
	}
}

/// Sample standard deviation from shifted state.
pub fn shifted_sample_deviation(sum: Number, sum_of_squares: Number, count: i64) -> Number {
	let variance = shifted_sample_variance(sum, sum_of_squares, count);
	if variance == Number::from(0.0) {
		Number::from(0.0)
	} else {
		variance.sqrt()
	}
}

impl AggregationStat {
	/// Groups persisted before the shift existed accumulated raw values, which
	/// is exactly `shift = 0`. The migration is lossless: their sums stay valid
	/// as they are, and only their conditioning is unimproved until the view is
	/// rebuilt.
	fn default_shift(_revision: u16) -> Result<Number, revision::Error> {
		Ok(0.0.into())
	}

	/// Returns a per group record count this aggregation list keeps track of, if any.
	pub fn get_count(aggregation_stats: &[AggregationStat]) -> Option<i64> {
		aggregation_stats.iter().find_map(|x| match x {
			AggregationStat::Count {
				count,
			}
			| AggregationStat::Mean {
				count,
				..
			}
			| AggregationStat::Variance {
				count,
				..
			}
			| AggregationStat::StdDev {
				count,
				..
			} => Some(*count),
			_ => None,
		})
	}
}

pub fn write_aggregate_field_name(s: &mut String, idx: usize) {
	// Writing into a string cannot error.
	write!(s, "_a{}", idx).expect("writing into a string cannot fail");
}

pub fn write_group_field_name(s: &mut String, idx: usize) {
	// Writing into a string cannot error.
	write!(s, "_g{}", idx).expect("writing into a string cannot fail");
}

/// Format `{prefix}{idx}` directly into an inline `Strand`, bypassing the intermediate heap
/// `String` that `Strand::from(format!(...))` would otherwise produce.
///
/// The public `aggregate_field_name` / `group_field_name` helpers below produce keys of the form
/// `_a{idx}` and `_g{idx}`. Even `usize::MAX` formats to 20 decimal digits on 64-bit, so the
/// 2-byte prefix + digits always fit inside [`INLINE_CAP`]. The expectation below is therefore
/// unreachable in practice; it exists purely as a safety net so the function stays total.
fn format_indexed_strand(prefix: &str, idx: usize) -> Strand {
	Strand::from_display(format_args!("{prefix}{idx}"))
}

/// Returns the name of aggregate n used within the fields expression to calculate the result for
/// the aggregate analysis.
pub fn aggregate_field_name(idx: usize) -> Strand {
	format_indexed_strand("_a", idx)
}

/// Returns the name of group expression n used within the fields expression to calculate the result
/// for the aggregate analysis.
pub fn group_field_name(idx: usize) -> Strand {
	format_indexed_strand("_g", idx)
}

/// One stat an incremental update could not maintain from the change alone,
/// because the value that left the group was the stored extreme.
///
/// The extreme has to be recomputed over the group's remaining rows, which the
/// caller does — this only names what to recompute and where to put it back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Recalculation {
	/// The aggregate function that recomputes the extreme.
	pub function: String,
	/// Index into the stats of the stat to restore.
	pub stat: usize,
	/// Index into the aggregate arguments the stat reads.
	pub arg: usize,
}

/// Withdraw one record's contribution from a group's stats.
///
/// The counterpart of [`add_to_aggregation_stats`], for a record leaving the
/// group: counts and sums move down, and the shifted variance state is
/// withdrawn in a way that resets an emptied group.
///
/// An extreme cannot be withdrawn — the stats hold the maximum, not the values
/// it was drawn from — so a record carrying the stored extreme leaves the stat
/// as it stands and is reported in the returned [`Recalculation`]s. Pass the
/// recomputed values back through [`apply_recalculations`]; a caller that
/// ignores them leaves the group holding an extreme no member has.
pub fn remove_from_aggregation_stats(
	arguments: &[Value],
	stats: &mut [AggregationStat],
) -> Result<Vec<Recalculation>> {
	let mut recalculations = Vec::new();
	for (idx, stat) in stats.iter_mut().enumerate() {
		if let Some((arg, _)) = skipping_stat(stat)
			&& is_absent(&arguments[arg])
		{
			continue;
		}
		match stat {
			AggregationStat::Count {
				count,
			} => {
				*count -= 1;
			}
			AggregationStat::CountValue {
				arg,
				count,
			} => {
				if arguments[*arg].is_truthy() {
					*count -= 1;
				}
			}
			AggregationStat::NumberMax {
				arg,
				max,
			} => {
				let Value::Number(n) = &arguments[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				if *n == *max {
					recalculations.push(Recalculation {
						function: "math::max".to_string(),
						stat: idx,
						arg: *arg,
					})
				}
			}
			AggregationStat::NumberMin {
				arg,
				min,
			} => {
				let Value::Number(n) = &arguments[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				if *n == *min {
					recalculations.push(Recalculation {
						function: "math::min".to_string(),
						stat: idx,
						arg: *arg,
					})
				}
			}
			AggregationStat::Sum {
				arg,
				sum,
			} => {
				let Value::Number(n) = &arguments[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				*sum = (*sum).try_sub(*n)?;
			}
			AggregationStat::Mean {
				arg,
				sum,
				count,
			} => {
				let Value::Number(n) = &arguments[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				*sum = (*sum).sub_exact(*n)?;
				*count -= 1;
			}
			AggregationStat::TimeMax {
				arg,
				max,
			} => {
				let Value::Datetime(n) = &arguments[*arg] else {
					fail!("Old record wasn't a datetime but was created with a number");
				};
				if *n == *max {
					recalculations.push(Recalculation {
						function: "time::max".to_string(),
						stat: idx,
						arg: *arg,
					});
				}
			}
			AggregationStat::TimeMin {
				arg,
				min,
			} => {
				let Value::Datetime(n) = &arguments[*arg] else {
					fail!("Old record wasn't a datetime but was created with a number");
				};
				if *n == *min {
					recalculations.push(Recalculation {
						function: "time::min".to_string(),
						stat: idx,
						arg: *arg,
					});
				}
			}
			AggregationStat::Variance {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			}
			| AggregationStat::StdDev {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			} => {
				let Value::Number(n) = &arguments[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				shifted_remove(shift, sum, sum_of_squares, count, *n)?;
			}
			AggregationStat::Accumulate {
				..
			} => fail!("Accumulate aggregation is not supported in materialized views"),
		}
	}
	Ok(recalculations)
}

/// Move a group's stats from one record's contribution to another's.
///
/// A record that stays in its group and changes value. Not a
/// [`remove_from_aggregation_stats`] followed by an
/// [`add_to_aggregation_stats`]: the group's membership does not change, so
/// `Count` stands still, and an extreme the new value exceeds is taken
/// directly rather than recomputed. Only a new value that *retreats* from the
/// stored extreme while the old value *was* it needs the group re-read, which
/// is what the returned [`Recalculation`]s name.
///
/// The shifted variance state is the exception: it is withdrawn and re-added,
/// because the shift and the count must move together with the sums and
/// [`shifted_remove`] resets an emptied group so the re-add picks a fresh
/// shift.
pub fn update_aggregation_stats(
	before_args: &[Value],
	after_args: &[Value],
	stats: &mut [AggregationStat],
) -> Result<Vec<Recalculation>> {
	let mut recalculations = Vec::new();
	for (idx, stat) in stats.iter_mut().enumerate() {
		if let Some((arg, name)) = skipping_stat(stat) {
			match (is_absent(&before_args[arg]), is_absent(&after_args[arg])) {
				(true, true) => continue,
				(true, false) => {
					fold_in(stat, &after_args[arg], name)?;
					continue;
				}
				(false, true) => {
					fold_out(idx, stat, &before_args[arg], &mut recalculations)?;
					continue;
				}
				(false, false) => {}
			}
		}
		match stat {
			AggregationStat::Count {
				..
			} => {}
			AggregationStat::CountValue {
				arg,
				count,
			} => {
				if before_args[*arg].is_truthy() {
					*count -= 1;
				}
				if after_args[*arg].is_truthy() {
					*count += 1;
				}
			}
			AggregationStat::NumberMax {
				arg,
				max,
			} => {
				let after = number(&after_args[*arg], "math::max")?;
				let Value::Number(before) = &before_args[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				if after >= *max {
					*max = after
				} else if *before == *max {
					recalculations.push(Recalculation {
						function: "math::max".to_string(),
						stat: idx,
						arg: *arg,
					})
				}
			}
			AggregationStat::NumberMin {
				arg,
				min,
			} => {
				let after = number(&after_args[*arg], "math::min")?;
				let Value::Number(before) = &before_args[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				if after <= *min {
					*min = after
				} else if *before == *min {
					recalculations.push(Recalculation {
						function: "math::min".to_string(),
						stat: idx,
						arg: *arg,
					})
				}
			}
			AggregationStat::Sum {
				arg,
				sum,
			} => {
				let after = number(&after_args[*arg], "math::sum")?;
				let Value::Number(before) = &before_args[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				*sum = (*sum).sub_exact(*before)?;
				*sum = match *sum {
					Number::Decimal(d)
						if matches!(before, Number::Int(_)) && matches!(after, Number::Int(_)) =>
					{
						let total = d
							.checked_add(after.to_decimal())
							.ok_or_else(|| ExprError::TryAdd(d.to_string(), after.to_sql()))?;
						Number::Int(
							i64::try_from(total)
								.map_err(|_| ExprError::TryAdd(d.to_string(), after.to_sql()))?,
						)
					}
					s => s.try_add(after)?,
				};
			}
			AggregationStat::Mean {
				arg,
				sum,
				..
			} => {
				let after = number(&after_args[*arg], "math::mean")?;
				let Value::Number(before) = &before_args[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				*sum = (*sum).sub_exact(*before)?;
				*sum = sum.add_exact(after)?;
			}
			AggregationStat::TimeMax {
				arg,
				max,
			} => {
				let after = datetime(&after_args[*arg], "time::max")?;
				let Value::Datetime(before) = &before_args[*arg] else {
					fail!("Old record wasn't a datetime but was created with a number");
				};
				if after >= *max {
					*max = after;
				} else if *before == *max {
					recalculations.push(Recalculation {
						function: "time::max".to_string(),
						stat: idx,
						arg: *arg,
					});
				}
			}
			AggregationStat::TimeMin {
				arg,
				min,
			} => {
				let after = datetime(&after_args[*arg], "time::min")?;
				let Value::Datetime(before) = &before_args[*arg] else {
					fail!("Old record wasn't a datetime but was created with a number");
				};
				if after <= *min {
					*min = after;
				} else if *before == *min && after != *min {
					recalculations.push(Recalculation {
						function: "time::min".to_string(),
						stat: idx,
						arg: *arg,
					});
				}
			}
			AggregationStat::Variance {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			}
			| AggregationStat::StdDev {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			} => {
				let Value::Number(before) = &before_args[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				let Value::Number(after) = &after_args[*arg] else {
					fail!("Old record wasn't a number but was created with a number");
				};
				shifted_remove(shift, sum, sum_of_squares, count, *before)?;
				shifted_accumulate(shift, sum, sum_of_squares, count, *after)?;
			}
			AggregationStat::Accumulate {
				..
			} => fail!("Accumulate aggregation is not supported in materialized views"),
		}
	}
	Ok(recalculations)
}

/// Put recomputed extremes back into the stats they were withdrawn from.
///
/// `values` is positional against `recalculations`, in the order
/// [`remove_from_aggregation_stats`] or [`update_aggregation_stats`] reported
/// them.
///
/// A value whose type does not match the stat it is restoring, and a
/// recalculation naming a stat that holds no extreme, are both disagreements
/// between the recomputation and the stats it was derived from, and are
/// reported rather than assumed away.
pub fn apply_recalculations(
	stats: &mut [AggregationStat],
	recalculations: &[Recalculation],
	values: Vec<Value>,
) -> Result<()> {
	if values.len() != recalculations.len() {
		fail!(
			"An aggregate recalculation returned {} values for {} extremes",
			values.len(),
			recalculations.len()
		)
	}
	for (recalculation, value) in recalculations.iter().zip(values) {
		let Some(stat) = stats.get_mut(recalculation.stat) else {
			fail!("An aggregate recalculation named a stat the group does not hold")
		};
		match stat {
			AggregationStat::TimeMin {
				min: stat,
				..
			} => match value {
				Value::Datetime(d) => *stat = d,
				// No record of the group holds a datetime any more: the stat
				// is the one a group that has seen none starts from.
				Value::None | Value::Null => *stat = Datetime::MAX_UTC,
				_ => fail!("Got wrong recalculation value"),
			},
			AggregationStat::TimeMax {
				max: stat,
				..
			} => match value {
				Value::Datetime(d) => *stat = d,
				Value::None | Value::Null => *stat = Datetime::MIN_UTC,
				_ => fail!("Got wrong recalculation value"),
			},
			AggregationStat::NumberMin {
				min: stat,
				..
			}
			| AggregationStat::NumberMax {
				max: stat,
				..
			} => {
				let Value::Number(n) = value else {
					fail!("Got wrong recalculation value")
				};
				*stat = n;
			}
			_ => fail!("An aggregate recalculation named a stat that holds no extreme"),
		}
	}
	Ok(())
}

/// Whether an aggregate's argument is absent rather than wrong: `NONE` and
/// `NULL` are skipped by `math::sum`, `math::min` and `math::max`, so a record
/// that lacks the field contributes nothing to a group and takes nothing out.
fn is_absent(value: &Value) -> bool {
	matches!(value, Value::None | Value::Null)
}

/// The argument and function name of a stat that skips absent values.
fn skipping_stat(stat: &AggregationStat) -> Option<(usize, &'static str)> {
	match stat {
		AggregationStat::NumberMax {
			arg,
			..
		} => Some((*arg, "math::max")),
		AggregationStat::NumberMin {
			arg,
			..
		} => Some((*arg, "math::min")),
		AggregationStat::Sum {
			arg,
			..
		} => Some((*arg, "math::sum")),
		AggregationStat::TimeMax {
			arg,
			..
		} => Some((*arg, "time::max")),
		AggregationStat::TimeMin {
			arg,
			..
		} => Some((*arg, "time::min")),
		_ => None,
	}
}

/// Fold one present value into a stat of [`skipping_stat`], a wrong kind
/// refused as the function's argument error.
fn fold_in(stat: &mut AggregationStat, value: &Value, function: &str) -> Result<()> {
	match stat {
		AggregationStat::NumberMax {
			max,
			..
		} => {
			let n = number(value, function)?;
			if *max < n {
				*max = n;
			}
		}
		AggregationStat::NumberMin {
			min,
			..
		} => {
			let n = number(value, function)?;
			if *min > n {
				*min = n;
			}
		}
		AggregationStat::Sum {
			sum,
			..
		} => *sum = (*sum).try_add(number(value, function)?)?,
		AggregationStat::TimeMax {
			max,
			..
		} => {
			let d = datetime(value, function)?;
			if *max < d {
				*max = d;
			}
		}
		AggregationStat::TimeMin {
			min,
			..
		} => {
			let d = datetime(value, function)?;
			if *min > d {
				*min = d;
			}
		}
		_ => {}
	}
	Ok(())
}

/// Take one present value out of a stat of [`skipping_stat`]; an extreme the
/// value held is reported for recomputation.
fn fold_out(
	idx: usize,
	stat: &mut AggregationStat,
	value: &Value,
	recalculations: &mut Vec<Recalculation>,
) -> Result<()> {
	let held_extreme = match (&*stat, value) {
		(
			AggregationStat::NumberMax {
				arg,
				max: held,
			},
			Value::Number(n),
		)
		| (
			AggregationStat::NumberMin {
				arg,
				min: held,
			},
			Value::Number(n),
		) => (n == held).then_some((*arg, stat_function(stat))),
		(
			AggregationStat::TimeMax {
				arg,
				max: held,
			},
			Value::Datetime(d),
		)
		| (
			AggregationStat::TimeMin {
				arg,
				min: held,
			},
			Value::Datetime(d),
		) => (d == held).then_some((*arg, stat_function(stat))),
		(
			AggregationStat::Sum {
				..
			},
			Value::Number(_),
		) => None,
		_ => fail!("Old record wasn't the kind its aggregate was created with"),
	};
	if let (
		AggregationStat::Sum {
			sum,
			..
		},
		Value::Number(n),
	) = (&mut *stat, value)
	{
		*sum = (*sum).try_sub(*n)?;
	}
	if let Some((arg, function)) = held_extreme {
		recalculations.push(Recalculation {
			function: function.to_string(),
			stat: idx,
			arg,
		});
	}
	Ok(())
}

/// The function a stat of [`skipping_stat`] is the state of.
fn stat_function(stat: &AggregationStat) -> &'static str {
	skipping_stat(stat).map_or("", |(_, name)| name)
}

/// The numeric argument an aggregate was defined over, named by the function
/// that reads it so a wrong type reports as that function's argument error.
fn number(value: &Value, function: &str) -> Result<Number> {
	let Value::Number(n) = value else {
		bail!(ExprError::InvalidFunctionArguments {
			name: function.to_string(),
			message: format!(
				"Argument 1 was the wrong type. Expected `number` but found `{}`",
				value.to_sql()
			),
		})
	};
	Ok(*n)
}

/// [`number`], for an aggregate over datetimes.
fn datetime(value: &Value, function: &str) -> Result<Datetime> {
	let Value::Datetime(d) = value else {
		bail!(ExprError::InvalidFunctionArguments {
			name: function.to_string(),
			message: format!(
				"Argument 1 was the wrong type. Expected `datetime` but found `{}`",
				value.to_sql()
			),
		})
	};
	Ok(*d)
}

/// Add one record's contribution to a group's stats.
///
/// Assumes the correct number of arguments are in the arguments array as
/// required by the aggregation stats.
pub fn add_to_aggregation_stats(arguments: &[Value], stats: &mut [AggregationStat]) -> Result<()> {
	for stat in stats {
		if let Some((arg, _)) = skipping_stat(stat)
			&& is_absent(&arguments[arg])
		{
			continue;
		}
		match stat {
			AggregationStat::Count {
				count,
			} => {
				*count += 1;
			}
			AggregationStat::CountValue {
				arg,
				count,
			} => {
				*count += arguments[*arg].is_truthy() as i64;
			}
			AggregationStat::NumberMax {
				arg,
				max,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "math::max".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg].to_sql()
						),
					})
				};
				if *max < *n {
					*max = *n
				}
			}
			AggregationStat::NumberMin {
				arg,
				min,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "math::min".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg].to_sql()
						),
					})
				};
				if *min > *n {
					*min = *n
				}
			}
			AggregationStat::Sum {
				arg,
				sum,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "math::sum".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg].to_sql()
						),
					})
				};
				*sum = (*sum).try_add(*n)?;
			}
			AggregationStat::Mean {
				arg,
				sum,
				count,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "math::mean".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg].to_sql()
						),
					})
				};

				*sum = (*sum).add_exact(*n)?;
				*count += 1;
			}
			AggregationStat::StdDev {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "math::stddev".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg].to_sql()
						),
					})
				};

				shifted_accumulate(shift, sum, sum_of_squares, count, *n)?;
			}
			AggregationStat::Variance {
				arg,
				shift,
				sum,
				sum_of_squares,
				count,
			} => {
				let Value::Number(ref n) = arguments[*arg] else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "math::variance".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `number` but found `{}`",
							arguments[*arg].to_sql()
						),
					})
				};

				shifted_accumulate(shift, sum, sum_of_squares, count, *n)?;
			}
			AggregationStat::TimeMax {
				arg,
				max,
			} => {
				let Value::Datetime(ref d) = arguments[*arg] else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "time::max".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `datetime` but found `{}`",
							arguments[*arg].to_sql()
						),
					})
				};

				if *max < *d {
					*max = *d;
				}
			}
			AggregationStat::TimeMin {
				arg,
				min,
			} => {
				let Value::Datetime(ref d) = arguments[*arg] else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "time::min".to_string(),
						message: format!(
							"Argument 1 was the wrong type. Expected `datetime` but found `{}`",
							arguments[*arg].to_sql()
						),
					})
				};

				if *min > *d {
					*min = *d;
				}
			}
			AggregationStat::Accumulate {
				arg,
				values,
			} => {
				values.push(arguments[*arg].clone());
			}
		}
	}
	Ok(())
}

/// Creates object that can act as a document to calculate the final value for an aggregated
/// statement.
pub fn create_field_document(group: &[Value], stats: &[AggregationStat]) -> Object {
	let mut res = Object::default();
	//setup the document for final value calculation
	for (idx, a) in stats.iter().enumerate() {
		let value = match a {
			AggregationStat::Count {
				count,
			}
			| AggregationStat::CountValue {
				count,
				..
			} => Value::from(Number::from(*count)),
			AggregationStat::NumberMax {
				max,
				..
			} => (*max).into(),
			AggregationStat::NumberMin {
				min,
				..
			} => (*min).into(),
			AggregationStat::Sum {
				sum,
				..
			} => (*sum).into(),
			AggregationStat::Mean {
				sum,
				count,
				..
			} => {
				if *count == 0 {
					f64::NAN.into()
				} else {
					match sum {
						// An integer total (or one widened to an integer-scale
						// decimal on i64 overflow) has a float mean, divided
						// once in f64 as scalar and ad-hoc math::mean do.
						Number::Int(x) => Number::Float(*x as f64 / *count as f64).into(),
						Number::Float(x) => Number::Float(*x / *count as f64).into(),
						Number::Decimal(d) if d.scale() == 0 => {
							Number::Float(sum.to_float() / *count as f64).into()
						}
						Number::Decimal(_) => {
							sum.try_float_div((*count).into()).unwrap_or(f64::NAN.into()).into()
						}
					}
				}
			}
			AggregationStat::StdDev {
				sum,
				sum_of_squares,
				count,
				..
			} => shifted_sample_deviation(*sum, *sum_of_squares, *count).into(),
			AggregationStat::Variance {
				sum,
				sum_of_squares,
				count,
				..
			} => {
				let num = shifted_sample_variance(*sum, *sum_of_squares, *count);
				num.into()
			}
			// A group that has seen no datetime holds the stat's starting value,
			// and answers `NONE` as the ad hoc aggregate does.
			AggregationStat::TimeMax {
				max,
				..
			} => match *max == Datetime::MIN_UTC {
				true => Value::None,
				false => (*max).into(),
			},
			AggregationStat::TimeMin {
				min,
				..
			} => match *min == Datetime::MAX_UTC {
				true => Value::None,
				false => (*min).into(),
			},
			AggregationStat::Accumulate {
				values,
				..
			} => Value::Array(Array(values.clone())),
		};
		res.0.insert(aggregate_field_name(idx), value);
	}
	for (idx, g) in group.iter().enumerate() {
		res.0.insert(group_field_name(idx), g.clone());
	}
	res
}

/// Visitor which walks an expression to pull out the aggregate expressions to calculate.
struct AggregateExprCollector<'a> {
	support_acummulate: bool,
	within_aggregate_argument: bool,
	exprs_map: &'a mut HashMap<Expr, usize>,
	aggregations: &'a mut Vec<Aggregation>,
	groups: &'a Groups,
}

impl AggregateExprCollector<'_> {
	/// Convenience function to add an aggreagtion which takes a single argument.
	fn push_aggregate_function<F: Fn(usize) -> Aggregation>(
		&mut self,
		name: &str,
		args: &[Expr],
		f: F,
	) -> Result<()> {
		ensure!(
			args.len() == 1,
			ExprError::InvalidFunctionArguments {
				name: name.to_string(),
				message: "Expected 1 argument".to_string()
			}
		);
		let expr = args[0].clone();
		let len = self.exprs_map.len();
		let arg = *self.exprs_map.entry(expr).or_insert_with(|| len);
		self.aggregations.push(f(arg));
		Ok(())
	}
}

impl MutVisitor for AggregateExprCollector<'_> {
	type Error = anyhow::Error;

	fn visit_mut_expr(&mut self, s: &mut Expr) -> Result<(), Self::Error> {
		match s {
			Expr::FunctionCall(f) => {
				if let Function::Normal(x) = &f.receiver {
					match x.as_str() {
						"count" => {
							if f.arguments.is_empty() {
								self.aggregations.push(Aggregation::Count);
							} else {
								self.push_aggregate_function(
									"count",
									&f.arguments,
									Aggregation::CountValue,
								)?;
							}
						}
						"math::max" => {
							self.push_aggregate_function(
								"math::max",
								&f.arguments,
								Aggregation::NumberMax,
							)?;
						}
						"math::min" => {
							self.push_aggregate_function(
								"math::min",
								&f.arguments,
								Aggregation::NumberMin,
							)?;
						}
						"math::sum" => {
							self.push_aggregate_function(
								"math::sum",
								&f.arguments,
								Aggregation::Sum,
							)?;
						}
						"math::mean" => {
							self.push_aggregate_function(
								"math::mean",
								&f.arguments,
								Aggregation::Mean,
							)?;
						}
						"math::stddev" => {
							self.push_aggregate_function(
								"math::stddev",
								&f.arguments,
								Aggregation::StdDev,
							)?;
						}
						"math::variance" => {
							self.push_aggregate_function(
								"math::variance",
								&f.arguments,
								Aggregation::Variance,
							)?;
						}
						"time::max" => {
							self.push_aggregate_function(
								"time::max",
								&f.arguments,
								Aggregation::DatetimeMax,
							)?;
						}
						"time::min" => {
							self.push_aggregate_function(
								"time::min",
								&f.arguments,
								Aggregation::DatetimeMin,
							)?;
						}
						_ => {
							return f.visit_mut(self);
						}
					}
				} else {
					return f.visit_mut(self);
				}
				self.within_aggregate_argument = true;
				for a in f.arguments.iter_mut() {
					a.visit_mut(self)?;
				}
				self.within_aggregate_argument = false;
				// HACK: We replace the aggregate expression here with an field so that we can later
				// inject the value via the current doc.
				*s = Expr::Idiom(Idiom::field(aggregate_field_name(self.aggregations.len() - 1)));
				Ok(())
			}
			Expr::Param(p) => {
				if p.as_str() == "this" {
					bail!(Error::Query{
						message: "Found a `$this` parameter refering to the document of a group by select statement\n\
							Select statements with a group by currently have no defined document to refer to".to_string()
					});
				}
				Ok(())
			}
			Expr::Idiom(i) => {
				if !self.within_aggregate_argument {
					if let Some(group_idx) = self.groups.0.iter().position(|x| x.0 == *i) {
						i.visit_mut(self)?;
						// HACK: We replace the idioms which refer to the grouping expression here
						// with an field so that we can later inject the value via the current
						// doc.
						*s = Expr::Idiom(Idiom::field(group_field_name(group_idx)));
					} else if let Some(Part::Field(field)) = i.0.first_mut() {
						if self.support_acummulate {
							let field_name = mem::replace(
								field,
								// HACK: We replace the aggregate expression here with an field so
								// that we can later inject the value via the current doc.
								aggregate_field_name(self.aggregations.len()),
							);
							let len = self.exprs_map.len();
							let arg = *self
								.exprs_map
								.entry(Expr::Idiom(Idiom::field(field_name)))
								.or_insert_with(|| len);
							self.aggregations.push(Aggregation::Accumulate(arg))
						} else {
							bail!(Error::Query {
								message: format!(
									"Found idiom `{}` within the selector of a materialized aggregate view.\n\
											 Selection of document fields which are not used within the argument of an optimized aggregate function is currently not supported",
									i.to_sql()
								)
							})
						}
					}
					Ok(())
				} else {
					i.visit_mut(self)
				}
			}
			x => x.visit_mut(self),
		}
	}

	// ---------------
	// We need to avoid trying to find aggregates in places where there are no aggregates.
	// Take `SELECT (SELECT foo FROM ONLY { foo: math::mean(bar)  }) FROM foo GROUP ALL`
	// `foo` here has nothing to do as it is calculated in a different context fromn `bar`
	// which is part of the aggregate calculation.
	//
	//
	// The implementations below ensure that only the places where we are within the same
	// context are traversed.
	// ---------------

	fn visit_mut_create(&mut self, s: &mut CreateStatement) -> Result<(), Self::Error> {
		for e in s.what.iter_mut() {
			self.visit_mut_expr(e)?;
		}
		self.visit_mut_expr(&mut s.timeout)?;
		if let Some(d) = &mut s.data {
			ParentRewritor.visit_mut_data(d)?;
		}
		Ok(())
	}

	fn visit_mut_select(&mut self, s: &mut SelectStatement) -> Result<(), Self::Error> {
		for v in s.what.iter_mut() {
			self.visit_mut_expr(v)?;
		}
		if let Some(l) = s.limit.as_mut() {
			self.visit_mut_expr(&mut l.0)?;
		}
		self.visit_mut_expr(&mut s.version)?;

		ParentRewritor.visit_mut_fields(&mut s.fields)?;
		for o in s.omit.iter_mut() {
			ParentRewritor.visit_mut_expr(o)?;
		}
		if let Some(c) = s.cond.as_mut() {
			ParentRewritor.visit_mut_expr(&mut c.0)?;
		}
		if let Some(s) = s.split.as_mut() {
			for s in s.0.iter_mut() {
				ParentRewritor.visit_mut_idiom(&mut s.0)?;
			}
		}
		if let Some(g) = s.group.as_mut() {
			for g in g.0.iter_mut() {
				ParentRewritor.visit_mut_idiom(&mut g.0)?;
			}
		}
		if let Some(o) = s.order.as_mut() {
			ParentRewritor.visit_mut_ordering(o)?;
		}
		if let Some(f) = s.fetch.as_mut() {
			for f in f.iter_mut() {
				ParentRewritor.visit_mut_expr(&mut f.0)?;
			}
		}

		Ok(())
	}

	fn visit_mut_update(&mut self, s: &mut UpdateStatement) -> Result<(), Self::Error> {
		for e in s.what.iter_mut() {
			self.visit_mut_expr(e)?;
		}
		if let Some(e) = &mut s.data {
			ParentRewritor.visit_mut_data(e)?;
		}
		if let Some(e) = &mut s.cond {
			ParentRewritor.visit_mut_expr(&mut e.0)?;
		}
		self.visit_mut_expr(&mut s.timeout)?;
		Ok(())
	}

	fn visit_mut_upsert(&mut self, s: &mut UpsertStatement) -> Result<(), Self::Error> {
		for e in s.what.iter_mut() {
			self.visit_mut_expr(e)?;
		}
		if let Some(d) = &mut s.data {
			ParentRewritor.visit_mut_data(d)?;
		}
		if let Some(e) = &mut s.cond {
			ParentRewritor.visit_mut_expr(&mut e.0)?;
		}
		self.visit_mut_expr(&mut s.timeout)?;
		Ok(())
	}

	fn visit_mut_relate(&mut self, s: &mut RelateStatement) -> Result<(), Self::Error> {
		self.visit_mut_expr(&mut s.through)?;
		self.visit_mut_expr(&mut s.from)?;
		self.visit_mut_expr(&mut s.to)?;
		self.visit_mut_expr(&mut s.timeout)?;

		if let Some(d) = s.data.as_mut() {
			ParentRewritor.visit_mut_data(d)?;
		}
		if let Some(o) = s.output.as_mut() {
			ParentRewritor.visit_mut_output(o)?;
		}
		Ok(())
	}

	fn visit_mut_insert(&mut self, i: &mut InsertStatement) -> Result<(), Self::Error> {
		if let Some(into) = &mut i.into {
			self.visit_mut_expr(into)?;
		}
		self.visit_mut_expr(&mut i.timeout)?;

		ParentRewritor.visit_mut_data(&mut i.data)?;
		if let Some(update) = i.update.as_mut() {
			ParentRewritor.visit_mut_data(update)?;
		}
		if let Some(o) = i.output.as_mut() {
			ParentRewritor.visit_mut_output(o)?;
		}
		Ok(())
	}

	fn visit_mut_define_api(
		&mut self,
		d: &mut DefineApiStatement,
	) -> std::result::Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.path)?;
		self.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_function(
		&mut self,
		d: &mut DefineFunctionStatement,
	) -> std::result::Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_access(
		&mut self,
		d: &mut DefineAccessStatement,
	) -> std::result::Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.name)?;
		self.visit_mut_expr(&mut d.comment)?;
		self.visit_mut_expr(&mut d.duration.grant)?;
		self.visit_mut_expr(&mut d.duration.token)?;
		self.visit_mut_expr(&mut d.duration.session)?;
		Ok(())
	}

	fn visit_mut_define_index(&mut self, d: &mut DefineIndexStatement) -> Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.name)?;
		self.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_field(&mut self, d: &mut DefineFieldStatement) -> Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.name)?;
		self.visit_mut_expr(&mut d.what)?;
		self.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}
}

// Rewrites all `$parent` parameters that are evauluated in the current context to `None`
struct ParentRewritor;

impl MutVisitor for ParentRewritor {
	type Error = anyhow::Error;

	fn visit_mut_expr(&mut self, e: &mut Expr) -> Result<(), Self::Error> {
		if let Expr::Param(p) = e
			&& p.as_str() == "parent"
		{
			return Err(Error::Query{
					message: "Found a `$parent` parameter refering to the document of a GROUP select statement\n\
						Select statements with a GROUP BY or GROUP ALL currently have no defined document to refer to".to_string()
				}.into());
		}
		e.visit_mut(self)
	}

	fn visit_mut_create(&mut self, s: &mut CreateStatement) -> Result<(), Self::Error> {
		for e in s.what.iter_mut() {
			self.visit_mut_expr(e)?;
		}
		self.visit_mut_expr(&mut s.timeout)?;
		Ok(())
	}

	fn visit_mut_select(&mut self, s: &mut SelectStatement) -> Result<(), Self::Error> {
		self.visit_mut_fields(&mut s.fields)?;
		for v in s.what.iter_mut() {
			self.visit_mut_expr(v)?;
		}
		if let Some(l) = s.limit.as_mut() {
			self.visit_mut_expr(&mut l.0)?;
		}
		self.visit_mut_expr(&mut s.version)?;
		Ok(())
	}

	fn visit_mut_update(&mut self, s: &mut UpdateStatement) -> Result<(), Self::Error> {
		for e in s.what.iter_mut() {
			self.visit_mut_expr(e)?;
		}

		self.visit_mut_expr(&mut s.timeout)?;
		Ok(())
	}

	fn visit_mut_upsert(&mut self, s: &mut UpsertStatement) -> Result<(), Self::Error> {
		for e in s.what.iter_mut() {
			self.visit_mut_expr(e)?;
		}
		self.visit_mut_expr(&mut s.timeout)?;
		Ok(())
	}

	fn visit_mut_relate(&mut self, s: &mut RelateStatement) -> Result<(), Self::Error> {
		self.visit_mut_expr(&mut s.through)?;
		self.visit_mut_expr(&mut s.from)?;
		self.visit_mut_expr(&mut s.to)?;
		self.visit_mut_expr(&mut s.timeout)?;

		Ok(())
	}

	fn visit_mut_insert(&mut self, i: &mut InsertStatement) -> Result<(), Self::Error> {
		if let Some(into) = &mut i.into {
			self.visit_mut_expr(into)?;
		}
		self.visit_mut_expr(&mut i.timeout)?;
		Ok(())
	}

	fn visit_mut_define_api(
		&mut self,
		d: &mut DefineApiStatement,
	) -> std::result::Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.path)?;
		self.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_permission(&mut self, _: &mut super::Permission) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_define_config(
		&mut self,
		_: &mut DefineConfigStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_define_function(
		&mut self,
		_: &mut DefineFunctionStatement,
	) -> std::result::Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_define_access(
		&mut self,
		d: &mut DefineAccessStatement,
	) -> std::result::Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.name)?;
		self.visit_mut_expr(&mut d.comment)?;
		self.visit_mut_expr(&mut d.duration.grant)?;
		self.visit_mut_expr(&mut d.duration.token)?;
		self.visit_mut_expr(&mut d.duration.session)?;
		Ok(())
	}

	fn visit_mut_define_index(&mut self, d: &mut DefineIndexStatement) -> Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.name)?;
		self.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}

	fn visit_mut_define_field(&mut self, d: &mut DefineFieldStatement) -> Result<(), Self::Error> {
		self.visit_mut_expr(&mut d.name)?;
		self.visit_mut_expr(&mut d.what)?;
		self.visit_mut_expr(&mut d.comment)?;
		Ok(())
	}
}

/// Enum for the field expression of an aggregate.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AggregateFields {
	/// the selector had a `VALUE` clause
	Value(Expr),
	/// Normal selector.
	Fields(Vec<(Idiom, Expr)>),
}

/// A struct which contains an anaylzed aggregation and data on how to compute that aggregation.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AggregationAnalysis {
	/// The expressions which calculate the arguments to an aggregate.
	pub aggregate_arguments: Vec<Expr>,
	/// The aggregated expressions that are calculated.
	pub aggregations: Vec<Aggregation>,
	/// The expressions which identify the group.
	pub group_expressions: Vec<Expr>,
	/// The expression to compute the resulting object from the calculated aggregates.
	pub fields: AggregateFields,
}

impl AggregationAnalysis {
	/// Analyze the groups and fields and produce a analysis for how to run the aggregate
	/// expressions.
	///
	/// if the `materialized_view` argument is true the function will add a `Count` aggregation when
	/// there is no aggregate which maintains a per group record count and will reject any
	/// accumulate aggregations as we currently don't have a way to support them on
	/// materialized views.
	pub fn analyze_fields_groups(
		fields: &Fields,
		groups: &Groups,
		materialized_view: bool,
	) -> Result<Self> {
		// Find all the aggregates within the select statement.
		let mut aggregations = Vec::new();
		let mut exprs_map = HashMap::default();
		let mut group_expressions = Vec::with_capacity(groups.len());

		for g in groups.0.iter() {
			group_expressions.push(Expr::Idiom(g.0.clone()))
		}

		let mut collect = AggregateExprCollector {
			support_acummulate: !materialized_view,
			within_aggregate_argument: false,
			exprs_map: &mut exprs_map,
			aggregations: &mut aggregations,
			groups,
		};

		// Collect the expressions which calculate the fields of the final object after
		// aggregation.
		let fields = match fields {
			Fields::Value(field) => {
				// alias is unused when using a value selector.
				let mut expr = field.expr.clone();
				collect.visit_mut_expr(&mut expr)?;
				AggregateFields::Value(expr)
			}
			Fields::Select(fields) => {
				let mut collect_fields = Vec::with_capacity(fields.len());
				for f in fields.iter() {
					let Field::Single(Selector {
						expr,
						alias,
					}) = f
					else {
						// all is not a valid aggregate selector.
						bail!(Error::InvalidAggregationSelector {
							expr: f.to_sql()
						})
					};

					// An alias names a group as `GROUP BY` wrote it, which `group_expressions`
					// stops holding once an earlier field has replaced that entry with its
					// expression: in `SELECT g1 AS flag, g0 AS g1 ... GROUP BY flag, g1` the
					// entry of `flag` becomes the path `g1`, and the second field's alias must
					// still find the group `g1`.
					if let Some((alias, x)) = alias.as_ref().and_then(|alias| {
						groups.0.iter().position(|g| g.0 == *alias).map(|x| (alias, x))
					}) {
						// The alias is used within the group statement, therefore this expression
						// is a group expression. i.e. year in the statement:
						// `SELECT time::year(time) as year FROM table GROUP BY year`;
						// Replace the calculated group with the expression and then add it to the
						// collected fields via a retrieval from the group.
						group_expressions[x] = expr.clone();
						collect_fields
							.push((alias.clone(), Expr::Idiom(Idiom::field(group_field_name(x)))));
					} else {
						let name = alias.clone().unwrap_or_else(|| expr.to_idiom());
						let mut expr = expr.clone();
						collect.visit_mut_expr(&mut expr)?;
						collect_fields.push((name, expr))
					}
				}
				AggregateFields::Fields(collect_fields)
			}
		};

		// Place the expression which need to be calculated for the aggregate in the right index.
		let mut aggregate_arguments = Vec::with_capacity(exprs_map.len());
		for (k, v) in exprs_map {
			if aggregate_arguments.len() > v {
				aggregate_arguments[v] = k
			} else {
				for _ in aggregate_arguments.len()..v {
					// push a temp expression that will be overwritten while we collect all the
					// expressions.
					aggregate_arguments.push(Expr::Break)
				}
				aggregate_arguments.push(k)
			}
		}

		// Ensure there is atleast one count aggregation to delete the record when the number of
		// entries reaches zero.
		if materialized_view
			&& !aggregations.iter().any(|x| {
				matches!(
					x,
					Aggregation::Count
						| Aggregation::Mean(_)
						| Aggregation::StdDev(_)
						| Aggregation::Variance(_)
				)
			}) {
			aggregations.push(Aggregation::Count)
		}

		Ok(Self {
			aggregations,
			aggregate_arguments,
			group_expressions,
			fields,
		})
	}
}

/// A value the select that fills an aggregated view computes for each group.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum BackfillValue {
	/// The result of the aggregate itself.
	Aggregate(Aggregation),
	/// The exact sum of aggregate argument `n` for reconstructing a mean's
	/// running total, widening integers to `Decimal` before summing so totals
	/// outside `i64` do not overflow `math::sum`.
	MeanSum(usize),
	/// The accurate two-pass sample variance of aggregate argument `n`, from
	/// which a variance or deviation's shifted state is reconstructed
	/// ([`shifted_state_from_summary`]).
	SampleVariance(usize),
}

impl BackfillValue {
	/// The expression that computes this value over a group's rows.
	fn expr(&self, analysis: &AggregationAnalysis) -> Result<Expr> {
		use crate::expr::FunctionCall;
		let call = |name: &str, arg: Option<usize>| {
			Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal(name.to_string()),
				arguments: arg
					.map(|arg| analysis.aggregate_arguments[arg].clone())
					.into_iter()
					.collect(),
			}))
		};
		Ok(match self {
			BackfillValue::MeanSum(arg) => {
				let arg = analysis.aggregate_arguments[*arg].clone();
				let widened = Expr::IfElse(Box::new(crate::expr::statements::IfelseStatement {
					exprs: vec![(
						Expr::FunctionCall(Box::new(FunctionCall {
							receiver: Function::Normal("type::is_int".to_string()),
							arguments: vec![arg.clone()],
						})),
						Expr::Prefix {
							op: crate::expr::PrefixOperator::Cast(crate::expr::Kind::Decimal),
							expr: Box::new(arg.clone()),
						},
					)],
					close: Some(arg),
				}));
				Expr::FunctionCall(Box::new(FunctionCall {
					receiver: Function::Normal("math::sum".to_string()),
					arguments: vec![widened],
				}))
			}
			BackfillValue::SampleVariance(arg) => call("math::variance", Some(*arg)),
			BackfillValue::Aggregate(aggregation) => match aggregation {
				Aggregation::Count => call("count", None),
				Aggregation::CountValue(arg) => call("count", Some(*arg)),
				Aggregation::NumberMax(arg) => call("math::max", Some(*arg)),
				Aggregation::NumberMin(arg) => call("math::min", Some(*arg)),
				Aggregation::Sum(arg) => call("math::sum", Some(*arg)),
				Aggregation::Mean(arg) => call("math::mean", Some(*arg)),
				Aggregation::DatetimeMax(arg) => call("time::max", Some(*arg)),
				Aggregation::DatetimeMin(arg) => call("time::min", Some(*arg)),
				Aggregation::StdDev(_) | Aggregation::Variance(_) => {
					fail!("A deviation or variance is computed from its mean and sample variance")
				}
				Aggregation::Accumulate(_) => {
					fail!("Accumulate aggregation is not supported in materialized views")
				}
			},
		})
	}
}

impl AggregationAnalysis {
	/// The values the select that fills the view computes per group, in the
	/// order of the array it answers them in: each aggregate the view stores
	/// needs its own value, a mean the sum and the count behind it, and a
	/// variance or deviation the mean, the sample variance and the count.
	pub fn backfill_values(&self) -> Result<Vec<BackfillValue>> {
		let mut values = Vec::new();
		let mut want = |value: BackfillValue| {
			if !values.contains(&value) {
				values.push(value);
			}
		};
		for aggregation in self.aggregations.iter() {
			match aggregation {
				Aggregation::Count
				| Aggregation::CountValue(_)
				| Aggregation::NumberMax(_)
				| Aggregation::NumberMin(_)
				| Aggregation::Sum(_)
				| Aggregation::DatetimeMax(_)
				| Aggregation::DatetimeMin(_) => {
					want(BackfillValue::Aggregate(aggregation.clone()));
				}
				Aggregation::Mean(arg) => {
					want(BackfillValue::MeanSum(*arg));
					want(BackfillValue::Aggregate(Aggregation::Count));
				}
				Aggregation::StdDev(arg) | Aggregation::Variance(arg) => {
					want(BackfillValue::Aggregate(Aggregation::Mean(*arg)));
					want(BackfillValue::SampleVariance(*arg));
					want(BackfillValue::Aggregate(Aggregation::Count));
				}
				Aggregation::Accumulate(_) => {
					fail!("Accumulate aggregation is not supported in materialized views")
				}
			}
		}
		Ok(values)
	}

	/// The select that computes every group of the view over `tables` and
	/// `condition`: the group keys as `g0`, `g1`, … and the array of `values`
	/// ([`Self::backfill_values`]) as `a`, grouped by those keys.
	pub fn backfill_select(
		&self,
		values: &[BackfillValue],
		tables: &[surrealdb_strand::TableName],
		condition: Option<&Expr>,
	) -> Result<SelectStatement> {
		use crate::expr::{Cond, Group, Literal};
		let mut fields = Vec::new();
		let mut groups = Vec::new();
		for (idx, g) in self.group_expressions.iter().enumerate() {
			let alias = format!("g{}", idx);
			fields.push(Field::Single(Selector {
				expr: g.clone(),
				alias: Some(Idiom::field(alias.clone())),
			}));
			groups.push(Group(Idiom::field(alias)));
		}
		let computed = values.iter().map(|value| value.expr(self)).collect::<Result<Vec<_>>>()?;
		fields.push(Field::Single(Selector {
			expr: Expr::Literal(Literal::Array(computed)),
			alias: Some(Idiom::field("a".to_string())),
		}));
		Ok(SelectStatement {
			fields: Fields::Select(fields),
			cond: condition.cloned().map(Cond),
			group: Some(Groups(groups)),
			what: tables.iter().map(|x| Expr::Table(x.clone())).collect(),
			omit: vec![],
			only: false,
			with: None,
			split: None,
			order: None,
			limit: None,
			start: None,
			fetch: None,
			version: Expr::Literal(Literal::None),
			timeout: Expr::Literal(Literal::None),
			explain: None,
			tempfiles: false,
			for_update: false,
		})
	}
}

/// The stats of one group of an aggregated view from the array `a` of the
/// select that fills it ([`AggregationAnalysis::backfill_select`]), or `None`
/// for a row that stands for no group.
///
/// A view row stands for a group that source rows are in, and the incremental
/// path keeps it that way: it creates the row with the group's first row and
/// deletes it with the group's last. `GROUP ALL` answers a query over no rows
/// with the grand total, which stands for no group and whose extremes are each
/// aggregate's answer over no values rather than a group's. Every view's
/// analysis carries a count, which is what tells the two apart.
pub fn backfill_stats(
	aggregations: &[Aggregation],
	values: &[BackfillValue],
	computed: &[Value],
) -> Result<Option<Vec<AggregationStat>>> {
	let at = |value: BackfillValue| -> Result<&Value> {
		let Some(idx) = values.iter().position(|v| *v == value) else {
			fail!("a backfill value the view's aggregations did not include")
		};
		let Some(value) = computed.get(idx) else {
			fail!("select result did not contain a value for an aggregation")
		};
		Ok(value)
	};
	let count_of = || -> Result<i64> {
		let Value::Number(Number::Int(count)) = at(BackfillValue::Aggregate(Aggregation::Count))?
		else {
			fail!("initial select statement did not return the right value")
		};
		Ok(*count)
	};
	let number_of = |value: BackfillValue| -> Result<Number> {
		let Value::Number(n) = at(value)? else {
			fail!("initial select statement did not return the right value")
		};
		Ok(*n)
	};
	let datetime_of = |value: BackfillValue| -> Result<Datetime> {
		let Value::Datetime(d) = at(value)? else {
			fail!("initial select statement did not return the right value")
		};
		Ok(*d)
	};
	if !values.contains(&BackfillValue::Aggregate(Aggregation::Count)) {
		fail!("a materialised view's aggregations did not include a count");
	}
	if count_of()? == 0 {
		return Ok(None);
	}
	let mut stats = Vec::with_capacity(aggregations.len());
	for a in aggregations.iter() {
		stats.push(match *a {
			Aggregation::Count => AggregationStat::Count {
				count: count_of()?,
			},
			Aggregation::CountValue(arg) => {
				let Value::Number(Number::Int(count)) =
					at(BackfillValue::Aggregate(Aggregation::CountValue(arg)))?
				else {
					fail!("initial select statement did not return the right value")
				};
				AggregationStat::CountValue {
					arg,
					count: *count,
				}
			}
			Aggregation::NumberMax(arg) => AggregationStat::NumberMax {
				arg,
				max: number_of(BackfillValue::Aggregate(Aggregation::NumberMax(arg)))?,
			},
			Aggregation::NumberMin(arg) => AggregationStat::NumberMin {
				arg,
				min: number_of(BackfillValue::Aggregate(Aggregation::NumberMin(arg)))?,
			},
			// A sum is kept from an integer zero, as [`Aggregation::to_stat`]
			// starts one, so the view holds the type the incremental path gives
			// it: integers stay exact integers.
			Aggregation::Sum(arg) => AggregationStat::Sum {
				arg,
				sum: Number::Int(0)
					.try_add(number_of(BackfillValue::Aggregate(Aggregation::Sum(arg)))?)?,
			},
			Aggregation::Mean(arg) => {
				let sum = match number_of(BackfillValue::MeanSum(arg))? {
					Number::Decimal(d) if d.scale() == 0 => {
						i64::try_from(d).map(Number::Int).unwrap_or(Number::Decimal(d))
					}
					n => Number::Int(0).add_exact(n)?,
				};
				AggregationStat::Mean {
					arg,
					sum,
					count: count_of()?,
				}
			}
			Aggregation::DatetimeMax(arg) => AggregationStat::TimeMax {
				arg,
				max: datetime_of(BackfillValue::Aggregate(Aggregation::DatetimeMax(arg)))?,
			},
			Aggregation::DatetimeMin(arg) => AggregationStat::TimeMin {
				arg,
				min: datetime_of(BackfillValue::Aggregate(Aggregation::DatetimeMin(arg)))?,
			},
			Aggregation::StdDev(arg) | Aggregation::Variance(arg) => {
				let mean = number_of(BackfillValue::Aggregate(Aggregation::Mean(arg)))?;
				let variance = number_of(BackfillValue::SampleVariance(arg))?;
				let count = count_of()?;
				let (shift, sum, sum_of_squares) =
					shifted_state_from_summary(mean, variance, count)?;
				if matches!(a, Aggregation::StdDev(_)) {
					AggregationStat::StdDev {
						arg,
						shift,
						sum,
						sum_of_squares,
						count,
					}
				} else {
					AggregationStat::Variance {
						arg,
						shift,
						sum,
						sum_of_squares,
						count,
					}
				}
			}
			Aggregation::Accumulate(_) => {
				fail!("Accumulate aggregation is not supported in materialized views")
			}
		});
	}
	Ok(Some(stats))
}

#[cfg(test)]
mod analysis_tests {
	use super::*;
	use crate::expr::{FunctionCall, Group};

	fn path(name: &str) -> Idiom {
		Idiom::field(name.to_owned())
	}

	fn column(from: &str, alias: &str) -> Field {
		Field::Single(Selector {
			expr: Expr::Idiom(path(from)),
			alias: Some(path(alias)),
		})
	}

	fn count(alias: &str) -> Field {
		Field::Single(Selector {
			expr: Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal("count".to_string()),
				arguments: Vec::new(),
			})),
			alias: Some(path(alias)),
		})
	}

	fn group_read(idx: usize) -> Expr {
		Expr::Idiom(Idiom::field(group_field_name(idx)))
	}

	/// A column's alias names the group as `GROUP BY` wrote it. An earlier column
	/// that is a path of the same name as this alias has replaced that group's
	/// entry, and must not take this column's place.
	#[test]
	fn an_alias_finds_its_group_after_an_earlier_column_replaced_the_group_named_like_it() {
		let fields = Fields::Select(vec![column("g1", "flag"), column("g0", "g1"), count("c")]);
		let groups = Groups(vec![Group(path("flag")), Group(path("g1"))]);
		let analysis = AggregationAnalysis::analyze_fields_groups(&fields, &groups, true)
			.expect("the view's fields and groups analyse");
		assert_eq!(
			analysis.group_expressions,
			vec![Expr::Idiom(path("g1")), Expr::Idiom(path("g0"))]
		);
		let AggregateFields::Fields(columns) = &analysis.fields else {
			panic!("a view of columns analyses to columns");
		};
		assert_eq!(columns[0], (path("flag"), group_read(0)));
		assert_eq!(columns[1], (path("g1"), group_read(1)));
	}

	/// Two columns that name each other's source, each grouped by its alias.
	#[test]
	fn columns_that_swap_the_names_of_their_sources_keep_their_own_groups() {
		let fields = Fields::Select(vec![column("g1", "g0"), column("g0", "g1"), count("c")]);
		let groups = Groups(vec![Group(path("g0")), Group(path("g1"))]);
		let analysis = AggregationAnalysis::analyze_fields_groups(&fields, &groups, true)
			.expect("the view's fields and groups analyse");
		assert_eq!(
			analysis.group_expressions,
			vec![Expr::Idiom(path("g1")), Expr::Idiom(path("g0"))]
		);
		let AggregateFields::Fields(columns) = &analysis.fields else {
			panic!("a view of columns analyses to columns");
		};
		assert_eq!(columns[0], (path("g0"), group_read(0)));
		assert_eq!(columns[1], (path("g1"), group_read(1)));
	}
}

#[cfg(test)]
mod backfill_tests {
	use super::*;

	/// A variance needs the mean, the sample variance and the count behind
	/// it, each once, and a mean needs the sum and the count.
	#[test]
	fn a_backfill_computes_each_value_the_stats_are_made_from_once() {
		let analysis = AggregationAnalysis {
			aggregate_arguments: vec![Expr::Idiom(Idiom::field("x".to_owned()))],
			aggregations: vec![Aggregation::Variance(0), Aggregation::Mean(0), Aggregation::Count],
			group_expressions: Vec::new(),
			fields: AggregateFields::Fields(Vec::new()),
		};
		let values = analysis.backfill_values().expect("the aggregations are computable");
		assert_eq!(
			values,
			vec![
				BackfillValue::Aggregate(Aggregation::Mean(0)),
				BackfillValue::SampleVariance(0),
				BackfillValue::Aggregate(Aggregation::Count),
				BackfillValue::MeanSum(0),
			]
		);
	}

	/// A row with no records in its group stands for no group.
	#[test]
	fn a_backfill_row_counting_no_records_has_no_stats() {
		let values = vec![BackfillValue::Aggregate(Aggregation::Count)];
		let none = backfill_stats(&[Aggregation::Count], &values, &[Value::from(0i64)]);
		assert!(matches!(none, Ok(None)));
		let some = backfill_stats(&[Aggregation::Count], &values, &[Value::from(3i64)])
			.expect("the count reads")
			.expect("three records stand for a group");
		assert!(matches!(
			some.as_slice(),
			[AggregationStat::Count {
				count: 3
			}]
		));
	}
}
