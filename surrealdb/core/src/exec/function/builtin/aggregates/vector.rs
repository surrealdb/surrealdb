//! Vector aggregate functions.
//!
//! Provides aggregates that fold a vector-valued expression across the rows of
//! a group.

use anyhow::{Result, bail};
use surrealdb_runtime::util::math::vector::add_assign;

use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::expr::{Error as ExprError, Kind};
use crate::val::{Number, Value};

// ============================================================================
// Sum
// ============================================================================

/// vector::sum - sums vectors elementwise across the rows of a group
#[derive(Debug, Clone, Copy, Default)]
pub struct VectorSum;

impl AggregateFunction for VectorSum {
	fn name(&self) -> &'static str {
		"vector::sum"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(VectorSumAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("vector", Kind::Any).returns(Kind::Array(Box::new(Kind::Number), None))
	}
}

/// Running elementwise total for [`VectorSum`].
///
/// State is a single vector of the group's dimension, so a group costs
/// O(dimension) however many rows it holds, and two partial totals combine by
/// adding one into the other.
///
/// `NONE` and `NULL` rows contribute nothing, so a sparsely populated vector
/// field still totals the rows that do have one. Any other non-vector value, or
/// a vector whose dimension disagrees with the running total, instead leaves the
/// group's total undefined: a partial sum over an inconsistent group is
/// indistinguishable from a correct one, so [`Accumulator::finalize`] fails for
/// the whole group rather than returning a total that silently omits rows. The
/// aggregate operator renders that failure as `NULL` for the field, keeping one
/// bad group from failing the surrounding query.
#[derive(Debug, Clone, Default)]
struct VectorSumAccumulator {
	total: Option<Vec<Number>>,
	/// Why the group's total is undefined, if a row ruled it out. Once set, all
	/// further rows are ignored and `finalize` reports this reason.
	undefined: Option<String>,
}

impl VectorSumAccumulator {
	/// Handle a value without taking ownership: a missing vector to skip, or a
	/// vector already shaped like the running total to add in place. Adding from
	/// the borrowed array costs no allocation, which both the per-row GROUP BY
	/// path and the batched GROUP ALL path take for every row after the first.
	///
	/// Returns whether the value was fully handled. A first vector establishing
	/// the dimension, a differing dimension, and a non-vector all need
	/// [`Self::add_coerced`] instead.
	fn add_borrowed(&mut self, value: &Value) -> bool {
		if matches!(value, Value::None | Value::Null) {
			return true;
		}
		if let Some(total) = self.total.as_mut()
			&& let Value::Array(array) = value
			&& array.len() == total.len()
			&& array.iter().all(|v| matches!(v, Value::Number(_)))
		{
			for (t, v) in total.iter_mut().zip(array.iter()) {
				if let Value::Number(n) = v {
					*t = *t + *n;
				}
			}
			return true;
		}
		false
	}

	/// Coerce a value to a vector and fold it into the running total,
	/// establishing the group's dimension if this is the first vector seen.
	///
	/// Coercion is exactly the scalar `vector::sum`'s, so both forms accept the
	/// same inputs. A rejection is recorded under the same
	/// `Incorrect arguments for function vector::sum()` prefix the scalar form
	/// carries; the two differ only in the argument-position detail the scalar's
	/// collection argument adds.
	fn add_coerced(&mut self, value: Value) {
		let vector = match value.coerce_to::<Vec<Number>>() {
			Ok(vector) => vector,
			Err(e) => {
				self.undefined = Some(
					ExprError::InvalidFunctionArguments {
						name: String::from("vector::sum"),
						message: e.to_string(),
					}
					.to_string(),
				);
				return;
			}
		};
		match &mut self.total {
			// `add_assign` reports a dimension mismatch against `vector::sum`,
			// so its message already carries the prefix.
			Some(total) => {
				if let Err(e) = add_assign("vector::sum", total, &vector) {
					self.undefined = Some(e.to_string());
				}
			}
			None => self.total = Some(vector),
		}
	}
}

impl Accumulator for VectorSumAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if self.undefined.is_some() {
			return Ok(());
		}
		if !self.add_borrowed(&value) {
			self.add_coerced(value);
		}
		Ok(())
	}

	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			if self.undefined.is_some() {
				return Ok(());
			}
			// Only a value the borrowed path cannot fold needs cloning to be
			// coerced.
			if !self.add_borrowed(value) {
				self.add_coerced(value.clone());
			}
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<VectorSumAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		if self.undefined.is_some() {
			return Ok(());
		}
		// An undefined partial total makes the combined total undefined too.
		if let Some(reason) = &other.undefined {
			self.undefined = Some(reason.clone());
			return Ok(());
		}
		if let Some(vector) = &other.total {
			match &mut self.total {
				Some(total) => {
					if let Err(e) = add_assign("vector::sum", total, vector) {
						self.undefined = Some(e.to_string());
					}
				}
				None => self.total = Some(vector.clone()),
			}
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		if let Some(reason) = &self.undefined {
			bail!("{reason}");
		}
		match &self.total {
			// No vector was seen, so there is no dimension to return a zero
			// vector for.
			None => Ok(Value::None),
			Some(total) => Ok(total.iter().copied().map(Value::from).collect::<Vec<_>>().into()),
		}
	}

	fn reset(&mut self) {
		self.total = None;
		self.undefined = None;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const PREFIX: &str = "Incorrect arguments for function vector::sum().";

	fn vector(values: [i64; 3]) -> Value {
		Value::Array(values.into_iter().map(|v| Value::from(Number::Int(v))).collect())
	}

	fn accumulator() -> Box<dyn Accumulator> {
		VectorSum.create_accumulator()
	}

	#[test]
	fn empty_group_has_no_dimension_to_total() {
		assert_eq!(accumulator().finalize().unwrap(), Value::None);
	}

	#[test]
	fn missing_vectors_contribute_nothing() {
		let mut acc = accumulator();
		acc.update(Value::None).unwrap();
		acc.update(vector([1, 2, 3])).unwrap();
		acc.update(Value::Null).unwrap();
		assert_eq!(acc.finalize().unwrap(), vector([1, 2, 3]));
	}

	#[test]
	fn a_group_of_only_missing_vectors_totals_to_none() {
		let mut acc = accumulator();
		acc.update(Value::None).unwrap();
		acc.update(Value::Null).unwrap();
		assert_eq!(acc.finalize().unwrap(), Value::None);
	}

	/// The per-row and batched paths take different routes into the running
	/// total — `update_batch` folds from the borrowed array where it can — so
	/// they must agree on the result.
	#[test]
	fn per_row_and_batched_updates_agree() {
		let rows = [vector([1, 0, 0]), vector([0, 2, 0]), vector([0, 0, 3])];

		let mut per_row = accumulator();
		for row in rows.clone() {
			per_row.update(row).unwrap();
		}

		let mut batched = accumulator();
		batched.update_batch(&rows).unwrap();

		assert_eq!(per_row.finalize().unwrap(), vector([1, 2, 3]));
		assert_eq!(batched.finalize().unwrap(), per_row.finalize().unwrap());
	}

	#[test]
	fn merging_partial_totals_adds_them() {
		let mut left = accumulator();
		left.update(vector([1, 2, 3])).unwrap();
		let mut right = accumulator();
		right.update(vector([10, 20, 30])).unwrap();

		left.merge(right.clone_box()).unwrap();
		assert_eq!(left.finalize().unwrap(), vector([11, 22, 33]));
	}

	#[test]
	fn merging_into_an_empty_total_adopts_the_other() {
		let mut empty = accumulator();
		let mut other = accumulator();
		other.update(vector([1, 2, 3])).unwrap();

		empty.merge(other.clone_box()).unwrap();
		assert_eq!(empty.finalize().unwrap(), vector([1, 2, 3]));
	}

	#[test]
	fn merging_an_undefined_total_makes_the_result_undefined() {
		let mut poisoned = accumulator();
		poisoned.update(vector([1, 2, 3])).unwrap();
		poisoned.update(Value::from(Number::Int(7))).unwrap();

		let mut left = accumulator();
		left.update(vector([1, 2, 3])).unwrap();
		left.merge(poisoned.clone_box()).unwrap();

		assert!(left.finalize().is_err());
	}

	/// A dimension mismatch must not yield a total summed from only the rows
	/// that happened to agree.
	#[test]
	fn a_dimension_mismatch_leaves_the_total_undefined() {
		let mut acc = accumulator();
		acc.update(vector([1, 2, 3])).unwrap();
		acc.update(Value::Array(vec![Value::from(Number::Int(1))].into())).unwrap();

		let err = acc.finalize().unwrap_err().to_string();
		assert!(err.starts_with(PREFIX), "reason lost the function prefix: {err}");
		assert!(err.contains("same dimension"), "unexpected reason: {err}");
	}

	/// `finalize` errors are rendered as `NULL` by the aggregate operator, so
	/// the reason is only ever read from a log — but it must still name the
	/// function, matching the scalar form, in case that changes.
	#[test]
	fn a_non_vector_reason_carries_the_function_prefix() {
		let mut acc = accumulator();
		acc.update(Value::from(Number::Int(7))).unwrap();

		let err = acc.finalize().unwrap_err().to_string();
		assert!(err.starts_with(PREFIX), "reason lost the function prefix: {err}");
	}

	#[test]
	fn reset_clears_both_the_total_and_the_undefined_reason() {
		let mut acc = accumulator();
		acc.update(Value::from(Number::Int(7))).unwrap();
		assert!(acc.finalize().is_err());

		acc.reset();
		acc.update(vector([1, 2, 3])).unwrap();
		assert_eq!(acc.finalize().unwrap(), vector([1, 2, 3]));
	}
}
