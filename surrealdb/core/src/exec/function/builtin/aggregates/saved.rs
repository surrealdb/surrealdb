//! An accumulator's partial state as a value, for an aggregate that spills
//! its groups (`exec::vm::groups`).
//!
//! A saved state is an array of the accumulator's data fields in a fixed
//! order; what the aggregate function configures (a separator, an extremum,
//! a spread's kind) is not saved, since a state is only loaded into a fresh
//! accumulator of the same function. The encoding lives only as long as the
//! query that spilled it.

use anyhow::{Context as _, Result};

use super::Rejected;
use crate::val::{Datetime, Number, Value};

/// A state being saved, a field at a time.
#[derive(Default)]
pub(super) struct Saving(Vec<Value>);

impl Saving {
	pub(super) fn number(mut self, n: Number) -> Self {
		self.0.push(Value::Number(n));
		self
	}

	pub(super) fn int(self, n: i64) -> Self {
		self.number(Number::Int(n))
	}

	pub(super) fn option_number(mut self, n: Option<Number>) -> Self {
		self.0.push(n.map(Value::Number).unwrap_or(Value::None));
		self
	}

	pub(super) fn option_datetime(mut self, d: Option<Datetime>) -> Self {
		self.0.push(d.map(Value::Datetime).unwrap_or(Value::None));
		self
	}

	pub(super) fn option_string(mut self, s: &Option<String>) -> Self {
		self.0.push(s.as_ref().map(|s| Value::String(s.as_str().into())).unwrap_or(Value::None));
		self
	}

	pub(super) fn option_value(mut self, v: &Option<Value>) -> Self {
		self.0.push(v.clone().unwrap_or(Value::None));
		// A present `NONE` and an absent value are told apart by a flag.
		self.0.push(Value::Bool(v.is_some()));
		self
	}

	pub(super) fn values(mut self, values: &[Value]) -> Self {
		self.0.push(Value::Array(values.to_vec().into()));
		self
	}

	pub(super) fn numbers(mut self, numbers: &[Number]) -> Self {
		self.0.push(Value::Array(
			numbers.iter().copied().map(Value::Number).collect::<Vec<_>>().into(),
		));
		self
	}

	pub(super) fn option_numbers(self, numbers: &Option<Vec<Number>>) -> Self {
		match numbers {
			Some(numbers) => self.numbers(numbers),
			None => {
				let mut this = self;
				this.0.push(Value::None);
				this
			}
		}
	}

	pub(super) fn rejected(self, rejected: &Rejected) -> Self {
		let (reason, folded) = rejected.parts();
		let mut this = self.option_string(reason);
		this.0.push(Value::Bool(folded));
		this
	}

	pub(super) fn done(self) -> Value {
		Value::Array(self.0.into())
	}
}

/// A saved state being loaded, a field at a time, in the order saved.
pub(super) struct Loading(std::vec::IntoIter<Value>);

impl Loading {
	pub(super) fn new(state: Value) -> Result<Self> {
		match state {
			Value::Array(array) => Ok(Self(array.0.into_iter())),
			_ => anyhow::bail!("A spilled aggregate state is not an array"),
		}
	}

	fn next(&mut self) -> Result<Value> {
		self.0.next().context("A spilled aggregate state ended early")
	}

	pub(super) fn number(&mut self) -> Result<Number> {
		match self.next()? {
			Value::Number(n) => Ok(n),
			_ => anyhow::bail!("A spilled aggregate state holds no number where one was saved"),
		}
	}

	pub(super) fn int(&mut self) -> Result<i64> {
		Ok(self.number()?.to_int())
	}

	pub(super) fn option_number(&mut self) -> Result<Option<Number>> {
		match self.next()? {
			Value::Number(n) => Ok(Some(n)),
			_ => Ok(None),
		}
	}

	pub(super) fn option_datetime(&mut self) -> Result<Option<Datetime>> {
		match self.next()? {
			Value::Datetime(d) => Ok(Some(d)),
			_ => Ok(None),
		}
	}

	pub(super) fn option_string(&mut self) -> Result<Option<String>> {
		match self.next()? {
			Value::String(s) => Ok(Some(s.as_str().to_owned())),
			_ => Ok(None),
		}
	}

	pub(super) fn option_value(&mut self) -> Result<Option<Value>> {
		let value = self.next()?;
		match self.next()? {
			Value::Bool(true) => Ok(Some(value)),
			_ => Ok(None),
		}
	}

	pub(super) fn values(&mut self) -> Result<Vec<Value>> {
		match self.next()? {
			Value::Array(array) => Ok(array.0),
			_ => anyhow::bail!("A spilled aggregate state holds no array where one was saved"),
		}
	}

	pub(super) fn numbers(&mut self) -> Result<Vec<Number>> {
		self.values()?
			.into_iter()
			.map(|v| match v {
				Value::Number(n) => Ok(n),
				_ => anyhow::bail!("A spilled aggregate state holds a value that is no number"),
			})
			.collect()
	}

	pub(super) fn option_numbers(&mut self) -> Result<Option<Vec<Number>>> {
		match self.next()? {
			Value::Array(array) => array
				.0
				.into_iter()
				.map(|v| match v {
					Value::Number(n) => Ok(n),
					_ => anyhow::bail!("A spilled aggregate state holds a value that is no number"),
				})
				.collect::<Result<Vec<_>>>()
				.map(Some),
			_ => Ok(None),
		}
	}

	pub(super) fn rejected(&mut self) -> Result<Rejected> {
		let reason = self.option_string()?;
		let folded = matches!(self.next()?, Value::Bool(true));
		Ok(Rejected::from_parts(reason, folded))
	}
}

#[cfg(test)]
mod tests {
	use crate::exec::function::FunctionRegistry;
	use crate::val::{Datetime, Number, Value};

	fn inputs(name: &str) -> Vec<Value> {
		match name {
			"time::min" | "time::max" => (0..5)
				.map(|i| {
					Value::Datetime(Datetime::from(
						chrono::DateTime::from_timestamp(1_700_000_000 + (i * 97) % 5, 0)
							.expect("a datetime"),
					))
				})
				.collect(),
			"vector::sum" => (0..5)
				.map(|i| Value::Array(vec![Value::from(i), Value::from(i * 2)].into()))
				.collect(),
			_ => (0..7)
				.map(|i| match i {
					3 => Value::None,
					_ => Value::Number(Number::Int((i * 37) % 11)),
				})
				.collect(),
		}
	}

	/// Every built-in aggregate's state saved part way through its input and
	/// loaded into a fresh accumulator folds the rest and finalizes as the
	/// accumulator that never stopped; merged after an earlier part, as a
	/// spilled group's later state is, it finalizes as the whole.
	#[test]
	fn a_saved_state_loads_and_merges_as_the_original() {
		let registry = FunctionRegistry::with_builtins();
		for name in [
			"count",
			"math::sum",
			"math::mean",
			"math::min",
			"math::max",
			"math::stddev",
			"math::variance",
			"math::median",
			"time::min",
			"time::max",
			"array::group",
			"array::collect",
			"array::join",
			"array::distinct",
			"array::min",
			"array::max",
			"vector::sum",
		] {
			let function = registry.get_aggregate(name).unwrap_or_else(|| panic!("{name}"));
			let values = inputs(name);
			let (head, tail) = values.split_at(values.len() / 2);
			let mut whole = function.accumulator_state();
			for v in &values {
				whole.update(v.clone()).expect("folds");
			}
			let mut first = function.accumulator_state();
			for v in head {
				first.update(v.clone()).expect("folds");
			}
			let mut loaded = function.accumulator_state();
			loaded.load(first.save().unwrap_or_else(|| panic!("{name} saves"))).expect("loads");
			let mut second = function.accumulator_state();
			for v in tail {
				loaded.update(v.clone()).expect("folds");
				second.update(v.clone()).expect("folds");
			}
			let mut later = function.accumulator_state();
			later.load(second.save().expect("saves")).expect("loads");
			first.merge(later).expect("merges");
			let want = format!("{:?}", whole.finalize());
			assert_eq!(format!("{:?}", loaded.finalize()), want, "{name} loaded");
			assert_eq!(format!("{:?}", first.finalize()), want, "{name} merged");
		}
	}
}
