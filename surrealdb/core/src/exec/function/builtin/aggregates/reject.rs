//! Shared handling for an aggregate input that cannot be accumulated.

use anyhow::{Result, bail};

use crate::expr::error::Error as ExprError;
use crate::val::{Number, Value};

/// Tracks what an aggregate could not accumulate, and whether it accumulated
/// anything at all.
///
/// A numeric aggregate skips every input that is not a number, in every group,
/// whether or not the group also holds a number. `NONE`, `NULL`, strings,
/// booleans and arrays are all skipped, so one malformed row never fails a
/// group, and a group with no numbers answers its identity: `0` for a sum, the
/// accumulator's empty value for the rest.
///
/// Datetime aggregates keep the stricter rule: a value of the wrong kind is
/// skipped only if the group folded at least one datetime, and raises otherwise.
///
/// The rejection is reported from `finalize` rather than from `update` because
/// only at `finalize` is it known whether anything else folded.
#[derive(Debug, Clone, Default)]
pub(super) struct Rejected {
	/// The first wrong-kind value seen, rendered as its error.
	reason: Option<String>,
	/// Whether any value was actually folded into the accumulator.
	folded: bool,
	/// The first error folding a value raised (an integer total leaving
	/// `i64`). Unlike a rejection it fails the group however many other
	/// values folded, since the total is then no answer.
	failure: Option<String>,
}

impl Rejected {
	/// Take a numeric input. Returns the number for a number and `None` for any
	/// other value, which is skipped without being recorded.
	pub(super) fn number(&mut self, _func: &str, value: &Value) -> Option<Number> {
		if let Value::Number(n) = value {
			self.folded = true;
			Some(n.clone())
		} else {
			None
		}
	}

	/// Take a date, time, timestamp or datetime input. An aggregate folds one of those kinds:
	/// `held` is the value the group holds so far, and an input of another kind than it is
	/// recorded as a rejection, as is a value that is none of the four.
	pub(super) fn temporal(
		&mut self,
		func: &str,
		value: &Value,
		held: Option<&Value>,
	) -> Option<Value> {
		let name = |v: &Value| match v {
			Value::Datetime(_) => Some("datetime"),
			Value::Date(_) => Some("date"),
			Value::Time(_) => Some("time"),
			Value::Timestamp(_) => Some("timestamp"),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)
			| Value::Interval(_) => None,
		};
		match (name(value), held.and_then(name)) {
			(Some(found), Some(expected)) if found != expected => {
				self.reject(func, expected, value);
				None
			}
			(Some(_), _) => {
				self.folded = true;
				Some(value.clone())
			}
			(None, _) if matches!(value, Value::None | Value::Null) => None,
			(None, _) => {
				self.reject(func, "datetime", value);
				None
			}
		}
	}

	/// Record a rejection, keeping the first one seen so the reported error does
	/// not depend on the order rows arrive in.
	fn reject(&mut self, func: &str, expected: &str, found: &Value) {
		use surrealdb_types::ToSql;

		if self.reason.is_some() {
			return;
		}
		self.reason = Some(
			ExprError::InvalidFunctionArguments {
				name: func.to_owned(),
				message: format!(
					"Argument 1 was the wrong type. Expected `{expected}` but found `{}`",
					found.to_sql()
				),
			}
			.to_string(),
		);
	}

	/// The rejection's reason and whether a value was folded, for a saved
	/// state.
	pub(super) fn parts(&self) -> (&Option<String>, bool, &Option<String>) {
		(&self.reason, self.folded, &self.failure)
	}

	/// A rejection rebuilt from the parts [`Self::parts`] answered.
	pub(super) fn from_parts(
		reason: Option<String>,
		folded: bool,
		failure: Option<String>,
	) -> Self {
		Self {
			reason,
			folded,
			failure,
		}
	}

	/// Record the error folding a value raised, keeping the first so the
	/// reported error does not depend on the order rows arrive in. The group
	/// fails when it is finalised.
	pub(super) fn fail(&mut self, error: &anyhow::Error) {
		if self.failure.is_none() {
			self.failure = Some(error.to_string());
		}
	}

	/// Combine with another partial accumulator over the same group: a value
	/// folded by either half gives the group an answer, and the first rejection
	/// is carried in case neither half folded anything.
	pub(super) fn merge_from(&mut self, other: &Self) {
		self.folded |= other.folded;
		if self.reason.is_none() {
			self.reason.clone_from(&other.reason);
		}
		if self.failure.is_none() {
			self.failure.clone_from(&other.failure);
		}
	}

	/// Fail if a wrong-kind value was seen and nothing folded. Called from
	/// `finalize`.
	pub(super) fn check(&self) -> Result<()> {
		if let Some(failure) = &self.failure {
			bail!("{failure}");
		}
		if let Some(reason) = &self.reason
			&& !self.folded
		{
			bail!("{reason}");
		}
		Ok(())
	}

	pub(super) fn reset(&mut self) {
		self.reason = None;
		self.folded = false;
		self.failure = None;
	}
}
