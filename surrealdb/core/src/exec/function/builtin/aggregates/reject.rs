//! Shared handling for an aggregate input that cannot be accumulated.

use anyhow::{Result, bail};

use crate::expr::error::Error as ExprError;
use crate::val::{Number, Value};

/// Tracks what an aggregate could not accumulate, and whether it accumulated
/// anything at all.
///
/// An aggregate reduces a group by folding one value per row, so it has to say
/// what happens when a row's value is not of the kind it folds. Neither extreme
/// is right:
///
/// - Dropping every wrong value silently reports the accumulator's identity — `0` for a sum,
///   `-Infinity` for a maximum — as though it were the answer. The caller cannot tell that from a
///   real result, which makes it the worst outcome.
/// - Refusing every wrong value fails a whole group because one row is malformed, and over
///   heterogeneous data in a schemaless store that is ordinary rather than exceptional.
///
/// So the distinction drawn here is whether the group has an answer at all:
///
/// - `NONE` and `NULL` are absent rather than wrong, and are always skipped. This matches SQL's
///   treatment of nulls in aggregates and keeps a missing field from failing a group.
/// - A value of the wrong kind is skipped **if the group folded at least one real value**, so a
///   single malformed row among good ones does not lose the rest.
/// - A value of the wrong kind **raises** if the group folded nothing, because that is precisely
///   when the identity would be returned as though it were an answer. The error is the same
///   argument-type error the scalar form raises.
///
/// The identity is thereby reserved for a group that genuinely saw no values,
/// which is what the scalar form returns for empty input (`math::sum([])` is
/// `0`, `math::max([])` is `-Infinity`).
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
	/// Take a numeric input, recording a rejection for a value that is neither
	/// a number nor absent.
	///
	/// Returns `None` both for an absent value and for a rejected one; a caller
	/// folds only what comes back as `Some`.
	pub(super) fn number(&mut self, func: &str, value: &Value) -> Option<Number> {
		match value {
			Value::Number(n) => {
				self.folded = true;
				Some(*n)
			}
			Value::None | Value::Null => None,
			other => {
				self.reject(func, "number", other);
				None
			}
		}
	}

	/// Take a datetime input, recording a rejection for a value that is neither
	/// a datetime nor absent.
	pub(super) fn datetime(&mut self, func: &str, value: &Value) -> Option<crate::val::Datetime> {
		match value {
			Value::Datetime(d) => {
				self.folded = true;
				Some(*d)
			}
			Value::None | Value::Null => None,
			other => {
				self.reject(func, "datetime", other);
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
