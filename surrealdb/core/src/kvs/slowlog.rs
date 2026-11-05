//! Slow query logging support.
//!
//! This module provides a lightweight slow-query logger that can be configured
//! with a duration threshold and optional parameter allow/deny lists. When a
//! query exceeds the configured threshold, we log a single-line message that
//! includes the normalized SQL statement and any selected bound parameters.
//!
//! Key behaviors:
//! - Parameters are extracted by visiting the value the caller names as the parameter source — the
//!   statement in expression form, or the plan of a statement the engine ran — for `$param`
//!   references. For each parameter, we lookup its value in the current `Context`.
//! - Optional filters can be applied to control which parameters are logged:
//!   - `param_deny` takes precedence and excludes any matching parameter names.
//!   - Only parameter names explicitly listed in `param_allow` are included; when `param_allow` is
//!     empty, no parameters are logged.
//! - For readability, both the SQL statement and parameter values are rendered to SQL and
//!   whitespace is collapsed so the entire log fits on one line.
//!
//! Note: Values considered "nullish" are not logged.
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use surrealdb_types::ToSql;
use web_time::Instant;

use crate::catalog::{Permission, Permissions};
use crate::ctx::FrozenContext;
use crate::ctx::reason::Reason;
use crate::expr::visit::{Visit, Visitor};

#[derive(Clone)]
/// Configuration and logic for slow query logging.
///
/// A `SlowLog` is constructed with:
/// - a `duration` threshold; queries taking less than this are ignored,
/// - an optional allow-list of parameter names to include in logs,
/// - an optional deny-list of parameter names to exclude from logs.
///
/// Deny rules take precedence over allow rules. When the allow-list is empty,
/// no parameters are logged by default. Only parameters present in the
/// allow-list (and not present in the deny-list) will be logged.
pub(crate) struct SlowLog(Arc<Inner>);

struct Inner {
	duration: Duration,
	param_allow: Vec<String>,
	param_deny: Vec<String>,
}

pub(crate) struct ParamVisitor<'a> {
	params: String,
	slow_log: &'a SlowLog,
	ctx: &'a FrozenContext,
}

impl Visitor for ParamVisitor<'_> {
	type Error = ();

	// Empty implementations so that the visitor won't recurse into permissions.
	fn visit_permissions(&mut self, _: &Permissions) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_permission(&mut self, _: &Permission) -> Result<(), Self::Error> {
		Ok(())
	}
	fn visit_param(&mut self, param: &crate::expr::Param) -> Result<(), Self::Error> {
		if !self.slow_log.is_param_allowed(param) {
			return Ok(());
		}
		if let Some(value) = self.ctx.value(param)
			&& !value.is_none()
			&& !value.is_null()
		{
			if !self.params.is_empty() {
				self.params.push_str(", ");
			}

			let value = value.to_sql().split_whitespace().collect::<Vec<_>>().join(" ");
			write!(&mut self.params, "{}={}", param.to_sql(), value)
				.expect("Writing into a string cannot fail");
		}
		Ok(())
	}
}

pub(crate) trait SlowLogVisit: for<'a> Visit<ParamVisitor<'a>> {}
impl<V: for<'a> Visit<ParamVisitor<'a>>> SlowLogVisit for V {}

impl SlowLog {
	/// Create a new slow log configuration.
	///
	/// Parameters:
	/// - `duration`: Minimum elapsed time a statement must take before it is considered "slow" and
	///   logged.
	/// - `param_allow`: The names of the parameters whose values are included in the log output.
	///   When empty, no parameter value is logged.
	/// - `param_deny`: Parameter names that should never be logged. This list always takes
	///   precedence over `param_allow`.
	pub(super) fn new(
		duration: Duration,
		param_allow: Vec<String>,
		param_deny: Vec<String>,
	) -> Self {
		Self(Arc::new(Inner {
			duration,
			param_allow,
			param_deny,
		}))
	}

	/// Returns true if the parameter with the given name should be logged according
	/// to the current allow/deny configuration.
	#[inline]
	pub(crate) fn is_param_allowed(&self, name: &str) -> bool {
		// Deny takes precedence
		if !self.0.param_deny.is_empty() && self.0.param_deny.iter().any(|s| s == name) {
			return false;
		}
		// If allow list is empty, no parameters are logged by default
		if self.0.param_allow.is_empty() {
			return false;
		}
		// Only names in the allow list are allowed
		self.0.param_allow.iter().any(|s| s == name)
	}

	/// Check whether the supplied statement should be slow-logged and emit a
	/// log line if the threshold is exceeded.
	///
	/// `stm` is rendered as the entry's query text. `params` is walked for
	/// the `$param` references whose current values the entry lists, after
	/// the deny-list and then the allow-list; it is `stm` itself for a
	/// statement in expression form, and the plan of a statement the
	/// streaming engine ran. Both renderings collapse whitespace so the entry
	/// is a single line.
	///
	/// Returns whether an entry was emitted.
	pub(crate) fn check_log<T: ToSql, P: SlowLogVisit>(
		&self,
		ctx: &FrozenContext,
		start: &Instant,
		stm: &T,
		params: &P,
	) -> bool {
		let elapsed = start.elapsed();
		if elapsed < self.0.duration {
			return false;
		}

		let params = self.extract_params(ctx, params);
		// Ensure the query is logged on a single line by collapsing whitespace
		let stm = stm.to_sql().split_whitespace().collect::<Vec<_>>().join(" ");
		warn!("Slow query detected - time: {elapsed:#?} - query: {stm} - params: [ {params} ]");
		true
	}

	/// Emit an entry for a batch of statements that its query timeout or its
	/// cancellation stopped before it completed, when the batch had run for at
	/// least the threshold by then.
	///
	/// The entry reports the time since `batch_start` and the `reason`, and
	/// names `stm`, the statement the interruption stopped: the one it cut
	/// short, whose `params` are listed as [`SlowLog::check_log`] lists them,
	/// or the one it kept from starting, which passes no `params` since it
	/// never ran. The statement is rendered only once the threshold is met.
	///
	/// Returns whether an entry was emitted.
	pub(crate) fn check_log_interrupted<T: ToSql, P: SlowLogVisit>(
		&self,
		ctx: &FrozenContext,
		batch_start: &Instant,
		reason: Reason,
		stm: &T,
		params: Option<&P>,
	) -> bool {
		let elapsed = batch_start.elapsed();
		if elapsed < self.0.duration {
			return false;
		}
		let reason = match reason {
			Reason::Timedout(timeout) => format!("timed out after {timeout}"),
			Reason::Canceled => "cancelled".to_string(),
		};
		let params = params.map_or_else(String::new, |params| self.extract_params(ctx, params));
		let stm = stm.to_sql().split_whitespace().collect::<Vec<_>>().join(" ");
		warn!(
			"Slow query detected - time: {elapsed:#?} - batch did not complete ({reason}) - query: {stm} - params: [ {params} ]"
		);
		true
	}

	fn extract_params<P: SlowLogVisit>(&self, ctx: &FrozenContext, params: &P) -> String {
		let mut visitor = ParamVisitor {
			params: String::new(),
			slow_log: self,
			ctx,
		};

		// no errors can happen
		let _ = params.visit(&mut visitor);

		visitor.params
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	fn slowlog(allow: &[&str], deny: &[&str]) -> SlowLog {
		SlowLog::new(
			Duration::from_millis(1),
			allow.iter().map(|s| (*s).to_string()).collect(),
			deny.iter().map(|s| (*s).to_string()).collect(),
		)
	}

	#[test]
	fn defaults_deny_all() {
		let s = slowlog(&[], &[]);
		assert!(!s.is_param_allowed("a"));
		assert!(!s.is_param_allowed("any"));
	}

	#[test]
	fn allow_list_filters() {
		let s = slowlog(&["a", "b"], &[]);
		assert!(s.is_param_allowed("a"));
		assert!(s.is_param_allowed("b"));
		assert!(!s.is_param_allowed("c"));
	}

	#[test]
	fn deny_list_without_allow_denies_all() {
		let s = slowlog(&[], &["secret", "token"]);
		assert!(!s.is_param_allowed("secret"));
		assert!(!s.is_param_allowed("token"));
		assert!(!s.is_param_allowed("other"));
	}

	#[test]
	fn deny_precedence_over_allow() {
		let s = slowlog(&["foo", "bar"], &["bar"]);
		assert!(!s.is_param_allowed("bar"));
		assert!(s.is_param_allowed("foo"));
	}

	#[test]
	fn allow_list_empty_denies_all() {
		let s = slowlog(&[], &["nope"]);
		assert!(!s.is_param_allowed("ok"));
		assert!(!s.is_param_allowed("nope"));
	}
}
