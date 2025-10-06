//! Slow query logging support.
//!
//! This module provides a lightweight slow-query logger that can be configured
//! with a duration threshold and optional parameter allow/deny lists. When a
//! query exceeds the configured threshold, we log a single-line message that
//! includes the normalized SQL statement and any selected bound parameters.
//!
//! Key behaviors:
//! - Parameters are extracted by traversing the AST using `VisitExpression` and looking for
//!   `Expr::Param` nodes. For each parameter, we lookup its value in the current `Context`.
//! - Optional filters can be applied to control which parameters are logged:
//!   - `param_deny` takes precedence and excludes any matching parameter names.
//!   - If `param_allow` is non-empty, only those parameter names are included.
//! - For readability, both the SQL statement and parameter values are rendered to SQL and
//!   whitespace is collapsed so the entire log fits on one line.
//!
//! Note: Values considered "nullish" are not logged.
use std::sync::Arc;
use std::time::Duration;

use surrealdb_types::sql::ToSql;
use trice::Instant;

use crate::ctx::Context;
use crate::expr::Expr;
use crate::expr::expression::VisitExpression;

#[derive(Clone)]
/// Configuration and logic for slow query logging.
///
/// A `SlowLog` is constructed with:
/// - a `duration` threshold; queries taking less than this are ignored,
/// - an optional allow-list of parameter names to include in logs,
/// - an optional deny-list of parameter names to exclude from logs.
///
/// Deny rules take precedence over allow rules. When the allow-list is empty,
/// all parameters are allowed by default (subject to the deny-list). When not
/// empty, only parameters present in the allow-list will be logged.
pub(crate) struct SlowLog(Arc<Inner>);

struct Inner {
	duration: Duration,
	param_allow: Vec<String>,
	param_deny: Vec<String>,
}

impl SlowLog {
	/// Create a new slow log configuration.
	///
	/// Parameters:
	/// - `duration`: Minimum elapsed time a statement must take before it is considered "slow" and
	///   logged.
	/// - `param_allow`: If non-empty, only parameters with names in this list are included in the
	///   log output.
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
		// If allow list is empty, everything is allowed by default
		if self.0.param_allow.is_empty() {
			return true;
		}
		// Otherwise only names in the allow list are allowed
		self.0.param_allow.iter().any(|s| s == name)
	}

	/// Check whether the supplied statement should be slow-logged and emit a
	/// log line if the threshold is exceeded.
	///
	/// This function:
	/// - Computes elapsed time since `start` and returns early if under the threshold.
	/// - Traverses the statement AST to collect `$param` names via `VisitExpression`.
	/// - Applies deny-list then allow-list filtering to parameter names, and looks up their current
	///   values from the `Context`.
	/// - Renders the SQL and parameter values, collapsing whitespace so the output is a single line
	///   suitable for log processing.
	pub(crate) fn check_log<S: VisitExpression + ToSql>(
		&self,
		ctx: &Context,
		start: &Instant,
		stm: &S,
	) {
		let elapsed = start.elapsed();
		if elapsed < self.0.duration {
			return;
		}
		// Extract params
		let mut params = vec![];
		stm.visit(&mut |e| {
			if let Expr::Param(p) = e {
				let name = p.as_str();
				if !self.is_param_allowed(name) {
					return;
				}
				if let Some(value) = ctx.value(name) {
					if !value.is_nullish() {
						let value = value.to_sql().split_whitespace().collect::<Vec<_>>().join(" ");
						params.push(format!("${name}={value}"));
					}
				}
			}
		});
		// Ensure the query is logged on a single line by collapsing whitespace
		let stm = stm.to_sql().split_whitespace().collect::<Vec<_>>().join(" ");
		let params = params.join(", ");
		warn!("Slow query detected - time: {elapsed:#?} - query: {stm} - params: [ {params} ]");
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
	fn defaults_allow_all() {
		let s = slowlog(&[], &[]);
		assert!(s.is_param_allowed("a"));
		assert!(s.is_param_allowed("any"));
	}

	#[test]
	fn allow_list_filters() {
		let s = slowlog(&["a", "b"], &[]);
		assert!(s.is_param_allowed("a"));
		assert!(s.is_param_allowed("b"));
		assert!(!s.is_param_allowed("c"));
	}

	#[test]
	fn deny_list_only_excludes() {
		let s = slowlog(&[], &["secret", "token"]);
		assert!(!s.is_param_allowed("secret"));
		assert!(!s.is_param_allowed("token"));
		assert!(s.is_param_allowed("other"));
	}

	#[test]
	fn deny_precedence_over_allow() {
		let s = slowlog(&["foo", "bar"], &["bar"]);
		assert!(!s.is_param_allowed("bar"));
		assert!(s.is_param_allowed("foo"));
	}

	#[test]
	fn allow_list_empty_means_all_except_denied() {
		let s = slowlog(&[], &["nope"]);
		assert!(s.is_param_allowed("ok"));
		assert!(!s.is_param_allowed("nope"));
	}
}
