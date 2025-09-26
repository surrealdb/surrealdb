use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use trice::Instant;

use crate::ctx::Context;
use crate::expr::Expr;
use crate::expr::expression::VisitExpression;
use crate::sql::ToSql;

#[derive(Clone)]
pub(crate) struct SlowLog(Arc<Inner>);

struct Inner {
	duration: Duration,
	param_allow: Vec<String>,
	param_deny: Vec<String>,
}

impl SlowLog {
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

	pub(crate) fn check_log<S: VisitExpression + Display>(
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
				// Apply deny filter first
				if !self.0.param_deny.is_empty() && self.0.param_deny.iter().any(|s| s == name) {
					return;
				}
				// Apply allow filter if present
				if !self.0.param_allow.is_empty() && !self.0.param_allow.iter().any(|s| s == name) {
					return;
				}
				if let Some(value) = ctx.value(name) {
					if !value.is_nullish() {
						let value = value.to_sql().split_whitespace().collect::<Vec<_>>().join(" ");
						params.push(format!("${}={}", name, value));
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
