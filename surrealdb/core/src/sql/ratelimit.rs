use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, PermissionKind};
use crate::types::PublicDuration;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RateLimit {
	pub actions: Vec<PermissionKind>,
	pub condition: Option<Expr>,
	pub bucket: Expr,
	pub limit: u64,
	pub period: PublicDuration,
	pub max: Option<u64>,
}

pub(crate) type RateLimits = Vec<RateLimit>;

impl RateLimit {
	pub(crate) fn fmt_sql_clause(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("FOR ");
		for (i, action) in self.actions.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			f.push_str(action.as_str().to_ascii_uppercase().as_str());
		}
		if let Some(condition) = &self.condition {
			write_sql!(f, sql_fmt, " WHERE {}", CoverStmts(condition));
		}
		write_sql!(f, sql_fmt, " BY {}", CoverStmts(&self.bucket));
		write_sql!(f, sql_fmt, " LIMIT {} PER {}", self.limit, self.period);
		if let Some(max) = self.max {
			write_sql!(f, sql_fmt, " MAX {}", max);
		}
	}
}

impl ToSql for RateLimit {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("RATELIMIT ");
		self.fmt_sql_clause(f, sql_fmt);
	}
}

pub(crate) fn fmt_ratelimits_block(f: &mut String, sql_fmt: SqlFormat, ratelimits: &[RateLimit]) {
	if ratelimits.is_empty() {
		return;
	}
	f.push_str("RATELIMIT ");
	for (i, ratelimit) in ratelimits.iter().enumerate() {
		if i > 0 {
			f.push_str(", ");
		}
		ratelimit.fmt_sql_clause(f, sql_fmt);
	}
}

impl From<crate::catalog::RateLimit> for RateLimit {
	fn from(v: crate::catalog::RateLimit) -> Self {
		Self {
			actions: v.actions.into_iter().map(Into::into).collect(),
			condition: v.condition.map(Into::into),
			bucket: v.bucket.into(),
			limit: v.limit,
			period: v.period.into(),
			max: v.max,
		}
	}
}
