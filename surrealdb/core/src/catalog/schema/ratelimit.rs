use std::time::Duration;

use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::PermissionKind;
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::types::PublicDuration;
use crate::val::Value;

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RateLimit {
	pub(crate) actions: Vec<PermissionKind>,
	pub(crate) condition: Option<Expr>,
	pub(crate) bucket: Expr,
	pub(crate) limit: u64,
	pub(crate) period: Duration,
	#[revision(start = 3)]
	pub(crate) max: Option<u64>,
}

pub(crate) type RateLimits = Vec<RateLimit>;

impl RateLimit {
	pub(crate) fn to_sql_definition(&self) -> crate::sql::RateLimit {
		crate::sql::RateLimit {
			actions: self.actions.clone().into_iter().map(Into::into).collect(),
			condition: self.condition.clone().map(Into::into),
			bucket: self.bucket.clone().into(),
			limit: self.limit,
			period: PublicDuration::from_std(self.period),
			max: self.max,
		}
	}
}

impl ToSql for RateLimit {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, sql_fmt);
	}
}

impl InfoStructure for RateLimit {
	fn structure(self) -> Value {
		Value::from(map! {
			"actions" => self.actions.into_iter().map(|a| Value::from(a.to_string())).collect::<Vec<_>>().into(),
			"condition", if let Some(v) = self.condition => v.structure(),
			"by" => self.bucket.structure(),
			"limit" => self.limit.into(),
			"period" => Value::Duration(self.period.into()),
			"max", if let Some(v) = self.max => v.into(),
		})
	}
}

impl From<crate::sql::RateLimit> for RateLimit {
	fn from(v: crate::sql::RateLimit) -> Self {
		Self {
			actions: v.actions.into_iter().map(Into::into).collect(),
			condition: v.condition.map(Into::into),
			bucket: v.bucket.into(),
			limit: v.limit,
			period: v.period.into_inner(),
			max: v.max,
		}
	}
}
