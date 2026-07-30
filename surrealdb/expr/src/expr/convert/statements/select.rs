//! `sql` -> `expr` conversions for [`crate::sql::statements::select`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::select::*;

impl From<SelectStatement> for crate::expr::statements::SelectStatement {
	fn from(v: SelectStatement) -> Self {
		Self {
			fields: v.fields.into(),
			omit: v.omit.into_iter().map(Into::into).collect(),
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			cond: v.cond.map(Into::into),
			split: v.split.map(Into::into),
			group: v.group.map(Into::into),
			order: v.order.map(Into::into),
			limit: v.limit.map(Into::into),
			start: v.start.map(Into::into),
			fetch: v.fetch.map(Into::into),
			version: v.version.into(),
			timeout: v.timeout.into(),
			explain: v.explain.map(Into::into),
			tempfiles: v.tempfiles,
		}
	}
}

impl From<crate::expr::statements::SelectStatement> for SelectStatement {
	fn from(v: crate::expr::statements::SelectStatement) -> Self {
		Self {
			fields: v.fields.into(),
			omit: v.omit.into_iter().map(Into::into).collect(),
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			cond: v.cond.map(Into::into),
			split: v.split.map(Into::into),
			group: v.group.map(Into::into),
			order: v.order.map(Into::into),
			limit: v.limit.map(Into::into),
			start: v.start.map(Into::into),
			fetch: v.fetch.map(Into::into),
			version: v.version.into(),
			timeout: v.timeout.into(),
			explain: v.explain.map(Into::into),
			tempfiles: v.tempfiles,
		}
	}
}
