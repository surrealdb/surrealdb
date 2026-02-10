use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::catalog::EventKind;
use crate::fmt::{CoverStmts, Fmt};
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineEventStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub target_table: Expr,
	pub when: Expr,
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub then: Vec<Expr>,
	pub comment: Expr,
	pub event_kind: EventKind,
}

impl ToSql for DefineEventStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("DEFINE EVENT");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}
		write_sql!(f, fmt, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.target_table),);
		if let EventKind::Async {
			retry,
			max_depth,
		} = self.event_kind
		{
			write_sql!(f, fmt, " ASYNC RETRY {} MAXDEPTH {}", retry, max_depth);
		}
		write_sql!(f, fmt, " WHEN {}", CoverStmts(&self.when),);
		if !self.then.is_empty() {
			write_sql!(f, fmt, " THEN {}", Fmt::comma_separated(self.then.iter().map(CoverStmts)));
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}

impl From<DefineEventStatement> for crate::expr::statements::DefineEventStatement {
	fn from(v: DefineEventStatement) -> Self {
		crate::expr::statements::DefineEventStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			target_table: v.target_table.into(),
			when: v.when.into(),
			then: v.then.into_iter().map(From::from).collect(),
			comment: v.comment.into(),
			event_kind: v.event_kind,
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineEventStatement> for DefineEventStatement {
	fn from(v: crate::expr::statements::DefineEventStatement) -> Self {
		DefineEventStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			target_table: v.target_table.into(),
			when: v.when.into(),
			then: v.then.into_iter().map(From::from).collect(),
			comment: v.comment.into(),
			event_kind: v.event_kind,
		}
	}
}
