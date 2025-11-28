use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineEventStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub target_table: Expr,
	pub when: Expr,
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub then: Vec<Expr>,
	pub comment: Option<Expr>,
}

impl ToSql for DefineEventStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("DEFINE EVENT");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}
		f.push(' ');
		self.name.fmt_sql(f, fmt);
		f.push_str(" ON ");
		self.target_table.fmt_sql(f, fmt);
		f.push_str(" WHEN ");
		self.when.fmt_sql(f, fmt);
		f.push_str(" THEN ");
		for (i, expr) in self.then.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			expr.fmt_sql(f, fmt);
		}
		if let Some(ref v) = self.comment {
			f.push_str(" COMMENT ");
			v.fmt_sql(f, fmt);
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
			comment: v.comment.map(|x| x.into()),
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
			comment: v.comment.map(|x| x.into()),
		}
	}
}
