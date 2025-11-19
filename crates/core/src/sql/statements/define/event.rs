use std::fmt::{self, Display};

use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::fmt::Fmt;
use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineEventStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub target_table: Expr,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Option<Expr>,
}

impl Display for DefineEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE EVENT",)?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(
			f,
			" {} ON {} WHEN {} THEN {}",
			self.name,
			self.target_table,
			self.when,
			Fmt::comma_separated(&self.then)
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
		Ok(())
	}
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
