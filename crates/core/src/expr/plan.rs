use std::fmt::{self, Display};

use crate::expr::Expr;
use crate::expr::statements::{
	AccessStatement, KillStatement, LiveStatement, OptionStatement, ShowStatement, UseStatement,
};
use crate::fmt::Fmt;

#[derive(Clone, Debug)]
pub(crate) struct LogicalPlan {
	pub(crate) expressions: Vec<TopLevelExpr>,
}

impl Display for LogicalPlan {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		Display::fmt(
			&Fmt::one_line_separated(
				self.expressions.iter().map(|v| Fmt::new(v, |v, f| write!(f, "{v};"))),
			),
			f,
		)
	}
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) enum TopLevelExpr {
	Begin,
	Cancel,
	Commit,
	Access(Box<AccessStatement>),
	Kill(KillStatement),
	Live(Box<LiveStatement>),
	Option(OptionStatement),
	Use(UseStatement),
	Show(ShowStatement),
	Expr(Expr),
}

impl TopLevelExpr {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		match self {
			TopLevelExpr::Begin
			| TopLevelExpr::Cancel
			| TopLevelExpr::Commit
			| TopLevelExpr::Show(_) => true,
			TopLevelExpr::Kill(_)
			| TopLevelExpr::Live(_)
			| TopLevelExpr::Option(_)
			| TopLevelExpr::Use(_)
			| TopLevelExpr::Access(_) => false,
			TopLevelExpr::Expr(expr) => expr.read_only(),
		}
	}
}

impl Display for TopLevelExpr {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Use the sql module's ToSql implementation for display
		use surrealdb_types::ToSql;
		let sql_expr: crate::sql::TopLevelExpr = self.clone().into();
		write!(f, "{}", sql_expr.to_sql())
	}
}

impl surrealdb_types::ToSql for TopLevelExpr {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_expr: crate::sql::TopLevelExpr = self.clone().into();
		sql_expr.fmt_sql(f, fmt);
	}
}
