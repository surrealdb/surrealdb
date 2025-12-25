use crate::expr::Expr;
use crate::expr::statements::{
	AccessStatement, KillStatement, LiveStatement, OptionStatement, ShowStatement, UseStatement,
};

#[derive(Clone, Debug)]
pub(crate) struct LogicalPlan {
	pub(crate) expressions: Vec<TopLevelExpr>,
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

impl surrealdb_types::ToSql for TopLevelExpr {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_expr: crate::sql::TopLevelExpr = self.clone().into();
		sql_expr.fmt_sql(f, fmt);
	}
}

impl surrealdb_types::ToSql for LogicalPlan {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_ast: crate::sql::Ast = self.clone().into();
		sql_ast.fmt_sql(f, fmt);
	}
}
