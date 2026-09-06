use crate::expr::Expr;
use crate::expr::statements::{
	AccessStatement, KillStatement, LiveStatement, OptionStatement, ShowStatement, UseStatement,
};

#[derive(Clone, Debug)]
pub struct LogicalPlan {
	pub expressions: Vec<TopLevelExpr>,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum TopLevelExpr {
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
	pub fn read_only(&self) -> bool {
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
		// Render a bare expression through `Expr`'s own `ToSql`, which carves
		// out the forms that have no SurrealQL surface to convert back to —
		// `Expr::Match`, which GQL lowers to and which the conversion below
		// would replace with a `NONE` placeholder after a `debug_assert!`.
		// `sql::TopLevelExpr::Expr` renders as its inner expression, so this
		// arm produces the same text the conversion would for everything else.
		if let TopLevelExpr::Expr(expr) = self {
			expr.fmt_sql(f, fmt);
			return;
		}
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
