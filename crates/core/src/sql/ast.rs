use std::fmt::{self, Display};

use surrealdb_types::{ToSql, write_sql};

use crate::expr;
use crate::fmt::{Fmt, Pretty};
use crate::sql::statements::{
	AccessStatement, KillStatement, LiveStatement, OptionStatement, ShowStatement, UseStatement,
};
use crate::sql::{Expr, Param};

#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Ast {
	pub(crate) expressions: Vec<TopLevelExpr>,
}

impl Ast {
	/// Creates an ast with a signle expression
	pub(crate) fn single_expr(expr: Expr) -> Self {
		Ast {
			expressions: vec![TopLevelExpr::Expr(expr)],
		}
	}

	pub fn num_statements(&self) -> usize {
		self.expressions.len()
	}

	pub fn get_let_statements(&self) -> Vec<String> {
		let mut let_var_names = Vec::new();
		for expr in &self.expressions {
			if let TopLevelExpr::Expr(Expr::Let(stmt)) = expr {
				let_var_names.push(stmt.name.clone());
			}
		}
		let_var_names
	}

	pub fn add_param(&mut self, name: String) {
		self.expressions.push(TopLevelExpr::Expr(Expr::Param(Param::new(name))));
	}
}

impl Display for Ast {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		use std::fmt::Write;
		write!(
			Pretty::from(f),
			"{}",
			&Fmt::one_line_separated(
				self.expressions.iter().map(|v| Fmt::new(v, |v, f| write!(f, "{v};"))),
			),
		)
	}
}

impl ToSql for Ast {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self)
	}
}

impl From<expr::LogicalPlan> for Ast {
	fn from(value: expr::LogicalPlan) -> Self {
		Ast {
			expressions: value.expressions.into_iter().map(From::from).collect(),
		}
	}
}
impl From<Ast> for expr::LogicalPlan {
	fn from(value: Ast) -> Self {
		expr::LogicalPlan {
			expressions: value.expressions.into_iter().map(From::from).collect(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<TopLevelExpr> for crate::expr::TopLevelExpr {
	fn from(value: TopLevelExpr) -> Self {
		match value {
			TopLevelExpr::Begin => crate::expr::TopLevelExpr::Begin,
			TopLevelExpr::Cancel => crate::expr::TopLevelExpr::Cancel,
			TopLevelExpr::Commit => crate::expr::TopLevelExpr::Commit,
			TopLevelExpr::Access(access_statement) => {
				crate::expr::TopLevelExpr::Access(Box::new((*access_statement).into()))
			}
			TopLevelExpr::Kill(kill_statement) => {
				crate::expr::TopLevelExpr::Kill(kill_statement.into())
			}
			TopLevelExpr::Live(live_statement) => {
				crate::expr::TopLevelExpr::Live(Box::new((*live_statement).into()))
			}
			TopLevelExpr::Option(option_statement) => {
				crate::expr::TopLevelExpr::Option(option_statement.into())
			}
			TopLevelExpr::Use(use_statement) => {
				crate::expr::TopLevelExpr::Use(use_statement.into())
			}
			TopLevelExpr::Show(show_statement) => {
				crate::expr::TopLevelExpr::Show(show_statement.into())
			}
			TopLevelExpr::Expr(expr) => crate::expr::TopLevelExpr::Expr(expr.into()),
		}
	}
}

impl From<crate::expr::TopLevelExpr> for TopLevelExpr {
	fn from(value: crate::expr::TopLevelExpr) -> Self {
		match value {
			crate::expr::TopLevelExpr::Begin => TopLevelExpr::Begin,
			crate::expr::TopLevelExpr::Cancel => TopLevelExpr::Cancel,
			crate::expr::TopLevelExpr::Commit => TopLevelExpr::Commit,
			crate::expr::TopLevelExpr::Access(access_statement) => {
				TopLevelExpr::Access(Box::new((*access_statement).into()))
			}
			crate::expr::TopLevelExpr::Kill(kill_statement) => {
				TopLevelExpr::Kill(kill_statement.into())
			}
			crate::expr::TopLevelExpr::Live(live_statement) => {
				TopLevelExpr::Live(Box::new((*live_statement).into()))
			}
			crate::expr::TopLevelExpr::Option(option_statement) => {
				TopLevelExpr::Option(option_statement.into())
			}
			crate::expr::TopLevelExpr::Use(use_statement) => {
				TopLevelExpr::Use(use_statement.into())
			}
			crate::expr::TopLevelExpr::Show(show_statement) => {
				TopLevelExpr::Show(show_statement.into())
			}
			crate::expr::TopLevelExpr::Expr(expr) => TopLevelExpr::Expr(expr.into()),
		}
	}
}

impl fmt::Display for TopLevelExpr {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			TopLevelExpr::Begin => write!(f, "BEGIN"),
			TopLevelExpr::Cancel => write!(f, "CANCEL"),
			TopLevelExpr::Commit => write!(f, "COMMIT"),
			TopLevelExpr::Access(s) => s.fmt(f),
			TopLevelExpr::Kill(s) => s.fmt(f),
			TopLevelExpr::Live(s) => s.fmt(f),
			TopLevelExpr::Option(s) => s.fmt(f),
			TopLevelExpr::Use(s) => s.fmt(f),
			TopLevelExpr::Show(s) => s.fmt(f),
			TopLevelExpr::Expr(e) => e.fmt(f),
		}
	}
}
