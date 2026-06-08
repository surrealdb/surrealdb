use std::fmt::{self};

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr;
use crate::fmt::Fmt;
use crate::sql::statements::{
	AccessStatement, KillStatement, LiveStatement, OptionStatement, ShowStatement, UseStatement,
};
use crate::sql::{Expr, Literal, Param};

#[derive(Clone, Copy, Eq, PartialEq, Debug, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum ExplainFormat {
	#[default]
	Text,
	Json,
}

impl From<ExplainFormat> for crate::expr::ExplainFormat {
	fn from(value: ExplainFormat) -> Self {
		match value {
			ExplainFormat::Text => crate::expr::ExplainFormat::Text,
			ExplainFormat::Json => crate::expr::ExplainFormat::Json,
		}
	}
}

impl From<crate::expr::ExplainFormat> for ExplainFormat {
	fn from(value: crate::expr::ExplainFormat) -> Self {
		match value {
			crate::expr::ExplainFormat::Text => ExplainFormat::Text,
			crate::expr::ExplainFormat::Json => ExplainFormat::Json,
		}
	}
}

#[derive(Debug, PartialEq, Clone)]
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

	/// Returns `true` if this AST is exactly one top-level expression and that
	/// expression describes inert data only: a literal (including objects,
	/// arrays and sets of inert data), a `$param` reference, or a built-in
	/// constant.
	///
	/// Use this to validate request payloads that are intended to supply
	/// values rather than to execute SurrealQL (e.g. the body of a REST
	/// `/key` request that is bound to `$data`). Function calls, idioms,
	/// statements (CREATE/UPDATE/...), blocks, binary expressions and other
	/// executable forms are all rejected, including when nested inside
	/// object or array literals.
	pub fn is_value_expression(&self) -> bool {
		if self.expressions.len() != 1 {
			return false;
		}
		let TopLevelExpr::Expr(expr) = &self.expressions[0] else {
			return false;
		};
		is_value_expr(expr)
	}

	pub fn get_let_statements(&self) -> Vec<String> {
		let mut let_var_names = Vec::new();
		for expr in &self.expressions {
			if let TopLevelExpr::Expr(Expr::Let(stmt)) = expr {
				let_var_names.push(stmt.name.as_str().to_owned());
			}
		}
		let_var_names
	}

	pub fn add_param(&mut self, name: String) {
		self.expressions.push(TopLevelExpr::Expr(Expr::Param(Param::new(name))));
	}
}

fn is_value_expr(expr: &Expr) -> bool {
	match expr {
		Expr::Param(_) | Expr::Constant(_) => true,
		Expr::Literal(lit) => is_value_literal(lit),
		_ => false,
	}
}

fn is_value_literal(lit: &Literal) -> bool {
	match lit {
		Literal::Array(items) | Literal::Set(items) => items.iter().all(is_value_expr),
		Literal::Object(entries) => entries.iter().all(|e| is_value_expr(&e.value)),
		Literal::None
		| Literal::Null
		| Literal::UnboundedRange
		| Literal::Bool(_)
		| Literal::Float(_)
		| Literal::Integer(_)
		| Literal::Decimal(_)
		| Literal::Duration(_)
		| Literal::String(_)
		| Literal::RecordId(_)
		| Literal::Datetime(_)
		| Literal::Uuid(_)
		| Literal::Regex(_)
		| Literal::Geometry(_)
		| Literal::File(_)
		| Literal::Bytes(_) => true,
	}
}

impl ToSql for Ast {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(
			f,
			fmt,
			"{}",
			&Fmt::one_line_separated(
				self.expressions
					.iter()
					.map(|v| Fmt::new(v, |v, f, fmt| write_sql!(f, fmt, "{v};"))),
			),
		)
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
		if f.alternate() {
			write!(f, "{}", self.to_sql_pretty())
		} else {
			write!(f, "{}", self.to_sql())
		}
	}
}

impl ToSql for TopLevelExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			TopLevelExpr::Begin => f.push_str("BEGIN"),
			TopLevelExpr::Cancel => f.push_str("CANCEL"),
			TopLevelExpr::Commit => f.push_str("COMMIT"),
			TopLevelExpr::Access(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Kill(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Live(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Option(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Use(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Show(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Expr(e) => e.fmt_sql(f, fmt),
		}
	}
}
