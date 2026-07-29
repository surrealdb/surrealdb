use std::fmt::{self};

use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::statements::{
	AccessStatement, KillStatement, LiveStatement, OptionStatement, ShowStatement, UseStatement,
};
use crate::{Expr, Literal, Param};

#[derive(Clone, Copy, Eq, PartialEq, Debug, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ExplainFormat {
	#[default]
	Text,
	Json,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Ast {
	pub expressions: Vec<TopLevelExpr>,
}

impl Ast {
	/// Creates an ast with a signle expression
	pub fn single_expr(expr: Expr) -> Self {
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

	/// Whether a top-level `CANCEL` (transaction rollback) appears in this AST.
	pub fn contains_cancel(&self) -> bool {
		self.expressions.iter().any(|e| matches!(e, TopLevelExpr::Cancel))
	}

	/// Whether this AST is exactly a single top-level `COMMIT`.
	pub fn is_sole_commit(&self) -> bool {
		matches!(self.expressions.as_slice(), [TopLevelExpr::Commit])
	}

	/// Whether this AST is exactly a single top-level `CANCEL`.
	pub fn is_sole_cancel(&self) -> bool {
		matches!(self.expressions.as_slice(), [TopLevelExpr::Cancel])
	}

	/// Split this query into sequentially executable units: each
	/// non-transactional top-level statement becomes a unit of its own, while
	/// the statements of a `BEGIN`..`COMMIT`/`CANCEL` block stay together as
	/// one unit. A `BEGIN` without a matching `COMMIT`/`CANCEL` keeps the
	/// remainder of the query in its unit.
	pub fn into_execution_units(self) -> Vec<Ast> {
		let mut units = Vec::new();
		let mut block: Option<Vec<TopLevelExpr>> = None;
		for expr in self.expressions {
			match expr {
				TopLevelExpr::Begin => {
					block.get_or_insert_with(Vec::new).push(expr);
				}
				TopLevelExpr::Commit | TopLevelExpr::Cancel => match block.take() {
					Some(mut statements) => {
						statements.push(expr);
						units.push(Ast {
							expressions: statements,
						});
					}
					None => units.push(Ast {
						expressions: vec![expr],
					}),
				},
				other => match block.as_mut() {
					Some(statements) => statements.push(other),
					None => units.push(Ast {
						expressions: vec![other],
					}),
				},
			}
		}
		if let Some(statements) = block {
			units.push(Ast {
				expressions: statements,
			});
		}
		units
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

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
