use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::{
	AccessStatement, KillStatement, LiveStatement, OptionStatement, ShowStatement, UseStatement,
};
use crate::expr::{BinaryOperator, Expr, Literal, Param};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Query {
	pub expressions: Vec<TopLevelExpr>,
}

/// How a transaction-bound staleness is written: a literal or a parameter
/// resolved when the transaction opens.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Staleness {
	Duration(std::time::Duration),
	Param(String),
}

/// The `READ ONLY` clause of `BEGIN`.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct ReadOnly {
	pub max_staleness: Option<Staleness>,
	pub strict: bool,
}

/// The options of a `BEGIN` statement. The default is a plain `BEGIN`.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct BeginOptions {
	pub read_only: Option<ReadOnly>,
}

impl ToSql for BeginOptions {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("BEGIN");
		let Some(read_only) = &self.read_only else {
			return;
		};
		f.push_str(" READ ONLY");
		match &read_only.max_staleness {
			None => {}
			Some(Staleness::Duration(d)) => {
				f.push_str(" MAX STALENESS ");
				surrealdb_types::Duration::from(*d).fmt_sql(f, fmt);
			}
			Some(Staleness::Param(name)) => {
				f.push_str(" MAX STALENESS ");
				Param::from(name.clone()).fmt_sql(f, fmt);
			}
		}
		if read_only.strict {
			f.push_str(" STRICT");
		}
	}
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum TopLevelExpr {
	Begin(BeginOptions),
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
	/// Prepare credentials only for a statement the executor has reached.
	pub fn prepare_credentials(&mut self) {
		use crate::expr::credentials::prepare;
		match self {
			Self::Access(x) => prepare(&mut **x),
			Self::Kill(x) => prepare(x),
			Self::Live(x) => {
				if x.id.is_nil() {
					x.id = uuid::Uuid::new_v4();
					x.node = uuid::Uuid::new_v4();
				}
				prepare(&mut **x)
			}
			Self::Option(x) => prepare(x),
			Self::Use(x) => prepare(x),
			Self::Show(x) => prepare(x),
			Self::Expr(x) => prepare(x),
			Self::Begin(_) | Self::Cancel | Self::Commit => {}
		}
	}

	/// Render text for logs without passwords or verifier material.
	pub fn to_redacted_sql(&self) -> String {
		let mut output = String::new();
		self.fmt_redacted_sql(&mut output, SqlFormat::SingleLine);
		output
	}

	pub fn fmt_redacted_sql(&self, output: &mut String, fmt: SqlFormat) {
		use surrealdb_types::ToSql;

		use crate::expr::credentials::redact;
		let mut value = self.clone();
		match &mut value {
			Self::Access(x) => redact(&mut **x),
			Self::Kill(x) => redact(x),
			Self::Live(x) => redact(&mut **x),
			Self::Option(x) => redact(x),
			Self::Use(x) => redact(x),
			Self::Show(x) => redact(x),
			Self::Expr(x) => redact(x),
			Self::Begin(_) | Self::Cancel | Self::Commit => {}
		}
		value.fmt_sql(output, fmt)
	}

	/// Check if we require a writeable transaction
	pub fn read_only(&self) -> bool {
		match self {
			TopLevelExpr::Begin(_)
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

impl ToSql for TopLevelExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			TopLevelExpr::Begin(options) => options.fmt_sql(f, fmt),
			TopLevelExpr::Cancel => f.push_str("CANCEL"),
			TopLevelExpr::Commit => f.push_str("COMMIT"),
			TopLevelExpr::Access(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Kill(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Live(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Option(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Use(s) => s.fmt_sql(f, fmt),
			TopLevelExpr::Show(s) => s.fmt_sql(f, fmt),
			// `$p = 1` alone is the deprecated `LET` without `let`, so an
			// equality on a parameter keeps the parentheses that make it a
			// comparison.
			TopLevelExpr::Expr(
				e @ Expr::Binary {
					left,
					op: BinaryOperator::Equal,
					..
				},
			) if matches!(**left, Expr::Param(_)) => {
				f.push('(');
				e.fmt_sql(f, fmt);
				f.push(')');
			}
			TopLevelExpr::Expr(e) => e.fmt_sql(f, fmt),
		}
	}
}

impl ToSql for Query {
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

impl Query {
	pub fn single_expr(expr: impl Into<Expr>) -> Self {
		Self {
			expressions: vec![TopLevelExpr::Expr(expr.into())],
		}
	}
	pub fn num_statements(&self) -> usize {
		self.expressions.len()
	}
}

impl Query {
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
		self.expressions.push(TopLevelExpr::Expr(Expr::Param(Param::from(name))));
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
	pub fn into_execution_units(self) -> Vec<Query> {
		let mut units = Vec::new();
		let mut block: Option<Vec<TopLevelExpr>> = None;
		for expr in self.expressions {
			match expr {
				TopLevelExpr::Begin(_) => {
					block.get_or_insert_with(Vec::new).push(expr);
				}
				TopLevelExpr::Commit | TopLevelExpr::Cancel => match block.take() {
					Some(mut statements) => {
						statements.push(expr);
						units.push(Query {
							expressions: statements,
						});
					}
					None => units.push(Query {
						expressions: vec![expr],
					}),
				},
				other => match block.as_mut() {
					Some(statements) => statements.push(other),
					None => units.push(Query {
						expressions: vec![other],
					}),
				},
			}
		}
		if let Some(statements) = block {
			units.push(Query {
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
