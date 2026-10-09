use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{AssignOperator, CoverStmts, Expr, Idiom};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[allow(clippy::enum_variant_names)]
pub enum Data {
	/// Represents an empty expression
	#[default]
	EmptyExpression,
	/// Represents a `UPDATE something SET a = b, x = y` clause
	SetExpression(Vec<Assignment>),
	/// Represents a `UPDATE something UNSET x, y, z` clause
	UnsetExpression(Vec<Idiom>),
	/// Represents a `UPDATE something PATCH { ... }` expression
	PatchExpression(Expr),
	/// Represents a `UPDATE something MERGE { ... }` expression
	MergeExpression(Expr),
	/// Represents a `UPDATE something REPLACE { ... }` expression
	ReplaceExpression(Expr),
	/// Represents a `CREATE something CONTENT { ... }` expression
	ContentExpression(Expr),
	/// Represents a `INSERT INTO table { ... }` expression
	SingleExpression(Expr),
	/// Represents a `INSERT INTO table (... fields ...) VALUES (... values ...)` expression
	ValuesExpression(Vec<Vec<(Idiom, Expr)>>),
	/// Represents a `ON DUPLICATE KEY UPDATE ...` clause
	UpdateExpression(Vec<Assignment>),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Assignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Expr,
}

impl ToSql for Data {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::EmptyExpression => {}
			Self::SetExpression(v) => {
				f.push_str("SET ");
				for (i, arg) in v.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, sql_fmt, "{} {} ", arg.place, arg.operator);
					CoverStmts(&arg.value).fmt_sql(f, sql_fmt);
				}
			}
			Self::UnsetExpression(v) => {
				f.push_str("UNSET ");
				for (i, idiom) in v.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, sql_fmt, "{}", idiom);
				}
			}
			Self::PatchExpression(v) => {
				write_sql!(f, sql_fmt, "PATCH {v}");
			}
			Self::MergeExpression(v) => {
				write_sql!(f, sql_fmt, "MERGE {v}");
			}
			Self::ReplaceExpression(v) => {
				write_sql!(f, sql_fmt, "REPLACE {v}");
			}
			Self::ContentExpression(v) => {
				write_sql!(f, sql_fmt, "CONTENT {v}");
			}
			Self::SingleExpression(v) => CoverStmts(v).fmt_sql(f, sql_fmt),
			Self::ValuesExpression(v) => {
				f.push('(');
				if let Some(first) = v.first() {
					for (i, (idiom, _)) in first.iter().enumerate() {
						if i > 0 {
							f.push_str(", ");
						}
						write_sql!(f, sql_fmt, "{idiom}");
					}
				}
				f.push_str(") VALUES ");
				for (i, row) in v.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					f.push('(');
					for (j, (_, expr)) in row.iter().enumerate() {
						if j > 0 {
							f.push_str(", ");
						}
						CoverStmts(expr).fmt_sql(f, sql_fmt);
					}
					f.push(')');
				}
			}
			Self::UpdateExpression(v) => {
				f.push_str("ON DUPLICATE KEY UPDATE ");
				for (i, arg) in v.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, sql_fmt, "{} {} ", arg.place, arg.operator);
					CoverStmts(&arg.value).fmt_sql(f, sql_fmt);
				}
			}
		}
	}
}
