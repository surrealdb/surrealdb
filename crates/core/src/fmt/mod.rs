//! SurrealQL formatting utilities.

#[cfg(test)]
mod test;

mod escape;
use std::cell::Cell;
use std::fmt::Display;

pub use escape::{EscapeIdent, EscapeKey, EscapeKwFreeIdent, EscapeKwIdent, EscapeRid, QuoteStr};
use surrealdb_types::{SqlFormat, ToSql};

use crate::sql;

/// Implements ToSql by calling formatter on contents.
pub(crate) struct Fmt<T, F> {
	contents: Cell<Option<T>>,
	formatter: F,
}

impl<T, F: Fn(T, &mut String, SqlFormat)> Fmt<T, F> {
	pub(crate) fn new(t: T, formatter: F) -> Self {
		Self {
			contents: Cell::new(Some(t)),
			formatter,
		}
	}
}

impl<T, F: Fn(T, &mut String, SqlFormat)> ToSql for Fmt<T, F> {
	/// fmt is single-use only.
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let contents = self.contents.replace(None).expect("only call Fmt::fmt once");
		(self.formatter)(contents, f, fmt)
	}
}

impl<I: IntoIterator<Item = T>, T: ToSql> Fmt<I, fn(I, &mut String, SqlFormat)> {
	/// Formats values with a comma and a space separating them.
	pub(crate) fn comma_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_comma_separated)
	}

	/// Formats values with a verbar and a space separating them.
	pub(crate) fn verbar_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_verbar_separated)
	}

	/// Formats values with a comma and a space separating them or, if pretty
	/// printing is in effect, a comma, a newline, and indentation.
	pub(crate) fn pretty_comma_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_pretty_comma_separated)
	}

	/// Formats values with a new line separating them.
	pub(crate) fn one_line_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_one_line_separated)
	}
}

fn fmt_comma_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	fmt: SqlFormat,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			f.push_str(", ");
		}
		v.fmt_sql(f, fmt);
	}
}

fn fmt_verbar_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	fmt: SqlFormat,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			f.push_str(" | ");
		}
		v.fmt_sql(f, fmt);
	}
}

fn fmt_pretty_comma_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	fmt: SqlFormat,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			if fmt.is_pretty() {
				f.push_str(",\n");
				// pretty_sequence_item();
			} else {
				f.push_str(", ");
			}
		}
		v.fmt_sql(f, fmt);
	}
}

fn fmt_one_line_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	fmt: SqlFormat,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			f.push('\n');
			// if fmt.is_pretty() {
			// 	f.push('\n');
			// } else {
			// 	f.push('\n');
			// }
		}
		v.fmt_sql(f, fmt);
	}
}

/// Creates a formatting function that joins iterators with an arbitrary
/// separator.
pub fn fmt_separated_by<T: ToSql, I: IntoIterator<Item = T>>(
	separator: impl Display,
) -> impl Fn(I, &mut String, SqlFormat) {
	move |into_iter: I, f: &mut String, fmt: SqlFormat| {
		let separator = separator.to_string();
		for (i, v) in into_iter.into_iter().enumerate() {
			if i > 0 {
				f.push_str(&separator);
			}
			v.fmt_sql(f, fmt);
		}
	}
}

pub struct CoverStmtsSql<'a>(pub &'a sql::Expr);

impl ToSql for CoverStmtsSql<'_> {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.0 {
			sql::Expr::Literal(_)
			| sql::Expr::Param(_)
			| sql::Expr::Idiom(_)
			| sql::Expr::Table(_)
			| sql::Expr::Mock(_)
			| sql::Expr::Block(_)
			| sql::Expr::Constant(_)
			| sql::Expr::Prefix {
				..
			}
			| sql::Expr::Postfix {
				..
			}
			| sql::Expr::Binary {
				..
			}
			| sql::Expr::FunctionCall(_)
			| sql::Expr::Closure(_)
			| sql::Expr::Break
			| sql::Expr::Continue
			| sql::Expr::Throw(_) => self.0.fmt_sql(f, fmt),
			sql::Expr::Return(_)
			| sql::Expr::IfElse(_)
			| sql::Expr::Select(_)
			| sql::Expr::Create(_)
			| sql::Expr::Update(_)
			| sql::Expr::Upsert(_)
			| sql::Expr::Delete(_)
			| sql::Expr::Relate(_)
			| sql::Expr::Insert(_)
			| sql::Expr::Define(_)
			| sql::Expr::Remove(_)
			| sql::Expr::Rebuild(_)
			| sql::Expr::Alter(_)
			| sql::Expr::Info(_)
			| sql::Expr::Foreach(_)
			| sql::Expr::Let(_)
			| sql::Expr::Sleep(_) => {
				f.push('(');
				self.0.fmt_sql(f, fmt);
				f.push(')')
			}
		}
	}
}

pub struct Float(pub f64);

impl ToSql for Float {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if !self.0.is_finite() {
			if self.0.is_nan() {
				f.push_str("NaN");
			} else if self.0.is_sign_positive() {
				f.push_str("Infinity");
			} else {
				f.push_str("-Infinity");
			}
		} else {
			self.0.fmt_sql(f, fmt);
			f.push('f');
		}
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use crate::syn::{expr, parse};

	#[test]
	fn pretty_query() {
		let query = parse("SELECT * FROM {foo: [1, 2, 3]};").unwrap();
		assert_eq!(query.to_sql(), "SELECT * FROM { foo: [1, 2, 3] };");
		assert_eq!(
			query.to_sql_pretty(),
			"SELECT * FROM {\n\tfoo: [\n\t\t1,\n\t\t2,\n\t\t3\n\t]\n};"
		);
	}

	#[test]
	fn pretty_define_query() {
		let query = parse("DEFINE TABLE test SCHEMAFULL PERMISSIONS FOR create, update, delete NONE FOR select WHERE public = true;").unwrap();
		assert_eq!(
			query.to_sql(),
			"DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select WHERE public = true, FOR create, update, delete NONE;"
		);
		assert_eq!(
			query.to_sql_pretty(),
			"DEFINE TABLE test TYPE NORMAL SCHEMAFULL\n\tPERMISSIONS\n\tFOR select WHERE public = true,\n\tFOR create, update, delete NONE;"
		);
	}

	#[test]
	fn pretty_value() {
		let value = expr("{foo: [1, 2, 3]}").unwrap();
		assert_eq!(value.to_sql(), "{ foo: [1, 2, 3] }");
		assert_eq!(value.to_sql_pretty(), "{\n\tfoo: [\n\t\t1,\n\t\t2,\n\t\t3\n\t]\n}");
	}

	#[test]
	fn pretty_array() {
		let array = expr("[1, 2, 3]").unwrap();
		assert_eq!(array.to_sql(), "[1, 2, 3]");
		assert_eq!(array.to_sql_pretty(), "[\n\t1,\n\t2,\n\t3\n]");
	}
}
