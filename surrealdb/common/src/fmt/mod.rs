//! SurrealQL rendering helpers.
//!
//! These live below the AST and the parser because every layer that renders
//! SurrealQL needs them: the sql AST, the expr logical plan, and the catalog's
//! stored-text forms.

mod escape;
use std::cell::Cell;
use std::fmt::Display;

pub use escape::{
	EscapeIdent, EscapeKwFreeIdent, EscapeKwIdent, EscapeObjectKey, EscapeRidKey, QuoteStr,
};
use surrealdb_types::{SqlFormat, ToSql, fmt_non_finite_f64, write_sql};

/// Implements ToSql by calling formatter on contents.
pub struct Fmt<T, F> {
	contents: Cell<Option<T>>,
	formatter: F,
}

impl<T, F: Fn(T, &mut String, SqlFormat)> Fmt<T, F> {
	pub fn new(t: T, formatter: F) -> Self {
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
	pub fn comma_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_comma_separated)
	}

	/// Formats values with a verbar and a space separating them.
	pub fn verbar_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_verbar_separated)
	}

	/// Formats values with a comma and a space separating them or, if pretty
	/// printing is in effect, a comma, a newline, and indentation.
	pub fn pretty_comma_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_pretty_comma_separated)
	}

	/// Formats values with a new line separating them.
	pub fn one_line_separated(into_iter: I) -> Self {
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

pub struct Float(pub f64);

impl ToSql for Float {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match fmt_non_finite_f64(self.0) {
			// Special case: Infinity, -Infinity or NaN
			Some(special) => f.push_str(special),
			// Regular float: add f to distinguish between int and float
			None => {
				self.0.fmt_sql(f, fmt);
				f.push('f');
			}
		}
	}
}

/// Prints a raw [`std::time::Duration`] using SurrealQL duration syntax.
///
/// `std::time::Duration` has no `ToSql` impl of its own (orphan rule: neither
/// the trait nor the type is local), so AST layers holding a raw duration
/// route through this newtype to reuse `surrealdb_types::fmt_duration_sql`.
pub struct SqlDuration(pub std::time::Duration);

impl ToSql for SqlDuration {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		surrealdb_types::fmt_duration_sql(self.0, f);
	}
}

/// Prints a raw [`chrono::DateTime<chrono::Utc>`] using SurrealQL datetime syntax
/// (`d"..."`).
///
/// `chrono::DateTime<Utc>` has no `ToSql` impl of its own (orphan rule: neither
/// the trait nor the type is local), so AST layers holding a raw datetime route
/// through this newtype to reuse `surrealdb_types::fmt_datetime_sql`.
pub struct SqlDatetime(pub chrono::DateTime<chrono::Utc>);

impl ToSql for SqlDatetime {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('d');
		write_sql!(f, fmt, "{}", QuoteStr(&surrealdb_types::fmt_datetime_sql(self.0)));
	}
}
