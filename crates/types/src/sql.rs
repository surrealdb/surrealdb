//! SQL utilities.

use crate::utils::escape::QuoteStr;

/// Trait for types that can be converted to SQL representation.
///
/// ⚠️ **EXPERIMENTAL**: This trait is not stable and may change
/// or be removed in any release without a major version bump.
/// Use at your own risk.
///
/// There's an important distinction between this trait and `Display`.
/// `Display` should be used for human-readable output, it does not particularly
/// need to be SQL compatible but it may happen to be.
/// `ToSql` should be used for SQL compatible output.
///
/// A good example is Datetime:
/// ```rust
/// use surrealdb_types::ToSql;
/// use surrealdb_types::Datetime;
/// use chrono::{TimeZone, Utc};
///
/// let datetime = Datetime::new(Utc.with_ymd_and_hms(2025, 10, 3, 10, 2, 32).unwrap() + chrono::Duration::microseconds(873077));
/// assert_eq!(datetime.to_string(), "2025-10-03T10:02:32.873077Z");
/// assert_eq!(datetime.to_sql(), "d'2025-10-03T10:02:32.873077Z'");
/// ```
pub trait ToSql {
	/// Convert the type to a SQL string.
	fn to_sql(&self) -> String {
		let mut f = String::new();
		self.fmt_sql(&mut f, SqlFormat::SingleLine);
		f
	}

	/// Convert the type to a pretty-printed SQL string with indentation.
	fn to_sql_pretty(&self) -> String {
		let mut f = String::new();
		self.fmt_sql(&mut f, SqlFormat::Indented(0));
		f
	}

	/// Format the type to a SQL string.
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat);
}

/// SQL formatting mode for pretty printing.
#[derive(Debug, Clone, Copy)]
pub enum SqlFormat {
	/// Single line formatting.
	SingleLine,
	/// Indented by the number of tabs specified (2 spaces per tab).
	Indented(u8),
}

impl SqlFormat {
	/// Returns true if this is pretty (indented) formatting.
	pub fn is_pretty(&self) -> bool {
		matches!(self, SqlFormat::Indented(_))
	}

	/// Increments the indentation level.
	pub fn increment(&self) -> Self {
		match self {
			SqlFormat::SingleLine => SqlFormat::SingleLine,
			SqlFormat::Indented(level) => SqlFormat::Indented(level.saturating_add(1)),
		}
	}

	/// Writes indentation to the string.
	pub fn write_indent(&self, f: &mut String) {
		if let SqlFormat::Indented(level) = self {
			for _ in 0..*level {
				f.push('\t');
			}
		}
	}

	/// Writes a separator (comma + space or comma + newline + indent).
	pub fn write_separator(&self, f: &mut String) {
		match self {
			SqlFormat::SingleLine => f.push_str(", "),
			SqlFormat::Indented(_) => {
				f.push(',');
				f.push('\n');
				self.write_indent(f);
			}
		}
	}
}

/// Formats a slice of items that implement ToSql with comma separation.
pub fn fmt_sql_comma_separated<T: ToSql>(items: &[T], f: &mut String, fmt: SqlFormat) {
	if fmt.is_pretty() && !items.is_empty() {
		f.push('\n');
		fmt.write_indent(f);
	}
	for (i, item) in items.iter().enumerate() {
		if i > 0 {
			fmt.write_separator(f);
		}
		item.fmt_sql(f, fmt);
	}
	if fmt.is_pretty() && !items.is_empty() {
		f.push('\n');
		// Write one level less indentation for the closing bracket
		if let SqlFormat::Indented(level) = fmt {
			if level > 0 {
				for _ in 0..(level - 1) {
					f.push('\t');
				}
			}
		}
	}
}

/// Formats key-value pairs with comma separation.
pub fn fmt_sql_key_value<'a, V: ToSql + 'a>(
	pairs: impl IntoIterator<Item = (impl AsRef<str>, &'a V)>,
	f: &mut String,
	fmt: SqlFormat,
) {
	use std::fmt::Write;

	use crate::utils::escape::EscapeKey;

	let pairs: Vec<_> = pairs.into_iter().collect();

	if fmt.is_pretty() && !pairs.is_empty() {
		f.push('\n');
		fmt.write_indent(f);
	}
	for (i, (key, value)) in pairs.iter().enumerate() {
		if i > 0 {
			fmt.write_separator(f);
		}
		write!(f, "{}: ", EscapeKey(key.as_ref()))
			.expect("Write cannot fail when writing to a String");
		value.fmt_sql(f, fmt);
	}
	if fmt.is_pretty() && !pairs.is_empty() {
		f.push('\n');
		// Write one level less indentation for the closing bracket
		if let SqlFormat::Indented(level) = fmt {
			if level > 0 {
				for _ in 0..(level - 1) {
					f.push('\t');
				}
			}
		}
	}
}

/// Macro for writing to a SQL string.
///
/// This will panic if the write fails but the expectation is that it is only used in ToSql
/// implementations which operate on a `&mut String`. `write!` cannot fail when writing to a
/// `String`.
#[macro_export]
macro_rules! write_sql {
	($f:expr, $($tt:tt)*) => {{
		use std::fmt::Write;
		let __f: &mut String = $f;
		write!(__f, $($tt)*).expect("Write cannot fail when writing to a String")
	}}
}

impl ToSql for String {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "{}", QuoteStr(self))
	}
}

impl ToSql for &str {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "{}", QuoteStr(self))
	}
}

impl ToSql for &&str {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "{}", QuoteStr(self))
	}
}

impl ToSql for bool {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(if *self {
			"true"
		} else {
			"false"
		})
	}
}

impl ToSql for i64 {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "{}", self)
	}
}
