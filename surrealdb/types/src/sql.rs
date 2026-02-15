//! SQL utilities.

use std::sync::Arc;

pub use surrealdb_types_derive::write_sql;

use crate as surrealdb_types;
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
/// let dt = Utc.with_ymd_and_hms(2025, 10, 3, 10, 2, 32).unwrap() + chrono::Duration::microseconds(873077);
/// let datetime = Datetime::from(dt);
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
	/// Indented by the number of tabs specified.
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
		if let SqlFormat::Indented(level) = fmt
			&& level > 0
		{
			for _ in 0..(level - 1) {
				f.push('\t');
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
	use crate::utils::escape::EscapeObjectKey;

	let pairs: Vec<_> = pairs.into_iter().collect();

	if fmt.is_pretty() && !pairs.is_empty() {
		f.push('\n');
		fmt.write_indent(f);
	}
	for (i, (key, value)) in pairs.iter().enumerate() {
		if i > 0 {
			fmt.write_separator(f);
		}
		write_sql!(f, fmt, "{}: {}", EscapeObjectKey(key.as_ref()), value);
	}
	if fmt.is_pretty() && !pairs.is_empty() {
		f.push('\n');
		// Write one level less indentation for the closing bracket
		if let SqlFormat::Indented(level) = fmt
			&& level > 0
		{
			for _ in 0..(level - 1) {
				f.push('\t');
			}
		}
	}
}

impl ToSql for String {
	#[inline]
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(self.as_str());
	}
}

impl ToSql for str {
	#[inline]
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(self);
	}
}

impl ToSql for &str {
	#[inline]
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(self);
	}
}

impl ToSql for char {
	#[inline]
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push(*self);
	}
}

impl ToSql for bool {
	#[inline]
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(if *self {
			"true"
		} else {
			"false"
		})
	}
}

macro_rules! impl_to_sql_for_numeric {
	($($t:ty),+) => {
		$(
			impl ToSql for $t {
				#[inline]
				fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
					f.push_str(&self.to_string())
				}
			}
		)+
	};
}

impl_to_sql_for_numeric!(u8, u16, u32, u64, i8, i16, i32, i64, usize, isize, f32, f64);

impl<T: ToSql> ToSql for &T {
	#[inline]
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		(**self).fmt_sql(f, fmt)
	}
}

// Blanket impl for Box
impl<T: ToSql + ?Sized> ToSql for Box<T> {
	#[inline]
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		(**self).fmt_sql(f, fmt)
	}
}

// Blanket impl for Arc
impl<T: ToSql + ?Sized> ToSql for Arc<T> {
	#[inline]
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		(**self).fmt_sql(f, fmt)
	}
}

impl ToSql for uuid::Uuid {
	#[inline]
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('u');
		QuoteStr(&self.to_string()).fmt_sql(f, fmt);
	}
}

impl ToSql for rust_decimal::Decimal {
	#[inline]
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_string().fmt_sql(f, fmt);
		f.push_str("dec");
	}
}
