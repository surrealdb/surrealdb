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
/// assert_eq!(datetime.to_sqlfn(), "d'2025-10-03T10:02:32.873077Z'");
/// ```
pub trait ToSql {
	/// Convert the type to a SQL string.
	fn to_sql(&self) -> String {
		let mut f = String::new();
		self.fmt_sql(&mut f, PrettyMode::None);
		f
	}

	/// Convert the type to a pretty SQL string.
	fn to_sql_pretty(&self) -> String {
		let mut f = String::new();
		self.fmt_sql(&mut f, PrettyMode::Indent(0));
		f
	}

	/// Format the type to a SQL string.
	fn fmt_sql(&self, f: &mut String, pretty: PrettyMode);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrettyMode {
	None,
	Indent(u16),
}

impl PrettyMode {
	pub fn is_none(&self) -> bool {
		matches!(self, Self::None)
	}

	pub fn is_indent(&self) -> bool {
		matches!(self, Self::Indent(_))
	}

	pub fn indent(&self) -> PrettyMode {
		match self {
			Self::None => Self::None,
			Self::Indent(n) => Self::Indent(n + 1),
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
	fn fmt_sql(&self, f: &mut String, _pretty: PrettyMode) {
		write_sql!(f, "{}", QuoteStr(self))
	}
}

impl ToSql for &str {
	fn fmt_sql(&self, f: &mut String, _pretty: PrettyMode) {
		write_sql!(f, "{}", QuoteStr(self))
	}
}

impl ToSql for &&str {
	fn fmt_sql(&self, f: &mut String, _pretty: PrettyMode) {
		write_sql!(f, "{}", QuoteStr(self))
	}
}

impl ToSql for bool {
	fn fmt_sql(&self, f: &mut String, _pretty: PrettyMode) {
		f.push_str(if *self {
			"true"
		} else {
			"false"
		})
	}
}

impl ToSql for i64 {
	fn fmt_sql(&self, f: &mut String, _pretty: PrettyMode) {
		write_sql!(f, "{}", self)
	}
}
