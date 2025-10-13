//! SQL utilities.

use crate::utils::escape::QuoteStr;

/// Trait for types that can be converted to SQL Object Notation (SQON).
///
/// ⚠️ **EXPERIMENTAL**: This trait is not stable and may change
/// or be removed in any release without a major version bump.
/// Use at your own risk.
///
/// There's an important distinction between this trait and `Display`.
/// `Display` should be used for human-readable output, it does not particularly
/// need to be SQON compatible but it may happen to be.
/// `ToSqon` should be used for SQON compatible output.
///
/// A good example is Datetime:
/// ```rust
/// use surrealdb_types::sql::ToSqon;
/// use surrealdb_types::Datetime;
/// use chrono::{TimeZone, Utc};
///
/// let datetime = Datetime::new(Utc.with_ymd_and_hms(2025, 10, 3, 10, 2, 32).unwrap() + chrono::Duration::microseconds(873077));
/// assert_eq!(datetime.to_string(), "2025-10-03T10:02:32.873077Z");
/// assert_eq!(datetime.to_sql(), "d'2025-10-03T10:02:32.873077Z'");
/// ```
pub trait ToSqon {
	/// Convert the type to a SQL string.
	fn to_sqon(&self) -> String {
		let mut f = String::new();
		self.fmt_sqon(&mut f);
		f
	}

	/// Format the type to a SQL string.
	fn fmt_sqon(&self, f: &mut String);
}

/// Macro for writing to SQON string.
///
/// This will panic if the write fails but the expectation is that it is only used in ToSqon
/// implementations which operate on a `&mut String`. `write!` cannot fail when writing to a
/// `String`.
macro_rules! write_sqon {
	($f:expr, $($tt:tt)*) => {{
		use std::fmt::Write;
		write!($f, $($tt)*).expect("Write cannot fail when writing to a String")
	}}
}

pub(crate) use write_sqon;

impl ToSqon for String {
	fn fmt_sqon(&self, f: &mut String) {
		write_sqon!(f, "{}", QuoteStr(self))
	}
}

impl ToSqon for &str {
	fn fmt_sqon(&self, f: &mut String) {
		write_sqon!(f, "{}", QuoteStr(self))
	}
}

impl ToSqon for &&str {
	fn fmt_sqon(&self, f: &mut String) {
		write_sqon!(f, "{}", QuoteStr(self))
	}
}

impl ToSqon for bool {
	fn fmt_sqon(&self, f: &mut String) {
		f.push_str(if *self {
			"true"
		} else {
			"false"
		})
	}
}

impl ToSqon for i64 {
	fn fmt_sqon(&self, f: &mut String) {
		write_sqon!(f, "{}", self)
	}
}
