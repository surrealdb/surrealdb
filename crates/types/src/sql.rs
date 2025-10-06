//! SQL utilities.

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
/// use surrealdb_types::sql::ToSql;
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
		self.fmt_sql(&mut f);
		f
	}

	/// Format the type to a SQL string.
	fn fmt_sql(&self, f: &mut String);
}

impl ToSql for String {
	fn fmt_sql(&self, f: &mut String) {
		f.push_str(&format!("'{self}'"))
	}
}

impl ToSql for &str {
	fn fmt_sql(&self, f: &mut String) {
		f.push_str(&format!("'{self}'"))
	}
}

impl ToSql for &&str {
	fn fmt_sql(&self, f: &mut String) {
		f.push_str(&format!("'{self}'"))
	}
}

impl ToSql for bool {
	fn fmt_sql(&self, f: &mut String) {
		f.push_str(if *self {
			"true"
		} else {
			"false"
		})
	}
}

impl ToSql for i64 {
	fn fmt_sql(&self, f: &mut String) {
		f.push_str(&format!("{}", self))
	}
}
