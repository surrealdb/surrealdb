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
///
/// let datetime = Datetime::now();
/// assert_eq!(datetime.to_string(), "2021-01-01T00:00:00Z");
/// assert_eq!(datetime.to_sql(), "'d2021-01-01T00:00:00Z'");
/// ```
pub trait ToSql {
	/// Convert the type to a SQL string.
	fn to_sql(&self) -> anyhow::Result<String>;
}

impl ToSql for String {
	fn to_sql(&self) -> anyhow::Result<String> {
		Ok(format!("'{self}'"))
	}
}

impl ToSql for &str {
	fn to_sql(&self) -> anyhow::Result<String> {
		Ok(format!("'{self}'"))
	}
}

impl ToSql for &&str {
	fn to_sql(&self) -> anyhow::Result<String> {
		Ok(format!("'{self}'"))
	}
}
