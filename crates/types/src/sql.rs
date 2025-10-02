//! SQL utilities.

use std::fmt::Write;
use std::ops::Bound;

use crate::{Array, RecordIdKeyRange, Uuid};

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
	fn to_sql(&self) -> anyhow::Result<String> {
		let mut f = String::new();
		self.fmt_sql(&mut f)?;
		Ok(f)
	}

	/// Format the type to a SQL string.
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result;
}

impl ToSql for String {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		f.write_fmt(format_args!("'{self}'"))
	}
}

impl ToSql for i64 {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		f.write_fmt(format_args!("{}", self))
	}
}

impl ToSql for &str {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		f.write_fmt(format_args!("'{self}'"))
	}
}

impl ToSql for &&str {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		f.write_fmt(format_args!("'{self}'"))
	}
}

impl ToSql for Uuid {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		f.write_fmt(format_args!("u'{}'", self))
	}
}

impl ToSql for Array {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		f.write_str("[")?;
		for (i, v) in self.iter().enumerate() {
			v.fmt_sql(f)?;
			if i < self.len() - 1 {
				f.write_str(", ")?;
			}
		}
		f.write_str("]")?;
		Ok(())
	}
}

impl ToSql for RecordIdKeyRange {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		match &self.start {
			Bound::Unbounded => {}
			Bound::Included(v) => {
				v.fmt_sql(f)?;
			}
			Bound::Excluded(v) => {
				f.write_str(">")?;
				v.fmt_sql(f)?
			}
		};

		f.write_str("..")?;

		match &self.end {
			Bound::Unbounded => {}
			Bound::Included(v) => {
				f.write_str("=")?;
				v.fmt_sql(f)?;
			}
			Bound::Excluded(v) => {
				v.fmt_sql(f)?;
			}
		};

		Ok(())
	}
}

impl ToSql for bool {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		f.write_str(if *self {
			"true"
		} else {
			"false"
		})
	}
}

impl ToSql for crate::Number {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}

impl ToSql for crate::Duration {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}

impl ToSql for crate::Datetime {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "d'{}'", self)
	}
}

impl ToSql for crate::Geometry {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}

impl ToSql for crate::Bytes {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}

impl ToSql for crate::File {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}

impl ToSql for crate::Range {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}

impl ToSql for crate::Regex {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "{}", self)
	}
}
