use std::ops::Bound;

use common::fmt::EscapeKwFreeIdent;
use common::range::TypedRange;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Mock {
	Count(TableName, i64),
	Range(TableName, TypedRange<i64>),
	// Add new variants here
}

impl ToSql for Mock {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Mock::Count(tb, c) => {
				write_sql!(f, fmt, "|{}:{}|", EscapeKwFreeIdent(tb.as_str()), c);
			}
			Mock::Range(tb, r) => {
				write_sql!(f, fmt, "|{}:", EscapeKwFreeIdent(tb.as_str()));
				match r.start {
					Bound::Included(x) => write_sql!(f, fmt, "{x}.."),
					Bound::Excluded(x) => write_sql!(f, fmt, "{x}>.."),
					Bound::Unbounded => f.push_str(".."),
				}
				match r.end {
					Bound::Included(x) => write_sql!(f, fmt, "={x}|"),
					Bound::Excluded(x) => write_sql!(f, fmt, "{x}|"),
					Bound::Unbounded => f.push('|'),
				}
			}
		}
	}
}
