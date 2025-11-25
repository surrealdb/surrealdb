use std::ops::Bound;

use surrealdb_types::{SqlFormat, ToSql};

use super::RecordIdKeyLit;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdKeyRangeLit {
	pub start: Bound<RecordIdKeyLit>,
	pub end: Bound<RecordIdKeyLit>,
}

impl From<RecordIdKeyRangeLit> for crate::expr::RecordIdKeyRangeLit {
	fn from(value: RecordIdKeyRangeLit) -> Self {
		crate::expr::RecordIdKeyRangeLit {
			start: value.start.map(|x| x.into()),
			end: value.end.map(|x| x.into()),
		}
	}
}

impl From<crate::expr::RecordIdKeyRangeLit> for RecordIdKeyRangeLit {
	fn from(value: crate::expr::RecordIdKeyRangeLit) -> Self {
		RecordIdKeyRangeLit {
			start: value.start.map(|x| x.into()),
			end: value.end.map(|x| x.into()),
		}
	}
}

impl ToSql for RecordIdKeyRangeLit {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match &self.start {
			Bound::Unbounded => {}
			Bound::Included(v) => v.fmt_sql(f, fmt),
			Bound::Excluded(v) => {
				v.fmt_sql(f, fmt);
				f.push('>');
			}
		}
		match &self.end {
			Bound::Unbounded => f.push_str(".."),
			Bound::Excluded(v) => {
				f.push_str("..");
				v.fmt_sql(f, fmt);
			}
			Bound::Included(v) => {
				f.push_str("..=");
				v.fmt_sql(f, fmt);
			}
		}
	}
}
