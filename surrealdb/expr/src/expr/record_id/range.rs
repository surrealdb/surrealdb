use std::ops::Bound;

use surrealdb_types::{SqlFormat, ToSql};

use super::RecordIdKeyLit;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RecordIdKeyRangeLit {
	pub start: Bound<RecordIdKeyLit>,
	pub end: Bound<RecordIdKeyLit>,
}

impl RecordIdKeyRangeLit {
	pub fn is_static(&self) -> bool {
		let res = match &self.start {
			Bound::Included(x) => x.is_static(),
			Bound::Excluded(x) => x.is_static(),
			Bound::Unbounded => true,
		};

		if !res {
			return false;
		}

		match &self.end {
			Bound::Included(x) => x.is_static(),
			Bound::Excluded(x) => x.is_static(),
			Bound::Unbounded => true,
		}
	}
}

impl ToSql for RecordIdKeyRangeLit {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let range: crate::sql::record_id::range::RecordIdKeyRangeLit = self.clone().into();
		range.fmt_sql(f, fmt);
	}
}
