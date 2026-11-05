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

	/// Whether evaluating this range can modify data; see [`Literal::read_only`].
	pub fn read_only(&self) -> bool {
		let bound_read_only = |bound: &Bound<RecordIdKeyLit>| match bound {
			Bound::Included(x) | Bound::Excluded(x) => x.read_only(),
			Bound::Unbounded => true,
		};
		bound_read_only(&self.start) && bound_read_only(&self.end)
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
