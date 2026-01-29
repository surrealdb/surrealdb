use priority_lfu::DeepSizeOf;
use surrealdb_types::{SqlFormat, ToSql};

use crate::val::range::{IntegerRangeIter, TypedRange};
use crate::val::{RecordId, RecordIdKey, TableName};

pub(crate) struct IntoIter {
	table: TableName,
	key: IntoIterKey,
}

pub(crate) enum IntoIterKey {
	Count(i64),
	Range(IntegerRangeIter),
}

impl Iterator for IntoIter {
	type Item = RecordId;

	fn next(&mut self) -> Option<RecordId> {
		match self.key {
			IntoIterKey::Count(ref mut c) => {
				if *c == 0 {
					None
				} else {
					*c -= 1;
					Some(RecordId {
						table: self.table.clone(),
						key: RecordIdKey::rand(),
					})
				}
			}
			IntoIterKey::Range(ref mut r) => {
				let k = r.next()?;
				Some(RecordId {
					table: self.table.clone(),
					key: RecordIdKey::from(k),
				})
			}
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		const MAX: usize = const {
			match usize::BITS {
				64 => i64::MAX as usize,
				32 => usize::MAX,
				_ => panic!("unsupported pointer width"),
			}
		};

		match &self.key {
			IntoIterKey::Count(x) => {
				let x = *x;
				let lower = x.min(MAX as i64) as usize;
				let upper = if x < MAX as i64 {
					Some(x as usize)
				} else {
					None
				};
				(lower, upper)
			}
			IntoIterKey::Range(r) => r.size_hint(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, DeepSizeOf)]
pub(crate) enum Mock {
	Count(TableName, i64),
	Range(TableName, TypedRange<i64>),
}

impl Mock {
	pub(crate) fn table(&self) -> &TableName {
		match self {
			Mock::Count(t, _) => t,
			Mock::Range(t, _) => t,
		}
	}
}

impl IntoIterator for Mock {
	type Item = RecordId;
	type IntoIter = IntoIter;
	fn into_iter(self) -> Self::IntoIter {
		match self {
			Mock::Count(t, k) => IntoIter {
				table: t,
				key: IntoIterKey::Count(k.max(0)),
			},
			Mock::Range(t, r) => IntoIter {
				table: t,
				key: IntoIterKey::Range(r.iter()),
			},
		}
	}
}

impl ToSql for Mock {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let mock: crate::sql::Mock = self.clone().into();
		mock.fmt_sql(f, sql_fmt);
	}
}
