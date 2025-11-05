use std::fmt;
use std::ops::Bound;

use crate::fmt::EscapeKwFreeIdent;
use crate::val::range::{IntegerRangeIter, TypedRange};
use crate::val::{RecordId, RecordIdKey};

pub(crate) struct IntoIter {
	table: String,
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Mock {
	Count(String, i64),
	Range(String, TypedRange<i64>),
	// Add new variants here
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

impl fmt::Display for Mock {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Mock::Count(tb, c) => {
				write!(f, "|{}:{}|", EscapeKwFreeIdent(tb), c)
			}
			Mock::Range(tb, r) => {
				write!(f, "|{}:", EscapeKwFreeIdent(tb))?;
				match r.start {
					Bound::Included(x) => write!(f, "{x}..")?,
					Bound::Excluded(x) => write!(f, "{x}>..")?,
					Bound::Unbounded => write!(f, "..")?,
				}
				match r.end {
					Bound::Included(x) => write!(f, "={x}|"),
					Bound::Excluded(x) => write!(f, "{x}|"),
					Bound::Unbounded => write!(f, "|"),
				}
			}
		}
	}
}
