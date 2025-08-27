use std::fmt;

use crate::expr::escape::EscapeIdent;
use crate::val::{RecordId, RecordIdKey};

pub struct IntoIter {
	model: Mock,
}

impl Iterator for IntoIter {
	type Item = RecordId;
	fn next(&mut self) -> Option<RecordId> {
		match self.model {
			Mock::Count(ref tb, ref mut c) => {
				if *c == 0 {
					None
				} else {
					*c -= 1;
					Some(RecordId {
						table: tb.to_string(),
						key: RecordIdKey::rand(),
					})
				}
			}
			Mock::Range(ref tb, ref mut b, e) => {
				if *b >= e {
					return None;
				}
				let idx = *b;
				*b += 1;
				Some(RecordId {
					table: tb.to_string(),
					key: RecordIdKey::from(idx as i64),
				})
			}
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Mock {
	Count(String, u64),
	Range(String, u64, u64),
	// Add new variants here
}

impl IntoIterator for Mock {
	type Item = RecordId;
	type IntoIter = IntoIter;
	fn into_iter(self) -> Self::IntoIter {
		IntoIter {
			model: self,
		}
	}
}

impl fmt::Display for Mock {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Mock::Count(tb, c) => {
				write!(f, "|{}:{}|", EscapeIdent(tb), c)
			}
			Mock::Range(tb, b, e) => {
				write!(f, "|{}:{}..{}|", EscapeIdent(tb), b, e)
			}
		}
	}
}
