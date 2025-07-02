use crate::expr::escape::EscapeIdent;
use crate::val::{RecordId, RecordIdKey};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Mock";

#[non_exhaustive]
pub struct IntoIter {
	model: Mock,
	index: u64,
}

impl Iterator for IntoIter {
	type Item = RecordId;
	fn next(&mut self) -> Option<RecordId> {
		match &self.model {
			Mock::Count(tb, c) => {
				if self.index < *c {
					self.index += 1;
					Some(RecordId {
						table: tb.to_string(),
						key: RecordIdKey::rand(),
					})
				} else {
					None
				}
			}
			Mock::Range(tb, b, e) => {
				if self.index == 0 {
					self.index = *b - 1;
				}
				if self.index < *e {
					self.index += 1;
					Some(RecordId {
						table: tb.to_string(),
						key: RecordIdKey::from(self.index),
					})
				} else {
					None
				}
			}
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Mock")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
			index: 0,
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
