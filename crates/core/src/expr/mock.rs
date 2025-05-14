use crate::expr::{escape::EscapeIdent, Id, Thing};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::expr::Mock";

#[non_exhaustive]
pub struct IntoIter {
	model: Mock,
	index: u64,
}

impl Iterator for IntoIter {
	type Item = Thing;
	fn next(&mut self) -> Option<Thing> {
		match &self.model {
			Mock::Count(tb, c) => {
				if self.index < *c {
					self.index += 1;
					Some(Thing {
						tb: tb.to_string(),
						id: Id::rand(),
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
					Some(Thing {
						tb: tb.to_string(),
						id: Id::from(self.index),
					})
				} else {
					None
				}
			}
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::expr::Mock")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Mock {
	Count(String, u64),
	Range(String, u64, u64),
	// Add new variants here
}

impl IntoIterator for Mock {
	type Item = Thing;
	type IntoIter = IntoIter;
	fn into_iter(self) -> Self::IntoIter {
		IntoIter {
			model: self,
			index: 0,
		}
	}
}

crate::expr::impl_display_from_sql!(Mock);

impl crate::expr::DisplaySql for Mock {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
