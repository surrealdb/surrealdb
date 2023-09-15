use crate::sql::common::take_u64;
use crate::sql::error::IResult;
use crate::sql::escape::escape_ident;
use crate::sql::id::Id;
use crate::sql::ident::ident_raw;
use crate::sql::thing::Thing;
use nom::character::complete::char;
use nom::combinator::map;
use nom::{branch::alt, combinator::value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Mock";

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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Mock")]
#[revisioned(revision = 1)]
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

impl fmt::Display for Mock {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Mock::Count(tb, c) => {
				write!(f, "|{}:{}|", escape_ident(tb), c)
			}
			Mock::Range(tb, b, e) => {
				write!(f, "|{}:{}..{}|", escape_ident(tb), b, e)
			}
		}
	}
}

pub fn mock(i: &str) -> IResult<&str, Mock> {
	let (i, _) = char('|')(i)?;
	let (i, t) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, c) = take_u64(i)?;
	let (i, e) = alt((value(None, char('|')), map(mock_range, Some)))(i)?;
	if let Some(e) = e {
		Ok((i, Mock::Range(t, c, e)))
	} else {
		Ok((i, Mock::Count(t, c)))
	}
}

fn mock_range(i: &str) -> IResult<&str, u64> {
	let (i, _) = char('.')(i)?;
	let (i, _) = char('.')(i)?;
	let (i, e) = take_u64(i)?;
	let (i, _) = char('|')(i)?;
	Ok((i, e))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn mock_count() {
		let sql = "|test:1000|";
		let res = mock(sql);
		let out = res.unwrap().1;
		assert_eq!("|test:1000|", format!("{}", out));
		assert_eq!(out, Mock::Count(String::from("test"), 1000));
	}

	#[test]
	fn mock_range() {
		let sql = "|test:1..1000|";
		let res = mock(sql);
		let out = res.unwrap().1;
		assert_eq!("|test:1..1000|", format!("{}", out));
		assert_eq!(out, Mock::Range(String::from("test"), 1, 1000));
	}
}
