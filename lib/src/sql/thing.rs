use crate::sql::error::IResult;
use crate::sql::escape::escape_id;
use crate::sql::id::{id, Id};
use crate::sql::ident::ident_raw;
use crate::sql::serde::is_internal_serialization;
use derive::Store;
use nom::branch::alt;
use nom::character::complete::char;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

const SINGLE: char = '\'';
const DOUBLE: char = '"';

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Deserialize, Store)]
pub struct Thing {
	pub tb: String,
	pub id: Id,
}

impl From<(String, Id)> for Thing {
	fn from(v: (String, Id)) -> Self {
		Thing {
			tb: v.0,
			id: v.1,
		}
	}
}

impl From<(String, String)> for Thing {
	fn from(v: (String, String)) -> Self {
		Thing {
			tb: v.0,
			id: Id::from(v.1),
		}
	}
}

impl From<(&str, &str)> for Thing {
	fn from(v: (&str, &str)) -> Self {
		Thing {
			tb: v.0.to_owned(),
			id: Id::from(v.1),
		}
	}
}

impl Thing {
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", escape_id(&self.tb), self.id)
	}
}

impl Serialize for Thing {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			let mut val = serializer.serialize_struct("Thing", 2)?;
			val.serialize_field("tb", &self.tb)?;
			val.serialize_field("id", &self.id)?;
			val.end()
		} else {
			let output = self.to_string();
			serializer.serialize_some(&output)
		}
	}
}

pub fn thing(i: &str) -> IResult<&str, Thing> {
	let (i, v) = thing_raw(i)?;
	Ok((i, v))
}

fn thing_raw(i: &str) -> IResult<&str, Thing> {
	alt((thing_normal, thing_single, thing_double))(i)
}

fn thing_normal(i: &str) -> IResult<&str, Thing> {
	let (i, t) = ident_raw(i)?;
	let (i, _) = char(':')(i)?;
	let (i, v) = id(i)?;
	Ok((
		i,
		Thing {
			tb: t,
			id: v,
		},
	))
}

fn thing_single(i: &str) -> IResult<&str, Thing> {
	let (i, _) = char(SINGLE)(i)?;
	let (i, v) = thing_normal(i)?;
	let (i, _) = char(SINGLE)(i)?;
	Ok((i, v))
}

fn thing_double(i: &str) -> IResult<&str, Thing> {
	let (i, _) = char(DOUBLE)(i)?;
	let (i, v) = thing_normal(i)?;
	let (i, _) = char(DOUBLE)(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn thing_normal() {
		let sql = "test:id";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_quoted_backtick() {
		let sql = "`test`:`id`";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}

	#[test]
	fn thing_quoted_brackets() {
		let sql = "⟨test⟩:⟨id⟩";
		let res = thing(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("test:id", format!("{}", out));
		assert_eq!(
			out,
			Thing {
				tb: String::from("test"),
				id: Id::from("id"),
			}
		);
	}
}
