use crate::sql::error::IResult;
use crate::sql::escape::escape_ident;
use crate::sql::id::{id, Id};
use crate::sql::ident::ident_raw;
use crate::sql::serde::is_internal_serialization;
use derive::Store;
use nom::character::complete::char;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

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

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", escape_ident(&self.tb), self.id)
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
			let output = format!("{}:{}", self.tb, self.id);
			serializer.serialize_some(&output)
		}
	}
}

pub fn thing(i: &str) -> IResult<&str, Thing> {
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
