use crate::sql::common::escape;
use crate::sql::common::val_char;
use crate::sql::error::IResult;
use crate::sql::ident::ident_raw;
use nom::bytes::complete::tag;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize)]
pub struct Thing {
	pub tb: String,
	pub id: String,
}

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let t = escape(&self.tb, &val_char, "`");
		let i = escape(&self.id, &val_char, "`");
		write!(f, "{}:{}", t, i)
	}
}

impl Serialize for Thing {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			let output = format!("{}:{}", self.tb, self.id);
			serializer.serialize_some(&output)
		} else {
			let mut val = serializer.serialize_struct("Thing", 2)?;
			val.serialize_field("tb", &self.tb)?;
			val.serialize_field("id", &self.id)?;
			val.end()
		}
	}
}

pub fn thing(i: &str) -> IResult<&str, Thing> {
	let (i, t) = ident_raw(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, v) = ident_raw(i)?;
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
				id: String::from("id"),
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
				id: String::from("id"),
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
				id: String::from("id"),
			}
		);
	}
}
