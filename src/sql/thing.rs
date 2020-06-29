use crate::sql::common::escape;
use crate::sql::common::val_char;
use crate::sql::ident::ident_raw;
use nom::bytes::complete::tag;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Thing {
	pub table: String,
	pub id: String,
}

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"{}:{}",
			escape(&self.table, &val_char, "`"),
			escape(&self.id, &val_char, "`"),
		)
	}
}

pub fn thing(i: &str) -> IResult<&str, Thing> {
	let (i, t) = ident_raw(i)?;
	let (i, _) = tag(":")(i)?;
	let (i, v) = ident_raw(i)?;
	Ok((
		i,
		Thing {
			table: String::from(t),
			id: String::from(v),
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
				table: String::from("test"),
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
				table: String::from("test"),
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
				table: String::from("test"),
				id: String::from("id"),
			}
		);
	}
}
