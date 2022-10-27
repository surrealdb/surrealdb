use crate::sql::error::IResult;
use nom::branch::alt;
use nom::character::complete::char;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub enum Dir {
	In,
	Out,
	Both,
}

impl Default for Dir {
	fn default() -> Self {
		Self::Both
	}
}

impl fmt::Display for Dir {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::In => "<-",
			Self::Out => "->",
			Self::Both => "<->",
		})
	}
}

pub fn dir(i: &str) -> IResult<&str, Dir> {
	alt((
		|i| {
			let (i, _) = char('<')(i)?;
			let (i, _) = char('-')(i)?;
			let (i, _) = char('>')(i)?;
			Ok((i, Dir::Both))
		},
		|i| {
			let (i, _) = char('<')(i)?;
			let (i, _) = char('-')(i)?;
			Ok((i, Dir::In))
		},
		|i| {
			let (i, _) = char('-')(i)?;
			let (i, _) = char('>')(i)?;
			Ok((i, Dir::Out))
		},
	))(i)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn dir_in() {
		let sql = "<-";
		let res = dir(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<-", format!("{}", out));
	}

	#[test]
	fn dir_out() {
		let sql = "->";
		let res = dir(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("->", format!("{}", out));
	}

	#[test]
	fn dir_both() {
		let sql = "<->";
		let res = dir(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("<->", format!("{}", out));
	}
}
