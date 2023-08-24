use crate::sql::error::IResult;
use nom::{branch::alt, bytes::complete::tag, combinator::map};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
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
	alt((map(tag("<->"), |_| Dir::Both), map(tag("<-"), |_| Dir::In), map(tag("->"), |_| Dir::Out)))(
		i,
	)
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn dir_in() {
		let sql = "<-";
		let res = dir(sql);
		let out = res.unwrap().1;
		assert_eq!("<-", format!("{}", out));
	}

	#[test]
	fn dir_out() {
		let sql = "->";
		let res = dir(sql);
		let out = res.unwrap().1;
		assert_eq!("->", format!("{}", out));
	}

	#[test]
	fn dir_both() {
		let sql = "<->";
		let res = dir(sql);
		let out = res.unwrap().1;
		assert_eq!("<->", format!("{}", out));
	}
}
