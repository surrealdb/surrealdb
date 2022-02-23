use crate::sql::error::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Script {
	pub value: String,
}

impl fmt::Display for Script {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "\"{}\"", self.value)
	}
}

pub fn script(_: &str) -> IResult<&str, Script> {
	unimplemented!()
}
