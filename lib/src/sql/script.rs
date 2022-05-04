use crate::sql::error::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::str;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Script(pub String);

impl Deref for Script {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Script {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "\"{}\"", self.0)
	}
}

pub fn script(_: &str) -> IResult<&str, Script> {
	unimplemented!()
}
