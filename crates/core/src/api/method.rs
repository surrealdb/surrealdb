use std::fmt::{self, Display};

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::{
	err::Error,
	sql::{statements::info::InfoStructure, Value},
};

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Method {
	Delete,
	Get,
	Patch,
	Post,
	Put,
	Trace,
}

impl TryFrom<&Value> for Method {
	type Error = Error;
	fn try_from(value: &Value) -> Result<Self, Self::Error> {
		match value {
			Value::Strand(s) => match s.to_ascii_lowercase().as_str() {
				"delete" => Ok(Self::Delete),
				"get" => Ok(Self::Get),
				"patch" => Ok(Self::Patch),
				"post" => Ok(Self::Post),
				"put" => Ok(Self::Put),
				"trace" => Ok(Self::Trace),
				_ => Err(Error::Thrown("method does not match".into())),
			},
			_ => Err(Error::Thrown("method does not match".into())),
		}
	}
}

impl Display for Method {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Delete => write!(f, "DELETE"),
			Self::Get => write!(f, "GET"),
			Self::Patch => write!(f, "PATCH"),
			Self::Post => write!(f, "POST"),
			Self::Put => write!(f, "PUT"),
			Self::Trace => write!(f, "TRACE"),
		}
	}
}

impl InfoStructure for Method {
	fn structure(self) -> Value {
		Value::from(self.to_string())
	}
}
