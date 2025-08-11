use std::fmt::{self, Display};

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
			Self::Delete => write!(f, "delete"),
			Self::Get => write!(f, "get"),
			Self::Patch => write!(f, "patch"),
			Self::Post => write!(f, "post"),
			Self::Put => write!(f, "put"),
			Self::Trace => write!(f, "trace"),
		}
	}
}

impl InfoStructure for Method {
	fn structure(self) -> Value {
		Value::from(self.to_string())
	}
}
