use std::fmt::{self, Display};

use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::{err::Error, sql::Value};

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
			Value::Strand(s) => match s.as_str() {
				v if v.eq_ignore_ascii_case("delete") => Ok(Self::Delete),
				v if v.eq_ignore_ascii_case("get") => Ok(Self::Get),
				v if v.eq_ignore_ascii_case("patch") => Ok(Self::Patch),
				v if v.eq_ignore_ascii_case("post") => Ok(Self::Post),
				v if v.eq_ignore_ascii_case("put") => Ok(Self::Put),
				v if v.eq_ignore_ascii_case("trace") => Ok(Self::Trace),
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
