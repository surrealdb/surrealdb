use core::fmt;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::sql::{statements::info::InfoStructure, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum BucketBackend {
	#[default]
	File,
}

impl fmt::Display for BucketBackend {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "file")
	}
}

impl Into<Value> for BucketBackend {
	fn into(self) -> Value {
		self.to_string().into()
	}
}

impl InfoStructure for BucketBackend {
	fn structure(self) -> Value {
		self.into()
	}
}
