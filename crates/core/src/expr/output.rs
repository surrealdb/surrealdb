use std::fmt::{self, Display};

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::field::Fields;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Output {
	None,
	Null,
	Diff,
	After,
	Before,
	Fields(Fields),
}

impl Default for Output {
	fn default() -> Self {
		Self::None
	}
}

impl Display for Output {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("RETURN ")?;
		match self {
			Self::None => f.write_str("NONE"),
			Self::Null => f.write_str("NULL"),
			Self::Diff => f.write_str("DIFF"),
			Self::After => f.write_str("AFTER"),
			Self::Before => f.write_str("BEFORE"),
			Self::Fields(v) => Display::fmt(v, f),
		}
	}
}
