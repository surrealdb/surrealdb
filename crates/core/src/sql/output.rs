use std::fmt::{self, Display};

use crate::sql::field::Fields;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<Output> for crate::expr::Output {
	fn from(v: Output) -> Self {
		match v {
			Output::None => Self::None,
			Output::Null => Self::Null,
			Output::Diff => Self::Diff,
			Output::After => Self::After,
			Output::Before => Self::Before,
			Output::Fields(v) => Self::Fields(v.into()),
		}
	}
}

impl From<crate::expr::Output> for Output {
	fn from(v: crate::expr::Output) -> Self {
		match v {
			crate::expr::Output::None => Self::None,
			crate::expr::Output::Null => Self::Null,
			crate::expr::Output::Diff => Self::Diff,
			crate::expr::Output::After => Self::After,
			crate::expr::Output::Before => Self::Before,
			crate::expr::Output::Fields(v) => Self::Fields(v.into()),
		}
	}
}
