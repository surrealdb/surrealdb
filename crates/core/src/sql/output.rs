use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::field::Fields;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Output {
	#[default]
	None,
	Null,
	Diff,
	After,
	Before,
	Fields(Fields),
}

impl ToSql for Output {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::None => write_sql!(f, fmt, "RETURN NONE"),
			Self::Null => write_sql!(f, fmt, "RETURN NULL"),
			Self::Diff => write_sql!(f, fmt, "RETURN DIFF"),
			Self::After => write_sql!(f, fmt, "RETURN AFTER"),
			Self::Before => write_sql!(f, fmt, "RETURN BEFORE"),
			Self::Fields(v) => write_sql!(f, fmt, "RETURN {}", v),
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
