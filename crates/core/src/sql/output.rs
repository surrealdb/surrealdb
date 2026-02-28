use crate::sql::{field::Fields, fmt::Fmt, Field};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Output {
	#[default]
	None,
	Null,
	Diff,
	After,
	Before,
	Fields(Fields),
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
			Self::Fields(v) => match v.single() {
				Some(v) => write!(f, "VALUE {}", &v),
				None => {
					if let Some(Field::Single {
						expr,
						alias,
					}) = v.0.first()
					{
						// Avoid conflict between the value NONE with the `Output::None`.
						if expr.has_left_none_or_null() {
							write!(f, "({})", expr)?;
							if let Some(a) = alias {
								write!(f, " AS {}", a)?;
							}

							for i in &v.0[1..] {
								write!(f, ", {}", i)?;
							}
							return Ok(());
						}
					}
					Display::fmt(&Fmt::comma_separated(&v.0), f)
				}
			},
		}
	}
}
