use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};

use crate::sql::escape::EscapeKwFreeIdent;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum With {
	NoIndex,
	Index(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
		Vec<String>,
	),
}

impl Display for With {
	fn fmt(&self, f: &mut Formatter) -> Result {
		f.write_str("WITH")?;
		match self {
			With::NoIndex => f.write_str(" NOINDEX"),
			With::Index(i) => {
				f.write_str(" INDEX ")?;
				for (idx, i) in i.iter().enumerate() {
					if idx != 0 {
						f.write_str(", ")?;
					}
					write!(f, "{}", EscapeKwFreeIdent(i))?;
				}
				Ok(())
			}
		}
	}
}
