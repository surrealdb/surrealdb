use std::fmt::{Display, Formatter, Result};

use crate::fmt::{EscapeKwFreeIdent, Fmt};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
				Fmt::comma_separated(i.iter().map(|x| EscapeKwFreeIdent(x))).fmt(f)
			}
		}
	}
}

impl From<With> for crate::expr::With {
	fn from(v: With) -> Self {
		match v {
			With::NoIndex => Self::NoIndex,
			With::Index(i) => Self::Index(i),
		}
	}
}
impl From<crate::expr::With> for With {
	fn from(v: crate::expr::With) -> Self {
		match v {
			crate::expr::With::NoIndex => Self::NoIndex,
			crate::expr::With::Index(i) => Self::Index(i),
		}
	}
}
