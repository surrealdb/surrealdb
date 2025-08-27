use std::fmt::{Display, Formatter, Result};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum With {
	NoIndex,
	Index(Vec<String>),
}

impl Display for With {
	fn fmt(&self, f: &mut Formatter) -> Result {
		f.write_str("WITH")?;
		match self {
			With::NoIndex => f.write_str(" NOINDEX"),
			With::Index(i) => {
				f.write_str(" INDEX ")?;
				f.write_str(&i.join(","))
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
