use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Splits(pub Vec<Split>);

impl fmt::Display for Splits {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SPLIT ON {}", Fmt::comma_separated(&self.0))
	}
}

impl From<Splits> for crate::expr::Splits {
	fn from(v: Splits) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Splits> for Splits {
	fn from(v: crate::expr::Splits) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Split(pub Idiom);

impl Deref for Split {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Split {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl From<Split> for crate::expr::Split {
	fn from(v: Split) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Split> for Split {
	fn from(v: crate::expr::Split) -> Self {
		Self(v.0.into())
	}
}
