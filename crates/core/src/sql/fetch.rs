use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::sql::Expr;
use crate::sql::fmt::Fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Fetchs(pub Vec<Fetch>);

impl Deref for Fetchs {
	type Target = Vec<Fetch>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl fmt::Display for Fetchs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FETCH {}", Fmt::comma_separated(&self.0))
	}
}

impl From<Fetchs> for crate::expr::Fetchs {
	fn from(v: Fetchs) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}
impl From<crate::expr::Fetchs> for Fetchs {
	fn from(v: crate::expr::Fetchs) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Fetch(pub Expr);

impl Display for Fetch {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl From<Fetch> for crate::expr::Fetch {
	fn from(v: Fetch) -> Self {
		crate::expr::Fetch(v.0.into())
	}
}

impl From<crate::expr::Fetch> for Fetch {
	fn from(v: crate::expr::Fetch) -> Self {
		Fetch(v.0.into())
	}
}
