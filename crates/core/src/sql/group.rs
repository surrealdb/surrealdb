use std::fmt::{self, Display, Formatter};

use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Groups(pub Vec<Group>);

impl Display for Groups {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.0.is_empty() {
			write!(f, "GROUP ALL")
		} else {
			write!(f, "GROUP BY {}", Fmt::comma_separated(&self.0))
		}
	}
}

impl From<Groups> for crate::expr::Groups {
	fn from(v: Groups) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Groups> for Groups {
	fn from(v: crate::expr::Groups) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Group(pub Idiom);

impl Display for Group {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl From<Group> for crate::expr::Group {
	fn from(v: Group) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Group> for Group {
	fn from(v: crate::expr::Group) -> Self {
		Self(v.0.into())
	}
}
