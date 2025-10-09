use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::value::VisitExpression;
use crate::sql::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Splits(pub Vec<Split>);

impl Deref for Splits {
	type Target = Vec<Split>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Splits {
	type Item = Split;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl fmt::Display for Splits {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SPLIT ON {}", Fmt::comma_separated(&self.0))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

impl VisitExpression for Splits {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Value),
	{
		self.0.iter().for_each(|s| s.visit(visitor));
	}
}

impl VisitExpression for Split {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Value),
	{
		self.0.visit(visitor);
	}
}
