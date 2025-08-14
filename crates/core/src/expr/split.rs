use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::fmt::Fmt;
use crate::expr::idiom::Idiom;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
