use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::statements::info::InfoStructure;
use crate::sql::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fetchs(pub Vec<Fetch>);

impl Deref for Fetchs {
	type Target = Vec<Fetch>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl IntoIterator for Fetchs {
	type Item = Fetch;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl fmt::Display for Fetchs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FETCH {}", Fmt::comma_separated(&self.0))
	}
}

impl InfoStructure for Fetchs {
	fn structure(self) -> Value {
		Value::Array(self.0.into_iter().map(|f| f.0.structure()).collect())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fetch(pub Idiom);

impl Deref for Fetch {
	type Target = Idiom;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Fetch {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}
