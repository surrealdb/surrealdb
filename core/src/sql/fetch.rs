use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::statements::info::InfoStructure;
use crate::sql::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use super::{Field, Part};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fetchs(pub Vec<Fetch>);

impl Fetchs {
	/// For `FETCH table.field1, table.field2` returns a vector of `filed1` and
	/// `filed2`
	pub fn fields(&self) -> Option<Vec<Field>> {
		let mut fields = Vec::new();

		for fetch in &self.0 {
			if fetch.0.len() != 2 {
				return None;
			}
			if fetch.0.iter().all(|f| matches!(f, Part::Field(_))) {
				if let Some(last_field) = fetch.0.last() {
					fields.push(Field::Single {
						expr: Value::Idiom(Idiom(vec![last_field.clone()])),
						alias: None,
					});
				}
			} else {
				return None;
			}
		}

		Some(fields)
	}
}

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
