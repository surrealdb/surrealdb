use super::Table;
use super::id::range::IdRange;
use crate::sql::{Strand, escape::EscapeRid, id::Id};
use crate::syn;
use anyhow::Result;
use futures::StreamExt;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

const ID: &str = "id";
pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Thing";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Thing")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Thing {
	/// Table name
	pub tb: String,
	pub id: Id,
}

impl From<(&str, Id)> for Thing {
	fn from((tb, id): (&str, Id)) -> Self {
		Self {
			tb: tb.to_owned(),
			id,
		}
	}
}

impl From<(String, Id)> for Thing {
	fn from((tb, id): (String, Id)) -> Self {
		Self {
			tb,
			id,
		}
	}
}

impl From<(&str, IdRange)> for Thing {
	fn from((tb, id): (&str, IdRange)) -> Self {
		Self {
			tb: tb.to_owned(),
			id: id.into(),
		}
	}
}

impl From<(String, IdRange)> for Thing {
	fn from((tb, id): (String, IdRange)) -> Self {
		Self {
			tb,
			id: id.into(),
		}
	}
}

impl From<(String, String)> for Thing {
	fn from((tb, id): (String, String)) -> Self {
		Self::from((tb, Id::from(id)))
	}
}

impl From<(&str, &str)> for Thing {
	fn from((tb, id): (&str, &str)) -> Self {
		Self::from((tb.to_owned(), Id::from(id)))
	}
}

impl FromStr for Thing {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Thing {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Thing {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Thing {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match syn::thing_with_range(v) {
			Ok(v) => Ok(v),
			_ => Err(()),
		}
	}
}

impl From<Thing> for crate::expr::Thing {
	fn from(v: Thing) -> Self {
		crate::expr::Thing {
			tb: v.tb,
			id: v.id.into(),
		}
	}
}

impl From<crate::expr::Thing> for Thing {
	fn from(v: crate::expr::Thing) -> Self {
		Thing {
			tb: v.tb,
			id: v.id.into(),
		}
	}
}

impl Thing {
	/// Convert the Thing to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
	/// Check if this Thing is a range
	pub fn is_range(&self) -> bool {
		matches!(self.id, Id::Range(_))
	}
	/// Check if this Thing is of a certain table type
	pub fn is_record_type(&self, types: &[Table]) -> bool {
		types.is_empty() || types.iter().any(|tb| tb.0 == self.tb)
	}
}

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.tb), self.id)
	}
}

impl Thing {}

#[cfg(test)]
mod test {
	use std::{ops::Bound, str::FromStr};

	use crate::sql::{Array, Id, IdRange, Object, SqlValue};

	use super::Thing;

	#[test]
	fn from() {
		{
			let string = "foo:bar";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::String("bar".into()),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
		{
			let string = "foo:1";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::Number(1),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
		{
			let string = "foo:[1, 'bar']";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::Array(Array(vec![1i64.into(), "bar".into()])),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
		{
			let string = "foo:{bar: 1}";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::Object(Object(
					[("bar".to_string(), SqlValue::from(1))].into_iter().collect(),
				)),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
		{
			let string = "foo:1..=2";
			let thing = Thing {
				tb: "foo".into(),
				id: Id::Range(Box::new(
					IdRange::try_from((
						Bound::Included(Id::Number(1)),
						Bound::Included(Id::Number(2)),
					))
					.unwrap(),
				)),
			};
			assert_eq!(thing, Thing::from_str(string).unwrap());
		}
	}
}
