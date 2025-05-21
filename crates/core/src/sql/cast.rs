use crate::sql::{Idiom, Kind, SqlValue};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Cast";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Cast")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Cast(pub Kind, pub SqlValue);

impl PartialOrd for Cast {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Cast {
	/// Convert cast to a field name
	pub fn to_idiom(&self) -> Idiom {
		self.1.to_idiom()
	}

	/// Checks whether all array values are static values
	pub(crate) fn is_static(&self) -> bool {
		self.1.is_static()
	}
}

impl fmt::Display for Cast {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<{}> {}", self.0, self.1)
	}
}

impl From<Cast> for crate::expr::Cast {
	fn from(v: Cast) -> Self {
		Self(v.0.into(), v.1.into())
	}
}
impl From<crate::expr::Cast> for Cast {
	fn from(v: crate::expr::Cast) -> Self {
		Self(v.0.into(), v.1.into())
	}
}
