use crate::sql::fmt::Fmt;

use crate::sql::{Idiom, Value};
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

crate::sql::impl_display_from_sql!(Fetchs);

impl crate::sql::DisplaySql for Fetchs {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FETCH {}", Fmt::comma_separated(&self.0))
	}
}



#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fetch(
	#[revision(end = 2, convert_fn = "convert_fetch_idiom")] pub Idiom,
	#[revision(start = 2)] pub Value,
);

impl Fetch {
	fn convert_fetch_idiom(&mut self, _revision: u16, old: Idiom) -> Result<(), revision::Error> {
		self.0 = if old.is_empty() {
			Value::None
		} else {
			Value::Idiom(old)
		};
		Ok(())
	}
}

impl From<Value> for Fetch {
	fn from(value: Value) -> Self {
		Self(value)
	}
}

impl Deref for Fetch {
	type Target = Value;
	fn deref(&self) -> &Self::Target {
		&self.0
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

crate::sql::impl_display_from_sql!(Fetch);

impl crate::sql::DisplaySql for Fetch {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}


