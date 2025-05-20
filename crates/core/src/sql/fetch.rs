use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::fmt::Fmt;

use crate::sql::{Idiom, SqlValue};
use crate::syn;
use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use super::Array;

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


#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Fetch(
	#[revision(end = 2, convert_fn = "convert_fetch_idiom")] pub Idiom,
	#[revision(start = 2)] pub SqlValue,
);

impl Fetch {
	fn convert_fetch_idiom(&mut self, _revision: u16, old: Idiom) -> Result<(), revision::Error> {
		self.0 = if old.is_empty() {
			SqlValue::None
		} else {
			SqlValue::Idiom(old)
		};
		Ok(())
	}


}

impl From<SqlValue> for Fetch {
	fn from(value: SqlValue) -> Self {
		Self(value)
	}
}

impl Deref for Fetch {
	type Target = SqlValue;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

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
