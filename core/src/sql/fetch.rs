use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::fmt::Fmt;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Idiom, Value};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
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
		self.into_iter().map(Fetch::structure).collect::<Vec<_>>().into()
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
		self.1 = Value::Idiom(old);
		Ok(())
	}

	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
	) -> Result<Cow<Idiom>, Error> {
		let i = match &self.1 {
			Value::Idiom(idiom) => Cow::Borrowed(idiom),
			Value::Param(param) => {
				let p = param.compute(stk, ctx, opt, None).await?;
				match p {
					Value::Strand(s) => Cow::Owned(Idiom::from(s.0)),
					Value::Idiom(i) => Cow::Owned(i),
					Value::Table(t) => Cow::Owned(Idiom::from(t.0)),
					v => {
						return Err(Error::InvalidFetch {
							value: v,
						});
					}
				}
			}
			v => {
				return Err(Error::InvalidFetch {
					value: v.clone(),
				});
			}
		};
		Ok(i)
	}
}

impl From<Value> for Fetch {
	fn from(value: Value) -> Self {
		Self(Idiom(vec![]), value)
	}
}

impl Deref for Fetch {
	type Target = Value;
	fn deref(&self) -> &Self::Target {
		&self.1
	}
}

impl Display for Fetch {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&self.1, f)
	}
}

impl InfoStructure for Fetch {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}
