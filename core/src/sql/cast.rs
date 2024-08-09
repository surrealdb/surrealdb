use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{Idiom, Kind, Value};
use reblessive::tree::Stk;
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
pub struct Cast(pub Kind, pub Value);

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
}

impl Cast {
	/// Was marked recursively
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Compute the value to be cast and convert it
		stk.run(|stk| self.1.compute(stk, ctx, opt, doc)).await?.convert_to(&self.0)
	}
}

impl fmt::Display for Cast {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<{}> {}", self.0, self.1)
	}
}
