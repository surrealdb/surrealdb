use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Idiom, Kind, Value};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

use super::{ControlFlow, FlowResult};

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
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.1.writeable()
	}
	/// Checks whether all array values are static values
	pub(crate) fn is_static(&self) -> bool {
		self.1.is_static()
	}
	/// Was marked recursively
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Compute the value to be cast and convert it
		stk.run(|stk| self.1.compute(stk, ctx, opt, doc))
			.await?
			.cast_to_kind(&self.0)
			.map_err(Error::from)
			.map_err(anyhow::Error::new)
			.map_err(ControlFlow::from)
	}
}

impl fmt::Display for Cast {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<{}> {}", self.0, self.1)
	}
}
