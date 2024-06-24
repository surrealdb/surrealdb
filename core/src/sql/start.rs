use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::number::Number;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Start(pub Value);

impl Start {
	pub(crate) async fn process(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<usize, Error> {
		match self.0.compute(stk, ctx, opt, doc).await {
			// This is a valid starting number
			Ok(Value::Number(Number::Int(v))) if v >= 0 => Ok(v as usize),
			// An invalid value was specified
			Ok(v) => Err(Error::InvalidStart {
				value: v.as_string(),
			}),
			// A different error occurred
			Err(e) => Err(e),
		}
	}
}

impl fmt::Display for Start {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "START {}", self.0)
	}
}
