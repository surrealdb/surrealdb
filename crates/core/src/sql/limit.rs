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
pub struct Limit(pub Value);

impl Limit {
	pub(crate) async fn process(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<u32, Error> {
		match self.0.compute(stk, ctx, opt, doc).await {
			// This is a valid limiting number
			Ok(Value::Number(Number::Int(v))) if v >= 0 => {
				if v > u32::MAX as i64 {
					Err(Error::InvalidLimit {
						value: v.to_string(),
					})
				} else {
					Ok(v as u32)
				}
			}
			// An invalid value was specified
			Ok(v) => Err(Error::InvalidLimit {
				value: v.as_string(),
			}),
			// A different error occurred
			Err(e) => Err(e),
		}
	}
}

impl fmt::Display for Limit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIMIT {}", self.0)
	}
}
