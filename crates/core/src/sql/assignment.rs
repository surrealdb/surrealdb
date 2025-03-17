use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use crate::sql::operator::Operator;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Assignment";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Assignment")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Assignment {
	pub(crate) l: Idiom,
	pub(crate) o: Operator,
	pub(crate) r: Value,
}

impl From<(Idiom, Operator, Value)> for Assignment {
	fn from(tuple: (Idiom, Operator, Value)) -> Self {
		Assignment {
			l: tuple.0,
			o: tuple.1,
			r: tuple.2,
		}
	}
}

impl Assignment {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		Ok(Value::from(Assignment::from((
			self.l.clone(),
			self.o.clone(),
			match self.r.compute(stk, ctx, opt, doc).await {
				Ok(v) => v,
				Err(e) => return Err(e),
			},
		))))
	}
}

impl fmt::Display for Assignment {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} {}", self.l, self.o, self.r)
	}
}
