use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::operator::Operator;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Data {
	EmptyExpression,
	SetExpression(Vec<(Idiom, Operator, Value)>),
	UnsetExpression(Vec<Idiom>),
	PatchExpression(Value),
	MergeExpression(Value),
	ReplaceExpression(Value),
	ContentExpression(Value),
	SingleExpression(Value),
	ValuesExpression(Vec<Vec<(Idiom, Value)>>),
	UpdateExpression(Vec<(Idiom, Operator, Value)>),
}

impl Default for Data {
	fn default() -> Self {
		Self::EmptyExpression
	}
}

impl Data {
	/// Fetch the 'id' field if one has been specified
	pub(crate) async fn rid(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
	) -> Result<Option<Value>, Error> {
		match self {
			Self::MergeExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.rid().some()),
				Value::Object(_) => Ok(v.rid().some()),
				_ => Ok(None),
			},
			Self::ReplaceExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.rid().some()),
				Value::Object(_) => Ok(v.rid().some()),
				_ => Ok(None),
			},
			Self::ContentExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.rid().some()),
				Value::Object(_) => Ok(v.rid().compute(stk, ctx, opt, None).await?.some()),
				_ => Ok(None),
			},
			Self::SetExpression(v) => match v.iter().find(|f| f.0.is_id()) {
				Some((_, _, v)) => {
					// This SET expression has an 'id' field
					Ok(v.compute(stk, ctx, opt, None).await?.some())
				}
				// This SET expression had no 'id' field
				_ => Ok(None),
			},
			// Generate a random id for all other data clauses
			_ => Ok(None),
		}
	}
}

impl Display for Data {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::EmptyExpression => Ok(()),
			Self::SetExpression(v) => write!(
				f,
				"SET {}",
				Fmt::comma_separated(
					v.iter().map(|args| Fmt::new(args, |(l, o, r), f| write!(f, "{l} {o} {r}",)))
				)
			),
			Self::UnsetExpression(v) => write!(
				f,
				"UNSET {}",
				Fmt::comma_separated(v.iter().map(|args| Fmt::new(args, |l, f| write!(f, "{l}",))))
			),
			Self::PatchExpression(v) => write!(f, "PATCH {v}"),
			Self::MergeExpression(v) => write!(f, "MERGE {v}"),
			Self::ReplaceExpression(v) => write!(f, "REPLACE {v}"),
			Self::ContentExpression(v) => write!(f, "CONTENT {v}"),
			Self::SingleExpression(v) => Display::fmt(v, f),
			Self::ValuesExpression(v) => write!(
				f,
				"({}) VALUES {}",
				Fmt::comma_separated(v.first().unwrap().iter().map(|(v, _)| v)),
				Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| write!(
					f,
					"({})",
					Fmt::comma_separated(v.iter().map(|(_, v)| v))
				))))
			),
			Self::UpdateExpression(v) => write!(
				f,
				"ON DUPLICATE KEY UPDATE {}",
				Fmt::comma_separated(
					v.iter().map(|args| Fmt::new(args, |(l, o, r), f| write!(f, "{l} {o} {r}",)))
				)
			),
		}
	}
}
