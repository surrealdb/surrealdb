use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::fmt::Fmt;
use crate::expr::idiom::Idiom;
use crate::expr::operator::Operator;
use crate::expr::part::Part;
use crate::expr::paths::ID;
use crate::expr::value::Value;
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

use super::FlowResultExt as _;

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
	) -> Result<Option<Value>> {
		// Handle subquery expressions (inline SELECT)
		if let Self::ContentExpression(Value::Subquery(sub_query)) = self {
			let result = Box::pin(sub_query.compute(stk, ctx, opt, None)).await.catch_return()?;
			return Ok(result.pick(&*ID).some());
		}

		// For all other cases, use a synchronous approach to avoid recursion
		match self {
			Self::ContentExpression(Value::Param(param)) => {
				// For param values, compute them and extract id
				Ok(param.compute(stk, ctx, opt, None).await?.pick(&*ID).some())
			}
			Self::ContentExpression(Value::Object(obj)) => {
				// For objects, extract id field directly
				Ok(obj.get("id").cloned())
			}
			Self::ContentExpression(Value::Thing(thing)) => {
				// For things, return the thing itself as the id
				Ok(Some(Value::Thing(thing.clone())))
			}
			Self::ContentExpression(Value::Idiom(idiom)) => {
				// For idiom expressions (like .content), compute the underlying value and extract id
				Ok(idiom.compute(stk, ctx, opt, None).await.catch_return()?.pick(&*ID).some())
			}
			Self::ContentExpression(Value::Function(func)) => {
				// For function calls, compute them and extract id
				Ok(func.compute(stk, ctx, opt, None).await.catch_return()?.pick(&*ID).some())
			}
			Self::ContentExpression(Value::Constant(constant)) => {
				// For constant expressions, compute them and extract id
				Ok(constant.compute()?.pick(&*ID).some())
			}
			Self::ContentExpression(Value::Expression(expr)) => {
				// For expression types, compute them and extract id
				Ok(expr.compute(stk, ctx, opt, None).await.catch_return()?.pick(&*ID).some())
			}
			Self::SetExpression(vec) => {
				// For set expressions, find the id field if it exists
				if let Some((_, _, val)) = vec.iter().find(|f| f.0.is_field(&*ID)) {
					// For now, just return the value as-is to avoid recursion
					Ok(Some(val.clone()))
				} else {
					Ok(None)
				}
			}
			// For all other cases, return None to avoid recursion
			_ => Ok(None),
		}
	}
	/// Fetch a field path value if one is specified
	pub(crate) async fn pick(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &[Part],
	) -> Result<Option<Value>> {
		match self {
			Self::MergeExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.pick(path).some()),
				Value::Object(_) => {
					Ok(v.pick(path).compute(stk, ctx, opt, None).await.catch_return()?.some())
				}
				_ => Ok(None),
			},
			Self::ReplaceExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.pick(path).some()),
				Value::Object(_) => {
					Ok(v.pick(path).compute(stk, ctx, opt, None).await.catch_return()?.some())
				}
				_ => Ok(None),
			},
			Self::ContentExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.pick(path).some()),
				Value::Object(_) => {
					Ok(v.pick(path).compute(stk, ctx, opt, None).await.catch_return()?.some())
				}
				_ => Ok(None),
			},
			Self::SetExpression(v) => match v.iter().find(|f| f.0.is_field(path)) {
				Some((_, _, v)) => {
					// This SET expression has this field
					Ok(v.compute(stk, ctx, opt, None).await.catch_return()?.some())
				}
				// This SET expression does not have this field
				_ => Ok(None),
			},
			// Return nothing
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
