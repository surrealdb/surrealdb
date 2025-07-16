use crate::ctx::Context;
use crate::dbs::Options;
use crate::expr::fmt::Fmt;
use crate::expr::paths::ID;
use crate::expr::{AssignOperator, Expr, Idiom, Literal, Part, Value};
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

use super::FlowResultExt as _;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Data {
	EmptyExpression,
	SetExpression(Vec<Assignment>),
	UnsetExpression(Vec<Idiom>),
	PatchExpression(Expr),
	MergeExpression(Expr),
	ReplaceExpression(Expr),
	ContentExpression(Expr),
	SingleExpression(Expr),
	ValuesExpression(Vec<Vec<(Idiom, Expr)>>),
	UpdateExpression(Vec<Assignment>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Assignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Expr,
}

impl fmt::Display for Assignment {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{} {} {}", self.place, self.operator, self.value)
	}
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
		self.pick(stk, ctx, opt, &*ID).await
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
			Self::MergeExpression(v) | Self::ReplaceExpression(v) | Self::ContentExpression(v) => {
				match v {
					Expr::Param(_) | Expr::Literal(Literal::Object(_)) => {
						let v = stk
							.run(|stk| v.compute(stk, ctx, opt, None))
							.await
							.catch_return()?
							.pick(path);
						if v.is_null() {
							Ok(None)
						} else {
							Ok(Some(v))
						}
					}
					_ => Ok(None),
				}
			}
			Self::SetExpression(v) => match v.iter().find(|f| f.place.is_field(path)) {
				Some(ass) => {
					let v = stk
						.run(|stk| ass.value.compute(stk, ctx, opt, None))
						.await
						.catch_return()?;
					if v.is_null() {
						Ok(None)
					} else {
						Ok(Some(v))
					}
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
			Self::SetExpression(v) => write!(f, "SET {}", Fmt::comma_separated(v.iter())),
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
			Self::UpdateExpression(v) => {
				write!(f, "ON DUPLICATE KEY UPDATE {}", Fmt::comma_separated(v.iter()))
			}
		}
	}
}
