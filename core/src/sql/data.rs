use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::operator::Operator;
use crate::sql::value::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Option<Value>, Error> {
		match self {
			Self::MergeExpression(v) => {
				// This MERGE expression has an 'id' field
				Ok(v.compute(ctx, opt, txn, None).await?.rid().some())
			}
			Self::ReplaceExpression(v) => {
				// This REPLACE expression has an 'id' field
				Ok(v.compute(ctx, opt, txn, None).await?.rid().some())
			}
			Self::ContentExpression(v) => {
				// This CONTENT expression has an 'id' field
				Ok(v.compute(ctx, opt, txn, None).await?.rid().some())
			}
			Self::SetExpression(v) => match v.iter().find(|f| f.0.is_id()) {
				Some((_, _, v)) => {
					// This SET expression has an 'id' field
					Ok(v.compute(ctx, opt, txn, None).await?.some())
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
