use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::operator::Operator;
use crate::sql::part::Part;
use crate::sql::paths::ID;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
	UpdateExpression(Vec<Vec<(Idiom, Operator, Value)>>),
	UpdatesExpression(HashMap<Thing, Vec<(Idiom, Operator, Value)>>),
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
		self.pick(stk, ctx, opt, &*ID).await
	}
	/// Fetch a field path value if one is specified
	pub(crate) async fn pick(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &[Part],
	) -> Result<Option<Value>, Error> {
		match self {
			Self::MergeExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.pick(path).some()),
				Value::Object(_) => Ok(v.pick(path).compute(stk, ctx, opt, None).await?.some()),
				_ => Ok(None),
			},
			Self::ReplaceExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.pick(path).some()),
				Value::Object(_) => Ok(v.pick(path).compute(stk, ctx, opt, None).await?.some()),
				_ => Ok(None),
			},
			Self::ContentExpression(v) => match v {
				Value::Param(v) => Ok(v.compute(stk, ctx, opt, None).await?.pick(path).some()),
				Value::Object(_) => Ok(v.pick(path).compute(stk, ctx, opt, None).await?.some()),
				_ => Ok(None),
			},
			Self::SetExpression(v) => match v.iter().find(|f| f.0.is_field(path)) {
				Some((_, _, v)) => {
					// This SET expression has this field
					Ok(v.compute(stk, ctx, opt, None).await?.some())
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
			Self::UpdateExpression(v) => {
				if v.len() == 1 {
					write!(
						f,
						"ON DUPLICATE KEY UPDATE {}",
						Fmt::comma_separated(
							v[0].iter().map(|args| Fmt::new(args, |(l, o, r), f| write!(
								f,
								"{l} {o} {r}"
							)))
						)
					)
				} else {
					write!(
						f,
						"ON DUPLICATE KEY UPDATE [{}]",
						Fmt::comma_separated(v.iter().map(|v| format!(
							"{{{}}}",
							Fmt::comma_separated(v.iter().map(|args| Fmt::new(
								args,
								|(l, o, r), f| write!(f, "{l} {o} {r}",)
							)))
						)))
					)
				}
			}
			Self::UpdatesExpression(v) => {
				if v.len() == 1 {
					let (_, update) = v.iter().next().unwrap();
					write!(
						f,
						"ON DUPLICATE KEY UPDATE {}",
						Fmt::comma_separated(
							update.iter().map(|args| Fmt::new(args, |(l, o, r), f| write!(
								f,
								"{l} {o} {r}"
							)))
						)
					)
				} else {
					write!(
						f,
						"ON DUPLICATE KEY UPDATE [{}]",
						Fmt::comma_separated(v.iter().map(|(_, update)| format!(
							"{{{}}}",
							Fmt::comma_separated(update.iter().map(|args| Fmt::new(
								args,
								|(l, o, r), f| write!(f, "{l} {o} {r}",)
							)))
						)))
					)
				}
			}
		}
	}
}

//I dont know if there is a better way to do this, not that experienced with rust,
//I also dont know if implementation for PartialOrd and Hash is required
//But since it was like that before, I wont remove them
impl PartialOrd for Data {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match (self, other) {
			(Self::EmptyExpression, Self::EmptyExpression) => Some(Ordering::Equal),
			(Self::SetExpression(a), Self::SetExpression(b)) => a.partial_cmp(b),
			(Self::UnsetExpression(a), Self::UnsetExpression(b)) => a.partial_cmp(b),
			(Self::PatchExpression(a), Self::PatchExpression(b)) => a.partial_cmp(b),
			(Self::MergeExpression(a), Self::MergeExpression(b)) => a.partial_cmp(b),
			(Self::ReplaceExpression(a), Self::ReplaceExpression(b)) => a.partial_cmp(b),
			(Self::ContentExpression(a), Self::ContentExpression(b)) => a.partial_cmp(b),
			(Self::SingleExpression(a), Self::SingleExpression(b)) => a.partial_cmp(b),
			(Self::ValuesExpression(a), Self::ValuesExpression(b)) => a.partial_cmp(b),
			(Self::UpdateExpression(a), Self::UpdateExpression(b)) => a.partial_cmp(b),
			(Self::UpdatesExpression(a), Self::UpdatesExpression(b)) => {
				if a.len() != b.len() {
					return a.len().partial_cmp(&b.len());
				}
				for (key_a, value_a) in a {
					if let Some(value_b) = b.get(key_a) {
						match value_a.partial_cmp(value_b) {
							Some(Ordering::Equal) => continue,
							non_eq => return non_eq,
						}
					} else {
						return Some(Ordering::Greater);
					}
				}
				Some(Ordering::Equal)
			}
			_ => None,
		}
	}
}

impl Hash for Data {
	fn hash<H: Hasher>(&self, state: &mut H) {
		match self {
			Self::EmptyExpression => {
				state.write_u8(0);
			}
			Self::SetExpression(v) => {
				state.write_u8(1);
				v.hash(state);
			}
			Self::UnsetExpression(v) => {
				state.write_u8(2);
				v.hash(state);
			}
			Self::PatchExpression(v) => {
				state.write_u8(3);
				v.hash(state);
			}
			Self::MergeExpression(v) => {
				state.write_u8(4);
				v.hash(state);
			}
			Self::ReplaceExpression(v) => {
				state.write_u8(5);
				v.hash(state);
			}
			Self::ContentExpression(v) => {
				state.write_u8(6);
				v.hash(state);
			}
			Self::SingleExpression(v) => {
				state.write_u8(7);
				v.hash(state);
			}
			Self::ValuesExpression(v) => {
				state.write_u8(8);
				v.hash(state);
			}
			Self::UpdateExpression(v) => {
				state.write_u8(9);
				v.hash(state);
			}
			Self::UpdatesExpression(v) => {
				state.write_u8(10);
				for (key, value) in v {
					key.hash(state);
					value.hash(state);
				}
			}
		}
	}
}
