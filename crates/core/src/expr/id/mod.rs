use super::FlowResultExt as _;
use crate::cnf::ID_CHARS;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::escape::EscapeRid;
use crate::expr::literal::ObjectEntry;
use crate::expr::{Expr, Value};
use crate::val::{Array, Number, Object, RecordId, RecordIdKey, RecordIdKeyRange, Uuid};

use anyhow::Result;
use nanoid::nanoid;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::{Bound, Deref};
use ulid::Ulid;

pub mod range;
pub use range::RecordIdKeyRangeLit;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Gen {
	Rand,
	Ulid,
	Uuid,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum RecordIdKeyLit {
	Number(i64),
	String(String),
	Uuid(Uuid),
	Array(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Generate(Gen),
	Range(Box<RecordIdKeyRangeLit>),
}

impl RecordIdKeyLit {
	/// Create a record id key from a value.
	///
	/// Returns the original value if the key can't be created from the value.
	pub fn from_value(v: Value) -> Result<Self, Value> {
		match v {
			Value::Number(Number::Int(v)) => Ok(v.into()),
			Value::Strand(v) => Ok(v.into()),
			Value::Array(v) => Ok(v.into()),
			Value::Object(v) => Ok(v.into()),
			Value::Range(v) => v.deref().to_owned().try_into(),
			x => Err(x),
		}
	}
}

impl From<RecordIdKeyRangeLit> for RecordIdKeyLit {
	fn from(v: RecordIdKeyRangeLit) -> Self {
		Self::Range(Box::new(v))
	}
}

impl RecordIdKeyLit {
	/// Check if this Id matches a value
	pub fn is(&self, val: &Value) -> bool {
		match (self, val) {
			(Self::Number(i), Value::Number(Number::Int(j))) if *i == *j => true,
			(Self::String(i), Value::Strand(j)) if *i == j.0 => true,
			(Self::Uuid(i), Value::Uuid(j)) if i == j => true,
			(Self::Array(i), Value::Array(j)) if i == j => true,
			(Self::Object(i), Value::Object(j)) if i == j => true,
			(i, Value::Thing(t)) if i == &t.key => true,
			_ => false,
		}
	}
}

impl Display for RecordIdKeyLit {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Number(v) => Display::fmt(v, f),
			Self::String(v) => EscapeRid(v).fmt(f),
			Self::Uuid(v) => Display::fmt(v, f),
			Self::Array(v) => Display::fmt(v, f),
			Self::Object(v) => Display::fmt(v, f),
			Self::Generate(v) => match v {
				Gen::Rand => Display::fmt("rand()", f),
				Gen::Ulid => Display::fmt("ulid()", f),
				Gen::Uuid => Display::fmt("uuid()", f),
			},
			Self::Range(v) => Display::fmt(v, f),
		}
	}
}

impl RecordIdKeyLit {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<RecordIdKey> {
		match self {
			RecordIdKeyLit::Number(v) => Ok(RecordIdKey::Number(*v)),
			RecordIdKeyLit::String(v) => Ok(RecordIdKey::String(v.clone())),
			RecordIdKeyLit::Uuid(v) => Ok(RecordIdKey::Uuid(*v)),
			RecordIdKeyLit::Array(v) => {
				let mut res = Vec::new();
				for v in v.iter() {
					let v = v.compute(stk, ctx, opt, doc).await.catch_return()?;
					res.push(v);
				}
				Ok(RecordIdKey::Array(Array(res)))
			}
			RecordIdKeyLit::Object(v) => {
				let mut res = Object::default();
				for entry in v.iter() {
					let v = entry.value.compute(stk, ctx, opt, doc).await.catch_return()?;
					res.insert(entry.key.clone(), v);
				}
				Ok(RecordIdKey::Object(res))
			}
			RecordIdKeyLit::Generate(v) => match v {
				Gen::Rand => Ok(RecordIdKey::rand()),
				Gen::Ulid => Ok(RecordIdKey::ulid()),
				Gen::Uuid => Ok(RecordIdKey::uuid()),
			},
			RecordIdKeyLit::Range(v) => {
				let start = match v.start {
					Bound::Included(x) => Bound::Included(x.compute(stk, ctx, opt, doc).await?),
					Bound::Excluded(x) => Bound::Excluded(x.compute(stk, ctx, opt, doc).await?),
					Bound::Unbounded => Bound::Unbounded,
				};
				let end = match v.end {
					Bound::Included(x) => Bound::Included(x.compute(stk, ctx, opt, doc).await?),
					Bound::Excluded(x) => Bound::Excluded(x.compute(stk, ctx, opt, doc).await?),
					Bound::Unbounded => Bound::Unbounded,
				};

				Ok(RecordIdKey::Range(Box::new(RecordIdKeyRange {
					start,
					end,
				})))
			}
		}
	}
}
