use super::FlowResultExt as _;
use crate::cnf::ID_CHARS;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Uuid, Value, escape::EscapeRid};
use crate::val::{Array, Number, Object};

use anyhow::Result;
use nanoid::nanoid;
use range::KeyRange;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use ulid::Ulid;

pub mod range;

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
	Array(Array),
	Object(Object),
	Generate(Gen),
	Range(Box<KeyRange>),
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

impl From<KeyRange> for RecordIdKeyLit {
	fn from(v: KeyRange) -> Self {
		Self::Range(Box::new(v))
	}
}

impl RecordIdKeyLit {
	/// Generate a new random ID
	pub fn rand() -> Self {
		Self::String(nanoid!(20, &ID_CHARS))
	}
	/// Generate a new random ULID
	pub fn ulid() -> Self {
		Self::String(Ulid::new().to_string())
	}
	/// Generate a new random UUID
	pub fn uuid() -> Self {
		Self::Uuid(Uuid::new_v7())
	}
	/// Check if this Id matches a value
	pub fn is(&self, val: &Value) -> bool {
		match (self, val) {
			(Self::Number(i), Value::Number(Number::Int(j))) if *i == *j => true,
			(Self::String(i), Value::Strand(j)) if *i == j.0 => true,
			(Self::Uuid(i), Value::Uuid(j)) if i == j => true,
			(Self::Array(i), Value::Array(j)) if i == j => true,
			(Self::Object(i), Value::Object(j)) if i == j => true,
			(i, Value::Thing(t)) if i == &t.id => true,
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
	) -> Result<RecordIdKeyLit> {
		match self {
			RecordIdKeyLit::Number(v) => Ok(RecordIdKeyLit::Number(*v)),
			RecordIdKeyLit::String(v) => Ok(RecordIdKeyLit::String(v.clone())),
			RecordIdKeyLit::Uuid(v) => Ok(RecordIdKeyLit::Uuid(*v)),
			RecordIdKeyLit::Array(v) => match v.compute(stk, ctx, opt, doc).await.catch_return()? {
				Value::Array(v) => Ok(RecordIdKeyLit::Array(v)),
				v => fail!("Expected a Value::Array but found {v:?}"),
			},
			RecordIdKeyLit::Object(v) => {
				match v.compute(stk, ctx, opt, doc).await.catch_return()? {
					Value::Object(v) => Ok(RecordIdKeyLit::Object(v)),
					v => fail!("Expected a Value::Object but found {v:?}"),
				}
			}
			RecordIdKeyLit::Generate(v) => match v {
				Gen::Rand => Ok(Self::rand()),
				Gen::Ulid => Ok(Self::ulid()),
				Gen::Uuid => Ok(Self::uuid()),
			},
			RecordIdKeyLit::Range(v) => {
				Ok(RecordIdKeyLit::Range(Box::new(v.compute(stk, ctx, opt, doc).await?)))
			}
		}
	}
}
