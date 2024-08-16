use crate::{
	cnf::ID_CHARS,
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::{escape::escape_rid, Array, Number, Object, Strand, Uuid, Value},
};
use nanoid::nanoid;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{
	collections::BTreeMap,
	fmt::{self, Display, Formatter},
};
use ulid::Ulid;

use super::Id;

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
pub enum IdValue {
	Number(i64),
	String(String),
	Array(Array),
	Object(Object),
	Generate(Gen),
}

impl From<i64> for IdValue {
	fn from(v: i64) -> Self {
		Self::Number(v)
	}
}

impl From<i32> for IdValue {
	fn from(v: i32) -> Self {
		Self::Number(v as i64)
	}
}

impl From<u64> for IdValue {
	fn from(v: u64) -> Self {
		Self::Number(v as i64)
	}
}

impl From<String> for IdValue {
	fn from(v: String) -> Self {
		Self::String(v)
	}
}

impl From<Array> for IdValue {
	fn from(v: Array) -> Self {
		Self::Array(v)
	}
}

impl From<Object> for IdValue {
	fn from(v: Object) -> Self {
		Self::Object(v)
	}
}

impl From<Uuid> for IdValue {
	fn from(v: Uuid) -> Self {
		Self::String(v.to_raw())
	}
}

impl From<Strand> for IdValue {
	fn from(v: Strand) -> Self {
		Self::String(v.as_string())
	}
}

impl From<&str> for IdValue {
	fn from(v: &str) -> Self {
		Self::String(v.to_owned())
	}
}

impl From<&String> for IdValue {
	fn from(v: &String) -> Self {
		Self::String(v.to_owned())
	}
}

impl From<Vec<&str>> for IdValue {
	fn from(v: Vec<&str>) -> Self {
		Self::Array(v.into())
	}
}

impl From<Vec<String>> for IdValue {
	fn from(v: Vec<String>) -> Self {
		Self::Array(v.into())
	}
}

impl From<Vec<Value>> for IdValue {
	fn from(v: Vec<Value>) -> Self {
		Self::Array(v.into())
	}
}

impl From<BTreeMap<String, Value>> for IdValue {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Self::Object(v.into())
	}
}

impl From<Number> for IdValue {
	fn from(v: Number) -> Self {
		match v {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.to_string().into(),
			Number::Decimal(v) => v.to_string().into(),
		}
	}
}

impl TryFrom<Value> for IdValue {
	type Error = Error;
	fn try_from(v: Value) -> Result<Self, Self::Error> {
		match v {
			Value::Number(Number::Int(v)) => Ok(v.into()),
			Value::Strand(v) => Ok(v.into()),
			Value::Array(v) => Ok(v.into()),
			Value::Object(v) => Ok(v.into()),
			v => Err(Error::IdInvalid {
				value: v.kindof().to_string(),
			}),
		}
	}
}

impl TryFrom<Id> for IdValue {
	type Error = Error;
	fn try_from(v: Id) -> Result<Self, Self::Error> {
		match v {
			Id::Number(v) => Ok(Self::Number(v)),
			Id::String(v) => Ok(Self::String(v)),
			Id::Array(v) => Ok(Self::Array(v)),
			Id::Object(v) => Ok(Self::Object(v)),
			Id::Generate(v) => Ok(Self::Generate(v)),
			Id::Range(_) => Err(Error::IdInvalid {
				value: "idrange".to_string(),
			}),
		}
	}
}

impl Display for IdValue {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Number(v) => Display::fmt(v, f),
			Self::String(v) => Display::fmt(&escape_rid(v), f),
			Self::Array(v) => Display::fmt(v, f),
			Self::Object(v) => Display::fmt(v, f),
			Self::Generate(v) => match v {
				Gen::Rand => Display::fmt("rand()", f),
				Gen::Ulid => Display::fmt("ulid()", f),
				Gen::Uuid => Display::fmt("uuid()", f),
			},
		}
	}
}

impl IdValue {
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
		Self::String(Uuid::new_v7().to_raw())
	}
	/// Convert the Id to a raw String
	pub fn to_raw(&self) -> String {
		match self {
			Self::Number(v) => v.to_string(),
			Self::String(v) => v.to_string(),
			Self::Array(v) => v.to_string(),
			Self::Object(v) => v.to_string(),
			Self::Generate(v) => match v {
				Gen::Rand => "rand()".to_string(),
				Gen::Ulid => "ulid()".to_string(),
				Gen::Uuid => "uuid()".to_string(),
			},
		}
	}
}

impl IdValue {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<IdValue, Error> {
		match self {
			IdValue::Number(v) => Ok(IdValue::Number(*v)),
			IdValue::String(v) => Ok(IdValue::String(v.clone())),
			IdValue::Array(v) => match v.compute(stk, ctx, opt, doc).await? {
				Value::Array(v) => Ok(IdValue::Array(v)),
				_ => unreachable!(),
			},
			IdValue::Object(v) => match v.compute(stk, ctx, opt, doc).await? {
				Value::Object(v) => Ok(IdValue::Object(v)),
				_ => unreachable!(),
			},
			IdValue::Generate(v) => match v {
				Gen::Rand => Ok(Self::rand()),
				Gen::Ulid => Ok(Self::ulid()),
				Gen::Uuid => Ok(Self::uuid()),
			},
		}
	}
}
