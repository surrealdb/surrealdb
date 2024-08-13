use crate::cnf::ID_CHARS;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{escape::escape_rid, Array, Number, Object, Strand, Thing, Uuid, Value};
use nanoid::nanoid;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::ops::Bound;
use ulid::Ulid;

use super::Range;

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
pub enum Id {
	Number(i64),
	String(String),
	Array(Array),
	Object(Object),
	Generate(Gen),
	Range(Box<Range>),
}

impl From<i64> for Id {
	fn from(v: i64) -> Self {
		Self::Number(v)
	}
}

impl From<i32> for Id {
	fn from(v: i32) -> Self {
		Self::Number(v as i64)
	}
}

impl From<u64> for Id {
	fn from(v: u64) -> Self {
		Self::Number(v as i64)
	}
}

impl From<String> for Id {
	fn from(v: String) -> Self {
		Self::String(v)
	}
}

impl From<Array> for Id {
	fn from(v: Array) -> Self {
		Self::Array(v)
	}
}

impl From<Object> for Id {
	fn from(v: Object) -> Self {
		Self::Object(v)
	}
}

impl From<Uuid> for Id {
	fn from(v: Uuid) -> Self {
		Self::String(v.to_raw())
	}
}

impl From<Strand> for Id {
	fn from(v: Strand) -> Self {
		Self::String(v.as_string())
	}
}

impl From<&str> for Id {
	fn from(v: &str) -> Self {
		Self::String(v.to_owned())
	}
}

impl From<&String> for Id {
	fn from(v: &String) -> Self {
		Self::String(v.to_owned())
	}
}

impl From<Vec<&str>> for Id {
	fn from(v: Vec<&str>) -> Self {
		Id::Array(v.into())
	}
}

impl From<Vec<String>> for Id {
	fn from(v: Vec<String>) -> Self {
		Id::Array(v.into())
	}
}

impl From<Vec<Value>> for Id {
	fn from(v: Vec<Value>) -> Self {
		Id::Array(v.into())
	}
}

impl From<BTreeMap<String, Value>> for Id {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Id::Object(v.into())
	}
}

impl From<Number> for Id {
	fn from(v: Number) -> Self {
		match v {
			Number::Int(v) => v.into(),
			Number::Float(v) => v.to_string().into(),
			Number::Decimal(v) => v.to_string().into(),
		}
	}
}

impl TryFrom<Range> for Id {
	type Error = Error;
	fn try_from(v: Range) -> Result<Self, Self::Error> {
		if let Bound::Included(v) | Bound::Excluded(v) = &v.beg {
			if !Id::value_valid_as_id(v) {
				return Err(Error::IdInvalid {
					value: v.kindof().to_string(),
				});
			}
		}

		if let Bound::Included(v) | Bound::Excluded(v) = &v.end {
			if !Id::value_valid_as_id(v) {
				return Err(Error::IdInvalid {
					value: v.kindof().to_string(),
				});
			}
		}

		Ok(Id::Range(Box::new(v)))
	}
}

impl TryFrom<Value> for Id {
	type Error = Error;
	fn try_from(v: Value) -> Result<Self, Self::Error> {
		match v {
			Value::Number(Number::Int(v)) => Ok(v.into()),
			Value::Strand(v) => Ok(v.into()),
			Value::Array(v) => Ok(v.into()),
			Value::Object(v) => Ok(v.into()),
			Value::Range(v) => Id::try_from(*v),
			v => Err(Error::IdInvalid {
				value: v.kindof().to_string(),
			}),
		}
	}
}

impl From<Thing> for Id {
	fn from(v: Thing) -> Self {
		v.id
	}
}

impl Id {
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
			Self::Range(v) => v.to_string(),
		}
	}
	/// Can this value be a valid Id?
	pub fn value_valid_as_id(v: &Value) -> bool {
		match v {
			Value::Number(Number::Int(_)) => true,
			Value::Strand(_) => true,
			Value::Array(_) => true,
			Value::Object(_) => true,
			Value::Range(_) => true,
			_ => false,
		}
	}
	/// Can this value be a valid Id?
	pub fn bound_valid_as_id(v: &Bound<Value>) -> bool {
		match v {
			Bound::Included(v) | Bound::Excluded(v) => Self::value_valid_as_id(v),
			Bound::Unbounded => true,
		}
	}
}

impl Display for Id {
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
			Self::Range(v) => Display::fmt(v, f),
		}
	}
}

impl Id {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Id, Error> {
		match self {
			Id::Number(v) => Ok(Id::Number(*v)),
			Id::String(v) => Ok(Id::String(v.clone())),
			Id::Array(v) => match v.compute(stk, ctx, opt, doc).await? {
				Value::Array(v) => Ok(Id::Array(v)),
				_ => unreachable!(),
			},
			Id::Object(v) => match v.compute(stk, ctx, opt, doc).await? {
				Value::Object(v) => Ok(Id::Object(v)),
				_ => unreachable!(),
			},
			Id::Generate(v) => match v {
				Gen::Rand => Ok(Self::rand()),
				Gen::Ulid => Ok(Self::ulid()),
				Gen::Uuid => Ok(Self::uuid()),
			},
			Id::Range(v) => match v.compute(stk, ctx, opt, doc).await? {
				Value::Range(v) => Ok(Id::Range(v)),
				_ => unreachable!(),
			},
		}
	}
}
