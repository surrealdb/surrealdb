use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{Array, Number, Object, Strand, Thing, Uuid, Value};
use range::IdRange;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::ops::Bound;
use value::IdValue;

use super::Range;

pub mod range;
pub mod value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Id {
	Value(IdValue),
	Range(IdRange),
}

impl From<i64> for Id {
	fn from(v: i64) -> Self {
		Self::Value(v.into())
	}
}

impl From<i32> for Id {
	fn from(v: i32) -> Self {
		Self::Value(v.into())
	}
}

impl From<u64> for Id {
	fn from(v: u64) -> Self {
		Self::Value(v.into())
	}
}

impl From<String> for Id {
	fn from(v: String) -> Self {
		Self::Value(v.into())
	}
}

impl From<Array> for Id {
	fn from(v: Array) -> Self {
		Self::Value(v.into())
	}
}

impl From<Object> for Id {
	fn from(v: Object) -> Self {
		Self::Value(v.into())
	}
}

impl From<Uuid> for Id {
	fn from(v: Uuid) -> Self {
		Self::Value(v.into())
	}
}

impl From<Strand> for Id {
	fn from(v: Strand) -> Self {
		Self::Value(v.into())
	}
}

impl From<&str> for Id {
	fn from(v: &str) -> Self {
		Self::Value(v.into())
	}
}

impl From<&String> for Id {
	fn from(v: &String) -> Self {
		Self::Value(v.into())
	}
}

impl From<Vec<&str>> for Id {
	fn from(v: Vec<&str>) -> Self {
		Id::Value(v.into())
	}
}

impl From<Vec<String>> for Id {
	fn from(v: Vec<String>) -> Self {
		Id::Value(v.into())
	}
}

impl From<Vec<Value>> for Id {
	fn from(v: Vec<Value>) -> Self {
		Id::Value(v.into())
	}
}

impl From<BTreeMap<String, Value>> for Id {
	fn from(v: BTreeMap<String, Value>) -> Self {
		Id::Value(v.into())
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

impl From<IdValue> for Id {
	fn from(v: IdValue) -> Self {
		Self::Value(v)
	}
}

impl From<IdRange> for Id {
	fn from(v: IdRange) -> Self {
		Self::Range(v)
	}
}

impl From<(Bound<IdValue>, Bound<IdValue>)> for Id {
	fn from(v: (Bound<IdValue>, Bound<IdValue>)) -> Self {
		Self::Range(v.into())
	}
}

impl TryFrom<Range> for Id {
	type Error = Error;
	fn try_from(v: Range) -> Result<Self, Self::Error> {
		Ok(Id::Range(IdRange::try_from(v)?))
	}
}

impl TryFrom<Value> for Id {
	type Error = Error;
	fn try_from(v: Value) -> Result<Self, Self::Error> {
		match v {
			Value::Range(v) => Ok(Id::Range(IdRange::try_from(*v)?)),
			v => Ok(Id::Value(IdValue::try_from(v)?)),
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
		Self::Value(IdValue::rand())
	}
	/// Generate a new random ULID
	pub fn ulid() -> Self {
		Self::Value(IdValue::ulid())
	}
	/// Generate a new random UUID
	pub fn uuid() -> Self {
		Self::Value(IdValue::uuid())
	}
	/// Convert the Id to a raw String
	pub fn to_raw(&self) -> String {
		match self {
			Self::Value(v) => v.to_raw(),
			Self::Range(v) => v.to_string(),
		}
	}
}

impl Display for Id {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Value(v) => Display::fmt(v, f),
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
			Id::Value(v) => Ok(Id::Value(v.compute(stk, ctx, opt, doc).await?)),
			Id::Range(v) => Ok(Id::Range(v.compute(stk, ctx, opt, doc).await?)),
		}
	}

	pub fn value(&self) -> Result<&IdValue, Error> {
		match self {
			Id::Value(v) => Ok(v),
			Id::Range(_) => Err(Error::IdInvalid {
				value: "idrange".to_string(),
			}),
		}
	}

	pub fn range(&self) -> Result<&IdRange, Error> {
		match self {
			Id::Range(v) => Ok(v),
			Id::Value(_) => Err(Error::IdInvalid {
				value: "idvalue".to_string(),
			}),
		}
	}
}
