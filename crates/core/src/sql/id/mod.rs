use crate::sql::escape::EscapeRid;
use crate::sql::literal::ObjectEntry;
use crate::sql::{Expr, Uuid};
use std::fmt::{self, Display, Formatter};

pub mod range;
pub use range::RecordIdKeyRangeLit;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Gen {
	Rand,
	Ulid,
	Uuid,
}

impl From<Gen> for crate::expr::id::Gen {
	fn from(value: Gen) -> Self {
		match value {
			Gen::Rand => crate::expr::id::Gen::Rand,
			Gen::Ulid => crate::expr::id::Gen::Ulid,
			Gen::Uuid => crate::expr::id::Gen::Uuid,
		}
	}
}

impl From<crate::expr::id::Gen> for Gen {
	fn from(value: crate::expr::id::Gen) -> Self {
		match value {
			crate::expr::id::Gen::Rand => Gen::Rand,
			crate::expr::id::Gen::Ulid => Gen::Ulid,
			crate::expr::id::Gen::Uuid => Gen::Uuid,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RecordIdKeyLit {
	Number(i64),
	String(String),
	Uuid(Uuid),
	Array(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Generate(Gen),
	Range(Box<RecordIdKeyRangeLit>),
}

impl From<RecordIdKeyLit> for crate::expr::RecordIdKeyLit {
	fn from(value: RecordIdKeyLit) -> Self {
		match value {
			RecordIdKeyLit::Number(x) => crate::expr::RecordIdKeyLit::Number(x),
			RecordIdKeyLit::String(x) => crate::expr::RecordIdKeyLit::String(x),
			RecordIdKeyLit::Uuid(x) => crate::expr::RecordIdKeyLit::Uuid(x.into()),
			RecordIdKeyLit::Array(x) => crate::expr::RecordIdKeyLit::Array(x.into()),
			RecordIdKeyLit::Object(x) => crate::expr::RecordIdKeyLit::Object(x.into()),
			RecordIdKeyLit::Generate(x) => crate::expr::RecordIdKeyLit::Generate(x.into()),
			RecordIdKeyLit::Range(x) => crate::expr::RecordIdKeyLit::Range(x.into()),
		}
	}
}

impl From<crate::expr::RecordIdKeyLit> for RecordIdKeyLit {
	fn from(value: RecordIdKeyLit) -> Self {
		match value {
			crate::expr::RecordIdKeyLit::Number(x) => RecordIdKeyLit::Number(x),
			crate::expr::RecordIdKeyLit::String(x) => RecordIdKeyLit::String(x),
			crate::expr::RecordIdKeyLit::Uuid(uuid) => RecordIdKeyLit::Uuid(uuid),
			crate::expr::RecordIdKeyLit::Array(exprs) => RecordIdKeyLit::Array(exprs),
			crate::expr::RecordIdKeyLit::Object(items) => RecordIdKeyLit::Object(items),
			crate::expr::RecordIdKeyLit::Generate(x) => RecordIdKeyLit::Generate(x),
			crate::expr::RecordIdKeyLit::Range(x) => RecordIdKeyLit::Range(x.into()),
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
