use crate::sql::{Expr, Uuid, escape::EscapeRid, literal::ObjectEntry};
use std::fmt::{self, Display, Formatter};

pub mod range;
pub use range::IdRange;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Gen {
	Rand,
	Ulid,
	Uuid,
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
	Range(Box<IdRange>),
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
