use std::fmt::{self, Display, Formatter, Write as _};
use std::ops::Bound;

use crate::fmt::{EscapeKey, EscapeRid, Fmt, Pretty, is_pretty, pretty_indent};
use crate::sql::literal::ObjectEntry;
use crate::sql::{Expr, RecordIdKeyRangeLit};
use crate::types::{PublicRecordIdKey, PublicUuid};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RecordIdKeyGen {
	Rand,
	Ulid,
	Uuid,
}

impl From<RecordIdKeyGen> for crate::expr::RecordIdKeyGen {
	fn from(value: RecordIdKeyGen) -> Self {
		match value {
			RecordIdKeyGen::Rand => crate::expr::RecordIdKeyGen::Rand,
			RecordIdKeyGen::Ulid => crate::expr::RecordIdKeyGen::Ulid,
			RecordIdKeyGen::Uuid => crate::expr::RecordIdKeyGen::Uuid,
		}
	}
}

impl From<crate::expr::RecordIdKeyGen> for RecordIdKeyGen {
	fn from(value: crate::expr::RecordIdKeyGen) -> Self {
		match value {
			crate::expr::RecordIdKeyGen::Rand => RecordIdKeyGen::Rand,
			crate::expr::RecordIdKeyGen::Ulid => RecordIdKeyGen::Ulid,
			crate::expr::RecordIdKeyGen::Uuid => RecordIdKeyGen::Uuid,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum RecordIdKeyLit {
	Number(i64),
	String(String),
	Uuid(PublicUuid),
	Array(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Generate(RecordIdKeyGen),
	Range(Box<RecordIdKeyRangeLit>),
}

impl RecordIdKeyLit {
	pub fn from_record_id_key(key: PublicRecordIdKey) -> Self {
		match key {
			PublicRecordIdKey::Number(x) => RecordIdKeyLit::Number(x),
			PublicRecordIdKey::String(x) => RecordIdKeyLit::String(x),
			PublicRecordIdKey::Uuid(x) => RecordIdKeyLit::Uuid(x),
			PublicRecordIdKey::Array(x) => {
				RecordIdKeyLit::Array(x.into_iter().map(Expr::from_public_value).collect())
			}
			PublicRecordIdKey::Object(x) => RecordIdKeyLit::Object(
				x.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k,
						value: Expr::from_public_value(v),
					})
					.collect(),
			),
			PublicRecordIdKey::Range(x) => RecordIdKeyLit::Range(Box::new(RecordIdKeyRangeLit {
				start: match x.start {
					Bound::Included(x) => Bound::Included(Self::from_record_id_key(x)),
					Bound::Excluded(x) => Bound::Excluded(Self::from_record_id_key(x)),
					Bound::Unbounded => Bound::Unbounded,
				},
				end: match x.end {
					Bound::Included(x) => Bound::Included(Self::from_record_id_key(x)),
					Bound::Excluded(x) => Bound::Excluded(Self::from_record_id_key(x)),
					Bound::Unbounded => Bound::Unbounded,
				},
			})),
		}
	}
}

impl From<RecordIdKeyLit> for crate::expr::RecordIdKeyLit {
	fn from(value: RecordIdKeyLit) -> Self {
		match value {
			RecordIdKeyLit::Number(x) => crate::expr::RecordIdKeyLit::Number(x),
			RecordIdKeyLit::String(x) => crate::expr::RecordIdKeyLit::String(x.clone()),
			RecordIdKeyLit::Uuid(x) => crate::expr::RecordIdKeyLit::Uuid(crate::val::Uuid(x.0)),
			RecordIdKeyLit::Array(x) => {
				crate::expr::RecordIdKeyLit::Array(x.into_iter().map(From::from).collect())
			}
			RecordIdKeyLit::Object(x) => {
				crate::expr::RecordIdKeyLit::Object(x.into_iter().map(From::from).collect())
			}
			RecordIdKeyLit::Generate(x) => crate::expr::RecordIdKeyLit::Generate(x.into()),
			RecordIdKeyLit::Range(x) => crate::expr::RecordIdKeyLit::Range(Box::new((*x).into())),
		}
	}
}

impl From<crate::expr::RecordIdKeyLit> for RecordIdKeyLit {
	fn from(value: crate::expr::RecordIdKeyLit) -> Self {
		match value {
			crate::expr::RecordIdKeyLit::Number(x) => RecordIdKeyLit::Number(x),
			crate::expr::RecordIdKeyLit::String(x) => RecordIdKeyLit::String(x.to_string()),
			crate::expr::RecordIdKeyLit::Uuid(uuid) => {
				RecordIdKeyLit::Uuid(surrealdb_types::Uuid(uuid.0))
			}
			crate::expr::RecordIdKeyLit::Array(exprs) => {
				RecordIdKeyLit::Array(exprs.into_iter().map(From::from).collect())
			}
			crate::expr::RecordIdKeyLit::Object(items) => {
				RecordIdKeyLit::Object(items.into_iter().map(From::from).collect())
			}
			crate::expr::RecordIdKeyLit::Generate(x) => RecordIdKeyLit::Generate(x.into()),
			crate::expr::RecordIdKeyLit::Range(x) => RecordIdKeyLit::Range(Box::new((*x).into())),
		}
	}
}

impl Display for RecordIdKeyLit {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Number(v) => Display::fmt(v, f),
			Self::String(v) => EscapeRid(v).fmt(f),
			Self::Uuid(v) => Display::fmt(v, f),
			Self::Array(v) => {
				let mut f = Pretty::from(f);
				f.write_char('[')?;
				if !v.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(v.iter()))?;
					drop(indent);
				}
				f.write_char(']')
			}
			Self::Object(v) => {
				let mut f = Pretty::from(f);
				if is_pretty() {
					f.write_char('{')?;
				} else {
					f.write_str("{ ")?;
				}
				if !v.is_empty() {
					let indent = pretty_indent();
					write!(
						f,
						"{}",
						Fmt::pretty_comma_separated(v.iter().map(|args| Fmt::new(
							args,
							|entry, f| write!(f, "{}: {}", EscapeKey(&entry.key), &entry.value)
						)),)
					)?;
					drop(indent);
				}
				if is_pretty() {
					f.write_char('}')
				} else {
					f.write_str(" }")
				}
			}
			Self::Generate(v) => match v {
				RecordIdKeyGen::Rand => Display::fmt("rand()", f),
				RecordIdKeyGen::Ulid => Display::fmt("ulid()", f),
				RecordIdKeyGen::Uuid => Display::fmt("uuid()", f),
			},
			Self::Range(v) => Display::fmt(v, f),
		}
	}
}
