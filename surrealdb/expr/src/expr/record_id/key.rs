use std::ops::Bound;

use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::literal::ObjectEntry;
use crate::expr::{Expr, Kind, KindLiteral, RecordIdKeyRangeLit};
use crate::val::{RecordIdKey, Uuid};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RecordIdKeyGen {
	Rand,
	Ulid,
	Uuid,
}

impl RecordIdKeyGen {
	pub fn compute(&self) -> RecordIdKey {
		match self {
			RecordIdKeyGen::Rand => RecordIdKey::rand(),
			RecordIdKeyGen::Ulid => RecordIdKey::ulid(),
			RecordIdKeyGen::Uuid => RecordIdKey::uuid(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RecordIdKeyLit {
	Number(i64),
	String(Strand),
	Uuid(Uuid),
	Array(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Generate(RecordIdKeyGen),
	Range(Box<RecordIdKeyRangeLit>),
}

impl RecordIdKeyLit {
	pub fn kind_supported(kind: &Kind) -> bool {
		match kind {
			Kind::Any => true,
			Kind::Number => true,
			Kind::Int => true,
			Kind::String => true,
			Kind::Uuid => true,
			Kind::Array(_, _) => true,
			Kind::Object => true,
			Kind::Literal(l) => matches!(
				l,
				KindLiteral::Integer(_)
					| KindLiteral::String(_)
					| KindLiteral::Array(_)
					| KindLiteral::Object(_)
			),
			Kind::Either(x) => x.iter().all(RecordIdKeyLit::kind_supported),
			_ => false,
		}
	}
}

impl From<RecordIdKeyRangeLit> for RecordIdKeyLit {
	fn from(v: RecordIdKeyRangeLit) -> Self {
		Self::Range(Box::new(v))
	}
}

impl ToSql for RecordIdKeyLit {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_record_id_key_lit: crate::sql::RecordIdKeyLit = self.clone().into();
		sql_record_id_key_lit.fmt_sql(f, fmt);
	}
}

impl RecordIdKeyLit {
	pub fn is_static(&self) -> bool {
		match self {
			RecordIdKeyLit::Number(_)
			| RecordIdKeyLit::String(_)
			| RecordIdKeyLit::Uuid(_)
			| RecordIdKeyLit::Generate(_) => true,
			RecordIdKeyLit::Range(record_id_key_range_lit) => record_id_key_range_lit.is_static(),
			RecordIdKeyLit::Array(exprs) => exprs.iter().all(|x| x.is_static()),
			RecordIdKeyLit::Object(items) => items.iter().all(|x| x.value.is_static()),
		}
	}

	/// Whether evaluating this key can modify data; see [`Literal::read_only`].
	pub fn read_only(&self) -> bool {
		match self {
			RecordIdKeyLit::Number(_)
			| RecordIdKeyLit::String(_)
			| RecordIdKeyLit::Uuid(_)
			| RecordIdKeyLit::Generate(_) => true,
			RecordIdKeyLit::Range(record_id_key_range_lit) => record_id_key_range_lit.read_only(),
			RecordIdKeyLit::Array(exprs) => exprs.iter().all(|x| x.read_only()),
			RecordIdKeyLit::Object(items) => items.iter().all(|x| x.value.read_only()),
		}
	}
}

impl From<crate::types::PublicRecordIdKey> for RecordIdKeyLit {
	fn from(value: crate::types::PublicRecordIdKey) -> Self {
		match value {
			crate::types::PublicRecordIdKey::Number(x) => Self::Number(x),
			crate::types::PublicRecordIdKey::String(x) => Self::String(x.into()),
			crate::types::PublicRecordIdKey::Uuid(x) => Self::Uuid(x.into()),
			crate::types::PublicRecordIdKey::Array(x) => {
				Self::Array(x.into_iter().map(Expr::from_public_value).collect())
			}
			crate::types::PublicRecordIdKey::Object(x) => Self::Object(
				x.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k.into(),
						value: Expr::from_public_value(v),
					})
					.collect(),
			),
			crate::types::PublicRecordIdKey::Range(x) => {
				Self::Range(Box::new(RecordIdKeyRangeLit {
					start: match x.start {
						Bound::Included(x) => Bound::Included(Self::from(x)),
						Bound::Excluded(x) => Bound::Excluded(Self::from(x)),
						Bound::Unbounded => Bound::Unbounded,
					},
					end: match x.end {
						Bound::Included(x) => Bound::Included(Self::from(x)),
						Bound::Excluded(x) => Bound::Excluded(Self::from(x)),
						Bound::Unbounded => Bound::Unbounded,
					},
				}))
			}
		}
	}
}
