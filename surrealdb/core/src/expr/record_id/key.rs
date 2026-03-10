use std::ops::Bound;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::literal::ObjectEntry;
use crate::expr::{Expr, FlowResultExt as _, Kind, KindLiteral, RecordIdKeyRangeLit};
use crate::val::{Array, Object, RecordIdKey, Uuid};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RecordIdKeyGen {
	Rand,
	Ulid,
	Uuid,
}

impl RecordIdKeyGen {
	pub(crate) fn compute(&self) -> RecordIdKey {
		match self {
			RecordIdKeyGen::Rand => RecordIdKey::rand(),
			RecordIdKeyGen::Ulid => RecordIdKey::ulid(),
			RecordIdKeyGen::Uuid => RecordIdKey::uuid(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RecordIdKeyLit {
	Number(i64),
	String(String),
	Uuid(Uuid),
	Array(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Generate(RecordIdKeyGen),
	Range(Box<RecordIdKeyRangeLit>),
}

impl RecordIdKeyLit {
	pub(crate) fn kind_supported(kind: &Kind) -> bool {
		match kind {
			Kind::Any => true,
			Kind::Number => true,
			Kind::Int => true,
			Kind::String => true,
			Kind::Uuid => true,
			Kind::Array(_, _) => true,
			Kind::Set(_, _) => true,
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
	pub(crate) fn is_static(&self) -> bool {
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

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
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
					let v = stk.run(|stk| v.compute(stk, ctx, opt, doc)).await.catch_return()?;
					res.push(v);
				}
				Ok(RecordIdKey::Array(Array(res)))
			}
			RecordIdKeyLit::Object(v) => {
				let mut res = Object::default();
				for entry in v.iter() {
					let v = stk
						.run(|stk| entry.value.compute(stk, ctx, opt, doc))
						.await
						.catch_return()?;
					res.insert(entry.key.clone(), v);
				}
				Ok(RecordIdKey::Object(res))
			}
			RecordIdKeyLit::Generate(v) => Ok(v.compute()),
			RecordIdKeyLit::Range(v) => {
				let range = v.compute(stk, ctx, opt, doc).await?;
				Ok(RecordIdKey::Range(Box::new(range)))
			}
		}
	}
}

impl From<crate::types::PublicRecordIdKey> for RecordIdKeyLit {
	fn from(value: crate::types::PublicRecordIdKey) -> Self {
		match value {
			crate::types::PublicRecordIdKey::Number(x) => Self::Number(x),
			crate::types::PublicRecordIdKey::String(x) => Self::String(x),
			crate::types::PublicRecordIdKey::Uuid(x) => Self::Uuid(x.into()),
			crate::types::PublicRecordIdKey::Array(x) => {
				Self::Array(x.into_iter().map(Expr::from_public_value).collect())
			}
			crate::types::PublicRecordIdKey::Object(x) => Self::Object(
				x.into_iter()
					.map(|(k, v)| ObjectEntry {
						key: k,
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
