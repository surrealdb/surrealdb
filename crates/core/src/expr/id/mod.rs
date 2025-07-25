use super::FlowResultExt as _;
use super::fmt::Fmt;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Expr;
use crate::expr::escape::EscapeRid;
use crate::expr::literal::ObjectEntry;
use crate::val::{Array, Object, RecordIdKey, Strand, Uuid};

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

pub mod range;
pub use range::RecordIdKeyRangeLit;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Gen {
	Rand,
	Ulid,
	Uuid,
}

impl Gen {
	pub fn compute(&self) -> RecordIdKey {
		match self {
			Gen::Rand => RecordIdKey::rand(),
			Gen::Ulid => RecordIdKey::ulid(),
			Gen::Uuid => RecordIdKey::uuid(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RecordIdKeyLit {
	Number(i64),
	String(Strand),
	Uuid(Uuid),
	Array(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Generate(Gen),
	Range(Box<RecordIdKeyRangeLit>),
}

impl From<RecordIdKeyRangeLit> for RecordIdKeyLit {
	fn from(v: RecordIdKeyRangeLit) -> Self {
		Self::Range(Box::new(v))
	}
}

impl Display for RecordIdKeyLit {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Number(v) => Display::fmt(v, f),
			Self::String(v) => EscapeRid(v).fmt(f),
			Self::Uuid(v) => Display::fmt(v, f),
			Self::Array(v) => {
				write!(f, "[{}]", Fmt::comma_separated(v.iter()))
			}
			Self::Object(v) => write!(f, "{{{}}}", Fmt::comma_separated(v.iter())),
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
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<RecordIdKey> {
		match self {
			RecordIdKeyLit::Number(v) => Ok(RecordIdKey::Number(*v)),
			RecordIdKeyLit::String(v) => Ok(RecordIdKey::String(v.clone().into_string())),
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
