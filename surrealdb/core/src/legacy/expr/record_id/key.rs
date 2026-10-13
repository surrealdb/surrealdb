use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::expr::record_id::key::RecordIdKeyLit;
use crate::val::{Array, Object, RecordIdKey};

/// Process this type returning a computed simple Value
pub(crate) async fn record_id_key_lit_compute(
	this: &RecordIdKeyLit,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<RecordIdKey> {
	match this {
		RecordIdKeyLit::Number(v) => Ok(RecordIdKey::Number(*v)),
		RecordIdKeyLit::String(v) => Ok(RecordIdKey::String(v.clone())),
		RecordIdKeyLit::Uuid(v) => Ok(RecordIdKey::Uuid(*v)),
		RecordIdKeyLit::Array(v) => {
			let mut res = Vec::new();
			for v in v.iter() {
				let v = stk
					.run(|stk| crate::legacy::expr_compute(v, stk, ctx, opt, doc))
					.await
					.catch_return()?;
				res.push(v);
			}
			Ok(RecordIdKey::Array(Array(res)))
		}
		RecordIdKeyLit::Object(v) => {
			let mut res = Object::default();
			for entry in v.iter() {
				let v = stk
					.run(|stk| crate::legacy::expr_compute(&entry.value, stk, ctx, opt, doc))
					.await
					.catch_return()?;
				res.insert(entry.key.clone(), v);
			}
			Ok(RecordIdKey::Object(res))
		}
		RecordIdKeyLit::Generate(v) => Ok(v.compute()),
		RecordIdKeyLit::Range(v) => {
			let range =
				crate::legacy::record_id_key_range_lit_compute(v, stk, ctx, opt, doc).await?;
			Ok(RecordIdKey::Range(Box::new(range)))
		}
	}
}
