use crate::ctx::Context;
use crate::dbs::Operable;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::dbs::{Options, Processed};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use std::sync::Arc;

impl Document {
	#[allow(dead_code)]
	pub(crate) async fn process(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		pro: Processed,
	) -> Result<Value, Error> {
		// Check current context
		if ctx.is_done(true)? {
			// Don't process the document
			return Err(Error::Ignore);
		}
		// Setup a new workable
		let ins = match pro.val {
			Operable::Value(v) => (v, Workable::Normal),
			Operable::Insert(v, o) => (v, Workable::Insert(o)),
			Operable::Relate(f, v, w, o) => (v, Workable::Relate(f, w, o)),
			Operable::Count(count) => (Arc::new(count.into()), Workable::Normal),
		};
		// Setup a new document
		let mut doc = Document::new(pro.rid, pro.ir, pro.generate, ins.0, ins.1, false, pro.rs);
		// Generate a new document id if necessary
		doc.generate_record_id(stk, ctx, opt, stm).await?;
		// Process the statement
		match stm {
			Statement::Select(_) => doc.select(stk, ctx, opt, stm).await,
			Statement::Create(_) => doc.create(stk, ctx, opt, stm).await,
			Statement::Upsert(_) => doc.upsert(stk, ctx, opt, stm).await,
			Statement::Update(_) => doc.update(stk, ctx, opt, stm).await,
			Statement::Relate(_) => doc.relate(stk, ctx, opt, stm).await,
			Statement::Delete(_) => doc.delete(stk, ctx, opt, stm).await,
			Statement::Insert(stm) => doc.insert(stk, ctx, opt, stm).await,
			stm => Err(fail!("Unexpected statement type: {stm:?}")),
		}
	}
}
