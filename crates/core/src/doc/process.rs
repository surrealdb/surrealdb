use reblessive::tree::Stk;

use super::IgnoreError;
use crate::ctx::Context;
use crate::dbs::{Operable, Options, Processed, Statement, Workable};
use crate::doc::Document;
use crate::err::Error;
use crate::val::Value;
use crate::val::record::Record;

impl Document {
	pub(crate) async fn process(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		pro: Processed,
	) -> Result<Value, IgnoreError> {
		// Check current context
		if ctx.is_done(true).await? {
			// Don't process the document
			return Err(IgnoreError::Ignore);
		}
		// Setup a new workable
		let ins = match pro.val {
			Operable::Value(v) => (v, Workable::Normal),
			Operable::Insert(v, o) => (v, Workable::Insert(o)),
			Operable::Relate(f, v, w, o) => (v, Workable::Relate(f, w, o)),
			Operable::Count(count) => {
				(Record::new(Value::from(count).into()).into_read_only(), Workable::Normal)
			}
		};
		// Setup a new document
		let mut doc = Document::new(pro.rid, pro.ir, pro.generate, ins.0, ins.1, false, pro.rs);
		// Generate a new document id if necessary
		doc.generate_record_id(stk, ctx, opt, stm).await?;
		// Process the statement
		let res = match stm {
			Statement::Select(_) => doc.select(stk, ctx, opt, stm).await?,
			Statement::Create(_) => doc.create(stk, ctx, opt, stm).await?,
			Statement::Upsert(_) => doc.upsert(stk, ctx, opt, stm).await?,
			Statement::Update(_) => doc.update(stk, ctx, opt, stm).await?,
			Statement::Relate(_) => doc.relate(stk, ctx, opt, stm).await?,
			Statement::Delete(_) => doc.delete(stk, ctx, opt, stm).await?,
			Statement::Insert(stm) => doc.insert(stk, ctx, opt, stm).await?,
			stm => {
				return Err(IgnoreError::from(anyhow::Error::new(Error::unreachable(
					format_args!("Unexpected statement type: {stm:?}"),
				))));
			}
		};
		Ok(res)
	}
}
