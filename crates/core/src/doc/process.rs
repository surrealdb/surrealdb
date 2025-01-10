use crate::ctx::Context;
use crate::dbs::Operable;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::dbs::{Options, Processed};
use crate::doc::Document;
use crate::err::Error;
use crate::idx::planner::RecordStrategy;
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
		mut pro: Processed,
	) -> Result<Value, Error> {
		// Whether we are retrying
		let mut retry = false;
		// Loop over maximum two times
		for _ in 0..2 {
			// Check current context
			if ctx.is_done() {
				// Don't process the document
				return Err(Error::Ignore);
			}
			// Setup a new workable
			let ins = match pro.val {
				Operable::Value(v) => (v, Workable::Normal),
				Operable::Insert(v, o) => (v, Workable::Insert(o)),
				Operable::Relate(f, v, w, o) => (v, Workable::Relate(f, w, o)),
				Operable::Count(c) => (Arc::new(Value::Count(c as i64)), Workable::Normal),
			};
			// Setup a new document
			let mut doc = Document::new(pro.rid, pro.ir, pro.generate, ins.0, ins.1, retry, pro.rs);
			// Generate a new document id if necessary
			doc.generate_record_id(stk, ctx, opt, stm).await?;
			// Optionally create a save point so we can roll back any upcoming changes
			let is_save_point = if stm.is_retryable() {
				ctx.tx().lock().await.new_save_point().await;
				true
			} else {
				false
			};
			// Process the statement
			let res = match stm {
				Statement::Select(_) => doc.select(stk, ctx, opt, stm).await,
				Statement::Create(_) => doc.create(stk, ctx, opt, stm).await,
				Statement::Upsert(_) => doc.upsert(stk, ctx, opt, stm).await,
				Statement::Update(_) => doc.update(stk, ctx, opt, stm).await,
				Statement::Relate(_) => doc.relate(stk, ctx, opt, stm).await,
				Statement::Delete(_) => doc.delete(stk, ctx, opt, stm).await,
				Statement::Insert(_) => doc.insert(stk, ctx, opt, stm).await,
				stm => return Err(fail!("Unexpected statement type: {stm:?}")),
			};
			// Check the result
			let res = match res {
				// We received an error suggesting that we
				// retry this request using a new ID, so
				// we load the new record, and reprocess
				Err(Error::RetryWithId(v)) => {
					// We roll back any change following the save point
					if is_save_point {
						ctx.tx().lock().await.rollback_to_save_point().await?;
					}
					// Fetch the data from the store
					let val = ctx
						.tx()
						.get_record(opt.ns()?, opt.db()?, &v.tb, &v.id, opt.version)
						.await?;
					pro = Processed {
						rs: RecordStrategy::KeysAndValues,
						generate: None,
						rid: Some(Arc::new(v)),
						ir: None,
						val: match doc.extras {
							Workable::Normal => Operable::Value(val),
							Workable::Insert(o) => Operable::Insert(val, o),
							Workable::Relate(f, w, o) => Operable::Relate(f, val, w, o),
						},
					};
					// Mark this as retrying
					retry = true;
					// Go to top of loop
					continue;
				}
				// This record didn't match conditions, so skip
				Err(Error::Ignore) => Err(Error::Ignore),
				// Pass other errors through and return the error
				Err(e) => {
					// We roll back any change following the save point
					if is_save_point {
						ctx.tx().lock().await.rollback_to_save_point().await?;
					}
					Err(e)
				}
				// Otherwise the record creation succeeded
				Ok(v) => {
					// The statement is successful, we can release the savepoint
					if is_save_point {
						ctx.tx().lock().await.release_last_save_point().await?;
					}
					Ok(v)
				}
			};
			// Send back the result
			return res;
		}
		// We shouldn't really reach this part, but if we
		// did it was probably due to the fact that we
		// encountered two Err::RetryWithId errors due to
		// two separate UNIQUE index definitions, and it
		// wasn't possible to detect which record was the
		// correct one to be updated
		Err(fail!("Internal error"))
	}
}
