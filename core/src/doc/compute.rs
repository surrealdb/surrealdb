use crate::ctx::Context;
use crate::dbs::Operable;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::dbs::{Options, Processed};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;
use channel::Sender;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	#[allow(dead_code)]
	pub(crate) async fn compute(
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		chn: Sender<Result<Value, Error>>,
		mut pro: Processed,
	) -> Result<(), Error> {
		// Loop over maximum two times
		for _ in 0..2 {
			// Check current context
			if ctx.is_done() {
				break;
			}
			// Setup a new workable
			let ins = match pro.val {
				Operable::Value(v) => (v, Workable::Normal),
				Operable::Mergeable(v, o) => (v, Workable::Insert(o)),
				Operable::Relatable(f, v, w, o) => (v, Workable::Relate(f, w, o)),
			};
			// Setup a new document
			let mut doc = Document::new(pro.rid.as_ref(), pro.ir.as_ref(), &ins.0, ins.1);
			// Process the statement
			let res = match stm {
				Statement::Select(_) => doc.select(stk, ctx, opt, stm).await,
				Statement::Create(_) => doc.create(stk, ctx, opt, stm).await,
				Statement::Update(_) => doc.update(stk, ctx, opt, stm).await,
				Statement::Relate(_) => doc.relate(stk, ctx, opt, stm).await,
				Statement::Delete(_) => doc.delete(stk, ctx, opt, stm).await,
				Statement::Insert(_) => doc.insert(stk, ctx, opt, stm).await,
				_ => unreachable!(),
			};
			// Check the result
			let res = match res {
				// We received an error suggesting that we
				// retry this request using a new ID, so
				// we load the new record, and reprocess
				Err(Error::RetryWithId(v)) => {
					// Fetch the data from the store
					let key = crate::key::thing::new(opt.ns()?, opt.db()?, &v.tb, &v.id);
					let val = ctx.tx_lock().await.get(key).await?;
					// Parse the data from the store
					let val = match val {
						Some(v) => Value::from(v),
						None => Value::None,
					};
					pro = Processed {
						rid: Some(v),
						ir: None,
						val: match doc.extras {
							Workable::Normal => Operable::Value(val),
							Workable::Insert(o) => Operable::Mergeable(val, o),
							Workable::Relate(f, w, o) => Operable::Relatable(f, val, w, o),
						},
					};
					// Go to top of loop
					continue;
				}
				// If any other error was received, then let's
				// pass that error through and return an error
				Err(e) => Err(e),
				// Otherwise the record creation succeeded
				Ok(v) => Ok(v),
			};
			// Send back the result
			let _ = chn.send(res).await;
			// Break the loop
			break;
		}
		// Everything went ok
		Ok(())
	}
}
