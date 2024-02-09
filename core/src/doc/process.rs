use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::dbs::{Operable, Transaction};
use crate::dbs::{Options, Processed};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	#[allow(dead_code)]
	pub(crate) async fn process(
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		mut pro: Processed,
	) -> Result<Value, Error> {
		// Loop over maximum two times
		for _ in 0..2 {
			// Setup a new workable
			let ins = match pro.val {
				Operable::Value(v) => (v, Workable::Normal),
				Operable::Mergeable(v, o) => (v, Workable::Insert(o)),
				Operable::Relatable(f, v, w) => (v, Workable::Relate(f, w)),
			};
			// Setup a new document
			let mut doc = Document::new(pro.ir, pro.rid.as_ref(), pro.doc_id, &ins.0, ins.1);
			// Process the statement
			let res = match stm {
				Statement::Select(_) => doc.select(ctx, opt, txn, stm).await,
				Statement::Create(_) => doc.create(ctx, opt, txn, stm).await,
				Statement::Update(_) => doc.update(ctx, opt, txn, stm).await,
				Statement::Relate(_) => doc.relate(ctx, opt, txn, stm).await,
				Statement::Delete(_) => doc.delete(ctx, opt, txn, stm).await,
				Statement::Insert(_) => doc.insert(ctx, opt, txn, stm).await,
				_ => unreachable!(),
			};
			// Check the result
			let res = match res {
				// We received an error suggesting that we
				// retry this request using a new ID, so
				// we load the new record, and reprocess
				Err(Error::RetryWithId(v)) => {
					// Fetch the data from the store
					let key = crate::key::thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
					let val = txn.clone().lock().await.get(key).await?;
					// Parse the data from the store
					let val = match val {
						Some(v) => Value::from(v),
						None => Value::None,
					};
					pro = Processed {
						ir: None,
						doc_id: None,
						rid: Some(v),
						val: match doc.extras {
							Workable::Normal => Operable::Value(val),
							Workable::Insert(o) => Operable::Mergeable(val, o),
							Workable::Relate(f, w) => Operable::Relatable(f, val, w),
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
			return res;
		}
		// We should never get here
		unreachable!()
	}
}
