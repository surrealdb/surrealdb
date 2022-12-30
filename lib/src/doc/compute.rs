use crate::ctx::Context;
use crate::dbs::Operable;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use channel::Sender;

impl<'a> Document<'a> {
	#[allow(dead_code)]
	pub(crate) async fn compute(
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		chn: Sender<Result<Value, Error>>,
		thg: Option<Thing>,
		val: Operable,
	) -> Result<(), Error> {
		// Setup a new workable
		let ins = match val {
			Operable::Value(v) => (v, Workable::Normal),
			Operable::Mergeable(v, o) => (v, Workable::Insert(o)),
			Operable::Relatable(f, v, w) => (v, Workable::Relate(f, w)),
		};
		// Setup a new document
		let mut doc = Document::new(thg, &ins.0, ins.1);
		// Process the statement
		let res = match stm {
			Statement::Select(_) => doc.select(ctx, opt, txn, stm).await,
			Statement::Create(_) => doc.create(ctx, opt, txn, stm).await,
			Statement::Update(_) => doc.update(ctx, opt, txn, stm).await,
			Statement::Relate(_) => doc.relate(ctx, opt, txn, stm).await,
			Statement::Delete(_) => doc.delete(ctx, opt, txn, stm).await,
			Statement::Insert(_) => doc.insert(ctx, opt, txn, stm).await,
		};
		// Send back the result
		let _ = chn.send(res).await;
		// Everything went ok
		Ok(())
	}
}
