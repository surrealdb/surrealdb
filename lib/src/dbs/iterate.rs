use crate::ctx::Context;
use crate::dbs::Iterator;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::key::thing;
use crate::sql::array::Array;
use crate::sql::id::Id;
use crate::sql::model::Model;
use crate::sql::object::Object;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use async_recursion::async_recursion;

impl Value {
	#[cfg_attr(feature = "parallel", async_recursion)]
	#[cfg_attr(not(feature = "parallel"), async_recursion(?Send))]
	pub(crate) async fn iterate(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match self {
				Value::Object(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
				Value::Array(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
				Value::Model(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
				Value::Thing(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
				Value::Table(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
				v => ite.process(ctx, opt, txn, stm, None, v).await,
			}
		}
		Ok(())
	}
}

impl Array {
	#[cfg_attr(feature = "parallel", async_recursion)]
	#[cfg_attr(not(feature = "parallel"), async_recursion(?Send))]
	pub(crate) async fn iterate(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		for v in self.into_iter() {
			if ctx.is_ok() {
				match v {
					Value::Object(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
					Value::Array(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
					Value::Model(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
					Value::Thing(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
					Value::Table(v) => v.iterate(ctx, opt, txn, stm, ite).await?,
					v => ite.process(ctx, opt, txn, stm, None, v).await,
				}
			}
		}
		Ok(())
	}
}

impl Object {
	#[cfg_attr(feature = "parallel", async_recursion)]
	#[cfg_attr(not(feature = "parallel"), async_recursion(?Send))]
	pub(crate) async fn iterate(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			if let Some(Value::Thing(id)) = self.get("id") {
				id.clone().iterate(ctx, opt, txn, stm, ite).await?;
			}
		}
		Ok(())
	}
}

impl Model {
	pub(crate) async fn iterate(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match self {
				Model::Count(tb, c) => {
					for _ in 0..c {
						Thing {
							tb: tb.to_string(),
							id: Id::rand(),
						}
						.iterate(ctx, opt, txn, stm, ite)
						.await?;
					}
				}
				Model::Range(tb, b, e) => {
					for x in b..=e {
						Thing {
							tb: tb.to_string(),
							id: Id::from(x),
						}
						.iterate(ctx, opt, txn, stm, ite)
						.await?;
					}
				}
			}
		}
		Ok(())
	}
}

impl Thing {
	pub(crate) async fn iterate(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			let key = thing::new(opt.ns(), opt.db(), &self.tb, &self.id);
			let val = txn.clone().lock().await.get(key).await?;
			let val = match val {
				Some(v) => Value::from(v),
				None => Value::None,
			};
			ite.process(ctx, opt, txn, stm, Some(self), val).await;
		}
		Ok(())
	}
}

impl Table {
	pub(crate) async fn iterate(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			let beg = thing::prefix(opt.ns(), opt.db(), &self);
			let end = thing::suffix(opt.ns(), opt.db(), &self);
			let mut nxt: Option<Vec<u8>> = None;
			loop {
				if ctx.is_ok() {
					let res = match nxt {
						None => {
							let min = beg.clone();
							let max = end.clone();
							txn.clone().lock().await.scan(min..max, 1000).await?
						}
						Some(ref mut beg) => {
							beg.push(0x00);
							let min = beg.clone();
							let max = end.clone();
							txn.clone().lock().await.scan(min..max, 1000).await?
						}
					};
					if !res.is_empty() {
						// Get total results
						let n = res.len();
						// Exit when settled
						if n == 0 {
							break;
						}
						// Loop over results
						for (i, (k, v)) in res.into_iter().enumerate() {
							if ctx.is_ok() {
								// Ready the next
								if n == i + 1 {
									nxt = Some(k.clone());
								}
								// Parse the key-value
								let k: crate::key::thing::Thing = (&k).into();
								let v: crate::sql::value::Value = (&v).into();
								let t = Thing::from((k.tb, k.id));
								// Process the record
								ite.process(ctx, opt, txn, stm, Some(t), v).await;
							}
						}
						continue;
					}
				}
				break;
			}
		}
		Ok(())
	}
}
