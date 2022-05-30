use crate::ctx::Context;
use crate::dbs::Iterable;
use crate::dbs::Iterator;
use crate::dbs::Operable;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::key::thing;
use crate::sql::thing::Thing;
use crate::sql::value::Value;

impl Iterable {
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
				Iterable::Value(v) => {
					// Pass the value through
					let val = Operable::Value(v);
					// Process the document record
					ite.process(ctx, opt, txn, stm, None, val).await;
				}
				Iterable::Thing(v) => {
					// Fetch the data from the store
					let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
					let val = txn.clone().lock().await.get(key).await?;
					// Parse the data from the store
					let val = Operable::Value(match val {
						Some(v) => Value::from(v),
						None => Value::None,
					});
					// Process the document record
					ite.process(ctx, opt, txn, stm, Some(v), val).await;
				}
				Iterable::Table(v) => {
					let beg = thing::prefix(opt.ns(), opt.db(), &v);
					let end = thing::suffix(opt.ns(), opt.db(), &v);
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
										// Parse the data from the store
										let key: crate::key::thing::Thing = (&k).into();
										let val: crate::sql::value::Value = (&v).into();
										let rid = Thing::from((key.tb, key.id));
										// Create a new operable value
										let val = Operable::Value(val);
										// Process the record
										ite.process(ctx, opt, txn, stm, Some(rid), val).await;
									}
								}
								continue;
							}
						}
						break;
					}
				}
				Iterable::Mergeable(v, o) => {
					// Fetch the data from the store
					let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
					let val = txn.clone().lock().await.get(key).await?;
					// Parse the data from the store
					let x = match val {
						Some(v) => Value::from(v),
						None => Value::None,
					};
					// Create a new operable value
					let val = Operable::Mergeable(x, o);
					// Process the document record
					ite.process(ctx, opt, txn, stm, Some(v), val).await;
				}
				Iterable::Relatable(f, v, w) => {
					// Fetch the data from the store
					let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
					let val = txn.clone().lock().await.get(key).await?;
					// Parse the data from the store
					let x = match val {
						Some(v) => Value::from(v),
						None => Value::None,
					};
					// Create a new operable value
					let val = Operable::Relatable(f, x, w);
					// Process the document record
					ite.process(ctx, opt, txn, stm, Some(v), val).await;
				}
			}
		}
		Ok(())
	}
}
