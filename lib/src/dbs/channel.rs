use crate::cnf::ID_CHARS;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::key::thing;
use crate::sql::array::Array;
use crate::sql::model::Model;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use async_recursion::async_recursion;
use nanoid::nanoid;
use tokio::sync::mpsc::Sender;

impl Value {
	pub async fn channel(
		self,
		ctx: Runtime,
		opt: Options,
		txn: Transaction,
		chn: Sender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match self {
				Value::Array(v) => v.process(&ctx, &opt, &txn, &chn).await?,
				Value::Model(v) => v.process(&ctx, &opt, &txn, &chn).await?,
				Value::Thing(v) => v.process(&ctx, &opt, &txn, &chn).await?,
				Value::Table(v) => v.process(&ctx, &opt, &txn, &chn).await?,
				v => chn.send((None, v)).await?,
			}
		}
		Ok(())
	}
}

impl Array {
	#[async_recursion]
	pub async fn process(
		self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		chn: &Sender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		for v in self.value.into_iter() {
			if ctx.is_ok() {
				match v {
					Value::Array(v) => v.process(ctx, opt, txn, chn).await?,
					Value::Model(v) => v.process(ctx, opt, txn, chn).await?,
					Value::Thing(v) => v.process(ctx, opt, txn, chn).await?,
					Value::Table(v) => v.process(ctx, opt, txn, chn).await?,
					v => chn.send((None, v)).await?,
				}
			}
		}
		Ok(())
	}
}

impl Model {
	pub async fn process(
		self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		chn: &Sender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			if let Some(c) = self.count {
				for _ in 0..c {
					Thing {
						tb: self.table.to_string(),
						id: nanoid!(20, &ID_CHARS),
					}
					.process(ctx, opt, txn, chn)
					.await?;
				}
			}
			if let Some(r) = self.range {
				for x in r.0..=r.1 {
					Thing {
						tb: self.table.to_string(),
						id: x.to_string(),
					}
					.process(ctx, opt, txn, chn)
					.await?;
				}
			}
		}
		Ok(())
	}
}

impl Thing {
	pub async fn process(
		self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		chn: &Sender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			let key = thing::new(opt.ns(), opt.db(), &self.tb, &self.id);
			let val = txn.clone().lock().await.get(key).await?;
			let val = match val {
				Some(v) => Value::from(v),
				None => Value::None,
			};
			chn.send((Some(self), val)).await?;
		}
		Ok(())
	}
}

impl Table {
	pub async fn process(
		self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		chn: &Sender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			let beg = thing::prefix(opt.ns(), opt.db(), &self.name);
			let end = thing::suffix(opt.ns(), opt.db(), &self.name);
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
						let n = res.len() - 1;
						// Loop over results
						for (i, (k, v)) in res.into_iter().enumerate() {
							if ctx.is_ok() {
								// Ready the next
								if i == n {
									nxt = Some(k.clone());
								}
								// Parse the key-value
								let k: crate::key::thing::Thing = (&k).into();
								let v: crate::sql::value::Value = (&v).into();
								let t = Thing::from((k.tb, k.id));
								// Process the record
								chn.send((Some(t), v)).await?;
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
