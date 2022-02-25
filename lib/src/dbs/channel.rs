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
use tokio::sync::mpsc::UnboundedSender;

impl Value {
	pub async fn channel(
		self,
		ctx: Runtime,
		opt: Options,
		txn: Transaction,
		chn: UnboundedSender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match self {
				Value::Array(v) => v.process(&ctx, &opt, &txn, &chn).await?,
				Value::Model(v) => v.process(&ctx, &opt, &txn, &chn).await?,
				Value::Thing(v) => v.process(&ctx, &opt, &txn, &chn).await?,
				Value::Table(v) => v.process(&ctx, &opt, &txn, &chn).await?,
				v => chn.send((None, v))?,
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
		chn: &UnboundedSender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		for v in self.value.into_iter() {
			if ctx.is_ok() {
				match v {
					Value::Array(v) => v.process(ctx, opt, txn, chn).await?,
					Value::Model(v) => v.process(ctx, opt, txn, chn).await?,
					Value::Thing(v) => v.process(ctx, opt, txn, chn).await?,
					Value::Table(v) => v.process(ctx, opt, txn, chn).await?,
					v => chn.send((None, v))?,
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
		chn: &UnboundedSender<(Option<Thing>, Value)>,
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
				for x in r.0..r.1 {
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
		chn: &UnboundedSender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		Ok(())
	}
}

impl Table {
	pub async fn process(
		self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		chn: &UnboundedSender<(Option<Thing>, Value)>,
	) -> Result<(), Error> {
		Ok(())
	}
}
