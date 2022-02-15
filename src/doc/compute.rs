use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn compute(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction<'_>,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		match stm {
			Statement::Select(_) => self.select(ctx, opt, txn, stm).await,
			Statement::Create(_) => self.create(ctx, opt, txn, stm).await,
			Statement::Update(_) => self.update(ctx, opt, txn, stm).await,
			Statement::Relate(_) => self.relate(ctx, opt, txn, stm).await,
			Statement::Delete(_) => self.delete(ctx, opt, txn, stm).await,
			Statement::Insert(_) => self.insert(ctx, opt, txn, stm).await,
			_ => unreachable!(),
		}
	}
}
