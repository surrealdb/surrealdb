use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn erase(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction<'_>,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		self.current.to_mut().clear(ctx, opt, txn).await
	}
}
