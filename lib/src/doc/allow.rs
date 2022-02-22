use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn allow(
		&self,
		_ctx: &Runtime,
		_opt: &Options,
		_txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		Ok(())
	}
}
