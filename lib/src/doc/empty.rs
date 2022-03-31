use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn empty(
		&self,
		_ctx: &Runtime,
		_opt: &Options,
		_txn: &Transaction,
		_stm: &Statement,
	) -> Result<(), Error> {
		// Check if this record exists
		if self.id.is_some() {
			if self.current.is_none() {
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
}
