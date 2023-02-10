use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::dbs::LOG;
use crate::doc::Document;
use crate::err::Error;
use std::backtrace::Backtrace;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		let bt = Backtrace::capture();
		info!(target: LOG, "Just entered the live query updates part, ctx={_ctx:?}   opt={_opt:?}   txn={_txn:?}   stm={_stm:?}");
		println!("{bt}");
		for lv in self.lv(_opt, _txn).await?.iter() {
			info!(target: LOG, "Found live query to process {}", lv);
		}
		// TODO this is where the magic happens
		// get lives same as tables.rs, print them out
		Ok(())
	}
}
