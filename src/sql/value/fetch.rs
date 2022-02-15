use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::sql::idiom::Idiom;
use crate::sql::value::Value;

impl Value {
	pub fn fetch(
		self,
		_ctx: &Runtime,
		_opt: &Options,
		_txn: &Transaction,
		_path: &Idiom,
	) -> Self {
		self
	}
}
