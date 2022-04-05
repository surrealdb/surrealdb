use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use std::borrow::Cow;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Document<'a> {
	pub(super) id: Option<Thing>,
	pub(super) current: Cow<'a, Value>,
	pub(super) initial: Cow<'a, Value>,
}

impl<'a> From<&Document<'a>> for Vec<u8> {
	fn from(val: &Document<'a>) -> Vec<u8> {
		msgpack::to_vec(&val.current).unwrap()
	}
}

impl<'a> Document<'a> {
	pub fn new(id: Option<Thing>, val: &'a Value) -> Self {
		Document {
			id,
			current: Cow::Borrowed(val),
			initial: Cow::Borrowed(val),
		}
	}
}

impl<'a> Document<'a> {
	// Check if document has changed
	pub fn changed(&self) -> bool {
		self.initial != self.current
	}
	// Get the table for this document
	pub async fn tb(
		&self,
		_ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
	) -> Result<DefineTableStatement, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the table definition
		txn.clone().lock().await.get_tb(opt.ns(), opt.db(), &id.tb).await
	}
}
