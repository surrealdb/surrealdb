use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::dbs::Workable;
use crate::err::Error;
use crate::sql::statements::define::DefineEventStatement;
use crate::sql::statements::define::DefineFieldStatement;
use crate::sql::statements::define::DefineIndexStatement;
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::statements::live::LiveStatement;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub(crate) struct Document<'a> {
	pub(super) id: Option<Thing>,
	pub(super) extras: Workable,
	pub(super) current: Cow<'a, Value>,
	pub(super) initial: Cow<'a, Value>,
}

impl<'a> Debug for Document<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "Document - id: <{:?}>", self.id)
	}
}

impl<'a> From<&Document<'a>> for Vec<u8> {
	fn from(val: &Document<'a>) -> Vec<u8> {
		val.current.as_ref().into()
	}
}

impl<'a> Document<'a> {
	pub fn new(id: Option<Thing>, val: &'a Value, ext: Workable) -> Self {
		Document {
			id,
			extras: ext,
			current: Cow::Borrowed(val),
			initial: Cow::Borrowed(val),
		}
	}
}

impl<'a> Document<'a> {
	/// Check if document has changed
	pub fn changed(&self) -> bool {
		self.initial != self.current
	}
	/// Check if document has changed
	pub fn is_new(&self) -> bool {
		self.initial.is_none()
	}
	/// Get the table for this document
	pub async fn tb(
		&self,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Arc<DefineTableStatement>, Error> {
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Get the table definition
		let tb = run.get_and_cache_tb(opt.ns(), opt.db(), &rid.tb).await;
		// Return the table or attempt to define it
		match tb {
			// The table doesn't exist
			Err(Error::TbNotFound) => match opt.auth.check(Level::Db) {
				// We can create the table automatically
				true => {
					run.add_and_cache_ns(opt.ns(), opt.strict).await?;
					run.add_and_cache_db(opt.ns(), opt.db(), opt.strict).await?;
					run.add_and_cache_tb(opt.ns(), opt.db(), &rid.tb, opt.strict).await
				}
				// We can't create the table so error
				false => Err(Error::QueryPermissions),
			},
			// There was an error
			Err(err) => Err(err),
			// The table exists
			Ok(tb) => Ok(tb),
		}
	}
	/// Get the foreign tables for this document
	pub async fn ft(
		&self,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the table definitions
		txn.clone().lock().await.all_ft(opt.ns(), opt.db(), &id.tb).await
	}
	/// Get the events for this document
	pub async fn ev(
		&self,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Arc<[DefineEventStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the event definitions
		txn.clone().lock().await.all_ev(opt.ns(), opt.db(), &id.tb).await
	}
	/// Get the fields for this document
	pub async fn fd(
		&self,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Arc<[DefineFieldStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the field definitions
		txn.clone().lock().await.all_fd(opt.ns(), opt.db(), &id.tb).await
	}
	/// Get the indexes for this document
	pub async fn ix(
		&self,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the index definitions
		txn.clone().lock().await.all_ix(opt.ns(), opt.db(), &id.tb).await
	}
	// Get the lives for this document
	pub async fn lv(
		&self,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Arc<[LiveStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the table definition
		txn.clone().lock().await.all_lv(opt.ns(), opt.db(), &id.tb).await
	}
}
