use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::dbs::Workable;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::idx::docids::DocId;
use crate::idx::planner::executor::IteratorRef;
use crate::sql::statements::define::DefineEventStatement;
use crate::sql::statements::define::DefineFieldStatement;
use crate::sql::statements::define::DefineIndexStatement;
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::statements::live::LiveStatement;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::Base;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub(crate) struct Document<'a> {
	pub(super) id: Option<&'a Thing>,
	pub(super) extras: Workable,
	pub(super) initial: CursorDoc<'a>,
	pub(super) current: CursorDoc<'a>,
}

#[non_exhaustive]
pub struct CursorDoc<'a> {
	pub(crate) ir: Option<IteratorRef>,
	pub(crate) rid: Option<&'a Thing>,
	pub(crate) doc: Cow<'a, Value>,
	pub(crate) doc_id: Option<DocId>,
}

impl<'a> CursorDoc<'a> {
	pub(crate) fn new(
		ir: Option<IteratorRef>,
		rid: Option<&'a Thing>,
		doc_id: Option<DocId>,
		doc: Cow<'a, Value>,
	) -> Self {
		Self {
			ir,
			rid,
			doc,
			doc_id,
		}
	}
}

impl<'a> From<&'a Value> for CursorDoc<'a> {
	fn from(doc: &'a Value) -> Self {
		Self {
			ir: None,
			rid: None,
			doc: Cow::Borrowed(doc),
			doc_id: None,
		}
	}
}

impl<'a> From<&'a mut Value> for CursorDoc<'a> {
	fn from(doc: &'a mut Value) -> Self {
		Self {
			ir: None,
			rid: None,
			doc: Cow::Borrowed(doc),
			doc_id: None,
		}
	}
}

impl<'a> Debug for Document<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "Document - id: <{:?}>", self.id)
	}
}

impl<'a> From<&Document<'a>> for Vec<u8> {
	fn from(val: &Document<'a>) -> Vec<u8> {
		val.current.doc.as_ref().into()
	}
}

impl<'a> Document<'a> {
	pub fn new(
		ir: Option<IteratorRef>,
		id: Option<&'a Thing>,
		doc_id: Option<DocId>,
		val: &'a Value,
		extras: Workable,
	) -> Self {
		Document {
			id,
			extras,
			current: CursorDoc::new(ir, id, doc_id, Cow::Borrowed(val)),
			initial: CursorDoc::new(ir, id, doc_id, Cow::Borrowed(val)),
		}
	}

	/// Create a new document that is not going through the standard lifecycle of documents
	///
	/// This allows for it to be crafted without needing statements to operate on it
	#[doc(hidden)]
	pub fn new_artificial(
		ir: Option<IteratorRef>,
		id: Option<&'a Thing>,
		doc_id: Option<DocId>,
		val: Cow<'a, Value>,
		initial: Cow<'a, Value>,
		extras: Workable,
	) -> Self {
		Document {
			id,
			extras,
			current: CursorDoc::new(ir, id, doc_id, val),
			initial: CursorDoc::new(ir, id, doc_id, initial),
		}
	}
}

impl<'a> Document<'a> {
	/// Check if document has changed
	pub fn changed(&self) -> bool {
		self.initial.doc != self.current.doc
	}
	/// Check if document has changed
	pub fn is_new(&self) -> bool {
		self.initial.doc.is_none()
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
			Err(Error::TbNotFound {
				value: _,
			}) => {
				// Allowed to run?
				opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
				// We can create the table automatically
				run.add_and_cache_ns(opt.ns(), opt.strict).await?;
				run.add_and_cache_db(opt.ns(), opt.db(), opt.strict).await?;
				run.add_and_cache_tb(opt.ns(), opt.db(), &rid.tb, opt.strict).await
			}
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
		txn.clone().lock().await.all_tb_views(opt.ns(), opt.db(), &id.tb).await
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
		txn.clone().lock().await.all_tb_events(opt.ns(), opt.db(), &id.tb).await
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
		txn.clone().lock().await.all_tb_fields(opt.ns(), opt.db(), &id.tb).await
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
		txn.clone().lock().await.all_tb_indexes(opt.ns(), opt.db(), &id.tb).await
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
		txn.clone().lock().await.all_tb_lives(opt.ns(), opt.db(), &id.tb).await
	}
}
