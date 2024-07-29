use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Workable;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::idx::planner::iterators::IteratorRecord;
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
	pub(super) id: Option<Box<Thing>>,
	pub(super) extras: Workable,
	pub(super) initial: CursorDoc<'a>,
	pub(super) current: CursorDoc<'a>,
}

#[non_exhaustive]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct CursorDoc<'a> {
	pub(crate) rid: Option<&'a Thing>,
	pub(crate) ir: Option<&'a IteratorRecord>,
	pub(crate) doc: Cow<'a, Value>,
}

impl<'a> CursorDoc<'a> {
	pub(crate) fn new(
		rid: Option<&'a Thing>,
		ir: Option<&'a IteratorRecord>,
		doc: Cow<'a, Value>,
	) -> Self {
		Self {
			rid,
			ir,
			doc,
		}
	}
}

impl<'a> From<&'a Value> for CursorDoc<'a> {
	fn from(doc: &'a Value) -> Self {
		Self {
			rid: None,
			ir: None,
			doc: Cow::Borrowed(doc),
		}
	}
}

impl<'a> From<&'a mut Value> for CursorDoc<'a> {
	fn from(doc: &'a mut Value) -> Self {
		Self {
			rid: None,
			ir: None,
			doc: Cow::Borrowed(doc),
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
		id: Option<&'a Thing>,
		ir: Option<&'a IteratorRecord>,
		val: &'a Value,
		extras: Workable,
	) -> Self {
		Document {
			id: match id {
				Some(id) => Some(Box::new(id.clone())),
				None => None,
			},
			extras,
			current: CursorDoc::new(id, ir, Cow::Borrowed(val)),
			initial: CursorDoc::new(id, ir, Cow::Borrowed(val)),
		}
	}

	/// Get the current document, as it is being modified
	#[allow(unused)]
	pub(crate) fn current_doc(&self) -> &Value {
		self.current.doc.as_ref()
	}

	/// Get the initial version of the document before it is modified
	#[allow(unused)]
	pub(crate) fn initial_doc(&self) -> &Value {
		self.initial.doc.as_ref()
	}
}

impl<'a> Document<'a> {
	/// Check if document has changed
	pub fn changed(&self) -> bool {
		self.initial.doc != self.current.doc
	}

	/// Check if document is being created
	pub fn is_new(&self) -> bool {
		self.initial.doc.is_none() && self.current.doc.is_some()
	}

	/// Get the table for this document
	pub async fn tb(
		&self,
		ctx: &Context<'a>,
		opt: &Options,
	) -> Result<Arc<DefineTableStatement>, Error> {
		// Get transaction
		let txn = ctx.tx();
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Get the table definition
		let tb = txn.get_tb(opt.ns()?, opt.db()?, &rid.tb).await;
		// Return the table or attempt to define it
		match tb {
			// The table doesn't exist
			Err(Error::TbNotFound {
				value: _,
			}) => {
				// Allowed to run?
				opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
				// We can create the table automatically
				txn.ensure_ns_db_tb(opt.ns()?, opt.db()?, &rid.tb, opt.strict).await
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
		ctx: &Context<'_>,
		opt: &Options,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the table definitions
		ctx.tx().all_tb_views(opt.ns()?, opt.db()?, &id.tb).await
	}
	/// Get the events for this document
	pub async fn ev(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
	) -> Result<Arc<[DefineEventStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the event definitions
		ctx.tx().all_tb_events(opt.ns()?, opt.db()?, &id.tb).await
	}
	/// Get the fields for this document
	pub async fn fd(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
	) -> Result<Arc<[DefineFieldStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the field definitions
		ctx.tx().all_tb_fields(opt.ns()?, opt.db()?, &id.tb).await
	}
	/// Get the indexes for this document
	pub async fn ix(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the index definitions
		ctx.tx().all_tb_indexes(opt.ns()?, opt.db()?, &id.tb).await
	}
	// Get the lives for this document
	pub async fn lv(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
	) -> Result<Arc<[LiveStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the table definition
		ctx.tx().all_tb_lives(opt.ns()?, opt.db()?, &id.tb).await
	}
}
