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
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::Arc;

pub(crate) struct Document {
	pub(super) id: Option<Arc<Thing>>,
	pub(super) extras: Workable,
	pub(super) initial: CursorDoc,
	pub(super) current: CursorDoc,
}

#[non_exhaustive]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct CursorDoc {
	pub(crate) rid: Option<Arc<Thing>>,
	pub(crate) ir: Option<Arc<IteratorRecord>>,
	pub(crate) doc: CursorValue,
}

#[non_exhaustive]
#[cfg_attr(debug_assertions, derive(Debug))]
pub(crate) struct CursorValue {
	mutable: Value,
	read_only: Option<Arc<Value>>,
}

impl CursorValue {
	pub(crate) fn to_mut(&mut self) -> &mut Value {
		if let Some(ro) = self.read_only.take() {
			self.mutable = ro.as_ref().clone();
		}
		&mut self.mutable
	}

	pub(crate) fn as_arc(&mut self) -> Arc<Value> {
		match &self.read_only {
			None => {
				let v = Arc::new(mem::take(&mut self.mutable));
				self.read_only = Some(v.clone());
				v
			}
			Some(v) => v.clone(),
		}
	}

	pub(crate) fn as_ref(&self) -> &Value {
		if let Some(ro) = &self.read_only {
			ro.as_ref()
		} else {
			&self.mutable
		}
	}
}

impl CursorDoc {
	pub(crate) fn new<T: Into<CursorValue>>(
		rid: Option<Arc<Thing>>,
		ir: Option<Arc<IteratorRecord>>,
		doc: T,
	) -> Self {
		Self {
			rid,
			ir,
			doc: doc.into(),
		}
	}
}

impl From<Value> for CursorValue {
	fn from(value: Value) -> Self {
		Self {
			mutable: value,
			read_only: None,
		}
	}
}

impl From<Arc<Value>> for CursorValue {
	fn from(value: Arc<Value>) -> Self {
		Self {
			mutable: Value::None,
			read_only: Some(value),
		}
	}
}

impl From<Value> for CursorDoc {
	fn from(val: Value) -> Self {
		Self {
			rid: None,
			ir: None,
			doc: CursorValue {
				mutable: val,
				read_only: None,
			},
		}
	}
}

impl From<Arc<Value>> for CursorDoc {
	fn from(doc: Arc<Value>) -> Self {
		Self {
			rid: None,
			ir: None,
			doc: CursorValue {
				mutable: Value::None,
				read_only: Some(doc),
			},
		}
	}
}

impl Debug for Document {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "Document - id: <{:?}>", self.id)
	}
}

impl From<&Document> for Vec<u8> {
	fn from(val: &Document) -> Vec<u8> {
		val.current.doc.as_ref().into()
	}
}

impl Document {
	pub fn new(
		id: Option<Arc<Thing>>,
		ir: Option<Arc<IteratorRecord>>,
		val: Arc<Value>,
		extras: Workable,
	) -> Self {
		Document {
			id: id.clone(),
			extras,
			current: CursorDoc::new(id.clone(), ir.clone(), val.clone()),
			initial: CursorDoc::new(id, ir, val),
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

impl Document {
	/// Check if document has changed
	pub fn changed(&self) -> bool {
		self.initial.doc.as_ref() != self.current.doc.as_ref()
	}

	/// Check if document is being created
	pub fn is_new(&self) -> bool {
		self.initial.doc.as_ref().is_none() && self.current.doc.as_ref().is_some()
	}

	/// Get the table for this document
	pub async fn tb(
		&self,
		ctx: &Context,
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
		ctx: &Context,
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
		ctx: &Context,
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
		ctx: &Context,
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
		ctx: &Context,
		opt: &Options,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the index definitions
		ctx.tx().all_tb_indexes(opt.ns()?, opt.db()?, &id.tb).await
	}
	// Get the lives for this document
	pub async fn lv(&self, ctx: &Context, opt: &Options) -> Result<Arc<[LiveStatement]>, Error> {
		// Get the record id
		let id = self.id.as_ref().unwrap();
		// Get the table definition
		ctx.tx().all_tb_lives(opt.ns()?, opt.db()?, &id.tb).await
	}
}
