use crate::ctx::Context;
use crate::ctx::MutableContext;
use crate::dbs::Options;
use crate::dbs::Workable;
use crate::err::Error;
use crate::iam::Action;
use crate::iam::ResourceKind;
use crate::idx::planner::iterators::IteratorRecord;
use crate::sql::permission::Permission;
use crate::sql::statements::define::DefineEventStatement;
use crate::sql::statements::define::DefineFieldStatement;
use crate::sql::statements::define::DefineIndexStatement;
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::statements::live::LiveStatement;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::Base;
use reblessive::tree::Stk;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) struct Document {
	pub(super) id: Option<Arc<Thing>>,
	pub(super) extras: Workable,
	pub(super) initial: CursorDoc,
	pub(super) current: CursorDoc,
	pub(super) initial_reduced: CursorDoc,
	pub(super) current_reduced: CursorDoc,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct CursorDoc {
	pub(crate) rid: Option<Arc<Thing>>,
	pub(crate) ir: Option<Arc<IteratorRecord>>,
	pub(crate) doc: CursorValue,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
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

	pub(crate) fn into_owned(self) -> Value {
		if let Some(ro) = &self.read_only {
			ro.as_ref().clone()
		} else {
			self.mutable
		}
	}
}

impl Deref for CursorValue {
	type Target = Value;
	fn deref(&self) -> &Self::Target {
		self.as_ref()
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

pub(crate) enum Permitted {
	Initial,
	Current,
	Both,
}

impl Document {
	/// Initialise a new document
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
			initial: CursorDoc::new(id.clone(), ir.clone(), val.clone()),
			current_reduced: CursorDoc::new(id.clone(), ir.clone(), val.clone()),
			initial_reduced: CursorDoc::new(id.clone(), ir.clone(), val.clone()),
		}
	}
	/// Check if document has changed
	pub fn changed(&self) -> bool {
		self.initial.doc.as_ref() != self.current.doc.as_ref()
	}

	/// Check if document is being created
	pub fn is_new(&self) -> bool {
		self.initial.doc.as_ref().is_none()
	}

	/// Checks if permissions are required to be run
	/// over a document. If permissions don't need to
	/// be processed, then we don't process the initial
	/// or current documents, and instead return
	/// `false`. If permissions need to be processed,
	/// then we take the initial or current documents,
	/// and remove those fields which the user is not
	/// allowed to view. We then use the `initial_reduced`
	/// and `current_reduced` documents in the code when
	/// processing the document that a user has access to.
	///
	/// The choice of which documents are reduced can be
	/// specified by passing in a `Permitted` type, allowing
	/// either `initial`, `current`, or `both` to be
	/// processed in a single function execution.
	///
	/// This function is used both to reduce documents
	/// to only the fields that are permitted by updating
	/// the reduced fields of the Document structure as
	/// well as to return whether or not they have been
	/// reduced so that these reduced documents are used
	/// instead of their non-reduced versions.
	///
	/// If there is no requirement to reduce a document
	/// based on the permissions, then this function will
	/// not have any performance impact by cloning the
	/// full and reduced documents.
	pub(crate) async fn reduced(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		permitted: Permitted,
	) -> Result<bool, Error> {
		// Check if this record exists
		if self.id.is_none() {
			return Ok(false);
		}
		// Are permissions being skipped?
		if !opt.check_perms(Action::View)? {
			return Ok(false);
		}
		// Fetch the fields for the table
		let fds = self.fd(ctx, opt).await?;
		// Fetch the targets to process
		let targets = match permitted {
			Permitted::Initial => vec![(&self.initial, &mut self.initial_reduced)],
			Permitted::Current => vec![(&self.current, &mut self.current_reduced)],
			Permitted::Both => vec![
				(&self.initial, &mut self.initial_reduced),
				(&self.current, &mut self.current_reduced),
			],
		};
		// Loop over the targets to process
		for target in targets {
			// Get the full document
			let full = target.0;
			// Process the full document
			let mut out = full.doc.as_ref().compute(stk, ctx, opt, Some(full)).await?;
			// Loop over each field in document
			for fd in fds.iter() {
				// Loop over each field in document
				for k in out.each(&fd.name).iter() {
					// Process the field permissions
					match &fd.permissions.select {
						Permission::Full => (),
						Permission::None => out.cut(k),
						Permission::Specific(e) => {
							// Disable permissions
							let opt = &opt.new_with_perms(false);
							// Get the initial value
							let val = Arc::new(full.doc.as_ref().pick(k));
							// Configure the context
							let mut ctx = MutableContext::new(ctx);
							ctx.add_value("value", val);
							let ctx = ctx.freeze();
							// Process the PERMISSION clause
							if !e.compute(stk, &ctx, opt, Some(full)).await?.is_truthy() {
								out.cut(k);
							}
						}
					}
				}
			}
			// Update the permitted document
			target.1.doc = out.into();
		}
		// Return the permitted document
		Ok(true)
	}

	/// Retrieve the record id for this document
	pub fn id(&self) -> Result<Arc<Thing>, Error> {
		match self.id.as_ref() {
			Some(id) => Ok(id.clone()),
			_ => Err(fail!("Expected a document id to be present")),
		}
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
		let id = self.id()?;
		// Get the table definition
		let tb = txn.get_tb(opt.ns()?, opt.db()?, &id.tb).await;
		// Return the table or attempt to define it
		match tb {
			// The table doesn't exist
			Err(Error::TbNotFound {
				value: _,
			}) => {
				// Allowed to run?
				opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
				// We can create the table automatically
				txn.ensure_ns_db_tb(opt.ns()?, opt.db()?, &id.tb, opt.strict).await
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
		let id = self.id()?;
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
		let id = self.id()?;
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
		let id = self.id()?;
		// Get the field definitions
		ctx.tx().all_tb_fields(opt.ns()?, opt.db()?, &id.tb, None).await
	}
	/// Get the indexes for this document
	pub async fn ix(
		&self,
		ctx: &Context,
		opt: &Options,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		// Get the record id
		let id = self.id()?;
		// Get the index definitions
		ctx.tx().all_tb_indexes(opt.ns()?, opt.db()?, &id.tb).await
	}
	// Get the lives for this document
	pub async fn lv(&self, ctx: &Context, opt: &Options) -> Result<Arc<[LiveStatement]>, Error> {
		// Get the record id
		let id = self.id()?;
		// Get the table definition
		ctx.tx().all_tb_lives(opt.ns()?, opt.db()?, &id.tb).await
	}
}
