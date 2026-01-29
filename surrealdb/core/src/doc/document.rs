use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::{CatalogProvider, TableProvider};
use crate::catalog::{
	self, Data, DatabaseDefinition, FieldDefinition, NamespaceDefinition, Permission, Record,
	TableDefinition,
};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Workable};
use crate::doc::alter::ComputedData;
use crate::expr::FlowResultExt as _;
use crate::iam::{Action, AuthLimit};
use crate::idx::planner::RecordStrategy;
use crate::idx::planner::iterators::IteratorRecord;
use crate::kvs::cache;
use crate::val::{RecordId, TableName, Value};

pub(crate) struct Document {
	/// The document context for this document
	pub(crate) doc_ctx: DocumentContext,
	/// The record id of this document
	pub(super) id: Option<Arc<RecordId>>,
	/// The table that we should generate a record id from
	pub(super) r#gen: Option<TableName>,
	/// Whether this is the second iteration of the processing
	pub(super) retry: bool,
	pub(super) extras: Workable,
	pub(super) initial: CursorDoc,
	pub(super) current: CursorDoc,
	pub(super) initial_reduced: CursorDoc,
	pub(super) current_reduced: CursorDoc,
	pub(super) record_strategy: RecordStrategy,
	pub(super) input_data: Option<ComputedData>,
}

#[derive(Clone, Debug)]
pub(crate) struct NsDbCtx {
	pub(crate) ns: Arc<NamespaceDefinition>,
	pub(crate) db: Arc<DatabaseDefinition>,
}

#[derive(Clone, Debug)]
pub(crate) struct NsDbTbCtx {
	pub(crate) ns: Arc<NamespaceDefinition>,
	pub(crate) db: Arc<DatabaseDefinition>,
	pub(crate) tb: Arc<TableDefinition>,
	pub(crate) fields: Arc<[FieldDefinition]>,
}

#[derive(Clone, Debug)]
pub(crate) enum DocumentContext {
	NsDbCtx(NsDbCtx),
	NsDbTbCtx(NsDbTbCtx),
}

impl DocumentContext {
	pub(crate) fn ns(&self) -> &Arc<NamespaceDefinition> {
		match self {
			DocumentContext::NsDbCtx(ctx) => &ctx.ns,
			DocumentContext::NsDbTbCtx(ctx) => &ctx.ns,
		}
	}

	pub(crate) fn db(&self) -> &Arc<DatabaseDefinition> {
		match self {
			DocumentContext::NsDbCtx(ctx) => &ctx.db,
			DocumentContext::NsDbTbCtx(ctx) => &ctx.db,
		}
	}

	pub(crate) fn tb(&self) -> Result<&Arc<TableDefinition>> {
		match self {
			DocumentContext::NsDbCtx(_) => Err(anyhow::anyhow!(
				"Table not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
			DocumentContext::NsDbTbCtx(ctx) => Ok(&ctx.tb),
		}
	}

	pub(crate) fn fd(&self) -> Result<&Arc<[FieldDefinition]>> {
		match self {
			DocumentContext::NsDbCtx(_) => Err(anyhow::anyhow!(
				"Fields not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
			DocumentContext::NsDbTbCtx(ctx) => Ok(&ctx.fields),
		}
	}
}

#[derive(Clone, Debug)]
pub(crate) struct CursorDoc {
	pub(crate) rid: Option<Arc<RecordId>>,
	pub(crate) ir: Option<Arc<IteratorRecord>>,
	pub(crate) doc: CursorRecord,
	pub(crate) fields_computed: bool,
}

impl CursorDoc {
	/// Updates the `"parent"` doc field for statements with a meaning full
	/// document.
	pub async fn update_parent<F, R>(ctx: &FrozenContext, doc: Option<&CursorDoc>, f: F) -> R
	where
		F: AsyncFnOnce(Cow<FrozenContext>) -> R,
	{
		let ctx = if let Some(doc) = doc {
			let mut new_ctx = Context::new(ctx);
			new_ctx.add_value("parent", doc.doc.as_ref().clone().into());
			Cow::Owned(new_ctx.freeze())
		} else {
			Cow::Borrowed(ctx)
		};

		f(ctx).await
	}
}

/// Wrapper around a Record for cursor operations
///
/// This struct provides a convenient interface for working with records in cursor contexts.
/// It implements Deref and DerefMut to allow direct access to the underlying Record's methods.
#[derive(Clone, Debug)]
pub(crate) struct CursorRecord {
	/// The underlying record containing data and metadata
	record: Record,
}

impl CursorRecord {
	/// Returns a mutable reference to the underlying value
	///
	/// This method delegates to the Record's data, converting read-only data to mutable if
	/// necessary.
	pub(crate) fn to_mut(&mut self) -> &mut Value {
		self.record.data.to_mut()
	}

	/// Converts the data to read-only format and returns an Arc reference
	///
	/// This method delegates to the Record's data, ensuring the data is in read-only format.
	pub(crate) fn as_arc(&mut self) -> Arc<Value> {
		self.record.data.read_only()
	}

	/// Converts the cursor record to a read-only record
	///
	/// This method ensures the underlying data is in read-only format for better sharing.
	pub(crate) fn into_read_only(self) -> Arc<Record> {
		self.record.into_read_only()
	}

	/// Returns a reference to the underlying value
	///
	/// This method provides uniform access to the value regardless of its storage format.
	pub(crate) fn as_ref(&self) -> &Value {
		self.record.data.as_ref()
	}

	/// Converts the cursor record to an owned Value
	///
	/// This method extracts the underlying value, taking ownership of the data.
	pub(crate) fn into_owned(mut self) -> Value {
		match self.record.data {
			Data::ReadOnly(ref mut arc) => mem::take(Arc::make_mut(arc)),
			Data::Mutable(value) => value,
		}
	}
}

impl Deref for CursorRecord {
	type Target = Record;
	fn deref(&self) -> &Self::Target {
		&self.record
	}
}

impl DerefMut for CursorRecord {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.record
	}
}

impl CursorDoc {
	pub(crate) fn new<T: Into<CursorRecord>>(
		rid: Option<Arc<RecordId>>,
		ir: Option<Arc<IteratorRecord>>,
		doc: T,
	) -> Self {
		Self {
			rid,
			ir,
			doc: doc.into(),
			fields_computed: false,
		}
	}
}

impl From<Record> for CursorRecord {
	fn from(record: Record) -> Self {
		Self {
			record,
		}
	}
}

impl From<Arc<Record>> for CursorRecord {
	fn from(arc: Arc<Record>) -> Self {
		Self {
			record: arc.as_ref().clone(),
		}
	}
}

impl From<Value> for CursorRecord {
	fn from(value: Value) -> Self {
		Self {
			record: Record::new(value.into()),
		}
	}
}

impl From<Arc<Value>> for CursorRecord {
	fn from(arc: Arc<Value>) -> Self {
		Self {
			record: Record::new(arc.into()),
		}
	}
}

impl From<Value> for CursorDoc {
	fn from(val: Value) -> Self {
		Self {
			rid: None,
			ir: None,
			doc: val.into(),
			fields_computed: false,
		}
	}
}

impl From<Arc<Value>> for CursorDoc {
	fn from(doc: Arc<Value>) -> Self {
		Self {
			rid: None,
			ir: None,
			doc: doc.into(),
			fields_computed: false,
		}
	}
}

impl Debug for Document {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "Document - id: <{:?}>", self.id)
	}
}

pub(crate) enum Permitted {
	Initial,
	Current,
	Both,
}

impl Document {
	/// Initialise a new document
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		doc_ctx: DocumentContext,
		id: Option<Arc<RecordId>>,
		ir: Option<Arc<IteratorRecord>>,
		r#gen: Option<TableName>,
		val: Arc<Record>,
		extras: Workable,
		retry: bool,
		rs: RecordStrategy,
	) -> Self {
		Document {
			doc_ctx,
			id: id.clone(),
			r#gen,
			retry,
			extras,
			current: CursorDoc::new(id.clone(), ir.clone(), val.clone()),
			initial: CursorDoc::new(id.clone(), ir.clone(), val.clone()),
			current_reduced: CursorDoc::new(id.clone(), ir.clone(), val.clone()),
			initial_reduced: CursorDoc::new(id, ir, val),
			record_strategy: rs,
			input_data: None,
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

	/// Check if this is the first iteration. When
	/// running an UPSERT or INSERT statement we don't
	/// first fetch the value from the storage engine.
	/// If there is an error when attempting to set the
	/// value in the storage engine, then we retry the
	/// document processing, and this will return false.
	pub(crate) fn is_iteration_initial(&self) -> bool {
		!self.retry && self.initial.doc.as_ref().is_none()
	}

	/// Check if the record id for this document
	/// has been specifically set upfront. This is true
	/// in the following instances:
	///
	/// CREATE some:thing;
	/// CREATE some SET id = some:thing;
	/// CREATE some CONTENT { id: some:thing };
	/// UPSERT some:thing;
	/// UPSERT some SET id = some:thing;
	/// UPSERT some CONTENT { id: some:thing };
	/// INSERT some (id) VALUES (some:thing);
	/// INSERT { id: some:thing };
	/// INSERT [{ id: some:thing }];
	/// RELATE from->some:thing->to;
	/// RELATE from->some->to SET id = some:thing;
	/// RELATE from->some->to CONTENT { id: some:thing };
	///
	/// In addition, when iterating over tables or ranges
	/// the record id will also be specified before we
	/// process the document in this module. So therefore
	/// although this function is not used or checked in
	/// these scenarios, this function will also be true
	/// in the following instances:
	///
	/// UPDATE some;
	/// UPDATE some:thing;
	/// UPDATE some:from..to;
	/// DELETE some;
	/// DELETE some:thing;
	/// DELETE some:from..to;
	pub(crate) fn is_specific_record_id(&self) -> bool {
		match self.extras {
			Workable::Insert(ref v) => !v.rid().is_nullish(),
			Workable::Normal => self.r#gen.is_none(),
			_ => false,
		}
	}

	/// Return true if the document has been extracted by an iterator that already matched the
	/// condition.
	pub(crate) fn is_condition_checked(&self) -> bool {
		matches!(self.record_strategy, RecordStrategy::Count | RecordStrategy::KeysOnly)
	}

	/// Update the document for a retry to update after an insert failed.
	pub fn modify_for_update_retry(&mut self, id: RecordId, record: Arc<Record>) {
		let retry = Arc::new(id);
		self.id = Some(retry.clone());
		self.r#gen = None;
		self.retry = true;
		self.record_strategy = RecordStrategy::KeysAndValues;

		self.current = CursorDoc::new(Some(retry), None, record);
		self.initial = self.current.clone();
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
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::reduced", skip_all)
	)]
	pub(crate) async fn reduced(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		permitted: Permitted,
	) -> Result<bool> {
		// Check if reduction is required
		if !self.check_reduction_required(opt)? {
			return Ok(false);
		}

		match permitted {
			Permitted::Initial => {
				self.initial_reduced =
					self.compute_reduced_target(stk, ctx, opt, &self.initial).await?;
			}
			Permitted::Current => {
				self.current_reduced =
					self.compute_reduced_target(stk, ctx, opt, &self.current).await?;
			}
			Permitted::Both => {
				self.initial_reduced =
					self.compute_reduced_target(stk, ctx, opt, &self.initial).await?;
				self.current_reduced =
					self.compute_reduced_target(stk, ctx, opt, &self.current).await?;
			}
		}

		// Document has been reduced
		Ok(true)
	}

	pub(crate) fn check_reduction_required(&self, opt: &Options) -> Result<bool> {
		// Check if this record exists
		if self.id.is_none() {
			return Ok(false);
		}
		// Are permissions being skipped?
		if !opt.check_perms(Action::View)? {
			return Ok(false);
		}

		// Reduction is required
		Ok(true)
	}

	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::compute_reduced_target", skip_all)
	)]
	pub(crate) async fn compute_reduced_target(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		full: &CursorDoc,
	) -> Result<CursorDoc> {
		// The document to be reduced
		let mut reduced = full.doc.clone();

		let table_fields = self.fd(ctx, opt).await?;

		// Loop over each field in document
		for fd in table_fields.iter() {
			// Limit auth
			let opt = AuthLimit::try_from(&fd.auth_limit)?.limit_opt(opt);
			// Loop over each field in document
			for k in reduced.as_ref().each(&fd.name).iter() {
				// Process the field permissions
				match &fd.select_permission {
					Permission::Full => (),
					Permission::None => reduced.to_mut().cut(k),
					Permission::Specific(e) => {
						// Disable permissions
						let opt = &opt.new_with_perms(false);
						// Get the initial value
						let val = Arc::new(full.doc.as_ref().pick(k));
						// Configure the context
						let mut ctx = Context::new(ctx);
						ctx.add_value("value", val);
						let ctx = ctx.freeze();
						// Process the PERMISSION clause
						if !stk
							.run(|stk| e.compute(stk, &ctx, opt, Some(full)))
							.await
							.catch_return()?
							.is_truthy()
						{
							reduced.to_mut().cut(k);
						}
					}
				}
			}
		}
		// Ok
		Ok(CursorDoc::new(full.rid.clone(), full.ir.clone(), reduced))
	}

	/// Retrieve the record id for this document
	pub(crate) fn id(&self) -> Result<Arc<RecordId>> {
		match &self.id {
			Some(id) => Ok(id.clone()),
			_ => fail!("Expected a document id to be present"),
		}
	}

	/// Retrieve the record id for this document
	pub fn inner_id(&self) -> Result<RecordId> {
		match self.id.clone() {
			Some(id) => Ok(Arc::unwrap_or_clone(id)),
			_ => fail!("Expected a document id to be present"),
		}
	}

	/// Get the database for this document
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::db", skip_all)
	)]
	pub(super) async fn db(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Arc<DatabaseDefinition>> {
		// Get the NS + DB
		let (ns, db) = opt.ns_db()?;
		// Get transaction
		let txn = ctx.tx();
		// Get the table definition
		match ctx.get_cache() {
			// A cache is present on the context
			Some(cache) if txn.is_local() => {
				// Get the cache entry key
				let key = cache::ds::key::DbCacheKey(ns.to_string(), db.to_string());
				// Get or update the cache entry
				match cache.get_clone(&key) {
					Some(val) => Ok(val),
					None => {
						let val = txn.get_or_add_db(Some(ctx), ns, db).await?;
						cache.insert(key, Arc::clone(&val));
						Ok(val)
					}
				}
			}
			// No cache is present on the context
			_ => txn.get_or_add_db(Some(ctx), ns, db).await,
		}
	}

	/// Get the table for this document
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::tb", skip_all)
	)]
	pub(super) async fn tb(&self) -> Result<&Arc<TableDefinition>> {
		self.doc_ctx.tb()
	}

	/// Get the foreign tables for this document
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::ft", skip_all)
	)]
	pub(super) async fn ft(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Arc<[TableDefinition]>> {
		// Get the NS + DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the document table
		let tb = self.tb().await?;
		// Get the cache from the context
		match ctx.get_cache() {
			// A cache is present on the context
			Some(cache) => {
				// Get the cache entry key
				let key = cache::ds::key::ForiegnTablesCacheKey(ns, db, tb.name.clone());
				// Get or update the cache entry
				match cache.get_clone(&key) {
					Some(val) => Ok(val),
					None => {
						let val = ctx.tx().all_tb_views(ns, db, &tb.name).await?;
						cache.insert(key, Arc::clone(&val));
						Ok(val)
					}
				}
			}
			// No cache is present on the context
			None => ctx.tx().all_tb_views(ns, db, &tb.name).await,
		}
	}

	/// Get the events for this document
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::ev", skip_all)
	)]
	pub(super) async fn ev(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Arc<[catalog::EventDefinition]>> {
		// Get the NS + DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the document table
		let tb = self.tb().await?;
		// Get the cache from the context
		match ctx.get_cache() {
			// A cache is present on the context
			Some(cache) => {
				// Get the cache entry key
				let key =
					cache::ds::key::EventsCacheKey(ns, db, tb.name.to_string(), tb.cache_events_ts);
				// Get or update the cache entry
				match cache.get_clone(&key) {
					Some(val) => Ok(val),
					None => {
						let val = ctx.tx().all_tb_events(ns, db, &tb.name).await?;
						cache.insert(key, Arc::clone(&val));
						Ok(val)
					}
				}
			}

			// No cache is present on the context
			None => ctx.tx().all_tb_events(ns, db, &tb.name).await,
		}
	}

	/// Get the fields for this document
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::fd", skip_all)
	)]
	pub(super) async fn fd(
		&self,
		_ctx: &FrozenContext,
		_opt: &Options,
	) -> Result<Arc<[catalog::FieldDefinition]>> {
		self.doc_ctx.fd().cloned()
	}

	/// Get the indexes for this document
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::ix", skip_all)
	)]
	pub(super) async fn ix(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Arc<[catalog::IndexDefinition]>> {
		// Get the NS + DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the document table
		let tb = self.tb().await?;
		// Get the cache from the context
		match ctx.get_cache() {
			// A cache is present on the context
			Some(cache) => {
				// Get the cache entry key
				let key = cache::ds::key::IndexesCacheKey(
					ns,
					db,
					tb.name.to_string(),
					tb.cache_indexes_ts,
				);
				// Get or update the cache entry
				match cache.get_clone(&key) {
					Some(val) => Ok(val),
					None => {
						let val = ctx.tx().all_tb_indexes(ns, db, &tb.name).await?;
						cache.insert(key, Arc::clone(&val));
						Ok(val)
					}
				}
			}
			// No cache is present on the context
			None => ctx.tx().all_tb_indexes(ns, db, &tb.name).await,
		}
	}

	// Get the lives for this document
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::lv", skip_all)
	)]
	pub(super) async fn lv(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<Arc<[catalog::SubscriptionDefinition]>> {
		// Get the NS + DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the document table
		let tb = self.tb().await?;
		// Get the cache from the context
		match ctx.get_cache() {
			// A cache is present on the context
			Some(cache) => {
				// Get the live-queries cache version
				let version = cache.get_live_queries_version(ns, db, &tb.name)?;
				// Get the cache entry key
				let key = cache::ds::key::LiveQueriesCacheKey(ns, db, tb.name.to_string(), version);
				// Get or update the cache entry
				match cache.get_clone(&key) {
					Some(val) => Ok(val),
					None => {
						let val = ctx.tx().all_tb_lives(ns, db, &tb.name).await?;
						cache.insert(key, Arc::clone(&val));
						Ok(val)
					}
				}
			}
			// No cache is present on the context
			None => ctx.tx().all_tb_lives(ns, db, &tb.name).await,
		}
	}
}
