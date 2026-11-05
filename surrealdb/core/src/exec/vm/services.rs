//! The narrow host interface a running program reads storage and the engine's
//! index structures through.
//!
//! A [`Services`] method takes plain arguments — names, descriptors, values,
//! ids — and returns ids, rows or an opaque cursor over ids. None takes or
//! returns a planned expression, an operator or the planner, and none plans,
//! binds or compiles anything: a program that reaches one has everything it
//! needs in its own instructions and sources. A host that runs a program
//! elsewhere implements the trait over what it has there.
//!
//! What a method returns is the structure's own answer. A table's `SELECT`
//! permission, its computed fields and the permissions of its fields are not
//! applied by any of them: the program applies them, through the permission
//! and layer instructions its lowering planned, to the rows it reads for what
//! a method names. Each is therefore applied exactly once, in the program.
//!
//! A B-tree index is read through [`Services::read_range`] like any other
//! range of the keyspace; its keys and entries are encoded and decoded by the
//! kernels the index write path shares (see `super::entries`). The index
//! reads that go through this trait are a bitmap evaluation and the two
//! candidate searches.

use std::borrow::Cow;
use std::sync::Arc;

use super::ir_host::{EngineIrHost, IrHostService};
use super::program::DefHash;
use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{
	DatabaseId, Error as CatalogError, FunctionDefinition, IndexId, NamespaceId, TableDefinition,
};
use crate::exec::index::access_path::{BitmapPlan, IndexRef};
use crate::exec::operators::check_cancelled;
use crate::exec::operators::fetch::fetch_raw_record;
use crate::exec::operators::scan::common::{ReadAhead, ensure_below_memory_threshold};
use crate::exec::operators::scan::index_count::sum_index_count_deltas;
use crate::exec::operators::scan::lightweight::{EdgeIdStream, lightweight_edge_ids};
use crate::exec::operators::scan::{
	FusedCandidates, evaluate_fusion_plan, exact_bitmap_cardinality, knn_candidates,
	open_full_text_index, record_key_range,
};
use crate::exec::permission::validate_record_user_access;
use crate::exec::{EvalContext, ExecutionContext, FlowResult};
use crate::expr::logical::LogicalPermissionRef;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::kernels::docids;
use crate::key::RawRange;
use crate::key::schema::{RecordKey, RecordPrefix};
use crate::kvs::{CachePolicy, Direction};
use crate::val::{Number, RecordId, RecordIdKey, RecordIdKeyRange, TableName, Value};

/// The reads of storage and index structures a program runs under, for one
/// database.
pub(super) trait Services {
	/// The host's services for IR-lowered instructions, or `None` when the host has none.
	#[allow(dead_code)]
	fn ir_host(&self) -> Option<&dyn IrHostService> {
		None
	}

	/// The next at most `limit` entries of `range` along `direction`, as
	/// stored keys and values, narrowing `range` to what is left to read; none
	/// once it is read. A read of the keyspace alone: what the keys hold is the
	/// caller's to decode.
	async fn read_range(
		&self,
		range: &mut RawRange,
		direction: Direction,
		limit: u32,
	) -> FlowResult<Vec<(Vec<u8>, Vec<u8>)>>;

	/// As [`Services::read_range`], answering the stored keys alone: no value
	/// is read, for a caller that needs only what the keys spell or how many
	/// there are.
	async fn read_range_keys(
		&self,
		range: &mut RawRange,
		direction: Direction,
		limit: u32,
	) -> FlowResult<Vec<Vec<u8>>>;

	/// The doc-IDs of `table` that bitmap plan `plan` leaves, or an overflow
	/// of its branch budget.
	///
	/// With `anchored`, an overflowing conjunct is dropped and the plan never
	/// overflows; without it, a branch that cannot be dropped overflows the
	/// plan. The plan reads the current state only, so a versioned context
	/// is refused.
	async fn evaluate_bitmap(
		&self,
		table: &TableName,
		plan: &BitmapPlan,
		anchored: bool,
	) -> FlowResult<FusedCandidates>;

	/// The record ids of `table` that doc-IDs `doc_ids` name, in their order;
	/// a doc-ID with no record names none.
	async fn resolve_doc_ids(
		&self,
		table: &TableName,
		doc_ids: &[u64],
	) -> FlowResult<Vec<RecordId>>;

	/// The values stored at `keys`, in their order; `None` where no value
	/// is stored. A read of the keyspace alone.
	async fn read_values(
		&self,
		keys: Vec<crate::key::Key<'static>>,
	) -> FlowResult<Vec<Option<Vec<u8>>>>;

	/// Full-text index `index` of `table`, opened for a query: its analyzer
	/// and the doc-id layout its postings resolve through.
	async fn open_full_text(
		&self,
		table: &TableName,
		index: &IndexRef,
	) -> FlowResult<FullTextIndex>;

	/// The changes to `term` of full-text index `index` the transaction
	/// buffered and has not yet written: the documents it added and those it
	/// removed.
	fn pending_term_changes(
		&self,
		index: &FullTextIndex,
		term: &str,
	) -> (roaring::RoaringTreemap, roaring::RoaringTreemap);

	/// The `k` records ANN index `index` of `table` ranks nearest to
	/// `vector`, nearest first, each with its distance; `ef` is the number of
	/// candidates the search explores. The table definition is read as of
	/// `version`. `cond_filter` gates each candidate inside the search on the
	/// table's permission and then a condition, and `allow` restricts
	/// admission to a bitmap of doc-IDs; without either the ranking is the
	/// index's own.
	#[allow(clippy::too_many_arguments)]
	async fn knn_candidates(
		&self,
		table: &TableName,
		index: &IndexRef,
		vector: &[Number],
		k: u32,
		ef: u32,
		version: Option<u64>,
		cond_filter: Option<crate::idx::trees::KnnCondFilter<'_>>,
		allow: Option<&roaring::RoaringTreemap>,
	) -> FlowResult<Vec<(RecordId, f64)>>;

	/// The ids of the edges of lightweight relation `table` as of `version`,
	/// in batches, in ascending canonical-id order: every edge, or those a
	/// `wanted` id or range of ids holds. Fails when `table` is not a
	/// lightweight relation.
	fn scan_lightweight_edges(
		&self,
		table: &TableName,
		wanted: Option<RecordIdKey>,
		direction: Direction,
		version: Option<u64>,
	) -> EdgeIdStream;

	/// The records `rids` name that exist as of `version`, in order, each as
	/// its row with the record id in place; a record that does not exist is
	/// passed over. Fails with the process memory threshold's error when the
	/// process is past `SURREAL_MEMORY_THRESHOLD` once they are decoded.
	async fn read_records(
		&self,
		rids: &[RecordId],
		version: Option<u64>,
		policy: CachePolicy,
	) -> FlowResult<Vec<Value>>;

	/// Read the records `rids` names as of `version` into `ahead` as their
	/// stored bytes, for it to decode; the bytes read and the number of
	/// records found, for sizing the next read.
	async fn read_stored(
		&self,
		ahead: &mut ReadAhead,
		rids: Vec<RecordId>,
		version: Option<u64>,
	) -> FlowResult<(usize, usize)>;

	/// The record `rid` names as of `version`, or `None` when it does not
	/// exist. An edge of a lightweight relation is synthesized from its id.
	async fn read_record(&self, rid: &RecordId, version: Option<u64>) -> FlowResult<Option<Value>>;

	/// The definition of `table` in the database the context runs in, as of
	/// `version`; `None` when it is not defined.
	async fn read_table(
		&self,
		table: &TableName,
		version: Option<u64>,
	) -> FlowResult<Option<Arc<TableDefinition>>>;

	/// The key range that holds every record of `table`.
	fn table_range(&self, table: &TableName) -> FlowResult<RawRange>;

	/// The key range that holds the records of `table` that `range` of record
	/// keys names.
	fn record_range(&self, table: &TableName, range: &RecordIdKeyRange) -> FlowResult<RawRange>;

	/// The number of keys in `range` as of `version`, counted by key alone:
	/// no value is read or decoded. The keys are charged to the fuel meter as
	/// they are counted, with a yield point between reads.
	async fn count_keys(&self, range: RawRange, version: Option<u64>) -> FlowResult<usize>;

	/// Whether record `rid` exists as of `version`, decided by its key alone.
	async fn record_exists(&self, rid: &RecordId, version: Option<u64>) -> FlowResult<bool>;

	/// The number of records of `table` that `COUNT` index `index` holds: the
	/// sum of its stored deltas and those the transaction has buffered.
	async fn count_index_deltas(&self, table: &TableName, index: IndexId) -> FlowResult<usize>;

	/// The cardinality of the exact fused bitmap `plan` over `table`, with no
	/// record read. The plan reads the current state only, so a versioned
	/// context is refused.
	async fn bitmap_cardinality(&self, table: &TableName, plan: &BitmapPlan) -> FlowResult<usize>;

	/// The definition of user-defined function `name` in the database the
	/// context runs in, as of `version`. Fails, as the catalog does, when no
	/// such function is defined.
	async fn read_function(
		&self,
		name: &str,
		version: Option<u64>,
	) -> FlowResult<Arc<FunctionDefinition>>;

	/// The `DEFINE PARAM` of `name` in database `db` of namespace `ns`, as
	/// of `version`; `None` when none is defined.
	async fn read_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		name: &str,
		version: Option<u64>,
	) -> anyhow::Result<Option<DbParam>>;

	/// Refuse a script call `ctx` cannot make, before its arguments are
	/// evaluated: a session whose capabilities deny scripting, and every call
	/// in a build without the script runtime.
	fn check_script(&self, ctx: &EvalContext<'_>) -> FlowResult<()>;

	/// The value script `source` answers for `args`, run in `ctx` with its
	/// current value as `this`; a call it makes back into SurrealQL continues
	/// the computation depth from `plan_depth`.
	///
	/// The code is data: the script runtime runs `source` as written, in the
	/// declared dynamic-code category.
	async fn call_script_dynamic(
		&self,
		source: &str,
		plan_depth: u32,
		args: Vec<Value>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<Value>;

	/// The definition of model `name` at `version` a call from `ctx` reads,
	/// as of the context's `VERSION`, once the session may call it; fails as
	/// the catalog does when there is none, and in a build without the
	/// machine-learning runtime.
	async fn read_model(
		&self,
		name: &str,
		version: &str,
		ctx: &EvalContext<'_>,
	) -> FlowResult<ModelDefinition>;

	/// The output of model `name` at `version`, `definition` as
	/// [`Self::read_model`] read it, for `args`.
	async fn compute_model(
		&self,
		name: &str,
		version: &str,
		definition: &ModelDefinition,
		args: Vec<Value>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<Value>;

	/// The definition of the module stored as `storage` a call of its export
	/// `sub` from `ctx` reads, as of the context's `VERSION`, once the session
	/// may call it; fails as the catalog does when there is none, and in a
	/// build without the Surrealism runtime, naming a package's call by its
	/// storage name when `silo`.
	async fn read_module(
		&self,
		storage: &str,
		sub: Option<&str>,
		silo: bool,
		ctx: &EvalContext<'_>,
	) -> FlowResult<ModuleDefinition>;

	/// Export `sub` of module `definition`, its signature resolved for a call
	/// from `ctx`.
	async fn module_export(
		&self,
		definition: &ModuleDefinition,
		sub: Option<&str>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<ModuleExport>;

	/// The value export `sub` of the module stored as `storage`, resolved as
	/// `export`, answers for `args`, called from `ctx`.
	async fn call_module(
		&self,
		storage: &str,
		export: &ModuleExport,
		sub: Option<&str>,
		args: Vec<Value>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<Value>;
}

/// A model definition as [`Services::read_model`] finds it. Models are not
/// executed, so no definition is ever read.
pub(super) type ModelDefinition = std::convert::Infallible;

/// A module definition as [`Services::read_module`] finds it.
#[cfg(feature = "surrealism")]
pub(super) type ModuleDefinition = Arc<crate::catalog::ModuleDefinition>;
/// A build without the Surrealism runtime reads no module.
#[cfg(not(feature = "surrealism"))]
pub(super) type ModuleDefinition = std::convert::Infallible;

/// A module export as [`Services::module_export`] resolves it.
#[cfg(feature = "surrealism")]
pub(super) type ModuleExport = crate::exec::vm::calls::ModuleExport;
/// A build without the Surrealism runtime resolves no export.
#[cfg(not(feature = "surrealism"))]
pub(super) type ModuleExport = std::convert::Infallible;

/// How a `DEFINE PARAM` admits a read of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Admission {
	/// Every reader.
	Full,
	/// No reader.
	None,
	/// The readers a predicate admits.
	Predicate,
}

/// A `DEFINE PARAM` as a read of it finds it: its value, the hash of its
/// `PERMISSIONS` clause, how the clause admits readers, and the definition
/// the clause is read from.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct DbParam {
	pub(super) value: Value,
	pub(super) permission: DefHash,
	pub(super) admission: Admission,
	pub(super) definition: Arc<crate::catalog::ParamDefinition>,
}

/// The services of the engine this process runs: the datastore, the index
/// stores and the transaction of the running statement's context.
pub(super) struct EngineServices<'a> {
	ctx: &'a ExecutionContext,
	ir: EngineIrHost<'a>,
}

impl<'a> EngineServices<'a> {
	/// The services of the database `ctx` runs in.
	pub(super) fn new(ctx: &'a ExecutionContext) -> Self {
		Self {
			ctx,
			ir: EngineIrHost::new(ctx),
		}
	}
}

impl Services for EngineServices<'_> {
	fn ir_host(&self) -> Option<&dyn IrHostService> {
		Some(&self.ir)
	}

	async fn read_range(
		&self,
		range: &mut RawRange,
		direction: Direction,
		limit: u32,
	) -> FlowResult<Vec<(Vec<u8>, Vec<u8>)>> {
		#[cfg(test)]
		super::morsel::note_txn_read_on_worker();
		check_cancelled(self.ctx)?;
		let txn = self.ctx.txn();
		match direction {
			Direction::Forward => crate::kvs::util::scan(range, &txn, limit).await,
			Direction::Backward => crate::kvs::util::scanr(range, &txn, limit).await,
		}
		.map_err(ControlFlow::Err)
	}

	async fn read_range_keys(
		&self,
		range: &mut RawRange,
		direction: Direction,
		limit: u32,
	) -> FlowResult<Vec<Vec<u8>>> {
		#[cfg(test)]
		super::morsel::note_txn_read_on_worker();
		check_cancelled(self.ctx)?;
		let txn = self.ctx.txn();
		match direction {
			Direction::Forward => crate::kvs::util::scan_keys(range, &txn, limit).await,
			Direction::Backward => crate::kvs::util::scanr_keys(range, &txn, limit).await,
		}
		.map_err(ControlFlow::Err)
	}

	async fn evaluate_bitmap(
		&self,
		table: &TableName,
		plan: &BitmapPlan,
		anchored: bool,
	) -> FlowResult<FusedCandidates> {
		evaluate_fusion_plan(self.ctx, table, plan, anchored).await
	}

	async fn resolve_doc_ids(
		&self,
		table: &TableName,
		doc_ids: &[u64],
	) -> FlowResult<Vec<RecordId>> {
		let db_ctx = self.ctx.database()?;
		let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
		let keys = doc_ids
			.iter()
			.map(|&doc_id| docids::record_key(ns, db, table, doc_id))
			.collect::<anyhow::Result<Vec<_>>>()
			.map_err(ControlFlow::Err)?;
		let values = self.read_values(keys).await?;
		values
			.into_iter()
			.flatten()
			.map(|bytes| {
				let key = docids::decode_record_key(&bytes)
					.map_err(|e| e.context("Failed to resolve doc-IDs to record IDs"))?;
				Ok(RecordId {
					table: table.clone(),
					key,
				})
			})
			.collect::<anyhow::Result<Vec<_>>>()
			.map_err(ControlFlow::Err)
	}

	async fn read_values(
		&self,
		keys: Vec<crate::key::Key<'static>>,
	) -> FlowResult<Vec<Option<Vec<u8>>>> {
		check_cancelled(self.ctx)?;
		let keys = keys.into_iter().map(StoredAt).collect();
		self.ctx.txn().getm_raw(keys, None).await.map_err(ControlFlow::Err)
	}

	async fn open_full_text(
		&self,
		table: &TableName,
		index: &IndexRef,
	) -> FlowResult<FullTextIndex> {
		validate_record_user_access(self.ctx.database()?)?;
		open_full_text_index(self.ctx, table, index.definition()).await
	}

	fn pending_term_changes(
		&self,
		index: &FullTextIndex,
		term: &str,
	) -> (roaring::RoaringTreemap, roaring::RoaringTreemap) {
		index.pending_term_changes(&self.ctx.txn(), term)
	}

	#[allow(clippy::too_many_arguments)]
	async fn knn_candidates(
		&self,
		table: &TableName,
		index: &IndexRef,
		vector: &[Number],
		k: u32,
		ef: u32,
		version: Option<u64>,
		cond_filter: Option<crate::idx::trees::KnnCondFilter<'_>>,
		allow: Option<&roaring::RoaringTreemap>,
	) -> FlowResult<Vec<(RecordId, f64)>> {
		validate_record_user_access(self.ctx.database()?)?;
		knn_candidates(
			self.ctx,
			table,
			index.definition(),
			vector,
			k,
			ef,
			version,
			cond_filter,
			allow,
		)
		.await
	}

	fn scan_lightweight_edges(
		&self,
		table: &TableName,
		wanted: Option<RecordIdKey>,
		direction: Direction,
		version: Option<u64>,
	) -> EdgeIdStream {
		lightweight_edge_ids(self.ctx, table.clone(), wanted, direction, version)
	}

	async fn read_records(
		&self,
		rids: &[RecordId],
		version: Option<u64>,
		policy: CachePolicy,
	) -> FlowResult<Vec<Value>> {
		#[cfg(test)]
		super::morsel::note_txn_read_on_worker();
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		let records = crate::exec::migrate::get_records(
			self.ctx,
			&self.ctx.txn(),
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			rids,
			version,
			policy,
		)
		.await?;
		ensure_below_memory_threshold()?;
		let values = records
			.into_iter()
			.filter(|record| !record.data.is_none())
			.map(|record| match std::sync::Arc::try_unwrap(record) {
				Ok(record) => record.data,
				Err(record) => record.data.clone(),
			})
			.collect();
		// A record the transaction's cache shares is cloned out of it above.
		ensure_below_memory_threshold()?;
		Ok(values)
	}

	async fn read_stored(
		&self,
		ahead: &mut ReadAhead,
		rids: Vec<RecordId>,
		version: Option<u64>,
	) -> FlowResult<(usize, usize)> {
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		ahead
			.read(
				&self.ctx.txn(),
				db_ctx.ns_ctx.ns.namespace_id,
				db_ctx.db.database_id,
				rids,
				version,
			)
			.await
	}

	async fn read_record(&self, rid: &RecordId, version: Option<u64>) -> FlowResult<Option<Value>> {
		#[cfg(test)]
		super::morsel::note_txn_read_on_worker();
		fetch_raw_record(self.ctx, rid, version).await
	}

	async fn read_table(
		&self,
		table: &TableName,
		version: Option<u64>,
	) -> FlowResult<Option<Arc<TableDefinition>>> {
		crate::exec::vm::descriptor::table_at(self.ctx, table, version)
			.await
			.map(|table| table.definition.clone())
			.map_err(ControlFlow::Err)
	}

	fn table_range(&self, table: &TableName) -> FlowResult<RawRange> {
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		Ok(RecordPrefix {
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			tb: Cow::Borrowed(table),
		}
		.range()?)
	}

	fn record_range(&self, table: &TableName, range: &RecordIdKeyRange) -> FlowResult<RawRange> {
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		record_key_range(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, table, range)
	}

	async fn count_keys(&self, range: RawRange, version: Option<u64>) -> FlowResult<usize> {
		check_cancelled(self.ctx)?;
		super::count::count_keys(self.ctx, range, version).await
	}

	async fn record_exists(&self, rid: &RecordId, version: Option<u64>) -> FlowResult<bool> {
		#[cfg(test)]
		super::morsel::note_txn_read_on_worker();
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		let key = RecordKey {
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
		};
		self.ctx.txn().exists_key(&key, version).await.context("Failed to check record existence")
	}

	async fn count_index_deltas(&self, table: &TableName, index: IndexId) -> FlowResult<usize> {
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		let txn = self.ctx.txn();
		sum_index_count_deltas(
			self.ctx,
			&txn,
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			table,
			index,
		)
		.await
	}

	async fn bitmap_cardinality(&self, table: &TableName, plan: &BitmapPlan) -> FlowResult<usize> {
		Ok(exact_bitmap_cardinality(self.ctx, table, plan).await? as usize)
	}

	async fn read_function(
		&self,
		name: &str,
		version: Option<u64>,
	) -> FlowResult<Arc<FunctionDefinition>> {
		let db_ctx = self.ctx.database().map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
		let txn = self.ctx.txn();
		crate::exec::vm::descriptor::object(txn.get_db_function(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			name,
			version,
		))
		.await
		.map_err(ControlFlow::Err)
	}

	async fn read_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		name: &str,
		version: Option<u64>,
	) -> anyhow::Result<Option<DbParam>> {
		let txn = self.ctx.txn();
		match crate::exec::vm::descriptor::object(txn.get_db_param(ns, db, name, version)).await {
			Ok(definition) => {
				let admission = match definition.permissions_body.logical_ref()? {
					LogicalPermissionRef::Full => Admission::Full,
					LogicalPermissionRef::None => Admission::None,
					LogicalPermissionRef::Specific(_) => Admission::Predicate,
				};
				Ok(Some(DbParam {
					value: definition.value.clone(),
					permission: DefHash::of_param(&definition),
					admission,
					definition,
				}))
			}
			Err(e) => {
				if matches!(e.downcast_ref(), Some(CatalogError::PaNotFound { .. })) {
					Ok(None)
				} else {
					Err(e)
				}
			}
		}
	}

	fn check_script(&self, ctx: &EvalContext<'_>) -> FlowResult<()> {
		crate::exec::vm::calls::check_script_allowed(ctx)
	}

	async fn call_script_dynamic(
		&self,
		source: &str,
		plan_depth: u32,
		args: Vec<Value>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<Value> {
		crate::exec::vm::calls::run_script(source, plan_depth, args, ctx).await
	}

	async fn read_model(
		&self,
		name: &str,
		version: &str,
		ctx: &EvalContext<'_>,
	) -> FlowResult<ModelDefinition> {
		crate::exec::vm::calls::enter_model(name, version, ctx).await
	}

	async fn compute_model(
		&self,
		name: &str,
		version: &str,
		definition: &ModelDefinition,
		args: Vec<Value>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<Value> {
		crate::exec::vm::calls::compute_model(name, version, definition, args, ctx).await
	}

	async fn read_module(
		&self,
		storage: &str,
		sub: Option<&str>,
		silo: bool,
		ctx: &EvalContext<'_>,
	) -> FlowResult<ModuleDefinition> {
		#[cfg(feature = "surrealism")]
		{
			let _ = silo;
			crate::exec::vm::calls::read_module(storage, sub, ctx).await
		}
		#[cfg(not(feature = "surrealism"))]
		{
			let _ = ctx;
			let name = match sub {
				Some(sub) if !silo => format!("{storage}::{sub}"),
				_ => storage.to_owned(),
			};
			Err(crate::exec::vm::calls::module_runtime_missing(&name, silo))
		}
	}

	async fn module_export(
		&self,
		definition: &ModuleDefinition,
		sub: Option<&str>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<ModuleExport> {
		#[cfg(feature = "surrealism")]
		{
			crate::exec::vm::calls::module_export(definition, sub, ctx).await
		}
		#[cfg(not(feature = "surrealism"))]
		{
			let _ = (sub, ctx);
			match *definition {}
		}
	}

	async fn call_module(
		&self,
		storage: &str,
		export: &ModuleExport,
		sub: Option<&str>,
		args: Vec<Value>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<Value> {
		#[cfg(feature = "surrealism")]
		{
			crate::exec::vm::calls::run_module_export(storage, export, sub, args, ctx).await
		}
		#[cfg(not(feature = "surrealism"))]
		{
			let _ = (storage, sub, args, ctx);
			match *export {}
		}
	}
}

/// A key whose bytes are already encoded, for a read of the value stored at
/// it as bytes.
#[derive(Debug)]
struct StoredAt(crate::key::Key<'static>);

impl surrealdb_kvs::key::KVKey for StoredAt {
	type Value = ();

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> anyhow::Result<()> {
		buffer.extend_from_slice(self.0.as_slice());
		Ok(())
	}

	fn value_context(&self) {}
}
