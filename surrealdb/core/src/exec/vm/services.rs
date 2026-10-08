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
//! The index reads are the one place a program calls an iterator over index
//! storage: a B-tree cursor, a bitmap evaluation and the two candidate
//! searches.

use std::borrow::Cow;
use std::sync::Arc;

use super::program::DefHash;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseId, Error as CatalogError, FunctionDefinition, IndexId, NamespaceId, TableDefinition,
};
use crate::exec::index::access_path::{BTreeAccess, BitmapPlan, IndexRef};
use crate::exec::index::iterator::btree::{BTreeIterator, INDEX_BATCH_SIZE};
use crate::exec::operators::check_cancelled;
use crate::exec::operators::fetch::fetch_raw_record;
use crate::exec::operators::scan::common::{ReadAhead, ensure_below_memory_threshold};
use crate::exec::operators::scan::index_count::{count_btree_index_keys, sum_index_count_deltas};
use crate::exec::operators::scan::lightweight::{EdgeIdStream, lightweight_edge_ids};
use crate::exec::operators::scan::{
	FullTextHits, FusedCandidates, evaluate_fusion_plan, exact_bitmap_cardinality, knn_candidates,
	open_full_text_hits, record_key_range, resolve_doc_ids,
};
use crate::exec::permission::validate_record_user_access;
use crate::exec::physical_expr::EvalContext;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::logical::LogicalPermissionRef;
use crate::expr::operator::MatchesOperator;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::key::RawRange;
use crate::key::schema::{RecordKey, RecordPrefix};
use crate::kvs::{CachePolicy, Direction};
use crate::val::{Number, RecordId, RecordIdKey, RecordIdKeyRange, TableName, Value};

/// The reads of storage and index structures a program runs under, for one
/// database.
pub(super) trait Services {
	/// A cursor over the record ids B-tree index `index` lists under
	/// `access`, in its key order along `direction`. Fails when `access` is
	/// not one a B-tree index answers.
	fn index_rids(
		&self,
		index: &IndexRef,
		access: &BTreeAccess,
		direction: Direction,
	) -> FlowResult<BTreeIterator>;

	/// The next batch of record ids of `cursor`; empty once it is exhausted.
	async fn next_index_rids(&self, cursor: &mut BTreeIterator) -> FlowResult<Vec<RecordId>>;

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

	/// The hits of `query` against full-text index `index` of `table` under
	/// `operator`, or `None` when no record can match. The caller takes them
	/// a batch at a time.
	async fn full_text_candidates(
		&self,
		table: &TableName,
		index: &IndexRef,
		query: &str,
		operator: &MatchesOperator,
	) -> FlowResult<Option<FullTextHits>>;

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
	/// passed over. Fails with
	/// [`DatastoreError::QueryBeyondMemoryThreshold`](crate::kvs::DatastoreError)
	/// when the process is past `SURREAL_MEMORY_THRESHOLD` once they are
	/// decoded.
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

	/// The number of entries of B-tree index `index` under `access`, counted
	/// by the index keys alone.
	async fn count_btree_keys(&self, index: &IndexRef, access: &BTreeAccess) -> FlowResult<usize>;

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

/// A model definition as [`Services::read_model`] finds it.
#[cfg(feature = "ml")]
pub(super) type ModelDefinition = Arc<crate::catalog::MlModelDefinition>;
/// A build without the machine-learning runtime reads no model.
#[cfg(not(feature = "ml"))]
pub(super) type ModelDefinition = std::convert::Infallible;

/// A module definition as [`Services::read_module`] finds it.
#[cfg(feature = "surrealism")]
pub(super) type ModuleDefinition = Arc<crate::catalog::ModuleDefinition>;
/// A build without the Surrealism runtime reads no module.
#[cfg(not(feature = "surrealism"))]
pub(super) type ModuleDefinition = std::convert::Infallible;

/// A module export as [`Services::module_export`] resolves it.
#[cfg(feature = "surrealism")]
pub(super) type ModuleExport = crate::exec::physical_expr::function::ModuleExport;
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
/// `PERMISSIONS` clause, and how the clause admits readers.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct DbParam {
	pub(super) value: Value,
	pub(super) permission: DefHash,
	pub(super) admission: Admission,
}

/// The services of the engine this process runs: the datastore, the index
/// stores and the transaction of the running statement's context.
pub(super) struct EngineServices<'a> {
	ctx: &'a ExecutionContext,
}

impl<'a> EngineServices<'a> {
	/// The services of the database `ctx` runs in.
	pub(super) fn new(ctx: &'a ExecutionContext) -> Self {
		Self {
			ctx,
		}
	}
}

impl Services for EngineServices<'_> {
	fn index_rids(
		&self,
		index: &IndexRef,
		access: &BTreeAccess,
		direction: Direction,
	) -> FlowResult<BTreeIterator> {
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		let Some(rids) = BTreeIterator::new(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			index.definition(),
			index.is_unique(),
			access,
			direction,
		)
		.map_err(ControlFlow::Err)?
		else {
			return Err(ControlFlow::Err(anyhow::anyhow!("an index source with no B-tree access")));
		};
		Ok(rids)
	}

	async fn next_index_rids(&self, cursor: &mut BTreeIterator) -> FlowResult<Vec<RecordId>> {
		let txn = self.ctx.txn();
		cursor.next_batch(&txn, INDEX_BATCH_SIZE).await.map_err(ControlFlow::Err)
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
		resolve_doc_ids(self.ctx, table, doc_ids).await
	}

	async fn full_text_candidates(
		&self,
		table: &TableName,
		index: &IndexRef,
		query: &str,
		operator: &MatchesOperator,
	) -> FlowResult<Option<FullTextHits>> {
		validate_record_user_access(self.ctx.database()?)?;
		open_full_text_hits(self.ctx, table, index.definition(), query, operator).await
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
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		let records = self
			.ctx
			.txn()
			.get_records(
				db_ctx.ns_ctx.ns.namespace_id,
				db_ctx.db.database_id,
				rids,
				version,
				policy,
			)
			.await
			.map_err(ControlFlow::Err)?;
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

	async fn count_btree_keys(&self, index: &IndexRef, access: &BTreeAccess) -> FlowResult<usize> {
		let db_ctx = self.ctx.database().context("the VM requires a database context")?;
		let txn = self.ctx.txn();
		count_btree_index_keys(
			self.ctx,
			&txn,
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			index,
			access,
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
		crate::exec::physical_expr::function::check_script_allowed(ctx)
	}

	async fn call_script_dynamic(
		&self,
		source: &str,
		plan_depth: u32,
		args: Vec<Value>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<Value> {
		crate::exec::physical_expr::function::run_script(source, plan_depth, args, ctx).await
	}

	async fn read_model(
		&self,
		name: &str,
		version: &str,
		ctx: &EvalContext<'_>,
	) -> FlowResult<ModelDefinition> {
		crate::exec::physical_expr::function::enter_model(name, version, ctx).await
	}

	async fn compute_model(
		&self,
		name: &str,
		version: &str,
		definition: &ModelDefinition,
		args: Vec<Value>,
		ctx: &EvalContext<'_>,
	) -> FlowResult<Value> {
		crate::exec::physical_expr::function::compute_model(name, version, definition, args, ctx)
			.await
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
			crate::exec::physical_expr::function::read_module(storage, sub, ctx).await
		}
		#[cfg(not(feature = "surrealism"))]
		{
			let _ = ctx;
			let name = match sub {
				Some(sub) if !silo => format!("{storage}::{sub}"),
				_ => storage.to_owned(),
			};
			Err(crate::exec::physical_expr::function::module_runtime_missing(&name, silo))
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
			crate::exec::physical_expr::function::module_export(definition, sub, ctx).await
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
			crate::exec::physical_expr::function::run_module_export(storage, export, sub, args, ctx)
				.await
		}
		#[cfg(not(feature = "surrealism"))]
		{
			let _ = (storage, sub, args, ctx);
			match *export {}
		}
	}
}
