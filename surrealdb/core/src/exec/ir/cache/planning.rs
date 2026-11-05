//! The per-planning catalog memo: the definitions one compile reads, held for that compile only.
//!
//! One memo serves one compile: a request's arrival compile, a statement planned when it is
//! reached, or one unit load. It is created when the compile starts and dropped when it returns.
//! Nothing in it is shared across compiles, transactions, requests or nodes, and no `static`,
//! `Datastore` field or global holds one.
//!
//! The memo reads only through the requesting transaction, using its catalog getters, so it sees
//! the transaction's snapshot plus that transaction's
//! uncommitted DDL. What it reads is bounded by what it may be asked, and no further:
//! - the tables and functions the statement names, and those the definitions of the tables and
//!   functions it loaded name, with the events and the views of every table it loads;
//! - a stored parameter only for a free name of the statement's own text that no binding the
//!   compile holds answers, and never in an `Isolated` compile;
//! - no function the request's capabilities deny.
//!
//! A binding is planned from only where the caller names it for the statement being planned
//! (see [`PlanBinding`]): bindings change from statement to statement, so none is held for the
//! whole compile. A name bound by a `LET` whose value only the run computes is read at run time,
//! whatever the context or the catalog holds for that name.
//!
//! A statement planned under a `VERSION` clause reads definitions at a stamp this memo does not
//! hold, so it answers every definition [`CatalogAnswer::Unknown`].
//!
//! A table is read as [`CatalogSnapshot`] reads it, which is the one shared structure it
//! consults: the datastore cache validates a snapshot by the catalog version the transaction
//! itself reads.
//!
//! Lookups are synchronous and never do I/O. A repeat lookup is answered from the memo and noted
//! in the transaction's open catalog-read recordings, so a recording opened mid-compile lists
//! every definition the code it guards depended on. A name the memo does not hold is answered
//! [`CatalogAnswer::NotLoaded`] and becomes pending: the caller awaits
//! [`PlanningCatalog::load_pending`] and builds again.
//!
//! The memo's lock is never held across an `.await`. A load computes the missing names under the
//! lock, drops the guard, awaits the reads, and locks again to insert what it read.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use surrealdb_expr::parser::TopLevelExpr;
use surrealdb_strand::Strand;

use super::answer::{
	CatalogAnswer, ExactAnswer, FreeName, LayoutFacts, PRINCIPAL_NAMES, SessionValues, kind_class,
};
use super::deps::{
	BindingOrigin, DepRecorder, LiteralTable, ValueDep, ValueDeps, ValueRef, ValueUse,
};
use super::names::{self, NameSet};
use super::slots::SlotTable;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseDefinition, DatabaseId, Error as CatalogError, EventDefinition, FunctionDefinition,
	IndexDefinition, NamespaceId, ParamDefinition, Permission, TableDefinition, TableType,
};
use crate::ctx::FrozenContext;
use crate::exec::catalog_deps::Referencing;
use crate::exec::ir::core::Ir;
use crate::exec::ir::iface::IndexRef;
use crate::exec::ir::iface::catalog::{
	BackendCaps, FieldView, IoCaps, PhysicalCatalog, SemanticCatalog, SourceKey, next_lineage,
};
use crate::exec::ir::iface::fold::FoldConsumer;
use crate::exec::ir::iface::ids::{FrameId, ValueId};
use crate::exec::ir::phys::access::{ReadVersion, TableAnswers};
use crate::exec::ir::timers::ReadCount;
use crate::exec::operators::mutate::MUTATION_PARAM_PREFIX;
use crate::expr::logical::{ITERATION_PARAMS, LogicalExpr};
use crate::iam::{Auth, AuthLimit};
use crate::kvs::Transaction;
use crate::kvs::cache::ds::{CatalogSnapshot, TableSnapshot};
use crate::val::{Datetime, TableName, Value};

/// The per-planning catalog memo. See the module documentation for its contract.
pub(crate) struct PlanningCatalog {
	txn: Arc<Transaction>,
	ctx: FrozenContext,
	/// The namespace and database ids definitions are read under. Without them every table,
	/// function and parameter lookup answers `Absent` and nothing is read.
	ns_db: Option<(NamespaceId, DatabaseId)>,
	/// The namespace and database names the database definition is read by.
	ns_db_names: Option<(String, String)>,
	session: SessionValues,
	/// The names read at run time: the names bound around a unit.
	shadowed: BTreeSet<String>,
	/// The table each request variable a request's statement writes as its one target names
	/// ([`SemanticCatalog::target_table`]).
	targets: Vec<(String, TableName)>,
	/// Whether a write of the container's request code may run linked stages
	/// ([`SemanticCatalog::links_stages`]).
	links: bool,
	/// Whether each call of a user-defined function runs the definition read when it is made
	/// ([`SemanticCatalog::calls_current`]).
	calls_current: bool,
	/// The principal the request is planned for, where it is known
	/// ([`SemanticCatalog::function_limit_is_inert`]).
	caller: Option<Arc<Auth>>,
	/// The `VERSION` stamp the compile's planner reads the catalog at, or `None` for the
	/// transaction's current state: the stamp function definitions are read at, and the stamp of
	/// every read under no `VERSION` of its own ([`PhysicalCatalog::read_stamp`]).
	read_stamp: Option<u64>,
	/// What the datastore offers, which the compile's caller states.
	backend: BackendCaps,
	/// Whether the container is planned as a statement of a request
	/// ([`PhysicalCatalog::at_request_root`]).
	request_root: bool,
	/// Whether a read after a catalog change is planned here as one that knows nothing of its
	/// table ([`PhysicalCatalog::plans_unknown_reads`]).
	plans_unknown_reads: bool,
	state: Mutex<MemoState>,
	deps: DepRecorder,
	/// The reads the memo issued to its transaction, for the compile timers.
	reads: ReadCount,
	/// The identity a fact cache files this memo's answers by
	/// ([`SemanticCatalog::memo_identity`]).
	lineage: u64,
}

/// What the memo holds. Every update leaves it consistent.
struct MemoState {
	snapshot: Option<Arc<CatalogSnapshot>>,
	events: HashMap<TableName, Arc<[EventDefinition]>>,
	/// The definitions of the view tables each held table's views fill.
	views: HashMap<TableName, Arc<[TableDefinition]>>,
	/// The `REFERENCE` fields that can name a record of each table asked for, by the table that
	/// defines them. Loaded on demand and never reached from a statement's names.
	referencing: HashMap<TableName, Referencing>,
	/// Whether a table of the database that can hold edges stores code a delete of one of its
	/// records runs ([`PlanningCatalog::edge_code`]). Loaded on demand, like `referencing`.
	edge_code: Option<bool>,
	/// The field view of each table asked for, as the planner binds it for the principal the
	/// compile is for. Loaded on demand by the planner's owner
	/// ([`PlanningCatalog::insert_field_view`]).
	field_views: HashMap<TableName, Arc<FieldView>>,
	/// `None` is a function the transaction's catalog does not define.
	functions: HashMap<String, Option<Arc<FunctionDefinition>>>,
	/// `None` is a parameter the transaction's catalog does not define.
	params: HashMap<String, Option<Arc<ParamDefinition>>>,
	/// `Some(None)` is a database the transaction's catalog does not define.
	database: Option<Option<Arc<DatabaseDefinition>>>,
	/// The names answered `NotLoaded` since the last load.
	pending: NameSet,
	/// The tables whose references were answered `NotLoaded` since the last load.
	pending_referencing: BTreeSet<TableName>,
	/// Whether the edge code was answered `NotLoaded` since the last load.
	pending_edge_code: bool,
	/// The tables whose field views were answered `NotLoaded` since the last take.
	pending_field_views: BTreeSet<TableName>,
	/// What an access-path decision reads about each table read the compile names, filled by the
	/// caller ([`PlanningCatalog::insert_answers`]).
	sources: HashMap<SourceKey, Arc<TableAnswers>>,
	/// The table reads answered `NotLoaded` since the pending reads were last taken.
	pending_sources: HashSet<SourceKey>,
	/// The conjuncts whose record paths the planner folded, as an access-path decision reads
	/// them ([`PhysicalCatalog::record_folded`]), filled by the caller
	/// ([`PlanningCatalog::insert_record_folded`]).
	record_folds: HashMap<LogicalExpr, LogicalExpr>,
	slots: BTreeMap<usize, Arc<SlotTable>>,
	current: Option<Current>,
	/// Whether the compile has passed a catalog change. Kept for the rest of the compile: it is
	/// not reset by entering or leaving a statement.
	catalog_changed: bool,
}

impl MemoState {
	/// Whether the definitions held are not the ones the current statement plans from: a catalog
	/// change has been passed, or the statement reads at a `VERSION` stamp.
	fn definitions_unknown(&self) -> bool {
		self.catalog_changed || self.current.as_ref().is_some_and(|current| current.versioned)
	}
}

/// How the compile may treat one bound name in the statement being planned.
///
/// The caller states it per statement, as the request binds the name at that statement's
/// position. Only a name with a binding whose origin is not [`BindingOrigin::LetComputed`] is
/// planned from; a name the context holds that the caller does not list is read at run time.
#[derive(Clone, Debug)]
pub(crate) struct PlanBinding {
	origin: BindingOrigin,
	/// The value at this statement. `None` reads the memo's context.
	value: Option<Arc<Value>>,
}

impl PlanBinding {
	/// A name whose value is the one the memo's context binds, from `origin`. A `LetComputed`
	/// origin is a name read at run time.
	pub(crate) fn context(origin: BindingOrigin) -> Self {
		Self {
			origin,
			value: None,
		}
	}

	/// A `LET` whose value is the constant `value` at this statement.
	pub(crate) fn constant(value: Value) -> Self {
		Self {
			origin: BindingOrigin::LetConst,
			value: Some(Arc::new(value)),
		}
	}

	/// A `LET` whose value only the run computes.
	pub(crate) fn computed() -> Self {
		Self::context(BindingOrigin::LetComputed)
	}
}

/// The statement being planned.
struct Current {
	index: usize,
	/// The frame of the statement's own text, the only frame a caller binding is answered in.
	top: FrameId,
	/// How each name the caller binds at this statement is treated.
	bindings: BTreeMap<String, PlanBinding>,
	/// Whether the statement reads at a `VERSION` stamp.
	versioned: bool,
	/// Whether an op of this statement changed the catalog. Ends with the statement; the
	/// compile-wide fact is [`MemoState::catalog_changed`].
	changed: bool,
}

/// What one load round read.
#[derive(Default)]
struct Loaded {
	snapshot: Option<Arc<CatalogSnapshot>>,
	events: Vec<(TableName, Arc<[EventDefinition]>)>,
	views: Vec<(TableName, Arc<[TableDefinition]>)>,
	functions: Vec<(String, Option<Arc<FunctionDefinition>>)>,
	params: Vec<(String, Option<Arc<ParamDefinition>>)>,
}

impl PlanningCatalog {
	/// A memo reading through `txn`, which holds no definition yet.
	pub(crate) fn new(
		txn: Arc<Transaction>,
		ctx: FrozenContext,
		ns_db: Option<(NamespaceId, DatabaseId)>,
		session: SessionValues,
	) -> Self {
		Self {
			txn,
			ctx,
			ns_db,
			ns_db_names: None,
			session,
			shadowed: BTreeSet::new(),
			targets: Vec::new(),
			links: false,
			calls_current: false,
			caller: None,
			read_stamp: None,
			backend: BackendCaps::default(),
			request_root: false,
			plans_unknown_reads: false,
			state: Mutex::new(MemoState {
				snapshot: None,
				events: HashMap::new(),
				views: HashMap::new(),
				referencing: HashMap::new(),
				edge_code: None,
				field_views: HashMap::new(),
				functions: HashMap::new(),
				params: HashMap::new(),
				database: None,
				pending: NameSet::default(),
				pending_referencing: BTreeSet::new(),
				pending_edge_code: false,
				pending_field_views: BTreeSet::new(),
				sources: HashMap::new(),
				pending_sources: HashSet::new(),
				record_folds: HashMap::new(),
				slots: BTreeMap::new(),
				current: None,
				catalog_changed: false,
			}),
			deps: DepRecorder::new(),
			reads: ReadCount::default(),
			lineage: next_lineage(),
		}
	}

	/// The names bound around the unit, which are read when it runs.
	pub(crate) fn with_shadowed(mut self, names: impl IntoIterator<Item = String>) -> Self {
		self.shadowed = names.into_iter().collect();
		self
	}

	/// The tables `targets` names for the request variables a request's statement writes as its
	/// one target, answered by [`SemanticCatalog::target_table`].
	pub(crate) fn with_targets(mut self, targets: &[(String, TableName)]) -> Self {
		self.targets = targets.to_vec();
		self
	}

	/// Whether a write of the container's request code may run linked stages, answered by
	/// [`SemanticCatalog::links_stages`].
	pub(crate) fn with_links(mut self, links: bool) -> Self {
		self.links = links;
		self
	}

	/// Whether each call of a user-defined function runs the definition read when it is made,
	/// answered by [`SemanticCatalog::calls_current`].
	pub(crate) fn with_calls_current(mut self, calls_current: bool) -> Self {
		self.calls_current = calls_current;
		self
	}

	/// States the principal the request is planned for.
	pub(crate) fn with_caller(mut self, caller: Option<Arc<Auth>>) -> Self {
		self.caller = caller;
		self
	}

	/// Reads at `stamp`, a `VERSION` stamp the compile's planner reads the catalog at, where it is
	/// one. A call compiled at a stamp runs the definition the stamp holds, so the definition the
	/// call states ([`SemanticCatalog::function`]) is that one: a call unit compiled at the stamp
	/// holds the hash of the definition its run enters. A read under no `VERSION` of its own reads
	/// the records and the indexes the stamp holds ([`PhysicalCatalog::read_stamp`]).
	pub(crate) fn at_stamp(mut self, stamp: Option<u64>) -> Self {
		self.read_stamp = stamp;
		self
	}

	/// Whether the container is planned as a statement of a request, answered by
	/// [`PhysicalCatalog::at_request_root`].
	pub(crate) fn with_request_root(mut self, request_root: bool) -> Self {
		self.request_root = request_root;
		self
	}

	/// What the datastore offers, answered by [`Self::backend`]. Without it the memo answers the
	/// conservative [`BackendCaps::default`].
	pub(crate) fn with_backend(mut self, backend: BackendCaps) -> Self {
		self.backend = backend;
		self
	}

	/// The memo of a compile that plans a read after a catalog change as one that knows nothing
	/// of its table, where `plans`: a compile no later compile of the statement follows (the
	/// late-bound one, or a stage bundle the run loads).
	pub(crate) fn planning_unknown_reads(mut self, plans: bool) -> Self {
		self.plans_unknown_reads = plans;
		self
	}

	/// The namespace and database names the database definition is read by. Without them
	/// [`Self::database`] answers `None`.
	pub(crate) fn with_database_names(
		mut self,
		ns: impl Into<String>,
		db: impl Into<String>,
	) -> Self {
		self.ns_db_names = Some((ns.into(), db.into()));
		self
	}

	/// Which plan-time values this compile may plan from.
	pub(crate) fn session(&self) -> SessionValues {
		self.session
	}

	/// Loads every catalog object statement `index` reaches, transitively through definitions, to
	/// a fixed point, and computes the statement's [`SlotTable`].
	///
	/// A read error propagates and leaves what was loaded before it in place.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::exec::ir", skip_all)
	)]
	pub(crate) async fn preload(&self, index: usize, stmt: &TopLevelExpr) -> anyhow::Result<()> {
		{
			let mut state = self.lock();
			state.slots.entry(index).or_insert_with(|| Arc::new(SlotTable::of(stmt)));
		}
		self.load_database().await?;
		let slots = self.slots(index).unwrap_or_default();
		// A delete repairs the references to what it purges, which the stage that purges states
		// from the references of the table. They are read with the statement's other names, so a
		// parsed statement binds once.
		let (mut names, deleted) = names::collect_with(stmt, &slots);
		names.params.retain(|name| self.may_read_param(name));
		if !deleted.is_empty() {
			self.load_referencing(&deleted).await?;
			self.load_edge_code().await?;
		}
		self.load_reaching(names).await
	}

	/// Loads the names answered `NotLoaded` since the last load, and what they reach. `Ok(false)`
	/// when none were pending.
	///
	/// A read error propagates. The names it left unloaded are answered `NotLoaded` again, and
	/// become pending again, by the next lookup.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::exec::ir", skip_all)
	)]
	pub(crate) async fn load_pending(&self) -> anyhow::Result<bool> {
		let (pending, references, edge_code) = {
			let mut state = self.lock();
			(
				std::mem::take(&mut state.pending),
				std::mem::take(&mut state.pending_referencing),
				std::mem::take(&mut state.pending_edge_code),
			)
		};
		if pending.is_empty() && references.is_empty() && !edge_code {
			return Ok(false);
		}
		if !references.is_empty() {
			self.load_referencing(&references).await?;
		}
		if edge_code {
			self.load_edge_code().await?;
		}
		if !pending.is_empty() {
			self.load_reaching(pending).await?;
		}
		Ok(true)
	}

	/// Starts the statement `index`, whose own text is in frame `top`. Entering a statement ends
	/// the one entered before it.
	///
	/// `bindings` are the names bound when the statement runs, each with where its value came
	/// from (the executor's `binding_origin` at the statement's position). `versioned` is whether
	/// the statement carries a `VERSION` clause: its definitions are then unknown, since the memo
	/// reads the current catalog and not the stamp's.
	pub(crate) fn enter_statement(
		&self,
		index: usize,
		top: FrameId,
		bindings: impl IntoIterator<Item = (String, PlanBinding)>,
		versioned: bool,
	) {
		self.lock().current = Some(Current {
			index,
			top,
			bindings: bindings.into_iter().collect(),
			versioned,
			changed: false,
		});
		self.deps.enter(index);
	}

	/// Ends the current statement.
	pub(crate) fn leave_statement(&self) {
		self.lock().current = None;
		self.deps.leave();
	}

	/// Records that an op changed the catalog. Definitions are unknown from here to the end of the
	/// compile: [`Self::table`], [`Self::events`], [`Self::indexes`], [`Self::function`] and
	/// [`Self::param`] answer [`CatalogAnswer::Unknown`], in this statement and in every later
	/// one. From here to [`Self::leave_statement`], every free-name lookup answers `RunTime`; a
	/// later statement answers a client binding or a principal value again, and a stored
	/// parameter at run time.
	pub(crate) fn mark_catalog_change(&self) {
		let mut state = self.lock();
		state.catalog_changed = true;
		if let Some(current) = state.current.as_mut() {
			current.changed = true;
		}
	}

	/// The slot table of statement `index`, once [`Self::preload`] has run for it.
	pub(crate) fn slots(&self, index: usize) -> Option<Arc<SlotTable>> {
		self.lock().slots.get(&index).cloned()
	}

	/// The reads the memo has issued to its transaction, and none for an answer served from what
	/// the memo holds. A preload of `n` tables counts `2n + 1`: the snapshot, then each table's
	/// events and views. It counts the calls made, not the datastore reads they cost, so a snapshot
	/// the datastore's cache serves counts all the same.
	#[cfg(feature = "compile-timers")]
	pub(crate) fn issued_reads(&self) -> u64 {
		self.reads.issued()
	}

	/// The value-dependence record of each statement, sorted by statement index. Consumes the
	/// memo.
	pub(crate) fn finish(self) -> Vec<(usize, ValueDeps)> {
		self.deps.finish()
	}

	/// The database definition, once [`Self::preload`] has read it. `None` when the memo holds
	/// none: it was not loaded, no names were given, the transaction's catalog defines none, or
	/// the current statement reads at a `VERSION` stamp.
	pub(crate) fn database(&self) -> Option<Arc<DatabaseDefinition>> {
		let held = {
			let state = self.lock();
			if state.current.as_ref().is_some_and(|current| current.versioned) {
				return None;
			}
			state.database.clone().flatten()
		};
		if let Some((ns, db)) = &self.ns_db_names {
			self.txn.note_database_read(ns, db);
		}
		held
	}

	/// The table `name` with its fields and indexes, as the transaction sees it. `Absent` for a
	/// table the transaction's catalog does not define. `Unknown` after a catalog change and in a
	/// statement that reads at a `VERSION` stamp, as for every definition lookup.
	pub(crate) fn table(&self, name: &TableName) -> CatalogAnswer<Arc<TableSnapshot>> {
		self.table_answer(name, |snapshot, _, _| Arc::clone(snapshot))
	}

	/// The events of the table `name`. `Absent` for a table the transaction's catalog does not
	/// define.
	pub(crate) fn events(&self, table: &TableName) -> CatalogAnswer<Arc<[EventDefinition]>> {
		self.table_answer(table, |_, events, _| Arc::clone(events))
	}

	/// The definitions of the view tables the views of the table `table` fill: the tables whose
	/// definition selects from it. `Absent` for a table the transaction's catalog does not define.
	pub(crate) fn views(&self, table: &TableName) -> CatalogAnswer<Arc<[TableDefinition]>> {
		self.table_answer(table, |_, _, views| Arc::clone(views))
	}

	/// The `REFERENCE` fields that can name a record of the table `table`, by the table that
	/// defines them, in the database's table order. A table the memo has not been asked about is
	/// `NotLoaded` and becomes pending, whatever the memo holds of the table itself: the answer is
	/// a database-wide read. `Unknown` after a catalog change and in a statement that reads at a
	/// `VERSION` stamp, as for every definition lookup.
	///
	/// A held answer notes in the transaction's recordings the read of the references and the
	/// catalog of each table that defines one, which the code lowered from them depends on.
	pub(crate) fn referencing(&self, table: &TableName) -> CatalogAnswer<Referencing> {
		let Some((ns, db)) = self.ns_db else {
			return CatalogAnswer::Absent;
		};
		let held = {
			let mut state = self.lock();
			if state.definitions_unknown() {
				return CatalogAnswer::Unknown;
			}
			match state.referencing.get(table) {
				Some(held) => Arc::clone(held),
				None => {
					state.pending_referencing.insert(table.clone());
					return CatalogAnswer::NotLoaded;
				}
			}
		};
		self.txn.note_references_read(ns, db, Some(table));
		for (defining, _) in held.iter() {
			self.txn.note_table_read(ns, db, defining);
		}
		CatalogAnswer::Found(held)
	}

	/// Whether a table of the database that can hold edges stores code a delete of one of its
	/// records runs ([`SemanticCatalog::edge_code`]). `NotLoaded` until it is loaded, and then
	/// pending: the answer is a database-wide read. `Unknown` after a catalog change and in a
	/// statement that reads at a `VERSION` stamp, as for every definition lookup.
	///
	/// A held answer notes in the transaction's recordings the read of the database's listing of
	/// tables, whose state holds the state of every table it lists: the code lowered from it
	/// depends on which tables are defined and on the catalog of each.
	pub(crate) fn edge_code(&self) -> CatalogAnswer<bool> {
		let Some((ns, db)) = self.ns_db else {
			return CatalogAnswer::Absent;
		};
		let held = {
			let mut state = self.lock();
			if state.definitions_unknown() {
				return CatalogAnswer::Unknown;
			}
			match state.edge_code {
				Some(held) => held,
				None => {
					state.pending_edge_code = true;
					return CatalogAnswer::NotLoaded;
				}
			}
		};
		self.txn.note_listing_read(ns, db);
		CatalogAnswer::Found(held)
	}

	/// The field view of the table `table` for the principal the compile is for. A table the memo
	/// has not been given the view of is `NotLoaded` and becomes pending: the owner of the planner
	/// takes the pending tables ([`Self::take_pending_field_views`]), binds each view and gives it
	/// back ([`Self::insert_field_view`]). `Unknown` after a catalog change and in a statement that
	/// reads at a `VERSION` stamp, as for every definition lookup.
	///
	/// A held answer notes in the transaction's recordings the read of the table's catalog, which
	/// the code lowered from the view depends on.
	pub(crate) fn field_view(&self, table: &TableName) -> CatalogAnswer<Arc<FieldView>> {
		let Some((ns, db)) = self.ns_db else {
			return CatalogAnswer::Absent;
		};
		let held = {
			let mut state = self.lock();
			if state.definitions_unknown() {
				return CatalogAnswer::Unknown;
			}
			match state.field_views.get(table) {
				Some(held) => Arc::clone(held),
				None => {
					state.pending_field_views.insert(table.clone());
					return CatalogAnswer::NotLoaded;
				}
			}
		};
		self.txn.note_table_read(ns, db, table);
		CatalogAnswer::Found(held)
	}

	/// The tables whose field views were answered `NotLoaded` since they were last taken.
	pub(crate) fn take_pending_field_views(&self) -> BTreeSet<TableName> {
		std::mem::take(&mut self.lock().pending_field_views)
	}

	/// Holds the field view of `table`, which [`Self::field_view`] answers from then on.
	pub(crate) fn insert_field_view(&self, table: TableName, view: FieldView) {
		self.lock().field_views.insert(table, Arc::new(view));
	}

	/// The custom function `name`, without the `fn::` prefix.
	///
	/// A function the request's capabilities deny is `Absent`, and its definition is never read.
	/// The definition is answered whatever its `PERMISSIONS` and `AUTH LIMIT`: a consumer that
	/// inlines or folds the body answers for those itself.
	pub(crate) fn function(&self, name: &str) -> CatalogAnswer<Arc<FunctionDefinition>> {
		let Some((ns, db)) = self.ns_db else {
			return CatalogAnswer::Absent;
		};
		if !self.function_allowed(name) {
			return CatalogAnswer::Absent;
		}
		let answer = {
			let mut state = self.lock();
			if state.definitions_unknown() {
				return CatalogAnswer::Unknown;
			}
			match state.functions.get(name) {
				Some(held) => held_answer(held),
				None => {
					state.pending.functions.insert(name.to_owned());
					CatalogAnswer::NotLoaded
				}
			}
		};
		if !matches!(answer, CatalogAnswer::NotLoaded) {
			self.txn.note_function_read(ns, db, name);
		}
		answer
	}

	/// The stored parameter `name`, without the `$`.
	pub(crate) fn param(&self, name: &str) -> CatalogAnswer<Arc<ParamDefinition>> {
		let Some((ns, db)) = self.ns_db else {
			return CatalogAnswer::Absent;
		};
		let answer = {
			let mut state = self.lock();
			if state.definitions_unknown() {
				return CatalogAnswer::Unknown;
			}
			match state.params.get(name) {
				Some(held) => held_answer(held),
				None => {
					state.pending.params.insert(name.to_owned());
					CatalogAnswer::NotLoaded
				}
			}
		};
		if !matches!(answer, CatalogAnswer::NotLoaded) {
			self.txn.note_param_read(ns, db, name);
		}
		answer
	}

	/// The indexes of the table `name`, as the transaction sees them. `Absent` for a table the
	/// transaction's catalog does not define.
	pub(crate) fn indexes(&self, table: &TableName) -> CatalogAnswer<Arc<[IndexDefinition]>> {
		self.table_answer(table, |snapshot, _, _| Arc::clone(&snapshot.indexes))
	}

	/// What an access-path decision reads about the table a read of `key` names.
	///
	/// A read the memo does not hold becomes pending and answers `NotLoaded`. After a catalog
	/// change, and in a statement that reads at a `VERSION` stamp, every read answers `Unknown`
	/// whatever the memo holds: the answers describe a catalog the code does not run against.
	pub(crate) fn table_answers(&self, key: &SourceKey) -> CatalogAnswer<Arc<TableAnswers>> {
		let mut state = self.lock();
		if state.definitions_unknown() {
			return CatalogAnswer::Unknown;
		}
		match state.sources.get(key) {
			Some(held) => CatalogAnswer::Found(Arc::clone(held)),
			None => {
				state.pending_sources.insert(key.clone());
				CatalogAnswer::NotLoaded
			}
		}
	}

	/// Holds the answers for the read `key`. A later fill of the same key replaces them.
	pub(crate) fn insert_answers(&self, key: SourceKey, answers: Arc<TableAnswers>) {
		let mut state = self.lock();
		state.pending_sources.remove(&key);
		state.sources.insert(key, answers);
	}

	/// Holds `folded` as the conjunct `conjunct` an access-path decision reads.
	pub(crate) fn insert_record_folded(&self, conjunct: LogicalExpr, folded: LogicalExpr) {
		self.lock().record_folds.insert(conjunct, folded);
	}

	/// Whether the memo holds the answers for the read `key`.
	pub(crate) fn holds_answers(&self, key: &SourceKey) -> bool {
		self.lock().sources.contains_key(key)
	}

	/// The answers the memo holds, whatever order: what a compile resolves the indexes its
	/// containers open from.
	pub(crate) fn held_answers(&self) -> Vec<Arc<TableAnswers>> {
		self.lock().sources.values().cloned().collect()
	}

	/// The table reads answered `NotLoaded` since they were last taken, which a top-up fill
	/// reads. The set is emptied.
	pub(crate) fn take_pending_sources(&self) -> Vec<SourceKey> {
		std::mem::take(&mut self.lock().pending_sources).into_iter().collect()
	}

	/// Whether `index` is built and serves reads of the current version.
	///
	/// The online indexes of a current read are those the memo holds in the answers of the table's
	/// `Current` read: an index the table defines that those leave out is not ready. Without that
	/// read the answer is `NotLoaded`, and the read becomes pending.
	pub(crate) fn index_ready(&self, index: &IndexRef) -> CatalogAnswer<bool> {
		let defined = match self.indexes(&index.table) {
			CatalogAnswer::Found(indexes) => indexes,
			CatalogAnswer::Absent => return CatalogAnswer::Absent,
			CatalogAnswer::NotLoaded => return CatalogAnswer::NotLoaded,
			CatalogAnswer::Unknown => return CatalogAnswer::Unknown,
		};
		if !defined.iter().any(|held| held.name == index.index) {
			return CatalogAnswer::Absent;
		}
		let key: SourceKey = (index.table.clone(), None, ReadVersion::Current);
		match self.table_answers(&key) {
			CatalogAnswer::Found(answers) => {
				CatalogAnswer::Found(answers.indexes.iter().any(|held| held.name == index.index))
			}
			CatalogAnswer::Absent => CatalogAnswer::Absent,
			CatalogAnswer::NotLoaded => CatalogAnswer::NotLoaded,
			CatalogAnswer::Unknown => CatalogAnswer::Unknown,
		}
	}

	/// What the datastore offers, as the compile's caller stated it.
	pub(crate) fn backend(&self) -> BackendCaps {
		self.backend
	}

	/// The layout facts of `table`. No table layout carries generations, so this answers `None`.
	pub(crate) fn layout(&self, _table: &TableName) -> Option<LayoutFacts> {
		None
	}

	/// The statistics epoch. No statistics exist, so this answers `None`.
	pub(crate) fn stats_epoch(&self) -> Option<u64> {
		None
	}

	/// Records that the walk from `value` reached a decision of `consumer`, for the current
	/// statement, with that statement's slot table. A statement with no slot table holds no slot,
	/// so a free name consumed there marks the statement overflowed.
	pub(crate) fn consumed(
		&self,
		ir: &Ir,
		value: ValueId,
		consumer: FoldConsumer,
		literals: &LiteralTable,
	) {
		let slots = {
			let state = self.lock();
			let Some(current) = &state.current else {
				return;
			};
			state.slots.get(&current.index).cloned()
		};
		let slots = slots.unwrap_or_default();
		self.deps.consumed(ir, value, consumer, &slots, literals);
	}

	/// Merges the record another phase produced for statement `index`. `complete == false` marks
	/// the statement overflowed.
	pub(crate) fn absorb(&self, index: usize, deps: &ValueDeps, complete: bool) {
		self.deps.absorb(index, deps, complete);
	}

	/// How the free name `name` read by an op in frame `at` is bound. Records nothing.
	pub(crate) fn free_name(&self, name: &str, at: FrameId) -> FreeName {
		match self.resolve(name, at) {
			Resolved::RunTime => FreeName::RunTime,
			Resolved::Principal => FreeName::Principal,
			Resolved::Bound(value, _) => FreeName::Bound(kind_class(value.get())),
			Resolved::Param(def) => FreeName::Bound(kind_class(&def.value)),
			Resolved::Absent => FreeName::Unbound,
			Resolved::NotLoaded => FreeName::NotLoaded,
		}
	}

	/// As [`Self::free_name`], for a consumer that branches on the class of the value. A bound
	/// name records `{Slot | Param, consumer, Kind(class)}` and an unbound one
	/// `{Absent, consumer, Unbound}`. A principal name records nothing, since its class is not
	/// planned from.
	pub(crate) fn free_kind(&self, name: &str, at: FrameId, consumer: FoldConsumer) -> FreeName {
		match self.resolve(name, at) {
			Resolved::RunTime => FreeName::RunTime,
			Resolved::Principal => FreeName::Principal,
			Resolved::Bound(value, origin) => {
				let class = kind_class(value.get());
				self.record_slot(name, origin, consumer, ValueUse::Kind(class));
				FreeName::Bound(class)
			}
			Resolved::Param(def) => {
				let class = kind_class(&def.value);
				self.record(ValueRef::Param(Strand::new(name)), consumer, ValueUse::Kind(class));
				FreeName::Bound(class)
			}
			Resolved::Absent => {
				self.record(ValueRef::Absent(Strand::new(name)), consumer, ValueUse::Unbound);
				FreeName::Unbound
			}
			Resolved::NotLoaded => FreeName::NotLoaded,
		}
	}

	/// The exact value of the free name `name` read in frame `at`, for a decision that plans from
	/// it. An answer of `Value` has recorded the dependence. A principal name answers a value in
	/// a `Fold` compile only, and a name bound nowhere answers `NONE`.
	pub(crate) fn free_value_exact(
		&self,
		name: &str,
		at: FrameId,
		consumer: FoldConsumer,
	) -> ExactAnswer {
		match self.resolve(name, at) {
			Resolved::RunTime => ExactAnswer::RunTime,
			Resolved::Principal => {
				if self.session != SessionValues::Fold {
					return ExactAnswer::RunTime;
				}
				self.record(ValueRef::Principal(Strand::new(name)), consumer, ValueUse::Exact);
				ExactAnswer::Value(self.ctx.value(name).cloned().unwrap_or(Value::None))
			}
			Resolved::Bound(value, origin) => {
				self.record_slot(name, origin, consumer, ValueUse::Exact);
				ExactAnswer::Value(value.get().clone())
			}
			Resolved::Param(def) => {
				self.record(ValueRef::Param(Strand::new(name)), consumer, ValueUse::Exact);
				ExactAnswer::Value(def.value.clone())
			}
			Resolved::Absent => {
				self.record(ValueRef::Absent(Strand::new(name)), consumer, ValueUse::Exact);
				ExactAnswer::Value(Value::None)
			}
			Resolved::NotLoaded => ExactAnswer::NotLoaded,
		}
	}

	/// Where the free name `name` read in frame `at` is bound, which is the first of these that
	/// applies:
	///
	/// 1. An `Isolated` compile, no current statement, a frame other than the statement's own text,
	///    or a statement that changed the catalog: `RunTime`.
	/// 2. An iteration name (`$this`, `$self`, `$parent`), a name the unit is told is shadowed, or
	///    a name the VM binds for a MATCH write (`__mutation_*`): `RunTime`. The other row names
	///    are read as any free name is.
	/// 3. A principal name: `Principal`.
	/// 4. A name the context binds, or the statement's bindings name: the value of the statement's
	///    binding, or the context's when the binding holds none, ahead of any stored parameter. A
	///    name whose binding is `LetComputed`, or that the context binds with no binding listed for
	///    the statement, is `RunTime`: its value at run time is not the one the context holds.
	/// 5. No namespace and database: `RunTime`.
	/// 6. A stored parameter the memo does not hold: `NotLoaded`, and the name becomes pending.
	/// 7. A stored parameter with `PERMISSIONS FULL`: its definition.
	/// 8. A stored parameter with any other permission, or one read after a catalog change anywhere
	///    earlier in the compile or in a statement that reads at a `VERSION` stamp: `RunTime`.
	/// 9. No such stored parameter: `Absent`.
	///
	/// Reads nothing and records nothing, except that a held stored parameter is noted in the
	/// transaction's open catalog-read recordings.
	fn resolve(&self, name: &str, at: FrameId) -> Resolved<'_> {
		if self.session == SessionValues::Isolated {
			return Resolved::RunTime;
		}
		let (in_top, binding) = {
			let state = self.lock();
			match state.current.as_ref() {
				Some(current) if current.top == at && !current.changed => {
					(true, current.bindings.get(name).cloned())
				}
				_ => (false, None),
			}
		};
		if !in_top || self.runtime_name(name) {
			return Resolved::RunTime;
		}
		if PRINCIPAL_NAMES.contains(&name) {
			return Resolved::Principal;
		}
		match binding {
			Some(binding) if binding.origin == BindingOrigin::LetComputed => {
				return Resolved::RunTime;
			}
			Some(binding) => {
				return match (binding.value, self.ctx.value(name)) {
					(Some(value), _) => {
						Resolved::Bound(BoundValue::Statement(value), binding.origin)
					}
					(None, Some(value)) => {
						Resolved::Bound(BoundValue::Context(value), binding.origin)
					}
					(None, None) => Resolved::RunTime,
				};
			}
			None if self.ctx.value(name).is_some() => return Resolved::RunTime,
			None => {}
		}
		if self.ns_db.is_none() {
			return Resolved::RunTime;
		}
		match self.param(name) {
			CatalogAnswer::NotLoaded => Resolved::NotLoaded,
			CatalogAnswer::Found(def) if matches!(def.permissions, Permission::Full) => {
				Resolved::Param(def)
			}
			CatalogAnswer::Found(_) | CatalogAnswer::Unknown => Resolved::RunTime,
			CatalogAnswer::Absent => Resolved::Absent,
		}
	}

	/// Records that `consumer` used `value` as `used`, for the current statement.
	fn record(&self, value: ValueRef, consumer: FoldConsumer, used: ValueUse) {
		self.deps.record(ValueDep {
			value,
			consumer,
			used,
		});
	}

	/// Records that `consumer` used the value bound to `name`, by the slot the current statement's
	/// slot table gives it, with where the value came from. A name with no slot marks the
	/// statement overflowed, since the dependence cannot be named.
	fn record_slot(
		&self,
		name: &str,
		origin: BindingOrigin,
		consumer: FoldConsumer,
		used: ValueUse,
	) {
		let slots = {
			let state = self.lock();
			state.current.as_ref().and_then(|current| state.slots.get(&current.index).cloned())
		};
		let Some(slot) = slots.and_then(|slots| slots.id_of(name)) else {
			self.deps.mark_overflowed();
			return;
		};
		self.record(ValueRef::Slot(slot), consumer, used);
		self.deps.note_origin(slot, origin);
	}

	/// The answer for table `name`, with `f` applied to the table, its events and its views when
	/// all are held. A held table the catalog does not define is `Absent`. A table the memo does
	/// not hold becomes pending.
	fn table_answer<T>(
		&self,
		name: &TableName,
		f: impl FnOnce(&Arc<TableSnapshot>, &Arc<[EventDefinition]>, &Arc<[TableDefinition]>) -> T,
	) -> CatalogAnswer<T> {
		let Some((ns, db)) = self.ns_db else {
			return CatalogAnswer::Absent;
		};
		let answer = {
			let mut state = self.lock();
			if state.definitions_unknown() {
				return CatalogAnswer::Unknown;
			}
			let held = state
				.snapshot
				.as_ref()
				.and_then(|snapshot| snapshot.entry(name))
				.zip(state.events.get(name))
				.zip(state.views.get(name))
				.map(|((entry, events), views)| {
					(entry.definition.is_some(), f(entry, events, views))
				});
			match held {
				Some((true, answer)) => CatalogAnswer::Found(answer),
				Some((false, _)) => CatalogAnswer::Absent,
				None => {
					state.pending.tables.insert(name.clone());
					CatalogAnswer::NotLoaded
				}
			}
		};
		if !matches!(answer, CatalogAnswer::NotLoaded) {
			self.txn.note_table_read(ns, db, name);
		}
		answer
	}

	/// Reads the `REFERENCE` fields of the database once and holds, for each of `tables`, those
	/// that can name a record of it. A read error leaves `tables` unheld, so a later lookup asks
	/// again.
	async fn load_referencing(&self, tables: &BTreeSet<TableName>) -> anyhow::Result<()> {
		let Some((ns, db)) = self.ns_db else {
			return Ok(());
		};
		self.reads.add(1);
		let read = self.txn.reference_fields(ns, db, None).await?;
		let mut state = self.lock();
		for table in tables {
			state.referencing.insert(
				table.clone(),
				crate::exec::catalog_deps::references_to(&read, Some(table)),
			);
		}
		Ok(())
	}

	/// Reads whether a table of the database that can hold edges stores code a delete of one of
	/// its records runs, once: from every table the database defines, the events, views and
	/// `REFERENCE` fields of each that is not `TYPE NORMAL`, and the `REFERENCE` fields of the
	/// database that can name one of its records.
	async fn load_edge_code(&self) -> anyhow::Result<()> {
		let Some((ns, db)) = self.ns_db else {
			return Ok(());
		};
		if self.lock().edge_code.is_some() {
			return Ok(());
		}
		self.reads.add(2);
		let tables = self.txn.all_tb_each(ns, db).await?;
		let references = self.txn.reference_fields(ns, db, None).await?;
		let mut held = false;
		for table in tables.iter().filter(|table| table.table_type != TableType::Normal) {
			self.reads.add(3);
			held =
				!crate::exec::catalog_deps::references_to(&references, Some(&table.name))
					.is_empty() || !self.txn.all_tb_events(ns, db, &table.name, None).await?.is_empty()
					|| !self.txn.all_tb_views(ns, db, &table.name, None).await?.is_empty()
					|| self
						.txn
						.all_tb_fields(ns, db, &table.name, None)
						.await?
						.iter()
						.any(|field| field.reference.is_some());
			if held {
				break;
			}
		}
		self.lock().edge_code = Some(held);
		Ok(())
	}

	/// Reads the database definition once, when the names it is read by are known.
	pub(crate) async fn load_database(&self) -> anyhow::Result<()> {
		let (Some(_), Some((ns, db))) = (self.ns_db, &self.ns_db_names) else {
			return Ok(());
		};
		if self.lock().database.is_some() {
			return Ok(());
		}
		self.reads.add(1);
		let def = self.txn.get_db_by_name(ns, db, None).await?;
		self.lock().database = Some(def);
		Ok(())
	}

	/// Loads `want` and everything its definitions reach, to a fixed point. Each round loads at
	/// least one name that was not held, so the loop ends.
	async fn load_reaching(&self, mut want: NameSet) -> anyhow::Result<()> {
		let Some((ns, db)) = self.ns_db else {
			return Ok(());
		};
		loop {
			let (missing, base) = {
				let state = self.lock();
				(want.missing_from(&Self::held(&state)), state.snapshot.clone())
			};
			if missing.is_empty() {
				return Ok(());
			}
			let loaded = self.read(ns, db, &missing, base).await?;
			let mut state = self.lock();
			if let Some(snapshot) = loaded.snapshot {
				state.snapshot = Some(snapshot);
			}
			for (table, events) in loaded.events {
				state.events.insert(table, events);
			}
			for (table, views) in loaded.views {
				state.views.insert(table, views);
			}
			for (name, def) in loaded.functions {
				state.functions.insert(name, def);
			}
			for (name, def) in loaded.params {
				state.params.insert(name, def);
			}
			for table in &missing.tables {
				if let Some(entry) = state.snapshot.as_ref().and_then(|s| s.entry(table)) {
					let events = state.events.get(table).map_or(&[][..], |events| &events[..]);
					want.extend(NameSet::from_table(entry, events));
				}
			}
			for name in &missing.functions {
				if let Some(Some(def)) = state.functions.get(name) {
					want.extend(NameSet::from_function(def));
				}
			}
		}
	}

	/// The names the memo holds. A table is held when its snapshot, its events and its views are
	/// all held, so a snapshot a concurrent load replaced reads as missing the tables it lost.
	fn held(state: &MemoState) -> NameSet {
		NameSet {
			tables: state
				.events
				.keys()
				.filter(|table| state.views.contains_key(*table))
				.filter(|table| state.snapshot.as_ref().is_some_and(|s| s.entry(table).is_some()))
				.cloned()
				.collect(),
			functions: state.functions.keys().cloned().collect(),
			params: state.params.keys().cloned().collect(),
		}
	}

	/// Reads `missing` through the transaction, with no lock held. `base` is the snapshot the
	/// tables extend.
	async fn read(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		missing: &NameSet,
		base: Option<Arc<CatalogSnapshot>>,
	) -> anyhow::Result<Loaded> {
		let mut loaded = Loaded::default();
		if !missing.tables.is_empty() {
			let tables: Vec<TableName> = missing.tables.iter().cloned().collect();
			// The snapshot, then the events and the views of each table
			let tables_read = u64::try_from(tables.len()).unwrap_or(u64::MAX);
			self.reads.add(tables_read.saturating_mul(2).saturating_add(1));
			let cache = self.ctx.get_cache();
			let snapshot = match base {
				Some(base) => base.extended(cache.as_deref(), &self.txn, &tables).await?,
				None => {
					CatalogSnapshot::for_tables(cache.as_deref(), &self.txn, ns, db, &tables)
						.await?
				}
			};
			// Each table's events and views are held by its entry, under the version the entry
			// is keyed by.
			for table in tables {
				let Some(entry) = snapshot.entry(&table) else {
					anyhow::bail!("table `{table}` was not loaded into the catalog snapshot");
				};
				loaded.events.push((table.clone(), Arc::clone(&entry.events)));
				loaded.views.push((table, Arc::clone(&entry.views)));
			}
			loaded.snapshot = Some(snapshot);
		}
		for name in &missing.functions {
			if !self.function_allowed(name) {
				loaded.functions.push((name.clone(), None));
				continue;
			}
			self.reads.add(1);
			let def = match self.txn.get_db_function(ns, db, name, self.read_stamp).await {
				Ok(def) => Some(def),
				Err(e)
					if is_catalog_error(&e, |e| matches!(e, CatalogError::FcNotFound { .. })) =>
				{
					None
				}
				Err(e) => return Err(e),
			};
			loaded.functions.push((name.clone(), def));
		}
		for name in &missing.params {
			self.reads.add(1);
			let def = match self.txn.get_db_param(ns, db, name, None).await {
				Ok(def) => Some(def),
				Err(e)
					if is_catalog_error(&e, |e| matches!(e, CatalogError::PaNotFound { .. })) =>
				{
					None
				}
				Err(e) => return Err(e),
			};
			loaded.params.push((name.clone(), def));
		}
		Ok(loaded)
	}

	/// Whether [`Self::resolve`] can reach the stored parameter `name` for a free name of a
	/// statement's own text: the compile may plan from values, no binding answers the name first,
	/// and it is not a name read at run time.
	fn may_read_param(&self, name: &str) -> bool {
		self.session != SessionValues::Isolated
			&& !self.runtime_name(name)
			&& self.ctx.value(name).is_none()
	}

	/// Whether the free name `name` is one the run binds: an iteration name, a name the unit is
	/// told is shadowed, or a name the VM binds for a MATCH write.
	fn runtime_name(&self, name: &str) -> bool {
		ITERATION_PARAMS.contains(&name)
			|| self.shadowed.contains(name)
			|| name.starts_with(MUTATION_PARAM_PREFIX)
	}

	/// Whether the request's capabilities allow a call of the custom function `name`, without the
	/// `fn::` prefix.
	fn function_allowed(&self, name: &str) -> bool {
		self.ctx.get_capabilities().allows_function_name(&format!("fn::{name}"))
	}

	/// The state. Every update leaves it consistent, so a lock poisoned by a panic elsewhere is
	/// still sound to read.
	fn lock(&self) -> MutexGuard<'_, MemoState> {
		self.state.lock().unwrap_or_else(PoisonError::into_inner)
	}
}

/// Where a free name is bound, as [`PlanningCatalog::resolve`] finds it.
enum Resolved<'a> {
	/// Resolved only when the statement runs.
	RunTime,
	/// `$auth`, `$session`, `$token` or `$access`.
	Principal,
	/// The value bound at the statement, with where it came from.
	Bound(BoundValue<'a>, BindingOrigin),
	/// A stored parameter with `PERMISSIONS FULL`.
	Param(Arc<ParamDefinition>),
	/// Bound nowhere.
	Absent,
	/// A stored parameter the memo has not loaded.
	NotLoaded,
}

/// The value a name is bound to at the statement being planned.
enum BoundValue<'a> {
	/// The memo's context holds it.
	Context(&'a Value),
	/// The statement's binding holds it.
	Statement(Arc<Value>),
}

impl BoundValue<'_> {
	fn get(&self) -> &Value {
		match self {
			BoundValue::Context(value) => value,
			BoundValue::Statement(value) => value,
		}
	}
}

/// The answer for a held definition: `None` is a definition the catalog does not hold.
fn held_answer<T>(held: &Option<Arc<T>>) -> CatalogAnswer<Arc<T>> {
	match held {
		Some(def) => CatalogAnswer::Found(Arc::clone(def)),
		None => CatalogAnswer::Absent,
	}
}

/// Whether `error` is a catalog error that `matches` accepts.
fn is_catalog_error(error: &anyhow::Error, matches: impl FnOnce(&CatalogError) -> bool) -> bool {
	error.downcast_ref::<CatalogError>().is_some_and(matches)
}

impl SemanticCatalog for PlanningCatalog {
	fn memo_identity(&self) -> (u64, &'static str) {
		(self.lineage, std::any::type_name::<Self>())
	}

	fn database(&self) -> Option<Arc<DatabaseDefinition>> {
		PlanningCatalog::database(self)
	}

	fn calls_current(&self) -> bool {
		self.calls_current
	}

	fn plans_at_stamp(&self) -> bool {
		self.read_stamp.is_some()
	}

	fn function_limit_is_inert(&self, definition: &FunctionDefinition) -> bool {
		let Ok(limit) = AuthLimit::try_from(&definition.auth_limit) else {
			return false;
		};
		match &self.caller {
			Some(auth) => auth.new_limited(&limit) == **auth,
			None => definition.auth_limit == crate::catalog::auth::AuthLimit::new_no_limit(),
		}
	}

	fn table(&self, name: &TableName) -> CatalogAnswer<Arc<TableSnapshot>> {
		PlanningCatalog::table(self, name)
	}

	fn events(&self, table: &TableName) -> CatalogAnswer<Arc<[EventDefinition]>> {
		PlanningCatalog::events(self, table)
	}

	fn views(&self, table: &TableName) -> CatalogAnswer<Arc<[TableDefinition]>> {
		PlanningCatalog::views(self, table)
	}

	fn referencing(&self, table: &TableName) -> CatalogAnswer<Referencing> {
		PlanningCatalog::referencing(self, table)
	}

	fn edge_code(&self) -> CatalogAnswer<bool> {
		PlanningCatalog::edge_code(self)
	}

	fn field_view(&self, table: &TableName) -> CatalogAnswer<Arc<FieldView>> {
		PlanningCatalog::field_view(self, table)
	}

	fn function(&self, name: &str) -> CatalogAnswer<Arc<FunctionDefinition>> {
		PlanningCatalog::function(self, name)
	}

	fn param(&self, name: &str) -> CatalogAnswer<Arc<ParamDefinition>> {
		PlanningCatalog::param(self, name)
	}

	fn links_stages(&self) -> bool {
		self.links
	}

	fn target_table(&self, name: &str) -> Option<TableName> {
		self.targets.iter().find(|(target, _)| target == name).map(|(_, table)| table.clone())
	}

	fn free_name(&self, name: &str, at: FrameId) -> FreeName {
		PlanningCatalog::free_name(self, name, at)
	}

	fn free_kind(&self, name: &str, at: FrameId, consumer: FoldConsumer) -> FreeName {
		PlanningCatalog::free_kind(self, name, at, consumer)
	}

	fn free_value_exact(&self, name: &str, at: FrameId, consumer: FoldConsumer) -> ExactAnswer {
		PlanningCatalog::free_value_exact(self, name, at, consumer)
	}
}

impl PhysicalCatalog for PlanningCatalog {
	fn indexes(&self, table: &TableName) -> CatalogAnswer<Arc<[IndexDefinition]>> {
		PlanningCatalog::indexes(self, table)
	}

	fn layout(&self, table: &TableName) -> Option<LayoutFacts> {
		PlanningCatalog::layout(self, table)
	}

	fn stats_epoch(&self) -> Option<u64> {
		PlanningCatalog::stats_epoch(self)
	}

	fn read_stamp(&self) -> Option<u64> {
		self.read_stamp
	}

	fn index_ready(&self, index: &IndexRef) -> CatalogAnswer<bool> {
		PlanningCatalog::index_ready(self, index)
	}

	fn version_stamp(&self, at: &Datetime) -> Option<u64> {
		at.to_version_stamp(self.txn.version_impl().as_ref()).ok()
	}

	fn record_folded(&self, conjunct: &LogicalExpr) -> Option<LogicalExpr> {
		self.lock().record_folds.get(conjunct).cloned()
	}

	fn backend(&self) -> BackendCaps {
		PlanningCatalog::backend(self)
	}

	fn io_caps(&self) -> IoCaps {
		IoCaps {
			untracked_reads: !self.txn.plain_reads_tracked(),
			scan_read_ahead: self.txn.scan_read_ahead(),
			multi_read: self.txn.multi_read(),
			read_views: self.txn.view_capabilities().read_only,
		}
	}

	fn at_request_root(&self) -> bool {
		self.request_root
	}

	fn table_answers(&self, key: &SourceKey) -> CatalogAnswer<Arc<TableAnswers>> {
		PlanningCatalog::table_answers(self, key)
	}

	fn plans_unknown_reads(&self) -> bool {
		self.plans_unknown_reads
	}
}
