//! The node's compiled function, closure and catalog units.
//!
//! A user-defined function as a call lowers it — its body programs, the
//! expression it inlines and the predicate that guards it — is a unit a node
//! keeps and later plans link instead of lowering the body again. A unit is
//! plain data: programs and the facts a lowering read off the definition. It
//! is looked up and lowered only while a query is planned, never while a
//! program runs.
//!
//! Function units are served from the compile cache
//! ([`super::compile_cache`]): keyed by the function and the version of its
//! definition, principal-generic, with one compilation per frame it is linked
//! into, single-flight on a miss, and checked against what else the
//! compilation read of the catalog ([`crate::exec::catalog_deps`]) — the
//! definitions of the functions its bundle holds, the tables its reads plan
//! against — before it is served. A body that binds a value from the planning
//! context — a session value or a client parameter its body reads from outside
//! — is not kept: the value is folded into its code and may differ for the
//! next request.
//!
//! What a transaction's uncommitted DDL defines is read under catalog
//! versions and content no other transaction sees until the DDL commits: a
//! table version is minted per write and is visible only to its writer, and
//! a function is identified by the content of its definition. A unit
//! compiled against uncommitted DDL is therefore served to no other
//! transaction until the same state commits, and to none at all once it
//! rolls back, when it ages out. A planning that speculates over a block's
//! own schema statements reads through an overlay, and neither looks up nor
//! keeps units.
//!
//! Closure bodies are kept by the compile cache too, by the closure as
//! written and the frame they were lowered for ([`super::closure::link`]).
//!
//! The node keeps catalog units the same way: what a plan lowers from the
//! catalog, identically for every statement of a database that needs it
//! ([`CatalogUnit`]) — the edge cascade of a delete. One is kept under
//! [`CatalogUnitKey`] — the unit, the instruction-set version, the frame it
//! was lowered for (the class of the principal, not its identity), the depth,
//! the names bound around it and the capabilities — with the catalog state its
//! lowering read, and the values the planning context held for every name the
//! definitions it read refer to, and for every name its own statement reads
//! where its lowering binds ([`ContextDeps`]), which binding may fold into its
//! code. A plan links it
//! while all of them hold, so a unit is compiled once per node for each state
//! of what it was compiled from.
//!
//! The stages a write that may reach any table applies to the records of one
//! table are kept by the compile cache, by the write ([`StageSetUnit`]), the
//! table and its catalog version, and loaded by name when a run writes a
//! record of the table ([`super::fault::serve_stage_bundle`]).
//!
//!
//! The node keeps at most the configured number of units
//! (`ExecConfig::compiled_unit_cache_size`) of each kind, evicting the least
//! recently used. A size of zero keeps none.

use std::sync::{Arc, Mutex};

use super::program::{DefHash, UnitFrame, UnitHead};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::catalog_deps::CatalogDeps;
use crate::exec::planner::Planner;
use crate::exec::vm::program::Program;
use crate::expr::logical::LogicalExpr;

/// How many units one key holds, one per catalog state it was compiled
/// against.
const VARIANTS: usize = 4;

/// A set of capabilities compared by identity. The key holds the set, so no
/// set made after it is freed can take its address and match a unit lowered
/// under it.
#[derive(Clone)]
pub(crate) struct CapabilitiesId(Arc<crate::dbs::Capabilities>);

impl CapabilitiesId {
	pub(crate) fn new(capabilities: Arc<crate::dbs::Capabilities>) -> Self {
		Self(capabilities)
	}
}

impl PartialEq for CapabilitiesId {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.0, &other.0)
	}
}

impl Eq for CapabilitiesId {}

impl std::hash::Hash for CapabilitiesId {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		Arc::as_ptr(&self.0).hash(state);
	}
}

impl std::fmt::Debug for CapabilitiesId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "CapabilitiesId({:p})", Arc::as_ptr(&self.0))
	}
}

/// The programs lowered for one body: the arena they and the functions they
/// reach are units of, which the first of them, the body itself, heads.
#[derive(Debug, Clone)]
pub(crate) struct Bundle {
	pub(crate) units: Arc<[Arc<Program>]>,
	pub(crate) head: UnitHead,
}

/// A function as a call of it lowers: what a later plan links.
#[derive(Debug, Clone)]
pub(crate) struct FunctionUnit {
	/// The hash of the definition everything here was lowered from.
	pub(crate) hash: DefHash,
	/// Whether the body is bound for a principal whose reads are checked.
	pub(crate) checked: bool,
	/// The expression a call inlines, with the names of the parameters it
	/// reads, for a body the call inlines.
	pub(crate) inline: Option<(Arc<LogicalExpr>, Vec<String>)>,
	/// The `PERMISSIONS` predicate and the `AUTH LIMIT` it is judged under,
	/// for a clause that is a predicate.
	pub(crate) guard: Option<(Arc<LogicalExpr>, crate::iam::AuthLimit)>,
	/// The body bound for the call's principal, for a call that is not
	/// inlined.
	pub(crate) body: Option<Bundle>,
	/// The body bound for the call's principal in a frame that waives
	/// permission checks, for a call in a frame that skips permissions.
	pub(crate) unchecked_body: Option<Bundle>,
	/// What running the body may do, the functions it calls named rather than
	/// folded in ([`super::footprint`]).
	#[cfg_attr(
		not(test),
		expect(
			dead_code,
			reason = "kept with the unit for the placement and reload decisions that read effect summaries; read by tests"
		)
	)]
	pub(crate) footprint: Arc<super::footprint::Footprint>,
}

/// A closure body as a host lowered it: the unit, and the table the ids it
/// holds index.
#[derive(Debug, Clone)]
pub(crate) struct ClosureUnit {
	pub(crate) unit: Arc<Program>,
	pub(crate) table: super::UnitTable,
}

/// A unit lowered from the catalog alone, as every plan in a database lowers
/// it the same.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CatalogUnit {
	/// A deleted record's edge cascade, `DELETE $__purge_vertex<->`, as a
	/// bundle whose entry is the statement, for a delete whose actor's own
	/// enforcement is suppressed where `caller_suppressed`.
	PurgeEdges {
		caller_suppressed: bool,
	},
}

impl CatalogUnit {
	/// The names of the parameters the unit's own source reads where its
	/// lowering may bind their values ([`StageSetUnit::param_names`]).
	pub(super) fn param_names(&self) -> std::collections::BTreeSet<String> {
		match self {
			CatalogUnit::PurgeEdges {
				..
			} => std::collections::BTreeSet::new(),
		}
	}
}

/// The stages a write that may reach any table names, which a run loads for
/// each table by name ([`super::fault::serve_stage_bundle`]): the bound write
/// statement, the runs of stages its body takes, and the frame its stages
/// run in. Keyed by a digest of all of them and compared whole, so two
/// statements that share a digest never share stages.
#[derive(Clone, Debug)]
pub(crate) struct StageSetUnit {
	digest: u64,
	pub(super) write: crate::expr::logical::LogicalWrite,
	pub(super) sites: Vec<Vec<crate::exec::operators::write::stage::StageId>>,
	/// Whether the stages are lowered ahead of the statement's run.
	pub(super) ahead: bool,
	/// Whether the actor's own frame suppresses permission enforcement, which
	/// the stages' edge cascades restore.
	pub(super) caller_suppressed: bool,
	/// Where the stages run beneath an edge cascade: the permission state of
	/// the actor the cascade restores.
	pub(super) edges: Option<bool>,
}

impl StageSetUnit {
	pub(super) fn new(
		write: crate::expr::logical::LogicalWrite,
		sites: Vec<Vec<crate::exec::operators::write::stage::StageId>>,
		ahead: bool,
		caller_suppressed: bool,
		edges: Option<bool>,
	) -> Self {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::hash::DefaultHasher::new();
		(&write, &sites, ahead, caller_suppressed, edges).hash(&mut hasher);
		Self {
			digest: hasher.finish(),
			write,
			sites,
			ahead,
			caller_suppressed,
			edges,
		}
	}

	/// The names of the parameters the statement reads inside what a
	/// lowering of its stages binds as a statement of its own — a graph
	/// lookup, a recursion, a subquery — where binding may substitute the
	/// value the planning context holds for the name into the code
	/// ([`ContextDeps`]). A name read anywhere else is read when the code
	/// runs.
	pub(super) fn param_names(&self) -> std::collections::BTreeSet<String> {
		use crate::expr::logical::{LogicalExpr, LogicalOp};
		type Names = std::collections::BTreeSet<String>;
		fn expr(e: &LogicalExpr, bound: bool, names: &mut Names) {
			let bound = bound
				|| matches!(
					e,
					LogicalExpr::Lookup { .. }
						| LogicalExpr::Recurse(_)
						| LogicalExpr::RepeatRecurse(_)
						| LogicalExpr::Subquery(_)
				);
			match e {
				LogicalExpr::Param(param) if bound => {
					names.insert(param.as_str().to_owned());
				}
				LogicalExpr::Subquery(plan) => op(plan, bound, names),
				_ => {}
			}
			e.for_each_child(&mut |child| expr(child, bound, names));
		}
		fn op(plan: &LogicalOp, bound: bool, names: &mut Names) {
			let _ = plan.visit_scalars_deep(&mut |scalar| {
				expr(scalar, bound, names);
				Ok::<(), std::convert::Infallible>(())
			});
		}
		let mut names = Names::new();
		op(&LogicalOp::Write(self.write.clone()), false, &mut names);
		names
	}
}

impl std::hash::Hash for StageSetUnit {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		state.write_u64(self.digest);
	}
}

impl PartialEq for StageSetUnit {
	fn eq(&self, other: &Self) -> bool {
		self.digest == other.digest
			&& self.ahead == other.ahead
			&& self.caller_suppressed == other.caller_suppressed
			&& self.edges == other.edges
			&& self.sites == other.sites
			&& self.write == other.write
	}
}

impl Eq for StageSetUnit {}

/// What a catalog unit was compiled for: everything its lowering
/// specializes on besides the catalog and the context values the
/// definitions it read refer to.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CatalogUnitKey {
	pub(crate) ns: String,
	pub(crate) db: String,
	pub(crate) ids: (NamespaceId, DatabaseId),
	pub(crate) unit: CatalogUnit,
	/// The instruction-set version the unit is lowered to
	/// ([`super::program::ISA_VERSION`]).
	pub(crate) isa: u32,
	/// The frame the unit is lowered for: the class of the principal
	/// ([`super::program::PrincipalClass`]), the namespace and database, and
	/// the permission contract.
	pub(crate) frame: UnitFrame,
	/// The re-entry depth the unit is planned at.
	pub(crate) depth: u32,
	/// The names bound around the unit, which binding leaves unsubstituted,
	/// sorted.
	pub(crate) shadowed: Vec<String>,
	/// The capabilities the unit was lowered under, by identity.
	pub(crate) capabilities: CapabilitiesId,
}

/// The values the planning context held, when a unit was lowered, for every
/// name the definitions its lowering read refer to; `None` for a name it
/// held no value for. Binding substitutes such a value into the code it
/// lowers, so the unit stands for another plan only where each name holds
/// the same value in that plan's context.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ContextDeps {
	values: Vec<(String, Option<crate::val::Value>)>,
}

impl ContextDeps {
	/// The values `ctx` holds for every name a definition `reads` names
	/// refers to, as `txn` reads the definitions, and for each of `also`;
	/// `None` where a definition cannot be read.
	pub(crate) async fn resolve(
		txn: &crate::kvs::Transaction,
		reads: &crate::kvs::catalog_reads::CatalogReads,
		ctx: &crate::ctx::FrozenContext,
		also: std::collections::BTreeSet<String>,
	) -> Option<Self> {
		use crate::catalog::providers::{DatabaseProvider, TableProvider};
		use crate::expr::visit::Visit as _;
		let mut names = Names(also);
		let mut tables: Vec<(NamespaceId, DatabaseId, crate::val::TableName)> =
			reads.tables.iter().cloned().collect();
		for (ns, db, tb) in &reads.named_tables {
			if let Some(def) = txn.get_db_by_name(ns, db, None).await.ok()? {
				tables.push((def.namespace_id, def.database_id, tb.clone()));
			}
		}
		for (ns, db, tb) in &tables {
			if let Some(def) = txn.get_tb(*ns, *db, tb, None).await.ok()? {
				let _ = def.permissions.visit(&mut names);
				if let Some(view) = &def.view {
					let _ = view.visit_clauses(&mut names);
				}
			}
			for field in txn.all_tb_fields(*ns, *db, tb, None).await.ok()?.iter() {
				for expr in [&field.value, &field.assert, &field.computed].into_iter().flatten() {
					let _ = expr.visit(&mut names);
				}
				if let crate::catalog::DefineDefault::Set(expr)
				| crate::catalog::DefineDefault::Always(expr) = &field.default
				{
					let _ = expr.visit(&mut names);
				}
				for permission in
					[&field.select_permission, &field.create_permission, &field.update_permission]
				{
					let _ = permission.visit(&mut names);
				}
			}
		}
		for (ns, db, name) in &reads.functions {
			match txn.get_db_function(*ns, *db, name, None).await {
				Ok(def) => {
					let _ = def.block.visit(&mut names);
					let _ = def.permissions.visit(&mut names);
				}
				Err(e)
					if matches!(
						e.downcast_ref::<crate::catalog::Error>(),
						Some(crate::catalog::Error::FcNotFound { .. })
					) => {}
				Err(_) => return None,
			}
		}
		Some(Self {
			values: names
				.0
				.into_iter()
				.map(|name| {
					let value = ctx.value(&name).cloned();
					(name, value)
				})
				.collect(),
		})
	}

	/// These values but those of `names`, which the unit's code reads when it
	/// runs and never folds ([`crate::exec::planner::UNIT_RUNTIME_PARAMS`]).
	pub(crate) fn without(mut self, names: &[&str]) -> Self {
		self.values.retain(|(name, _)| !names.contains(&name.as_str()));
		self
	}

	/// Whether `ctx` holds, for every name, the value it held when the unit
	/// was lowered: the same value, of the same type, not merely an equal one
	/// ([`super::compile_cache::same_value`]).
	pub(crate) fn hold(&self, ctx: &crate::ctx::FrozenContext) -> bool {
		self.values.iter().all(|(name, value)| match (ctx.value(name), value) {
			(Some(now), Some(then)) => super::compile_cache::same_value(now, then),
			(None, None) => true,
			_ => false,
		})
	}
}

/// Catalog unit `unit` for the frame `planner` lowers for: the one the node
/// keeps under its key while the catalog and the context values it was
/// lowered against hold, and otherwise the program `lower` lowers, which the
/// node keeps for later plans. Where the planner keeps nothing — a planner
/// [`Planner::unit_cache`] answers nothing for, or one planning with what
/// the key does not hold ([`Planner::catalog_unit_key`]) — and where the
/// lowering read something its recording cannot name, the program is
/// lowered for this plan alone.
///
/// A catalog unit is no part of the plan being lowered: whether a kept one
/// stands is judged against the catalog as the transaction reads it now, and
/// `lower` maps the unit's own statement, so it runs unsealed
/// ([`crate::kvs::catalog_reads::sealed`]); the lowering of that statement
/// is sealed as any.
pub(super) async fn catalog_unit(
	planner: &Planner<'_>,
	unit: CatalogUnit,
	lower: impl std::future::Future<Output = super::lower::Lowered<Program>>,
) -> super::lower::Lowered<Arc<Program>> {
	crate::kvs::catalog_reads::sealed(false, catalog_unit_unsealed(planner, unit, lower)).await
}

async fn catalog_unit_unsealed(
	planner: &Planner<'_>,
	unit: CatalogUnit,
	lower: impl std::future::Future<Output = super::lower::Lowered<Program>>,
) -> super::lower::Lowered<Arc<Program>> {
	let cache = planner.unit_cache();
	let key = match &cache {
		Some(_) => {
			crate::exec::vm::host::out_of_line(|| {
				planner.catalog_unit_key(unit, super::lower::statement_frame(planner))
			})
			.await
		}
		None => None,
	};
	let cache = cache.filter(|_| key.is_some());
	if let (Some((units, txn)), Some(key)) = (&cache, &key) {
		if let Some(kept) = units.catalog_unit(key, txn, planner.frozen()).await {
			return Ok(kept);
		}
		units.note_catalog_unit_lowered();
	}
	let recording = cache.as_ref().map(|(_, txn)| txn.record_catalog_reads());
	let lowered = crate::exec::vm::host::out_of_line(|| lower).await;
	let reads = recording.map(|recording| recording.finish());
	let program = Arc::new(lowered?);
	if let (Some((units, txn)), Some(key), Some(reads)) = (cache, key, reads)
		&& let Some(deps) = CatalogDeps::resolve(&txn, &reads).await
		&& let Some(context) =
			ContextDeps::resolve(&txn, &reads, planner.frozen(), key.unit.param_names()).await
	{
		units.keep_catalog_unit(&key, Arc::clone(&program), deps, context);
	}
	Ok(program)
}

pub(crate) use crate::exec::catalog_deps::Referencing;

/// What the node keeps a database's `REFERENCE` fields under: the database.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReferencingKey {
	ids: (NamespaceId, DatabaseId),
}

/// The `REFERENCE` fields of the database `ids` names that can name a record
/// of `target`, or of any table where it is `None`, as `txn` reads the
/// catalog: the fields each table defines with a `REFERENCE` clause whose
/// kind may hold a record of the table, by the table that defines them, in
/// the database's table order.
///
/// The fields are found from those the node keeps for the database
/// ([`reference_fields`]), so lowering the repairs of each table of a
/// database reads the fields of every table at most once, and after a schema
/// change only those of the tables it changed. Nothing is noted in the open
/// recordings: the compilation that takes them notes what it depends on
/// ([`note_referencing`]).
pub(super) async fn referencing_unnoted(
	planner: &Planner<'_>,
	txn: &crate::kvs::Transaction,
	ids: (NamespaceId, DatabaseId),
	target: Option<&crate::val::TableName>,
) -> super::lower::Lowered<Referencing> {
	let read = reference_fields(planner, txn, ids).await?;
	Ok(crate::exec::catalog_deps::references_to(&read, target))
}

/// Note in `txn`'s open recordings what a compilation lowered from `found`,
/// the `REFERENCE` fields that can name a record of `target`, depends on:
/// those fields, which a change to a field of any table can change, and the
/// catalog of each table that defines one, which its repair is lowered from
/// ([`crate::exec::catalog_deps`]). A change to a table none of those name,
/// whatever it is, leaves the compilation standing.
pub(super) fn note_referencing(
	txn: &crate::kvs::Transaction,
	(ns, db): (NamespaceId, DatabaseId),
	target: Option<&crate::val::TableName>,
	found: &Referencing,
) {
	txn.note_references_read(ns, db, target);
	for (table, _) in found.iter() {
		txn.note_table_read(ns, db, table);
	}
}

/// The `REFERENCE` fields of every table of the database `ids` names, as
/// `txn` reads the catalog, with no read noted. The node keeps the latest it
/// found for the database, for a planner [`Planner::unit_cache`] answers for:
/// a transaction under the catalog epoch it was found under takes it as it
/// is, and any other reads the fields only of the tables whose catalog
/// version differs from the one it was found in
/// ([`crate::kvs::Transaction::reference_fields`]).
async fn reference_fields(
	planner: &Planner<'_>,
	txn: &crate::kvs::Transaction,
	ids: (NamespaceId, DatabaseId),
) -> super::lower::Lowered<Arc<crate::kvs::tx::ReferenceFields>> {
	let units = planner.unit_cache().map(|(units, _)| units);
	let key = ReferencingKey {
		ids,
	};
	let kept = units.as_ref().and_then(|units| units.referencing_kept(&key));
	if let Some((read, Some(epoch))) = &kept
		&& txn.catalog_epoch() == Some(*epoch)
	{
		return Ok(Arc::clone(read));
	}
	let previous = kept.as_ref().map(|(read, _)| read);
	let read = txn.reference_fields(ids.0, ids.1, previous).await.map_err(|e| {
		super::NotCovered::internal(format!("the database's REFERENCE fields did not read: {e}"))
	})?;
	#[cfg(test)]
	if previous.is_none_or(|previous| *previous.state != *read.state) {
		REFERENCING_FOUND.with(|found| found.set(found.get() + 1));
	}
	if let Some(units) = units {
		units.keep_referencing(&key, Arc::clone(&read), txn.catalog_epoch());
	}
	Ok(read)
}

/// The parameter names an expression refers to, at any depth.
#[derive(Default)]
struct Names(std::collections::BTreeSet<String>);

impl crate::expr::visit::Visitor for Names {
	type Error = std::convert::Infallible;

	fn visit_param(&mut self, param: &crate::expr::Param) -> Result<(), Self::Error> {
		self.0.insert(param.as_str().to_owned());
		Ok(())
	}
}

/// A catalog unit and what its lowering depended on.
struct KeptTable {
	unit: Arc<Program>,
	deps: Arc<CatalogDeps>,
	context: Arc<ContextDeps>,
}

/// The node's function-body, closure-body and catalog units; see the module.
pub(crate) struct CompiledUnits {
	/// The function units, by function and version.
	functions: super::compile_cache::UnitCache<Option<FunctionUnit>>,
	/// The read units of tables, by table and catalog version
	/// ([`super::dispatch::table_unit`]).
	table_reads: super::compile_cache::UnitCache<super::dispatch::TableReadUnit>,
	/// The field states of tables' reads, by table and catalog version
	/// ([`super::descriptor::field_state`]).
	field_states:
		super::compile_cache::UnitCache<Arc<crate::exec::operators::scan::pipeline::FieldState>>,
	/// The `SELECT` permissions of tables, by table and catalog version.
	select_permissions:
		super::compile_cache::UnitCache<crate::exec::permission::PhysicalPermission>,
	/// The bundles of the stages a write that may reach any table applies to
	/// the records of a table, by write, table and catalog version
	/// ([`super::fault::serve_stage_bundle`]).
	stage_bundles: super::compile_cache::UnitCache<Arc<Program>>,
	/// Expressions reached at run time through one the program did not compile,
	/// by text ([`super::serve::expr`]).
	exprs: super::compile_cache::UnitCache<crate::exec::Clause>,
	/// Statements reached at run time through an expression the program did not
	/// compile, by text ([`super::serve::statement`]).
	statements: super::compile_cache::UnitCache<Arc<dyn crate::exec::ExecOperator>>,
	/// Top-level statements of requests served for the session values they
	/// fold ([`super::serve::request_statement`]), at most
	/// `ExecConfig::specialised_body_cache_size` of them.
	session_statements: super::compile_cache::UnitCache<Arc<dyn crate::exec::ExecOperator>>,
	/// The units of closure bodies, by closure ([`super::closure::link`]),
	/// or the failure a body no unit of which is lowered reports.
	closure_units: super::compile_cache::UnitCache<Result<ClosureUnit, surrealdb_types::Error>>,
	/// The call units of user-defined functions, by function, arity and
	/// version, and the bodies compiled for a call's values
	/// ([`super::call_unit`]).
	function_calls: super::compile_cache::UnitCache<Arc<Program>>,
	/// The permission predicates of parameters, by parameter and clause
	/// ([`super::param_unit`]).
	param_guards: super::compile_cache::UnitCache<Arc<Program>>,
	/// The bodies of functions compiled for a call's argument values, by
	/// function, version and values ([`super::call_unit::specialised`]),
	/// at most `ExecConfig::specialised_body_cache_size` of them.
	specialised_bodies: super::compile_cache::UnitCache<Arc<Program>>,
	/// The body of each such function compiled to read its arguments when it
	/// runs, `None` where it does not compile, by function and version.
	generic_bodies: super::compile_cache::UnitCache<Option<Arc<Program>>>,
	/// How many distinct argument values each such function, by function and
	/// version, has had its body compiled for.
	specialisations: Option<
		quick_cache::sync::Cache<
			super::compile_cache::UnitKey,
			Arc<std::sync::atomic::AtomicUsize>,
		>,
	>,
	/// How many distinct argument values a function's body is compiled for
	/// before its calls run its generic body (`ExecConfig::specialised_body_values`).
	specialised_values: usize,
	/// The size of the largest argument values a body compiled for them is
	/// kept for (`ExecConfig::specialised_body_max_arg_bytes`).
	specialised_arg_bytes: usize,
	tables: Option<quick_cache::sync::Cache<CatalogUnitKey, Arc<Mutex<Vec<KeptTable>>>>>,
	referencing: Option<quick_cache::sync::Cache<ReferencingKey, KeptReferencing>>,
}

/// The `REFERENCE` fields of a database the node found last, and the catalog
/// epoch of the transaction that found them, where it names one.
type KeptReferencing = (Arc<crate::kvs::tx::ReferenceFields>, Option<u64>);

impl std::fmt::Debug for CompiledUnits {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompiledUnits").finish_non_exhaustive()
	}
}

impl Default for CompiledUnits {
	fn default() -> Self {
		Self::new(crate::exec::config::ExecConfig::default().compiled_unit_cache_size)
	}
}

#[cfg(test)]
thread_local! {
	/// How many function units this thread's plans compiled, and how many
	/// they took from the compile cache.
	pub(crate) static BODIES: std::cell::Cell<(usize, usize)> =
		const { std::cell::Cell::new((0, 0)) };
	/// How many catalog units this thread lowered that no kept unit stood
	/// for, and how many it took from kept units.
	pub(crate) static CATALOG_UNITS: std::cell::Cell<(usize, usize)> =
		const { std::cell::Cell::new((0, 0)) };
	/// Whether plans on this thread compile every body with the plan, as a
	/// node keeping no units does, for measurements that compare the two.
	pub(crate) static OFF: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
	/// How many times this thread read a database's `REFERENCE` fields from
	/// the catalog rather than from what the node keeps.
	pub(crate) static REFERENCING_FOUND: std::cell::Cell<usize> =
		const { std::cell::Cell::new(0) };
}

impl CompiledUnits {
	/// Units for at most `capacity` keys.
	pub(crate) fn new(capacity: usize) -> Self {
		Self {
			functions: super::compile_cache::UnitCache::functions(capacity),
			table_reads: super::compile_cache::UnitCache::tables(capacity),
			field_states: super::compile_cache::UnitCache::tables(capacity),
			select_permissions: super::compile_cache::UnitCache::tables(capacity),
			stage_bundles: super::compile_cache::UnitCache::tables(capacity),
			exprs: super::compile_cache::UnitCache::functions(capacity),
			statements: super::compile_cache::UnitCache::functions(capacity),
			session_statements: super::compile_cache::UnitCache::functions(
				crate::exec::config::ExecConfig::default().specialised_body_cache_size,
			),
			closure_units: super::compile_cache::UnitCache::functions(capacity),
			function_calls: super::compile_cache::UnitCache::functions(capacity),
			param_guards: super::compile_cache::UnitCache::functions(capacity),
			specialised_bodies: super::compile_cache::UnitCache::functions(
				crate::exec::config::ExecConfig::default().specialised_body_cache_size,
			),
			generic_bodies: super::compile_cache::UnitCache::functions(capacity),
			specialisations: (capacity > 0).then(|| quick_cache::sync::Cache::new(capacity)),
			specialised_values: crate::exec::config::ExecConfig::default().specialised_body_values,
			specialised_arg_bytes: crate::exec::config::ExecConfig::default()
				.specialised_body_max_arg_bytes,
			tables: (capacity > 0).then(|| quick_cache::sync::Cache::new(capacity)),
			referencing: (capacity > 0).then(|| quick_cache::sync::Cache::new(capacity)),
		}
	}

	/// Units for the limits `config` sets.
	pub(crate) fn for_config(config: &crate::exec::config::ExecConfig) -> Self {
		let mut units = Self::new(config.compiled_unit_cache_size);
		units.specialised_bodies =
			super::compile_cache::UnitCache::functions(config.specialised_body_cache_size);
		units.session_statements =
			super::compile_cache::UnitCache::functions(config.specialised_body_cache_size);
		units.specialised_values = config.specialised_body_values;
		units.specialised_arg_bytes = config.specialised_body_max_arg_bytes;
		units
	}

	/// The size of the encoding of the largest argument values a body
	/// compiled for them is kept for: one compiled for larger values holds
	/// them, and is compiled for its call alone.
	pub(crate) fn specialised_arg_bytes(&self) -> usize {
		self.specialised_arg_bytes
	}

	/// The `REFERENCE` fields the node found last for the database `key`
	/// names.
	fn referencing_kept(&self, key: &ReferencingKey) -> Option<KeptReferencing> {
		self.referencing.as_ref()?.get(key)
	}

	/// Keep `found`, the `REFERENCE` fields of the database `key` names, found
	/// by a transaction under catalog epoch `epoch`.
	fn keep_referencing(
		&self,
		key: &ReferencingKey,
		found: Arc<crate::kvs::tx::ReferenceFields>,
		epoch: Option<u64>,
	) {
		if let Some(referencing) = &self.referencing {
			referencing.insert(key.clone(), (found, epoch));
		}
	}

	/// The catalog unit kept under `key` whose catalog `txn` still reads as
	/// it was lowered against, and whose context values `ctx` still holds.
	pub(crate) async fn catalog_unit(
		&self,
		key: &CatalogUnitKey,
		txn: &crate::kvs::Transaction,
		ctx: &crate::ctx::FrozenContext,
	) -> Option<Arc<Program>> {
		let variants = self.tables.as_ref()?.get(key)?;
		let candidates: Vec<(Arc<Program>, Arc<CatalogDeps>, Arc<ContextDeps>)> = variants
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.iter()
			.map(|kept| (Arc::clone(&kept.unit), Arc::clone(&kept.deps), Arc::clone(&kept.context)))
			.collect();
		for (unit, deps, context) in candidates {
			if context.hold(ctx) && deps.hold(txn).await {
				#[cfg(test)]
				CATALOG_UNITS.with(|tables| {
					let (compiled, linked) = tables.get();
					tables.set((compiled, linked + 1));
				});
				return Some(unit);
			}
		}
		None
	}

	/// Keep catalog `unit`, lowered for `key` against `deps` and `context`.
	pub(crate) fn keep_catalog_unit(
		&self,
		key: &CatalogUnitKey,
		unit: Arc<Program>,
		deps: CatalogDeps,
		context: ContextDeps,
	) {
		let Some(tables) = &self.tables else {
			return;
		};
		let variants = tables.get_or_insert_with(key, || {
			Ok::<_, std::convert::Infallible>(Arc::new(Mutex::new(Vec::new())))
		});
		let Ok(variants) = variants;
		let mut variants = variants.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
		if variants.len() == VARIANTS {
			variants.remove(0);
		}
		variants.push(KeptTable {
			unit,
			deps: Arc::new(deps),
			context: Arc::new(context),
		});
	}

	/// Note a catalog unit lowered that no kept unit stood for.
	pub(crate) fn note_catalog_unit_lowered(&self) {
		#[cfg(test)]
		CATALOG_UNITS.with(|tables| {
			let (compiled, linked) = tables.get();
			tables.set((compiled + 1, linked));
		});
	}

	/// The bundles of the stages of writes that may reach any table.
	pub(crate) fn stage_bundles(&self) -> &super::compile_cache::UnitCache<Arc<Program>> {
		&self.stage_bundles
	}

	/// Expressions reached at run time through one the program did not compile.
	pub(crate) fn exprs(&self) -> &super::compile_cache::UnitCache<crate::exec::Clause> {
		&self.exprs
	}

	/// Statements reached at run time through an expression the program did not
	/// compile.
	pub(crate) fn statements(
		&self,
	) -> &super::compile_cache::UnitCache<Arc<dyn crate::exec::ExecOperator>> {
		&self.statements
	}

	/// Top-level statements of requests served for the session values they
	/// fold.
	pub(crate) fn session_statements(
		&self,
	) -> &super::compile_cache::UnitCache<Arc<dyn crate::exec::ExecOperator>> {
		&self.session_statements
	}

	/// The units of closure bodies, by closure, or the failure a body no
	/// unit of which is lowered reports.
	pub(crate) fn closure_units(
		&self,
	) -> &super::compile_cache::UnitCache<Result<ClosureUnit, surrealdb_types::Error>> {
		&self.closure_units
	}

	/// The function units, by function and version: `None` for a function
	/// a call cannot enter.
	pub(crate) fn functions(&self) -> &super::compile_cache::UnitCache<Option<FunctionUnit>> {
		&self.functions
	}

	/// The field states of tables' reads, by table and catalog version.
	pub(crate) fn field_states(
		&self,
	) -> &super::compile_cache::UnitCache<Arc<crate::exec::operators::scan::pipeline::FieldState>>
	{
		&self.field_states
	}

	/// The `SELECT` permissions of tables, by table and catalog version.
	pub(crate) fn select_permissions(
		&self,
	) -> &super::compile_cache::UnitCache<crate::exec::permission::PhysicalPermission> {
		&self.select_permissions
	}

	/// The permission predicates of parameters, by parameter and clause.
	pub(crate) fn param_guards(&self) -> &super::compile_cache::UnitCache<Arc<Program>> {
		&self.param_guards
	}

	/// The bodies of functions compiled for a call's argument values.
	pub(crate) fn specialised_bodies(&self) -> &super::compile_cache::UnitCache<Arc<Program>> {
		&self.specialised_bodies
	}

	/// The bodies of functions compiled to read their arguments when they run.
	pub(crate) fn generic_bodies(&self) -> &super::compile_cache::UnitCache<Option<Arc<Program>>> {
		&self.generic_bodies
	}

	/// Whether the body of the function `key` names has been compiled for as
	/// many distinct argument values as its calls may specialise it for, and
	/// otherwise counts one more where `compiling`.
	pub(crate) fn specialised_enough(
		&self,
		key: &super::compile_cache::UnitKey,
		compiling: bool,
	) -> bool {
		use std::sync::atomic::Ordering;
		let Some(counts) = &self.specialisations else {
			return true;
		};
		let count = counts
			.get_or_insert_with(key, || {
				Ok::<_, std::convert::Infallible>(Arc::new(std::sync::atomic::AtomicUsize::new(0)))
			})
			.unwrap_or_else(|never| match never {});
		if count.load(Ordering::Relaxed) >= self.specialised_values {
			return true;
		}
		if compiling {
			count.fetch_add(1, Ordering::Relaxed);
		}
		false
	}

	/// The call units of user-defined functions, by function, arity and
	/// version, and the bodies compiled for a call's values.
	pub(crate) fn function_calls(&self) -> &super::compile_cache::UnitCache<Arc<Program>> {
		&self.function_calls
	}

	/// The read units of tables, by table and catalog version.
	pub(crate) fn table_reads(
		&self,
	) -> &super::compile_cache::UnitCache<super::dispatch::TableReadUnit> {
		&self.table_reads
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::CapabilitiesId;
	use crate::dbs::Capabilities;

	/// Two sets of capabilities are one key only when they are one set, and a
	/// key holds its set, so no set made after the first is dropped matches
	/// it, whatever address it is given.
	#[test]
	fn a_capabilities_key_is_the_set_it_holds() {
		let first = Arc::new(Capabilities::all());
		let key = CapabilitiesId::new(Arc::clone(&first));
		assert_eq!(key, CapabilitiesId::new(Arc::clone(&first)));
		assert_ne!(key, CapabilitiesId::new(Arc::new(Capabilities::all())));
		drop(first);
		for _ in 0..64 {
			assert_ne!(key, CapabilitiesId::new(Arc::new(Capabilities::all())));
		}
	}
}
