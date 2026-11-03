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

use std::sync::Arc;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::vm::program::Program;

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

/// A closure body as a host lowered it: the unit, and the table the ids it
/// holds index.
#[derive(Debug, Clone)]
pub(crate) struct ClosureUnit {
	pub(crate) unit: Arc<Program>,
	pub(crate) table: super::UnitTable,
}

/// The stages a write that may reach any table names, which a run loads for
/// each table by name ([`super::fault::serve_stage_bundle`]): the bound write
/// statement, which the bundle of each table binds again with that table, and
/// the runs of stages its body takes, in the order of its sites. Keyed by a
/// digest of both and compared whole, so two statements that share a digest
/// never share stages.
#[derive(Clone, Debug)]
pub(crate) struct StageSetUnit {
	digest: u64,
	pub(super) write: Arc<crate::expr::logical::LogicalWrite>,
	pub(super) sites: Vec<Vec<crate::exec::operators::write::stage::StageId>>,
}

/// The name a linked write's stage set reads its subjects by in place of its targets
/// ([`StageSetUnit::linked`]). No statement can spell it, so it names nothing a statement reads.
pub(super) const LINKED_SUBJECT: &str = "linked subject";

impl StageSetUnit {
	/// The stage set of a write whose stages are linked, `write` with its subjects read through
	/// [`LINKED_SUBJECT`] in place of the targets it names: the targets decide which records the
	/// statement writes and nothing its stages apply, so every statement of one shape shares one
	/// bundle of a table's stages whatever records it names.
	pub(super) fn linked(
		write: &crate::expr::logical::LogicalWrite,
		sites: Vec<Vec<crate::exec::operators::write::stage::StageId>>,
	) -> Self {
		Self::new(Arc::new(subjects_erased(write)), sites)
	}

	pub(super) fn new(
		write: Arc<crate::expr::logical::LogicalWrite>,
		sites: Vec<Vec<crate::exec::operators::write::stage::StageId>>,
	) -> Self {
		use std::hash::{Hash, Hasher};
		let mut hasher = std::hash::DefaultHasher::new();
		(&*write, &sites).hash(&mut hasher);
		Self {
			digest: hasher.finish(),
			write,
			sites,
		}
	}

	/// The names of every parameter the statement reads, in its clauses,
	/// its targets and its `RETURN` fields. A bundle compiled with them read
	/// when its stages run serves every value they hold.
	pub(super) fn read_names(&self) -> std::collections::BTreeSet<String> {
		use crate::expr::logical::{LogicalExpr, LogicalField, LogicalFields, LogicalOp};
		let mut names = std::collections::BTreeSet::new();
		let mut read = |expr: &LogicalExpr| {
			let _ = expr.visit_deep(&mut |node| {
				if let LogicalExpr::Param(param) = node {
					names.insert(param.as_str().to_owned());
				}
				std::ops::ControlFlow::<()>::Continue(())
			});
		};
		let _ = LogicalOp::Write((*self.write).clone()).visit_scalars_deep(&mut |scalar| {
			read(scalar);
			Ok::<(), std::convert::Infallible>(())
		});
		if let Some(crate::expr::output::Output::Fields(fields)) = self.write.statement.output()
			&& let Ok(fields) = crate::expr::logical::lower_fields(fields.clone())
		{
			match fields {
				LogicalFields::Value {
					expr,
					..
				} => read(&expr),
				LogicalFields::Select(fields) => {
					for field in &fields {
						if let LogicalField::Single {
							expr,
							..
						} = field
						{
							read(expr);
						}
					}
				}
			}
		}
		names.extend(self.param_names());
		names
	}

	/// The names of the parameters the statement reads inside a part that a
	/// lowering of its stages binds as a statement of its own: a graph lookup,
	/// a recursion or a subquery. Binding may substitute the value the planning
	/// context holds for such a name into the code, so these names are not
	/// folded. A name read anywhere else is read when the code runs. The bundle
	/// serves every run of the write, so none of them is folded from the run
	/// that compiles it.
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
			if let LogicalExpr::Param(param) = e {
				if bound {
					names.insert(param.as_str().to_owned());
				}
			} else if let LogicalExpr::Subquery(plan) = e {
				op(plan, bound, names);
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
		op(&LogicalOp::Write((*self.write).clone()), false, &mut names);
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
		self.digest == other.digest && self.sites == other.sites && self.write == other.write
	}
}

impl Eq for StageSetUnit {}

/// `write` reading its subjects through [`LINKED_SUBJECT`]: each target, the one record or
/// variable whose rows it writes, and the targets its statement holds as written.
fn subjects_erased(
	write: &crate::expr::logical::LogicalWrite,
) -> crate::expr::logical::LogicalWrite {
	use crate::expr::logical::{
		LogicalExpr, LogicalOp, LogicalSource, LogicalWrite, LogicalWriteSource,
		LogicalWriteStatement, SourceRef,
	};
	let subject = || crate::expr::Param::from(LINKED_SUBJECT.to_owned());
	let written = || crate::expr::Expr::Param(subject());
	let source = match &write.source {
		LogicalWriteSource::Targets(targets) => match &write.statement {
			// The edge table of a `RELATE` is the table its stages apply to.
			LogicalWriteStatement::Relate(_) => LogicalWriteSource::Targets(
				targets
					.iter()
					.enumerate()
					.map(|(at, target)| match at {
						1 => target.clone(),
						_ => LogicalExpr::Param(subject()),
					})
					.collect(),
			),
			LogicalWriteStatement::Create(_)
			| LogicalWriteStatement::Update(_)
			| LogicalWriteStatement::Upsert(_)
			| LogicalWriteStatement::Delete(_)
			| LogicalWriteStatement::Insert(_) => LogicalWriteSource::Targets(
				targets.iter().map(|_| LogicalExpr::Param(subject())).collect(),
			),
		},
		LogicalWriteSource::Rows(rows) => match rows.as_ref() {
			LogicalOp::Source(source) => {
				// What the catalog declares for the subject's rows is the subject's: the stages
				// read their table's definitions where they are compiled.
				LogicalWriteSource::Rows(Box::new(LogicalOp::Source(LogicalSource {
					source: SourceRef::Param(subject()),
					schema: None,
					..source.clone()
				})))
			}
			LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => write.source.clone(),
		},
		LogicalWriteSource::Inserts(_) => write.source.clone(),
	};
	let statement = match &write.statement {
		LogicalWriteStatement::Create(stmt) => {
			let mut stmt = stmt.clone();
			stmt.what = stmt.what.iter().map(|_| written()).collect();
			LogicalWriteStatement::Create(stmt)
		}
		LogicalWriteStatement::Update(stmt) => {
			let mut stmt = stmt.clone();
			stmt.what = stmt.what.iter().map(|_| written()).collect();
			LogicalWriteStatement::Update(stmt)
		}
		LogicalWriteStatement::Upsert(stmt) => {
			let mut stmt = stmt.clone();
			stmt.what = stmt.what.iter().map(|_| written()).collect();
			LogicalWriteStatement::Upsert(stmt)
		}
		LogicalWriteStatement::Delete(stmt) => {
			let mut stmt = stmt.clone();
			stmt.what = stmt.what.iter().map(|_| written()).collect();
			LogicalWriteStatement::Delete(stmt)
		}
		LogicalWriteStatement::Relate(stmt) => {
			let mut stmt = stmt.clone();
			stmt.from = written();
			stmt.to = written();
			LogicalWriteStatement::Relate(stmt)
		}
		LogicalWriteStatement::Insert(_) => write.statement.clone(),
	};
	LogicalWrite {
		statement,
		source,
		data: write.data.clone(),
		duplicate_update: write.duplicate_update.clone(),
		cond: write.cond.clone(),
		trace: write.trace,
	}
}

/// The values the planning context held, when a unit was lowered, for every
/// name the definitions its lowering read refer to; `None` for a name it
/// held no value for. Binding substitutes such a value into the code it
/// lowers, so the unit stands for another plan only where each name holds
/// the same value in that plan's context. A value is the one the code reads
/// there ([`crate::ctx::Context::lexical_value`]), which inside a function
/// body is none a caller bound with `LET` or `FOR`.
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
		let mut names = crate::exec::ir::cache::names::Collector::params(also);
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
				.into_params()
				.into_iter()
				.map(|name| {
					let value = ctx.lexical_value(&name).map(|value| value.as_ref().clone());
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
		self.values.iter().all(|(name, value)| match (ctx.lexical_value(name), value) {
			(Some(now), Some(then)) => super::compile_cache::same_value(now, then),
			(None, None) => true,
			_ => false,
		})
	}
}

/// The node's function-body, closure-body and catalog units; see the module.
pub(crate) struct CompiledUnits {
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
	/// Top-level statements of requests, by text ([`super::serve::request_statement`]),
	/// at most `ExecConfig::statement_cache_size` of them.
	request_statements: super::compile_cache::UnitCache<Arc<dyn crate::exec::ExecOperator>>,
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
	/// The bodies of functions compiled for a call's argument values, by
	/// function, version and values ([`super::call_unit::specialised`]),
	/// at most `ExecConfig::specialised_body_cache_size` of them.
	specialised_bodies: super::compile_cache::UnitCache<Arc<Program>>,
	/// How many distinct argument values a function's body is compiled for
	/// before its calls run its generic body (`ExecConfig::specialised_body_values`).
	specialised_values: usize,
	/// The size of the largest argument values a body compiled for them is
	/// kept for (`ExecConfig::specialised_body_max_arg_bytes`).
	specialised_arg_bytes: usize,
}

impl std::fmt::Debug for CompiledUnits {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompiledUnits").finish_non_exhaustive()
	}
}

/// The morsel test hooks name a datastore by the address of its units, so they forget it when
/// the units drop and a later datastore may take the address.
#[cfg(test)]
impl Drop for CompiledUnits {
	fn drop(&mut self) {
		super::morsel::forget_datastore(self as *const Self as usize);
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
	/// Whether compiles on this thread bind every write's stages as its own ops, as main does,
	/// for tests that compare a linked write with the inline one.
	pub(crate) static INLINE_STAGES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
	/// How many write statements this thread compiled again with their stages inline after a
	/// compile that linked them failed.
	pub(crate) static INLINED_AGAIN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many bundles of linked stages this thread's compiles compiled, and
	/// how many they took from the compile cache.
	pub(crate) static LINKED: std::cell::Cell<(usize, usize)> =
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
			table_reads: super::compile_cache::UnitCache::tables(capacity),
			field_states: super::compile_cache::UnitCache::tables(capacity),
			select_permissions: super::compile_cache::UnitCache::tables(capacity),
			stage_bundles: super::compile_cache::UnitCache::tables(capacity),
			exprs: super::compile_cache::UnitCache::functions(capacity),
			statements: super::compile_cache::UnitCache::functions(capacity),
			request_statements: super::compile_cache::UnitCache::functions(
				crate::exec::config::ExecConfig::default().statement_cache_size,
			),
			session_statements: super::compile_cache::UnitCache::functions(
				crate::exec::config::ExecConfig::default().specialised_body_cache_size,
			),
			closure_units: super::compile_cache::UnitCache::functions(capacity),
			function_calls: super::compile_cache::UnitCache::functions(capacity),
			specialised_bodies: super::compile_cache::UnitCache::functions(
				crate::exec::config::ExecConfig::default().specialised_body_cache_size,
			),
			specialised_values: crate::exec::config::ExecConfig::default().specialised_body_values,
			specialised_arg_bytes: crate::exec::config::ExecConfig::default()
				.specialised_body_max_arg_bytes,
		}
	}

	/// Units for the limits `config` sets.
	pub(crate) fn for_config(config: &crate::exec::config::ExecConfig) -> Self {
		let mut units = Self::new(config.compiled_unit_cache_size);
		units.request_statements =
			super::compile_cache::UnitCache::functions(config.statement_cache_size);
		units.specialised_bodies =
			super::compile_cache::UnitCache::functions(config.specialised_body_cache_size);
		units.session_statements =
			super::compile_cache::UnitCache::functions(config.specialised_body_cache_size);
		units.specialised_values = config.specialised_body_values;
		units.specialised_arg_bytes = config.specialised_body_max_arg_bytes;
		units
	}

	/// Whether the node keeps compiled top-level statements for later requests
	/// (`ExecConfig::statement_cache_size` is not `0`).
	pub(crate) fn caches_statements(&self) -> bool {
		self.request_statements.capacity() > 0
	}

	/// How the requests for compiled top-level statements were served.
	pub(crate) fn statement_stats(&self) -> super::compile_cache::Stats {
		let kept = self.request_statements.stats();
		let session = self.session_statements.stats();
		super::compile_cache::Stats {
			hits: kept.hits + session.hits,
			compiles: kept.compiles + session.compiles,
			waits: kept.waits + session.waits,
			stale: kept.stale + session.stale,
			private: kept.private + session.private,
		}
	}

	/// The size of the encoding of the largest argument values a body
	/// compiled for them is kept for. A body compiled for larger values
	/// serves its own call only and is not kept.
	pub(crate) fn specialised_arg_bytes(&self) -> usize {
		self.specialised_arg_bytes
	}

	/// The bundles of the stages of writes that may reach any table, and the
	/// orders of view tables' rows.
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

	/// Top-level statements of requests, by text.
	pub(crate) fn request_statements(
		&self,
	) -> &super::compile_cache::UnitCache<Arc<dyn crate::exec::ExecOperator>> {
		&self.request_statements
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
