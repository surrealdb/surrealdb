//! What one statement owns across every table and every nesting level it
//! writes.
//!
//! A [`WriteCtx`] is what a statement resolves once per table; this is what it
//! resolves once per *statement*, and it is the only thing a nested write can
//! be reached through. Three kinds of state live here:
//!
//! - the resolved write contexts, keyed by table **and** purpose, so a table written directly and
//!   the same table written as a view target do not share a clause set;
//! - the compiled plans of the statements a write performs on its own behalf — the referential
//!   cascades a purge runs — so a delete cascading over ten thousand references compiles once;
//! - the frame each of those runs in, derived from the statement's.
//!
//! # Reach
//!
//! [`WriteCtx`] holds a [`Weak`] back to the scope that resolved it, rather
//! than the stage signature carrying it: a fourth argument on
//! [`WriteStage::apply`](super::stage::WriteStage::apply) would touch every
//! stage body and every direct-call test to serve the three that need it. The
//! reference is weak because the scope owns the contexts; the upgrade cannot
//! fail while a stage runs, since the operator's stream holds the `Arc` for its
//! whole life, so [`WriteCtx::scope`] reports a broken invariant rather than a
//! condition a caller handles.
//!
//! # Re-entrancy
//!
//! Both caches are behind a mutex a nested write may ask for while an outer
//! one is resolving: a cascade resolves its own context, and the record it
//! deletes may cascade again. Neither lock is therefore held across the
//! resolution it guards — the entry is looked up, the lock dropped, the work
//! done, and the lock retaken to publish — so the second asker finds the
//! entry and no frame waits on a lock the frame beneath it holds.

use std::sync::Arc;

use tokio::sync::Mutex;

use super::clauses::WriteClauses;
use super::ctx::{TableExistence, WriteCtx};
use super::plan::{WritePlan, WriteProgram};
use super::row::WriteRow;
use crate::catalog::Record;
use crate::doc::Action;
use crate::exec::permission::view_frame;
use crate::exec::planner::Planner;
use crate::exec::vm::Subroutine;
use crate::exec::{Error as ExecError, ExecutionContext, PhysicalExpr};
use crate::expr::ControlFlow;
use crate::expr::logical::LogicalOp;
use crate::expr::output::Output;
use crate::val::{RecordId, TableName, Value};

/// Whose write a resolved context is.
///
/// Part of the cache key, and what selects the clauses, the output, the table's
/// existence requirement and the frame. Keying by table alone would be silently
/// wrong: an `INSERT` whose rows name their own tables can write directly to a
/// table that is also the view target of another table the same statement
/// writes, and whichever of the two resolved first would hand the other its
/// clause set. It also keeps [`TableViews`](crate::exec::table_view::TableViews)'
/// rule that one resolver must not be shared across contexts that differ in
/// permission frame true by construction rather than by accident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WritePurpose {
	/// The statement's own write: its clauses, its `RETURN`, its frame, and the
	/// existence requirement its form carries.
	Statement,
	/// A row this statement synthesised on a view table. It has no clauses and
	/// no `RETURN` of its own, the table is defined on demand as
	/// `get_or_add_tb` defines it, and it runs in the view frame.
	ViewMaintenance,
}

/// A statement a write performs on its own behalf, compiled once per scope.
///
/// The key is what makes two such statements the same plan. It never carries
/// the record: the record travels as a bound parameter, so a cascade over many
/// references compiles one plan and runs it per record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NestedKey {
	/// The delete a purged vertex's graph edges receive.
	PurgeEdges,
	/// The read of a referencing record that decides which shape its
	/// `ON DELETE UNSET` takes, addressed by the referencing table.
	ReadReferencing {
		/// The table the referencing record belongs to.
		table: TableName,
	},
	/// One `REFERENCE ON DELETE` clause, addressed by the field it is declared
	/// on and by the shape it takes. The shape is part of the key because a
	/// field holding a link and a field holding a collection of links need
	/// different statements, and which one applies is decided per record.
	Reference {
		/// The table the referencing field is declared on.
		table: TableName,
		/// The referencing field's path, as the definition names it.
		field: String,
		/// The statement shape the clause takes for this record.
		shape: ReferenceShape,
	},
	/// The re-read of an aggregated view's group that recomputes the extremes
	/// a change left unmaintainable, addressed by the view table and by what
	/// it recomputes: each recomputing function and the argument it reads.
	/// The group's key values are bound parameters.
	ViewRecalculation {
		/// The view table.
		view: TableName,
		/// Each extreme recomputed, as its function and argument index.
		extremes: Vec<(String, usize)>,
	},
}

/// What a `REFERENCE ON DELETE` clause does to the referencing record.
///
/// `Ignore` and `Reject` are absent because neither runs a statement: the first
/// does nothing and the second raises.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReferenceShape {
	/// `ON DELETE CASCADE` — the referencing record is deleted.
	Cascade,
	/// `ON DELETE UNSET` where the field holds a single link, which is removed.
	Unset,
	/// `ON DELETE UNSET` where the field holds a collection, from which the
	/// link is subtracted.
	Subtract,
	/// `ON DELETE THEN <expr>` — the definition's own expression, which is a
	/// clause rather than a statement and is compiled by
	/// [`WriteScope::nested_clause`].
	Custom,
}

/// The write contexts a scope has resolved: one per table per purpose.
///
/// A `Vec` rather than a map because a statement writes to a handful of tables
/// at most, and the key is compared rather than hashed.
type ResolvedContexts = Vec<((TableName, WritePurpose), Arc<WriteCtx>)>;

/// The write contexts [`KeptContexts`] keeps, by table, purpose and the
/// version they were read at.
type KeptWriteContexts = Vec<((TableName, WritePurpose, Option<u64>), Arc<WriteCtx>)>;

/// What a statement run again and again compiled on earlier runs, while the
/// catalog it read holds: a block statement's across its reaches in one
/// epoch.
#[derive(Debug, Default)]
pub(crate) struct KeptContexts {
	/// The write contexts. A later run takes their catalog reads rather than reading again,
	/// and compiles its own expressions.
	contexts: std::sync::Mutex<KeptWriteContexts>,
	/// The nested statements, as [`WriteScope::nested_plan`] keys them.
	nested: std::sync::Mutex<Vec<(NestedPlanKey, Arc<Subroutine>)>>,
}

/// What identifies a compiled nested plan: the statement it compiles, and
/// whether the frame that asked for it suppresses permission enforcement.
type NestedPlanKey = (NestedKey, bool);

/// What one statement owns across every table and nesting level it writes.
#[derive(Debug)]
pub(crate) struct WriteScope {
	/// The context the statement itself runs in, and the frame every derived
	/// one is derived from.
	exec: ExecutionContext,
	/// The statement's compiled clauses.
	clauses: Arc<WriteClauses>,
	/// The clause set a write with no clauses of its own is given, built once
	/// rather than per view table.
	no_clauses: Arc<WriteClauses>,
	/// The statement's `RETURN`.
	output: Option<Output>,
	/// Whether the statement's own form requires its table to exist.
	existence: TableExistence,
	/// The write contexts resolved so far.
	contexts: Mutex<ResolvedContexts>,
	/// The contexts earlier runs of the same statement resolved, whose
	/// catalog reads this one takes; `None` for a statement run once.
	kept: Option<Arc<KeptContexts>>,
	/// Whether a context resolved earlier is checked against the catalog
	/// before it is taken again, and resolved anew where its table's catalog
	/// changed: the scope of a late-bound write, whose tables' catalog may
	/// change while it runs.
	revalidates: bool,
	/// The view-maintenance program, one per action, bound on first use. A
	/// statement that maintains no view binds none.
	view_programs: Mutex<[Option<Arc<WriteProgram>>; 3]>,
	/// The nested statements compiled so far, bounded by the number of distinct
	/// `REFERENCE` fields targeting the tables this statement writes.
	///
	/// Keyed by the statement **and** by whether the frame that asked for it
	/// suppresses permission enforcement, because a compiled plan carries the
	/// enforcement it was bound under: the permission filter and the field
	/// guards are nodes of the plan, not a decision the runtime remakes. One
	/// statement can ask for the same nested plan from frames that differ in
	/// it — the rows it writes on a view table are maintained unenforced, and
	/// its own records are not.
	nested: Mutex<Vec<(NestedPlanKey, Arc<Subroutine>)>>,
	/// The stored clauses a write evaluates on its own behalf, compiled so far
	/// and bounded the same way. Separate from `nested` because a clause is an
	/// expression evaluated against a row the write binds, not a statement with
	/// a source of its own.
	clauses_compiled: Mutex<Vec<(NestedKey, Arc<dyn PhysicalExpr>)>>,
}

impl WriteScope {
	/// Open the scope for one statement.
	pub(crate) fn new(
		exec: ExecutionContext,
		clauses: Arc<WriteClauses>,
		output: Option<Output>,
		existence: TableExistence,
	) -> Arc<Self> {
		Arc::new(Self {
			exec,
			clauses,
			no_clauses: Arc::new(WriteClauses::default()),
			output,
			existence,
			contexts: Mutex::new(Vec::new()),
			kept: None,
			revalidates: false,
			view_programs: Mutex::new([None, None, None]),
			nested: Mutex::new(Vec::new()),
			clauses_compiled: Mutex::new(Vec::new()),
		})
	}

	/// [`Self::new`] for a late-bound write: each context it resolves is
	/// checked against the catalog before it is taken again
	/// ([`WriteCtx::stands`]).
	pub(crate) fn revalidating(
		exec: ExecutionContext,
		clauses: Arc<WriteClauses>,
		output: Option<Output>,
		existence: TableExistence,
	) -> Arc<Self> {
		let mut scope = Self::new(exec, clauses, output, existence);
		Arc::get_mut(&mut scope).expect("a scope just made has no other owner").revalidates = true;
		scope
	}

	/// Open the scope for one run of a statement run again and again, taking
	/// the catalog reads of the contexts earlier runs left in `kept` and
	/// leaving its own there.
	///
	/// The caller guarantees the catalog has not changed since `kept` was
	/// first filled.
	pub(crate) fn kept(
		exec: ExecutionContext,
		clauses: Arc<WriteClauses>,
		output: Option<Output>,
		existence: TableExistence,
		kept: Arc<KeptContexts>,
	) -> Arc<Self> {
		let mut scope = Self::new(exec, clauses, output, existence);
		Arc::get_mut(&mut scope).expect("a scope just opened is not shared").kept = Some(kept);
		scope
	}

	/// The context the statement itself runs in.
	pub(crate) fn exec(&self) -> &ExecutionContext {
		&self.exec
	}

	/// The write context for one table and one purpose, resolved on first use.
	///
	/// Resolution reads the catalog and compiles expressions, which must not
	/// happen per record; a statement that writes to more than one table — an
	/// `INSERT` whose rows name their own — holds one of these per table.
	pub(crate) async fn write_ctx_for(
		self: &Arc<Self>,
		table: &TableName,
		purpose: WritePurpose,
	) -> Result<Arc<WriteCtx>, ControlFlow> {
		if let Some(resolved) = self.cached_ctx(table, purpose).await {
			if !self.revalidates || resolved.stands().await? {
				return Ok(resolved);
			}
			self.contexts.lock().await.retain(|((t, p), _)| !(t == table && *p == purpose));
		}
		let (exec, existence, output, clauses) = match purpose {
			WritePurpose::Statement => {
				(self.exec.clone(), self.existence, self.output.clone(), Arc::clone(&self.clauses))
			}
			WritePurpose::ViewMaintenance => (
				view_frame(&self.exec).dive().map_err(ControlFlow::Err)?,
				TableExistence::DefinedOnDemand,
				None,
				Arc::clone(&self.no_clauses),
			),
		};
		let key = (table.clone(), purpose, exec.version_stamp());
		let earlier = self.kept.as_ref().and_then(|kept| {
			let kept = kept.contexts.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
			kept.iter().find(|(k, _)| *k == key).map(|(_, resolved)| Arc::clone(resolved))
		});
		let same_database = |earlier: &WriteCtx| {
			exec.database().is_ok_and(|db| {
				db.ns_ctx.ns.namespace_id == earlier.ns.namespace_id
					&& db.db.database_id == earlier.db.database_id
			})
		};
		let mut resolved = match earlier {
			Some(earlier) if same_database(&earlier) => earlier.rebound(&exec, output, clauses),
			_ => {
				let resolved = crate::exec::vm::descriptor::write_unit(WriteCtx::resolve(
					&exec, table, existence, output, clauses,
				))
				.await?;
				if let Some(kept) = &self.kept {
					let mut kept =
						kept.contexts.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
					kept.retain(|(k, _)| *k != key);
					kept.push((
						key,
						Arc::new(resolved.rebound(&exec, None, Arc::clone(&self.no_clauses))),
					));
				}
				resolved
			}
		};
		resolved.scope = Arc::downgrade(self);
		let resolved = Arc::new(resolved);
		let mut contexts = self.contexts.lock().await;
		// Resolution awaits, so a nested write may have published the same
		// entry meanwhile. Its context is as good as this one and is the one
		// already in use, so it wins and this one is dropped.
		if let Some((_, published)) =
			contexts.iter().find(|((t, p), _)| t == table && *p == purpose)
		{
			return Ok(Arc::clone(published));
		}
		contexts.push(((table.clone(), purpose), Arc::clone(&resolved)));
		Ok(resolved)
	}

	async fn cached_ctx(&self, table: &TableName, purpose: WritePurpose) -> Option<Arc<WriteCtx>> {
		let contexts = self.contexts.lock().await;
		contexts
			.iter()
			.find(|((t, p), _)| t == table && *p == purpose)
			.map(|(_, resolved)| Arc::clone(resolved))
	}

	/// Run the view-maintenance order over a row this write synthesised on
	/// `table`.
	///
	/// The row is driven through the program directly rather than through the
	/// write operator's per-record path, because that path is the only place
	/// the statement's affected-row total is counted and a view row is not one
	/// of the statement's records.
	pub(crate) async fn run_view_row(
		self: &Arc<Self>,
		table: &TableName,
		id: RecordId,
		action: Action,
		initial: Option<&Record>,
		current: Option<&Record>,
	) -> Result<(), ControlFlow> {
		let ctx = self.write_ctx_for(table, WritePurpose::ViewMaintenance).await?;
		let program = self.view_program(action).await?;
		let mut row = WriteRow::for_view_row(id, initial, current);
		program.run_record(&mut row, &ctx).await.map(|_| ())
	}

	/// The view-maintenance program for one action, bound on first use.
	async fn view_program(&self, action: Action) -> Result<Arc<WriteProgram>, ControlFlow> {
		let slot = match action {
			Action::Create => 0,
			Action::Update => 1,
			Action::Delete => 2,
		};
		let mut programs = self.view_programs.lock().await;
		if let Some(program) = &programs[slot] {
			return Ok(Arc::clone(program));
		}
		let program = Arc::new(WritePlan::view_maintenance(action).map_err(|missing| {
			ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
				"maintaining a view table cannot run on the engine: the {} stage is \
				 not implemented",
				missing.name()
			))))
		})?);
		programs[slot] = Some(Arc::clone(&program));
		Ok(program)
	}

	/// The plan for a statement this write performs on its own behalf,
	/// compiled on first use and reused for every record after it.
	///
	/// `build` is called only on a miss, because building the logical form
	/// reads the definition the statement comes from.
	///
	/// `row_scoped` names the parameters the plan binds per record, which
	/// binding must not substitute a value for. Omitting one is silent:
	/// [`resolve_params`](crate::exec::logical::params) substitutes an unknown
	/// name in a source position as `NONE`, so a `DELETE $target` compiled
	/// without its name becomes `DELETE NONE`, which deletes nothing and
	/// raises nothing.
	pub(crate) async fn nested_plan(
		&self,
		key: NestedKey,
		exec: &ExecutionContext,
		row_scoped: &[&str],
		build: impl FnOnce() -> Result<LogicalOp, ControlFlow>,
	) -> Result<Arc<Subroutine>, ControlFlow> {
		#[cfg(test)]
		if !matches!(key, NestedKey::ViewRecalculation { .. }) {
			CASCADE_PLANS.with(|plans| plans.set(plans.get() + 1));
		}
		let key = (key, exec.root().perms_suppressed);
		if let Some(plan) = self.cached_plan(&key).await {
			return Ok(plan);
		}
		let earlier = self.kept.as_ref().and_then(|kept| {
			let kept = kept.nested.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
			kept.iter().find(|(k, _)| *k == key).map(|(_, plan)| Arc::clone(plan))
		});
		let plan = match earlier {
			Some(plan) => plan,
			None => {
				let plan = compile_nested(build()?, exec, row_scoped, self.kept.as_ref()).await?;
				if let Some(kept) = &self.kept {
					let mut kept =
						kept.nested.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
					if !kept.iter().any(|(k, _)| *k == key) {
						kept.push((key.clone(), Arc::clone(&plan)));
					}
				}
				plan
			}
		};
		let mut nested = self.nested.lock().await;
		if let Some((_, published)) = nested.iter().find(|(k, _)| *k == key) {
			return Ok(Arc::clone(published));
		}
		nested.push((key, Arc::clone(&plan)));
		Ok(plan)
	}

	async fn cached_plan(&self, key: &NestedPlanKey) -> Option<Arc<Subroutine>> {
		let nested = self.nested.lock().await;
		nested.iter().find(|(k, _)| k == key).map(|(_, plan)| Arc::clone(plan))
	}

	/// A stored clause this write evaluates on its own behalf, compiled on
	/// first use and reused for every record after it.
	///
	/// [`Self::nested_plan`]'s counterpart for a clause. `build` is a future
	/// rather than a value because it is only polled on a miss: compiling a
	/// clause is the decision to run it, so a clause the planner declines must
	/// not fail a record whose strategy never reaches it.
	pub(crate) async fn nested_clause(
		&self,
		key: NestedKey,
		build: impl Future<Output = Result<Arc<dyn PhysicalExpr>, ControlFlow>>,
	) -> Result<Arc<dyn PhysicalExpr>, ControlFlow> {
		if let Some(clause) = self.cached_clause(&key).await {
			return Ok(clause);
		}
		let clause = build.await?;
		let mut compiled = self.clauses_compiled.lock().await;
		if let Some((_, published)) = compiled.iter().find(|(k, _)| *k == key) {
			return Ok(Arc::clone(published));
		}
		compiled.push((key, Arc::clone(&clause)));
		Ok(clause)
	}

	async fn cached_clause(&self, key: &NestedKey) -> Option<Arc<dyn PhysicalExpr>> {
		let compiled = self.clauses_compiled.lock().await;
		compiled.iter().find(|(k, _)| k == key).map(|(_, clause)| Arc::clone(clause))
	}
}

/// Run a nested plan to completion and collect what it produced.
///
/// The frame is entered one re-entry deeper, which is what bounds a cascade
/// that cascades again: [`ExecutionContext::dive`] spends the computation-depth
/// budget, so one `max_computation_depth` covers a chain however its hops are
/// nested. The dive
/// is taken here rather than by the caller so that no nested execution can be
/// written without it.
pub(crate) async fn run_nested(
	plan: &Subroutine,
	exec: &ExecutionContext,
) -> Result<Vec<Value>, ControlFlow> {
	let exec = exec.dive().map_err(ControlFlow::Err)?;
	plan.run(&exec).await
}

/// Compile and run one nested statement that is not worth caching.
///
/// Both callers run their statement once against a plan that serves only it: an
/// aggregate recalculation splices the group's values in as literals, and a
/// view's backfill reads the whole of the view's source. Either way the scan
/// dwarfs the planning, so the plan is built where it is used and dropped with
/// it.
pub(crate) async fn run_once(
	op: LogicalOp,
	exec: &ExecutionContext,
	row_scoped: &[&str],
) -> Result<Vec<Value>, ControlFlow> {
	let plan = compile_nested(op, exec, row_scoped, None).await?;
	run_nested(&plan, exec).await
}

/// Bind a nested statement for this write's transaction and principal and
/// lower it to a VM subroutine.
///
/// Neither free planner entry point serves this. `plan_statement_inner`
/// declines a write before binding, which is how a form whose stages are not
/// all bound is refused, and this statement is one the engine is choosing to
/// run. `plan_logical_inner` skips binding altogether, which would cost the
/// nested statement its permission layers and its computed fields. So the two
/// halves are called directly, which is `plan_statement_inner` minus the
/// pre-flight.
///
/// The planner's re-entry depth continues from what the frame has already
/// spent, so a nested statement is compiled at the depth it will run at rather
/// than from zero. The subroutine's writes keep what they compile in `kept`,
/// the asking scope's, so the levels of a cascade share one lowering of each
/// statement they reach. A statement the VM does not cover fails with the
/// reason.
async fn compile_nested(
	op: LogicalOp,
	exec: &ExecutionContext,
	row_scoped: &[&str],
	kept: Option<&Arc<KeptContexts>>,
) -> Result<Arc<Subroutine>, ControlFlow> {
	let subroutine = crate::exec::vm::descriptor::write_unit(async {
		let planner = planner_for(exec)?;
		let bound = Box::pin(planner.bind_scoped(op, row_scoped))
			.await
			.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
		Box::pin(Subroutine::lower(&planner, &bound, kept.cloned()))
			.await
			.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
	})
	.await?;
	#[cfg(test)]
	NESTED_ON_VM.with(|lowered| lowered.set(lowered.get() + 1));
	Ok(subroutine)
}

#[cfg(test)]
thread_local! {
	/// How many times this thread asked for the plan of a cascade's statement,
	/// for tests that must know a write run as bytecode asked for none.
	pub(crate) static CASCADE_PLANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many nested statements this thread has lowered to the VM, for
	/// tests that must know a cascade or a view's re-read ran as bytecode.
	pub(crate) static NESTED_ON_VM: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The planner a write compiles its own nested work through: this write's
/// transaction, database, principal and enforcement.
///
/// The re-entry depth continues from what the frame has already spent, so
/// nested work is compiled at the depth it will run at rather than from zero.
///
/// The frame's suppression is carried too, because it decides what the plan
/// computes and not only what it is allowed to return: a suppressed frame
/// reads every row of the tables it names, so the aggregate a view
/// recalculation stores is the view's and not the writer's view of it. The
/// runtime's scan gate honours suppression on its own, but the nodes binding
/// plants — the permission filter over a source, the guard columns over its
/// fields — are compiled in or not at all.
pub(crate) fn planner_for(exec: &ExecutionContext) -> Result<Planner<'_>, ControlFlow> {
	let db_ctx = exec.database().map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?;
	let ns = db_ctx.ns_ctx.ns.name.as_str().to_owned();
	let db = db_ctx.db.name.as_str().to_owned();
	let budget = exec.ctx().config.exec.max_computation_depth;
	let depth = exec.options().map_or(0, |options| budget.saturating_sub(options.dive));
	Ok(Planner::with_txn(exec.ctx(), exec.function_registry(), exec.txn(), Some(ns), Some(db))
		.with_depth(depth)
		.with_auth(Arc::clone(&exec.root().auth))
		.with_perms_suppressed(exec.root().perms_suppressed))
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::TestDb;
	use crate::kvs::TransactionType;

	/// A database-level context on `test`/`test` over a write transaction.
	async fn exec_ctx(db: &TestDb) -> ExecutionContext {
		db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await
	}

	/// A scope for a statement with no clauses and no `RETURN`.
	fn scope(exec: &ExecutionContext) -> Arc<WriteScope> {
		WriteScope::new(
			exec.clone(),
			Arc::new(WriteClauses::default()),
			None,
			TableExistence::Required,
		)
	}

	/// `SELECT * FROM $name` as a logical plan, which is the shape every
	/// nested statement takes: a source named by a parameter the caller binds
	/// per record.
	fn sourced_by(name: &str) -> LogicalOp {
		let expr = crate::syn::expr(&format!("SELECT * FROM ${name}")).expect("it parses");
		crate::expr::logical::lower(expr).expect("a SELECT has a logical form")
	}

	#[tokio::test]
	async fn a_context_is_resolved_once_per_table_and_purpose() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = exec_ctx(&db).await;
		let scope = scope(&exec);

		let first = scope.write_ctx_for(&"t".into(), WritePurpose::Statement).await.unwrap();
		let again = scope.write_ctx_for(&"t".into(), WritePurpose::Statement).await.unwrap();
		assert!(Arc::ptr_eq(&first, &again), "a table is resolved once per statement");

		let view = scope.write_ctx_for(&"t".into(), WritePurpose::ViewMaintenance).await.unwrap();
		assert!(
			!Arc::ptr_eq(&first, &view),
			"the same table written as a view target is a different context"
		);
	}

	/// The purpose selects the clauses, the `RETURN` and the frame. Keying the
	/// cache by table alone would hand one of the two the other's: an `INSERT`
	/// whose rows name their own tables can write directly to a table that is
	/// also a view target of another table the same statement writes.
	#[tokio::test]
	async fn view_maintenance_carries_no_clauses_and_runs_unenforced() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = exec_ctx(&db).await;
		let clauses = WriteClauses {
			cond: Some(crate::exec::operators::test_util::physical_expr("true", &exec).await),
			..WriteClauses::default()
		};
		let scope = WriteScope::new(
			exec.clone(),
			Arc::new(clauses),
			Some(crate::expr::output::Output::After),
			TableExistence::Required,
		);

		let statement = scope.write_ctx_for(&"t".into(), WritePurpose::Statement).await.unwrap();
		assert!(statement.clauses.cond.is_some());
		assert!(statement.output.is_some());
		assert!(!statement.exec().root().perms_suppressed);

		let view = scope.write_ctx_for(&"t".into(), WritePurpose::ViewMaintenance).await.unwrap();
		assert!(view.clauses.cond.is_none(), "a view row has no WHERE of the statement's");
		assert!(view.output.is_none(), "a view row is not returned to the caller");
		assert!(view.exec().root().perms_suppressed, "view maintenance is not enforced");
	}

	/// The reach a nested stage has. A context the scope resolved finds it; one
	/// built outside a scope says so rather than appearing to work.
	#[tokio::test]
	async fn a_resolved_context_reaches_back_to_its_scope() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = exec_ctx(&db).await;
		let scope = scope(&exec);

		let resolved = scope.write_ctx_for(&"t".into(), WritePurpose::Statement).await.unwrap();
		assert!(Arc::ptr_eq(&resolved.scope().expect("the scope is reachable"), &scope));

		let detached = WriteCtx::resolve(
			&exec,
			&"t".into(),
			TableExistence::Required,
			None,
			Arc::new(WriteClauses::default()),
		)
		.await
		.unwrap();
		assert!(detached.scope().is_err(), "a context built outside a scope has none");
	}

	#[tokio::test]
	async fn a_nested_statement_is_compiled_once_per_key() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let exec = exec_ctx(&db).await;
		let scope = scope(&exec);
		let key = NestedKey::PurgeEdges;

		let first = scope
			.nested_plan(key.clone(), &exec, &["target"], || Ok(sourced_by("target")))
			.await
			.unwrap();
		let again = scope
			.nested_plan(key, &exec, &["target"], || panic!("a compiled plan is not built again"))
			.await
			.unwrap();
		assert!(Arc::ptr_eq(&first, &again));
	}

	/// The trap a nested statement is one omission away from. Binding
	/// substitutes a name it does not know as `NONE` in a source position, so a
	/// cascade compiled without its parameter named runs against nothing,
	/// affects nothing and raises nothing.
	#[tokio::test]
	async fn a_parameter_missing_from_row_scoped_is_substituted_away() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:1 SET n = 1;").await;
		let exec = exec_ctx(&db).await;
		let scope = scope(&exec);
		let bound = exec.with_param(
			"target",
			crate::val::Value::from(crate::val::RecordId {
				table: "t".into(),
				key: crate::val::RecordIdKey::Number(1),
			}),
		);

		let scoped = scope
			.nested_plan(NestedKey::PurgeEdges, &exec, &["target"], || Ok(sourced_by("target")))
			.await
			.unwrap();
		assert_eq!(
			run_nested(&scoped, &bound).await.unwrap().len(),
			1,
			"a row-scoped name is resolved against the value bound per record"
		);

		let unscoped = scope
			.nested_plan(
				NestedKey::Reference {
					table: "t".into(),
					field: "n".to_owned(),
					shape: ReferenceShape::Cascade,
				},
				&exec,
				&[],
				|| Ok(sourced_by("target")),
			)
			.await
			.unwrap();
		assert!(
			run_nested(&unscoped, &bound).await.unwrap().is_empty(),
			"a name binding did not know became NONE, and the statement reached no record"
		);
	}
}
