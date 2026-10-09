//! Planning a block against what its own schema statements write.
//!
//! A statement after a schema statement reads a catalog the schema statement
//! has changed. Rather than compile it once the schema statement has run, the
//! block runs the schema statement speculatively while it is planned: its
//! program, lowered as the block lowers it, run over the transaction while the
//! transaction speculates (`kvs::Transaction::speculate`), so its writes land
//! in an overlay the statements after it are planned against and never reach
//! the storage. The statement's real run, when the block reaches it, writes
//! the same definitions, since it is the same program over the same catalog.
//!
//! What a speculative run leaves out is what the transaction's key-value view
//! does not hold — the node's caches and services, an index build or a view's
//! backfill, actions at commit (`store::speculative_skip`) — none of which a
//! plan reads. An id the statement leases from a catalog sequence (a
//! namespace's, a database's, a table's, an index's) is leased for real,
//! held (`Sequences::hold_catalog_id`), noted with the speculation's writes,
//! and reserved: the program's run of the same draw takes it
//! (`Program::reservations`) and confirms it, so what the later statements
//! were planned knowing is the id the definition gets.
//!
//! The ids a statement's planning holds are its [`HeldIds`], which release
//! every id no run confirmed once nothing may run the draw reserving it: a
//! speculation that raises, or whose mapping is undone, releases its own ids
//! there and then; a statement the VM declines releases them all before the
//! operator tree plans it; and a program releases them when its operator and
//! its run are dropped. A block that fails or is cancelled before its
//! definitions run leaves the sequences where its operator-tree run leaves
//! them, and one that runs them takes the ids the operator tree would.
//!
//! A speculation is a view of the transaction (`kvs::Speculation`): the
//! schema statement's run and the planning of the statements after it are
//! handed the view, through a planner over it ([`view`]) and a frame holding
//! it, and nothing else reads through the overlay. Every other holder of the
//! transaction — a task of an earlier statement, a concurrent caller of a
//! client's transaction — reads and writes the storage as before, and a task
//! of the speculative run still holding the view once the speculation ends
//! takes no write. Only the executor's planner for a statement it is about to
//! run speculates ([`Planner::speculation`]).

use std::sync::Arc;

use crate::ctx::Context;
use crate::exec::context::{RootContext, SessionInfo};
use crate::exec::planner::Planner;
use crate::exec::vm::plan::PhysicalPlan;
use crate::exec::{ExecutionContext, FlowResult};
use crate::kvs::speculation::Overlay;
use crate::val::Value;

#[cfg(test)]
thread_local! {
	/// How many schema statements this thread has run speculatively, for
	/// tests that count them.
	pub(super) static SPECULATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many statements this thread's planning folded away: a schema
	/// statement known to do nothing, and the statements after one known to
	/// raise.
	pub(super) static FOLDED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// Whether this thread's blocks are planned without speculating, so a
	/// measurement can compare planning against the overlay with compiling
	/// again.
	pub(super) static WITHOUT_SPECULATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
	/// How many write stages this thread's lowering left out for a table
	/// the catalog holds nothing for them for.
	pub(super) static STAGES_LEFT_OUT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// What this thread's speculative runs drew, for tests that compare it
	/// with what the real runs stamped.
	pub(super) static DRAWN: std::cell::RefCell<Vec<u128>> = const { std::cell::RefCell::new(Vec::new()) };
	/// How many blocks this thread's lowering declined whole for a catalog
	/// change a later statement of theirs reads (`lower_block`).
	pub(super) static DECLINED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many more catalog ids this thread's speculative runs hold before
	/// the run holding the last of them yields to its poller once
	/// ([`pause_after_hold`]); `0` never yields.
	pub(super) static PAUSE_AFTER_HOLD: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// Whether a speculative run on this thread has yielded for
	/// [`PAUSE_AFTER_HOLD`].
	pub(super) static PAUSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Yield to the poller once when this is the catalog id [`PAUSE_AFTER_HOLD`]
/// counts down to, so a test can drop the planning future in the middle of a
/// speculative run that holds ids.
#[cfg(test)]
pub(super) async fn pause_after_hold() {
	let pause = PAUSE_AFTER_HOLD.with(|left| match left.get() {
		0 => false,
		n => {
			left.set(n - 1);
			n == 1
		}
	});
	if !pause {
		return;
	}
	PAUSED.with(|paused| paused.set(true));
	let mut yielded = false;
	std::future::poll_fn(|cx| {
		if std::mem::replace(&mut yielded, true) {
			std::task::Poll::Ready(())
		} else {
			cx.waker().wake_by_ref();
			std::task::Poll::Pending
		}
	})
	.await;
}

/// Why a block is declined whole when a statement of it reads a catalog
/// change no speculative run modelled (`lower_block`).
pub(super) const DATA_DEPENDENT_DDL: &str = "data-dependent DDL";

/// Whether a speculative run may evaluate what has effects `effects`: what a
/// second evaluation, the statement's real run, answers alike — reading no
/// clock, entropy, world outside or rows — and what acts on nothing outside
/// the transaction. Anything else the real run alone evaluates
/// ([`real_run_only`]).
pub(super) fn admits(effects: crate::expr::logical::EffectSet) -> bool {
	effects.is_deterministic() && !effects.intersects(crate::expr::logical::EffectSet::READS_DATA)
}

/// The error a speculative run raises where it reaches what its real run
/// alone evaluates. It is no refusal the statement declares, so the block
/// plans on without the speculation's model and the real run evaluates it.
pub(super) fn real_run_only() -> crate::expr::ControlFlow {
	crate::expr::ControlFlow::Err(anyhow::anyhow!(
		"a schema statement evaluating what only its run may evaluate"
	))
}

/// Note that a run of draw `draw` drew `value` — an id it leased, a uuid it
/// minted — when the run is speculative: kept with the speculation's writes,
/// whichever task runs the draw.
pub(super) fn drawn(ctx: &ExecutionContext, draw: u16, value: u128) {
	if let Some(txn) = ctx.try_txn()
		&& txn.speculating()
	{
		txn.note_speculative((draw, value));
	}
}

/// What the executor's planner for a statement it is about to run
/// speculates with ([`Planner::speculation`]).
pub(crate) struct Speculating {
	/// The session's options, which a speculative run's frame is made of.
	pub(crate) options: Arc<crate::dbs::Options>,
	/// The catalog ids the statement's speculative runs hold.
	pub(crate) held: Arc<HeldIds>,
}

impl Speculating {
	/// Speculation for a statement planned in `ctx` with `options`.
	pub(crate) fn new(options: Arc<crate::dbs::Options>, ctx: &crate::ctx::FrozenContext) -> Self {
		Self {
			options,
			held: Arc::new(HeldIds {
				sequences: ctx.try_get_sequences().ok().cloned(),
				ids: std::sync::Mutex::new(Vec::new()),
			}),
		}
	}
}

/// The catalog ids one statement's speculative runs held
/// (`Sequences::hold_catalog_id`), in the order held. Dropping it releases
/// every id it lists, which does nothing to an id a run confirmed: it is
/// dropped once nothing may run a draw reserving one of its ids, which is
/// when the statement's plan and every run of it are.
pub(crate) struct HeldIds {
	sequences: Option<crate::kvs::sequences::Sequences>,
	ids: std::sync::Mutex<Vec<(crate::kvs::sequences::CatalogSequence, u32)>>,
}

impl std::fmt::Debug for HeldIds {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("HeldIds").field("ids", &*self.ids()).finish_non_exhaustive()
	}
}

impl HeldIds {
	fn ids(&self) -> std::sync::MutexGuard<'_, Vec<(crate::kvs::sequences::CatalogSequence, u32)>> {
		self.ids.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// Whether it lists no id.
	pub(super) fn is_empty(&self) -> bool {
		self.ids().is_empty()
	}

	/// How many ids it lists, for [`Self::release_since`].
	pub(super) fn mark(&self) -> usize {
		self.ids().len()
	}

	/// Release the ids held since `mark`: those of speculative runs whose
	/// reservations no program holds.
	pub(super) fn release_since(&self, mark: usize) {
		let released: Vec<_> = {
			let mut ids = self.ids();
			let mark = mark.min(ids.len());
			ids.drain(mark..).collect()
		};
		self.release(released);
	}

	/// Release every id it lists: the statement is not run from its plan.
	pub(crate) fn release_all(&self) {
		self.release_since(0);
	}

	fn release(&self, ids: Vec<(crate::kvs::sequences::CatalogSequence, u32)>) {
		if let Some(sequences) = &self.sequences {
			for (sequence, id) in ids {
				sequences.release_catalog_id(&sequence, id);
			}
		}
	}
}

impl Drop for HeldIds {
	fn drop(&mut self) {
		let ids = std::mem::take(&mut *self.ids());
		self.release(ids);
	}
}

/// Hold catalog id `id` of `sequence`, leased by a speculative run in `ctx`,
/// with the speculation's writes. Released at once when the speculation has
/// ended, since nothing it planned can reserve it.
pub(super) fn held(
	ctx: &ExecutionContext,
	sequences: &crate::kvs::sequences::Sequences,
	sequence: &crate::kvs::sequences::CatalogSequence,
	id: u32,
) {
	let kept = ctx.try_txn().is_some_and(|txn| txn.note_speculative_hold((sequence.clone(), id)));
	if !kept {
		sequences.release_catalog_id(sequence, id);
	}
}

/// What a schema statement's speculative run did.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Speculated {
	/// The catalog's writes after the run: those it started from and its own.
	pub(crate) overlay: Overlay,
	/// What its draws drew — ids leased, uuids minted — which its real
	/// run takes.
	pub(crate) draws: Vec<(u16, u128)>,
	/// Whether the run wrote anything.
	pub(crate) changed: bool,
	/// Whether the run left an effect beyond the overlay for the real run.
	pub(crate) skipped: bool,
	/// The statement's value.
	pub(crate) value: Value,
	/// The table the run defined, as the statement defines one where there is
	/// none, that a write of the block before the statement may define first:
	/// its definition's reservations are shared with that write
	/// ([`super::program::ImplicitTable`]).
	pub(crate) shared: Option<surrealdb_strand::TableName>,
}

/// How a speculative run ended.
#[derive(Debug, Clone)]
pub(crate) enum Outcome {
	/// It ran to its end.
	Ran(Box<Speculated>),
	/// It raised. `refused` is whether it raised a refusal the statement
	/// declares — a definition that exists or is missing, a capability the
	/// caller lacks — after leaving no effect for the real run: then the real
	/// run, the same program over the same catalog, raises it too. What it
	/// raises is left to that run.
	Raised {
		refused: bool,
	},
}

/// Run schema statement `physical`, mapped by `planner`, speculatively over
/// the catalog `from` leaves: `None` when the planner may not speculate or the
/// statement does not lower.
pub(super) async fn run(
	planner: &Planner<'_>,
	physical: &PhysicalPlan,
	from: &Overlay,
) -> Option<Outcome> {
	let speculating = planner.speculation.clone()?;
	let speculation = planner.txn.as_ref()?.speculate(from.clone())?;
	let planner = planner.over(Arc::clone(speculation.txn()));
	let txn = Arc::clone(speculation.txn());
	let ran = execute(&planner, physical, &speculating.options, &txn).await;
	drop(planner);
	let mut overlay = speculation.end();
	let draws = overlay.take_notes();
	let skipped = overlay.take_skipped();
	let refused = overlay.take_refused();
	// The ids the run held are the statement's to release. No reservation
	// models a run that did not lower or that raised, so its ids are released
	// now.
	let mark = speculating.held.mark();
	speculating.held.ids().extend(overlay.take_held());
	if !matches!(ran, Some(Ok(_))) {
		speculating.held.release_since(mark);
	}
	#[cfg(test)]
	{
		SPECULATED.with(|count| count.set(count.get() + 1));
		DRAWN.with(|drawn| drawn.borrow_mut().extend(draws.iter().map(|(_, value)| *value)));
	}
	match ran? {
		Ok(value) => Some(Outcome::Ran(Box::new(Speculated {
			changed: !overlay.writes_as(from),
			skipped,
			overlay,
			draws,
			value,
			shared: None,
		}))),
		Err(_) => Some(Outcome::Raised {
			refused: refused && !skipped,
		}),
	}
}

/// Lower `physical` and run it to its end in the session's frame.
async fn execute(
	planner: &Planner<'_>,
	physical: &PhysicalPlan,
	options: &Arc<crate::dbs::Options>,
	txn: &Arc<crate::kvs::Transaction>,
) -> Option<FlowResult<Value>> {
	let (program, served) = Box::pin(super::lower_to_program(planner, physical)).await.ok()?;
	let program = Arc::new(program);
	let exec = frame(planner, options, txn).await;
	let exec = match served.is_empty() {
		true => exec,
		false => exec.with_vm_served(served.into()),
	};
	Some(
		async {
			let stream = super::host::execute_program(&program, &exec)?;
			let mut values = crate::exec::operators::collect_stream(stream).await?;
			Ok(match values.len() {
				0 => Value::None,
				_ => values.swap_remove(0),
			})
		}
		.await,
	)
}

/// The frame the executor runs a statement in, for `planner`'s session.
async fn frame(
	planner: &Planner<'_>,
	options: &Arc<crate::dbs::Options>,
	txn: &Arc<crate::kvs::Transaction>,
) -> ExecutionContext {
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
	let mut ctx = Context::new_child(planner.frozen_ctx());
	ctx.set_transaction(Arc::clone(txn));
	let ctx = ctx.freeze();
	let root = ExecutionContext::Root(RootContext {
		cancellation: Arc::new(ctx.cancel_token().map(|t| t.child_token()).unwrap_or_default()),
		session: SessionInfo::from_context(&ctx).map(Arc::new),
		ctx,
		options: Some((**options).clone()),
		datastore: None,
		auth: Arc::clone(&options.auth),
		current_value: None,
		current_row: None,
		skip_fetch_perms: false,
		perms_suppressed: false,
		caller_perms_suppressed: false,
		computing_record: None,
		no_write: None,
		version_stamp: None,
		zero_limits: Vec::new(),
		topk_thresholds: None,
		guard_verdicts: Default::default(),
		writing: false,
	});
	let (Ok(ns), Ok(db)) = (options.ns(), options.db()) else {
		return root;
	};
	let (Ok(ns_def), Ok(db_def)) =
		(txn.expect_ns_by_name(ns).await, txn.expect_db_by_name(ns, db).await)
	else {
		return root;
	};
	root.with_database(ns_def, db_def)
}

/// A speculation from `overlay` and a planner like `planner` over its view,
/// which binds, maps and lowers against the catalog as `overlay` leaves it:
/// `None` when `planner` has no transaction, or its transaction is itself a
/// view. The speculation ends when the guard is dropped; what is planned over
/// the view holds no transaction, so it runs over the real one.
pub(super) fn view<'p>(
	planner: &Planner<'p>,
	overlay: &Overlay,
) -> Option<(crate::kvs::tx::Speculation, Planner<'p>)> {
	let speculation = planner.txn.as_ref()?.speculate(overlay.clone())?;
	let over = planner.over(Arc::clone(speculation.txn()));
	Some((speculation, over))
}

/// Whether the catalog as `overlay` leaves it defines table `table`, as
/// `planner`'s transaction reads it; `false` when it cannot tell.
pub(super) async fn defines_table(
	planner: &Planner<'_>,
	overlay: &Overlay,
	table: &surrealdb_strand::TableName,
) -> bool {
	use crate::catalog::providers::TableProvider;
	let Some((_speculation, over)) = view(planner, overlay) else {
		return false;
	};
	let (Some(txn), Some((ns, db))) = (over.txn.clone(), over.ns_db_ids().await) else {
		return false;
	};
	matches!(txn.get_tb(ns, db, table, None).await, Ok(Some(_)))
}

/// The catalog as a planner's transaction reads it — through a speculation's
/// overlay when the planner is over its view — for a footprint's expansion.
pub(super) struct TxnCatalog {
	pub(super) txn: Arc<crate::kvs::Transaction>,
	pub(super) ids: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	/// Whether the footprint is a schema statement's, which reads a table's
	/// rows as stored — an index's build, a view's backfill, run by a caller
	/// no permission predicate binds — so that only a computed field reads
	/// beyond them.
	pub(super) schema: bool,
}

impl crate::exec::logical::footprint::CatalogView for TxnCatalog {
	fn triggers<'a>(
		&'a self,
		table: &'a surrealdb_strand::TableName,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = (bool, bool)> + Send + 'a>> {
		use crate::catalog::providers::TableProvider;
		Box::pin(async move {
			let Some((ns, db)) = self.ids else {
				return (true, true);
			};
			let events = match self.txn.all_tb_events(ns, db, table, None).await {
				Ok(events) => !events.is_empty(),
				Err(_) => true,
			};
			let views = match self.txn.all_tb_views(ns, db, table, None).await {
				Ok(views) => !views.is_empty(),
				Err(_) => true,
			};
			(events, views)
		})
	}

	fn any_triggers(
		&self,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
		Box::pin(async { true })
	}

	fn reads_through<'a>(
		&'a self,
		table: &'a surrealdb_strand::TableName,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
		use crate::catalog::Permission;
		use crate::catalog::providers::TableProvider;
		Box::pin(async move {
			let Some((ns, db)) = self.ids else {
				return true;
			};
			let table_def = match self.txn.get_tb(ns, db, table, None).await {
				Ok(Some(def)) => def,
				Ok(None) => return false,
				Err(_) => return true,
			};
			if !self.schema && matches!(table_def.permissions.select, Permission::Specific(_)) {
				return true;
			}
			match self.txn.all_tb_fields(ns, db, table, None).await {
				Ok(fields) => fields.iter().any(|field| {
					field.computed.is_some()
						|| (!self.schema
							&& matches!(field.select_permission, Permission::Specific(_)))
				}),
				Err(_) => true,
			}
		})
	}

	fn defines<'a>(
		&'a self,
		table: &'a surrealdb_strand::TableName,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
		use crate::catalog::providers::TableProvider;
		Box::pin(async move {
			let Some((ns, db)) = self.ids else {
				return false;
			};
			matches!(self.txn.get_tb(ns, db, table, None).await, Ok(Some(_)))
		})
	}
}

/// Whether running block statement `op` may change the catalog through code
/// that is not written in it: the stored bodies it calls, and what the
/// catalog runs for its writes — a written table's events that run in the
/// statement's transaction, its fields' `VALUE`, `DEFAULT` and `ASSERT`
/// clauses, the tables of the views over it, and, for a delete, the reference
/// actions of every table, with the referencing tables a `CASCADE` or `UNSET`
/// action writes followed as written tables. Read through `planner`'s
/// transaction, so through a speculation's overlay when the planner is over
/// its view.
///
/// Conservative: `true` when any code reached holds a schema statement, a
/// body it cannot inspect (a script, a module or silo function, a builtin
/// that evaluates statements other than `eval::surql` of a written query), a
/// call of a function the catalog does not define, or when the catalog cannot
/// be read. A value invoked, or a method no builtin answers, runs a closure:
/// one written in the block or in a body it reaches, whose body is followed
/// where it is written, unless `closures_from_outside` says one may come from
/// a parameter bound outside the block, when it may run anything. A
/// permission predicate and a computed field are not followed: a write from
/// either is refused when it runs.
pub(super) async fn runs_unseen_schema(
	planner: &Planner<'_>,
	op: &crate::expr::logical::LogicalOp,
	closures_from_outside: bool,
) -> bool {
	reached(planner, op, closures_from_outside).await.is_none()
}

/// What block statement `op`, of footprint `footprint`, writes, when nothing
/// it reaches changes the catalog unseen ([`runs_unseen_schema`]): its own
/// writes, and in place of what the footprint could not name, the records of
/// every table the code it reaches writes; with whether any of that code
/// writes to a table it evaluates, which may be any table, defined or not. A
/// closure it invokes that is written in an earlier statement of the block is
/// in that statement's footprint, which holds what the closure's body writes.
/// `None` when something it reaches may change the catalog.
pub(super) async fn seen_writes(
	planner: &Planner<'_>,
	op: &crate::expr::logical::LogicalOp,
	footprint: &crate::exec::logical::footprint::Footprint,
	closures_from_outside: bool,
) -> Option<(crate::exec::logical::footprint::Footprint, bool)> {
	use crate::exec::logical::footprint::Resource;
	let reached = reached(planner, op, closures_from_outside).await?;
	let mut seen = footprint.clone();
	seen.writes.retain(|written| !matches!(written, Resource::Unknown));
	let tables = reached.tables.into_iter().map(Resource::table);
	let data = match reached.every_table {
		true => vec![Resource::any_data()],
		false => tables.collect(),
	};
	for written in data {
		if !seen.writes.contains(&written) {
			seen.writes.push(written);
		}
	}
	Some((seen, reached.every_table))
}

/// What [`reached`] found running a statement reaches, when none of it may
/// change the catalog.
struct Reached {
	/// The tables written by the statement and by what it reaches.
	tables: std::collections::BTreeSet<surrealdb_strand::TableName>,
	/// Whether a write's table is evaluated, so it may be any table.
	every_table: bool,
}

/// What running `op` reaches, as [`runs_unseen_schema`] follows it; `None`
/// when any of it may change the catalog.
async fn reached(
	planner: &Planner<'_>,
	op: &crate::expr::logical::LogicalOp,
	closures_from_outside: bool,
) -> Option<Reached> {
	use crate::catalog::providers::{DatabaseProvider, TableProvider};
	let mut reach = Reach {
		closures_from_outside,
		..Reach::default()
	};
	reach.op(op);
	if reach.schema {
		return None;
	}
	let mut tables = std::collections::BTreeSet::new();
	if reach.calls.is_empty() && reach.written.is_empty() && !reach.every_table {
		return Some(Reached {
			tables,
			every_table: false,
		});
	}
	let txn = planner.txn.clone()?;
	let (ns, db) = planner.ns_db_ids().await?;
	let mut functions = std::collections::BTreeSet::new();
	let (mut every_table, mut references) = (false, false);
	while !reach.schema {
		if let Some(name) = reach.calls.pop_first() {
			if !functions.insert(name.clone()) {
				continue;
			}
			let def = txn.get_db_function(ns, db, &name, None).await.ok()?;
			reach.block(&def.block);
			continue;
		}
		if reach.every_table && !every_table {
			every_table = true;
			let all = txn.all_tb(ns, db, None).await.ok()?;
			reach.written.extend(all.iter().map(|tb| tb.name.clone()));
			continue;
		}
		if reach.deletes && !references {
			references = true;
			let all = txn.all_tb(ns, db, None).await.ok()?;
			for tb in all.iter() {
				let fields = txn.all_tb_fields(ns, db, &tb.name, None).await.ok()?;
				for field in fields.iter() {
					use crate::expr::reference::{Reference, ReferenceDeleteStrategy};
					match &field.reference {
						// A custom action runs its expression.
						Some(Reference {
							on_delete: ReferenceDeleteStrategy::Custom(then),
						}) => reach.expr(then),
						// A cascade deletes the referencing record and an
						// unset updates it: either is a write of its table.
						Some(Reference {
							on_delete:
								ReferenceDeleteStrategy::Cascade | ReferenceDeleteStrategy::Unset,
						}) => {
							reach.written.insert(tb.name.clone());
						}
						_ => {}
					}
				}
			}
			continue;
		}
		let Some(table) = reach.written.pop_first() else {
			return Some(Reached {
				tables,
				every_table,
			});
		};
		if !tables.insert(table.clone()) {
			continue;
		}
		let events = txn.all_tb_events(ns, db, &table, None).await.ok()?;
		let fields = txn.all_tb_fields(ns, db, &table, None).await.ok()?;
		let views = txn.all_tb_views(ns, db, &table, None).await.ok()?;
		// An asynchronous event runs in a transaction of its own, after this
		// one commits.
		for event in events.iter().filter(|event| !event.is_async()) {
			reach.expr(&event.when);
			event.then.iter().for_each(|then| reach.expr(then));
		}
		for field in fields.iter() {
			field.value.iter().chain(&field.assert).for_each(|clause| reach.expr(clause));
			if let crate::catalog::DefineDefault::Always(clause)
			| crate::catalog::DefineDefault::Set(clause) = &field.default
			{
				reach.expr(clause);
			}
		}
		reach.written.extend(views.iter().map(|view| view.name.clone()));
	}
	None
}

/// Whether a parameter `ops` read and the block does not bind — one of
/// `bound` — holds a closure when the block arrives: a closure the block may
/// invoke whose body is written outside it.
pub(super) fn closures_from_outside<'o>(
	planner: &Planner<'_>,
	ops: impl IntoIterator<Item = &'o crate::expr::logical::LogicalOp>,
	bound: &[String],
) -> bool {
	use crate::expr::logical::LogicalExpr;
	fn holds_closure(value: &Value) -> bool {
		match value {
			Value::Closure(_) => true,
			Value::Array(items) => items.iter().any(holds_closure),
			Value::Object(fields) => fields.values().any(holds_closure),
			_ => false,
		}
	}
	let ctx = planner.frozen();
	let mut found = false;
	for op in ops {
		let _ = op.visit_ops_deep(&mut |node| {
			node.for_each_scalar(&mut |scalar| {
				let _ = scalar.visit(&mut |expr| {
					if let LogicalExpr::Param(param) = expr
						&& !bound.iter().any(|name| name == param.as_str())
						&& ctx.value(param.as_str()).is_some_and(holds_closure)
					{
						found = true;
					}
					std::ops::ControlFlow::<()>::Continue(())
				});
			});
			match found {
				true => std::ops::ControlFlow::Break(()),
				false => std::ops::ControlFlow::Continue(()),
			}
		});
		if found {
			return true;
		}
	}
	false
}

/// What code reaches that may change the catalog, gathered from one body
/// before the catalog is read: [`runs_unseen_schema`]'s worklist.
#[derive(Default)]
struct Reach {
	/// A schema statement, or a body that cannot be inspected, was reached.
	schema: bool,
	/// The user-defined functions called, without the `fn::` prefix.
	calls: std::collections::BTreeSet<String>,
	/// The tables written.
	written: std::collections::BTreeSet<surrealdb_strand::TableName>,
	/// Whether a write's table is evaluated, so it may be any table.
	every_table: bool,
	/// Whether a delete was reached, which runs the reference actions of the
	/// tables referencing what it deletes.
	deletes: bool,
	/// Whether a closure invoked may be a value bound outside the block,
	/// whose body nothing here follows.
	closures_from_outside: bool,
}

impl Reach {
	fn op(&mut self, op: &crate::expr::logical::LogicalOp) {
		use crate::expr::logical::{LogicalOp, LogicalWriteStatement};
		match op {
			LogicalOp::Ddl(_) => {
				self.schema = true;
				return;
			}
			LogicalOp::Write(write) => {
				match crate::exec::logical::footprint::write_targets(&write.source) {
					Some(tables) => self.written.extend(tables),
					None => self.every_table = true,
				}
				self.deletes |= matches!(write.statement, LogicalWriteStatement::Delete(_));
			}
			_ => {}
		}
		let _ = op.visit_scalars(&mut |scalar| {
			self.logical(scalar);
			Ok::<(), std::convert::Infallible>(())
		});
		for child in op.children() {
			self.op(child);
		}
	}

	/// A scalar, closure bodies and the statements it holds included.
	fn logical(&mut self, expr: &crate::expr::logical::LogicalExpr) {
		use crate::expr::logical::LogicalExpr;
		let _ = expr.visit(&mut |node| {
			match node {
				LogicalExpr::Subquery(op) => self.op(op),
				LogicalExpr::Call(call) => match (&call.receiver, call.args.first()) {
					// A written query is followed as written.
					(
						crate::expr::Function::Normal(name),
						Some(LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::String(
							query,
						))),
					) if name == "eval::surql" => self.surql(query),
					(receiver, _) => self.function(receiver),
				},
				LogicalExpr::Invoke {
					target,
					..
				} if !matches!(target.as_ref(), LogicalExpr::Closure(_)) => self.invoked(),
				LogicalExpr::Method {
					name,
					..
				} if !crate::expr::method::is_builtin_method(name) => self.invoked(),
				_ => {}
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
	}

	/// A closure invoked, written where the walk follows it unless it may
	/// come from outside the block.
	fn invoked(&mut self) {
		self.schema |= self.closures_from_outside;
	}

	/// The statement `eval::surql` evaluates, written as `query`; a query that
	/// does not parse is followed as code that cannot be inspected.
	fn surql(&mut self, query: &str) {
		let Ok(ast) = crate::syn::parse(query) else {
			self.schema = true;
			return;
		};
		for statement in ast.expressions {
			match statement {
				surrealdb_expr::parser::TopLevelExpr::Expr(expr) => {
					self.expr(&crate::expr::Expr::from(expr))
				}
				_ => self.schema = true,
			}
		}
	}

	fn function(&mut self, function: &crate::expr::Function) {
		match function {
			crate::expr::Function::Custom(name) => {
				self.calls.insert(name.clone());
			}
			other => self.schema |= !other.read_only(),
		}
	}

	/// A stored clause.
	fn expr(&mut self, expr: &crate::expr::Expr) {
		use crate::expr::visit::Visitor;
		let Ok(()) = StoredReach(self).visit_expr(expr);
	}

	/// A stored function body.
	fn block(&mut self, block: &crate::expr::Block) {
		use crate::expr::visit::Visitor;
		let Ok(()) = StoredReach(self).visit_block(block);
	}
}

/// [`Reach`] over stored code, which the catalog holds as expressions.
struct StoredReach<'r>(&'r mut Reach);

impl StoredReach<'_> {
	fn target(&mut self, target: &crate::expr::Expr) {
		match target {
			crate::expr::Expr::Table(table) => {
				self.0.written.insert(table.clone());
			}
			crate::expr::Expr::Literal(crate::expr::Literal::RecordId(rid)) => {
				self.0.written.insert(rid.table.clone());
			}
			_ => self.0.every_table = true,
		}
	}
}

impl crate::expr::visit::Visitor for StoredReach<'_> {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &crate::expr::Expr) -> Result<(), Self::Error> {
		use crate::expr::Expr;
		use crate::expr::operator::PostfixOperator;
		use crate::expr::visit::Visit as _;
		match expr {
			Expr::Define(_) | Expr::Remove(_) | Expr::Rebuild(_) | Expr::Alter(_) => {
				self.0.schema = true;
			}
			Expr::Create(stmt) => stmt.what.iter().for_each(|what| self.target(what)),
			Expr::Update(stmt) => stmt.what.iter().for_each(|what| self.target(what)),
			Expr::Upsert(stmt) => stmt.what.iter().for_each(|what| self.target(what)),
			Expr::Delete(stmt) => {
				stmt.what.iter().for_each(|what| self.target(what));
				self.0.deletes = true;
			}
			Expr::Relate(stmt) => self.target(&stmt.through),
			Expr::Insert(stmt) => match &stmt.into {
				Some(into) => self.target(into),
				None => self.0.every_table = true,
			},
			Expr::Postfix {
				expr: target,
				op: PostfixOperator::Call(_),
			} if !matches!(target.as_ref(), Expr::Closure(_)) => self.0.invoked(),
			Expr::Postfix {
				op: PostfixOperator::MethodCall(name, _),
				..
			} if !crate::expr::method::is_builtin_method(name) => self.0.invoked(),
			_ => {}
		}
		expr.visit(self)
	}

	fn visit_part(&mut self, part: &crate::expr::Part) -> Result<(), Self::Error> {
		use crate::expr::visit::Visit as _;
		if let crate::expr::Part::Method(name, _) = part
			&& !crate::expr::method::is_builtin_method(name)
		{
			self.0.invoked();
		}
		part.visit(self)
	}

	fn visit_function(&mut self, function: &crate::expr::Function) -> Result<(), Self::Error> {
		self.0.function(function);
		Ok(())
	}
}

/// The footprint of block statement `op` as `planner` plans it, with what the
/// catalog the transaction reads now says it triggers.
///
/// Its reads are expanded through the catalog only when `reads` is asked
/// for; otherwise they are `Unknown`, which conflicts with any write, so a
/// caller that asks only after what a statement writes pays nothing for
/// what it reads and a reader of them is never told less than it may read.
pub(super) async fn footprint(
	planner: &Planner<'_>,
	op: &crate::expr::logical::LogicalOp,
	reads: bool,
) -> crate::exec::logical::footprint::Footprint {
	use crate::exec::logical::footprint;
	let mut fp = footprint::of(op, planner.function_registry());
	let Some(txn) = planner.txn.clone() else {
		return footprint::Footprint::unknown();
	};
	if !reads {
		fp.reads = vec![footprint::Resource::Unknown];
	}
	let catalog = TxnCatalog {
		txn,
		ids: planner.ns_db_ids().await,
		schema: crate::exec::planner::fold::changes_the_catalog(op),
	};
	footprint::expand(&mut fp, &catalog).await;
	fp
}
