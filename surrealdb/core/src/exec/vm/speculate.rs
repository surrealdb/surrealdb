//! Planning a block against what its own schema statements write.
//!
//! A statement after a schema statement reads a catalog the schema statement
//! has changed. So the
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
//! there and then; a statement the VM declines releases them all, since it is
//! not run from its plan; and a program releases them when its operator and
//! its run are dropped. A block that fails or is cancelled before its
//! definitions run leaves the sequences advanced only by the definitions that
//! ran, and one that runs them takes the ids it would take with no
//! speculation.
//!
//! A speculation is a view of the transaction (`kvs::Speculation`): the
//! schema statement's run and the planning of the statements after it are
//! handed the view, through a planner over it ([`view`]) and a frame holding
//! it, and nothing else reads through the overlay. Every other holder of the
//! transaction — a task of an earlier statement, a concurrent caller of a
//! client's transaction — reads and writes the storage directly, and a task
//! of the speculative run still holding the view once the speculation ends
//! takes no write. Only the executor's planner for a statement it is about to
//! run speculates ([`Planner::speculation`]).

use std::sync::Arc;

use crate::exec::ExecutionContext;
use crate::exec::planner::Planner;
use crate::expr::logical::LogicalOp;
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
	reached(planner, op, closures_from_outside, None).await.is_none()
}

/// The tables each variable the write `op` is (under its `TIMEOUT` at most) names
/// as a target holds in every run of the plan `planner` makes: the table a kept statement's class
/// of value names ([`Planner::named_targets`]), or the tables the value the planning
/// context binds the name to names, which the bind folds into the plan. A name
/// read when the plan runs, whose value the plan does not fix, holds none.
fn variable_targets(
	planner: &Planner<'_>,
	op: &crate::expr::logical::LogicalOp,
) -> std::collections::BTreeMap<String, Vec<surrealdb_strand::TableName>> {
	use crate::expr::logical::LogicalOp;
	fn tables(value: &Value) -> Option<Vec<surrealdb_strand::TableName>> {
		match value {
			Value::RecordId(rid) => Some(vec![rid.table.clone()]),
			Value::Table(table) => Some(vec![table.clone()]),
			Value::Array(items) => items
				.iter()
				.map(|item| match item {
					Value::RecordId(rid) => Some(rid.table.clone()),
					Value::Table(table) => Some(table.clone()),
					Value::None
					| Value::Null
					| Value::Bool(_)
					| Value::Number(_)
					| Value::String(_)
					| Value::Duration(_)
					| Value::Datetime(_)
					| Value::Date(_)
					| Value::Time(_)
					| Value::Timestamp(_)
					| Value::Interval(_)
					| Value::Uuid(_)
					| Value::Array(_)
					| Value::Set(_)
					| Value::Object(_)
					| Value::Geometry(_)
					| Value::Bytes(_)
					| Value::File(_)
					| Value::Regex(_)
					| Value::Range(_)
					| Value::Closure(_) => None,
				})
				.collect(),
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => None,
		}
	}
	let root = match op {
		LogicalOp::Write(write) => write,
		LogicalOp::Timeout(timeout) => match timeout.input.as_ref() {
			LogicalOp::Write(write) => write,
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
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
			| LogicalOp::Sleep(_) => return std::collections::BTreeMap::new(),
		},
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
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
		| LogicalOp::Sleep(_) => return std::collections::BTreeMap::new(),
	};
	let mut out = std::collections::BTreeMap::new();
	for name in target_variables(&root.source) {
		let held = match planner.named_targets().iter().find(|(named, _)| named == name) {
			Some((_, table)) => Some(vec![table.clone()]),
			None if planner.param_is_shadowed(name) => None,
			None => planner.frozen().value(name).and_then(tables),
		};
		if let Some(held) = held {
			out.insert(name.to_owned(), held);
		}
	}
	// A write nested in the statement may name its target by a variable a `LET` or a `FOR` of the
	// statement binds, which the request's value does not fix: a name such a write reads reaches
	// every table.
	let _ = op.visit_ops_deep(&mut |node| {
		if let LogicalOp::Write(write) = node
			&& !std::ptr::eq(write, root)
		{
			for name in target_variables(&write.source) {
				out.remove(name);
			}
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	out
}

/// The variables `source` names its subjects by: a target that is a variable, or the variable
/// the one source of the rows a write writes reads.
fn target_variables(source: &crate::expr::logical::LogicalWriteSource) -> Vec<&str> {
	use crate::expr::logical::{
		LogicalExpr, LogicalOp, LogicalSource, LogicalWriteSource, SourceRef,
	};
	match source {
		LogicalWriteSource::Targets(targets) => targets
			.iter()
			.filter_map(|target| match target {
				LogicalExpr::Param(param) => Some(param.as_str()),
				LogicalExpr::Literal(_)
				| LogicalExpr::Array(_)
				| LogicalExpr::Set(_)
				| LogicalExpr::Object(_)
				| LogicalExpr::RecordId(_)
				| LogicalExpr::Table(_)
				| LogicalExpr::Mock(_)
				| LogicalExpr::Constant(_)
				| LogicalExpr::Field(_)
				| LogicalExpr::Doc
				| LogicalExpr::Current
				| LogicalExpr::Access {
					..
				}
				| LogicalExpr::Index {
					..
				}
				| LogicalExpr::First(_)
				| LogicalExpr::Last(_)
				| LogicalExpr::Each(_)
				| LogicalExpr::Flatten {
					..
				}
				| LogicalExpr::Filter {
					..
				}
				| LogicalExpr::Method {
					..
				}
				| LogicalExpr::Destructure {
					..
				}
				| LogicalExpr::Optional {
					..
				}
				| LogicalExpr::Lookup {
					..
				}
				| LogicalExpr::Recurse(_)
				| LogicalExpr::RepeatRecurse(_)
				| LogicalExpr::Broadcast {
					..
				}
				| LogicalExpr::SetField {
					..
				}
				| LogicalExpr::Prefix {
					..
				}
				| LogicalExpr::Postfix {
					..
				}
				| LogicalExpr::Invoke {
					..
				}
				| LogicalExpr::Binary {
					..
				}
				| LogicalExpr::Call(_)
				| LogicalExpr::Closure(_)
				| LogicalExpr::If(_)
				| LogicalExpr::Throw(_)
				| LogicalExpr::Break
				| LogicalExpr::Continue
				| LogicalExpr::Subquery(_) => None,
			})
			.collect(),
		LogicalWriteSource::Rows(rows) => match rows.as_ref() {
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Param(param),
				..
			}) => vec![param.as_str()],
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
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
			| LogicalOp::Sleep(_) => Vec::new(),
		},
		LogicalWriteSource::Inserts(_) => Vec::new(),
	}
}

/// What [`reached`] found running a statement reaches, when none of it may
/// change the catalog.
struct Reached {}

/// What running `op` reaches, as [`runs_unseen_schema`] follows it; `None`
/// when any of it may change the catalog.
async fn reached(
	planner: &Planner<'_>,
	op: &crate::expr::logical::LogicalOp,
	closures_from_outside: bool,
	root_table: Option<&surrealdb_strand::TableName>,
) -> Option<Reached> {
	use crate::catalog::providers::{DatabaseProvider, TableProvider};
	let mut reach = Reach {
		closures_from_outside,
		variables: variable_targets(planner, op),
		root_table: root_table.cloned(),
		..Reach::default()
	};
	reach.op(op);
	if reach.schema {
		return None;
	}
	let mut tables = std::collections::BTreeSet::new();
	if reach.calls.is_empty() && reach.written.is_empty() && !reach.every_table {
		return Some(Reached {});
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
			// The `REFERENCE` fields of every table, read as one entry a compilation can name.
			let referencing = txn.reference_fields(ns, db, None).await.ok()?;
			txn.note_references_read(ns, db, None);
			for (tb, fields) in referencing.tables.iter() {
				for field in fields.iter() {
					use crate::expr::reference::{Reference, ReferenceDeleteStrategy};
					match &field.reference {
						// A custom action runs its expression.
						Some(Reference {
							on_delete: ReferenceDeleteStrategy::Custom(then),
							..
						}) => reach.expr(then),
						// A cascade deletes the referencing record and an
						// unset updates it: either is a write of its table.
						Some(Reference {
							on_delete:
								ReferenceDeleteStrategy::Cascade | ReferenceDeleteStrategy::Unset,
							..
						}) => {
							reach.written.insert(tb.clone());
						}
						_ => {}
					}
				}
			}
			continue;
		}
		let Some(table) = reach.written.pop_first() else {
			return Some(Reached {});
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
			// A materialized list is written onto the referenced table, and its condition is
			// judged on this table's record.
			if let Some(materialize) =
				field.reference.as_ref().and_then(|reference| reference.materialize.as_ref())
			{
				reach.written.insert(materialize.table.clone());
				materialize.filter.iter().for_each(|clause| reach.expr(clause));
			}
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
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_) => false,
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
						&& ctx
							.lexical_value(param.as_str())
							.is_some_and(|value| holds_closure(value))
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
	/// The tables each variable a write of the statement names as a target holds, where the plan
	/// is made for them ([`variable_targets`]).
	variables: std::collections::BTreeMap<String, Vec<surrealdb_strand::TableName>>,
	/// The table the statement's own write writes, whatever its targets name: the table of the
	/// stage bundle being planned ([`catalog_within_writing`]). Taken by the first write reached.
	root_table: Option<surrealdb_strand::TableName>,
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
				let written = match self.root_table.take() {
					Some(table) => Some(vec![table]),
					None => crate::exec::logical::footprint::write_targets(&write.source)
						.or_else(|| self.variable_written(&write.source)),
				};
				match written {
					Some(tables) => self.written.extend(tables),
					None => self.every_table = true,
				}
				self.deletes |= matches!(write.statement, LogicalWriteStatement::Delete(_));
			}
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
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
			| LogicalOp::Sleep(_) => {}
		}
		let _ = op.visit_scalars(&mut |scalar| {
			self.logical(scalar);
			Ok::<(), std::convert::Infallible>(())
		});
		for child in op.children() {
			self.op(child);
		}
	}

	/// The tables the targets `source` names, where each is a table, a record id, or a variable
	/// whose tables [`Self::variables`] holds, or the tables of such a variable the rows a write
	/// writes are read from; `None` otherwise.
	fn variable_written(
		&self,
		source: &crate::expr::logical::LogicalWriteSource,
	) -> Option<Vec<surrealdb_strand::TableName>> {
		use crate::expr::logical::{
			LogicalExpr, LogicalOp, LogicalSource, LogicalWriteSource, SourceRef,
		};
		let targets = match source {
			LogicalWriteSource::Targets(targets) => targets,
			LogicalWriteSource::Rows(rows) => {
				let LogicalOp::Source(LogicalSource {
					source: SourceRef::Param(param),
					..
				}) = rows.as_ref()
				else {
					return None;
				};
				return self.variables.get(param.as_str()).cloned();
			}
			LogicalWriteSource::Inserts(_) => return None,
		};
		let mut out = Vec::new();
		for target in targets {
			match target {
				LogicalExpr::Table(table) => out.push(table.clone()),
				LogicalExpr::RecordId(rid) => out.push(rid.table.clone()),
				LogicalExpr::Param(param) => {
					out.extend(self.variables.get(param.as_str())?.iter().cloned());
				}
				LogicalExpr::Literal(_)
				| LogicalExpr::Array(_)
				| LogicalExpr::Set(_)
				| LogicalExpr::Object(_)
				| LogicalExpr::Mock(_)
				| LogicalExpr::Constant(_)
				| LogicalExpr::Field(_)
				| LogicalExpr::Doc
				| LogicalExpr::Current
				| LogicalExpr::Access {
					..
				}
				| LogicalExpr::Index {
					..
				}
				| LogicalExpr::First(_)
				| LogicalExpr::Last(_)
				| LogicalExpr::Each(_)
				| LogicalExpr::Flatten {
					..
				}
				| LogicalExpr::Filter {
					..
				}
				| LogicalExpr::Method {
					..
				}
				| LogicalExpr::Destructure {
					..
				}
				| LogicalExpr::Optional {
					..
				}
				| LogicalExpr::Lookup {
					..
				}
				| LogicalExpr::Recurse(_)
				| LogicalExpr::RepeatRecurse(_)
				| LogicalExpr::Broadcast {
					..
				}
				| LogicalExpr::SetField {
					..
				}
				| LogicalExpr::Prefix {
					..
				}
				| LogicalExpr::Postfix {
					..
				}
				| LogicalExpr::Invoke {
					..
				}
				| LogicalExpr::Binary {
					..
				}
				| LogicalExpr::Call(_)
				| LogicalExpr::Closure(_)
				| LogicalExpr::If(_)
				| LogicalExpr::Throw(_)
				| LogicalExpr::Break
				| LogicalExpr::Continue
				| LogicalExpr::Subquery(_) => return None,
			}
		}
		Some(out)
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
				LogicalExpr::Literal(_)
				| LogicalExpr::Array(_)
				| LogicalExpr::Set(_)
				| LogicalExpr::Object(_)
				| LogicalExpr::RecordId(_)
				| LogicalExpr::Param(_)
				| LogicalExpr::Table(_)
				| LogicalExpr::Mock(_)
				| LogicalExpr::Constant(_)
				| LogicalExpr::Field(_)
				| LogicalExpr::Doc
				| LogicalExpr::Current
				| LogicalExpr::Access {
					..
				}
				| LogicalExpr::Index {
					..
				}
				| LogicalExpr::First(_)
				| LogicalExpr::Last(_)
				| LogicalExpr::Each(_)
				| LogicalExpr::Flatten {
					..
				}
				| LogicalExpr::Filter {
					..
				}
				| LogicalExpr::Method {
					..
				}
				| LogicalExpr::Destructure {
					..
				}
				| LogicalExpr::Optional {
					..
				}
				| LogicalExpr::Lookup {
					..
				}
				| LogicalExpr::Recurse(_)
				| LogicalExpr::RepeatRecurse(_)
				| LogicalExpr::Broadcast {
					..
				}
				| LogicalExpr::SetField {
					..
				}
				| LogicalExpr::Prefix {
					..
				}
				| LogicalExpr::Postfix {
					..
				}
				| LogicalExpr::Invoke {
					..
				}
				| LogicalExpr::Binary {
					..
				}
				| LogicalExpr::Closure(_)
				| LogicalExpr::If(_)
				| LogicalExpr::Throw(_)
				| LogicalExpr::Break
				| LogicalExpr::Continue => {}
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
				surrealdb_expr::parser::TopLevelExpr::Expr(expr) => self.expr(&expr),
				surrealdb_expr::parser::TopLevelExpr::Begin(_)
				| surrealdb_expr::parser::TopLevelExpr::Cancel
				| surrealdb_expr::parser::TopLevelExpr::Commit
				| surrealdb_expr::parser::TopLevelExpr::Access(_)
				| surrealdb_expr::parser::TopLevelExpr::Kill(_)
				| surrealdb_expr::parser::TopLevelExpr::Live(_)
				| surrealdb_expr::parser::TopLevelExpr::Option(_)
				| surrealdb_expr::parser::TopLevelExpr::Use(_)
				| surrealdb_expr::parser::TopLevelExpr::Show(_) => self.schema = true,
			}
		}
	}

	fn function(&mut self, function: &crate::expr::Function) {
		match function {
			crate::expr::Function::Custom(name) => {
				self.calls.insert(name.clone());
			}
			other @ (crate::expr::Function::Normal(_)
			| crate::expr::Function::Script(_)
			| crate::expr::Function::Model(_)
			| crate::expr::Function::Module(..)
			| crate::expr::Function::Silo {
				..
			}) => self.schema |= !other.read_only(),
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
			crate::expr::Expr::Literal(_)
			| crate::expr::Expr::Param(_)
			| crate::expr::Expr::Idiom(_)
			| crate::expr::Expr::Mock(_)
			| crate::expr::Expr::Block(_)
			| crate::expr::Expr::Constant(_)
			| crate::expr::Expr::Prefix {
				..
			}
			| crate::expr::Expr::Postfix {
				..
			}
			| crate::expr::Expr::Binary {
				..
			}
			| crate::expr::Expr::FunctionCall(_)
			| crate::expr::Expr::Closure(_)
			| crate::expr::Expr::Break
			| crate::expr::Expr::Continue
			| crate::expr::Expr::Return(_)
			| crate::expr::Expr::Throw(_)
			| crate::expr::Expr::IfElse(_)
			| crate::expr::Expr::Select(_)
			| crate::expr::Expr::Create(_)
			| crate::expr::Expr::Update(_)
			| crate::expr::Expr::Upsert(_)
			| crate::expr::Expr::Delete(_)
			| crate::expr::Expr::Relate(_)
			| crate::expr::Expr::Insert(_)
			| crate::expr::Expr::Define(_)
			| crate::expr::Expr::Remove(_)
			| crate::expr::Expr::Rebuild(_)
			| crate::expr::Expr::Alter(_)
			| crate::expr::Expr::Info(_)
			| crate::expr::Expr::Foreach(_)
			| crate::expr::Expr::Let(_)
			| crate::expr::Expr::Sleep(_)
			| crate::expr::Expr::Explain {
				..
			} => self.0.every_table = true,
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
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Block(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_)
			| Expr::Explain {
				..
			} => {}
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

/// Whether a statement of the list block `op` holds may change the catalog for the statements
/// after it through code no speculative run models: a body its text does not show, which
/// [`runs_unseen_schema`] finds may run a schema statement. A statement that holds a schema
/// statement of its own is not judged here. The statements of a `FOR` body follow each other on
/// every iteration, so the last of them is judged too. `false` for a planner that already plans
/// the block's statements after a change when the block reaches them.
pub(crate) async fn hides_a_change_point(planner: &Planner<'_>, op: &LogicalOp) -> bool {
	if planner.is_late_bound() || planner.binds_late_after_schema() || planner.txn().is_none() {
		return false;
	}
	let (statements, looped) = match op {
		LogicalOp::Sequence(sequence) => (sequence.stmts.as_slice(), false),
		LogicalOp::Foreach(foreach) => match foreach.body.as_ref() {
			LogicalOp::Sequence(sequence) => (sequence.stmts.as_slice(), true),
			body @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
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
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)) => (std::slice::from_ref(body), true),
		},
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
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
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => return false,
	};
	let judged = match looped {
		true => statements,
		false => statements.split_last().map_or(&[][..], |(_, before)| before),
	};
	for statement in judged {
		if crate::exec::planner::fold::changes_the_catalog(statement) {
			continue;
		}
		let outside = closures_from_outside(planner, [statement], &[]);
		if runs_unseen_schema(planner, statement, outside).await {
			return true;
		}
	}
	false
}

/// How statement `op`, which `bound` names are bound around, reaches what code
/// it runs may redefine while it runs ([`crate::exec::planner::CatalogWithin`]).
///
/// The code is what the plan cannot see into: an embedded script, a closure
/// invoked through a value that may come from outside the block, a function
/// body that reaches either or holds a schema statement, an event or field
/// clause of a table it writes that does, and a schema statement held in one
/// of its expressions. A statement that reaches none of it plans as it is.
/// A schema statement is not judged here. A block is judged whole, for what
/// it binds; each statement it runs is judged again for itself as the block
/// plans it.
pub(crate) async fn catalog_within(
	planner: &Planner<'_>,
	op: &LogicalOp,
	bound: &[String],
) -> crate::exec::planner::CatalogWithin {
	catalog_within_reaching(planner, op, bound, None).await
}

/// [`catalog_within`] for `op`, a write planned as the stage bundle of `table`: the write writes
/// `table`, whatever its targets name.
pub(crate) async fn catalog_within_writing(
	planner: &Planner<'_>,
	op: &LogicalOp,
	table: &surrealdb_strand::TableName,
) -> crate::exec::planner::CatalogWithin {
	catalog_within_reaching(planner, op, &[], Some(table)).await
}

async fn catalog_within_reaching(
	planner: &Planner<'_>,
	op: &LogicalOp,
	bound: &[String],
	root_table: Option<&surrealdb_strand::TableName>,
) -> crate::exec::planner::CatalogWithin {
	if planner.is_late_bound()
		|| planner.txn().is_none()
		|| matches!(op, LogicalOp::Ddl(_) | LogicalOp::Explain(_))
	{
		return crate::exec::planner::CatalogWithin::default();
	}
	// A block's own schema statements are the block's to plan around.
	let block = matches!(op, LogicalOp::Sequence(_) | LogicalOp::Foreach(_));
	let holds_schema = !block && crate::exec::planner::fold::changes_the_catalog(op);
	let may_redefine = holds_schema || {
		let outside = closures_from_outside(planner, [op], bound);
		reached(planner, op, outside, root_table).await.is_none()
	};
	if !may_redefine {
		return crate::exec::planner::CatalogWithin::default();
	}
	// A write, and a statement holding a write or a schema statement, plan
	// the statements in their expressions as the catalog stands when each
	// runs.
	let writes = matches!(op, LogicalOp::Write(_))
		|| holds_schema
		|| op
			.visit_ops_deep(&mut |node| match node {
				LogicalOp::Write(_) => std::ops::ControlFlow::Break(()),
				LogicalOp::Source(_)
				| LogicalOp::Values(_)
				| LogicalOp::Match(_)
				| LogicalOp::Subquery(_)
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
				| LogicalOp::Sleep(_) => std::ops::ControlFlow::Continue(()),
			})
			.is_break();
	crate::exec::planner::CatalogWithin {
		calls_current: true,
		nested_late: writes,
	}
}
