//! Table dispatch: reading records and tables whose identity only the data
//! knows.
//!
//! A program reads a table or a record by a name its text does not hold when
//! a `FROM` takes a parameter or a call, when a `FETCH` follows a link, when
//! a path dereferences one. Which table the read reaches is a fact of the
//! data. Each table is read by a unit that is the table read as
//! `SELECT * FROM table` reads it, with the table's permission, its computed
//! fields and the permissions of its fields lowered as for a scan of the
//! table named in the text.
//!
//! A unit serves two reads. Run as a program it streams every record of the
//! table the caller may select; its one pipeline's body, run over the raw
//! record a read by id fetched, is the dereference of that record
//! ([`Deref::fetch_slot`]).
//!
//! The units are kept by the compile cache ([`super::compile_cache`]), keyed
//! by the table, the catalog version of its definitions and the build, with
//! one compilation per frame: the class of the principal and the permission
//! contract ([`table_unit`]). A program holds none of them. When a query
//! arrives, planning asks the cache for the units of the tables its sites name
//! by analysis ([`build`]): the tables of record ids and table names written
//! in the text, and the tables that the kinds of the fields a `FETCH` walks
//! name for the records it fetches ([`fetch_reach`]), with the tables those
//! units read in turn. The count follows from the statement and the
//! definitions it reaches, never from how many tables the database holds, so
//! planning never lists the database's tables for a read. A run loads a
//! table's unit by the table's name the first time it meets the table in a
//! frame ([`Deref::serving`]), and later reads of that table find it loaded,
//! by that program or any other program of the run. A table no plan asked
//! for, such as a table a site may name at run time or a record of a table
//! the analysis did not name, is compiled in a fault when the cache holds no
//! unit of it ([`super::fault::link_read`]). A read in a frame the unit was
//! not compiled for, or of a table with no compiled read unit, is made by the
//! record-fetch helpers of `operators::fetch`.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::Arc;

use surrealdb_strand::TableName;

use super::host::{host_frames, run_cow_row_from, run_unit_inline, unit_state};
use super::program::{Program, UnitHead, UnitSig};
use super::{Lowered, NotCovered};
use crate::exec::operators::fetch::{fetch_raw_deref_slot, require_table};
use crate::exec::operators::link_window::Slot;
use crate::exec::permission::validate_record_user_access;
use crate::exec::planner::Planner;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::logical::{LogicalFields, LogicalOp, LogicalProject, LogicalSource};
use crate::val::{RecordId, Value};

#[cfg(test)]
thread_local! {
	/// How many records this thread's dispatch reads ran through the host
	/// rather than in place, for tests that must know which did.
	pub(super) static HOSTED_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many of those the host continued on the state their run in place
	/// left, rather than running them whole on a fresh one.
	pub(super) static CONTINUED_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The tables a read can reach.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Reach {
	/// These tables.
	Tables(BTreeSet<TableName>),
	/// Any table of the database. The tables it holds are those the program's sites name, whose
	/// read units are linked when the query arrives; the run loads the rest.
	Any(BTreeSet<TableName>),
}

assert_plain!(Reach);

impl Reach {
	/// The reach of no read.
	pub(super) fn none() -> Self {
		Reach::Tables(BTreeSet::new())
	}

	/// The reach of a read of any table, naming none.
	pub(crate) fn any() -> Self {
		Reach::Any(BTreeSet::new())
	}

	/// The reach of `self` and `other` together: the tables either names, and any table where
	/// either reaches any.
	pub(super) fn union(self, other: Reach) -> Reach {
		match (self, other) {
			(Reach::Tables(mut a), Reach::Tables(b)) => {
				a.extend(b);
				Reach::Tables(a)
			}
			(Reach::Tables(mut a) | Reach::Any(mut a), Reach::Tables(b) | Reach::Any(b)) => {
				a.extend(b);
				Reach::Any(a)
			}
		}
	}

	/// Whether `table` is reached.
	pub(super) fn reaches(&self, table: &TableName) -> bool {
		match self {
			Reach::Any(_) => true,
			Reach::Tables(tables) => tables.contains(table),
		}
	}
}

/// The tables a program's sites name, in table order, whose read units the
/// compile cache was asked for when the query arrived ([`build`]): a run
/// loads each by name ([`Deref::serving`]), and finds it kept.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableDispatch {
	pub(super) tables: Vec<TableName>,
}

assert_plain!(TableDispatch);

/// The tables a program reads records of by id, and the read units the run
/// loaded for them: what a context carries while the program runs
/// ([`ExecutionContext::vm_deref`]), so that a read of a record by id
/// compiles nothing outside the fault that compiles a table's unit the
/// compile cache does not hold.
pub(crate) struct Deref {
	dispatch: Arc<TableDispatch>,
	/// The context the program started in: the frame the read units the run
	/// loads are linked for.
	start: ExecutionContext,
	/// The class of the principal and the permission contract of `start`,
	/// which a load is kept under.
	frame: (u64, bool),
	/// The units the run loaded, shared with every program of the run
	/// ([`super::closure::ClosureUnits::linked_reads`]).
	linked: Arc<LinkedReads>,
	/// The loads of the first [`MET`] tables this program's reads met, each
	/// set once: a later read of a table met before finds its unit here, with
	/// no lock and no lookup.
	met: [std::sync::OnceLock<Met>; MET],
	/// How many entries of `met` have been claimed.
	met_len: std::sync::atomic::AtomicUsize,
}

/// How many tables a program's reads keep the loads of ([`Deref::met`]).
const MET: usize = 8;

/// A table a program's reads met, and the read the run loaded for it.
struct Met {
	table: TableName,
	unchecked: bool,
	/// The `VERSION` stamp the read was made at.
	stamp: Option<u64>,
	linked: Arc<LinkedRead>,
}

impl std::fmt::Debug for Deref {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Deref").field("dispatch", &self.dispatch).finish_non_exhaustive()
	}
}

/// What a run loaded for a table, by the scope it was loaded in and the
/// table.
#[derive(Default)]
pub(crate) struct LinkedReads {
	reads: std::sync::RwLock<LinkedScopes>,
}

/// The links of a run, by scope and table.
type LinkedScopes =
	std::collections::HashMap<LinkScope, std::collections::HashMap<TableName, Arc<LinkedRead>>>;

/// What a load is kept under besides its table: the dispatch, whether the
/// read waives permission checks, and the class of the principal and the
/// permission contract of the program's start. The dispatch is named by its
/// address, which the load's own handle on it keeps from naming another while
/// the run lasts.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct LinkScope {
	dispatch: usize,
	unchecked: bool,
	stamp: Option<u64>,
	frame: (u64, bool),
}

/// A table's read as a run loaded it: the unit that reads it, or none where
/// the table is not defined or no unit of the read is compiled for the
/// frame. It stands while the transaction has stamped no more table catalog
/// versions than it had when the read was loaded.
pub(crate) struct LinkedRead {
	_dispatch: Arc<TableDispatch>,
	unit: Option<Arc<LinkedUnit>>,
	stamps: u64,
}

/// A loaded unit, and whether its body has handed the host a step over a
/// record this run read.
pub(crate) struct LinkedUnit {
	program: Arc<Program>,
	steps: std::sync::atomic::AtomicBool,
	/// The table the unit reads.
	table: TableName,
	/// Whether deriving a record of `table` can derive a record again: a
	/// `COMPUTED` field of it has a body that holds a statement, a closure or a
	/// call of a user-defined function ([`needs_caller`]). Set from the table's
	/// definitions by the first read whose body the host runs, so a read the
	/// unit answers in place looks nothing up.
	///
	/// [`needs_caller`]: crate::exec::ir::logical::stages::needs_caller
	reenters: tokio::sync::OnceCell<bool>,
}

impl LinkedUnit {
	/// Set [`Self::reenters`] from the table's definitions, and answer it.
	async fn learn_reenters(&self, ctx: &ExecutionContext) -> FlowResult<bool> {
		let reenters = self
			.reenters
			.get_or_try_init(|| async {
				let table = super::descriptor::table_at(ctx, &self.table, ctx.version_stamp())
					.await
					.map_err(ControlFlow::Err)?;
				let reenters = table.fields.iter().any(|field| {
					field.computed_logical().is_some_and(|body| {
						body.is_ok_and(crate::exec::ir::logical::stages::needs_caller)
					})
				});
				Ok::<_, ControlFlow>(reenters)
			})
			.await?;
		Ok(*reenters)
	}
}

/// The unit that serves a read: one the run loaded, handed on, or kept by
/// the program's reads for the table ([`Deref::met`]).
pub(super) enum Served<'a> {
	Linked(Arc<LinkedUnit>),
	Met(&'a LinkedUnit),
}

impl Served<'_> {
	/// The unit.
	pub(super) fn program(&self) -> &Arc<Program> {
		match self {
			Served::Linked(linked) => &linked.program,
			Served::Met(linked) => &linked.program,
		}
	}

	/// Whether the unit's body has handed the host a step over a record this
	/// run read.
	fn steps(&self) -> &std::sync::atomic::AtomicBool {
		match self {
			Served::Linked(linked) => &linked.steps,
			Served::Met(linked) => &linked.steps,
		}
	}

	/// The loaded unit.
	fn unit(&self) -> &LinkedUnit {
		match self {
			Served::Linked(linked) => linked,
			Served::Met(linked) => linked,
		}
	}
}

/// Whether `program`, a table's read, was lowered for the frame `ctx` runs
/// in, where it waives permission checks when `unchecked`.
fn admits(program: &Program, ctx: &ExecutionContext, unchecked: bool) -> bool {
	program.unit.as_ref().is_some_and(|head| {
		super::host::frame_admits_as(&head.frame, ctx, unchecked, |frame| {
			super::host::class_digest(ctx, frame)
		})
	})
}

impl Deref {
	/// The reads of `dispatch` for a program starting in `start`: the loads of
	/// the run `start` is inside of are shared.
	pub(super) fn new(dispatch: Arc<TableDispatch>, start: &ExecutionContext) -> Self {
		let linked = match start.vm_closures() {
			Some(run) => Arc::clone(run.linked_reads()),
			None => start.vm_deref().map(|outer| Arc::clone(&outer.linked)).unwrap_or_default(),
		};
		let frame = (super::host::class_digest_here(start), start.root().perms_suppressed);
		Self {
			dispatch,
			start: start.clone(),
			frame,
			linked,
			met: std::array::from_fn(|_| std::sync::OnceLock::new()),
			met_len: std::sync::atomic::AtomicUsize::new(0),
		}
	}

	/// The unit that reads `table` in the frame `ctx` runs in, waiving
	/// permission checks when `unchecked`: the one the run loaded from the
	/// compile cache by the table's name, loading it the first time the run
	/// meets the table. `None` where the unit was compiled for another frame,
	/// or none is compiled for the read.
	pub(super) async fn serving(
		&self,
		ctx: &ExecutionContext,
		table: &TableName,
		unchecked: bool,
	) -> FlowResult<Option<Served<'_>>> {
		let stamps = ctx.try_txn().map_or(0, |txn| txn.catalog_stamps());
		let claimed = self.met_len.load(std::sync::atomic::Ordering::Acquire).min(MET);
		for slot in &self.met[..claimed] {
			if let Some(met) = slot.get()
				&& met.unchecked == unchecked
				&& met.stamp == ctx.version_stamp()
				&& met.linked.stamps == stamps
				&& met.table == *table
			{
				return Ok(met
					.linked
					.unit
					.as_deref()
					.filter(|unit| admits(&unit.program, ctx, unchecked))
					.map(Served::Met));
			}
		}
		let linked = self.link(ctx, table, unchecked, stamps).await?;
		let at = self.met_len.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
		if let Some(slot) = self.met.get(at) {
			let _ = slot.set(Met {
				table: table.clone(),
				unchecked,
				stamp: ctx.version_stamp(),
				linked: Arc::clone(&linked),
			});
		}
		Ok(linked
			.unit
			.as_ref()
			.filter(|unit| admits(&unit.program, ctx, unchecked))
			.map(|unit| Served::Linked(Arc::clone(unit))))
	}

	/// What the run linked for `table`, read waiving permission checks when
	/// `unchecked`, while the transaction has stamped `stamps` table catalog
	/// versions, linking it the first time the run meets it under them, there
	/// in the frame `ctx` runs in.
	async fn link(
		&self,
		ctx: &ExecutionContext,
		table: &TableName,
		unchecked: bool,
		stamps: u64,
	) -> FlowResult<Arc<LinkedRead>> {
		let scope = LinkScope {
			dispatch: Arc::as_ptr(&self.dispatch) as usize,
			unchecked,
			stamp: ctx.version_stamp(),
			frame: self.frame,
		};
		let held = self
			.linked
			.reads
			.read()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.get(&scope)
			.and_then(|tables| tables.get(table))
			.filter(|linked| linked.stamps == stamps)
			.map(Arc::clone);
		if let Some(linked) = held {
			return Ok(linked);
		}
		let unit = match load(&self.start, table, unchecked, ctx.version_stamp()).await? {
			Some(unit) => Some(unit),
			None => super::fault::link_read(&self.start, ctx, table, unchecked).await?,
		};
		let linked = Arc::new(LinkedRead {
			_dispatch: Arc::clone(&self.dispatch),
			unit: unit.map(|program| {
				Arc::new(LinkedUnit {
					program,
					steps: std::sync::atomic::AtomicBool::new(false),
					table: table.clone(),
					reenters: tokio::sync::OnceCell::new(),
				})
			}),
			stamps,
		});
		// A read linked by another reader of the run meanwhile stands, while
		// the catalog it was linked against does.
		let mut reads =
			self.linked.reads.write().unwrap_or_else(std::sync::PoisonError::into_inner);
		let tables = reads.entry(scope).or_default();
		let entry = tables.entry(table.clone()).or_insert_with(|| Arc::clone(&linked));
		if entry.stamps != stamps {
			*entry = Arc::clone(&linked);
		}
		Ok(Arc::clone(entry))
	}

	/// Whether the program's sites name `table`, one the catalog held when
	/// the program was lowered.
	pub(crate) fn holds(&self, table: &TableName) -> bool {
		self.dispatch.tables.binary_search(table).is_ok()
	}

	/// The record `rid` names, as its table's unit reads it: `NONE` where no
	/// record answers or the caller may not select it, and an error where
	/// the table does not exist. `None` where no unit serves the read
	/// ([`Self::serving`]), which the caller then makes with the record-fetch
	/// helpers of `operators::fetch`.
	///
	/// With `checked` the caller's table and field permissions apply and the
	/// caller is confined to its own tenant; without it the record is read
	/// as a permission predicate reads it, its computed fields derived and
	/// no permission applied.
	///
	/// The record's stored value is read from `slot` where a window holds it for `rid`, and
	/// otherwise from the transaction. A read no unit serves leaves `slot` for the caller's own
	/// read of the record.
	pub(crate) async fn fetch_slot(
		&self,
		ctx: &ExecutionContext,
		rid: &RecordId,
		checked: bool,
		slot: &mut Slot<'_>,
	) -> Option<FlowResult<Value>> {
		let skips = ctx.root().skip_fetch_perms;
		let unchecked = !checked || skips;
		// The unit for the frame decides whether a unit serves the read; the
		// read itself is made by it, unless the read waives the checks the
		// frame applies, which another unit is lowered for. That unit is only
		// asked whether it would serve, and is linked only by a read it makes.
		if unchecked != skips && !self.frame_serves(ctx, &rid.table, skips) {
			return None;
		}
		let served = match self.serving(ctx, &rid.table, unchecked).await {
			Ok(Some(served)) => served,
			Ok(None) => return None,
			Err(e) => return Some(Err(e)),
		};
		let slot = std::mem::replace(slot, Slot::Unread);
		Some(self.fetch_served(ctx, rid, checked, served, unchecked, slot).await)
	}

	/// Read `row`, a record of `table` as storage holds it, in place, by the
	/// table's read unit, permission-checked where `checked`, as
	/// [`Self::fetch_slot`] reads a record it fetched: whether the record is shown.
	/// `None`, with `row` untouched, where no unit serves the read in this
	/// frame, or where the unit's rows are enforced by the host
	/// ([`super::program::Pipeline::enforce`]), which is the enforcement this
	/// read stands for.
	pub(crate) async fn read_record(
		&self,
		ctx: &ExecutionContext,
		table: &TableName,
		row: &mut Value,
		checked: bool,
	) -> Option<FlowResult<bool>> {
		let skips = ctx.root().skip_fetch_perms;
		let unchecked = !checked || skips;
		if unchecked != skips && !self.frame_serves(ctx, table, skips) {
			return None;
		}
		let served = match self.serving(ctx, table, unchecked).await {
			Ok(Some(served)) => served,
			Ok(None) => return None,
			Err(e) => return Some(Err(e)),
		};
		let pipeline = match read_pipeline(served.program()) {
			Ok(pipeline) => pipeline,
			Err(e) => return Some(Err(e)),
		};
		if pipeline.enforce.is_some() {
			return None;
		}
		if checked {
			let confined = || -> FlowResult<()> {
				validate_record_user_access(ctx.database()?)?;
				Ok(())
			};
			if let Err(e) = confined() {
				return Some(Err(e));
			}
		}
		let raw = std::mem::take(row);
		Some(match self.read_with(ctx, &served, raw, unchecked).await {
			Ok(Value::None) => Ok(false),
			Ok(read) => {
				*row = read;
				Ok(true)
			}
			Err(e) => Err(e),
		})
	}

	/// Whether the unit that reads `table` for the frame `ctx` runs in, which
	/// waives permission checks when `skips`, would serve a read there, where
	/// the unit that waives them, compiled for the same class of principal,
	/// namespace and database, serves the read: a unit the run loads is linked
	/// for the permission contract of the program's start, which is all of its
	/// frame left to judge.
	fn frame_serves(&self, ctx: &ExecutionContext, _table: &TableName, skips: bool) -> bool {
		skips || !self.frame.1 || ctx.root().perms_suppressed
	}

	/// [`Self::fetch_slot`], for a read the unit `served` serves.
	async fn fetch_served(
		&self,
		ctx: &ExecutionContext,
		rid: &RecordId,
		checked: bool,
		served: Served<'_>,
		unchecked: bool,
		slot: Slot<'_>,
	) -> FlowResult<Value> {
		let Some(raw) = fetch_raw_deref_slot(ctx, rid, slot, ctx.version_stamp()).await? else {
			if checked {
				require_table(ctx, rid).await?;
			}
			return Ok(Value::None);
		};
		if checked {
			validate_record_user_access(ctx.database()?)?;
		}
		self.read_with(ctx, &served, raw, unchecked).await
	}

	/// `raw`, a record of `table` as storage holds it, as the table's unit
	/// reads it; `NONE` where the unit does not admit it. The unit is the one
	/// for a frame that waives permission checks when `unchecked`, which must
	/// serve the frame `ctx` runs in.
	pub(super) async fn read(
		&self,
		ctx: &ExecutionContext,
		table: &TableName,
		raw: Value,
		unchecked: bool,
	) -> FlowResult<Value> {
		let Some(served) = self.serving(ctx, table, unchecked).await? else {
			return Err(internal(format!(
				"the table {table} read in a frame no unit of it was lowered for"
			)));
		};
		self.read_with(ctx, &served, raw, unchecked).await
	}

	/// [`Self::read`] by the unit `served`, admitted for the frame `ctx`
	/// runs in.
	async fn read_with(
		&self,
		ctx: &ExecutionContext,
		served: &Served<'_>,
		raw: Value,
		unchecked: bool,
	) -> FlowResult<Value> {
		let program: &Program = served.program();
		// A loaded unit holds the units it calls.
		let state = || -> FlowResult<_> {
			let mut state = unit_state(program, ctx)?;
			state.units = None;
			state.permissions_bypassed |= unchecked;
			// A block instruction of the body runs a group of the program, which the
			// state reaches through the program it names.
			if !program.groups.is_empty() {
				state.program = Some(Arc::clone(served.program()));
			}
			state.read_in_place_permission_errors = true;
			Ok(state)
		};
		let pipeline = read_pipeline(program)?;
		// The body of a table with no computed field, view or permission to
		// apply answers the record as it is.
		if pipeline.hands_row_on() {
			return Ok(raw);
		}
		// A body that answers without the host, its layers aside, runs in
		// place, over a state without the frames only the host's steps read;
		// one that hands the host a step is continued by the host where it
		// stopped, on that state where it can be.
		let steps = served.steps();
		let (raw, start, mut state) = if steps.load(std::sync::atomic::Ordering::Relaxed) {
			(raw, 0, state()?)
		} else {
			let mut inline = state()?;
			match run_unit_inline(program, pipeline, Cow::Owned(raw), &mut inline) {
				Ok(_) => return Ok(inline.output.pop().unwrap_or(Value::None)),
				Err(unfinished) => {
					if !unfinished.yielded {
						steps.store(true, std::sync::atomic::Ordering::Relaxed);
					}
					let raw = unfinished.row.into_owned();
					match unfinished.resume {
						Some(start) => {
							#[cfg(test)]
							CONTINUED_READS.with(|continued| continued.set(continued.get() + 1));
							(raw, start, inline)
						}
						None => (raw, 0, state()?),
					}
				}
			}
		};
		#[cfg(test)]
		HOSTED_READS.with(|hosted| hosted.set(hosted.get() + 1));
		// A body that can derive a record again spends a unit of the computation-depth budget for
		// the record, as deriving the fields of a record read without a unit does
		// ([`crate::exec::operators::scan::pipeline::compute_fields_for_value`]), so a cycle of
		// derivations ends in the computation-depth error.
		let unit = served.unit();
		let reenters = match unit.reenters.get() {
			Some(reenters) => *reenters,
			None => super::host::out_of_line(|| unit.learn_reenters(ctx)).await?,
		};
		let dived;
		let ctx = match reenters {
			true => {
				dived = ctx.dive().map_err(ControlFlow::Err)?;
				&dived
			}
			false => ctx,
		};
		host_frames(program, &mut state, ctx);
		// The body derives the record's computed fields, which may dereference a link and so reach
		// this read again. It runs as a nested evaluation of the statement's driver
		// ([`crate::exec::trampoline::nest`]), so a cycle of links holds the native stack of one
		// level whatever its depth, and only the computation-depth budget bounds it.
		let program = Arc::clone(served.program());
		let (ran, mut state) = crate::exec::trampoline::nest_boxed(hosted_read(
			program,
			raw,
			state,
			ctx.clone(),
			start,
		))
		.await;
		ran?;
		Ok(state.output.pop().unwrap_or(Value::None))
	}
}

/// The body of `program`'s read pipeline run by the host over `raw` from
/// instruction `start`, on `state`, which it hands back. Boxed with its type
/// named, since the body may reach [`Deref::read_with`] again.
fn hosted_read(
	program: Arc<Program>,
	raw: Value,
	mut state: super::interp::PooledState,
	ctx: ExecutionContext,
	start: usize,
) -> crate::exec::BoxFut<'static, (FlowResult<super::host::Ended>, super::interp::PooledState)> {
	Box::pin(async move {
		let ran = match read_pipeline(&program) {
			Ok(pipeline) => {
				run_cow_row_from(&program, pipeline, Cow::Owned(raw), &mut state, &ctx, start).await
			}
			Err(e) => Err(e),
		};
		(ran, state)
	})
}

fn internal(why: String) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(why)))
}

/// The pipeline a table's read unit runs over a record it reads in place
/// ([`Program::own_pipeline`]). A unit that holds no such pipeline is an
/// invariant break, since [`lower_table_unit`] admits only units that hold one.
fn read_pipeline(program: &Program) -> FlowResult<&super::program::Pipeline> {
	program
		.own_pipeline()
		.ok_or_else(|| internal("a table's read unit holds no pipeline of its own".to_owned()))
}

/// The statement that reads every record of `table` the caller may select.
pub(super) fn read_all(table: &TableName) -> LogicalOp {
	LogicalOp::Project(LogicalProject {
		input: Box::new(LogicalOp::Source(LogicalSource {
			source: crate::expr::logical::SourceRef::Table(table.clone()),
			schema: None,
			with: None,
			for_update: false,
		})),
		fields: LogicalFields::all(),
		omit: Vec::new(),
	})
}

/// The unit that reads `table` for the frame `planner` lowers for, served
/// by the compile cache ([`super::compile_cache`]): keyed by the table, the
/// catalog version its definitions were read at and the build, with one
/// compilation per frame — the class of the principal and the permission
/// contract. It is compiled at the depth of a statement, binding no name of
/// the scope around the read ([`Planner::unit_root`]), and kept against what
/// else its compilation read: the definitions of the tables and functions its
/// permissions and computed fields read, and the values its binding folded in
/// from the planning context. A table whose catalog no write of this build has
/// stamped, and a planner the cache serves nothing for, compile it for this
/// plan alone.
///
/// What the cache reads to judge a kept unit is read unsealed; the lowering
/// itself is sealed as any ([`crate::kvs::catalog_reads::sealed`]).
pub(super) async fn table_unit(planner: &Planner<'_>, table: &TableName) -> Lowered<Arc<Program>> {
	Ok(table_read_unit(planner, table).await?.program)
}

/// A table's read unit as the compile cache keeps it.
#[derive(Debug, Clone)]
pub(crate) struct TableReadUnit {
	pub(crate) program: Arc<Program>,
}

/// [`table_unit`], as the compile cache keeps it.
pub(super) async fn table_read_unit(
	planner: &Planner<'_>,
	table: &TableName,
) -> Lowered<TableReadUnit> {
	use crate::kvs::catalog_reads::sealed;
	let compile = |record: Option<Arc<crate::kvs::Transaction>>| async move {
		let root = planner.unit_root();
		let recording = record.as_ref().map(|txn| txn.record_catalog_reads());
		let lowered =
			sealed(false, crate::exec::vm::host::out_of_line(|| lower_table_unit(&root, table)))
				.await;
		let recorded = recording.map(|recording| recording.finish());
		let program = lowered?;
		let unit = TableReadUnit {
			program: Arc::new(program),
		};
		Ok::<_, NotCovered>((unit, recorded))
	};
	let cached = planner.unit_cache().filter(|_| planner.keeps_catalog_units());
	let key = match &cached {
		Some(_) => sealed(false, planner.table_unit_key(table)).await,
		None => None,
	};
	let (Some((units, txn)), Some(key)) = (cached, key) else {
		return Ok(compile(None).await?.0);
	};
	let frame = super::statement_frame(planner);
	let compiled = std::sync::atomic::AtomicBool::new(false);
	let served = sealed(
		false,
		units.table_reads().get_or_compile(&key, &frame, &txn, planner.frozen(), || async {
			compiled.store(true, std::sync::atomic::Ordering::Relaxed);
			let (program, recorded) = compile(Some(Arc::clone(&txn))).await?;
			let deps = match recorded {
				Some(recorded) => {
					match crate::exec::catalog_deps::CatalogDeps::resolve(&txn, &recorded).await {
						Some(deps) => super::units::ContextDeps::resolve(
							&txn,
							&recorded,
							planner.frozen(),
							BTreeSet::new(),
						)
						.await
						.map(|context| {
							(deps, context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS))
						}),
						None => None,
					}
				}
				None => None,
			};
			Ok::<_, NotCovered>((program, deps))
		}),
	)
	.await?;
	#[cfg(test)]
	super::units::CATALOG_UNITS.with(|units| {
		let (c, l) = units.get();
		units.set(if compiled.load(std::sync::atomic::Ordering::Relaxed) {
			(c + 1, l)
		} else {
			(c, l + 1)
		});
	});
	Ok(served)
}

/// The unit that reads `table`, lowered for the frame `planner` lowers for.
async fn lower_table_unit(planner: &Planner<'_>, table: &TableName) -> Lowered<Program> {
	let statement = read_all(table);
	// A read whose binding fails fails with the error the binding raises.
	let bound = crate::exec::planner::bind_statement(planner, statement).await.map_err(|e| {
		match super::payload::Refusal::of(&e) {
			Some(refusal) => NotCovered::refused(refusal),
			None => NotCovered::unsupported(format!("the table {table} did not bind: {e}")),
		}
	})?;
	// A table unit is cached under its own key and served to any frame that asks for it, so its
	// pipelines are placed in the scope of its own statement alone.
	let mut program =
		crate::exec::vm::host::out_of_line(|| super::compile::compile_unit(planner, &bound))
			.await?;
	// The read is the one pipeline no block statement owns
	// ([`Program::own_pipeline`]). The others are the bodies of statements
	// nested in the table's permissions, which the read runs as groups of the
	// program.
	if program.write.is_some() || program.own_pipeline().is_none() {
		return Err(NotCovered::unsupported(format!(
			"the table {table} is not read by one pipeline"
		)));
	}
	program.unit = Some(UnitHead {
		sig: UnitSig {
			params: Vec::new(),
			takes_row: true,
		},
		frame: super::statement_frame(planner),
		entry: None,
		closure: None,
	});
	Ok(program)
}

/// What `program` and the units it holds, which hold no dispatch of their
/// own, read records of by id.
pub(super) fn reach_of(program: &Program) -> Reach {
	let mut reach = program.reach.clone().unwrap_or_else(Reach::none);
	for unit in program.units.iter().filter(|unit| unit.dispatch.is_none()) {
		reach = reach.union(reach_of(unit));
	}
	reach
}

/// The context `ctx` runs `program` in: the one that reads records by id
/// through the program's dispatch when it holds one, and `None` when `ctx`
/// itself serves, which it does when it reads through that same dispatch
/// already, as a unit run again in the run that set it up does.
pub(super) fn with_dispatch(program: &Program, ctx: &ExecutionContext) -> Option<ExecutionContext> {
	let dispatch = program.dispatch.as_ref()?;
	if ctx.vm_deref().is_some_and(|deref| Arc::ptr_eq(&deref.dispatch, dispatch)) {
		return None;
	}
	Some(ctx.with_vm_deref(Arc::new(Deref::new(Arc::clone(dispatch), ctx))))
}

/// The read unit of `table` for a program that started in `start`, waiving
/// permission checks where `unchecked`, loaded from the compile cache by the
/// table's name: keyed by the table's catalog version as the transaction
/// reads it, and linked for the frame a plan made in `start` compiles it for
/// ([`table_unit`]). `None` where the cache holds none that stands, and for a
/// read at a `VERSION` stamp, which the run then asks to be compiled
/// ([`super::fault::link_read`]). The load reads
/// the catalog version and what the unit's dependencies are, a seam of the
/// run-time seal ([`crate::exec::compiles::seam`]); it compiles nothing.
async fn load(
	start: &ExecutionContext,
	table: &TableName,
	unchecked: bool,
	stamp: Option<u64>,
) -> FlowResult<Option<Arc<Program>>> {
	use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
	// A read at a `VERSION` stamp is compiled from the definitions the stamp
	// holds, which no unit the cache keeps is.
	if stamp.is_some() {
		return Ok(None);
	}
	let Some(units) = start.ctx().compiled_units() else {
		return Ok(None);
	};
	let Ok(db_ctx) = start.database() else {
		return Ok(None);
	};
	let Some(txn) = start.try_txn() else {
		return Ok(None);
	};
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let (ns_name, db_name) = (db_ctx.ns_name().to_owned(), db_ctx.db_name().to_owned());
	let root = start.root();
	let auth_enabled = start.ctx().auth_enabled();
	// The frame `read_planner` and `table_unit` compile the read for: a
	// principal whose reads are checked waives them in a frame of its own.
	let checked = !root.perms_suppressed
		&& crate::exec::permission::action_is_checked(
			auth_enabled,
			&root.auth,
			crate::iam::Action::View,
			Some(&ns_name),
			Some(&db_name),
		);
	let frame = super::program::UnitFrame {
		class_digest: super::program::UnitFrame::digest(
			auth_enabled,
			Some(&root.auth),
			Some(&ns_name),
			Some(&db_name),
		),
		ns: Some(ns_name.clone()),
		db: Some(db_name.clone()),
		perms_suppressed: root.perms_suppressed || (unchecked && checked),
		checked: true,
	};
	crate::exec::compiles::seam(async move {
		let Some(version) = txn.catalog_version(ns, db, table).await.map_err(ControlFlow::Err)?
		else {
			return Ok(None);
		};
		let key = UnitKey {
			ns: ns_name,
			db: db_name,
			ids: (ns, db),
			object: UnitObject::TableRead(table.clone()),
			version: ObjectVersion::Stamped(version),
			build: BuildId::current(start.ctx().get_capabilities()),
		};
		let loaded = units
			.table_reads()
			.lookup(&key, &frame, &txn, start.ctx())
			.await
			.map(|unit| unit.program);
		#[cfg(test)]
		if loaded.is_some() {
			super::units::CATALOG_UNITS.with(|units| {
				let (c, l) = units.get();
				units.set((c, l + 1));
			});
		}
		Ok(loaded)
	})
	.await
}
