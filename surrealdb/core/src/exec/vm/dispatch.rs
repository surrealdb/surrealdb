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
//! ([`Deref::fetch`]).
//!
//! The units are served by the compile cache ([`super::compile_cache`]),
//! keyed by the table, the catalog version of its definitions and the build,
//! with one compilation per frame: the class of the principal and the
//! permission contract ([`table_unit`]). A program holds none of them. When
//! the query arrives, the plan asks the cache for the units of the tables its
//! sites name by analysis ([`build`]): the tables of the record ids and tables
//! written in the text, and the tables the kinds of the fields a `FETCH`
//! walks name for the records of a table it fetches into ([`fetch_reach`]),
//! with the tables their units read in turn. What they number follows from
//! the statement and the definitions it reaches, never from how many tables
//! the database holds: planning never lists the database's tables for a
//! read. A run loads the unit of each table it reads by the table's name, the
//! first time it meets the table in a frame ([`Deref::serving`]), and finds
//! it kept; a table no plan asked for — a site that may name any table, a
//! record of a table its analysis did not name, which data written before a
//! field's kind changed may hold — is compiled in a fault where the cache
//! holds no unit of it ([`super::fault::link_read`]). Every later read of the
//! table, by that program or by any other program of the run, finds it
//! loaded. A read in a frame the unit of its table was not compiled for, or of
//! a table no unit of whose read is compiled, is made as the operator tree's
//! helpers make it.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::Arc;

use surrealdb_strand::TableName;

use super::NotCovered;
use super::host::{host_frames, out_of_line, run_cow_row_from, run_unit_inline, unit_state};
use super::lower::Lowered;
use super::program::{Program, UnitHead, UnitSig};
use crate::exec::operators::fetch::{fetch_raw_record, require_table};
use crate::exec::permission::validate_record_user_access;
use crate::exec::planner::Planner;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::logical::{LogicalExpr, LogicalFields, LogicalOp, LogicalProject, LogicalSource};
use crate::expr::{ControlFlow, Kind};
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
	/// Any table of the database.
	Any,
}

assert_plain!(Reach);

impl Reach {
	/// The reach of no read.
	pub(super) fn none() -> Self {
		Reach::Tables(BTreeSet::new())
	}

	/// The reach of `self` and `other` together.
	pub(super) fn union(self, other: Reach) -> Reach {
		match (self, other) {
			(Reach::Tables(mut a), Reach::Tables(b)) => {
				a.extend(b);
				Reach::Tables(a)
			}
			_ => Reach::Any,
		}
	}

	/// The reach of a value written in the text of a statement: the tables of
	/// the record ids and table names it holds, or any table where its value
	/// is not known until the statement runs.
	pub(super) fn of_expr(expr: &LogicalExpr) -> Reach {
		match expr {
			LogicalExpr::Literal(_) | LogicalExpr::Constant(_) => Reach::none(),
			LogicalExpr::Table(table) => Reach::Tables(BTreeSet::from([table.clone()])),
			LogicalExpr::RecordId(rid) => Reach::Tables(BTreeSet::from([rid.table.clone()])),
			LogicalExpr::Mock(mock) => Reach::Tables(BTreeSet::from([mock.table().clone()])),
			LogicalExpr::Array(items) | LogicalExpr::Set(items) => {
				items.iter().fold(Reach::none(), |reach, item| reach.union(Reach::of_expr(item)))
			}
			LogicalExpr::Object(entries) => entries
				.iter()
				.fold(Reach::none(), |reach, (_, value)| reach.union(Reach::of_expr(value))),
			_ => Reach::Any,
		}
	}

	fn is_empty(&self) -> bool {
		matches!(self, Reach::Tables(tables) if tables.is_empty())
	}

	/// Whether a table both `self` and `other` reach can exist.
	pub(super) fn meets(&self, other: &Reach) -> bool {
		match (self, other) {
			(Reach::Any, reach) | (reach, Reach::Any) => !reach.is_empty(),
			(Reach::Tables(a), Reach::Tables(b)) => !a.is_disjoint(b),
		}
	}

	/// Whether `table` is reached.
	pub(super) fn reaches(&self, table: &TableName) -> bool {
		match self {
			Reach::Any => true,
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
	/// ([`Self::serving`]), which is then made as the operator tree makes it.
	///
	/// With `checked` the caller's table and field permissions apply and the
	/// caller is confined to its own tenant; without it the record is read
	/// as a permission predicate reads it, its computed fields derived and
	/// no permission applied.
	pub(crate) async fn fetch(
		&self,
		ctx: &ExecutionContext,
		rid: &RecordId,
		checked: bool,
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
		Some(self.fetch_served(ctx, rid, checked, served, unchecked).await)
	}

	/// Read `row`, a record of `table` as storage holds it, in place, by the
	/// table's read unit, permission-checked where `checked`, as
	/// [`Self::fetch`] reads a record it fetched: whether the record is shown.
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

	/// [`Self::fetch`], for a read the unit `served` serves.
	async fn fetch_served(
		&self,
		ctx: &ExecutionContext,
		rid: &RecordId,
		checked: bool,
		served: Served<'_>,
		unchecked: bool,
	) -> FlowResult<Value> {
		let Some(raw) = fetch_raw_record(ctx, rid, ctx.version_stamp()).await? else {
			if checked {
				require_table(ctx, &rid.table).await?;
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
					steps.store(true, std::sync::atomic::Ordering::Relaxed);
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
		host_frames(program, &mut state, ctx);
		out_of_line(|| {
			run_cow_row_from(program, pipeline, Cow::Owned(raw), &mut state, ctx, start)
		})
		.await?;
		Ok(state.output.pop().unwrap_or(Value::None))
	}
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

/// The tables of the database `planner` is lowering against that `named`
/// names, in table order: the definition of each of them alone is read, so
/// what it costs does not grow with the tables of the database. A table the
/// catalog does not hold is not a candidate: reading it fails before its unit
/// is looked for.
pub(super) async fn candidates(
	planner: &Planner<'_>,
	named: &BTreeSet<TableName>,
) -> Lowered<Vec<TableName>> {
	use crate::catalog::providers::TableProvider;
	let Some(txn) = planner.txn() else {
		return Err(NotCovered::internal("no transaction at plan time"));
	};
	let Some((ns, db)) = planner.ns_db_ids().await else {
		return Err(NotCovered::unresolved_database(planner));
	};
	let unread = |_| NotCovered::internal("the table catalog could not be read");
	let mut names = Vec::with_capacity(named.len());
	for name in named {
		if txn.get_tb(ns, db, name, None).await.map_err(unread)?.is_some() {
			names.push(name.clone());
		}
	}
	names.sort();
	Ok(names)
}

/// Where a `FETCH` walk stands: on a record of a table, or on a value of a
/// kind that is not a record id.
enum At {
	Record(TableName),
	Value(Kind),
}

/// The tables a `FETCH` of `parts`, over records of `tables` as the catalog
/// defines them, reads records of: the tables the kinds of the fields it
/// walks through name for the record ids it meets, as
/// [`crate::exec::operators::fetch::fetch_fields_with`] walks them — a record
/// id met mid-path or at its end is read, and so is each record id of an
/// array at its end. Any table where a field it walks through is not
/// defined, or is of a kind that may hold a record id of any table.
///
/// The reach is what the definitions promise of the data. A record of a
/// table outside it, which data written before a field's kind changed may
/// hold, is read through the unit the run links for its table
/// ([`Deref::serving`]).
pub(super) async fn fetch_reach(
	planner: &Planner<'_>,
	tables: &BTreeSet<TableName>,
	parts: &[crate::exec::operators::fetch::FetchPart<'_>],
) -> Reach {
	let Some(txn) = planner.txn() else {
		return Reach::Any;
	};
	let Some((ns, db)) = planner.ns_db_ids().await else {
		return Reach::Any;
	};
	let mut reach = BTreeSet::new();
	let mut at: Vec<At> = tables.iter().cloned().map(At::Record).collect();
	for part in parts {
		let mut next = Vec::new();
		for pos in at {
			let walked = crate::exec::vm::host::out_of_line(|| {
				walk(txn, (ns, db), pos, part, &mut next, &mut reach)
			})
			.await;
			if walked.is_none() {
				return Reach::Any;
			}
		}
		at = next;
	}
	// At its end the walk reads a record id, or each record id of an array.
	for pos in at {
		let At::Value(kind) = pos else {
			continue;
		};
		let mut ends = vec![kind.clone()];
		ends.extend(elements(&kind));
		for end in ends {
			if records(&end, &mut reach).is_none() {
				return Reach::Any;
			}
		}
	}
	Reach::Tables(reach)
}

/// Take step `part` from `pos` into `next`, adding to `reach` the tables of
/// the record ids the walk reads on the way: a record id it stands on is
/// read before the step is taken, and an array a field step meets applies
/// the step to each element. `None` where the step may reach a record id of
/// any table.
async fn walk(
	txn: &crate::kvs::Transaction,
	(ns, db): (crate::catalog::NamespaceId, crate::catalog::DatabaseId),
	pos: At,
	part: &crate::exec::operators::fetch::FetchPart<'_>,
	next: &mut Vec<At>,
	reach: &mut BTreeSet<TableName>,
) -> Option<()> {
	use crate::catalog::providers::TableProvider;
	use crate::exec::operators::fetch::FetchPart;
	use crate::expr::KindLiteral;
	match pos {
		At::Record(table) => match part {
			FetchPart::Field(name) => {
				let fields = txn.all_tb_fields(ns, db, &table, None).await.ok()?;
				let field = fields.iter().find(|field| {
					matches!(field.name.0.as_slice(), [crate::expr::Part::Field(f)] if f.as_str() == name.as_str())
				})?;
				let kind = field.field_kind.clone()?;
				land(kind, next, reach)
			}
			// The values of every field, or a field a computed key names.
			FetchPart::All | FetchPart::Index(_) => None,
			FetchPart::First | FetchPart::Last | FetchPart::Unsupported => Some(()),
		},
		At::Value(kind) => match kind {
			Kind::Any | Kind::Object => match part {
				FetchPart::Unsupported => Some(()),
				_ => None,
			},
			Kind::Either(kinds) => {
				for kind in kinds {
					crate::exec::vm::host::out_of_line(|| {
						walk(txn, (ns, db), At::Value(kind), part, next, reach)
					})
					.await?;
				}
				Some(())
			}
			Kind::Array(..) | Kind::Set(..) | Kind::Literal(KindLiteral::Array(_)) => {
				let inner = elements(&kind);
				match part {
					// A field step is taken from each element, each record
					// id among them read first.
					FetchPart::Field(_) => {
						for element in inner {
							let mut landed = Vec::new();
							land(element, &mut landed, reach)?;
							for pos in landed {
								crate::exec::vm::host::out_of_line(|| {
									walk(txn, (ns, db), pos, part, next, reach)
								})
								.await?;
							}
						}
						Some(())
					}
					FetchPart::All | FetchPart::First | FetchPart::Last | FetchPart::Index(_) => {
						for element in inner {
							land(element, next, reach)?;
						}
						Some(())
					}
					FetchPart::Unsupported => Some(()),
				}
			}
			Kind::Literal(KindLiteral::Object(fields)) => match part {
				FetchPart::Field(name) => match fields.get(*name) {
					Some(kind) => land(kind.clone(), next, reach),
					None => Some(()),
				},
				FetchPart::All | FetchPart::Index(_) => {
					for kind in fields.into_values() {
						land(kind, next, reach)?;
					}
					Some(())
				}
				FetchPart::First | FetchPart::Last | FetchPart::Unsupported => Some(()),
			},
			// A value that holds no record id and no value that does.
			_ => Some(()),
		},
	}
}

/// Stand on a value of `kind`: on a record of each table its record ids
/// may name, which the walk reads, adding them to `reach`, and on the
/// value where it may be something else. `None` where it may be a record
/// id of any table.
fn land(kind: Kind, next: &mut Vec<At>, reach: &mut BTreeSet<TableName>) -> Option<()> {
	let mut tables = BTreeSet::new();
	records(&kind, &mut tables)?;
	for table in tables {
		reach.insert(table.clone());
		next.push(At::Record(table));
	}
	next.push(At::Value(kind));
	Some(())
}

/// Add to `out` the tables a value of `kind` may be a record id of; `None`
/// where it may be one of any table.
fn records(kind: &Kind, out: &mut BTreeSet<TableName>) -> Option<()> {
	match kind {
		Kind::Any => None,
		Kind::Record(tables) if tables.is_empty() => None,
		Kind::Record(tables) => {
			out.extend(tables.iter().cloned());
			Some(())
		}
		Kind::Either(kinds) => {
			for kind in kinds {
				records(kind, out)?;
			}
			Some(())
		}
		_ => Some(()),
	}
}

/// The kinds of the elements of an array or set of `kind`.
fn elements(kind: &Kind) -> Vec<Kind> {
	use crate::expr::KindLiteral;
	match kind {
		Kind::Array(inner, _) | Kind::Set(inner, _) => vec![(**inner).clone()],
		Kind::Literal(KindLiteral::Array(kinds)) => kinds.clone(),
		Kind::Either(kinds) => kinds.iter().flat_map(elements).collect(),
		Kind::Any => vec![Kind::Any],
		_ => Vec::new(),
	}
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

/// A table's read unit as the compile cache keeps it: the program, and what
/// running it may do ([`super::footprint`]).
#[derive(Debug, Clone)]
pub(crate) struct TableReadUnit {
	pub(crate) program: Arc<Program>,
	#[cfg_attr(
		not(test),
		expect(
			dead_code,
			reason = "kept with the unit for the placement and reload decisions that read effect summaries; read by tests"
		)
	)]
	pub(crate) footprint: Arc<super::footprint::Footprint>,
}

/// [`table_unit`], with the unit's footprint.
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
		let (program, footprint) = lowered?;
		let unit = TableReadUnit {
			program: Arc::new(program),
			footprint: Arc::new(footprint),
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
	let frame = super::lower::statement_frame(planner);
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
async fn lower_table_unit(
	planner: &Planner<'_>,
	table: &TableName,
) -> Lowered<(Program, super::footprint::Footprint)> {
	let statement = read_all(table);
	// A read whose binding fails fails with the error the binding raises.
	let bound = crate::exec::planner::bind_statement(planner, statement).await.map_err(|e| {
		match super::plan::Refusal::of(&e) {
			Some(refusal) => NotCovered::refused(refusal),
			None => NotCovered::new(format!("the table {table} did not bind: {e}")),
		}
	})?;
	let mapped = super::map::map(planner, &bound).await?;
	let physical = super::optimize::optimize(mapped);
	let mut program =
		crate::exec::vm::host::out_of_line(|| super::lower::lower_unit(planner, &physical, None))
			.await?;
	// The read is the one pipeline no block statement owns
	// ([`Program::own_pipeline`]). The others are the bodies of statements
	// nested in the table's permissions, which the read runs as groups of the
	// program.
	if program.write.is_some() || program.own_pipeline().is_none() {
		return Err(NotCovered::new(format!("the table {table} is not read by one pipeline")));
	}
	program.unit = Some(UnitHead {
		sig: UnitSig {
			params: Vec::new(),
			takes_row: true,
		},
		frame: super::lower::statement_frame(planner),
		entry: None,
		closure: None,
	});
	let footprint =
		super::footprint::Footprint::of(&bound, planner.function_registry(), reach_of(&program));
	Ok((program, footprint))
}

/// Ask the compile cache for the read units of the tables `reach` names, and
/// of `known`, and of the tables those units read records of in turn, so a
/// run finds each kept when it loads it by name; the dispatch naming them, or
/// `None` where `reach` reaches no table.
///
/// Each table has a unit for a caller whose reads are checked and, where
/// `planner` lowers for such a caller, one for a frame that waives the checks
/// (a permission predicate, an event). What a reach that may name any table
/// names beyond them, the run loads the same way, and a unit the cache does
/// not hold is compiled in a fault ([`super::fault::link_read`]).
pub(super) async fn build(
	planner: &Planner<'_>,
	reach: Reach,
	known: BTreeSet<TableName>,
) -> Lowered<Option<Arc<TableDispatch>>> {
	if reach.is_empty() {
		return Ok(None);
	}
	let waived =
		planner.body_is_checked(planner.auth.as_ref()).then(|| planner.waiving_permissions());
	let mut tables: Vec<TableName> = Vec::new();
	let mut named = tables_named(reach);
	named.extend(known);
	loop {
		let mut grew = false;
		for table in candidates(planner, &named).await? {
			if tables.contains(&table) {
				continue;
			}
			// A table whose unit reads records of other tables names them.
			let program = table_unit(planner, &table).await?;
			named.extend(tables_named(program.reach.clone().unwrap_or_else(Reach::none)));
			if let Some(waived) = &waived {
				let program = table_unit(waived, &table).await?;
				named.extend(tables_named(program.reach.clone().unwrap_or_else(Reach::none)));
			}
			tables.push(table);
			grew = true;
		}
		if !grew {
			tables.sort();
			return Ok(Some(Arc::new(TableDispatch {
				tables,
			})));
		}
	}
}

/// The tables `reach` names: none beyond those a reach of any table names.
fn tables_named(reach: Reach) -> BTreeSet<TableName> {
	match reach {
		Reach::Tables(tables) => tables,
		Reach::Any => BTreeSet::new(),
	}
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

/// Give `program`, a statement lowered against `planner`, the dispatch over
/// the tables it and its units read records of by id.
pub(super) async fn seal(planner: &Planner<'_>, program: &mut Program) -> Lowered<()> {
	seal_knowing(planner, program, BTreeSet::new()).await
}

/// [`seal`] for a statement whose records by id its caller knows to be of
/// `known`, as a cascade knows the table of the record a key named: the
/// units of `known` are asked for with the program wherever its sites may
/// name any table, so a read of those records compiles nothing, and a record
/// of another table is loaded by the run as for any program.
pub(super) async fn seal_knowing(
	planner: &Planner<'_>,
	program: &mut Program,
	known: BTreeSet<TableName>,
) -> Lowered<()> {
	let reach = match reach_of(program) {
		reach if reach.is_empty() => reach,
		reach => reach.union(Reach::Tables(known.clone())),
	};
	if let Some(dispatch) = build(planner, reach, known).await? {
		program.dispatch = Some(dispatch);
	}
	Ok(())
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
