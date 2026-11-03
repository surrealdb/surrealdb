//! Lowering of the pipelines of a container: the source, the body run for each row, and the sink.
//!
//! A pipeline is the [`Source`], the instructions and the `limits`, `enforce` and `parallel` the
//! interpreter and the placement boundary expect of the chain it stands for. The instructions are
//! the fused forms the interpreter answers fastest for the same body: a filter that ends the row
//! and a hand-off that ends it.
//!
//! # The cut
//!
//! A `phys.pipeline` op holds one `Pipeline` region that ends in a `phys.yield` of the last op's
//! result. Two shapes of region are lowered, and both give the same program.
//!
//! - The chain: ops in data-flow order, the head first and every later op streaming. The head is a
//!   source op of [`read`], or a `phys.sort`, whose rows are the sorted rows of another pipeline.
//! - The select loop: a `phys.loop` of kind `Select` is the head. Its operand is the rows it reads,
//!   defined by a source op or a `phys.sort`, and its one body region holds the ops that run for
//!   each row, in data-flow order, ended by a `phys.yield` of the row the body delivers. Streaming
//!   ops that follow the loop in the region run after the body.
//!
//! The streaming ops that follow a pipeline in its region's container, reading its rows and read by
//! nothing else, are steps of its body: the cut leaves the paging and the distinct of a select
//! above the loop, and they run in the loop's body after the ops of the pipeline's own region. A
//! sort reads a pipeline through them.
//!
//! The cut and the placement are the physical plan's: this lowers the ops it is given and copies
//! the placement decision of the pipeline op ([`parallel_of`]), and never regroups ops or chooses
//! a width.
//!
//! # Feeders
//!
//! A `phys.sort` consumes every row of the pipeline that feeds it before the pipeline that reads
//! the sort starts. The feeder is lowered first, ending in the sort's push of the row's keys
//! instead of the statement's output, and the reader's source is [`Source::Sorted`]. A
//! `phys.knn_topk` is a sort that keeps the `k` rows nearest a vector: its feeder pushes each row's
//! distance (`knn`). A step of a `MATCH`'s binding table is a feeder in the same way: the feeder
//! ends in the hand-off of each row to the step's split, and the reader's source is
//! [`Source::Split`] (`step`). A spent limit of the reader is also a limit of every pipeline that
//! feeds it, because a limit spent before the first row spares the feeders their records. A
//! `phys.join` is a breaker over two inputs (`join`): its left input is a feeder that hands each
//! row to the join, and its right input is a group of the program read into the build table, or,
//! for a join that holds its left row, a second feeder streamed past that row. A pipeline whose
//! rows another kind of op reads answers `NotYet` naming that op.
//!
//! # Continued pipelines
//!
//! A select loop that reads the rows of another pipeline, through the streaming ops that follow
//! it, continues that pipeline: the two are one body, over the earlier pipeline's source, and the
//! earlier pipeline is lowered by the later one. The body takes the placement of the first
//! pipeline of the chain, whose facts the physical plan derived over every pipeline that continues
//! it. The streaming ops that stand between a sort, a step or a source and the select loop that
//! reads it run in that loop's body before it.
//!
//! # A write over rows
//!
//! The loop of a write that changes the rows a read yields is a head like a select loop: it
//! reads the rows of a source, a sort or another pipeline through the streaming ops before it, and
//! it continues the pipeline it reads. Its body holds the stage ops of the write, which this does
//! not lower as steps: the pipeline ends in [`Sink::Write`], which emits the body of the write
//! ([`write_body`]) over the row as the record in flight, judged by the statement gate. A
//! `phys.write` that reads the pipeline's rows makes it the statement's output. The pipeline runs
//! serially.
//!
//! # The body
//!
//! The ops of the body lower in order, each over the row [`ROW`] until a projection replaces it:
//!
//! - `phys.enforced` puts the rows through the table's enforcement ([`Enforce::Rows`]); it emits
//!   nothing, and a pipeline that enforces never runs on stored bytes.
//! - `phys.permission_filter` ends the row where a table's permission does not admit it, judged in
//!   the permission frame of its guard (`permission`).
//! - `phys.layer` lands the columns of the field permissions and `COMPUTED` fields on the row
//!   (`layer`).
//! - `phys.filter` ends the row where its predicate is falsy. The predicate is threaded through
//!   `AND`, `OR` and `NOT` into jumps to the row's end, and a conditional jump to the row's end is
//!   a filter instruction ([`Instr::Filter`], [`Instr::FilterCompare`], [`Instr::FilterFieldInt`]).
//!   The conjuncts that read nothing of the row, a second region, are judged once for the run of
//!   the statement. The block argument of a predicate region is the row, and a top-level field of
//!   it, over rows that are records, is an operand of the row itself. A comparison of such a field
//!   with an integer literal is the typed form when the source's declared kind for the field admits
//!   an integer.
//! - `phys.limit` pages the rows: `START` skips, `LIMIT` takes, and a spent limit stops the
//!   pipeline where the source is one that is not opened under a spent limit and where it is not. A
//!   paging clause is the [`HostScalar`] of the value its slot denotes: a constant where the value
//!   is one, and a host pipeline of the program that evaluates it when the program starts
//!   otherwise.
//! - `phys.distinct` ends the row where an earlier row of the run equals it.
//! - `phys.project` replaces the row with the value its shape names: the row itself, the value of
//!   its one expression, an object of its fields (after `*`, the row), or the value of a sort key
//!   read from the rows a sort handed over. A plain path omission is removed from the object in
//!   place; any other walk of an omission answers `NotYet`.
//! - `phys.fetch` fetches the records its paths reach into the row held at that point, in place
//!   (`fetch`).
//!
//! The body ends with the hand-off of the row and the checks of the limits that are spent, then
//! the row's end and the pipeline's stop. The hand-off is the statement's output, the sort's
//! push of the keys of the row, or the hand-off of the row to a join. The row's end after a
//! hand-off to the output is the hand-off itself ([`Instr::EmitEnd`]) where nothing jumps to the
//! end.
//!
//! # What a pipeline holds of the plan
//!
//! The decision is copied from the placement table, and it is narrowed to serial where the
//! body this lowers disagrees with the facts the decision was made from: a paged decision over a
//! body whose paging a merge cannot replay, a row read into its record where the facts say none
//! is, or a different number of distinct sets.

mod aggregate;
mod fetch;
mod join;
mod knn;
mod layer;
mod nested;
mod permission;
mod probe;
mod projection;
mod step;
mod window;

use std::ops::Range;
use std::sync::Arc;

pub(super) use fetch::path_steps;
pub(super) use knn::gate_code;
pub(super) use layer::{key_parts, lower_layers, lower_stored_guards};
pub(super) use projection::project_image;
use surrealdb_strand::TableName;

use super::super::payload::Spill;
use super::super::program::{
	AggregateSink, DeferredVector, Enforce, FrontierRowsSpec, GroupSpec, HostScalar, Instr,
	JoinSpec, KeyOrder, KnnQuery, KnnRank, KnnVector, Label, LimitSpec, Operand, Pipeline, Program,
	ROW, Reg, ReturnCatch, RowsAheadSpec, SortSpec, Source, SplitKind, SplitSpec, TailSpec,
};
use super::closure::wrapped_value_loop;
use super::dispatch::{apply_needs, check_claims, lowers_where_read, phys_refusal};
use super::emit::Emitter;
use super::expr::{Claim, Lowerer, Needs, ends_context};
use super::place::{Lowered, Provenance, parallel_of};
use super::read::{host_scalar_of, is_source, lower_source_under, producer_of, schema_of};
use super::stage::{StageRun, Stages};
use super::units::LowerCx;
use super::write_stmt::{SiteSink, WriteEntry, cut_of_rows, write_body, write_body_cut};
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{RegionKind, RegionProperties};
use crate::exec::ir::iface::statements::BindStepShape;
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::facts::facts_of;
use crate::exec::ir::phys::ops::{
	KnnTargetShape, KnnTopKAttrs, MatchStepAttrs, PhysOp, PhysTag, PipelineId, ProjectionShape,
	SortAttrs, SortKeyReadShape,
};
use crate::exec::ir::phys::pipeline::streams;
use crate::exec::ir::phys::placement_attr::PipelineIo;
use crate::exec::operators::SplitTarget;
use crate::exec::planner::placement::Parallel;
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::PrefixOperator;
use crate::expr::logical::Schema;
use crate::val::Value;

/// Where the pools of the pipelines of a region start in the program that holds them: zero for a
/// program of one statement, the lengths the statements before it left for a block statement
/// lowered as a group of its own. Every index the region's code names is the base plus its
/// position in [`Reads`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PoolBase {
	pub(super) pipelines: usize,
	pub(super) limits: u16,
	pub(super) distincts: u16,
	pub(super) sorts: u16,
	pub(super) aggregates: u16,
	pub(super) splits: u16,
	pub(super) joins: u16,
	pub(super) probes: u16,
	/// The index the first group a join's build side or a statement in `FROM` opens takes: the
	/// groups of the program before it, the statement's own among them.
	pub(super) groups: u16,
}

/// What lowering the pipelines of a region leaves for the program that holds them.
#[derive(Debug)]
pub(super) struct Reads {
	/// Where the pools below start in the program.
	pub(super) base: PoolBase,
	/// The pipelines, in the order they run.
	pub(super) pipelines: Vec<Pipeline>,
	/// The paging clauses the pipelines' `Skip`, `Take` and limit checks name.
	pub(super) limits: Vec<LimitSpec>,
	/// For each distinct set, the group whose start empties it: none, for the program's own.
	pub(super) distincts: Vec<Option<u16>>,
	/// The sorts the pipelines feed and read.
	pub(super) sorts: Vec<SortSpec>,
	/// The aggregates the pipelines feed and read.
	pub(super) aggregates: Vec<AggregateSink>,
	/// The splits the pipelines feed and read.
	pub(super) splits: Vec<SplitSpec>,
	/// The joins the pipelines feed and read.
	pub(super) joins: Vec<JoinSpec>,
	/// For each probe the pipelines read through, the group whose start empties its kept rows:
	/// none, for the program's own.
	pub(super) probes: Vec<Option<u16>>,
	/// The groups the build sides of joins and the statements in `FROM` are lowered as, in the
	/// order their indices were taken.
	pub(super) groups: Vec<GroupSpec>,
	/// The pipelines no pipeline op of the region stands for: the parts a split cuts from a select
	/// loop's body, the readers of a breaker no pipeline op reads, and the pipelines of the groups
	/// the statements in `FROM` are lowered as.
	pub(super) unowned: usize,
	/// The table the first source reads, for the program's `table`.
	pub(super) table: Option<TableName>,
	/// Where each pipeline came from.
	pub(super) provenance: Provenance,
	/// What the lowered bodies need of the run.
	pub(super) needs: Needs,
	/// For each pipeline, the instructions each op of its body emitted for itself, which the
	/// program that holds the pipeline checks against what the op claims.
	pub(super) claims: Vec<Vec<Claim>>,
	/// For each pipeline, the index of the pipeline that feeds the sort it reads.
	feeders: Vec<Option<usize>>,
	/// Whether a row the pipeline lowered last hands its sink may be a record id.
	fed_ids: bool,
}

/// What the pipelines a group lowered leave for the program that holds them: its pipelines and
/// the pools they number from, appended to the program's at the group's [`PoolBase`].
#[derive(Debug)]
pub(super) struct Pools {
	/// The group's pipelines, in the order they run.
	pub(super) pipelines: Vec<Pipeline>,
	/// For each pipeline, the instructions each op of its body emitted for itself.
	pub(super) claims: Vec<Vec<Claim>>,
	/// The paging clauses the pipelines name.
	pub(super) limits: Vec<LimitSpec>,
	/// For each distinct set, the group whose start empties it.
	pub(super) distincts: Vec<Option<u16>>,
	/// The sorts the pipelines feed and read.
	pub(super) sorts: Vec<SortSpec>,
	/// The aggregates the pipelines feed and read.
	pub(super) aggregates: Vec<AggregateSink>,
	/// The splits the pipelines feed and read.
	pub(super) splits: Vec<SplitSpec>,
	/// The joins the pipelines feed and read.
	pub(super) joins: Vec<JoinSpec>,
	/// For each probe the pipelines read through, the group whose start empties its kept rows.
	pub(super) probes: Vec<Option<u16>>,
	/// The groups the statements nested in the group's sources are lowered as, numbered from the
	/// group's own index plus one.
	pub(super) groups: Vec<GroupSpec>,
	/// Where each pipeline came from, at its index in the program.
	pub(super) provenance: Provenance,
}

impl Reads {
	/// The pipelines and pools of no region lowered yet, numbered from `base`.
	pub(super) fn empty(base: PoolBase) -> Self {
		Self {
			base,
			pipelines: Vec::new(),
			limits: Vec::new(),
			distincts: Vec::new(),
			sorts: Vec::new(),
			aggregates: Vec::new(),
			splits: Vec::new(),
			joins: Vec::new(),
			probes: Vec::new(),
			groups: Vec::new(),
			unowned: 0,
			table: None,
			provenance: Provenance::default(),
			needs: Needs::none(),
			claims: Vec::new(),
			feeders: Vec::new(),
			fed_ids: false,
		}
	}

	/// Appends the pipeline `pipeline`, which no op of the container stands for and which reads
	/// no sort, after the pipelines lowered so far.
	pub(super) fn append(&mut self, pipeline: Pipeline) {
		self.claims.push(Vec::new());
		self.feeders.push(None);
		self.pipelines.push(pipeline);
	}

	/// What the program that holds these pipelines appends to its own pools.
	///
	/// Every field is named, so a field a later change gives [`Reads`] must be taken or dropped
	/// here on purpose.
	pub(super) fn into_pools(self) -> Pools {
		let Reads {
			base: _,
			pipelines,
			limits,
			distincts,
			sorts,
			aggregates,
			splits,
			joins,
			probes,
			groups,
			unowned: _,
			table: _,
			provenance,
			needs: _,
			claims,
			feeders: _,
			fed_ids: _,
		} = self;
		Pools {
			pipelines,
			claims,
			limits,
			distincts,
			sorts,
			aggregates,
			splits,
			joins,
			probes,
			groups,
			provenance,
		}
	}

	/// The index the next group these reads open takes, and where the pools of its pipelines
	/// start: after everything these reads hold, with the groups nested in it numbered after it.
	pub(super) fn next_group(&self) -> Result<(u16, PoolBase), IrError> {
		let group = pool_index(self.base.groups, self.groups.len(), "vm groups")?;
		let base = PoolBase {
			pipelines: self.base.pipelines + self.pipelines.len(),
			limits: pool_index(self.base.limits, self.limits.len(), "vm limits")?,
			distincts: pool_index(self.base.distincts, self.distincts.len(), "vm distinct sets")?,
			sorts: pool_index(self.base.sorts, self.sorts.len(), "vm sorts")?,
			aggregates: pool_index(self.base.aggregates, self.aggregates.len(), "vm aggregates")?,
			splits: pool_index(self.base.splits, self.splits.len(), "vm splits")?,
			joins: pool_index(self.base.joins, self.joins.len(), "vm joins")?,
			probes: pool_index(self.base.probes, self.probes.len(), "vm probes")?,
			groups: group.checked_add(1).ok_or(IrError::Capacity("vm groups"))?,
		};
		Ok((group, base))
	}

	/// Appends the group lowered at [`Reads::next_group`], whose reads are `group` and whose spec
	/// is `spec`: its pipelines, which no pipeline op of the region these reads lower stands for,
	/// and its pools. The groups nested in it follow its own.
	pub(super) fn append_group(&mut self, group: Reads, spec: GroupSpec) {
		let Pools {
			pipelines,
			claims,
			limits,
			distincts,
			sorts,
			aggregates,
			splits,
			joins,
			probes,
			groups,
			provenance,
		} = group.into_pools();
		self.unowned += pipelines.len();
		self.feeders.extend(pipelines.iter().map(|_| None));
		self.pipelines.extend(pipelines);
		self.claims.extend(claims);
		self.limits.extend(limits);
		self.distincts.extend(distincts);
		self.sorts.extend(sorts);
		self.aggregates.extend(aggregates);
		self.splits.extend(splits);
		self.joins.extend(joins);
		self.probes.extend(probes);
		self.provenance.pipelines.extend(provenance.pipelines);
		self.groups.push(spec);
		self.groups.extend(groups);
	}

	/// The [`GroupSpec`] of the block statement these are the pipelines of: its sorts and
	/// aggregates, the permission frames its group filled (`permissions`, from
	/// [`Emitter::close_group`]) and its tail. `scalar` and `rowless` are the statement's shape,
	/// and a write sets its spec on the answer.
	pub(super) fn group_spec(
		&self,
		permissions: Range<u16>,
		tail: Option<TailSpec>,
		scalar: bool,
		rowless: bool,
	) -> Result<GroupSpec, IrError> {
		let sorts = self.base.sorts..pool_index(self.base.sorts, self.sorts.len(), "vm sorts")?;
		let aggregates = self.base.aggregates
			..pool_index(self.base.aggregates, self.aggregates.len(), "vm aggregates")?;
		let (version, timeout, only) = match tail {
			Some(tail) => (tail.version, tail.timeout, tail.only),
			None => (None, None, false),
		};
		Ok(GroupSpec {
			write: None,
			scalar,
			rowless,
			sorts,
			aggregates,
			permissions,
			version,
			timeout,
			only,
		})
	}
}

/// Lowers the container `ir`, whose top region holds the pipelines of one read statement, to a
/// program.
///
/// The top region is lowered as [`lower_scope`] lowers the region of a statement, and the program
/// reads one table at most through its first source, runs at the database level where it reads a
/// table, and keeps row order where a body calls something that does. The wrappers of the
/// statement's rows are the program's tail ([`TailSpec`]).
///
/// `yields` is [`lower_scope`]'s: true for the top region of a statement copied out of an
/// expression as a unit, which ends in the yield of the statement's rows.
pub(super) fn lower_reads(cx: &mut LowerCx<'_>, ir: &Ir, yields: bool) -> Result<Lowered, IrError> {
	let mut em = Emitter::new();
	let mut facts = FactCache::new();
	let ReadScope {
		reads,
		tail,
	} = lower_scope(&mut em, cx, ir, &mut facts, ir.top(), PoolBase::default(), yields)?;
	let (program, provenance) =
		program_of(em, ir, cx.registry(), reads, tail, AccessMode::ReadOnly)?;
	Ok(Lowered {
		program,
		provenance,
		needs: Vec::new(),
	})
}

/// The program of the pipelines `reads` holds: the pools they number, the groups of their joins,
/// the tail of the statement and what its bodies need of the run, with each pipeline's claims
/// checked against its code. The access mode is `access_mode`, or a write where a body needs one.
pub(super) fn program_of(
	em: Emitter,
	ir: &Ir,
	registry: &FunctionRegistry,
	reads: Reads,
	tail: Option<TailSpec>,
	access_mode: AccessMode,
) -> Result<(Program, Provenance), IrError> {
	let Reads {
		base: _,
		pipelines,
		limits,
		distincts,
		sorts,
		aggregates,
		splits,
		joins,
		probes,
		groups,
		unowned: _,
		table,
		provenance,
		needs,
		claims,
		feeders: _,
		fed_ids: _,
	} = reads;
	let mut program = em.finish_program(pipelines, access_mode)?;
	apply_needs(&mut program, &needs);
	if table.is_some() {
		program.required_context = program.required_context.max(ContextLevel::Database);
	}
	program.table = table;
	program.limits = limits;
	program.distincts = distincts;
	program.sorts = sorts;
	program.aggregates = aggregates;
	program.splits = splits;
	program.joins = joins;
	program.probes = probes;
	program.groups = groups;
	program.tail = tail;
	for (pipeline, claimed) in program.pipelines.iter().zip(&claims) {
		check_claims(ir, claimed, &pipeline.code, &program, registry)?;
	}
	super::super::morsel::wire::attach_units(&mut program);
	Ok((program, provenance))
}

/// What lowering the region of a read statement leaves: its pipelines and the clauses that wrap
/// its rows.
#[derive(Debug)]
pub(crate) struct ReadScope {
	/// The pipelines, with the pools they number from.
	pub(super) reads: Reads,
	/// The wrappers of the statement's rows, `None` for a statement with none.
	pub(super) tail: Option<TailSpec>,
}

/// The ops that wrap the rows of a read statement.
#[derive(Clone, Copy, Default)]
struct Wrappers {
	only: bool,
	version: Option<ValueId>,
	timeout: Option<ValueId>,
}

/// Lowers the read statement `region` holds, each pipeline into `em`, numbering the pools from
/// `base`.
///
/// The region holds the pipelines and the ops they read: the sources of a union, the sources and
/// sorts a select loop reads, the values a source or a paging clause evaluates when the statement
/// starts, and the `phys.only`, `phys.version_scope` and `phys.timeout` that wrap the statement's
/// rows. A `phys.timeout` holds the timed body in its `Deadline` region, which is where the
/// pipelines are then. A source whose rows no pipeline reads is the statement's output, read by a
/// pipeline of its own. An expression nothing of those reads answers `NotYet`, and so does any
/// other op, naming its mnemonic. A `phys.yield` of the region delivers the statement's rows,
/// which are the output. `yields` says that the region may also end in the `expr.yield` of the
/// statement's value and hold the `EXPLAIN ANALYZE` of its statement, as the region of a block's
/// statement does and the top region of a table's read unit does not.
///
/// The wrappers are the statement's tail ([`TailSpec`]): `ONLY` unwraps the statement's one row,
/// and the `VERSION` and the `TIMEOUT` are host scalars the run evaluates when the statement
/// starts, the version first.
pub(super) fn lower_scope(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	region: RegionId,
	base: PoolBase,
	yields: bool,
) -> Result<ReadScope, IrError> {
	let mut wrappers = Wrappers::default();
	let holder = scan_region(ir, cx.registry(), region, yields, &mut wrappers)?.unwrap_or(region);
	let mut reads = lower_pipelines_at(em, cx, ir, facts, holder, base)?;
	if reads.pipelines.is_empty() {
		return Err(invariant(NO_ROWS));
	}
	let tail = tail_of(em, cx, ir, wrappers, &mut reads)?;
	Ok(ReadScope {
		reads,
		tail,
	})
}

/// Lowers the read statement `region` holds into `reads`, after what it already holds, as
/// [`lower_scope`] lowers it into pools of its own, and answers its tail. The pipelines belong to
/// the group `em` has open.
pub(super) fn lower_statement_into(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	region: RegionId,
	reads: &mut Reads,
) -> Result<Option<TailSpec>, IrError> {
	let mut wrappers = Wrappers::default();
	let holder = scan_region(ir, cx.registry(), region, true, &mut wrappers)?.unwrap_or(region);
	let held = reads.pipelines.len();
	lower_region_into(em, cx, ir, facts, holder, reads)?;
	if reads.pipelines.len() == held {
		return Err(invariant(NO_ROWS));
	}
	tail_of(em, cx, ir, wrappers, reads)
}

/// The invariant break of a read statement's region from which no pipeline is lowered.
///
/// [`lower_region_into`] lowers a pipeline for every pipeline op, breaker and source whose rows
/// the region's output or a write reads, and a region whose ops [`scan_region`] admits holds one
/// of these: the binder makes every statement that runs as a group from a source, a statement in
/// `FROM`, a union, a breaker or a loop over one (`bind_relational`), the physical phase wraps
/// each select and write loop in a pipeline, a statement whose binding diverges holds the
/// expression that raises, which `scan_region` refuses, and no pass replaces a statement's rows
/// with a literal. The other entries hand it a region that holds a read: the top region and a
/// statement in `FROM` once `is_read_op` holds for one of its ops, and a statement in value
/// position run in place only under a `Hash` or `Lookup` strategy, which the physical phase gives
/// only a statement whose rows a select loop reads (`ir/phys/nested.rs`).
const NO_ROWS: &str = "a read statement's region with no op that makes its rows";

/// The tail the wrappers of a statement's rows make: `None` for a statement with none, and the
/// `VERSION` and `TIMEOUT` as host scalars the run evaluates when the statement starts, the version
/// first. What the host scalars need is merged into `reads`.
fn tail_of(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	wrappers: Wrappers,
	reads: &mut Reads,
) -> Result<Option<TailSpec>, IrError> {
	let Wrappers {
		only,
		version,
		timeout,
	} = wrappers;
	match (only, version, timeout) {
		(false, None, None) => Ok(None),
		(only, version, timeout) => {
			let mut needs = Needs::none();
			let version =
				version.map(|value| host_scalar_of(em, cx, ir, value, &mut needs)).transpose()?;
			let timeout =
				timeout.map(|value| host_scalar_of(em, cx, ir, value, &mut needs)).transpose()?;
			reads.needs = reads.needs.merged(&needs);
			Ok(Some(TailSpec {
				version,
				timeout,
				only,
			}))
		}
	}
}

/// Checks the ops of `region`, a read statement's, and collects its wrappers into `wrappers`.
/// Answers the region that holds the pipelines when it is not `region` itself: the `Deadline`
/// region of a `phys.timeout`.
fn scan_region(
	ir: &Ir,
	registry: &FunctionRegistry,
	region: RegionId,
	yields: bool,
	wrappers: &mut Wrappers,
) -> Result<Option<RegionId>, IrError> {
	let mut trailing = Vec::new();
	for op in ops_in(ir, region)
		.filter(|op| is_pipeline(ir, *op) || is_breaker(ir, *op) || is_row_source(ir, *op))
	{
		trailing.extend(trail_of(ir, region, op)?);
	}
	let mut holder = None;
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		match &data.kind {
			OpKind::Phys(PhysOp::Pipeline(_)) if is_pipeline(ir, op) => {}
			OpKind::Expr(ExprOp::Literal(_)) => {}
			// A streaming op that follows a pipeline, a breaker or a source runs in the pipeline
			// that reads those rows, after its body.
			OpKind::Phys(_) if trailing.contains(&op) => {}
			// The rows a `phys.yield` of the region delivers are the statement's output
			// ([`consumer_of_value`]), in the top region of a table's read unit as in a block's
			// statement region.
			OpKind::Phys(PhysOp::Yield) => {}
			// The value of a statement held in an expression, which ends the statement's region.
			OpKind::Expr(ExprOp::Yield) if yields => {}
			// The `EXPLAIN ANALYZE` a block's statement holds is the statement's analysis: the
			// driver renders it, and the region's own pipelines are the statement it measures.
			OpKind::Phys(PhysOp::Explain {
				analyze: true,
				..
			}) if yields => {}
			// A plain `EXPLAIN` reads no rows: every entry renders it as its own group or program
			// (`explain::lower_plain`, `explained_group`, a block's `lower_explain_group`), and the
			// physical phase gives one in value position no strategy that runs it in place.
			OpKind::Phys(PhysOp::Explain {
				analyze: false,
				..
			}) => {
				return Err(IrError::Invariant {
					what: "a plain EXPLAIN is rendered by its group, never scanned",
				});
			}
			OpKind::Phys(PhysOp::Only) => {
				if data.operands.len() != 1 {
					return Err(invariant("a phys.only with other than one operand"));
				}
				wrappers.only = true;
			}
			OpKind::Phys(PhysOp::VersionScope) => {
				let [_, scoped] = <[ValueId; 2]>::try_from(data.operands.as_slice())
					.map_err(|_| invariant("a phys.version_scope with other than two operands"))?;
				// A statement states one `VERSION`, bound as the one wrapper over its rows, and a
				// statement in `FROM` holds its own in its own region.
				if wrappers.version.replace(scoped).is_some() {
					return Err(invariant("more than one version scope over a statement's rows"));
				}
			}
			OpKind::Phys(PhysOp::Timeout) => {
				let [duration] = <[ValueId; 1]>::try_from(data.operands.as_slice())
					.map_err(|_| invariant("a phys.timeout with other than one operand"))?;
				let [deadline] = <[RegionId; 1]>::try_from(data.regions.as_slice())
					.map_err(|_| invariant("a phys.timeout with other than one region"))?;
				// A statement states one `TIMEOUT`, bound as the one wrapper over its rows, and a
				// statement in `FROM` holds its own in its own region.
				if wrappers.timeout.replace(duration).is_some() {
					return Err(invariant("more than one timeout over a statement's rows"));
				}
				let inner =
					scan_region(ir, registry, deadline, true, wrappers)?.unwrap_or(deadline);
				// Every timed body is a timeout's, and a second timeout is refused above.
				if holder.replace(inner).is_some() {
					return Err(invariant("more than one timed body"));
				}
			}
			// A breaker nothing reads is the statement's output, which a pipeline of its own reads.
			OpKind::Phys(_) if is_breaker(ir, op) => {}
			// A source is opened by the pipeline that reads its rows, or by a pipeline of its own
			// where they are the statement's output.
			OpKind::Phys(_) if is_row_source(ir, op) => {}
			// A value loop, wrapped in a pipeline by the physical phase or not, and a `{ ... }`
			// block in value position are values, as an expression is. One read only where the
			// statement starts is a host scalar of the program; one a clause of a pipeline reads
			// too is lowered by each reader, which needs a value that does not change between
			// evaluations.
			OpKind::Expr(_)
			| OpKind::Phys(PhysOp::Pipeline(_) | PhysOp::Block)
			| OpKind::Phys(PhysOp::Loop {
				kind:
					LoopKind::ElementFilter
					| LoopKind::Broadcast
					| LoopKind::Closure
					| LoopKind::Recurse
					| LoopKind::GraphStep,
				..
			}) => {
				if !feeds_host_scalar(ir, data) && !lowers_where_read(ir, registry, op, data) {
					return Err(IrError::NotYet {
						what: "ir_lower::pipe: an expression outside every pipeline",
					});
				}
			}
			OpKind::Phys(kind) => {
				return Err(IrError::NotYet {
					what: phys_refusal(kind.tag()),
				});
			}
			// The verification that ends the physical phase rejects a logical op outside stored
			// code, and a statement's region is never stored code.
			OpKind::Logical(_) => {
				return Err(invariant("a logical operation in a physical container"));
			}
		}
	}
	// The binder puts every clause of a statement in its `Deadline` region and only the wrappers
	// of its rows beside it, so a value the `TIMEOUT` or `VERSION` evaluates is the only other
	// pipeline, and that is a value pipeline.
	if holder.is_some() && ops_in(ir, region).any(|op| is_pipeline(ir, op)) {
		return Err(invariant("a pipeline of rows beside a timed body"));
	}
	Ok(holder)
}

/// Whether the expression op `data`, defined in the top region, is read only to give a source or a
/// clause of the statement a value it evaluates when the program starts.
///
/// Such a value is lowered where its reader asks for it, as a host pipeline of the program, so it
/// is never a step of the top region. One nothing reads would never run. A read from inside the
/// regions of an expression or a value pipeline beside `data` is that value's: a value loop the
/// physical phase wraps reads its collection from inside its pipeline, and the host code that
/// evaluates the pipeline evaluates the loop. Any other reader is judged itself, wherever it
/// stands: a paging clause the physical phase moves into a pipeline still reads its operand.
pub(super) fn feeds_host_scalar(ir: &Ir, data: &OpData) -> bool {
	let mut read = false;
	for result in &data.results {
		for used in ir.uses(*result) {
			read = true;
			// A read from inside the regions of a value beside `data` is the value's.
			if let Some(holder) = op_beside(ir, data, used.user)
				&& holder != used.user
				&& let Some(held) = ir.op(holder)
				&& (matches!(held.kind, OpKind::Expr(_)) || is_value_pipeline(ir, holder))
			{
				if !feeds_host_scalar(ir, held) {
					return false;
				}
				continue;
			}
			let fed = match ir.op(used.user).map(|user| &user.kind) {
				Some(OpKind::Phys(
					PhysOp::Limit {
						..
					}
					| PhysOp::RecordScan(..)
					| PhysOp::Values {
						..
					}
					| PhysOp::KeyRangeScan(_)
					| PhysOp::KnnScan(_)
					| PhysOp::IndexLookup(_)
					| PhysOp::Count(_)
					| PhysOp::VersionScope
					| PhysOp::Timeout,
				)) => true,
				// The query vector of a brute-force search, which is never its rows.
				Some(OpKind::Phys(PhysOp::KnnTopK(_))) => used.operand != 0,
				// The key of a probe, which the probe reads as a path into `$parent` when it opens.
				Some(OpKind::Phys(PhysOp::Probe {
					..
				})) => used.operand == 1,
				// The query vector of a `KNN` operator, which the search that answers the operator
				// reads, and a row's judgement of the operator never does.
				Some(OpKind::Expr(ExprOp::Binary {
					op: crate::expr::BinaryOperator::NearestNeighbor(_),
				})) if used.operand == 1 => true,
				Some(OpKind::Expr(_)) => {
					ir.op(used.user).is_some_and(|user| feeds_host_scalar(ir, user))
				}
				Some(OpKind::Phys(_) | OpKind::Logical(_)) | None => false,
			};
			if !fed {
				return false;
			}
		}
	}
	read
}

/// The op that stands in the block of `data` and holds `op`: `op` itself where it stands there,
/// or the op whose regions it is nested in. `None` where `op` is outside every op of that block.
fn op_beside(ir: &Ir, data: &OpData, op: OpId) -> Option<OpId> {
	let mut at = op;
	loop {
		if ir.op(at)?.parent == data.parent {
			return Some(at);
		}
		at = ir.parent_op(ir.enclosing_region(at))?;
	}
}

/// Whether `op` is one the read a write takes its rows from is made of: a pipeline, a source, a
/// breaker, or a streaming op that stands between them.
pub(super) fn is_read_of_rows(ir: &Ir, op: OpId) -> bool {
	match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Phys(kind)) => {
			is_pipeline(ir, op)
				|| is_breaker(ir, op)
				|| streams(kind)
				|| is_source(kind.tag())
				|| matches!(
					kind,
					PhysOp::Loop {
						kind: LoopKind::Select,
						..
					}
				)
		}
		Some(OpKind::Expr(_) | OpKind::Logical(_)) | None => false,
	}
}

/// Whether `op` gives rows of its own: a source op ([`is_source`]) or a statement in `FROM`.
fn is_row_source(ir: &Ir, op: OpId) -> bool {
	match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Phys(kind)) => is_source(kind.tag()) || kind.tag() == PhysTag::Subquery,
		Some(OpKind::Expr(_) | OpKind::Logical(_)) | None => false,
	}
}

/// Whether the rows of `op` are an input of a `phys.union`, which opens them with its other inputs.
fn in_union(ir: &Ir, op: OpId) -> bool {
	ir.op(op).is_some_and(|data| {
		data.results.iter().any(|result| {
			ir.uses(*result).iter().any(|read| {
				matches!(ir.op(read.user).map(|user| &user.kind), Some(OpKind::Phys(PhysOp::Union)))
			})
		})
	})
}

/// Whether `op` is a `phys.pipeline` of rows: any pipeline but the wrapper of a value loop
/// ([`is_value_pipeline`]), which is a value the expression that reads it lowers.
pub(super) fn is_pipeline(ir: &Ir, op: OpId) -> bool {
	matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Pipeline(_))))
		&& !is_value_pipeline(ir, op)
}

/// Whether `op` is the `phys.pipeline` the physical phase wraps a value loop in
/// ([`wrapped_value_loop`]): a value, lowered in place by the expression that reads it, never a
/// pipeline of the program.
pub(super) fn is_value_pipeline(ir: &Ir, op: OpId) -> bool {
	ir.op(op).is_some_and(|data| {
		matches!(data.kind, OpKind::Phys(PhysOp::Pipeline(_)))
			&& matches!(wrapped_value_loop(ir, data), Ok(Some(_)))
	})
}

/// Whether `op` is a `phys.pipeline` that holds a `FOR` loop: the wrapper the physical phase puts
/// around the loop, which a statement list runs in place ([`super::block`]).
pub(super) fn is_for_pipeline(ir: &Ir, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	if !matches!(&data.kind, OpKind::Phys(PhysOp::Pipeline(_))) {
		return false;
	}
	data.regions.iter().any(|region| {
		ops_in(ir, *region).any(|inner| {
			matches!(
				ir.op(inner).map(|held| &held.kind),
				Some(OpKind::Phys(PhysOp::Loop {
					kind: LoopKind::For,
					..
				}))
			)
		})
	})
}

/// Whether `op` makes the region that holds it the region of a read statement: a `phys.pipeline`
/// that wraps neither a `FOR` nor a loop in value position, a `phys.count`, or a `phys.timeout`
/// whose timed body holds one. A timed write is not a read.
pub(super) fn is_read_op(ir: &Ir, op: OpId) -> bool {
	match ir.op(op) {
		Some(data) => match &data.kind {
			OpKind::Phys(PhysOp::Pipeline(_)) => {
				!is_for_pipeline(ir, op) && !is_value_pipeline(ir, op)
			}
			OpKind::Phys(PhysOp::Timeout) => data
				.regions
				.iter()
				.any(|region| ops_in(ir, *region).any(|inner| is_read_op(ir, inner))),
			// A count stands for the select loop it replaced, and its rows are a statement's.
			OpKind::Phys(PhysOp::Count(_)) => true,
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => false,
		},
		None => false,
	}
}

/// The ops of `region` that run in the body of one of its pipelines without standing in the
/// pipeline's own region: the streaming ops that follow a pipeline, and those that stand between
/// a select loop and the op that gives it its rows.
pub(super) fn bodies_outside_regions(ir: &Ir, region: RegionId) -> Result<Vec<OpId>, IrError> {
	let mut outside = Vec::new();
	for pipeline in ops_in(ir, region).filter(|op| is_pipeline(ir, *op)) {
		outside.extend(trail_of(ir, region, pipeline)?);
		let data = ir.op(pipeline).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let [held] = <[RegionId; 1]>::try_from(data.regions.as_slice())
			.map_err(|_| invariant("a pipeline with other than one region"))?;
		let mut chain: Vec<OpId> = ops_in(ir, held).collect();
		chain.pop();
		outside.extend(head_of(ir, &chain)?.before);
	}
	Ok(outside)
}

/// Whether the op `op` stands in the region of a `phys.pipeline` that itself stands in `region`:
/// the pipeline a select loop at its head starts.
fn starts_pipeline_in(ir: &Ir, region: RegionId, op: OpId) -> bool {
	let held = ir.enclosing_region(op);
	ir.region(held).is_some_and(|data| data.kind == RegionKind::Pipeline)
		&& ir
			.parent_op(held)
			.is_some_and(|owner| is_pipeline(ir, owner) && ir.enclosing_region(owner) == region)
}

/// What reads the rows of a pipeline.
enum Consumer {
	/// The region's own `phys.yield`, or nothing: the statement's output.
	Output,
	/// A `phys.sort`, a `phys.knn_topk`, a `phys.aggregate`, a `phys.split`, a side of a
	/// `phys.join` or the build input of a `phys.probe`, which the pipeline feeds.
	Breaker,
	/// A step of a binding table, which the pipeline feeds.
	Step,
	/// A select loop or a write loop of another pipeline, which continues this one.
	Fused,
	/// The `phys.write` that reads the rows of the pipeline of its write loop: the pipeline ends in
	/// the statement's output through the body of the write.
	Write(OpId),
	/// A `phys.union` that reads the rows as one of its inputs: the union's reader lowers them as a
	/// group of the program ([`member_group`]).
	Member,
}

/// Lowers the pipelines `region` holds, each into `em`, as the pipelines of a program of one
/// statement: [`lower_pipelines_at`] with every pool starting at zero.
pub(super) fn lower_pipelines(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	region: RegionId,
) -> Result<Reads, IrError> {
	lower_pipelines_at(em, cx, ir, facts, region, PoolBase::default())
}

/// Lowers the pipelines `region` holds, each into `em`, numbering their limits, distinct sets and
/// sorts from `base`.
///
/// Exactly one pipeline ends in the statement's output, because a statement has one output; it is
/// lowered with the pipelines that feed it, which come first. A pipeline that feeds a sort is
/// lowered by the pipeline that reads the sort, and one that nothing reads is `NotYet`. The
/// pipelines, paging clauses and distinct sets belong to the group `em` has open
/// ([`Emitter::open_group`]); the caller appends the pools of the answer to the program's at
/// `base`.
pub(super) fn lower_pipelines_at(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	region: RegionId,
	base: PoolBase,
) -> Result<Reads, IrError> {
	let mut reads = Reads::empty(base);
	lower_region_into(em, cx, ir, facts, region, &mut reads)?;
	Ok(reads)
}

/// Lowers the pipelines `region` holds into `reads`, after the pipelines it already holds: the
/// pipeline that ends in the region's output, with the pipelines that feed it, and the reader of a
/// breaker or a source whose rows are that output. The pipelines belong to the group `em` has
/// open.
fn lower_region_into(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	region: RegionId,
	reads: &mut Reads,
) -> Result<(), IrError> {
	let (held, unowned) = (reads.pipelines.len(), reads.unowned);
	let mut output = false;
	let mut owned = 0usize;
	for op in ops_in(ir, region).filter(|op| is_pipeline(ir, *op)) {
		let trail = trail_of(ir, region, op)?;
		let sink = match consumer_of(ir, region, trail.last().copied().unwrap_or(op))? {
			Consumer::Output => Sink::Output,
			Consumer::Write(write) => Sink::Write {
				write,
			},
			Consumer::Breaker | Consumer::Step => {
				owned += 1;
				continue;
			}
			// A pipeline another continues is lowered with it, and one a union reads is lowered by
			// the union's reader as a group, which counts it.
			Consumer::Fused | Consumer::Member => continue,
		};
		claim_output(&mut output)?;
		owned += 1;
		lower_pipeline(em, cx, ir, facts, op, &trail, &sink, reads)?;
	}
	// A breaker whose rows are the region's output, with no pipeline op reading them, is read by a
	// pipeline of its own.
	for op in ops_in(ir, region).filter(|op| is_breaker(ir, *op)) {
		let trail = trail_of(ir, region, op)?;
		if let Consumer::Output = consumer_of(ir, region, trail.last().copied().unwrap_or(op))? {
			claim_output(&mut output)?;
			lower_reader(em, cx, ir, facts, op, &trail, &Sink::Output, reads)?;
		}
	}
	// A source or a statement in `FROM` whose rows the region's output or a write reads, with no
	// pipeline op reading them, is read by a pipeline of its own that holds only that source.
	// A member of a union is opened with the union, and a source a breaker, a step
	// or a select loop reads is opened by that reader.
	for op in ops_in(ir, region).filter(|op| is_row_source(ir, *op) && !in_union(ir, *op)) {
		let trail = trail_of(ir, region, op)?;
		let sink = match consumer_of(ir, region, trail.last().copied().unwrap_or(op))? {
			Consumer::Output => Sink::Output,
			Consumer::Write(write) => Sink::Write {
				write,
			},
			Consumer::Breaker | Consumer::Step | Consumer::Fused | Consumer::Member => continue,
		};
		claim_output(&mut output)?;
		lower_reader(em, cx, ir, facts, op, &trail, &sink, reads)?;
	}
	// Each pipeline op that does not continue another is one pipeline: the one that ends in the
	// output, and each that feeds a breaker or a step, lowered by the pipeline that reads it. The
	// pipelines no op stands for count themselves in `unowned`: the readers of a breaker or a
	// source, the parts a split in a select loop's body cuts, and the pipelines of the groups a
	// source nested in the region opens.
	let lowered = reads.pipelines.len() - held;
	if lowered != owned + (reads.unowned - unowned) {
		return Err(IrError::NotYet {
			what: "ir_lower::pipe: a pipeline feeding a breaker or a step no pipeline reads",
		});
	}
	Ok(())
}

/// Marks the region's output as delivered by the pipeline being lowered: a statement has one
/// output, so a second answers `NotYet`.
fn claim_output(output: &mut bool) -> Result<(), IrError> {
	if std::mem::replace(output, true) {
		return Err(IrError::NotYet {
			what: "ir_lower::pipe: more than one pipeline ending in the output",
		});
	}
	Ok(())
}

/// The streaming ops that follow the pipeline `pipeline` in `region`, in data-flow order.
///
/// A streaming op whose input is the pipeline's rows, and that nothing else reads, is a step of the
/// pipeline: the cut puts the loop of a select in the pipeline and leaves the paging and the
/// distinct that follow it above, so they run in the body after it. The rows the last of them
/// delivers are the pipeline's rows. The ops after a breaker are a pipeline of their own.
pub(super) fn trail_of(ir: &Ir, region: RegionId, pipeline: OpId) -> Result<Vec<OpId>, IrError> {
	let mut trail = Vec::new();
	let mut at = pipeline;
	loop {
		let data = ir.op(at).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let [result] = data.results.as_slice() else {
			return Ok(trail);
		};
		let [read] = ir.uses(*result) else {
			return Ok(trail);
		};
		let Some(OpKind::Phys(kind)) = ir.op(read.user).map(|user| &user.kind) else {
			return Ok(trail);
		};
		if !streams(kind) || read.operand != 0 || ir.enclosing_region(read.user) != region {
			return Ok(trail);
		}
		trail.push(read.user);
		at = read.user;
	}
}

/// The pipeline `rows` are the rows of, with the streaming ops that follow it before they are read:
/// a pipeline op, or an op whose rows a pipeline with no op of its own reads ([`lower_reader`]): a
/// breaker, a MATCH step, a source or a statement in `FROM`.
///
/// The inverse of [`trail_of`]: a breaker reads the rows of a pipeline through the ops that follow
/// it. Every op that makes rows is one of these, a streaming op over one, or a wrapper of the
/// statement's rows, which only another wrapper, an `EXPLAIN` or the region's yield reads
/// ([`consumer_of_value`]), so rows that reach none of them are an invariant break.
fn pipeline_behind(ir: &Ir, rows: ValueId) -> Result<(OpId, Vec<OpId>), IrError> {
	let mut trail = Vec::new();
	let mut at = producer_of(ir, rows)?;
	loop {
		if is_pipeline(ir, at) || is_breaker(ir, at) || is_row_source(ir, at) || is_step(ir, at) {
			trail.reverse();
			return Ok((at, trail));
		}
		let data = ir.op(at).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		match (&data.kind, data.operands.first()) {
			(OpKind::Phys(kind), Some(input)) if streams(kind) => {
				trail.push(at);
				at = producer_of(ir, *input)?;
			}
			_ => return Err(invariant("rows a breaker or a step reads that no op makes as rows")),
		}
	}
}

/// Whether `op` is a `phys.match_step` that breaks its pipeline, whose rows [`open_source`] opens:
/// every step but the binding of each row under a name, which streams in the body of the pipeline
/// that holds it ([`streams`]).
fn is_step(ir: &Ir, op: OpId) -> bool {
	matches!(
		ir.op(op).map(|data| &data.kind),
		Some(OpKind::Phys(kind @ PhysOp::MatchStep(_))) if !streams(kind)
	)
}

/// What reads the rows of the pipeline `pipeline`.
///
/// A pipeline whose rows nothing reads, or only the `phys.yield` that ends `region` or the
/// wrappers of the statement's rows (`phys.only`, `phys.version_scope`), ends in the statement's
/// output. One that a `phys.sort` or a `phys.aggregate` reads feeds it. The consumer of any other
/// kind answers `NotYet` naming it: it is a breaker whose feeder this does not lower.
fn consumer_of(ir: &Ir, region: RegionId, pipeline: OpId) -> Result<Consumer, IrError> {
	let data = ir.op(pipeline).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let mut consumer = Consumer::Output;
	for result in &data.results {
		let found = consumer_of_value(ir, region, *result)?;
		if !matches!(found, Consumer::Output) {
			consumer = found;
		}
	}
	Ok(consumer)
}

/// What reads the rows `value`, looking through the wrappers of the statement's rows.
fn consumer_of_value(ir: &Ir, region: RegionId, value: ValueId) -> Result<Consumer, IrError> {
	let mut consumer = Consumer::Output;
	for read in ir.uses(value) {
		let user = ir.op(read.user).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		match &user.kind {
			OpKind::Phys(PhysOp::Yield) | OpKind::Expr(ExprOp::Yield)
				if ir.enclosing_region(read.user) == region => {}
			OpKind::Phys(PhysOp::Explain {
				analyze: true,
				..
			}) if ir.enclosing_region(read.user) == region => {}
			OpKind::Phys(
				PhysOp::Sort(_)
				| PhysOp::Window(_)
				| PhysOp::KnnTopK(_)
				| PhysOp::Aggregate(_)
				| PhysOp::Split {
					..
				},
			) if read.operand == 0 => consumer = Consumer::Breaker,
			OpKind::Phys(PhysOp::Join(_)) if read.operand <= 1 => consumer = Consumer::Breaker,
			OpKind::Phys(PhysOp::Probe {
				..
			}) if read.operand == 0 => consumer = Consumer::Breaker,
			OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Select | LoopKind::Write,
				..
			}) if read.operand == 0 && starts_pipeline_in(ir, region, read.user) => {
				consumer = Consumer::Fused;
			}
			OpKind::Phys(PhysOp::Write(_))
				if read.operand == 0 && ir.enclosing_region(read.user) == region =>
			{
				consumer = Consumer::Write(read.user);
			}
			// An input of a `FROM` naming several sources that is not a source alone (a table's
			// stack over its scan) is read as a statement of its own.
			OpKind::Phys(PhysOp::Union) if ir.enclosing_region(read.user) == region => {
				consumer = Consumer::Member;
			}
			OpKind::Phys(PhysOp::MatchStep(attrs)) => match attrs.as_ref() {
				MatchStepAttrs::Bind(BindStepShape::Bind {
					..
				}) => {
					return Err(invariant(
						"a binding that follows a pipeline but is not in its body",
					));
				}
				MatchStepAttrs::Bind(_) => consumer = Consumer::Step,
				MatchStepAttrs::Read {
					..
				}
				| MatchStepAttrs::Update(_)
				| MatchStepAttrs::Delete {
					..
				}
				| MatchStepAttrs::Insert(_)
				| MatchStepAttrs::Output(_) => {
					return Err(invariant("a MATCH stage op in a plan the fold built"));
				}
			},
			OpKind::Phys(PhysOp::Only | PhysOp::VersionScope)
				if ir.enclosing_region(read.user) == region && read.operand == 0 =>
			{
				// The AST lowering wraps a `SELECT` in `VERSION` and `ONLY` after every other
				// clause, so only another wrapper, an `EXPLAIN` or the region's yield reads a
				// wrapper's rows: a statement in `FROM` is a `Subquery` region of its own, and no
				// pass moves a reader of rows beneath a wrapper.
				for wrapped in &user.results {
					if !matches!(consumer_of_value(ir, region, *wrapped)?, Consumer::Output) {
						return Err(invariant("rows a breaker or a step reads through a wrapper"));
					}
				}
			}
			// The binder binds a tree: rows have one reader, their parent, which reads them as its
			// rows in the region that holds them, and no pass hands them to a second reader or
			// moves that reader into another region. A streaming reader is in the pipeline's
			// trail, so one that reaches here is a second reader. A non-analyze
			// `phys.explain` reads the root of its statement, which [`scan_region`] refuses before
			// the region is lowered; a MATCH region holds none, and a write's top region roots its
			// statement at the write.
			OpKind::Phys(_) => {
				return Err(invariant("rows read by an op that is not their reader"));
			}
			// No expression op takes rows as an operand but a yield, which the arms above admit in
			// the region. The only pipeline whose result an expression reads is a value loop's,
			// which is a value ([`is_pipeline`]), and a statement in value position is a region of
			// its own expression, so no expression reads the rows of a pipeline, a breaker or a
			// source.
			OpKind::Expr(_) | OpKind::Logical(_) => {
				return Err(invariant("rows read by an expression"));
			}
		}
	}
	Ok(consumer)
}

/// How the rows of a source reach the body, for the paging that guards the source.
fn opens_under_spent_limit(source: &Source) -> bool {
	matches!(
		source,
		Source::Scan { .. }
			| Source::LightweightEdges {
				record: None,
				..
			} | Source::Index { .. }
			| Source::IndexUnion { .. }
			| Source::Bitmap { .. }
			| Source::Count { .. }
			| Source::Sorted { .. }
			| Source::Groups { .. }
			| Source::Probe { .. }
			| Source::Join { .. }
	)
}

/// Whether the rows a source yields are records: every source this lowers but a value in `FROM`
/// and a count.
fn yields_records(source: &Source) -> bool {
	match source {
		Source::Union(sources) => sources.iter().all(yields_records),
		Source::Values {
			..
		}
		| Source::Nested {
			..
		}
		| Source::Join {
			..
		}
		| Source::Count {
			..
		} => false,
		Source::Scan {
			..
		}
		| Source::Record {
			..
		}
		| Source::Index {
			..
		}
		| Source::Lookup {
			..
		}
		| Source::IndexUnion {
			..
		}
		| Source::Bitmap {
			..
		}
		| Source::FullText {
			..
		}
		| Source::Knn {
			..
		}
		| Source::Probe {
			..
		}
		| Source::Sorted {
			..
		}
		| Source::Groups {
			..
		}
		| Source::Split {
			..
		}
		| Source::Once
		| Source::Changes {
			..
		}
		| Source::WriteTargets {
			..
		}
		| Source::WriteInserts {
			..
		}
		| Source::WriteEdges {
			..
		}
		| Source::LightweightEdges {
			..
		}
		| Source::Vacant {
			..
		} => true,
	}
}

/// Whether `source` names one record whose row is read and judged whatever the paging.
fn names_one_record(source: &Source) -> bool {
	matches!(
		source,
		Source::Record {
			record: HostScalar::Const(record),
			..
		} if matches!(**record, Value::RecordId(_))
	)
}

/// Where the rows a pipeline's body finishes with go.
enum Sink {
	/// The statement's output.
	Output,
	/// The sort `id`, which the op `op` is: the keys of the row are pushed to it.
	Sort {
		id: u16,
		op: OpId,
	},
	/// The sort `id` of a brute-force search: the row is ranked by the distance `rank` measures of
	/// the vector at `field`, and its rank is pushed.
	Knn {
		id: u16,
		field: FieldPath,
		rank: Arc<KnnRank>,
	},
	/// The aggregate `id`, which the op `op` is and whose fields `plan` reads: the row is folded
	/// into its group.
	Aggregate {
		id: u16,
		op: OpId,
		plan: Box<aggregate::Plan>,
	},
	/// The split `id`: the row is split into the rows of the pipeline the split is the source of.
	Split {
		id: u16,
	},
	/// The join `id`, whose left input the pipeline is: the row is handed to the join keyed by the
	/// values `keys`, the join's left key regions, take over it.
	JoinProbe {
		id: u16,
		keys: Vec<RegionId>,
	},
	/// The join `id`, which holds its left input's one row and whose right input the pipeline is:
	/// the row is paired with the held row.
	JoinPair {
		id: u16,
	},
	/// The body of the write `write`, a `phys.write`: the row is the record each order of the write
	/// runs its stages over, and the pipeline's output is what the write hands the statement.
	Write {
		write: OpId,
	},
}

/// What the body emitted that the placement's facts are checked against.
#[derive(Default)]
struct Emitted {
	/// The number of distinct sets the rows pass through.
	distincts: usize,
	/// Whether a row is read into its record.
	reads_record_rows: bool,
	/// Whether a binding replaced the row.
	rebound: bool,
	/// Whether the paging is one a merge can replay.
	paging_replayable: bool,
}

/// The jumps the body aimed at the end of the row, which the row's end resolves once it is
/// emitted.
#[derive(Default)]
struct EndJumps {
	/// Conditional jumps that become filters when the row's end follows: their instruction
	/// indices.
	filters: Vec<Label>,
	/// Jumps that stay jumps: skips, distinct tests and spent-limit checks.
	other: usize,
}

/// The source a pipeline opens, with what the program that holds it records of it.
struct Opened {
	source: Source,
	/// The IR source op (`PhysOp::IndexScan`, `PhysOp::IndexUnion`, `PhysOp::Bitmap`, etc.) when
	/// the pipeline opens a leaf source directly.
	source_op: Option<OpId>,
	table: Option<TableName>,
	/// What the catalog declares for the rows the source reads, when it reads one table's rows.
	schema: Option<Arc<Schema>>,
	/// The pipeline that fills the sort the source reads, when it reads one.
	feeder: Option<usize>,
	/// Whether the rows are records, whose fields the body reads itself.
	objects: bool,
	/// Whether a row may be a record id beyond what the source states ([`yields_ids`]): the rows
	/// of a sort whose feeder's rows may be.
	ids: bool,
}

/// Where the rows of a pipeline come from and what runs for each row ahead of the region's other
/// steps.
struct Head {
	/// The source op, the sort or the step the pipeline reads.
	input: OpId,
	/// The streaming ops that stand between `input` and a select loop, in data-flow order, which
	/// run for each row before the loop's body. None for a chain.
	before: Vec<OpId>,
	/// The rows the head op delivers: the first step after the head reads them.
	after: ValueId,
	/// The ops of a select loop's body, in order; none for a chain.
	body: Vec<OpId>,
	/// The row a select loop's body starts from and the row it delivers.
	element: Option<(ValueId, ValueId)>,
}

/// The head of the chain `chain` of a pipeline region.
///
/// A select loop is the head when the chain starts at one: its input is the producer of its
/// operand, and the ops of its body run first. Any other chain starts at its input.
fn head_of(ir: &Ir, chain: &[OpId]) -> Result<Head, IrError> {
	let first = chain.first().ok_or_else(|| invariant("a pipeline with no op"))?;
	let data = ir.op(*first).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let after = first_result(data)?;
	let OpKind::Phys(PhysOp::Loop {
		kind,
		first: first_iteration,
		..
	}) = &data.kind
	else {
		return Ok(Head {
			input: *first,
			before: Vec::new(),
			after,
			body: Vec::new(),
			element: None,
		});
	};
	// A value loop's pipeline is a value ([`is_pipeline`]), and a `FOR` loop's stands in a
	// statement list, never in a read statement's region.
	if !matches!(kind, LoopKind::Select | LoopKind::Write) {
		return Err(invariant(
			"a pipeline of rows whose loop is not a select loop or a write loop",
		));
	}
	// The binder gives a select loop and a write loop one body and no step, so conversion states
	// no body for the first row.
	if *first_iteration {
		return Err(invariant("a select loop with a body for its first row"));
	}
	let [source] = <[ValueId; 1]>::try_from(data.operands.as_slice())
		.map_err(|_| invariant("a select loop with other than one operand"))?;
	let [region] = <[RegionId; 1]>::try_from(data.regions.as_slice())
		.map_err(|_| invariant("a select loop with other than one body"))?;
	let element = first_arg(ir, region).ok_or_else(|| invariant("a loop body with no row"))?;
	let mut ops: Vec<OpId> = ops_in(ir, region).collect();
	let terminator = ops.pop().ok_or_else(|| invariant("a loop body with no op"))?;
	// The ops of a write loop's body are its stage ops, which the write's body emits for the
	// record in flight ([`Sink::Write`]), so the body of the pipeline runs none of them.
	if *kind == LoopKind::Write {
		ops.clear();
	}
	let delivered = match ir.op(terminator).map(|data| (&data.kind, data.operands.as_slice())) {
		Some((OpKind::Phys(PhysOp::Yield), [delivered])) => *delivered,
		_ => return Err(invariant("a loop body that does not end in a yield of one row")),
	};
	let (input, before) = before_of(ir, source)?;
	Ok(Head {
		input,
		before,
		after,
		body: ops,
		element: Some((element, delivered)),
	})
}

/// The op that produces the rows `rows` of a select loop, and the streaming ops that stand between
/// it and the loop, in data-flow order.
///
/// A paging that stands between a sort and the select loop that reads its rows is one of them. The
/// walk stops at the first op that does not stream.
fn before_of(ir: &Ir, rows: ValueId) -> Result<(OpId, Vec<OpId>), IrError> {
	let mut before = Vec::new();
	let mut at = producer_of(ir, rows)?;
	loop {
		let data = ir.op(at).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		match (&data.kind, data.operands.first()) {
			(OpKind::Phys(kind), Some(input)) if streams(kind) => {
				before.push(at);
				at = producer_of(ir, *input)?;
			}
			_ => {
				before.reverse();
				return Ok((at, before));
			}
		}
	}
}

/// The frame of the container that the ops of the region `region` run under: the frame of its last
/// op, which every op of a region the binder frames carries.
pub(super) fn region_frame(ir: &Ir, region: RegionId) -> Result<FrameId, IrError> {
	let last = ops_in(ir, region).last().ok_or_else(|| invariant("a framed region with no op"))?;
	Ok(ir.op(last).ok_or(IrError::Stale("ir_lower::pipe: op"))?.frame)
}

/// The block argument of the region `region` that is the row it is evaluated against: the first,
/// or the one after the statement timestamp where the region is isolated from above.
fn row_arg(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let held = ir.region(region)?;
	let block = ir.block(*held.blocks.first()?)?;
	block.args.get(usize::from(held.kind.isolated_from_above())).copied()
}

/// The first block argument of the region `region`, when it has one.
pub(super) fn first_arg(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let block = ir.region(region)?.blocks.first()?;
	ir.block(*block)?.args.first().copied()
}

/// The first result of the op `data`.
fn first_result(data: &OpData) -> Result<ValueId, IrError> {
	data.results.first().copied().ok_or_else(|| invariant("a pipeline op with no result"))
}

/// Whether an input of the union `op` is rows a pipeline delivers rather than a source alone
/// ([`member_group`]).
fn holds_a_member(ir: &Ir, op: OpId) -> Result<bool, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	for input in &data.operands {
		if !is_row_source(ir, producer_of(ir, *input)?) {
			return Ok(true);
		}
	}
	Ok(false)
}

/// Lowers the input `input` of a union as a group of the program and answers the group, or answers
/// `None` where the input is a source alone, which the union opens itself.
///
/// An input of a `FROM` naming several sources that is not a source alone (a table's stack of
/// permissions, layers and filters over its own scan, or any other rows) is read as a statement of
/// its own, so those rows have one reading: the pipeline that delivers them,
/// ending in the group's output. The group holds that pipeline's
/// permission frames; it states no tail, since the outer statement's wrappers stand above the
/// union, and it reads no top-k threshold of a sort above the union. A table the group reads raises
/// the program to the database context and is not the table the union reports
/// ([`nested::lower_group`]).
pub(super) fn member_group(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	input: ValueId,
) -> Result<Option<u16>, IrError> {
	if is_row_source(ir, producer_of(ir, input)?) {
		return Ok(None);
	}
	let (member, trail) = pipeline_behind(ir, input)?;
	let placed = is_pipeline(ir, member);
	let group = nested::lower_group(em, cx, facts, reads, |em, cx, facts, reads| {
		lower_feeder(em, cx, ir, facts, member, &trail, &Sink::Output, reads)?;
		Ok(None)
	})?;
	// The pipeline op the member is counts once, as the region's output pipeline does; a pipeline
	// no op stands for counts itself where it is lowered.
	if placed {
		reads.unowned += 1;
	}
	Ok(Some(group))
}

/// Lowers a statement in `FROM` whose region `region` holds the plain `EXPLAIN` `explain` as a
/// group of the program, and answers the group's index.
///
/// The rendering of the plan is a constant of that plan, so the group is one pipeline over
/// [`Source::Once`] that loads it, and nothing of the explained statement runs: a scalar statement
/// that reads nothing, like the group of an evaluated statement. It states no tail: an
/// `EXPLAIN` wraps the statement it explains, its tail included. The rendering hides what the
/// frame `cx` lowers in hides, as every rendering of a plan does.
fn explained_group(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	facts: &mut FactCache,
	reads: &mut Reads,
	ir: &Ir,
	region: RegionId,
	explain: OpId,
) -> Result<u16, IrError> {
	let at = reads.groups.len();
	let group = nested::lower_group(em, cx, facts, reads, |em, cx, _facts, reads| {
		let base = PoolBase::default();
		let plain = super::explain_hook::Purpose::Plan;
		let hook = super::explain_hook::explained_served(cx)?;
		match hook.lower_explained(cx, em, ir, region, explain, base, plain)? {
			super::explain_hook::Explained::Plain(pipeline) => reads.append(pipeline),
			super::explain_hook::Explained::Analysed {
				..
			}
			| super::explain_hook::Explained::AnalysedValue {
				..
			} => return Err(invariant("an analysed EXPLAIN lowered as a plain one")),
		}
		Ok(None)
	})?;
	let spec = reads.groups.get_mut(at).ok_or_else(|| invariant("a group reserved and lost"))?;
	spec.scalar = true;
	spec.rowless = true;
	Ok(group)
}

/// Opens the source the op `input` stands for: a source op, a statement in `FROM` (alone or among
/// the inputs of a union) lowered as a group of the program (`nested`), or a sort or an aggregate
/// whose feeder is lowered first.
///
/// `topk_sort` is the sort the pipeline being opened feeds, which a scan's top-k key reads the
/// threshold of.
fn open_source(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	input: OpId,
	topk_sort: Option<u16>,
) -> Result<Opened, IrError> {
	let tag = phys_tag(ir, input)?;
	// A statement in `FROM`, alone or among the inputs of a union, is a group of the program.
	if tag == PhysTag::Subquery {
		return nested::open_nested(em, cx, ir, facts, reads, input);
	}
	if tag == PhysTag::Union
		&& (nested::holds_a_statement(ir, input)? || holds_a_member(ir, input)?)
	{
		return nested::open_union(em, cx, ir, facts, reads, input, topk_sort);
	}
	if is_source(tag) {
		let mut needs = Needs::none();
		let leaf = lower_source_under(em, cx, ir, input, topk_sort, &mut needs)?;
		reads.needs = reads.needs.merged(&needs);
		// A sort that cuts the scan by the second, or that a full-text scan or an index union of
		// full-text branches passes hits over by the score of, publishes its threshold as soon as
		// it holds what it keeps: each row the sort takes before then is a record the scan read
		// in full.
		let floors_early = match &leaf.source {
			Source::Scan {
				topk: Some(topk),
				..
			} => topk.coarse.is_some(),
			Source::FullText {
				topk,
				..
			} => topk.is_some(),
			Source::IndexUnion {
				topk,
				..
			} => topk.is_some(),
			Source::Scan {
				..
			}
			| Source::Record {
				..
			}
			| Source::Index {
				..
			}
			| Source::Lookup {
				..
			}
			| Source::Bitmap {
				..
			}
			| Source::Knn {
				..
			}
			| Source::Count {
				..
			}
			| Source::Union(_)
			| Source::Values {
				..
			}
			| Source::Nested {
				..
			}
			| Source::Probe {
				..
			}
			| Source::Sorted {
				..
			}
			| Source::Groups {
				..
			}
			| Source::Split {
				..
			}
			| Source::Join {
				..
			}
			| Source::Once
			| Source::Changes {
				..
			}
			| Source::WriteTargets {
				..
			}
			| Source::WriteInserts {
				..
			}
			| Source::WriteEdges {
				..
			}
			| Source::LightweightEdges {
				..
			}
			| Source::Vacant {
				..
			} => false,
		};
		if let Some(sort) = topk_sort
			&& floors_early
			&& let Some(spec) = usize::from(sort)
				.checked_sub(usize::from(reads.base.sorts))
				.and_then(|at| reads.sorts.get_mut(at))
		{
			spec.floors_at_keep = true;
		}
		return Ok(Opened {
			objects: yields_records(&leaf.source),
			ids: false,
			source: leaf.source,
			source_op: Some(input),
			table: leaf.table,
			schema: schema_of(ir, input),
			feeder: None,
		});
	}
	let data = ir.op(input).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	if tag == PhysTag::MatchStep {
		return step::open_step(em, cx, ir, facts, reads, input);
	}
	// A breaker no op reads is the statement's output, which its reader opens. Nothing builds one
	// read twice: the binder binds a tree, where a value's one reader is its parent; conversion is
	// one-to-one; logical CSE merges only expression ops and `phys_cse` merges no physical op; the
	// e-graph's write-back adds no sharing (`egraph/elaborate.rs`); and a physical rule that
	// rewires rows hands the rows of the op it replaces to that op's reader.
	if data.results.iter().map(|result| ir.uses(*result).len()).sum::<usize>() > 1 {
		return Err(invariant("a breaker read more than once"));
	}
	if tag == PhysTag::Join {
		return join::open_join(em, cx, ir, facts, reads, input);
	}
	if tag == PhysTag::Probe {
		return probe::open_probe(em, cx, ir, facts, reads, input);
	}
	let (feeder, trail) = match (&data.kind, data.operands.first()) {
		(
			OpKind::Phys(
				PhysOp::Sort(_)
				| PhysOp::Window(_)
				| PhysOp::KnnTopK(_)
				| PhysOp::Aggregate(_)
				| PhysOp::Split {
					..
				},
			),
			Some(rows),
		) => pipeline_behind(ir, *rows)?,
		(
			OpKind::Phys(
				PhysOp::Sort(_)
				| PhysOp::Window(_)
				| PhysOp::KnnTopK(_)
				| PhysOp::Aggregate(_)
				| PhysOp::Split {
					..
				},
			),
			None,
		) => {
			return Err(invariant("a breaker with no input"));
		}
		// The physical phase wraps only a fan-out loop in a pipeline (`pipeline::build`), so the
		// op a pipeline opens is what gives a select or write loop its rows, past the streaming
		// ops before it: a source, a statement in `FROM`, a union, a breaker or a MATCH step, each
		// opened above. The rules after the wrap put only a probe or an index lookup before a
		// loop. A reader of its own opens only a breaker, a source or a step.
		_ => {
			return Err(invariant("rows a pipeline opens that no source, breaker or step makes"));
		}
	};
	let (sink, source) = match &data.kind {
		OpKind::Phys(PhysOp::Aggregate(attrs)) => {
			let id = pool_index(reads.base.aggregates, reads.aggregates.len(), "vm aggregates")?;
			let plan = aggregate::plan(ir, cx.registry(), data, attrs)?;
			let mut needs = Needs::none();
			reads.aggregates.push(aggregate::sink_spec(em, cx, ir, &plan, attrs, &mut needs)?);
			reads.needs = reads.needs.merged(&needs);
			(
				Sink::Aggregate {
					id,
					op: input,
					plan: Box::new(plan),
				},
				Source::Groups {
					aggregate: id,
				},
			)
		}
		OpKind::Phys(PhysOp::Sort(attrs)) => {
			let id = pool_index(reads.base.sorts, reads.sorts.len(), "vm sorts")?;
			reads.sorts.push(sort_spec(attrs));
			(
				Sink::Sort {
					id,
					op: input,
				},
				Source::Sorted {
					sort: id,
				},
			)
		}
		OpKind::Phys(PhysOp::KnnTopK(attrs)) => {
			let id = pool_index(reads.base.sorts, reads.sorts.len(), "vm sorts")?;
			let (vector, query) = match &attrs.vector {
				KnnTargetShape::Literal(vector) => {
					(KnnVector::Literal(Arc::from(vector.as_slice())), None)
				}
				KnnTargetShape::Deferred(text, before_rows) => {
					let target = *data
						.operands
						.get(1)
						.ok_or_else(|| invariant("a deferred KNN target with no operand"))?;
					let mut needs = Needs::none();
					let scalar = host_scalar_of(em, cx, ir, target, &mut needs)?;
					reads.needs = reads.needs.merged(&needs);
					(
						KnnVector::Query(id),
						Some(KnnQuery {
							vector: DeferredVector {
								scalar,
								text: Arc::from(text.as_str()),
							},
							group: em.group(),
							before_rows: *before_rows,
						}),
					)
				}
			};
			let before_rows = query.as_ref().is_none_or(|query| query.before_rows);
			reads.sorts.push(knn_sort_spec(attrs, em.knn_slot(), query));
			(
				Sink::Knn {
					id,
					field: attrs.field.clone(),
					rank: Arc::new(KnnRank {
						vector,
						distance: attrs.distance.clone(),
						before_rows,
					}),
				},
				Source::Sorted {
					sort: id,
				},
			)
		}
		OpKind::Phys(PhysOp::Window(shape)) => {
			let id = window::push_window(em, cx, ir, reads, data, shape)?;
			(
				Sink::Split {
					id,
				},
				Source::Split {
					split: id,
				},
			)
		}
		OpKind::Phys(PhysOp::Split {
			targets,
		}) => {
			let id = push_split(reads, targets)?;
			(
				Sink::Split {
					id,
				},
				Source::Split {
					split: id,
				},
			)
		}
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
			return Err(invariant(
				"a breaker that is not a sort, a window, an aggregate or a split",
			));
		}
	};
	let fed = lower_feeder(em, cx, ir, facts, feeder, &trail, &sink, reads)?;
	// The rows of a sort are those its feeder handed it, record ids where the feeder's may be.
	let ids = matches!(source, Source::Sorted { .. }) && reads.fed_ids;
	// A split rewrites the fields it splits on, and a window appends the fields of its calls, so
	// their rows are records when the rows they read are and they are no `VALUE` projection. A
	// sort's rows are the rows its feeder handed it, so they are records when the feeder's are;
	// the rows of a search or a group are records.
	let objects = match &source {
		Source::Split {
			..
		} => fed && !data.operands.first().is_some_and(|rows| projects_value(ir, *rows)),
		Source::Sorted {
			..
		} => fed,
		Source::Scan {
			..
		}
		| Source::Record {
			..
		}
		| Source::Index {
			..
		}
		| Source::Lookup {
			..
		}
		| Source::IndexUnion {
			..
		}
		| Source::Bitmap {
			..
		}
		| Source::FullText {
			..
		}
		| Source::Knn {
			..
		}
		| Source::Count {
			..
		}
		| Source::Union(_)
		| Source::Values {
			..
		}
		| Source::Nested {
			..
		}
		| Source::Probe {
			..
		}
		| Source::Groups {
			..
		}
		| Source::Join {
			..
		}
		| Source::Once
		| Source::Changes {
			..
		}
		| Source::WriteTargets {
			..
		}
		| Source::WriteInserts {
			..
		}
		| Source::WriteEdges {
			..
		}
		| Source::LightweightEdges {
			..
		}
		| Source::Vacant {
			..
		} => true,
	};
	Ok(Opened {
		source,
		source_op: None,
		table: None,
		schema: None,
		feeder: reads.pipelines.len().checked_sub(1),
		objects,
		ids,
	})
}

/// Registers a split on `targets` in the pools of `reads`, and answers its index in the program.
/// The pipeline its rows run through is set when that pipeline is lowered.
fn push_split(reads: &mut Reads, targets: &[SplitTarget]) -> Result<u16, IrError> {
	let id = pool_index(reads.base.splits, reads.splits.len(), "vm splits")?;
	reads.splits.push(SplitSpec {
		kind: SplitKind::Targets(targets.to_vec()),
		pipeline: 0,
		build_before: None,
	});
	Ok(id)
}

/// Whether `rows` are the values of a `VALUE` projection, looking through the pipeline or the
/// select loop that delivers them to the op that made them.
fn projects_value(ir: &Ir, rows: ValueId) -> bool {
	let Some(data) = producer_of(ir, rows).ok().and_then(|op| ir.op(op)) else {
		return false;
	};
	match &data.kind {
		OpKind::Phys(PhysOp::Project(shape)) => {
			matches!(**shape, ProjectionShape::Value | ProjectionShape::OmittedValue { .. })
		}
		OpKind::Phys(
			PhysOp::Pipeline(_)
			| PhysOp::Loop {
				..
			},
		) => data
			.regions
			.first()
			.and_then(|region| ops_in(ir, *region).last())
			.and_then(|last| ir.op(last))
			.and_then(|last| match (&last.kind, last.operands.as_slice()) {
				(OpKind::Phys(PhysOp::Yield), [delivered]) => Some(*delivered),
				_ => None,
			})
			.is_some_and(|delivered| projects_value(ir, delivered)),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// Whether `op` is a breaker whose rows a pipeline reads as its source: a sort, a window, a
/// brute-force search, an aggregate, a split, a join or a probe.
pub(super) fn is_breaker(ir: &Ir, op: OpId) -> bool {
	matches!(
		ir.op(op).map(|data| &data.kind),
		Some(OpKind::Phys(
			PhysOp::Sort(_)
				| PhysOp::Window(_)
				| PhysOp::KnnTopK(_)
				| PhysOp::Aggregate(_)
				| PhysOp::Split { .. }
				| PhysOp::Join(_)
				| PhysOp::Probe { .. }
		))
	)
}

/// Whether `op` is a `phys.split`.
fn is_split(ir: &Ir, op: OpId) -> bool {
	matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Split { .. })))
}

/// Opens the split `split`, at `at` (the stage of the pipeline `op` and the position in the body of
/// that stage's select loop), as the source of the ops that follow it: the ops before it are
/// lowered first, into a pipeline that ends in the split.
#[allow(clippy::too_many_arguments)]
fn open_cut(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	op: OpId,
	split: OpId,
	at: (usize, usize),
) -> Result<Opened, IrError> {
	let data = ir.op(split).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let OpKind::Phys(PhysOp::Split {
		targets,
	}) = &data.kind
	else {
		return Err(invariant("a cut at an op that is no split"));
	};
	let id = push_split(reads, targets)?;
	let split_sink = Sink::Split {
		id,
	};
	let fed = lower_part(em, cx, ir, facts, op, &[], &split_sink, reads, Some(at))?;
	reads.unowned += 1;
	Ok(Opened {
		source: Source::Split {
			split: id,
		},
		source_op: None,
		table: None,
		schema: None,
		feeder: reads.pipelines.len().checked_sub(1),
		objects: fed && !data.operands.first().is_some_and(|rows| projects_value(ir, *rows)),
		ids: false,
	})
}

/// The sort `attrs` stands for.
///
/// The statement's `TEMPFILES` holds on disk only a sort that keeps all its rows in an order.
fn sort_spec(attrs: &SortAttrs) -> SortSpec {
	SortSpec {
		keys: attrs
			.keys
			.iter()
			.map(|key| KeyOrder {
				ascending: key.ascending,
				collate: key.collate,
				numeric: key.numeric,
			})
			.collect(),
		keep: attrs.keep,
		held: attrs.spill,
		spill: attrs.tempfiles && attrs.keep.is_none() && !attrs.random,
		pushdown: attrs.pushdown,
		floors_at_keep: false,
		shares_floor: attrs.keep.is_some() && !attrs.random,
		random: attrs.random,
		knn_slot: None,
		knn_query: None,
	}
}

/// One pipeline of the body being lowered, with what the lowering reads of it.
struct Stage {
	op: OpId,
	/// The ops of the pipeline's region but its terminator, in data-flow order.
	chain: Vec<OpId>,
	/// The rows the region yields.
	yielded: ValueId,
	head: Head,
	/// The streaming ops that follow the pipeline.
	trail: Vec<OpId>,
}

impl Stage {
	/// The stage of the pipeline `op`, followed by the streaming ops `trail`.
	fn of(ir: &Ir, op: OpId, trail: Vec<OpId>) -> Result<Self, IrError> {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let [region] = <[RegionId; 1]>::try_from(data.regions.as_slice())
			.map_err(|_| invariant("a pipeline with other than one region"))?;
		let mut chain: Vec<OpId> = ops_in(ir, region).collect();
		let terminator = chain.pop().ok_or_else(|| invariant("a pipeline region with no op"))?;
		let yielded = match ir.op(terminator).map(|data| (&data.kind, data.operands.as_slice())) {
			Some((OpKind::Phys(PhysOp::Yield), [yielded])) => *yielded,
			_ => return Err(invariant("a pipeline region that does not end in a yield")),
		};
		let head = head_of(ir, &chain)?;
		Ok(Self {
			op,
			chain,
			yielded,
			head,
			trail,
		})
	}
}

/// The sort of the brute-force search `attrs` stands for: the `k` rows of least distance, in
/// order of their distance, held in memory. `slot` publishes each kept row's distance for
/// `vector::distance::knn()`, and `query` is the query vector the host evaluates when the
/// statement starts, where it is not a literal. The sort takes its floor as soon as it holds `k`
/// rows, so a row ranked farther than every kept one is dropped before it is kept from then on.
fn knn_sort_spec(attrs: &KnnTopKAttrs, slot: u16, query: Option<KnnQuery>) -> SortSpec {
	SortSpec {
		keys: vec![KeyOrder {
			ascending: true,
			collate: false,
			numeric: false,
		}],
		keep: Some(attrs.k as usize),
		held: Spill::Bounded,
		spill: false,
		pushdown: false,
		floors_at_keep: true,
		shares_floor: true,
		random: false,
		knn_slot: Some(slot),
		knn_query: query,
	}
}

/// Lowers the pipeline `op` and the streaming ops `trail` that follow it, which end in `sink`, and
/// answers whether the rows it hands the sink are records.
///
/// A select loop that reads the rows of another pipeline continues that pipeline: the two are one
/// body, the earlier pipeline's ops running first. Any other `op` [`pipeline_behind`] answers is
/// read by a pipeline of its own ([`lower_reader`]).
#[allow(clippy::too_many_arguments)]
fn lower_pipeline(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	op: OpId,
	trail: &[OpId],
	sink: &Sink,
	reads: &mut Reads,
) -> Result<bool, IrError> {
	if !is_pipeline(ir, op) {
		return lower_reader(em, cx, ir, facts, op, trail, sink, reads);
	}
	lower_part(em, cx, ir, facts, op, trail, sink, reads, None)
}

/// Lowers the pipeline `op` with the streaming ops `trail` where `upto` is `None`, or, where it is
/// `Some((stage, at))`, only its ops that run before position `at` of the select loop's body of the
/// stage `stage` (the pipelines `op` continues first), which hold the rows a split at that
/// position reads. Answers whether the rows it hands `sink` are records.
///
/// A split in a select loop's body cuts the pipeline, as the VM runs a split: the ops before it are
/// a pipeline that ends in the split, and the ops after it, the loop's other ops, the stages that
/// continue it and `trail` are a pipeline whose source is the split. Each part runs serially,
/// because the placement the op states was derived for the pipeline uncut.
#[allow(clippy::too_many_arguments)]
fn lower_part(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	op: OpId,
	trail: &[OpId],
	sink: &Sink,
	reads: &mut Reads,
	upto: Option<(usize, usize)>,
) -> Result<bool, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let OpKind::Phys(PhysOp::Pipeline(attrs)) = &data.kind else {
		return Err(IrError::Stale("ir_lower::pipe: not a pipeline"));
	};
	// The pipelines this one continues come first: a select loop that reads the rows of another
	// pipeline runs in the body of that pipeline, after the ops that follow it.
	let mut stages = vec![Stage::of(ir, op, trail.to_vec())?];
	while let Some(earlier) = stages.first().map(|stage| stage.head.input)
		&& is_pipeline(ir, earlier)
	{
		// `before_of` and `trail_of` walk the same streaming ops over operand 0, one back from the
		// select loop and the other forward from the pipeline, and every op between stands in the
		// region of both. The two differ only where a second op reads one of those rows, which no
		// producer builds: the binder binds a tree and no physical rule shares rows.
		let held = trail_of(ir, ir.enclosing_region(earlier), earlier)?;
		if stages.first().is_some_and(|stage| stage.head.before != held) {
			return Err(invariant("a pipeline read through ops another op also reads"));
		}
		stages.insert(0, Stage::of(ir, earlier, held)?);
	}
	let fused = stages.len() > 1;
	let is_cut = stages.iter().any(|stage| stage.head.body.iter().any(|step| is_split(ir, *step)));
	// The stages this part runs, and the ops of each one's select loop body it runs: every stage
	// and every op where `upto` is `None`; otherwise the stages up to the one `upto` names, whose
	// body ends before the position it names.
	let last = upto.map_or(stages.len(), |(stage, _)| stage + 1);
	let stages = stages.get(..last).ok_or_else(|| invariant("a cut beyond the last stage"))?;
	let bodies = stages
		.iter()
		.enumerate()
		.map(|(at, stage)| match upto {
			Some((cut_stage, cut_at)) if cut_stage == at => stage
				.head
				.body
				.get(..cut_at)
				.ok_or_else(|| invariant("a cut beyond the end of a loop body")),
			_ => Ok(stage.head.body.as_slice()),
		})
		.collect::<Result<Vec<_>, _>>()?;
	// The last split this part runs, as its stage and its position in that stage's body.
	let cut = bodies.iter().enumerate().rev().find_map(|(at, body)| {
		body.iter().rposition(|step| is_split(ir, *step)).map(|position| (at, position))
	});
	let head = &stages.first().ok_or_else(|| invariant("a pipeline with no stage"))?.head;

	// The feeders come first: nothing of this pipeline is emitted before its source is open.
	let topk_sort = match sink {
		Sink::Sort {
			id,
			op: sort,
		} if sort_pushes_down(ir, *sort) => Some(*id),
		Sink::Sort {
			..
		}
		| Sink::Knn {
			..
		}
		| Sink::Aggregate {
			..
		}
		| Sink::Split {
			..
		}
		| Sink::JoinProbe {
			..
		}
		| Sink::JoinPair {
			..
		}
		| Sink::Write {
			..
		}
		| Sink::Output => None,
	};
	// The stage and the op of its loop's body this part starts at, and the row that op reads.
	let (opened, (first_stage, first_step), element) = match cut {
		Some((stage, at)) => {
			let split = bodies
				.get(stage)
				.and_then(|body| body.get(at))
				.copied()
				.ok_or_else(|| invariant("a cut beyond the end of a loop body"))?;
			let opened = open_cut(em, cx, ir, facts, reads, op, split, (stage, at))?;
			let rows = first_result(ir.op(split).ok_or(IrError::Stale("ir_lower::pipe: op"))?)?;
			(opened, (stage, at + 1), Some(rows))
		}
		None => {
			let opened = open_source(em, cx, ir, facts, reads, head.input, topk_sort)?;
			// A threshold rejects an entry before the body runs, which only a pipeline that
			// raises for no row may leave unrun. The source is the first pipeline's, and so is
			// the placement, which states whether the pipeline prunes. A full-text scan, or an
			// index union of full-text branches, rejecting by a sum of clause scores may do so
			// under the narrower verdict that every op a rejected hit skips raises for no row but
			// the scores the scan already computed.
			let first = stages.first().map_or(op, |stage| stage.op);
			let scored = matches!(
				opened.source,
				Source::FullText { .. }
					| Source::IndexUnion {
						topk: Some(_),
						..
					}
			);
			if opened.source.rejects_by_threshold()
				&& !ir
					.placement(first)
					.is_some_and(|placed| placed.prunes() || (scored && placed.prunes_by_score()))
			{
				return Err(invariant(
					"a scan rejects entries by a threshold in a pipeline that may raise",
				));
			}
			(opened, (0, 0), head.element.map(|(element, _)| element))
		}
	};
	let mut runs = Vec::with_capacity(3 * stages.len() + 1);
	// The ops that stand between the source and the first stage's select loop run first, for each
	// row the source yields.
	if cut.is_none() && !head.before.is_empty() {
		let input = ir.op(head.input).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		runs.push(Run {
			from: first_result(input)?,
			ops: &head.before,
			delivers: None,
		});
	}
	// Each stage runs a select loop's body for each row, then the ops that follow the loop, then
	// the ops that follow the pipeline, which read the rows the op of the pipeline defines. A part
	// past a cut starts in the cut's stage, past the split, and a part a split cuts ends in the
	// stage `upto` names, with the ops of its body before the split.
	for (at, (stage, body)) in stages.iter().zip(&bodies).enumerate().skip(first_stage) {
		let ends = upto.is_some_and(|(cut_stage, _)| cut_stage == at);
		let (from, ops) = match at == first_stage {
			true => (
				element,
				body.get(first_step..)
					.ok_or_else(|| invariant("a cut beyond the end of a loop body"))?,
			),
			false => (stage.head.element.map(|(element, _)| element), *body),
		};
		if let Some(from) = from {
			runs.push(Run {
				from,
				ops,
				delivers: match ends {
					false => stage.head.element.map(|(_, delivered)| delivered),
					true => None,
				},
			});
		}
		if ends {
			break;
		}
		runs.push(Run {
			from: stage.head.after,
			ops: stage.chain.get(1..).unwrap_or(&[]),
			delivers: Some(stage.yielded),
		});
		let own = ir.op(stage.op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		runs.push(Run {
			from: first_result(own)?,
			ops: &stage.trail,
			delivers: None,
		});
	}
	let origin = match (is_cut, fused) {
		(true, _) => Origin::Cut {
			op,
			id: attrs.id,
		},
		(false, true) => Origin::Fused {
			op,
			id: attrs.id,
			head: stages.first().map(|stage| stage.op).unwrap_or(op),
			pipelines: stages.len(),
		},
		(false, false) => Origin::Placed {
			op,
			id: attrs.id,
		},
	};
	emit_pipeline(em, cx, ir, facts, reads, opened, &runs, sink, origin)
}

/// Lowers a pipeline that no op stands for: it reads the rows of the breaker `breaker` (a sort, an
/// aggregate or a split whose rows no pipeline op reads), or of a source no pipeline op reads,
/// through the streaming ops `trail` into `sink`, as the reader of a sort, a grouping or a source
/// that ends a statement. Answers whether the rows it hands `sink` are records.
#[allow(clippy::too_many_arguments)]
fn lower_reader(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	breaker: OpId,
	trail: &[OpId],
	sink: &Sink,
	reads: &mut Reads,
) -> Result<bool, IrError> {
	let opened = open_source(em, cx, ir, facts, reads, breaker, None)?;
	let from = first_result(ir.op(breaker).ok_or(IrError::Stale("ir_lower::pipe: op"))?)?;
	let runs = [Run {
		from,
		ops: trail,
		delivers: None,
	}];
	reads.unowned += 1;
	emit_pipeline(em, cx, ir, facts, reads, opened, &runs, sink, Origin::Reader)
}

/// Lowers the pipeline `op` (a pipeline op, or a breaker that a pipeline with no op reads) and the
/// streaming ops `trail` that follow it into `sink`.
#[allow(clippy::too_many_arguments)]
fn lower_feeder(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	op: OpId,
	trail: &[OpId],
	sink: &Sink,
	reads: &mut Reads,
) -> Result<bool, IrError> {
	match is_pipeline(ir, op) {
		true => lower_pipeline(em, cx, ir, facts, op, trail, sink, reads),
		false => lower_reader(em, cx, ir, facts, op, trail, sink, reads),
	}
}

/// A run of streaming ops of a pipeline's body: the rows the first reads, the ops in data-flow
/// order, and the rows the last must deliver where the container states them.
struct Run<'o> {
	from: ValueId,
	ops: &'o [OpId],
	delivers: Option<ValueId>,
}

/// Where a pipeline being emitted came from, which decides how it is placed.
#[derive(Clone, Copy)]
enum Origin {
	/// The pipeline op `op` of id `id`, whose placement the pipeline takes.
	Placed {
		op: OpId,
		id: PipelineId,
	},
	/// A part of the pipeline op `op` that a split in its loop's body cut: serial, because the
	/// op's placement was derived for the pipeline uncut.
	Cut {
		op: OpId,
		id: PipelineId,
	},
	/// The pipeline op `op` of id `id` with the pipelines it continues, the first of which is
	/// `head`: the body holds the bodies of the `pipelines` of the chain, and the placement of
	/// `head` is the one the physical plan derived for the chain it starts, taken only when it
	/// judged a chain of as many pipelines.
	Fused {
		op: OpId,
		id: PipelineId,
		head: OpId,
		pipelines: usize,
	},
	/// A reader of a breaker that no op stands for: serial, like the reader of a sort or a
	/// grouping.
	Reader,
}

/// Emits the pipeline whose source `opened` is and whose body lowers `runs` in order, ending in
/// `sink`, into `reads`. Answers whether the rows it hands `sink` are records.
#[allow(clippy::too_many_arguments)]
fn emit_pipeline(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	opened: Opened,
	runs: &[Run<'_>],
	sink: &Sink,
	origin: Origin,
) -> Result<bool, IrError> {
	reads.table = reads.table.take().or(opened.table);
	let registry = cx.registry();
	let feeder = opened.feeder;
	let mut source = opened.source;
	let source_op = opened.source_op;
	let schema = opened.schema;
	let rows_are_objects = opened.objects;
	let rows_may_be_ids = opened.ids || yields_ids(&source);
	let split_source = if let Source::Split {
		split,
	} = &source
	{
		Some(*split)
	} else {
		None
	};
	let join_source = if let Source::Join {
		join,
	} = &source
	{
		Some(*join)
	} else {
		None
	};

	let mut body = Body {
		em,
		cx,
		ir,
		reads,
		source: &source,
		schema,
		objects: rows_are_objects,
		ids: rows_may_be_ids,
		fetched: None,
		one_record: names_one_record(&source),
		end: 0,
		halt: 0,
		jumps: EndJumps::default(),
		limit_checks: Vec::new(),
		pipeline_limits: Vec::new(),
		enforce: None,
		emitted: Emitted::default(),
		claims: Vec::new(),
		needs: Needs {
			context: ContextLevel::Root,
			keeps_row_order: false,
			reach: None,
			access_mode: crate::exec::AccessMode::ReadOnly,
		},
		current: ROW,
		// A fused body holds the ops of every pipeline it continues, and the decision of the last
		// is derived over all of them.
		io: match origin {
			Origin::Placed {
				op,
				..
			}
			| Origin::Fused {
				op,
				..
			} => ir.placement(op).map(|attr| attr.io().clone()).unwrap_or_default(),
			Origin::Cut {
				..
			}
			| Origin::Reader => PipelineIo::default(),
		},
		rows_ahead: None,
	};
	body.needs = body.reads.needs.clone();
	body.em.set_frontier_hops(body.io.frontier_hops.clone());
	body.end = body.em.label()?;
	body.halt = body.em.label()?;

	for run in runs {
		let mut rows = run.from;
		for step in run.ops {
			rows = body.step(*step, rows)?;
		}
		if run.delivers.is_some_and(|delivered| rows != delivered) {
			return Err(invariant("a pipeline whose ops do not deliver its rows"));
		}
	}
	// The rows are enforced for the fields the ops after the enforcement read of them.
	if let Some(Enforce::Rows {
		needed,
		..
	}) = &mut body.enforce
	{
		*needed = enforced_demand(ir, runs, sink)?;
	}
	body.finish(sink)?;
	let (objects, ids) = (body.objects, body.ids);

	let Body {
		em,
		cx,
		reads,
		jumps: _,
		limit_checks: _,
		pipeline_limits,
		enforce,
		emitted,
		claims,
		needs,
		io,
		rows_ahead,
		..
	} = body;

	// The dereferences of the projection have no step of their own to carry the decision.
	let rows_ahead = match (rows_ahead, &io.rows_ahead) {
		(None, Some(ahead)) if ahead.derefs => Some(RowsAheadSpec {
			fetch: None,
			fields: ahead.fields.iter().cloned().collect(),
			paging: fetch::paging_slot(ahead.paging, &pipeline_limits)?,
		}),
		(spec, _) => spec,
	};
	let limit_slot = match pipeline_limits.as_slice() {
		[limit] => Some(*limit),
		_ => None,
	};
	// The pipeline whose placement the body takes: the pipeline itself, or the first of the
	// pipelines it continues when the physical plan judged a chain of as many pipelines as the
	// body holds.
	let placed = match origin {
		Origin::Placed {
			op,
			..
		} => Some(op),
		Origin::Fused {
			head,
			pipelines,
			..
		} => ir
			.placement(head)
			.and_then(|placed| placed.chain())
			.filter(|judged| *judged == pipelines)
			.map(|_| head),
		Origin::Cut {
			..
		}
		| Origin::Reader => None,
	};
	let parallel = match placed {
		Some(op) => {
			let physical = facts_of(ir, facts, registry, op)?;
			match parallel_of(ir, op, limit_slot)? {
				// What the plan assumed of the body must be no more than the body holds: a
				// disagreement in the direction that would run a body as morsels that must not
				// leaves the pipeline serial, which always answers as the serial run.
				Parallel::Morsels(plan)
					if (plan.paged.is_some() && !emitted.paging_replayable)
						|| (!physical.fetches_row && emitted.reads_record_rows)
						|| (!physical.binds_row && emitted.rebound)
						|| physical.distincts != emitted.distincts =>
				{
					Parallel::Serial
				}
				parallel @ (Parallel::Serial | Parallel::Morsels(_)) => parallel,
			}
		}
		None => Parallel::Serial,
	};

	// A limit spent before the first row spares every pipeline feeding this one its records, as
	// it spares a source it reads directly.
	let mut feeding = feeder;
	while let Some(input) = feeding {
		if let Some(fed) = reads.pipelines.get_mut(input) {
			fed.limits.extend(pipeline_limits.iter().copied());
			fed.read_ahead &= fed.limits.is_empty();
		}
		feeding = reads.feeders.get(input).copied().flatten();
	}

	if enforce.is_none()
		&& !emitted.reads_record_rows
		&& !emitted.rebound
		&& rows_ahead.is_none()
		&& io.frontier_rows.is_none()
		&& let Some(source_op) = source_op
		&& let Some(decode) = spatial_source_decode(ir, cx.indexes(), source_op)?
	{
		set_source_decode(&mut source, decode);
	}

	let mut pipeline = finish(em, source, enforce, pipeline_limits, parallel)?;
	pipeline.deref_windows = io.deref_windows;
	pipeline.frontier_rows = frontier_rows_of(&pipeline, &io, em)?;
	em.end_hops();
	// Only a scan that runs its body in the storage cursor reads through a chunk source, and a
	// pipeline whose limits a reader spends may end before its first chunk.
	pipeline.read_ahead = io.scan_read_ahead && pipeline.in_place && pipeline.limits.is_empty();
	pipeline.rows_ahead = rows_ahead;
	let index = reads.base.pipelines + reads.pipelines.len();
	match origin {
		Origin::Placed {
			op,
			id,
		}
		| Origin::Cut {
			op,
			id,
		}
		| Origin::Fused {
			op,
			id,
			..
		} => reads.provenance.record(index, op, id)?,
		Origin::Reader => {}
	}
	// A split's rows run through the pipeline whose source it is.
	if let Some(split) = split_source {
		let spec = usize::from(split)
			.checked_sub(usize::from(reads.base.splits))
			.and_then(|at| reads.splits.get_mut(at))
			.ok_or_else(|| invariant("a split source outside the pool"))?;
		spec.pipeline = index;
	}
	// A join's pairings run through the pipeline whose source it is.
	if let Some(join) = join_source {
		let spec = usize::from(join)
			.checked_sub(usize::from(reads.base.joins))
			.and_then(|at| reads.joins.get_mut(at))
			.ok_or_else(|| invariant("a join source outside the pool"))?;
		spec.pipeline = index;
	}
	reads.claims.push(claims);
	reads.feeders.push(feeder);
	reads.pipelines.push(pipeline);
	reads.fed_ids = ids;
	reads.needs = needs;
	Ok(objects)
}

/// The fields the ops of `runs` after their `phys.enforced` read of the rows it delivers: the
/// `needed` of [`Enforce::Rows`], `None` where they read the row whole
/// ([`crate::exec::ir::analysis::liveness::pipeline_demand`]).
///
/// Rows the pipeline hands a sort are read by the sort's keys and then by the streaming ops that
/// read the sorted rows, so the demand follows them there; a sort whose rows another pipeline
/// reads hands on the record, which needs it whole. Rows the pipeline hands an aggregate are read
/// by the aggregate's key and field regions alone, since the groups it delivers are no longer the
/// record.
fn enforced_demand(
	ir: &Ir,
	runs: &[Run<'_>],
	sink: &Sink,
) -> Result<Option<Arc<std::collections::HashSet<String>>>, IrError> {
	use crate::exec::ir::analysis::liveness;
	use crate::exec::ir::iface::effects::FieldSet;
	let enforced = |op: &OpId| {
		matches!(ir.op(*op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Enforced { .. })))
	};
	let Some((run, at)) = runs
		.iter()
		.enumerate()
		.find_map(|(run, part)| part.ops.iter().position(enforced).map(|at| (run, at)))
	else {
		return Err(invariant("an enforcement no step of the pipeline states"));
	};
	let ops = runs[run].ops;
	let rows = first_result(ir.op(ops[at]).ok_or(IrError::Stale("ir_lower::pipe: op"))?)?;
	let mut parts: Vec<(ValueId, &[OpId])> = vec![(rows, &ops[at + 1..])];
	parts.extend(runs[run + 1..].iter().map(|part| (part.from, part.ops)));
	let sort_op;
	let sorted;
	if let Sink::Sort {
		op,
		..
	} = sink
	{
		let data = ir.op(*op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let input = *data.operands.first().ok_or_else(|| invariant("a sort with no input"))?;
		sort_op = [*op];
		sorted = reader_runs(ir, *op)?;
		parts.push((input, &sort_op));
		parts.extend(sorted.iter().map(|(from, ops)| (*from, ops.as_slice())));
	}
	if let Sink::Aggregate {
		op,
		..
	} = sink
	{
		let data = ir.op(*op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let input =
			*data.operands.first().ok_or_else(|| invariant("an aggregate with no input"))?;
		parts.push((input, std::slice::from_ref(op)));
	}
	let demand = liveness::pipeline_demand(ir, rows, &parts);
	Ok(match demand.stored {
		FieldSet::All => None,
		FieldSet::Some(names) => {
			Some(Arc::new(names.iter().map(|name| name.as_str().to_owned()).collect()))
		}
	})
}

fn source_decode_fields(
	ir: &Ir,
	runs: &[Run<'_>],
	sort_sink: Option<OpId>,
	agg_sink: Option<OpId>,
) -> Result<Option<Vec<String>>, IrError> {
	use crate::exec::ir::analysis::liveness;
	use crate::exec::ir::core::ir::{Walk, WalkOrder};
	use crate::exec::ir::iface::effects::FieldSet;
	let Some(first) = runs.first() else {
		return Ok(None);
	};
	let rows = first.from;
	let mut parts: Vec<(ValueId, &[OpId])> =
		runs.iter().map(|part| (part.from, part.ops)).collect();
	let sort_op;
	let sorted;
	if let Some(op) = sort_sink {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let input = *data.operands.first().ok_or_else(|| invariant("a sort with no input"))?;
		sort_op = [op];
		sorted = reader_runs(ir, op)?;
		parts.push((input, &sort_op));
		parts.extend(sorted.iter().map(|(from, ops)| (*from, ops.as_slice())));
	}
	let agg_op;
	if let Some(op) = agg_sink {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let input =
			*data.operands.first().ok_or_else(|| invariant("an aggregate with no input"))?;
		agg_op = [op];
		parts.push((input, &agg_op));
	}
	for (_, steps) in &parts {
		for step in steps.iter().copied() {
			let Some(data) = ir.op(step) else {
				return Ok(None);
			};
			if matches!(
				&data.kind,
				OpKind::Phys(
					PhysOp::Layer(_)
						| PhysOp::Enforced { .. }
						| PhysOp::Fetch | PhysOp::MatchStep(_)
				)
			) {
				return Ok(None);
			}
			let mut has_implicit_row_read = false;
			for region in data.regions.iter().copied() {
				ir.walk(region, WalkOrder::PreOrder, &mut |op| {
					let Some(op_data) = ir.op(op) else {
						return Walk::Continue;
					};
					let reads_row_implicitly = if matches!(
						&op_data.kind,
						OpKind::Expr(
							ExprOp::Subquery(_)
								| ExprOp::ClosureCreate(_)
								| ExprOp::Invoke | ExprOp::Dispatch(_),
						) | OpKind::Phys(
							PhysOp::Subquery
								| PhysOp::Loop {
									kind: LoopKind::Closure,
									..
								},
						)
					) {
						true
					} else if let OpKind::Expr(ExprOp::Call(call)) = &op_data.kind {
						if matches!(&call.function, crate::expr::Function::Script(_)) {
							true
						} else if let crate::expr::Function::Normal(name) = &call.function {
							name.as_str() == "function::call"
						} else {
							false
						}
					} else if let OpKind::Expr(ExprOp::FreeParam {
						name,
					}) = &op_data.kind
					{
						matches!(name.as_str(), "parent" | "this")
					} else {
						false
					};
					if reads_row_implicitly {
						has_implicit_row_read = true;
					}
					Walk::Continue
				});
			}
			if has_implicit_row_read {
				return Ok(None);
			}
		}
	}
	let demand = liveness::pipeline_demand(ir, rows, &parts);
	// Without a `PhysOp::Layer` step in `parts`, `pipeline_demand` never populates `computed` or
	// `guarded`; guard explicitly so any future computed-field demand falls back to whole-record
	// decoding.
	debug_assert!(demand.computed.is_empty() && demand.guarded.is_empty());
	if !demand.computed.is_empty() || !demand.guarded.is_empty() {
		return Ok(None);
	}
	let FieldSet::Some(names) = demand.stored else {
		return Ok(None);
	};
	Ok(Some(names.iter().map(|name| name.as_str().to_owned()).collect()))
}

fn sink_of_tail_value(
	ir: &Ir,
	region: RegionId,
	value: ValueId,
) -> Option<(Option<OpId>, Option<OpId>)> {
	let mut sink = (None, None);
	for read in ir.uses(value) {
		let user = ir.op(read.user)?;
		if matches!(
			&user.kind,
			OpKind::Phys(PhysOp::Yield)
				| OpKind::Expr(ExprOp::Yield)
				| OpKind::Phys(PhysOp::Explain { .. })
		) && ir.enclosing_region(read.user) == region
		{
		} else if matches!(&user.kind, OpKind::Phys(PhysOp::Only | PhysOp::VersionScope))
			&& ir.enclosing_region(read.user) == region
			&& read.operand == 0
		{
			for wrapped in &user.results {
				if sink_of_tail_value(ir, region, *wrapped)? != (None, None) {
					return None;
				}
			}
		} else if matches!(&user.kind, OpKind::Phys(PhysOp::Sort(_))) && read.operand == 0 {
			if sink != (None, None) {
				return None;
			}
			sink = (Some(read.user), None);
		} else if matches!(&user.kind, OpKind::Phys(PhysOp::Aggregate(_))) && read.operand == 0 {
			if sink != (None, None) {
				return None;
			}
			sink = (None, Some(read.user));
		} else {
			return None;
		}
	}
	Some(sink)
}

fn sink_of_tail(ir: &Ir, region: RegionId, tail_op: OpId) -> Option<(Option<OpId>, Option<OpId>)> {
	let data = ir.op(tail_op)?;
	let mut sink = (None, None);
	for result in &data.results {
		let found = sink_of_tail_value(ir, region, *result)?;
		if found != (None, None) {
			if sink != (None, None) {
				return None;
			}
			sink = found;
		}
	}
	Some(sink)
}

fn bitmap_shape_uses_spatial(
	shape: &crate::exec::ir::phys::ops::BitmapShape,
	indexes: &dyn super::read::IndexResolver,
) -> bool {
	use crate::exec::ir::phys::ops::BitmapShape;
	match shape {
		BitmapShape::BTree {
			index,
			..
		} => indexes
			.resolve(index)
			.is_some_and(|r| matches!(r.definition().index, crate::catalog::Index::Spatial)),
		BitmapShape::FullText {
			..
		}
		| BitmapShape::Graph {
			..
		} => false,
		BitmapShape::And(children) | BitmapShape::Or(children) => {
			children.iter().any(|child| bitmap_shape_uses_spatial(child, indexes))
		}
		BitmapShape::AndNot {
			base,
			subtract,
		} => {
			bitmap_shape_uses_spatial(base, indexes) || bitmap_shape_uses_spatial(subtract, indexes)
		}
	}
}

fn union_merge_field(
	merge: &Option<crate::exec::operators::scan::MergeMode>,
) -> Option<Option<String>> {
	if let Some(
		crate::exec::operators::scan::MergeMode::ByIndexKey {
			path,
			..
		}
		| crate::exec::operators::scan::MergeMode::ByIndexKeyDedup {
			path,
			..
		},
	) = merge
	{
		if let Some(FieldPathPart::Field(name)) = path.0.first() {
			Some(Some(name.clone()))
		} else {
			None
		}
	} else {
		Some(None)
	}
}

/// Computes the [`DecodeFields`](crate::exec::operators::scan::common::DecodeFields) that pipeline
/// lowering attaches to a spatial `PhysOp::IndexScan`, `PhysOp::IndexUnion`, or `PhysOp::Bitmap`
/// source `source_op`, shared between pipeline lowering (`emit_pipeline`) and `EXPLAIN`.
pub(super) fn spatial_source_decode(
	ir: &Ir,
	indexes: &dyn super::read::IndexResolver,
	source_op: OpId,
) -> Result<Option<crate::exec::operators::scan::common::DecodeFields>, IrError> {
	use crate::exec::operators::scan::common::DecodeFields;
	if in_union(ir, source_op) {
		return Ok(None);
	}
	let Some(source_data) = ir.op(source_op) else {
		return Ok(None);
	};
	let merge_field = if let OpKind::Phys(PhysOp::IndexScan(attrs)) = &source_data.kind {
		let Some(resolved) = indexes.resolve(&attrs.index) else {
			return Ok(None);
		};
		if !matches!(resolved.definition().index, crate::catalog::Index::Spatial) {
			return Ok(None);
		}
		None
	} else if let OpKind::Phys(PhysOp::IndexUnion(attrs)) = &source_data.kind {
		let has_spatial = attrs.branches.iter().any(|b| {
			indexes
				.resolve(&b.index)
				.is_some_and(|r| matches!(r.definition().index, crate::catalog::Index::Spatial))
		});
		if !has_spatial {
			return Ok(None);
		}
		let Some(merge_field) = union_merge_field(&attrs.merge) else {
			return Ok(None);
		};
		merge_field
	} else if let OpKind::Phys(PhysOp::Bitmap(attrs)) = &source_data.kind {
		if !bitmap_shape_uses_spatial(&attrs.root, indexes) {
			return Ok(None);
		}
		if let Some(&fallback_region) = source_data.regions.first()
			&& let Some(last) = ops_in(ir, fallback_region).last()
			&& let Some(terminator) = ir.op(last)
			&& let (OpKind::Phys(PhysOp::Yield), [yielded]) =
				(&terminator.kind, terminator.operands.as_slice())
			&& let Ok(fallback_op) = producer_of(ir, *yielded)
			&& let Some(OpKind::Phys(PhysOp::IndexUnion(fallback_attrs))) =
				ir.op(fallback_op).map(|d| &d.kind)
		{
			let Some(merge_field) = union_merge_field(&fallback_attrs.merge) else {
				return Ok(None);
			};
			merge_field
		} else {
			None
		}
	} else {
		return Ok(None);
	};
	let region = ir.enclosing_region(source_op);
	let before_trail = trail_of(ir, region, source_op)?;
	let last_before = before_trail.last().copied().unwrap_or(source_op);
	let last_before_val =
		first_result(ir.op(last_before).ok_or(IrError::Stale("ir_lower::pipe: op"))?)?;

	let mut stages = Vec::new();
	if let [read] = ir.uses(last_before_val)
		&& read.operand == 0
		&& starts_pipeline_in(ir, region, read.user)
		&& matches!(
			ir.op(read.user).map(|d| &d.kind),
			Some(OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Select,
				..
			}))
		) && let Some(first_pipe) = ir.parent_op(ir.enclosing_region(read.user))
	{
		let first_trail = trail_of(ir, region, first_pipe)?;
		let first_stage = Stage::of(ir, first_pipe, first_trail)?;
		if first_stage.head.input != source_op || first_stage.head.before != before_trail {
			return Ok(None);
		}
		stages.push(first_stage);
		while let Some(last_stage) = stages.last() {
			let tail_op = last_stage.trail.last().copied().unwrap_or(last_stage.op);
			let tail_val =
				first_result(ir.op(tail_op).ok_or(IrError::Stale("ir_lower::pipe: op"))?)?;
			if let [next_read] = ir.uses(tail_val)
				&& next_read.operand == 0
				&& starts_pipeline_in(ir, region, next_read.user)
				&& matches!(
					ir.op(next_read.user).map(|d| &d.kind),
					Some(OpKind::Phys(PhysOp::Loop {
						kind: LoopKind::Select,
						..
					}))
				) && let Some(next_pipe) = ir.parent_op(ir.enclosing_region(next_read.user))
			{
				let next_trail = trail_of(ir, region, next_pipe)?;
				let next_stage = Stage::of(ir, next_pipe, next_trail)?;
				if next_stage.head.input != last_stage.op
					|| next_stage.head.before != last_stage.trail
				{
					return Ok(None);
				}
				stages.push(next_stage);
			} else {
				break;
			}
		}
	}

	let mut fields = if !stages.is_empty() {
		if stages.iter().any(|stage| stage.head.body.iter().any(|step| is_split(ir, *step))) {
			return Ok(None);
		}
		let last_stage = stages.last().ok_or_else(|| invariant("empty stages"))?;
		if let Some(placed) = ir.placement(last_stage.op) {
			let io = placed.io();
			if io.frontier_rows.is_some()
				|| io.rows_ahead.as_ref().is_some_and(|ahead| ahead.derefs)
			{
				return Ok(None);
			}
		}
		let tail_op = last_stage.trail.last().copied().unwrap_or(last_stage.op);
		let Some((sort_sink, agg_sink)) = sink_of_tail(ir, region, tail_op) else {
			return Ok(None);
		};
		let head = &stages[0].head;
		let mut runs = Vec::with_capacity(3 * stages.len() + 1);
		if !head.before.is_empty() {
			let input = ir.op(head.input).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
			runs.push(Run {
				from: first_result(input)?,
				ops: &head.before,
				delivers: None,
			});
		}
		for stage in &stages {
			if let Some((from, delivered)) = stage.head.element {
				runs.push(Run {
					from,
					ops: &stage.head.body,
					delivers: Some(delivered),
				});
			}
			runs.push(Run {
				from: stage.head.after,
				ops: stage.chain.get(1..).unwrap_or(&[]),
				delivers: Some(stage.yielded),
			});
			let own = ir.op(stage.op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
			runs.push(Run {
				from: first_result(own)?,
				ops: &stage.trail,
				delivers: None,
			});
		}
		let Some(fields) = source_decode_fields(ir, &runs, sort_sink, agg_sink)? else {
			return Ok(None);
		};
		fields
	} else {
		let Some((sort_sink, agg_sink)) = sink_of_tail(ir, region, last_before) else {
			return Ok(None);
		};
		let from = first_result(source_data)?;
		let runs = [Run {
			from,
			ops: &before_trail,
			delivers: None,
		}];
		let Some(fields) = source_decode_fields(ir, &runs, sort_sink, agg_sink)? else {
			return Ok(None);
		};
		fields
	};

	if let Some(merge_field) = merge_field
		&& !fields.contains(&merge_field)
	{
		fields.push(merge_field);
	}
	Ok(Some(DecodeFields::new(fields)))
}

fn set_source_decode(
	source: &mut Source,
	decode: crate::exec::operators::scan::common::DecodeFields,
) {
	if let Source::Index {
		decode: slot @ None,
		..
	}
	| Source::IndexUnion {
		decode: slot @ None,
		..
	} = source
	{
		*slot = Some(decode);
	} else if let Source::Bitmap {
		decode: slot @ None,
		fallback,
		..
	} = source
	{
		if let Some(fallback) = fallback.as_deref_mut() {
			set_source_decode(fallback, decode.clone());
		}
		*slot = Some(decode);
	}
}

/// The runs of streaming ops that read the rows of the sort `sort`: the ops that follow the sort,
/// then, where a select loop reads what they deliver, its body, and the ops that follow it; where
/// a pipeline reads them, its ops as [`lower_part`] runs them.
fn reader_runs(ir: &Ir, sort: OpId) -> Result<Vec<(ValueId, Vec<OpId>)>, IrError> {
	let data = ir.op(sort).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let trail = trail_of(ir, ir.enclosing_region(sort), sort)?;
	let last = trail.last().copied().unwrap_or(sort);
	let mut runs = vec![(first_result(data)?, trail)];
	let delivered = first_result(ir.op(last).ok_or(IrError::Stale("ir_lower::pipe: op"))?)?;
	let [read] = ir.uses(delivered) else {
		return Ok(runs);
	};
	if read.operand != 0 {
		return Ok(runs);
	}
	let reader = read.user;
	let is_loop = matches!(
		ir.op(reader).map(|data| &data.kind),
		Some(OpKind::Phys(PhysOp::Loop {
			kind: LoopKind::Select,
			..
		}))
	);
	let (head, chain, own, follows) = if is_loop
		&& starts_pipeline_in(ir, ir.enclosing_region(sort), reader)
		&& let Some(pipe) = ir.parent_op(ir.enclosing_region(reader))
	{
		let follows = trail_of(ir, ir.enclosing_region(pipe), pipe)?;
		let stage = Stage::of(ir, pipe, follows)?;
		let chain = stage.chain.get(1..).unwrap_or(&[]).to_vec();
		(stage.head, chain, stage.op, stage.trail)
	} else if is_loop {
		let head = head_of(ir, &[reader])?;
		(head, Vec::new(), reader, trail_of(ir, ir.enclosing_region(reader), reader)?)
	} else if is_pipeline(ir, reader) {
		let follows = trail_of(ir, ir.enclosing_region(reader), reader)?;
		let stage = Stage::of(ir, reader, follows)?;
		let chain = stage.chain.get(1..).unwrap_or(&[]).to_vec();
		(stage.head, chain, stage.op, stage.trail)
	} else {
		return Ok(runs);
	};
	// The reader's head walks back over the ops that follow the sort, which the first run holds.
	if head.input != last && head.input != sort {
		return Ok(runs);
	}
	if head.input == last && !head.before.is_empty() {
		runs.push((delivered, head.before));
	}
	if let Some((element, _)) = head.element {
		runs.push((element, head.body));
	}
	runs.push((head.after, chain));
	let own = ir.op(own).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	runs.push((first_result(own)?, follows));
	Ok(runs)
}

/// The index in the program of the entry `len` entries into a pool of [`Reads`] that starts at
/// `base`.
fn pool_index(base: u16, len: usize, pool: &'static str) -> Result<u16, IrError> {
	u16::try_from(len).ok().and_then(|len| base.checked_add(len)).ok_or(IrError::Capacity(pool))
}

/// Whether a row `source` yields may be a record id: a statement in `FROM` yields its rows as
/// they are.
fn yields_ids(source: &Source) -> bool {
	match source {
		Source::Nested {
			..
		} => true,
		Source::Union(sources) => sources.iter().any(yields_ids),
		Source::Scan {
			..
		}
		| Source::Record {
			..
		}
		| Source::Index {
			..
		}
		| Source::Lookup {
			..
		}
		| Source::IndexUnion {
			..
		}
		| Source::Bitmap {
			..
		}
		| Source::FullText {
			..
		}
		| Source::Knn {
			..
		}
		| Source::Count {
			..
		}
		| Source::Values {
			..
		}
		| Source::Probe {
			..
		}
		| Source::Sorted {
			..
		}
		| Source::Groups {
			..
		}
		| Source::Split {
			..
		}
		| Source::Join {
			..
		}
		| Source::Once
		| Source::Changes {
			..
		}
		| Source::WriteTargets {
			..
		}
		| Source::WriteInserts {
			..
		}
		| Source::WriteEdges {
			..
		}
		| Source::LightweightEdges {
			..
		}
		| Source::Vacant {
			..
		} => false,
	}
}

/// Whether the sort `sort` publishes the threshold of its first key to the scan that feeds it.
fn sort_pushes_down(ir: &Ir, sort: OpId) -> bool {
	matches!(
		ir.op(sort).map(|data| &data.kind),
		Some(OpKind::Phys(PhysOp::Sort(attrs))) if attrs.pushdown
	)
}

/// The body of the pipeline being lowered: the instructions emitted for each row, and what they
/// need of the pipeline that holds them.
struct Body<'a, 's, 'c> {
	em: &'a mut Emitter,
	cx: &'a mut LowerCx<'c>,
	ir: &'a Ir,
	reads: &'a mut Reads,
	source: &'s Source,
	/// What the catalog declares for the rows the source reads: the kinds a typed comparison of
	/// a field reads. A step that rewrites the fields of the rows clears it.
	schema: Option<Arc<Schema>>,
	/// Whether the rows are records, whose fields the body reads itself.
	objects: bool,
	/// Whether a row may be a record id, which `*` reads the record of.
	ids: bool,
	/// The record a `*` beside fields fetched for a row that is a record id, which the fields of
	/// the projection being lowered read in place of the row.
	fetched: Option<Reg>,
	/// Whether the source names one record, whose row is read and judged whatever the paging.
	one_record: bool,
	/// The label the row's end binds, and the one the pipeline's stop binds.
	end: Label,
	halt: Label,
	jumps: EndJumps,
	/// The limits whose check follows the hand-off of the row.
	limit_checks: Vec<u16>,
	/// The limits this pipeline holds.
	pipeline_limits: Vec<u16>,
	enforce: Option<Enforce>,
	emitted: Emitted,
	claims: Vec<Claim>,
	needs: Needs,
	/// The register that holds the row the next step reads: [`ROW`] until a projection.
	current: Reg,
	/// How the physical plan decided the pipeline reads storage beyond its ops: the default, no
	/// window, for a pipeline the plan did not place as one.
	io: PipelineIo,
	/// The `FETCH` whose links the pipeline reads ahead of the rows ([`PipelineIo::rows_ahead`]).
	rows_ahead: Option<RowsAheadSpec>,
}

impl Body<'_, '_, '_> {
	/// Lowers the streaming op `step`, which reads `rows`, and answers the rows it delivers.
	fn step(&mut self, step: OpId, rows: ValueId) -> Result<ValueId, IrError> {
		let ir = self.ir;
		let step_data = ir.op(step).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let OpKind::Phys(kind) = &step_data.kind else {
			return Err(invariant("a pipeline holding an op outside the physical dialect"));
		};
		if !streams(kind) {
			return Err(invariant("a pipeline holding a breaker beneath its last op"));
		}
		if step_data.operands.first() != Some(&rows) {
			return Err(invariant("a pipeline whose ops do not follow one another"));
		}
		match kind {
			PhysOp::Enforced {
				table,
			} => {
				self.enforce = Some(Enforce::Rows {
					table: table.clone(),
					needed: None,
				})
			}
			PhysOp::Filter => self.filter(step_data)?,
			PhysOp::Fetch => self.fetch(step_data)?,
			PhysOp::PermissionFilter(attrs) => self.permission_filter(step_data, attrs)?,
			PhysOp::Layer(attrs) => self.layer(
				&step_data.regions,
				&attrs.fields,
				&attrs.computed,
				attrs.judged.as_ref(),
			)?,
			PhysOp::Limit {
				limit,
				start,
			} => self.limit(step_data, *limit, *start)?,
			PhysOp::Project(shape) => self.project(step_data, shape)?,
			PhysOp::MatchStep(attrs) => match attrs.as_ref() {
				MatchStepAttrs::Bind(BindStepShape::Bind {
					name,
				}) => self.bind_row(name)?,
				MatchStepAttrs::Bind(_)
				| MatchStepAttrs::Read {
					..
				}
				| MatchStepAttrs::Update(_)
				| MatchStepAttrs::Delete {
					..
				}
				| MatchStepAttrs::Insert(_)
				| MatchStepAttrs::Output(_) => {
					return Err(invariant("a breaker beneath the last op of a pipeline"));
				}
			},
			PhysOp::Distinct {
				..
			} => {
				let distinct = pool_index(
					self.reads.base.distincts,
					self.reads.distincts.len(),
					"vm distinct sets",
				)?;
				self.reads.distincts.push(self.em.group());
				self.emitted.distincts += 1;
				self.em.emit(Instr::Distinct {
					distinct,
					src: self.current,
					target: self.end,
				});
				self.jumps.other += 1;
			}
			// The check above admits only the ops `streams` admits, and each has an arm here.
			other @ (PhysOp::TableScan(_)
			| PhysOp::IndexScan(_)
			| PhysOp::IndexUnion(_)
			| PhysOp::Bitmap(_)
			| PhysOp::IndexLookup(_)
			| PhysOp::FullTextScan(_)
			| PhysOp::KeyRangeScan(_)
			| PhysOp::KnnScan(_)
			| PhysOp::KnnTopK(_)
			| PhysOp::Count(_)
			| PhysOp::RecordScan(_)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(_)
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Eval
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(_)
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Block
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(_)
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next) => return Err(invariant(non_step(other.tag()))),
		}
		first_result(step_data)
	}

	/// Checks that the row is still the one the source yielded, which `what` needs.
	fn require_row(&self, what: &'static str) -> Result<(), IrError> {
		if self.current == ROW {
			Ok(())
		} else {
			Err(invariant(what))
		}
	}

	/// The value the clause region `region` yields, evaluated against the row in `row`.
	fn clause_value(&mut self, region: RegionId, row: Reg) -> Result<Operand, IrError> {
		let ir = self.ir;
		let (objects, schema) = (self.objects && row == ROW, self.schema.clone());
		let mut lowerer = Lowerer::in_context(&mut *self.em, ir, self.cx)
			.with_schema(schema)
			.with_reads(&mut *self.reads);
		if let Some(arg) = first_arg(ir, region) {
			lowerer.bind_arg(arg, Operand::Reg(row));
			if row == ROW {
				lowerer.bind_row(arg, objects);
			}
		}
		let value = lowerer.region_value(region)?;
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		Ok(value)
	}

	/// Lowers a filter: its predicate, preceded by the conjuncts that read nothing of the row
	/// where the op holds a second region.
	fn filter(&mut self, step_data: &OpData) -> Result<(), IrError> {
		self.require_row("a filter over a projection")?;
		let (predicate, once) = match step_data.regions.as_slice() {
			[predicate] => (*predicate, None),
			[predicate, once] => (*predicate, Some(*once)),
			_ => return Err(invariant("a filter with other than one or two regions")),
		};
		if let Some(once) = once {
			self.once_verdict(once, None)?;
		}
		self.judge(predicate, self.end, None)
	}

	/// Ends the row where the predicate the region `region` yields is falsy, jumping to `target`,
	/// and falls through where it is truthy. The block argument that is the row of the region is
	/// the row of the body.
	///
	/// With a `frame`, the predicate is judged in that frame of the container, which the program
	/// names by the permission frame it holds.
	fn judge(
		&mut self,
		region: RegionId,
		target: Label,
		frame: Option<(FrameId, u16)>,
	) -> Result<(), IrError> {
		let ir = self.ir;
		// A predicate that raises ends in its raise rather than a yield: every row that reaches it
		// raises, so the raise stands in place of the branch.
		let predicate = if region_raises(ir, region) {
			None
		} else {
			Some(yielded_value(ir, region)?)
		};
		let end = self.end;
		let (objects, schema) = (self.objects, self.schema.clone());
		let mut lowerer = Lowerer::in_context(&mut *self.em, ir, self.cx)
			.with_schema(schema)
			.with_reads(&mut *self.reads);
		if let Some(arg) = row_arg(ir, region) {
			lowerer.bind_arg(arg, Operand::Reg(ROW));
			lowerer.bind_row(arg, objects);
		}
		let jumps = &mut self.jumps;
		// A predicate a `RETURN` held in it ends is judged by the value of the context, which the
		// `RETURN` may set before the predicate's own branches are reached.
		let context = ends_context(ir, region);
		let mut branch = |lowerer: &mut Lowerer<'_>| match predicate {
			Some(_) if context => {
				let cond = lowerer.region_value(region)?;
				Branching {
					lowerer,
					ir,
					end,
					jumps,
				}
				.jump(cond, false, target, false)
			}
			Some(predicate) => Branching {
				lowerer,
				ir,
				end,
				jumps,
			}
			.branch(predicate, false, target),
			None => lowerer.eager_region_value(region).map(|_| ()),
		};
		match frame {
			Some((container, program)) => lowerer.in_frame(container, program, branch)?,
			None => branch(&mut lowerer)?,
		}
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		Ok(())
	}

	fn limit(
		&mut self,
		step_data: &OpData,
		limit: Option<u16>,
		start: Option<u16>,
	) -> Result<(), IrError> {
		let id = pool_index(self.reads.base.limits, self.reads.limits.len(), "vm limits")?;
		let limit_scalar = limit.map(|slot| self.paging_scalar(step_data, slot)).transpose()?;
		let start_scalar = start.map(|slot| self.paging_scalar(step_data, slot)).transpose()?;
		self.reads.limits.push(LimitSpec {
			limit: limit_scalar,
			start: start_scalar,
			group: self.em.group(),
		});
		self.pipeline_limits.push(id);
		self.emitted.paging_replayable = limit.is_some() || start.is_some();
		if start.is_some() {
			self.em.emit(Instr::Skip {
				limit: id,
				target: self.end,
			});
			self.jumps.other += 1;
		}
		if limit.is_some() {
			let joins = matches!(self.source, Source::Join { .. } | Source::Split { .. });
			if (!self.objects && !joins) || (self.one_record && !self.emitted.rebound) {
				// Past the limit a row is judged and not taken: the pipeline reads every row.
				self.emitted.paging_replayable = false;
				self.em.emit(Instr::JumpIfLimitReached {
					limit: id,
					done: self.end,
				});
				self.jumps.other += 1;
			} else if opens_under_spent_limit(self.source) {
				// A scan, sort, aggregate or join is not opened under a spent limit, so only the
				// rows it takes are checked.
				self.limit_checks.push(id);
			} else {
				// A range or a union stops at its limit, as a scan does: no row past it is read
				// or judged.
				self.emitted.paging_replayable = false;
				self.em.emit(Instr::JumpIfLimitReached {
					limit: id,
					done: self.halt,
				});
				self.limit_checks.push(id);
			}
			self.em.emit(Instr::Take {
				limit: id,
			});
		}
		Ok(())
	}

	/// The host scalar a paging clause names: the value the op's operand `slot` denotes,
	/// evaluated when the program starts.
	fn paging_scalar(&mut self, limit: &OpData, slot: u16) -> Result<HostScalar, IrError> {
		let value = *limit
			.operands
			.get(usize::from(slot))
			.ok_or_else(|| invariant("a paging slot past the operands of its op"))?;
		let mut needs = Needs::none();
		let scalar = host_scalar_of(self.em, self.cx, self.ir, value, &mut needs)?;
		self.needs = self.needs.merged(&needs);
		Ok(scalar)
	}

	/// Lowers a projection, leaving the register that holds the projected row in `current`.
	fn project(&mut self, step_data: &OpData, shape: &ProjectionShape) -> Result<(), IrError> {
		self.require_row("a projection over a projection")?;
		// `SELECT *` over a row that is a record id is the record, and no row where none answers.
		// `*` beside fields over rows that are not records reads the record too, and its fields
		// read the record.
		let fetches = match shape {
			ProjectionShape::All => self.ids,
			ProjectionShape::Star {
				..
			} => self.ids && !self.objects,
			ProjectionShape::Value
			| ProjectionShape::OmittedValue {
				..
			}
			| ProjectionShape::SortedValue(_)
			| ProjectionShape::Fields {
				..
			} => false,
		};
		if fetches {
			self.needs.reach = Some(match self.needs.reach.take() {
				Some(held) => held.union(crate::exec::vm::dispatch::Reach::any()),
				None => crate::exec::vm::dispatch::Reach::any(),
			});
			let dst = self.em.reg()?;
			self.em.emit(Instr::FetchRow {
				dst,
				none: self.end,
			});
			self.jumps.other += 1;
			self.current = dst;
			self.ids = false;
			if matches!(shape, ProjectionShape::All) {
				return Ok(());
			}
			self.fetched = Some(dst);
		}
		let ir = self.ir;
		let regions = step_data.regions.as_slice();
		let projected = projection::project(self, ir, shape, regions);
		self.fetched = None;
		self.current = projected?;
		match shape {
			ProjectionShape::All => {}
			ProjectionShape::Value
			| ProjectionShape::OmittedValue {
				..
			}
			| ProjectionShape::SortedValue(_) => {
				self.objects = false;
				self.ids = false;
			}
			ProjectionShape::Fields {
				fields,
				..
			}
			| ProjectionShape::Star {
				fields,
				..
			} => {
				if !self.objects && projection::fields_make_an_object(self, ir, fields, regions)? {
					self.objects = true;
				}
				self.ids = false;
			}
		}
		Ok(())
	}

	/// Ends the body: the hand-off of the row, the checks of the limits that are spent, the row's
	/// end and the pipeline's stop.
	fn finish(&mut self, sink: &Sink) -> Result<(), IrError> {
		match sink {
			Sink::Output => {
				self.em.emit(Instr::Emit {
					src: self.current,
				});
			}
			Sink::Sort {
				id,
				op,
			} => self.sort_push(*id, *op)?,
			Sink::Knn {
				id,
				field,
				rank,
			} => self.knn_push(*id, field, rank)?,
			Sink::Aggregate {
				id,
				plan,
				..
			} => {
				self.require_row("an aggregate over a projection")?;
				self.fold_into(*id, plan)?;
			}
			Sink::Split {
				id,
			} => {
				self.em.emit(Instr::SplitPush {
					split: *id,
					src: self.current,
				});
			}
			Sink::JoinProbe {
				id,
				keys,
			} => {
				self.require_row("a join over a projection")?;
				self.join_probe(*id, keys)?;
			}
			Sink::JoinPair {
				id,
			} => {
				self.require_row("a join over a projection")?;
				self.em.emit(Instr::JoinPair {
					join: *id,
				});
			}
			Sink::Write {
				write,
			} => {
				self.require_row("a write over a projection")?;
				self.write_rows(*write)?;
			}
		}
		for id in std::mem::take(&mut self.limit_checks) {
			self.em.emit(Instr::JumpIfLimitReached {
				limit: id,
				done: self.halt,
			});
		}
		end_row(self.em, self.end, &self.jumps)?;
		self.em.bind(self.halt)?;
		self.em.emit(Instr::StopPipeline);
		Ok(())
	}

	/// The body of the write `write` over the record in flight: the record is made from the row,
	/// the statement gate judges it, and each order of the write runs its stages over it.
	///
	/// The stages' host pipelines need what the write's other pipelines need, and the program is a
	/// transaction that writes at least in the database.
	fn write_rows(&mut self, write: OpId) -> Result<(), IrError> {
		let ir = self.ir;
		let data = ir.op(write).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let OpKind::Phys(PhysOp::Write(attrs)) = &data.kind else {
			return Err(invariant("a write sink that is not a write"));
		};
		let entry = WriteEntry {
			from_row: true,
			gated: true,
		};
		if let Some(linked) = &attrs.write.linked {
			// A write whose stages are linked holds no stage op: it runs each run of the stages it
			// states as a site, applied by the bundle its spec holds.
			write_body(self.em, &attrs.write.body, &linked.run, entry, &mut SiteSink::default())?;
		} else if attrs.write.statement.is_some() {
			// A write that names no one table runs each run of stages as a site, which its spec
			// dispatches by the table a record is written to.
			let run = StageRun::of(ir, data, self.cx.reads_checked())?;
			write_body(self.em, &attrs.write.body, &run.stages, entry, &mut SiteSink::default())?;
		} else {
			let run = StageRun::of(ir, data, self.cx.reads_checked())?;
			let mut stages = Stages::new(&run, ir, self.cx.limits(), self.cx.registry())
				.in_context(&mut *self.cx);
			let cut = cut_of_rows(ir, data);
			write_body_cut(self.em, &attrs.write.body, &run.stages, entry, &mut stages, cut)?;
			if let Some(held) = stages.needs.take() {
				self.needs = self.needs.merged(&held);
			}
		}
		self.needs.access_mode = crate::exec::AccessMode::ReadWrite;
		self.needs.context = self.needs.context.max(ContextLevel::Database);
		Ok(())
	}

	/// The hand-off of the row to the join `id`, keyed by the values the left key regions `keys`
	/// take over it, in order.
	fn join_probe(&mut self, id: u16, keys: &[RegionId]) -> Result<(), IrError> {
		let first = self.em.regs(keys.len())?;
		for (at, region) in keys.iter().enumerate() {
			let at = Reg::try_from(at).map_err(|_| IrError::Capacity("vm registers"))?;
			let value = self.clause_value(*region, ROW)?;
			self.em.emit(Instr::Load {
				dst: first + at,
				src: value,
			});
		}
		let count = u16::try_from(keys.len()).map_err(|_| IrError::Capacity("vm join keys"))?;
		self.em.emit(Instr::JoinProbe {
			join: id,
			keys: first,
			count,
		});
		Ok(())
	}

	/// The push of the keys of the row, and of the row, to the sort `id`, which the op `sort` is.
	/// The row is the one a projection built where one precedes the sort.
	///
	/// A key that is a path is read from the row as a field, or structurally when the rows may be
	/// any value or a projection built the row; a computed key is the value its region yields. A
	/// path that does not start at a field answers `NotYet`.
	fn sort_push(&mut self, id: u16, sort: OpId) -> Result<(), IrError> {
		let ir = self.ir;
		let data = ir.op(sort).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let OpKind::Phys(PhysOp::Sort(attrs)) = &data.kind else {
			return Err(invariant("a sort sink that is not a sort"));
		};
		let first = self.em.regs(attrs.keys.len())?;
		let mut computed = data.regions.iter();
		let row = self.current;
		for (at, key) in attrs.keys.iter().enumerate() {
			let at = Reg::try_from(at).map_err(|_| IrError::Capacity("vm registers"))?;
			let dst = first + at;
			match &key.read {
				// Over rows that may be any value, and over the row a projection built, the key
				// is read from the row as a path step.
				SortKeyReadShape::Path(path) if !self.objects || row != ROW => {
					let path = self.em.path(path.clone())?;
					self.em.emit(Instr::Extract {
						src: Operand::Reg(row),
						path,
						dst,
					});
				}
				SortKeyReadShape::Path(path) => match path.0.split_first() {
					Some((FieldPathPart::Field(name), rest)) => {
						let field = self.em.field(name)?;
						if rest.is_empty() {
							self.em.emit(Instr::Load {
								dst,
								src: Operand::Field(field),
							});
						} else {
							let rest = self.em.path(FieldPath(rest.to_vec()))?;
							self.em.emit(Instr::LoadPath {
								dst,
								field,
								rest,
							});
						}
					}
					// Over object rows a key path starts at a field; any other is the construct
					// the planner declines, with its reason.
					_ => {
						return Err(IrError::Unsupported {
							reason: "a sort key path that does not start at a field".to_owned(),
						});
					}
				},
				SortKeyReadShape::Computed => {
					let region = computed
						.next()
						.ok_or_else(|| invariant("a computed sort key with no region"))?;
					let value = self.clause_value(*region, row)?;
					self.em.emit(Instr::Load {
						dst,
						src: value,
					});
				}
			}
		}
		let count =
			u16::try_from(attrs.keys.len()).map_err(|_| IrError::Capacity("vm sort keys"))?;
		self.em.emit(Instr::SortPush {
			sort: id,
			keys: first,
			count,
			src: row,
		});
		Ok(())
	}
}

/// The invariant break a pipeline's body answers for an op of tag `tag` that it holds as a step and
/// has no arm for.
///
/// The step tags are those of the ops [`streams`] admits, and each has an arm in [`Body::step`];
/// every other op is a breaker, a source or an op outside a row pipeline. The match names every
/// tag, so a tag that joins the dialect is classified here before it compiles.
fn non_step(tag: PhysTag) -> &'static str {
	match tag {
		PhysTag::Enforced
		| PhysTag::Filter
		| PhysTag::Fetch
		| PhysTag::PermissionFilter
		| PhysTag::Layer
		| PhysTag::Limit
		| PhysTag::Project
		| PhysTag::Distinct
		| PhysTag::MatchStep => "a streaming op with no arm in a pipeline's body",
		PhysTag::TableScan
		| PhysTag::IndexScan
		| PhysTag::IndexUnion
		| PhysTag::Bitmap
		| PhysTag::IndexLookup
		| PhysTag::FullTextScan
		| PhysTag::KeyRangeScan
		| PhysTag::KnnScan
		| PhysTag::KnnTopK
		| PhysTag::Count
		| PhysTag::RecordScan
		| PhysTag::Vacant
		| PhysTag::Values
		| PhysTag::Subquery
		| PhysTag::Probe
		| PhysTag::Union
		| PhysTag::Join
		| PhysTag::Split
		| PhysTag::Sort
		| PhysTag::Window
		| PhysTag::Aggregate
		| PhysTag::Timeout
		| PhysTag::VersionScope
		| PhysTag::Only
		| PhysTag::Explain
		| PhysTag::Eval
		| PhysTag::Request
		| PhysTag::Txn
		| PhysTag::Stmt
		| PhysTag::Statement
		| PhysTag::Match
		| PhysTag::DdlGuard
		| PhysTag::Define
		| PhysTag::Remove
		| PhysTag::Alter
		| PhysTag::Rebuild
		| PhysTag::Info
		| PhysTag::Use
		| PhysTag::Option
		| PhysTag::Access
		| PhysTag::Kill
		| PhysTag::Live
		| PhysTag::Subscriber
		| PhysTag::Show
		| PhysTag::Write
		| PhysTag::WriteStage
		| PhysTag::Events
		| PhysTag::Assign
		| PhysTag::Unset
		| PhysTag::Content
		| PhysTag::Block
		| PhysTag::Let
		| PhysTag::Sleep
		| PhysTag::If
		| PhysTag::Loop
		| PhysTag::Pipeline
		| PhysTag::Choose
		| PhysTag::Prefetch
		| PhysTag::Materialize
		| PhysTag::LoadUnit
		| PhysTag::Yield
		| PhysTag::Return
		| PhysTag::Break
		| PhysTag::Next => "a pipeline holding a breaker beneath its last op",
	}
}

impl projection::ProjectionSite for Body<'_, '_, '_> {
	fn emitter(&mut self) -> &mut Emitter {
		self.em
	}

	fn registry(&self) -> &FunctionRegistry {
		self.cx.registry()
	}

	fn objects(&self) -> bool {
		self.objects
	}

	fn clause_value(&mut self, region: RegionId, row: Reg) -> Result<Operand, IrError> {
		let row = match self.fetched {
			Some(fetched) if row == ROW => fetched,
			_ => row,
		};
		Body::clause_value(self, region, row)
	}

	fn call_operands(
		&mut self,
		region: RegionId,
		args: &[ValueId],
	) -> Result<(Vec<Operand>, Option<ReturnCatch>), IrError> {
		let ir = self.ir;
		let mut lowerer =
			Lowerer::in_context(&mut *self.em, ir, self.cx).with_reads(&mut *self.reads);
		if let Some(arg) = first_arg(ir, region) {
			let row = self.fetched.unwrap_or(ROW);
			lowerer.bind_arg(arg, Operand::Reg(row));
			if row == ROW {
				lowerer.bind_row(arg, self.objects);
			}
		}
		let operands = projection::operands_in(&mut lowerer, region, args)?;
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		Ok(operands)
	}

	fn needs_call(&mut self, context: ContextLevel) {
		self.needs.context = self.needs.context.max(context);
		self.needs.keeps_row_order = true;
	}

	/// The register that holds the value of the key `key` of the sort that handed over the rows.
	fn fetched(&self) -> Option<Reg> {
		self.fetched
	}

	fn sort_key(&mut self, key: u16) -> Result<Reg, IrError> {
		if !matches!(self.source, Source::Sorted { .. }) {
			return Err(invariant("a sort key read over rows no sort handed over"));
		}
		let dst = self.em.reg()?;
		self.em.emit(Instr::SortKey {
			key,
			dst,
		});
		Ok(dst)
	}

	fn host_over_value(&mut self, region: RegionId) -> Result<u16, IrError> {
		let ir = self.ir;
		let mut lowerer =
			Lowerer::in_context(&mut *self.em, ir, self.cx).with_reads(&mut *self.reads);
		let code = projection::value_pipeline(&mut lowerer, region)?;
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		Ok(code)
	}
}

/// The finished pipeline: the emitter's code over `source`, with what the plan decided.
///
/// A pipeline that enforces holds values, never stored bytes, so it runs on neither the stored
/// bytes of a record nor of a sorted row.
fn finish(
	em: &mut Emitter,
	source: Source,
	enforce: Option<Enforce>,
	limits: Vec<u16>,
	parallel: Parallel,
) -> Result<Pipeline, IrError> {
	let mut pipeline = em.finish_pipeline(source)?;
	if enforce.is_some() {
		pipeline.in_place = false;
		pipeline.stored_rows = false;
	}
	pipeline.enforce = enforce;
	pipeline.limits = limits;
	pipeline.parallel = parallel;
	Ok(pipeline)
}

/// The rows window the plan states for `pipeline`, over the lookups its code runs.
///
/// The plan states the chains of hops the projection runs over the row, each by the graph-step
/// ops that state them, and which paging clause stands before them; this names the lookup the
/// emitted code holds for each hop. Hops the lowering joined into one lookup have the one lookup,
/// which ends at the last of them and covers as many of the chain's hops as it has. A chain ends
/// where no lookup the pipeline emitted covers exactly the next hops. It answers `None` where no
/// chain holds a lookup.
fn frontier_rows_of(
	pipeline: &Pipeline,
	io: &PipelineIo,
	em: &Emitter,
) -> Result<Option<FrontierRowsSpec>, IrError> {
	let Some(rows) = &io.frontier_rows else {
		return Ok(None);
	};
	let paging = match rows.paging {
		None => None,
		Some(place) => Some(pipeline.limits.get(place).copied().ok_or_else(|| {
			invariant("the paging clause a lookup window reads ahead of is not lowered")
		})?),
	};
	let mut chains: Vec<Box<[u32]>> = Vec::new();
	for stated in &rows.chains {
		let mut chain: Vec<u32> = Vec::new();
		let mut next = 0;
		while next < stated.len() {
			// The lookup that ends at one of the hops from `next` on and covers all of them up to
			// it.
			let covering = (next..stated.len()).find_map(|at| {
				let answer = em.hop_lookup(stated[at])?;
				(answer.hops == at - next + 1).then_some((at, answer))
			});
			let Some((at, answer)) = covering else {
				break;
			};
			if !em.emitted_lookup(answer.lookup) {
				break;
			}
			chain.push(answer.lookup);
			next = at + 1;
		}
		if !chain.is_empty() {
			chains.push(chain.into_boxed_slice());
		}
	}
	Ok((!chains.is_empty()).then(|| FrontierRowsSpec {
		chains: chains.into_boxed_slice(),
		paging,
	}))
}

/// The tag of the physical op `op`.
fn phys_tag(ir: &Ir, op: OpId) -> Result<PhysTag, IrError> {
	match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Phys(kind)) => Ok(kind.tag()),
		Some(OpKind::Expr(_) | OpKind::Logical(_)) => {
			Err(invariant("a pipeline op outside the physical dialect"))
		}
		None => Err(IrError::Stale("ir_lower::pipe: op")),
	}
}

/// Whether the clause region `region` ends in a throw or a raise, which yields no value.
fn region_raises(ir: &Ir, region: RegionId) -> bool {
	ops_in(ir, region)
		.last()
		.and_then(|last| ir.op(last))
		.is_some_and(|data| matches!(data.kind, OpKind::Expr(ExprOp::Throw | ExprOp::Raise(_))))
}

/// The value the clause region `region` yields.
pub(super) fn yielded_value(ir: &Ir, region: RegionId) -> Result<ValueId, IrError> {
	let last = ops_in(ir, region).last().ok_or_else(|| invariant("a clause region with no op"))?;
	match ir.op(last).map(|data| (&data.kind, data.operands.as_slice())) {
		Some((OpKind::Expr(ExprOp::Yield), [yielded])) => Ok(*yielded),
		_ => Err(invariant("a clause region that does not yield one value")),
	}
}

/// Threads a predicate into jumps to the end of the row.
struct Branching<'l, 'a> {
	lowerer: &'l mut Lowerer<'a>,
	ir: &'l Ir,
	/// The label the row's end binds.
	end: Label,
	jumps: &'l mut EndJumps,
}

impl Branching<'_, '_> {
	/// Ends the row where the truthiness of `value` is false, and falls through otherwise.
	fn predicate(&mut self, value: ValueId) -> Result<(), IrError> {
		let end = self.end;
		self.branch(value, false, end)
	}

	/// Jumps to `target` when the truthiness of `value` is `when`, and falls through otherwise.
	///
	/// `AND` taken when false and `OR` taken when true jump from either side; `AND` taken when
	/// true and `OR` taken when false jump from the right side alone, the left side deciding the
	/// other way skipping it. `NOT` flips what is waited for.
	fn branch(&mut self, value: ValueId, when: bool, target: Label) -> Result<(), IrError> {
		let ir = self.ir;
		let defining = match ir.value(value).map(|data| data.def) {
			Some(crate::exec::ir::core::value::ValueDef::OpResult {
				op,
				index: 0,
			}) => ir.op(op),
			_ => None,
		};
		if let Some(data) = defining
			&& let OpKind::Expr(expr) = &data.kind
		{
			match (expr, data.operands.as_slice(), data.regions.as_slice()) {
				(
					ExprOp::ShortCircuit {
						op,
					},
					[left],
					[right],
				) if matches!(op, ShortCircuitOp::And | ShortCircuitOp::Or) => {
					let or = matches!(op, ShortCircuitOp::Or);
					return if or == when {
						self.branch(*left, when, target)?;
						self.region_branch(*right, when, target)
					} else {
						let decided = self.lowerer.emitter().label()?;
						self.branch(*left, !when, decided)?;
						self.region_branch(*right, when, target)?;
						self.lowerer.emitter().bind(decided)
					};
				}
				(ExprOp::Prefix(operator), [subject], [])
					if matches!(**operator, PrefixOperator::Not) =>
				{
					return self.branch(*subject, !when, target);
				}
				_ => {}
			}
		}
		let cond = self.lowerer.value(value)?;
		self.jump(cond, when, target, ir.uses(value).len() == 1)
	}

	/// As [`Self::branch`] over the value the lazy region `region` yields, lowered in a scope of
	/// its own.
	fn region_branch(
		&mut self,
		region: RegionId,
		when: bool,
		target: Label,
	) -> Result<(), IrError> {
		let yielded = yielded_value(self.ir, region)?;
		let cond = self.lowerer.region_value(region)?;
		self.jump(cond, when, target, self.ir.uses(yielded).len() == 1)
	}

	/// Jumps to `target` when the truthiness of `cond` is `when`.
	///
	/// A comparison that only this jump reads, and that was emitted last, is the jump: the typed
	/// or general comparison fused with the condition it feeds. `read_once` says nothing else
	/// reads the comparison's register.
	fn jump(
		&mut self,
		cond: Operand,
		when: bool,
		target: Label,
		read_once: bool,
	) -> Result<(), IrError> {
		let em = self.lowerer.emitter();
		if read_once && let Operand::Reg(reg) = cond {
			match em.code().last() {
				Some(Instr::CompareFieldInt {
					field,
					op,
					lit,
					reversed,
					fallback,
					dst,
				}) if *dst == reg => {
					let fused = Instr::JumpIfFieldInt {
						field: *field,
						op: *op,
						lit: *lit,
						reversed: *reversed,
						fallback: *fallback,
						truthy: when,
						target,
					};
					self.fuse(fused, target);
					return Ok(());
				}
				Some(Instr::Compare {
					op,
					left,
					right,
					dst,
				}) if *dst == reg => {
					let fused = Instr::JumpIfCompare {
						op: *op,
						left: *left,
						right: *right,
						truthy: when,
						target,
					};
					self.fuse(fused, target);
					return Ok(());
				}
				_ => {}
			}
		}
		let at = em.emit(Instr::JumpIf {
			cond,
			truthy: when,
			target,
		});
		self.aimed(at, target);
		Ok(())
	}

	/// Replaces the comparison emitted last with the jump `fused`, which is the same instruction
	/// position.
	fn fuse(&mut self, fused: Instr, target: Label) {
		let em = self.lowerer.emitter();
		let at = em.here() - 1;
		if let Some(last) = em.code_mut().last_mut() {
			*last = fused;
		}
		self.aimed(at, target);
	}

	/// Records that the conditional jump at `at` is aimed at `target`, which is a filter if
	/// `target` is the row's end.
	fn aimed(&mut self, at: Label, target: Label) {
		if target == self.end {
			self.jumps.filters.push(at);
		}
	}
}

/// Emits the row's end, resolving the jumps aimed at it.
///
/// A conditional jump to the row's end is a filter that ends the row, and the row's end after a
/// hand-off is the hand-off itself where nothing jumps to the end. Each form is the same
/// instruction the jump and the pair stand for, so no path runs anything it would not run apart.
fn end_row(em: &mut Emitter, end: Label, jumps: &EndJumps) -> Result<(), IrError> {
	for at in &jumps.filters {
		if let Some(instr) = em.code_mut().get_mut(*at as usize) {
			to_filter(instr);
		}
	}
	let at = em.here() as usize;
	if jumps.other == 0
		&& let Some(Instr::Emit {
			src,
		}) = at.checked_sub(1).and_then(|last| em.code().get(last))
	{
		let src = *src;
		// The hand-off, with the value loaded or the object built for it where nothing jumps to it.
		let fused = Instr::EmitEnd {
			src: Operand::Reg(src),
		};
		if let Some(last) = em.code_mut().last_mut() {
			*last = fused;
		}
		return Ok(());
	}
	em.bind(end)?;
	em.emit(Instr::EndRow);
	Ok(())
}

/// `instr`, a conditional jump to the row's end, as the filter that ends the row where it would
/// jump.
fn to_filter(instr: &mut Instr) {
	match *instr {
		Instr::JumpIfFieldInt {
			field,
			op,
			lit,
			reversed,
			fallback,
			truthy,
			..
		} => {
			*instr = Instr::FilterFieldInt {
				field,
				op,
				lit,
				reversed,
				fallback,
				reject: truthy,
			};
		}
		Instr::JumpIfCompare {
			op,
			left,
			right,
			truthy,
			..
		} => {
			*instr = Instr::FilterCompare {
				op,
				left,
				right,
				reject: truthy,
			};
		}
		Instr::JumpIf {
			cond,
			truthy,
			..
		} => {
			*instr = Instr::Filter {
				cond,
				reject: truthy,
			};
		}
		Instr::CoerceField {
			..
		}
		| Instr::BeginComputing {
			..
		}
		| Instr::LandRow {
			..
		}
		| Instr::BindRow {
			..
		}
		| Instr::JumpIfPermissionsBypassed {
			..
		}
		| Instr::CompareFieldInt {
			..
		}
		| Instr::Compare {
			..
		}
		| Instr::Arith {
			..
		}
		| Instr::Unary {
			..
		}
		| Instr::NewArray {
			..
		}
		| Instr::ArrayPush {
			..
		}
		| Instr::LoadParam {
			..
		}
		| Instr::LoadSlot {
			..
		}
		| Instr::SortKey {
			..
		}
		| Instr::Fetch {
			..
		}
		| Instr::Access {
			..
		}
		| Instr::Each {
			..
		}
		| Instr::Throw {
			..
		}
		| Instr::FilterBegin {
			..
		}
		| Instr::MapBegin {
			..
		}
		| Instr::CollectPush {
			..
		}
		| Instr::Nav {
			..
		}
		| Instr::CallAllowed {
			..
		}
		| Instr::Call {
			..
		}
		| Instr::FunctionEnter {
			..
		}
		| Instr::FunctionBind {
			..
		}
		| Instr::FunctionReturn {
			..
		}
		| Instr::FunctionCall {
			..
		}
		| Instr::ArrayArg {
			..
		}
		| Instr::ElemNext {
			..
		}
		| Instr::ClosureBind {
			..
		}
		| Instr::ClosureReturn {
			..
		}
		| Instr::Dive {
			..
		}
		| Instr::Subquery {
			..
		}
		| Instr::MakeClosure {
			..
		}
		| Instr::ClosureField {
			..
		}
		| Instr::RequireClosure {
			..
		}
		| Instr::Invoke {
			..
		}
		| Instr::Lookup {
			..
		}
		| Instr::Signal {
			..
		}
		| Instr::Extract {
			..
		}
		| Instr::KnnRank {
			..
		}
		| Instr::Measure {
			..
		}
		| Instr::Mock {
			..
		}
		| Instr::Recurse {
			..
		}
		| Instr::Destructure {
			..
		}
		| Instr::RecordId {
			..
		}
		| Instr::MethodResolve {
			..
		}
		| Instr::Method {
			..
		}
		| Instr::MethodClosure {
			..
		}
		| Instr::JumpIfNullish {
			..
		}
		| Instr::JumpIfNone {
			..
		}
		| Instr::Skip {
			..
		}
		| Instr::Take {
			..
		}
		| Instr::JumpIfLimitReached {
			..
		}
		| Instr::NewObject {
			..
		}
		| Instr::SeedObject {
			..
		}
		| Instr::BuildObject {
			..
		}
		| Instr::LoadPath {
			..
		}
		| Instr::LoadRow {
			..
		}
		| Instr::FetchRow {
			..
		}
		| Instr::RemoveField {
			..
		}
		| Instr::RemovePath {
			..
		}
		| Instr::Omit {
			..
		}
		| Instr::SetField {
			..
		}
		| Instr::WriteProjection {
			..
		}
		| Instr::Load {
			..
		}
		| Instr::Emit {
			..
		}
		| Instr::EmitEnd {
			..
		}
		| Instr::BuildObjectEmit {
			..
		}
		| Instr::FilterFieldInt {
			..
		}
		| Instr::FilterCompare {
			..
		}
		| Instr::Filter {
			..
		}
		| Instr::SplitPush {
			..
		}
		| Instr::SortPush {
			..
		}
		| Instr::Distinct {
			..
		}
		| Instr::JoinProbe {
			..
		}
		| Instr::VerdictJump {
			..
		}
		| Instr::VerdictSet {
			..
		}
		| Instr::JoinPair {
			..
		}
		| Instr::GroupKey {
			..
		}
		| Instr::GroupSelect {
			..
		}
		| Instr::AccUpdate {
			..
		}
		| Instr::Jump {
			..
		}
		| Instr::Next {
			..
		}
		| Instr::Write(_)
		| Instr::Store(_)
		| Instr::Block(_)
		| Instr::EndRow
		| Instr::StopPipeline
		| Instr::TryNone {
			..
		}
		| Instr::TryEnd
		| Instr::MethodRegion {
			..
		}
		| Instr::PermissionRegion {
			..
		}
		| Instr::GroupFirstNeeded {
			..
		}
		| Instr::GroupFirstSet {
			..
		}
		| Instr::CallIndexFn {
			..
		}
		| Instr::Matches {
			..
		}
		| Instr::ExternalEnter {
			..
		}
		| Instr::ExternalCall {
			..
		}
		| Instr::NewSet {
			..
		}
		| Instr::Degree {
			..
		}
		| Instr::RepeatRecurse {
			..
		}
		| Instr::JumpIfNotObject {
			..
		}
		| Instr::JumpIfOpaque {
			..
		}
		| Instr::InvalidControlFlow
		| Instr::JumpIfNotRecordId {
			..
		}
		| Instr::DocumentRoot {
			..
		}
		| Instr::JudgeCopy {
			..
		}
		| Instr::Ir(_) => {}
	}
}

#[cfg(test)]
pub(super) mod tests {
	use smallvec::SmallVec;
	use surrealdb_strand::Strand;

	use super::*;
	use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
	use crate::exec::ir::expr::ops::{KeyShape, RecordIdAttrs};
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::phys::ops::{
		Batch, OmissionShape, PipelineAttrs, PipelineId, ProjectedFieldShape, RecordScanAttrs,
		SortKeySpecShape, TableScanAttrs,
	};
	use crate::exec::ir::phys::placement::{NO_BACKEND_CAPS, Placement, install, place_all};
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::payload::{Spill, ValuesRead};
	use crate::expr::BinaryOperator;
	use crate::kvs::Direction;

	/// The rows of a scan of the table `t`.
	pub(in crate::exec::vm::ir_lower) fn scan(b: &mut Builder) -> ValueId {
		let op = b.create_kind(
			OpKind::Phys(PhysOp::TableScan(Box::new(TableScanAttrs {
				probes: None,
				bounds: None,
				prefilter: None,
				table: TableName::from("t"),
				direction: Direction::Forward,
				topk: None,
				lightweight: false,
				id_order: false,
				read: Default::default(),
			}))),
			&[],
			1,
		);
		b.result_of(op)
	}

	/// The rows of the streaming op `kind` over `input` and `operands`.
	pub(super) fn step(
		b: &mut Builder,
		kind: PhysOp,
		input: ValueId,
		operands: &[ValueId],
	) -> OpId {
		let mut all = vec![input];
		all.extend_from_slice(operands);
		b.create_kind(OpKind::Phys(kind), &all, 1)
	}

	/// A `phys.pipeline` of id `id` whose region holds the ops `chain` builds and yields the
	/// rows of the last one.
	pub(super) fn pipeline(
		b: &mut Builder,
		id: u32,
		chain: impl FnOnce(&mut Builder) -> ValueId,
	) -> OpId {
		let op = b.create_kind(
			OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
				id: PipelineId(id),
				edges: SmallVec::new(),
				logical: None,
			}))),
			&[],
			1,
		);
		b.open(op, RegionKind::Pipeline, &[]);
		let rows = chain(b);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[rows], 0);
		b.close();
		op
	}

	/// The container `b`, past the physical boundary and placed.
	pub(in crate::exec::vm::ir_lower) fn placed(mut b: Builder) -> Builder {
		b.physical();
		let mut facts = FactCache::new();
		let decided = place_all(
			&b.ir,
			&mut facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&Placement::default(),
			NO_BACKEND_CAPS,
		)
		.expect("the container is a DAG");
		install(b.ir.placement_table_mut(), decided);
		b
	}

	pub(super) fn lowered(b: &Builder) -> Lowered {
		match lower_reads(&mut LowerCx::detached(), &b.ir, false) {
			Ok(lowered) => lowered,
			Err(error) => panic!("the container lowers: {error}"),
		}
	}

	pub(super) fn refused(b: &Builder) -> &'static str {
		match lower_reads(&mut LowerCx::detached(), &b.ir, false) {
			Err(IrError::NotYet {
				what,
			}) => what,
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the container lowers"),
		}
	}

	/// The detail of the invariant break lowering `b` answers: a shape no producer builds, which
	/// the lowering reports as an op-local verification failure or as an invariant.
	pub(super) fn broken(b: &Builder) -> String {
		match lower_reads(&mut LowerCx::detached(), &b.ir, false) {
			Err(IrError::Invariant {
				what,
			}) => what.to_owned(),
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				detail,
				..
			})) => detail,
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the container lowers"),
		}
	}

	pub(super) fn param(b: &mut Builder, name: &str) -> ValueId {
		b.add(
			ExprOp::FreeParam {
				name: Strand::from(name),
			},
			&[],
		)
	}

	pub(super) fn where_clause(
		b: &mut Builder,
		filter: OpId,
		body: impl FnOnce(&mut Builder) -> ValueId,
	) {
		b.region(filter, RegionKind::Clause(ClauseRole::Where), body);
	}

	/// A scan filtered by the predicate `body` builds.
	fn filtered(body: impl FnOnce(&mut Builder) -> ValueId) -> Builder {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let filter = step(b, PhysOp::Filter, rows, &[]);
			where_clause(b, filter, body);
			b.result_of(filter)
		});
		placed(b)
	}

	pub(super) fn the_code(b: &Builder) -> Vec<Instr> {
		lowered(b).program.pipelines[0].code.clone()
	}

	#[test]
	fn a_scan_hands_each_row_on_and_ends_it_in_one_instruction() {
		let mut b = Builder::new();
		let op = pipeline(&mut b, 0, scan);
		let b = placed(b);
		let lowered = lowered(&b);
		let program = &lowered.program;
		assert_eq!(program.pipelines.len(), 1);
		let pipeline = &program.pipelines[0];
		assert!(matches!(
			&pipeline.source,
			Source::Scan {
				prefilter: None,
				topk: None,
				range: None,
				..
			}
		));
		assert_eq!(
			pipeline.code,
			vec![
				Instr::EmitEnd {
					src: Operand::Reg(ROW)
				},
				Instr::StopPipeline,
			]
		);
		assert!(pipeline.in_place, "a scan whose body the interpreter answers runs in place");
		assert_eq!(program.table, Some(TableName::from("t")));
		assert_eq!(program.required_context, ContextLevel::Database);
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
		assert!(!program.scalar && !program.rows);
		assert_eq!(lowered.provenance.pipelines.len(), 1);
		assert_eq!(lowered.provenance.origin(0).map(|(origin, _)| origin), Some(op));
	}

	#[test]
	fn the_placement_decision_is_copied_to_the_pipeline() {
		let mut b = Builder::new();
		let op = pipeline(&mut b, 0, scan);
		let b = placed(b);
		let expected = parallel_of(&b.ir, op, None).expect("the pipeline is placed");
		assert_eq!(lowered(&b).program.pipelines[0].parallel, expected);
	}

	#[test]
	fn a_limit_takes_the_rows_it_pages_and_stops_the_pipeline_when_spent() {
		let mut b = Builder::new();
		let op = pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let five = b.literal(Value::from(5_i64));
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[five],
			);
			let rows = b.result_of(limited);
			let all = step(b, PhysOp::Project(Box::new(ProjectionShape::All)), rows, &[]);
			b.result_of(all)
		});
		let b = placed(b);
		let lowered = lowered(&b);
		let program = &lowered.program;
		let pipeline = &program.pipelines[0];
		assert_eq!(
			pipeline.code,
			vec![
				Instr::Take {
					limit: 0
				},
				Instr::Emit {
					src: ROW
				},
				Instr::JumpIfLimitReached {
					limit: 0,
					done: 4
				},
				Instr::EndRow,
				Instr::StopPipeline,
			]
		);
		assert_eq!(pipeline.limits, vec![0]);
		assert_eq!(program.limits.len(), 1);
		assert!(matches!(
			&program.limits[0].limit,
			Some(HostScalar::Const(value)) if **value == Value::from(5_i64)
		));
		assert!(program.limits[0].start.is_none());
		assert_eq!(program.limits[0].group, None);
		// The one paging clause of the pipeline is the slot its paged decision replays.
		let expected = parallel_of(&b.ir, op, Some(0)).expect("the pipeline is placed");
		assert_eq!(pipeline.parallel, expected);
	}

	#[test]
	fn a_start_skips_to_the_end_of_the_row() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let two = b.literal(Value::from(2_i64));
			let paged = step(
				b,
				PhysOp::Limit {
					limit: None,
					start: Some(1),
				},
				rows,
				&[two],
			);
			b.result_of(paged)
		});
		let b = placed(b);
		assert_eq!(
			the_code(&b),
			vec![
				Instr::Skip {
					limit: 0,
					target: 2
				},
				Instr::Emit {
					src: ROW
				},
				Instr::EndRow,
				Instr::StopPipeline,
			]
		);
	}

	#[test]
	fn a_paging_clause_that_is_not_a_literal_is_evaluated_by_host_code() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let n = param(b, "n");
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[n],
			);
			b.result_of(limited)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		assert!(
			matches!(program.limits[0].limit, Some(HostScalar::Code(0))),
			"{:?}",
			program.limits
		);
		assert_eq!(program.host_code.len(), 1);
		assert!(
			program.host_code[0].code.iter().any(|instr| matches!(instr, Instr::LoadParam { .. })),
			"{:?}",
			program.host_code[0].code
		);
	}

	#[test]
	fn a_value_no_statement_clause_reads_is_refused() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, scan);
		param(&mut b, "n");
		let b = placed(b);
		assert_eq!(refused(&b), "ir_lower::pipe: an expression outside every pipeline");
	}

	#[test]
	fn only_and_a_version_wrap_the_statements_rows_as_its_tail() {
		let mut b = Builder::new();
		let version = param(&mut b, "at");
		let op = pipeline(&mut b, 0, scan);
		let rows = b.result_of(op);
		let only = b.create_kind(OpKind::Phys(PhysOp::Only), &[rows], 1);
		let only_rows = b.result_of(only);
		b.create_kind(OpKind::Phys(PhysOp::VersionScope), &[only_rows, version], 1);
		let b = placed(b);
		let program = lowered(&b).program;
		let tail = program.tail.expect("the wrappers are the program's tail");
		assert!(tail.only);
		assert!(tail.timeout.is_none());
		assert!(matches!(tail.version, Some(HostScalar::Code(0))), "{:?}", tail.version);

		let mut b = Builder::new();
		pipeline(&mut b, 0, scan);
		let b = placed(b);
		assert!(lowered(&b).program.tail.is_none());
	}

	/// The container `b` with a `phys.timeout` over `duration` whose `Deadline` region holds the
	/// pipeline of a scan and yields its rows.
	fn timed(duration: ValueId, mut b: Builder) -> Builder {
		let timeout = b.create_kind(OpKind::Phys(PhysOp::Timeout), &[duration], 1);
		b.open(timeout, RegionKind::Deadline, &[]);
		let op = pipeline(&mut b, 0, scan);
		let rows = b.result_of(op);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[rows], 0);
		b.close();
		b
	}

	#[test]
	fn a_timeout_holds_the_pipelines_of_its_body_and_is_the_tail_s_timeout() {
		let mut b = Builder::new();
		let duration = b.literal(Value::from(1_i64));
		let b = placed(timed(duration, b));
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 1);
		let tail = program.tail.expect("the timeout is the program's tail");
		assert!(matches!(tail.timeout, Some(HostScalar::Const(_))), "{:?}", tail.timeout);
		assert!(tail.version.is_none() && !tail.only);
	}

	#[test]
	fn a_timeout_with_a_version_and_only_states_the_version_first() {
		let mut b = Builder::new();
		let duration = param(&mut b, "limit");
		let version = param(&mut b, "at");
		let mut b = timed(duration, b);
		let timeout = ops_in(&b.ir, b.ir.top())
			.find(|op| {
				matches!(b.ir.op(*op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Timeout)))
			})
			.expect("the timeout stands in the top region");
		let timed_rows = b.result_of(timeout);
		let only = b.create_kind(OpKind::Phys(PhysOp::Only), &[timed_rows], 1);
		let only_rows = b.result_of(only);
		b.create_kind(OpKind::Phys(PhysOp::VersionScope), &[only_rows, version], 1);
		let b = placed(b);
		let program = lowered(&b).program;
		let tail = program.tail.expect("the wrappers are the program's tail");
		assert!(tail.only);
		// Each clause is a host pipeline of its own, the version's the first.
		assert!(matches!(tail.version, Some(HostScalar::Code(0))), "{:?}", tail.version);
		assert!(matches!(tail.timeout, Some(HostScalar::Code(1))), "{:?}", tail.timeout);
	}

	#[test]
	fn a_second_timeout_is_an_invariant_break() {
		let mut b = Builder::new();
		let duration = b.literal(Value::from(1_i64));
		let mut b = timed(duration, b);
		let again = b.literal(Value::from(2_i64));
		let outer = b.create_kind(OpKind::Phys(PhysOp::Timeout), &[again], 1);
		b.open(outer, RegionKind::Deadline, &[]);
		let timeout = ops_in(&b.ir, b.ir.top())
			.find(|op| {
				matches!(b.ir.op(*op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Timeout)))
			})
			.expect("the first timeout stands in the top region");
		let rows = b.result_of(timeout);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[rows], 0);
		b.close();
		let b = placed(b);
		assert_eq!(broken(&b), "more than one timeout over a statement's rows");
	}

	#[test]
	fn a_second_version_scope_is_an_invariant_break() {
		let mut b = Builder::new();
		let first = param(&mut b, "at");
		let second = param(&mut b, "again");
		let op = pipeline(&mut b, 0, scan);
		let rows = b.result_of(op);
		let inner = b.create_kind(OpKind::Phys(PhysOp::VersionScope), &[rows, first], 1);
		let inner_rows = b.result_of(inner);
		b.create_kind(OpKind::Phys(PhysOp::VersionScope), &[inner_rows, second], 1);
		let b = placed(b);
		assert_eq!(broken(&b), "more than one version scope over a statement's rows");
	}

	/// `$xs[WHERE true]` as the physical phase leaves it: an element filter wrapped in the pipeline
	/// of id `id`. Answers the pipeline's value.
	fn value_pipeline(b: &mut Builder, id: u32) -> ValueId {
		let xs = param(b, "xs");
		let op = pipeline(b, id, |b| {
			let looped = b.create_kind(
				OpKind::Phys(PhysOp::Loop {
					kind: LoopKind::ElementFilter,
					first: false,
					batch: Batch {
						max_iterations: 1,
						max_bytes: 1,
					},
					unroll: 1,
					value: false,
					step: None,
				}),
				&[xs],
				1,
			);
			b.open(looped, RegionKind::Loop, &[TypeKnowledge::Unknown, TypeKnowledge::Unknown]);
			let yes = b.literal(Value::Bool(true));
			let truth = b.add(ExprOp::Truthy, &[yes]);
			b.create_kind(OpKind::Phys(PhysOp::Yield), &[truth], 0);
			b.close();
			b.result_of(looped)
		});
		b.result_of(op)
	}

	#[test]
	fn a_value_loop_a_paging_clause_reads_is_evaluated_by_the_clause_s_host_code() {
		let mut b = Builder::new();
		let n = value_pipeline(&mut b, 1);
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[n],
			);
			b.result_of(limited)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 1);
		assert!(
			matches!(program.limits[0].limit, Some(HostScalar::Code(0))),
			"{:?}",
			program.limits
		);
		assert!(
			program.host_code[0]
				.code
				.iter()
				.any(|instr| matches!(instr, Instr::FilterBegin { .. })),
			"{:?}",
			program.host_code[0].code
		);
	}

	#[test]
	fn a_paging_operand_a_pipeline_holds_the_limit_of_is_read_by_the_limit() {
		// The physical phase moves a select loop's limit into its pipeline and leaves the operand
		// the limit reads in the statement's region.
		let mut b = Builder::new();
		let n = param(&mut b, "n");
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[n],
			);
			b.result_of(limited)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		assert!(
			matches!(program.limits[0].limit, Some(HostScalar::Code(0))),
			"{:?}",
			program.limits
		);
	}

	#[test]
	fn a_value_loop_a_timeout_reads_is_the_tail_s_host_code_beside_the_timed_body() {
		let mut b = Builder::new();
		let duration = value_pipeline(&mut b, 1);
		let b = placed(timed(duration, b));
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 1);
		let tail = program.tail.expect("the timeout is the program's tail");
		assert!(matches!(tail.timeout, Some(HostScalar::Code(0))), "{:?}", tail.timeout);
	}

	#[test]
	fn a_pipeline_of_rows_beside_a_timed_body_is_an_invariant_break() {
		let mut b = Builder::new();
		let duration = b.literal(Value::from(1_i64));
		let mut b = timed(duration, b);
		pipeline(&mut b, 1, scan);
		let b = placed(b);
		assert_eq!(broken(&b), "a pipeline of rows beside a timed body");
	}

	#[test]
	fn rows_an_expression_reads_are_an_invariant_break() {
		let mut b = Builder::new();
		let read = pipeline(&mut b, 0, scan);
		let rows = b.result_of(read);
		let n = b.add(ExprOp::Truthy, &[rows]);
		pipeline(&mut b, 1, |b| {
			let rows = scan(b);
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[n],
			);
			b.result_of(limited)
		});
		let b = placed(b);
		assert_eq!(broken(&b), "rows read by an expression");
	}

	#[test]
	fn a_breaker_over_a_wrapper_is_an_invariant_break() {
		let mut b = Builder::new();
		let op = pipeline(&mut b, 0, scan);
		let rows = b.result_of(op);
		let only = b.create_kind(OpKind::Phys(PhysOp::Only), &[rows], 1);
		let only_rows = b.result_of(only);
		sort(&mut b, only_rows, &["a"]);
		let b = placed(b);
		assert_eq!(broken(&b), "rows a breaker or a step reads through a wrapper");
	}

	#[test]
	fn a_breaker_read_by_two_pipelines_is_an_invariant_break() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		let sorted = sort(&mut b, fed, &["a"]);
		let rows = b.result_of(sorted);
		select_loop(&mut b, 1, rows, |_, element| element);
		select_loop(&mut b, 2, rows, |_, element| element);
		let b = placed(b);
		assert_eq!(broken(&b), "a breaker read more than once");
	}

	#[test]
	fn rows_no_source_breaker_or_step_makes_are_an_invariant_break() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let made = b.create_kind(OpKind::Phys(PhysOp::Block), &[], 1);
			b.result_of(made)
		});
		let b = placed(b);
		assert_eq!(broken(&b), "rows a pipeline opens that no source, breaker or step makes");
	}

	#[test]
	fn a_logical_op_in_a_statement_region_is_an_invariant_break() {
		use crate::exec::ir::logical::ops::LogOp;
		let mut b = Builder::new();
		pipeline(&mut b, 0, scan);
		let mut b = placed(b);
		b.create_kind(OpKind::Logical(LogOp::Eval), &[], 1);
		assert_eq!(broken(&b), "a logical operation in a physical container");
	}

	#[test]
	fn a_breaker_beneath_the_last_op_of_a_pipeline_is_an_invariant_break() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let sorted = sort(b, rows, &["a"]);
			let sorted_rows = b.result_of(sorted);
			let distinct = step(
				b,
				PhysOp::Distinct {
					spill: Spill::Bounded,
				},
				sorted_rows,
				&[],
			);
			b.result_of(distinct)
		});
		let b = placed(b);
		assert_eq!(broken(&b), "a pipeline holding a breaker beneath its last op");
	}

	/// Every op `streams` admits is a step [`Body::step`] has an arm for, so its fallback is never
	/// reached: the step tags [`non_step`] names are exactly the tags of the streaming ops.
	#[test]
	fn every_op_that_streams_is_a_step_the_body_lowers() {
		use crate::exec::ir::logical::ops::FieldsShape;
		use crate::exec::ir::phys::ops::LayerAttrs;
		let streaming = [
			PhysOp::Enforced {
				table: TableName::from("t"),
			},
			PhysOp::Filter,
			PhysOp::Fetch,
			PhysOp::Layer(Box::new(LayerAttrs {
				fields: FieldsShape::Value {
					alias: None,
				},
				names: Vec::new(),
				computed: Vec::new(),
				judged: None,
			})),
			PhysOp::Limit {
				limit: None,
				start: None,
			},
			PhysOp::Project(Box::new(ProjectionShape::Value)),
			PhysOp::Distinct {
				spill: Spill::Bounded,
			},
			PhysOp::MatchStep(Box::new(MatchStepAttrs::Bind(BindStepShape::Bind {
				name: "row".to_owned(),
			}))),
		];
		let step = non_step(PhysTag::Filter);
		for op in &streaming {
			assert!(streams(op), "{:?} streams", op.tag());
			assert_eq!(non_step(op.tag()), step, "{:?} is a step", op.tag());
		}
		let steps: Vec<PhysTag> =
			PhysTag::ALL.iter().copied().filter(|tag| non_step(*tag) == step).collect();
		for tag in &steps {
			assert!(
				*tag == PhysTag::PermissionFilter || streaming.iter().any(|op| op.tag() == *tag),
				"{tag:?} is named a step but no op of it streams"
			);
		}
		assert_eq!(steps.len(), streaming.len() + 1);
	}

	/// A scan of the table `t` whose rows declare the field `name` of kind `kind`.
	pub(super) fn scan_declaring(b: &mut Builder, name: &str, kind: crate::expr::Kind) -> ValueId {
		use crate::expr::logical::{FieldPath as DeclaredPath, FieldSchema, LogicalPermission};
		let schema = Schema::closed(vec![FieldSchema {
			path: DeclaredPath::field(name),
			kind,
			checked: false,
			flexible: false,
			computed: None,
			select_permission: LogicalPermission::Full,
		}]);
		let op = b.create_kind(
			OpKind::Phys(PhysOp::TableScan(Box::new(TableScanAttrs {
				probes: None,
				bounds: None,
				prefilter: None,
				table: TableName::from("t"),
				direction: Direction::Forward,
				topk: None,
				lightweight: false,
				id_order: false,
				read: crate::exec::ir::phys::ops::ReadClauses {
					schema: Some(Arc::new(schema)),
					with: None,
				},
			}))),
			&[],
			1,
		);
		b.result_of(op)
	}

	/// A key-range scan of the table `t` over its whole key, whose rows declare the field `name`
	/// of kind `kind`.
	pub(super) fn key_range_declaring(
		b: &mut Builder,
		name: &str,
		kind: crate::expr::Kind,
	) -> ValueId {
		use crate::exec::index::key_range::DeliveredKeyOrder;
		use crate::exec::ir::phys::ops::{
			KeyRangeScanAttrs, KeyRangeShape, ReadClauses, key_range_fixtures,
		};
		use crate::expr::logical::{FieldPath as DeclaredPath, FieldSchema, LogicalPermission};
		let schema = Schema::closed(vec![FieldSchema {
			path: DeclaredPath::field(name),
			kind,
			checked: false,
			flexible: false,
			computed: None,
			select_permission: LogicalPermission::Full,
		}]);
		let op = b.create_kind(
			OpKind::Phys(PhysOp::KeyRangeScan(Box::new(KeyRangeScanAttrs {
				prefilter: None,
				table: TableName::from("t"),
				read: ReadClauses {
					schema: Some(Arc::new(schema)),
					with: None,
				},
				access: KeyRangeShape {
					key: key_range_fixtures::pair(),
					eq: Vec::new(),
					range: None,
					direction: Direction::Forward,
					delivers: DeliveredKeyOrder::None,
					ordered_by: Vec::new(),
				},
				topk: None,
			}))),
			&[],
			1,
		);
		b.result_of(op)
	}

	#[test]
	fn a_field_against_an_integer_is_the_typed_form_where_its_declared_kind_admits_one() {
		use crate::expr::Kind;
		for (kind, typed) in [(Kind::Int, true), (Kind::Any, true), (Kind::String, false)] {
			let mut b = Builder::new();
			pipeline(&mut b, 0, |b| {
				let rows = scan_declaring(b, "a", kind.clone());
				let filter = step(b, PhysOp::Filter, rows, &[]);
				row_region(b, filter, RegionKind::Clause(ClauseRole::Where), |b, row| {
					let a = b.add(
						ExprOp::Field {
							name: Strand::from("a"),
						},
						&[row],
					);
					let one = b.literal(Value::from(1_i64));
					b.binary(BinaryOperator::MoreThan, a, one)
				});
				b.result_of(filter)
			});
			let b = placed(b);
			let code = the_code(&b);
			assert_eq!(
				matches!(code[0], Instr::FilterFieldInt { .. }),
				typed,
				"{kind:?}: {code:?}"
			);
			if !typed {
				assert!(matches!(code[0], Instr::FilterCompare { .. }), "{kind:?}: {code:?}");
			}
		}
	}

	#[test]
	fn an_integer_on_the_left_swaps_the_typed_form_and_an_undeclared_field_admits_one() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let filter = step(b, PhysOp::Filter, rows, &[]);
			row_region(b, filter, RegionKind::Clause(ClauseRole::Where), |b, row| {
				let a = b.add(
					ExprOp::Field {
						name: Strand::from("a"),
					},
					&[row],
				);
				let one = b.literal(Value::from(1_i64));
				b.binary(BinaryOperator::LessThan, one, a)
			});
			b.result_of(filter)
		});
		let b = placed(b);
		let code = the_code(&b);
		assert!(
			matches!(
				code[0],
				Instr::FilterFieldInt {
					reversed: true,
					..
				}
			),
			"{code:?}"
		);
	}

	/// A vector measure computes the builtin it stands for, so a call of a name the registry binds
	/// to another function lowers as that call, which resolves and invokes the function bound.
	#[test]
	fn a_vector_builtin_the_registry_binds_elsewhere_is_called_not_measured() {
		use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
		use crate::exec::ir::iface::Fallibility;
		use crate::expr::logical::EffectSet;

		#[derive(Debug)]
		struct Dot;

		impl ScalarFunction for Dot {
			fn name(&self) -> &'static str {
				"vector::dot"
			}

			fn signature(&self) -> Signature {
				Signature::new()
			}

			fn effects(&self) -> EffectSet {
				EffectSet::PURE
			}

			fn fallibility(&self) -> Fallibility {
				Fallibility::VALUE
			}
		}

		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let filter = step(b, PhysOp::Filter, rows, &[]);
			row_region(b, filter, RegionKind::Clause(ClauseRole::Where), |b, row| {
				let v = b.add(
					ExprOp::Field {
						name: Strand::from("v"),
					},
					&[row],
				);
				let query =
					b.literal(Value::Array(vec![Value::from(1_i64), Value::from(2_i64)].into()));
				let dot = b.call("vector::dot", &[v, query]);
				let zero = b.literal(Value::from(0_i64));
				b.binary(BinaryOperator::MoreThan, dot, zero)
			});
			b.result_of(filter)
		});
		let b = placed(b);
		let code = |registry: &FunctionRegistry| match lower_reads(
			&mut LowerCx::detached_over(registry),
			&b.ir,
			false,
		) {
			Ok(lowered) => lowered.program.pipelines[0].code.clone(),
			Err(error) => panic!("the container lowers: {error}"),
		};
		let measured =
			|code: &[Instr]| code.iter().any(|instr| matches!(instr, Instr::Measure { .. }));
		let called = |code: &[Instr]| code.iter().any(|instr| matches!(instr, Instr::Call { .. }));

		let builtin = code(crate::exec::ir::analysis::cx::builtin_registry());
		assert!(measured(&builtin) && !called(&builtin), "the builtin is measured: {builtin:?}");

		let mut registry = FunctionRegistry::with_builtins();
		registry.register(&Dot);
		let bound = code(&registry);
		assert!(!measured(&bound) && called(&bound), "the function bound is called: {bound:?}");
	}

	#[test]
	fn a_distinct_ends_the_row_it_has_seen_and_keeps_the_row_end_a_jump_target() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let distinct = step(
				b,
				PhysOp::Distinct {
					spill: Spill::Bounded,
				},
				rows,
				&[],
			);
			b.result_of(distinct)
		});
		let b = placed(b);
		let lowered = lowered(&b);
		assert_eq!(
			lowered.program.pipelines[0].code,
			vec![
				Instr::Distinct {
					distinct: 0,
					src: ROW,
					target: 2
				},
				Instr::Emit {
					src: ROW
				},
				Instr::EndRow,
				Instr::StopPipeline,
			]
		);
		assert_eq!(lowered.program.distincts, vec![None]);
	}

	#[test]
	fn enforcement_marks_the_pipeline_and_keeps_it_off_the_stored_bytes() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let enforced = step(
				b,
				PhysOp::Enforced {
					table: TableName::from("t"),
				},
				rows,
				&[],
			);
			b.result_of(enforced)
		});
		let b = placed(b);
		let lowered = lowered(&b);
		let pipeline = &lowered.program.pipelines[0];
		assert!(matches!(
			&pipeline.enforce,
			Some(Enforce::Rows { table, .. }) if table.as_str() == "t"
		));
		assert!(!pipeline.in_place);
	}

	#[test]
	fn a_filter_that_ends_the_row_is_a_filter_instruction() {
		let b = filtered(|b| param(b, "a"));
		assert_eq!(
			the_code(&b),
			vec![
				Instr::LoadParam {
					param: 0,
					dst: ROW + 1,
					frame: None
				},
				Instr::Filter {
					cond: Operand::Reg(ROW + 1),
					reject: false
				},
				Instr::EmitEnd {
					src: Operand::Reg(ROW)
				},
				Instr::StopPipeline,
			]
		);
	}

	#[test]
	fn a_comparison_only_the_filter_reads_is_the_filter() {
		let b = filtered(|b| {
			let a = param(b, "a");
			let one = b.literal(Value::from(1_i64));
			b.binary(BinaryOperator::MoreThan, a, one)
		});
		let code = the_code(&b);
		assert!(
			matches!(
				code[1],
				Instr::FilterCompare {
					reject: false,
					..
				}
			),
			"{code:?}"
		);
		assert!(!code.iter().any(|instr| matches!(instr, Instr::Compare { .. })), "{code:?}");
	}

	#[test]
	fn and_is_threaded_into_one_filter_per_side() {
		let b = filtered(|b| {
			let a = param(b, "a");
			b.short_circuit(ShortCircuitOp::And, a, |b| param(b, "b"))
		});
		assert_eq!(
			the_code(&b),
			vec![
				Instr::LoadParam {
					param: 0,
					dst: ROW + 1,
					frame: None
				},
				Instr::Filter {
					cond: Operand::Reg(ROW + 1),
					reject: false
				},
				Instr::LoadParam {
					param: 1,
					dst: ROW + 2,
					frame: None
				},
				Instr::Filter {
					cond: Operand::Reg(ROW + 2),
					reject: false
				},
				Instr::EmitEnd {
					src: Operand::Reg(ROW)
				},
				Instr::StopPipeline,
			]
		);
	}

	#[test]
	fn or_is_threaded_through_the_side_that_decides() {
		// `a OR b` ends the row when both are falsy: the left side deciding true skips the right.
		let b = filtered(|b| {
			let a = param(b, "a");
			b.short_circuit(ShortCircuitOp::Or, a, |b| param(b, "b"))
		});
		let code = the_code(&b);
		assert!(
			matches!(
				code[1],
				Instr::JumpIf {
					truthy: true,
					target: 4,
					..
				}
			),
			"{code:?}"
		);
		assert!(
			matches!(
				code[3],
				Instr::Filter {
					reject: false,
					..
				}
			),
			"{code:?}"
		);
		assert!(matches!(code[4], Instr::EmitEnd { .. }), "{code:?}");
	}

	#[test]
	fn not_waits_for_the_other_truthiness() {
		let b = filtered(|b| {
			let a = param(b, "a");
			b.add(ExprOp::Prefix(Box::new(PrefixOperator::Not)), &[a])
		});
		let code = the_code(&b);
		assert!(
			matches!(
				code[1],
				Instr::Filter {
					reject: true,
					..
				}
			),
			"{code:?}"
		);
	}

	/// A split of the rows `rows` on the field `a`, with its clause region.
	fn split_on_a(b: &mut Builder, rows: ValueId) -> ValueId {
		let split = step(
			b,
			PhysOp::Split {
				targets: Box::new([SplitTarget::At(field_path(&["a"]))]),
			},
			rows,
			&[],
		);
		b.region(split, RegionKind::Clause(ClauseRole::Split), |b| param(b, "a"));
		b.result_of(split)
	}

	#[test]
	fn a_split_reads_the_rows_of_a_pipeline_and_is_the_source_of_the_next() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let rows = b.result_of(feeder);
		pipeline(&mut b, 1, |b| split_on_a(b, rows));
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 2);
		assert!(
			program.pipelines[0].code.iter().any(|instr| matches!(
				instr,
				Instr::SplitPush {
					split: 0,
					src: ROW
				}
			)),
			"{:?}",
			program.pipelines[0].code
		);
		assert!(matches!(
			program.pipelines[1].source,
			Source::Split {
				split: 0
			}
		));
		assert_eq!(program.splits.len(), 1);
		assert_eq!(program.splits[0].pipeline, 1);
		assert_eq!(
			program.splits[0].kind,
			SplitKind::Targets(vec![SplitTarget::At(field_path(&["a"]))])
		);
	}

	#[test]
	fn a_split_in_a_select_loop_body_cuts_the_pipeline_in_two() {
		let mut b = Builder::new();
		let two = b.literal(Value::from(2_i64));
		let rows = scan(&mut b);
		select_loop(&mut b, 0, rows, |b, element| {
			let filter = step(b, PhysOp::Filter, element, &[]);
			where_clause(b, filter, |b| param(b, "a"));
			let filtered = b.result_of(filter);
			let split = split_on_a(b, filtered);
			let paged = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				split,
				&[two],
			);
			b.result_of(paged)
		});
		let b = placed(b);
		let lowered = lowered(&b);
		let program = lowered.program;
		assert_eq!(program.pipelines.len(), 2);
		// The filter runs before the split, and the paging over the split's rows.
		let before = &program.pipelines[0].code;
		assert!(before.iter().any(|instr| matches!(
			instr,
			Instr::SplitPush {
				split: 0,
				..
			}
		)));
		assert!(!before.iter().any(|instr| matches!(instr, Instr::JumpIfLimitReached { .. })));
		assert!(matches!(
			program.pipelines[1].source,
			Source::Split {
				split: 0
			}
		));
		assert_eq!(program.splits[0].pipeline, 1);
		assert_eq!(lowered.provenance.pipelines.len(), 2);
	}

	/// A select loop over `rows` whose body splits on `a` where `split` is true, paged by a limit
	/// of two that follows its pipeline: the shape of `SELECT ... SPLIT a LIMIT 2`.
	fn paged_select(b: &mut Builder, id: u32, rows: ValueId, split: bool) -> ValueId {
		let two = b.literal(Value::from(2_i64));
		let looped = select_loop(b, id, rows, |b, element| match split {
			true => split_on_a(b, element),
			false => element,
		});
		let looped = b.result_of(looped);
		let paged = step(
			b,
			PhysOp::Limit {
				limit: Some(1),
				start: None,
			},
			looped,
			&[two],
		);
		b.result_of(paged)
	}

	#[test]
	fn a_pipeline_continued_through_rows_a_second_op_reads_is_an_invariant_break() {
		// The select loop of pipeline 1 reads pipeline 0 through a paging that the region's yield
		// does not see, because the yield reads pipeline 0's rows as well.
		let mut b = Builder::new();
		let rows = scan(&mut b);
		let first = select_loop(&mut b, 0, rows, |_, element| element);
		let first_rows = b.result_of(first);
		let paged = paged_after(&mut b, first_rows);
		let continued = select_loop(&mut b, 1, paged, |_, element| element);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[first_rows], 0);
		let b = placed(b);
		let mut em = Emitter::new();
		let mut facts = FactCache::new();
		let mut reads = Reads::empty(PoolBase::default());
		let answer = lower_part(
			&mut em,
			&mut LowerCx::detached(),
			&b.ir,
			&mut facts,
			continued,
			&[],
			&Sink::Output,
			&mut reads,
			None,
		);
		assert!(
			matches!(
				&answer,
				Err(IrError::Verify(VerifyError { rule: VerifyRule::PerOp, detail, .. }))
					if detail == "a pipeline read through ops another op also reads"
			),
			"{answer:?}"
		);
	}

	/// The rows of a paging of two over `rows`.
	fn paged_after(b: &mut Builder, rows: ValueId) -> ValueId {
		let two = b.literal(Value::from(2_i64));
		let paged = step(
			b,
			PhysOp::Limit {
				limit: Some(1),
				start: None,
			},
			rows,
			&[two],
		);
		b.result_of(paged)
	}

	#[test]
	fn a_split_in_a_pipeline_another_continues_cuts_the_body_they_share() {
		// `SELECT * FROM t SPLIT a LIMIT 2`: the projection's select loop continues the split's
		// pipeline through the paging.
		let mut b = Builder::new();
		let rows = scan(&mut b);
		let paged = paged_select(&mut b, 0, rows, true);
		select_loop(&mut b, 1, paged, |_, element| element);
		let b = placed(b);
		let lowered = lowered(&b);
		let program = lowered.program;
		assert_eq!(program.pipelines.len(), 2);
		let before = &program.pipelines[0].code;
		assert!(before.iter().any(|instr| matches!(
			instr,
			Instr::SplitPush {
				split: 0,
				..
			}
		)));
		// The paging runs over the split's rows, in the part the split is the source of.
		assert!(!before.iter().any(|instr| matches!(instr, Instr::Take { .. })), "{before:?}");
		assert!(matches!(
			program.pipelines[1].source,
			Source::Split {
				split: 0
			}
		));
		assert!(
			program.pipelines[1].code.iter().any(|instr| matches!(instr, Instr::Take { .. })),
			"{:?}",
			program.pipelines[1].code
		);
		assert_eq!(program.splits[0].pipeline, 1);
		assert!(program.pipelines.iter().all(|pipeline| pipeline.parallel == Parallel::Serial));
		assert_eq!(lowered.provenance.pipelines.len(), 2);
	}

	#[test]
	fn a_yield_of_the_rows_in_the_top_region_delivers_the_output() {
		// The top region of a table's read unit ends in the yield of the statement's rows.
		let mut b = Builder::new();
		let op = pipeline(&mut b, 0, scan);
		let rows = b.result_of(op);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[rows], 0);
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 1);
		assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
	}

	#[test]
	fn a_split_in_a_continuing_stage_cuts_after_the_stages_before_it() {
		let mut b = Builder::new();
		let rows = scan(&mut b);
		let paged = paged_select(&mut b, 0, rows, false);
		paged_select(&mut b, 1, paged, true);
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 2);
		// The first stage's paging runs before the split, the second stage's after it.
		for pipeline in &program.pipelines {
			assert_eq!(
				pipeline.code.iter().filter(|instr| matches!(instr, Instr::Take { .. })).count(),
				1,
				"{:?}",
				pipeline.code
			);
		}
		assert!(program.pipelines[0].code.iter().any(|instr| matches!(
			instr,
			Instr::SplitPush {
				split: 0,
				..
			}
		)));
		assert!(matches!(
			program.pipelines[1].source,
			Source::Split {
				split: 0
			}
		));
	}

	#[test]
	fn a_streaming_op_that_follows_a_pipeline_runs_in_its_body() {
		let mut b = Builder::new();
		let five = b.literal(Value::from(5_i64));
		let op = pipeline(&mut b, 0, scan);
		let rows = b.result_of(op);
		step(
			&mut b,
			PhysOp::Limit {
				limit: Some(1),
				start: None,
			},
			rows,
			&[five],
		);
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 1);
		assert_eq!(program.limits.len(), 1);
		assert!(
			program.pipelines[0].code.iter().any(|instr| matches!(instr, Instr::Take { .. })),
			"{:?}",
			program.pipelines[0].code
		);
	}

	#[test]
	fn a_sort_reads_the_rows_of_a_pipeline_through_the_streaming_ops_that_follow_it() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let rows = b.result_of(feeder);
		let distinct = step(
			&mut b,
			PhysOp::Distinct {
				spill: Spill::Bounded,
			},
			rows,
			&[],
		);
		let distinct_rows = b.result_of(distinct);
		pipeline(&mut b, 1, |b| {
			let sorted = b.create_kind(
				OpKind::Phys(PhysOp::Sort(Box::new(SortAttrs {
					keys: vec![SortKeySpecShape {
						read: SortKeyReadShape::Path(field_path(&["a"])),
						ascending: true,
						collate: false,
						numeric: false,
					}],
					keep: None,
					pushdown: false,
					random: false,
					spill: Spill::Bounded,
					tempfiles: false,
				}))),
				&[distinct_rows],
				1,
			);
			b.result_of(sorted)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 2);
		assert!(
			program.pipelines[0].code.iter().any(|instr| matches!(instr, Instr::Distinct { .. })),
			"{:?}",
			program.pipelines[0].code
		);
		assert!(matches!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		));
	}

	/// A pipeline over a scan, through a distinct set, feeding a sort that a second pipeline reads
	/// and pages with `LIMIT 5`.
	fn sorted_and_paged() -> Builder {
		let mut b = Builder::new();
		let five = b.literal(Value::from(5_i64));
		let feeder = pipeline(&mut b, 0, scan);
		let rows = b.result_of(feeder);
		let distinct = step(
			&mut b,
			PhysOp::Distinct {
				spill: Spill::Bounded,
			},
			rows,
			&[],
		);
		let distinct_rows = b.result_of(distinct);
		let reader = pipeline(&mut b, 1, |b| {
			let sorted = b.create_kind(
				OpKind::Phys(PhysOp::Sort(Box::new(SortAttrs {
					keys: vec![SortKeySpecShape {
						read: SortKeyReadShape::Path(field_path(&["a"])),
						ascending: true,
						collate: false,
						numeric: false,
					}],
					keep: None,
					pushdown: false,
					random: false,
					spill: Spill::Spillable,
					tempfiles: false,
				}))),
				&[distinct_rows],
				1,
			);
			b.result_of(sorted)
		});
		let read = b.result_of(reader);
		step(
			&mut b,
			PhysOp::Limit {
				limit: Some(1),
				start: None,
			},
			read,
			&[five],
		);
		placed(b)
	}

	/// A region lowered as a block statement's group numbers its limits, distinct sets and sorts
	/// after those of the code lowered before it, and stamps its pipelines, paging and distinct
	/// sets with the group; the program's own code stamps none.
	#[test]
	fn a_group_numbers_its_pools_from_its_base_and_stamps_its_group() {
		let b = sorted_and_paged();
		let mut em = Emitter::new();
		let mut cx = LowerCx::detached();
		let mut facts = FactCache::new();
		let own = lower_pipelines(&mut em, &mut cx, &b.ir, &mut facts, b.ir.top())
			.expect("the program's own pipelines lower");
		assert_eq!(own.distincts, vec![None]);
		assert!(own.limits.iter().all(|limit| limit.group.is_none()));
		assert!(own.pipelines.iter().all(|pipeline| pipeline.group.is_none()));
		let base = PoolBase {
			pipelines: own.pipelines.len(),
			limits: u16::try_from(own.limits.len()).expect("few limits"),
			distincts: u16::try_from(own.distincts.len()).expect("few distinct sets"),
			sorts: u16::try_from(own.sorts.len()).expect("few sorts"),
			aggregates: u16::try_from(own.aggregates.len()).expect("few aggregates"),
			splits: u16::try_from(own.splits.len()).expect("few splits"),
			joins: u16::try_from(own.joins.len()).expect("few joins"),
			probes: u16::try_from(own.probes.len()).expect("few probes"),
			groups: u16::try_from(own.groups.len()).expect("few groups"),
		};
		let mark = em.open_group(3);
		let group = lower_pipelines_at(&mut em, &mut cx, &b.ir, &mut facts, b.ir.top(), base)
			.expect("the group's pipelines lower");
		let permissions = em.close_group(mark).expect("the permission range");
		assert_eq!(em.group(), None);
		assert_eq!(group.distincts, vec![Some(3)]);
		assert!(group.limits.iter().all(|limit| limit.group == Some(3)));
		assert!(group.pipelines.iter().all(|pipeline| pipeline.group == Some(3)));
		let code: Vec<&Instr> =
			group.pipelines.iter().flat_map(|pipeline| pipeline.code.iter()).collect();
		assert!(code.iter().any(|instr| matches!(
			instr,
			Instr::Distinct {
				distinct: 1,
				..
			}
		)));
		assert!(code.iter().any(|instr| matches!(
			instr,
			Instr::Take {
				limit: 1,
				..
			}
		)));
		assert!(matches!(
			group.pipelines.last().map(|pipeline| &pipeline.source),
			Some(Source::Sorted {
				sort: 1
			})
		));
		let spec = group.group_spec(permissions, None, false, false).expect("the group's spec");
		assert_eq!(spec.sorts, 1..2);
		assert_eq!(spec.permissions, 0..0);
		assert!(spec.aggregates.is_empty());
	}

	/// A permission frame is shared with an equal frame of the same group only: each group's
	/// frames are a range of their own, which each reach of the statement starts afresh.
	#[test]
	fn a_group_shares_no_permission_frame_with_the_code_before_it() {
		use crate::exec::vm::program::{FrameMode, PermissionSpec};
		use crate::expr::logical::GuardKind;
		let frame = || PermissionSpec {
			mode: FrameMode::Guard(GuardKind::Read),
			auth: None,
			column: false,
		};
		let mut em = Emitter::new();
		assert_eq!(em.permission_frame(frame()).ok(), Some(0));
		let mark = em.open_group(0);
		assert_eq!(em.permission_frame(frame()).ok(), Some(1));
		assert_eq!(em.permission_frame(frame()).ok(), Some(1));
		assert_eq!(em.close_group(mark).ok(), Some(1..2));
		assert_eq!(em.permission_frame(frame()).ok(), Some(0));
	}

	#[test]
	fn a_statement_has_one_output() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, scan);
		pipeline(&mut b, 1, scan);
		let b = placed(b);
		assert_eq!(refused(&b), "ir_lower::pipe: more than one pipeline ending in the output");
	}

	#[test]
	fn a_source_beside_the_output_pipeline_is_a_second_output() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, scan);
		scan(&mut b);
		let b = placed(b);
		assert_eq!(refused(&b), "ir_lower::pipe: more than one pipeline ending in the output");
	}

	#[test]
	fn a_source_whose_rows_are_the_output_is_read_by_a_pipeline_of_its_own() {
		let mut b = Builder::new();
		scan(&mut b);
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 1);
		assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
	}

	#[test]
	fn a_sort_whose_rows_are_the_output_counts_its_reader_once() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		sort(&mut b, fed, &["a"]);
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 2);
		assert_eq!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		);
	}

	#[test]
	fn a_sort_over_a_source_reads_it_through_a_pipeline_of_its_own() {
		let mut b = Builder::new();
		let rows = scan(&mut b);
		sort(&mut b, rows, &["a"]);
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 2);
		assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		);
	}

	/// `FROM a, b` where `a` reads through a stack over its scan: the union of a pipeline's rows
	/// and a scan.
	fn union_of_a_stack_and_a_scan(b: &mut Builder) -> ValueId {
		let rows = scan(b);
		let stack = select_loop(b, 0, rows, |b, element| {
			let filter = step(b, PhysOp::Filter, element, &[]);
			where_clause(b, filter, |b| param(b, "a"));
			b.result_of(filter)
		});
		let first = b.result_of(stack);
		let second = scan(b);
		let union = b.create_kind(OpKind::Phys(PhysOp::Union), &[first, second], 1);
		b.result_of(union)
	}

	/// The input of a union that a pipeline delivers is a group of the program, and a
	/// table's stack among several sources is read as its own statement; the scan beside it is a
	/// source of the union.
	#[test]
	fn a_union_input_a_pipeline_delivers_is_a_group_the_union_reads() {
		let mut b = Builder::new();
		union_of_a_stack_and_a_scan(&mut b);
		let program = lowered(&placed(b)).program;
		assert_eq!(program.groups.len(), 1);
		assert_eq!(program.pipelines.len(), 2);
		assert_eq!(program.pipelines[0].group, Some(0));
		assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(program.pipelines[1].group, None);
		let Source::Union(inputs) = &program.pipelines[1].source else {
			panic!("an unexpected source: {:?}", program.pipelines[1].source);
		};
		assert!(matches!(
			inputs.as_slice(),
			[
				Source::Nested {
					group: 0
				},
				Source::Scan { .. }
			]
		));
		assert!(!program.groups[0].scalar);
		assert!(program.groups[0].version.is_none());
		assert!(program.groups[0].timeout.is_none());
		assert_eq!(program.table, Some(TableName::from("t")));
	}

	/// A sort above such a union reads it through a pipeline of its own, which opens the group;
	/// each of the three pipelines counts once.
	#[test]
	fn a_sort_over_a_union_with_a_group_input_counts_each_pipeline_once() {
		let mut b = Builder::new();
		let rows = union_of_a_stack_and_a_scan(&mut b);
		sort(&mut b, rows, &["a"]);
		let program = lowered(&placed(b)).program;
		assert_eq!(program.groups.len(), 1);
		assert_eq!(program.pipelines.len(), 3);
		assert_eq!(program.pipelines[0].group, Some(0));
		assert!(matches!(program.pipelines[1].source, Source::Union(_)));
		assert_eq!(
			program.pipelines[2].source,
			Source::Sorted {
				sort: 0
			}
		);
	}

	/// The rows of a pipeline read by the region's yield and by a second op, which no producer
	/// builds, are an invariant break where the region is lowered without a statement's scan.
	#[test]
	fn rows_read_by_an_op_that_is_not_their_reader_are_an_invariant_break() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let rows = b.result_of(feeder);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[rows], 0);
		step(&mut b, PhysOp::Filter, rows, &[]);
		let b = placed(b);
		let mut em = Emitter::new();
		let mut facts = FactCache::new();
		match lower_pipelines(&mut em, &mut LowerCx::detached(), &b.ir, &mut facts, b.ir.top()) {
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				detail,
				..
			})) => assert_eq!(detail, "rows read by an op that is not their reader"),
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the container lowers"),
		}
	}

	/// `SELECT * FROM (SELECT * FROM t ORDER BY a)`: the reader of the inner sort, which counts
	/// itself in the inner statement's region, counts once in the region that reads the group.
	#[test]
	fn a_statement_in_from_whose_output_a_reader_delivers_counts_that_reader_once() {
		let mut b = Builder::new();
		let nested = b.create_kind(OpKind::Phys(PhysOp::Subquery), &[], 1);
		b.open(nested, RegionKind::Subquery, &[]);
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		let sorted = sort(&mut b, fed, &["a"]);
		let sorted_rows = b.result_of(sorted);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[sorted_rows], 0);
		b.close();
		let rows = b.result_of(nested);
		select_loop(&mut b, 1, rows, |_, row| row);
		let program = lowered(&placed(b)).program;
		assert_eq!(program.groups.len(), 1);
		assert_eq!(program.pipelines.len(), 3);
		assert_eq!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		);
		assert_eq!(program.pipelines[1].group, Some(0));
		assert_eq!(
			program.pipelines[2].source,
			Source::Nested {
				group: 0
			}
		);
	}

	/// The rows of the `EXPLAIN` of `SELECT * FROM t`, `analyze` or plain, in the open region.
	fn explained_scan(b: &mut Builder, analyze: bool) -> ValueId {
		let explained = pipeline(b, 0, scan);
		let rows = b.result_of(explained);
		let explain = b.create_kind(
			OpKind::Phys(PhysOp::Explain {
				analyze,
				format: crate::expr::ExplainFormat::Text,
			}),
			&[rows],
			1,
		);
		b.result_of(explain)
	}

	/// `SELECT * FROM (SELECT * FROM t EXPLAIN)`, or its `EXPLAIN ANALYZE`.
	fn explained_in_from(analyze: bool) -> Builder {
		let mut b = Builder::new();
		let nested = b.create_kind(OpKind::Phys(PhysOp::Subquery), &[], 1);
		b.open(nested, RegionKind::Subquery, &[]);
		let explained = explained_scan(&mut b, analyze);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[explained], 0);
		b.close();
		let rows = b.result_of(nested);
		select_loop(&mut b, 1, rows, |_, row| row);
		placed(b)
	}

	/// A plain `EXPLAIN` in `FROM` is the group of an evaluated statement: one scalar pipeline
	/// that loads the rendering, with no scan of the explained statement lowered.
	#[test]
	fn a_plain_explain_in_from_is_a_scalar_group_that_loads_the_rendering() {
		let program = lowered(&explained_in_from(false)).program;
		assert_eq!(program.groups.len(), 1);
		assert!(program.groups[0].scalar && program.groups[0].rowless);
		assert!(program.groups[0].version.is_none() && program.groups[0].timeout.is_none());
		assert_eq!(program.pipelines.len(), 2);
		assert_eq!(program.pipelines[0].group, Some(0));
		assert_eq!(program.pipelines[0].source, Source::Once);
		assert!(program.pipelines[0].code.iter().any(|instr| matches!(instr, Instr::Load { .. })));
		assert_eq!(
			program.pipelines[1].source,
			Source::Nested {
				group: 0
			}
		);
		assert!(
			program
				.pipelines
				.iter()
				.all(|pipeline| !matches!(pipeline.source, Source::Scan { .. }))
		);
	}

	/// `EXPLAIN ANALYZE` in `FROM` is refused with the error that a block's statement raises
	/// there.
	#[test]
	fn an_analysed_explain_in_from_is_refused_as_main_refuses_it() {
		assert_eq!(broken(&explained_in_from(true)), "a block or schema statement in FROM");
	}

	/// A context that holds no `EXPLAIN` hook refuses every form of `EXPLAIN` with
	/// [`NOT_SERVED_EXPLAIN`]: a plain one in `FROM`, and a plain and an analysed one in value
	/// position.
	#[test]
	fn an_explain_is_refused_where_the_context_holds_no_explain_hook() {
		let without_hook = || LowerCx::detached().with_explain(None);
		let refusal = |result: Result<Lowered, IrError>| match result {
			Err(IrError::NotYet {
				what,
			}) => what,
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the container lowers"),
		};
		let from = explained_in_from(false);
		assert_eq!(
			refusal(lower_reads(&mut without_hook(), &from.ir, false)),
			super::super::explain_hook::NOT_SERVED_EXPLAIN
		);
		for analyze in [false, true] {
			let mut b = Builder::new();
			let explained = explained_scan(&mut b, analyze);
			b.create_kind(OpKind::Phys(PhysOp::Yield), &[explained], 0);
			let b = placed(b);
			assert_eq!(
				refusal(super::super::dispatch::lower_subquery_unit(
					&mut without_hook(),
					&b.ir,
					&FactCache::new(),
				)),
				super::super::explain_hook::NOT_SERVED_EXPLAIN,
				"analyze: {analyze}"
			);
		}
	}

	/// A region scanned as a read that holds a plain `EXPLAIN` is a shape no entry hands the
	/// scan: each renders the `EXPLAIN` as its own group.
	#[test]
	fn a_plain_explain_scanned_as_a_read_is_an_invariant_break() {
		let mut b = Builder::new();
		let explained = explained_scan(&mut b, false);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[explained], 0);
		let b = placed(b);
		assert_eq!(broken(&b), "a plain EXPLAIN is rendered by its group, never scanned");
	}

	/// A statement in value position that is a plain `EXPLAIN` is a unit that answers the
	/// rendering, as the top-level `EXPLAIN` is, and never the rows of the statement it explains.
	#[test]
	fn a_plain_explain_in_value_position_answers_the_rendering() {
		let mut b = Builder::new();
		let explained = explained_scan(&mut b, false);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[explained], 0);
		let b = placed(b);
		let program = match super::super::dispatch::lower_subquery_unit(
			&mut LowerCx::detached(),
			&b.ir,
			&FactCache::new(),
		) {
			Ok(lowered) => lowered.program,
			Err(error) => panic!("the unit lowers: {error}"),
		};
		assert!(program.scalar);
		assert_eq!(program.pipelines.len(), 1);
		assert_eq!(program.pipelines[0].source, Source::Once);
	}

	/// A statement in value position that is an `EXPLAIN ANALYZE` is a unit that answers the
	/// rendering of what the run of the explained statement measured, as `RETURN (EXPLAIN ANALYZE
	/// ...)` does, and never the rows of that statement: the explained statement is the program's
	/// one group, and the program's own pipeline runs once.
	#[test]
	fn an_analysed_explain_in_value_position_is_not_the_rows_it_explains() {
		let mut b = Builder::new();
		let explained = explained_scan(&mut b, true);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[explained], 0);
		let b = placed(b);
		let program = match super::super::dispatch::lower_subquery_unit(
			&mut LowerCx::detached(),
			&b.ir,
			&FactCache::new(),
		) {
			Ok(lowered) => lowered.program,
			Err(error) => panic!("the unit lowers: {error}"),
		};
		assert!(program.scalar);
		assert_eq!(program.groups.len(), 1);
		assert_eq!(program.analyses.len(), 1);
		assert_eq!(program.pipelines.len(), 2);
		assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(program.pipelines[0].group, Some(0));
		assert_eq!(program.pipelines[1].source, Source::Once);
		assert_eq!(program.pipelines[1].group, None);
		assert!(program.pipelines[1].code.iter().any(|instr| matches!(
			instr,
			Instr::Block(crate::exec::vm::program::BlockInstr::RunAnalyzed {
				statement: 0,
				analysis: 0,
				..
			})
		)));
	}

	/// The run of an analysed statement starts a statement nested in the body, as a block
	/// statement does: the host counts it before it takes the step, so a region or an aggregate
	/// that evaluates it does not read a failure that follows it as `NONE`.
	#[test]
	fn an_analysed_run_counts_as_a_nested_statement() {
		use crate::exec::vm::host::runs_nested_statement;
		use crate::exec::vm::interp::Step;

		let mut b = Builder::new();
		let explained = explained_scan(&mut b, true);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[explained], 0);
		let b = placed(b);
		let program = match super::super::dispatch::lower_subquery_unit(
			&mut LowerCx::detached(),
			&b.ir,
			&FactCache::new(),
		) {
			Ok(lowered) => lowered.program,
			Err(error) => panic!("the unit lowers: {error}"),
		};
		let pipeline = &program.pipelines[1];
		let at = pipeline
			.code
			.iter()
			.position(|instr| {
				matches!(
					instr,
					Instr::Block(crate::exec::vm::program::BlockInstr::RunAnalyzed { .. })
				)
			})
			.expect("the unit runs the analysed group");
		assert!(runs_nested_statement(
			&program,
			pipeline,
			&Step::Block {
				at
			}
		));
	}

	#[test]
	fn rows_a_breaker_reads_that_no_op_makes_as_rows_are_an_invariant_break() {
		let mut b = Builder::new();
		let value = param(&mut b, "a");
		match pipeline_behind(&b.ir, value) {
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				detail,
				..
			})) => assert_eq!(detail, "rows a breaker or a step reads that no op makes as rows"),
			other => panic!("an unexpected answer: {other:?}"),
		}
	}

	#[test]
	fn a_statement_region_with_nothing_that_makes_rows_is_an_invariant_break() {
		let b = placed(Builder::new());
		assert_eq!(broken(&b), NO_ROWS);
	}

	#[test]
	fn a_pipeline_whose_ops_do_not_follow_one_another_is_an_internal_error() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let _ = scan(b);
			let rows = scan(b);
			let filter = step(b, PhysOp::Filter, rows, &[]);
			where_clause(b, filter, |b| param(b, "a"));
			b.result_of(filter)
		});
		let b = placed(b);
		assert!(matches!(
			lower_reads(&mut LowerCx::detached(), &b.ir, false),
			Err(IrError::Verify(_))
		));
	}

	#[test]
	fn the_pipeline_entry_of_a_container_answers_through_the_scalar_entry_point() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, scan);
		let b = placed(b);
		let through_lower = crate::exec::vm::ir_lower::lower(&b.ir, &FactCache::new())
			.expect("a container of pipelines lowers through the entry point");
		assert_eq!(through_lower.program, lowered(&b).program);
	}

	/// The block argument of the last region `owner` owns: the row it receives.
	pub(in crate::exec::vm::ir_lower) fn region_row(ir: &Ir, owner: OpId) -> ValueId {
		let region = *ir.op(owner).expect("the op exists").regions.last().expect("a region");
		let block = ir.region(region).expect("the region exists").blocks[0];
		ir.block(block).expect("the block exists").args[0]
	}

	/// A region of `kind` owned by `owner` that receives one row: `body` builds it over the row and
	/// the region yields the value `body` answers.
	pub(super) fn row_region(
		b: &mut Builder,
		owner: OpId,
		kind: RegionKind,
		body: impl FnOnce(&mut Builder, ValueId) -> ValueId,
	) {
		b.open(owner, kind, &[TypeKnowledge::Unknown]);
		let row = region_row(&b.ir, owner);
		let value = body(b, row);
		b.create_kind(OpKind::Expr(ExprOp::Yield), &[value], 0);
		b.close();
	}

	/// A `phys.pipeline` of id `id` whose region holds a select loop over `rows`, with the body
	/// `body` builds over the element, and yields the loop's rows.
	pub(in crate::exec::vm::ir_lower) fn select_loop(
		b: &mut Builder,
		id: u32,
		rows: ValueId,
		body: impl FnOnce(&mut Builder, ValueId) -> ValueId,
	) -> OpId {
		let op = b.create_kind(
			OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
				id: PipelineId(id),
				edges: SmallVec::new(),
				logical: None,
			}))),
			&[],
			1,
		);
		b.open(op, RegionKind::Pipeline, &[]);
		let looped = b.create_kind(
			OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Select,
				first: false,
				batch: Batch {
					max_iterations: 1024,
					max_bytes: 1 << 20,
				},
				unroll: 1,
				value: false,
				step: None,
			}),
			&[rows],
			1,
		);
		b.open(looped, RegionKind::Loop, &[TypeKnowledge::Unknown]);
		let element = region_row(&b.ir, looped);
		let delivered = body(b, element);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[delivered], 0);
		b.close();
		let result = b.result_of(looped);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[result], 0);
		b.close();
		op
	}

	fn field_path(names: &[&str]) -> FieldPath {
		FieldPath(names.iter().map(|name| FieldPathPart::Field((*name).to_owned())).collect())
	}

	/// A sort over `rows` by the fields `names`, ascending, that keeps every row.
	pub(super) fn sort(b: &mut Builder, rows: ValueId, names: &[&str]) -> OpId {
		let keys = names
			.iter()
			.map(|name| SortKeySpecShape {
				read: SortKeyReadShape::Path(field_path(&[name])),
				ascending: true,
				collate: false,
				numeric: false,
			})
			.collect();
		b.create_kind(
			OpKind::Phys(PhysOp::Sort(Box::new(SortAttrs {
				keys,
				keep: None,
				pushdown: false,
				random: false,
				spill: Spill::Spillable,
				tempfiles: false,
			}))),
			&[rows],
			1,
		)
	}

	/// `TEMPFILES` holds on disk a sort that keeps all its rows in an order, and neither a bounded
	/// top nor a random order.
	#[test]
	fn tempfiles_spill_only_a_sort_that_keeps_every_row_in_order() {
		let attrs = |keep: Option<usize>, random: bool| SortAttrs {
			keys: Vec::new(),
			keep,
			pushdown: false,
			random,
			spill: Spill::of_sort(keep),
			tempfiles: true,
		};
		assert!(sort_spec(&attrs(None, false)).spill);
		assert!(!sort_spec(&attrs(Some(3), false)).spill);
		assert!(!sort_spec(&attrs(None, true)).spill);
		let plain = SortAttrs {
			tempfiles: false,
			..attrs(None, false)
		};
		assert!(!sort_spec(&plain).spill);
	}

	fn record(b: &mut Builder, key: i64) -> ValueId {
		b.add(
			ExprOp::RecordId(Box::new(RecordIdAttrs {
				table: TableName::from("t"),
				key_shape: KeyShape::Number(key),
			})),
			&[],
		)
	}

	#[test]
	fn a_predicate_reads_the_row_through_the_block_argument_of_its_region() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let filter = step(b, PhysOp::Filter, rows, &[]);
			row_region(b, filter, RegionKind::Clause(ClauseRole::Where), |b, row| {
				let a = b.add(
					ExprOp::Access {
						field: Strand::from("a"),
					},
					&[row],
				);
				let one = b.literal(Value::from(1_i64));
				b.binary(BinaryOperator::MoreThan, a, one)
			});
			b.result_of(filter)
		});
		let b = placed(b);
		let code = the_code(&b);
		assert!(
			matches!(
				code[0],
				Instr::Access {
					src: Operand::Reg(ROW),
					..
				}
			),
			"{code:?}"
		);
		assert!(
			matches!(
				code[1],
				Instr::FilterCompare {
					reject: false,
					..
				}
			),
			"{code:?}"
		);
	}

	#[test]
	fn a_select_loop_lowers_its_body_as_the_chain_does() {
		let mut chain = Builder::new();
		pipeline(&mut chain, 0, |b| {
			let rows = scan(b);
			let filter = step(b, PhysOp::Filter, rows, &[]);
			where_clause(b, filter, |b| param(b, "a"));
			b.result_of(filter)
		});
		let chain = placed(chain);

		let mut looped = Builder::new();
		let rows = scan(&mut looped);
		select_loop(&mut looped, 0, rows, |b, element| {
			let filter = step(b, PhysOp::Filter, element, &[]);
			where_clause(b, filter, |b| param(b, "a"));
			b.result_of(filter)
		});
		let looped = placed(looped);

		let from_loop = lowered(&looped);
		let from_chain = lowered(&chain);
		assert_eq!(from_loop.program.pipelines[0].source, from_chain.program.pipelines[0].source);
		assert_eq!(from_loop.program.pipelines[0].code, from_chain.program.pipelines[0].code);
		assert_eq!(from_loop.program.table, Some(TableName::from("t")));
		assert_eq!(from_loop.provenance.pipelines.len(), 1);
	}

	#[test]
	fn a_select_loop_that_delivers_its_element_hands_each_row_on() {
		let mut b = Builder::new();
		let rows = scan(&mut b);
		select_loop(&mut b, 0, rows, |_, element| element);
		let b = placed(b);
		assert_eq!(
			the_code(&b),
			vec![
				Instr::EmitEnd {
					src: Operand::Reg(ROW)
				},
				Instr::StopPipeline,
			]
		);
	}

	#[test]
	fn a_row_loop_that_is_not_a_select_loop_and_a_first_body_are_invariant_breaks() {
		for (kind, first, what) in [
			(
				LoopKind::For,
				false,
				"a pipeline of rows whose loop is not a select loop or a write loop",
			),
			(LoopKind::Select, true, "a select loop with a body for its first row"),
		] {
			let mut b = Builder::new();
			let rows = scan(&mut b);
			let op = b.create_kind(
				OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
					id: PipelineId(0),
					edges: SmallVec::new(),
					logical: None,
				}))),
				&[],
				1,
			);
			b.open(op, RegionKind::Pipeline, &[]);
			let looped = b.create_kind(
				OpKind::Phys(PhysOp::Loop {
					kind,
					first,
					batch: Batch {
						max_iterations: 1,
						max_bytes: 1,
					},
					unroll: 1,
					value: false,
					step: None,
				}),
				&[rows],
				1,
			);
			b.open(looped, RegionKind::Loop, &[TypeKnowledge::Unknown]);
			let element = region_row(&b.ir, looped);
			b.create_kind(OpKind::Phys(PhysOp::Yield), &[element], 0);
			b.close();
			let result = b.result_of(looped);
			b.create_kind(OpKind::Phys(PhysOp::Yield), &[result], 0);
			b.close();
			let b = placed(b);
			assert_eq!(broken(&b), what);
		}
	}

	#[test]
	fn a_sort_is_fed_by_the_pipeline_below_it_and_read_as_sorted_rows() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		pipeline(&mut b, 1, |b| {
			let sorted = sort(b, fed, &["a"]);
			b.result_of(sorted)
		});
		let b = placed(b);
		let lowered = lowered(&b);
		let program = &lowered.program;
		assert_eq!(program.pipelines.len(), 2);
		// The feeder comes first and ends in the push of the key of each row.
		assert!(matches!(&program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::Load {
					dst: ROW + 1,
					src: Operand::Field(0)
				},
				Instr::SortPush {
					sort: 0,
					keys: ROW + 1,
					count: 1,
					src: ROW
				},
				Instr::EndRow,
				Instr::StopPipeline,
			]
		);
		assert_eq!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		);
		assert_eq!(
			program.pipelines[1].code,
			vec![
				Instr::EmitEnd {
					src: Operand::Reg(ROW)
				},
				Instr::StopPipeline,
			]
		);
		assert_eq!(program.sorts.len(), 1);
		assert_eq!(program.sorts[0].keys.len(), 1);
		assert!(program.sorts[0].keys[0].ascending);
		assert_eq!(program.sorts[0].keep, None);
		assert!(!program.sorts[0].spill && !program.sorts[0].pushdown && !program.sorts[0].random);
		assert_eq!(lowered.provenance.pipelines.len(), 2);
		assert_eq!(lowered.provenance.origin(0).map(|(origin, _)| origin), Some(feeder));
	}

	#[test]
	fn a_select_loop_reads_the_rows_of_a_sort_outside_its_pipeline() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		let sorted = sort(&mut b, fed, &["a", "b"]);
		let rows = b.result_of(sorted);
		select_loop(&mut b, 1, rows, |_, element| element);
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 2);
		assert_eq!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		);
		assert!(matches!(
			program.pipelines[0].code[2],
			Instr::SortPush {
				sort: 0,
				count: 2,
				..
			}
		));
	}

	#[test]
	fn a_limit_of_the_reader_is_a_limit_of_the_pipeline_that_feeds_the_sort() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		pipeline(&mut b, 1, |b| {
			let sorted = sort(b, fed, &["a"]);
			let rows = b.result_of(sorted);
			let two = b.literal(Value::from(2_i64));
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[two],
			);
			b.result_of(limited)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines[0].limits, vec![0]);
		assert_eq!(program.pipelines[1].limits, vec![0]);
		assert_eq!(program.limits.len(), 1);
	}

	#[test]
	fn a_sort_over_rows_that_may_be_any_value_reads_its_key_as_a_path_step() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, |b| {
			let one = b.literal(Value::from(1_i64));
			let values = b.create_kind(
				OpKind::Phys(PhysOp::Values {
					read: ValuesRead::Expression {
						written: true,
					},
				}),
				&[one],
				1,
			);
			b.result_of(values)
		});
		let fed = b.result_of(feeder);
		pipeline(&mut b, 1, |b| {
			let sorted = sort(b, fed, &["a"]);
			b.result_of(sorted)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		assert!(matches!(&program.pipelines[0].source, Source::Values { .. }));
		assert_eq!(
			program.pipelines[0].code[0],
			Instr::Extract {
				src: Operand::Reg(ROW),
				path: 0,
				dst: ROW + 1
			}
		);
		assert_eq!(program.paths[0], field_path(&["a"]));
	}

	#[test]
	fn a_computed_sort_key_is_the_value_of_its_region_over_the_row() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		pipeline(&mut b, 1, |b| {
			let keyed = b.create_kind(
				OpKind::Phys(PhysOp::Sort(Box::new(SortAttrs {
					keys: vec![SortKeySpecShape {
						read: SortKeyReadShape::Computed,
						ascending: false,
						collate: true,
						numeric: false,
					}],
					keep: Some(3),
					pushdown: false,
					random: false,
					spill: Spill::Bounded,
					tempfiles: false,
				}))),
				&[fed],
				1,
			);
			row_region(b, keyed, RegionKind::Clause(ClauseRole::OrderKey), |b, row| {
				b.add(
					ExprOp::Access {
						field: Strand::from("a"),
					},
					&[row],
				)
			});
			b.result_of(keyed)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		let code = &program.pipelines[0].code;
		assert!(
			matches!(
				code[0],
				Instr::Access {
					src: Operand::Reg(ROW),
					..
				}
			),
			"{code:?}"
		);
		assert!(matches!(code[1], Instr::Load { .. }), "{code:?}");
		assert!(
			matches!(
				code[2],
				Instr::SortPush {
					count: 1,
					..
				}
			),
			"{code:?}"
		);
		assert_eq!(program.sorts[0].keep, Some(3));
		assert!(!program.sorts[0].keys[0].ascending && program.sorts[0].keys[0].collate);
	}

	#[test]
	fn a_sort_key_path_that_does_not_start_at_a_field_is_unsupported() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		pipeline(&mut b, 1, |b| {
			let sorted = b.create_kind(
				OpKind::Phys(PhysOp::Sort(Box::new(SortAttrs {
					keys: vec![SortKeySpecShape {
						read: SortKeyReadShape::Path(FieldPath(vec![FieldPathPart::First])),
						ascending: true,
						collate: false,
						numeric: false,
					}],
					keep: None,
					pushdown: false,
					random: false,
					spill: Spill::Spillable,
					tempfiles: false,
				}))),
				&[fed],
				1,
			);
			b.result_of(sorted)
		});
		let b = placed(b);
		match lower_reads(&mut LowerCx::detached(), &b.ir, false) {
			Err(IrError::Unsupported {
				reason,
			}) => assert_eq!(reason, "a sort key path that does not start at a field"),
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the container lowers"),
		}
	}

	#[test]
	fn a_sort_nobody_reads_is_the_output_and_a_sort_read_twice_is_refused() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		sort(&mut b, fed, &["a"]);
		let b = placed(b);
		let lowered = lowered(&b);
		assert_eq!(lowered.program.pipelines.len(), 2);
		assert_eq!(
			lowered.program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		);
		assert_eq!(
			lowered.program.pipelines[1].code,
			vec![
				Instr::EmitEnd {
					src: Operand::Reg(ROW)
				},
				Instr::StopPipeline,
			]
		);
		// The reader stands for no pipeline op.
		assert_eq!(lowered.provenance.pipelines.len(), 1);

		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		let sorted = sort(&mut b, fed, &["a"]);
		let rows = b.result_of(sorted);
		select_loop(&mut b, 1, rows, |_, element| element);
		select_loop(&mut b, 2, rows, |_, element| element);
		let b = placed(b);
		assert_eq!(broken(&b), "a breaker read more than once");
	}

	#[test]
	fn a_sort_over_a_sort_reads_it_through_a_pipeline_of_its_own() {
		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		let first = sort(&mut b, fed, &["a"]);
		let rows = b.result_of(first);
		sort(&mut b, rows, &["b"]);
		let b = placed(b);
		let program = lowered(&b).program;
		// The output's reader opens the outer sort first, so it is sort 0.
		assert_eq!(program.pipelines.len(), 3);
		assert!(program.pipelines[0].code.iter().any(|instr| matches!(
			instr,
			Instr::SortPush {
				sort: 1,
				..
			}
		)));
		assert_eq!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 1
			}
		);
		assert!(program.pipelines[1].code.iter().any(|instr| matches!(
			instr,
			Instr::SortPush {
				sort: 0,
				..
			}
		)));
		assert_eq!(
			program.pipelines[2].source,
			Source::Sorted {
				sort: 0
			}
		);
	}

	#[test]
	fn a_projection_of_a_value_loads_it_and_hands_that_register_on() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let project = step(b, PhysOp::Project(Box::new(ProjectionShape::Value)), rows, &[]);
			b.region(project, RegionKind::Clause(ClauseRole::Projection), |b| {
				b.literal(Value::from(7_i64))
			});
			b.result_of(project)
		});
		let b = placed(b);
		assert_eq!(
			the_code(&b),
			vec![
				Instr::Load {
					dst: ROW + 1,
					src: Operand::Const(0)
				},
				Instr::EmitEnd {
					src: Operand::Reg(ROW + 1)
				},
				Instr::StopPipeline,
			]
		);
	}

	fn field(output: &str, aliased: bool) -> ProjectedFieldShape {
		ProjectedFieldShape {
			output: field_path(&[output]),
			aliased,
			sort_key: None,
		}
	}

	#[test]
	fn a_projection_of_fields_builds_an_object_and_removes_the_omitted_paths() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let project = step(
				b,
				PhysOp::Project(Box::new(ProjectionShape::Fields {
					fields: vec![field("x", true)],
					omit: vec![
						OmissionShape::Path(field_path(&["y"])),
						OmissionShape::Path(field_path(&["y", "z"])),
					],
				})),
				rows,
				&[],
			);
			b.region(project, RegionKind::Clause(ClauseRole::Projection), |b| {
				b.literal(Value::from(7_i64))
			});
			b.result_of(project)
		});
		let b = placed(b);
		let lowered = lowered(&b);
		assert_eq!(
			lowered.program.pipelines[0].code,
			vec![
				Instr::NewObject {
					dst: ROW + 1
				},
				Instr::SetField {
					object: ROW + 1,
					path: 0,
					value: Operand::Const(0)
				},
				Instr::RemoveField {
					object: ROW + 1,
					name: 0
				},
				Instr::RemovePath {
					object: ROW + 1,
					path: 1
				},
				Instr::EmitEnd {
					src: Operand::Reg(ROW + 1)
				},
				Instr::StopPipeline,
			]
		);
		assert_eq!(lowered.program.paths[0], field_path(&["x"]));
		assert_eq!(lowered.program.paths[1], field_path(&["y", "z"]));
	}

	#[test]
	fn a_star_projection_writes_its_fields_onto_the_row_or_onto_an_object_seeded_from_it() {
		let star = |source: fn(&mut Builder) -> ValueId| {
			let mut b = Builder::new();
			pipeline(&mut b, 0, |b| {
				let rows = source(b);
				let project = step(
					b,
					PhysOp::Project(Box::new(ProjectionShape::Star {
						fields: vec![field("x", true)],
						omit: Vec::new(),
					})),
					rows,
					&[],
				);
				b.region(project, RegionKind::Clause(ClauseRole::Projection), |b| {
					b.literal(Value::from(7_i64))
				});
				b.result_of(project)
			});
			placed(b)
		};
		let b = star(scan);
		assert_eq!(
			the_code(&b)[..2],
			[
				Instr::LoadRow {
					dst: ROW + 1
				},
				Instr::SetField {
					object: ROW + 1,
					path: 0,
					value: Operand::Const(0)
				},
			]
		);
		let b = star(|b| {
			let one = b.literal(Value::from(1_i64));
			let values = b.create_kind(
				OpKind::Phys(PhysOp::Values {
					read: ValuesRead::Expression {
						written: true,
					},
				}),
				&[one],
				1,
			);
			b.result_of(values)
		});
		// Rows that may not be objects are tested: an object has the field written onto it, and
		// any other row is seeded as an object first, since the field computes a value.
		let code = the_code(&b);
		assert!(
			matches!(
				code[..6],
				[
					Instr::LoadRow {
						dst
					},
					Instr::JumpIfNotObject {
						cond: Operand::Reg(cond),
						..
					},
					Instr::SetField {
						object: first,
						..
					},
					Instr::Jump {
						..
					},
					Instr::SeedObject {
						dst: seeded
					},
					Instr::SetField {
						object: second,
						..
					},
				] if dst == ROW + 1 && cond == dst && first == dst && seeded == dst && second == dst
			),
			"{code:?}"
		);
	}

	#[test]
	fn an_omitted_value_is_read_from_the_row_without_its_omitted_paths() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let project = step(
				b,
				PhysOp::Project(Box::new(ProjectionShape::OmittedValue {
					omit: vec![OmissionShape::Path(field_path(&["y"]))],
				})),
				rows,
				&[],
			);
			row_region(b, project, RegionKind::Clause(ClauseRole::Projection), |b, row| {
				b.add(
					ExprOp::Access {
						field: Strand::from("x"),
					},
					&[row],
				)
			});
			b.result_of(project)
		});
		let b = placed(b);
		let code = the_code(&b);
		assert_eq!(
			code[0],
			Instr::LoadRow {
				dst: ROW + 1
			}
		);
		assert!(matches!(code[1], Instr::RemoveField { object, .. } if object == ROW + 1));
		// The value reads the object the omission left, not the row.
		assert!(
			matches!(
				code[2],
				Instr::Access {
					src: Operand::Reg(reg),
					..
				} if reg == ROW + 1
			),
			"{code:?}"
		);
	}

	#[test]
	fn the_value_of_a_sort_key_needs_a_sort_to_have_handed_the_rows_over() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let project =
				step(b, PhysOp::Project(Box::new(ProjectionShape::SortedValue(0))), rows, &[]);
			b.result_of(project)
		});
		let b = placed(b);
		assert!(matches!(
			lower_reads(&mut LowerCx::detached(), &b.ir, false),
			Err(IrError::Verify(_))
		));

		let mut b = Builder::new();
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		pipeline(&mut b, 1, |b| {
			let sorted = sort(b, fed, &["a"]);
			let rows = b.result_of(sorted);
			let project =
				step(b, PhysOp::Project(Box::new(ProjectionShape::SortedValue(0))), rows, &[]);
			b.result_of(project)
		});
		let b = placed(b);
		let code = lowered(&b).program.pipelines[1].code.clone();
		assert_eq!(
			code[0],
			Instr::SortKey {
				key: 0,
				dst: ROW + 1
			}
		);
	}

	/// A walk omission holds the region of its source, which it does not evaluate, and lowers to
	/// a walk the host applies; one that lacks that region is an invariant break. A second
	/// projection over the rows of a first is refused.
	#[test]
	fn a_walk_omission_and_a_second_projection_are_refused() {
		let walked = |source: bool| {
			let mut b = Builder::new();
			pipeline(&mut b, 0, |b| {
				let rows = scan(b);
				let project = step(
					b,
					PhysOp::Project(Box::new(ProjectionShape::Fields {
						fields: Vec::new(),
						omit: vec![OmissionShape::Walk {
							steps: Vec::new(),
							action: crate::exec::ir::phys::ops::WalkActionShape::Nothing,
						}],
					})),
					rows,
					&[],
				);
				if source {
					row_region(b, project, RegionKind::Clause(ClauseRole::Projection), |_, row| {
						row
					});
				}
				b.result_of(project)
			});
			placed(b)
		};
		let program = lowered(&walked(true)).program;
		assert!(
			program.pipelines[0].code.iter().any(|instr| matches!(instr, Instr::Omit { .. })),
			"{:?}",
			program.pipelines[0].code
		);
		assert_eq!(broken(&walked(false)), "an omission walk with fewer regions than it reads");

		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let first = step(
				b,
				PhysOp::Project(Box::new(ProjectionShape::Fields {
					fields: Vec::new(),
					omit: Vec::new(),
				})),
				rows,
				&[],
			);
			let rows = b.result_of(first);
			let second = step(b, PhysOp::Project(Box::new(ProjectionShape::All)), rows, &[]);
			b.result_of(second)
		});
		let b = placed(b);
		assert!(matches!(
			lower_reads(&mut LowerCx::detached(), &b.ir, false),
			Err(IrError::Verify(_))
		));
	}

	#[test]
	fn a_limit_over_one_named_record_judges_the_row_whatever_the_paging() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let id = record(b, 1);
			let read = b.create_kind(
				OpKind::Phys(PhysOp::RecordScan(Box::new(RecordScanAttrs {
					table: TableName::from("t"),
					for_update: false,
					lightweight: false,
					read: Default::default(),
				}))),
				&[id],
				1,
			);
			let rows = b.result_of(read);
			let one = b.literal(Value::from(1_i64));
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[one],
			);
			b.result_of(limited)
		});
		let b = placed(b);
		let lowered = lowered(&b);
		assert!(matches!(&lowered.program.pipelines[0].source, Source::Record { .. }));
		assert_eq!(
			lowered.program.pipelines[0].code,
			vec![
				Instr::JumpIfLimitReached {
					limit: 0,
					done: 3
				},
				Instr::Take {
					limit: 0
				},
				Instr::Emit {
					src: ROW
				},
				Instr::EndRow,
				Instr::StopPipeline,
			]
		);
	}

	#[test]
	fn a_paging_clause_that_denotes_a_constant_is_a_constant() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let two = b.literal(Value::from(2_i64));
			let three = b.literal(Value::from(3_i64));
			let pair = b.add(ExprOp::Array, &[two, three]);
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[pair],
			);
			b.result_of(limited)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		assert!(matches!(
			&program.limits[0].limit,
			Some(HostScalar::Const(value)) if matches!(**value, Value::Array(_))
		));
	}

	#[test]
	fn a_spent_limit_before_the_first_row_is_pushed_down_through_two_feeders() {
		let mut b = Builder::new();
		let inner = pipeline(&mut b, 0, scan);
		let inner_rows = b.result_of(inner);
		let middle = pipeline(&mut b, 1, |b| {
			let sorted = sort(b, inner_rows, &["a"]);
			b.result_of(sorted)
		});
		let middle_rows = b.result_of(middle);
		pipeline(&mut b, 2, |b| {
			let sorted = sort(b, middle_rows, &["b"]);
			let rows = b.result_of(sorted);
			let one = b.literal(Value::from(1_i64));
			let limited = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				rows,
				&[one],
			);
			b.result_of(limited)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 3);
		for pipeline in &program.pipelines {
			assert_eq!(pipeline.limits, vec![0]);
		}
		assert_eq!(
			program.pipelines[0].source,
			Source::Scan {
				table: TableName::from("t"),
				direction: Direction::Forward,
				topk: None,
				prefilter: None,
				range: None,
				probes: None,
			}
		);
		// Each later pipeline reads the rows of its own sort; the sorts are numbered in the order
		// lowering reaches them, which is no order of the pipelines.
		let sorted = |at: usize| {
			let Source::Sorted {
				sort,
			} = program.pipelines[at].source
			else {
				panic!("pipeline {at} reads a sort: {:?}", program.pipelines[at].source);
			};
			sort
		};
		let mut sorts = vec![sorted(1), sorted(2)];
		sorts.sort_unstable();
		assert_eq!(sorts, vec![0, 1]);
		assert_eq!(program.sorts.len(), 2);
	}

	/// The ops a plan names its hops by, made of a slot map the way a container makes its own.
	fn hop_ops(count: usize) -> Vec<OpId> {
		let mut ops: slotmap::SlotMap<OpId, ()> = slotmap::SlotMap::with_key();
		(0..count).map(|_| ops.insert(())).collect()
	}

	/// Appends a lookup that ends at `hops[ends]` and covers `covered` hops, with no hop of its
	/// own, since the translation reads none.
	fn emitted(em: &mut Emitter, hops: &[OpId], ends: usize, covered: usize) -> u32 {
		let lookup = em
			.lookup(crate::exec::vm::program::LookupSpec {
				hops: Vec::new(),
				extract_id: false,
				only: false,
				only_path: false,
				checked: true,
				page: None,
				window: false,
				trace: crate::expr::logical::LookupTrace::None,
			})
			.expect("a lookup is appended");
		em.note_hop_lookup(
			hops[ends],
			crate::exec::vm::ir_lower::emit::HopLookup {
				lookup,
				hops: covered,
			},
		);
		lookup
	}

	/// The chains the translation names for the plan's `chains`, over the lookups `em` emitted.
	fn translated(em: &mut Emitter, chains: Vec<Vec<OpId>>) -> Option<Vec<Vec<u32>>> {
		let pipeline = em.finish_pipeline(Source::Once).expect("a pipeline");
		let io = PipelineIo {
			frontier_rows: Some(crate::exec::ir::phys::placement_attr::FrontierRows {
				chains,
				paging: None,
			}),
			..PipelineIo::default()
		};
		frontier_rows_of(&pipeline, &io, em)
			.expect("the rows window is translated")
			.map(|spec| spec.chains.iter().map(|chain| chain.to_vec()).collect())
	}

	#[test]
	fn a_chain_of_hops_is_named_by_the_lookups_that_cover_exactly_those_hops() {
		let hops = hop_ops(3);
		// One lookup for each hop.
		let mut em = Emitter::new();
		let first = emitted(&mut em, &hops, 0, 1);
		let second = emitted(&mut em, &hops, 1, 1);
		let third = emitted(&mut em, &hops, 2, 1);
		assert_eq!(translated(&mut em, vec![hops.clone()]), Some(vec![vec![first, second, third]]));
		// The first two hops joined into one lookup, which covers both and names both.
		let mut em = Emitter::new();
		let joined = emitted(&mut em, &hops, 1, 2);
		let last = emitted(&mut em, &hops, 2, 1);
		assert_eq!(translated(&mut em, vec![hops.clone()]), Some(vec![vec![joined, last]]));
		// A lookup that covers a hop the plan does not state for the chain ends the chain before
		// it, and a chain with no lookup leaves no window.
		let mut em = Emitter::new();
		emitted(&mut em, &hops, 2, 3);
		assert_eq!(translated(&mut em, vec![hops[..2].to_vec()]), None);
	}

	#[test]
	fn a_lookup_another_pipeline_emitted_names_no_chain() {
		let hops = hop_ops(1);
		let mut em = Emitter::new();
		let aside = em.set_aside();
		emitted(&mut em, &hops, 0, 1);
		em.restore(aside);
		assert_eq!(translated(&mut em, vec![hops.clone()]), None);
	}
}
