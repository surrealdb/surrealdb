//! The per-record body of a write statement.
//!
//! A write is one pipeline over its subjects whose body runs each record through the orders of its
//! form. [`write_body`] emits that body from the write's [`WriteBody`] alone: which orders there
//! are, which stages each runs, where the savepoint of a creating attempt falls, and what becomes
//! of a failed attempt are attributes of the `phys.write` op, and nothing here chooses between
//! them. The stages themselves are emitted by a [`StageSink`], so one body serves a write that
//! names its one table (every stage inline) and a write that reaches tables only data names (each
//! run of stages is a [`WriteInstr::DispatchTable`] site whose stages are lowered per table).
//!
//! # The statement
//!
//! [`lower_write`] lowers one `phys.write` as a program: the source its subjects name
//! ([`write_source`](super::write_source)), the body of its orders over the stages of its subject
//! loop, and the [`WriteSpec`] the run reads the clauses of the statement from.
//!
//! A write over the rows a read yields has no source of its own: the pipelines of the read are
//! lowered as any read's are ([`pipe`](super::pipe)), and the one that ends in the write's loop
//! runs the body over each row. The records an `UPSERT` over rows guarantees are written by a
//! pipeline of their own, which runs once the read's pipelines have judged every record. A write
//! that names no one table runs each run of stages as a site, whose stages the run loads by the
//! table a record is written to ([`StageTables::by_name`]); the bundle of one table's stages is
//! lowered by [`lower_stage_bundle`].
//!
//! # Shape of the body
//!
//! Every path of the body ends in one of two ends: `End { skipped: false }`, which hands the
//! record's output to the statement, and `End { skipped: true }`, which does not. A stage that
//! abandons the record jumps to the skipped end. The orders a form selects between:
//!
//! - one order ([`WriteBranching::Sole`]): its stages in sequence;
//! - the order chosen from the record the subject names ([`WriteBranching::UpFront`]): the updating
//!   order when the record exists, the creating order when it does not;
//! - a creating attempt that is rolled back on a classified failure ([`WriteBranching::Retry`]): a
//!   record its source read takes the updating order; any other takes the creating order. When the
//!   policy neither retries nor skips, a failed attempt fails the statement, so both orders run
//!   with no savepoint. Otherwise the stages ahead of the savepoint run first, the rest of the
//!   order runs inside the attempt, and a failed attempt is rolled back before its classification
//!   either raises it, skips the record, or runs the updating order against the record the
//!   classification chose, carrying the original failure when the policy says so. Both updating
//!   paths of such a policy run behind a savepoint of their own.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use smallvec::SmallVec;

use super::super::program::{
	GroupSpec, Instr, Label, Operand, Pipeline, Source, StageTables, WriteInstr, WriteSpec,
};
use super::emit::Emitter;
use super::expr::{Lowerer, Needs};
use super::pipe::{PoolBase, Reads, lower_pipelines_at};
use super::place::{Lowered, Provenance, parallel_of};
use super::stage::{StageRun, Stages};
use super::units::LowerCx;
use super::write::{WriteCutEmit, emit_cut, emit_skip};
use super::write_place::write_shape;
use super::write_source::{
	HostPlaces, edges_source, guaranteed_source, inserts_source, targets_source,
};
use crate::doc::Action;
use crate::exec::ContextLevel;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{
	WriteAttrs, WriteBody, WriteBranching, WriteRetry, WriteStageKind,
};
use crate::exec::ir::logical::stages::{plan_form, stage_id, view_row_order, write_loop};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{PhysOp, PhysWriteAttrs, WriteSubjectsShape};
use crate::exec::ir::phys::write_cut::WriteCut;
use crate::exec::operators::write::plan::RetryPolicy;
use crate::exec::planner::placement::Parallel;
use crate::exec::vm::ir_instr::{IrInstr, WriteCutSpec, WriteResumeSpec};
use crate::val::Value;

/// What emits the stages of one run into the body of a write.
pub(super) trait StageSink {
	/// Emits `stages`, in order, jumping to `skip` where a stage abandons the record.
	fn stages(
		&mut self,
		em: &mut Emitter,
		stages: &[WriteStageKind],
		skip: Label,
	) -> Result<(), IrError>;
}

/// How a write body is entered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct WriteEntry {
	/// The record in flight is made from the pipeline's row, a stored record its source read.
	pub(super) from_row: bool,
	/// The statement gate judges the record's table before any stage runs: a write whose subjects
	/// are targets takes it, while `RELATE` and `INSERT` ingest their subjects without it and let
	/// the creating order's own permission check answer.
	pub(super) gated: bool,
}

/// The stage sink of a write that may reach tables its statement does not name: each run of stages
/// is a site, applied to a record by the units that hold the stages as they lower for its table.
#[derive(Debug, Default)]
pub(super) struct SiteSink {
	/// The stages of each run, in the order the body reached them: the index of a run is the
	/// number of its [`WriteInstr::DispatchTable`] site.
	pub(super) sites: Vec<Vec<WriteStageKind>>,
}

impl StageSink for SiteSink {
	fn stages(
		&mut self,
		em: &mut Emitter,
		stages: &[WriteStageKind],
		skip: Label,
	) -> Result<(), IrError> {
		let site = u16::try_from(self.sites.len())
			.map_err(|_| IrError::Capacity("runs of stages in a write body"))?;
		self.sites.push(stages.to_vec());
		em.emit(Instr::Write(WriteInstr::DispatchTable {
			site,
			skip,
		}));
		Ok(())
	}
}

/// The stages of `run` at `positions`, which name stage ops of the subject loop's body.
fn order(run: &[WriteStageKind], positions: &[u16]) -> Result<Vec<WriteStageKind>, IrError> {
	positions
		.iter()
		.map(|at| {
			run.get(usize::from(*at)).copied().ok_or(IrError::Stale(
				"ir_lower::write_stmt: a position beyond the stages of a write",
			))
		})
		.collect()
}

/// The runtime policy of a failed creating attempt.
fn policy(retry: WriteRetry) -> RetryPolicy {
	RetryPolicy {
		savepoint_after: usize::from(retry.savepoint_after),
		retry_conflict: retry.retry_conflict,
		skip_failures: retry.skip_failures,
		carry_schema_failure: retry.carry_schema_failure,
	}
}

fn write(instr: WriteInstr) -> Instr {
	Instr::Write(instr)
}

/// The labels and instruction indices of the cut a write body is emitted with.
struct CutMarks {
	/// Where a candidate that stopped at the cut continues.
	cut: Label,
	/// Where a candidate that stopped at the skip continues.
	skip: Label,
	/// The index of the body's first instruction, the `WriteResume`.
	resume_at: usize,
}

/// Emits the body that writes the record in flight through the orders `body` names, the stages of
/// which are the ops `run` lists, and ends the row.
///
/// The stages are emitted by `sink`. Answers [`IrError::Stale`] for a position of `body` that
/// `run` has no stage at. The body is emitted whole: see [`write_body_cut`] for a write the plan
/// cuts.
pub(super) fn write_body(
	em: &mut Emitter,
	body: &WriteBody,
	run: &[WriteStageKind],
	entry: WriteEntry,
	sink: &mut dyn StageSink,
) -> Result<(), IrError> {
	write_body_cut(em, body, run, entry, sink, None).map(|_| ())
}

/// The pipeline op that yields the rows of the write `write`.
fn rows_pipeline_of(ir: &Ir, write: &OpData) -> Option<OpId> {
	let rows = write.operands.first()?;
	match ir.value(*rows)?.def {
		ValueDef::OpResult {
			op,
			..
		} => Some(op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// The cut the placement of the pipeline that yields the rows of the write `write` states, where
/// the pipeline fans out.
pub(super) fn cut_of_rows(ir: &Ir, write: &OpData) -> Option<WriteCut> {
	ir.placement(rows_pipeline_of(ir, write)?)?.write_cut()
}

/// [`write_body`] with the `cut` the plan states for the write, where it has one. Answers whether
/// the body holds the markers of a write that fans out.
///
/// With a `cut`, the body of a write of one order that starts its pipeline is emitted in two
/// halves: the stages ahead of `cut.stage` run on any task, the `WriteCut` ends them, and the
/// stages after it run for the candidate a `WriteResume`, the body's first instruction, installs.
/// The plan states a cut only for such a body, and a cut for any other is
/// [`IrError::Invariant`]. Without a `cut` the body is emitted whole.
pub(super) fn write_body_cut(
	em: &mut Emitter,
	body: &WriteBody,
	run: &[WriteStageKind],
	entry: WriteEntry,
	sink: &mut dyn StageSink,
	cut: Option<WriteCut>,
) -> Result<bool, IrError> {
	let skip = em.label()?;
	let done = em.label()?;
	let start = em.here() as usize;
	if cut.is_some() && (start != 0 || body.branching != WriteBranching::Sole) {
		return Err(IrError::Invariant {
			what: "ir_lower::write_stmt: a write cut for a body that is not the pipeline's first code of one order",
		});
	}
	let marks = match cut {
		Some(_) => {
			let cut = em.label()?;
			let skip = em.label()?;
			let resume_at = em.emit(Instr::Ir(IrInstr::write_resume(WriteResumeSpec {
				cut,
				skip,
				live: SmallVec::new(),
			}))) as usize;
			Some(CutMarks {
				cut,
				skip,
				resume_at,
			})
		}
		None => None,
	};
	if entry.from_row {
		em.emit(write(WriteInstr::Record));
	}
	if entry.gated {
		em.emit(write(WriteInstr::Gate {
			skip,
		}));
	}
	em.emit(write(WriteInstr::Begin));
	let first = order(run, &body.first)?;
	let second = order(run, &body.second)?;
	let mut cut_at = None;
	match body.branching {
		WriteBranching::Sole => match (&marks, cut) {
			(Some(marks), Some(cut)) => {
				let at = body.first.iter().position(|position| *position == cut.stage).ok_or(
					IrError::Invariant {
						what: "ir_lower::write_stmt: a write cut at a stage its order does not run",
					},
				)?;
				sink.stages(em, &first[..at], skip)?;
				cut_at = Some(emit_cut_marker(em, marks)?);
				sink.stages(em, &first[at..], skip)?;
			}
			_ => sink.stages(em, &first, skip)?,
		},
		// A view row is written by its view's maintenance, which runs the order of the action it
		// settled from the unit of the view table's order ([`view_row_sites`]); no statement's
		// body writes one.
		WriteBranching::ByAction => {
			return Err(IrError::Invariant {
				what: "ir_lower::write_stmt: a statement body that writes a view row",
			});
		}
		WriteBranching::UpFront => {
			let new = em.label()?;
			em.emit(write(WriteInstr::LoadNamedSubject));
			em.emit(write(WriteInstr::JumpIfNew {
				target: new,
			}));
			sink.stages(em, &second, skip)?;
			em.emit(Instr::Jump {
				target: done,
			});
			em.bind(new)?;
			sink.stages(em, &first, skip)?;
		}
		// A policy that neither retries nor skips raises every failure of the creating attempt,
		// which fails the statement, so the attempt has nothing of its own to undo: it runs as a
		// sole order does, with no savepoint. What a failed statement wrote is undone with its
		// transaction, refused with a transaction its client owns, or rolled back by the caller
		// that catches its error ([`crate::exec::catch_scope`]). An aggregate that reads a value's
		// failure about the shape of the data as `NONE` never reads so a value whose evaluation ran
		// a statement nested in it: that failure fails the statement holding it.
		WriteBranching::Retry(retry)
			if !retry.retry_conflict && !retry.skip_failures && !retry.carry_schema_failure =>
		{
			let read = em.label()?;
			em.emit(write(WriteInstr::JumpIfNotIterationInitial {
				target: read,
			}));
			sink.stages(em, &first, skip)?;
			em.emit(Instr::Jump {
				target: done,
			});
			// A record its source read has nothing to create.
			em.bind(read)?;
			sink.stages(em, &second, skip)?;
		}
		WriteBranching::Retry(retry) => {
			let read = em.label()?;
			em.emit(write(WriteInstr::JumpIfNotIterationInitial {
				target: read,
			}));
			// Stages ahead of the savepoint are not part of the attempt and are not undone by
			// rolling it back.
			let split = usize::from(retry.savepoint_after).min(first.len());
			sink.stages(em, &first[..split], skip)?;
			em.emit(write(WriteInstr::SavepointPush));
			let attempt = em.label()?;
			em.emit(write(WriteInstr::TryBegin {
				handler: attempt,
			}));
			let abandoned = em.label()?;
			sink.stages(em, &first[split..], abandoned)?;
			em.emit(write(WriteInstr::TryEnd));
			em.emit(write(WriteInstr::SavepointRelease));
			em.emit(Instr::Jump {
				target: done,
			});
			// An attempt that abandoned the record keeps what it wrote once the record reached its
			// store, and none of it before then.
			em.bind(abandoned)?;
			em.emit(write(WriteInstr::TryEnd));
			em.emit(write(WriteInstr::SavepointEndSkipped));
			em.emit(Instr::Jump {
				target: skip,
			});
			// A failed attempt is undone before anything else looks at the transaction, and an
			// error is classified: a retry reads the record it names afresh unless the statement
			// ran out of time in between. A signal leaves with what the attempt wrote.
			em.bind(attempt)?;
			em.emit(write(WriteInstr::SavepointRollback));
			em.emit(write(WriteInstr::ClearMutated));
			em.emit(write(WriteInstr::RethrowSignal));
			em.emit(write(WriteInstr::ClassifyCreateFailure {
				policy: policy(retry),
				skip,
			}));
			em.emit(write(WriteInstr::JumpIfDone {
				target: skip,
			}));
			em.emit(write(WriteInstr::ReloadForUpdate));
			em.emit(write(WriteInstr::CarryBegin));
			let carried = em.label()?;
			update_attempt(em, sink, &second, carried)?;
			em.emit(write(WriteInstr::CarryEnd));
			em.emit(Instr::Jump {
				target: done,
			});
			// An update that abandoned the record before storing it did not satisfy the failure the
			// attempt carried; one that stored it and then withheld its output did.
			em.bind(carried)?;
			em.emit(write(WriteInstr::RaiseCarried));
			em.emit(write(WriteInstr::CarryEnd));
			em.emit(Instr::Jump {
				target: skip,
			});
			// A record its source read has nothing to create.
			em.bind(read)?;
			update_attempt(em, sink, &second, skip)?;
		}
	}
	em.bind(done)?;
	em.emit(write(WriteInstr::End {
		skipped: false,
	}));
	em.emit(Instr::EndRow);
	em.bind(skip)?;
	if let Some(marks) = &marks {
		emit_skip(em.code_mut());
		em.bind(marks.skip)?;
	}
	em.emit(write(WriteInstr::End {
		skipped: true,
	}));
	em.emit(Instr::EndRow);
	if let (Some(marks), Some(cut_at)) = (&marks, cut_at) {
		settle_live(em, marks, start, cut_at, cut.is_some_and(|cut| cut.encode_at_cut));
	}
	Ok(marks.is_some())
}

/// Emits `stages`, the updating order of a form that retries, as an attempt behind a savepoint of
/// its own, so an update that stops part-way leaves none of its writes behind while the statement
/// goes on: the record is stored before its index entries and its observers run. A failure rolls
/// the attempt back and goes on to the enclosing handler or out of the statement. A skip ends the
/// savepoint as [`WriteInstr::SavepointEndSkipped`] decides and jumps to `skip`.
fn update_attempt(
	em: &mut Emitter,
	sink: &mut dyn StageSink,
	stages: &[WriteStageKind],
	skip: Label,
) -> Result<(), IrError> {
	em.emit(write(WriteInstr::SavepointPush));
	let attempt = em.label()?;
	em.emit(write(WriteInstr::TryBegin {
		handler: attempt,
	}));
	let abandoned = em.label()?;
	sink.stages(em, stages, abandoned)?;
	em.emit(write(WriteInstr::TryEnd));
	em.emit(write(WriteInstr::SavepointRelease));
	let kept = em.label()?;
	em.emit(Instr::Jump {
		target: kept,
	});
	em.bind(abandoned)?;
	em.emit(write(WriteInstr::TryEnd));
	em.emit(write(WriteInstr::SavepointEndSkipped));
	em.emit(Instr::Jump {
		target: skip,
	});
	em.bind(attempt)?;
	em.emit(write(WriteInstr::SavepointRollback));
	em.emit(write(WriteInstr::ClearMutated));
	em.emit(write(WriteInstr::Rethrow));
	em.bind(kept)?;
	Ok(())
}

/// Emits the `WriteCut` that ends the halves ahead of the cut, answers its index, and binds the
/// label a candidate that stopped there continues at to the instruction after it. The registers it
/// carries are settled once the body is emitted ([`settle_live`]).
fn emit_cut_marker(em: &mut Emitter, marks: &CutMarks) -> Result<usize, IrError> {
	let at = em.here() as usize;
	emit_cut(
		em.code_mut(),
		&WriteCutEmit {
			cut: marks.cut,
			skip: marks.skip,
			live: SmallVec::new(),
			encode: false,
		},
	);
	em.bind(marks.cut)?;
	Ok(at)
}

/// Gives the `WriteResume` and the `WriteCut` of a body emitted from `start`, cut at the
/// instruction `cut_at`, the registers written ahead of the cut that the code after it reads.
fn settle_live(em: &mut Emitter, marks: &CutMarks, start: usize, cut_at: usize, encode: bool) {
	let registers = em.register_count();
	let emitted = WriteCutEmit::scanned(
		&em.code()[start..],
		cut_at - start,
		registers,
		marks.cut,
		marks.skip,
	);
	let code = em.code_mut();
	code[marks.resume_at] = Instr::Ir(IrInstr::write_resume(WriteResumeSpec {
		cut: marks.cut,
		skip: marks.skip,
		live: emitted.live.clone(),
	}));
	code[cut_at] = Instr::Ir(IrInstr::write_cut(WriteCutSpec {
		live: emitted.live,
		encode,
	}));
}

/// The pipeline of a write body over `source`: [`write_body`], then the pipeline it ends.
///
/// A write the plan does not cut runs serially: its own source produces its records, and the
/// pipeline takes no decision from the physical plan ([`write_pipeline_cut`] is the pipeline of a
/// write it does).
pub(super) fn write_pipeline(
	em: &mut Emitter,
	body: &WriteBody,
	run: &[WriteStageKind],
	entry: WriteEntry,
	sink: &mut dyn StageSink,
	source: Source,
) -> Result<Pipeline, IrError> {
	write_body(em, body, run, entry, sink)?;
	em.finish_pipeline(source)
}

/// [`write_pipeline`] for a write the plan cuts: the body is emitted with the markers of `cut`,
/// and the pipeline takes the parallelism `parallel` the plan decided for it.
pub(super) fn write_pipeline_cut(
	em: &mut Emitter,
	body: &WriteBody,
	run: &[WriteStageKind],
	entry: WriteEntry,
	sink: &mut dyn StageSink,
	source: Source,
	(cut, parallel): (WriteCut, Parallel),
) -> Result<Pipeline, IrError> {
	write_body_cut(em, body, run, entry, sink, Some(cut))?;
	let mut pipeline = em.finish_pipeline(source)?;
	pipeline.parallel = parallel;
	Ok(pipeline)
}

/// One pipeline per run of `sites`, each the run's stages answering `true` when every stage kept
/// the record and `false` when one abandoned it. The pipelines read no source: the unit that holds
/// them runs each over the record a body's [`WriteInstr::DispatchTable`] hands it.
pub(super) fn site_pipelines(
	em: &mut Emitter,
	sites: &[Vec<WriteStageKind>],
	sink: &mut dyn StageSink,
) -> Result<Vec<Pipeline>, IrError> {
	let mut pipelines = Vec::with_capacity(sites.len());
	for stages in sites {
		let skip = em.label()?;
		sink.stages(em, stages, skip)?;
		for (kept, answer) in [(true, true), (false, false)] {
			if !kept {
				em.bind(skip)?;
			}
			let src = Operand::Const(em.constant(Value::Bool(answer))?);
			let dst = em.reg()?;
			em.emit(Instr::Load {
				dst,
				src,
			});
			em.emit(Instr::Emit {
				src: dst,
			});
			em.emit(Instr::EndRow);
		}
		pipelines.push(em.finish_pipeline(Source::Once)?);
	}
	Ok(pipelines)
}

/// The runs of stages a body of the orders `body` names over the stages `run` reaches, in the order
/// of the sites a [`SiteSink`] numbers them by. They follow from the body and the stages alone, so
/// the write that dispatches them and the bundle of each table it reaches number them alike.
pub(super) fn sites_of(
	body: &WriteBody,
	run: &[WriteStageKind],
) -> Result<Vec<Vec<WriteStageKind>>, IrError> {
	let mut em = Emitter::new();
	let mut sink = SiteSink::default();
	let entry = WriteEntry {
		from_row: false,
		gated: false,
	};
	write_body(&mut em, body, run, entry, &mut sink)?;
	Ok(sink.sites)
}

/// The runs of stages of the write of a view row, whose body is `body` over the stages `run`: the
/// order of each action a view's maintenance settles: create, update and delete, in that order,
/// which is the order the maintenance numbers the pipelines of the view table's order by.
fn view_row_sites(
	body: &WriteBody,
	run: &[WriteStageKind],
) -> Result<Vec<Vec<WriteStageKind>>, IrError> {
	[Action::Create, Action::Update, Action::Delete]
		.into_iter()
		.map(|action| view_row_order(body, run, action))
		.collect()
}

/// The stage tables of `write`, a write that names no one table, whose statement is `statement`:
/// no unit of its own, and the stages of each site loaded by the table a record is written to, the
/// bundle the run takes from the compile cache ([`crate::exec::vm::fault::serve_stage_bundle`]).
/// A write that may change the catalog takes each bundle again for the catalog version the
/// record's write context reads.
fn tables_by_name(
	write: &WriteAttrs,
	statement: &Arc<crate::expr::logical::LogicalWrite>,
	run: &[WriteStageKind],
) -> Result<StageTables, IrError> {
	let sites = sites_of(&write.body, run)?
		.into_iter()
		.map(|stages| stages.into_iter().map(stage_id).collect())
		.collect();
	Ok(StageTables {
		tables: Vec::new(),
		default: None,
		reach: crate::exec::vm::dispatch::Reach::any(),
		by_name: Some(Arc::new(crate::exec::vm::units::StageSetUnit::new(
			Arc::clone(statement),
			sites,
		))),
		revalidates: !write.stored.catalog_fixed,
		linked: None,
	})
}

/// The stage set of a write whose stages are linked, whose statement is `statement` and whose
/// orders, over the stages `run`, are `body`: the statement with its subjects erased
/// ([`crate::exec::vm::units::StageSetUnit::linked`]) and the stages of each site its body
/// numbers, which the bundle of its table is compiled for and kept under.
pub(crate) fn stage_set(
	statement: &crate::expr::logical::LogicalWrite,
	body: &WriteBody,
	run: &[WriteStageKind],
) -> Result<crate::exec::vm::units::StageSetUnit, IrError> {
	let sites = sites_of(body, run)?
		.into_iter()
		.map(|stages| stages.into_iter().map(stage_id).collect())
		.collect();
	Ok(crate::exec::vm::units::StageSetUnit::linked(statement, sites))
}

/// The table and stage set of each write of `ir` whose stages are linked, which the compile driver
/// resolves the bundle of before the container is lowered ([`LowerCx::link_stages`]).
pub(crate) fn linked_stage_sets(
	ir: &Ir,
) -> Result<Vec<(surrealdb_strand::TableName, crate::exec::vm::units::StageSetUnit)>, IrError> {
	let mut sets = Vec::new();
	let mut failed = None;
	ir.walk(ir.top(), crate::exec::ir::core::ir::WalkOrder::PreOrder, &mut |op| {
		let Some(OpKind::Phys(PhysOp::Write(attrs))) = ir.op(op).map(|data| &data.kind) else {
			return crate::exec::ir::core::ir::Walk::Continue;
		};
		if let (Some(linked), Some(statement)) = (&attrs.write.linked, &attrs.write.statement) {
			match stage_set(statement, &attrs.write.body, &linked.run) {
				Ok(set) => sets.push((linked.table.clone(), set)),
				Err(e) => failed = Some(e),
			}
		}
		crate::exec::ir::core::ir::Walk::Continue
	});
	match failed {
		Some(e) => Err(e),
		None => Ok(sets),
	}
}

/// The stage tables of the write `write` states: the bundle the compile linked for a write whose
/// stages are linked, by name for a write that names no one table, and none for one whose stages
/// are inline.
fn tables_of(
	cx: &LowerCx<'_>,
	write: &WriteAttrs,
	run: &[WriteStageKind],
) -> Result<Option<StageTables>, IrError> {
	let Some(statement) = &write.statement else {
		return Ok(None);
	};
	let Some(linked) = &write.linked else {
		return tables_by_name(write, statement, run).map(Some);
	};
	let set = stage_set(statement, &write.body, run)?;
	let bundle = cx.linked_stages(&linked.table, &set).ok_or(IrError::Invariant {
		what: "ir_lower::write_stmt: a write whose linked stages the compile did not resolve",
	})?;
	Ok(Some(StageTables {
		tables: Vec::new(),
		default: None,
		reach: crate::exec::vm::dispatch::Reach::any(),
		by_name: None,
		revalidates: false,
		linked: Some(crate::exec::vm::program::LinkedBundle {
			table: linked.table.clone(),
			bundle,
		}),
	}))
}

/// The write a stage bundle's container `ir` binds: the write the region of the statement its top
/// region holds holds, or `None` where the top region holds no statement of a write.
pub(super) fn bundled_write(ir: &Ir) -> Option<OpId> {
	crate::exec::ir::core::walk::ops_in(ir, ir.top()).find_map(|op| {
		let data = ir.op(op)?;
		let OpKind::Phys(PhysOp::Statement {
			..
		}) = &data.kind
		else {
			return None;
		};
		data.regions.iter().find_map(|region| super::dispatch::write_of(ir, *region))
	})
}

/// Lowers the container `ir` of a stage bundle: the one write its top region holds, bound for the
/// one table the bundle is for, as one pipeline per site its body reaches, in site order, each
/// answering whether the stages kept the record ([`site_pipelines`]). The write's source is not
/// lowered: the write that dispatches to the bundle hands it each record. The program states the
/// write's [`WriteSpec`], which its stages read.
///
/// The order of a view table's rows is lowered the same way from the write of a view row its
/// container binds, one pipeline per action ([`view_row_sites`]): the view's maintenance hands it
/// each row it settled.
pub(super) fn lower_stage_bundle(cx: &mut LowerCx<'_>, ir: &Ir) -> Result<Lowered, IrError> {
	let write = bundled_write(ir).ok_or(IrError::NotYet {
		what: "ir_lower::write_stmt: a stage bundle whose write is not its top statement",
	})?;
	let data = ir.op(write).ok_or(IrError::Stale("ir_lower::write_stmt: the write is gone"))?;
	let OpKind::Phys(PhysOp::Write(attrs)) = &data.kind else {
		return Err(IrError::Stale("ir_lower::write_stmt: the op of a write is no phys.write"));
	};
	if attrs.write.statement.is_some() {
		return Err(IrError::Invariant {
			what: "ir_lower::write_stmt: a stage bundle whose write names no one table",
		});
	}
	let run = StageRun::of(ir, data, cx.reads_checked())?;
	let view_row = attrs.write.body.branching == WriteBranching::ByAction;
	let sites = match view_row {
		true => view_row_sites(&attrs.write.body, &run.stages)?,
		false => sites_of(&attrs.write.body, &run.stages)?,
	};
	let mut em = Emitter::new();
	let (pipelines, stage_needs) = {
		let mut stages = Stages::new(&run, ir, cx.limits(), cx.registry()).in_context(cx);
		let pipelines = site_pipelines(&mut em, &sites, &mut stages)?;
		(pipelines, stages.needs.take())
	};
	let (shape, shape_needs) = {
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		let shape = write_shape(ir, data, &mut HostPlaces::new(&mut lowerer))?;
		(shape, lowerer.needs())
	};
	let spec = WriteSpec {
		form: match view_row {
			true => crate::exec::operators::write::plan::WriteForm::View,
			false => plan_form(attrs.write.form),
		},
		shape,
		output: attrs.write.output,
		only: attrs.write.only,
		tables: None,
	};
	let needs = writing(match stage_needs {
		Some(held) => shape_needs.merged(&held),
		None => shape_needs,
	});
	let mut program = em.finish_program(pipelines, needs.access_mode)?;
	program.write = Some(spec);
	super::dispatch::apply_needs(&mut program, &needs);
	Ok(Lowered {
		program,
		provenance: Provenance::default(),
		needs: cx.take_needed(),
	})
}

/// What lowering one write statement produced: its pipelines, the clauses its run reads, and
/// what the code of the pipeline and of the host pipelines it holds needs of the program.
pub(super) struct LoweredWrite {
	/// The pipelines that run each record through the body.
	pub(super) pipelines: WritePipelines,
	/// The statement's form, clauses and output.
	pub(super) spec: WriteSpec,
	/// What the program needs: a transaction that writes, at least a database context, and what
	/// the host pipelines of the subjects, the stages and the places need.
	pub(super) needs: Needs,
}

/// The pipelines of a write statement.
// `WritePipelines` is a stack-only return value of `lower_write`, consumed
// once to assemble the lowered `Unit`.
#[allow(clippy::large_enum_variant)]
pub(super) enum WritePipelines {
	/// The one pipeline over the subjects the write names.
	Own(Pipeline),
	/// The pipelines of the read the write's rows come from, with the pools they number: the last
	/// of the read's ends in the body of the write, and the pipeline of the records an `UPSERT`
	/// guarantees follows it.
	Over(Reads),
}

/// The value the subject loop of `write` takes its subjects from: the targets, the endpoints or the
/// rows of an `INSERT`, as the statement named them.
///
/// Answers [`IrError::Stale`] for a write whose first operand is not the result of a write loop,
/// or of the pipeline that holds one.
fn subjects_of(ir: &Ir, write: &OpData) -> Result<ValueId, IrError> {
	write_loop(ir, write)
		.and_then(|looped| ir.op(looped))
		.and_then(|looped| looped.operands.first().copied())
		.ok_or(IrError::Stale("ir_lower::write_stmt: the subjects of a write are no write loop"))
}

/// Lowers the write `write`, which names its subjects (its targets, its endpoints or the rows of
/// an `INSERT`) and one table, as the one pipeline of its program.
///
/// The pipeline's source is built from the subject loop's operand, its body from the write's
/// [`WriteBody`] over the stage ops of the loop, and the statement's clauses are read from the
/// stages' regions into the [`WriteSpec`]. Targets are judged by the statement gate before any
/// stage runs; `RELATE` and `INSERT` ingest their subjects without it. The write's `ONLY` and
/// `RETURN` are the spec's, and the output stage shows the images.
///
/// Whether the caller's reads are permission-checked is a fact of the frame the lowering is for:
/// a checked caller's images are cut for the fields it may select, never shown whole.
///
/// A write over the ids of one range that the plan cuts takes the cut and the decision of its
/// pipeline ([`write_pipeline_cut`]). A write over rows a read yields is lowered with the read
/// ([`lower_write_over_rows`]). Answers [`IrError::NotYet`] for the stages and clauses the stage
/// arms have no form for. A write to tables the statement does not name runs each run of stages
/// as a site ([`SiteSink`]).
pub(super) fn lower_write(
	cx: &mut LowerCx<'_>,
	em: &mut Emitter,
	ir: &Ir,
	write: OpId,
) -> Result<LoweredWrite, IrError> {
	let data = ir.op(write).ok_or(IrError::Stale("ir_lower::write_stmt: the write is gone"))?;
	let OpKind::Phys(PhysOp::Write(attrs)) = &data.kind else {
		return Err(IrError::Stale("ir_lower::write_stmt: the op of a write is no phys.write"));
	};
	let subjects = attrs.subjects;
	let gated = match subjects {
		WriteSubjectsShape::Targets => true,
		WriteSubjectsShape::Edges | WriteSubjectsShape::Inserts => false,
		WriteSubjectsShape::Rows | WriteSubjectsShape::Guaranteed => {
			return lower_write_over_rows(cx, em, ir, data, attrs, ir.top(), PoolBase::default());
		}
	};
	let named = subjects_of(ir, data)?;

	let (source, source_needs) = {
		let mut lowerer = Lowerer::in_context(em, ir, cx);
		let source = match subjects {
			WriteSubjectsShape::Targets => {
				targets_source(&mut lowerer, named, attrs.write.body.deferable, false)?
			}
			WriteSubjectsShape::Edges => edges_source(&mut lowerer, named)?,
			WriteSubjectsShape::Inserts => {
				let into = match (data.operands.as_slice(), attrs.write.into_text.as_deref()) {
					([_], None) => None,
					([_, into], Some(text)) => Some((*into, text)),
					_ => {
						return Err(IrError::Stale(
							"ir_lower::write_stmt: the INTO of an INSERT is a value and its text",
						));
					}
				};
				inserts_source(&mut lowerer, named, into, attrs.write.flags.relation)?
			}
			WriteSubjectsShape::Rows | WriteSubjectsShape::Guaranteed => {
				return Err(IrError::Stale("ir_lower::write_stmt: subjects past the check"));
			}
		};
		(source, lowerer.needs())
	};

	let entry = WriteEntry {
		from_row: false,
		gated,
	};
	// A write whose stages are linked holds no stage op: its stages are those it states.
	let run_stages = match &attrs.write.linked {
		Some(linked) => linked.run.to_vec(),
		None => StageRun::of(ir, data, cx.reads_checked())?.stages,
	};
	let (pipeline, body_needs) = match &attrs.write.statement {
		Some(_) => {
			let mut sites = SiteSink::default();
			let pipeline =
				write_pipeline(em, &attrs.write.body, &run_stages, entry, &mut sites, source)?;
			(pipeline, None)
		}
		None => {
			let run = StageRun::of(ir, data, cx.reads_checked())?;
			let mut stages = Stages::new(&run, ir, cx.limits(), cx.registry()).in_context(cx);
			// A write over the ids of a range, which the plan cuts, runs its compute half as
			// morsels under the decision the plan states for its pipeline.
			let placed = match (subjects, cut_of_rows(ir, data), rows_pipeline_of(ir, data)) {
				(WriteSubjectsShape::Targets, Some(cut), Some(pipeline)) => {
					Some((cut, parallel_of(ir, pipeline, None)?))
				}
				_ => None,
			};
			let pipeline = match placed {
				Some(placed) => write_pipeline_cut(
					em,
					&attrs.write.body,
					&run.stages,
					entry,
					&mut stages,
					source,
					placed,
				)?,
				None => {
					write_pipeline(em, &attrs.write.body, &run.stages, entry, &mut stages, source)?
				}
			};
			(pipeline, stages.needs.take())
		}
	};

	let (shape, shape_needs) = {
		let mut lowerer = Lowerer::in_context(em, ir, cx);
		let shape = write_shape(ir, data, &mut HostPlaces::new(&mut lowerer))?;
		(shape, lowerer.needs())
	};

	let spec = WriteSpec {
		form: plan_form(attrs.write.form),
		shape,
		output: attrs.write.output,
		only: attrs.write.only,
		tables: tables_of(cx, &attrs.write, &run_stages)?,
	};
	let mut needs = source_needs.merged(&shape_needs);
	if let Some(body) = body_needs {
		needs = needs.merged(&body);
	}
	Ok(LoweredWrite {
		pipelines: WritePipelines::Own(pipeline),
		spec,
		needs: writing(needs),
	})
}

/// [`lower_write`] for the write `write` of a statement nested in another program: the statement
/// `region` holds, whose pools number from `base`.
///
/// A write that names its subjects lowers as [`lower_write`] lowers it, its one pipeline numbering
/// no pool. A write over the rows a read yields lowers the read's pipelines over `region`, the
/// statement's own, rather than the container's top region, and numbers their pools from `base`;
/// the caller appends them to the program's at `base`.
pub(super) fn lower_write_in(
	cx: &mut LowerCx<'_>,
	em: &mut Emitter,
	ir: &Ir,
	write: OpId,
	region: RegionId,
	base: PoolBase,
) -> Result<LoweredWrite, IrError> {
	let data = ir.op(write).ok_or(IrError::Stale("ir_lower::write_stmt: the write is gone"))?;
	let OpKind::Phys(PhysOp::Write(attrs)) = &data.kind else {
		return Err(IrError::Stale("ir_lower::write_stmt: the op of a write is no phys.write"));
	};
	match attrs.subjects {
		WriteSubjectsShape::Rows | WriteSubjectsShape::Guaranteed => {
			lower_write_over_rows(cx, em, ir, data, attrs, region, base)
		}
		WriteSubjectsShape::Targets | WriteSubjectsShape::Edges | WriteSubjectsShape::Inserts => {
			lower_write(cx, em, ir, write)
		}
	}
}

/// `needs` of a program that writes: a transaction that writes, at least in the database.
fn writing(mut needs: Needs) -> Needs {
	needs.access_mode = crate::exec::AccessMode::ReadWrite;
	needs.context = needs.context.max(ContextLevel::Database);
	needs
}

/// Why a write over the rows `reads` yields cannot be lowered, or `None` when it can.
///
/// The rows are the write's own to consume as they arrive, so the read holds nothing back from the
/// write: a paging clause of the program, or a sort or an aggregate the program owns, would hold
/// or drop the rows before the write saw them. What a nested statement of the read pages or holds
/// is in a group of its own, which the write reads only once that statement has run.
fn rows_held(reads: &Reads, group: Option<u16>) -> Option<&'static str> {
	let own = |at: usize, of: fn(&GroupSpec) -> std::ops::Range<u16>| {
		!reads
			.groups
			.iter()
			.any(|nested| u16::try_from(at).is_ok_and(|at| of(nested).contains(&at)))
	};
	let pages = reads.limits.iter().any(|spec| spec.group == group);
	let holds = (0..reads.sorts.len()).any(|at| own(at, |nested| nested.sorts.clone()))
		|| (0..reads.aggregates.len()).any(|at| own(at, |nested| nested.aggregates.clone()));
	(pages || holds).then_some("ir_lower::write_stmt: a write over rows a read holds or pages")
}

/// Lowers the write `data`, a `phys.write` whose subjects are the rows a read yields, as the
/// pipelines of that read with the body of the write at the end of the last.
///
/// The read's pipelines are lowered over `region`, the region of the write's statement, with their
/// pools numbered from `base`. The one that ends in the write's loop runs each row through the
/// body, gated by the statement; an `UPSERT` that guarantees records adds the pipeline that writes
/// them, over the targets the write's other operands name, and shares the statement's one
/// [`WriteSpec`].
///
/// Answers [`IrError::Invariant`] for a read that pages its rows or holds them in a sort or an
/// aggregate of the program ([`rows_held`]).
fn lower_write_over_rows(
	cx: &mut LowerCx<'_>,
	em: &mut Emitter,
	ir: &Ir,
	data: &OpData,
	attrs: &PhysWriteAttrs,
	region: RegionId,
	base: PoolBase,
) -> Result<LoweredWrite, IrError> {
	let mut facts = FactCache::new();
	let mut reads = lower_pipelines_at(em, cx, ir, &mut facts, region, base)?;
	if let Some(what) = rows_held(&reads, em.group()) {
		return Err(IrError::Invariant {
			what,
		});
	}
	if reads.pipelines.is_empty() {
		return Err(IrError::Invariant {
			what: "ir_lower::write_stmt: a write over rows no pipeline runs",
		});
	}
	let mut needs = reads.needs.clone();

	if attrs.subjects == WriteSubjectsShape::Guaranteed {
		let targets = data.operands.get(1..).filter(|targets| !targets.is_empty()).ok_or(
			IrError::Stale("ir_lower::write_stmt: an UPSERT that guarantees names no target"),
		)?;
		let source = {
			let mut lowerer = Lowerer::in_context(em, ir, cx);
			let source = guaranteed_source(&mut lowerer, targets)?;
			needs = needs.merged(&lowerer.needs());
			source
		};
		let run = StageRun::of(ir, data, cx.reads_checked())?;
		let entry = WriteEntry {
			from_row: false,
			gated: true,
		};
		let (pipeline, stage_needs) = match &attrs.write.statement {
			Some(_) => {
				let mut sites = SiteSink::default();
				let pipeline =
					write_pipeline(em, &attrs.write.body, &run.stages, entry, &mut sites, source)?;
				(pipeline, None)
			}
			None => {
				let mut stages = Stages::new(&run, ir, cx.limits(), cx.registry()).in_context(cx);
				let pipeline =
					write_pipeline(em, &attrs.write.body, &run.stages, entry, &mut stages, source)?;
				(pipeline, stages.needs.take())
			}
		};
		if let Some(held) = stage_needs {
			needs = needs.merged(&held);
		}
		reads.append(pipeline);
	}

	let (shape, shape_needs) = {
		let mut lowerer = Lowerer::in_context(em, ir, cx);
		let shape = write_shape(ir, data, &mut HostPlaces::new(&mut lowerer))?;
		(shape, lowerer.needs())
	};
	let tables = match (&attrs.write.linked, &attrs.write.statement) {
		(Some(linked), Some(_)) => tables_of(cx, &attrs.write, &linked.run)?,
		(None, Some(_)) => {
			tables_of(cx, &attrs.write, &StageRun::of(ir, data, cx.reads_checked())?.stages)?
		}
		(_, None) => None,
	};
	let spec = WriteSpec {
		form: plan_form(attrs.write.form),
		shape,
		output: attrs.write.output,
		only: attrs.write.only,
		tables,
	};
	Ok(LoweredWrite {
		pipelines: WritePipelines::Over(reads),
		spec,
		needs: writing(needs.merged(&shape_needs)),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A sink that emits one site marker per run it is handed and keeps the runs.
	#[derive(Default)]
	struct Marking {
		runs: Vec<Vec<WriteStageKind>>,
	}

	impl StageSink for Marking {
		fn stages(
			&mut self,
			em: &mut Emitter,
			stages: &[WriteStageKind],
			skip: Label,
		) -> Result<(), IrError> {
			let site = self.runs.len() as u16;
			self.runs.push(stages.to_vec());
			em.emit(write(WriteInstr::DispatchTable {
				site,
				skip,
			}));
			Ok(())
		}
	}

	/// The run of stages the bodies below draw their orders from: the stages of an `UPSERT`'s
	/// creating attempt, then the ones only its updating order holds.
	fn run() -> Vec<WriteStageKind> {
		vec![
			WriteStageKind::CheckRecordUserAccess,
			WriteStageKind::GenerateRecordId,
			WriteStageKind::StoreRecordData,
			WriteStageKind::RecheckUpdatePermissions,
		]
	}

	fn body(branching: WriteBranching, first: &[u16], second: &[u16]) -> WriteBody {
		WriteBody {
			branching,
			first: first.into(),
			second: second.into(),
			deferable: false,
		}
	}

	fn lowered(body: &WriteBody, entry: WriteEntry) -> (Vec<Instr>, Vec<Vec<WriteStageKind>>) {
		let mut em = Emitter::new();
		let mut sink = Marking::default();
		let pipeline = match write_pipeline(&mut em, body, &run(), entry, &mut sink, Source::Once) {
			Ok(pipeline) => pipeline,
			Err(error) => panic!("the body lowers: {error}"),
		};
		(pipeline.code, sink.runs)
	}

	fn marker(site: u16, skip: Label) -> Instr {
		write(WriteInstr::DispatchTable {
			site,
			skip,
		})
	}

	fn end(skipped: bool) -> Instr {
		write(WriteInstr::End {
			skipped,
		})
	}

	#[test]
	fn a_sole_order_runs_its_stages_between_the_gate_and_the_ends() {
		let (code, runs) = lowered(
			&body(WriteBranching::Sole, &[0, 1, 2], &[]),
			WriteEntry {
				from_row: true,
				gated: true,
			},
		);
		assert_eq!(
			code,
			[
				write(WriteInstr::Record),
				write(WriteInstr::Gate {
					skip: 6
				}),
				write(WriteInstr::Begin),
				marker(0, 6),
				end(false),
				Instr::EndRow,
				end(true),
				Instr::EndRow,
			]
		);
		assert_eq!(
			runs,
			[vec![
				WriteStageKind::CheckRecordUserAccess,
				WriteStageKind::GenerateRecordId,
				WriteStageKind::StoreRecordData
			]]
		);
	}

	#[test]
	fn an_order_chosen_up_front_runs_the_updating_order_for_an_existing_record() {
		let (code, runs) = lowered(
			&body(WriteBranching::UpFront, &[0, 1], &[0, 3]),
			WriteEntry {
				from_row: false,
				gated: false,
			},
		);
		assert_eq!(
			code,
			[
				write(WriteInstr::Begin),
				write(WriteInstr::LoadNamedSubject),
				write(WriteInstr::JumpIfNew {
					target: 5
				}),
				marker(0, 8),
				Instr::Jump {
					target: 6
				},
				marker(1, 8),
				end(false),
				Instr::EndRow,
				end(true),
				Instr::EndRow,
			]
		);
		// The updating order is emitted first, and the creating order after the jump to it.
		assert_eq!(
			runs[0],
			[WriteStageKind::CheckRecordUserAccess, WriteStageKind::RecheckUpdatePermissions]
		);
		assert_eq!(
			runs[1],
			[WriteStageKind::CheckRecordUserAccess, WriteStageKind::GenerateRecordId]
		);
	}

	#[test]
	fn a_policy_that_neither_retries_nor_skips_runs_the_creating_order_without_a_savepoint() {
		let retry = WriteRetry {
			savepoint_after: 1,
			retry_conflict: false,
			skip_failures: false,
			carry_schema_failure: false,
		};
		let (code, runs) = lowered(
			&body(WriteBranching::Retry(retry), &[0, 1, 2], &[0, 3]),
			WriteEntry {
				from_row: false,
				gated: false,
			},
		);
		assert_eq!(
			code,
			[
				write(WriteInstr::Begin),
				write(WriteInstr::JumpIfNotIterationInitial {
					target: 4
				}),
				marker(0, 7),
				Instr::Jump {
					target: 5
				},
				marker(1, 7),
				end(false),
				Instr::EndRow,
				end(true),
				Instr::EndRow,
			]
		);
		assert_eq!(runs.len(), 2);
		assert_eq!(runs[0].len(), 3);
		assert_eq!(runs[1].len(), 2);
	}

	#[test]
	fn a_retried_creating_attempt_runs_behind_a_savepoint_and_falls_back_to_the_update() {
		let retry = WriteRetry {
			savepoint_after: 1,
			retry_conflict: true,
			skip_failures: false,
			carry_schema_failure: true,
		};
		let (code, runs) = lowered(
			&body(WriteBranching::Retry(retry), &[0, 1, 2], &[0, 3]),
			WriteEntry {
				from_row: false,
				gated: false,
			},
		);
		let skip = 50;
		assert_eq!(
			code,
			[
				write(WriteInstr::Begin),
				write(WriteInstr::JumpIfNotIterationInitial {
					target: 36
				}),
				marker(0, skip),
				write(WriteInstr::SavepointPush),
				write(WriteInstr::TryBegin {
					handler: 12
				}),
				marker(1, 9),
				write(WriteInstr::TryEnd),
				write(WriteInstr::SavepointRelease),
				Instr::Jump {
					target: 48
				},
				write(WriteInstr::TryEnd),
				write(WriteInstr::SavepointEndSkipped),
				Instr::Jump {
					target: skip
				},
				write(WriteInstr::SavepointRollback),
				write(WriteInstr::ClearMutated),
				write(WriteInstr::RethrowSignal),
				write(WriteInstr::ClassifyCreateFailure {
					policy: policy(retry),
					skip,
				}),
				write(WriteInstr::JumpIfDone {
					target: skip
				}),
				write(WriteInstr::ReloadForUpdate),
				write(WriteInstr::CarryBegin),
				write(WriteInstr::SavepointPush),
				write(WriteInstr::TryBegin {
					handler: 28
				}),
				marker(2, 25),
				write(WriteInstr::TryEnd),
				write(WriteInstr::SavepointRelease),
				Instr::Jump {
					target: 31
				},
				write(WriteInstr::TryEnd),
				write(WriteInstr::SavepointEndSkipped),
				Instr::Jump {
					target: 33
				},
				write(WriteInstr::SavepointRollback),
				write(WriteInstr::ClearMutated),
				write(WriteInstr::Rethrow),
				write(WriteInstr::CarryEnd),
				Instr::Jump {
					target: 48
				},
				write(WriteInstr::RaiseCarried),
				write(WriteInstr::CarryEnd),
				Instr::Jump {
					target: skip
				},
				write(WriteInstr::SavepointPush),
				write(WriteInstr::TryBegin {
					handler: 45
				}),
				marker(3, 42),
				write(WriteInstr::TryEnd),
				write(WriteInstr::SavepointRelease),
				Instr::Jump {
					target: 48
				},
				write(WriteInstr::TryEnd),
				write(WriteInstr::SavepointEndSkipped),
				Instr::Jump {
					target: skip
				},
				write(WriteInstr::SavepointRollback),
				write(WriteInstr::ClearMutated),
				write(WriteInstr::Rethrow),
				end(false),
				Instr::EndRow,
				end(true),
				Instr::EndRow,
			]
		);
		// The stages ahead of the savepoint, the rest of the attempt, then the updating order for
		// the carried retry and for a record its source read.
		assert_eq!(runs.len(), 4);
		assert_eq!(runs[0], [WriteStageKind::CheckRecordUserAccess]);
		assert_eq!(runs[1], [WriteStageKind::GenerateRecordId, WriteStageKind::StoreRecordData]);
		assert_eq!(runs[2], runs[3]);
	}

	#[test]
	fn a_savepoint_beyond_the_creating_order_takes_the_whole_order_ahead_of_it() {
		let retry = WriteRetry {
			savepoint_after: 9,
			retry_conflict: true,
			skip_failures: false,
			carry_schema_failure: false,
		};
		let (_, runs) = lowered(
			&body(WriteBranching::Retry(retry), &[0, 1], &[0, 3]),
			WriteEntry {
				from_row: false,
				gated: false,
			},
		);
		assert_eq!(runs[0].len(), 2);
		assert!(runs[1].is_empty());
	}

	#[test]
	fn a_position_beyond_the_stages_is_stale() {
		let mut em = Emitter::new();
		let mut sink = Marking::default();
		let entry = WriteEntry {
			from_row: false,
			gated: false,
		};
		let result =
			write_body(&mut em, &body(WriteBranching::Sole, &[7], &[]), &run(), entry, &mut sink);
		assert!(matches!(result, Err(IrError::Stale(_))));
	}

	#[test]
	fn the_sites_of_a_body_are_numbered_in_the_order_it_reaches_them() {
		let mut em = Emitter::new();
		let mut sink = SiteSink::default();
		let retry = WriteRetry {
			savepoint_after: 1,
			retry_conflict: true,
			skip_failures: false,
			carry_schema_failure: false,
		};
		let entry = WriteEntry {
			from_row: false,
			gated: false,
		};
		let result = write_body(
			&mut em,
			&body(WriteBranching::Retry(retry), &[0, 1, 2], &[0, 3]),
			&run(),
			entry,
			&mut sink,
		);
		assert!(result.is_ok());
		assert_eq!(sink.sites.len(), 4);
		let code = match em.finish_pipeline(Source::Once) {
			Ok(pipeline) => pipeline.code,
			Err(error) => panic!("the body finishes: {error}"),
		};
		let sites: Vec<u16> = code
			.iter()
			.filter_map(|instr| {
				let Instr::Write(WriteInstr::DispatchTable {
					site,
					..
				}) = instr
				else {
					return None;
				};
				Some(*site)
			})
			.collect();
		assert_eq!(sites, [0, 1, 2, 3]);
	}

	#[test]
	fn a_run_of_a_site_pipeline_answers_whether_every_stage_kept_the_record() {
		let mut em = Emitter::new();
		let mut sink = Marking::default();
		let sites = [vec![WriteStageKind::CheckRecordUserAccess]];
		let pipelines = match site_pipelines(&mut em, &sites, &mut sink) {
			Ok(pipelines) => pipelines,
			Err(error) => panic!("the sites lower: {error}"),
		};
		assert_eq!(pipelines.len(), 1);
		let code = &pipelines[0].code;
		// The marker, the `true` answer, then the `false` answer the abandoning stage jumps to.
		assert_eq!(code[0], marker(0, 4));
		assert_eq!(code.iter().filter(|instr| matches!(instr, Instr::Emit { .. })).count(), 2);
		assert_eq!(code.last(), Some(&Instr::EndRow));
	}

	/// The containers of a write statement below: the subject loop over `source`, whose body holds
	/// one stage op per stage, then the `phys.write` over the loop's result and `extra`.
	mod statement {
		use smallvec::SmallVec;
		use surrealdb_strand::TableName;

		use super::super::*;
		use crate::exec::ir::core::facts::FactCache;
		use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
		use crate::exec::ir::iface::statements::{OutputShape, WriteFlags, WriteForm};
		use crate::exec::ir::iface::types::TypeKnowledge;
		use crate::exec::ir::logical::ops::{
			GateClause, ImageView, LoopKind, RecurseAttrs, RecursePathForm, StepAttrs, WriteAttrs,
			WriteGates, WriteImage,
		};
		use crate::exec::ir::logical::stages::static_mark;
		use crate::exec::ir::phys::ops::{
			Batch, PhysWriteAttrs, PipelineAttrs, PipelineId, ProjectionShape, TableScanAttrs,
		};
		use crate::exec::operators::write::plan::WriteForm as PlanForm;
		use crate::exec::vm::ir_lower::pipe::tests::placed;
		use crate::exec::vm::ir_lower::tests::Builder;
		use crate::exec::vm::program::{InsertRowsSpec, Target, WriteOutput};
		use crate::exec::{AccessMode, ContextLevel};
		use crate::expr::part::Recurse;
		use crate::kvs::Direction;

		/// The stages of a `CREATE` of one record, over a table that stores nothing.
		const CREATE: [WriteStageKind; 5] = [
			WriteStageKind::CheckRecordUserAccess,
			WriteStageKind::CheckCreatePermissions,
			WriteStageKind::GenerateRecordId,
			WriteStageKind::StoreRecordData,
			WriteStageKind::OutputWrite,
		];

		pub(super) fn attrs(form: WriteForm, output: OutputShape) -> WriteAttrs {
			WriteAttrs {
				form,
				only: false,
				output,
				with: None,
				explain: None,
				flags: WriteFlags::default(),
				body: WriteBody {
					branching: WriteBranching::Sole,
					first: (0..CREATE.len() as u16).collect(),
					second: Box::new([]),
					deferable: form == WriteForm::Create,
				},
				gates: WriteGates {
					create: GateClause::Full,
					update: GateClause::Full,
					delete: GateClause::Full,
					select: GateClause::Full,
				},
				image: WriteImage {
					view: ImageView::Plain,
					keyed: false,
					id_default: false,
				},
				stored: Default::default(),
				derived: Default::default(),
				into_text: None,
				statement: None,
				linked: None,
			}
		}

		/// A write of `subjects` shape whose loop takes `source` and whose write takes `extra`
		/// after the loop's result, with the loop wrapped in the pipeline placement builds for it.
		/// Answers the write op.
		pub(super) fn container(
			b: &mut Builder,
			write: WriteAttrs,
			subjects: WriteSubjectsShape,
			source: ValueId,
			extra: &[ValueId],
		) -> OpId {
			build(b, write, subjects, source, extra, true)
		}

		/// [`container`], with the loop standing directly in the top region as binding leaves it.
		pub(super) fn bare_container(
			b: &mut Builder,
			write: WriteAttrs,
			subjects: WriteSubjectsShape,
			source: ValueId,
			extra: &[ValueId],
		) -> OpId {
			build(b, write, subjects, source, extra, false)
		}

		fn build(
			b: &mut Builder,
			write: WriteAttrs,
			subjects: WriteSubjectsShape,
			source: ValueId,
			extra: &[ValueId],
			wrapped: bool,
		) -> OpId {
			let pipeline = wrapped.then(|| {
				let pipeline = b.create_kind(
					OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
						id: PipelineId(0),
						edges: SmallVec::new(),
						logical: None,
					}))),
					&[],
					1,
				);
				b.open(pipeline, RegionKind::Pipeline, &[]);
				pipeline
			});
			let looped = b.add_phys_value(
				PhysOp::Loop {
					kind: LoopKind::Write,
					first: false,
					batch: Batch {
						max_iterations: 1,
						max_bytes: 1,
					},
					unroll: 1,
					value: false,
					step: None,
				},
				&[source],
			);
			b.open(looped, RegionKind::Loop, &[TypeKnowledge::Unknown]);
			for stage in CREATE {
				b.add_phys(
					PhysOp::WriteStage {
						stage,
						mark: static_mark(stage),
					},
					&[],
				);
			}
			let element = b.block_args()[0];
			b.yield_phys(element);
			b.close();
			let delivered = b.result_of(looped);
			let subjects_value = match pipeline {
				Some(pipeline) => {
					b.yield_phys(delivered);
					b.close();
					b.result_of(pipeline)
				}
				None => delivered,
			};
			let mut operands = vec![subjects_value];
			operands.extend_from_slice(extra);
			b.add_phys_value(
				PhysOp::Write(Box::new(PhysWriteAttrs {
					write,
					subjects,
				})),
				&operands,
			)
		}

		fn lowered(b: &Builder) -> Result<crate::exec::vm::program::Program, IrError> {
			super::super::super::dispatch::lower(&b.ir, &FactCache::new())
				.map(|lowered| lowered.program)
		}

		fn targets(b: &mut Builder) -> ValueId {
			let table = b.literal(Value::from("person"));
			b.add(crate::exec::ir::expr::ops::ExprOp::Array, &[table])
		}

		#[test]
		fn a_create_is_one_pipeline_over_its_targets_gated_before_its_stages() {
			let mut b = Builder::new();
			let source = targets(&mut b);
			container(
				&mut b,
				attrs(WriteForm::Create, OutputShape::After),
				WriteSubjectsShape::Targets,
				source,
				&[],
			);
			b.physical();
			let program = lowered(&b).expect("the write lowers");
			assert_eq!(program.pipelines.len(), 1);
			let pipeline = &program.pipelines[0];
			let Source::WriteTargets {
				targets,
				deferable,
				guarantees,
			} = &pipeline.source
			else {
				panic!("the source is the write's targets: {:?}", pipeline.source);
			};
			assert_eq!(targets.len(), 1);
			assert!(matches!(targets[0], Target::Scalar(_)));
			assert!(*deferable && !*guarantees);
			assert!(matches!(pipeline.code[0], Instr::Write(WriteInstr::Gate { .. })));
			assert!(matches!(pipeline.code[1], Instr::Write(WriteInstr::Begin)));
			let spec = program.write.as_ref().expect("a write program holds its spec");
			assert_eq!(spec.form, PlanForm::Create);
			assert_eq!(spec.output, OutputShape::After);
			assert!(!spec.only && spec.tables.is_none());
			assert_eq!(program.access_mode, AccessMode::ReadWrite);
			assert!(program.required_context >= ContextLevel::Database);
			assert!(!program.scalar && !program.rows);
		}

		#[test]
		fn a_loop_binding_left_unwrapped_lowers_as_the_wrapped_one_does() {
			let lower_with = |wrapped: bool| {
				let mut b = Builder::new();
				let source = targets(&mut b);
				let write = attrs(WriteForm::Create, OutputShape::After);
				if wrapped {
					container(&mut b, write, WriteSubjectsShape::Targets, source, &[]);
				} else {
					bare_container(&mut b, write, WriteSubjectsShape::Targets, source, &[]);
				}
				b.physical();
				lowered(&b).expect("the write lowers")
			};
			let (wrapped, bare) = (lower_with(true), lower_with(false));
			assert_eq!(wrapped.pipelines[0].code, bare.pipelines[0].code);
			assert_eq!(wrapped.write, bare.write);
		}

		/// A write in value position, whose subject loop the physical phase wraps in a pipeline,
		/// lowers as the write it is and not as a read of that pipeline's rows.
		#[test]
		fn a_write_in_value_position_lowers_as_the_write() {
			let mut b = Builder::new();
			let source = targets(&mut b);
			let write = container(
				&mut b,
				attrs(WriteForm::Create, OutputShape::After),
				WriteSubjectsShape::Targets,
				source,
				&[],
			);
			let value = b.result_of(write);
			b.yield_phys(value);
			b.physical();
			let program = match super::super::super::dispatch::lower_subquery_unit(
				&mut crate::exec::vm::ir_lower::units::LowerCx::detached(),
				&b.ir,
				&FactCache::new(),
			) {
				Ok(lowered) => lowered.program,
				Err(error) => panic!("the unit lowers: {error}"),
			};
			assert!(program.write.is_some());
			assert_eq!(program.access_mode, AccessMode::ReadWrite);
		}

		#[test]
		fn an_unspecified_return_states_no_output() {
			let mut b = Builder::new();
			let source = targets(&mut b);
			container(
				&mut b,
				attrs(WriteForm::Create, OutputShape::Default),
				WriteSubjectsShape::Targets,
				source,
				&[],
			);
			b.physical();
			let program = lowered(&b).expect("the write lowers");
			assert_eq!(program.write.expect("a spec").output, OutputShape::Default);
		}

		#[test]
		fn a_relate_reads_its_three_endpoints_and_takes_no_statement_gate() {
			let mut b = Builder::new();
			let from = b.literal(Value::from("a"));
			let through = b.literal(Value::from("knows"));
			let to = b.literal(Value::from("b"));
			let source = b.add(crate::exec::ir::expr::ops::ExprOp::Array, &[from, through, to]);
			container(
				&mut b,
				attrs(WriteForm::Relate, OutputShape::None),
				WriteSubjectsShape::Edges,
				source,
				&[],
			);
			b.physical();
			let program = lowered(&b).expect("the write lowers");
			let pipeline = &program.pipelines[0];
			assert!(matches!(pipeline.source, Source::WriteEdges { .. }));
			assert!(
				!pipeline
					.code
					.iter()
					.any(|instr| matches!(instr, Instr::Write(WriteInstr::Gate { .. })))
			);
			assert_eq!(program.write.expect("a spec").form, PlanForm::Relate);
		}

		#[test]
		fn an_insert_reads_its_rows_and_the_table_it_names() {
			let mut b = Builder::new();
			let rows = b.literal(Value::Object(crate::val::Object::default()));
			let into = b.literal(Value::from("person"));
			let mut write = attrs(WriteForm::Insert, OutputShape::After);
			write.into_text = Some("person".into());
			write.flags.relation = true;
			container(&mut b, write, WriteSubjectsShape::Inserts, rows, &[into]);
			b.physical();
			let program = lowered(&b).expect("the write lowers");
			let Source::WriteInserts {
				into,
				rows,
				relation,
			} = &program.pipelines[0].source
			else {
				panic!("the source is the write's rows");
			};
			assert_eq!(into.as_ref().map(|into| into.sql.as_str()), Some("person"));
			assert!(matches!(rows, InsertRowsSpec::Value(_)));
			assert!(*relation);
		}

		#[test]
		fn the_into_of_an_insert_is_a_value_and_its_text_or_neither() {
			let mut b = Builder::new();
			let rows = b.literal(Value::Object(crate::val::Object::default()));
			let into = b.literal(Value::from("person"));
			container(
				&mut b,
				attrs(WriteForm::Insert, OutputShape::After),
				WriteSubjectsShape::Inserts,
				rows,
				&[into],
			);
			b.physical();
			assert!(matches!(lowered(&b), Err(IrError::Stale(_))));
		}

		/// The rows of a scan of the table `person`, standing in the top region.
		fn scan(b: &mut Builder) -> ValueId {
			let op = b.create_kind(
				OpKind::Phys(PhysOp::TableScan(Box::new(TableScanAttrs {
					probes: None,
					bounds: None,
					prefilter: None,
					table: TableName::from("person"),
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

		#[test]
		fn an_update_of_the_rows_a_scan_yields_runs_the_body_over_each_row() {
			let mut b = Builder::new();
			let rows = scan(&mut b);
			container(
				&mut b,
				attrs(WriteForm::Update, OutputShape::After),
				WriteSubjectsShape::Rows,
				rows,
				&[],
			);
			let b = placed(b);
			let program = lowered(&b).expect("the write lowers");
			assert_eq!(program.pipelines.len(), 1);
			let pipeline = &program.pipelines[0];
			assert!(matches!(pipeline.source, Source::Scan { .. }));
			// The record is made from the row and judged by the statement gate before any stage
			// runs, and the pipeline hands the statement nothing but what the body ends with.
			assert!(matches!(pipeline.code[0], Instr::Write(WriteInstr::Record)));
			assert!(matches!(pipeline.code[1], Instr::Write(WriteInstr::Gate { .. })));
			assert!(matches!(pipeline.code[2], Instr::Write(WriteInstr::Begin)));
			assert!(
				!pipeline
					.code
					.iter()
					.any(|instr| matches!(instr, Instr::Emit { .. } | Instr::EmitEnd { .. }))
			);
			assert!(matches!(pipeline.code.last(), Some(Instr::StopPipeline)));
			assert_eq!(pipeline.parallel, crate::exec::planner::placement::Parallel::Serial);
			assert!(!pipeline.in_place);
			let spec = program.write.as_ref().expect("a write program holds its spec");
			assert_eq!(spec.form, PlanForm::Update);
			assert_eq!(program.access_mode, AccessMode::ReadWrite);
			assert!(program.required_context >= ContextLevel::Database);
		}

		#[test]
		fn a_delete_of_filtered_rows_filters_them_before_the_record_is_made() {
			let mut b = Builder::new();
			let source = scan(&mut b);
			let filter = b.create_kind(OpKind::Phys(PhysOp::Filter), &[source], 1);
			b.region(filter, RegionKind::Clause(ClauseRole::Where), |b| {
				b.add(
					crate::exec::ir::expr::ops::ExprOp::FreeParam {
						name: surrealdb_strand::Strand::from("keep"),
					},
					&[],
				)
			});
			let rows = b.result_of(filter);
			container(
				&mut b,
				attrs(WriteForm::Delete, OutputShape::Before),
				WriteSubjectsShape::Rows,
				rows,
				&[],
			);
			let b = placed(b);
			let program = lowered(&b).expect("the write lowers");
			assert_eq!(program.pipelines.len(), 1);
			let code = &program.pipelines[0].code;
			let record = code
				.iter()
				.position(|instr| matches!(instr, Instr::Write(WriteInstr::Record)))
				.expect("the record is made from the row");
			assert!(
				code[..record].iter().any(|instr| matches!(
					instr,
					Instr::Filter { .. } | Instr::JumpIf { .. } | Instr::FilterCompare { .. }
				)),
				"the filter runs ahead of the write: {code:?}"
			);
			assert_eq!(program.write.as_ref().map(|spec| spec.form), Some(PlanForm::Delete));
		}

		#[test]
		fn the_records_an_upsert_over_rows_guarantees_are_written_by_a_pipeline_after_the_read() {
			let mut b = Builder::new();
			let rows = scan(&mut b);
			let target = b.literal(Value::from("person:1"));
			container(
				&mut b,
				attrs(WriteForm::Upsert, OutputShape::After),
				WriteSubjectsShape::Guaranteed,
				rows,
				&[target],
			);
			let b = placed(b);
			let program = lowered(&b).expect("the write lowers");
			assert_eq!(program.pipelines.len(), 2);
			assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
			let Source::WriteTargets {
				targets,
				deferable,
				guarantees,
			} = &program.pipelines[1].source
			else {
				panic!("the guaranteed records are the write's targets");
			};
			assert_eq!(targets.len(), 1);
			assert!(!*deferable && *guarantees);
			// The guaranteed pipeline makes no record from a row, and the statement gate judges it
			// as it judges the rows.
			let code = &program.pipelines[1].code;
			assert!(matches!(code[0], Instr::Write(WriteInstr::Gate { .. })));
			assert!(!code.iter().any(|instr| matches!(instr, Instr::Write(WriteInstr::Record))));
			assert_eq!(program.write.as_ref().map(|spec| spec.form), Some(PlanForm::Upsert));
		}

		#[test]
		fn an_upsert_that_guarantees_no_target_is_stale() {
			let mut b = Builder::new();
			let rows = scan(&mut b);
			container(
				&mut b,
				attrs(WriteForm::Upsert, OutputShape::After),
				WriteSubjectsShape::Guaranteed,
				rows,
				&[],
			);
			let b = placed(b);
			assert!(matches!(lowered(&b), Err(IrError::Stale(_))));
		}

		#[test]
		fn a_read_that_pages_the_rows_of_a_write_is_refused() {
			let mut b = Builder::new();
			let source = scan(&mut b);
			let two = b.literal(Value::from(2_i64));
			let paged = b.create_kind(
				OpKind::Phys(PhysOp::Limit {
					limit: Some(1),
					start: None,
				}),
				&[source, two],
				1,
			);
			let rows = b.result_of(paged);
			container(
				&mut b,
				attrs(WriteForm::Update, OutputShape::After),
				WriteSubjectsShape::Rows,
				rows,
				&[],
			);
			let b = placed(b);
			assert!(matches!(
				lowered(&b),
				Err(IrError::Invariant { what }) if what.contains("holds or pages")
			));
		}

		#[test]
		fn a_write_over_rows_whose_loop_is_no_pipeline_is_an_invariant_break() {
			// Placement wraps every subject loop of a write in a pipeline, so a physical container
			// whose loop stands bare never leaves it: the verifier refuses the rows the bare loop
			// reads before the write is lowered.
			let mut b = Builder::new();
			let rows = scan(&mut b);
			bare_container(
				&mut b,
				attrs(WriteForm::Update, OutputShape::After),
				WriteSubjectsShape::Rows,
				rows,
				&[],
			);
			let b = placed(b);
			assert!(matches!(
				lowered(&b),
				Err(IrError::Verify(error)) if error.detail.contains("not their reader")
			));
		}

		#[test]
		fn a_write_whose_targets_are_read_from_a_bare_value_loop_is_one_program() {
			// Placement leaves a recursion standing bare in the top region, beside the write
			// whose targets it makes: the loop is a value the targets read, not a statement.
			let mut b = Builder::new();
			let start = b.literal(Value::from("person"));
			let looped = b.add_phys_value(
				PhysOp::Loop {
					kind: LoopKind::Recurse,
					first: false,
					batch: Batch {
						max_iterations: 1,
						max_bytes: 1,
					},
					unroll: 1,
					value: false,
					step: Some(Box::new(StepAttrs::Recurse(RecurseAttrs {
						depth: Recurse::Fixed(1),
						form: RecursePathForm::Absent,
						instruction: None,
					}))),
				},
				&[start],
			);
			b.open_with(
				looped,
				RegionKind::Loop,
				&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
				|b, args| {
					b.yield_phys(args[0]);
				},
			);
			let recursed = b.result_of(looped);
			let source = b.add(crate::exec::ir::expr::ops::ExprOp::Array, &[recursed]);
			container(
				&mut b,
				attrs(WriteForm::Update, OutputShape::After),
				WriteSubjectsShape::Targets,
				source,
				&[],
			);
			b.physical();
			let program = lowered(&b).expect("the write lowers");
			assert!(
				program
					.pipelines
					.iter()
					.chain(&program.host_code)
					.flat_map(|pipeline| &pipeline.code)
					.any(|instr| matches!(instr, Instr::Recurse { .. })),
				"the targets evaluate the recursion"
			);
		}

		#[test]
		fn a_write_over_rows_beside_another_statement_is_not_one_program() {
			let mut b = Builder::new();
			let rows = scan(&mut b);
			container(
				&mut b,
				attrs(WriteForm::Update, OutputShape::After),
				WriteSubjectsShape::Rows,
				rows,
				&[],
			);
			let one = b.literal(Value::from(1_i64));
			b.add_phys(PhysOp::Eval, &[one]);
			b.physical();
			assert!(matches!(
				lowered(&b),
				Err(IrError::NotYet { what }) if what.contains("more than one statement")
			));
		}

		#[test]
		fn a_return_of_fields_is_a_host_pipeline_the_output_stage_sets_as_the_output() {
			let mut b = Builder::new();
			let source = targets(&mut b);
			let write = container(
				&mut b,
				attrs(WriteForm::Create, OutputShape::Fields),
				WriteSubjectsShape::Targets,
				source,
				&[],
			);
			b.open_with(
				write,
				RegionKind::Clause(ClauseRole::ReturnClause),
				&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
				|b, args| {
					let project = b.add_phys_value(
						PhysOp::Project(Box::new(ProjectionShape::Value)),
						&[args[0]],
					);
					b.region(project, RegionKind::Clause(ClauseRole::Projection), |b| {
						b.literal(Value::from(1_i64))
					});
					let value = b.result_of(project);
					b.terminate(crate::exec::ir::expr::ops::ExprOp::Yield, &[value]);
				},
			);
			b.physical();
			let program = lowered(&b).expect("the write lowers");
			assert_eq!(program.host_code.len(), 1, "the projection is a host pipeline");
			let code = &program.pipelines[0].code;
			assert!(code.iter().any(|instr| matches!(
				instr,
				Instr::Write(WriteInstr::SetOutput {
					output: WriteOutput::Projection(0)
				})
			)));
			assert_eq!(program.write.expect("a spec").output, OutputShape::Fields);
		}

		#[test]
		fn a_write_whose_table_is_not_stated_is_not_lowered_yet() {
			let mut b = Builder::new();
			let source = targets(&mut b);
			let mut write = attrs(WriteForm::Create, OutputShape::After);
			write.gates = WriteGates::default();
			container(&mut b, write, WriteSubjectsShape::Targets, source, &[]);
			b.physical();
			assert!(matches!(lowered(&b), Err(IrError::NotYet { .. })));
		}

		#[test]
		fn a_container_of_two_statements_is_not_one_program() {
			let mut b = Builder::new();
			let source = targets(&mut b);
			container(
				&mut b,
				attrs(WriteForm::Create, OutputShape::After),
				WriteSubjectsShape::Targets,
				source,
				&[],
			);
			let one = b.literal(Value::from(1_i64));
			b.add_phys(PhysOp::Eval, &[one]);
			b.physical();
			assert!(matches!(
				lowered(&b),
				Err(IrError::NotYet { what }) if what.contains("more than one statement")
			));
		}

		#[test]
		fn a_write_stage_reached_as_a_statement_is_an_invariant_break() {
			let mut b = Builder::new();
			b.add_phys(
				PhysOp::WriteStage {
					stage: WriteStageKind::OutputWrite,
					mark: static_mark(WriteStageKind::OutputWrite),
				},
				&[],
			);
			b.physical();
			assert!(matches!(lowered(&b), Err(IrError::Invariant { .. })));
		}
	}
}
