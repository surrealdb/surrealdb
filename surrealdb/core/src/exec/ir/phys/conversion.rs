//! The conversion patterns from the logical dialect to the physical one.
//!
//! One pattern converts one logical tag. A pattern consults the physical catalog only through the
//! context the conversion driver hands it, and every logical op it consumes ends up in some
//! physical op's provenance or in the container's eliminated list. An expression derived from the
//! statement timestamp is accepted as an index range bound and stays a per-execution operand,
//! never a literal.
//!
//! Every pattern here is the same mechanic: [`build`] states the physical op a logical op becomes
//! from the logical op's attributes, its regions and the container around it, and [`apply`]
//! creates that op where the logical op stands, moves the logical op's regions into it (keeping
//! their kinds), and replaces the logical op's results by the new op's. The physical op carries the
//! logical op's operands, frame, snapshot and location. A pattern creates only physical ops.
//!
//! [`build`] answers `None` for an op shape the physical plan does not take. A pattern never
//! states a default for an attribute whose answer no source reachable from a pattern gives.
//!
//! `logical.source` is converted by [`SourcePattern`](super::access::SourcePattern), which decides
//! the access path. Every other logical tag has a physical twin that this module converts.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::OnceLock;

use smallvec::SmallVec;

use super::access::{SourcePattern, refuse_statement};
use super::access_regions::{Stands, value_region};
use super::ops::{
	AggregateAttrs, JoinAttrs, LayerAttrs, MatchStepAttrs, OmissionShape, PermissionFilterAttrs,
	PhysOp, PhysWriteAttrs, ProjectedFieldShape, ProjectionShape, SortAttrs as PhysSortAttrs,
	SortKeyReadShape, SortKeySpecShape, WalkActionShape, WalkStepShape, WriteSubjectsShape,
};
use super::placement::Placement;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::ir::cache::deps::{ValueDep, ValueDeps, ValueRef, ValueUse};
use crate::exec::ir::core::convert::{ConversionPattern, Converted};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::location::Location;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{DestructureEntry, ExprOp};
use crate::exec::ir::iface::catalog::PhysCx;
use crate::exec::ir::iface::effects::{ElemPath, Multiplicity};
use crate::exec::ir::iface::fold::FoldConsumer;
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::policy::LoopFacts;
use crate::exec::ir::iface::region_kind::{ClauseRole, ErrorScope, RegionKind, RegionKinds};
use crate::exec::ir::iface::statements::WriteForm;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::iface::{ApplyCut, ChooseSpec, Exit, LoopMode, LoopRefusal, SourceDelivery};
use crate::exec::ir::logical::ops::{
	AggregateShape, FieldShape, FieldsShape, LogOp, LogTag, LoopKind, LoopSink, ProjectAttrs,
	SortAttrs as LogSortAttrs, SortShape,
};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::logical::stages::write_loop;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::planner::placement::{NestedUse, PhysicalFacts, SinkKind, SourceKind};
use crate::exec::planner::{CatalogWithin, output_field_path};
use crate::exec::vm::payload::{JoinBuild, JoinMatch, Refusal, Spill, ValuesRead};
use crate::expr::logical::{JoinKind, OutputPath, RelationAlias};
use crate::val::{Number, Value};

/// The conversion patterns, in the order they are registered: one per logical tag, in the tags'
/// own order. The source is converted by the access-path pattern, every other tag by the
/// attribute-for-attribute pattern of this module.
pub(crate) fn patterns() -> Vec<Box<dyn ConversionPattern>> {
	LogTag::ALL
		.iter()
		.copied()
		.map(|tag| {
			if tag == LogTag::Source {
				Box::new(SourcePattern) as Box<dyn ConversionPattern>
			} else {
				Box::new(TagPattern(tag)) as Box<dyn ConversionPattern>
			}
		})
		.collect()
}

/// The pattern that converts the logical tag it holds.
struct TagPattern(LogTag);

impl ConversionPattern for TagPattern {
	fn name(&self) -> &'static str {
		self.0.mnemonic()
	}

	fn source(&self) -> LogTag {
		self.0
	}

	fn convert(
		&self,
		op: OpId,
		rw: &mut Rewriter<'_>,
		cx: &PhysCx<'_>,
	) -> Result<Converted, IrError> {
		if locks_a_written_value(rw.ir(), op, self.0) {
			let refusal = Refusal::Query {
				message: crate::dbs::FOR_UPDATE_TARGETS_ERROR.to_string(),
			};
			return refuse_statement(rw, op, refusal);
		}
		if let Some(message) = locked_clause_refusal(rw.ir(), op, self.0) {
			let refusal = Refusal::Query {
				message: message.to_string(),
			};
			return refuse_statement(rw, op, refusal);
		}
		match plan(rw.ir(), op, self.0, cx)? {
			Some(planned) => apply(rw, op, planned),
			None => Ok(Converted::NotApplicable),
		}
	}
}

/// Whether `op`, converted by the pattern of `tag`, is a locked read of a value written in `FROM`:
/// a target that is no record id, which the planner refuses for the whole statement.
fn locks_a_written_value(ir: &Ir, op: OpId, tag: LogTag) -> bool {
	tag == LogTag::Values
		&& matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Logical(LogOp::Values {
				for_update: true,
				..
			}))
		)
}

/// The refusal of a clause over a locked read (`FOR UPDATE`) that the planner refuses for the
/// whole statement: a grouping or a split, whose rows are no records to lock, and a `LIMIT` over
/// more than one source, which stops reading before the later sources are locked.
fn locked_clause_refusal(ir: &Ir, op: OpId, tag: LogTag) -> Option<&'static str> {
	let limit = matches!(tag, LogTag::Limit);
	if !limit && !matches!(tag, LogTag::Aggregate | LogTag::Split) {
		return None;
	}
	let input = *ir.op(op)?.operands.first()?;
	let (locked, sources) = read_sources(ir, input, 0);
	if limit {
		(locked && sources > 1).then_some(crate::dbs::FOR_UPDATE_LIMIT_ERROR)
	} else {
		locked.then_some(crate::dbs::FOR_UPDATE_GROUP_ERROR)
	}
}

/// Whether the rows `value` holds are read from a locked source, and how many sources they are
/// read from: the sources beneath the ops and loops that carry them, a union counting each of its
/// inputs. A source is read in its logical form or, once converted, as its record scan or locked
/// value read.
fn read_sources(ir: &Ir, value: ValueId, depth: u8) -> (bool, usize) {
	if depth > 64 {
		return (false, 0);
	}
	let op = match ir.value(value).map(|data| data.def) {
		Some(ValueDef::OpResult {
			op,
			..
		}) => op,
		// The row a loop's body is entered with is a row of the loop's source.
		Some(ValueDef::BlockArg {
			block,
			index: 0,
		}) => {
			let Some(owner) = ir.block(block).and_then(|held| ir.parent_op(held.parent)) else {
				return (false, 0);
			};
			let looped = matches!(
				ir.op(owner).map(|data| &data.kind),
				Some(OpKind::Logical(LogOp::Loop { .. }) | OpKind::Phys(PhysOp::Loop { .. }))
			);
			let source = ir.op(owner).and_then(|data| data.operands.first().copied());
			return match source {
				Some(source) if looped => read_sources(ir, source, depth + 1),
				Some(_) | None => (false, 0),
			};
		}
		Some(ValueDef::BlockArg {
			..
		})
		| None => return (false, 0),
	};
	let Some(data) = ir.op(op) else {
		return (false, 0);
	};
	let source = match &data.kind {
		OpKind::Logical(LogOp::Source(attrs)) => Some(attrs.for_update),
		OpKind::Logical(LogOp::Values {
			for_update,
			..
		}) => Some(*for_update),
		OpKind::Phys(PhysOp::RecordScan(attrs)) => Some(attrs.for_update),
		OpKind::Phys(PhysOp::Values {
			read,
		}) => Some(matches!(read, ValuesRead::Locked)),
		OpKind::Logical(_) | OpKind::Phys(_) | OpKind::Expr(_) => None,
	};
	if let Some(locked) = source {
		return (locked, 1);
	}
	if matches!(data.kind, OpKind::Logical(LogOp::UnionAll) | OpKind::Phys(PhysOp::Union)) {
		return data.operands.iter().fold((false, 0), |(locked, count), input| {
			let (held, more) = read_sources(ir, *input, depth + 1);
			(locked || held, count + more)
		});
	}
	let carries = matches!(
		data.kind,
		OpKind::Logical(
			LogOp::Loop { .. }
				| LogOp::Filter
				| LogOp::Project(_)
				| LogOp::Sort(_)
				| LogOp::Window(_)
				| LogOp::Distinct
				| LogOp::Limit { .. }
				| LogOp::Split { .. }
				| LogOp::Aggregate(_)
		) | OpKind::Phys(
			PhysOp::Loop { .. }
				| PhysOp::Filter
				| PhysOp::Project(_)
				| PhysOp::Sort(_)
				| PhysOp::Window(_)
				| PhysOp::Distinct { .. }
				| PhysOp::Limit { .. }
				| PhysOp::Split { .. }
				| PhysOp::Aggregate(_)
				| PhysOp::Enforced { .. }
				| PhysOp::PermissionFilter(_)
				| PhysOp::Layer(_)
		)
	);
	match data.operands.first() {
		Some(input) if carries => read_sources(ir, *input, depth + 1),
		Some(_) | None => (false, 0),
	}
}

/// What converting one logical op creates.
struct Planned {
	kind: PhysOp,
	operands: SmallVec<[ValueId; 4]>,
	results: SmallVec<[TypeKnowledge; 2]>,
	frame: FrameId,
	snapshot: SnapshotId,
	loc: Location,
	/// The regions of the logical op that move to the new op, in order ([`kept_regions`]).
	regions: SmallVec<[RegionId; 2]>,
	/// The regions that follow them, in order: a region of the logical op that moves, or one built
	/// from the ops of another ([`Omitted`]).
	tail: Vec<Slot>,
}

/// One region of the physical op after the regions it keeps from the logical op.
enum Slot {
	/// A region of the logical op that moves to the new op.
	Move(RegionId),
	/// A region of kind `Clause(Projection)` built from the cone of `value` in the region `source`
	/// ([`value_region`]).
	Value {
		source: RegionId,
		/// The values of `source` that read the region's argument.
		document: Vec<ValueId>,
		/// The values of `source` that read `NONE`.
		absent: Vec<ValueId>,
		value: ValueId,
	},
}

/// What converting `op`, an op of tag `tag`, creates; `None` when the pattern does not apply to
/// the op.
fn plan(ir: &Ir, op: OpId, tag: LogTag, cx: &PhysCx<'_>) -> Result<Option<Planned>, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("op to convert"))?;
	let OpKind::Logical(log) = &data.kind else {
		return Ok(None);
	};
	if log.tag() != tag {
		return Ok(None);
	}
	let Some(kind) = build(ir, data, log, cx)? else {
		return Ok(None);
	};
	declared_regions_hold(ir, data, &kind)?;
	let regions = kept_regions(data, &kind);
	let tail = match (&kind, log) {
		(PhysOp::Project(shape), LogOp::Project(attrs))
			if shape
				.omissions()
				.iter()
				.any(|omission| matches!(omission, OmissionShape::Walk { .. })) =>
		{
			omission_tail(ir, data, attrs)?
		}
		_ => Vec::new(),
	};
	let results = data
		.results
		.iter()
		.map(|value| {
			ir.value(*value).map(|value| value.ty.clone()).ok_or(IrError::Stale("result value"))
		})
		.collect::<Result<_, _>>()?;
	Ok(Some(Planned {
		kind,
		operands: data.operands.clone(),
		results,
		frame: data.frame,
		snapshot: data.snapshot,
		loc: data.loc.clone(),
		regions,
		tail,
	}))
}

/// Creates the planned op where `op` stands, moves the regions of `op` into it and replaces `op`.
fn apply(rw: &mut Rewriter<'_>, op: OpId, planned: Planned) -> Result<Converted, IrError> {
	let Planned {
		kind,
		operands,
		results,
		frame,
		snapshot,
		loc,
		regions,
		tail,
	} = planned;
	rw.derived(&[op], |rw| {
		let new = rw.create(
			InsertPoint::Before(op),
			OpSpec {
				kind: OpKind::Phys(kind),
				operands,
				results,
				frame,
				snapshot,
				loc,
			},
		)?;
		for region in regions {
			rw.move_region(region, new)?;
		}
		for slot in tail {
			match slot {
				Slot::Move(region) => rw.move_region(region, new)?,
				Slot::Value {
					source,
					document,
					absent,
					value,
				} => {
					let stands = Stands {
						document: &document,
						absent: &absent,
					};
					value_region(
						rw,
						new,
						op,
						RegionKind::Clause(ClauseRole::Projection),
						source,
						&stands,
						value,
					)?;
				}
			}
		}
		let produced: SmallVec<[ValueId; 2]> =
			rw.ir().op(new).map(|data| data.results.clone()).ok_or(IrError::Stale("new op"))?;
		rw.replace_op(op, &produced)?;
		Ok(Converted::Replaced)
	})
}

// ---------------------------------------------------------------------------------------------
// The physical op of each logical op
// ---------------------------------------------------------------------------------------------

/// Checks that the regions the logical op holds are the regions the physical op `kind` declares,
/// for the shapes whose declaration the op's attributes state: an `IF` and a `FOR` by position, a
/// `MATCH` and its read stages by count, a group key per region, a window's key and argument
/// regions, and a write's return clause. The attributes are read from the regions the logical op
/// holds, so a mismatch is a binder that built regions of kinds the op does not declare, which is
/// an invariant break.
fn declared_regions_hold(ir: &Ir, data: &OpData, kind: &PhysOp) -> Result<(), IrError> {
	let what = if matches!(kind, PhysOp::If { .. }) {
		"phys.if region kinds"
	} else if matches!(
		kind,
		PhysOp::Loop {
			kind: LoopKind::For,
			..
		}
	) {
		"phys.loop region kinds"
	} else if matches!(kind, PhysOp::Match { .. }) {
		"phys.match region kinds"
	} else if matches!(kind, PhysOp::MatchStep(step) if matches!(step.as_ref(), MatchStepAttrs::Read { .. }))
	{
		"phys.match_step read regions"
	} else if matches!(kind, PhysOp::Aggregate(..)) {
		"phys.aggregate group key regions"
	} else if matches!(kind, PhysOp::Window(..)) {
		"phys.window key and argument regions"
	} else if matches!(kind, PhysOp::Write(..)) {
		"phys.write regions"
	} else {
		return Ok(());
	};
	let held: RegionKinds =
		data.regions.iter().filter_map(|region| ir.region(*region).map(|data| data.kind)).collect();
	if held == kind.region_kinds() {
		Ok(())
	} else {
		Err(invariant(&format!("a logical op's regions differ from the declaration of {what}")))
	}
}

/// The physical op the logical op `log` becomes, or `None` for a shape the physical plan does not
/// take. The match is exhaustive: a logical tag that gains no arm does not compile. `_cx` is the
/// physical catalog's context, which a pattern consults for the answers only the catalog gives.
fn build(ir: &Ir, data: &OpData, log: &LogOp, _cx: &PhysCx<'_>) -> Result<Option<PhysOp>, IrError> {
	let kind = match log {
		LogOp::Request {
			fragment,
			nested,
		} => PhysOp::Request {
			fragment: *fragment,
			nested: *nested,
		},
		LogOp::Txn {
			mode,
		} => PhysOp::Txn {
			mode: *mode,
		},
		LogOp::Stmt {
			commit,
			control,
		} => PhysOp::Stmt {
			commit: *commit,
			control: *control,
		},
		LogOp::Statement {
			changes_catalog,
			served,
		} => PhysOp::Statement {
			changes_catalog: *changes_catalog,
			served: *served,
			within: CatalogWithin::default(),
			raises_when_reached: false,
		},
		LogOp::Source(_) => return Ok(None),
		LogOp::Assign(attrs) => PhysOp::Assign(attrs.clone()),
		LogOp::Unset {
			place,
		} => PhysOp::Unset {
			place: place.clone(),
		},
		LogOp::Content {
			mode,
		} => PhysOp::Content {
			mode: *mode,
		},
		LogOp::Timeout => PhysOp::Timeout,
		LogOp::VersionScope => PhysOp::VersionScope,
		LogOp::Only => PhysOp::Only,
		LogOp::Explain {
			analyze,
			format,
		} => PhysOp::Explain {
			analyze: *analyze,
			format: *format,
		},
		LogOp::Values {
			for_update,
			written,
		} => {
			// A locked read of a value is the refusal the pattern raises before it builds.
			if *for_update {
				return Ok(None);
			}
			PhysOp::Values {
				read: ValuesRead::Expression {
					written: *written,
				},
			}
		}
		LogOp::Match {
			bindings,
		} => PhysOp::Match {
			bindings: bindings.clone(),
			stages: region_count(ir, data, RegionKind::Subquery)?,
		},
		LogOp::MatchRead {
			optional_group,
			patterns,
		} => PhysOp::MatchStep(Box::new(MatchStepAttrs::Read {
			optional_group: *optional_group,
			patterns: patterns.clone(),
			predicates: region_count(ir, data, RegionKind::Clause(ClauseRole::Where))?,
		})),
		LogOp::MatchUpdate(attrs) => {
			PhysOp::MatchStep(Box::new(MatchStepAttrs::Update((**attrs).clone())))
		}
		LogOp::MatchDelete {
			target,
			detach,
		} => PhysOp::MatchStep(Box::new(MatchStepAttrs::Delete {
			target: *target,
			detach: *detach,
		})),
		LogOp::MatchInsert(attrs) => {
			PhysOp::MatchStep(Box::new(MatchStepAttrs::Insert((**attrs).clone())))
		}
		LogOp::MatchOutput(shape) => {
			PhysOp::MatchStep(Box::new(MatchStepAttrs::Output((**shape).clone())))
		}
		LogOp::BindStep(shape) => {
			PhysOp::MatchStep(Box::new(MatchStepAttrs::Bind((**shape).clone())))
		}
		LogOp::Subquery => PhysOp::Subquery,
		LogOp::Write(attrs) => PhysOp::Write(Box::new(PhysWriteAttrs {
			write: (**attrs).clone(),
			subjects: write_subjects(
				ir,
				loop_subjects(ir, data),
				attrs.form,
				data.operands.len() > 1,
			),
		})),
		LogOp::DdlGuard {
			guard,
		} => PhysOp::DdlGuard {
			guard: guard.clone(),
		},
		LogOp::Define(attrs) => PhysOp::Define(attrs.clone()),
		LogOp::Remove(attrs) => PhysOp::Remove(attrs.clone()),
		LogOp::Alter(attrs) => PhysOp::Alter(attrs.clone()),
		LogOp::Rebuild(attrs) => PhysOp::Rebuild(attrs.clone()),
		LogOp::UnionAll => PhysOp::Union,
		LogOp::Join {
			kind,
			alias,
		} => match join(ir, data, *kind, alias)? {
			Some(join) => join,
			None => return Ok(None),
		},
		LogOp::Filter => PhysOp::Filter,
		LogOp::PermissionFilter {
			guard,
			computed,
		} => PhysOp::PermissionFilter(Box::new(PermissionFilterAttrs {
			guard: *guard,
			hides_access_paths: super::access::permission_drives_access(ir, data),
			computed: computed.as_deref().cloned(),
		})),
		LogOp::Split {
			targets,
		} => PhysOp::Split {
			targets: targets.clone(),
		},
		LogOp::Aggregate(shape) => aggregate(ir, data, shape)?,
		LogOp::Distinct => PhysOp::Distinct {
			spill: Spill::Spillable,
		},
		LogOp::Sort(attrs) => sort(ir, data, attrs),
		LogOp::Window(shape) => PhysOp::Window(shape.clone()),
		LogOp::Limit {
			limit,
			start,
		} => PhysOp::Limit {
			limit: *limit,
			start: *start,
		},
		LogOp::Project(attrs) => match project(ir, data, attrs)? {
			Some(project) => project,
			None => return Ok(None),
		},
		LogOp::Fetch => PhysOp::Fetch,
		LogOp::Eval => PhysOp::Eval,
		LogOp::Block => PhysOp::Block,
		LogOp::Let(attrs) => PhysOp::Let(attrs.clone()),
		LogOp::Return => PhysOp::Return,
		LogOp::Loop {
			kind,
			step,
			..
		} => {
			let placement = placement();
			let loop_facts = unknown_loop_facts();
			PhysOp::Loop {
				kind: *kind,
				first: step.as_deref().is_none_or(|step| !step.takes_clauses())
					&& data.regions.len() > 1,
				batch: placement.batch_of(*kind, &loop_facts, &unplaced_facts()),
				unroll: placement.unroll(&loop_facts),
				value: *kind == LoopKind::For && first_region_is(ir, data, RegionKind::ValueFor),
				step: step.clone(),
			}
		}
		LogOp::If => PhysOp::If {
			value: first_region_is(ir, data, RegionKind::ValueIf),
		},
		LogOp::Info {
			subject,
			structure,
		} => PhysOp::Info {
			subject: *subject,
			structure: *structure,
		},
		LogOp::Sleep => PhysOp::Sleep,
		LogOp::Use {
			target,
		} => PhysOp::Use {
			target: *target,
		},
		LogOp::Option {
			name,
			value,
		} => PhysOp::Option {
			name: name.clone(),
			value: *value,
		},
		LogOp::Access(attrs) => PhysOp::Access(attrs.clone()),
		LogOp::Kill => PhysOp::Kill,
		LogOp::Live(attrs) => PhysOp::Live(attrs.clone()),
		LogOp::Subscriber(attrs) => PhysOp::Subscriber(attrs.clone()),
		LogOp::Show(attrs) => PhysOp::Show(attrs.clone()),
		LogOp::Choose(spec) => choose(spec),
		LogOp::WriteStage {
			stage,
			mark,
		} => PhysOp::WriteStage {
			stage: *stage,
			mark: *mark,
		},
		LogOp::Events(attrs) => PhysOp::Events(attrs.clone()),
		LogOp::Yield => PhysOp::Yield,
		LogOp::Next => PhysOp::Next,
		LogOp::Break => PhysOp::Break,
	};
	Ok(Some(kind))
}

/// How many regions of `data` have kind `kind`, as the count an op's attribute states.
fn region_count(ir: &Ir, data: &OpData, kind: RegionKind) -> Result<u16, IrError> {
	u16::try_from(regions_of_kind(ir, data, kind)).map_err(|_| IrError::Capacity("op regions"))
}

/// Whether the first region of `data` has kind `kind`.
fn first_region_is(ir: &Ir, data: &OpData, kind: RegionKind) -> bool {
	data.regions
		.first()
		.and_then(|region| ir.region(*region))
		.is_some_and(|region| region.kind == kind)
}

/// How many regions of `data` have kind `kind`.
fn regions_of_kind(ir: &Ir, data: &OpData, kind: RegionKind) -> usize {
	data.regions
		.iter()
		.filter(|region| ir.region(**region).is_some_and(|region| region.kind == kind))
		.count()
}

/// Where a write's subjects come from.
///
/// An `INSERT` holds its values, a `RELATE` its three endpoints and a `CREATE` its targets. Any
/// other write names the records it changes: the targets the statement names are records the
/// write reads from storage for itself, and every other input is the rows a read yields. An
/// `UPSERT` over the rows of a read whose write names targets after them (`targets_after`)
/// guarantees the records those targets name when the read yields none. `subjects` is the
/// operand of the write's subject loop.
fn write_subjects(
	ir: &Ir,
	subjects: Option<ValueId>,
	form: WriteForm,
	targets_after: bool,
) -> WriteSubjectsShape {
	match form {
		WriteForm::Insert => WriteSubjectsShape::Inserts,
		WriteForm::Relate => WriteSubjectsShape::Edges,
		WriteForm::Create => WriteSubjectsShape::Targets,
		WriteForm::Update | WriteForm::Upsert | WriteForm::Delete => {
			if subjects.is_some_and(|subjects| named_targets(ir, subjects)) {
				WriteSubjectsShape::Targets
			} else if form == WriteForm::Upsert && targets_after {
				WriteSubjectsShape::Guaranteed
			} else {
				WriteSubjectsShape::Rows
			}
		}
	}
}

/// The operand of the subject loop of the write `write`, the value its subjects are taken from.
fn loop_subjects(ir: &Ir, write: &OpData) -> Option<ValueId> {
	let looped = write_loop(ir, write)?;
	ir.op(looped)?.operands.first().copied()
}

/// Whether `subjects` are targets the write reads for itself: the array of the targets a statement
/// names, the literal that array folded to, the rows of a subquery, each a record the write
/// names, or a written literal that is no array, which the write judges as a target and refuses
/// where it names no record, `NONE` included.
fn named_targets(ir: &Ir, subjects: ValueId) -> bool {
	let Some(op) = defining_op(ir, subjects) else {
		return false;
	};
	let Some(data) = ir.op(op) else {
		return false;
	};
	match &data.kind {
		OpKind::Expr(ExprOp::Array) => true,
		OpKind::Expr(ExprOp::Literal(literal)) => matches!(&**literal, Value::Array(_)),
		OpKind::Logical(LogOp::Subquery) | OpKind::Phys(PhysOp::Subquery) => true,
		OpKind::Logical(LogOp::Values {
			for_update: false,
			..
		})
		| OpKind::Phys(PhysOp::Values {
			read: ValuesRead::Expression {
				..
			},
		}) => data.operands.first().and_then(|value| defining_op(ir, *value)).is_some_and(|held| {
			matches!(
				ir.op(held).map(|held| &held.kind),
				Some(OpKind::Expr(ExprOp::Literal(literal))) if !matches!(&**literal, Value::Array(_))
			)
		}),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// The op whose result `value` is.
fn defining_op(ir: &Ir, value: ValueId) -> Option<OpId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => Some(op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// A join: the key pairs and the residual are the `Clause(Where)` regions, a left and a right key
/// region per pair and then the residual when one is present. The right side is the build side and
/// is read through no index lookup: [`super::join_lookup::run`] states a lookup afterwards, as a
/// region of the join. A join that is neither inner, left nor semi is not a physical join.
fn join(
	ir: &Ir,
	data: &OpData,
	kind: JoinKind,
	alias: &RelationAlias,
) -> Result<Option<PhysOp>, IrError> {
	let matching = match kind {
		JoinKind::Inner => JoinMatch::Inner,
		JoinKind::Left => JoinMatch::Left,
		JoinKind::Semi => JoinMatch::Semi,
		JoinKind::Anti => return Ok(None),
	};
	let clauses = regions_of_kind(ir, data, RegionKind::Clause(ClauseRole::Where));
	let keys = u16::try_from(clauses / 2).map_err(|_| IrError::Capacity("join key pairs"))?;
	Ok(Some(PhysOp::Join(Box::new(JoinAttrs {
		matching,
		build: JoinBuild::Right,
		spill: Spill::Spillable,
		alias: alias.name().to_string(),
		keys,
		residual: clauses % 2 == 1,
		lookup: None,
	}))))
}

/// An aggregation. One `ErrorScope(ShapeToNone)` region is a group key; with none, the grouping is
/// one group and its state is bounded.
fn aggregate(ir: &Ir, data: &OpData, shape: &AggregateShape) -> Result<PhysOp, IrError> {
	let groups = region_count(ir, data, RegionKind::ErrorScope(ErrorScope::ShapeToNone))?;
	Ok(PhysOp::Aggregate(Box::new(AggregateAttrs {
		groups,
		fields: shape.fields.clone(),
		names: shape.names.clone(),
		first_value: shape.first_value.clone(),
		spill: if groups == 0 {
			Spill::Bounded
		} else {
			Spill::Spillable
		},
	})))
}

/// A sort. A key the binder marks a path ([`SortKeyShape::path`]) whose `Clause(OrderKey)` region
/// reads a plain path of fields off the row is read as that path; every other key is computed from
/// its region, which the sort keeps ([`kept_regions`]). A sort whose only user is a limit with
/// literal bounds keeps only the rows the limit can reach.
///
/// [`SortKeyShape::path`]: crate::exec::ir::logical::ops::SortKeyShape::path
fn sort(ir: &Ir, data: &OpData, attrs: &LogSortAttrs) -> PhysOp {
	let (keys, random) = match &attrs.ordering {
		SortShape::Random => (Vec::new(), true),
		SortShape::Keys(keys) => (
			keys.iter()
				.zip(data.regions.iter())
				.map(|(key, region)| SortKeySpecShape {
					read: match key.path.then(|| super::rewrite::region_path(ir, *region)).flatten()
					{
						Some(path) => SortKeyReadShape::Path(path),
						None => SortKeyReadShape::Computed,
					},
					ascending: key.ascending,
					collate: key.collate,
					numeric: key.numeric,
				})
				.collect(),
			false,
		),
	};
	let keep = bounded_keep(ir, data);
	PhysOp::Sort(Box::new(PhysSortAttrs {
		keys,
		keep,
		pushdown: false,
		random,
		spill: Spill::of_sort(keep),
		tempfiles: attrs.tempfiles,
	}))
}

/// The number of rows the sort `sort` keeps: the sum of the literal limit and start of the limit
/// that is the only reader of its rows, `None` when no such limit bounds it.
fn bounded_keep(ir: &Ir, sort: &OpData) -> Option<usize> {
	let result = *sort.results.first()?;
	let [only] = ir.uses(result) else {
		return None;
	};
	if only.operand != 0 {
		return None;
	}
	let reader = ir.op(only.user)?;
	let (limit, start) = if let OpKind::Logical(LogOp::Limit {
		limit,
		start,
	}) = &reader.kind
	{
		(*limit, *start)
	} else if let OpKind::Phys(PhysOp::Limit {
		limit,
		start,
	}) = &reader.kind
	{
		(*limit, *start)
	} else {
		return None;
	};
	let count = |slot: u16| literal_count(ir, *reader.operands.get(usize::from(slot))?);
	let limit = count(limit?)?;
	match start {
		Some(start) => limit.checked_add(count(start)?),
		None => Some(limit),
	}
}

/// The non-negative integer the literal `value` holds.
fn literal_count(ir: &Ir, value: ValueId) -> Option<usize> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	if let OpKind::Expr(ExprOp::Literal(literal)) = &ir.op(op)?.kind
		&& let Value::Number(Number::Int(count)) = &**literal
	{
		usize::try_from(*count).ok()
	} else {
		None
	}
}

/// A projection. A resolver layer keeps its shape. Any other projection is stated field by field:
/// a field is landed at its alias or, unaliased, under the name the binder carried for it
/// ([`ProjectAttrs::names`]).
///
/// `None` for a column coerced to a kind, which the physical plan does not take. An omitted path
/// that is a path of fields and literal positions is an [`OmissionShape::Path`], whose
/// expression the shape does not keep ([`kept_regions`]). An omission that walks to the places it
/// removes (a filter, a destructure, a computed position) names its steps by regions the binder
/// does not build, so it has no answer here.
fn project(ir: &Ir, data: &OpData, attrs: &ProjectAttrs) -> Result<Option<PhysOp>, IrError> {
	if attrs.layer {
		return Ok(Some(PhysOp::Layer(Box::new(LayerAttrs {
			fields: attrs.fields.clone(),
			names: attrs.names.clone(),
			computed: attrs.computed.clone(),
			judged: attrs.judged.as_deref().cloned(),
		}))));
	}
	let omit = omissions(ir, data, attrs)?;
	let Some(mut shape) = projection_shape(&attrs.fields, &attrs.names, omit)? else {
		return Ok(None);
	};
	for &(field, key) in &attrs.sort_keys {
		if matches!(shape, ProjectionShape::Value) && field == 0 {
			shape = ProjectionShape::SortedValue(key);
			continue;
		}
		match &mut shape {
			ProjectionShape::Fields {
				fields,
				..
			}
			| ProjectionShape::Star {
				fields,
				..
			} => {
				let at = projected_position(&attrs.fields, field)
					.ok_or_else(|| invariant("a sort key shared with no projected field"))?;
				let projected = fields
					.get_mut(at)
					.ok_or_else(|| invariant("a sort key shared with no projected field"))?;
				projected.sort_key = Some(key);
			}
			// A projection that removes paths from its value evaluates the value again.
			ProjectionShape::All
			| ProjectionShape::Value
			| ProjectionShape::SortedValue(_)
			| ProjectionShape::OmittedValue {
				..
			} => {}
		}
	}
	Ok(Some(PhysOp::Project(Box::new(shape))))
}

/// The position among the fields of a projection's shape of the field at `field` of `fields`:
/// the shape lists each expression and leaves `*` out.
fn projected_position(fields: &FieldsShape, field: u16) -> Option<usize> {
	let FieldsShape::Select(list) = fields else {
		return None;
	};
	let field = usize::from(field);
	matches!(list.get(field), Some(FieldShape::Single { .. })).then(|| {
		list[..field].iter().filter(|held| matches!(held, FieldShape::Single { .. })).count()
	})
}

/// The shape of the projection of `fields`, which lands each unaliased field under the name
/// `names` carries for it ([`ProjectAttrs::names`]) and then removes the paths `omit` names.
///
/// `None` for a column coerced to a kind, which the physical plan does not take.
pub(crate) fn projection_shape(
	fields: &FieldsShape,
	names: &[OutputPath],
	omit: Vec<OmissionShape>,
) -> Result<Option<ProjectionShape>, IrError> {
	let shape = match fields {
		FieldsShape::Value {
			..
		} => {
			if omit.is_empty() {
				ProjectionShape::Value
			} else {
				ProjectionShape::OmittedValue {
					omit,
				}
			}
		}
		FieldsShape::Select(fields) => {
			if omit.is_empty() && matches!(&**fields, [FieldShape::All]) {
				ProjectionShape::All
			} else {
				let star = fields.iter().any(|field| matches!(field, FieldShape::All));
				let mut names = names.iter();
				let mut out = Vec::with_capacity(fields.len());
				for field in fields {
					match field {
						FieldShape::All => {}
						FieldShape::Single {
							alias,
							kind,
						} => {
							if kind.is_some() {
								return Ok(None);
							}
							let output = match alias {
								Some(alias) => output_field_path(alias),
								None => output_field_path(names.next().ok_or_else(|| {
									invariant("an unaliased projected field carries no name")
								})?),
							};
							out.push(ProjectedFieldShape {
								output,
								aliased: alias.is_some(),
								sort_key: None,
							});
						}
					}
				}
				if star {
					ProjectionShape::Star {
						fields: out,
						omit,
					}
				} else {
					ProjectionShape::Fields {
						fields: out,
						omit,
					}
				}
			}
		}
	};
	Ok(Some(shape))
}

/// The omissions of a projection: the paths each `OMIT` region names, which follow the field
/// regions.
fn omissions(ir: &Ir, data: &OpData, attrs: &ProjectAttrs) -> Result<Vec<OmissionShape>, IrError> {
	Ok(read_omissions(ir, data, attrs)?.into_iter().flat_map(|omitted| omitted.shapes).collect())
}

/// The regions of the omissions that walk, after the fields' regions: for each, the region of the
/// expression as written, then the regions of the steps and the action that evaluate a value, in
/// the order [`OmissionShape::Walk`] lists them.
fn omission_tail(ir: &Ir, data: &OpData, attrs: &ProjectAttrs) -> Result<Vec<Slot>, IrError> {
	let mut tail = Vec::new();
	for omitted in read_omissions(ir, data, attrs)? {
		if matches!(omitted.shapes.as_slice(), [OmissionShape::Walk { .. }]) {
			tail.push(Slot::Move(omitted.source));
			tail.extend(omitted.parts);
		}
	}
	Ok(tail)
}

/// What the expression of one `OMIT` region names.
struct Omitted {
	/// The region of the expression, which reads the row it is given.
	source: RegionId,
	/// The omissions it states: one path for each location a plain path names, or one walk.
	shapes: Vec<OmissionShape>,
	/// The regions a walk holds after its source, in order. None for a plain path.
	parts: Vec<Slot>,
}

/// The expressions of the `OMIT` regions of the projection `data`, read in order.
fn read_omissions(ir: &Ir, data: &OpData, attrs: &ProjectAttrs) -> Result<Vec<Omitted>, IrError> {
	data.regions
		.iter()
		.skip(super::field_regions(&attrs.fields))
		.take(usize::from(attrs.omit))
		.map(|region| read_omission(ir, *region))
		.collect()
}

/// One link of the path an `OMIT` expression names, from the row outward.
enum Link {
	/// `.name`, of the row or of the value before it.
	Field(String),
	/// `[n]`: a literal position, or the value that spells one.
	Index {
		literal: Option<usize>,
		at: ValueId,
		row: ValueId,
	},
	/// `.*`.
	Each,
	/// `[0]` at the front.
	First,
	/// `[$]` at the back.
	Last,
	/// `[WHERE p]`: the loop whose body judges the predicate of each element.
	Filter {
		body: RegionId,
		row: ValueId,
	},
	/// `.{ a, b }`, with the region of each aliased entry.
	Destructure {
		entries: Box<[DestructureEntry]>,
		aliases: Vec<RegionId>,
		row: ValueId,
	},
	/// A step that names no location.
	Nowhere,
}

/// What an `OMIT` walk removes where its links land.
enum Act {
	RemoveField(String),
	RemoveIndex(usize),
	RemoveIndexAt {
		at: ValueId,
		row: ValueId,
	},
	RemoveMatching {
		body: RegionId,
		row: ValueId,
	},
	RemoveFirst,
	RemoveLast,
	Clear,
	Destructure {
		entries: Box<[DestructureEntry]>,
		aliases: Vec<RegionId>,
		row: ValueId,
	},
	Nothing,
}

/// The single block's first argument and the value the `Clause` or `Loop` region `region` yields.
fn row_and_yield(ir: &Ir, region: RegionId) -> Option<(ValueId, ValueId)> {
	let block = *ir.region(region)?.blocks.first()?;
	let row = *ir.block(block)?.args.first()?;
	Some((row, region_yield(ir, region)?))
}

/// The value the body of `region` yields.
fn region_yield(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let last = ops_in(ir, region).last()?;
	match &ir.op(last)?.kind {
		OpKind::Expr(ExprOp::Yield)
		| OpKind::Logical(LogOp::Yield)
		| OpKind::Phys(PhysOp::Yield) => ir.op(last)?.operands.first().copied(),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// The loop kind of `data` when it is a loop of either dialect.
fn loop_kind_of(data: &OpData) -> Option<LoopKind> {
	match &data.kind {
		OpKind::Logical(LogOp::Loop {
			kind,
			..
		})
		| OpKind::Phys(PhysOp::Loop {
			kind,
			..
		}) => Some(*kind),
		OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => None,
	}
}

/// The links of the path `value` computes from `row`, appended to `out` from the row outward. A
/// value that is none of the steps of a path reaches nothing, so neither does anything built on
/// it ([`Link::Nowhere`]).
fn read_links(ir: &Ir, value: ValueId, row: ValueId, out: &mut Vec<Link>) {
	if value == row {
		return;
	}
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(value).map(|held| held.def)
	else {
		out.push(Link::Nowhere);
		return;
	};
	let Some(data) = ir.op(op) else {
		out.push(Link::Nowhere);
		return;
	};
	let base = |out: &mut Vec<Link>| match data.operands.first() {
		Some(base) => read_links(ir, *base, row, out),
		None => out.push(Link::Nowhere),
	};
	if let Some(kind) = loop_kind_of(data) {
		let Some(body) = data.regions.first().copied() else {
			out.push(Link::Nowhere);
			return;
		};
		match kind {
			LoopKind::ElementFilter => {
				base(out);
				out.push(Link::Filter {
					body,
					row,
				});
			}
			LoopKind::Broadcast => {
				base(out);
				match row_and_yield(ir, body) {
					Some((element, yielded)) => read_links(ir, yielded, element, out),
					None => out.push(Link::Nowhere),
				}
			}
			LoopKind::Select
			| LoopKind::Write
			| LoopKind::For
			| LoopKind::Closure
			| LoopKind::GraphStep
			| LoopKind::Recurse => out.push(Link::Nowhere),
		}
		return;
	}
	let OpKind::Expr(kind) = &data.kind else {
		out.push(Link::Nowhere);
		return;
	};
	if let ExprOp::Field {
		name,
	} = kind
		&& data.operands.first() == Some(&row)
	{
		out.push(Link::Field(name.as_str().to_owned()));
	} else if let ExprOp::Access {
		field,
	} = kind
	{
		base(out);
		out.push(Link::Field(field.as_str().to_owned()));
	} else if let ExprOp::Each = kind {
		base(out);
		out.push(Link::Each);
	} else if let ExprOp::First = kind {
		base(out);
		out.push(Link::First);
	} else if let ExprOp::Last = kind {
		base(out);
		out.push(Link::Last);
	} else if let ExprOp::Index = kind {
		base(out);
		match data.operands.get(1) {
			Some(at) => out.push(Link::Index {
				literal: literal_count(ir, *at),
				at: *at,
				row,
			}),
			None => out.push(Link::Nowhere),
		}
	} else if let ExprOp::Destructure {
		shape,
	} = kind
	{
		base(out);
		out.push(Link::Destructure {
			entries: shape.clone(),
			aliases: data.regions.to_vec(),
			row,
		});
	} else {
		out.push(Link::Nowhere);
	}
}

/// The walk the path `value` computes from `row` describes: the outermost step names what is
/// removed, and everything under it is the walk to where. A path that names no place removes
/// nothing.
fn read_walk(ir: &Ir, value: ValueId, row: ValueId) -> (Vec<Link>, Act) {
	let mut steps = Vec::new();
	if value == row {
		return (steps, Act::Nothing);
	}
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(value).map(|held| held.def)
	else {
		return (steps, Act::Nothing);
	};
	let Some(data) = ir.op(op) else {
		return (steps, Act::Nothing);
	};
	let base = |steps: &mut Vec<Link>| match data.operands.first() {
		Some(base) => read_links(ir, *base, row, steps),
		None => steps.push(Link::Nowhere),
	};
	if let Some(kind) = loop_kind_of(data) {
		let Some(body) = data.regions.first().copied() else {
			return (steps, Act::Nothing);
		};
		return match kind {
			LoopKind::ElementFilter => {
				base(&mut steps);
				(
					steps,
					Act::RemoveMatching {
						body,
						row,
					},
				)
			}
			// A broadcast applies its body to everything its base reaches, which is the base's
			// walk followed by the body's own.
			LoopKind::Broadcast => {
				base(&mut steps);
				match row_and_yield(ir, body) {
					Some((element, yielded)) => {
						let (inner, act) = read_walk(ir, yielded, element);
						steps.extend(inner);
						(steps, act)
					}
					None => (steps, Act::Nothing),
				}
			}
			LoopKind::Select
			| LoopKind::Write
			| LoopKind::For
			| LoopKind::Closure
			| LoopKind::GraphStep
			| LoopKind::Recurse => (steps, Act::Nothing),
		};
	}
	let OpKind::Expr(kind) = &data.kind else {
		return (steps, Act::Nothing);
	};
	if let ExprOp::Field {
		name,
	} = kind
		&& data.operands.first() == Some(&row)
	{
		(steps, Act::RemoveField(name.as_str().to_owned()))
	} else if let ExprOp::Access {
		field,
	} = kind
	{
		base(&mut steps);
		(steps, Act::RemoveField(field.as_str().to_owned()))
	} else if let ExprOp::Each = kind {
		base(&mut steps);
		(steps, Act::Clear)
	} else if let ExprOp::First = kind {
		base(&mut steps);
		(steps, Act::RemoveFirst)
	} else if let ExprOp::Last = kind {
		base(&mut steps);
		(steps, Act::RemoveLast)
	} else if let ExprOp::Index = kind {
		base(&mut steps);
		let Some(at) = data.operands.get(1).copied() else {
			return (steps, Act::Nothing);
		};
		match literal_count(ir, at) {
			Some(index) => (steps, Act::RemoveIndex(index)),
			None => (
				steps,
				Act::RemoveIndexAt {
					at,
					row,
				},
			),
		}
	} else if let ExprOp::Destructure {
		shape,
	} = kind
	{
		base(&mut steps);
		(
			steps,
			Act::Destructure {
				entries: shape.clone(),
				aliases: data.regions.to_vec(),
				row,
			},
		)
	} else {
		(Vec::new(), Act::Nothing)
	}
}

/// A step of a plain path.
enum Plain<'a> {
	Part(FieldPathPart),
	Destructure(&'a [DestructureEntry]),
}

/// The plain paths `links` name, each removed in turn, or `None` when some link is no plain step.
///
/// A destructure lists locations rather than naming one, so a path through it is the path through
/// each field it lists: `a.{ b, c.{ d } }.x` removes `a.b.x` and `a.c.d.x`. Every other step is a
/// field, a literal position, `[0]` or `[$]`. A filter, `.*`, a position known only once evaluated,
/// a step that names no location, and a destructure entry that empties a field, names an
/// expression or is a rest entry are no plain steps.
fn plain_paths(links: &[Link]) -> Option<Vec<FieldPath>> {
	if !matches!(links.first(), Some(Link::Field(_) | Link::Destructure { .. })) {
		return None;
	}
	let steps = links
		.iter()
		.map(|link| match link {
			Link::Field(name) => Some(Plain::Part(FieldPathPart::Field(name.clone()))),
			Link::Index {
				literal: Some(index),
				..
			} => Some(Plain::Part(FieldPathPart::Index(*index))),
			Link::First => Some(Plain::Part(FieldPathPart::First)),
			Link::Last => Some(Plain::Part(FieldPathPart::Last)),
			Link::Destructure {
				entries,
				..
			} => Some(Plain::Destructure(entries)),
			Link::Index {
				literal: None,
				..
			}
			| Link::Each
			| Link::Filter {
				..
			}
			| Link::Nowhere => None,
		})
		.collect::<Option<Vec<_>>>()?;
	let mut out = Vec::new();
	expand_plain(&mut Vec::with_capacity(steps.len()), &steps, &mut out)?;
	Some(out)
}

/// Appends to `out` each plain path `steps` expands to beneath `prefix`; `None` where a destructure
/// entry is no plain path.
fn expand_plain(
	prefix: &mut Vec<FieldPathPart>,
	steps: &[Plain<'_>],
	out: &mut Vec<FieldPath>,
) -> Option<()> {
	let Some((step, rest)) = steps.split_first() else {
		out.push(FieldPath(prefix.clone()));
		return Some(());
	};
	match step {
		Plain::Part(part) => {
			prefix.push(part.clone());
			expand_plain(prefix, rest, out)?;
			prefix.pop();
		}
		Plain::Destructure(entries) => {
			for entry in *entries {
				match entry {
					DestructureEntry::Field(name) => {
						prefix.push(FieldPathPart::Field(name.as_str().to_owned()));
						expand_plain(prefix, rest, out)?;
					}
					DestructureEntry::Destructure(name, nested) => {
						prefix.push(FieldPathPart::Field(name.as_str().to_owned()));
						let mut steps = Vec::with_capacity(rest.len() + 1);
						steps.push(Plain::Destructure(nested));
						steps.extend(rest.iter().map(|step| match step {
							Plain::Part(part) => Plain::Part(part.clone()),
							Plain::Destructure(entries) => Plain::Destructure(entries),
						}));
						expand_plain(prefix, &steps, out)?;
					}
					DestructureEntry::All(_)
					| DestructureEntry::Aliased(_)
					| DestructureEntry::Rest => return None,
				}
				prefix.pop();
			}
		}
	}
	Some(())
}

/// Reads the expression of the `OMIT` region `source`: the plain paths it names, or the walk to
/// the places it removes.
fn read_omission(ir: &Ir, source: RegionId) -> Result<Omitted, IrError> {
	let (row, value) = row_and_yield(ir, source).ok_or(IrError::Stale("an OMIT path's region"))?;
	let mut links = Vec::new();
	read_links(ir, value, row, &mut links);
	if let Some(paths) = plain_paths(&links) {
		return Ok(Omitted {
			source,
			shapes: paths.into_iter().map(OmissionShape::Path).collect(),
			parts: Vec::new(),
		});
	}
	let (steps, act) = read_walk(ir, value, row);
	let mut parts = Vec::new();
	let mut shaped = Vec::with_capacity(steps.len());
	for step in &steps {
		shaped.push(match step {
			Link::Field(name) => WalkStepShape::Field(name.clone()),
			Link::Index {
				literal: Some(index),
				..
			} => WalkStepShape::Index(*index),
			Link::Index {
				literal: None,
				at,
				row: held,
			} => {
				parts.push(value_slot(source, &[*held, row], &[], *at));
				WalkStepShape::IndexAt
			}
			Link::Each => WalkStepShape::Each,
			Link::First => WalkStepShape::First,
			Link::Last => WalkStepShape::Last,
			Link::Filter {
				body,
				row: held,
			} => {
				parts.push(predicate_slot(ir, source, *body, *held, row)?);
				WalkStepShape::Matching
			}
			Link::Destructure {
				entries,
				aliases,
				row: held,
			} => {
				alias_slots(ir, source, entries, aliases, *held, row, &mut parts)?;
				WalkStepShape::Destructure(entries.clone())
			}
			Link::Nowhere => WalkStepShape::Nowhere,
		});
	}
	let action = match &act {
		Act::RemoveField(name) => WalkActionShape::RemoveField(name.clone()),
		Act::RemoveIndex(index) => WalkActionShape::RemoveIndex(*index),
		Act::RemoveIndexAt {
			at,
			row: held,
		} => {
			parts.push(value_slot(source, &[*held, row], &[], *at));
			WalkActionShape::RemoveIndexAt
		}
		Act::RemoveMatching {
			body,
			row: held,
		} => {
			parts.push(predicate_slot(ir, source, *body, *held, row)?);
			WalkActionShape::RemoveMatching
		}
		Act::RemoveFirst => WalkActionShape::RemoveFirst,
		Act::RemoveLast => WalkActionShape::RemoveLast,
		Act::Clear => WalkActionShape::Clear,
		Act::Destructure {
			entries,
			aliases,
			row: held,
		} => {
			alias_slots(ir, source, entries, aliases, *held, row, &mut parts)?;
			WalkActionShape::Destructure(entries.clone())
		}
		Act::Nothing => WalkActionShape::Nothing,
	};
	Ok(Omitted {
		source,
		shapes: vec![OmissionShape::Walk {
			steps: shaped,
			action,
		}],
		parts,
	})
}

/// The region that evaluates `value`, a value of `source` computed from the row that the values
/// `document` stand for, over the document a walk applies it to; the values `absent` read `NONE`.
fn value_slot(source: RegionId, document: &[ValueId], absent: &[ValueId], value: ValueId) -> Slot {
	let mut distinct: Vec<ValueId> = Vec::with_capacity(document.len());
	for held in document {
		if !distinct.contains(held) {
			distinct.push(*held);
		}
	}
	Slot::Value {
		source,
		document: distinct,
		absent: absent.to_vec(),
		value,
	}
}

/// The region that judges the predicate of the element-filter loop body `body` over the element a
/// walk applies it to. The row the filter stands under and the row of the whole path are not the
/// element, and read `NONE`: a walk judges the predicate of an element over that element alone.
fn predicate_slot(
	ir: &Ir,
	source: RegionId,
	body: RegionId,
	row: ValueId,
	path_row: ValueId,
) -> Result<Slot, IrError> {
	let (element, yielded) =
		row_and_yield(ir, body).ok_or(IrError::Stale("an OMIT filter body"))?;
	let mut absent = vec![row];
	if path_row != row {
		absent.push(path_row);
	}
	Ok(value_slot(source, &[element], &absent, yielded))
}

/// The regions of the aliased entries of a destructure, one for each, in the order its entries
/// list them. A walk never evaluates an aliased entry, which names an expression and no location.
/// An entry's region reads the value destructured through its own argument, which stands for the
/// document as the destructure's row does.
fn alias_slots(
	ir: &Ir,
	source: RegionId,
	entries: &[DestructureEntry],
	aliases: &[RegionId],
	row: ValueId,
	path_row: ValueId,
	out: &mut Vec<Slot>,
) -> Result<(), IrError> {
	if aliases.len() != super::destructure_regions(entries) {
		return Err(invariant("a destructure holds a region for each aliased entry"));
	}
	for alias in aliases {
		let yielded =
			region_yield(ir, *alias).ok_or(IrError::Stale("an aliased entry's region"))?;
		let mut document = vec![row, path_row];
		if let Some((arg, _)) = row_and_yield(ir, *alias) {
			document.push(arg);
		}
		out.push(value_slot(source, &document, &[], yielded));
	}
	Ok(())
}

/// The regions of `data` that move into the physical op `kind`: all of them, except that a
/// projection keeps only the regions of its fields, which leaves out the regions of its omitted
/// paths. The regions of the omissions that walk follow them ([`omission_tail`]).
fn kept_regions(data: &OpData, kind: &PhysOp) -> SmallVec<[RegionId; 2]> {
	if let PhysOp::Project(shape) = kind {
		let held = kind.region_kinds().len() - super::omission_regions(shape.omissions());
		data.regions.iter().copied().take(held).collect()
	} else if let PhysOp::Sort(attrs) = kind
		&& !attrs.random
	{
		// A sort keeps the regions of its computed keys; a key read as a path holds none.
		attrs
			.keys
			.iter()
			.zip(data.regions.iter().copied())
			.filter(|(key, _)| matches!(key.read, SortKeyReadShape::Computed))
			.map(|(_, region)| region)
			.collect()
	} else {
		data.regions.clone()
	}
}

/// A choice carried into the physical dialect: the arms, guards and bodies of the logical choice.
///
/// A choice never nests a readiness choice. The access paths in its arms are chosen over the
/// online indexes of the bound version, so an index an arm names is ready
/// for every plan the choice is part of.
fn choose(spec: &ChooseSpec) -> PhysOp {
	PhysOp::Choose(Box::new(spec.clone()))
}

// ---------------------------------------------------------------------------------------------
// Placement inputs of a loop
// ---------------------------------------------------------------------------------------------

/// The placement every loop's bounds are taken from: the shipped default configuration. A pattern
/// receives only the physical catalog's context, so no other configuration reaches it.
fn placement() -> &'static Placement {
	static PLACEMENT: OnceLock<Placement> = OnceLock::new();
	PLACEMENT.get_or_init(Placement::default)
}

/// The verdict of a loop nothing is known of: one iteration at a time, no apply residue and no
/// exit, over a source read whole whose identities may repeat. The analysis of a loop needs the
/// fact cache, which a pattern does not reach.
fn unknown_loop_facts() -> LoopFacts {
	LoopFacts {
		mode: LoopMode::Serial(LoopRefusal::Unsplittable),
		apply: ApplyCut::None,
		exit: Exit::None,
		sink: LoopSink::Output,
		delivery: SourceDelivery::ReadWhole,
		multiplicity: Multiplicity::MayRepeat,
		key_path: ElemPath::Id,
		compute_reads_data: true,
		cut_at_marks: false,
	}
}

/// The physical facts of a loop that has no pipeline yet: nothing is known of its rows.
fn unplaced_facts() -> PhysicalFacts {
	PhysicalFacts {
		source: SourceKind::Other,
		enforce: false,
		writes: false,
		limits: 0,
		output: false,
		sink: SinkKind::Output,
		binds_row: false,
		fetches_row: false,
		distincts: 0,
		nested: NestedUse::None,
		host_blocks: false,
		paging_replayable: false,
		estimate: None,
	}
}

// ---------------------------------------------------------------------------------------------
// Value dependences
// ---------------------------------------------------------------------------------------------

/// The decision a physical op makes from a folded value, if it makes one: a dynamic target's
/// table, an index choice from a folded bound, a loop's unroll.
fn fold_consumer(kind: &PhysOp) -> Option<FoldConsumer> {
	if matches!(kind, PhysOp::Choose(..)) {
		Some(FoldConsumer::DynamicTargetTable)
	} else if matches!(
		kind,
		PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::IndexLookup(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::KnnScan(..)
			| PhysOp::Count(..)
	) {
		Some(FoldConsumer::IndexChoice)
	} else if matches!(kind, PhysOp::Loop { .. }) {
		Some(FoldConsumer::Unroll)
	} else {
		None
	}
}

/// The values known at plan time that the value `value` is computed from, appended to `named`:
/// the stored parameters and the principal variables at the leaves of its computation. Answers
/// `false` when a leaf has no name the dependence record admits: a literal, which the record
/// names by its position in the statement text that the container does not carry, and a free name,
/// which may resolve through the client's bindings and whose slot is not assigned here.
fn leaf_values(ir: &Ir, value: ValueId, named: &mut Vec<ValueRef>) -> bool {
	let mut complete = true;
	let mut seen = FxHashSet::default();
	let mut pending = vec![value];
	while let Some(at) = pending.pop() {
		if !seen.insert(at) {
			continue;
		}
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(at).map(|value| value.def)
		else {
			continue;
		};
		let Some(data) = ir.op(op) else {
			continue;
		};
		let OpKind::Expr(kind) = &data.kind else {
			continue;
		};
		if let ExprOp::Literal(..)
		| ExprOp::FreeParam {
			..
		} = kind
		{
			complete = false;
		} else if let ExprOp::CatalogParam {
			name,
		} = kind
		{
			named.push(ValueRef::Param(name.clone()));
		} else if let ExprOp::SessionParam {
			name,
		} = kind
		{
			named.push(ValueRef::Principal(name.clone()));
		} else {
			pending.extend(data.operands.iter().copied());
		}
	}
	complete
}

/// The dependences of the decisions the physical ops of `ir` made from folded values: for each
/// operand of an op that makes such a decision whose value the fold cache holds an outcome for, one
/// dependence per stored parameter or principal variable the operand is computed from, and whether
/// every value the operands are computed from was named.
///
/// A stored parameter is named by its name as [`ValueRef::Param`] and a principal variable as
/// [`ValueRef::Principal`]. A literal and a name that resolves through the request's bindings or
/// the session are not recorded: the record names them by slot and by position in the statement
/// text, and neither is known here. The answer is `false` when any such value was consumed, and
/// the record then lists only part of what the decisions depend on.
pub(crate) fn value_deps(ir: &Ir, facts: &FactCache) -> (ValueDeps, bool) {
	let mut deps = ValueDeps::new();
	let mut complete = true;
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		let OpKind::Phys(kind) = &data.kind else {
			return Walk::Continue;
		};
		let Some(consumer) = fold_consumer(kind) else {
			return Walk::Continue;
		};
		for operand in &data.operands {
			if facts.fold_outcome(*operand).is_none() {
				continue;
			}
			let mut named = Vec::new();
			complete &= leaf_values(ir, *operand, &mut named);
			for value in named {
				deps.record(ValueDep {
					value,
					consumer,
					used: ValueUse::Exact,
				});
			}
		}
		Walk::Continue
	});
	(deps, complete)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::phys::ops::Batch;
	use crate::exec::ir::registry;

	fn empty_ir() -> Ir {
		Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		)
	}

	#[test]
	fn every_logical_tag_has_a_pattern() {
		let set = &registry::build().conversion;
		for tag in LogTag::ALL {
			assert_eq!(set.for_source(*tag).count(), 1, "{} has no single pattern", tag.mnemonic());
		}
	}

	#[test]
	fn a_pattern_is_named_by_its_tag() {
		for pattern in patterns() {
			assert_eq!(pattern.name(), pattern.source().mnemonic());
		}
	}

	/// The text from the delimiter at `open` through the delimiter that closes it.
	fn delimited(code: &str, open: usize) -> &str {
		let mut depth = 0_usize;
		for (at, c) in code[open..].char_indices() {
			match c {
				'(' | '[' | '{' => depth += 1,
				')' | ']' | '}' => {
					depth = depth.saturating_sub(1);
					if depth == 0 {
						return &code[open..=open + at];
					}
				}
				_ => {}
			}
		}
		&code[open..]
	}

	/// A pattern makes an op only through `Rewriter::create`, an `OpSpec` or
	/// `Rewriter::update_kind`, so none of those names a logical op. A logical op elsewhere in a
	/// pattern is matched, never made.
	#[test]
	fn patterns_create_only_phys_and_expr_ops() {
		let planted = "rw.create(at, OpSpec { kind: OpKind::Logical(LogOp::Values { .. }) })?;";
		let opener = ".create(";
		let at = planted.find(opener).expect("the planted call") + opener.len() - 1;
		assert!(delimited(planted, at).contains("LogOp::Values"), "the scan reads a whole call");
		for source in [include_str!("conversion.rs"), include_str!("access.rs")] {
			let code = source.split("#[cfg(test)]").next().expect("the module has code");
			let mut sites = 0;
			for opener in [".create(", ".update_kind(", "OpSpec {"] {
				for (at, _) in code.match_indices(opener) {
					sites += 1;
					let made = delimited(code, at + opener.len() - 1);
					assert!(
						!made.contains("OpKind::Logical") && !made.contains("LogOp::"),
						"a logical op is constructed: {made}"
					);
				}
			}
			assert!(sites > 0, "the module makes its ops through the rewriter");
			for line in code.lines().filter(|line| line.contains("kind: OpKind::")) {
				assert!(
					line.contains("OpKind::Phys("),
					"a pattern creates a non-physical op: {line}"
				);
			}
		}
	}

	#[test]
	fn a_loop_is_not_unrolled() {
		let placement = placement();
		let facts = unknown_loop_facts();
		assert_eq!(placement.unroll(&facts), 1);
		let batch = placement.loop_batch(&facts, &unplaced_facts());
		assert!(batch.max_iterations > 0);
		assert!(batch.max_bytes > 0);
	}

	#[test]
	fn an_empty_container_records_no_dependences() {
		let (deps, complete) = value_deps(&empty_ir(), &FactCache::new());
		assert!(deps.deps().is_empty());
		assert!(!deps.overflowed());
		assert!(complete);
	}

	#[test]
	fn only_a_decision_from_a_value_is_a_consumer() {
		assert_eq!(fold_consumer(&PhysOp::Break), None);
		assert_eq!(fold_consumer(&PhysOp::Filter), None);
		let looped = PhysOp::Loop {
			kind: LoopKind::For,
			first: false,
			batch: Batch {
				max_iterations: 1,
				max_bytes: 1,
			},
			unroll: 1,
			value: false,
			step: None,
		};
		assert_eq!(fold_consumer(&looped), Some(FoldConsumer::Unroll));
	}

	#[test]
	fn a_write_names_its_subjects_by_form() {
		let ir = empty_ir();
		assert_eq!(
			write_subjects(&ir, None, WriteForm::Insert, false),
			WriteSubjectsShape::Inserts
		);
		assert_eq!(write_subjects(&ir, None, WriteForm::Relate, false), WriteSubjectsShape::Edges);
		assert_eq!(
			write_subjects(&ir, None, WriteForm::Create, false),
			WriteSubjectsShape::Targets
		);
		assert_eq!(write_subjects(&ir, None, WriteForm::Update, false), WriteSubjectsShape::Rows);
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod bound_tests {
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::build::logical::bind_request;
	use crate::exec::ir::core::convert::{ConversionSet, convert};
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::iface::catalog::{EffectCx, PhysicalCatalog, SemanticCatalog};
	use crate::exec::ir::iface::conversion::PHYSICAL_PHASE;
	use crate::exec::ir::iface::fold::NotYetFolder;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::logical::ops::StepAttrs;
	use crate::exec::ir::phys::ops::PhysTag;
	use crate::exec::ir::phys::pipeline;
	use crate::exec::ir::{RequestInput, RequestStatement};

	/// A semantic catalog that holds no definition.
	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// A physical catalog that holds no index and no statistics.
	struct NoPhysical;

	impl PhysicalCatalog for NoPhysical {}

	/// Binds `sql` and converts it with the patterns of this module.
	async fn converted(sql: &str) -> Ir {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let effects = EffectCx {
			registry: &registry,
			catalog: &NoCatalog,
			frames: &frames,
		};
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(
			&request_statements,
			IrScope::Request {
				fragment: None,
			},
		);
		let mut ir = bind_request(&input, &effects).await.expect("the request binds");
		let mut set = ConversionSet::new();
		for pattern in patterns() {
			set.add(pattern);
		}
		let cx = PhysCx {
			effects,
			physical: &NoPhysical,
			config: crate::exec::ir::iface::catalog::default_config(),
		};
		let mut facts = FactCache::new();
		convert(&mut ir, &mut facts, &set, &cx, &NotYetFolder, &PHYSICAL_PHASE)
			.await
			.expect("the container converts");
		ir
	}

	/// The physical ops of `ir` tagged `tag`, in pre-order.
	fn phys_ops(ir: &Ir, tag: PhysTag) -> Vec<PhysOp> {
		let mut found = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Phys(kind)) = ir.op(op).map(|data| &data.kind)
				&& kind.tag() == tag
			{
				found.push(kind.clone());
			}
			Walk::Continue
		});
		found
	}

	/// A graph step converts to a loop that keeps the lookup it states and the clause regions the
	/// lookup holds, and holds no region for the first element.
	#[tokio::test]
	async fn a_graph_step_converts_with_its_step_and_clause_regions() {
		let ir = converted("RETURN p:1->(knows WHERE score > 1)").await;
		let loops = phys_ops(&ir, PhysTag::Loop);
		assert_eq!(loops.len(), 1);
		let PhysOp::Loop {
			kind,
			first,
			step,
			..
		} = &loops[0]
		else {
			panic!("a loop op");
		};
		assert_eq!(*kind, LoopKind::GraphStep);
		assert!(!*first);
		let Some(StepAttrs::Lookup(lookup)) = step.as_deref() else {
			panic!("the loop states its lookup");
		};
		assert!(lookup.cond);
		assert_eq!(
			loops[0].region_kinds().to_vec(),
			vec![RegionKind::Loop, RegionKind::Clause(ClauseRole::Where)]
		);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn a_for_loop_converts_to_a_phys_loop_that_is_not_unrolled() {
		let ir = converted("FOR $i IN [1, 2, 3] { $i }").await;
		let loops = phys_ops(&ir, PhysTag::Loop);
		assert_eq!(loops.len(), 1);
		let PhysOp::Loop {
			kind,
			unroll,
			batch,
			..
		} = &loops[0]
		else {
			panic!("a loop op");
		};
		assert_eq!(*kind, LoopKind::For);
		assert_eq!(*unroll, 1);
		assert!(batch.max_iterations > 0);
		assert_eq!(phys_ops(&ir, PhysTag::Pipeline).len(), 0);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn pipeline_build_wraps_every_fan_out_loop() {
		let mut ir = converted("FOR $i IN [1, 2, 3] { $i }").await;
		let mut facts = FactCache::new();
		let top = ir.top();
		assert_eq!(pipeline::build(&mut ir, &mut facts, top).expect("the loop is wrapped"), 1);
		assert_eq!(phys_ops(&ir, PhysTag::Pipeline).len(), 1);
		assert_eq!(phys_ops(&ir, PhysTag::Loop).len(), 1);
		assert_eq!(pipeline::build(&mut ir, &mut facts, top).expect("nothing is left to wrap"), 0);
		assert_eq!(phys_ops(&ir, PhysTag::Pipeline).len(), 1);
	}

	/// Conversion creates no `phys.pipeline`, whatever loop it converts. The placement table
	/// is also empty after conversion, but a caller cannot observe that (`placement_table`
	/// answers `NotYet`), so only the absence of the pipelines is asserted.
	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn convert_builds_no_pipeline() {
		for sql in [
			"FOR $i IN [1, 2, 3] { $i }",
			"SELECT * FROM [1, 2, 3]",
			"RETURN [1, 2, 3].map(|$x| $x)",
		] {
			let ir = converted(sql).await;
			assert_eq!(phys_ops(&ir, PhysTag::Pipeline).len(), 0, "{sql} built a pipeline");
		}
	}

	#[ignore = "needs R-L4-1 PhysicalCatalog answers"]
	#[tokio::test]
	async fn a_record_id_read_converts_to_a_record_scan() {
		let ir = converted("SELECT * FROM t:1").await;
		assert_eq!(phys_ops(&ir, PhysTag::RecordScan).len(), 1);
	}

	/// The targets an `UPSERT` names, and the rows of a subquery a write reads, the operand of its
	/// subject loop, are its subjects.
	#[tokio::test]
	async fn the_targets_a_write_names_are_its_subjects() {
		for sql in [
			"UPSERT t:1 SET a = 1",
			"UPSERT t:1, t:2 SET a = 1",
			"UPDATE (SELECT * FROM [{ id: t:1 }]) SET a = 1",
		] {
			let ir = converted(sql).await;
			let writes = phys_ops(&ir, PhysTag::Write);
			let [PhysOp::Write(attrs)] = writes.as_slice() else {
				panic!("{sql} converts to one phys.write");
			};
			assert_eq!(attrs.subjects, WriteSubjectsShape::Targets, "{sql}");
		}
	}
}

/// The conversion of the ops whose physical twins state the regions of their attributes, over
/// containers built by hand.
#[cfg(test)]
mod region_tests {
	use smallvec::smallvec;
	use surrealdb_strand::{Strand, TableName};

	use super::super::ops::{ComputedReadsShape, DerivedReads};
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::build::phys::UNBATCHED;
	use crate::exec::ir::core::convert::{ConversionSet, convert};
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::location::LocOrigin;
	use crate::exec::ir::expr::ops::ComputedReadAttrs;
	use crate::exec::ir::iface::catalog::{EffectCx, PhysicalCatalog, SemanticCatalog};
	use crate::exec::ir::iface::choose::{
		ArmBody, ArmEquivalence, ArmLabel, ArmSpec, ChooseGranularity, ChooseOrigin, GenericArm,
		Guard,
	};
	use crate::exec::ir::iface::conversion::PHYSICAL_PHASE;
	use crate::exec::ir::iface::fold::NotYetFolder;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::ids::BlockId;
	use crate::exec::ir::iface::premise::CatalogObjectKey;
	use crate::exec::ir::iface::region_kind::{FieldRole, StatementCommit};
	use crate::exec::ir::iface::statements::{ContentMode, MatchBindings, Place, PlacePart};
	use crate::exec::ir::logical::ops::{AssignAttrs, SourceOrder, TxnControl};
	use crate::exec::ir::phys::ops::{PhysTag, ProjectedFieldShape};
	use crate::exec::logical::footprint::CatalogKind;
	use crate::exec::vm::compile_cache::ObjectVersion;
	use crate::expr::logical::{GuardKind, OutputPath};
	use crate::expr::{AssignOperator, ExplainFormat};

	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	struct NoPhysical;

	impl PhysicalCatalog for NoPhysical {}

	/// A container under construction: its facts and the block ops are appended to.
	struct Fixture {
		ir: Ir,
		facts: FactCache,
		top: BlockId,
	}

	impl Fixture {
		fn new() -> Self {
			let ir = Ir::new(
				IrScope::Request {
					fragment: None,
				},
				RegionKind::Request,
			);
			let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
			Self {
				ir,
				facts: FactCache::new(),
				top,
			}
		}

		fn op(
			&mut self,
			block: BlockId,
			kind: OpKind,
			operands: &[ValueId],
			results: usize,
		) -> OpId {
			Rewriter::new(&mut self.ir, &mut self.facts)
				.create(
					InsertPoint::End(block),
					OpSpec {
						kind,
						operands: operands.iter().copied().collect(),
						results: (0..results).map(|_| TypeKnowledge::Unknown).collect(),
						frame: FrameId::new(0),
						snapshot: SnapshotId::new(0),
						loc: Location {
							stmt: 0,
							span: None,
							origin: LocOrigin::Synthesized,
						},
					},
				)
				.expect("the op is created")
		}

		/// The first result of `op`.
		fn value(&self, op: OpId) -> ValueId {
			self.ir.op(op).expect("the op exists").results[0]
		}

		fn literal(&mut self, block: BlockId, value: i64) -> ValueId {
			let op =
				self.op(block, OpKind::Expr(ExprOp::Literal(Box::new(Value::from(value)))), &[], 1);
			self.value(op)
		}

		/// A region of `kind` owned by `owner` whose block takes `args` arguments, and the
		/// arguments.
		fn region(
			&mut self,
			owner: OpId,
			kind: RegionKind,
			args: usize,
		) -> (BlockId, Vec<ValueId>) {
			let typed: Vec<TypeKnowledge> = (0..args).map(|_| TypeKnowledge::Unknown).collect();
			let (_, block) = Rewriter::new(&mut self.ir, &mut self.facts)
				.create_region(owner, kind, &typed)
				.expect("the region is created");
			let held =
				self.ir.block(block).expect("the block exists").args.iter().copied().collect();
			(block, held)
		}

		fn yield_in(&mut self, block: BlockId, values: &[ValueId]) {
			self.op(block, OpKind::Expr(ExprOp::Yield), values, 0);
		}

		/// A region of `kind` that yields a literal.
		fn simple_region(&mut self, owner: OpId, kind: RegionKind, args: usize) {
			let (block, _) = self.region(owner, kind, args);
			let value = self.literal(block, 0);
			self.yield_in(block, &[value]);
		}

		async fn convert(&mut self) -> Result<(), IrError> {
			let registry = FunctionRegistry::with_builtins();
			let frames = FrameTable::default();
			let effects = EffectCx {
				registry: &registry,
				catalog: &NoCatalog,
				frames: &frames,
			};
			let cx = PhysCx {
				effects,
				physical: &NoPhysical,
				config: crate::exec::ir::iface::catalog::default_config(),
			};
			let mut set = ConversionSet::new();
			for pattern in patterns() {
				set.add(pattern);
			}
			convert(&mut self.ir, &mut self.facts, &set, &cx, &NotYetFolder, &PHYSICAL_PHASE)
				.await
				.map(|_| ())
		}

		/// The physical ops tagged `tag`, in pre-order.
		fn phys(&self, tag: PhysTag) -> Vec<(OpId, PhysOp)> {
			let mut found = Vec::new();
			self.ir.walk(self.ir.top(), WalkOrder::PreOrder, &mut |op| {
				if let Some(OpKind::Phys(kind)) = self.ir.op(op).map(|data| &data.kind)
					&& kind.tag() == tag
				{
					found.push((op, kind.clone()));
				}
				Walk::Continue
			});
			found
		}

		/// The kinds of the regions `op` holds.
		fn region_kinds(&self, op: OpId) -> Vec<RegionKind> {
			self.ir
				.op(op)
				.expect("the op exists")
				.regions
				.iter()
				.map(|region| self.ir.region(*region).expect("the region exists").kind)
				.collect()
		}

		/// Whether no logical op is left.
		fn is_physical(&self) -> bool {
			let mut logical = false;
			self.ir.walk(self.ir.top(), WalkOrder::PreOrder, &mut |op| {
				logical |=
					matches!(self.ir.op(op).map(|data| &data.kind), Some(OpKind::Logical(_)));
				Walk::Continue
			});
			!logical
		}
	}

	fn place(name: &str) -> Place {
		Place {
			parts: Box::new([PlacePart::Field(Strand::new(name))]),
		}
	}

	#[tokio::test]
	async fn the_clauses_of_a_statement_convert_to_their_physical_twins() {
		let mut fx = Fixture::new();
		let top = fx.top;
		let duration = fx.literal(top, 1);
		let timeout = fx.op(top, OpKind::Logical(LogOp::Timeout), &[duration], 1);
		let (body, _) = fx.region(timeout, RegionKind::Deadline, 0);
		let inner = fx.literal(body, 2);
		fx.yield_in(body, &[inner]);
		let timed = fx.value(timeout);
		let only = fx.op(top, OpKind::Logical(LogOp::Only), &[timed], 1);
		let version = fx.literal(top, 3);
		let only_rows = fx.value(only);
		let scoped = fx.op(top, OpKind::Logical(LogOp::VersionScope), &[only_rows, version], 1);
		let scoped_rows = fx.value(scoped);
		fx.op(
			top,
			OpKind::Logical(LogOp::Explain {
				analyze: true,
				format: ExplainFormat::Json,
			}),
			&[scoped_rows],
			1,
		);
		fx.convert().await.expect("the clauses convert");
		assert!(fx.is_physical());
		for tag in [PhysTag::Timeout, PhysTag::Only, PhysTag::VersionScope, PhysTag::Explain] {
			assert_eq!(fx.phys(tag).len(), 1, "{}", tag.mnemonic());
		}
		let (timeout, _) = fx.phys(PhysTag::Timeout).remove(0);
		assert_eq!(fx.region_kinds(timeout), vec![RegionKind::Deadline]);
		assert!(matches!(
			fx.phys(PhysTag::Explain)[0].1,
			PhysOp::Explain {
				analyze: true,
				format: ExplainFormat::Json,
			}
		));
	}

	#[tokio::test]
	async fn the_data_clauses_of_a_write_convert_to_their_physical_twins() {
		let mut fx = Fixture::new();
		let top = fx.top;
		let value = fx.literal(top, 1);
		fx.op(
			top,
			OpKind::Logical(LogOp::Assign(Box::new(AssignAttrs {
				place: place("a"),
				op: AssignOperator::Assign,
			}))),
			&[value],
			0,
		);
		fx.op(
			top,
			OpKind::Logical(LogOp::Unset {
				place: place("b"),
			}),
			&[],
			0,
		);
		fx.op(
			top,
			OpKind::Logical(LogOp::Content {
				mode: ContentMode::Merge,
			}),
			&[value],
			0,
		);
		fx.convert().await.expect("the clauses convert");
		assert!(fx.is_physical());
		assert!(matches!(
			&fx.phys(PhysTag::Assign)[0].1,
			PhysOp::Assign(attrs) if attrs.place == place("a") && attrs.op == AssignOperator::Assign
		));
		assert!(matches!(
			&fx.phys(PhysTag::Unset)[0].1,
			PhysOp::Unset { place: held } if *held == place("b")
		));
		assert!(matches!(
			fx.phys(PhysTag::Content)[0].1,
			PhysOp::Content {
				mode: ContentMode::Merge,
			}
		));
	}

	#[tokio::test]
	async fn an_if_states_its_position() {
		for (kind, value) in [(RegionKind::StatementIf, false), (RegionKind::ValueIf, true)] {
			let mut fx = Fixture::new();
			let top = fx.top;
			let condition = fx.literal(top, 1);
			let branch = fx.op(top, OpKind::Logical(LogOp::If), &[condition], 1);
			fx.simple_region(branch, kind, 0);
			fx.simple_region(branch, kind, 0);
			fx.convert().await.expect("the branch converts");
			let (converted, op) = fx.phys(PhysTag::If).remove(0);
			assert!(matches!(
				op,
				PhysOp::If {
					value: held,
				} if held == value
			));
			assert_eq!(fx.region_kinds(converted), vec![kind, kind]);
		}
	}

	#[tokio::test]
	async fn a_for_states_its_position_and_is_never_batched() {
		for (kind, value) in [(RegionKind::StatementFor, false), (RegionKind::ValueFor, true)] {
			let mut fx = Fixture::new();
			let top = fx.top;
			let source = fx.literal(top, 3);
			let looped = fx.op(
				top,
				OpKind::Logical(LogOp::Loop {
					kind: LoopKind::For,
					sink: LoopSink::Discard,
					order: SourceOrder::Positional,
					step: None,
				}),
				&[source],
				1,
			);
			fx.simple_region(looped, kind, 2);
			fx.convert().await.expect("the loop converts");
			let (converted, op) = fx.phys(PhysTag::Loop).remove(0);
			let PhysOp::Loop {
				kind: LoopKind::For,
				batch,
				unroll,
				value: held,
				..
			} = op
			else {
				panic!("a FOR converts to a FOR");
			};
			assert_eq!(batch, UNBATCHED);
			assert_eq!(unroll, 1);
			assert_eq!(held, value);
			assert_eq!(fx.region_kinds(converted), vec![kind]);
		}
	}

	#[tokio::test]
	async fn a_loop_that_is_not_a_for_takes_the_policy_batch() {
		let mut fx = Fixture::new();
		let top = fx.top;
		let source = fx.literal(top, 3);
		let looped = fx.op(
			top,
			OpKind::Logical(LogOp::Loop {
				kind: LoopKind::Select,
				sink: LoopSink::Output,
				order: SourceOrder::Delivery,
				step: None,
			}),
			&[source],
			1,
		);
		fx.simple_region(looped, RegionKind::Loop, 2);
		fx.convert().await.expect("the loop converts");
		let PhysOp::Loop {
			batch,
			value,
			..
		} = fx.phys(PhysTag::Loop).remove(0).1
		else {
			panic!("a loop converts to a loop");
		};
		assert_ne!(batch, UNBATCHED);
		assert!(!value);
	}

	#[tokio::test]
	async fn an_aggregation_states_its_group_keys() {
		let mut fx = Fixture::new();
		let top = fx.top;
		let rows = fx.literal(top, 1);
		let grouped = fx.op(
			top,
			OpKind::Logical(LogOp::Aggregate(Box::new(AggregateShape {
				groups: 2,
				fields: FieldsShape::Value {
					alias: None,
				},
				names: Vec::new(),
				first_value: Vec::new(),
			}))),
			&[rows],
			1,
		);
		for _ in 0..2 {
			fx.simple_region(grouped, RegionKind::ErrorScope(ErrorScope::ShapeToNone), 1);
		}
		fx.simple_region(grouped, RegionKind::Clause(ClauseRole::Projection), 1);
		fx.convert().await.expect("the aggregation converts");
		let (converted, op) = fx.phys(PhysTag::Aggregate).remove(0);
		let PhysOp::Aggregate(attrs) = op else {
			panic!("an aggregation converts to an aggregation");
		};
		assert_eq!(attrs.groups, 2);
		assert_eq!(attrs.spill, Spill::Spillable);
		assert_eq!(fx.region_kinds(converted).len(), 3);
	}

	#[tokio::test]
	async fn a_match_states_its_stages_and_a_read_stage_its_predicates() {
		let mut fx = Fixture::new();
		let top = fx.top;
		let query = fx.op(
			top,
			OpKind::Logical(LogOp::Match {
				bindings: MatchBindings(Box::new([])),
			}),
			&[],
			1,
		);
		for _ in 0..2 {
			let (block, _) = fx.region(query, RegionKind::Subquery, 0);
			fx.yield_in(block, &[]);
		}
		let read = fx.op(
			top,
			OpKind::Logical(LogOp::MatchRead {
				optional_group: None,
				patterns: Box::new([]),
			}),
			&[],
			0,
		);
		for _ in 0..3 {
			fx.simple_region(read, RegionKind::Clause(ClauseRole::Where), 1);
		}
		fx.convert().await.expect("the query converts");
		assert!(matches!(
			fx.phys(PhysTag::Match)[0].1,
			PhysOp::Match {
				stages: 2,
				..
			}
		));
		let (step, op) = fx.phys(PhysTag::MatchStep).remove(0);
		assert!(matches!(
			&op,
			PhysOp::MatchStep(attrs)
				if matches!(attrs.as_ref(), MatchStepAttrs::Read { predicates: 3, .. })
		));
		assert_eq!(fx.region_kinds(step).len(), 3);
	}

	#[tokio::test]
	async fn a_transaction_control_slot_keeps_its_control() {
		for control in [None, Some(TxnControl::Begin), Some(TxnControl::Cancel)] {
			let mut fx = Fixture::new();
			let top = fx.top;
			let stmt = fx.op(
				top,
				OpKind::Logical(LogOp::Stmt {
					commit: StatementCommit::Auto,
					control,
				}),
				&[],
				0,
			);
			let (block, _) = fx.region(stmt, RegionKind::Statement(StatementCommit::Auto), 1);
			let none = fx.literal(block, 0);
			fx.yield_in(block, &[none]);
			fx.convert().await.expect("the slot converts");
			assert!(matches!(
				fx.phys(PhysTag::Stmt)[0].1,
				PhysOp::Stmt {
					control: held,
					..
				} if held == control
			));
		}
	}

	/// The `logical.statement` of a statement list converts to the `phys.statement` of the same
	/// catalog flag, which is neither late-bound nor refused, and keeps its region.
	#[tokio::test]
	async fn a_statement_of_a_list_keeps_its_catalog_flag() {
		for changes_catalog in [false, true] {
			let mut fx = Fixture::new();
			let top = fx.top;
			let owner = fx.op(
				top,
				OpKind::Logical(LogOp::Statement {
					changes_catalog,
					served: None,
				}),
				&[],
				1,
			);
			let (block, _) = fx.region(owner, RegionKind::Statement(StatementCommit::InTxn), 1);
			let none = fx.literal(block, 0);
			fx.yield_in(block, &[none]);
			fx.convert().await.expect("the statement converts");
			assert!(fx.is_physical());
			let statements = fx.phys(PhysTag::Statement);
			assert_eq!(statements.len(), 1);
			assert!(matches!(
				statements[0].1,
				PhysOp::Statement {
					changes_catalog: held,
					served: None,
					raises_when_reached: false,
					..
				} if held == changes_catalog
			));
			assert_eq!(
				fx.region_kinds(statements[0].0),
				vec![RegionKind::Statement(StatementCommit::InTxn)]
			);
		}
	}

	/// A `logical.permission_filter` over a literal that states `computed`, whose predicate region
	/// `body` fills with the ops of the predicate, given the row the region is evaluated against.
	fn permission(
		computed: Option<ComputedReadsShape>,
		body: impl FnOnce(&mut Fixture, BlockId, ValueId),
	) -> Fixture {
		let mut fx = Fixture::new();
		let top = fx.top;
		let rows = fx.literal(top, 1);
		let filter = fx.op(
			top,
			OpKind::Logical(LogOp::PermissionFilter {
				guard: GuardKind::Read,
				computed: computed.map(Box::new),
			}),
			&[rows],
			1,
		);
		let (block, args) = fx.region(filter, RegionKind::PermissionFrame, 2);
		body(&mut fx, block, args[1]);
		fx
	}

	fn computed_read(fx: &mut Fixture, block: BlockId, row: ValueId, field: &str) -> ValueId {
		let read = fx.op(
			block,
			OpKind::Expr(ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
				field: Strand::new(field),
				def: (
					CatalogObjectKey {
						kind: CatalogKind::Field,
						ns: None,
						db: None,
						table: Some(TableName::new(Strand::new("t"))),
						name: Some(Strand::new(field)),
					},
					ObjectVersion::Inline,
				),
			}))),
			&[row],
			1,
		);
		fx.value(read)
	}

	fn permission_computed(fx: &Fixture) -> Option<ComputedReadsShape> {
		let PhysOp::PermissionFilter(attrs) = fx.phys(PhysTag::PermissionFilter).remove(0).1 else {
			panic!("a permission filter converts to one");
		};
		attrs.computed
	}

	#[tokio::test]
	async fn a_permission_carries_the_computed_reads_its_binder_stated() {
		for stated in [
			Some(ComputedReadsShape {
				table: TableName::new(Strand::new("t")),
				fields: Some(vec!["a".to_owned(), "b".to_owned()]),
				derived: DerivedReads::Stated(Box::default()),
			}),
			Some(ComputedReadsShape {
				table: TableName::new(Strand::new("t")),
				fields: None,
				derived: DerivedReads::LateBound,
			}),
		] {
			let mut fx = permission(stated.clone(), |fx, block, row| {
				let read = computed_read(fx, block, row, "a");
				let invoked = fx.op(block, OpKind::Expr(ExprOp::Invoke), &[row], 1);
				let held = fx.value(invoked);
				fx.yield_in(block, &[read, held]);
			});
			fx.convert().await.expect("the permission converts");
			assert_eq!(permission_computed(&fx), stated);
		}
	}

	#[tokio::test]
	async fn a_permission_states_no_computed_read_its_binder_did_not() {
		// The predicate's ops are not read: the binder decided the reads from the table's schema.
		let mut fx = permission(None, |fx, block, row| {
			let read = computed_read(fx, block, row, "a");
			fx.yield_in(block, &[read]);
		});
		fx.convert().await.expect("the permission converts");
		assert_eq!(permission_computed(&fx), None);
	}

	/// A `logical.project` over a literal with the fields `fields`, the names of its unaliased
	/// fields, and one region per field and per entry of `omit`, each of which `fill` builds.
	fn projected(
		fields: Vec<FieldShape>,
		names: Vec<OutputPath>,
		omit: Vec<fn(&mut Fixture, BlockId, ValueId) -> ValueId>,
	) -> Fixture {
		let mut fx = Fixture::new();
		let top = fx.top;
		let rows = fx.literal(top, 1);
		let regions =
			fields.iter().filter(|field| matches!(field, FieldShape::Single { .. })).count();
		let attrs = ProjectAttrs {
			fields: FieldsShape::Select(fields.into_boxed_slice()),
			layer: false,
			omit: u16::try_from(omit.len()).expect("few omissions"),
			names,
			computed: Vec::new(),
			sort_keys: Vec::new(),
			judged: None,
		};
		let project = fx.op(top, OpKind::Logical(LogOp::Project(Box::new(attrs))), &[rows], 1);
		for _ in 0..regions {
			fx.simple_region(project, RegionKind::Clause(ClauseRole::Projection), 1);
		}
		for build in omit {
			let (block, args) = fx.region(project, RegionKind::Clause(ClauseRole::Projection), 1);
			let value = build(&mut fx, block, args[0]);
			fx.yield_in(block, &[value]);
		}
		fx
	}

	fn field_of_row(fx: &mut Fixture, block: BlockId, row: ValueId) -> ValueId {
		let op = fx.op(
			block,
			OpKind::Expr(ExprOp::Field {
				name: Strand::new("secret"),
			}),
			&[row],
			1,
		);
		fx.value(op)
	}

	fn each_of_field(fx: &mut Fixture, block: BlockId, row: ValueId) -> ValueId {
		let field = field_of_row(fx, block, row);
		let each = fx.op(block, OpKind::Expr(ExprOp::Each), &[field], 1);
		fx.value(each)
	}

	#[tokio::test]
	async fn a_projection_carries_its_names_and_its_plain_omissions() {
		let mut fx = projected(
			vec![
				FieldShape::Single {
					alias: None,
					kind: None,
				},
				FieldShape::Single {
					alias: Some(OutputPath::field("x")),
					kind: None,
				},
			],
			vec![OutputPath::field("a")],
			vec![field_of_row],
		);
		fx.convert().await.expect("the projection converts");
		let (converted, op) = fx.phys(PhysTag::Project).remove(0);
		let expected = ProjectionShape::Fields {
			fields: vec![
				ProjectedFieldShape {
					output: FieldPath::field("a"),
					aliased: false,
					sort_key: None,
				},
				ProjectedFieldShape {
					output: FieldPath::field("x"),
					aliased: true,
					sort_key: None,
				},
			],
			omit: vec![OmissionShape::Path(FieldPath::field("secret"))],
		};
		assert!(matches!(&op, PhysOp::Project(shape) if **shape == expected));
		// The shape reads the two field regions; the omitted path is not an expression of it.
		assert_eq!(fx.region_kinds(converted).len(), 2);
	}

	#[tokio::test]
	async fn a_layer_carries_the_names_of_its_unaliased_columns() {
		let mut fx = Fixture::new();
		let top = fx.top;
		let rows = fx.literal(top, 1);
		let fields = FieldsShape::Select(Box::new([
			FieldShape::All,
			FieldShape::Single {
				alias: None,
				kind: None,
			},
			FieldShape::Single {
				alias: Some(OutputPath::field("x")),
				kind: None,
			},
		]));
		let names = vec![OutputPath::field("a")];
		let attrs = ProjectAttrs {
			fields: fields.clone(),
			layer: true,
			omit: 0,
			names: names.clone(),
			computed: Vec::new(),
			sort_keys: Vec::new(),
			judged: None,
		};
		let layer = fx.op(top, OpKind::Logical(LogOp::Project(Box::new(attrs))), &[rows], 1);
		for _ in 0..2 {
			fx.simple_region(layer, RegionKind::Clause(ClauseRole::Projection), 1);
		}
		for _ in 0..2 {
			fx.simple_region(layer, RegionKind::FieldFrame(FieldRole::Computed), 1);
		}
		fx.convert().await.expect("the layer converts");
		let (_, op) = fx.phys(PhysTag::Layer).remove(0);
		let PhysOp::Layer(converted) = op else {
			panic!("a layer converts to one");
		};
		assert_eq!(converted.fields, fields);
		assert_eq!(converted.names, names);
	}

	/// A keyed inner join converts with its right input as the build side and no index lookup,
	/// counting its key pairs from its `WHERE` regions.
	#[tokio::test]
	async fn a_keyed_join_converts_with_no_index_lookup() {
		let mut fx = Fixture::new();
		let top = fx.top;
		let left = fx.literal(top, 1);
		let right = fx.literal(top, 2);
		let joined = fx.op(
			top,
			OpKind::Logical(LogOp::Join {
				kind: JoinKind::Inner,
				alias: RelationAlias::new(0),
			}),
			&[left, right],
			1,
		);
		for _ in 0..2 {
			fx.simple_region(joined, RegionKind::Clause(ClauseRole::Where), 1);
		}
		fx.convert().await.expect("the join converts");
		let (_, op) = fx.phys(PhysTag::Join).remove(0);
		let PhysOp::Join(attrs) = op else {
			panic!("a join converts to one");
		};
		assert_eq!(attrs.matching, JoinMatch::Inner);
		assert_eq!(attrs.build, JoinBuild::Right);
		assert_eq!(attrs.keys, 1);
		assert!(!attrs.residual);
		assert_eq!(attrs.lookup, None);
	}

	/// `secret.{ a, b.{ c } }`.
	fn destructured_field(fx: &mut Fixture, block: BlockId, row: ValueId) -> ValueId {
		let field = field_of_row(fx, block, row);
		let shape: Box<[DestructureEntry]> = Box::new([
			DestructureEntry::Field(Strand::new("a")),
			DestructureEntry::Destructure(
				Strand::new("b"),
				Box::new([DestructureEntry::Field(Strand::new("c"))]),
			),
		]);
		let destructure = fx.op(
			block,
			OpKind::Expr(ExprOp::Destructure {
				shape,
			}),
			&[field],
			1,
		);
		fx.value(destructure)
	}

	/// `secret[$i]`.
	fn indexed_by_parameter(fx: &mut Fixture, block: BlockId, row: ValueId) -> ValueId {
		let field = field_of_row(fx, block, row);
		let at = fx.op(
			block,
			OpKind::Expr(ExprOp::FreeParam {
				name: Strand::new("i"),
			}),
			&[],
			1,
		);
		let at = fx.value(at);
		let index = fx.op(block, OpKind::Expr(ExprOp::Index), &[field, at], 1);
		fx.value(index)
	}

	/// The omissions of the projection `fx` converted.
	fn omissions_of(fx: &Fixture) -> (OpId, Vec<OmissionShape>) {
		let (op, kind) = fx.phys(PhysTag::Project).remove(0);
		match kind {
			PhysOp::Project(shape) => (op, shape.omissions().to_vec()),
			PhysOp::TableScan(_)
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
			| PhysOp::Enforced {
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
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Limit {
				..
			}
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
			| PhysOp::MatchStep(_)
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
			| PhysOp::Next => panic!("a projection converts to one"),
		}
	}

	/// `.*` is walked: the field to the container it empties, with no region but the expression's.
	#[tokio::test]
	async fn an_omission_that_walks_states_its_steps_and_action() {
		let mut fx = projected(vec![FieldShape::All], Vec::new(), vec![each_of_field]);
		fx.convert().await.expect("the projection converts");
		let (op, omit) = omissions_of(&fx);
		assert_eq!(
			omit,
			vec![OmissionShape::Walk {
				steps: vec![WalkStepShape::Field("secret".to_owned())],
				action: WalkActionShape::Clear,
			}]
		);
		assert_eq!(fx.region_kinds(op), vec![RegionKind::Clause(ClauseRole::Projection)]);
	}

	/// A destructure in a path lists locations: the path through it is the path through each
	/// field it lists, and the nested entry names its own.
	#[tokio::test]
	async fn a_destructure_in_a_path_expands_to_each_plain_path() {
		let mut fx = projected(vec![FieldShape::All], Vec::new(), vec![destructured_field]);
		fx.convert().await.expect("the projection converts");
		let (op, omit) = omissions_of(&fx);
		let secret = |tail: &[&str]| {
			let mut parts = vec![FieldPathPart::Field("secret".to_owned())];
			parts.extend(tail.iter().map(|name| FieldPathPart::Field((*name).to_owned())));
			OmissionShape::Path(FieldPath(parts))
		};
		assert_eq!(omit, vec![secret(&["a"]), secret(&["b", "c"])]);
		assert!(fx.region_kinds(op).is_empty(), "a plain path keeps no region");
	}

	/// A position known only once evaluated is a region of the projection: it reads the document
	/// the walk applies it to, which is `NONE` for a position, and holds the cone that computes the
	/// position.
	#[tokio::test]
	async fn a_position_evaluated_at_the_walk_is_a_region_of_the_projection() {
		let mut fx = projected(vec![FieldShape::All], Vec::new(), vec![indexed_by_parameter]);
		fx.convert().await.expect("the projection converts");
		let (op, omit) = omissions_of(&fx);
		assert_eq!(
			omit,
			vec![OmissionShape::Walk {
				steps: vec![WalkStepShape::Field("secret".to_owned())],
				action: WalkActionShape::RemoveIndexAt,
			}]
		);
		let kinds = fx.region_kinds(op);
		assert_eq!(kinds, vec![RegionKind::Clause(ClauseRole::Projection); 2]);
		let regions = fx.ir.op(op).expect("the op exists").regions.clone();
		let body = ops_in(&fx.ir, regions[1]).count();
		assert_eq!(body, 2, "the parameter read and the yield");
	}

	#[tokio::test]
	async fn a_choice_converts_to_its_arms_alone() {
		let spec = ChooseSpec {
			granularity: ChooseGranularity::Statement,
			arms: smallvec![
				ArmSpec {
					guard: Guard::TableIs(TableName::new(Strand::new("t"))),
					body: ArmBody::PreLowered,
					label: ArmLabel("t"),
				},
				ArmSpec {
					guard: Guard::Otherwise,
					body: ArmBody::PreLowered,
					label: ArmLabel("other"),
				},
			],
			generic: GenericArm::Present,
			equivalence: ArmEquivalence::SameRowsSameOrder,
			origin: ChooseOrigin::LogicalDynamicTarget,
		};
		let mut fx = Fixture::new();
		let top = fx.top;
		let chosen = fx.op(top, OpKind::Logical(LogOp::Choose(Box::new(spec.clone()))), &[], 1);
		fx.simple_region(chosen, RegionKind::ChooseArm, 0);
		fx.simple_region(chosen, RegionKind::ChooseArm, 0);
		fx.convert().await.expect("the choice converts");
		assert!(matches!(&fx.phys(PhysTag::Choose)[0].1, PhysOp::Choose(held) if **held == spec));
	}
}
