//! The steps of a `MATCH` binding table: `phys.match_step`.
//!
//! A binding row is an object keyed by binding name. The step that binds a row under a name
//! ([`BindStepShape::Bind`]) streams: it is [`Instr::BindRow`] in the body of the pipeline that
//! holds it. Every other step is a breaker the host runs between two pipelines. The pipeline that
//! feeds it ends in [`Instr::SplitPush`], which hands each row to the step.
//! The step's spec is a [`SplitSpec`] held as a [`ProgramStep`]. The pipeline that reads the
//! step's rows starts at [`Source::Split`].
//!
//! # The spec of a step
//!
//! A step's scalars are the clause regions its shape lists, in order: an expansion's predicate, a
//! join's residual, an update's values and an insert's properties. The host evaluates each over the
//! row it judges, with that row as the document. A literal region yields its constant, and any
//! other region is a host pipeline over the row.
//!
//! A step reads adjacency and records through the database. A step that writes keeps its rows until
//! the pipeline above it has finished, then writes them in row order. The program therefore takes
//! the database context and, for a write, a transaction that may write.
//!
//! # A join
//!
//! A join takes the build rows first and the probe rows second. The probe side is the step's
//! feeder, as for any step. The build side is a group of the program. Its pipelines are lowered
//! into that group, and the host runs the group when the first probe row reaches the join. A probe
//! that yields no row therefore never runs the build side. The group takes the next index of the
//! program's groups before the build side is lowered, so a join inside the build side takes a later
//! index. A group's index is fixed when the group is reserved, before its pipelines are lowered.

use super::{
	Body, Needs, Opened, Reads, Sink, first_arg, lower_pipeline, pipeline_behind, pool_index,
	yielded_value,
};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::statements::{BindJoinShape, BindStepShape, MatchData};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{MatchStepAttrs, PhysOp};
use crate::exec::operators::mutate::{InsertEdgeOp, InsertNodeOp, UpdateOp};
use crate::exec::vm::binding::{
	BindingStep, EndpointStep, ExpandStep, JoinStep, MutationStep, PathStep, ProgramStep,
};
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::expr::Lowerer;
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::program::{GroupSpec, HostScalar, Instr, Source, SplitKind, SplitSpec};
use crate::exec::{AccessMode, ContextLevel};

impl Body<'_, '_, '_> {
	/// Lowers the step that binds each row under `name`. The row becomes an object that holds
	/// it under that field. Register values held before the bind are not kept.
	pub(super) fn bind_row(&mut self, name: &str) -> Result<(), IrError> {
		self.require_row("a binding over a projection")?;
		let name = self.em.field(name)?;
		self.em.emit(Instr::BindRow {
			name,
		});
		self.emitted.rebound = true;
		self.objects = true;
		self.schema = None;
		Ok(())
	}
}

/// Opens the source that is the rows of the step `step`: its feeder is lowered first, ending in
/// the hand-off of each row to the step's split, and the pipeline that reads the step starts at
/// that split.
///
/// The step is read by one pipeline only. A step that streams is lowered in the body of the
/// pipeline above it, so reaching one here is an invariant break.
///
/// A step's rows have exactly one reader. The MATCH fold gives each node of its plan tree a fresh
/// `logical.bind_step`, read only by the node above it. Conversion maps it to one
/// `phys.match_step`. No optimizer pass merges steps: logical CSE merges only expression ops, and
/// physical CSE never merges a physical op. The e-graph keeps a binding step as a skeleton and
/// aborts a write-back that would read one leaf from two new ops.
/// A step read more than once is therefore an invariant break.
pub(super) fn open_step(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	step: OpId,
) -> Result<Opened, IrError> {
	let data = ir.op(step).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let OpKind::Phys(PhysOp::MatchStep(attrs)) = &data.kind else {
		return Err(invariant("a step source that is not a binding step"));
	};
	let MatchStepAttrs::Bind(shape) = attrs.as_ref() else {
		return Err(invariant("a MATCH stage op in a plan the fold built"));
	};
	if matches!(shape, BindStepShape::Bind { .. }) {
		return Err(invariant("a binding that streams, reached as the source of a pipeline"));
	}
	if data.results.iter().map(|result| ir.uses(*result).len()).sum::<usize>() != 1 {
		return Err(IrError::Invariant {
			what: "ir_lower::pipe: a step read more than once",
		});
	}
	let (kind, writes, probe, build_writes) = match shape {
		BindStepShape::Join(join) => {
			let (kind, build_writes) = join_step(em, cx, ir, facts, reads, data, join)?;
			(kind, false, 1, build_writes)
		}
		BindStepShape::Bind {
			..
		}
		| BindStepShape::Endpoint {
			..
		}
		| BindStepShape::Expand(_)
		| BindStepShape::Path(_)
		| BindStepShape::DistinctEdges {
			..
		}
		| BindStepShape::Distinct
		| BindStepShape::Drain
		| BindStepShape::Update {
			..
		}
		| BindStepShape::Delete {
			..
		}
		| BindStepShape::Insert {
			..
		} => {
			let (kind, writes) = program_step(em, cx, ir, facts, reads, data, shape)?;
			(kind, writes, 0, false)
		}
	};
	reads.needs = reads.needs.merged(&Needs {
		context: ContextLevel::Database,
		keeps_row_order: false,
		reach: None,
		access_mode: if writes {
			AccessMode::ReadWrite
		} else {
			AccessMode::ReadOnly
		},
	});
	let rows = *data.operands.get(probe).ok_or_else(|| invariant("a step with no input"))?;
	let (feeder, trail) = pipeline_behind(ir, rows)?;
	let id = pool_index(reads.base.splits, reads.splits.len(), "vm splits")?;
	// Where the build side of a join writes, it runs before the rows the join takes are read:
	// when the first pipeline that produces them starts, so those rows read what it wrote.
	let first = reads.base.pipelines + reads.pipelines.len();
	reads.splits.push(SplitSpec {
		kind: SplitKind::Step(kind),
		pipeline: 0,
		build_before: build_writes.then_some(first),
	});
	lower_pipeline(
		em,
		cx,
		ir,
		facts,
		feeder,
		&trail,
		&Sink::Split {
			id,
		},
		reads,
	)?;
	Ok(Opened {
		source: Source::Split {
			split: id,
		},
		source_op: None,
		table: None,
		schema: None,
		feeder: reads.pipelines.len().checked_sub(1),
		objects: true,
		ids: false,
	})
}

/// The join `data`, which `join` states, as the program holds it: its build side lowered as a group
/// of the program and its residual predicate as a host scalar; and whether the build side writes.
///
/// The group is reserved before the build side lowers, and its spec is the sorts and permission
/// frames the build side filled. The build rows come out of the group as the output of its last
/// pipeline. The group takes the next index that `reads` counts from. Inside a block statement's
/// group it follows that group, as the groups nested in the statement's sources do.
fn join_step(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	data: &OpData,
	join: &BindJoinShape,
) -> Result<(ProgramStep, bool), IrError> {
	let [build, _] = <[ValueId; 2]>::try_from(data.operands.as_slice())
		.map_err(|_| invariant("a join with other than two inputs"))?;
	let group = pool_index(reads.base.groups, reads.groups.len(), "vm groups")?;
	let at = reads.groups.len();
	reads.groups.push(GroupSpec {
		write: None,
		scalar: false,
		rowless: false,
		sorts: 0..0,
		aggregates: 0..0,
		permissions: 0..0,
		version: None,
		timeout: None,
		only: false,
	});
	let sorts_from = pool_index(reads.base.sorts, reads.sorts.len(), "vm sorts")?;
	let mark = em.open_group(group);
	let (feeder, trail) = pipeline_behind(ir, build)?;
	let outer = std::mem::replace(&mut reads.needs, Needs::none());
	let lowered = lower_pipeline(em, cx, ir, facts, feeder, &trail, &Sink::Output, reads);
	let build_writes = reads.needs.access_mode == AccessMode::ReadWrite;
	reads.needs = outer.merged(&reads.needs);
	let permissions = em.close_group(mark);
	lowered?;
	let sorts_to = pool_index(reads.base.sorts, reads.sorts.len(), "vm sorts")?;
	if let Some(spec) = reads.groups.get_mut(at) {
		spec.sorts = sorts_from..sorts_to;
		spec.permissions = permissions?;
	}
	let residual = match (join.residual, data.regions.as_slice()) {
		(false, []) => None,
		(true, [region]) => {
			let mut needs = Needs::none();
			let scalar = row_scalar(em, cx, ir, *region, &mut needs)?;
			reads.needs = reads.needs.merged(&needs);
			Some(scalar)
		}
		_ => return Err(invariant("a join whose residual disagrees with its regions")),
	};
	Ok((
		BindingStep::Join(JoinStep {
			build: group,
			keys: join.keys.to_vec(),
			join_type: join.join_type,
			null_template: join.null_template.to_vec(),
			residual,
		}),
		build_writes,
	))
}

/// The step `data`, which `shape` states, as the program holds it, and whether it writes.
///
/// The clause regions of the op are the step's scalars, in the order
/// [`BindStepShape::region_kinds`] lists them. The `Subquery` regions that follow them each hold
/// one write the step applies to a record, lowered as a group of the program
/// ([`super::nested::write_region_group`]) that the step names. A step that streams or reads two
/// sides answers an error, because neither has a split spec that this builds.
fn program_step(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	data: &OpData,
	shape: &BindStepShape,
) -> Result<(ProgramStep, bool), IrError> {
	let held = data.regions.as_slice();
	let (regions, write_regions) = held.split_at(
		held.len()
			.checked_sub(shape.write_regions())
			.ok_or_else(|| invariant("a write step without its write regions"))?,
	);
	let mut groups = Vec::with_capacity(write_regions.len());
	for region in write_regions {
		groups.push(super::nested::write_region_group(em, cx, ir, facts, reads, *region)?);
	}
	let group = |at: usize| {
		groups.get(at).copied().ok_or_else(|| invariant("a write step short of a write group"))
	};
	let mut scalar = |region: RegionId| -> Result<HostScalar, IrError> {
		let mut needs = Needs::none();
		let scalar = row_scalar(em, cx, ir, region, &mut needs)?;
		reads.needs = reads.needs.merged(&needs);
		Ok(scalar)
	};
	let step = match shape {
		BindStepShape::Bind {
			..
		} => return Err(invariant("a binding that streams has no spec of a split")),
		BindStepShape::Join(_) => return Err(invariant("a join is lowered with its build side")),
		BindStepShape::Expand(expand) => {
			let predicate = match (expand.predicate, regions) {
				(false, []) => None,
				(true, [region]) => Some(scalar(*region)?),
				_ => {
					return Err(invariant(
						"an expansion whose predicate disagrees with its regions",
					));
				}
			};
			BindingStep::Expand(ExpandStep {
				source: expand.source.clone(),
				direction: expand.direction,
				edge_tables: expand.edge_tables.to_vec(),
				edge_binding: expand.edge_binding.clone(),
				target_binding: expand.target_binding.clone(),
				target_label: expand.target_label.clone(),
				predicate,
				optional: expand.optional,
			})
		}
		BindStepShape::Endpoint {
			edge,
			field,
			name,
			label,
		} => BindingStep::Endpoint(EndpointStep {
			edge: edge.clone(),
			field: *field,
			name: name.clone(),
			label: label.clone(),
		}),
		BindStepShape::Path(path) => BindingStep::Path(PathStep {
			source: path.source.clone(),
			direction: path.direction,
			edge_tables: path.edge_tables.to_vec(),
			min: path.min,
			max: path.max,
			target_binding: path.target_binding.clone(),
			target_label: path.target_label.clone(),
			group_binding: path.group_binding.clone(),
			path_binding: path.path_binding.clone(),
			mode: path.mode,
			selector: path.selector,
			reversed: path.reversed,
		}),
		BindStepShape::DistinctEdges {
			edges,
		} => BindingStep::DistinctEdges {
			edges: edges.to_vec(),
		},
		BindStepShape::Distinct => BindingStep::Distinct,
		BindStepShape::Drain => BindingStep::Drain,
		BindStepShape::Update {
			target,
			data: update,
		} => {
			let op = match update {
				MatchData::Set(names) => {
					if names.len() != regions.len() {
						return Err(invariant("a SET step with a region short of an assignment"));
					}
					let mut assignments = Vec::with_capacity(names.len());
					for (name, region) in names.iter().zip(regions) {
						assignments.push((name.clone(), scalar(*region)?));
					}
					UpdateOp::Set(assignments)
				}
				MatchData::Unset(names) => UpdateOp::Unset(names.to_vec()),
				MatchData::Content => {
					let [region] = regions else {
						return Err(invariant("a CONTENT step with other than one region"));
					};
					UpdateOp::Content(scalar(*region)?)
				}
			};
			BindingStep::Mutate(MutationStep::Update {
				target: target.clone(),
				op,
				write: group(0)?,
			})
		}
		BindStepShape::Delete {
			target,
			detach,
			is_edge,
		} => BindingStep::Mutate(MutationStep::Delete {
			target: target.clone(),
			detach: *detach,
			is_edge: *is_edge,
			write: group(0)?,
		}),
		BindStepShape::Insert {
			nodes,
			edges,
		} => {
			if nodes.len() + edges.len() != regions.len() {
				return Err(invariant("an INSERT step with a region short of a property object"));
			}
			let (node_regions, edge_regions) = regions.split_at(nodes.len());
			let mut lowered_nodes = Vec::with_capacity(nodes.len());
			for (at, (node, region)) in nodes.iter().zip(node_regions).enumerate() {
				lowered_nodes.push(InsertNodeOp {
					name: node.name.clone(),
					table: node.table.clone(),
					props: scalar(*region)?,
					write: group(at)?,
				});
			}
			let mut lowered_edges = Vec::with_capacity(edges.len());
			for (at, (edge, region)) in edges.iter().zip(edge_regions).enumerate() {
				lowered_edges.push(InsertEdgeOp {
					name: edge.name.clone(),
					table: edge.table.clone(),
					from: edge.from.clone(),
					to: edge.to.clone(),
					props: scalar(*region)?,
					write: group(nodes.len() + at)?,
				});
			}
			BindingStep::Mutate(MutationStep::Insert {
				nodes: lowered_nodes,
				edges: lowered_edges,
			})
		}
	};
	Ok((step, shape.writes()))
}

/// The scalar the host evaluates for the clause region `region`, over the row it judges: the
/// constant the region yields where it yields a literal, and a host pipeline of its own that
/// reads the row the region's argument names otherwise.
///
/// What the host pipeline needs of the run is merged into `needs`.
pub(super) fn row_scalar(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	region: RegionId,
	needs: &mut Needs,
) -> Result<HostScalar, IrError> {
	if let Some(constant) = literal_of(ir, region)? {
		let constant = crate::exec::vm::plain::ValueData::new(constant, "a host scalar")
			.map_err(|_| IrError::Stale("ir_lower::pipe: a host scalar"))?;
		return Ok(HostScalar::Const(constant));
	}
	if first_arg(ir, region).is_none() {
		return Err(invariant("a step clause with no row"));
	}
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	let code = lowerer.host_code_over_row(region)?;
	*needs = needs.merged(&lowerer.needs());
	Ok(HostScalar::Code(code))
}

/// The value the clause region `region` yields when it is a literal.
fn literal_of(ir: &Ir, region: RegionId) -> Result<Option<crate::val::Value>, IrError> {
	let yielded = yielded_value(ir, region)?;
	let Some(ValueDef::OpResult {
		op,
		index: 0,
	}) = ir.value(yielded).map(|data| data.def)
	else {
		return Ok(None);
	};
	Ok(match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Expr(ExprOp::Literal(literal))) => Some((**literal).clone()),
		Some(OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_)) | None => None,
	})
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::super::tests::{placed, row_region, scan, select_loop, sort, step};
	use super::*;
	use crate::exec::ir::iface::ids::ValueId;
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::iface::statements::{BindExpandShape, BindJoinShape, MatchBindings};
	use crate::exec::operators::{EdgeBinding, ExpandDir, JoinType};
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::ir_lower::{Lowered, lower_indexed};
	use crate::exec::vm::plain::ValueData;
	use crate::exec::vm::program::{Operand, ROW};
	use crate::val::Value;

	/// A `phys.match` over a region built by `body`, which yields the rows `body` answers.
	fn matched(b: &mut Builder, body: impl FnOnce(&mut Builder) -> ValueId) -> OpId {
		let op = b.create_kind(
			OpKind::Phys(PhysOp::Match {
				bindings: MatchBindings(Box::new([])),
				stages: 1,
			}),
			&[],
			1,
		);
		b.open(op, RegionKind::Subquery, &[]);
		let rows = body(b);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[rows], 0);
		b.close();
		op
	}

	fn binding_step(b: &mut Builder, shape: BindStepShape, inputs: &[ValueId]) -> OpId {
		b.create_kind(
			OpKind::Phys(PhysOp::MatchStep(Box::new(MatchStepAttrs::Bind(shape)))),
			inputs,
			1,
		)
	}

	fn bind(b: &mut Builder, rows: ValueId, name: &str) -> ValueId {
		let op = binding_step(
			b,
			BindStepShape::Bind {
				name: name.to_owned(),
			},
			&[rows],
		);
		b.result_of(op)
	}

	/// The rows of a scan of `t`, read by a loop that binds each row unchanged under `name`.
	fn bound_scan(b: &mut Builder, id: u32, name: &str) -> ValueId {
		let rows = scan(b);
		let read = select_loop(b, id, rows, |_, element| element);
		let read = b.result_of(read);
		bind(b, read, name)
	}

	fn lower(b: &Builder, op: OpId) -> Result<Lowered, IrError> {
		super::super::super::matching::lower_match(
			&mut crate::exec::vm::ir_lower::units::LowerCx::detached(),
			&b.ir,
			op,
		)
	}

	fn lowered(b: &Builder, op: OpId) -> Lowered {
		match lower(b, op) {
			Ok(lowered) => lowered,
			Err(error) => panic!("the MATCH lowers: {error}"),
		}
	}

	fn expansion() -> BindStepShape {
		BindStepShape::Expand(Box::new(BindExpandShape {
			source: "a".to_owned(),
			direction: ExpandDir::Out,
			edge_tables: Box::new([TableName::from("knows")]),
			edge_binding: EdgeBinding::IdOnly("e".to_owned()),
			target_binding: "b".to_owned(),
			target_label: None,
			predicate: false,
			optional: false,
		}))
	}

	fn constant(value: Value) -> HostScalar {
		HostScalar::Const(ValueData::new(value, "a test").expect("a plain value"))
	}

	/// A select loop that reads the rows of a pipeline continues it: the binding that follows the
	/// first loop and the second loop's body run in the first's body, over the first's source.
	#[test]
	fn a_binding_and_the_loop_that_reads_it_run_in_the_body_of_the_pipeline_above() {
		let mut b = Builder::new();
		let op = matched(&mut b, |b| {
			let bound = bound_scan(b, 0, "a");
			let rows = select_loop(b, 1, bound, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		let lowered = lowered(&b, op);
		let program = &lowered.program;
		assert_eq!(program.pipelines.len(), 1);
		assert!(matches!(&program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::BindRow {
					name: 0
				},
				Instr::EmitEnd {
					src: Operand::Reg(ROW)
				},
				Instr::StopPipeline,
			]
		);
		assert!(!program.pipelines[0].in_place, "a binding hands back to the host");
		assert_eq!(program.fields[0].as_str(), "a");
		assert!(program.splits.is_empty());
		assert_eq!(program.required_context, ContextLevel::Database);
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
	}

	/// A step is a split: the pipeline above it hands each row on, the step is the program's spec
	/// for it, and the pipeline that reads the step starts at the split.
	#[test]
	fn a_step_splits_the_pipeline_above_from_the_pipeline_that_reads_it() {
		let mut b = Builder::new();
		let op = matched(&mut b, |b| {
			let bound = bound_scan(b, 0, "a");
			let expanded = binding_step(b, expansion(), &[bound]);
			let expanded = b.result_of(expanded);
			let rows = select_loop(b, 1, expanded, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		let lowered = lowered(&b, op);
		let program = &lowered.program;
		assert_eq!(program.pipelines.len(), 2);
		assert!(matches!(&program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::BindRow {
					name: 0
				},
				Instr::SplitPush {
					split: 0,
					src: ROW
				},
				Instr::EndRow,
				Instr::StopPipeline,
			]
		);
		assert_eq!(
			program.pipelines[1].source,
			Source::Split {
				split: 0
			}
		);
		let [split] = &program.splits[..] else {
			panic!("one split");
		};
		assert_eq!(split.pipeline, 1);
		assert_eq!(split.build_before, None);
		assert_eq!(
			split.kind,
			SplitKind::Step(BindingStep::Expand(ExpandStep {
				source: "a".to_owned(),
				direction: ExpandDir::Out,
				edge_tables: vec![TableName::from("knows")],
				edge_binding: EdgeBinding::IdOnly("e".to_owned()),
				target_binding: "b".to_owned(),
				target_label: None,
				predicate: None,
				optional: false,
			}))
		);
		// A step reads through the database, and nothing here writes.
		assert_eq!(program.required_context, ContextLevel::Database);
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
	}

	/// A clause of a step is a host scalar: the constant a region yields where it yields a literal,
	/// and a host pipeline over the row otherwise.
	#[test]
	fn a_clause_of_a_step_is_a_constant_or_a_host_pipeline_over_the_row() {
		let expansion = BindStepShape::Expand(Box::new(BindExpandShape {
			predicate: true,
			..{
				let BindStepShape::Expand(expand) = expansion() else {
					unreachable!("an expansion");
				};
				*expand
			}
		}));
		let mut b = Builder::new();
		let op = matched(&mut b, |b| {
			let bound = bound_scan(b, 0, "a");
			let expanded = binding_step(b, expansion, &[bound]);
			row_region(b, expanded, RegionKind::Clause(ClauseRole::Where), |_, row| row);
			let expanded = b.result_of(expanded);
			let rows = select_loop(b, 1, expanded, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		let program = lowered(&b, op).program;
		let SplitKind::Step(BindingStep::Expand(expand)) = &program.splits[0].kind else {
			panic!("an expansion");
		};
		assert_eq!(expand.predicate, Some(HostScalar::Code(0)));
		assert_eq!(program.host_code.len(), 1);
	}

	/// A paging between a sort and the loop that reads it runs in the loop's pipeline, over the
	/// sorted rows.
	#[test]
	fn a_paging_between_a_sort_and_the_loop_that_reads_it_runs_before_the_loop() {
		let mut b = Builder::new();
		let op = matched(&mut b, |b| {
			let bound = bound_scan(b, 0, "a");
			let sorted = sort(b, bound, &["a"]);
			let sorted = b.result_of(sorted);
			let two = b.literal(Value::from(2_i64));
			let paged = step(
				b,
				PhysOp::Limit {
					limit: Some(1),
					start: None,
				},
				sorted,
				&[two],
			);
			let paged = b.result_of(paged);
			let rows = select_loop(b, 1, paged, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		let program = lowered(&b, op).program;
		assert_eq!(program.pipelines.len(), 2);
		assert!(matches!(&program.pipelines[0].code[..], [Instr::BindRow { .. }, ..]));
		assert_eq!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		);
		assert_eq!(program.limits.len(), 1);
		assert_eq!(program.pipelines[1].limits, vec![0]);
	}

	/// A join's build side is a group of the program, its probe side the step's feeder, and the
	/// step names the group.
	#[test]
	fn a_join_lowers_its_build_side_as_a_group_and_its_probe_side_as_the_feeder() {
		let join = BindStepShape::Join(Box::new(BindJoinShape {
			keys: Box::new(["a".to_owned()]),
			join_type: JoinType::Inner,
			null_template: Box::new(["b".to_owned()]),
			residual: false,
		}));
		let mut b = Builder::new();
		let op = matched(&mut b, |b| {
			let build = bound_scan(b, 0, "b");
			let probe = bound_scan(b, 1, "a");
			let joined = binding_step(b, join, &[build, probe]);
			let joined = b.result_of(joined);
			let rows = select_loop(b, 2, joined, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		let program = lowered(&b, op).program;
		// The build side, the probe side, then the reader of the join.
		assert_eq!(program.pipelines.len(), 3);
		assert_eq!(
			program.pipelines.iter().map(|pipeline| pipeline.group).collect::<Vec<_>>(),
			vec![Some(0), None, None]
		);
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::BindRow {
					name: 0
				},
				Instr::EmitEnd {
					src: Operand::Reg(ROW)
				},
				Instr::StopPipeline,
			]
		);
		assert!(matches!(
			program.pipelines[1].code.as_slice(),
			[
				Instr::BindRow { .. },
				Instr::SplitPush {
					split: 0,
					..
				},
				..
			]
		));
		assert_eq!(
			program.pipelines[2].source,
			Source::Split {
				split: 0
			}
		);
		assert_eq!(program.groups.len(), 1);
		assert_eq!(program.groups[0].sorts, 0..0);
		let [split] = &program.splits[..] else {
			panic!("one split");
		};
		assert_eq!(split.pipeline, 2);
		assert_eq!(
			split.kind,
			SplitKind::Step(BindingStep::Join(JoinStep {
				build: 0,
				keys: vec!["a".to_owned()],
				join_type: JoinType::Inner,
				null_template: vec!["b".to_owned()],
				residual: None,
			}))
		);
	}

	/// A join lowered inside the group of a block statement takes the group after that statement's
	/// for its build side. Each pipeline belongs to the group it is lowered in.
	#[test]
	fn a_join_inside_the_group_of_a_statement_builds_in_the_group_after_it() {
		let join = BindStepShape::Join(Box::new(BindJoinShape {
			keys: Box::new(["a".to_owned()]),
			join_type: JoinType::Inner,
			null_template: Box::new(["b".to_owned()]),
			residual: false,
		}));
		let mut b = Builder::new();
		let op = matched(&mut b, |b| {
			let build = bound_scan(b, 0, "b");
			let probe = bound_scan(b, 1, "a");
			let joined = binding_step(b, join, &[build, probe]);
			let joined = b.result_of(joined);
			let rows = select_loop(b, 2, joined, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		let region = b.ir.op(op).expect("the MATCH is built").regions[0];
		let mut em = Emitter::new();
		let mut cx = crate::exec::vm::ir_lower::units::LowerCx::detached();
		let mut facts = FactCache::new();
		// The statement is group 0, so the groups lowered in it number from 1.
		let mark = em.open_group(0);
		let base = super::super::PoolBase {
			groups: 1,
			..super::super::PoolBase::default()
		};
		let reads =
			super::super::lower_pipelines_at(&mut em, &mut cx, &b.ir, &mut facts, region, base)
				.expect("the join lowers inside the group");
		em.close_group(mark).expect("the group closes");
		assert_eq!(reads.groups.len(), 1);
		assert_eq!(
			reads.pipelines.iter().map(|pipeline| pipeline.group).collect::<Vec<_>>(),
			vec![Some(1), Some(0), Some(0)]
		);
		let [split] = &reads.splits[..] else {
			panic!("one split");
		};
		let SplitKind::Step(BindingStep::Join(step)) = &split.kind else {
			panic!("a join step");
		};
		assert_eq!(step.build, 1);
	}

	/// A binding streams in the body of the pipeline above it, so opening one as the source of a
	/// pipeline is an invariant break.
	#[test]
	fn a_binding_opened_as_the_source_of_a_pipeline_is_an_invariant_break() {
		let mut b = Builder::new();
		let mut bound = None;
		matched(&mut b, |b| {
			let rows = bound_scan(b, 0, "a");
			bound = Some(rows);
			let rows = select_loop(b, 1, rows, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		let bind = super::super::producer_of(&b.ir, bound.expect("the scan is bound"))
			.expect("the binding is an op");
		let answer = open_step(
			&mut Emitter::new(),
			&mut LowerCx::detached(),
			&b.ir,
			&mut FactCache::new(),
			&mut Reads::empty(super::super::PoolBase::default()),
			bind,
		);
		match answer {
			Err(IrError::Verify(error)) => {
				assert_eq!(
					error.detail,
					"a binding that streams, reached as the source of a pipeline"
				);
			}
			Err(error) => panic!("an invariant break, not {error}"),
			Ok(_) => panic!("a binding never opens as a source"),
		}
	}

	/// A step that no pipeline reads, and a stage with no reader of its rows, are refused rather
	/// than skipped.
	#[test]
	fn a_step_nothing_reads_is_refused() {
		let mut b = Builder::new();
		let op = matched(&mut b, |b| {
			let bound = bound_scan(b, 0, "a");
			binding_step(b, expansion(), &[bound]);
			let rows = select_loop(b, 1, bound, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		assert!(lower(&b, op).is_err());
	}

	/// A step whose rows two loops read is an invariant break: the fold gives each step one
	/// reader.
	#[test]
	fn a_step_read_by_two_loops_is_an_invariant_break() {
		let mut b = Builder::new();
		let op = matched(&mut b, |b| {
			let bound = bound_scan(b, 0, "a");
			let expanded = binding_step(b, expansion(), &[bound]);
			let expanded = b.result_of(expanded);
			select_loop(b, 2, expanded, |_, element| element);
			let rows = select_loop(b, 1, expanded, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		assert!(matches!(
			lower(&b, op),
			Err(IrError::Invariant {
				what: "ir_lower::pipe: a step read more than once"
			})
		));
	}

	/// The entry of the dispatch lowers a container whose statement is a `phys.match`.
	#[test]
	fn the_dispatch_lowers_a_container_whose_statement_is_a_match() {
		let mut b = Builder::new();
		matched(&mut b, |b| {
			let bound = bound_scan(b, 0, "a");
			let rows = select_loop(b, 1, bound, |_, element| element);
			b.result_of(rows)
		});
		let b = placed(b);
		let lowered = lower_indexed(
			&b.ir,
			&crate::exec::ir::core::facts::FactCache::new(),
			&crate::exec::vm::ir_lower::NoIndexes,
		)
		.expect("the container lowers");
		assert_eq!(lowered.program.pipelines.len(), 1);
	}
}
