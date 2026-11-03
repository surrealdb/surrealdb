//! The nearest-neighbour searches: the gate a `phys.knn_scan` judges its candidates on, and the
//! ranking of a `phys.knn_topk`.
//!
//! # The gate of a KNN scan
//!
//! An index search judges each candidate it finds before it counts it among the `k` it returns, so
//! the `k` records it returns are records the statement may see. What it judges a candidate on is a
//! host pipeline of the program ([`gate_code`]) that runs once over the candidate's record as its
//! row, and emits one row where the record passes and none where it does not. The row is read as
//! a value, not as an object row of the pipeline: its fields are read from it as from any value.
//!
//! A gate pipeline judges each permission it is given in turn, then its condition. A permission
//! is judged in its guard's frame under the `AUTH LIMIT` of its definition, and is skipped where
//! the statement's permissions are not checked ([`Instr::JumpIfPermissionsBypassed`]). One that
//! reads `COMPUTED` fields is judged over a copy of the record with them derived onto it, as
//! `phys.permission_filter` judges it ([`Instr::JudgeCopy`]). The condition is judged in the
//! statement's own frame.
//!
//! # The ranking of a brute-force search
//!
//! `phys.knn_topk` is a breaker. The pipeline that feeds it ranks each row by the distance of the
//! vector at the op's field path from the query vector ([`Instr::KnnRank`]) and pushes it to a
//! sort that keeps the `k` nearest; a row with no vector the distance applies to takes no rank and
//! ends. The pipeline above reads the kept rows in order of their distance.

use std::sync::Arc;

use super::{Body, Branching, EndJumps, end_row, row_arg, yielded_value};
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{FrameId, RegionId};
use crate::exec::ir::phys::ops::KnnPermissionShape;
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::expr::{Lowerer, Needs, ends_context};
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::program::{Instr, KnnRank, Label, Operand, ROW, Source};
use crate::val::Value;

/// The host pipeline, of the program `em` builds, that judges one candidate of a KNN search: it
/// emits a row when the record passes each of `permissions` in turn and then the condition region
/// `cond`, and none when it does not.
///
/// Each permission is its `PermissionFrame` region and the `FieldFrame(Computed)` regions of the
/// fields it derives, with the shape the scan's filter states for it.
/// The pipeline is emitted apart from the pipeline being built and shares only the program's
/// pools with it; what it needs of the run is merged into `needs`.
pub(in crate::exec::vm::ir_lower) fn gate_code(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	permissions: &[(&[RegionId], &KnnPermissionShape)],
	cond: Option<RegionId>,
	needs: &mut Needs,
) -> Result<u16, IrError> {
	let aside = em.set_aside();
	let built = gate_pipeline(em, cx, ir, permissions, cond, needs);
	em.restore(aside);
	em.host_code(built?)
}

/// The pipeline of [`gate_code`], emitted into `em` with no other pipeline open.
fn gate_pipeline(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	permissions: &[(&[RegionId], &KnnPermissionShape)],
	cond: Option<RegionId>,
	needs: &mut Needs,
) -> Result<crate::exec::vm::program::Pipeline, IrError> {
	let end = em.label()?;
	let mut jumps = EndJumps::default();
	for (regions, shape) in permissions {
		let derived = super::permission::derived_fields(shape.computed.as_ref())?;
		let Some((region, derivations)) = regions.split_first() else {
			return Err(IrError::Invariant {
				what: "ir_lower::pipe: a KNN permission with no predicate",
			});
		};
		if derivations.len() != derived.len() {
			return Err(IrError::Invariant {
				what: "ir_lower::pipe: a KNN permission hosts a region for each derived field",
			});
		}
		let frame = super::permission::guard_frame(em, ir, *region, shape.guard)?;
		let bypassed = em.label()?;
		em.emit(Instr::JumpIfPermissionsBypassed {
			target: bypassed,
		});
		if derived.is_empty() {
			judge(em, cx, ir, *region, end, Some(frame), &mut jumps, needs)?;
		} else {
			let fields: Vec<_> = derived.iter().zip(derivations.iter().copied()).collect();
			super::permission::judge_copy(
				em, cx, ir, *region, frame, &fields, end, &mut jumps, needs,
			)?;
		}
		em.bind(bypassed)?;
	}
	if let Some(cond) = cond {
		judge(em, cx, ir, cond, end, None, &mut jumps, needs)?;
	}
	let passed = em.constant(Value::Bool(true))?;
	let dst = em.reg()?;
	em.emit(Instr::Load {
		dst,
		src: Operand::Const(passed),
	});
	em.emit(Instr::Emit {
		src: dst,
	});
	end_row(em, end, &jumps)?;
	em.finish_pipeline(Source::Once)
}

/// Ends the row where the predicate the region `region` yields over the candidate is falsy, and
/// falls through where it is truthy, judged in `frame` where one is given.
#[allow(clippy::too_many_arguments)]
fn judge(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	region: RegionId,
	end: Label,
	frame: Option<(FrameId, u16)>,
	jumps: &mut EndJumps,
	needs: &mut Needs,
) -> Result<(), IrError> {
	let predicate = yielded_value(ir, region)?;
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	if let Some(arg) = row_arg(ir, region) {
		lowerer.bind_arg(arg, Operand::Reg(ROW));
		lowerer.bind_row(arg, false);
	}
	// A predicate a `RETURN` held in it ends is judged by the value of the context, which the
	// `RETURN` may set before the predicate's own branches are reached.
	let context = ends_context(ir, region);
	let mut branch = |lowerer: &mut Lowerer<'_>| match context {
		true => {
			let cond = lowerer.region_value(region)?;
			Branching {
				lowerer,
				ir,
				end,
				jumps,
			}
			.jump(cond, false, end, false)
		}
		false => Branching {
			lowerer,
			ir,
			end,
			jumps,
		}
		.branch(predicate, false, end),
	};
	match frame {
		Some((container, program)) => lowerer.in_frame(container, program, branch)?,
		None => branch(&mut lowerer)?,
	}
	*needs = needs.merged(&lowerer.needs());
	Ok(())
}

impl Body<'_, '_, '_> {
	/// Ranks the row by the distance `rank` measures of the vector at `field`, and pushes the rank
	/// to the sort `id`, which keeps the nearest rows. A row with no vector to rank ends.
	pub(super) fn knn_push(
		&mut self,
		id: u16,
		field: &FieldPath,
		rank: &Arc<KnnRank>,
	) -> Result<(), IrError> {
		self.require_row("a KNN over a projection")?;
		let path = self.em.path(field.clone())?;
		let root = match field.0.split_first() {
			Some((FieldPathPart::Field(name), rest)) => {
				Some((self.em.field(name)?, self.em.path(FieldPath(rest.to_vec()))?))
			}
			_ => None,
		};
		let key = self.em.reg()?;
		self.em.emit(Instr::KnnRank {
			src: Operand::Reg(ROW),
			path,
			root,
			rank: Arc::clone(rank),
			dst: key,
			none: self.end,
		});
		self.jumps.other += 1;
		self.em.emit(Instr::SortPush {
			sort: id,
			keys: key,
			count: 1,
			src: ROW,
		});
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use surrealdb_strand::{Strand, TableName};

	use super::super::tests::{lowered, param, pipeline, placed, row_region, scan};
	use super::super::{Lowered, lower_reads};
	use crate::catalog::{Distance, Index, IndexDefinition, IndexId};
	use crate::exec::field_path::FieldPath;
	use crate::exec::index::access_path;
	use crate::exec::ir::error::IrError;
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::IndexRef;
	use crate::exec::ir::iface::ids::{OpId, ValueId};
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::op_kind::OpKind;
	use crate::exec::ir::phys::ops::{
		ComputedReadsShape, DerivedReads, KnnFilterShape, KnnPermissionShape, KnnScanAttrs,
		KnnTargetShape, KnnTopKAttrs, PhysOp,
	};
	use crate::exec::vm::ir_lower::LowerCx;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::payload::Spill;
	use crate::exec::vm::program::{
		FrameMode, HostScalar, Instr, KnnGate, KnnVector, Operand, PermissionSpec, ROW, Source,
	};
	use crate::expr::BinaryOperator;
	use crate::expr::logical::{EvalFrame, GuardKind};
	use crate::val::{Number, Value};

	/// A brute-force search of the `2` rows of `rows` nearest `target` by their field `v`, the
	/// target read from `deferred` where it is not a literal, evaluated before any row is read
	/// where its flag says so.
	fn knn_topk(b: &mut Builder, rows: ValueId, deferred: Option<(ValueId, bool)>) -> OpId {
		let (vector, operands) = match deferred {
			Some((target, before_rows)) => {
				(KnnTargetShape::Deferred("$v".to_owned(), before_rows), vec![rows, target])
			}
			None => (KnnTargetShape::Literal(vec![Number::Int(1), Number::Int(2)]), vec![rows]),
		};
		b.create_kind(
			OpKind::Phys(PhysOp::KnnTopK(Box::new(KnnTopKAttrs {
				field: FieldPath::field("v"),
				vector,
				k: 2,
				distance: Distance::Euclidean,
			}))),
			&operands,
			1,
		)
	}

	/// A scan feeding a brute-force search, which a pipeline of its own reads; the target is a
	/// parameter where `deferred` is given, evaluated before any row is read where it says so.
	fn searched(deferred: Option<bool>) -> Builder {
		let mut b = Builder::new();
		let target = deferred.map(|before_rows| (param(&mut b, "q"), before_rows));
		let feeder = pipeline(&mut b, 0, scan);
		let fed = b.result_of(feeder);
		pipeline(&mut b, 1, |b| {
			let nearest = knn_topk(b, fed, target);
			b.result_of(nearest)
		});
		placed(b)
	}

	#[test]
	fn a_brute_force_search_ranks_the_rows_of_its_feeder_and_keeps_the_nearest() {
		let lowered = lowered(&searched(None));
		let program = &lowered.program;
		assert_eq!(program.pipelines.len(), 2);
		let feeder = &program.pipelines[0].code;
		let ranked = feeder
			.iter()
			.position(|instr| {
				matches!(
					instr,
					Instr::KnnRank {
						src: Operand::Reg(ROW),
						..
					}
				)
			})
			.expect("the feeder ranks each row");
		let Instr::KnnRank {
			path,
			rank,
			dst,
			none,
			..
		} = &feeder[ranked]
		else {
			unreachable!()
		};
		assert_eq!(program.paths[*path as usize], FieldPath::field("v"));
		assert_eq!(rank.vector, KnnVector::Literal(vec![Number::Int(1), Number::Int(2)].into()));
		assert_eq!(
			feeder[ranked + 1],
			Instr::SortPush {
				sort: 0,
				keys: *dst,
				count: 1,
				src: ROW
			}
		);
		// A row with no vector to rank ends with no push.
		assert_eq!(feeder[*none as usize], Instr::EndRow);
		assert_eq!(
			program.pipelines[1].source,
			Source::Sorted {
				sort: 0
			}
		);
		let sort = &program.sorts[0];
		assert_eq!(sort.keep, Some(2));
		assert_eq!(sort.held, Spill::Bounded);
		assert!(sort.keys[0].ascending && !sort.spill && !sort.pushdown);
		assert_eq!(sort.knn_slot, Some(0));
		assert_eq!(sort.knn_query, None);
	}

	#[test]
	fn a_deferred_query_vector_is_evaluated_by_the_host_when_the_statement_starts() {
		let lowered = lowered(&searched(Some(false)));
		let program = &lowered.program;
		let KnnVector::Query(sort) = program.pipelines[0]
			.code
			.iter()
			.find_map(|instr| {
				if let Instr::KnnRank {
					rank,
					..
				} = instr
				{
					Some(rank.vector.clone())
				} else {
					None
				}
			})
			.expect("the feeder ranks each row")
		else {
			panic!("a deferred vector is the query of its sort");
		};
		let query = program.sorts[usize::from(sort)].knn_query.as_ref().expect("a query");
		assert!(matches!(query.vector.scalar, HostScalar::Code(_)));
		assert_eq!(&*query.vector.text, "$v");
		assert_eq!(query.group, None);
	}

	/// A ranking against a vector in hand before any row is read — a literal, or a query the run
	/// evaluates as it starts — runs in the scan's body over the stored record, reading the vector
	/// from the field its path starts at; one whose vector the first ranked row has the host
	/// evaluate does not.
	#[test]
	fn a_ranking_against_a_vector_in_hand_runs_in_place() {
		for (deferred, in_place) in [(None, true), (Some(true), true), (Some(false), false)] {
			let lowered = lowered(&searched(deferred));
			let program = &lowered.program;
			let feeder = &program.pipelines[0];
			let (root, rank) = feeder
				.code
				.iter()
				.find_map(|instr| {
					if let Instr::KnnRank {
						root,
						rank,
						..
					} = instr
					{
						Some((*root, rank))
					} else {
						None
					}
				})
				.expect("the feeder ranks each row");
			assert_eq!(rank.before_rows, in_place, "{deferred:?}");
			assert_eq!(feeder.in_place, in_place, "{deferred:?}");
			let (field, rest) = root.expect("the path starts at a field");
			assert_eq!(program.fields[field as usize].as_str(), "v");
			assert!(program.paths[rest as usize].is_empty());
			assert!(program.sorts[0].floors_at_keep);
		}
	}

	fn definition() -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(1),
			name: Strand::from("by_v"),
			table_name: "t".into(),
			cols: Vec::new(),
			index: Index::Idx,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
			nulls_not_distinct: false,
		}
	}

	fn resolver() -> HashMap<(TableName, Strand), access_path::IndexRef> {
		HashMap::from([(
			(TableName::from("t"), Strand::from("by_v")),
			access_path::IndexRef::new(&[definition()], 0),
		)])
	}

	/// A pipeline of a KNN scan of `t` whose filter holds one read permission, judged in a
	/// permission frame, and a residual `a = 1`; the permission reads `computed` where given.
	fn gated(computed: Option<ComputedReadsShape>) -> Builder {
		let mut b = Builder::new();
		let frame = b.frame(EvalFrame::permission(None));
		pipeline(&mut b, 0, |b| {
			let scan = b.create_kind(
				OpKind::Phys(PhysOp::KnnScan(Box::new(KnnScanAttrs {
					table: TableName::from("t"),
					read: Default::default(),
					index: IndexRef {
						table: TableName::from("t"),
						index: Strand::from("by_v"),
						position: 0,
						scalar: Box::new([]),
					},
					vector: vec![Number::Int(1)].into(),
					vector_text: None,
					k: 2,
					ef: 40,
					filter: Some(KnnFilterShape {
						permissions: vec![KnnPermissionShape {
							auth: None,
							guard: GuardKind::Read,
							computed,
						}],
						residual: true,
						matches_condition: false,
						prefilter: None,
					}),
					ids_only: false,
				}))),
				&[],
				1,
			);
			b.in_frame(frame, |b| {
				b.region_with_args(
					scan,
					RegionKind::PermissionFrame,
					&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
					|b, args| {
						let owner = b.add(
							ExprOp::Access {
								field: Strand::from("owner"),
							},
							&[args[1]],
						);
						let one = b.literal(Value::from(1_i64));
						b.binary(BinaryOperator::Equal, owner, one)
					},
				);
			});
			row_region(b, scan, RegionKind::Clause(ClauseRole::Where), |b, row| {
				let a = b.add(
					ExprOp::Access {
						field: Strand::from("a"),
					},
					&[row],
				);
				let one = b.literal(Value::from(1_i64));
				b.binary(BinaryOperator::Equal, a, one)
			});
			b.result_of(scan)
		});
		placed(b)
	}

	fn lowered_with_indexes(b: &Builder) -> Result<Lowered, IrError> {
		let resolver = resolver();
		lower_reads(&mut LowerCx::detached().with_indexes(&resolver), &b.ir, false)
	}

	#[test]
	fn a_knn_scan_gates_its_candidates_on_its_permissions_then_each_condition() {
		let lowered = lowered_with_indexes(&gated(None)).expect("the container lowers");
		let program = &lowered.program;
		let Source::Knn {
			slot,
			gate: Some(gate),
			k,
			ef,
			..
		} = &program.pipelines[0].source
		else {
			panic!("a KNN source with a gate: {:?}", program.pipelines[0].source);
		};
		assert_eq!((*k, *ef, *slot), (2, 40, Some(0)));
		assert_eq!(
			*gate,
			KnnGate {
				permission: Some(0),
				residual: Some(1),
				matches_condition: None,
				prefilter: None,
			}
		);
		assert_eq!(
			program.permissions,
			vec![PermissionSpec {
				mode: FrameMode::Guard(GuardKind::Read),
				auth: None,
				column: false,
			}]
		);
		for code in &program.host_code {
			assert_eq!(code.source, Source::Once);
			assert!(matches!(code.code[0], Instr::JumpIfPermissionsBypassed { .. }), "{code:?}");
			assert!(matches!(code.code.last(), Some(Instr::EndRow | Instr::EmitEnd { .. })));
		}
		// The permission is judged in its frame; the residual in the statement's.
		let framed = |code: &[Instr]| {
			code.iter()
				.filter(|instr| {
					matches!(
						instr,
						Instr::Access {
							frame: Some(0),
							..
						}
					)
				})
				.count()
		};
		assert_eq!(framed(&program.host_code[0].code), 1);
		assert_eq!(framed(&program.host_code[1].code), 1);
		assert!(program.host_code[1].code.iter().any(|instr| matches!(
			instr,
			Instr::Access {
				frame: None,
				..
			}
		)));
	}

	#[test]
	fn a_knn_permission_whose_derivation_predates_a_catalog_change_binds_late() {
		let b = gated(Some(ComputedReadsShape {
			table: TableName::from("t"),
			fields: Some(vec!["c".to_owned()]),
			derived: DerivedReads::LateBound,
		}));
		assert!(matches!(lowered_with_indexes(&b), Err(IrError::LateBind { .. })));
	}
}
