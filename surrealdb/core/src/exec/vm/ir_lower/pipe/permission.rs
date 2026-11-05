//! `phys.permission_filter` and the conjuncts of a filter that are judged once.
//!
//! # The permission filter
//!
//! A table's permission predicate is judged in the frame its guard names, under the `AUTH LIMIT`
//! of the definition that supplied it, and is skipped when the statement's permissions are not
//! checked ([`Instr::JumpIfPermissionsBypassed`]). The ops of its region carry that frame, so the
//! instructions emitted for them name the permission frame of the program and run in a privileged
//! frame, which holds none of the caller's bindings. A row the predicate does not admit ends.
//!
//! The conjuncts that read nothing of the row are the op's last region and are judged first.
//!
//! A predicate that reads `COMPUTED` fields of the table is judged by a host pipeline over a copy
//! of the row ([`Instr::JudgeCopy`]): the fields the op
//! states are derived onto the copy in order, each body a `FieldFrame(Computed)` region of the op
//! evaluated in the derived frame under its field's `AUTH LIMIT`, and the predicate is then
//! evaluated over the copy in the frame of its guard. The row as stored carries none of them and
//! goes on as it came. A derivation the catalog answered before a catalog change is planned again
//! when the statement is reached ([`IrError::LateBind`]).
//!
//! # Conjuncts judged once
//!
//! The conjuncts of a filter or a permission that read nothing of the row have one answer for the
//! whole run of the statement. They are judged at the first row that reaches them and the verdict
//! is held ([`Instr::VerdictJump`], [`Instr::VerdictSet`]): a later row jumps past them where the
//! verdict admits and ends where it refuses.

use super::{Body, EndJumps, region_frame, row_arg};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{FrameId, RegionId};
use crate::exec::ir::logical::ops::DerivedField;
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::phys::ops::{ComputedReadsShape, DerivedReads, PermissionFilterAttrs};
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::expr::{Lowerer, Needs};
use crate::exec::vm::ir_lower::stage::{Derive, derive_fields};
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::program::{
	FrameMode, Instr, Label, Operand, PermissionSpec, Pipeline, ROW, Source,
};
use crate::expr::logical::{EvalMode, GuardKind};

impl Body<'_, '_, '_> {
	/// Lowers the permission filter `step_data`, whose attributes are `attrs`.
	pub(super) fn permission_filter(
		&mut self,
		step_data: &OpData,
		attrs: &PermissionFilterAttrs,
	) -> Result<(), IrError> {
		self.require_row("a permission over a projection")?;
		let derived = derived_fields(attrs.computed.as_ref())?;
		let Some((predicate, rest)) = step_data.regions.split_first() else {
			return Err(invariant("a permission filter with no predicate"));
		};
		// The predicate, a region per derived field, and the conjuncts judged once, if any.
		let (derivations, once) = if rest.len() == derived.len() {
			(rest, None)
		} else if rest.len() == derived.len() + 1 {
			(&rest[..derived.len()], rest.last().copied())
		} else {
			return Err(invariant("a permission filter hosts a region for each derived field"));
		};
		let predicate = *predicate;
		let frame = guard_frame(self.em, self.ir, predicate, attrs.guard)?;
		let bypassed = self.em.label()?;
		self.em.emit(Instr::JumpIfPermissionsBypassed {
			target: bypassed,
		});
		if let Some(once) = once {
			self.once_verdict(once, Some(frame))?;
		}
		if derived.is_empty() {
			self.judge(predicate, self.end, Some(frame))?;
		} else {
			let fields: Vec<(&DerivedField, RegionId)> =
				derived.iter().zip(derivations.iter().copied()).collect();
			self.judge_copy(predicate, frame, &fields)?;
		}
		self.em.bind(bypassed)
	}

	/// Ends the row where the predicate the region `predicate` yields is falsy over a copy of the
	/// row with `fields` derived onto it, and falls through where it is truthy. The copy is judged
	/// by a host pipeline of the program ([`derived_verdict`]); the row goes on as it came.
	fn judge_copy(
		&mut self,
		predicate: RegionId,
		frame: (FrameId, u16),
		fields: &[(&DerivedField, RegionId)],
	) -> Result<(), IrError> {
		let ir = self.ir;
		judge_copy(
			self.em,
			self.cx,
			ir,
			predicate,
			frame,
			fields,
			self.end,
			&mut self.jumps,
			&mut self.needs,
		)
	}

	/// Judges the conjuncts the region `once` yields, which read nothing of the row, at the first
	/// row that reaches them in a run of the statement, and keeps the verdict for the rest of the
	/// run.
	///
	/// A row after the first jumps past them where the verdict admits and to the end of the row
	/// where it refuses. The conjuncts are judged in `frame` where one is given.
	pub(super) fn once_verdict(
		&mut self,
		once: RegionId,
		frame: Option<(FrameId, u16)>,
	) -> Result<(), IrError> {
		let cell = self.em.verdict()?;
		let (refused, rejected, admitted) = (self.em.label()?, self.em.label()?, self.em.label()?);
		self.em.emit(Instr::VerdictJump {
			cell,
			admitted,
			refused: rejected,
		});
		self.judge(once, refused, frame)?;
		self.em.emit(Instr::VerdictSet {
			cell,
			admits: true,
		});
		self.em.emit(Instr::Jump {
			target: admitted,
		});
		self.em.bind(refused)?;
		self.em.emit(Instr::VerdictSet {
			cell,
			admits: false,
		});
		self.em.bind(rejected)?;
		self.em.emit(Instr::Jump {
			target: self.end,
		});
		self.jumps.other += 1;
		self.em.bind(admitted)
	}
}

/// The fields `computed` states a permission predicate derives, in order: none where it reads no
/// computed field. A derivation the catalog answered before a catalog change of the request is
/// planned again when the statement is reached; one whose body has no logical form is an internal
/// error.
pub(super) fn derived_fields(
	computed: Option<&ComputedReadsShape>,
) -> Result<&[DerivedField], IrError> {
	match computed.map(|computed| &computed.derived) {
		None => Ok(&[]),
		Some(DerivedReads::Stated(fields)) => Ok(fields),
		Some(DerivedReads::LateBound) => Err(IrError::LateBind {
			what: "ir_lower::pipe: the computed fields of a permission read before a catalog change",
		}),
		Some(DerivedReads::Unlowerable) => Err(IrError::Invariant {
			what: "ir_lower::pipe: a computed field a permission derives has no logical form",
		}),
	}
}

/// Ends the row of the pipeline in flight in `em` where the predicate the region `predicate`
/// yields is falsy over a copy of [`ROW`] with `fields` derived onto it, and falls through where
/// it is truthy ([`Instr::JudgeCopy`]). The jump to `end` is recorded in `jumps`; what the host
/// pipeline that judges the copy needs of the run is merged into `needs`. [`ROW`] goes on as it
/// came.
#[allow(clippy::too_many_arguments)]
pub(super) fn judge_copy(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	predicate: RegionId,
	frame: (FrameId, u16),
	fields: &[(&DerivedField, RegionId)],
	end: Label,
	jumps: &mut EndJumps,
	needs: &mut Needs,
) -> Result<(), IrError> {
	let pipeline = {
		let aside = em.set_aside();
		let mut lowerer = Lowerer::in_context(&mut *em, ir, cx);
		let beside = lowerer.beside();
		let built = derived_verdict(&mut lowerer, predicate, frame, fields);
		lowerer.rejoin(beside);
		*needs = needs.merged(&lowerer.needs());
		lowerer.em.restore(aside);
		built.and_then(|pipeline| em.host_code(pipeline))?
	};
	let verdict = em.reg()?;
	em.emit(Instr::JudgeCopy {
		pipeline,
		src: ROW,
		dst: verdict,
	});
	let at = em.emit(Instr::JumpIf {
		cond: Operand::Reg(verdict),
		truthy: false,
		target: end,
	});
	jumps.filters.push(at);
	Ok(())
}

/// The host pipeline over a copy of a row that derives `fields` onto it in order, in the derived
/// frame each body's region carries, and emits the value of the predicate region `predicate` over
/// it, evaluated in `frame`, the frame of the predicate's guard.
///
/// The copy is the pipeline's row, which need not be a record, so it is no object row.
fn derived_verdict(
	lowerer: &mut Lowerer<'_>,
	predicate: RegionId,
	(container, program): (FrameId, u16),
	fields: &[(&DerivedField, RegionId)],
) -> Result<Pipeline, IrError> {
	let ir = lowerer.ir;
	let row = row_arg(ir, predicate)
		.ok_or(IrError::Stale("ir_lower::pipe: a permission predicate takes the row"))?;
	lowerer.bind_row(row, false);
	derive_fields(lowerer, fields, Derive::Gate, None)?;
	lowerer.in_frame(container, program, |lowerer| {
		let verdict = lowerer.region_value(predicate)?;
		let out = lowerer.em.reg()?;
		lowerer.emit(Instr::Load {
			dst: out,
			src: verdict,
		});
		lowerer.emit(Instr::Emit {
			src: out,
		});
		lowerer.emit(Instr::EndRow);
		lowerer.em.finish_pipeline(Source::Once)
	})
}

/// The frame the predicate region `region` is judged in: the container frame its ops carry, and the
/// permission frame of the program, in `em`'s pool, that evaluates under its guard and `AUTH
/// LIMIT`.
///
/// The frame the ops carry is the guard's own: a read guard is a permission frame and a write guard
/// a mutable one, which `guard` says too. A guard that disagrees is an internal error.
pub(super) fn guard_frame(
	em: &mut Emitter,
	ir: &Ir,
	region: RegionId,
	guard: GuardKind,
) -> Result<(FrameId, u16), IrError> {
	let container = region_frame(ir, region)?;
	let held = ir.frame(container).ok_or(IrError::Stale("ir_lower::pipe: frame"))?;
	let agrees = matches!(
		(held.eval.mode, guard),
		(EvalMode::Permission, GuardKind::Read) | (EvalMode::MutablePermission, GuardKind::Write)
	);
	if !agrees {
		return Err(invariant("a permission filter outside the frame of its guard"));
	}
	let program = em.permission_frame(PermissionSpec {
		mode: FrameMode::Guard(guard),
		auth: held.eval.auth.clone(),
		column: false,
	})?;
	Ok((container, program))
}

#[cfg(test)]
mod tests {
	use surrealdb_iam::{AuthLimit, Level};
	use surrealdb_strand::{Strand, TableName};

	use super::super::tests::{lowered, pipeline, placed, row_region, scan, step};
	use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
	use crate::exec::ir::error::IrError;
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::ids::{OpId, ValueId};
	use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::DerivedField;
	use crate::exec::ir::phys::ops::{
		ComputedReadsShape, DerivedReads, PermissionFilterAttrs, PhysOp,
	};
	use crate::exec::vm::ir_lower::LowerCx;
	use crate::exec::vm::ir_lower::pipe::lower_reads;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::{FrameMode, Instr, Operand, PermissionSpec, ROW};
	use crate::expr::BinaryOperator;
	use crate::expr::logical::{EvalFrame, GuardKind};
	use crate::val::Value;

	/// The region of `owner` that a frame isolates from above: the statement timestamp, then the
	/// row `body` builds its value over.
	fn permission_region(
		b: &mut Builder,
		owner: OpId,
		body: impl FnOnce(&mut Builder, ValueId) -> ValueId,
	) {
		b.region_with_args(
			owner,
			RegionKind::PermissionFrame,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| body(b, args[1]),
		);
	}

	/// A scan whose rows a permission filter of `attrs` judges with `owner = 1`, in the frame
	/// `frame`, and with a second region of conjuncts judged once when `once`.
	fn judged(attrs: PermissionFilterAttrs, frame: EvalFrame, once: bool) -> Builder {
		let mut b = Builder::new();
		let id = b.frame(frame);
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let filter: OpId = b.in_frame(id, |b| {
				let filter = step(b, PhysOp::PermissionFilter(Box::new(attrs)), rows, &[]);
				permission_region(b, filter, |b, row| {
					let owner = b.add(
						ExprOp::Access {
							field: Strand::from("owner"),
						},
						&[row],
					);
					let one = b.literal(Value::from(1_i64));
					b.binary(BinaryOperator::Equal, owner, one)
				});
				if once {
					permission_region(b, filter, |b, _| b.literal(Value::Bool(true)));
				}
				filter
			});
			b.result_of(filter)
		});
		placed(b)
	}

	fn guard(guard: GuardKind) -> PermissionFilterAttrs {
		PermissionFilterAttrs {
			guard,
			hides_access_paths: false,
			computed: None,
		}
	}

	#[test]
	fn a_permission_is_judged_in_its_frame_unless_permissions_are_bypassed() {
		let b = judged(guard(GuardKind::Read), EvalFrame::permission(None), false);
		let lowered = lowered(&b);
		let program = &lowered.program;
		assert_eq!(
			program.permissions,
			vec![PermissionSpec {
				mode: FrameMode::Guard(GuardKind::Read),
				auth: None,
				column: false,
			}]
		);
		let code = &program.pipelines[0].code;
		assert!(matches!(code[0], Instr::JumpIfPermissionsBypassed { .. }), "{code:?}");
		assert!(
			code.iter().any(|instr| matches!(
				instr,
				Instr::Access {
					src: Operand::Reg(ROW),
					frame: Some(0),
					..
				}
			)),
			"{code:?}"
		);
	}

	#[test]
	fn a_write_guard_is_a_mutable_permission_frame_under_the_definers_limit() {
		let limit = AuthLimit::new(Level::Root, None);
		let b = judged(
			guard(GuardKind::Write),
			EvalFrame::mutable_permission(Some(limit.clone())),
			false,
		);
		let program = lowered(&b).program;
		assert_eq!(
			program.permissions,
			vec![PermissionSpec {
				mode: FrameMode::Guard(GuardKind::Write),
				auth: Some(limit),
				column: false,
			}]
		);
	}

	#[test]
	fn a_guard_that_disagrees_with_the_frame_of_its_op_is_an_internal_error() {
		let b = judged(guard(GuardKind::Write), EvalFrame::permission(None), false);
		assert!(matches!(
			lower_reads(&mut LowerCx::detached(), &b.ir, false),
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				..
			}))
		));
	}

	#[test]
	fn the_conjuncts_that_read_nothing_of_the_row_are_judged_once_and_first() {
		let b = judged(guard(GuardKind::Read), EvalFrame::permission(None), true);
		let lowered = lowered(&b);
		let program = &lowered.program;
		assert_eq!(program.verdicts, vec![None]);
		let code = &program.pipelines[0].code;
		let verdict = code.iter().position(|instr| matches!(instr, Instr::VerdictJump { .. }));
		let access = code.iter().position(|instr| matches!(instr, Instr::Access { .. }));
		assert!(verdict < access, "{code:?}");
		assert!(code.iter().any(|instr| matches!(
			instr,
			Instr::VerdictSet {
				admits: true,
				..
			}
		)));
		assert!(code.iter().any(|instr| matches!(
			instr,
			Instr::VerdictSet {
				admits: false,
				..
			}
		)));
	}

	#[test]
	fn a_filter_judges_the_conjuncts_it_holds_once_before_its_predicate() {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let filter = step(b, PhysOp::Filter, rows, &[]);
			row_region(b, filter, RegionKind::Clause(ClauseRole::Where), |b, row| {
				b.add(
					ExprOp::Access {
						field: Strand::from("a"),
					},
					&[row],
				)
			});
			row_region(b, filter, RegionKind::Clause(ClauseRole::Where), |b, _| {
				b.literal(Value::Bool(false))
			});
			b.result_of(filter)
		});
		let b = placed(b);
		let lowered = lowered(&b);
		assert_eq!(lowered.program.verdicts, vec![None]);
		let code = &lowered.program.pipelines[0].code;
		assert!(matches!(code[0], Instr::VerdictJump { .. }), "{code:?}");
		// The jumps of a held verdict land on code positions: a refusal on the jump that ends the
		// row, right after the instruction that holds the refusal, and an admission just past it.
		let Instr::VerdictJump {
			admitted,
			refused,
			..
		} = code[0]
		else {
			unreachable!()
		};
		let (admitted, refused) = (admitted as usize, refused as usize);
		assert!(
			matches!(
				code.get(refused - 1),
				Some(Instr::VerdictSet {
					admits: false,
					..
				})
			),
			"{code:?}"
		);
		assert!(matches!(code.get(refused), Some(Instr::Jump { .. })), "{code:?}");
		assert_eq!(admitted, refused + 1, "{code:?}");
	}

	fn computed(derived: DerivedReads) -> PermissionFilterAttrs {
		PermissionFilterAttrs {
			guard: GuardKind::Read,
			hides_access_paths: false,
			computed: Some(ComputedReadsShape {
				table: TableName::from("t"),
				fields: Some(vec!["c".to_owned()]),
				derived,
			}),
		}
	}

	#[test]
	fn a_permission_whose_derivation_predates_a_catalog_change_binds_late() {
		let b = judged(computed(DerivedReads::LateBound), EvalFrame::permission(None), false);
		assert!(matches!(
			lower_reads(&mut LowerCx::detached(), &b.ir, false),
			Err(IrError::LateBind { .. })
		));
	}

	/// A permission that reads the computed field `c` judges a copy of the row with `c` derived
	/// onto it, by a host pipeline, and the row goes on as it came.
	#[test]
	fn a_permission_that_reads_computed_fields_judges_a_copy_with_them_derived() {
		let attrs = computed(DerivedReads::Stated(Box::new([DerivedField {
			name: Strand::from("c"),
			kind: None,
			caller_bound: false,
		}])));
		let mut b = Builder::new();
		let guard = b.frame(EvalFrame::permission(None));
		let derived = b.frame(EvalFrame::permission(None));
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let filter: OpId = b.in_frame(guard, |b| {
				let filter = step(b, PhysOp::PermissionFilter(Box::new(attrs)), rows, &[]);
				permission_region(b, filter, |b, row| {
					let c = b.add(
						ExprOp::Access {
							field: Strand::from("c"),
						},
						&[row],
					);
					let one = b.literal(Value::from(1_i64));
					b.binary(BinaryOperator::Equal, c, one)
				});
				filter
			});
			b.in_frame(derived, |b| {
				b.region_with_args(
					filter,
					RegionKind::FieldFrame(FieldRole::Computed),
					&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
					|b, _| b.literal(Value::from(1_i64)),
				);
			});
			b.result_of(filter)
		});
		let b = placed(b);
		let program = lowered(&b).program;
		let code = &program.pipelines[0].code;
		assert!(matches!(code[0], Instr::JumpIfPermissionsBypassed { .. }), "{code:?}");
		assert!(
			code.iter().any(|instr| matches!(
				instr,
				Instr::JudgeCopy {
					pipeline: 0,
					src: ROW,
					..
				}
			)),
			"{code:?}"
		);
		let judged = &program.host_code[0].code;
		let begins = judged.iter().position(|instr| matches!(instr, Instr::BeginComputing { .. }));
		let lands = judged.iter().position(|instr| matches!(instr, Instr::LandRow { .. }));
		assert!(matches!((begins, lands), (Some(b), Some(l)) if b < l), "{judged:?}");
		assert!(
			program.permissions.iter().any(|spec| spec.mode == FrameMode::Derived),
			"{:?}",
			program.permissions
		);
	}

	#[test]
	fn a_permission_missing_a_region_for_a_derived_field_is_an_invariant() {
		let attrs = computed(DerivedReads::Stated(Box::new([DerivedField {
			name: Strand::from("c"),
			kind: None,
			caller_bound: false,
		}])));
		let b = judged(attrs, EvalFrame::permission(None), false);
		assert!(matches!(
			lower_reads(&mut LowerCx::detached(), &b.ir, false),
			Err(IrError::Verify(..))
		));
	}
}
