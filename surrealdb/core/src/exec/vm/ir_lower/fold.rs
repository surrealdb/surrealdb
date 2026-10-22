//! The lowering the constant folder runs a subexpression through.
//!
//! The folder's subset is the operand subset of `expr.rs` without the ops that read the request's
//! bindings, the row or the statement's clock, or that run code the folder does not see. The
//! folder's eligibility check (`ir::fold::evaluate::eligible_under`) refuses each of those before
//! the fold host lowers a value, so one reached here answers `IrError::Invariant`.

use super::super::fold_host::{ExprLowering, FoldBatch, FoldProgram};
use super::super::program::{Instr, Operand, Pipeline, Program, Source};
use super::dispatch::check_claims;
use super::emit::Emitter;
use super::expr::{Claim, Lowerer, Needs};
use crate::exec::AccessMode;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::ValueId;

/// Lowers the subexpression rooted at `root` to a program of one pipeline and no groups whose
/// body computes the value into the returned register for one empty row and ends the row.
///
/// Every op of the subexpression must be in the folder's operation subset: literal, array, set,
/// object, record_id (any key but a generated one), binary, prefix, postfix, convert, access and
/// index over non-record values, call and method of builtins, truthy, short_circuit, if and yield.
/// Any other op is one the folder's eligibility check refuses first, and answers
/// `IrError::Invariant` naming it.
///
/// The pipeline reads no source and the program is read-only. The body raises no host step but
/// `CallAllowed` and `Call`, and the builtins and methods it calls are resolved against
/// `registry`.
pub(crate) fn lower_expr_parts(
	ir: &Ir,
	registry: &FunctionRegistry,
	root: ValueId,
) -> Result<(Program, u16), IrError> {
	let mut em = Emitter::new();
	let (pipeline, result, claims, needs) = lower_pipeline(&mut em, ir, registry, root)?;
	let mut program = em.finish_program(vec![pipeline], AccessMode::ReadOnly)?;
	program.required_context = needs.context;
	program.keeps_row_order = needs.keeps_row_order;
	if let Some(pipeline) = program.pipelines.first() {
		check_claims(ir, &claims, &pipeline.code, &program, registry)?;
	}
	Ok((program, result))
}

/// Lowers `root` into `em` as one finished pipeline, with the register that holds the value, the
/// claims its instructions made and what its lowering needs. The emitter holds no pipeline in
/// flight on entry. On an error the pipeline in flight is whatever the lowering left.
fn lower_pipeline(
	em: &mut Emitter,
	ir: &Ir,
	registry: &FunctionRegistry,
	root: ValueId,
) -> Result<(Pipeline, u16, Vec<Claim>, Needs), IrError> {
	let mut lowerer = Lowerer::for_fold(em, ir, registry);
	let value = lowerer.value(root)?;
	let needs = lowerer.needs();
	let claims = lowerer.take_claims();
	let result = match value {
		Operand::Reg(reg) => reg,
		Operand::Const(_) | Operand::Field(_) => {
			let dst = em.reg()?;
			em.emit(Instr::Load {
				dst,
				src: value,
			});
			dst
		}
	};
	em.emit(Instr::EndRow);
	let pipeline = em.finish_pipeline(Source::Once)?;
	Ok((pipeline, result, claims, needs))
}

/// Lowers every value of `roots` into one program of one pipeline each, in the order of `roots`,
/// over one set of pools.
///
/// Each value is lowered and checked on its own: the answer for a value is the answer
/// [`lower_expr_parts`] gives it alone, an error included. A value whose lowering or claim check
/// fails leaves no pipeline behind it that any other value uses; its slot holds the error and its
/// pipeline is never run. A slot that holds [`IrError::Capacity`] may have overflowed a pool the
/// values share, so the caller lowers that value alone. The program holds no more than the pools
/// and pipelines, and does not carry the context or row-order needs of its values, which a run of a
/// fold program never reads.
///
/// Answers `Err` only when the program of the whole batch cannot be assembled, in which case the
/// caller lowers each value alone.
pub(crate) fn lower_batch_parts(
	ir: &Ir,
	registry: &FunctionRegistry,
	roots: &[ValueId],
) -> Result<FoldBatch, IrError> {
	let mut em = Emitter::new();
	let mut pipelines = Vec::with_capacity(roots.len());
	let mut claimed: Vec<Vec<Claim>> = Vec::with_capacity(roots.len());
	let mut slots: Vec<Result<(usize, u16), IrError>> = Vec::with_capacity(roots.len());
	for root in roots {
		match lower_pipeline(&mut em, ir, registry, *root) {
			Ok((pipeline, result, claims, _needs)) => {
				slots.push(Ok((pipelines.len(), result)));
				pipelines.push(pipeline);
				claimed.push(claims);
			}
			Err(error) => {
				em.abandon_pipeline();
				slots.push(Err(error));
			}
		}
	}
	let program = em.finish_program(pipelines, AccessMode::ReadOnly)?;
	for slot in &mut slots {
		if let Ok((pipeline, _)) = slot
			&& let Some(held) = program.pipelines.get(*pipeline)
			&& let Err(error) =
				check_claims(ir, &claimed[*pipeline], &held.code, &program, registry)
		{
			*slot = Err(error);
		}
	}
	Ok(FoldBatch {
		program,
		slots,
	})
}

/// The lowering the fold host installs. A unit struct, so a `&'static` reference to it exists
/// with no static state.
pub(crate) struct IrExprLowering;

impl ExprLowering for IrExprLowering {
	/// The program of [`lower_expr_parts`], for the register that holds the value. A value the
	/// lowering does not cover answers its error, which the host reports as not foldable.
	fn lower(
		&self,
		ir: &Ir,
		registry: &FunctionRegistry,
		root: ValueId,
	) -> Result<FoldProgram, IrError> {
		let (program, result) = lower_expr_parts(ir, registry, root)?;
		Ok(FoldProgram {
			program,
			result,
		})
	}

	/// The batch of [`lower_batch_parts`], or `None` when the program of the whole batch cannot
	/// be assembled.
	fn lower_all(
		&self,
		ir: &Ir,
		registry: &FunctionRegistry,
		roots: &[ValueId],
	) -> Option<FoldBatch> {
		lower_batch_parts(ir, registry, roots).ok()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::expr::ops::{
		ExprOp, KeyShape, NestedStrategy, NowWhich, RecordIdAttrs, ShortCircuitOp,
	};
	use crate::exec::vm::interp::{State, Step, run_row};
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::{ArithOp, CompareOp};
	use crate::exec::vm::row::{FieldName, RowReader};
	use crate::exec::vm::scalar::Scalar;
	use crate::expr::BinaryOperator;
	use crate::val::{RecordId, RecordIdKey, Value};

	fn lowered(b: &Builder, root: ValueId) -> (Program, u16) {
		match lower_expr_parts(&b.ir, crate::exec::ir::analysis::cx::builtin_registry(), root) {
			Ok(lowered) => lowered,
			Err(error) => panic!("the expression lowers: {error}"),
		}
	}

	/// What `root` is refused for.
	fn refused(b: &Builder, root: ValueId) -> &'static str {
		match lower_expr_parts(&b.ir, crate::exec::ir::analysis::cx::builtin_registry(), root) {
			Err(IrError::Invariant {
				what,
			}) => what,
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the expression lowers"),
		}
	}

	/// `1 + 2 * 3`.
	fn one_plus_two_times_three(b: &mut Builder) -> ValueId {
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let three = b.literal(Value::from(3_i64));
		let product = b.binary(BinaryOperator::Multiply, two, three);
		b.binary(BinaryOperator::Add, one, product)
	}

	#[test]
	fn lower_expr_parts_arith_and_compare() {
		let mut b = Builder::new();
		let sum = one_plus_two_times_three(&mut b);
		let (program, result) = lowered(&b, sum);
		assert_eq!(program.pipelines.len(), 1);
		assert!(program.groups.is_empty());
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
		let pipeline = &program.pipelines[0];
		assert!(matches!(pipeline.source, Source::Once));
		// The operands are evaluated left to right, the product before the sum that reads it.
		assert_eq!(
			pipeline.code,
			vec![
				Instr::Arith {
					op: ArithOp::Multiply,
					left: Operand::Const(1),
					right: Operand::Const(2),
					dst: 1,
				},
				Instr::Arith {
					op: ArithOp::Add,
					left: Operand::Const(0),
					right: Operand::Reg(1),
					dst: 2,
				},
				Instr::EndRow,
			]
		);
		assert_eq!(result, 2);
		assert_eq!(pipeline.registers, 3);

		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let less = b.binary(BinaryOperator::LessThan, one, two);
		let (program, result) = lowered(&b, less);
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::Compare {
					op: CompareOp::LessThan,
					left: Operand::Const(0),
					right: Operand::Const(1),
					dst: 1,
				},
				Instr::EndRow,
			]
		);
		assert_eq!(result, 1);
	}

	#[test]
	fn lower_expr_parts_short_circuit_and_if() {
		let mut b = Builder::new();
		let left = b.literal(Value::Bool(true));
		let right = b.literal(Value::Bool(false));
		let and = b.short_circuit(ShortCircuitOp::And, left, |_| right);
		let (program, result) = lowered(&b, and);
		// The left value is moved to the result, which stays when the left does not hold.
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::Load {
					dst: 1,
					src: Operand::Const(0),
				},
				Instr::JumpIf {
					cond: Operand::Reg(1),
					truthy: false,
					target: 3,
				},
				Instr::Load {
					dst: 1,
					src: Operand::Const(1),
				},
				Instr::EndRow,
			]
		);
		assert_eq!(result, 1);

		let mut b = Builder::new();
		let cond = b.literal(Value::Bool(true));
		let chosen =
			b.if_else(cond, |b| b.literal(Value::from(1_i64)), |b| b.literal(Value::from(2_i64)));
		let (program, result) = lowered(&b, chosen);
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::JumpIf {
					cond: Operand::Const(0),
					truthy: false,
					target: 3,
				},
				Instr::Load {
					dst: 1,
					src: Operand::Const(1),
				},
				Instr::Jump {
					target: 4,
				},
				Instr::Load {
					dst: 1,
					src: Operand::Const(2),
				},
				Instr::EndRow,
			]
		);
		assert_eq!(result, 1);
	}

	#[test]
	fn lower_expr_parts_collections() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let array = b.add(ExprOp::Array, &[one, two]);
		let (program, result) = lowered(&b, array);
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::NewArray {
					dst: 1,
				},
				Instr::ArrayPush {
					array: 1,
					value: Operand::Const(0),
				},
				Instr::ArrayPush {
					array: 1,
					value: Operand::Const(1),
				},
				Instr::EndRow,
			]
		);
		assert_eq!(result, 1);

		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let set = b.add(ExprOp::Set, &[one]);
		let (program, _) = lowered(&b, set);
		assert_eq!(
			program.pipelines[0].code[..2],
			[
				Instr::NewSet {
					dst: 1,
				},
				Instr::CollectPush {
					collection: 1,
					value: Operand::Const(0),
				},
			]
		);

		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let object = b.add(
			ExprOp::Object {
				keys: vec!["a".to_owned(), "b".to_owned()].into_boxed_slice(),
			},
			&[one, two],
		);
		let (program, result) = lowered(&b, object);
		assert_eq!(program.paths.len(), 2);
		assert_eq!(
			program.pipelines[0].code,
			vec![
				Instr::BuildObject {
					dst: 1,
					fields: vec![(0, Operand::Const(0)), (1, Operand::Const(1))].into_boxed_slice(),
				},
				Instr::EndRow,
			]
		);
		assert_eq!(result, 1);

		let mut b = Builder::new();
		let empty = b.add(
			ExprOp::Object {
				keys: Box::new([]),
			},
			&[],
		);
		let (program, _) = lowered(&b, empty);
		assert_eq!(
			program.pipelines[0].code[0],
			Instr::NewObject {
				dst: 1,
			}
		);
	}

	#[test]
	fn lower_expr_parts_refuses_outside_subset() {
		let mut b = Builder::new();
		let param = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		assert_eq!(refused(&b, param), "ir_lower::fold: expr.free_param");

		let mut b = Builder::new();
		let instant = b.literal(Value::None);
		let now = b.add(
			ExprOp::Now {
				which: NowWhich::Datetime,
			},
			&[instant],
		);
		assert_eq!(refused(&b, now), "ir_lower::fold: expr.now");

		let mut b = Builder::new();
		let row = b.literal(Value::None);
		let field = b.add(
			ExprOp::Field {
				name: "a".into(),
			},
			&[row],
		);
		assert_eq!(refused(&b, field), "ir_lower::fold: expr.field");

		let mut b = Builder::new();
		let subquery = b.add(ExprOp::Subquery(NestedStrategy::Apply), &[]);
		assert_eq!(refused(&b, subquery), "ir_lower::fold: expr.subquery");

		// A refusal anywhere under the root refuses the root.
		let mut b = Builder::new();
		let param = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let one = b.literal(Value::from(1_i64));
		let sum = b.binary(BinaryOperator::Add, one, param);
		assert_eq!(refused(&b, sum), "ir_lower::fold: expr.free_param");
	}

	/// A row with no field and no value.
	struct NoRow;

	impl RowReader for NoRow {
		fn field(&self, _name: &FieldName) -> anyhow::Result<Scalar<'_>> {
			Ok(Scalar::None)
		}

		fn record(&self) -> anyhow::Result<Value> {
			Ok(Value::None)
		}
	}

	#[test]
	fn fold_program_runs_on_the_row_interpreter() {
		let mut b = Builder::new();
		let sum = one_plus_two_times_three(&mut b);
		let (program, result) = lowered(&b, sum);
		let registry = FunctionRegistry::with_builtins();
		let mut state = State::new(&program, Vec::new(), &registry).expect("a state for the run");
		let pipeline = &program.pipelines[0];
		let row = NoRow;
		let mut regs: Vec<Scalar<'_>> = (0..pipeline.registers).map(|_| Scalar::None).collect();
		let step =
			run_row(&program, pipeline, &row, &mut regs, &mut state, 0).expect("the row runs");
		assert!(matches!(step, Step::Next));
		assert_eq!(regs[usize::from(result)].to_value(), Value::from(7_i64));
	}

	#[test]
	fn a_record_id_whose_key_is_computed_folds_to_the_record_id() {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let two = b.literal(Value::from(2_i64));
		let id = b.add(
			ExprOp::RecordId(Box::new(RecordIdAttrs {
				table: "t".into(),
				key_shape: KeyShape::Array {
					len: 2,
				},
			})),
			&[one, two],
		);
		let (program, result) = lowered(&b, id);
		let registry = FunctionRegistry::with_builtins();
		let mut state = State::new(&program, Vec::new(), &registry).expect("a state for the run");
		let pipeline = &program.pipelines[0];
		let row = NoRow;
		let mut regs: Vec<Scalar<'_>> = (0..pipeline.registers).map(|_| Scalar::None).collect();
		let step =
			run_row(&program, pipeline, &row, &mut regs, &mut state, 0).expect("the row runs");
		assert!(matches!(step, Step::Next));
		let key =
			RecordIdKey::Array(crate::val::Array(vec![Value::from(1_i64), Value::from(2_i64)]));
		assert_eq!(
			regs[usize::from(result)].to_value(),
			Value::RecordId(RecordId::new("t".into(), key))
		);
	}

	/// A value whose lowering fails halfway leaves no trace: the values around it answer what
	/// they answer alone, and it answers the refusal it gets alone.
	#[test]
	fn lower_batch_contains_a_failing_value() {
		use crate::exec::ir::fold::evaluate::{CapabilityPolicy, FoldRun};
		use crate::exec::ir::iface::fold::NotFoldable;
		use crate::exec::vm::fold_host::HOST_RUNNER;

		let mut b = Builder::new();
		let first = one_plus_two_times_three(&mut b);
		let param = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let one = b.literal(Value::from(1_i64));
		let bad = b.binary(BinaryOperator::Add, one, param);
		let two = b.literal(Value::from(2_i64));
		let three = b.literal(Value::from(3_i64));
		let last = b.binary(BinaryOperator::LessThan, two, three);
		let registry = crate::exec::ir::analysis::cx::builtin_registry();
		let batch = lower_batch_parts(&b.ir, registry, &[first, bad, last]).expect("assembles");
		assert!(matches!(batch.slots[0], Ok((0, _))));
		assert!(matches!(batch.slots[1], Err(IrError::Invariant { .. })));
		assert!(matches!(batch.slots[2], Ok((1, _))));
		assert_eq!(batch.program.pipelines.len(), 2);

		let policy = CapabilityPolicy::AllowAll;
		let runs = HOST_RUNNER.run_batch(&b.ir, registry, &[first, bad, last], &policy);
		let alone: Vec<FoldRun> = [first, bad, last]
			.iter()
			.map(|v| HOST_RUNNER.run_batch(&b.ir, registry, &[*v], &policy).remove(0))
			.collect();
		assert_eq!(format!("{runs:?}"), format!("{alone:?}"));
		assert!(matches!(&runs[0], FoldRun::Value(v, _) if *v == Value::from(7_i64)), "{runs:?}");
		assert!(matches!(&runs[1], FoldRun::Refused(NotFoldable::HostRefused("lowering"))));
		assert!(matches!(&runs[2], FoldRun::Value(v, _) if *v == Value::Bool(true)), "{runs:?}");
	}
}
