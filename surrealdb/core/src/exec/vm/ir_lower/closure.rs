//! Lowering of the loops that stand in value position, and of the invocation of a closure value.
//!
//! # Loops in value position
//!
//! A `phys.loop` whose result an expression reads is lowered where the expression reads it, into
//! the pipeline that holds the expression. Its first operand is the collection and its one body
//! region has the block arguments `(element, position)`. The loop's kind fixes its form:
//!
//! - `ElementFilter` (`array[WHERE ..]`) copies the elements of an array or a set, walks them in
//!   order and keeps those for which the body yields a true value, into a collection of the same
//!   kind. A value that is neither is the result `NONE`.
//! - `Broadcast` and `GraphStep` are in `graph.rs`. A broadcast may state [`StepAttrs::Flatten`],
//!   and a graph step states its lookup.
//! - `Recurse` is in `path.rs`.
//! - `Closure` is the call of the closure builtin the loop states ([`StepAttrs::Closure`]): the
//!   function or the method as it was written, over the collection, the initial accumulator of a
//!   `fold` and the closure the body invokes. The body is not walked: it states how the builtin
//!   applies the closure, and the builtin does.
//!
//! An element filter is always the instruction loop, never an unrolled one: the loop's `unroll` is
//! one, and a loop that carries another number is an invariant error. A loop that has a second
//! region for its first iteration is an invariant error too, because only a `FOR` has one, and so
//! is a loop that states a step its kind does not take. The loops of another kind answer `NotYet`
//! naming their kind, and so does a loop in the plan-time folder's subset, which has no element to
//! walk.
//!
//! A value the body of an element filter reads that is defined outside it is read from the
//! register an earlier read of the enclosing scope lowered it into, so the body evaluates nothing
//! the loop's other iterations would evaluate again ([`Lowerer::region_value_with`]).
//!
//! The physical phase wraps each such loop in a `phys.pipeline` that holds the loop and the yield
//! of its value. An expression that reads the pipeline reads the loop, which is lowered in place
//! and serially whatever the pipeline's placement states ([`wrapped_value_loop`]).
//!
//! # Making a closure value
//!
//! `expr.closure_create` is an [`Instr::MakeClosure`]: the value is the closure the literal
//! states, carrying what its body captures and no code. Its operands are the captured values, one
//! per name of its attributes in order, each read from the register that holds it. The value reads
//! no row: its body runs over the row of the frame that invokes it.
//!
//! The body runs as a unit of the program: the op asks for it as
//! [`UnitRequest::ClosureBody`], keyed by the digest of the closure as written, and the value's
//! [`ClosureValueSpec`] names the unit's slot. The names the value reads from the frame it is made
//! in are the free parameters its body reads, which the host captures when they are bound there.
//! The value's access mode is that of the lowered body unit, which the compile driver writes into
//! the spec, and the program's access mode is at least that of every closure it makes. Neither op
//! is in the plan-time folder's subset.
//!
//! # Invoking a closure value
//!
//! `expr.dispatch` of an invoke is the check that its callee is a closure, raised before the
//! arguments are evaluated, and its value is the callee. `expr.invoke` evaluates its arguments in
//! order, then invokes the callee with them in the authority frame the program runs in. Lowering
//! does not read what the closure does, so an invocation keeps rows serial. Neither is in the
//! plan-time folder's subset.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use super::super::program::{ClosureDigest, ClosureValueSpec, Instr, Operand};
use super::expr::{Lowerer, exactly, invariant, not_yet, region_at};
use super::units::UnitRequest;
use crate::exec::AccessMode;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ClosureAttrs, DispatchTarget, ExprOp};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{ClosureLoopAttrs, LoopKind, StepAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::val::Value;

impl Lowerer<'_> {
	/// The value the loop `data` yields, lowered into the pipeline in flight.
	///
	/// The loops that stand in value position are `ElementFilter`, `Broadcast` and `Closure`. A
	/// loop of any other kind is a statement or a source, which another arm lowers. A select or
	/// write loop is never read as a value: the binder binds a read or a write written in value
	/// position as the one statement of an `expr.subquery`, whose region runs as a unit, and the
	/// statement lowerings read the loop of a statement region themselves.
	pub(super) fn value_loop(&mut self, data: &OpData) -> Result<Operand, IrError> {
		let OpKind::Phys(PhysOp::Loop {
			kind,
			first,
			unroll,
			step,
			..
		}) = &data.kind
		else {
			return Err(IrError::Stale("ir_lower::closure: a loop that is not a loop"));
		};
		match kind {
			LoopKind::ElementFilter
			| LoopKind::Broadcast
			| LoopKind::Closure
			| LoopKind::Recurse
			| LoopKind::GraphStep => {}
			LoopKind::Select => return Err(invariant("a select loop read as a value")),
			LoopKind::Write => return Err(invariant("a write loop read as a value")),
			LoopKind::For => return not_yet("ir_lower::expr: phys.loop of kind for"),
		}
		self.refuse_in_fold("ir_lower::fold: phys.loop")?;
		self.require_frame(data)?;
		if *first {
			return Err(invariant("a value loop has no second region for its first iteration"));
		}
		if *unroll != 1 {
			return Err(invariant("a value loop is not unrolled"));
		}
		match (kind, step.as_deref()) {
			(LoopKind::ElementFilter, None) => self.element_filter(data),
			(LoopKind::Broadcast, None) => self.broadcast(data, false),
			(LoopKind::Broadcast, Some(StepAttrs::Flatten)) => self.broadcast(data, true),
			(LoopKind::Closure, Some(StepAttrs::Closure(attrs))) => self.closure_loop(data, attrs),
			(LoopKind::Recurse, Some(StepAttrs::Recurse(attrs))) => self.recurse_loop(data, attrs),
			(LoopKind::GraphStep, Some(StepAttrs::Lookup(attrs))) => self.graph_step(data, attrs),
			_ => Err(invariant("a value loop states a step its kind does not take")),
		}
	}

	/// The builtin a loop of kind `Closure` stands for, called on the collection and the closure
	/// the loop's body applies.
	///
	/// The loop is the call as the written site makes it: a function is the capability check, the
	/// arguments in order, then the call; a method is the resolution on the receiver, then the
	/// call with the arguments evaluated for it. The collection is the loop's first operand and a
	/// `fold`'s initial accumulator its second; the closure is the callee the body invokes, a
	/// value defined before the loop, which is read here and not in the body. What the builtin
	/// does with the closure's answers is the builtin's, so a failure, a denied capability and a
	/// refused argument are the call's.
	fn closure_loop(
		&mut self,
		data: &OpData,
		attrs: &ClosureLoopAttrs,
	) -> Result<Operand, IrError> {
		let body = region_at(data, 0, 1)?;
		let callee = callee_of(self.ir, body)?;
		let (source, seed) = match data.operands.as_slice() {
			[source] => (*source, None),
			[source, seed] => (*source, Some(*seed)),
			_ => return Err(IrError::Stale("ir_lower::closure: operand arity")),
		};
		let mut args: Vec<ValueId> = seed.into_iter().collect();
		args.push(callee);
		if attrs.method {
			let recv = self.value(source)?;
			let resolved = self.resolve_method(attrs.function.as_str(), recv)?;
			return self.call_resolved(resolved, &args);
		}
		args.insert(0, source);
		self.builtin_call(attrs.function.as_str(), &args)
	}

	/// `collection[WHERE predicate]`: the elements of an array or a set, in order, for which the
	/// body yields a true value, collected into an empty collection of the same kind; `NONE` for
	/// any other value.
	///
	/// The body reads the element as its first block argument and the element's position as its
	/// second, and yields the truth of the predicate.
	fn element_filter(&mut self, data: &OpData) -> Result<Operand, IrError> {
		let [source] = exactly::<1>(data)?;
		let body = region_at(data, 0, 1)?;
		let src = self.value(source)?;
		let items = self.em.reg()?;
		let dst = self.em.reg()?;
		let end = self.em.label()?;
		self.emit(Instr::FilterBegin {
			src,
			items,
			dst,
			none: end,
		});
		let pos = self.em.reg()?;
		let zero = self.em.constant(Value::from(0i64))?;
		self.emit(Instr::Load {
			dst: pos,
			src: Operand::Const(zero),
		});
		let element = self.em.reg()?;
		let index = self.em.reg()?;
		let top = self.em.label()?;
		self.em.bind(top)?;
		self.emit(Instr::ElemNext {
			array: items,
			pos,
			elem: element,
			index,
			done: end,
		});
		let admitted = self.element_value(body, element, &[Operand::Reg(index)])?;
		self.emit(Instr::JumpIf {
			cond: admitted,
			truthy: false,
			target: top,
		});
		self.emit(Instr::CollectPush {
			collection: dst,
			value: Operand::Reg(element),
		});
		self.emit(Instr::Jump {
			target: top,
		});
		self.em.bind(end)?;
		Ok(Operand::Reg(dst))
	}

	/// The closure value the literal `attrs` makes, where the op `data` stands.
	pub(super) fn closure_create(
		&mut self,
		data: &OpData,
		attrs: &ClosureAttrs,
	) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.closure_create")?;
		self.require_frame(data)?;
		let body = region_at(data, 0, 1)?;
		let Some(literal) = attrs.literal.0.as_ref() else {
			return Err(IrError::Stale(
				"ir_lower::closure: a closure parsed from text carries no literal",
			));
		};
		if data.operands.len() != attrs.captures.len() {
			return Err(IrError::Stale("ir_lower::closure: operand arity"));
		}
		let slot_operands =
			data.operands.iter().map(|value| self.value(*value)).collect::<Result<Vec<_>, _>>()?;
		let slots: Vec<String> =
			attrs.captures.iter().map(|name| name.as_str().to_owned()).collect();
		let framed = free_names(self.ir, body, &slots);
		let digest = ClosureDigest::of(literal);
		let unit = self.unit_for(UnitRequest::ClosureBody {
			digest,
			captures: slots.clone(),
		})?;
		let index = self.em.closure_value(ClosureValueSpec {
			closure: Arc::clone(literal),
			digest,
			body: Some(unit),
			slots,
			framed,
			frame: self.frame.program,
			// The compile driver writes the access mode of the lowered body unit here.
			access_mode: AccessMode::ReadOnly,
			reads_row: attrs.reads_row,
		})?;
		let dst = self.em.reg()?;
		self.emit(Instr::MakeClosure {
			closure: index,
			slots: slot_operands,
			dst,
		});
		Ok(Operand::Reg(dst))
	}

	/// The check that the callee of an invocation is a closure, which runs before the arguments
	/// are evaluated. The value is the callee, which `expr.invoke` reads.
	pub(super) fn dispatch_invoke(&mut self, data: &OpData) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.invoke")?;
		self.require_frame(data)?;
		let [callee] = exactly::<1>(data)?;
		let src = self.value(callee)?;
		self.emit(Instr::RequireClosure {
			src,
		});
		Ok(src)
	}

	/// The closure the dispatch resolved, invoked with the arguments the op names after it, in
	/// order. An invocation may fail, and what the closure does is not read, so rows stay serial.
	pub(super) fn invoke(&mut self, data: &OpData) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.invoke")?;
		self.require_frame(data)?;
		let Some((&dispatched, args)) = data.operands.split_first() else {
			return Err(IrError::Stale("ir_lower::closure: operand arity"));
		};
		let target = self.value(dispatched)?;
		let args = args.iter().map(|arg| self.value(*arg)).collect::<Result<Vec<_>, _>>()?;
		let dst = self.em.reg()?;
		self.emit(Instr::Invoke {
			target,
			args,
			dst,
			frame: self.frame.program,
			elem: self.element(),
		});
		self.keeps_row_order = true;
		// A callee that no closure literal makes may write, so the invocation counts as a write.
		if !is_literal_callee(self.ir, dispatched) {
			self.writes();
		}
		Ok(Operand::Reg(dst))
	}
}

/// Whether the dispatch result `dispatched` checks a closure that an `expr.closure_create` makes,
/// so that the body the invocation runs is one the program holds.
fn is_literal_callee(ir: &Ir, dispatched: ValueId) -> bool {
	let defined_by = |value: ValueId| match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => ir.op(op),
		ValueDef::BlockArg {
			..
		} => None,
	};
	let Some(dispatch) = defined_by(dispatched) else {
		return false;
	};
	dispatch
		.operands
		.first()
		.and_then(|callee| defined_by(*callee))
		.is_some_and(|callee| matches!(callee.kind, OpKind::Expr(ExprOp::ClosureCreate(_))))
}

/// The closure value the body `body` of a closure loop applies: the operand of the dispatch that
/// checks the callee of its invocation.
fn callee_of(ir: &Ir, body: RegionId) -> Result<ValueId, IrError> {
	ops_in(ir, body)
		.find_map(|op| match &ir.op(op)?.kind {
			OpKind::Expr(ExprOp::Dispatch(target))
				if matches!(**target, DispatchTarget::Invoke) =>
			{
				ir.op(op)?.operands.first().copied()
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
		})
		.ok_or(IrError::Stale("ir_lower::closure: a closure loop that invokes no callee"))
}

/// The names of the free parameters the ops of the region `body` read, once each in the order they
/// are met, except those in `captured`: the names a closure value reads from the frame it is made
/// in.
fn free_names(ir: &Ir, body: RegionId, captured: &[String]) -> Vec<String> {
	let mut names: Vec<String> = Vec::new();
	ir.walk(body, WalkOrder::PreOrder, &mut |op| {
		if let Some(OpData {
			kind: OpKind::Expr(ExprOp::FreeParam {
				name,
			}),
			..
		}) = ir.op(op)
		{
			let name = name.as_str();
			if !captured.iter().any(|held| held == name) && !names.iter().any(|held| held == name) {
				names.push(name.to_owned());
			}
		}
		Walk::Continue
	});
	names
}

/// The value loop the `phys.pipeline` `data` wraps, where its one region holds exactly that loop
/// and the `phys.yield` of the loop's result; `None` for a pipeline of any other shape, and for one
/// that wraps a loop of a kind that does not stand in value position (a select, a write or a
/// `FOR`).
///
/// The physical phase wraps every loop that may fan out, value loops included, so an expression
/// reads a value loop through its pipeline. The loop is lowered in place, serially, whatever the
/// pipeline's placement states: the iterations, their order and the value are those of the loop
/// alone, and no instruction reads the placement of a value loop.
pub(super) fn wrapped_value_loop(ir: &Ir, data: &OpData) -> Result<Option<OpId>, IrError> {
	let Some((looped, loop_data)) = wrapped_loop(ir, data)? else {
		return Ok(None);
	};
	let value_kind = match &loop_data.kind {
		OpKind::Phys(PhysOp::Loop {
			kind,
			..
		}) => match kind {
			LoopKind::ElementFilter
			| LoopKind::Broadcast
			| LoopKind::Closure
			| LoopKind::Recurse
			| LoopKind::GraphStep => true,
			LoopKind::Select | LoopKind::Write | LoopKind::For => false,
		},
		OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => false,
	};
	Ok(value_kind.then_some(looped))
}

/// Whether the `phys.pipeline` `data` wraps a `FOR` written in value position: its one region
/// holds exactly that loop and the `phys.yield` of the loop's result.
pub(super) fn wraps_value_for(ir: &Ir, data: &OpData) -> Result<bool, IrError> {
	let Some((_, loop_data)) = wrapped_loop(ir, data)? else {
		return Ok(false);
	};
	Ok(matches!(
		loop_data.kind,
		OpKind::Phys(PhysOp::Loop {
			kind: LoopKind::For,
			value: true,
			..
		})
	))
}

/// The loop the `phys.pipeline` `data` wraps, of any kind, where its one region holds exactly an
/// op of one result and the `phys.yield` of that result.
fn wrapped_loop<'ir>(ir: &'ir Ir, data: &OpData) -> Result<Option<(OpId, &'ir OpData)>, IrError> {
	let [region] = data.regions.as_slice() else {
		return Ok(None);
	};
	let mut inner = ops_in(ir, *region);
	let Some(looped) = inner.next() else {
		return Ok(None);
	};
	let Some(ended) = inner.next() else {
		return Ok(None);
	};
	if inner.next().is_some() {
		return Ok(None);
	}
	let loop_data = ir.op(looped).ok_or(IrError::Stale("ir_lower::closure: op"))?;
	let ended = ir.op(ended).ok_or(IrError::Stale("ir_lower::closure: op"))?;
	let yields_loop = matches!(ended.kind, OpKind::Phys(PhysOp::Yield))
		&& ended.operands.as_slice() == loop_data.results.as_slice()
		&& loop_data.results.len() == 1;
	Ok(yields_loop.then_some((looped, loop_data)))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::AccessMode;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::Ir;
	use crate::exec::ir::expr::ops::{DispatchTarget, ExprOp};
	use crate::exec::ir::iface::ids::{OpId, ValueId};
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::phys::ops::Batch;
	use crate::exec::vm::ir_lower::emit::Emitter;
	use crate::exec::vm::ir_lower::expr::Needs;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::{ExternalCallee, Program, Source};
	use crate::expr::BinaryOperator;

	/// A loop op of `kind`, with a second region for its first iteration when `first` is set.
	fn loop_with(kind: LoopKind, first: bool, unroll: u8) -> PhysOp {
		PhysOp::Loop {
			kind,
			first,
			batch: Batch {
				max_iterations: 1,
				max_bytes: 1,
			},
			unroll,
			value: false,
			step: None,
		}
	}

	/// The loop op of `kind` as the binder builds one for a value: one body region, not unrolled.
	fn loop_of(kind: LoopKind) -> PhysOp {
		loop_with(kind, false, 1)
	}

	/// A read of the free parameter `name`.
	fn param(b: &mut Builder, name: &str) -> ValueId {
		b.add(
			ExprOp::FreeParam {
				name: name.into(),
			},
			&[],
		)
	}

	/// Lowers `value` of `ir` into one scalar pipeline that ends the row, and answers the program
	/// and what the lowering needs.
	fn lower_value(ir: &Ir, value: ValueId) -> Result<(Operand, Program, Needs), IrError> {
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, ir, crate::exec::ir::analysis::cx::builtin_registry());
		let operand = lowerer.value(value)?;
		let needs = lowerer.needs();
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly)?;
		Ok((operand, program, needs))
	}

	fn code_of(program: &Program) -> &[Instr] {
		&program.pipelines[0].code
	}

	/// `xs[WHERE a = 1]` as a loop whose body reads the field `a` of the element: the loop op and
	/// its result.
	fn element_filter_of_a_field(b: &mut Builder) -> (OpId, ValueId) {
		let xs = param(b, "xs");
		let looped = b.add_phys_value(loop_of(LoopKind::ElementFilter), &[xs]);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				let field = b.add(
					ExprOp::Field {
						name: "a".into(),
					},
					&[args[0]],
				);
				let one = b.literal(crate::val::Value::from(1_i64));
				let same = b.binary(BinaryOperator::Equal, field, one);
				let truth = b.add(ExprOp::Truthy, &[same]);
				b.yield_phys(truth);
			},
		);
		(looped, b.result_of(looped))
	}

	/// The index of the first instruction of `code` that `pick` answers `true` for.
	fn position(code: &[Instr], pick: impl Fn(&Instr) -> bool) -> usize {
		code.iter().position(pick).expect("the instruction is emitted")
	}

	#[test]
	fn an_element_filter_walks_the_elements_and_collects_the_admitted() {
		let mut b = Builder::new();
		let (_, result) = element_filter_of_a_field(&mut b);
		let (operand, program, _) = lower_value(&b.ir, result).expect("the loop lowers");
		let code = code_of(&program);
		let begin = position(code, |i| matches!(i, Instr::FilterBegin { .. }));
		let next = position(code, |i| matches!(i, Instr::ElemNext { .. }));
		let push = position(code, |i| matches!(i, Instr::CollectPush { .. }));
		assert!(begin < next && next < push);
		let Instr::FilterBegin {
			items,
			dst,
			none,
			..
		} = code[begin]
		else {
			unreachable!()
		};
		let Instr::ElemNext {
			array,
			elem,
			done,
			..
		} = code[next]
		else {
			unreachable!()
		};
		// The collection starts empty in `dst`, the elements are walked from `items`, and a value
		// that is no collection and the end of the elements both continue past the loop.
		assert_eq!(operand, Operand::Reg(dst));
		assert_eq!(array, items);
		assert_eq!(none, done);
		assert_eq!(none as usize, code.len() - 1, "the loop ends the pipeline's code but its row");
		assert_eq!(
			code[push],
			Instr::CollectPush {
				collection: dst,
				value: Operand::Reg(elem),
			}
		);
		// The body is read from the element: the field `a` is a path step over it, and a body that
		// rejects the element goes back to the next one.
		assert!(
			code[next + 1..push].iter().any(
				|i| matches!(i, Instr::Access { src: Operand::Reg(read), .. } if *read == elem)
			)
		);
		let jump = code.len() - 2;
		assert_eq!(
			code[jump],
			Instr::Jump {
				target: next as _,
			}
		);
	}

	#[test]
	fn an_element_filter_is_the_loop_form_whatever_the_body() {
		// A body that reads nothing of the element still loops: the form is never a decision of
		// the lowering.
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let looped = b.add_phys_value(loop_of(LoopKind::ElementFilter), &[xs]);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, _| {
				let yes = b.literal(crate::val::Value::Bool(true));
				let truth = b.add(ExprOp::Truthy, &[yes]);
				b.yield_phys(truth);
			},
		);
		let result = b.result_of(looped);
		let (_, program, _) = lower_value(&b.ir, result).expect("the loop lowers");
		assert!(code_of(&program).iter().any(|i| matches!(i, Instr::ElemNext { .. })));
	}

	#[test]
	fn a_value_loop_is_refused_by_kind_in_the_subset_it_does_not_belong_to() {
		for (kind, what) in [
			(LoopKind::Select, "a select loop read as a value"),
			(LoopKind::Write, "a write loop read as a value"),
		] {
			let mut b = Builder::new();
			let xs = param(&mut b, "xs");
			let looped = b.add_phys_value(loop_of(kind), &[xs]);
			let result = b.result_of(looped);
			assert!(
				matches!(lower_value(&b.ir, result), Err(IrError::Invariant { what: got }) if got == what),
				"{what}"
			);
		}
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let looped = b.add_phys_value(loop_of(LoopKind::For), &[xs]);
		let result = b.result_of(looped);
		assert!(matches!(
			lower_value(&b.ir, result),
			Err(IrError::NotYet {
				what: "ir_lower::expr: phys.loop of kind for"
			})
		));
	}

	#[test]
	fn a_value_loop_with_a_second_region_or_an_unroll_is_an_invariant_error() {
		for op in [
			loop_with(LoopKind::ElementFilter, true, 1),
			loop_with(LoopKind::ElementFilter, false, 2),
		] {
			let mut b = Builder::new();
			let xs = param(&mut b, "xs");
			let looped = b.add_phys_value(op, &[xs]);
			let result = b.result_of(looped);
			assert!(matches!(lower_value(&b.ir, result), Err(IrError::Invariant { .. })));
		}
	}

	#[test]
	fn a_loop_is_not_in_the_folders_subset() {
		let mut b = Builder::new();
		let (_, result) = element_filter_of_a_field(&mut b);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.value(result),
			Err(IrError::Invariant {
				what: "ir_lower::fold: phys.loop"
			})
		));
	}

	#[test]
	fn a_loop_through_the_direct_entry_passes_the_claim_check_of_its_ops() {
		// The field of a non-object row emits a path step, which the op's claim admits.
		let mut b = Builder::new();
		let (_, result) = element_filter_of_a_field(&mut b);
		b.add_phys(PhysOp::Eval, &[result]);
		b.physical();
		let lowered =
			crate::exec::vm::ir_lower::lower(&b.ir, &FactCache::new()).expect("the loop lowers");
		assert!(
			lowered.program.pipelines[0].code.iter().any(|i| matches!(i, Instr::Access { .. }))
		);
	}

	#[test]
	fn a_loop_that_defines_no_value_is_refused_not_skipped() {
		for kind in [LoopKind::For, LoopKind::Select, LoopKind::ElementFilter] {
			let mut b = Builder::new();
			let xs = param(&mut b, "xs");
			b.add_phys(loop_of(kind), &[xs]);
			let one = b.literal(crate::val::Value::from(1_i64));
			b.add_phys(PhysOp::Eval, &[one]);
			b.physical();
			// A loop with no body region defines no value. The statement list refuses it as a
			// stale form where it checks the loop's regions, and as a loop it has no lowering for
			// otherwise.
			let lowered = crate::exec::vm::ir_lower::lower(&b.ir, &FactCache::new());
			assert!(
				matches!(
					lowered,
					Err(IrError::Stale("ir_lower::block: region arity")
						| IrError::NotYet {
							what: "ir_lower::lower: phys.loop"
						})
				),
				"{kind:?}: {:?}",
				lowered.err()
			);
		}
	}

	#[test]
	fn a_loop_of_a_statement_kind_at_the_top_is_refused_by_the_direct_entry() {
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let looped = b.add_phys_value(loop_of(LoopKind::For), &[xs]);
		let result = b.result_of(looped);
		b.add_phys(PhysOp::Eval, &[result]);
		b.physical();
		// The loop holds no body region, a stale form the statement list refuses.
		assert!(matches!(
			crate::exec::vm::ir_lower::lower(&b.ir, &FactCache::new()),
			Err(IrError::Stale("ir_lower::block: region arity"))
		));
	}

	/// Wraps the loops of `b` that fan out in pipelines, as the physical phase does, and answers
	/// how many it wrapped.
	fn wrap_loops(b: &mut Builder) -> usize {
		b.physical();
		let top = b.ir.top();
		crate::exec::ir::phys::pipeline::build(&mut b.ir, &mut b.facts, top)
			.expect("the loops are wrapped")
	}

	/// The result of the one `phys.pipeline` of the top region of `ir`.
	fn pipeline_result(ir: &Ir) -> ValueId {
		ops_in(ir, ir.top())
			.find_map(|op| {
				let data = ir.op(op)?;
				matches!(data.kind, OpKind::Phys(PhysOp::Pipeline(_)))
					.then(|| data.results.first().copied())
					.flatten()
			})
			.expect("the top region holds a pipeline")
	}

	#[test]
	fn a_value_loop_the_physical_phase_wraps_lowers_as_the_bare_loop() {
		let mut bare = Builder::new();
		let (_, result) = element_filter_of_a_field(&mut bare);
		let (_, unwrapped, _) = lower_value(&bare.ir, result).expect("the bare loop lowers");

		let mut b = Builder::new();
		let (_, result) = element_filter_of_a_field(&mut b);
		b.add_phys(PhysOp::Eval, &[result]);
		assert_eq!(wrap_loops(&mut b), 1);
		let wrapped = pipeline_result(&b.ir);
		let (_, program, _) = lower_value(&b.ir, wrapped).expect("the wrapped loop lowers");
		assert_eq!(code_of(&program), code_of(&unwrapped));
	}

	#[test]
	fn a_value_loop_wrapped_in_the_body_of_another_reads_its_collection_from_that_body() {
		// `xs[WHERE ys[WHERE true]]`: the inner loop's collection is defined in the outer body,
		// beside the inner pipeline and outside the region the pipeline wraps the loop in.
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let outer = b.add_phys_value(loop_of(LoopKind::ElementFilter), &[xs]);
		b.open_with(
			outer,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, _| {
				let ys = param(b, "ys");
				let inner = b.add_phys_value(loop_of(LoopKind::ElementFilter), &[ys]);
				b.open_with(
					inner,
					RegionKind::Loop,
					&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
					|b, _| {
						let yes = b.literal(crate::val::Value::Bool(true));
						let truth = b.add(ExprOp::Truthy, &[yes]);
						b.yield_phys(truth);
					},
				);
				let kept = b.result_of(inner);
				let truth = b.add(ExprOp::Truthy, &[kept]);
				b.yield_phys(truth);
			},
		);
		let result = b.result_of(outer);
		b.add_phys(PhysOp::Eval, &[result]);
		assert_eq!(wrap_loops(&mut b), 2);
		let wrapped = pipeline_result(&b.ir);
		let (_, program, _) = lower_value(&b.ir, wrapped).expect("both loops lower");
		let begins =
			code_of(&program).iter().filter(|i| matches!(i, Instr::FilterBegin { .. })).count();
		assert_eq!(begins, 2);
	}

	#[test]
	fn a_wrapped_loop_of_a_statement_kind_in_value_position_is_still_refused() {
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let looped = b.add_phys_value(loop_of(LoopKind::For), &[xs]);
		let result = b.result_of(looped);
		b.add_phys(PhysOp::Eval, &[result]);
		assert_eq!(wrap_loops(&mut b), 1);
		let wrapped = pipeline_result(&b.ir);
		assert!(matches!(
			lower_value(&b.ir, wrapped),
			Err(IrError::NotYet {
				what: "ir_lower::expr: an op outside the expr dialect"
			})
		));
	}

	#[test]
	fn a_wrapped_for_in_value_position_runs_as_the_unit_of_its_pipeline() {
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let for_loop = PhysOp::Loop {
			kind: LoopKind::For,
			first: false,
			batch: Batch {
				max_iterations: 1,
				max_bytes: 1,
			},
			unroll: 1,
			value: true,
			step: None,
		};
		let looped = b.add_phys_value(for_loop, &[xs]);
		b.open_with(
			looped,
			RegionKind::ValueFor,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, _| {
				b.add_phys(PhysOp::Yield, &[]);
			},
		);
		let result = b.result_of(looped);
		b.add_phys(PhysOp::Eval, &[result]);
		assert_eq!(wrap_loops(&mut b), 1);
		let wrapped = pipeline_result(&b.ir);
		let crate::exec::ir::core::value::ValueDef::OpResult {
			op: pipeline,
			..
		} = b.ir.value(wrapped).expect("the pipeline's value").def
		else {
			panic!("the pipeline defines its value");
		};
		let region = b.ir.op(pipeline).expect("the pipeline").regions[0];

		let mut cx = crate::exec::vm::ir_lower::units::LowerCx::detached();
		let mut em = Emitter::new();
		let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
		lowerer.value(wrapped).expect("the FOR lowers");
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		assert_eq!(
			pipeline.code.iter().filter(|i| matches!(i, Instr::Subquery { .. })).count(),
			1,
			"the FOR runs as one unit"
		);
		let needed = cx.take_needed();
		assert!(
			matches!(
				needed.as_slice(),
				[crate::exec::vm::ir_lower::units::Needed {
					request: crate::exec::vm::ir_lower::units::UnitRequest::Subquery {
						region: asked,
					},
					..
				}] if *asked == region
			),
			"the unit is the pipeline's region: {needed:?}"
		);
	}

	/// `callee(args)`: the check that the callee is a closure, then the invocation. Answers the
	/// invocation's value.
	fn invoke_of(b: &mut Builder, callee: ValueId, args: &[ValueId]) -> ValueId {
		let dispatch = b.add(ExprOp::Dispatch(Box::new(DispatchTarget::Invoke)), &[callee]);
		let mut operands = vec![dispatch];
		operands.extend_from_slice(args);
		b.add(ExprOp::Invoke, &operands)
	}

	#[test]
	fn an_invocation_checks_its_callee_before_it_evaluates_the_arguments() {
		let mut b = Builder::new();
		let callee = param(&mut b, "f");
		let one = b.literal(crate::val::Value::from(1_i64));
		let two = b.literal(crate::val::Value::from(2_i64));
		let sum = b.binary(BinaryOperator::Add, one, two);
		let call = invoke_of(&mut b, callee, &[sum, one]);
		let (operand, program, needs) = lower_value(&b.ir, call).expect("the invocation lowers");
		let code = code_of(&program);
		let check = position(code, |i| matches!(i, Instr::RequireClosure { .. }));
		let arith = position(code, |i| matches!(i, Instr::Arith { .. }));
		let invoke = position(code, |i| matches!(i, Instr::Invoke { .. }));
		assert!(check < arith && arith < invoke);
		let Instr::RequireClosure {
			src,
		} = code[check]
		else {
			unreachable!()
		};
		let Instr::Invoke {
			target,
			args,
			dst,
			frame,
			elem,
		} = &code[invoke]
		else {
			unreachable!()
		};
		assert_eq!(*target, src);
		assert_eq!(args.len(), 2);
		assert_eq!(*frame, None);
		assert_eq!(*elem, None, "outside an element body the invocation runs over the row");
		assert_eq!(operand, Operand::Reg(*dst));
		assert!(needs.keeps_row_order);
	}

	#[test]
	fn an_invocation_is_not_in_the_folders_subset() {
		let mut b = Builder::new();
		let callee = b.literal(crate::val::Value::None);
		let call = invoke_of(&mut b, callee, &[]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.value(call),
			Err(IrError::Invariant {
				what: "ir_lower::fold: expr.invoke"
			})
		));
	}

	#[test]
	fn a_dispatch_two_invocations_read_checks_its_callee_once() {
		let mut b = Builder::new();
		let callee = param(&mut b, "f");
		let dispatch = b.add(ExprOp::Dispatch(Box::new(DispatchTarget::Invoke)), &[callee]);
		let first = b.add(ExprOp::Invoke, &[dispatch]);
		let second = b.add(ExprOp::Invoke, &[dispatch]);
		let sum = b.binary(BinaryOperator::Add, first, second);
		let (_, program, _) = lower_value(&b.ir, sum).expect("the invocations lower");
		let code = code_of(&program);
		assert_eq!(code.iter().filter(|i| matches!(i, Instr::RequireClosure { .. })).count(), 1);
		assert_eq!(code.iter().filter(|i| matches!(i, Instr::Invoke { .. })).count(), 2);
	}

	use crate::exec::ir::expr::ops::ClosureLiteral;
	use crate::exec::vm::ir_lower::units::{LowerCx, Needed};

	/// The attributes of the literal `|| NONE` that captures `captures`, with the written form.
	fn closure_attrs(captures: &[&str]) -> ClosureAttrs {
		ClosureAttrs {
			params: Box::new([]),
			returns: None,
			body_sql: Arc::from("NONE"),
			captures: captures.iter().map(surrealdb_strand::Strand::new).collect(),
			reads_row: false,
			reads_root: false,
			literal: ClosureLiteral::none_written_as("NONE"),
		}
	}

	/// A closure creation over `captures` and `row`, whose body is the value `body` builds from
	/// the body's block arguments, and its result.
	fn make_closure(
		b: &mut Builder,
		attrs: ClosureAttrs,
		operands: &[ValueId],
		body: impl FnOnce(&mut Builder, &[ValueId]) -> ValueId,
	) -> ValueId {
		let owner = b.add_with_regions(ExprOp::ClosureCreate(Box::new(attrs)), operands);
		let args = vec![TypeKnowledge::Unknown; 1 + operands.len()];
		b.region_with_args(owner, RegionKind::ClosureBody, &args, body);
		b.result_of(owner)
	}

	/// [`lower_value`] for a lowerer that asks `cx` for the units its ops run.
	fn lower_in(
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		value: ValueId,
	) -> Result<(Operand, Program, Vec<Needed>), IrError> {
		let mut em = Emitter::new();
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		let operand = lowerer.value(value)?;
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly)?;
		Ok((operand, program, cx.take_needed()))
	}

	#[test]
	fn a_closure_literal_is_one_make_closure_over_its_captures_and_asks_for_its_body() {
		let mut b = Builder::new();
		let captured = param(&mut b, "c");
		let made = make_closure(&mut b, closure_attrs(&["a"]), &[captured], |b, _| {
			// The body reads one name the literal does not capture, from the frame it runs in.
			param(b, "g")
		});
		let mut cx = LowerCx::detached();
		let (operand, program, needed) =
			lower_in(&mut cx, &b.ir, made).expect("the literal lowers");
		let code = code_of(&program);
		let make = position(code, |i| matches!(i, Instr::MakeClosure { .. }));
		let Instr::MakeClosure {
			closure,
			slots,
			dst,
		} = &code[make]
		else {
			unreachable!()
		};
		assert_eq!(operand, Operand::Reg(*dst));
		assert_eq!(slots.len(), 1);
		// The captured value is read where the literal is written, and the closure is made once.
		assert_eq!(code.iter().filter(|i| matches!(i, Instr::MakeClosure { .. })).count(), 1);
		let spec = &program.closure_values[*closure as usize];
		assert_eq!(spec.slots, ["a"]);
		assert_eq!(spec.framed, ["g"]);
		assert_eq!(spec.frame, None);
		assert_eq!(spec.access_mode, AccessMode::ReadOnly);
		assert!(!spec.reads_row);
		// The body is the unit the request names, keyed by the digest of the literal.
		assert_eq!(
			needed,
			vec![Needed {
				request: UnitRequest::ClosureBody {
					digest: spec.digest,
					captures: vec!["a".to_owned()],
				},
				id: spec.body,
			}]
		);
		assert_eq!(spec.body, Some(crate::exec::vm::program::UnitId(0)));
		assert_eq!(spec.digest, ClosureDigest::of(&spec.closure));
	}

	#[test]
	fn two_creations_of_one_literal_share_the_unit_of_its_body() {
		let mut b = Builder::new();
		let first = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "x"));
		let second = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "y"));
		let both = b.add(ExprOp::Array, &[first, second]);
		let mut cx = LowerCx::detached();
		let (_, program, needed) = lower_in(&mut cx, &b.ir, both).expect("the literals lower");
		assert_eq!(program.closure_values.len(), 2, "each site has its own entry");
		assert_eq!(program.closure_values[0].body, program.closure_values[1].body);
		assert_eq!(needed.len(), 1, "the body is asked for once");
	}

	#[test]
	fn a_closure_literal_is_refused_where_it_cannot_be_lowered() {
		let mut b = Builder::new();
		let made = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "g"));
		// Every lowerer of the operand subset holds a unit table: one without it is an
		// invariant break.
		assert!(matches!(lower_value(&b.ir, made), Err(IrError::Invariant { .. })));
		// The plan-time folder runs no closure.
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.value(made),
			Err(IrError::Invariant {
				what: "ir_lower::fold: expr.closure_create"
			})
		));
		// A creation read from text has no written form to carry.
		let mut b = Builder::new();
		let mut attrs = closure_attrs(&[]);
		attrs.literal = ClosureLiteral::default();
		let made = make_closure(&mut b, attrs, &[], |b, _| param(b, "g"));
		let mut cx = LowerCx::detached();
		assert!(matches!(lower_in(&mut cx, &b.ir, made), Err(IrError::Stale(_))));
		// An operand count its names do not explain is stale.
		let mut b = Builder::new();
		let extra = param(&mut b, "c");
		let made =
			make_closure(&mut b, closure_attrs(&["a"]), &[extra, extra, extra, extra], |b, _| {
				param(b, "g")
			});
		let mut cx = LowerCx::detached();
		assert!(matches!(lower_in(&mut cx, &b.ir, made), Err(IrError::Stale(_))));
	}

	#[test]
	fn a_program_that_makes_a_closure_takes_the_access_mode_of_the_closures_it_makes() {
		let mut em = Emitter::new();
		let closure = ClosureLiteral::none_written_as("NONE").0.expect("the literal is written");
		em.closure_value(ClosureValueSpec {
			digest: ClosureDigest::of(&closure),
			closure,
			body: None,
			slots: Vec::new(),
			framed: Vec::new(),
			frame: None,
			access_mode: AccessMode::ReadWrite,
			reads_row: false,
		})
		.expect("the spec is held");
		let program = em.finish_program(Vec::new(), AccessMode::ReadOnly).expect("a program");
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
	}

	/// A loop of kind `kind` that states `step`.
	fn loop_stating(kind: LoopKind, step: StepAttrs) -> PhysOp {
		PhysOp::Loop {
			kind,
			first: false,
			batch: Batch {
				max_iterations: 1,
				max_bytes: 1,
			},
			unroll: 1,
			value: false,
			step: Some(Box::new(step)),
		}
	}

	/// A loop of kind `Closure` that states the builtin `function`.
	fn closure_loop_op(function: &str, method: bool) -> PhysOp {
		loop_stating(
			LoopKind::Closure,
			StepAttrs::Closure(ClosureLoopAttrs {
				function: surrealdb_strand::Strand::new(function),
				method,
			}),
		)
	}

	/// A closure loop over `operands` whose body invokes `closure` on its element and position,
	/// and its result.
	fn closure_loop_of(
		b: &mut Builder,
		op: PhysOp,
		operands: &[ValueId],
		closure: ValueId,
	) -> ValueId {
		let looped = b.add_phys_value(op, operands);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				let target = b.add(ExprOp::Dispatch(Box::new(DispatchTarget::Invoke)), &[closure]);
				let mut invoked = vec![target];
				invoked.extend_from_slice(args);
				let answer = b.add(ExprOp::Invoke, &invoked);
				b.yield_phys(answer);
			},
		);
		b.result_of(looped)
	}

	#[test]
	fn a_closure_loop_over_a_function_is_the_call_of_the_builtin_with_the_closure_last() {
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let made = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "g"));
		let result = closure_loop_of(&mut b, closure_loop_op("array::some", false), &[xs], made);
		let mut cx = LowerCx::detached();
		let (operand, program, _) = lower_in(&mut cx, &b.ir, result).expect("the loop lowers");
		let code = code_of(&program);
		// Making the closure has no effect and comes first. The capability check comes before the
		// collection is read, and both come before the call.
		let allowed = position(code, |i| matches!(i, Instr::CallAllowed { .. }));
		let collection = position(code, |i| matches!(i, Instr::LoadParam { .. }));
		let make = position(code, |i| matches!(i, Instr::MakeClosure { .. }));
		let call = position(code, |i| matches!(i, Instr::Call { .. }));
		assert!(make < allowed && allowed < collection && collection < call);
		let Instr::Call {
			call: spec,
			args,
			dst,
			..
		} = &code[call]
		else {
			unreachable!()
		};
		assert_eq!(operand, Operand::Reg(*dst));
		assert_eq!(program.calls[*spec as usize].name, "array::some");
		assert_eq!(args.len(), 2);
		// The body is not walked: the builtin applies the closure.
		assert!(!code.iter().any(|i| matches!(i, Instr::ElemNext { .. } | Instr::Invoke { .. })));
		// What the builtin does with a closure keeps rows in order.
		assert!(program.calls[*spec as usize].closure_args.contains(&1));
	}

	#[test]
	fn a_fold_over_a_function_passes_its_initial_accumulator_between_the_collection_and_closure() {
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let init = b.literal(Value::from(0_i64));
		let made = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "g"));
		let result =
			closure_loop_of(&mut b, closure_loop_op("array::fold", false), &[xs, init], made);
		let mut cx = LowerCx::detached();
		let (_, program, _) = lower_in(&mut cx, &b.ir, result).expect("the loop lowers");
		let code = code_of(&program);
		let Instr::Call {
			args,
			..
		} = &code[position(code, |i| matches!(i, Instr::Call { .. }))]
		else {
			unreachable!()
		};
		assert_eq!(args.len(), 3);
		assert_eq!(args[1], Operand::Const(0));
	}

	#[test]
	fn a_closure_loop_over_a_method_resolves_it_on_the_receiver_before_the_arguments() {
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let made = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "g"));
		let result = closure_loop_of(&mut b, closure_loop_op("map", true), &[xs], made);
		let mut cx = LowerCx::detached();
		let (operand, program, _) = lower_in(&mut cx, &b.ir, result).expect("the loop lowers");
		let code = code_of(&program);
		// Making the closure has no effect and comes first. The method is resolved on the receiver,
		// which is read first, and only then called.
		let receiver = position(code, |i| matches!(i, Instr::LoadParam { .. }));
		let resolve = position(code, |i| matches!(i, Instr::MethodResolve { .. }));
		let make = position(code, |i| matches!(i, Instr::MakeClosure { .. }));
		let call = position(code, |i| matches!(i, Instr::Method { .. }));
		assert!(make < receiver && receiver < resolve && resolve < call);
		let Instr::Method {
			method,
			args,
			dst,
			..
		} = &code[call]
		else {
			unreachable!()
		};
		assert_eq!(operand, Operand::Reg(*dst));
		assert_eq!(args.len(), 1, "the receiver is not an argument");
		assert_eq!(program.methods[*method as usize].name, "map");
	}

	#[test]
	fn a_closure_loop_that_is_not_well_formed_is_an_invariant_or_stale_error() {
		// A closure loop states its builtin.
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let made = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "g"));
		let result = closure_loop_of(&mut b, loop_of(LoopKind::Closure), &[xs], made);
		let mut cx = LowerCx::detached();
		assert!(matches!(lower_in(&mut cx, &b.ir, result), Err(IrError::Invariant { .. })));
		// An element filter states none.
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let step = StepAttrs::Closure(ClosureLoopAttrs {
			function: surrealdb_strand::Strand::new("array::map"),
			method: false,
		});
		let looped = b.add_phys_value(loop_stating(LoopKind::ElementFilter, step), &[xs]);
		let result = b.result_of(looped);
		let mut cx = LowerCx::detached();
		assert!(matches!(lower_in(&mut cx, &b.ir, result), Err(IrError::Invariant { .. })));
		// A body that invokes nothing has no closure to pass.
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let looped = b.add_phys_value(closure_loop_op("array::map", false), &[xs]);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				b.yield_phys(args[0]);
			},
		);
		let result = b.result_of(looped);
		let mut cx = LowerCx::detached();
		assert!(matches!(lower_in(&mut cx, &b.ir, result), Err(IrError::Stale(_))));
	}

	#[test]
	fn a_closure_loop_through_the_direct_entry_lowers() {
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let made = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "g"));
		let result = closure_loop_of(&mut b, closure_loop_op("array::map", false), &[xs], made);
		b.add_phys(PhysOp::Eval, &[result]);
		b.physical();
		let mut cx = LowerCx::detached();
		let lowered =
			crate::exec::vm::ir_lower::dispatch::lower_with(&mut cx, &b.ir, &FactCache::new())
				.expect("the closure loop lowers");
		assert!(lowered.program.pipelines[0].code.iter().any(|i| matches!(i, Instr::Call { .. })));
		assert!(lowered.program.keeps_row_order);
		assert_eq!(lowered.needs.len(), 1, "the body of the closure is asked for");
	}

	#[test]
	fn an_invocation_may_write_unless_it_runs_a_literal_the_program_holds() {
		// A closure from a parameter has a body the program does not hold.
		let mut b = Builder::new();
		let callee = param(&mut b, "f");
		let call = invoke_of(&mut b, callee, &[]);
		let (_, _, needs) = lower_value(&b.ir, call).expect("the invocation lowers");
		assert_eq!(needs.access_mode, AccessMode::ReadWrite);

		// A literal made by the same container states what its body does with its value.
		let mut b = Builder::new();
		let made = make_closure(&mut b, closure_attrs(&[]), &[], |b, _| param(b, "g"));
		let call = invoke_of(&mut b, made, &[]);
		let mut em = Emitter::new();
		let mut cx = LowerCx::detached();
		let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
		lowerer.value(call).expect("the invocation lowers");
		assert_eq!(lowerer.needs().access_mode, AccessMode::ReadOnly);
	}

	/// A call of the embedded script `source` over `args`.
	fn script(b: &mut Builder, source: &str, args: &[ValueId]) -> ValueId {
		b.add(
			ExprOp::call(crate::expr::Function::Script(crate::expr::Script(source.to_owned()))),
			args,
		)
	}

	#[test]
	fn a_script_call_enters_before_its_arguments_and_writes() {
		let mut b = Builder::new();
		let x = param(&mut b, "x");
		let call = script(&mut b, "return arguments[0];", &[x]);
		let (operand, program, needs) = lower_value(&b.ir, call).expect("the call lowers");
		let code = code_of(&program);
		let enter = position(code, |i| matches!(i, Instr::ExternalEnter { .. }));
		let load = position(code, |i| matches!(i, Instr::LoadParam { .. }));
		let run = position(code, |i| matches!(i, Instr::ExternalCall { .. }));
		assert!(enter < load && load < run, "the session is checked before the arguments run");
		let Instr::ExternalCall {
			external,
			ref args,
			this,
			dst,
			..
		} = code[run]
		else {
			unreachable!()
		};
		// Outside an element body the host hands the call the row.
		assert_eq!(this, None);
		assert_eq!(args.len(), 1);
		assert_eq!(operand, Operand::Reg(dst));
		assert_eq!(
			program.externals[external as usize].callee,
			ExternalCallee::Script {
				source: "return arguments[0];".to_owned(),
			}
		);
		assert_eq!(needs.access_mode, AccessMode::ReadWrite);
	}

	#[test]
	fn a_script_call_in_an_element_body_takes_the_element_as_its_current_value() {
		let mut b = Builder::new();
		let xs = param(&mut b, "xs");
		let looped = b.add_phys_value(loop_of(LoopKind::ElementFilter), &[xs]);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, _| {
				let ran = script(b, "return this.a;", &[]);
				let truth = b.add(ExprOp::Truthy, &[ran]);
				b.yield_phys(truth);
			},
		);
		let result = b.result_of(looped);
		let (_, program, _) = lower_value(&b.ir, result).expect("the loop lowers");
		let code = code_of(&program);
		let Instr::ElemNext {
			elem,
			..
		} = code[position(code, |i| matches!(i, Instr::ElemNext { .. }))]
		else {
			unreachable!()
		};
		let Instr::ExternalCall {
			this,
			..
		} = code[position(code, |i| matches!(i, Instr::ExternalCall { .. }))]
		else {
			unreachable!()
		};
		assert_eq!(this, Some(Operand::Reg(elem)));
	}

	#[test]
	fn a_script_call_is_not_in_the_folders_subset() {
		let mut b = Builder::new();
		let call = script(&mut b, "return 1;", &[]);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.value(call),
			Err(IrError::Invariant {
				what: "ir_lower::fold: expr.call of a script"
			})
		));
	}
}
