//! Lowering closure literals and their invocation.
//!
//! A closure literal is an [`Instr::MakeClosure`]: the value is the
//! `CompiledClosure` a closure value is, made from the literal as written and
//! what it captures, with no compiled body. What the literal captures is read
//! where the literal is evaluated: a parameter the program holds
//! in a register or a block slot from there, any other from the frame the
//! literal is evaluated in, when bound there. `$f(args)` is an
//! [`Instr::Invoke`] of the target's value, checked a closure before the
//! arguments are evaluated.
//!
//! The body of a literal is lowered with the program that makes the literal,
//! as a unit of the root ([`ClosureHead`]), and so is the body of every
//! closure a parameter holds when the program is planned: a value finds its
//! unit by the digest of the closure it carries and the names it captures, in
//! a frame the unit admits ([`crate::exec::vm::closure`]). A closure that only
//! exists once the program runs has no unit of the program: the run faults
//! for one, and the host lowers it here ([`super::lower_closure_body`]).

use std::sync::Arc;

use super::{Lowered, Lowering, NotCovered};
use crate::exec::vm::program::{
	ClosureDigest, ClosureHead, ClosureValueSpec, ISA_VERSION, Instr, Operand, UnitFrame, UnitHead,
	UnitId, UnitSig,
};
use crate::expr::logical::{LogicalClosure, LogicalEval, LogicalExpr, LogicalOp};

/// A closure body linked into an arena: the closure as written, the names
/// its values capture, the frame the body was lowered for, and its unit.
///
/// The arena finds a body again by the closure's digest and compares the
/// closures as written too, so two closures whose digests agree share a
/// unit only when they are the same closure.
pub(super) struct ClosureLink {
	closure: Arc<LogicalClosure>,
	digest: ClosureDigest,
	captured: Vec<String>,
	frame: UnitFrame,
	/// The `VERSION` stamp the body was lowered at, if any.
	version: Option<u64>,
	unit: UnitId,
}

impl ClosureLink {
	/// Whether the link names a unit before `len`.
	pub(super) fn names_before(&self, len: usize) -> bool {
		(self.unit.0 as usize) < len
	}
}

impl Lowering<'_, '_> {
	/// The value closure literal `expr` makes, or `None` when the literal
	/// does not compile to one.
	pub(super) async fn closure_value(&mut self, expr: &LogicalExpr) -> Lowered<Option<Operand>> {
		let LogicalExpr::Closure(closure) = expr else {
			return Ok(None);
		};
		// The compilation of the literal answers what its body does to
		// the transaction; it plans the body's statements over the planner's
		// catalog, as mapping does, so it is made unsealed
		// ([`crate::kvs::catalog_reads::sealed`]).
		let compiled = crate::kvs::catalog_reads::sealed(
			false,
			crate::exec::vm::host::out_of_line(|| self.planner.scalar_metadata(expr.clone())),
		)
		.await;
		// A body the planner refuses to compile is refused with its error.
		let compiled = compiled.map_err(|e| match super::super::plan::Refusal::of(&e) {
			Some(refusal) => NotCovered::refused(refusal),
			None => NotCovered::new(format!("a closure the planner does not compile: {e}")),
		})?;
		// What the literal's body does to the transaction is the compiled
		// body's own answer; the value made is inert.
		let access_mode = compiled.access_mode();
		// The parameters the body reads, as the literal's capture walks them.
		let mut names: Vec<String> = Vec::new();
		let _ = closure.body.visit_deep(&mut |node| {
			if let LogicalExpr::Param(param) = node {
				let name = param.as_str();
				if !surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name)
					&& !names.iter().any(|n| n == name)
				{
					names.push(name.to_owned());
				}
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		let mut slots = Vec::new();
		let mut operands = Vec::new();
		let mut framed = Vec::new();
		for name in names {
			if let Some((_, reg)) = self.inline_params.iter().find(|(n, _)| *n == name) {
				operands.push(Operand::Reg(*reg));
				slots.push(name);
				continue;
			}
			if let Some(&(_, slot)) = self.slot_names.iter().rev().find(|(n, _)| *n == name) {
				if self.slot_hidden == 0 && self.eval_frame.is_none() {
					let dst = self.register()?;
					self.emit(Instr::LoadSlot {
						slot,
						dst,
					});
					operands.push(Operand::Reg(dst));
					slots.push(name);
					continue;
				}
				self.frame_reads += 1;
			}
			framed.push(name);
		}
		// The body is lowered for the values this literal makes: they
		// capture its slots, and the names read from the frame that are
		// bound where it is evaluated — a name the context or the block holds.
		let mut captured = slots.clone();
		captured.extend(framed.iter().filter(|name| self.binds_where_evaluated(name)).cloned());
		let mut closure = Arc::new((**closure).clone());
		let body =
			crate::exec::vm::host::out_of_line(|| self.closure_unit(&closure, captured)).await?;
		// The values of a literal another literal's unit stands for share
		// that literal's closure, so a run finds the unit by identity.
		if let Some(held) = body.and_then(|unit| self.linked_closure(unit)) {
			closure = held;
		}
		let index = u32::try_from(self.closure_values.len())
			.map_err(|_| NotCovered::internal("more closure literals than a program holds"))?;
		self.closure_values.push(ClosureValueSpec {
			digest: ClosureDigest::of(&closure),
			closure,
			body,
			slots,
			framed,
			frame: self.eval_frame,
			access_mode,
		});
		let dst = self.register()?;
		self.emit(Instr::MakeClosure {
			closure: index,
			slots: operands,
			dst,
		});
		self.uses_eval = true;
		Ok(Some(Operand::Reg(dst)))
	}

	/// The closure as written that unit `unit` of the arena is the body of.
	fn linked_closure(&self, unit: UnitId) -> Option<Arc<LogicalClosure>> {
		self.closure_links
			.iter()
			.find(|link| link.unit == unit)
			.map(|link| Arc::clone(&link.closure))
	}

	/// Whether `name`, read from the frame a closure literal is evaluated in,
	/// is bound there: a name the block, a loop or a call binds around the
	/// literal, or one the context holds when the program is planned.
	fn binds_where_evaluated(&self, name: &str) -> bool {
		self.slot_names.iter().any(|(bound, _)| bound == name)
			|| self.planner.param_is_shadowed(name)
			|| self.caller_value(name).is_some()
	}

	/// Lower the body of every closure `value`, a parameter's value when the
	/// program is planned, holds, as a unit for the invocations the program
	/// makes of it.
	pub(super) async fn prepare_closures_in(&mut self, value: &crate::val::Value) {
		let mut closures = Vec::new();
		collect_closures(value, &mut closures, 64);
		for compiled in closures {
			let captured =
				compiled.captures().0.keys().map(|name| name.as_str().to_owned()).collect();
			let logical = Arc::clone(compiled.logical());
			// A body the VM does not lower here is one an invocation faults
			// for, as a value with no unit is.
			let _ =
				crate::exec::vm::host::out_of_line(|| self.closure_unit(&logical, captured)).await;
		}
	}

	/// The unit that is `closure`'s body for values capturing `captured`, in
	/// the frame this lowering is planned for, as [`Self::closure_body_unit`]
	/// lowers it; `None` where it lowers none, for an invocation to fault for.
	pub(super) async fn closure_unit(
		&mut self,
		closure: &Arc<LogicalClosure>,
		captured: Vec<String>,
	) -> Lowered<Option<UnitId>> {
		Ok(crate::exec::vm::host::out_of_line(|| self.closure_body_unit(closure, captured))
			.await?
			.ok())
	}

	/// The unit that is `closure`'s body for values capturing `captured`, in
	/// the frame this lowering is planned for: lowered into the arena the
	/// first time and found there after. Refused, with the reason, when the
	/// body binds differently on each reach, when the planner has no
	/// principal or transaction to lower for, or when the VM does not lower
	/// it; an error where the lowering broke an invariant of every program.
	///
	/// The body is one pipeline over the row an invocation reads, emitting
	/// the body's value, reading the arguments and `captured` from its slots.
	/// Every name the body reads is shadowed, so binding substitutes none of
	/// them: a name read where the slots are not in scope, and a name neither
	/// an argument nor captured, is read from the invocation's frame, which
	/// binds the arguments and captures. The row is the invocation's
	/// document root, or its current value where it has no document
	/// ([`DocumentRoot::RowElseReceiver`](super::DocumentRoot)). A planner
	/// reading at a `VERSION` stamp lowers the body at that stamp, and the
	/// unit's head names it: the unit runs only invocations read at it.
	pub(super) async fn closure_body_unit(
		&mut self,
		closure: &Arc<LogicalClosure>,
		mut captured: Vec<String>,
	) -> Lowered<Result<UnitId, NotCovered>> {
		if self.planner.auth.is_none() {
			return Ok(Err(NotCovered::new("a closure body lowered for no principal")));
		}
		let Some(txn) = self.planner.txn().cloned() else {
			return Ok(Err(NotCovered::new("a closure body lowered with no transaction")));
		};
		captured.sort();
		captured.dedup();
		let digest = ClosureDigest::of(closure);
		let frame = Self::statement_frame(self.planner);
		let version = match self.planner.plan_time_version {
			crate::exec::planner::PlanTimeVersion::At(stamp) => Some(stamp),
			_ => None,
		};
		let held = self.closure_links.iter().find(|link| {
			link.digest == digest
				&& link.captured == captured
				&& link.frame == frame
				&& link.version == version
				&& *link.closure == **closure
		});
		if let Some(held) = held {
			return Ok(Ok(held.unit));
		}
		crate::exec::compiles::note();
		let mut slots: Vec<String> =
			closure.args.iter().map(|(arg, _)| arg.as_str().to_owned()).collect();
		for name in &captured {
			if !slots.contains(name) {
				slots.push(name.clone());
			}
		}
		let mut names = slots.clone();
		let _ = closure.body.visit_deep(&mut |node| {
			if let LogicalExpr::Param(param) = node
				&& !names.iter().any(|n| n == param.as_str())
			{
				names.push(param.as_str().to_owned());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		// A schema statement the body holds is planned as a block's is: what
		// follows it is bound when it runs.
		let planner =
			self.planner.own_scope().at_stamp(version).shadowing(names).binding_late_after_schema();
		let op = LogicalOp::Eval(LogicalEval {
			expr: closure.body.clone(),
		});
		if !crate::exec::vm::map::binds_alike_closure(&op, &planner) {
			return Ok(Err(NotCovered::new("a closure body that binds differently on each reach")));
		}
		let recording = txn.record_catalog_reads();
		let stamps = txn.catalog_stamps();
		let bound = match crate::exec::planner::bind_statement(&planner, op).await {
			Ok(LogicalOp::Eval(LogicalEval {
				expr,
			})) => expr,
			Ok(_) => return Ok(Err(NotCovered::internal("a closure body bound to a statement"))),
			Err(e) => {
				return Ok(Err(NotCovered::new(format!("a closure body that fails to bind: {e}"))));
			}
		};
		let effects = crate::exec::logical::effects_of_body(&bound, planner.function_registry());
		let mut l = Lowering::new(&planner);
		l.catalog = Arc::clone(&self.catalog);
		// The row is whatever value the invocation reads, not a record, and
		// it is the document root of what the body evaluates.
		l.rows_are_objects = false;
		l.document = super::DocumentRoot::RowElseReceiver;
		let Ok(slot_names) = slots
			.iter()
			.enumerate()
			.map(|(slot, name)| u16::try_from(slot).map(|slot| (name.clone(), slot)))
			.collect::<Result<Vec<_>, _>>()
		else {
			return Ok(Err(NotCovered::internal("a closure binding more names than a unit holds")));
		};
		l.slot_names = slot_names;
		l.put_arena(self.take_arena());
		l.bundle = self.bundle;
		l.versioned = self.versioned;
		let lowered = Box::pin(l.host_pipeline(&bound)).await;
		// The root's table goes back whatever the body did, nested units
		// included.
		self.put_arena(l.take_arena());
		let pipeline = match lowered {
			Ok(pipeline) => pipeline,
			Err(refused) if refused.internal => return Err(refused),
			Err(refused) => return Ok(Err(refused)),
		};
		let registry = planner.function_registry();
		let frameless = l.frame_reads == 0
			&& l.params.is_empty()
			&& l.call_needs.is_empty()
			&& l.externals.is_empty()
			&& l.host_code.is_empty()
			&& l.subqueries.is_empty()
			&& l.nested.is_empty()
			&& l.groups.is_empty()
			&& l.functions.is_empty()
			&& l.methods.is_empty()
			&& l.closure_values.is_empty()
			&& l.closures.is_empty()
			&& l.lookups.is_empty()
			&& l.invokes == 0
			&& l.calls.iter().all(|call| {
				registry.get(&call.name).is_some_and(|function| function.closure_args().is_empty())
			});
		l.pipelines.push(pipeline);
		l.scalar = true;
		let mut program = match l.finish() {
			Ok(program) => program,
			Err(refused) if refused.internal => return Err(refused),
			Err(refused) => return Ok(Err(refused)),
		};
		let recorded = recording.finish();
		// The state of what the body was lowered from, kept with the unit for
		// a later run to judge it by: the unit cache's read, made unsealed.
		let reads = crate::kvs::catalog_reads::sealed(
			false,
			crate::exec::vm::closure::ClosureReads::of(&txn, &recorded),
		)
		.await;
		program.unit = Some(UnitHead {
			sig: UnitSig {
				params: slots.iter().map(|name| name.as_str().into()).collect(),
				takes_row: true,
			},
			frame: frame.clone(),
			entry: None,
			closure: Some(ClosureHead {
				digest,
				closure: Arc::clone(closure),
				isa: ISA_VERSION,
				captured: captured.clone(),
				slots,
				frameless,
				effects,
				stamps,
				reads,
				version,
			}),
		});
		let id = u32::try_from(self.units.len())
			.map_err(|_| NotCovered::internal("more units than a program holds"))?;
		self.units.push(Arc::new(program));
		self.closure_links.push(ClosureLink {
			closure: Arc::clone(closure),
			digest,
			captured,
			frame,
			version,
			unit: UnitId(id),
		});
		Ok(Ok(UnitId(id)))
	}

	/// `base.name(args)` for a name no builtin method has: the closure the
	/// receiver's field of that name holds, checked before the arguments are
	/// evaluated, invoked with them.
	pub(super) async fn closure_field_call(
		&mut self,
		base: &LogicalExpr,
		name: &surrealdb_strand::Strand,
		args: &[LogicalExpr],
	) -> Lowered<Operand> {
		let recv = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
		let callee = self.register()?;
		self.emit(Instr::ClosureField {
			name: name.clone(),
			recv,
			dst: callee,
		});
		let mut operands = Vec::with_capacity(args.len());
		for arg in args {
			operands.push(crate::exec::vm::host::out_of_line(|| self.step_arg(arg, recv)).await?);
		}
		let dst = self.register()?;
		self.emit(Instr::Invoke {
			target: Operand::Reg(callee),
			args: operands,
			dst,
			frame: self.eval_frame,
		});
		// The field's body is not the program's to read.
		self.invokes += 1;
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// `target(args)`: the target's value checked a closure, then the
	/// arguments, then the invocation.
	pub(super) async fn invoke(
		&mut self,
		target: &LogicalExpr,
		args: &[LogicalExpr],
	) -> Lowered<Operand> {
		let callee = crate::exec::vm::host::out_of_line(|| self.value(target)).await?;
		self.emit(Instr::RequireClosure {
			src: callee,
		});
		let mut operands = Vec::with_capacity(args.len());
		for arg in args {
			operands.push(crate::exec::vm::host::out_of_line(|| self.value(arg)).await?);
		}
		let dst = self.register()?;
		self.emit(Instr::Invoke {
			target: callee,
			args: operands,
			dst,
			frame: self.eval_frame,
		});
		// A literal's body is the program's to read; any other target's is not.
		if !matches!(target, LogicalExpr::Closure(_)) {
			self.invokes += 1;
		}
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}
}

/// The compiled closures `value` holds, looking at most `budget` values deep
/// into arrays and objects.
fn collect_closures<'a>(
	value: &'a crate::val::Value,
	out: &mut Vec<&'a crate::exec::vm::calls::CompiledClosure>,
	budget: usize,
) {
	use crate::val::{Closure, Value};
	if budget == 0 {
		return;
	}
	match value {
		Value::Closure(closure) => {
			if let Closure::Builtin(obj) = closure.as_ref()
				&& let Some(compiled) =
					obj.as_any().downcast_ref::<crate::exec::vm::calls::CompiledClosure>()
			{
				out.push(compiled);
			}
		}
		Value::Array(array) => {
			for item in array.iter() {
				collect_closures(item, out, budget - 1);
			}
		}
		Value::Object(object) => {
			for item in object.values() {
				collect_closures(item, out, budget - 1);
			}
		}
		_ => {}
	}
}
