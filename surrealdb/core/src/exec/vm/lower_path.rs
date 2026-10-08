//! Lowering the path steps that build a value of their own from the value
//! they step from: a destructure, `v.{ a, b: body }`, and an optional chain,
//! `v.?rest`.
//!
//! A recursion, `v.{min..max+instruction}(body)`, is an [`Instr::Recurse`]
//! the host takes with the operator tree's recursion strategies, its body a
//! unit of the root program over each depth's value ([`super::program::UnitId`])
//! lowered with the statement's planner, its catalog reads the statement's.
//! The body reads the row only through that value: its dynamic indexes and
//! method arguments, which the tree evaluates against the row, read nothing
//! of it, and it reads no `$parent`, statement, closure reading more than its
//! own parameters, or name the pipeline holds in a register or slot. A body
//! that re-enters through `.@` ([`super::program::RecurseSpec::repeat`]) is
//! run breadth first in discovery and then assembled from the deepest level
//! back, as the operator tree runs it, its `.@` markers
//! [`Instr::RepeatRecurse`]. A shortest path's target is evaluated where the
//! recursion is, when it reads no row.
//!
//! An optional chain is `NONE` or `NULL` as its base is, and otherwise the rest
//! of the path with the base as the element it steps from, as the operator
//! tree's `OptionalChainPart` answers; the rest is lowered where a broadcast's
//! body would be, reading the row nowhere the tree reads it.
//!
//! A destructure is an [`Instr::Destructure`] the host takes: an object has
//! its listed fields gathered, a record id is fetched and its record
//! destructured, an array has the destructure mapped over its elements, and
//! anything else is `NONE`, as the operator tree's `DestructurePart` answers.
//! An aliased field's body is a host pipeline of the program over the value
//! destructured, which is its current value and its document.

use std::sync::Arc;

use super::{Lowered, Lowering, NotCovered};
use crate::exec::vm::plan::Refusal;
use crate::exec::vm::program::{
	DestructureStep, Instr, Operand, RecurseCollect, RecurseSpec, UnitFrame, UnitHead, UnitId,
	UnitSig,
};
use crate::expr::logical::{
	LogicalClosure, LogicalDestructureField, LogicalExpr, LogicalRecurse, LogicalRecurseInstruction,
};

/// Whether closure literal `closure`'s body reads nothing but its own
/// parameters: no field, current value or document, no name it does not
/// declare, and nothing that plans or enters code anew — a statement, a
/// closure, a lookup or a call of anything but a builtin.
pub(super) fn reads_only_its_parameters(closure: &LogicalClosure) -> bool {
	!closure.body.any(|node| match node {
		LogicalExpr::Field(_)
		| LogicalExpr::Current
		| LogicalExpr::Doc
		| LogicalExpr::Subquery(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::Lookup {
			..
		} => true,
		LogicalExpr::Call(call) => !matches!(call.receiver, crate::expr::Function::Normal(_)),
		LogicalExpr::Param(param) => {
			!closure.args.iter().any(|(arg, _)| arg.as_str() == param.as_str())
		}
		_ => false,
	})
}

impl Lowering<'_, '_> {
	/// Whether a destructure of `fields` from `base` is lowered to an
	/// [`Instr::Destructure`] whose bodies are instructions: outside any frame,
	/// where no register the pipeline binds a name to is read.
	pub(super) fn is_native_destructure(
		&self,
		base: &LogicalExpr,
		fields: &[LogicalDestructureField],
	) -> bool {
		fn bodies_native(l: &Lowering<'_, '_>, fields: &[LogicalDestructureField]) -> bool {
			fields.iter().all(|field| match field {
				LogicalDestructureField::All(_)
				| LogicalDestructureField::Field(_)
				| LogicalDestructureField::Rest => true,
				LogicalDestructureField::Aliased(_, body) => l.is_native(body),
				LogicalDestructureField::Destructure(_, nested) => bodies_native(l, nested),
			})
		}
		self.inline_params.is_empty() && self.is_native(base) && bodies_native(self, fields)
	}

	/// `base.{ fields }` as an [`Instr::Destructure`].
	pub(super) async fn destructure(
		&mut self,
		base: &LogicalExpr,
		fields: &[LogicalDestructureField],
	) -> Lowered<Operand> {
		if !self.inline_params.is_empty() {
			return Err(NotCovered::internal("a destructure reading names bound in registers"));
		}
		let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
		let steps = crate::exec::vm::host::out_of_line(|| self.destructure_steps(fields)).await?;
		self.reaches(crate::exec::vm::dispatch::Reach::Any);
		let dst = self.register()?;
		self.emit(Instr::Destructure {
			src,
			fields: steps,
			dst,
			frame: self.eval_frame,
		});
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// The steps of `fields`, each aliased body a host pipeline over the value
	/// destructured.
	async fn destructure_steps(
		&mut self,
		fields: &[LogicalDestructureField],
	) -> Lowered<Arc<[DestructureStep]>> {
		let mut steps = Vec::with_capacity(fields.len());
		for field in fields {
			steps.push(match field {
				LogicalDestructureField::All(name) => DestructureStep::All(name.clone()),
				LogicalDestructureField::Field(name) => DestructureStep::Field(name.clone()),
				LogicalDestructureField::Rest => DestructureStep::Rest,
				LogicalDestructureField::Aliased(name, body) => {
					// The body's pipeline reads the value destructured, not
					// the element or the catch the destructure is lowered in.
					let element = self.element.take();
					let catch = self.return_catch.take();
					let aside = self.set_aside();
					let built =
						crate::exec::vm::host::out_of_line(|| self.image_value_body(body)).await;
					let pipeline = self.restore(aside, built);
					self.element = element;
					self.return_catch = catch;
					DestructureStep::Aliased(name.clone(), pipeline?)
				}
				LogicalDestructureField::Destructure(name, nested) => DestructureStep::Nested(
					name.clone(),
					crate::exec::vm::host::out_of_line(|| self.destructure_steps(nested)).await?,
				),
			});
		}
		Ok(steps.into())
	}

	/// Whether recursion `recurse` lowers to an [`Instr::Recurse`]; see the
	/// module.
	pub(super) fn is_native_recurse(&self, recurse: &LogicalRecurse) -> bool {
		if !self.is_native(&recurse.base) {
			return false;
		}
		// A shortest path's target is evaluated against the row the recursion
		// steps from, which instructions read only where it reads no row.
		if let Some(LogicalRecurseInstruction::Shortest {
			expects,
			..
		}) = &recurse.instruction
			&& crate::exec::planner::static_value(expects).is_none()
			&& (!self.is_native(expects) || super::graph::reads_row(expects))
		{
			return false;
		}
		let Some(path) = &recurse.path else {
			return true;
		};
		let body = path.body();
		// An instruction beside a trailing body that re-enters through `.@` is
		// the operator tree's to refuse.
		if recurse.instruction.is_some()
			&& matches!(path, crate::expr::logical::LogicalRecursePath::Trailing(_))
			&& crate::exec::planner::contains_repeat_recurse(body)
		{
			return false;
		}
		!self.body_reads_outside(body, false)
	}

	/// Whether recursion body `expr` reads what the body's unit does not
	/// hold: the enclosing row the operator tree's recursion carries across,
	/// or a name the pipeline holds in a register or slot. Within an aliased
	/// field of a destructure (`destructured`), the document is the value
	/// destructured, which the unit holds, so `$parent`, a statement reading
	/// the enclosing row, and a closure written or invoked there read that
	/// value.
	fn body_reads_outside(&self, expr: &LogicalExpr, destructured: bool) -> bool {
		let reads = match expr {
			LogicalExpr::Destructure {
				base,
				fields,
			} => {
				return self.body_reads_outside(base, destructured)
					|| self.fields_read_outside(fields);
			}
			// A closure the body writes is invoked against the document the
			// operator tree's recursion carries across, the enclosing row,
			// which the body's unit does not hold: one reading nothing but its
			// own parameters reads none of it.
			LogicalExpr::Closure(closure) => !destructured && !reads_only_its_parameters(closure),
			// A statement is taken over that document too: one reading no
			// row, and no name the program binds in a register, answers
			// alike in the body's unit.
			LogicalExpr::Subquery(op) => {
				(!destructured && crate::exec::planner::row_scope::reads_enclosing_row(op))
					|| self.reads_bound_name(op)
			}
			LogicalExpr::Invoke {
				..
			} => !destructured,
			LogicalExpr::Param(param) => {
				(!destructured && param.as_str() == "parent")
					|| self.inline_params.iter().any(|(name, _)| name == param.as_str())
					|| self.slot_names.iter().any(|(name, _)| name == param.as_str())
			}
			LogicalExpr::Index {
				index,
				..
			} => super::graph::reads_row(index),
			LogicalExpr::Method {
				args,
				..
			} => args.iter().any(super::graph::reads_row),
			_ => false,
		};
		if reads {
			return true;
		}
		let mut found = false;
		expr.for_each_child(&mut |child| {
			found = found || self.body_reads_outside(child, destructured);
		});
		found
	}

	/// Whether an aliased field of `fields`, or of a destructure nested in
	/// them, reads what the recursion body's unit does not hold.
	fn fields_read_outside(&self, fields: &[LogicalDestructureField]) -> bool {
		fields.iter().any(|field| match field {
			LogicalDestructureField::Aliased(_, body) => self.body_reads_outside(body, true),
			LogicalDestructureField::Destructure(_, fields) => self.fields_read_outside(fields),
			_ => false,
		})
	}

	/// Whether plan `op`, or a plan nested in it, reads a name this program
	/// binds in a register rather than in the frame: an inlined parameter or
	/// a block's slot.
	fn reads_bound_name(&self, op: &crate::expr::logical::LogicalOp) -> bool {
		let bound = |name: &str| {
			self.inline_params.iter().any(|(held, _)| held == name)
				|| self.slot_names.iter().any(|(held, _)| held == name)
		};
		op.visit_ops_deep(&mut |node| {
			let mut reads = false;
			let _ = node.visit_scalars(&mut |scalar| {
				reads = reads
					|| scalar.any(|e| matches!(e, LogicalExpr::Param(p) if bound(p.as_str())));
				Ok::<(), std::convert::Infallible>(())
			});
			match reads {
				true => std::ops::ControlFlow::Break(()),
				false => std::ops::ControlFlow::Continue(()),
			}
		})
		.is_break()
	}

	/// The refusal a recursion in `expr` is answered with, where the planner
	/// refuses it as it plans it: a bound outside `1..=idiom_recursion_limit`,
	/// or an instruction beside a trailing body that re-enters through `.@`,
	/// which decides where the recursion re-enters and cannot be honoured
	/// beside the instruction that decides what it collects.
	pub(super) fn recurse_refusal(&self, expr: &LogicalExpr) -> Option<Refusal> {
		let limit = self.planner.frozen().config.exec.idiom_recursion_limit;
		let mut refusal = None;
		expr.any(|node| {
			if let LogicalExpr::Recurse(recurse) = node {
				refusal = recursion_refusal(recurse, limit);
			}
			refusal.is_some()
		});
		refusal
	}

	/// `recurse` as an [`Instr::Recurse`].
	pub(super) async fn recurse(&mut self, recurse: &LogicalRecurse) -> Lowered<Operand> {
		use crate::expr::part::Recurse;
		let (min, max) = match recurse.recurse {
			Recurse::Fixed(n) => (n, Some(n)),
			Recurse::Range(min, max) => (min.unwrap_or(1), max),
		};
		let limit = self.planner.frozen().config.exec.idiom_recursion_limit;
		if let Some(refusal) = recursion_refusal(recurse, limit) {
			return Err(NotCovered::refused(refusal));
		}
		let collect = match &recurse.instruction {
			None => RecurseCollect::Last,
			Some(LogicalRecurseInstruction::Collect {
				inclusive,
			}) => RecurseCollect::All {
				inclusive: *inclusive,
			},
			Some(LogicalRecurseInstruction::Path {
				inclusive,
			}) => RecurseCollect::Paths {
				inclusive: *inclusive,
			},
			Some(LogicalRecurseInstruction::Shortest {
				inclusive,
				..
			}) => RecurseCollect::Shortest {
				inclusive: *inclusive,
			},
		};
		let repeat = recurse
			.path
			.as_ref()
			.is_some_and(|path| crate::exec::planner::contains_repeat_recurse(path.body()));
		let body = match &recurse.path {
			None => None,
			Some(path) => {
				Some(crate::exec::vm::host::out_of_line(|| self.unit(path.body())).await?)
			}
		};
		let src = crate::exec::vm::host::out_of_line(|| self.value(&recurse.base)).await?;
		let target = match &recurse.instruction {
			Some(LogicalRecurseInstruction::Shortest {
				expects,
				..
			}) => Some(crate::exec::vm::host::out_of_line(|| self.value(expects)).await?),
			_ => None,
		};
		let dst = self.register()?;
		self.emit(Instr::Recurse {
			src,
			spec: Arc::new(RecurseSpec {
				body,
				min,
				max,
				collect,
				repeat,
			}),
			target,
			dst,
		});
		self.delegate_context = self.delegate_context.max(crate::exec::ContextLevel::Database);
		self.uses_eval = true;
		self.may_fail = true;
		Ok(Operand::Reg(dst))
	}

	/// `body` as a unit of the root: a program of its own over the value it is
	/// run with, emitting `body`'s value.
	async fn unit(&mut self, body: &LogicalExpr) -> Lowered<UnitId> {
		let mut l = Lowering::new(self.planner);
		l.catalog = Arc::clone(&self.catalog);
		// The body runs in this lowering's frame, privileged where it is.
		l.privileged = self.in_privileged_frame();
		// Each depth's value is whatever the body reached, not a record.
		l.rows_are_objects = false;
		l.put_arena(self.take_arena());
		l.bundle = self.bundle;
		l.versioned = self.versioned;
		let lowered = Box::pin(l.host_pipeline(body)).await;
		// The root's table goes back whatever the body did, nested units included.
		self.put_arena(l.take_arena());
		l.pipelines.push(lowered?);
		l.scalar = true;
		let mut program = l.finish()?;
		// A body that writes makes the recursion a write of the program
		// holding it, run in order, as a writing call is.
		if program.access_mode != crate::exec::AccessMode::ReadOnly {
			self.invokes += 1;
		}
		self.delegate_context = self.delegate_context.max(program.required_context);
		program.unit = Some(UnitHead {
			sig: UnitSig {
				params: Vec::new(),
				takes_row: true,
			},
			frame: UnitFrame {
				class_digest: self.planner.class_digest(self.planner.auth.as_deref()),
				ns: self.planner.ns.clone(),
				db: self.planner.db.clone(),
				perms_suppressed: self.planner.perms_suppressed,
				checked: !self.reads_unchecked(),
			},
			entry: None,
			closure: None,
		});
		let id = u32::try_from(self.units.len())
			.map_err(|_| NotCovered::internal("more units than a program holds"))?;
		self.units.push(Arc::new(program));
		Ok(UnitId(id))
	}

	/// `base.?then`: `base` when it is `NONE` or `NULL`, and otherwise `then`
	/// with `base` as its element.
	pub(super) async fn optional(
		&mut self,
		base: &LogicalExpr,
		then: &LogicalExpr,
	) -> Lowered<Operand> {
		let src = crate::exec::vm::host::out_of_line(|| self.value(base)).await?;
		let held = self.register()?;
		self.emit(Instr::Load {
			dst: held,
			src,
		});
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: Operand::Reg(held),
		});
		let skip = self.emit(Instr::JumpIfNullish {
			cond: Operand::Reg(held),
			nullish: true,
			target: 0,
		});
		let outer = self.element.replace(held);
		let value = crate::exec::vm::host::out_of_line(|| self.value(then)).await;
		self.element = outer;
		self.emit(Instr::Load {
			dst,
			src: value?,
		});
		let end = self.here();
		self.patch(skip, end);
		Ok(Operand::Reg(dst))
	}

	/// The body of a host pipeline over a value, emitting `body`'s value.
	async fn image_value_body(&mut self, body: &LogicalExpr) -> Lowered<()> {
		// The value destructured is an object: a record id is fetched first.
		let value = crate::exec::vm::host::out_of_line(|| self.value(body)).await?;
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}
}

/// The refusal for `recurse` under the recursion limit `limit`, in the order
/// the planner checks it: the bounds, then the instruction beside a repeat.
fn recursion_refusal(recurse: &LogicalRecurse, limit: u32) -> Option<Refusal> {
	use crate::expr::part::Recurse;
	let (min, max) = match recurse.recurse {
		Recurse::Fixed(n) => (n, Some(n)),
		Recurse::Range(min, max) => (min.unwrap_or(1), max),
	};
	if min < 1 {
		return Some(Refusal::InvalidBound {
			found: min.to_string(),
			expected: "at least 1".into(),
		});
	}
	if let Some(max) = max
		&& max > limit
	{
		return Some(Refusal::InvalidBound {
			found: max.to_string(),
			expected: format!("{limit} at most"),
		});
	}
	let conflict = recurse.instruction.is_some()
		&& recurse.path.as_ref().is_some_and(|path| {
			matches!(path, crate::expr::logical::LogicalRecursePath::Trailing(_))
				&& crate::exec::planner::contains_repeat_recurse(path.body())
		});
	conflict.then_some(Refusal::RecursionInstructionPlanConflict)
}
