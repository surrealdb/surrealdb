//! Lowering of the path steps: a field, the record identity, the first and last element,
//! flattening, every element, an object with one key set, an optional chain, a destructure and
//! the re-entry marker of a recursion.
//!
//! A step reads the value it steps from as its first operand and builds a value of its own.
//!
//! # Fields
//!
//! A field of an object row is an operand of the row ([`Operand::Field`]) and emits nothing. A
//! field of any other value is an [`Instr::Access`], which the host answers: it fetches a record
//! id, and reads an array or a set element by element. `expr.computed_read` reads the field the
//! row holds under the field's name: the derivation of a `COMPUTED` field is a step of the
//! pipeline that holds the row, which has run before the row is read.
//!
//! # Optional chains and destructures
//!
//! An optional chain is its base when that is `NONE` or `NULL`, and otherwise the value of its
//! region, whose one block argument is the base. A destructure is an [`Instr::Destructure`] the
//! host takes: an object has its listed fields gathered, a record id is fetched and its record
//! destructured, an array has the destructure mapped over its elements, and anything else is
//! `NONE`. The body of an aliased field is a host pipeline of the program over the value
//! destructured, which is its current value and its document, and the one block argument of the
//! alias's region. Such a pipeline holds nothing the program's pipeline holds in a register, so a
//! body that reads a value defined outside its region answers `NotYet`.

use std::sync::Arc;

use surrealdb_strand::Strand;

use super::super::program::{
	DestructureStep, Instr, NavOp, Operand, RecurseCollect, RecurseSpec, StoreInstr,
};
use super::expr::{Lowerer, exactly, region_at};
use super::units::UnitRequest;
use crate::exec::ContextLevel;
use crate::exec::field_path::FieldPath;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{DestructureEntry, ExprOp};
use crate::exec::ir::iface::ids::{RegionId, ValueId};
use crate::exec::ir::logical::ops::{
	LoopKind, RecurseAttrs, RecurseInstructionShape, RecursePathForm,
};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::vm::dispatch::Reach;
use crate::exec::vm::payload::Refusal;
use crate::expr::part::Recurse;
use crate::val::Value;

impl Lowerer<'_> {
	/// The field `name` of the row `data` reads, which is the op's one operand.
	///
	/// `in_fold` names the op for the plan-time folder, whose subset has no row.
	pub(super) fn field(
		&mut self,
		data: &OpData,
		name: &Strand,
		in_fold: &'static str,
	) -> Result<Operand, IrError> {
		self.refuse_in_fold(in_fold)?;
		self.require_frame(data)?;
		let [row] = exactly::<1>(data)?;
		let field = self.em.field(name.as_str())?;
		if self.is_object_row(row) {
			return Ok(Operand::Field(field));
		}
		let src = self.value(row)?;
		let dst = self.em.reg()?;
		self.emit(Instr::Access {
			src,
			field,
			dst,
			frame: self.frame.program,
		});
		Ok(Operand::Reg(dst))
	}

	/// The record identity of the value `data` reads, which is the op's one operand: the id a
	/// record carries, and the value itself otherwise.
	pub(super) fn doc(&mut self, data: &OpData) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.doc")?;
		self.require_frame(data)?;
		self.nav(data, NavOp::Identity, "ir_lower::fold: expr.doc")
	}

	/// The step `op` over the one operand of `data`.
	pub(super) fn nav(
		&mut self,
		data: &OpData,
		op: NavOp,
		in_fold: &'static str,
	) -> Result<Operand, IrError> {
		self.refuse_in_fold(in_fold)?;
		let [base] = exactly::<1>(data)?;
		let src = self.value(base)?;
		let dst = self.em.reg()?;
		self.emit(Instr::Nav {
			op,
			src,
			index: None,
			dst,
		});
		Ok(Operand::Reg(dst))
	}

	/// Every element of the operand: an object as it is, an array or a set with each record id
	/// fetched, a record id fetched, and anything else `NONE`. The records fetched may be of any
	/// table, so the program's reach is every table.
	///
	/// Where the operand is the element of the broadcast whose body is being lowered
	/// (`each_guard`), the step is chosen when it runs. While the broadcast walks a collection, it
	/// is taken only over an element that is a record id, and any other element is the step's own
	/// value. Over a lone value it is taken as written. Both forms are emitted here, so the body
	/// that holds the step is lowered once for both of the broadcast's paths.
	pub(super) fn each(&mut self, data: &OpData) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.each")?;
		self.require_frame(data)?;
		let [base] = exactly::<1>(data)?;
		let src = self.value(base)?;
		self.reaches(Reach::any());
		if let Some((element, single)) = self.each_guard
			&& element == base
		{
			let plain = self.em.reg()?;
			self.emit(Instr::Load {
				dst: plain,
				src,
			});
			let lone = self.em.label()?;
			let end = self.em.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(single),
				truthy: true,
				target: lone,
			});
			self.emit(Instr::JumpIfNotRecordId {
				cond: src,
				target: end,
			});
			self.emit(Instr::Each {
				src,
				dst: plain,
				frame: None,
			});
			self.emit(Instr::Jump {
				target: end,
			});
			self.em.bind(lone)?;
			self.emit(Instr::Each {
				src,
				dst: plain,
				frame: self.frame.program,
			});
			self.em.bind(end)?;
			return Ok(Operand::Reg(plain));
		}
		let dst = self.em.reg()?;
		self.emit(Instr::Each {
			src,
			dst,
			frame: self.frame.program,
		});
		Ok(Operand::Reg(dst))
	}

	/// The recursion a loop of kind `Recurse` is: an [`Instr::Recurse`] over the value it steps
	/// from.
	///
	/// The loop's operands are the value the recursion starts from and, for a shortest path, the
	/// value it looks for. Its depth is one level or a range of them, and the range's lower bound
	/// is one where it states none. A bound below one, or above the configuration's recursion
	/// limit, is refused, and so is an instruction beside a trailing body that re-enters through
	/// `.@`. The refusal is raised where the recursion is evaluated, before its operands are, and
	/// not where it is lowered: a recursion in a branch the run never takes, or in a closure
	/// applied to no element, raises nothing. The instruction decides what the recursion answers:
	/// the value its last depth reached, every value, every path, or the shortest path.
	///
	/// A recursion with a path steps by the body, which runs as a unit of the program over the
	/// value of a depth and answers the value the next depth starts from; one with no path steps
	/// by nothing, and holds no unit. The values defined outside the body that it reads are the
	/// unit's inputs ([`Lowerer::unit_inputs`]), evaluated once where the instruction sits and read
	/// alike by every depth. The instruction reads the database whatever the path does, so the
	/// program needs the database context, and a body whose effects write makes the program
	/// write.
	pub(super) fn recurse_loop(
		&mut self,
		data: &OpData,
		attrs: &RecurseAttrs,
	) -> Result<Operand, IrError> {
		let body = region_at(data, 0, 1)?;
		let (min, max) = match attrs.depth {
			Recurse::Fixed(depth) => (depth, Some(depth)),
			Recurse::Range(min, max) => (min.unwrap_or(1), max),
		};
		let repeat = holds_repeat_recurse(self.ir, body);
		if let Some(refusal) = recursion_refusal(
			min,
			max,
			self.limits().idiom_recursion_limit,
			attrs.instruction.is_some() && attrs.form == RecursePathForm::Trailing && repeat,
		) {
			// Emitted with no owner, as an `expr.raise` of a refusal is: the refusal reads
			// nothing of the row. The `NONE` it answers is never read.
			self.em.emit(Instr::Store(StoreInstr::Refuse {
				refusal: Box::new(refusal),
			}));
			return Ok(Operand::Const(self.em.constant(Value::None)?));
		}
		let (collect, shortest) = match &attrs.instruction {
			None => (RecurseCollect::Last, false),
			Some(RecurseInstructionShape::Collect {
				inclusive,
			}) => (
				RecurseCollect::All {
					inclusive: *inclusive,
				},
				false,
			),
			Some(RecurseInstructionShape::Path {
				inclusive,
			}) => (
				RecurseCollect::Paths {
					inclusive: *inclusive,
				},
				false,
			),
			Some(RecurseInstructionShape::Shortest {
				inclusive,
			}) => (
				RecurseCollect::Shortest {
					inclusive: *inclusive,
				},
				true,
			),
		};
		let (source, target) = match (data.operands.as_slice(), shortest) {
			([source], false) => (*source, None),
			([source, target], true) => (*source, Some(*target)),
			_ => return Err(IrError::Stale("ir_lower::path: recursion operand arity")),
		};
		let (unit, inputs) = match attrs.form {
			RecursePathForm::Absent => (None, Vec::new()),
			RecursePathForm::Nested | RecursePathForm::Trailing => {
				let inputs = self.unit_inputs(body)?;
				let unit = self.unit_for(UnitRequest::LoopBody {
					region: body,
				})?;
				(Some(unit), inputs)
			}
		};
		let src = self.value(source)?;
		let target = target.map(|target| self.value(target)).transpose()?;
		let dst = self.em.reg()?;
		self.emit(Instr::Recurse {
			src,
			spec: Arc::new(RecurseSpec {
				body: unit,
				min,
				max,
				collect,
				repeat,
				inputs,
			}),
			target,
			dst,
		});
		// What the body unit does to the transaction is written into the program by the compile
		// driver, once the unit is lowered.
		self.requires(ContextLevel::Database);
		Ok(Operand::Reg(dst))
	}

	/// The `.@` marker over the operand, in the phase of the recursion whose body holds it.
	pub(super) fn repeat_recurse(&mut self, data: &OpData) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.repeat_recurse")?;
		let [base] = exactly::<1>(data)?;
		let src = self.value(base)?;
		let dst = self.em.reg()?;
		self.emit(Instr::RepeatRecurse {
			src,
			dst,
		});
		Ok(Operand::Reg(dst))
	}

	/// The base with the key `field` set to the value: a `NONE` value removes the key, and a base
	/// that is not an object is its own result, the value unevaluated.
	///
	/// A value that no other op reads is lowered after the test of the base, so it runs only for an
	/// object. One another op reads is lowered before the test, as the op that defines it is the
	/// first to read it.
	pub(super) fn set_field(&mut self, data: &OpData, field: &Strand) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.set_field")?;
		let [base, value] = exactly::<2>(data)?;
		let src = self.value(base)?;
		let dst = self.em.reg()?;
		self.emit(Instr::Load {
			dst,
			src,
		});
		let lazy = self.read_only_by_owner(value);
		let eager = match lazy {
			true => None,
			false => Some(self.value(value)?),
		};
		let end = self.em.label()?;
		self.emit(Instr::JumpIfNotObject {
			cond: Operand::Reg(dst),
			target: end,
		});
		let value = match eager {
			Some(value) => value,
			None => {
				let held = self.scope.clone();
				let lowered = self.value(value);
				self.scope = held;
				lowered?
			}
		};
		let path = self.em.path(FieldPath::field(field.as_str()))?;
		self.emit(Instr::SetField {
			object: dst,
			path,
			value,
		});
		self.emit(Instr::JumpIfNone {
			cond: value,
			none: false,
			target: end,
		});
		let name = self.em.field(field.as_str())?;
		self.emit(Instr::RemoveField {
			object: dst,
			name,
		});
		self.em.bind(end)?;
		Ok(Operand::Reg(dst))
	}

	/// The operand when it is `NONE` or `NULL`, and otherwise the value of the region, whose one
	/// block argument is the operand.
	pub(super) fn optional(&mut self, data: &OpData) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.optional")?;
		let [base] = exactly::<1>(data)?;
		let then = region_at(data, 0, 1)?;
		let src = self.value(base)?;
		let held = self.em.reg()?;
		self.emit(Instr::Load {
			dst: held,
			src,
		});
		let dst = self.em.reg()?;
		self.emit(Instr::Load {
			dst,
			src: Operand::Reg(held),
		});
		let end = self.em.label()?;
		self.emit(Instr::JumpIfNullish {
			cond: Operand::Reg(held),
			nullish: true,
			target: end,
		});
		let value = self.element_value(then, held, &[])?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.em.bind(end)?;
		Ok(Operand::Reg(dst))
	}

	/// The destructure of the operand into the fields `shape` lists.
	///
	/// The op holds one region per aliased entry, in the depth-first order the entries are listed
	/// in.
	pub(super) fn destructure(
		&mut self,
		data: &OpData,
		shape: &[DestructureEntry],
	) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.destructure")?;
		self.require_frame(data)?;
		let [base] = exactly::<1>(data)?;
		let src = self.value(base)?;
		let mut regions = data.regions.iter().copied();
		let steps = self.destructure_steps(shape, &mut regions)?;
		if regions.next().is_some() {
			return Err(IrError::Stale(
				"ir_lower::path: a destructure holds a region no alias uses",
			));
		}
		self.reaches(Reach::any());
		let dst = self.em.reg()?;
		self.emit(Instr::Destructure {
			src,
			fields: steps.into(),
			dst,
			frame: self.frame.program,
		});
		Ok(Operand::Reg(dst))
	}

	/// The steps of `shape`, each aliased body a host pipeline over the value destructured.
	fn destructure_steps(
		&mut self,
		shape: &[DestructureEntry],
		regions: &mut impl Iterator<Item = RegionId>,
	) -> Result<Vec<DestructureStep>, IrError> {
		let mut steps = Vec::with_capacity(shape.len());
		for entry in shape {
			steps.push(match entry {
				DestructureEntry::All(name) => DestructureStep::All(name.clone()),
				DestructureEntry::Field(name) => DestructureStep::Field(name.clone()),
				DestructureEntry::Rest => DestructureStep::Rest,
				DestructureEntry::Aliased(name) => {
					let region = regions
						.next()
						.ok_or(IrError::Stale("ir_lower::path: an alias with no region"))?;
					DestructureStep::Aliased(name.clone(), self.aliased_body(region)?)
				}
				DestructureEntry::Destructure(name, nested) => DestructureStep::Nested(
					name.clone(),
					self.destructure_steps(nested, regions)?.into(),
				),
			});
		}
		Ok(steps)
	}

	/// The host pipeline, of the program's host pipelines, that emits the value `region` yields
	/// once, over the value destructured, which the region's one block argument names.
	fn aliased_body(&mut self, region: RegionId) -> Result<u16, IrError> {
		self.host_code_over_row(region)
	}
}

/// Whether the recursion body `body` re-enters through `.@` anywhere along its path.
///
/// The path is the chain of steps from the value the body yields back to the value it starts
/// from: each step's base operand. The marker counts on that chain, inside a broadcast's or an
/// optional chain's body, inside the body of a destructure's alias, and inside a nested
/// recursion's own body. The values a step holds beside its base (an index, a filter's predicate,
/// a method's arguments) are no part of the path and are not searched.
fn holds_repeat_recurse(ir: &Ir, body: RegionId) -> bool {
	region_re_enters(ir, body)
}

/// [`holds_repeat_recurse`] over the value `region` yields.
fn region_re_enters(ir: &Ir, region: RegionId) -> bool {
	let yielded = ops_in(ir, region).last().and_then(|op| ir.op(op)).and_then(|data| {
		match (&data.kind, data.operands.as_slice()) {
			(OpKind::Expr(ExprOp::Yield) | OpKind::Phys(PhysOp::Yield), [value]) => Some(*value),
			(OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_), _) => None,
		}
	});
	yielded.is_some_and(|value| path_re_enters(ir, value))
}

/// [`holds_repeat_recurse`] over the chain of steps that ends in `value`.
fn path_re_enters(ir: &Ir, value: ValueId) -> bool {
	let mut current = value;
	loop {
		let Some(ValueDef::OpResult {
			op,
			index: 0,
		}) = ir.value(current).map(|data| data.def)
		else {
			return false;
		};
		let Some(data) = ir.op(op) else {
			return false;
		};
		let searched_regions = || data.regions.iter().any(|region| region_re_enters(ir, *region));
		let base = match &data.kind {
			OpKind::Expr(ExprOp::RepeatRecurse) => return true,
			OpKind::Expr(
				ExprOp::Access {
					..
				}
				| ExprOp::Index
				| ExprOp::Flatten {
					..
				}
				| ExprOp::First
				| ExprOp::Last
				| ExprOp::Each,
			) => data.operands.first(),
			// A method's receiver is the operand after the dispatch that checked it.
			OpKind::Expr(ExprOp::Method {
				..
			}) => data.operands.get(1),
			OpKind::Expr(
				ExprOp::Destructure {
					..
				}
				| ExprOp::Optional,
			) => {
				if searched_regions() {
					return true;
				}
				data.operands.first()
			}
			OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::Broadcast | LoopKind::Recurse,
				..
			}) => {
				if searched_regions() {
					return true;
				}
				data.operands.first()
			}
			OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::ElementFilter | LoopKind::GraphStep,
				..
			}) => data.operands.first(),
			// Any other op ends the path.
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return false,
		};
		let Some(base) = base else {
			return false;
		};
		current = *base;
	}
}

/// The refusal of a recursion that states the depths `min` to `max` under the limit `limit`, in
/// the order the planner checks them: the bounds, then an instruction beside a trailing body that
/// re-enters.
fn recursion_refusal(
	min: u32,
	max: Option<u32>,
	limit: u32,
	instruction_beside_repeat: bool,
) -> Option<Refusal> {
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
	instruction_beside_repeat.then_some(Refusal::RecursionInstructionPlanConflict)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::AccessMode;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::Ir;
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::ids::ValueId;
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::vm::ir_lower::emit::Emitter;
	use crate::exec::vm::ir_lower::expr::Needs;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::{ArithOp, Pipeline, Program, ROW, Source};
	use crate::expr::BinaryOperator;
	use crate::val::Value;

	/// Lowers `value` of `ir` with `seed` run first on the lowerer, as a program of one pipeline
	/// that ends the row. Answers the operand, the program and what the lowering needs.
	fn lower_seeded(
		ir: &Ir,
		value: ValueId,
		seed: impl FnOnce(&mut Lowerer<'_>),
	) -> Result<(Operand, Program, Needs), IrError> {
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, ir, crate::exec::ir::analysis::cx::builtin_registry());
		seed(&mut lowerer);
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

	/// A field of the row `name`, read in a pipeline whose rows are, or are not, objects, and the
	/// row argument it reads.
	fn field_of_a_row(b: &mut Builder, op: ExprOp) -> (ValueId, ValueId) {
		let owner = b.add_with_regions(ExprOp::If, &[]);
		let mut seen = None;
		let kind = RegionKind::Clause(ClauseRole::Where);
		b.region_with_args(owner, kind, &[TypeKnowledge::Unknown], |b, args| {
			let row = args[0];
			let read = b.add(op, &[row]);
			seen = Some((read, row));
			read
		});
		seen.expect("the body ran")
	}

	#[test]
	fn a_field_of_an_object_row_is_an_operand_of_the_row() {
		let mut b = Builder::new();
		let (field, row) = field_of_a_row(
			&mut b,
			ExprOp::Field {
				name: "a".into(),
			},
		);
		let (operand, program, _) =
			lower_seeded(&b.ir, field, |l| l.bind_row(row, true)).expect("the field lowers");
		assert_eq!(operand, Operand::Field(0));
		assert_eq!(program.fields.len(), 1);
		// The field is read from the row where it stands: nothing is emitted for it.
		assert_eq!(code_of(&program), [Instr::EndRow]);
	}

	#[test]
	fn a_field_of_any_other_row_is_a_path_step_over_the_row_as_a_value() {
		let mut b = Builder::new();
		let (field, row) = field_of_a_row(
			&mut b,
			ExprOp::Field {
				name: "a".into(),
			},
		);
		let (operand, program, _) =
			lower_seeded(&b.ir, field, |l| l.bind_row(row, false)).expect("the field lowers");
		assert_eq!(operand, Operand::Reg(2));
		assert_eq!(
			code_of(&program),
			[
				Instr::LoadRow {
					dst: 1
				},
				Instr::Access {
					src: Operand::Reg(1),
					field: 0,
					dst: 2,
					frame: None
				},
				Instr::EndRow,
			]
		);
	}

	#[test]
	fn a_computed_read_reads_the_field_the_row_holds_under_its_name() {
		use crate::exec::ir::expr::ops::ComputedReadAttrs;
		use crate::exec::ir::iface::CatalogObjectKey;
		use crate::exec::logical::footprint::CatalogKind;
		use crate::exec::vm::compile_cache::ObjectVersion;
		let mut b = Builder::new();
		let attrs = ComputedReadAttrs {
			field: "total".into(),
			def: (
				CatalogObjectKey {
					kind: CatalogKind::Field,
					ns: None,
					db: None,
					table: Some("t".into()),
					name: Some("total".into()),
				},
				ObjectVersion::Inline,
			),
		};
		let (read, row) = field_of_a_row(&mut b, ExprOp::ComputedRead(Box::new(attrs)));
		let (operand, program, _) =
			lower_seeded(&b.ir, read, |l| l.bind_row(row, true)).expect("the read lowers");
		assert_eq!(operand, Operand::Field(0));
		assert_eq!(program.fields[0].as_str(), "total");
	}

	#[test]
	fn the_identity_of_a_row_is_a_step_over_the_row_as_a_value() {
		let mut b = Builder::new();
		let (doc, row) = field_of_a_row(&mut b, ExprOp::Doc);
		let (operand, program, _) =
			lower_seeded(&b.ir, doc, |l| l.bind_row(row, true)).expect("the identity lowers");
		assert_eq!(operand, Operand::Reg(2));
		assert_eq!(
			code_of(&program),
			[
				Instr::LoadRow {
					dst: 1
				},
				Instr::Nav {
					op: NavOp::Identity,
					src: Operand::Reg(1),
					index: None,
					dst: 2
				},
				Instr::EndRow,
			]
		);
	}

	#[test]
	fn a_block_argument_nothing_bound_is_an_invariant() {
		let mut b = Builder::new();
		let (field, _) = field_of_a_row(
			&mut b,
			ExprOp::Field {
				name: "a".into(),
			},
		);
		assert!(matches!(
			lower_seeded(&b.ir, field, |_| {}),
			Err(IrError::Invariant {
				what: "a block argument nothing bound"
			})
		));
		// A bound argument is read from the operand it was given.
		let (field, row) = field_of_a_row(
			&mut b,
			ExprOp::Field {
				name: "a".into(),
			},
		);
		let (operand, program, _) = lower_seeded(&b.ir, field, |l| {
			l.bind_arg(row, Operand::Reg(ROW));
		})
		.expect("the field lowers");
		assert!(matches!(
			code_of(&program)[0],
			Instr::Access {
				src: Operand::Reg(ROW),
				..
			}
		));
		assert!(matches!(operand, Operand::Reg(_)));
	}

	#[test]
	fn first_last_and_flatten_are_steps_over_their_operand() {
		for (op, nav) in [
			(ExprOp::First, NavOp::First),
			(ExprOp::Last, NavOp::Last),
			(
				ExprOp::Flatten {
					explicit: true,
				},
				NavOp::Flatten,
			),
		] {
			let mut b = Builder::new();
			let base = b.add(
				ExprOp::FreeParam {
					name: "x".into(),
				},
				&[],
			);
			let step = b.add(op, &[base]);
			let (operand, program, needs) =
				lower_seeded(&b.ir, step, |_| {}).expect("the step lowers");
			assert!(matches!(operand, Operand::Reg(_)));
			assert!(matches!(
				code_of(&program)[1],
				Instr::Nav {
					op,
					index: None,
					..
				} if op == nav
			));
			assert_eq!(needs.reach, None, "a step that fetches nothing reaches no table");
		}
	}

	#[test]
	fn every_element_may_fetch_a_record_of_any_table() {
		let mut b = Builder::new();
		let base = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let each = b.add(ExprOp::Each, &[base]);
		let (_, program, needs) = lower_seeded(&b.ir, each, |_| {}).expect("the step lowers");
		assert!(matches!(
			code_of(&program)[1],
			Instr::Each {
				frame: None,
				..
			}
		));
		assert_eq!(needs.reach, Some(Reach::any()));
	}

	#[test]
	fn the_recursion_marker_reads_the_value_so_far() {
		let mut b = Builder::new();
		let base = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let marker = b.add(ExprOp::RepeatRecurse, &[base]);
		let (operand, program, _) = lower_seeded(&b.ir, marker, |_| {}).expect("the marker lowers");
		assert!(matches!(
			code_of(&program)[1],
			Instr::RepeatRecurse {
				src: Operand::Reg(1),
				dst: 2
			}
		));
		assert_eq!(operand, Operand::Reg(2));
	}

	/// `{ a: 1 }` with `a` set to the call `math::abs(-1)`, whose value `other` may also read.
	fn set_field_over_a_call(shared: bool) -> (Builder, ValueId) {
		let mut b = Builder::new();
		let one = b.literal(Value::from(1_i64));
		let base = b.add(
			ExprOp::Object {
				keys: Box::new(["k".to_owned()]),
			},
			&[one],
		);
		let minus = b.literal(Value::from(-1_i64));
		let value = b.call("math::abs", &[minus]);
		let set = b.add(
			ExprOp::SetField {
				field: "a".into(),
			},
			&[base, value],
		);
		if shared {
			// Another op reads the value, so the set is not the first to read it.
			let twice = b.binary(BinaryOperator::Add, value, value);
			let both = b.binary(BinaryOperator::Add, set, twice);
			return (b, both);
		}
		(b, set)
	}

	/// The index of the first instruction of `code` that `is` answers.
	fn position(code: &[Instr], is: impl Fn(&Instr) -> bool) -> usize {
		code.iter().position(is).expect("the instruction is emitted")
	}

	#[test]
	fn a_value_only_the_set_reads_is_evaluated_after_the_test_of_the_base() {
		let (b, set) = set_field_over_a_call(false);
		let (_, program, _) = lower_seeded(&b.ir, set, |_| {}).expect("the set lowers");
		let code = code_of(&program);
		let test = position(code, |i| matches!(i, Instr::JumpIfNotObject { .. }));
		let call = position(code, |i| matches!(i, Instr::Call { .. }));
		assert!(test < call, "the value runs only for an object");
		assert!(code.iter().any(|i| matches!(i, Instr::SetField { .. })));
		assert!(code.iter().any(|i| matches!(
			i,
			Instr::JumpIfNone {
				none: false,
				..
			}
		)));
		assert!(code.iter().any(|i| matches!(i, Instr::RemoveField { .. })));
	}

	#[test]
	fn a_value_another_op_reads_is_evaluated_before_the_test_of_the_base() {
		let (b, both) = set_field_over_a_call(true);
		let (_, program, _) = lower_seeded(&b.ir, both, |_| {}).expect("the set lowers");
		let code = code_of(&program);
		let test = position(code, |i| matches!(i, Instr::JumpIfNotObject { .. }));
		let call = position(code, |i| matches!(i, Instr::Call { .. }));
		assert!(call < test, "the value is the one the other op reads, evaluated once");
		assert_eq!(code.iter().filter(|i| matches!(i, Instr::Call { .. })).count(), 1);
	}

	#[test]
	fn an_optional_chain_is_its_base_when_nullish_and_else_its_region() {
		let mut b = Builder::new();
		let base = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let one = b.literal(Value::from(1_i64));
		let owner = b.add_with_regions(ExprOp::Optional, &[base]);
		b.region_with_args(owner, RegionKind::LazyOperand, &[TypeKnowledge::Unknown], |b, args| {
			b.binary(BinaryOperator::Add, args[0], one)
		});
		let chain = b.result_of(owner);
		let (operand, program, _) = lower_seeded(&b.ir, chain, |_| {}).expect("the chain lowers");
		let code = code_of(&program);
		// The base is held, the result starts as the base, and a nullish base skips the region.
		let skip = position(code, |i| {
			matches!(
				i,
				Instr::JumpIfNullish {
					nullish: true,
					..
				}
			)
		});
		let step = position(code, |i| {
			matches!(
				i,
				Instr::Arith {
					op: ArithOp::Add,
					..
				}
			)
		});
		assert!(skip < step);
		let Instr::JumpIfNullish {
			cond: Operand::Reg(held),
			..
		} = code[skip]
		else {
			panic!("the test reads the held base");
		};
		// The region reads its argument from the register that holds the base.
		assert!(matches!(
			code[step],
			Instr::Arith {
				left: Operand::Reg(read),
				..
			} if read == held
		));
		assert!(matches!(operand, Operand::Reg(_)));
	}

	#[test]
	fn a_destructure_holds_a_host_pipeline_per_aliased_field() {
		let mut b = Builder::new();
		let base = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let shape: Box<[DestructureEntry]> = Box::new([
			DestructureEntry::Field("a".into()),
			DestructureEntry::Aliased("b".into()),
			DestructureEntry::Destructure(
				"c".into(),
				Box::new([
					DestructureEntry::All("e".into()),
					DestructureEntry::Aliased("d".into()),
				]),
			),
			DestructureEntry::Rest,
		]);
		let owner = b.add_with_regions(
			ExprOp::Destructure {
				shape,
			},
			&[base],
		);
		// The aliases' regions are in depth-first order: `b`, then `d`. Each body reads a field of
		// the value destructured, which is its one block argument.
		for name in ["x", "y"] {
			b.region_with_args(
				owner,
				RegionKind::LazyOperand,
				&[TypeKnowledge::Unknown],
				|b, args| {
					b.add(
						ExprOp::Field {
							name: name.into(),
						},
						&[args[0]],
					)
				},
			);
		}
		let value = b.result_of(owner);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		let operand = lowerer.value(value).expect("the destructure lowers");
		let needs = lowerer.needs();
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		let program =
			em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("the program is built");
		assert!(matches!(operand, Operand::Reg(_)));
		assert_eq!(needs.reach, Some(Reach::any()));
		assert_eq!(program.host_code.len(), 2);
		let Instr::Destructure {
			fields,
			frame: None,
			..
		} = program.pipelines[0].code.iter().find(|i| matches!(i, Instr::Destructure { .. })).unwrap()
		else {
			panic!("the destructure is one instruction");
		};
		assert_eq!(fields.len(), 4);
		assert!(matches!(&fields[1], DestructureStep::Aliased(_, 0)));
		let DestructureStep::Nested(_, nested) = &fields[2] else {
			panic!("the third entry is a nested destructure");
		};
		assert!(matches!(&nested[1], DestructureStep::Aliased(_, 1)));
		// Each body is a pipeline of its own over one row: it reads a field of the object row and
		// emits it. Registers are its own, counted from the row, and the field pool is the
		// program's.
		for (host, name) in program.host_code.iter().zip(["x", "y"]) {
			assert!(matches!(host.source, Source::Once));
			let Pipeline {
				code,
				registers,
				..
			} = host;
			assert_eq!(*registers, 2);
			let field = program
				.fields
				.iter()
				.position(|held| held.as_str() == name)
				.expect("the body's field is in the program's pool");
			assert_eq!(
				code,
				&vec![
					Instr::Load {
						dst: 1,
						src: Operand::Field(field as u32)
					},
					Instr::Emit {
						src: 1
					},
					Instr::EndRow,
				]
			);
		}
		// Building the bodies leaves the pipeline that holds the destructure unchanged: its first
		// instruction is still the parameter load.
		assert!(matches!(program.pipelines[0].code[0], Instr::LoadParam { .. }));
	}

	/// An aliased body that reads a value the destructure's own pipeline computed is a host
	/// pipeline that computes that value itself: it holds no register of the pipeline it is
	/// called from.
	#[test]
	fn a_destructure_whose_aliased_body_reads_the_pipelines_register_is_not_yet() {
		let mut b = Builder::new();
		let base = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		// The body reads a value the op's own pipeline computed, which the host pipeline does not
		// hold.
		let outer = b.call("math::abs", &[base]);
		let owner = b.add_with_regions(
			ExprOp::Destructure {
				shape: Box::new([DestructureEntry::Aliased("b".into())]),
			},
			&[base],
		);
		b.region_with_args(owner, RegionKind::LazyOperand, &[TypeKnowledge::Unknown], |b, _| {
			b.binary(BinaryOperator::Add, outer, outer)
		});
		let value = b.result_of(owner);
		let (operand, program, _) = lower_seeded(&b.ir, value, |_| {}).expect("the value lowers");
		assert!(matches!(operand, Operand::Reg(_)));
		assert!(matches!(
			program.pipelines[0].code.iter().find(|i| matches!(i, Instr::Destructure { .. })),
			Some(Instr::Destructure { fields, .. })
				if matches!(fields.as_ref(), [DestructureStep::Aliased(_, 0)])
		));
		let [body] = program.host_code.as_slice() else {
			panic!("one aliased body: {:?}", program.host_code);
		};
		assert!(matches!(body.source, Source::Once));
		let call = body
			.code
			.iter()
			.position(|i| matches!(i, Instr::Call { .. }))
			.expect("the body calls the builtin itself");
		let sum = body
			.code
			.iter()
			.position(|i| matches!(i, Instr::Arith { .. }))
			.expect("the body adds what it computed");
		assert!(call < sum);
		assert!(matches!(body.code[body.code.len() - 2..], [Instr::Emit { .. }, Instr::EndRow]));
	}

	#[test]
	fn a_destructure_with_a_region_too_many_or_too_few_is_stale() {
		let mut b = Builder::new();
		let base = b.add(
			ExprOp::FreeParam {
				name: "x".into(),
			},
			&[],
		);
		let owner = b.add_with_regions(
			ExprOp::Destructure {
				shape: Box::new([DestructureEntry::Aliased("b".into())]),
			},
			&[base],
		);
		let value = b.result_of(owner);
		assert!(matches!(lower_seeded(&b.ir, value, |_| {}), Err(IrError::Stale(_))));
	}

	use crate::exec::ir::logical::ops::{LoopKind, StepAttrs};
	use crate::exec::ir::phys::ops::{Batch, PhysOp};
	use crate::exec::vm::ir_lower::expr::LowerLimits;
	use crate::exec::vm::ir_lower::units::{LowerCx, Needed};
	use crate::exec::vm::program::{UnitId, unit_input_name};

	/// A recursion that states `attrs` over `operands`, whose body is the value `body` builds from
	/// the element and the depth, and its result.
	fn recursion_of(
		b: &mut Builder,
		attrs: RecurseAttrs,
		operands: &[ValueId],
		body: impl FnOnce(&mut Builder, &[ValueId]) -> ValueId,
	) -> ValueId {
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
				step: Some(Box::new(StepAttrs::Recurse(attrs))),
			},
			operands,
		);
		b.open_with(
			looped,
			RegionKind::Loop,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| {
				let value = body(b, args);
				b.yield_phys(value);
			},
		);
		b.result_of(looped)
	}

	fn depths(depth: Recurse, form: RecursePathForm) -> RecurseAttrs {
		RecurseAttrs {
			depth,
			form,
			instruction: None,
		}
	}

	/// Lowers `value` for the context `cx` into one pipeline, and answers its program, what it
	/// needs and the units it asked for.
	fn lower_in(
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		value: ValueId,
	) -> Result<(Program, Needs, Vec<Needed>), IrError> {
		let mut em = Emitter::new();
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		lowerer.value(value)?;
		let needs = lowerer.needs();
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let program = em.finish_program(vec![pipeline], needs.access_mode)?;
		Ok((program, needs, cx.take_needed()))
	}

	fn recurse_of(program: &Program) -> (&RecurseSpec, Option<Operand>) {
		match code_of(program).iter().find(|i| matches!(i, Instr::Recurse { .. })) {
			Some(Instr::Recurse {
				spec,
				target,
				..
			}) => (spec.as_ref(), *target),
			_ => panic!("the recursion is emitted"),
		}
	}

	/// The refusal the program raises where its recursion is evaluated, in place of the
	/// recursion, if the lowering refused it.
	fn refused_where_reached(
		lowered: Result<(Program, Needs, Vec<Needed>), IrError>,
	) -> Option<Refusal> {
		let (program, _, _) = lowered.expect("a refused recursion still lowers");
		let code = code_of(&program);
		let refusal = code.iter().find_map(|instr| {
			if let Instr::Store(StoreInstr::Refuse {
				refusal,
			}) = instr
			{
				Some((**refusal).clone())
			} else {
				None
			}
		})?;
		assert!(
			!code.iter().any(|instr| matches!(instr, Instr::Recurse { .. })),
			"a refused recursion emits no recursion"
		);
		Some(refusal)
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

	fn friend(b: &mut Builder, element: ValueId) -> ValueId {
		b.add(
			ExprOp::Access {
				field: "friend".into(),
			},
			&[element],
		)
	}

	#[test]
	fn a_recursion_with_a_path_is_one_instruction_over_a_unit_of_its_body() {
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			depths(Recurse::Range(Some(2), Some(4)), RecursePathForm::Nested),
			&[start],
			|b, args| friend(b, args[0]),
		);
		let mut cx = LowerCx::detached();
		let (program, needs, needed) =
			lower_in(&mut cx, &b.ir, result).expect("the recursion lowers");
		let (spec, target) = recurse_of(&program);
		assert_eq!((spec.min, spec.max, spec.repeat), (2, Some(4), false));
		assert_eq!(spec.collect, RecurseCollect::Last);
		assert_eq!(spec.body, Some(UnitId(0)));
		assert!(target.is_none());
		// The body is the unit the request names, and the instruction reads the database.
		assert_eq!(needed.len(), 1);
		assert!(matches!(needed[0].request, UnitRequest::LoopBody { .. }));
		assert_eq!(needed[0].id, Some(UnitId(0)));
		assert_eq!(needs.context, ContextLevel::Database);
		assert_eq!(needs.access_mode, AccessMode::ReadOnly);
	}

	#[test]
	fn a_recursion_with_no_path_steps_by_nothing_and_has_no_unit() {
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			depths(Recurse::Fixed(3), RecursePathForm::Absent),
			&[start],
			|_, args| args[0],
		);
		let mut cx = LowerCx::detached();
		let (program, _, needed) = lower_in(&mut cx, &b.ir, result).expect("the recursion lowers");
		let (spec, _) = recurse_of(&program);
		assert_eq!((spec.min, spec.max), (3, Some(3)));
		assert!(spec.body.is_none());
		assert!(needed.is_empty());
		// A range with no lower bound starts at one.
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			depths(Recurse::Range(None, None), RecursePathForm::Absent),
			&[start],
			|_, args| args[0],
		);
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, result).expect("the recursion lowers");
		assert_eq!((recurse_of(&program).0.min, recurse_of(&program).0.max), (1, None));
	}

	#[test]
	fn an_instruction_decides_what_the_recursion_collects_and_a_shortest_path_reads_its_target() {
		for (instruction, collect) in [
			(
				RecurseInstructionShape::Collect {
					inclusive: true,
				},
				RecurseCollect::All {
					inclusive: true,
				},
			),
			(
				RecurseInstructionShape::Path {
					inclusive: false,
				},
				RecurseCollect::Paths {
					inclusive: false,
				},
			),
		] {
			let mut b = Builder::new();
			let start = b.literal(Value::from(1_i64));
			let attrs = RecurseAttrs {
				instruction: Some(instruction),
				..depths(Recurse::Fixed(2), RecursePathForm::Nested)
			};
			let result = recursion_of(&mut b, attrs, &[start], |b, args| friend(b, args[0]));
			let (program, _, _) =
				lower_in(&mut LowerCx::detached(), &b.ir, result).expect("the recursion lowers");
			assert_eq!(recurse_of(&program).0.collect, collect);
		}
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let goal = param(&mut b, "goal");
		let attrs = RecurseAttrs {
			instruction: Some(RecurseInstructionShape::Shortest {
				inclusive: true,
			}),
			..depths(Recurse::Fixed(2), RecursePathForm::Nested)
		};
		let result =
			recursion_of(&mut b, attrs.clone(), &[start, goal], |b, args| friend(b, args[0]));
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, result).expect("the recursion lowers");
		let (spec, target) = recurse_of(&program);
		assert_eq!(
			spec.collect,
			RecurseCollect::Shortest {
				inclusive: true
			}
		);
		assert!(target.is_some());
		// The target is the instruction's alone.
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(&mut b, attrs, &[start], |b, args| friend(b, args[0]));
		assert!(matches!(
			lower_in(&mut LowerCx::detached(), &b.ir, result),
			Err(IrError::Stale(_))
		));
	}

	#[test]
	fn a_body_that_re_enters_is_marked_and_cannot_stand_beside_an_instruction() {
		let reentering =
			|b: &mut Builder, args: &[ValueId]| b.add(ExprOp::RepeatRecurse, &[args[0]]);
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			depths(Recurse::Fixed(2), RecursePathForm::Trailing),
			&[start],
			reentering,
		);
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, result).expect("the recursion lowers");
		assert!(recurse_of(&program).0.repeat);
		for (form, refused) in [(RecursePathForm::Trailing, true), (RecursePathForm::Nested, false)]
		{
			let mut b = Builder::new();
			let start = b.literal(Value::from(1_i64));
			let attrs = RecurseAttrs {
				instruction: Some(RecurseInstructionShape::Collect {
					inclusive: false,
				}),
				..depths(Recurse::Fixed(2), form)
			};
			let result = recursion_of(&mut b, attrs, &[start], reentering);
			let lowered = lower_in(&mut LowerCx::detached(), &b.ir, result);
			assert_eq!(
				refused_where_reached(lowered),
				refused.then_some(Refusal::RecursionInstructionPlanConflict)
			);
		}
	}

	#[test]
	fn only_a_marker_on_the_path_counts_as_a_re_entry() {
		let with_instruction = |form| RecurseAttrs {
			instruction: Some(RecurseInstructionShape::Collect {
				inclusive: false,
			}),
			..depths(Recurse::Fixed(2), form)
		};
		// A marker in the argument of a step is no part of the path, so the body does not
		// re-enter and may stand beside an instruction.
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			with_instruction(RecursePathForm::Trailing),
			&[start],
			|b, args| {
				let marker = b.add(ExprOp::RepeatRecurse, &[args[0]]);
				b.method("array::push", args[0], &[marker])
			},
		);
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, result).expect("the recursion lowers");
		assert!(!recurse_of(&program).0.repeat);

		// The receiver of a step is on the path.
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			with_instruction(RecursePathForm::Trailing),
			&[start],
			|b, args| {
				let marker = b.add(ExprOp::RepeatRecurse, &[args[0]]);
				b.method("array::len", marker, &[])
			},
		);
		assert_eq!(
			refused_where_reached(lower_in(&mut LowerCx::detached(), &b.ir, result)),
			Some(Refusal::RecursionInstructionPlanConflict)
		);

		// So is a marker deep in the body of an optional chain on the path.
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			depths(Recurse::Fixed(2), RecursePathForm::Trailing),
			&[start],
			|b, args| {
				let owner = b.add_with_regions(ExprOp::Optional, &[args[0]]);
				b.region_with_args(
					owner,
					RegionKind::LazyOperand,
					&[TypeKnowledge::Unknown],
					|b, inner| b.add(ExprOp::RepeatRecurse, &[inner[0]]),
				);
				b.result_of(owner)
			},
		);
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, result).expect("the recursion lowers");
		assert!(recurse_of(&program).0.repeat);
	}

	#[test]
	fn a_bound_outside_the_limit_is_refused_where_reached_as_the_planner_refuses_it() {
		let bound = |found: &str, expected: &str| Refusal::InvalidBound {
			found: found.to_owned(),
			expected: expected.to_owned(),
		};
		let cases = [
			(Recurse::Fixed(0), 256, bound("0", "at least 1")),
			(Recurse::Range(Some(0), Some(3)), 256, bound("0", "at least 1")),
			(Recurse::Range(None, Some(257)), 256, bound("257", "256 at most")),
			(Recurse::Fixed(5), 4, bound("5", "4 at most")),
		];
		for (depth, limit, refusal) in cases {
			let mut b = Builder::new();
			let start = b.literal(Value::from(1_i64));
			let result = recursion_of(
				&mut b,
				depths(depth.clone(), RecursePathForm::Absent),
				&[start],
				|_, args| args[0],
			);
			let mut cx = LowerCx::detached().with_limits(LowerLimits {
				idiom_recursion_limit: limit,
				..LowerLimits::default()
			});
			assert_eq!(
				refused_where_reached(lower_in(&mut cx, &b.ir, result)),
				Some(refusal),
				"{depth:?} under {limit}"
			);
		}
	}

	#[test]
	fn a_body_that_reads_outside_its_region_takes_what_it_reads_as_inputs() {
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let outer = param(&mut b, "outer");
		let result = recursion_of(
			&mut b,
			depths(Recurse::Fixed(2), RecursePathForm::Nested),
			&[start],
			|b, args| {
				let once = b.binary(BinaryOperator::Add, args[0], outer);
				b.binary(BinaryOperator::Add, once, outer)
			},
		);
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, result).expect("the recursion lowers");
		assert_eq!(needed.len(), 1);
		let (spec, _) = recurse_of(&program);
		// The value read twice is one input, named by its place, evaluated before the recursion.
		assert_eq!(spec.inputs.len(), 1);
		assert_eq!(spec.inputs[0].name, unit_input_name(0));
		let code = code_of(&program);
		let Operand::Reg(input) = spec.inputs[0].value else {
			panic!("the input is read into a register");
		};
		let read = code
			.iter()
			.position(|i| matches!(i, Instr::LoadParam { dst, .. } if *dst == input))
			.expect("the outer value is read");
		let recurse = code
			.iter()
			.position(|i| matches!(i, Instr::Recurse { .. }))
			.expect("the recursion is emitted");
		assert!(read < recurse);
		// A literal defined outside is copied into the unit, and is no input.
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let one = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			depths(Recurse::Fixed(2), RecursePathForm::Nested),
			&[start],
			|b, args| b.binary(BinaryOperator::Add, args[0], one),
		);
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, result).expect("the recursion lowers");
		assert!(recurse_of(&program).0.inputs.is_empty());
	}

	#[test]
	fn a_recursion_through_the_direct_entry_lowers_and_is_not_in_the_folders_subset() {
		let mut b = Builder::new();
		let start = b.literal(Value::from(1_i64));
		let result = recursion_of(
			&mut b,
			depths(Recurse::Fixed(2), RecursePathForm::Nested),
			&[start],
			|b, args| friend(b, args[0]),
		);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.value(result),
			Err(IrError::Invariant {
				what: "ir_lower::fold: phys.loop"
			})
		));
		b.add_phys(PhysOp::Eval, &[result]);
		b.physical();
		let mut cx = LowerCx::detached();
		let lowered =
			crate::exec::vm::ir_lower::dispatch::lower_with(&mut cx, &b.ir, &FactCache::new())
				.expect("the recursion lowers");
		assert_eq!(lowered.program.required_context, ContextLevel::Database);
		assert_eq!(lowered.needs.len(), 1);
	}
}
