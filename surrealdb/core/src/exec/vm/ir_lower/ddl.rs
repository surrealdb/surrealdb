//! Lowering of schema statements from the physical dialect.
//!
//! A schema statement is a run of `phys.ddl_guard` ops, the operand expressions it evaluates, and
//! one `phys.define`, `phys.remove`, `phys.alter` or `phys.rebuild` op that names those operands
//! by slot. [`lower_ddl`] lowers the statement of one such op into store instructions in the
//! order the statement's body takes its steps, and ends on the row that answers it, `NONE`.
//!
//! # Order
//!
//! The binder lays a statement's ops out in the order its guards and operands bind in. Lowering
//! does not follow that layout: each form's emitter reads an operand at the step the body
//! evaluates it, so an operand a definition's existence branch skips (an evaluated `COMMENT` under
//! `IF NOT EXISTS`) runs on the path that reaches it and on no other, and the two authority guards
//! are emitted where the body makes the checks. An operand is lowered where it is first read, and
//! each operand is read exactly once on every path through the statement. A statement holds the
//! guards of its own run only: the guards that stand between the statement before it and its own
//! op.
//!
//! # What the statement carries
//!
//! A definition is the template its op carries, a constant of the stored type with the fields an
//! operand fills left as holes; lowering sets a hole from the operand's register when the operand
//! is evaluated, and leaves the template's own value when the statement wrote it. Lowering reads
//! no catalog: every check against the stored schema is an instruction that runs with the
//! statement.
//!
//! A name an operand holds as a string literal is written, and emits nothing. Any other operand is
//! evaluated, and read as a name or a comment by the instruction that coerces it. An evaluated
//! operand whose value a second evaluation could change, or that the speculative run of a block
//! may not evaluate, is preceded by a run-only marker.
//!
//! # Credentials
//!
//! A user's hash, session code and SCRAM verifier, and the keys and URLs an access method's
//! statement writes, are set into the constant of the definition the statement writes, from the
//! secrets the statement carries; they appear in no other instruction operand and in no printed
//! form.
//!
//! # Forms
//!
//! Every schema statement form has an emitter here.
#![deny(clippy::wildcard_enum_match_arm)]

mod access;
mod api;
mod field;
mod index;
mod table;
mod user;

use std::collections::{HashMap, VecDeque};

use super::super::codec::TypeId;
use super::super::codec::key::RouteId;
use super::super::program::{
	ClauseConversion, ClauseObject, CompareOp, IdSequence, ImplicitTable, Instr, Label, NameClause,
	Operand, Reg, Service, StatementError, StoreInstr, StoreRefusal,
};
use super::emit::Emitter;
use super::expr::{Lowerer, Needs};
use super::units::LowerCx;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::statements::{
	AlterObject, AlterTemplate, DdlGuard, DefineFunction, DefineObject, OperandSlot, RegionSlot,
	RemoveObject,
};
use crate::exec::ir::logical::ddl as statement;
use crate::exec::ir::logical::ops::{AlterAttrs, DefineAttrs, LoopKind, RebuildAttrs, RemoveAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::Base;
use crate::expr::module::ModuleExecutable;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::remove::SqlTypeObject;
use crate::iam::{Action, ConfigKind, ResourceKind};
use crate::val::Value;

/// The ids a speculative run of a block drew for its schema statements, and the ids the program
/// takes of them.
///
/// `reserving` holds the draws the statement being lowered may take, as `(draw, value)` pairs, and
/// `taken` the values of every draw the program has taken, in the order it took them. A draw that
/// takes a reservation answers its index in `taken`, which is the index into
/// `Program::reservations`. A table the statement defines implicitly, whose id, stamp and
/// catalog version are all reserved draws, is noted in `implicit_tables` for the program's writes
/// to share.
#[derive(Debug, Default)]
pub(crate) struct Reservations {
	pub(super) reserving: Vec<(u16, u128)>,
	pub(super) taken: Vec<u128>,
	/// The tables the statements lowered so far define implicitly with reserved draws, in the
	/// order they were lowered: the program's `implicit_tables`.
	pub(super) implicit_tables: Vec<ImplicitTable>,
}

/// The ops of `ir` that end a schema statement: the op a run of guards stands in front of.
fn is_statement_op(kind: &OpKind) -> bool {
	matches!(
		kind,
		OpKind::Phys(PhysOp::Define(_) | PhysOp::Remove(_) | PhysOp::Alter(_) | PhysOp::Rebuild(_))
	)
}

/// The ops that end a statement the binder refused: the raise that stands in place of the
/// statement's own op. The guards in front of one belong to it alone.
fn ends_statement(kind: &OpKind) -> bool {
	matches!(kind, OpKind::Expr(ExprOp::Raise(_) | ExprOp::Throw))
}

/// The `phys.define`, `phys.remove`, `phys.alter` or `phys.rebuild` op that lowers the
/// `phys.ddl_guard` `guard`: the first such op after it in its region, unless a raise comes
/// first.
///
/// A guard with no statement op is the guard of a statement the binder refused; the driver lowers
/// it with [`lower_guard`] and lowers the refusal after it.
pub(super) fn statement_op(ir: &Ir, guard: OpId) -> Option<OpId> {
	let region = ir.enclosing_region(guard);
	for op in ops_in(ir, region).skip_while(|op| *op != guard).skip(1) {
		let kind = &ir.op(op)?.kind;
		if is_statement_op(kind) {
			return Some(op);
		}
		if ends_statement(kind) {
			return None;
		}
	}
	None
}

/// Emits the check `guard` states: the refusal to write from a frame that may not write, or the
/// authority check of the statement.
pub(super) fn lower_guard(em: &mut Emitter, guard: &DdlGuard) -> Result<(), IrError> {
	match guard {
		DdlGuard::RefuseNoWrite => {
			em.emit(Instr::Store(StoreInstr::RefuseNoWrite));
		}
		DdlGuard::Allow {
			action,
			resource,
			base,
		} => {
			em.emit(Instr::Store(StoreInstr::Allow {
				action: *action,
				resource: resource.clone(),
				base: *base,
			}));
		}
	}
	Ok(())
}

/// The guards of the statement whose op is `op`: the `phys.ddl_guard` ops in its region that stand
/// between the previous statement's op, or the raise that ended a refused statement, and `op`, in
/// order.
fn guards_of(ir: &Ir, op: OpId) -> VecDeque<DdlGuard> {
	let mut guards = VecDeque::new();
	for held in ops_in(ir, ir.enclosing_region(op)) {
		if held == op {
			break;
		}
		let Some(data) = ir.op(held) else {
			continue;
		};
		match &data.kind {
			OpKind::Phys(PhysOp::DdlGuard {
				guard,
			}) => guards.push_back(guard.clone()),
			kind if is_statement_op(kind) || ends_statement(kind) => guards.clear(),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {}
		}
	}
	guards
}

/// The op that ends the schema statement `region` holds, `None` for a region with no such op: a
/// region that holds no statement, or the one a refused statement stands in as a raise.
///
/// A schema statement's region holds its guards, the expressions of its operands, its one op and
/// the yield of its value. A second statement op is [`IrError::Stale`], and any other op beside
/// one is [`IrError::Invariant`]: the binder puts nothing else there, and nothing would lower it.
pub(super) fn schema_op(ir: &Ir, region: RegionId) -> Result<Option<OpId>, IrError> {
	let mut found = None;
	let mut others = false;
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::ddl: op"))?;
		match &data.kind {
			kind if is_statement_op(kind) => {
				if found.replace(op).is_some() {
					return Err(IrError::Stale(
						"ir_lower::ddl: a region of more than one schema statement",
					));
				}
			}
			OpKind::Expr(_)
			| OpKind::Phys(
				PhysOp::DdlGuard {
					..
				}
				| PhysOp::Yield,
			) => {}
			// A loop read as a value, or the pipeline the physical phase wraps one in, is the
			// expression of an operand, as a closure's application is in
			// `DEFINE PARAM $p VALUE array::fold(...)`.
			OpKind::Phys(PhysOp::Loop {
				kind:
					LoopKind::ElementFilter
					| LoopKind::Broadcast
					| LoopKind::Closure
					| LoopKind::Recurse
					| LoopKind::GraphStep,
				..
			}) => {}
			OpKind::Phys(PhysOp::Pipeline(_)) if super::pipe::is_value_pipeline(ir, op) => {}
			// A `{ ... }` block read as a value is the expression of an operand too, as in
			// `DEFINE TABLE t COMMENT { 'x' }`: the operand's read lowers it in place or runs its
			// region as a unit of its own.
			OpKind::Phys(PhysOp::Block) => {}
			OpKind::Phys(_) | OpKind::Logical(_) => others = true,
		}
	}
	match (found, others) {
		(Some(_), true) => Err(IrError::Invariant {
			what: "ir_lower::ddl: an op beside a schema statement",
		}),
		(found, _) => Ok(found),
	}
}

/// Lowers the schema statement whose op is `op` into `em`, and answers what the instructions need
/// of the program that holds them. The expressions the statement evaluates lower in `cx`: its
/// registry and limits, and the units they run are asked of its table.
///
/// The code is one pipeline body: it ends on the row that answers the statement (`NONE`), and the
/// caller finishes the pipeline over `Source::Once`, marks the program as one that writes and
/// takes `reservations` as the draws of the statement. The statement's id draws are numbered from
/// zero.
///
/// Answers [`IrError::Stale`] for a statement op whose operands or attributes do not stand as its
/// form states.
pub(super) fn lower_ddl(
	em: &mut Emitter,
	ir: &Ir,
	facts: &FactCache,
	cx: &mut LowerCx<'_>,
	op: OpId,
	reservations: &mut Reservations,
) -> Result<Needs, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::ddl: op"))?;
	let mut lowering = Ddl {
		em,
		ir,
		facts,
		cx,
		operands: data.operands.to_vec(),
		regions: data.regions.to_vec(),
		guards: guards_of(ir, op),
		needs: Needs {
			context: crate::exec::ContextLevel::Root,
			keeps_row_order: false,
			reach: None,
			access_mode: crate::exec::AccessMode::ReadOnly,
		},
		id_draws: 0,
		reservations,
		read: HashMap::new(),
	};
	match &data.kind {
		OpKind::Phys(PhysOp::Define(attrs)) => lowering.define(attrs)?,
		OpKind::Phys(PhysOp::Remove(attrs)) => lowering.remove(attrs)?,
		OpKind::Phys(PhysOp::Alter(attrs)) => lowering.alter(attrs)?,
		OpKind::Phys(PhysOp::Rebuild(attrs)) => lowering.rebuild(attrs)?,
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
			return Err(IrError::Stale("ir_lower::ddl: not a schema statement op"));
		}
	}
	// Every path through a schema statement answers it: the program ends on the row it answers,
	// never past its last step.
	if !matches!(
		lowering.em.code().last(),
		Some(Instr::EndRow | Instr::EmitEnd { .. } | Instr::StopPipeline)
	) {
		return Err(IrError::Stale("ir_lower::ddl: a statement whose program does not answer it"));
	}
	if !lowering.guards.is_empty() {
		return Err(IrError::Stale("ir_lower::ddl: a guard the statement's body never makes"));
	}
	if lowering.read.len() != lowering.operands.len() {
		return Err(IrError::Stale("ir_lower::ddl: an operand the statement's body never reads"));
	}
	Ok(lowering.needs)
}

/// A name a schema statement is written with, as the program reads it.
struct Name {
	/// The name, when it is written rather than evaluated.
	written: Option<String>,
	/// A constant holding the written name, or the register the evaluated name is read into.
	operand: Operand,
}

/// A `COMMENT` clause, as the program reads it.
struct Comment {
	/// Whether the comment is evaluated rather than written.
	evaluated: bool,
	/// A constant holding the written comment, or the register the evaluated comment is read
	/// into.
	operand: Operand,
}

/// The name of the definition a database-level removal deletes.
enum DbName {
	/// A name fixed by the statement's syntax: the key's name, and the name the missing error
	/// shows.
	Written(String, String),
	/// A name the statement resolves as [`Ddl::name`] does: the operand slot, and the clause a
	/// refusal names.
	Named(OperandSlot, NameClause),
}

/// The state of the lowering of one schema statement.
struct Ddl<'a, 'c> {
	em: &'a mut Emitter,
	ir: &'a Ir,
	facts: &'a FactCache,
	/// The context the statement's expressions are lowered in: its registry and limits.
	cx: &'a mut LowerCx<'c>,
	/// The operands of the statement's op, by slot.
	operands: Vec<ValueId>,
	/// The regions of the statement's op, by slot.
	regions: Vec<RegionId>,
	/// The guards the body has yet to make, in the order the binder bound them.
	guards: VecDeque<DdlGuard>,
	needs: Needs,
	/// The draws of ids and stamps emitted so far, which number the next.
	id_draws: u16,
	reservations: &'a mut Reservations,
	/// Where each operand read so far is read from, by slot.
	read: HashMap<OperandSlot, Operand>,
}

impl Ddl<'_, '_> {
	// -----------------------------------------------------------------------------------------
	// Emission
	// -----------------------------------------------------------------------------------------

	fn emit(&mut self, instr: Instr) -> Label {
		self.em.emit(instr)
	}

	fn reg(&mut self) -> Result<Reg, IrError> {
		self.em.reg()
	}

	/// A label to bind later, for a jump placed before its target.
	fn label(&mut self) -> Result<Label, IrError> {
		self.em.label()
	}

	fn bind(&mut self, label: Label) -> Result<(), IrError> {
		self.em.bind(label)
	}

	/// Emits a store instruction. A draw of an id, a uuid or a stamp is numbered, and takes the
	/// reservation the speculative run drew for its number, when it drew one.
	fn store(&mut self, mut instr: StoreInstr) -> Label {
		if let StoreInstr::NextId {
			draw,
			reserved,
			..
		}
		| StoreInstr::NextIndexId {
			draw,
			reserved,
			..
		}
		| StoreInstr::NewUuid {
			draw,
			reserved,
			..
		}
		| StoreInstr::StampCatalog {
			draw,
			reserved,
			..
		} = &mut instr
		{
			*draw = self.id_draws;
			self.id_draws += 1;
			if let Some(&(_, id)) = self.reservations.reserving.iter().find(|(at, _)| *at == *draw)
			{
				*reserved = Some(self.reservations.taken.len() as u32);
				self.reservations.taken.push(id);
			}
		}
		self.em.emit(Instr::Store(instr))
	}

	/// An operand denoting `value`.
	fn constant(&mut self, value: Value) -> Result<Operand, IrError> {
		Ok(Operand::Const(self.em.constant(value)?))
	}

	/// A register holding `value`.
	fn load(&mut self, value: Operand) -> Result<Reg, IrError> {
		let dst = self.reg()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		Ok(dst)
	}

	/// A register holding a new object with `fields`, set in order.
	fn object(&mut self, fields: &[(&str, Operand)]) -> Result<Reg, IrError> {
		let dst = self.reg()?;
		self.emit(Instr::NewObject {
			dst,
		});
		for (name, value) in fields {
			self.set_field(dst, name, *value)?;
		}
		Ok(dst)
	}

	/// Sets the value at `path`, a field name per step, of the object in `object`.
	fn set_path(&mut self, object: Reg, path: &[&str], value: Operand) -> Result<(), IrError> {
		let path = self.em.path(FieldPath(
			path.iter().map(|name| FieldPathPart::Field((*name).to_owned())).collect(),
		))?;
		self.emit(Instr::SetField {
			object,
			path,
			value,
		});
		Ok(())
	}

	/// Sets the field `name` of the object in `object` to `value`.
	fn set_field(&mut self, object: Reg, name: &str, value: Operand) -> Result<(), IrError> {
		let path = self.em.path(FieldPath::field(name))?;
		self.emit(Instr::SetField {
			object,
			path,
			value,
		});
		Ok(())
	}

	/// `dst` = the field `name` of the object in `src`.
	fn get_field(&mut self, src: Reg, name: &str, dst: Reg) -> Result<(), IrError> {
		let name = self.em.field(name)?;
		self.store(StoreInstr::GetField {
			src,
			name,
			dst,
		});
		Ok(())
	}

	/// Ends the program answering `NONE`.
	fn answer_none(&mut self) -> Result<(), IrError> {
		let none = self.constant(Value::None)?;
		let out = self.load(none)?;
		self.emit(Instr::Emit {
			src: out,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	/// Raises `error` naming the value in `name` unless `value` holds one.
	fn expect(&mut self, value: Reg, error: StatementError, name: Reg) -> Result<(), IrError> {
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(value),
			truthy: true,
			target: present,
		});
		self.store(StoreInstr::Raise {
			error,
			arg: name,
		});
		self.bind(present)
	}

	/// Stops answering `NONE` when `IF EXISTS` and the definition in `found` is missing; raises
	/// `error` naming `name` when it is missing otherwise.
	fn must_exist(
		&mut self,
		if_exists: bool,
		found: Reg,
		error: StatementError,
		name: Reg,
	) -> Result<(), IrError> {
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(found),
			truthy: true,
			target: present,
		});
		if if_exists {
			self.answer_none()?;
		} else {
			self.store(StoreInstr::Raise {
				error,
				arg: name,
			});
		}
		self.bind(present)
	}

	/// The existence branch over the definition in `existing`: raises `error` naming `name` unless
	/// the statement runs as an import, stops answering `NONE` under `IF NOT EXISTS`, or
	/// continues under `OVERWRITE`. Falls through when the definition exists and the statement
	/// continues; answers the label a statement over a missing definition continues at, which the
	/// caller binds.
	fn exists(
		&mut self,
		mode: &DefineKind,
		existing: Reg,
		error: StatementError,
		name: Reg,
	) -> Result<Label, IrError> {
		self.exists_with(
			mode,
			existing,
			|l| {
				l.store(StoreInstr::Raise {
					error,
					arg: name,
				});
				Ok(())
			},
			|_| Ok(()),
		)
	}

	/// [`Self::exists`] for a definition whose refusal `refuse` emits, and whose replacement
	/// `overwrite` prepares: `refuse` ends the path it emits for, and `overwrite` runs on the path
	/// that continues under `OVERWRITE`.
	fn exists_with(
		&mut self,
		mode: &DefineKind,
		existing: Reg,
		refuse: impl FnOnce(&mut Self) -> Result<(), IrError>,
		overwrite: impl FnOnce(&mut Self) -> Result<(), IrError>,
	) -> Result<Label, IrError> {
		let missing = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(existing),
			truthy: false,
			target: missing,
		});
		match mode {
			DefineKind::Default => {
				let import = self.label()?;
				self.store(StoreInstr::JumpIfImport {
					target: import,
				});
				refuse(self)?;
				self.bind(import)?;
			}
			DefineKind::Overwrite => overwrite(self)?,
			DefineKind::IfNotExists => self.answer_none()?,
		}
		Ok(missing)
	}

	/// Runs `body` over each element of the array in `list`, the element in the register `body`
	/// is given.
	fn each(
		&mut self,
		list: Reg,
		body: impl FnOnce(&mut Self, Reg) -> Result<(), IrError>,
	) -> Result<(), IrError> {
		let zero = self.constant(Value::from(0i64))?;
		let at = self.load(zero)?;
		let item = self.reg()?;
		let head = self.label()?;
		let done = self.label()?;
		self.bind(head)?;
		self.emit(Instr::Next {
			list,
			at,
			dst: item,
			done,
		});
		body(self, item)?;
		self.emit(Instr::Jump {
			target: head,
		});
		self.bind(done)
	}

	/// A register holding the values stored beneath the `route` bound over `fields`, decoded as
	/// `ty`.
	fn scan(
		&mut self,
		route: &'static str,
		fields: &[(&str, Operand)],
		ty: &'static str,
	) -> Result<Reg, IrError> {
		let key = self.object(fields)?;
		let dst = self.reg()?;
		self.store(StoreInstr::Scan {
			route: RouteId::named(route),
			key,
			ty: TypeId::named(ty),
			dst,
			unread: None,
		});
		Ok(dst)
	}

	/// Deletes the key, and under `EXPUNGE` every earlier version of it.
	fn delete(&mut self, route: &'static str, key: Reg, expunge: bool) {
		self.store(match expunge {
			true => StoreInstr::Clear {
				route: RouteId::named(route),
				key,
			},
			false => StoreInstr::Delete {
				route: RouteId::named(route),
				key,
			},
		});
	}

	/// Deletes every key beneath the bound, and under `EXPUNGE` every earlier version of each.
	fn delete_range(&mut self, route: &'static str, key: Reg, expunge: bool) {
		self.store(match expunge {
			true => StoreInstr::ClearRange {
				route: RouteId::named(route),
				key,
			},
			false => StoreInstr::DeleteRange {
				route: RouteId::named(route),
				key,
			},
		});
	}

	/// Skips what `body` emits when the datastore runs without `service`.
	fn with_service(
		&mut self,
		service: Service,
		body: impl FnOnce(&mut Self) -> Result<(), IrError>,
	) -> Result<(), IrError> {
		let absent = self.label()?;
		self.store(StoreInstr::JumpIfNoService {
			service,
			target: absent,
		});
		body(self)?;
		self.bind(absent)
	}

	// -----------------------------------------------------------------------------------------
	// Guards
	// -----------------------------------------------------------------------------------------

	/// Emits the refusal to write from a frame that may not write, which the statement's guards
	/// hold first.
	fn refuse_no_write(&mut self) -> Result<(), IrError> {
		match self.guards.pop_front() {
			Some(DdlGuard::RefuseNoWrite) => lower_guard(self.em, &DdlGuard::RefuseNoWrite),
			Some(DdlGuard::Allow {
				..
			})
			| None => Err(IrError::Stale("ir_lower::ddl: the no-write guard is missing")),
		}
	}

	/// Emits the authority check, which the statement's guards hold after the no-write refusal.
	fn allow(&mut self) -> Result<(), IrError> {
		match self.guards.pop_front() {
			Some(
				guard @ DdlGuard::Allow {
					..
				},
			) => lower_guard(self.em, &guard),
			Some(DdlGuard::RefuseNoWrite) | None => {
				Err(IrError::Stale("ir_lower::ddl: the authority guard is missing"))
			}
		}
	}

	/// Emits both guards, as the body of a statement that checks before it reads anything does.
	fn guard(&mut self) -> Result<(), IrError> {
		self.refuse_no_write()?;
		self.allow()
	}

	// -----------------------------------------------------------------------------------------
	// Operands
	// -----------------------------------------------------------------------------------------

	/// The value operand `slot` holds.
	fn value_of(&self, slot: OperandSlot) -> Result<ValueId, IrError> {
		self.operands
			.get(usize::from(slot))
			.copied()
			.ok_or(IrError::Stale("ir_lower::ddl: operand slot"))
	}

	/// The constant a literal operand holds.
	fn literal(&self, slot: OperandSlot) -> Result<Option<&Value>, IrError> {
		statement::literal_operand(self.ir, &self.operands, slot)
	}

	/// Whether a second evaluation of the operand could answer differently, or the speculative
	/// run of a block may not evaluate it: the effects of the expression that defines it, or a
	/// node whose evaluation a repeat or a skip would change.
	fn real_run_only(&self, slot: OperandSlot) -> Result<bool, IrError> {
		self.value_real_run_only(self.value_of(slot)?)
	}

	/// [`Self::real_run_only`] of the value `value`.
	fn value_real_run_only(&self, value: ValueId) -> Result<bool, IrError> {
		let data = self.ir.value(value).ok_or(IrError::Stale("ir_lower::ddl: value"))?;
		let ValueDef::OpResult {
			op,
			..
		} = data.def
		else {
			return Ok(true);
		};
		let cx = structural_cx(self.cx.registry(), self.ir);
		let set = match self.facts.get(op, self.ir.generation(op), &cx.memo_key()) {
			Some(facts) => {
				#[cfg(test)]
				super::super::compile::LOWERING_FACT_HITS.with(|hits| hits.set(hits.get() + 1));
				facts.effects.set
			}
			None => effects::summary_with(self.ir, &mut FactCache::new(), &cx, op).set,
		};
		Ok(!super::super::speculate::admits(set) || self.holds_opaque_node(op))
	}

	/// Whether the expression defined by `op`, or an expression it reads, is a node whose repeat
	/// or skip changes what the statement does: a method, an applied value, a closure, control
	/// flow or a nested statement.
	fn holds_opaque_node(&self, op: OpId) -> bool {
		let Some(data) = self.ir.op(op) else {
			return true;
		};
		let opaque = match &data.kind {
			OpKind::Expr(
				ExprOp::Method {
					..
				}
				| ExprOp::Invoke
				| ExprOp::ClosureCreate(_)
				| ExprOp::If
				| ExprOp::Throw
				| ExprOp::Subquery(_)
				| ExprOp::Raise(_),
			) => true,
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
		};
		opaque
			|| data.operands.iter().any(|value| {
				match self.ir.value(*value).map(|value| value.def) {
					Some(ValueDef::OpResult {
						op,
						..
					}) => self.holds_opaque_node(op),
					Some(ValueDef::BlockArg {
						..
					}) => false,
					None => true,
				}
			})
	}

	/// The region of the statement's op at `slot`.
	fn region_slot(&self, slot: RegionSlot) -> Result<RegionId, IrError> {
		self.regions
			.get(usize::from(slot))
			.copied()
			.ok_or(IrError::Stale("ir_lower::ddl: region slot"))
	}

	/// Where operand `slot` is read from, lowering the expression that defines it where it is
	/// first read. An operand is read once.
	fn read(&mut self, slot: OperandSlot) -> Result<Operand, IrError> {
		self.read_labelled(slot, false)
	}

	/// [`Self::read`], with the method calls of the expression naming their argument errors as
	/// code a write reaches does where `writing` holds.
	fn read_labelled(&mut self, slot: OperandSlot, writing: bool) -> Result<Operand, IrError> {
		if self.read.contains_key(&slot) {
			return Err(IrError::Stale("ir_lower::ddl: an operand read twice"));
		}
		let value = self.value_of(slot)?;
		let mut lowerer = Lowerer::in_context(&mut *self.em, self.ir, &mut *self.cx);
		lowerer.label_as_writing(writing);
		let operand = lowerer.value(value)?;
		let needs = lowerer.needs();
		self.needs = self.needs.merged(&needs);
		self.read.insert(slot, operand);
		Ok(operand)
	}

	/// A register holding what the operand `slot` evaluates to in the statement's frame. A value a
	/// second evaluation may answer differently is evaluated by the statement's run alone. Its
	/// method calls name their argument errors as code a write reaches does.
	fn evaluated(&mut self, slot: OperandSlot) -> Result<Reg, IrError> {
		if self.real_run_only(slot)? {
			self.store(StoreInstr::RealRunOnly);
		}
		match self.read_labelled(slot, true)? {
			Operand::Reg(reg) => Ok(reg),
			other @ (Operand::Const(_) | Operand::Field(_)) => self.load(other),
		}
	}

	/// The name operand `slot` gives. A written name emits nothing. Anything else is evaluated here
	/// and read as a string, a value of another kind raising a refusal that names `into`; so a
	/// caller reads it at the point the statement's body resolves it, which fixes which of its
	/// failures, and of the checks around it, is raised first.
	fn name(&mut self, slot: OperandSlot, into: NameClause) -> Result<Name, IrError> {
		if let Some(Value::String(written)) = self.literal(slot)? {
			let written = written.as_str().to_owned();
			let operand = self.constant(Value::String(written.as_str().into()))?;
			self.read.insert(slot, operand);
			return Ok(Name {
				written: Some(written),
				operand,
			});
		}
		let value = self.evaluated(slot)?;
		self.store(StoreInstr::CoerceName {
			src: value,
			dst: value,
			into,
			optional: false,
		});
		Ok(Name {
			written: None,
			operand: Operand::Reg(value),
		})
	}

	/// The comment operand `slot` gives, a string or `NONE`. A written comment emits nothing.
	/// Anything else is evaluated here and cast as the body casts it; so a caller reads it at the
	/// point the statement's body evaluates it, for most definitions after their existence check.
	fn comment(&mut self, slot: OperandSlot) -> Result<Comment, IrError> {
		if let Some(written @ (Value::None | Value::String(_))) = self.literal(slot)? {
			let written = written.clone();
			let operand = self.constant(written)?;
			self.read.insert(slot, operand);
			return Ok(Comment {
				evaluated: false,
				operand,
			});
		}
		let value = self.evaluated(slot)?;
		self.store(StoreInstr::CastComment {
			src: value,
			dst: value,
		});
		Ok(Comment {
			evaluated: true,
			operand: Operand::Reg(value),
		})
	}

	/// Sets the `comment` of the definition in `definition`, whose template holds the written
	/// comment, to the comment, when it was evaluated.
	fn set_comment(&mut self, definition: Reg, comment: &Comment) -> Result<(), IrError> {
		if comment.evaluated {
			self.set_field(definition, "comment", comment.operand)?;
		}
		Ok(())
	}

	/// Sets `field` of the definition in `definition`, whose template holds the written name, to
	/// the name, when it was evaluated.
	fn set_name(&mut self, definition: Reg, field: &str, name: &Name) -> Result<(), IrError> {
		if name.written.is_none() {
			self.set_field(definition, field, name.operand)?;
		}
		Ok(())
	}

	/// Marks the operand `slot` as read from `operand`: a value the statement writes, which the
	/// definition's template holds and the body reads at no instruction.
	fn read_as(&mut self, slot: OperandSlot, operand: Operand) -> Result<(), IrError> {
		self.value_of(slot)?;
		match self.read.insert(slot, operand) {
			None => Ok(()),
			Some(_) => Err(IrError::Stale("ir_lower::ddl: an operand read twice")),
		}
	}

	/// A register holding the operand `slot` converted as the body converts `clause`.
	fn converted(&mut self, slot: OperandSlot, clause: ClauseConversion) -> Result<Reg, IrError> {
		let value = self.evaluated(slot)?;
		self.store(StoreInstr::Convert {
			src: value,
			dst: value,
			clause,
		});
		Ok(value)
	}

	/// A duration clause, cast to an optional duration: a constant holding the value the statement
	/// writes, or the register the program converts the evaluated clause into. With `cast_written`
	/// a written value of any kind that casts to one is a written value, and one that does not is
	/// converted when the statement runs, so that the conversion raises its error there; without
	/// it only a written duration or `NONE` is.
	///
	/// Answers the operand and whether it was evaluated.
	fn duration(
		&mut self,
		slot: OperandSlot,
		cast_written: bool,
	) -> Result<(Operand, bool), IrError> {
		let written = match self.literal(slot)? {
			Some(value @ (Value::Duration(_) | Value::None)) => Some(value.clone()),
			Some(value) if cast_written => {
				match value.clone().cast_to::<Option<crate::val::Duration>>() {
					Ok(Some(duration)) => Some(Value::Duration(duration)),
					Ok(None) => Some(Value::None),
					Err(_) => None,
				}
			}
			Some(_) | None => None,
		};
		if let Some(written) = written {
			let operand = self.constant(written)?;
			self.read_as(slot, operand)?;
			return Ok((operand, false));
		}
		let value = self.converted(slot, ClauseConversion::Duration)?;
		Ok((Operand::Reg(value), true))
	}

	/// Sets the definition's auth limit to the session's.
	fn session_auth_limit(&mut self, definition: Reg) -> Result<(), IrError> {
		let limit = self.reg()?;
		self.store(StoreInstr::SessionAuthLimit {
			dst: limit,
		});
		self.set_field(definition, "auth_limit", Operand::Reg(limit))
	}

	// -----------------------------------------------------------------------------------------
	// The session's catalog scope
	// -----------------------------------------------------------------------------------------

	/// The id of the session's namespace, in a register, creating the namespace when it does not
	/// exist yet (`get_or_add_ns`).
	fn namespace_id_creating(&mut self) -> Result<Reg, IrError> {
		let ns_name = self.reg()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(namespace),
			truthy: true,
			target: present,
		});
		// Reached only when the namespace does not exist and is about to be
		// created to hold the definition. Creating it needs the same authority
		// as an explicit DEFINE NAMESPACE (Edit on Namespace at root), so a
		// database-level principal whose namespace was removed cannot recreate
		// it through an ordinary DDL statement (GHSA-3287-hpm5-3f8r).
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Namespace,
			base: Base::Root,
		});
		let id = self.reg()?;
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Namespace,
			ns: None,
			db: None,
			dst: id,
			draw: 0,
			reserved: None,
		});
		let none = self.constant(Value::None)?;
		let created = self.object(&[
			("namespace_id", Operand::Reg(id)),
			("name", Operand::Reg(ns_name)),
			("comment", none),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			value: created,
		});
		self.emit(Instr::Load {
			dst: namespace,
			src: Operand::Reg(created),
		});
		self.bind(present)?;
		self.get_field(namespace, "namespace_id", id)?;
		Ok(id)
	}

	/// The ids of the session's namespace and database, creating whichever does not exist yet
	/// (`ensure_ns_db`).
	fn ids_creating(&mut self) -> Result<(Reg, Reg), IrError> {
		let ns = self.namespace_id_creating()?;
		let db_name = self.reg()?;
		self.store(StoreInstr::SessionDb {
			dst: db_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db_name))])?;
		let database = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(database),
			truthy: true,
			target: present,
		});
		// Reached only when the database does not exist and is about to be
		// created to hold the definition. Creating it needs the same authority
		// as an explicit DEFINE DATABASE (Edit on Database at the namespace),
		// matching the USE path (GHSA-3287-hpm5-3f8r).
		self.store(StoreInstr::Allow {
			action: Action::Edit,
			resource: ResourceKind::Database,
			base: Base::Ns,
		});
		let id = self.reg()?;
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Database,
			ns: Some(ns),
			db: None,
			dst: id,
			draw: 0,
			reserved: None,
		});
		let level = self.reg()?;
		self.store(StoreInstr::NewDatabaseCompatibility {
			ns,
			db: id,
			dst: level,
		});
		let (none, no) = (self.constant(Value::None)?, self.constant(Value::Bool(false))?);
		let created = self.object(&[
			("namespace_id", Operand::Reg(ns)),
			("database_id", Operand::Reg(id)),
			("name", Operand::Reg(db_name)),
			("comment", none),
			("changefeed", none),
			("strict", no),
			("compatibility", Operand::Reg(level)),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			value: created,
		});
		self.emit(Instr::Load {
			dst: database,
			src: Operand::Reg(created),
		});
		self.bind(present)?;
		self.get_field(database, "database_id", id)?;
		Ok((ns, id))
	}

	/// The ids of the session's namespace and database, which must exist (`expect_ns_db_ids`): a
	/// namespace that does not exist holds no database, so either missing is the database not
	/// found.
	fn session_ids(&mut self) -> Result<(Reg, Reg), IrError> {
		let (ns, db, _) = self.selected_ids(StatementError::DbNotFound)?;
		Ok((ns, db))
	}

	/// The ids of the session's namespace and database, which must exist, and the database's
	/// definition in a register: a namespace that does not exist raises `no_namespace`
	/// (`NsNotFound` naming the namespace, or `DbNotFound` naming the database), a database that
	/// does not exist `DbNotFound`.
	fn selected_ids(&mut self, no_namespace: StatementError) -> Result<(Reg, Reg, Reg), IrError> {
		let db_name = self.reg()?;
		self.store(StoreInstr::SessionDb {
			dst: db_name,
		});
		self.named_database_ids(db_name, no_namespace)
	}

	/// [`Self::selected_ids`] for the database of the session's namespace named in `db_name`.
	fn named_database_ids(
		&mut self,
		db_name: Reg,
		no_namespace: StatementError,
	) -> Result<(Reg, Reg, Reg), IrError> {
		let ns_name = self.reg()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let ns_key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key: ns_key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		let named = if matches!(no_namespace, StatementError::NsNotFound) {
			ns_name
		} else {
			db_name
		};
		self.expect(namespace, no_namespace, named)?;
		let ns = self.reg()?;
		self.get_field(namespace, "namespace_id", ns)?;
		let db_key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db_name))])?;
		let database = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key: db_key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		self.expect(database, StatementError::DbNotFound, db_name)?;
		let db = self.reg()?;
		self.get_field(database, "database_id", db)?;
		Ok((ns, db, database))
	}

	/// Bumps the table's catalog version (`bump_catalog_version`).
	fn stamp(&mut self, ns: Reg, db: Reg, tb: Operand) -> Result<(), IrError> {
		let tb = match tb {
			Operand::Reg(reg) => reg,
			other @ (Operand::Const(_) | Operand::Field(_)) => self.load(other)?,
		};
		self.store(StoreInstr::StampCatalog {
			ns,
			db,
			tb,
			draw: 0,
			reserved: None,
		});
		Ok(())
	}

	// -----------------------------------------------------------------------------------------
	// Tables and indexes
	// -----------------------------------------------------------------------------------------

	/// `replace_tb` of the table defined in `table`, at `key`, with its `stamp_field` cache stamp
	/// refreshed: a locked read serialises the write against a concurrent one, then the write and
	/// the catalog version.
	fn replace_table(
		&mut self,
		(ns, db, tb): (Reg, Reg, Operand),
		key: Reg,
		table: Reg,
		stamp_field: &str,
	) -> Result<(), IrError> {
		let refreshed = self.reg()?;
		self.store(StoreInstr::NewUuid {
			dst: refreshed,
			draw: 0,
			reserved: None,
		});
		self.set_field(table, stamp_field, Operand::Reg(refreshed))?;
		self.store(StoreInstr::Lock {
			route: RouteId::named("TableKey"),
			key,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			value: table,
		});
		self.stamp(ns, db, tb)
	}

	/// The table defined at `key`, which must exist.
	fn expect_table(&mut self, key: Reg, name: Reg) -> Result<Reg, IrError> {
		let table = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: table,
		});
		self.expect(table, StatementError::TbNotFound, name)?;
		Ok(table)
	}

	/// The session's database's definition, in namespace `ns`.
	fn database_definition(&mut self, ns: Reg) -> Result<Reg, IrError> {
		let name = self.reg()?;
		self.store(StoreInstr::SessionDb {
			dst: name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(name))])?;
		let database = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		Ok(database)
	}

	/// The table defined at `key`, named `table`, created when it does not exist yet
	/// (`get_or_add_tb`): refused in a `STRICT` database, whose definition is in `database`, and
	/// otherwise defined with the defaults a table takes, a fresh id and fresh cache stamps, and
	/// the table's catalog version bumped.
	fn table_creating(
		&mut self,
		ids: (Reg, Reg, Reg),
		table: Operand,
		key: Reg,
	) -> Result<Reg, IrError> {
		self.table_creating_with(ids, table, key, |_| Ok(()))
	}

	/// [`Self::table_creating`] for a statement that refuses on what its transaction has written
	/// before it writes anything: `refuse` emits the refusal on the path that creates the table,
	/// after a `STRICT` database has refused the creation and before its first write, so a
	/// statement refused there leaves no table behind.
	fn table_creating_with(
		&mut self,
		(ns, db, database): (Reg, Reg, Reg),
		table: Operand,
		key: Reg,
		refuse: impl FnOnce(&mut Self) -> Result<(), IrError>,
	) -> Result<Reg, IrError> {
		let defined = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: defined,
		});
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(defined),
			truthy: true,
			target: present,
		});
		let strict = self.reg()?;
		self.get_field(database, "strict", strict)?;
		let open = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(strict),
			truthy: false,
			target: open,
		});
		let name = self.load(table)?;
		self.store(StoreInstr::Raise {
			error: StatementError::TbNotFound,
			arg: name,
		});
		self.bind(open)?;
		refuse(self)?;
		let template = crate::catalog::StoredTableDefinition::new(
			crate::catalog::NamespaceId(0),
			crate::catalog::DatabaseId(0),
			crate::catalog::TableId(0),
			crate::val::TableName::new(""),
		);
		let template = stored(&template, "StoredTableDefinition")?;
		let template = self.constant(template)?;
		self.emit(Instr::Load {
			dst: defined,
			src: template,
		});
		let id = self.reg()?;
		let id_drawn = self.store(StoreInstr::NextId {
			sequence: IdSequence::Table,
			ns: Some(ns),
			db: Some(db),
			dst: id,
			draw: 0,
			reserved: None,
		});
		let stamp = self.reg()?;
		let stamp_drawn = self.store(StoreInstr::NewUuid {
			dst: stamp,
			draw: 0,
			reserved: None,
		});
		for (field, value) in [
			("namespace_id", Operand::Reg(ns)),
			("database_id", Operand::Reg(db)),
			("table_id", Operand::Reg(id)),
			("name", table),
			("cache_fields_ts", Operand::Reg(stamp)),
			("cache_events_ts", Operand::Reg(stamp)),
			("cache_tables_ts", Operand::Reg(stamp)),
			("cache_indexes_ts", Operand::Reg(stamp)),
			("cache_lives_ts", Operand::Reg(stamp)),
		] {
			self.set_field(defined, field, value)?;
		}
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			value: defined,
		});
		self.stamp(ns, db, table)?;
		let version_drawn = self.em.here() - 1;
		self.share_table_definition(table, [id_drawn, stamp_drawn, version_drawn]);
		self.bind(present)?;
		Ok(defined)
	}

	/// Notes the definition of `table`, made by the draws at `drawn` (its id, its stamp and its
	/// catalog version), as one a write of the block may make first, when the draws take what a
	/// speculative run of the statement reserved and the table is named as written.
	fn share_table_definition(&mut self, table: Operand, drawn: [Label; 3]) {
		let Operand::Const(at) = table else {
			return;
		};
		let table = match self.em.constants().get(at as usize) {
			Some(Value::String(name)) => crate::val::TableName::from(name.as_str()),
			Some(Value::Table(name)) => name.clone(),
			Some(_) | None => return,
		};
		let reserved = drawn.map(|label| match self.em.code().get(label as usize) {
			Some(Instr::Store(
				StoreInstr::NextId {
					reserved,
					..
				}
				| StoreInstr::NewUuid {
					reserved,
					..
				}
				| StoreInstr::StampCatalog {
					reserved,
					..
				},
			)) => *reserved,
			Some(_) | None => None,
		});
		if let [Some(id), Some(stamp), Some(version)] = reserved {
			self.reservations.implicit_tables.push(ImplicitTable {
				table,
				id,
				stamp,
				version,
			});
		}
	}

	/// Raises what the session's missing namespace or database raises (`Options::ns_db`) before a
	/// body that checks the session resolves an evaluated name. Emits nothing when every name in
	/// `slots` is written, which cannot fail.
	fn session_selected(&mut self, slots: &[OperandSlot]) -> Result<(), IrError> {
		let mut written = true;
		for slot in slots {
			written &= matches!(self.literal(*slot)?, Some(Value::String(_)));
		}
		if written {
			return Ok(());
		}
		let selected = self.reg()?;
		self.store(StoreInstr::SessionNs {
			dst: selected,
		});
		self.store(StoreInstr::SessionDb {
			dst: selected,
		});
		Ok(())
	}

	/// Writes the index definition in `definition` at `key`, with its id's name lookup, and bumps
	/// the table's catalog version (`put_tb_index`).
	fn put_index_definition(
		&mut self,
		(ns, db, tb): (Reg, Reg, Operand),
		key: Reg,
		definition: Reg,
		name: Operand,
	) -> Result<(), IrError> {
		self.store(StoreInstr::AdmitIndexFormat {
			definition,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			value: definition,
		});
		let id = self.reg()?;
		self.get_field(definition, "index_id", id)?;
		let lookup = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("tb", tb),
			("ix", Operand::Reg(id)),
		])?;
		let name = self.load(name)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("IndexNameKey"),
			key: lookup,
			ty: TypeId::UTF8,
			value: name,
		});
		self.stamp(ns, db, tb)
	}

	/// Retires the index defined in `index`, at `key`, of the table defined in `table`: the
	/// process's loaded state evicted and a local build aborted once the transaction commits, the
	/// durable build state deleted, the definition and its id's name lookup deleted and the
	/// index's data enqueued for reclaim. With `purge`, the table's shared doc-id space is
	/// enqueued for reclaim too when this was its last consumer, read before the index goes.
	fn retire_index(
		&mut self,
		(ns, db, tb): (Reg, Reg, Operand),
		key: Reg,
		index: Reg,
		table: Reg,
		purge: bool,
	) -> Result<(), IrError> {
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let ix = self.reg()?;
		self.get_field(index, "index_id", ix)?;
		let last = self.reg()?;
		if purge {
			self.last_of_doc_ids((ns, db, tb), index, table, ix, last)?;
		}
		self.store(StoreInstr::RetireIndexLocally {
			ns,
			db,
			table,
			index,
		});
		// The index's durable build state and its queues, deleted at the current version: the
		// removal of one index is never an EXPUNGE.
		let build =
			self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", Operand::Reg(ix))])?;
		self.delete("BuildStateKey", build, false);
		for route in [
			"BuildAppendIxPrefix",
			"BuildPrimaryIxPrefix",
			"BuildReservationIxPrefix",
			"BuildTicketIxPrefix",
		] {
			self.delete_range(route, build, false);
		}
		self.store(StoreInstr::Delete {
			route: RouteId::named("IndexDefKey"),
			key,
		});
		self.store(StoreInstr::Delete {
			route: RouteId::named("IndexNameKey"),
			key: build,
		});
		self.stamp(ns, db, tb)?;
		self.enqueue_reclaim(2, [ns_op, db_op, tb, Operand::Reg(ix)], false)?;
		if purge {
			// The shared doc-id space, in the order its kinds are reclaimed: DocLookup,
			// DocLookupIdentity, DocKey, DocPending.
			let kept = self.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(last),
				truthy: false,
				target: kept,
			});
			let (zero, _) = self.unused_scope()?;
			for kind in [4, 6, 3, 5] {
				self.enqueue_reclaim(kind, [ns_op, db_op, tb, zero], false)?;
			}
			self.bind(kept)?;
		}
		Ok(())
	}

	/// Leaves in `last` whether the index in `index`, with id `ix`, is the last consumer of the
	/// table's shared doc-id space: the table's other indexes and its graph hold none of it.
	fn last_of_doc_ids(
		&mut self,
		(ns, db, tb): (Reg, Reg, Operand),
		index: Reg,
		table: Reg,
		ix: Reg,
		last: Reg,
	) -> Result<(), IrError> {
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		self.store(StoreInstr::SharesDocIds {
			index,
			dst: last,
		});
		let not_last = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(last),
			truthy: false,
			target: not_last,
		});
		let graph = self.reg()?;
		self.get_field(table, "graph_doc_ids", graph)?;
		let graph_held = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(graph),
			truthy: true,
			target: graph_held,
		});
		let others = self.scan(
			"IndexDefPrefix",
			&[("ns", ns_op), ("db", db_op), ("tb", tb)],
			"StoredIndexDefinition",
		)?;
		let held = self.label()?;
		self.each(others, |l, other| {
			let other_ix = l.reg()?;
			l.get_field(other, "index_id", other_ix)?;
			let same = l.reg()?;
			l.emit(Instr::Compare {
				op: super::super::program::CompareOp::Equal,
				left: Operand::Reg(other_ix),
				right: Operand::Reg(ix),
				dst: same,
			});
			let skip = l.label()?;
			l.emit(Instr::JumpIf {
				cond: Operand::Reg(same),
				truthy: true,
				target: skip,
			});
			let shares = l.reg()?;
			l.store(StoreInstr::SharesDocIds {
				index: other,
				dst: shares,
			});
			l.emit(Instr::JumpIf {
				cond: Operand::Reg(shares),
				truthy: true,
				target: held,
			});
			l.bind(skip)
		})?;
		let still_last = self.label()?;
		self.emit(Instr::Jump {
			target: still_last,
		});
		self.bind(held)?;
		self.bind(not_last)?;
		self.bind(graph_held)?;
		let no = self.constant(Value::Bool(false))?;
		self.emit(Instr::Load {
			dst: last,
			src: no,
		});
		self.bind(still_last)
	}

	// -----------------------------------------------------------------------------------------
	// Retirement and reclaim
	// -----------------------------------------------------------------------------------------

	/// Enqueues a reclaim of `kind`, the `ReclaimKind` discriminant its key codec writes, over the
	/// scope `ns`, `db`, `tb`, `ix`; a kind leaves the fields it does not use zeroed.
	fn enqueue_reclaim(
		&mut self,
		kind: i64,
		scope: [Operand; 4],
		expunge: bool,
	) -> Result<(), IrError> {
		let [ns, db, tb, ix] = scope;
		let uid = self.reg()?;
		self.store(StoreInstr::NewUuid {
			dst: uid,
			draw: 0,
			reserved: None,
		});
		let kind = self.constant(Value::from(kind))?;
		let expunge = self.constant(Value::from(expunge as i64))?;
		let key = self.object(&[
			("kind", kind),
			("ns", ns),
			("db", db),
			("tb", tb),
			("ix", ix),
			("expunge", expunge),
			("uid", Operand::Reg(uid)),
		])?;
		let zero = self.constant(Value::from(0i64))?;
		let none = self.constant(Value::None)?;
		let state = self.object(&[("observed_ms", zero), ("cursor", none)])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ReclaimKey"),
			key,
			ty: TypeId::named("ReclaimState"),
			value: state,
		});
		Ok(())
	}

	/// The operands of a reclaim scope's unused fields: id `0` and the empty table name.
	fn unused_scope(&mut self) -> Result<(Operand, Operand), IrError> {
		Ok((self.constant(Value::from(0i64))?, self.constant(Value::String("".into()))?))
	}

	/// The databases of the namespace whose id is in `ns`.
	fn databases(&mut self, ns: Reg) -> Result<Reg, IrError> {
		self.scan("DatabasePrefix", &[("ns", Operand::Reg(ns))], "DatabaseDefinition")
	}

	/// Runs `body` over each database of the namespace in `ns`, given the database's id.
	fn each_database(
		&mut self,
		ns: Reg,
		body: impl FnOnce(&mut Self, Reg) -> Result<(), IrError>,
	) -> Result<(), IrError> {
		let databases = self.databases(ns)?;
		self.each(databases, |l, database| {
			let db = l.reg()?;
			l.get_field(database, "database_id", db)?;
			body(l, db)
		})
	}

	/// The tables of the database in `ns`/`db`.
	fn tables(&mut self, ns: Reg, db: Reg) -> Result<Reg, IrError> {
		self.scan(
			"TablePrefix",
			&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
			"StoredTableDefinition",
		)
	}

	/// Runs `body` over each table of the database in `ns`/`db`, given the table's definition and
	/// its name.
	fn each_table(
		&mut self,
		ns: Reg,
		db: Reg,
		body: impl FnOnce(&mut Self, Reg, Reg) -> Result<(), IrError>,
	) -> Result<(), IrError> {
		let tables = self.tables(ns, db)?;
		self.each(tables, |l, table| {
			let tb = l.reg()?;
			l.get_field(table, "name", tb)?;
			body(l, table, tb)
		})
	}

	/// Retires every index of every table of the database in `ns`/`db`
	/// (`retire_database_indexes`).
	fn retire_database_indexes(&mut self, ns: Reg, db: Reg, expunge: bool) -> Result<(), IrError> {
		self.each_table(ns, db, |l, table, tb| l.retire_table_indexes(ns, db, table, tb, expunge))
	}

	/// Retires every index of the table defined in `table`, named in `tb`
	/// (`retire_table_indexes`): the process's loaded state and local build, and the build's
	/// durable state and queues.
	fn retire_table_indexes(
		&mut self,
		ns: Reg,
		db: Reg,
		table: Reg,
		tb: Reg,
		expunge: bool,
	) -> Result<(), IrError> {
		let (ns_op, db_op, tb_op) = (Operand::Reg(ns), Operand::Reg(db), Operand::Reg(tb));
		let indexes = self.scan(
			"IndexDefPrefix",
			&[("ns", ns_op), ("db", db_op), ("tb", tb_op)],
			"StoredIndexDefinition",
		)?;
		self.each(indexes, |l, index| {
			l.store(StoreInstr::RetireIndexLocally {
				ns,
				db,
				table,
				index,
			});
			let ix = l.reg()?;
			l.get_field(index, "index_id", ix)?;
			let key =
				l.object(&[("ns", ns_op), ("db", db_op), ("tb", tb_op), ("ix", Operand::Reg(ix))])?;
			l.delete("BuildStateKey", key, expunge);
			for route in [
				"BuildAppendIxPrefix",
				"BuildPrimaryIxPrefix",
				"BuildReservationIxPrefix",
				"BuildTicketIxPrefix",
			] {
				l.delete_range(route, key, expunge);
			}
			Ok(())
		})
	}

	/// Queues a `KILLED`, sent on commit, for every subscription on every table of the database
	/// in `ns`/`db` (`kill_database_subscriptions`).
	fn kill_database_subscriptions(&mut self, ns: Reg, db: Reg) -> Result<(), IrError> {
		self.each_table(ns, db, |l, _, tb| l.kill_table_subscriptions(ns, db, tb))
	}

	/// Queues a `KILLED`, sent on commit, for every subscription on the table named in `tb`
	/// (`kill_table_subscriptions`). Subscriptions that do not read are passed over.
	fn kill_table_subscriptions(&mut self, ns: Reg, db: Reg, tb: Reg) -> Result<(), IrError> {
		let key = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("tb", Operand::Reg(tb)),
		])?;
		let lives = self.reg()?;
		let unread = self.label()?;
		self.store(StoreInstr::Scan {
			route: RouteId::named("SubscriptionPrefix"),
			key,
			ty: TypeId::named("StoredSubscriptionDefinition"),
			dst: lives,
			unread: Some(unread),
		});
		self.each(lives, |l, subscription| {
			l.store(StoreInstr::KillOnCommit {
				subscription,
			});
			Ok(())
		})?;
		self.bind(unread)
	}

	/// Evicts the sequence service's state for every sequence of the database in `ns`/`db`
	/// (`Sequences::database_removed`).
	fn evict_database_sequences(&mut self, ns: Reg, db: Reg) -> Result<(), IrError> {
		let sequences = self.scan(
			"SequencePrefix",
			&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
			"SequenceDefinition",
		)?;
		self.each(sequences, |l, sequence| {
			let name = l.reg()?;
			l.get_field(sequence, "name", name)?;
			l.store(StoreInstr::EvictSequence {
				ns,
				db,
				name,
			});
			Ok(())
		})
	}

	/// Removes every subscription in the database in `ns`/`db` that runs as the actor named in
	/// `actor`, and queues its `KILLED` for commit (`kill_principal_subscriptions`). A table whose
	/// subscriptions do not read is passed over; one that loses a subscription has its lives cache
	/// stamp moved (`bump_table_lives_cache`).
	fn revoke_database_subscriptions(
		&mut self,
		ns: Reg,
		db: Reg,
		actor: Reg,
	) -> Result<(), IrError> {
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		self.each_table(ns, db, |l, _, tb| {
			let tb_op = Operand::Reg(tb);
			let key = l.object(&[("ns", ns_op), ("db", db_op), ("tb", tb_op)])?;
			let lives = l.reg()?;
			let unread = l.label()?;
			l.store(StoreInstr::Scan {
				route: RouteId::named("SubscriptionPrefix"),
				key,
				ty: TypeId::named("StoredSubscriptionDefinition"),
				dst: lives,
				unread: Some(unread),
			});
			let no = l.constant(Value::Bool(false))?;
			let removed = l.load(no)?;
			l.each(lives, |l, live| {
				let next = l.label()?;
				let owner = l.reg()?;
				l.get_field(live, "auth", owner)?;
				l.emit(Instr::JumpIf {
					cond: Operand::Reg(owner),
					truthy: false,
					target: next,
				});
				for field in ["actor", "res", "id"] {
					l.get_field(owner, field, owner)?;
				}
				l.emit(Instr::Compare {
					op: CompareOp::Equal,
					left: Operand::Reg(owner),
					right: Operand::Reg(actor),
					dst: owner,
				});
				l.emit(Instr::JumpIf {
					cond: Operand::Reg(owner),
					truthy: false,
					target: next,
				});
				let (lq, nd) = (l.reg()?, l.reg()?);
				l.get_field(live, "id", lq)?;
				l.get_field(live, "node", nd)?;
				let subscription = l.object(&[
					("ns", ns_op),
					("db", db_op),
					("tb", tb_op),
					("lq", Operand::Reg(lq)),
				])?;
				l.store(StoreInstr::Clear {
					route: RouteId::named("SubscriptionKey"),
					key: subscription,
				});
				let node = l.object(&[("nd", Operand::Reg(nd)), ("lq", Operand::Reg(lq))])?;
				l.store(StoreInstr::Clear {
					route: RouteId::named("NodeLiveQueryKey"),
					key: node,
				});
				let yes = l.constant(Value::Bool(true))?;
				l.emit(Instr::Load {
					dst: removed,
					src: yes,
				});
				l.store(StoreInstr::KillOnCommit {
					subscription: live,
				});
				l.bind(next)
			})?;
			let done = l.label()?;
			l.emit(Instr::JumpIf {
				cond: Operand::Reg(removed),
				truthy: false,
				target: done,
			});
			// Under a locked read, the table's lives stamp moves, when the table is still defined.
			let table_key = l.object(&[("ns", ns_op), ("db", db_op), ("tb", tb_op)])?;
			l.store(StoreInstr::Lock {
				route: RouteId::named("TableKey"),
				key: table_key,
			});
			let table = l.reg()?;
			l.store(StoreInstr::Get {
				route: RouteId::named("TableKey"),
				key: table_key,
				ty: TypeId::named("StoredTableDefinition"),
				dst: table,
			});
			l.emit(Instr::JumpIf {
				cond: Operand::Reg(table),
				truthy: false,
				target: done,
			});
			let stamp = l.reg()?;
			l.store(StoreInstr::NewUuid {
				dst: stamp,
				draw: 0,
				reserved: None,
			});
			l.set_field(table, "cache_lives_ts", Operand::Reg(stamp))?;
			l.store(StoreInstr::Put {
				route: RouteId::named("TableKey"),
				key: table_key,
				ty: TypeId::named("StoredTableDefinition"),
				value: table,
			});
			l.store(StoreInstr::ClearCache);
			l.bind(done)?;
			l.bind(unread)
		})
	}

	/// Answers `NONE` under `IF EXISTS`; raises `missing` otherwise, whose fields are the `name`
	/// and the names of the session's namespace and database down to `base`.
	fn missing_at_base(
		&mut self,
		if_exists: bool,
		base: Base,
		name: (&str, Operand),
		missing: StatementError,
	) -> Result<(), IrError> {
		if if_exists {
			return self.answer_none();
		}
		let mut detail = vec![name];
		if !matches!(base, Base::Root) {
			let ns = self.reg()?;
			self.store(StoreInstr::SessionNs {
				dst: ns,
			});
			detail.push(("ns", Operand::Reg(ns)));
		}
		if matches!(base, Base::Db) {
			let db = self.reg()?;
			self.store(StoreInstr::SessionDb {
				dst: db,
			});
			detail.push(("db", Operand::Reg(db)));
		}
		let detail = self.object(&detail)?;
		self.store(StoreInstr::Raise {
			error: missing,
			arg: detail,
		});
		Ok(())
	}

	/// The definition of type `ty` at `key` under `route`, which must exist: `IF EXISTS` answers
	/// `NONE` for a missing one, and any other statement raises `missing` naming `name`.
	fn stored_at(
		&mut self,
		(route, ty): (&'static str, &'static str),
		key: Reg,
		if_exists: bool,
		(missing, name): (StatementError, Operand),
	) -> Result<Reg, IrError> {
		let definition = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named(ty),
			dst: definition,
		});
		let name = self.load(name)?;
		self.must_exist(if_exists, definition, missing, name)?;
		Ok(definition)
	}

	// -----------------------------------------------------------------------------------------
	// DEFINE
	// -----------------------------------------------------------------------------------------

	fn define(&mut self, attrs: &DefineAttrs) -> Result<(), IrError> {
		let mode = &attrs.mode;
		match &attrs.object {
			DefineObject::Namespace {
				name,
				comment,
				..
			} => self.define_namespace(mode, *name, *comment),
			DefineObject::Database {
				name,
				strict,
				changefeed,
				comment,
				compatibility,
				..
			} => self.define_database(
				mode,
				*name,
				(*strict, changefeed.as_ref(), *compatibility),
				*comment,
			),
			DefineObject::Param {
				name,
				value,
				comment,
				..
			} => self.define_param(attrs, name.as_str(), *value, *comment),
			DefineObject::Function(function) => self.define_function(attrs, function),
			DefineObject::Model {
				name,
				version,
				comment,
				..
			} => self.define_model(attrs, name.as_str(), version.as_str(), *comment),
			DefineObject::Analyzer {
				name,
				comment,
				..
			} => self.define_analyzer(attrs, *name, *comment),
			DefineObject::Table(table) => self.define_table(attrs, table),
			DefineObject::Event {
				name,
				target_table,
				comment,
				..
			} => self.define_event(attrs, *name, *target_table, *comment),
			DefineObject::Field(field) => self.define_field(attrs, field),
			DefineObject::Index {
				name,
				name_text,
				what,
				columns,
				index,
				comment,
				concurrently,
			} => self.define_index(
				attrs,
				(*name, name_text),
				*what,
				(columns, index),
				(*comment, *concurrently),
			),
			DefineObject::User(user) => self.define_user(attrs, user),
			DefineObject::Module {
				name,
				executable,
				unsigned,
				signed,
				comment,
				..
			} => self.define_module(attrs, name, executable, *unsigned || *signed, *comment),
			DefineObject::Access(access) => self.define_access(attrs, access),
			DefineObject::Config(config) => self.define_config(attrs, config),
			DefineObject::Api(api) => self.define_api(attrs, api),
			DefineObject::Bucket {
				name,
				base,
				backend,
				comment,
				..
			} => self.define_bucket(attrs, *name, *base, *backend, *comment),
			DefineObject::Sequence {
				name,
				batch,
				start,
				timeout,
			} => self.define_sequence(attrs, *name, (*batch, *start, *timeout)),
			DefineObject::Type {
				name,
				comment,
				..
			} => self.define_type(attrs, name.as_str(), *comment),
		}
	}

	/// `DEFINE NAMESPACE`: the namespace's id is the existing definition's, or a fresh one.
	fn define_namespace(
		&mut self,
		mode: &DefineKind,
		name: OperandSlot,
		comment: OperandSlot,
	) -> Result<(), IrError> {
		self.guard()?;
		let name = self.name(name, NameClause::Namespace)?.operand;
		let name_reg = self.load(name)?;
		let key = self.object(&[("ns", name)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: existing,
		});
		let id = self.reg()?;
		let missing = self.exists(mode, existing, StatementError::NsAlreadyExists, name_reg)?;
		self.get_field(existing, "namespace_id", id)?;
		let built = self.label()?;
		self.emit(Instr::Jump {
			target: built,
		});
		self.bind(missing)?;
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Namespace,
			ns: None,
			db: None,
			dst: id,
			draw: 0,
			reserved: None,
		});
		self.bind(built)?;
		let comment = self.comment(comment)?.operand;
		let definition = self.object(&[
			("namespace_id", Operand::Reg(id)),
			("name", name),
			("comment", comment),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE DATABASE`: the session's namespace is created when it does not exist, and the
	/// database's id is the existing definition's, or a fresh one. Its compatibility level is the
	/// clause's, else the level the redefined database stands at, else the level a new database
	/// takes; a redefinition that raises the level holds the stored rows to the rules of the new
	/// one.
	fn define_database(
		&mut self,
		mode: &DefineKind,
		name: OperandSlot,
		(strict, changefeed, compatibility): (bool, Option<&crate::expr::ChangeFeed>, Option<u8>),
		comment: OperandSlot,
	) -> Result<(), IrError> {
		self.guard()?;
		let ns = self.namespace_id_creating()?;
		let name = self.name(name, NameClause::Database)?.operand;
		let name_reg = self.load(name)?;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", name)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: existing,
		});
		let id = self.reg()?;
		let level = self.reg()?;
		let written = match compatibility {
			Some(written) => {
				let written = self.constant(Value::from(i64::from(written)))?;
				self.emit(Instr::Load {
					dst: level,
					src: written,
				});
				true
			}
			None => false,
		};
		let missing = self.exists(mode, existing, StatementError::DbAlreadyExists, name_reg)?;
		self.get_field(existing, "database_id", id)?;
		if written {
			let previous = self.reg()?;
			self.get_field(existing, "compatibility", previous)?;
			self.store(StoreInstr::VerifyCompatibility {
				ns,
				db: id,
				previous,
				level,
			});
		} else {
			self.get_field(existing, "compatibility", level)?;
		}
		let built = self.label()?;
		self.emit(Instr::Jump {
			target: built,
		});
		self.bind(missing)?;
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Database,
			ns: Some(ns),
			db: None,
			dst: id,
			draw: 0,
			reserved: None,
		});
		if !written {
			self.store(StoreInstr::NewDatabaseCompatibility {
				ns,
				db: id,
				dst: level,
			});
		}
		self.bind(built)?;
		let comment = self.comment(comment)?.operand;
		let changefeed_value = self.constant(match changefeed {
			Some(cf) => changefeed_value(cf),
			None => Value::None,
		})?;
		let strict = self.constant(Value::Bool(strict))?;
		let definition = self.object(&[
			("namespace_id", Operand::Reg(ns)),
			("database_id", Operand::Reg(id)),
			("name", name),
			("comment", comment),
			("changefeed", changefeed_value),
			("strict", strict),
			("compatibility", Operand::Reg(level)),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			value: definition,
		});
		// A retention written here makes the changefeed collector's watermark stale, so its fence
		// moves in the same transaction.
		if changefeed.is_some() {
			self.store(StoreInstr::BumpChangefeedFence {
				ns,
				db: id,
			});
		}
		self.store(StoreInstr::ClearDatastoreCache);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE PARAM`: the value is evaluated before the database is resolved, and the session's
	/// namespace and database are created when they do not exist.
	fn define_param(
		&mut self,
		attrs: &DefineAttrs,
		param: &str,
		value: OperandSlot,
		comment: OperandSlot,
	) -> Result<(), IrError> {
		self.guard()?;
		let value = self.evaluated(value)?;
		self.store(StoreInstr::CheckDepth {
			value,
		});
		let (ns, db) = self.ids_creating()?;
		let name = self.constant(Value::String(param.into()))?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("pa", name)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ParamKey"),
			key,
			ty: TypeId::named("StoredParamDefinition"),
			dst: existing,
		});
		let name_reg = self.load(name)?;
		let missing =
			self.exists(&attrs.mode, existing, StatementError::PaAlreadyExists, name_reg)?;
		self.bind(missing)?;
		let written: crate::catalog::ParamDefinition =
			definition_of(&attrs.template.value, "StoredParamDefinition")?;
		let names = super::super::schema_host::permission_named_types(&written.permissions);
		self.check_types_defined((ns, db), names, || format!("The parameter '${param}'"));
		let comment = self.comment(comment)?;
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_comment(definition, &comment)?;
		self.set_field(definition, "value", Operand::Reg(value))?;
		self.store(StoreInstr::RefuseClauseEffects {
			ns,
			db,
			definition,
			previous: None,
			object: ClauseObject::Param,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("ParamKey"),
			key,
			ty: TypeId::named("StoredParamDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// Refuses, where it runs, the definition `subject` of the database `ns`/`db` when one of
	/// `names`, the types its clauses name, is not a type the database defines. Emitted where the
	/// definition is about to be stored, so an `IF NOT EXISTS` that meets a definition judges
	/// nothing.
	pub(super) fn check_types_defined(
		&mut self,
		(ns, db): (Reg, Reg),
		names: Vec<String>,
		subject: impl FnOnce() -> String,
	) {
		if !names.is_empty() {
			self.store(StoreInstr::CheckTypesDefined {
				ns,
				db,
				names: names.into(),
				subject: subject(),
			});
		}
	}

	/// `kind` followed by the name in operand `slot` quoted, as a refusal shows a definition, or
	/// `kind` alone for a name the statement evaluates.
	pub(super) fn shown_subject(&self, kind: &str, slot: OperandSlot) -> Result<String, IrError> {
		Ok(match self.literal(slot)? {
			Some(Value::String(name)) => format!("{kind} '{}'", name.as_str()),
			Some(_) | None => kind.to_owned(),
		})
	}

	/// `DEFINE FUNCTION`: the definition is stamped with the session's authority.
	fn define_function(
		&mut self,
		attrs: &DefineAttrs,
		defined: &DefineFunction,
	) -> Result<(), IrError> {
		let (function, comment) = (defined.name.as_str(), defined.comment);
		self.guard()?;
		let (ns, db) = self.ids_creating()?;
		let name = self.constant(Value::String(function.into()))?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("fc", name)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FunctionKey"),
			key,
			ty: TypeId::named("StoredFunctionDefinition"),
			dst: existing,
		});
		let shown = self.constant(Value::String(format!("fn::{function}").into()))?;
		let shown = self.load(shown)?;
		let missing = self.exists(&attrs.mode, existing, StatementError::FcAlreadyExists, shown)?;
		self.bind(missing)?;
		// The types the function names are judged only where it is defined: an `IF NOT EXISTS`
		// that meets the function stores nothing.
		let names = super::super::schema_host::function_named_types(&function_definition_of(
			&attrs.template.value,
		)?);
		self.check_types_defined((ns, db), names, || format!("The function 'fn::{function}'"));
		let comment = self.comment(comment)?;
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_comment(definition, &comment)?;
		self.session_auth_limit(definition)?;
		self.store(StoreInstr::RefuseClauseEffects {
			ns,
			db,
			definition,
			previous: None,
			object: ClauseObject::Function,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("FunctionKey"),
			key,
			ty: TypeId::named("StoredFunctionDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE MODEL`.
	fn define_model(
		&mut self,
		attrs: &DefineAttrs,
		model: &str,
		version: &str,
		comment: OperandSlot,
	) -> Result<(), IrError> {
		self.guard()?;
		let (ns, db) = self.ids_creating()?;
		let shown = format!("The model 'ml::{model}<{version}>'");
		let name = self.constant(Value::String(model.into()))?;
		let version = self.constant(Value::String(version.into()))?;
		let key = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("ml", name),
			("vn", version),
		])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("MlModelKey"),
			key,
			ty: TypeId::named("StoredMlModelDefinition"),
			dst: existing,
		});
		let name_reg = self.load(name)?;
		let missing =
			self.exists(&attrs.mode, existing, StatementError::MlAlreadyExists, name_reg)?;
		self.bind(missing)?;
		let written: crate::catalog::MlModelDefinition =
			definition_of(&attrs.template.value, "StoredMlModelDefinition")?;
		let names = super::super::schema_host::permission_named_types(&written.permissions);
		self.check_types_defined((ns, db), names, || shown);
		let comment = self.comment(comment)?;
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_comment(definition, &comment)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("MlModelKey"),
			key,
			ty: TypeId::named("StoredMlModelDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE EVENT`: the names are resolved between the no-write refusal and the authority
	/// check. The event's table is created when it does not exist, and refused when it is a
	/// lightweight relation. The definition is stamped with the session's authority.
	fn define_event(
		&mut self,
		attrs: &DefineAttrs,
		name: OperandSlot,
		table: OperandSlot,
		comment: OperandSlot,
	) -> Result<(), IrError> {
		self.refuse_no_write()?;
		let name_slot = name;
		let name = self.name(name, NameClause::Event)?;
		let table = self.name(table, NameClause::TargetTable)?;
		self.allow()?;
		let (ns, db) = self.ids_creating()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let (tb, ev) = (table.operand, name.operand);
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ev", ev)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("EventKey"),
			key,
			ty: TypeId::named("StoredEventDefinition"),
			dst: existing,
		});
		let name_reg = self.load(ev)?;
		let missing =
			self.exists(&attrs.mode, existing, StatementError::EvAlreadyExists, name_reg)?;
		self.bind(missing)?;
		// The types the event names are judged only where it is defined: an `IF NOT EXISTS` that
		// meets the event stores nothing.
		let written: crate::catalog::EventDefinition =
			definition_of(&attrs.template.value, "StoredEventDefinition")?;
		let names = super::super::schema_host::event_named_types(&written);
		if !names.is_empty() {
			let subject = self.shown_subject("The event", name_slot)?;
			self.check_types_defined((ns, db), names, || subject);
		}
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let database = self.database_definition(ns)?;
		let table_def = self.table_creating((ns, db, database), tb, tb_key)?;
		self.store(StoreInstr::RefuseLightweight {
			table: table_def,
			refusal: StoreRefusal::Event,
		});
		let comment = self.comment(comment)?;
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_comment(definition, &comment)?;
		self.set_name(definition, "name", &name)?;
		self.set_name(definition, "target_table", &table)?;
		self.session_auth_limit(definition)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("EventKey"),
			key,
			ty: TypeId::named("StoredEventDefinition"),
			value: definition,
		});
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_events_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE ANALYZER`: the comment is evaluated before the name and the existence check, and the
	/// analyzer is loaded for the process, with its mappers, before it is written.
	fn define_analyzer(
		&mut self,
		attrs: &DefineAttrs,
		name: OperandSlot,
		comment: OperandSlot,
	) -> Result<(), IrError> {
		self.guard()?;
		let comment = self.comment(comment)?;
		let name = self.name(name, NameClause::Analyzer)?;
		let (ns, db) = self.ids_creating()?;
		let az = name.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("az", az)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("AnalyzerKey"),
			key,
			ty: TypeId::named("AnalyzerDefinition"),
			dst: existing,
		});
		let name_reg = self.load(az)?;
		let missing =
			self.exists(&attrs.mode, existing, StatementError::AzAlreadyExists, name_reg)?;
		self.bind(missing)?;
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_comment(definition, &comment)?;
		self.set_name(definition, "name", &name)?;
		self.store(StoreInstr::LoadAnalyzer {
			analyzer: definition,
			mappers: true,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("AnalyzerKey"),
			key,
			ty: TypeId::named("AnalyzerDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE SEQUENCE`: the definition, and the batches and state of the sequence it replaces,
	/// dropped. The timeout is read before the database is resolved; the batch and the start are
	/// converted after the existence check, and a batch out of range is refused there.
	fn define_sequence(
		&mut self,
		attrs: &DefineAttrs,
		name: OperandSlot,
		(batch, start, timeout): (OperandSlot, OperandSlot, OperandSlot),
	) -> Result<(), IrError> {
		self.guard()?;
		let name = self.name(name, NameClause::Sequence)?;
		let (timeout, timeout_evaluated) = self.duration(timeout, true)?;
		let (ns, db) = self.ids_creating()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let sq = name.operand;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("sq", sq)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("SequenceKey"),
			key,
			ty: TypeId::named("SequenceDefinition"),
			dst: existing,
		});
		let name_reg = self.load(sq)?;
		let missing =
			self.exists(&attrs.mode, existing, StatementError::SeqAlreadyExists, name_reg)?;
		self.bind(missing)?;
		let batch = match written_integer(self.literal(batch)?) {
			Some(written) if u32::try_from(written).is_ok() => {
				let written = self.constant(Value::from(written))?;
				self.read_as(batch, written)?;
				None
			}
			Some(_) | None => Some(self.converted(batch, ClauseConversion::Batch)?),
		};
		let start = match written_integer(self.literal(start)?) {
			Some(written) => {
				let written = self.constant(Value::from(written))?;
				self.read_as(start, written)?;
				None
			}
			None => Some(self.converted(start, ClauseConversion::Integer)?),
		};
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_name(definition, "name", &name)?;
		for (field, value) in [("batch", batch), ("start", start)] {
			if let Some(value) = value {
				self.set_field(definition, field, Operand::Reg(value))?;
			}
		}
		if timeout_evaluated {
			self.set_field(definition, "timeout", timeout)?;
		}
		self.store(StoreInstr::Put {
			route: RouteId::named("SequenceKey"),
			key,
			ty: TypeId::named("SequenceDefinition"),
			value: definition,
		});
		self.delete_range("SeqBatchPrefix", key, false);
		self.delete_range("SeqStatePrefix", key, false);
		// The node's allocator reads definitions through transactions of its own and cannot see
		// this one until it commits, so the transaction records the definition it wrote, and the
		// node drops the allocator it cached for the sequence this one replaces.
		let defined = self.load(sq)?;
		self.store(StoreInstr::SequenceDefinedHere {
			ns,
			db,
			name: defined,
		});
		self.with_service(Service::Sequences, |l| {
			l.store(StoreInstr::EvictSequence {
				ns,
				db,
				name: defined,
			});
			Ok(())
		})?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE TYPE`: the type's id is the existing definition's, or a fresh one, so a
	/// redefinition keeps the identity the database knows it by. The definition is refused when
	/// its body names a type the database does not define, or leads back to itself.
	fn define_type(
		&mut self,
		attrs: &DefineAttrs,
		name: &str,
		comment: OperandSlot,
	) -> Result<(), IrError> {
		self.guard()?;
		let (ns, db) = self.ids_creating()?;
		let name = self.constant(Value::String(name.into()))?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", name)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("UserTypeKey"),
			key,
			ty: TypeId::named("StoredTypeDefinition"),
			dst: existing,
		});
		let name_reg = self.load(name)?;
		let id = self.reg()?;
		let missing =
			self.exists(&attrs.mode, existing, StatementError::TyAlreadyExists, name_reg)?;
		self.get_field(existing, "id", id)?;
		let built = self.label()?;
		self.emit(Instr::Jump {
			target: built,
		});
		self.bind(missing)?;
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Enum,
			ns: Some(ns),
			db: Some(db),
			dst: id,
			draw: 0,
			reserved: None,
		});
		self.bind(built)?;
		let comment = self.comment(comment)?;
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_comment(definition, &comment)?;
		self.set_field(definition, "id", Operand::Reg(id))?;
		self.store(StoreInstr::CheckTypeDefinition {
			ns,
			db,
			existing,
			definition,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("UserTypeKey"),
			key,
			ty: TypeId::named("StoredTypeDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// The key route of a bucket at `base`, and the ids of the namespace and database its key
	/// lies under: none at the root, the session's namespace at `NAMESPACE`, its namespace and
	/// database at `DATABASE`. With `creating`, a namespace or database that does not exist is
	/// created; without, it must exist.
	fn bucket_scope(
		&mut self,
		base: Base,
		creating: bool,
	) -> Result<(&'static str, Option<Reg>, Option<Reg>), IrError> {
		Ok(match base {
			Base::Root => ("RootBucketKey", None, None),
			Base::Ns if creating => ("NsBucketKey", Some(self.namespace_id_creating()?), None),
			Base::Ns => ("NsBucketKey", Some(self.session_ns_id()?), None),
			Base::Db => {
				let (ns, db) = if creating {
					self.ids_creating()?
				} else {
					self.session_ids()?
				};
				("BucketKey", Some(ns), Some(db))
			}
		})
	}

	/// The key of bucket `bu` under the namespace and database [`Self::bucket_scope`] gave.
	fn bucket_key(
		&mut self,
		ns: Option<Reg>,
		db: Option<Reg>,
		bu: Operand,
	) -> Result<Reg, IrError> {
		let mut fields = Vec::with_capacity(3);
		if let Some(ns) = ns {
			fields.push(("ns", Operand::Reg(ns)));
		}
		if let Some(db) = db {
			fields.push(("db", Operand::Reg(db)));
		}
		fields.push(("bu", bu));
		self.object(&fields)
	}

	/// The id of the session's namespace, which must exist: one that does not raises
	/// `NsNotFound` naming it.
	fn session_ns_id(&mut self) -> Result<Reg, IrError> {
		let ns_name = self.reg()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let ns_key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key: ns_key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(namespace),
			truthy: true,
			target: present,
		});
		self.store(StoreInstr::Raise {
			error: StatementError::NsNotFound,
			arg: ns_name,
		});
		self.bind(present)?;
		let ns = self.reg()?;
		self.get_field(namespace, "namespace_id", ns)?;
		Ok(ns)
	}

	/// `DEFINE BUCKET`: a definition that exists is refused under the name it holds. The backend
	/// is converted, and the bucket connected to it, before the comment is evaluated. A statement
	/// with no backend is refused outside an import; an import stores the bucket with none.
	fn define_bucket(
		&mut self,
		attrs: &DefineAttrs,
		name: OperandSlot,
		base: Base,
		backend: Option<OperandSlot>,
		comment: OperandSlot,
	) -> Result<(), IrError> {
		self.guard()?;
		if backend.is_none() {
			let import = self.label()?;
			self.store(StoreInstr::JumpIfImport {
				target: import,
			});
			let arg = self.constant(Value::None)?;
			let arg = self.load(arg)?;
			self.store(StoreInstr::Raise {
				error: StatementError::BuNeedsBackend,
				arg,
			});
			self.bind(import)?;
		}
		let name_slot = name;
		let name = self.name(name, NameClause::Bucket)?;
		let (route, ns, db) = self.bucket_scope(base, true)?;
		let bu = name.operand;
		let key = self.bucket_key(ns, db, bu)?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredBucketDefinition"),
			dst: existing,
		});
		let nonce = self.reg()?;
		let missing = self.exists_with(
			&attrs.mode,
			existing,
			|l| {
				let shown = l.reg()?;
				l.get_field(existing, "name", shown)?;
				l.store(StoreInstr::Raise {
					error: StatementError::BuAlreadyExists,
					arg: shown,
				});
				Ok(())
			},
			|_| Ok(()),
		)?;
		self.get_field(existing, "presign_nonce", nonce)?;
		let built = self.label()?;
		self.emit(Instr::Jump {
			target: built,
		});
		self.bind(missing)?;
		self.store(StoreInstr::NewUuid {
			dst: nonce,
			draw: 0,
			reserved: None,
		});
		self.bind(built)?;
		// A named type is a type of a database, so only a database bucket's permission can name
		// one.
		if let (Some(ns), Some(db)) = (ns, db) {
			let written: crate::catalog::BucketDefinition =
				definition_of(&attrs.template.value, "StoredBucketDefinition")?;
			let names = super::super::schema_host::permission_named_types(&written.permissions);
			if !names.is_empty() {
				let subject = self.shown_subject("The bucket", name_slot)?;
				self.check_types_defined((ns, db), names, || subject);
			}
		}
		let backend = match backend {
			Some(slot) => match self.literal(slot)? {
				Some(Value::String(written)) => {
					let written = written.clone();
					let written = self.constant(Value::String(written))?;
					self.read_as(slot, written)?;
					None
				}
				Some(_) | None => Some(self.converted(slot, ClauseConversion::Coerced)?),
			},
			None => None,
		};
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_name(definition, "name", &name)?;
		self.set_field(definition, "presign_nonce", Operand::Reg(nonce))?;
		if let Some(backend) = backend {
			self.set_field(definition, "backend", Operand::Reg(backend))?;
		}
		self.store(StoreInstr::ConnectBucket {
			ns,
			db,
			bucket: definition,
		});
		let comment = self.comment(comment)?;
		self.set_comment(definition, &comment)?;
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredBucketDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE EXTENSION`: under `OVERWRITE`, the runtime the stored definition loaded is evicted
	/// from the process, and the new one is loaded after it is written. `trust_stated` is whether
	/// the statement names a `SIGNER` or says `UNSIGNED`: a definition with neither is refused
	/// outside an import; an import stores it as a legacy row, the way a 3.x catalog row reads.
	fn define_module(
		&mut self,
		attrs: &DefineAttrs,
		name: &Option<String>,
		executable: &ModuleExecutable,
		trust_stated: bool,
		comment: OperandSlot,
	) -> Result<(), IrError> {
		let storage = module_storage_name(name, executable)?;
		self.guard()?;
		let (ns, db) = self.ids_creating()?;
		let md = self.constant(Value::String(storage.as_str().into()))?;
		if !trust_stated {
			let import = self.label()?;
			self.store(StoreInstr::JumpIfImport {
				target: import,
			});
			let arg = self.load(md)?;
			self.store(StoreInstr::Raise {
				error: StatementError::MdNeedsTrust,
				arg,
			});
			self.bind(import)?;
		}
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("md", md)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ModuleKey"),
			key,
			ty: TypeId::named("StoredModuleDefinition"),
			dst: existing,
		});
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		let missing = self.exists_with(
			&attrs.mode,
			existing,
			|l| {
				let name_reg = l.load(md)?;
				l.store(StoreInstr::Raise {
					error: StatementError::MdAlreadyExists,
					arg: name_reg,
				});
				Ok(())
			},
			|l| {
				// The runtime to drop is the one the stored row loaded, keyed by
				// the DIGEST that row pinned; the new statement's DIGEST, if it
				// gives one, names another archive.
				l.store(StoreInstr::EvictModule {
					ns,
					db,
					module: existing,
				});
				Ok(())
			},
		)?;
		self.bind(missing)?;
		let written: crate::catalog::ModuleDefinition =
			definition_of(&attrs.template.value, "StoredModuleDefinition")?;
		let names = super::super::schema_host::permission_named_types(&written.permissions);
		self.check_types_defined((ns, db), names, || format!("The module '{storage}'"));
		let comment = self.comment(comment)?;
		self.set_comment(definition, &comment)?;
		// `BINDINGS` are evaluated with the rights of the session that wrote
		// the definition.
		self.session_auth_limit(definition)?;
		self.store(StoreInstr::VerifyModule {
			ns,
			db,
			module: definition,
		});
		let stored_name = self.constant(Value::String(storage.as_str().into()))?;
		let stored_at = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("md", stored_name),
		])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ModuleKey"),
			key: stored_at,
			ty: TypeId::named("StoredModuleDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::LoadModule {
			ns,
			db,
			module: definition,
		});
		self.answer_none()
	}

	// -----------------------------------------------------------------------------------------
	// REMOVE
	// -----------------------------------------------------------------------------------------

	fn remove(&mut self, attrs: &RemoveAttrs) -> Result<(), IrError> {
		let (if_exists, expunge) = (attrs.if_exists, attrs.expunge);
		match &attrs.object {
			RemoveObject::Namespace {
				name,
			} => self.remove_namespace(*name, if_exists, expunge),
			RemoveObject::Database {
				name,
			} => self.remove_database(*name, if_exists, expunge),
			RemoveObject::Param {
				name,
			} => self.remove_db_definition(
				("ParamKey", "pa", "StoredParamDefinition"),
				DbName::Written(name.as_str().to_owned(), name.as_str().to_owned()),
				if_exists,
				StatementError::PaNotFound,
			),
			RemoveObject::Function {
				name,
			} => self.remove_db_definition(
				("FunctionKey", "fc", "StoredFunctionDefinition"),
				DbName::Written(name.as_str().to_owned(), format!("fn::{}", name.as_str())),
				if_exists,
				StatementError::FcNotFound,
			),
			RemoveObject::Bucket {
				name,
				base,
			} => self.remove_bucket(*name, *base, if_exists),
			RemoveObject::Type {
				name,
				object,
			} => self.remove_type(name.as_str(), *object, if_exists),
			RemoveObject::Module {
				name,
			} => self.remove_module(&name.get_storage_name(), if_exists),
			RemoveObject::Sequence {
				name,
			} => self.remove_sequence(*name, if_exists),
			RemoveObject::Config {
				kind,
			} => self.remove_config(kind, if_exists),
			RemoveObject::Model {
				name,
				version,
			} => self.remove_model(name.as_str(), version.as_str(), if_exists),
			RemoveObject::Index {
				name,
				table,
			} => self.remove_index(*name, *table, if_exists),
			RemoveObject::Analyzer {
				name,
			} => self.remove_analyzer(*name, if_exists),
			RemoveObject::Access {
				name,
				base,
			} => self.remove_access(*name, *base, if_exists),
			RemoveObject::Table {
				name,
			} => self.remove_table(*name, if_exists, expunge),
			RemoveObject::Event {
				name,
				table,
			} => self.remove_event(*name, *table, if_exists),
			RemoveObject::Field {
				name,
				table,
			} => self.remove_field((*name, *table), &attrs.checks, if_exists),
			RemoveObject::User {
				name,
				base,
			} => self.remove_user(*name, *base, if_exists),
			RemoveObject::Api {
				name,
			} => self.remove_api(*name, if_exists),
		}
	}

	/// `REMOVE NAMESPACE`: every database's indexes are retired, its subscriptions are killed and
	/// its sequences evicted before the definition goes and its data is enqueued for reclaim.
	fn remove_namespace(
		&mut self,
		name: OperandSlot,
		if_exists: bool,
		expunge: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		let name = self.name(name, NameClause::Namespace)?.operand;
		let name_reg = self.load(name)?;
		let key = self.object(&[("ns", name)])?;
		let namespace = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		self.must_exist(if_exists, namespace, StatementError::NsNotFound, name_reg)?;
		let ns = self.reg()?;
		self.get_field(namespace, "namespace_id", ns)?;
		self.each_database(ns, |l, db| l.retire_database_indexes(ns, db, expunge))?;
		self.with_service(Service::Broker, |l| {
			l.each_database(ns, |l, db| l.kill_database_subscriptions(ns, db))
		})?;
		self.with_service(Service::Sequences, |l| {
			l.each_database(ns, |l, db| l.evict_database_sequences(ns, db))
		})?;
		self.delete("NamespaceKey", key, expunge);
		// The table migration job's progress and requests name the namespace by an id no later
		// namespace is given. They are left for the job, which deletes those of a namespace that
		// is gone: deleting them here would conflict with a page of the job.
		let (zero, empty) = self.unused_scope()?;
		self.enqueue_reclaim(0, [Operand::Reg(ns), zero, empty, zero], expunge)?;
		self.store(StoreInstr::ClearDatastoreCache);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE DATABASE`: a database of a namespace that does not exist is missing.
	fn remove_database(
		&mut self,
		name: OperandSlot,
		if_exists: bool,
		expunge: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		let name = self.name(name, NameClause::Database)?.operand;
		let name_reg = self.load(name)?;
		let ns_name = self.reg()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let ns_key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key: ns_key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		let no_namespace = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(namespace),
			truthy: false,
			target: no_namespace,
		});
		let ns = self.reg()?;
		self.get_field(namespace, "namespace_id", ns)?;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", name)])?;
		let database = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		let found = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(database),
			truthy: true,
			target: found,
		});
		self.bind(no_namespace)?;
		if if_exists {
			self.answer_none()?;
		} else {
			self.store(StoreInstr::Raise {
				error: StatementError::DbNotFound,
				arg: name_reg,
			});
		}
		self.bind(found)?;
		let db = self.reg()?;
		self.get_field(database, "database_id", db)?;
		self.retire_database_indexes(ns, db, expunge)?;
		self.with_service(Service::Broker, |l| l.kill_database_subscriptions(ns, db))?;
		self.with_service(Service::Sequences, |l| l.evict_database_sequences(ns, db))?;
		self.delete("DatabaseKey", key, expunge);
		// The table migration job's progress and requests name the database by an id no later
		// database is given. They are left for the job, which deletes those of a database that is
		// gone: deleting them here would conflict with a page of the job.
		let (zero, empty) = self.unused_scope()?;
		self.enqueue_reclaim(1, [Operand::Reg(ns), Operand::Reg(db), empty, zero], expunge)?;
		self.store(StoreInstr::ClearDatastoreCache);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE PARAM`, `REMOVE FUNCTION` and `REMOVE BUCKET`: a database-level definition deleted
	/// at the key its definition names.
	fn remove_db_definition(
		&mut self,
		(route, field, ty): (&'static str, &str, &'static str),
		name: DbName,
		if_exists: bool,
		missing: StatementError,
	) -> Result<(), IrError> {
		self.guard()?;
		let (name, shown) = match name {
			DbName::Written(name, shown) => (
				self.constant(Value::String(name.into()))?,
				self.constant(Value::String(shown.into()))?,
			),
			DbName::Named(slot, into) => {
				let name = self.name(slot, into)?.operand;
				(name, name)
			}
		};
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), (field, name)])?;
		let definition = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named(ty),
			dst: definition,
		});
		let shown = self.load(shown)?;
		self.must_exist(if_exists, definition, missing, shown)?;
		let stored_name = self.reg()?;
		self.get_field(definition, "name", stored_name)?;
		let stored_at =
			self.object(&[("ns", ns_op), ("db", db_op), (field, Operand::Reg(stored_name))])?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(route),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE TYPE`: refused when `object` is set and the stored type is not that kind of SQL
	/// object, and while a field, a function or another type names the type.
	fn remove_type(
		&mut self,
		name: &str,
		object: Option<SqlTypeObject>,
		if_exists: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let name = self.constant(Value::String(name.into()))?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", name)])?;
		let definition = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("UserTypeKey"),
			key,
			ty: TypeId::named("StoredTypeDefinition"),
			dst: definition,
		});
		let name_reg = self.load(name)?;
		self.must_exist(if_exists, definition, StatementError::TyNotFound, name_reg)?;
		if let Some(object) = object {
			self.store(StoreInstr::CheckTypeObject {
				definition,
				object,
			});
		}
		self.store(StoreInstr::CheckTypeUnreferenced {
			ns,
			db,
			name: name_reg,
		});
		self.store(StoreInstr::Delete {
			route: RouteId::named("UserTypeKey"),
			key,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE BUCKET`: the definition at `base`, deleted under the name it is stored by.
	fn remove_bucket(
		&mut self,
		name: OperandSlot,
		base: Base,
		if_exists: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		let bu = self.name(name, NameClause::Bucket)?.operand;
		let (route, ns, db) = self.bucket_scope(base, false)?;
		let key = self.bucket_key(ns, db, bu)?;
		let definition = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredBucketDefinition"),
			dst: definition,
		});
		let shown = self.load(bu)?;
		self.must_exist(if_exists, definition, StatementError::BuNotFound, shown)?;
		let stored_name = self.reg()?;
		self.get_field(definition, "name", stored_name)?;
		let stored_at = self.bucket_key(ns, db, Operand::Reg(stored_name))?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(route),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER TYPE`: the stored definition's labels edited by the statement's changes and written
	/// back.
	fn alter_type(
		&mut self,
		attrs: &AlterAttrs,
		name: &str,
		edits: &[crate::exec::ir::iface::statements::EnumEdit],
	) -> Result<(), IrError> {
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let name = self.constant(Value::String(name.into()))?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", name)])?;
		let definition = self.stored_at(
			("UserTypeKey", "StoredTypeDefinition"),
			key,
			attrs.if_exists,
			(StatementError::TyNotFound, name),
		)?;
		self.store(StoreInstr::AlterType {
			definition,
			edits: edits.to_vec(),
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("UserTypeKey"),
			key,
			ty: TypeId::named("StoredTypeDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		if edits
			.iter()
			.any(|edit| matches!(edit, crate::exec::ir::iface::statements::EnumEdit::Rename { .. }))
		{
			let name = self.load(name)?;
			self.store(StoreInstr::RewriteEnumLabels {
				ns,
				db,
				name,
				edits: edits.to_vec(),
			});
		}
		self.answer_none()
	}

	/// `REMOVE EXTENSION`: the definition, at its storage name, and the process's loaded runtime
	/// for it.
	fn remove_module(&mut self, storage: &str, if_exists: bool) -> Result<(), IrError> {
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let name = self.constant(Value::String(storage.into()))?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("md", name)])?;
		let module = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ModuleKey"),
			key,
			ty: TypeId::named("StoredModuleDefinition"),
			dst: module,
		});
		let name_reg = self.load(name)?;
		self.must_exist(if_exists, module, StatementError::MdNotFound, name_reg)?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("ModuleKey"),
			key,
		});
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::EvictModule {
			ns,
			db,
			module,
		});
		self.answer_none()
	}

	/// `REMOVE SEQUENCE`: the service's state, the batches and the state keys, then the
	/// definition.
	fn remove_sequence(&mut self, name: OperandSlot, if_exists: bool) -> Result<(), IrError> {
		self.guard()?;
		let name = self.name(name, NameClause::Sequence)?.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("sq", name)])?;
		let sequence = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("SequenceKey"),
			key,
			ty: TypeId::named("SequenceDefinition"),
			dst: sequence,
		});
		let name_reg = self.load(name)?;
		self.must_exist(if_exists, sequence, StatementError::SeqNotFound, name_reg)?;
		self.with_service(Service::Sequences, |l| {
			l.store(StoreInstr::EvictSequence {
				ns,
				db,
				name: name_reg,
			});
			Ok(())
		})?;
		let stored_name = self.reg()?;
		self.get_field(sequence, "name", stored_name)?;
		let state =
			self.object(&[("ns", ns_op), ("db", db_op), ("sq", Operand::Reg(stored_name))])?;
		self.delete_range("SeqBatchPrefix", state, false);
		self.delete_range("SeqStatePrefix", state, false);
		self.store(StoreInstr::Delete {
			route: RouteId::named("SequenceKey"),
			key,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE CONFIG`: the config of the kind, at the base the kind lives at.
	fn remove_config(&mut self, kind: &ConfigKind, if_exists: bool) -> Result<(), IrError> {
		let base = crate::ddl::define::config::config_kind_base(kind);
		let cg = match kind {
			ConfigKind::GraphQL => "graphql",
			ConfigKind::Api => "api",
			ConfigKind::Default => "default",
		};
		self.guard()?;
		let ty = self.constant(Value::String(cg.into()))?;
		let (route, key) = match base {
			crate::catalog::base::Base::Root => ("RootConfigKey", self.object(&[("ty", ty)])?),
			crate::catalog::base::Base::Db => {
				let (ns, db) = self.session_ids()?;
				(
					"DbConfigKey",
					self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ty", ty)])?,
				)
			}
			crate::catalog::base::Base::Ns => {
				return Err(IrError::Stale("ir_lower::ddl: a namespace config"));
			}
		};
		let found = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			dst: found,
		});
		let name = self.load(ty)?;
		self.must_exist(if_exists, found, StatementError::CgNotFound, name)?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(route),
			key,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE MODEL`: the definition and the stored artifact it names.
	fn remove_model(&mut self, model: &str, version: &str, if_exists: bool) -> Result<(), IrError> {
		self.guard()?;
		let name = self.constant(Value::String(model.into()))?;
		let version_op = self.constant(Value::String(version.into()))?;
		let shown = self.constant(Value::String(format!("{model}<{version}>").into()))?;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("ml", name), ("vn", version_op)])?;
		let definition = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("MlModelKey"),
			key,
			ty: TypeId::named("StoredMlModelDefinition"),
			dst: definition,
		});
		let shown = self.load(shown)?;
		self.must_exist(if_exists, definition, StatementError::MlNotFound, shown)?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("MlModelKey"),
			key,
		});
		self.store(StoreInstr::DeleteModelArtifact {
			model: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE INDEX`: the index is retired, and the table's version is bumped.
	fn remove_index(
		&mut self,
		name: OperandSlot,
		table: OperandSlot,
		if_exists: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		let name = self.name(name, NameClause::Index)?.operand;
		let tb = self.name(table, NameClause::What)?.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", name)])?;
		let index = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			dst: index,
		});
		let name_reg = self.load(name)?;
		self.must_exist(if_exists, index, StatementError::IxNotFound, name_reg)?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table = self.expect_table(tb_key, tb_name)?;
		self.retire_index((ns, db, tb), key, index, table, true)?;
		self.replace_table((ns, db, tb), tb_key, table, "cache_indexes_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE EVENT`: the event is deleted at the key its definition names, and the table is
	/// rewritten with its events stamp refreshed.
	fn remove_event(
		&mut self,
		name: OperandSlot,
		table: OperandSlot,
		if_exists: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		self.session_selected(&[table, name])?;
		let tb = self.name(table, NameClause::Table)?.operand;
		let name = self.name(name, NameClause::Event)?.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ev", name)])?;
		let event = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("EventKey"),
			key,
			ty: TypeId::named("StoredEventDefinition"),
			dst: event,
		});
		let name_reg = self.load(name)?;
		self.must_exist(if_exists, event, StatementError::EvNotFound, name_reg)?;
		// The key the event is removed at is the one its definition names.
		let (target, ev) = (self.reg()?, self.reg()?);
		self.get_field(event, "target_table", target)?;
		self.get_field(event, "name", ev)?;
		let stored_at = self.object(&[
			("ns", ns_op),
			("db", db_op),
			("tb", Operand::Reg(target)),
			("ev", Operand::Reg(ev)),
		])?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("EventKey"),
			key: stored_at,
		});
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table = self.expect_table(tb_key, tb_name)?;
		self.replace_table((ns, db, tb), tb_key, table, "cache_events_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE ANALYZER`: the session's database is resolved before the authority check, and an
	/// analyzer that a full-text index of any table names is refused. The mappers no analyzer
	/// names are dropped from the process afterwards.
	fn remove_analyzer(&mut self, name: OperandSlot, if_exists: bool) -> Result<(), IrError> {
		self.refuse_no_write()?;
		let (ns, db) = self.session_ids()?;
		self.allow()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let name = self.name(name, NameClause::Analyzer)?.operand;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("az", name)])?;
		let analyzer = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("AnalyzerKey"),
			key,
			ty: TypeId::named("AnalyzerDefinition"),
			dst: analyzer,
		});
		let name_reg = self.load(name)?;
		self.must_exist(if_exists, analyzer, StatementError::AzNotFound, name_reg)?;
		let az = self.reg()?;
		self.get_field(analyzer, "name", az)?;
		self.each_table(ns, db, |l, _, tb| {
			let indexes = l.scan(
				"IndexDefPrefix",
				&[("ns", ns_op), ("db", db_op), ("tb", Operand::Reg(tb))],
				"StoredIndexDefinition",
			)?;
			l.each(indexes, |l, index| {
				let next = l.label()?;
				let params = l.reg()?;
				l.get_field(index, "index", params)?;
				l.get_field(params, "FullText", params)?;
				l.emit(Instr::JumpIf {
					cond: Operand::Reg(params),
					truthy: false,
					target: next,
				});
				let used = l.reg()?;
				l.get_field(params, "analyzer", used)?;
				let same = l.reg()?;
				l.emit(Instr::Compare {
					op: CompareOp::Equal,
					left: Operand::Reg(used),
					right: Operand::Reg(az),
					dst: same,
				});
				l.emit(Instr::JumpIf {
					cond: Operand::Reg(same),
					truthy: false,
					target: next,
				});
				let ix = l.reg()?;
				l.get_field(index, "name", ix)?;
				let detail = l.object(&[
					("name", Operand::Reg(az)),
					("table", Operand::Reg(tb)),
					("index", Operand::Reg(ix)),
				])?;
				l.store(StoreInstr::Raise {
					error: StatementError::AzInUse,
					arg: detail,
				});
				l.bind(next)
			})
		})?;
		let stored_at = self.object(&[("ns", ns_op), ("db", db_op), ("az", Operand::Reg(az))])?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("AnalyzerKey"),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		let analyzers =
			self.scan("AnalyzerPrefix", &[("ns", ns_op), ("db", db_op)], "AnalyzerDefinition")?;
		self.store(StoreInstr::CleanupMappers {
			analyzers,
		});
		self.answer_none()
	}

	/// `REMOVE ACCESS`: the method's definition and everything beneath its root, its grants among
	/// them.
	fn remove_access(
		&mut self,
		name: OperandSlot,
		base: Base,
		if_exists: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		let ac = self.name(name, NameClause::Access)?.operand;
		let no_namespace = self.label()?;
		let (scope, missing): (Vec<(&str, Operand)>, _) = match base {
			Base::Root => (Vec::new(), StatementError::AccessRootNotFound),
			Base::Ns => {
				let ns_name = self.reg()?;
				self.store(StoreInstr::SessionNs {
					dst: ns_name,
				});
				let ns_key = self.object(&[("ns", Operand::Reg(ns_name))])?;
				let namespace = self.reg()?;
				self.store(StoreInstr::Get {
					route: RouteId::named("NamespaceKey"),
					key: ns_key,
					ty: TypeId::named("NamespaceDefinition"),
					dst: namespace,
				});
				self.emit(Instr::JumpIf {
					cond: Operand::Reg(namespace),
					truthy: false,
					target: no_namespace,
				});
				let ns = self.reg()?;
				self.get_field(namespace, "namespace_id", ns)?;
				(vec![("ns", Operand::Reg(ns))], StatementError::AccessNsNotFound)
			}
			Base::Db => {
				let (ns, db) = self.session_ids()?;
				(
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))],
					StatementError::AccessDbNotFound,
				)
			}
		};
		let (method, root) = match base {
			Base::Root => ("RootAccessMethodKey", "RootAccessRoot"),
			Base::Ns => ("NsAccessMethodKey", "NsAccessRoot"),
			Base::Db => ("DbAccessMethodKey", "DbAccessRoot"),
		};
		let mut fields = scope.clone();
		fields.push(("ac", ac));
		let key = self.object(&fields)?;
		let access = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(method),
			key,
			ty: TypeId::named("StoredAccessDefinition"),
			dst: access,
		});
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(access),
			truthy: true,
			target: present,
		});
		self.bind(no_namespace)?;
		self.missing_at_base(if_exists, base, ("ac", ac), missing)?;
		self.bind(present)?;
		let stored_name = self.reg()?;
		self.get_field(access, "name", stored_name)?;
		let mut fields = scope;
		fields.push(("ac", Operand::Reg(stored_name)));
		let stored_at = self.object(&fields)?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(method),
			key: stored_at,
		});
		self.delete_range(root, stored_at, false);
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE USER`: the user's definition, and every subscription running as the user in its
	/// scope, removed and told.
	fn remove_user(
		&mut self,
		name: OperandSlot,
		base: Base,
		if_exists: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		let user = self.name(name, NameClause::User)?.operand;
		let no_namespace = self.label()?;
		let (route, scope, missing) = match base {
			Base::Root => ("RootUserKey", None, StatementError::UserRootNotFound),
			Base::Ns => {
				let ns_name = self.reg()?;
				self.store(StoreInstr::SessionNs {
					dst: ns_name,
				});
				let ns_key = self.object(&[("ns", Operand::Reg(ns_name))])?;
				let namespace = self.reg()?;
				self.store(StoreInstr::Get {
					route: RouteId::named("NamespaceKey"),
					key: ns_key,
					ty: TypeId::named("NamespaceDefinition"),
					dst: namespace,
				});
				self.emit(Instr::JumpIf {
					cond: Operand::Reg(namespace),
					truthy: false,
					target: no_namespace,
				});
				let ns = self.reg()?;
				self.get_field(namespace, "namespace_id", ns)?;
				("NsUserKey", Some((ns, None)), StatementError::UserNsNotFound)
			}
			Base::Db => {
				let (ns, db) = self.session_ids()?;
				("DbUserKey", Some((ns, Some(db))), StatementError::UserDbNotFound)
			}
		};
		let fields = |user: Operand| -> Vec<(&'static str, Operand)> {
			match scope {
				None => vec![("user", user)],
				Some((ns, None)) => vec![("ns", Operand::Reg(ns)), ("user", user)],
				Some((ns, Some(db))) => {
					vec![("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("user", user)]
				}
			}
		};
		let key = self.object(&fields(user))?;
		let definition = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			dst: definition,
		});
		let present = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(definition),
			truthy: true,
			target: present,
		});
		self.bind(no_namespace)?;
		self.missing_at_base(if_exists, base, ("name", user), missing)?;
		self.bind(present)?;
		let actor = self.reg()?;
		self.get_field(definition, "name", actor)?;
		match scope {
			None => {
				let namespaces = self.scan("NamespacePrefix", &[], "NamespaceDefinition")?;
				self.each(namespaces, |l, namespace| {
					let ns = l.reg()?;
					l.get_field(namespace, "namespace_id", ns)?;
					l.each_database(ns, |l, db| l.revoke_database_subscriptions(ns, db, actor))
				})?;
			}
			Some((ns, None)) => {
				self.each_database(ns, |l, db| l.revoke_database_subscriptions(ns, db, actor))?;
			}
			Some((ns, Some(db))) => self.revoke_database_subscriptions(ns, db, actor)?,
		}
		let stored_at = self.object(&fields(Operand::Reg(actor)))?;
		self.store(StoreInstr::Delete {
			route: RouteId::named(route),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE API`, for an API whose path renders as the name it is removed by: the definition
	/// is deleted at its path's rendering, which is not always the name the statement gives.
	fn remove_api(&mut self, name: OperandSlot, if_exists: bool) -> Result<(), IrError> {
		self.guard()?;
		let name = self.name(name, NameClause::Api)?.operand;
		let (ns, db) = self.session_ids()?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("ap", name)])?;
		let api = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("ApiKey"),
			key,
			ty: TypeId::named("StoredApiDefinition"),
			dst: api,
		});
		let detail = self.object(&[("value", name)])?;
		self.must_exist(if_exists, api, StatementError::ApNotFound, detail)?;
		let (text, rendered) = (self.reg()?, self.reg()?);
		self.get_field(api, "path", text)?;
		self.store(StoreInstr::ParseApiPath {
			src: text,
			text,
			rendered,
		});
		let stored_at = self.object(&[
			("ns", Operand::Reg(ns)),
			("db", Operand::Reg(db)),
			("ap", Operand::Reg(rendered)),
		])?;
		self.store(StoreInstr::Delete {
			route: RouteId::named("ApiKey"),
			key: stored_at,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	// -----------------------------------------------------------------------------------------
	// ALTER
	// -----------------------------------------------------------------------------------------

	fn alter(&mut self, attrs: &AlterAttrs) -> Result<(), IrError> {
		match &attrs.object {
			AlterObject::Namespace {
				compact,
			} => self.alter_namespace(*compact),
			AlterObject::Database {
				name,
				compact,
				compatibility,
			} => self.alter_database(*name, *compact, *compatibility),
			AlterObject::System {
				query_timeout,
				compact,
				compatibility,
			} => self.alter_system(query_timeout, *compact, *compatibility),
			AlterObject::Table(table) => self.alter_table(attrs, table),
			AlterObject::Api {
				path,
				clauses,
				security,
				comment,
			} => self.alter_api(attrs, *path, clauses, *security, comment),
			AlterObject::Event(event) => self.alter_event(attrs, event.name, event.what),
			AlterObject::Index {
				name,
				table,
				prepare_remove,
				comment,
			} => self.alter_index(attrs.if_exists, (*name, *table), *prepare_remove, comment),
			AlterObject::Sequence {
				name,
				timeout,
			} => self.alter_sequence(attrs.if_exists, *name, *timeout),
			AlterObject::Field(field) => self.alter_field(attrs, field),
			AlterObject::Param {
				name,
				value,
				..
			} => self.alter_param(attrs, name.as_str(), *value),
			AlterObject::Bucket {
				name,
				base,
				backend,
				readonly,
				revoke_signatures,
				..
			} => self.alter_bucket(
				attrs,
				*name,
				*base,
				matches!(backend, AlterKind::Set(_)) || !matches!(readonly, AlterKind::None),
				*revoke_signatures,
			),
			AlterObject::Type {
				name,
				edits,
			} => self.alter_type(attrs, name.as_str(), edits),
			AlterObject::Config {
				inner,
				..
			} => self.alter_config(attrs, inner),
			AlterObject::Analyzer {
				name,
				..
			} => self.alter_analyzer(attrs, *name),
			AlterObject::Function(function) => self.alter_function(attrs, function.name.as_str()),
			AlterObject::User(user) => self.alter_user(attrs, user),
			AlterObject::Access(access) => self.alter_access(attrs, access),
			AlterObject::Module {
				name,
				..
			} => self.alter_module(attrs, &name.get_storage_name()),
		}
	}

	/// `ALTER NAMESPACE`: the session's namespace, which must exist, compacted.
	fn alter_namespace(&mut self, compact: bool) -> Result<(), IrError> {
		self.guard()?;
		let ns_name = self.reg()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		self.expect(namespace, StatementError::NsNotFound, ns_name)?;
		if compact {
			let ns = self.reg()?;
			self.get_field(namespace, "namespace_id", ns)?;
			let root = self.object(&[("ns", Operand::Reg(ns))])?;
			self.store(StoreInstr::Compact {
				route: RouteId::named("NsRoot"),
				key: root,
			});
		}
		self.answer_none()
	}

	/// `ALTER DATABASE`: the named database of the session's namespace, or the session's
	/// database, which must exist: compacted, and set to a compatibility level. Raising the level
	/// to 4 is a change of the definition alone, and queues a verification of the stored rows of
	/// each table that comes to enforce migrations; lowering it changes nothing else.
	fn alter_database(
		&mut self,
		name: Option<OperandSlot>,
		compact: bool,
		compatibility: Option<u8>,
	) -> Result<(), IrError> {
		self.guard()?;
		let (ns, db, database) = match name {
			None => self.selected_ids(StatementError::DbNotFound)?,
			Some(name) => {
				let name = self.name(name, NameClause::Database)?;
				let db_name = self.load(name.operand)?;
				self.named_database_ids(db_name, StatementError::DbNotFound)?
			}
		};
		if compact {
			let root = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))])?;
			self.store(StoreInstr::Compact {
				route: RouteId::named("DbRoot"),
				key: root,
			});
		}
		if let Some(level) = compatibility {
			let previous = self.reg()?;
			self.get_field(database, "compatibility", previous)?;
			let level = self.constant(Value::from(i64::from(level)))?;
			self.set_field(database, "compatibility", level)?;
			let name = self.reg()?;
			self.get_field(database, "name", name)?;
			let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(name))])?;
			self.store(StoreInstr::Put {
				route: RouteId::named("DatabaseKey"),
				key,
				ty: TypeId::named("DatabaseDefinition"),
				value: database,
			});
			let level = self.load(level)?;
			self.store(StoreInstr::VerifyCompatibility {
				ns,
				db,
				previous,
				level,
			});
			self.store(StoreInstr::ClearDatastoreCache);
			self.store(StoreInstr::ClearCache);
		}
		self.answer_none()
	}

	/// `ALTER SYSTEM`: the compaction runs first, and the timeout is evaluated and cast to a
	/// duration once it has; then the compatibility level databases created later take by
	/// default is set.
	fn alter_system(
		&mut self,
		query_timeout: &AlterKind<OperandSlot>,
		compact: bool,
		compatibility: Option<u8>,
	) -> Result<(), IrError> {
		self.guard()?;
		if compact {
			self.store(StoreInstr::CompactAll);
		}
		let timeout = match query_timeout {
			AlterKind::None => None,
			AlterKind::Set(slot) => {
				Some(self.converted(*slot, ClauseConversion::RequiredDuration)?)
			}
			AlterKind::Drop => {
				let none = self.constant(Value::None)?;
				Some(self.load(none)?)
			}
		};
		if let Some(timeout) = timeout {
			self.store(StoreInstr::SetQueryTimeout {
				timeout,
			});
		}
		if let Some(level) = compatibility {
			let level = self.constant(Value::from(i64::from(level)))?;
			let level = self.load(level)?;
			self.store(StoreInstr::SetDefaultCompatibility {
				level,
			});
		}
		self.answer_none()
	}

	/// `ALTER SEQUENCE`: a timeout written as a duration or `NONE` is a constant; any other is
	/// evaluated once the sequence is read, and cast to a duration or none.
	fn alter_sequence(
		&mut self,
		if_exists: bool,
		name: OperandSlot,
		timeout: Option<OperandSlot>,
	) -> Result<(), IrError> {
		self.guard()?;
		let sq = self.name(name, NameClause::Sequence)?.operand;
		let (ns, db) = self.session_ids()?;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("sq", sq)])?;
		let sequence = self.stored_at(
			("SequenceKey", "SequenceDefinition"),
			key,
			if_exists,
			(StatementError::SeqNotFound, sq),
		)?;
		if let Some(slot) = timeout {
			let (timeout, _) = self.duration(slot, false)?;
			self.set_field(sequence, "timeout", timeout)?;
		}
		self.store(StoreInstr::Put {
			route: RouteId::named("SequenceKey"),
			key,
			ty: TypeId::named("SequenceDefinition"),
			value: sequence,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER INDEX`: the definition and its id's name lookup are rewritten, and the table is
	/// rewritten with its indexes stamp refreshed.
	fn alter_index(
		&mut self,
		if_exists: bool,
		(name, table): (OperandSlot, OperandSlot),
		prepare_remove: bool,
		comment: &AlterKind<String>,
	) -> Result<(), IrError> {
		self.guard()?;
		let ix = self.name(name, NameClause::Index)?.operand;
		let tb = self.name(table, NameClause::Table)?.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", ix)])?;
		let index = self.stored_at(
			("IndexDefKey", "StoredIndexDefinition"),
			key,
			if_exists,
			(StatementError::IxNotFound, ix),
		)?;
		match comment {
			AlterKind::Set(comment) => {
				let comment = self.constant(Value::String(comment.as_str().into()))?;
				self.set_field(index, "comment", comment)?;
			}
			AlterKind::Drop => {
				let none = self.constant(Value::None)?;
				self.set_field(index, "comment", none)?;
			}
			AlterKind::None => {}
		}
		if prepare_remove {
			let yes = self.constant(Value::Bool(true))?;
			self.set_field(index, "prepare_remove", yes)?;
		}
		self.store(StoreInstr::AdmitIndexFormat {
			definition: index,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			value: index,
		});
		let (id, stored_name) = (self.reg()?, self.reg()?);
		self.get_field(index, "index_id", id)?;
		self.get_field(index, "name", stored_name)?;
		let lookup =
			self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", Operand::Reg(id))])?;
		self.store(StoreInstr::Put {
			route: RouteId::named("IndexNameKey"),
			key: lookup,
			ty: TypeId::UTF8,
			value: stored_name,
		});
		self.stamp(ns, db, tb)?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table_def = self.expect_table(tb_key, tb_name)?;
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_indexes_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// The template the alteration `attrs` carries.
	fn alter_template(attrs: &AlterAttrs) -> Result<&AlterTemplate, IrError> {
		attrs
			.template
			.as_ref()
			.ok_or(IrError::Stale("ir_lower::ddl: an alteration with no template"))
	}

	/// Replaces the fields of the definition in `definition` that the alteration's template names
	/// with those of its value.
	fn replace_fields(&mut self, definition: Reg, template: &AlterTemplate) -> Result<(), IrError> {
		for field in &template.fields {
			let value = if let Value::Object(altered) = &template.value {
				altered.get(field.as_str()).cloned().unwrap_or(Value::None)
			} else {
				Value::None
			};
			let value = self.constant(value)?;
			self.set_field(definition, field.as_str(), value)?;
		}
		Ok(())
	}

	/// Refuses, where it runs, the alteration of `subject` in the database `ns`/`db` when a type
	/// the clauses it sets name is not one the database defines, as `DEFINE` refuses the
	/// definition. The template decodes to the definition the statement forms, whose clauses the
	/// statement leaves unset name no type; `named` reads the types it names. Emitted once the
	/// altered definition is known to exist, so an `IF EXISTS` that finds nothing judges nothing.
	fn check_altered_types<D>(
		&mut self,
		template: &AlterTemplate,
		database: (Reg, Reg),
		named: impl FnOnce(&D) -> Vec<String>,
		subject: impl FnOnce() -> String,
	) -> Result<(), IrError>
	where
		D: crate::catalog::FromStored,
		D::Stored: crate::key::KVValue<KeyContext = ()>,
	{
		let altered: D = definition_of(&template.value, template.ty)?;
		self.check_types_defined(database, named(&altered), subject);
		Ok(())
	}

	/// `ALTER PARAM`: a value is evaluated once the parameter is read.
	fn alter_param(
		&mut self,
		attrs: &AlterAttrs,
		param: &str,
		value: Option<OperandSlot>,
	) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let name = self.constant(Value::String(param.into()))?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("pa", name)])?;
		let definition = self.stored_at(
			("ParamKey", "StoredParamDefinition"),
			key,
			attrs.if_exists,
			(StatementError::PaNotFound, name),
		)?;
		self.check_altered_types(
			template,
			(ns, db),
			|altered: &crate::catalog::ParamDefinition| {
				super::super::schema_host::permission_named_types(&altered.permissions)
			},
			|| format!("The parameter '${param}'"),
		)?;
		if let Some(slot) = value {
			let value = self.evaluated(slot)?;
			self.store(StoreInstr::CheckDepth {
				value,
			});
			self.set_field(definition, "value", Operand::Reg(value))?;
		}
		self.replace_fields(definition, template)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ParamKey"),
			key,
			ty: TypeId::named("StoredParamDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER FUNCTION`: the definition is stamped with the session's authority.
	/// `ALTER EVENT`: the clauses the template names replace the event's, which is stamped with
	/// the session's authority, and the table, when it exists, is rewritten with its events
	/// stamp refreshed.
	fn alter_event(
		&mut self,
		attrs: &AlterAttrs,
		name: OperandSlot,
		table: OperandSlot,
	) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		self.guard()?;
		let ev = self.name(name, NameClause::Event)?.operand;
		let tb = self.name(table, NameClause::Table)?.operand;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ev", ev)])?;
		let event = self.stored_at(
			("EventKey", "StoredEventDefinition"),
			key,
			attrs.if_exists,
			(StatementError::EvNotFound, ev),
		)?;
		let subject = self.shown_subject("The event", name)?;
		self.check_altered_types(
			template,
			(ns, db),
			super::super::schema_host::event_named_types,
			|| subject,
		)?;
		self.replace_fields(event, template)?;
		self.session_auth_limit(event)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("EventKey"),
			key,
			ty: TypeId::named("StoredEventDefinition"),
			value: event,
		});
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let table_def = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key: tb_key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: table_def,
		});
		let undefined = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(table_def),
			truthy: false,
			target: undefined,
		});
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_events_ts")?;
		self.bind(undefined)?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	fn alter_function(&mut self, attrs: &AlterAttrs, function: &str) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let name = self.constant(Value::String(function.into()))?;
		let key =
			self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("fc", name)])?;
		let shown = self.constant(Value::String(format!("fn::{function}").into()))?;
		let definition = self.stored_at(
			("FunctionKey", "StoredFunctionDefinition"),
			key,
			attrs.if_exists,
			(StatementError::FcNotFound, shown),
		)?;
		self.check_altered_types(
			template,
			(ns, db),
			super::super::schema_host::function_named_types,
			|| format!("The function 'fn::{function}'"),
		)?;
		self.replace_fields(definition, template)?;
		self.session_auth_limit(definition)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("FunctionKey"),
			key,
			ty: TypeId::named("StoredFunctionDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER BUCKET`: the session's database is resolved before the name.
	fn alter_bucket(
		&mut self,
		attrs: &AlterAttrs,
		name: OperandSlot,
		base: Base,
		reconnects: bool,
		revoke_signatures: bool,
	) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		self.guard()?;
		let (route, ns, db) = self.bucket_scope(base, false)?;
		let bu = self.name(name, NameClause::Bucket)?.operand;
		let key = self.bucket_key(ns, db, bu)?;
		let definition = self.stored_at(
			(route, "StoredBucketDefinition"),
			key,
			attrs.if_exists,
			(StatementError::BuNotFound, bu),
		)?;
		// A named type is a type of a database, so only a database bucket's permission can name
		// one.
		if let (Some(ns), Some(db)) = (ns, db) {
			let subject = self.shown_subject("The bucket", name)?;
			self.check_altered_types(
				template,
				(ns, db),
				|altered: &crate::catalog::BucketDefinition| {
					super::super::schema_host::permission_named_types(&altered.permissions)
				},
				|| subject,
			)?;
		}
		self.replace_fields(definition, template)?;
		// A new backend, or a new mode a provider may open its handle in, is
		// connected before it is stored.
		if reconnects {
			self.store(StoreInstr::ConnectBucket {
				ns,
				db,
				bucket: definition,
			});
		}
		if revoke_signatures {
			let nonce = self.reg()?;
			self.store(StoreInstr::NewUuid {
				dst: nonce,
				draw: 0,
				reserved: None,
			});
			self.set_field(definition, "presign_nonce", Operand::Reg(nonce))?;
		}
		self.store(StoreInstr::Put {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("StoredBucketDefinition"),
			value: definition,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER ANALYZER`: the altered analyzer is loaded for the process, without its mappers,
	/// before it is written.
	fn alter_analyzer(&mut self, attrs: &AlterAttrs, name: OperandSlot) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let az = self.name(name, NameClause::Analyzer)?.operand;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("az", az)])?;
		let analyzer = self.stored_at(
			("AnalyzerKey", "AnalyzerDefinition"),
			key,
			attrs.if_exists,
			(StatementError::AzNotFound, az),
		)?;
		self.replace_fields(analyzer, template)?;
		self.store(StoreInstr::LoadAnalyzer {
			analyzer,
			mappers: false,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("AnalyzerKey"),
			key,
			ty: TypeId::named("AnalyzerDefinition"),
			value: analyzer,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER EXTENSION`: the extension at its storage name, rewritten at that name.
	fn alter_module(&mut self, attrs: &AlterAttrs, storage: &str) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let md = self.constant(Value::String(storage.into()))?;
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("md", md)])?;
		let module = self.stored_at(
			("ModuleKey", "StoredModuleDefinition"),
			key,
			attrs.if_exists,
			(StatementError::MdNotFound, md),
		)?;
		self.check_altered_types(
			template,
			(ns, db),
			|altered: &crate::catalog::ModuleDefinition| {
				super::super::schema_host::permission_named_types(&altered.permissions)
			},
			|| format!("The module '{storage}'"),
		)?;
		self.replace_fields(module, template)?;
		self.store(StoreInstr::Put {
			route: RouteId::named("ModuleKey"),
			key,
			ty: TypeId::named("StoredModuleDefinition"),
			value: module,
		});
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	// -----------------------------------------------------------------------------------------
	// REBUILD
	// -----------------------------------------------------------------------------------------

	/// `REBUILD INDEX`: the index is looked for before its table, so `IF EXISTS` answers `NONE`
	/// for an index on a table that does not exist too. The definition is rewritten at the
	/// current format version and the build runs, blocking unless `CONCURRENTLY`.
	fn rebuild(&mut self, attrs: &RebuildAttrs) -> Result<(), IrError> {
		self.guard()?;
		let name = self.constant(Value::String(attrs.index.as_str().into()))?;
		let tb = self.constant(Value::String(attrs.table.as_str().into()))?;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("ix", name)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("IndexDefKey"),
			key,
			ty: TypeId::named("StoredIndexDefinition"),
			dst: existing,
		});
		let name_reg = self.load(name)?;
		self.must_exist(attrs.if_exists, existing, StatementError::IxNotFound, name_reg)?;
		let tb_name = self.load(tb)?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let table_def = self.expect_table(tb_key, tb_name)?;
		let tb_reg = self.load(tb)?;
		self.store(StoreInstr::ClaimDocIdsReclaim {
			ns,
			db,
			tb: tb_reg,
			upgrading: Some(existing),
		});
		let format_version = self.constant(Value::from(crate::catalog::INDEX_FORMAT_VERSION))?;
		self.set_field(existing, "format_version", format_version)?;
		self.put_index_definition((ns, db, tb), key, existing, name)?;
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_indexes_ts")?;
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::BuildIndex {
			table: table_def,
			index: existing,
			blocking: !attrs.concurrently,
			fresh: false,
			prior: None,
		});
		self.answer_none()
	}
}

/// A stored definition as the VM's codec reads it.
fn stored<T: crate::key::KVValue>(value: &T, ty: &'static str) -> Result<Value, IrError> {
	decode_stored(value.kv_encode_value(), ty)
}

/// [`stored`] over the bytes the definition encoded to.
fn decode_stored(bytes: anyhow::Result<Vec<u8>>, ty: &'static str) -> Result<Value, IrError> {
	let bytes = bytes
		.map_err(|_| IrError::Stale("ir_lower::ddl: a stored definition that does not encode"))?;
	crate::exec::vm::codec::decode(&crate::exec::vm::codec::FULL_CODEC, ty, &bytes)
		.map_err(|_| IrError::Stale("ir_lower::ddl: a stored definition the codec does not read"))
}

/// The integer a clause is written as, when it is written as one.
fn written_integer(value: Option<&Value>) -> Option<i64> {
	match value {
		Some(Value::Number(crate::val::Number::Int(written))) => Some(*written),
		Some(_) | None => None,
	}
}

/// The name a module is stored under: its written name, or the silo package release it names.
fn module_storage_name(
	name: &Option<String>,
	executable: &ModuleExecutable,
) -> Result<String, IrError> {
	match (name, executable) {
		(Some(name), _) => Ok(format!("mod::{name}")),
		(None, ModuleExecutable::Silo(silo)) => Ok(format!(
			"silo::{}::{}::<{}.{}.{}>",
			silo.organisation, silo.package, silo.major, silo.minor, silo.patch
		)),
		(None, ModuleExecutable::Surrealism(_)) => {
			Err(IrError::Stale("ir_lower::ddl: a module without a name"))
		}
	}
}

/// A changefeed clause as the codec reads a stored one.
fn changefeed_value(cf: &crate::expr::ChangeFeed) -> Value {
	let mut object = crate::val::Object::default();
	object.insert("expiry".to_owned(), Value::Duration(crate::val::Duration(cf.expiry.0)));
	object.insert("store_diff".to_owned(), Value::Bool(cf.store_diff));
	Value::Object(object)
}

/// The function definition the definition template `template` holds, as the catalog compiles it.
fn function_definition_of(template: &Value) -> Result<crate::catalog::FunctionDefinition, IrError> {
	definition_of(template, "StoredFunctionDefinition")
}

/// The runtime definition the template `template`, an encoding of the stored type `ty`, decodes
/// to: the definition as the statement writes it, with its holes at their defaults.
pub(super) fn definition_of<D>(template: &Value, ty: &'static str) -> Result<D, IrError>
where
	D: crate::catalog::FromStored,
	D::Stored: crate::key::KVValue<KeyContext = ()>,
{
	use crate::key::KVValue as _;
	let bytes = crate::exec::vm::codec::encode(&crate::exec::vm::codec::FULL_CODEC, ty, template)
		.map_err(|_| {
		IrError::Stale("ir_lower::ddl: a definition template that does not encode")
	})?;
	let stored = D::Stored::kv_decode_value(&bytes, ())
		.map_err(|_| IrError::Stale("ir_lower::ddl: a definition template that does not decode"))?;
	D::from_stored(&stored)
		.map_err(|_| IrError::Stale("ir_lower::ddl: a definition template that does not compile"))
}

#[cfg(test)]
mod tests {
	use crate::exec::operators::test_util::TestDb;

	/// The value of the one statement `sql` runs, as SurrealQL.
	async fn answer(db: &TestDb, sql: &str) -> String {
		use surrealdb_types::ToSql;
		let mut responses =
			db.datastore().execute(sql, &TestDb::owner(), None).await.expect("the query runs");
		responses.remove(0).result.expect("the statement answers").to_sql()
	}

	/// A `{ ... }` block read as a clause's value is evaluated as the operand's expression, both
	/// where the schema statement is the program's top statement and where it is one statement of
	/// a block, and the definition holds its value.
	#[tokio::test]
	async fn a_block_is_a_clause_value_of_a_schema_statement() {
		let db = TestDb::new("").await;
		db.run(
			"DEFINE TABLE a COMMENT { 'x' };
			DEFINE TABLE b COMMENT {;};
			DEFINE PARAM $p VALUE { LET $v = 2; $v * 3 };
			ALTER PARAM $p VALUE { 7 };
			{ DEFINE TABLE c COMMENT { 'y' } };
			IF true { DEFINE FIELD f ON c DEFAULT { 1 } COMMENT { 'z' } };",
		)
		.await;
		assert_eq!(
			answer(&db, "(INFO FOR DB).tables").await,
			"{ a: \"DEFINE TABLE a TYPE ANY SCHEMALESS COMMENT 'x' PERMISSIONS NONE\", \
			 b: 'DEFINE TABLE b TYPE ANY SCHEMALESS PERMISSIONS NONE', \
			 c: \"DEFINE TABLE c TYPE ANY SCHEMALESS COMMENT 'y' PERMISSIONS NONE\" }"
		);
		assert_eq!(
			answer(&db, "(INFO FOR DB).params.p").await,
			"'DEFINE PARAM $p VALUE 7 PERMISSIONS FULL'"
		);
		assert_eq!(
			answer(&db, "(INFO FOR TABLE c).fields.f").await,
			"\"DEFINE FIELD f ON c DEFAULT { 1 } COMMENT 'z' PERMISSIONS FULL\""
		);
	}
}
