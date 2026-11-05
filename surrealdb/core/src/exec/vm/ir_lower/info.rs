//! Lowering of `phys.info`: an `INFO` statement as one scalar pipeline of store instructions.
//!
//! The pipeline spells out the steps the statement takes, in this order: the permission check, the
//! operands the subject names (the name it reports on, then the `VERSION`), and then, for each kind
//! of definition the subject lists, the scan of its prefix and the rendering of what the scan
//! returned, gathered into the object the statement answers. The program requires the context level
//! the subject needs, which the executor resolves, raising for a namespace or database that does
//! not exist, before the program runs.
//!
//! # Contract
//!
//! - The permission check is the first instruction on every path, so no operand is evaluated for a
//!   session that may not view the subject. `INFO FOR USER` and `INFO FOR INDEX` check the actor's
//!   view of the base; every other subject checks the view of anything at its base.
//! - A name operand is read as a string. One that is a string literal (a name written bare binds as
//!   one) is loaded as it stands; any other is evaluated and coerced to a string.
//! - A `VERSION` operand is evaluated after the name, coerced to a version stamp, and every scan
//!   and rendering of the object reads the catalog as it stood at that stamp. The cluster's nodes,
//!   which are not versioned, are the one part that does not.
//! - `INFO FOR USER` with no written base holds one branch per base, chosen by the base the session
//!   selects when it runs, each branch evaluating the name itself. The branches share no register,
//!   so each operand is lowered afresh inside its branch.
//! - Every operand is lowered by a lowerer of its own, so a value read by two branches is computed
//!   in each of them and never reused across a jump.
//!
//! # What lowering decides
//!
//! Nothing. The subject, whether the answer is structured, the base and the operand order are
//! attributes of the op. The context level follows from the subject and from what the operand calls
//! need, and the program is read-only.
//!
//! # `KILL`
//!
//! [`lower_kill`] lowers a `phys.kill` as one scalar pipeline that takes the statement's steps in
//! its order: the checks that precede the evaluation of the id ([`StoreInstr::RealtimeChecks`]),
//! the id operand, and the removal of the live query it names ([`StoreInstr::Kill`]), which answers
//! `NONE` as the statement does. The id as the statement wrote it is the surface text the op's
//! location carries, which the refusals that name the statement print; a `KILL` whose text the
//! binder did not record is [`IrError::Stale`]. The program writes, since the removal deletes the
//! subscription's keys, and it requires no context level of its own: the checks raise for a
//! session that selects no namespace or database.
//!
//! # `LIVE`
//!
//! [`lower_live`] lowers a `phys.live` as one scalar pipeline: the checks, the target operand, the
//! constant the op carries as the subscription's stored definition, and [`StoreInstr::Live`], whose
//! result, the id of the live query, the statement answers. The stored clauses of the statement
//! (the projection, the filter and the fetch paths) are code the subscription stores and never
//! runs here: they are the text of the template, and their regions are not lowered. The
//! template's holes (the node, the authentication and session of the client and the variables the
//! clauses capture) are what the running session fills, which [`StoreInstr::Live`] does; a
//! template of another stored type, or with other holes, is [`IrError::Stale`]. The program writes,
//! and requires no context level of its own.
//!
//! # `OPTION`, `USE` and a session `LET`
//!
//! These three statements change the request's session, which the executor owns. Each lowers as one
//! scalar pipeline whose instructions do the part that belongs to the statement's transaction and
//! record the rest as a session effect ([`crate::exec::context::SessionEffect`]) for the executor
//! to apply when the program ends. They lower only as a statement of the request's top-level list,
//! and only a program run in the context of such a statement holds the sink they record in.
//!
//! [`lower_option`] is [`StoreInstr::SessionOption`] and a `NONE` answer. [`lower_use`] evaluates
//! the operands the target names, in order, then [`StoreInstr::SessionUse`], whose answer is the
//! row `{ namespace, database }`; it requires no context level of its own and its program writes,
//! since the namespace and database it selects may be created. [`lower_let`] is the refusal of a
//! protected name ([`StoreInstr::SessionLetChecks`], before anything is evaluated), the value
//! operand, and [`StoreInstr::SessionLet`], which coerces the value to the declared kind, checks
//! its depth and records it; the statement answers `NONE`.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::codec::TypeId;
use super::super::codec::key::RouteId;
use super::super::dispatch::Reach;
use super::super::program::{
	Definitions, Instr, Operand, Pipeline, ProcessPart, Reg, Source, StatementError, StoreInstr,
};
use super::emit::Emitter;
use super::expr::{Claim, Lowerer, Needs};
use super::units::LowerCx;
use crate::exec::field_path::FieldPath;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::statements::{InfoSubject, TemplateHole};
use crate::exec::ir::logical::ops::{LetBinding, UseTarget};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// One definition kind of an `INFO` object: the route of its prefix, the type its entries decode
/// as, and the kind that renders them.
type Part = (&'static str, &'static str, Definitions);

/// What lowering a `phys.info` produced.
pub(crate) struct InfoLowered {
	/// The one pipeline of the statement, over no source.
	pub(super) pipeline: Pipeline,
	/// What the program needs of its run: the context level the subject and its operands require,
	/// and whether its calls keep row order.
	pub(super) needs: Needs,
	/// The instructions each operand op emitted for itself.
	pub(super) claims: Vec<Claim>,
}

/// Lowers the `phys.info` `op` of `ir` into `em`, and ends the pipeline it emitted.
///
/// The operands of the op are, in order: the table (`Table`), the user (`User`), the index and its
/// table (`Index`), and then the `VERSION` value for the subjects that take one (`Root`, `Ns`,
/// `Db` and `Table`). An operand count the subject does not take is [`IrError::Stale`].
pub(super) fn lower_info(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	op: OpId,
) -> Result<InfoLowered, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::info: op"))?;
	let OpKind::Phys(PhysOp::Info {
		subject,
		structure,
	}) = &data.kind
	else {
		return Err(IrError::Stale("ir_lower::info: not a phys.info"));
	};
	let structured = *structure;
	let mut info = InfoLowerer {
		em,
		cx,
		ir,
		delegate: ContextLevel::Root,
		needs: ContextLevel::Root,
		keeps_row_order: false,
		reach: None,
		access_mode: AccessMode::ReadOnly,
		claims: Vec::new(),
	};
	let mut operands = data.operands.iter().copied();
	match subject {
		InfoSubject::Root => info.root(structured, operands.next())?,
		InfoSubject::Ns => info.ns(structured, operands.next())?,
		InfoSubject::Db => info.db(structured, operands.next())?,
		InfoSubject::Table => {
			let table = operands.next().ok_or(IrError::Stale("ir_lower::info: operand arity"))?;
			info.table(table, structured, operands.next())?;
		}
		InfoSubject::User {
			base,
		} => {
			let user = operands.next().ok_or(IrError::Stale("ir_lower::info: operand arity"))?;
			info.user(user, *base, structured)?;
		}
		InfoSubject::Index => {
			let index = operands.next().ok_or(IrError::Stale("ir_lower::info: operand arity"))?;
			let table = operands.next().ok_or(IrError::Stale("ir_lower::info: operand arity"))?;
			info.index(index, table)?;
		}
	}
	if operands.next().is_some() {
		return Err(IrError::Stale("ir_lower::info: operand arity"));
	}
	let InfoLowerer {
		em,
		delegate,
		needs,
		keeps_row_order,
		reach,
		access_mode,
		claims,
		..
	} = info;
	Ok(InfoLowered {
		pipeline: em.finish_pipeline(Source::Once)?,
		needs: Needs {
			context: delegate.max(needs),
			keeps_row_order,
			reach,
			access_mode,
		},
		claims,
	})
}

/// Lowers the `phys.kill` `op` of `ir` into `em`, and ends the pipeline it emitted.
///
/// The op's one operand is the id of the live query. Answers [`IrError::Stale`] for an operand
/// count other than one and for an op whose location holds no written id.
pub(super) fn lower_kill(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	op: OpId,
) -> Result<InfoLowered, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::info: op"))?;
	if !matches!(&data.kind, OpKind::Phys(PhysOp::Kill)) {
		return Err(IrError::Stale("ir_lower::info: not a phys.kill"));
	}
	let [id] = data.operands.as_slice() else {
		return Err(IrError::Stale("ir_lower::info: phys.kill operand arity"));
	};
	let written = data.loc.text().ok_or(IrError::Stale(
		"ir_lower::info: a KILL whose written id the binder did not record",
	))?;
	let mut kill = InfoLowerer {
		em,
		cx,
		ir,
		delegate: ContextLevel::Root,
		needs: ContextLevel::Root,
		keeps_row_order: false,
		reach: None,
		access_mode: AccessMode::ReadWrite,
		claims: Vec::new(),
	};
	kill.store(StoreInstr::RealtimeChecks);
	let id = kill.operand(*id)?;
	let id = kill.register_of(id)?;
	let written = kill.constant(Value::String(written.into()))?;
	let written = kill.load(written)?;
	kill.store(StoreInstr::Kill {
		id,
		written,
	});
	kill.answer_none()?;
	let InfoLowerer {
		em,
		delegate,
		needs,
		keeps_row_order,
		reach,
		access_mode,
		claims,
		..
	} = kill;
	Ok(InfoLowered {
		pipeline: em.finish_pipeline(Source::Once)?,
		needs: Needs {
			context: delegate.max(needs),
			keeps_row_order,
			reach,
			access_mode,
		},
		claims,
	})
}

/// Lowers the `phys.live` `op` of `ir` into `em`, and ends the pipeline it emitted.
///
/// The op's one operand is the target of the subscription. Answers [`IrError::Stale`] for an
/// operand count other than one and for a template that is not a live subscription's.
pub(super) fn lower_live(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	op: OpId,
) -> Result<InfoLowered, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::info: op"))?;
	let OpKind::Phys(PhysOp::Live(attrs)) = &data.kind else {
		return Err(IrError::Stale("ir_lower::info: not a phys.live"));
	};
	let [target] = data.operands.as_slice() else {
		return Err(IrError::Stale("ir_lower::info: phys.live operand arity"));
	};
	let template = &attrs.template;
	if template.ty != "StoredSubscriptionDefinition"
		|| template.holes.as_slice()
			!= [
				TemplateHole::Other("node"),
				TemplateHole::Other("auth"),
				TemplateHole::Other("session"),
				TemplateHole::Other("vars"),
			] {
		return Err(IrError::Stale("ir_lower::info: a phys.live template of another shape"));
	}
	let mut live = InfoLowerer {
		em,
		cx,
		ir,
		delegate: ContextLevel::Root,
		needs: ContextLevel::Root,
		keeps_row_order: false,
		reach: None,
		access_mode: AccessMode::ReadWrite,
		claims: Vec::new(),
	};
	live.store(StoreInstr::RealtimeChecks);
	let target = live.operand(*target)?;
	let target = live.register_of(target)?;
	let template = live.constant(template.value.clone())?;
	let template = live.load(template)?;
	let dst = live.em.reg()?;
	live.store(StoreInstr::Live {
		target,
		template,
		dst,
	});
	live.emit(Instr::Emit {
		src: dst,
	});
	live.emit(Instr::EndRow);
	let InfoLowerer {
		em,
		delegate,
		needs,
		keeps_row_order,
		reach,
		access_mode,
		claims,
		..
	} = live;
	Ok(InfoLowered {
		pipeline: em.finish_pipeline(Source::Once)?,
		needs: Needs {
			context: delegate.max(needs),
			keeps_row_order,
			reach,
			access_mode,
		},
		claims,
	})
}

/// A lowerer of one statement of the session family, which reads what it needs of its operands
/// through the same lowerer as `INFO` and ends the pipeline it emitted.
fn session_lowerer<'a, 'b>(
	em: &'a mut Emitter,
	cx: &'a mut LowerCx<'b>,
	ir: &'a Ir,
	access_mode: AccessMode,
) -> InfoLowerer<'a, 'b> {
	InfoLowerer {
		em,
		cx,
		ir,
		delegate: ContextLevel::Root,
		needs: ContextLevel::Root,
		keeps_row_order: false,
		reach: None,
		access_mode,
		claims: Vec::new(),
	}
}

impl InfoLowerer<'_, '_> {
	/// A register of its own that holds the name the `USE` operand `value` gives: the operand
	/// evaluated, copied, and read as a string in place.
	fn name_of(&mut self, value: ValueId, database: bool) -> Result<Reg, IrError> {
		let operand = self.operand(value)?;
		let reg = self.load(operand)?;
		self.store(StoreInstr::SessionUseName {
			value: reg,
			database,
		});
		Ok(reg)
	}

	/// Ends the pipeline of a session statement, whatever the statement emitted.
	fn finish(self) -> Result<InfoLowered, IrError> {
		let InfoLowerer {
			em,
			delegate,
			needs,
			keeps_row_order,
			reach,
			access_mode,
			claims,
			..
		} = self;
		Ok(InfoLowered {
			pipeline: em.finish_pipeline(Source::Once)?,
			needs: Needs {
				context: delegate.max(needs),
				keeps_row_order,
				reach,
				access_mode,
			},
			claims,
		})
	}
}

/// Refuses to lower a statement that writes the request's session into a unit.
fn refuse_in_unit(cx: &LowerCx<'_>) -> Result<(), IrError> {
	match cx.in_unit() {
		true => Err(IrError::Invariant {
			what: "a statement that writes the request's session is never lowered into a unit",
		}),
		false => Ok(()),
	}
}

/// Lowers the `phys.option` `op` of `ir` into `em`, and ends the pipeline it emitted.
///
/// The statement has no operand. The program only reads: it records the option for the executor.
pub(super) fn lower_option(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	op: OpId,
) -> Result<InfoLowered, IrError> {
	// SECURITY: a statement that writes the request's session is lowered only for a statement of
	// the request, so the program of a unit (a function body, a closure, an event, a subscription)
	// never holds one.
	refuse_in_unit(cx)?;
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::info: op"))?;
	let OpKind::Phys(PhysOp::Option {
		name,
		value,
	}) = &data.kind
	else {
		return Err(IrError::Stale("ir_lower::info: not a phys.option"));
	};
	if !data.operands.is_empty() {
		return Err(IrError::Stale("ir_lower::info: phys.option operand arity"));
	}
	let mut option = session_lowerer(em, cx, ir, AccessMode::ReadOnly);
	option.store(StoreInstr::SessionOption {
		name: name.clone(),
		what: *value,
	});
	option.answer_none()?;
	option.finish()
}

/// Lowers the `phys.use` `op` of `ir` into `em`, and ends the pipeline it emitted.
///
/// The operands are the namespace (`Ns`, `NsDb`) and then the database (`Db`, `NsDb`); `Default`
/// has none. Each is evaluated before the instruction reads it, in the order the statement wrote
/// them. An operand count the target does not take is [`IrError::Stale`].
pub(super) fn lower_use(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	op: OpId,
) -> Result<InfoLowered, IrError> {
	// SECURITY: a statement that writes the request's session is lowered only for a statement of
	// the request, so the program of a unit (a function body, a closure, an event, a subscription)
	// never holds one.
	refuse_in_unit(cx)?;
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::info: op"))?;
	let OpKind::Phys(PhysOp::Use {
		target,
	}) = &data.kind
	else {
		return Err(IrError::Stale("ir_lower::info: not a phys.use"));
	};
	let mut operands = data.operands.iter().copied();
	let (ns, db) = match (target, operands.next(), operands.next(), operands.next()) {
		(UseTarget::Ns, Some(ns), None, None) => (Some(ns), None),
		(UseTarget::Db, Some(db), None, None) => (None, Some(db)),
		(UseTarget::NsDb, Some(ns), Some(db), None) => (Some(ns), Some(db)),
		(UseTarget::Default, None, None, None) => (None, None),
		_ => return Err(IrError::Stale("ir_lower::info: phys.use operand arity")),
	};
	let mut used = session_lowerer(em, cx, ir, AccessMode::ReadWrite);
	// Each operand is evaluated and read as a name before the next is evaluated, as the statement
	// run as written does, so a namespace that is no string raises before the database is
	// evaluated.
	let ns = match ns {
		Some(ns) => Some(used.name_of(ns, false)?),
		None => None,
	};
	let db = match db {
		Some(db) => Some(used.name_of(db, true)?),
		None => None,
	};
	let dst = used.em.reg()?;
	used.store(StoreInstr::SessionUse {
		default: matches!(target, UseTarget::Default),
		ns,
		db,
		dst,
	});
	used.emit(Instr::Emit {
		src: dst,
	});
	used.emit(Instr::EndRow);
	used.finish()
}

/// Lowers the session `phys.let` `op` of `ir` into `em`, and ends the pipeline it emitted.
///
/// The op's one operand is the value. A `LET` that binds a local name is not a statement of the
/// request and is [`IrError::Stale`] here.
pub(super) fn lower_let(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	op: OpId,
) -> Result<InfoLowered, IrError> {
	// SECURITY: a statement that writes the request's session is lowered only for a statement of
	// the request, so the program of a unit (a function body, a closure, an event, a subscription)
	// never holds one.
	refuse_in_unit(cx)?;
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::info: op"))?;
	let OpKind::Phys(PhysOp::Let(attrs)) = &data.kind else {
		return Err(IrError::Stale("ir_lower::info: not a phys.let"));
	};
	if attrs.binding != LetBinding::Session {
		return Err(IrError::Stale("ir_lower::info: a local phys.let outside a statement program"));
	}
	let [value] = data.operands.as_slice() else {
		return Err(IrError::Stale("ir_lower::info: phys.let operand arity"));
	};
	let mut bound = session_lowerer(em, cx, ir, AccessMode::ReadOnly);
	bound.store(StoreInstr::SessionLetChecks {
		name: attrs.name.clone(),
	});
	let value = bound.operand(*value)?;
	let value = bound.register_of(value)?;
	let kind = match &attrs.ty {
		Some(kind) => Some(bound.em.kind(kind.clone())?),
		None => None,
	};
	bound.store(StoreInstr::SessionLet {
		name: attrs.name.clone(),
		kind,
		value,
	});
	bound.answer_none()?;
	bound.finish()
}

/// The `INFO` statement being lowered.
struct InfoLowerer<'a, 'b> {
	em: &'a mut Emitter,
	/// What the operands' lowering may read and the units it asks for.
	cx: &'a mut LowerCx<'b>,
	ir: &'a Ir,
	/// The context level the subject needs, which the executor resolves before the program runs.
	delegate: ContextLevel,
	/// The most demanding context level of any builtin an operand called.
	needs: ContextLevel,
	keeps_row_order: bool,
	/// The tables an operand may read through a value it is given.
	reach: Option<Reach>,
	/// What an operand may do to the transaction.
	access_mode: AccessMode,
	claims: Vec<Claim>,
}

impl InfoLowerer<'_, '_> {
	fn emit(&mut self, instr: Instr) {
		self.em.emit(instr);
	}

	fn store(&mut self, instr: StoreInstr) {
		self.em.emit(Instr::Store(instr));
	}

	/// Where the value `value` is read from, lowered by a lowerer of its own.
	fn operand(&mut self, value: ValueId) -> Result<Operand, IrError> {
		let mut lowerer = Lowerer::in_context(&mut *self.em, self.ir, &mut *self.cx);
		let operand = lowerer.value(value)?;
		let needs = lowerer.needs();
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.max(needs.context);
		self.keeps_row_order |= needs.keeps_row_order;
		self.access_mode = self.access_mode.combine(needs.access_mode);
		self.reach = match (self.reach.take(), needs.reach) {
			(Some(held), Some(more)) => Some(held.union(more)),
			(held, more) => held.or(more),
		};
		Ok(operand)
	}

	/// A register holding `operand`: the register it names, or one it is loaded into.
	fn register_of(&mut self, operand: Operand) -> Result<Reg, IrError> {
		if let Operand::Reg(reg) = operand {
			return Ok(reg);
		}
		self.load(operand)
	}

	/// A register holding a copy of `src`.
	fn load(&mut self, src: Operand) -> Result<Reg, IrError> {
		let dst = self.em.reg()?;
		self.emit(Instr::Load {
			dst,
			src,
		});
		Ok(dst)
	}

	fn constant(&mut self, value: Value) -> Result<Operand, IrError> {
		Ok(Operand::Const(self.em.constant(value)?))
	}

	/// Ends the pipeline answering `NONE` as one row.
	fn answer_none(&mut self) -> Result<(), IrError> {
		let none = self.constant(Value::None)?;
		let out = self.load(none)?;
		self.emit(Instr::Emit {
			src: out,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	/// A register holding a new object with `fields`, set in order.
	fn object(&mut self, fields: &[(&str, Operand)]) -> Result<Reg, IrError> {
		let dst = self.em.reg()?;
		self.emit(Instr::NewObject {
			dst,
		});
		for (name, value) in fields {
			self.set(dst, name, *value)?;
		}
		Ok(dst)
	}

	/// Sets field `name` of the object in `object` to `value`.
	fn set(&mut self, object: Reg, name: &str, value: Operand) -> Result<(), IrError> {
		let path = self.em.path(FieldPath::field(name))?;
		self.emit(Instr::SetField {
			object,
			path,
			value,
		});
		Ok(())
	}

	/// The string a name operand spells, when its value is a string literal.
	fn spelled(&self, value: ValueId) -> Option<Value> {
		let ValueDef::OpResult {
			op,
			index: 0,
		} = self.ir.value(value)?.def
		else {
			return None;
		};
		match &self.ir.op(op)?.kind {
			OpKind::Expr(ExprOp::Literal(literal)) if matches!(**literal, Value::String(_)) => {
				Some((**literal).clone())
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
		}
	}

	/// A register holding the name `value` gives: a string literal as it stands, and anything else
	/// evaluated and read as a string.
	fn name(&mut self, value: ValueId) -> Result<Reg, IrError> {
		if let Some(spelled) = self.spelled(value) {
			let name = self.constant(spelled)?;
			return self.load(name);
		}
		let operand = self.operand(value)?;
		let reg = self.register_of(operand)?;
		self.store(StoreInstr::CoerceString {
			src: reg,
			dst: reg,
		});
		Ok(reg)
	}

	/// The permission check every `INFO` form but `USER` and `INDEX` makes.
	fn allowed(&mut self, base: Base) {
		self.store(StoreInstr::Allow {
			action: Action::View,
			resource: ResourceKind::Any,
			base,
		});
	}

	/// The permission check of `INFO FOR USER` and `INFO FOR INDEX`, which is the actor's.
	fn actor_allowed(&mut self, base: Base) {
		self.store(StoreInstr::Allow {
			action: Action::View,
			resource: ResourceKind::Actor,
			base,
		});
	}

	/// A register holding the version stamp `version` names, evaluated and read as a datetime;
	/// `None` for a statement with no `VERSION`.
	fn version(&mut self, version: Option<ValueId>) -> Result<Option<Reg>, IrError> {
		let Some(version) = version else {
			return Ok(None);
		};
		let operand = self.operand(version)?;
		let reg = self.register_of(operand)?;
		self.store(StoreInstr::VersionStamp {
			src: reg,
			dst: reg,
		});
		Ok(Some(reg))
	}

	/// Sets field `name` of `out` to the rendering of the definitions of `kind` stored beneath the
	/// `route` bound over `fields`, decoded as `ty`, as the store stood at the version stamp in
	/// `version` when there is one.
	#[allow(clippy::too_many_arguments)]
	fn part(
		&mut self,
		out: Reg,
		name: &str,
		(route, ty, kind): Part,
		fields: &[(&str, Operand)],
		structured: bool,
		scope: Option<(Reg, Reg)>,
		version: Option<Reg>,
	) -> Result<(), IrError> {
		let key = self.object(fields)?;
		let list = self.em.reg()?;
		match version {
			Some(version) => self.store(StoreInstr::ScanAt {
				route: RouteId::named(route),
				key,
				ty: TypeId::named(ty),
				version,
				dst: list,
			}),
			None => self.store(StoreInstr::Scan {
				route: RouteId::named(route),
				key,
				ty: TypeId::named(ty),
				dst: list,
				unread: None,
			}),
		}
		self.store(StoreInstr::Render {
			definitions: kind,
			structured,
			list,
			dst: list,
			scope,
			version,
		});
		self.set(out, name, Operand::Reg(list))
	}

	/// Answers the object in `out`: emits it and ends the row.
	fn answer(&mut self, out: Reg) {
		self.emit(Instr::Emit {
			src: out,
		});
		self.emit(Instr::EndRow);
	}

	fn new_object(&mut self) -> Result<Reg, IrError> {
		let out = self.em.reg()?;
		self.emit(Instr::NewObject {
			dst: out,
		});
		Ok(out)
	}

	/// `INFO FOR ROOT`.
	fn root(&mut self, structured: bool, version: Option<ValueId>) -> Result<(), IrError> {
		self.allowed(Base::Root);
		let version = self.version(version)?;
		let out = self.new_object()?;
		self.part(
			out,
			"accesses",
			("RootAccessMethodPrefix", "StoredAccessDefinition", Definitions::Access),
			&[],
			structured,
			None,
			version,
		)?;
		self.part(
			out,
			"buckets",
			("RootBucketPrefix", "StoredBucketDefinition", Definitions::RootBucket),
			&[],
			structured,
			None,
			version,
		)?;
		// The root's defaults are its `default` config's, whichever form.
		let ty = self.constant(Value::String("default".into()))?;
		let key = self.object(&[("ty", ty)])?;
		let defaults = self.em.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("RootConfigKey"),
			key,
			ty: TypeId::named("StoredConfigDefinition"),
			dst: defaults,
		});
		self.store(StoreInstr::RenderOne {
			definitions: Definitions::Defaults,
			structured: true,
			src: defaults,
			dst: defaults,
		});
		self.set(out, "defaults", Operand::Reg(defaults))?;
		// The cluster's nodes are not versioned.
		let parts: [(&str, Part, Option<Reg>); 2] = [
			(
				"namespaces",
				("NamespacePrefix", "NamespaceDefinition", Definitions::Namespace),
				version,
			),
			("nodes", ("NodePrefix", "Node", Definitions::Node), None),
		];
		for (name, part, version) in parts {
			self.part(out, name, part, &[], structured, None, version)?;
		}
		let system = self.em.reg()?;
		self.store(StoreInstr::ProcessInfo {
			part: ProcessPart::System,
			dst: system,
		});
		self.set(out, "system", Operand::Reg(system))?;
		self.part(
			out,
			"users",
			("RootUserPrefix", "UserDefinition", Definitions::User),
			&[],
			structured,
			None,
			version,
		)?;
		let config = self.em.reg()?;
		self.store(StoreInstr::ProcessInfo {
			part: ProcessPart::Config,
			dst: config,
		});
		self.set(out, "config", Operand::Reg(config))?;
		self.answer(out);
		Ok(())
	}

	/// The session's namespace id, which the executor resolved.
	fn ns_id(&mut self) -> Result<Reg, IrError> {
		let ns_name = self.em.reg()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.em.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		let ns = self.em.reg()?;
		let name = self.em.field("namespace_id")?;
		self.store(StoreInstr::GetField {
			src: namespace,
			name,
			dst: ns,
		});
		Ok(ns)
	}

	/// The session's namespace and database ids, which the executor resolved.
	fn ids(&mut self) -> Result<(Reg, Reg), IrError> {
		let (ns, db, _) = self.ids_and_database()?;
		Ok((ns, db))
	}

	/// [`Self::ids`], and the database's definition in a register.
	fn ids_and_database(&mut self) -> Result<(Reg, Reg, Reg), IrError> {
		let ns = self.ns_id()?;
		let db_name = self.em.reg()?;
		self.store(StoreInstr::SessionDb {
			dst: db_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db_name))])?;
		let database = self.em.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		let db = self.em.reg()?;
		let name = self.em.field("database_id")?;
		self.store(StoreInstr::GetField {
			src: database,
			name,
			dst: db,
		});
		Ok((ns, db, database))
	}

	/// The id of the session's namespace, read by name, for a statement the executor resolved no
	/// namespace for. One that does not exist raises what `user` names when it is set (the user's
	/// not-found error, naming the session's namespace and database), and otherwise the
	/// namespace's not-found error.
	fn found_ns_id(&mut self, user: Option<Reg>) -> Result<Reg, IrError> {
		let ns_name = self.em.reg()?;
		self.store(StoreInstr::SessionNs {
			dst: ns_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns_name))])?;
		let namespace = self.em.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("NamespaceKey"),
			key,
			ty: TypeId::named("NamespaceDefinition"),
			dst: namespace,
		});
		let present = self.em.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(namespace),
			truthy: true,
			target: present,
		});
		match user {
			Some(user) => self.missing_user(Base::Db, user, StatementError::UserDbNotFound)?,
			None => self.store(StoreInstr::Raise {
				error: StatementError::NsNotFound,
				arg: ns_name,
			}),
		}
		self.em.bind(present)?;
		let ns = self.em.reg()?;
		let name = self.em.field("namespace_id")?;
		self.store(StoreInstr::GetField {
			src: namespace,
			name,
			dst: ns,
		});
		Ok(ns)
	}

	/// The ids of the session's namespace and database, read by name, for a statement the executor
	/// resolved neither for. Either not existing raises the not-found error of user `user`, naming
	/// the session's namespace and database.
	fn found_ids(&mut self, user: Reg) -> Result<(Reg, Reg), IrError> {
		let ns = self.found_ns_id(Some(user))?;
		let db_name = self.em.reg()?;
		self.store(StoreInstr::SessionDb {
			dst: db_name,
		});
		let key = self.object(&[("ns", Operand::Reg(ns)), ("db", Operand::Reg(db_name))])?;
		let database = self.em.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("DatabaseKey"),
			key,
			ty: TypeId::named("DatabaseDefinition"),
			dst: database,
		});
		let present = self.em.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(database),
			truthy: true,
			target: present,
		});
		self.missing_user(Base::Db, user, StatementError::UserDbNotFound)?;
		self.em.bind(present)?;
		let db = self.em.reg()?;
		let name = self.em.field("database_id")?;
		self.store(StoreInstr::GetField {
			src: database,
			name,
			dst: db,
		});
		Ok((ns, db))
	}

	/// `INFO FOR NS`.
	fn ns(&mut self, structured: bool, version: Option<ValueId>) -> Result<(), IrError> {
		self.delegate = self.delegate.max(ContextLevel::Namespace);
		self.allowed(Base::Ns);
		let ns = self.ns_id()?;
		let version = self.version(version)?;
		let out = self.new_object()?;
		let scope = [("ns", Operand::Reg(ns))];
		let parts: [(&str, Part); 4] = [
			("accesses", ("NsAccessMethodPrefix", "StoredAccessDefinition", Definitions::Access)),
			("buckets", ("NsBucketPrefix", "StoredBucketDefinition", Definitions::NsBucket)),
			("databases", ("DatabasePrefix", "DatabaseDefinition", Definitions::Database)),
			("users", ("NsUserPrefix", "UserDefinition", Definitions::User)),
		];
		for (name, part) in parts {
			self.part(out, name, part, &scope, structured, None, version)?;
		}
		self.answer(out);
		Ok(())
	}

	/// `INFO FOR DB`: the database's definitions, as the store stood at the version stamp when
	/// there is one, and its compatibility level as it stands now.
	fn db(&mut self, structured: bool, version: Option<ValueId>) -> Result<(), IrError> {
		self.delegate = self.delegate.max(ContextLevel::Database);
		self.allowed(Base::Db);
		let (ns, db, database) = self.ids_and_database()?;
		let version = self.version(version)?;
		let out = self.new_object()?;
		let compatibility = self.em.reg()?;
		let name = self.em.field("compatibility")?;
		self.store(StoreInstr::GetField {
			src: database,
			name,
			dst: compatibility,
		});
		self.set(out, "compatibility", Operand::Reg(compatibility))?;
		let scope = [("ns", Operand::Reg(ns)), ("db", Operand::Reg(db))];
		let parts: [(&str, Part); 13] = [
			("accesses", ("DbAccessMethodPrefix", "StoredAccessDefinition", Definitions::Access)),
			("apis", ("ApiPrefix", "StoredApiDefinition", Definitions::Api)),
			("analyzers", ("AnalyzerPrefix", "AnalyzerDefinition", Definitions::Analyzer)),
			("buckets", ("BucketPrefix", "StoredBucketDefinition", Definitions::Bucket)),
			("functions", ("FunctionPrefix", "StoredFunctionDefinition", Definitions::Function)),
			("extensions", ("ModulePrefix", "StoredModuleDefinition", Definitions::Extension)),
			("models", ("MlModelPrefix", "StoredMlModelDefinition", Definitions::Model)),
			("params", ("ParamPrefix", "StoredParamDefinition", Definitions::Param)),
			("tables", ("TablePrefix", "StoredTableDefinition", Definitions::Table)),
			("types", ("UserTypePrefix", "StoredTypeDefinition", Definitions::Type)),
			("users", ("DbUserPrefix", "UserDefinition", Definitions::User)),
			("configs", ("DbConfigPrefix", "StoredConfigDefinition", Definitions::Config)),
			("sequences", ("SequencePrefix", "SequenceDefinition", Definitions::Sequence)),
		];
		for (name, part) in parts {
			self.part(out, name, part, &scope, structured, Some((ns, db)), version)?;
		}
		self.answer(out);
		Ok(())
	}

	/// `INFO FOR TABLE`.
	fn table(
		&mut self,
		table: ValueId,
		structured: bool,
		version: Option<ValueId>,
	) -> Result<(), IrError> {
		self.delegate = self.delegate.max(ContextLevel::Database);
		self.allowed(Base::Db);
		let (ns, db) = self.ids()?;
		let tb = self.name(table)?;
		let version = self.version(version)?;
		let out = self.new_object()?;
		let scope = [("ns", Operand::Reg(ns)), ("db", Operand::Reg(db)), ("tb", Operand::Reg(tb))];
		let parts: [(&str, Part); 5] = [
			("events", ("EventPrefix", "StoredEventDefinition", Definitions::Event)),
			("fields", ("FieldPrefix", "StoredFieldDefinition", Definitions::Field)),
			("indexes", ("IndexDefPrefix", "StoredIndexDefinition", Definitions::Index)),
			("lives", ("SubscriptionPrefix", "StoredSubscriptionDefinition", Definitions::Live)),
			("tables", ("ForeignTablePrefix", "StoredTableDefinition", Definitions::Table)),
		];
		for (name, part) in parts {
			self.part(out, name, part, &scope, structured, None, version)?;
		}
		self.store(StoreInstr::InfoMigrations {
			ns,
			db,
			tb,
			out,
		});
		self.answer(out);
		Ok(())
	}

	/// `INFO FOR USER`: at the base written, or at the base the session selects when it runs, each
	/// base its own branch.
	fn user(&mut self, user: ValueId, base: Option<Base>, structured: bool) -> Result<(), IrError> {
		if let Some(base) = base {
			self.delegate = self.delegate.max(match base {
				Base::Root => ContextLevel::Root,
				Base::Ns => ContextLevel::Namespace,
				Base::Db => ContextLevel::Database,
			});
			return self.user_at(user, base, structured, true);
		}
		let to_db = self.em.label()?;
		let to_ns = self.em.label()?;
		let end = self.em.label()?;
		self.store(StoreInstr::JumpIfSessionBase {
			base: Base::Db,
			target: to_db,
		});
		self.store(StoreInstr::JumpIfSessionBase {
			base: Base::Ns,
			target: to_ns,
		});
		self.user_at(user, Base::Root, structured, false)?;
		self.emit(Instr::Jump {
			target: end,
		});
		self.em.bind(to_ns)?;
		self.user_at(user, Base::Ns, structured, false)?;
		self.emit(Instr::Jump {
			target: end,
		});
		self.em.bind(to_db)?;
		self.user_at(user, Base::Db, structured, false)?;
		self.em.bind(end)
	}

	/// `INFO FOR USER` at `base`. Where the base is `written`, the executor resolved the session's
	/// namespace and database down to it; at the session's own base the statement resolves them
	/// itself, raising for a namespace that does not exist, and for a database that does not as for
	/// a user it does not find.
	fn user_at(
		&mut self,
		user: ValueId,
		base: Base,
		structured: bool,
		written: bool,
	) -> Result<(), IrError> {
		self.actor_allowed(base);
		let name = self.name(user)?;
		let (route, fields, missing) = match base {
			Base::Root => (
				"RootUserKey",
				vec![("user", Operand::Reg(name))],
				StatementError::UserRootNotFound,
			),
			Base::Ns => {
				let ns = if written {
					self.ns_id()?
				} else {
					self.found_ns_id(None)?
				};
				(
					"NsUserKey",
					vec![("ns", Operand::Reg(ns)), ("user", Operand::Reg(name))],
					StatementError::UserNsNotFound,
				)
			}
			Base::Db => {
				let (ns, db) = if written {
					self.ids()?
				} else {
					self.found_ids(name)?
				};
				(
					"DbUserKey",
					vec![
						("ns", Operand::Reg(ns)),
						("db", Operand::Reg(db)),
						("user", Operand::Reg(name)),
					],
					StatementError::UserDbNotFound,
				)
			}
		};
		let key = self.object(&fields)?;
		let definition = self.em.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named(route),
			key,
			ty: TypeId::named("UserDefinition"),
			dst: definition,
		});
		let present = self.em.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(definition),
			truthy: true,
			target: present,
		});
		self.missing_user(base, name, missing)?;
		self.em.bind(present)?;
		self.store(StoreInstr::RenderOne {
			definitions: Definitions::User,
			structured,
			src: definition,
			dst: definition,
		});
		self.answer(definition);
		Ok(())
	}

	/// `INFO FOR INDEX index ON TABLE table`.
	fn index(&mut self, index: ValueId, table: ValueId) -> Result<(), IrError> {
		self.delegate = self.delegate.max(ContextLevel::Database);
		self.actor_allowed(Base::Db);
		let index = self.name(index)?;
		let table = self.name(table)?;
		let out = self.em.reg()?;
		self.store(StoreInstr::InfoIndex {
			table,
			index,
			dst: out,
		});
		self.answer(out);
		Ok(())
	}

	/// Raises the missing user's error, naming the user and the session's namespace and database
	/// names down to `base`.
	fn missing_user(
		&mut self,
		base: Base,
		name: Reg,
		missing: StatementError,
	) -> Result<(), IrError> {
		let mut detail = vec![("name", Operand::Reg(name))];
		if !matches!(base, Base::Root) {
			let ns = self.em.reg()?;
			self.store(StoreInstr::SessionNs {
				dst: ns,
			});
			detail.push(("ns", Operand::Reg(ns)));
		}
		if matches!(base, Base::Db) {
			let db = self.em.reg()?;
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
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::iface::statements::{DefinitionTemplate, LiveShape};
	use crate::exec::ir::logical::ops::LiveAttrs;
	use crate::exec::vm::ir_lower::lower;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::Program;

	/// A container holding one `INFO` of `subject` over the operands `operands` builds.
	fn info(
		subject: InfoSubject,
		structure: bool,
		operands: impl FnOnce(&mut Builder) -> Vec<ValueId>,
	) -> Program {
		let mut b = Builder::new();
		let operands = operands(&mut b);
		b.add_phys(
			PhysOp::Info {
				subject,
				structure,
			},
			&operands,
		);
		b.physical();
		match lower(&b.ir, &FactCache::new()) {
			Ok(lowered) => lowered.program,
			Err(error) => panic!("the INFO lowers: {error}"),
		}
	}

	fn name(b: &mut Builder, name: &str) -> ValueId {
		b.literal(Value::String(name.into()))
	}

	/// The store instructions of the program's one pipeline, in order.
	fn stores(program: &Program) -> Vec<&StoreInstr> {
		program.pipelines[0]
			.code
			.iter()
			.filter_map(|instr| {
				if let Instr::Store(store) = instr {
					Some(store)
				} else {
					None
				}
			})
			.collect()
	}

	fn scans(program: &Program) -> usize {
		stores(program)
			.iter()
			.filter(|store| matches!(store, StoreInstr::Scan { .. } | StoreInstr::ScanAt { .. }))
			.count()
	}

	fn renders(program: &Program) -> Vec<Definitions> {
		stores(program)
			.into_iter()
			.filter_map(|store| {
				if let StoreInstr::Render {
					definitions,
					..
				} = store
				{
					Some(*definitions)
				} else {
					None
				}
			})
			.collect()
	}

	/// A container holding one `KILL` of the id `$id`, written as `text` when it is `Some`.
	fn kill(text: Option<&str>, operands: usize) -> Builder {
		let mut b = Builder::new();
		let ids: Vec<ValueId> = (0..operands)
			.map(|_| {
				b.add(
					ExprOp::FreeParam {
						name: "id".into(),
					},
					&[],
				)
			})
			.collect();
		let op = b.add_phys(PhysOp::Kill, &ids);
		if let Some(text) = text {
			b.written(op, text);
		}
		b.add_phys(PhysOp::Yield, &[]);
		b.physical();
		b
	}

	fn position(code: &[Instr], is: impl Fn(&Instr) -> bool) -> usize {
		code.iter().position(is).expect("the instruction is emitted")
	}

	#[test]
	fn a_kill_checks_then_evaluates_its_id_then_removes_and_answers_none() {
		let b = kill(Some("$id"), 1);
		let lowered = lower(&b.ir, &FactCache::new()).expect("the KILL lowers");
		let program = lowered.program;
		assert_eq!(program.pipelines.len(), 1);
		assert!(program.scalar);
		assert!(matches!(program.pipelines[0].source, Source::Once));
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
		assert_eq!(program.required_context, ContextLevel::Root);
		let code = &program.pipelines[0].code;
		let checks =
			position(code, |instr| matches!(instr, Instr::Store(StoreInstr::RealtimeChecks)));
		let evaluated = position(code, |instr| matches!(instr, Instr::LoadParam { .. }));
		let removed =
			position(code, |instr| matches!(instr, Instr::Store(StoreInstr::Kill { .. })));
		assert!(checks < evaluated && evaluated < removed, "{code:?}");
		// The statement answers `NONE` as one row.
		assert!(matches!(code[code.len() - 1], Instr::EndRow));
		assert!(matches!(code[code.len() - 2], Instr::Emit { .. }));
		// The id as written is a constant of the program, which the removal reads.
		assert!(
			program
				.consts
				.iter()
				.any(|value| matches!(value, Value::String(text) if text.as_str() == "$id"))
		);
	}

	/// A container holding one `LIVE` over `operands` target operands and the template `template`.
	fn live(template: DefinitionTemplate, operands: usize) -> Builder {
		let mut b = Builder::new();
		let targets: Vec<ValueId> =
			(0..operands).map(|_| b.literal(Value::Table("person".into()))).collect();
		b.add_phys(
			PhysOp::Live(Box::new(LiveAttrs {
				id: crate::val::Uuid(uuid::Uuid::nil()),
				node: crate::val::Uuid(uuid::Uuid::nil()),
				fields: LiveShape::Diff,
				template,
			})),
			&targets,
		);
		b.add_phys(PhysOp::Yield, &[]);
		b.physical();
		b
	}

	/// The template of a live subscription, whose stored form is the string `text`.
	fn subscription_template(text: &str) -> DefinitionTemplate {
		DefinitionTemplate {
			ty: "StoredSubscriptionDefinition",
			value: Value::String(text.into()),
			holes: smallvec::smallvec![
				TemplateHole::Other("node"),
				TemplateHole::Other("auth"),
				TemplateHole::Other("session"),
				TemplateHole::Other("vars"),
			],
		}
	}

	#[test]
	fn a_live_checks_then_evaluates_its_target_then_registers_and_answers_the_id() {
		let b = live(subscription_template("template"), 1);
		let lowered = lower(&b.ir, &FactCache::new()).expect("the LIVE lowers");
		let program = lowered.program;
		assert_eq!(program.pipelines.len(), 1);
		assert!(program.scalar);
		assert!(matches!(program.pipelines[0].source, Source::Once));
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
		assert_eq!(program.required_context, ContextLevel::Root);
		let code = &program.pipelines[0].code;
		let checks =
			position(code, |instr| matches!(instr, Instr::Store(StoreInstr::RealtimeChecks)));
		let registered =
			position(code, |instr| matches!(instr, Instr::Store(StoreInstr::Live { .. })));
		assert!(checks < registered, "{code:?}");
		// The statement answers the id the registration left, as one row.
		let Instr::Store(StoreInstr::Live {
			dst,
			..
		}) = &code[registered]
		else {
			panic!("the registration is a Live instruction");
		};
		assert_eq!(
			code[code.len() - 2],
			Instr::Emit {
				src: *dst
			}
		);
		assert!(matches!(code[code.len() - 1], Instr::EndRow));
		// The stored definition is a constant of the program.
		assert!(
			program
				.consts
				.iter()
				.any(|value| matches!(value, Value::String(text) if text.as_str() == "template"))
		);
	}

	#[test]
	fn a_live_template_of_another_shape_is_stale() {
		let mut other_type = subscription_template("template");
		other_type.ty = "StoredFieldDefinition";
		let mut other_holes = subscription_template("template");
		other_holes.holes.pop();
		for template in [other_type, other_holes] {
			let b = live(template, 1);
			assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Stale(_))));
		}
	}

	#[test]
	fn a_live_over_no_target_or_two_is_stale() {
		for operands in [0, 2] {
			let b = live(subscription_template("template"), operands);
			assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Stale(_))));
		}
	}

	#[test]
	fn a_kill_whose_written_id_was_not_recorded_is_stale() {
		let b = kill(None, 1);
		assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Stale(_))));
	}

	#[test]
	fn a_kill_over_no_id_or_two_is_stale() {
		for operands in [0, 2] {
			let b = kill(Some("$id"), operands);
			assert!(matches!(lower(&b.ir, &FactCache::new()), Err(IrError::Stale(_))));
		}
	}

	#[test]
	fn every_info_is_one_scalar_pipeline_over_no_source() {
		for subject in [InfoSubject::Root, InfoSubject::Ns, InfoSubject::Db] {
			let program = info(subject, false, |_| Vec::new());
			assert_eq!(program.pipelines.len(), 1);
			assert!(program.scalar);
			assert!(matches!(program.pipelines[0].source, Source::Once));
			let code = &program.pipelines[0].code;
			assert!(matches!(code[code.len() - 1], Instr::EndRow));
			assert!(matches!(code[code.len() - 2], Instr::Emit { .. }));
		}
	}

	#[test]
	fn the_permission_check_comes_first_and_the_context_follows_the_subject() {
		for (subject, base, context) in [
			(InfoSubject::Root, Base::Root, ContextLevel::Root),
			(InfoSubject::Ns, Base::Ns, ContextLevel::Namespace),
			(InfoSubject::Db, Base::Db, ContextLevel::Database),
		] {
			let program = info(subject, true, |_| Vec::new());
			assert_eq!(
				program.pipelines[0].code[0],
				Instr::Store(StoreInstr::Allow {
					action: Action::View,
					resource: ResourceKind::Any,
					base,
				})
			);
			assert_eq!(program.required_context, context);
		}
		let program = info(InfoSubject::Table, true, |b| vec![name(b, "person")]);
		assert_eq!(program.required_context, ContextLevel::Database);
		assert!(matches!(
			program.pipelines[0].code[0],
			Instr::Store(StoreInstr::Allow {
				resource: ResourceKind::Any,
				base: Base::Db,
				..
			})
		));
	}

	#[test]
	fn each_subject_scans_the_kinds_it_lists() {
		let root = info(InfoSubject::Root, false, |_| Vec::new());
		assert_eq!(
			renders(&root),
			vec![
				Definitions::Access,
				Definitions::RootBucket,
				Definitions::Namespace,
				Definitions::Node,
				Definitions::User
			]
		);
		let ns = info(InfoSubject::Ns, false, |_| Vec::new());
		assert_eq!(
			renders(&ns),
			vec![
				Definitions::Access,
				Definitions::NsBucket,
				Definitions::Database,
				Definitions::User
			]
		);
		let db = info(InfoSubject::Db, false, |_| Vec::new());
		assert_eq!(renders(&db).len(), 13);
		let table = info(InfoSubject::Table, false, |b| vec![name(b, "t")]);
		assert_eq!(
			renders(&table),
			vec![
				Definitions::Event,
				Definitions::Field,
				Definitions::Index,
				Definitions::Live,
				Definitions::Table
			]
		);
	}

	#[test]
	fn a_version_reads_every_versioned_part_at_its_stamp_and_the_nodes_unversioned() {
		let root = info(InfoSubject::Root, false, |b| vec![b.literal(Value::from(1_i64))]);
		assert!(stores(&root).iter().any(|store| matches!(store, StoreInstr::VersionStamp { .. })));
		// The accesses, the buckets, the namespaces and the users are read at the stamp, the nodes
		// are not.
		assert_eq!(scans(&root), 5);
		let at =
			stores(&root).iter().filter(|store| matches!(store, StoreInstr::ScanAt { .. })).count();
		assert_eq!(at, 4);
		let plain = info(InfoSubject::Root, false, |_| Vec::new());
		assert!(!stores(&plain).iter().any(|store| matches!(
			store,
			StoreInstr::ScanAt { .. } | StoreInstr::VersionStamp { .. }
		)));
	}

	#[test]
	fn a_table_name_is_loaded_when_it_is_a_string_and_coerced_when_it_is_computed() {
		let spelled = info(InfoSubject::Table, false, |b| vec![name(b, "person")]);
		assert!(
			!stores(&spelled).iter().any(|store| matches!(store, StoreInstr::CoerceString { .. }))
		);
		let computed = info(InfoSubject::Table, false, |b| {
			let left = name(b, "per");
			let right = name(b, "son");
			vec![b.binary(crate::expr::BinaryOperator::Add, left, right)]
		});
		let order = stores(&computed);
		let allow = order.iter().position(|store| matches!(store, StoreInstr::Allow { .. }));
		let coerce =
			order.iter().position(|store| matches!(store, StoreInstr::CoerceString { .. }));
		assert!(allow < coerce, "the permission check precedes the operand");
	}

	#[test]
	fn a_user_at_a_written_base_is_one_branch_and_with_none_it_is_three() {
		let written = info(
			InfoSubject::User {
				base: Some(Base::Ns),
			},
			false,
			|b| vec![name(b, "alice")],
		);
		assert_eq!(written.required_context, ContextLevel::Namespace);
		assert_eq!(
			stores(&written)
				.iter()
				.filter(|store| matches!(store, StoreInstr::Allow { .. }))
				.count(),
			1
		);
		assert!(matches!(
			written.pipelines[0].code[0],
			Instr::Store(StoreInstr::Allow {
				resource: ResourceKind::Actor,
				base: Base::Ns,
				..
			})
		));
		let open = info(
			InfoSubject::User {
				base: None,
			},
			false,
			|b| vec![name(b, "alice")],
		);
		let code = &open.pipelines[0].code;
		let bases: Vec<Base> = stores(&open)
			.iter()
			.filter_map(|store| {
				if let StoreInstr::Allow {
					base,
					resource: ResourceKind::Actor,
					..
				} = store
				{
					Some(*base)
				} else {
					None
				}
			})
			.collect();
		assert_eq!(bases, vec![Base::Root, Base::Ns, Base::Db]);
		// Every jump of the program lands on an instruction index, and the two that leave the
		// root and namespace branches land at the end of the code.
		let jumps: Vec<u32> = code
			.iter()
			.filter_map(|instr| {
				if let Instr::Jump {
					target,
				} = instr
				{
					Some(*target)
				} else {
					None
				}
			})
			.collect();
		assert_eq!(jumps, vec![code.len() as u32; 2]);
		assert_eq!(open.required_context, ContextLevel::Root);
	}

	#[test]
	fn an_index_checks_the_actor_and_reads_both_names() {
		let program = info(InfoSubject::Index, false, |b| vec![name(b, "idx"), name(b, "t")]);
		assert_eq!(program.required_context, ContextLevel::Database);
		let order = stores(&program);
		assert!(matches!(
			order[0],
			StoreInstr::Allow {
				resource: ResourceKind::Actor,
				base: Base::Db,
				..
			}
		));
		assert!(order.iter().any(|store| matches!(store, StoreInstr::InfoIndex { .. })));
	}

	#[test]
	fn an_operand_the_subject_does_not_take_is_stale() {
		let mut b = Builder::new();
		let one = name(&mut b, "a");
		let two = name(&mut b, "b");
		b.add_phys(
			PhysOp::Info {
				subject: InfoSubject::Index,
				structure: false,
			},
			&[one, two, one],
		);
		b.physical();
		assert!(matches!(
			lower(&b.ir, &FactCache::new()),
			Err(IrError::Stale("ir_lower::info: operand arity"))
		));
		let mut b = Builder::new();
		let one = name(&mut b, "a");
		b.add_phys(
			PhysOp::Info {
				subject: InfoSubject::Index,
				structure: false,
			},
			&[one],
		);
		b.physical();
		assert!(matches!(
			lower(&b.ir, &FactCache::new()),
			Err(IrError::Stale("ir_lower::info: operand arity"))
		));
	}
	/// The program of the statement `op` builds over the operands `operands` builds.
	fn session_statement(
		op: PhysOp,
		operands: impl FnOnce(&mut Builder) -> Vec<ValueId>,
	) -> Result<Program, IrError> {
		let mut b = Builder::new();
		let operands = operands(&mut b);
		b.add_phys(op, &operands);
		b.add_phys(PhysOp::Yield, &[]);
		b.physical();
		lower(&b.ir, &FactCache::new()).map(|lowered| lowered.program)
	}

	#[test]
	fn an_option_records_itself_and_answers_none() {
		use surrealdb_strand::Strand;
		let program = session_statement(
			PhysOp::Option {
				name: Strand::new("IMPORT"),
				value: true,
			},
			|_| Vec::new(),
		)
		.expect("the OPTION lowers");
		assert!(program.scalar);
		assert_eq!(program.pipelines.len(), 1);
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
		assert_eq!(program.required_context, ContextLevel::Root);
		let [
			StoreInstr::SessionOption {
				name,
				what: true,
			},
		] = stores(&program)[..]
		else {
			panic!("one SessionOption, got {:?}", stores(&program));
		};
		assert_eq!(name.as_str(), "IMPORT");
		let code = &program.pipelines[0].code;
		assert!(matches!(code[code.len() - 1], Instr::EndRow));
	}

	#[test]
	fn a_use_evaluates_its_operands_in_order_then_selects_and_answers_the_row() {
		let program = session_statement(
			PhysOp::Use {
				target: UseTarget::NsDb,
			},
			|b| vec![name(b, "a"), name(b, "b")],
		)
		.expect("the USE lowers");
		assert!(program.scalar);
		assert_eq!(program.access_mode, AccessMode::ReadWrite);
		assert_eq!(program.required_context, ContextLevel::Root);
		let [
			StoreInstr::SessionUseName {
				value: named_ns,
				database: false,
			},
			StoreInstr::SessionUseName {
				value: named_db,
				database: true,
			},
			StoreInstr::SessionUse {
				default: false,
				ns: Some(ns),
				db: Some(db),
				dst,
			},
		] = stores(&program)[..]
		else {
			panic!("two names then one SessionUse, got {:?}", stores(&program));
		};
		// Each operand is read as a name, in its own register, before the statement selects.
		assert_eq!((named_ns, named_db), (ns, db));
		assert_ne!(ns, db);
		// The statement answers the row the instruction leaves.
		let code = &program.pipelines[0].code;
		assert!(matches!(code[code.len() - 2], Instr::Emit { src } if src == *dst));
		assert!(matches!(code[code.len() - 1], Instr::EndRow));
	}

	#[test]
	fn a_use_names_the_levels_its_target_has_and_default_has_none() {
		for (target, operands, ns, db) in [
			(UseTarget::Ns, 1, true, false),
			(UseTarget::Db, 1, false, true),
			(UseTarget::Default, 0, false, false),
		] {
			let program = session_statement(
				PhysOp::Use {
					target,
				},
				|b| (0..operands).map(|_| name(b, "x")).collect(),
			)
			.expect("the USE lowers");
			let Some(StoreInstr::SessionUse {
				default,
				ns: held_ns,
				db: held_db,
				..
			}) = stores(&program)
				.into_iter()
				.find(|instr| matches!(instr, StoreInstr::SessionUse { .. }))
			else {
				panic!("one SessionUse, got {:?}", stores(&program));
			};
			assert_eq!(*default, target == UseTarget::Default);
			assert_eq!((held_ns.is_some(), held_db.is_some()), (ns, db), "{target:?}");
		}
	}

	#[test]
	fn a_use_over_the_wrong_number_of_operands_is_stale() {
		for (target, operands) in
			[(UseTarget::Ns, 0), (UseTarget::Ns, 2), (UseTarget::NsDb, 1), (UseTarget::Default, 1)]
		{
			let refused = session_statement(
				PhysOp::Use {
					target,
				},
				|b| (0..operands).map(|_| name(b, "x")).collect(),
			);
			assert!(matches!(refused, Err(IrError::Stale(_))), "{target:?} over {operands}");
		}
	}

	fn let_statement(
		ty: Option<crate::expr::Kind>,
		binding: LetBinding,
	) -> Result<Program, IrError> {
		use crate::exec::ir::logical::ops::LetAttrs;
		session_statement(
			PhysOp::Let(Box::new(LetAttrs {
				name: surrealdb_strand::Strand::new("a"),
				binding,
				ty,
			})),
			|b| vec![b.literal(Value::from(1_i64))],
		)
	}

	#[test]
	fn a_session_let_refuses_a_protected_name_before_its_value_then_coerces_and_records() {
		let program = let_statement(Some(crate::expr::Kind::Int), LetBinding::Session)
			.expect("the LET lowers");
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
		let code = &program.pipelines[0].code;
		let checks = position(code, |instr| {
			matches!(instr, Instr::Store(StoreInstr::SessionLetChecks { .. }))
		});
		let recorded =
			position(code, |instr| matches!(instr, Instr::Store(StoreInstr::SessionLet { .. })));
		assert_eq!(checks, 0);
		assert!(checks < recorded);
		let Instr::Store(StoreInstr::SessionLet {
			kind: Some(kind),
			..
		}) = &code[recorded]
		else {
			panic!("the declared kind is carried: {:?}", code[recorded]);
		};
		assert_eq!(program.kinds[*kind as usize], crate::expr::Kind::Int);
		// With no declared kind the value is recorded as it is.
		let untyped = let_statement(None, LetBinding::Session).expect("the LET lowers");
		assert!(stores(&untyped).iter().any(|instr| matches!(
			instr,
			StoreInstr::SessionLet {
				kind: None,
				..
			}
		)));
	}
	/// Takes the store instruction `instr` as the host does: a data statement's by its own step,
	/// every other through the full surface's runtime.
	async fn take(
		program: &Program,
		instr: &StoreInstr,
		regs: &mut [crate::exec::vm::scalar::Scalar<'_>],
		reserved_taken: &mut [bool],
		ctx: &crate::exec::ExecutionContext,
	) -> crate::exec::FlowResult<Option<usize>> {
		use crate::exec::vm::store::{FULL_SCHEMA_STEPS, SchemaSteps, step_dml};
		match instr.is_dml() {
			true => step_dml(program, instr, regs, reserved_taken, ctx).await,
			false => FULL_SCHEMA_STEPS.step(program, instr, regs, reserved_taken, ctx, None).await,
		}
	}

	/// Runs the one store instruction of `program` in `ctx` over `regs`.
	async fn step_one(
		program: &Program,
		regs: &mut [crate::exec::vm::scalar::Scalar<'_>],
		ctx: &crate::exec::ExecutionContext,
	) -> crate::exec::FlowResult<()> {
		let [instr] = stores(program)[..] else {
			panic!("one store instruction, got {:?}", stores(program));
		};
		take(program, instr, regs, &mut [], ctx).await.map(|_| ())
	}

	#[tokio::test]
	async fn an_option_records_in_the_sink_of_a_top_level_statement_and_refuses_without_one() {
		use surrealdb_strand::Strand;

		use crate::exec::context::{SessionEffect, SessionSink};
		use crate::exec::operators::test_util::TestDb;
		let program = session_statement(
			PhysOp::Option {
				name: Strand::new("FORCE"),
				value: true,
			},
			|_| Vec::new(),
		)
		.expect("the OPTION lowers");
		// An option is set on the session's database, so the statement runs in a context that
		// selects one. A context derived to run other code holds no sink: the statement is
		// refused there.
		let db = TestDb::new("").await;
		let bare = db.exec_ctx().await;
		assert!(step_one(&program, &mut [], &bare).await.is_err());
		let sink = std::sync::Arc::new(SessionSink::default());
		let top_level = bare.with_session_sink(std::sync::Arc::clone(&sink));
		step_one(&program, &mut [], &top_level).await.expect("the option is recorded");
		assert_eq!(
			sink.take(),
			vec![SessionEffect::Option {
				name: Strand::new("FORCE"),
				what: true,
			}]
		);
	}

	#[tokio::test]
	async fn a_session_let_refuses_a_protected_name_and_records_nothing_else() {
		use surrealdb_strand::Strand;

		use crate::exec::context::{SessionEffect, SessionSink};
		use crate::exec::operators::test_util::root_ctx_with_options;
		use crate::exec::vm::scalar::{Held, Scalar};
		let sink = std::sync::Arc::new(SessionSink::default());
		let ctx = root_ctx_with_options().with_session_sink(std::sync::Arc::clone(&sink));
		let checks = |name: &str| {
			let mut b = Builder::new();
			let one = b.literal(Value::from(1_i64));
			b.add_phys(
				PhysOp::Let(Box::new(crate::exec::ir::logical::ops::LetAttrs {
					name: Strand::new(name),
					binding: LetBinding::Session,
					ty: None,
				})),
				&[one],
			);
			b.add_phys(PhysOp::Yield, &[]);
			b.physical();
			lower(&b.ir, &FactCache::new()).expect("the LET lowers").program
		};
		for protected in ["auth", "token", "session", "access"] {
			let program = checks(protected);
			let instr = program.pipelines[0]
				.code
				.iter()
				.find_map(|instr| {
					let Instr::Store(
						held @ StoreInstr::SessionLetChecks {
							..
						},
					) = instr
					else {
						return None;
					};
					Some(held)
				})
				.expect("the name is checked");
			let refused = take(&program, instr, &mut [], &mut [], &ctx).await;
			assert!(refused.is_err(), "{protected}");
		}
		let program = checks("x");
		let Some(Instr::Store(record)) = program.pipelines[0]
			.code
			.iter()
			.find(|instr| matches!(instr, Instr::Store(StoreInstr::SessionLet { .. })))
		else {
			panic!("the value is recorded");
		};
		let StoreInstr::SessionLet {
			value,
			..
		} = record
		else {
			unreachable!("matched above")
		};
		let mut regs: Vec<Scalar<'_>> = (0..=*value).map(|_| Scalar::None).collect();
		regs[*value as usize] = Scalar::Value(Held::Owned(Value::from(5_i64)));
		take(&program, record, &mut regs, &mut [], &ctx).await.expect("the value is recorded");
		assert_eq!(
			sink.take(),
			vec![SessionEffect::Let {
				name: Strand::new("x"),
				value: std::sync::Arc::new(Value::from(5_i64)),
			}]
		);
	}
	/// A statement that writes the request's session is lowered for a statement of the request
	/// and never into a unit: the program of a function body, a closure or a subscription holds
	/// none.
	#[test]
	fn a_session_statement_is_never_lowered_into_a_unit() {
		use surrealdb_strand::Strand;

		use crate::exec::ir::logical::ops::LetAttrs;
		use crate::exec::vm::ir_lower::lower_unit;
		use crate::exec::vm::program::UnitSig;
		let statements = [
			(
				PhysOp::Option {
					name: Strand::new("IMPORT"),
					value: true,
				},
				0,
			),
			(
				PhysOp::Use {
					target: UseTarget::Ns,
				},
				1,
			),
			(
				PhysOp::Let(Box::new(LetAttrs {
					name: Strand::new("a"),
					binding: LetBinding::Session,
					ty: None,
				})),
				1,
			),
		];
		for (op, operands) in statements {
			let mut b = Builder::new();
			let operands: Vec<ValueId> = (0..operands).map(|_| name(&mut b, "x")).collect();
			b.add_phys(op, &operands);
			b.add_phys(PhysOp::Yield, &[]);
			b.physical();
			let sig = UnitSig {
				params: Vec::new(),
				takes_row: false,
			};
			let lowered = lower_unit(&mut LowerCx::detached(), &b.ir, &FactCache::new(), sig);
			assert!(lowered.is_err());
			// The same container lowers as a statement of the request.
			assert!(lower(&b.ir, &FactCache::new()).is_ok());
		}
	}
	#[tokio::test]
	async fn a_use_operand_that_is_no_string_is_refused_before_the_next_is_evaluated() {
		use crate::exec::operators::test_util::root_ctx_with_options;
		use crate::exec::vm::scalar::{Held, Scalar};
		let program = session_statement(
			PhysOp::Use {
				target: UseTarget::NsDb,
			},
			|b| vec![b.literal(Value::from(1_i64)), name(b, "b")],
		)
		.expect("the USE lowers");
		let Some(
			instr @ StoreInstr::SessionUseName {
				value,
				database: false,
			},
		) = stores(&program).into_iter().next()
		else {
			panic!("the namespace is read first: {:?}", stores(&program));
		};
		let mut regs: Vec<Scalar<'_>> = (0..=*value).map(|_| Scalar::None).collect();
		regs[*value as usize] = Scalar::Value(Held::Owned(Value::from(1_i64)));
		let ctx = root_ctx_with_options();
		let refused = take(&program, instr, &mut regs, &mut [], &ctx).await;
		assert!(refused.is_err());
		regs[*value as usize] = Scalar::Value(Held::Owned(Value::String("a".into())));
		take(&program, instr, &mut regs, &mut [], &ctx).await.expect("a string names a namespace");
	}
}
