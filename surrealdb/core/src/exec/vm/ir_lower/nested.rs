//! Lowering of the code an expression runs as a unit of its own: a statement held in an expression
//! (`expr.subquery`), a `{ ... }` block in value position (`phys.block`), and the body of a
//! user-defined function a call runs (`expr.call` of a `fn::` name).
//!
//! The statement is a unit of its own. Its region is not lowered into the program that holds the
//! expression: the lowering asks for [`UnitRequest::Subquery`] and holds the slot the unit takes
//! in the program's unit table, and the compile driver plans the region as a container of its own
//! and lowers it into that slot. The expression runs the unit through [`Instr::Subquery`] over a
//! [`SubquerySpec`] of the program's pool, which holds the slot, whether the unit reads the row of
//! the enclosing statement as its document, and what the unit does to the transaction.
//!
//! # What the spec holds
//!
//! - `inherits_row` is whether the statement evaluates its expressions against the row of the
//!   statement that holds it: a block, a `RETURN`, a `LET`, a `FOR`, `INFO`, a schema statement, a
//!   `SLEEP`, an `EXPLAIN` or a bare evaluation. A region that holds a relational read, a select
//!   loop, a write, a nested read or a `MATCH` supplies its own rows and does not. It is read from
//!   the ops of the region, and any statement the list does not name inherits.
//! - `access_mode` is a write when the effects of the region write data or change the catalog, and
//!   the program that holds the subquery then needs a transaction that may write.
//! - `required_context` and `keeps_row_order` are left at their neutral value. They are facts of
//!   the unit's program, which the compile driver reads after it lowers the unit and folds into the
//!   requesting program.
//! - `inputs` are the values defined outside the region that its code reads (an enclosing row, a
//!   name a block binds, an element, a computed value), in the order [`Ir::region_inputs`] names
//!   them, each evaluated where the instruction sits. The unit is [`Ir::copy_region`] of the
//!   region, whose top block takes them after the region's own arguments, and reads the `at`th as
//!   the parameter [`unit_input_name`]`(at)` the run binds (`LowerCx::with_unit_inputs`). A literal
//!   is copied into the unit and is no input; the statement timestamp, where only clock reads take
//!   it, is handed over as `NONE` ([`Lowerer::unit_inputs`]).
//!
//! # Where a `RETURN` goes
//!
//! Neither a parenthesised statement nor a block is a context, so a `RETURN` the unit signals ends
//! the context the code holding the instruction evaluates: its value is stored where that context
//! takes it and the code resumes past the rest of the context's evaluation
//! ([`Lowerer::context_value`]). Where that context is outside the code being lowered, the signal
//! propagates to the run that holds the code. A block always evaluates against the row of the
//! statement that holds it.
//!
//! # Strategies
//!
//! `Apply` runs the unit for every evaluation. Every other strategy answers the rows, order and
//! errors `Apply` does; it changes how often the statement's reads happen.
//!
//! `Once`, `Hash` and `Lookup` run the statement in place, as a group of the program that holds
//! the expression ([`BlockInstr::Nested`] over a [`NestedSpec`] of the program's pool), as a
//! statement in scalar position: a `Once` statement keeps its value from the first evaluation that
//! reaches it ([`NestedSpec::once`]), and a `Hash` statement reads through a probe whose kept rows
//! outlive one evaluation, neither of which a unit run afresh holds. The kept value and the probes
//! the group reads through are owned by the group open where the expression is lowered (`None` for
//! the program's own code), whose start forgets the value and empties the probes' kept rows.
//! A `Once` statement whose every reader tests membership in its rows
//! ([`held_as_members`](crate::exec::ir::phys::nested::held_as_members)) keeps them as a
//! membership index ([`BlockInstr::Member`], [`NestedSpec::members`]): each `INSIDE` or
//! `NOT INSIDE` that reads it is lowered to the membership step of its probe ([`Lowerer::member`]),
//! which answers what the comparison over the gathered array answers.
//!
//! The hashed plan of a `Lookup` is a second group, under the same owner, which the run reads
//! through in place of the lookup past [`LOOKUP_EVALUATIONS`] evaluations of one run of that
//! owner, and for a key the index may not list every equal record under; the spec holds it with
//! the path into `$parent` of the key the lookup reads.
//!
//! The instruction binds the row it evaluates the statement for as `$parent` and `$this`, so the
//! group's code reads that row, wherever the statement reads it from outside its regions, as
//! `$parent` ([`Emitter::read_as_parent`]). A statement runs in place only where that holds: the
//! code being lowered runs over the program's running state (the list of a statement program, or
//! the body of a pipeline of a read statement) in the authority frame the program's own code runs
//! in, not a permission predicate's or a field clause's, sets no document aside for the statement,
//! and every value the statement reads from outside its regions is the row of the pipeline being
//! lowered or a statement timestamp only clock reads take. A `Once` statement also runs in place
//! only in the body of a pipeline of a read statement: the list of a statement program may run
//! again, in a loop, over the state that keeps the value. Elsewhere a `Lookup` runs its own region
//! as a unit, as `Apply` does, which reads through the index for every evaluation, and so do a
//! `Once`, whose value no evaluation keeps there, and a `Hash`, whose probe no evaluation owns
//! there: the unit reads the rows beneath the predicate for every evaluation, as a statement run
//! as a program of its own does.
//!
//! [`LOOKUP_EVALUATIONS`]: super::super::plan::LOOKUP_EVALUATIONS
//! [`Emitter::read_as_parent`]: super::emit::Emitter::read_as_parent
//!
//! # Calls of user-defined functions
//!
//! A call holds what the physical phase read of the definition
//! ([`CallAttrs::callee`](crate::exec::ir::expr::ops::CallAttrs::callee)): its hash, the kind of
//! its `PERMISSIONS` clause and what its body spends of the computation-depth budget. The body is a
//! unit the lowering asks for ([`UnitRequest::FunctionBody`]), bound for the caller's principal
//! capped by the definition's `AUTH LIMIT`, which the planning callback knows. Where the code being
//! lowered reads with its permissions unchecked ([`Lowerer::reads_unchecked`]) the call also asks
//! for the body bound in a frame that waives them, and states the body checked, so a call run where
//! permissions are skipped runs that one. The run checks the capability, the definition's hash and
//! its `PERMISSIONS` as it enters the call, and coerces the arguments and the result to the kinds
//! the definition declares.
//!
//! Where the code being lowered reads with its permissions checked, a `PERMISSIONS` predicate is
//! judged as the call enters by a unit the lowering asks for ([`UnitRequest::FunctionGuard`]),
//! whose verdict stands as long as the scope the physical phase read of the predicate. A predicate
//! with no scope (a clause or an `AUTH LIMIT` the run raises reading) holds no guard, as the run
//! raises before it would judge one.
//!
//! The call keeps the rows of its statement in order, as any call does. A body whose text writes
//! makes the call and the program write; what the functions the body calls do is the body unit's,
//! which the compile driver folds into the call and the program as it folds a subquery unit's.
//!
//! A call whose definition the plan cannot know (`CalleeFacts::LateBound`) answers
//! [`IrError::LateBind`]: its statement is planned again when it is reached. A call the physical
//! phase stated nothing on is an internal error: the phase reads the definition of every call
//! outside a stored-clause region. A function the plan found
//! undefined or denied is called with no body: entering it raises what the catalog or the
//! capabilities say.
//!
//! # Refusals
//!
//! A `LET` written as the statement of an expression is refused by [`LET_IN_AN_EXPRESSION`]. A
//! statement in a pipeline's element body that no document is set aside for answers `NotYet`.

use surrealdb_strand::Strand;

use super::super::program::{
	BlockInstr, CallDepth, Catch, DefHash, FunctionSpec, GuardCode, Instr, NestedSpec, Operand,
	Reg, SubquerySpec, UnitFrame, UnitInput, unit_input_name,
};
use super::expr::{Lowerer, call_args, exactly, invariant, region_at};
use super::pipe::ReadScope;
use super::read::parent_path;
use super::units::UnitRequest;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{
	CallAttrs, CalleeFacts, CalleePermission, ExprOp, NestedStrategy,
};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{LetBinding, LoopKind};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{LookupKeyShape, PhysOp, PhysTag};
use crate::exec::vm::payload::Refusal;
use crate::exec::{AccessMode, ContextLevel};
use crate::val::Value;

/// What planning raises for a `LET` written as the statement of an expression.
pub(super) const LET_IN_AN_EXPRESSION: &str =
	"LET statements can only appear at the top level of a query or inside a block expression";

impl Lowerer<'_> {
	/// The value the statement `data` holds in an expression, run as the unit its region is.
	///
	/// The result register holds the unit's value: its one value where the unit's value is one
	/// row, and an array of its rows otherwise.
	pub(super) fn subquery(
		&mut self,
		data: &OpData,
		strategy: &NestedStrategy,
	) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.subquery")?;
		self.require_frame(data)?;
		exactly::<0>(data)?;
		match strategy {
			NestedStrategy::Apply => self.subquery_region(region_at(data, 0, 1)?),
			NestedStrategy::Once => {
				let region = region_at(data, 0, 1)?;
				// A value kept by the list of a statement program would outlive an iteration of
				// a loop around it, which may change what the statement reads.
				let kept = match self.lowers_a_read_pipeline() {
					true => self.statement_in_place(region, None, true, None)?,
					false => None,
				};
				match kept {
					Some(value) => Ok(value),
					// Run as a unit, the statement keeps no value past one evaluation.
					None => self.subquery_region(region),
				}
			}
			NestedStrategy::Hash {
				..
			} => {
				let region = region_at(data, 0, 1)?;
				match self.statement_in_place(region, None, false, None)? {
					Some(value) => Ok(value),
					// Run as a unit, the probe no evaluation owns reads the rows beneath the
					// predicate for every evaluation.
					None => self.subquery_region(region),
				}
			}
			NestedStrategy::Lookup {
				hashed,
				..
			} => {
				let count = strategy.region_count();
				let region = region_at(data, 0, count)?;
				let hashed = match hashed {
					true => Some(region_at(data, 1, count)?),
					false => None,
				};
				match self.statement_in_place(region, hashed, false, None)? {
					Some(value) => Ok(value),
					// The lookup's own region reads through no probe, and reads through the
					// index for every evaluation.
					None => self.subquery_region(region),
				}
			}
		}
	}

	/// Whether `probe` is a member of the rows of the statement `subquery` (not a member when
	/// `negate`), as `probe INSIDE (subquery)` (`NOT INSIDE`) answers, read through the statement's
	/// membership index ([`BlockInstr::Member`]). `None`, with nothing emitted, unless the
	/// statement runs once, its one reader tests membership in its rows
	/// ([`held_as_members`](crate::exec::ir::phys::nested::held_as_members)) and the code being
	/// lowered runs it in place, kept across evaluations; the comparison then reads the statement's
	/// value as any other operand. A statement two comparisons read keeps its value once for both,
	/// where an index per reader would run it once per reader.
	pub(super) fn member(
		&mut self,
		subquery: OpId,
		probe: Operand,
		negate: bool,
	) -> Result<Option<Operand>, IrError> {
		let ir = self.ir;
		let Some(data) = ir.op(subquery) else {
			return Ok(None);
		};
		let one_reader = data.results.first().is_some_and(|result| ir.uses(*result).len() == 1);
		if !matches!(data.kind, OpKind::Expr(ExprOp::Subquery(NestedStrategy::Once)))
			|| !one_reader
			|| !crate::exec::ir::phys::nested::held_as_members(ir, subquery)
			|| !self.lowers_a_read_pipeline()
		{
			return Ok(None);
		}
		self.refuse_in_fold("ir_lower::fold: expr.subquery")?;
		self.require_frame(data)?;
		exactly::<0>(data)?;
		let region = region_at(data, 0, 1)?;
		self.lower_reads_of_regions(subquery, data)?;
		self.owners.push(subquery);
		let lowered = self.statement_in_place(region, None, true, Some((probe, negate)));
		self.owners.pop();
		lowered
	}

	/// The value the statement region `region` holds, run in place as a group of the program
	/// ([`BlockInstr::Nested`]), with `hashed`, for a `Lookup` that keeps one, the region of its
	/// hashed plan as a second group, and with `once` its value kept from the first evaluation
	/// until the group it is evaluated in starts again. With `member`, a probe and whether the
	/// test negates, the value is instead whether the probe is a member of the statement's rows,
	/// kept as a membership index ([`BlockInstr::Member`]); only a statement kept with `once` is
	/// read that way. `None`, with nothing emitted, where the code being lowered does not run the
	/// statement in place (see the module).
	fn statement_in_place(
		&mut self,
		region: RegionId,
		hashed: Option<RegionId>,
		once: bool,
		member: Option<(Operand, bool)>,
	) -> Result<Option<Operand>, IrError> {
		if member.is_some() && !once {
			return Err(invariant("a membership index over a statement that is not kept"));
		}
		if self.next_group()?.is_none()
			|| self.sets_statement_document()
			|| self.in_frame_of_its_own()
		{
			return Ok(None);
		}
		if holds_let(self.ir, region)? {
			return Err(IrError::Rejected(Box::new(Refusal::InvalidStatement {
				message: LET_IN_AN_EXPRESSION.to_owned(),
			})));
		}
		let mut parents = Vec::new();
		for held in std::iter::once(region).chain(hashed) {
			for value in self.ir.region_inputs(held)? {
				if read_only_as_clock(self.ir, held, value) {
					continue;
				}
				if !self.is_pipeline_row(value) {
					return Ok(None);
				}
				if !parents.contains(&value) {
					parents.push(value);
				}
			}
		}
		let hashed = match hashed {
			Some(plan) => Some((plan, lookup_key_path(self.ir, region)?)),
			None => None,
		};
		let inherits_row = inherits_row(self.ir, region)?;
		let outer = self.em.read_as_parent(parents);
		let lowered = self.probed_group(region).and_then(|(group, probed, access_mode)| {
			let hashed = match hashed {
				Some((plan, path)) => {
					let (plan, probed, plan_mode) = self.probed_group(plan)?;
					Some((plan, path, probed, plan_mode))
				}
				None => None,
			};
			let access_mode = hashed
				.as_ref()
				.map_or(access_mode, |(_, _, _, plan_mode)| access_mode.combine(*plan_mode));
			Ok((
				group,
				probed,
				hashed.map(|(plan, path, probed, _)| (plan, path, probed)),
				access_mode,
			))
		});
		self.em.restore_parent_rows(outer);
		let (group, probed, hashed, access_mode) = lowered?;
		let probed = probed || hashed.as_ref().is_some_and(|(_, _, probed)| *probed);
		let nested = self.em.nested(NestedSpec {
			group,
			inherits_row,
			once,
			owner: self.em.group(),
			hoist: None,
			read_only: access_mode == AccessMode::ReadOnly,
			probed,
			members: member.is_some(),
			hashed: hashed.map(|(plan, path, _)| (plan, path)),
		})?;
		let member = match member {
			Some((Operand::Reg(probe), negate)) => Some((probe, negate)),
			Some((probe, negate)) => {
				let reg = self.em.reg()?;
				self.emit(Instr::Load {
					dst: reg,
					src: probe,
				});
				Some((reg, negate))
			}
			None => None,
		};
		let dst = self.em.reg()?;
		let caught = self.own_catch();
		let frame = self.frame.program;
		self.emit(Instr::Block(match member {
			Some((probe, negate)) => BlockInstr::Member {
				nested,
				probe,
				dst,
				negate,
				frame,
				caught,
			},
			None => BlockInstr::Nested {
				nested,
				dst,
				frame,
				caught,
			},
		}));
		if access_mode == AccessMode::ReadWrite {
			self.writes();
		}
		Ok(Some(Operand::Reg(dst)))
	}

	/// Lowers the statement region `region` as the next group of the program, whose probes the
	/// group open now owns, and answers the group's index, whether it reads through a probe and
	/// what the group's own code does to the transaction. A unit the group evaluates is folded
	/// into the program by the compile driver.
	fn probed_group(&mut self, region: RegionId) -> Result<(u16, bool, AccessMode), IrError> {
		let Some((group, base)) = self.next_group()? else {
			return Err(invariant("a statement run in place where no group is held"));
		};
		let owner = self.em.own_probes();
		let lowered = self.lower_group(region, base, group);
		self.em.release_probes(owner);
		let (
			ReadScope {
				reads,
				tail,
			},
			permissions,
		) = lowered?;
		let probed = !reads.probes.is_empty();
		let access_mode = reads.needs.access_mode;
		let scalar = tail.as_ref().is_some_and(|tail| tail.only);
		let spec = reads.group_spec(permissions, tail, scalar, false)?;
		self.append_group(reads, spec)?;
		Ok((group, probed, access_mode))
	}

	/// The value the statement region `region` holds, run as a unit of its own in the frame being
	/// lowered: its one value where the unit's value is one row, and an array of its rows
	/// otherwise. A `RETURN` the unit signals ends the context the code being lowered evaluates,
	/// and propagates where that context is outside it ([`Lowerer::own_catch`]).
	pub(super) fn subquery_region(&mut self, region: RegionId) -> Result<Operand, IrError> {
		if holds_let(self.ir, region)? {
			return Err(IrError::Rejected(Box::new(Refusal::InvalidStatement {
				message: LET_IN_AN_EXPRESSION.to_owned(),
			})));
		}
		let inherits_row = inherits_row(self.ir, region)?;
		let caught = self.own_catch();
		self.run_unit(region, inherits_row, caught)
	}

	/// The value of the `{ ... }` block `data` in value position.
	///
	/// A block whose region holds only expressions, ending in the yield of its value or in a
	/// raise, binds no name and signals nothing past itself, so it lowers in place, where it
	/// stands ([`Lowerer::eager_region_value`]), after a [`BlockInstr::Enter`] that counts it as a
	/// nested statement when it is entered: a failure of the block, or of the expression around
	/// it, is one an aggregate does not read as `NONE`, as for a block run as a unit. It is not
	/// folded, since a folded raise would no longer be the block's. Any other block runs as the
	/// unit its region is, in the frame being lowered: a `LET` binds in it. A block is no context,
	/// so a `RETURN` it signals ends the context the code being lowered evaluates, as one a
	/// subquery signals does ([`Lowerer::own_catch`]). A `BREAK` or `CONTINUE` passes to the loop
	/// around it, and is an error where the code being lowered says one is. Its expressions are
	/// evaluated against the row of the statement that holds it.
	pub(super) fn value_block(&mut self, data: &OpData) -> Result<Operand, IrError> {
		exactly::<0>(data)?;
		let region = region_at(data, 0, 1)?;
		if holds_only_values(self.ir, region)? {
			self.refuse_in_fold("ir_lower::fold: a block evaluated in place")?;
			self.emit(Instr::Block(BlockInstr::Enter));
			return self.eager_region_value(region);
		}
		self.refuse_in_fold("ir_lower::fold: phys.block")?;
		self.require_frame(data)?;
		let caught = self.own_catch();
		self.run_unit(region, true, caught)
	}

	/// The value of the `FOR` the `phys.pipeline` `data` wraps in value position, run as the unit
	/// the pipeline's region is, in the frame being lowered. A `FOR` is no context, so a `RETURN`
	/// its body signals ends the context the code being lowered evaluates
	/// ([`Lowerer::own_catch`]), and its expressions are evaluated against the row of the
	/// statement that holds it.
	pub(super) fn value_for(&mut self, data: &OpData) -> Result<Operand, IrError> {
		let region = region_at(data, 0, 1)?;
		self.refuse_in_fold("ir_lower::fold: a FOR in value position")?;
		self.require_frame(data)?;
		let caught = self.own_catch();
		self.run_unit(region, true, caught)
	}

	/// Runs the region `region` as a unit of its own in the frame being lowered, and answers the
	/// register that holds its value. `caught` names where a signal the unit raises is taken.
	///
	/// The spec states nothing of what the unit does: the compile driver writes the access mode,
	/// required context and row-order flag of the lowered unit into it and into this program.
	fn run_unit(
		&mut self,
		region: RegionId,
		inherits_row: bool,
		caught: Option<Catch>,
	) -> Result<Operand, IrError> {
		let inputs = self.unit_inputs(region)?;
		let doc = self.statement_document()?;
		let unit = self.unit_for(UnitRequest::Subquery {
			region,
		})?;
		let subquery = self.em.subquery(SubquerySpec {
			unit,
			inherits_row,
			access_mode: AccessMode::ReadOnly,
			required_context: ContextLevel::Root,
			keeps_row_order: false,
			inputs,
		})?;
		let dst = self.em.reg()?;
		let frame = self.frame.program;
		self.emit(Instr::Subquery {
			subquery,
			dst,
			frame,
			caught,
			doc,
		});
		Ok(Operand::Reg(dst))
	}

	/// The value the call `data` is evaluated against where it is not the row: the element of the
	/// element body being lowered where the `$this` the call passes is that element, and the
	/// document root of a closure body where the `$this` it passes is that root, as an index or a
	/// method argument of the body passes it. `None` where the call is evaluated against the row.
	fn called_on_element(
		&mut self,
		data: &OpData,
		call: &CallAttrs,
	) -> Result<Option<Reg>, IrError> {
		let this = data
			.operands
			.len()
			.checked_sub(usize::from(call.row_names))
			.and_then(|at| data.operands.get(at));
		let Some(element) = self.element() else {
			return match this {
				Some(&root) if self.is_closure_root(root) => match self.value(root)? {
					Operand::Reg(root) => Ok(Some(root)),
					Operand::Const(_) | Operand::Field(_) => {
						Err(invariant("a closure body's document root read into no register"))
					}
				},
				_ => Ok(None),
			};
		};
		let Some(this) = this else {
			return Ok(Some(element));
		};
		Ok((self.value(*this)? == Operand::Reg(element)).then_some(element))
	}

	/// The value the call `data` of the user-defined function `name` answers, run as the unit its
	/// body is.
	///
	/// The call enters its function ([`Instr::FunctionEnter`]), which checks the capability, reads
	/// the definition and judges its `PERMISSIONS`, then evaluates its arguments in the frame it
	/// entered, under the definition's `AUTH LIMIT`, and runs the body ([`Instr::FunctionCall`]).
	/// The row names the op passes after its arguments are no arguments: the body reads the row it
	/// is called for from the frame. A call of a function the plan found undefined or denied holds
	/// no body, and entering it raises what the catalog or the capabilities say.
	pub(super) fn user_call(
		&mut self,
		data: &OpData,
		call: &CallAttrs,
		name: &str,
	) -> Result<Operand, IrError> {
		self.refuse_in_fold("ir_lower::fold: expr.call of a user function")?;
		let args = call_args(data, call)?;
		// A call planned per call holds no body and no guard: entering it runs the call unit of
		// the definition the transaction reads then.
		let per_call = matches!(call.callee, Some(CalleeFacts::PerCall(_)));
		let defined = match &call.callee {
			Some(CalleeFacts::Defined(defined)) => Some(defined),
			Some(CalleeFacts::Absent | CalleeFacts::PerCall(_)) => None,
			Some(CalleeFacts::LateBound) => {
				return Err(IrError::LateBind {
					what: "ir_lower::nested: a call of a user function after a catalog change",
				});
			}
			// The physical phase states facts on every call outside a stored-clause region, which
			// no program runs.
			None => {
				return Err(invariant(
					"ir_lower::nested: a call of a user function whose definition the plan did not read",
				));
			}
		};
		let unchecked = self.reads_unchecked();
		// Calls are lowered in a lowering context
		// ([`Lowerer::in_context`]), which holds the frame their units are lowered for. The
		// folder refuses a call before this point.
		let Some(lowered_for) = self.unit_frame.clone() else {
			return Err(invariant(
				"ir_lower::nested: a call of a user function with no lowering context",
			));
		};
		let limit = call_limit(self.ir, data)?;
		let (body, unchecked_body) = match defined {
			None => (None, None),
			Some(_) => {
				let body = self.unit_for(UnitRequest::FunctionBody {
					name: name.to_owned(),
					unchecked: false,
					limit: limit.clone(),
				})?;
				let unchecked_body = match unchecked {
					true => Some(self.unit_for(UnitRequest::FunctionBody {
						name: name.to_owned(),
						unchecked: true,
						limit: limit.clone(),
					})?),
					false => None,
				};
				(Some(body), unchecked_body)
			}
		};
		// A frame that reads unchecked judges no predicate, as the run skips it there.
		let guard = match defined.map(|defined| (defined.hash, defined.permission)) {
			Some((
				hash,
				CalleePermission::Predicate {
					scope: Some(scope),
				},
			)) if !unchecked => {
				let unit = self.unit_for(UnitRequest::FunctionGuard {
					name: name.to_owned(),
					limit,
				})?;
				Some((DefHash(hash), GuardCode::Unit(unit), scope))
			}
			Some(_) | None => None,
		};
		// A body whose text writes makes the call write; what the bodies it calls do is the
		// driver's to fold in.
		let writes = match &call.callee {
			Some(CalleeFacts::Defined(defined) | CalleeFacts::PerCall(defined)) => defined.writes,
			_ => false,
		};
		let body_access_mode = match writes {
			true => AccessMode::ReadWrite,
			false => AccessMode::ReadOnly,
		};
		let function = self.em.function(FunctionSpec {
			name: name.to_owned(),
			qualified: format!("fn::{name}"),
			plan_depth: self.depth,
			body_access_mode,
			defined: defined.is_some(),
			per_call,
			def_hash: DefHash(defined.map_or(0, |defined| defined.hash)),
			frame: UnitFrame {
				class_digest: 0,
				ns: lowered_for.ns,
				db: lowered_for.db,
				perms_suppressed: lowered_for.perms_suppressed,
				checked: true,
			},
			checked: unchecked,
			body,
			unchecked_body,
			guard,
			depth: CallDepth {
				site: self.call_site_depth(name),
				writes: defined.is_some_and(|defined| defined.writes),
				body: defined.map_or(0, |defined| defined.body_depth),
				deepest: defined.map_or(0, |defined| defined.deepest),
			},
			light: false,
		})?;
		let frame = self.em.call_frame()?;
		let outer = self.frame.program;
		let elem = self.called_on_element(data, call)?;
		self.emit(Instr::FunctionEnter {
			function,
			frame,
			outer,
			elem,
		});
		self.frame.program = Some(frame);
		let args = self.values(args);
		self.frame.program = outer;
		let args = args?;
		let dst = self.em.reg()?;
		self.emit(Instr::FunctionCall {
			function,
			frame,
			args,
			dst,
			outer,
			elem,
		});
		self.requires(ContextLevel::Database);
		self.keeps_row_order = true;
		if body_access_mode == AccessMode::ReadWrite {
			self.writes();
		}
		Ok(Operand::Reg(dst))
	}
}

/// The `AUTH LIMIT` the call `data` runs under, where the frame of the call carries one the
/// container's own frame does not: the clause of a definition that caps the principal. `None` for
/// a call that runs in the container's principal.
fn call_limit(ir: &Ir, data: &OpData) -> Result<Option<crate::iam::AuthLimit>, IrError> {
	let frame =
		ir.frame(data.frame).ok_or(IrError::Stale("ir_lower::nested: the frame of a call"))?;
	let base = ir
		.frame(crate::exec::ir::iface::ids::FrameId::new(0))
		.ok_or(IrError::Stale("ir_lower::nested: the container's frame"))?;
	Ok(frame.eval.auth.clone().filter(|limit| base.eval.auth.as_ref() != Some(limit)))
}

impl Lowerer<'_> {
	/// The inputs of the unit `region` is ([`Ir::region_inputs`]), each read where the
	/// instruction that runs the unit sits, and named by its position among them: one for every
	/// input of [`Ir::copy_region`]'s unit, so the two counts are equal.
	///
	/// An input the region reads only as the timestamp of a clock read is handed over as `NONE`:
	/// `expr.now` lowers to the clock builtin, which reads the statement's instant from the context
	/// the unit shares, so the unit never reads it and the requester need not hold it.
	pub(super) fn unit_inputs(&mut self, region: RegionId) -> Result<Vec<UnitInput>, IrError> {
		let order = self.ir.region_inputs(region)?;
		let mut inputs = Vec::with_capacity(order.len());
		for (at, value) in order.into_iter().enumerate() {
			let value = match read_only_as_clock(self.ir, region, value) {
				true => Operand::Const(self.em.constant(Value::None)?),
				false => self.value(value)?,
			};
			inputs.push(UnitInput {
				name: unit_input_name(at),
				value,
			});
		}
		Ok(inputs)
	}
}

/// The fields of `$parent` the key of the index lookup the statement `region` reads through takes,
/// outermost first: the path the hashed plan of a `Lookup` is keyed by.
fn lookup_key_path(ir: &Ir, region: RegionId) -> Result<Vec<Strand>, IrError> {
	let mut regions = vec![region];
	while let Some(at) = regions.pop() {
		for op in ops_in(ir, at) {
			let data = ir.op(op).ok_or(IrError::Stale("ir_lower::nested: op"))?;
			if let OpKind::Phys(PhysOp::IndexLookup(attrs)) = &data.kind
				&& attrs.key == LookupKeyShape::Parent
			{
				let key = *data
					.operands
					.first()
					.ok_or_else(|| invariant("an index lookup with no key"))?;
				return parent_path(ir, key);
			}
			regions.extend(data.regions.iter().copied());
		}
	}
	Err(invariant("a hashed lookup statement with no lookup keyed by its row"))
}

/// Whether `value` is read inside `region`, and only as the timestamp operand of `expr.now`.
pub(super) fn read_only_as_clock(ir: &Ir, region: RegionId, value: ValueId) -> bool {
	let Some(held) = ir.value(value) else {
		return false;
	};
	let mut inside = held.uses.iter().filter(|read| lies_within(ir, region, read.user)).peekable();
	inside.peek().is_some()
		&& inside.all(|read| {
			read.operand == 0
				&& ir
					.op(read.user)
					.is_some_and(|data| matches!(data.kind, OpKind::Expr(ExprOp::Now { .. })))
		})
}

/// Whether the op `op` lies in `region` or in a region nested in it.
fn lies_within(ir: &Ir, region: RegionId, op: OpId) -> bool {
	let mut at = ir.enclosing_region(op);
	loop {
		if at == region {
			return true;
		}
		let Some(owner) = ir.parent_op(at) else {
			return false;
		};
		at = ir.enclosing_region(owner);
	}
}

/// Whether the ops of `region` hold a `LET` of a local name, which is refused where it is
/// written as the statement of an expression.
/// Whether the ops of `region` are expressions alone: ops of the `expr` dialect, which end the
/// region in a yield or a raise, and the loops and blocks that stand in value position.
fn holds_only_values(ir: &Ir, region: RegionId) -> Result<bool, IrError> {
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::nested: op"))?;
		let value = match &data.kind {
			OpKind::Expr(_) | OpKind::Phys(PhysOp::Block | PhysOp::Pipeline(_)) => true,
			OpKind::Phys(PhysOp::Loop {
				kind,
				..
			}) => !matches!(kind, LoopKind::For | LoopKind::Select | LoopKind::Write),
			OpKind::Phys(_) | OpKind::Logical(_) => false,
		};
		if !value {
			return Ok(false);
		}
	}
	Ok(true)
}

fn holds_let(ir: &Ir, region: RegionId) -> Result<bool, IrError> {
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::nested: op"))?;
		if let OpKind::Phys(PhysOp::Let(attrs)) = &data.kind
			&& attrs.binding == LetBinding::Local
		{
			return Ok(true);
		}
	}
	Ok(false)
}

/// Whether the statement `region` holds evaluates its expressions against the row of the
/// statement that holds it.
///
/// A region inherits the row unless one of its ops supplies rows of its own: a source, a
/// streaming read, a select or a write loop, a nested read, a write or a `MATCH`. The
/// expression ops a bare evaluation leaves behind do not decide it.
fn inherits_row(ir: &Ir, region: RegionId) -> Result<bool, IrError> {
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::nested: op"))?;
		let supplies_rows = match &data.kind {
			OpKind::Phys(PhysOp::Loop {
				kind,
				..
			}) => !matches!(kind, LoopKind::For),
			OpKind::Phys(phys) => supplies_rows(phys.tag()),
			OpKind::Expr(_) => false,
			OpKind::Logical(_) => {
				return Err(IrError::Stale(
					"ir_lower::nested: a logical op in a physical container",
				));
			}
		};
		if supplies_rows {
			return Ok(false);
		}
	}
	Ok(true)
}

/// Whether an op of `tag` reads or writes rows of its own, so that a statement holding it does not
/// evaluate against the row of the statement around it.
fn supplies_rows(tag: PhysTag) -> bool {
	match tag {
		PhysTag::TableScan
		| PhysTag::IndexScan
		| PhysTag::IndexUnion
		| PhysTag::Bitmap
		| PhysTag::IndexLookup
		| PhysTag::FullTextScan
		| PhysTag::KeyRangeScan
		| PhysTag::KnnScan
		| PhysTag::KnnTopK
		| PhysTag::Count
		| PhysTag::RecordScan
		| PhysTag::Vacant
		| PhysTag::Enforced
		| PhysTag::Values
		| PhysTag::Subquery
		| PhysTag::Probe
		| PhysTag::Union
		| PhysTag::Join
		| PhysTag::Distinct
		| PhysTag::PermissionFilter
		| PhysTag::Layer
		| PhysTag::Filter
		| PhysTag::Fetch
		| PhysTag::Split
		| PhysTag::Sort
		| PhysTag::Window
		| PhysTag::Aggregate
		| PhysTag::Limit
		| PhysTag::Project
		| PhysTag::Timeout
		| PhysTag::VersionScope
		| PhysTag::Only
		| PhysTag::Match
		| PhysTag::MatchStep
		| PhysTag::Write
		| PhysTag::WriteStage
		| PhysTag::Events
		| PhysTag::Assign
		| PhysTag::Unset
		| PhysTag::Content
		| PhysTag::Loop
		| PhysTag::Pipeline
		| PhysTag::Choose
		| PhysTag::Prefetch
		| PhysTag::Materialize
		| PhysTag::LoadUnit => true,
		PhysTag::Explain
		| PhysTag::Eval
		| PhysTag::Request
		| PhysTag::Txn
		| PhysTag::Stmt
		| PhysTag::Statement
		| PhysTag::DdlGuard
		| PhysTag::Define
		| PhysTag::Remove
		| PhysTag::Alter
		| PhysTag::Rebuild
		| PhysTag::Info
		| PhysTag::Use
		| PhysTag::Option
		| PhysTag::Access
		| PhysTag::Kill
		| PhysTag::Live
		| PhysTag::Subscriber
		| PhysTag::Show
		| PhysTag::Block
		| PhysTag::Let
		| PhysTag::Sleep
		| PhysTag::If
		| PhysTag::Yield
		| PhysTag::Return
		| PhysTag::Break
		| PhysTag::Next => false,
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::exec::guard_verdicts::GuardScope;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::value::ValueDef;
	use crate::exec::ir::expr::ops::{ExprOp, NowWhich};
	use crate::exec::ir::iface::ids::ValueId;
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::ir::logical::ops::LetAttrs;
	use crate::exec::ir::phys::ops::IndexLookupAttrs;
	use crate::exec::vm::ir_lower::block::Groups;
	use crate::exec::vm::ir_lower::emit::Emitter;
	use crate::exec::vm::ir_lower::expr::{Needs, ReturnScope};
	use crate::exec::vm::ir_lower::pipe::tests::{placed, region_row, scan, select_loop};
	use crate::exec::vm::ir_lower::pipe::{PoolBase, Reads};
	use crate::exec::vm::ir_lower::read::tests::{named, resolver};
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::ir_lower::units::{LowerCx, Needed};
	use crate::exec::vm::program::{Program, Source, UnitId};
	use crate::expr::BinaryOperator;
	use crate::expr::logical::EvalFrame;
	use crate::val::Value;

	/// The statement `body` builds in value position, held in an expression of `strategy`.
	fn subquery_of(
		b: &mut Builder,
		strategy: NestedStrategy,
		body: impl FnOnce(&mut Builder) -> ValueId,
	) -> ValueId {
		let owner = b.add_with_regions(ExprOp::Subquery(strategy), &[]);
		b.region(owner, RegionKind::Subquery, body);
		b.result_of(owner)
	}

	/// A statement that evaluates the literal `1`.
	fn one(b: &mut Builder, strategy: NestedStrategy) -> ValueId {
		subquery_of(b, strategy, |b| b.literal(Value::from(1_i64)))
	}

	/// Lowers `value` into one pipeline for the context `cx`, under `scope` where one is given.
	fn lower_in(
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		value: ValueId,
		scope: Option<ReturnScope>,
	) -> Result<(Program, Needs, Vec<Needed>), IrError> {
		let mut em = Emitter::new();
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		match scope {
			Some(scope) => lowerer.catching(scope, |lowerer| lowerer.value(value))?,
			None => lowerer.value(value)?,
		};
		let needs = lowerer.needs();
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let program = em.finish_program(vec![pipeline], needs.access_mode)?;
		Ok((program, needs, cx.take_needed()))
	}

	/// The one `Instr::Subquery` of `program`'s one pipeline, with the index it sits at.
	fn instruction(program: &Program) -> (usize, &Instr) {
		program.pipelines[0]
			.code
			.iter()
			.enumerate()
			.find(|(_, instr)| matches!(instr, Instr::Subquery { .. }))
			.expect("the subquery is emitted")
	}

	fn spec_of(program: &Program) -> &SubquerySpec {
		let (
			_,
			Instr::Subquery {
				subquery,
				..
			},
		) = instruction(program)
		else {
			panic!("the subquery is emitted");
		};
		&program.subqueries[*subquery as usize]
	}

	#[test]
	fn a_statement_in_an_expression_is_one_instruction_over_a_unit_of_its_region() {
		let mut b = Builder::new();
		let value = one(&mut b, NestedStrategy::Apply);
		let (program, needs, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the subquery lowers");
		assert_eq!(program.subqueries.len(), 1, "the site has an entry of the program's pool");
		let spec = spec_of(&program);
		assert_eq!(spec.unit, UnitId(0));
		assert!(spec.inherits_row, "a bare evaluation reads the row of its statement");
		assert_eq!(spec.access_mode, AccessMode::ReadOnly);
		// What the unit's program says of itself is the compile driver's to fold in.
		assert_eq!(spec.required_context, ContextLevel::Root);
		assert!(!spec.keeps_row_order);
		let (
			_,
			Instr::Subquery {
				caught,
				doc,
				frame,
				..
			},
		) = instruction(&program)
		else {
			panic!("the subquery is emitted");
		};
		assert!(caught.is_none() && doc.is_none() && frame.is_none());
		assert_eq!(needed.len(), 1);
		assert!(matches!(needed[0].request, UnitRequest::Subquery { .. }));
		assert_eq!(needed[0].id, Some(UnitId(0)));
		assert_eq!(needs.access_mode, AccessMode::ReadOnly);
	}

	#[test]
	fn two_statements_are_two_sites_and_two_units() {
		let mut b = Builder::new();
		let first = one(&mut b, NestedStrategy::Apply);
		let second = one(&mut b, NestedStrategy::Apply);
		let both = b.binary(BinaryOperator::Add, first, second);
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, both, None).expect("the subqueries lower");
		assert_eq!(program.subqueries.len(), 2);
		assert_ne!(program.subqueries[0].unit, program.subqueries[1].unit);
		assert_eq!(needed.len(), 2);
	}

	/// Where the code being lowered holds no group of the program, a `Once` statement runs as a
	/// unit, which keeps no value past one evaluation.
	#[test]
	fn with_no_group_held_a_statement_that_runs_once_runs_as_a_unit() {
		let mut b = Builder::new();
		let value = one(&mut b, NestedStrategy::Once);
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the subquery lowers");
		assert!(program.nested.is_empty());
		assert_eq!(program.subqueries.len(), 1);
		assert_eq!(needed.len(), 1);
	}

	/// `$parent` followed by the field reads `path`.
	fn parent_path(b: &mut Builder, path: &[&str]) -> ValueId {
		let mut at = b.add(
			ExprOp::FreeParam {
				name: Strand::from("parent"),
			},
			&[],
		);
		for field in path {
			at = b.add(
				ExprOp::Access {
					field: Strand::from(*field),
				},
				&[at],
			);
		}
		at
	}

	/// The rows of the table scan `rows` kept by a probe keyed by their field `a` against
	/// `$parent.x`, the probe a `Hash` statement reads through.
	fn probed(b: &mut Builder) -> ValueId {
		let rows = scan(b);
		let key = parent_path(b, &["x"]);
		let probe = b.create_kind(
			OpKind::Phys(PhysOp::Probe {
				field: Strand::from("a"),
			}),
			&[rows, key],
			1,
		);
		b.result_of(probe)
	}

	/// The rows of an index lookup of `t` through `by_a`, keyed by `$parent.x`.
	fn looked_up(b: &mut Builder) -> ValueId {
		let key = parent_path(b, &["x"]);
		let lookup = b.create_kind(
			OpKind::Phys(PhysOp::IndexLookup(Box::new(IndexLookupAttrs {
				table: TableName::from("t"),
				index: named("by_a"),
				field: Strand::from("a"),
				key: LookupKeyShape::Parent,
				read: Default::default(),
			}))),
			&[key],
			1,
		);
		b.result_of(lookup)
	}

	/// A read statement of `strategy` in value position, past the physical boundary and placed:
	/// each of `regions` builds the rows a select loop of a pipeline of its own reads.
	fn read_statement(
		strategy: NestedStrategy,
		regions: Vec<fn(&mut Builder) -> ValueId>,
	) -> (Builder, ValueId) {
		let mut b = Builder::new();
		let owner = b.add_with_regions(ExprOp::Subquery(strategy), &[]);
		for (at, rows) in regions.into_iter().enumerate() {
			b.open(owner, RegionKind::Subquery, &[]);
			let rows = rows(&mut b);
			let id = u32::try_from(at).expect("a pipeline id");
			let pipeline = select_loop(&mut b, id, rows, |_, element| element);
			let delivered = b.result_of(pipeline);
			b.create_kind(OpKind::Phys(PhysOp::Yield), &[delivered], 0);
			b.close();
		}
		let value = b.result_of(owner);
		(placed(b), value)
	}

	/// Lowers `value` into the one pipeline of a statement program for the context `cx`, inside
	/// the group `group` when one is given, and answers the program and the groups it holds.
	fn lower_held(
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		value: ValueId,
		group: Option<u16>,
	) -> Result<(Program, Groups), IrError> {
		let mut em = Emitter::new();
		let mark = group.map(|group| em.open_group(group));
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		lowerer.hold_block_groups();
		lowerer.value(value)?;
		let groups = lowerer.take_block_groups();
		let needs = lowerer.needs();
		if let Some(mark) = mark {
			em.close_group(mark)?;
		}
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let program = em.finish_program(vec![pipeline], needs.access_mode)?;
		Ok((program, groups))
	}

	/// The one `BlockInstr::Nested` of `program`'s one pipeline.
	fn nested_instruction(program: &Program) -> &BlockInstr {
		program.pipelines[0]
			.code
			.iter()
			.find_map(|instr| {
				if let Instr::Block(
					block @ BlockInstr::Nested {
						..
					},
				) = instr
				{
					Some(block)
				} else {
					None
				}
			})
			.expect("the nested statement is emitted")
	}

	/// Lowers `value` into the body of a pipeline of a read statement for the context `cx`, and
	/// answers the program.
	fn lower_reading(cx: &mut LowerCx<'_>, ir: &Ir, value: ValueId) -> Result<Program, IrError> {
		let mut em = Emitter::new();
		let mut reads = Reads::empty(PoolBase::default());
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx).with_reads(&mut reads);
		lowerer.value(value)?;
		let needs = lowerer.needs();
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		em.finish_program(vec![pipeline], needs.access_mode)
	}

	/// In the body of a pipeline of a read statement, a `Once` statement is a group of the program
	/// run in place, whose value is kept from its first evaluation until the group that evaluates
	/// it, here the program's own code, starts again.
	#[test]
	fn a_statement_that_runs_once_in_a_read_runs_in_place_and_keeps_its_value() {
		let (b, value) = read_statement(NestedStrategy::Once, vec![scan]);
		let program =
			lower_reading(&mut LowerCx::detached(), &b.ir, value).expect("the statement lowers");
		assert!(program.subqueries.is_empty(), "no unit runs the statement");
		assert_eq!(
			program.nested,
			vec![NestedSpec {
				group: 0,
				inherits_row: false,
				once: true,
				owner: None,
				hoist: None,
				read_only: true,
				probed: false,
				members: false,
				hashed: None,
			}]
		);
	}

	/// In the list of a statement program, which a loop may run again over the same state, a
	/// `Once` statement runs as a unit.
	#[test]
	fn a_statement_that_runs_once_in_a_statement_list_runs_as_a_unit() {
		let (b, value) = read_statement(NestedStrategy::Once, vec![scan]);
		let (program, _) =
			lower_held(&mut LowerCx::detached(), &b.ir, value, None).expect("the statement lowers");
		assert!(program.nested.is_empty());
		assert_eq!(program.subqueries.len(), 1);
	}

	/// A `Hash` statement is a group of the program run in place: its pipelines are the program's,
	/// its probe's build input a group nested in it, and the probe is owned by the code that
	/// evaluates the statement, here the program's own.
	#[test]
	fn a_hash_runs_in_place_as_a_group_whose_probe_the_evaluating_code_owns() {
		let (b, value) = read_statement(
			NestedStrategy::Hash {
				field: Strand::from("a"),
			},
			vec![probed],
		);
		let (program, groups) =
			lower_held(&mut LowerCx::detached(), &b.ir, value, None).expect("the statement lowers");
		assert!(program.subqueries.is_empty(), "no unit runs the statement");
		assert_eq!(
			program.nested,
			vec![NestedSpec {
				group: 0,
				inherits_row: false,
				once: false,
				owner: None,
				hoist: None,
				read_only: true,
				probed: true,
				members: false,
				hashed: None,
			}]
		);
		let BlockInstr::Nested {
			nested,
			caught,
			frame,
			..
		} = nested_instruction(&program)
		else {
			panic!("the nested statement is emitted");
		};
		assert_eq!(*nested, 0);
		assert!(caught.is_none() && frame.is_none());
		// The statement's group, then the build input of its probe nested in it.
		assert_eq!(groups.specs.len(), 2);
		assert_eq!(groups.probes, vec![None]);
		assert_eq!(groups.pipelines.len(), 2);
		assert_eq!(groups.pipelines[0].group, Some(1));
		assert!(matches!(groups.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(groups.pipelines[1].group, Some(0));
		assert_eq!(
			groups.pipelines[1].source,
			Source::Probe {
				build: 1,
				probe: 0,
				field: Strand::from("a"),
				outer: vec![Strand::from("x")],
			}
		);
	}

	/// The probe of a statement lowered inside a group is owned by that group, whose start empties
	/// its kept rows, and so is the statement: not by the statement's own group, which starts again
	/// at every evaluation.
	#[test]
	fn the_probe_of_a_statement_in_a_group_is_owned_by_that_group() {
		let (b, value) = read_statement(
			NestedStrategy::Hash {
				field: Strand::from("a"),
			},
			vec![probed],
		);
		let (program, groups) = lower_held(&mut LowerCx::detached(), &b.ir, value, Some(7))
			.expect("the statement lowers");
		assert_eq!(program.nested[0].owner, Some(7));
		assert_eq!(groups.probes, vec![Some(7)]);
	}

	/// A `Lookup` that keeps a hashed plan is two groups: its own, which reads through the index,
	/// and the hashed plan's, which reads through a probe. The spec names the second with the path
	/// into `$parent` of the key the lookup reads.
	#[test]
	fn a_lookup_that_keeps_a_hashed_plan_runs_in_place_with_both_groups() {
		let (b, value) = read_statement(
			NestedStrategy::Lookup {
				index: Box::new(named("by_a")),
				hashed: true,
			},
			vec![looked_up, probed],
		);
		let resolver = resolver();
		let (program, groups) =
			lower_held(&mut LowerCx::detached().with_indexes(&resolver), &b.ir, value, None)
				.expect("the statement lowers");
		let spec = &program.nested[0];
		assert_eq!(spec.group, 0);
		assert_eq!(spec.hashed, Some((1, vec![Strand::from("x")])));
		assert!(spec.probed, "the hashed plan reads through a probe");
		assert!(!spec.once && !spec.members);
		// The lookup's group, the hashed plan's, and the build input of its probe.
		assert_eq!(groups.specs.len(), 3);
		assert_eq!(groups.probes, vec![None]);
		assert!(matches!(
			groups.pipelines[0].source,
			Source::Lookup {
				outer: Some(_),
				..
			}
		));
	}

	/// Where the code being lowered holds no group of the program, a `Lookup` runs its own region
	/// as a unit, which reads through the index for every evaluation, and so does a `Hash`, whose
	/// probe no evaluation owns in the unit.
	#[test]
	fn with_no_group_held_a_lookup_and_a_hash_run_as_a_unit() {
		let (b, value) = read_statement(
			NestedStrategy::Lookup {
				index: Box::new(named("by_a")),
				hashed: false,
			},
			vec![looked_up],
		);
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the statement lowers");
		assert!(program.nested.is_empty());
		assert_eq!(program.subqueries.len(), 1);
		assert!(matches!(needed[0].request, UnitRequest::Subquery { .. }));

		let (b, value) = read_statement(
			NestedStrategy::Hash {
				field: Strand::from("a"),
			},
			vec![probed],
		);
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the statement lowers");
		assert!(program.nested.is_empty());
		assert_eq!(program.subqueries.len(), 1);
		assert!(matches!(needed[0].request, UnitRequest::Subquery { .. }));
	}

	/// The code of a statement run in place reads the row it is evaluated for, which it reads from
	/// outside its regions, as `$parent`.
	#[test]
	fn a_row_read_from_outside_a_statement_run_in_place_is_parent() {
		let mut b = Builder::new();
		let rows = scan(&mut b);
		let looped = select_loop(&mut b, 0, rows, |b, element| {
			b.add(
				ExprOp::Access {
					field: Strand::from("x"),
				},
				&[element],
			)
		});
		let b = placed(b);
		let loop_op = ops_in(&b.ir, b.ir.op(looped).expect("the pipeline").regions[0])
			.find(|op| {
				matches!(
					b.ir.op(*op).map(|data| &data.kind),
					Some(OpKind::Phys(PhysOp::Loop { .. }))
				)
			})
			.expect("the select loop");
		let row = region_row(&b.ir, loop_op);
		let field =
			ops_in(&b.ir, *b.ir.op(loop_op).expect("the loop").regions.last().expect("a body"))
				.find_map(|op| {
					let data = b.ir.op(op)?;
					matches!(data.kind, OpKind::Expr(ExprOp::Access { .. }))
						.then(|| data.results[0])
				})
				.expect("the field read");
		let mut em = Emitter::new();
		let outer = em.read_as_parent(vec![row]);
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		lowerer.value(field).expect("the field read lowers");
		em.restore_parent_rows(outer);
		assert!(!em.is_parent_row(row));
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline");
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("the program");
		assert_eq!(program.params.len(), 1);
		assert_eq!(program.params[0].name.as_str(), "parent");
		assert!(matches!(
			program.pipelines[0].code[0],
			Instr::LoadParam {
				param: 0,
				..
			}
		));
	}

	/// The path the hashed plan of a `Lookup` is keyed by is the key of the lookup its own region
	/// reads through, wherever in the region the lookup stands.
	#[test]
	fn the_hashed_path_is_the_key_of_the_lookup_the_statement_reads_through() {
		let (b, value) = read_statement(
			NestedStrategy::Lookup {
				index: Box::new(named("by_a")),
				hashed: false,
			},
			vec![looked_up],
		);
		let owner = match b.ir.value(value).map(|held| held.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => op,
			_ => panic!("the statement's value"),
		};
		let region = b.ir.op(owner).expect("the statement").regions[0];
		assert_eq!(lookup_key_path(&b.ir, region).expect("the path"), vec![Strand::from("x")]);
		let (b, value) = read_statement(NestedStrategy::Apply, vec![|b: &mut Builder| scan(b)]);
		let owner = match b.ir.value(value).map(|held| held.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => op,
			_ => panic!("the statement's value"),
		};
		let region = b.ir.op(owner).expect("the statement").regions[0];
		assert!(matches!(lookup_key_path(&b.ir, region), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_return_ends_the_context_it_is_lowered_in() {
		let mut b = Builder::new();
		let value = one(&mut b, NestedStrategy::Apply);
		let mut em = Emitter::new();
		let mut cx = LowerCx::detached();
		let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
		let context =
			lowerer.context_value(|lowerer| lowerer.value(value)).expect("the subquery lowers");
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("a program");
		let (
			at,
			Instr::Subquery {
				dst,
				caught,
				..
			},
		) = instruction(&program)
		else {
			panic!("the subquery is emitted");
		};
		let on_return = caught.and_then(|catch| catch.on_return).expect("the signal is taken");
		// The RETURN's value is the context's, not the statement's that signals it, and the code
		// resumes past the context's evaluation.
		assert_ne!(on_return.value, *dst);
		assert_eq!(Operand::Reg(on_return.value), context);
		assert!(on_return.label as usize > at + 1);

		// Without a context the signal propagates.
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the subquery lowers");
		let (
			_,
			Instr::Subquery {
				caught,
				..
			},
		) = instruction(&program)
		else {
			panic!("the subquery is emitted");
		};
		assert!(caught.is_none());
	}

	#[test]
	fn a_permission_frame_owns_the_signals_raised_in_it_unless_the_context_is_its_own() {
		for inside in [false, true] {
			let mut b = Builder::new();
			let frame = b.frame(EvalFrame::permission(None));
			let value = b.in_frame(frame, |b| one(b, NestedStrategy::Apply));
			let mut em = Emitter::new();
			let mut cx = LowerCx::detached();
			let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
			let lowered = match inside {
				true => lowerer.in_frame(frame, 0, |lowerer| {
					lowerer.context_value(|lowerer| lowerer.value(value))
				}),
				false => lowerer.context_value(|lowerer| {
					lowerer.in_frame(frame, 0, |lowerer| lowerer.value(value))
				}),
			};
			lowered.expect("the subquery lowers");
			em.emit(Instr::EndRow);
			let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
			let program =
				em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("a program");
			let (
				_,
				Instr::Subquery {
					caught,
					frame,
					..
				},
			) = instruction(&program)
			else {
				panic!("the subquery is emitted");
			};
			assert_eq!(*frame, Some(0), "the unit runs in the frame it was lowered for");
			assert_eq!(caught.is_some(), inside, "context in the frame = {inside}");
		}
	}

	#[test]
	fn a_statement_that_supplies_its_own_rows_does_not_inherit_the_row() {
		let mut b = Builder::new();
		let owner = b.add_with_regions(ExprOp::Subquery(NestedStrategy::Apply), &[]);
		b.open(owner, RegionKind::Subquery, &[]);
		b.add_phys(PhysOp::Only, &[]);
		b.close();
		let relational = b.result_of(owner);
		let (program, _, _) = lower_in(&mut LowerCx::detached(), &b.ir, relational, None)
			.expect("the subquery lowers");
		assert!(!spec_of(&program).inherits_row);

		// A statement of control flow evaluates against the row it is held in.
		let mut b = Builder::new();
		let owner = b.add_with_regions(ExprOp::Subquery(NestedStrategy::Apply), &[]);
		b.open(owner, RegionKind::Subquery, &[]);
		let returned = b.literal(Value::from(1_i64));
		b.add_phys(PhysOp::Return, &[returned]);
		b.close();
		let control = b.result_of(owner);
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, control, None).expect("the subquery lowers");
		assert!(spec_of(&program).inherits_row);
	}

	/// The lowering states nothing of what a unit it runs does: the spec and the program it is
	/// lowered into stay `ReadOnly` until the compile driver folds the lowered unit into them
	/// (`ir_compile::tests::a_subquery_unit_gives_its_requester_what_only_its_lowering_knows`).
	#[test]
	fn a_statement_run_as_a_unit_leaves_its_access_mode_to_the_driver() {
		let mut b = Builder::new();
		let value = subquery_of(&mut b, NestedStrategy::Apply, |b| b.call("api::invoke", &[]));
		let (program, needs, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the subquery lowers");
		assert_eq!(spec_of(&program).access_mode, AccessMode::ReadOnly);
		assert_eq!(needs.access_mode, AccessMode::ReadOnly);
		assert_eq!(program.access_mode, AccessMode::ReadOnly);
	}

	#[test]
	fn a_let_written_as_the_statement_of_an_expression_is_refused_with_mains_text() {
		let mut b = Builder::new();
		let owner = b.add_with_regions(ExprOp::Subquery(NestedStrategy::Apply), &[]);
		b.open(owner, RegionKind::Subquery, &[]);
		let bound = b.literal(Value::from(1_i64));
		b.add_phys(
			PhysOp::Let(Box::new(LetAttrs {
				name: Strand::new("a"),
				binding: LetBinding::Local,
				ty: None,
			})),
			&[bound],
		);
		b.close();
		let value = b.result_of(owner);
		let Err(IrError::Rejected(refusal)) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None)
		else {
			panic!("the LET is refused");
		};
		assert_eq!(
			*refusal,
			Refusal::InvalidStatement {
				message: LET_IN_AN_EXPRESSION.to_owned(),
			}
		);
	}

	/// The free parameter `name`, a value defined by an op that is no literal.
	fn free(b: &mut Builder, name: &str) -> ValueId {
		b.add(
			ExprOp::FreeParam {
				name: Strand::new(name),
			},
			&[],
		)
	}

	/// The region of the statement `value` is the result of.
	fn region_of(b: &Builder, value: ValueId) -> RegionId {
		let Some(ValueDef::OpResult {
			op,
			..
		}) = b.ir.value(value).map(|held| held.def)
		else {
			panic!("the statement is an op's result");
		};
		b.ir.op(op).and_then(|data| data.regions.first().copied()).expect("the op holds a region")
	}

	#[test]
	fn a_statement_takes_the_values_it_reads_from_outside_as_inputs_in_the_copys_order() {
		let mut b = Builder::new();
		let x = free(&mut b, "x");
		let y = free(&mut b, "y");
		let value = subquery_of(&mut b, NestedStrategy::Apply, |b| {
			let one = b.literal(Value::from(1_i64));
			let first = b.binary(BinaryOperator::Add, y, one);
			let second = b.binary(BinaryOperator::Add, first, x);
			b.binary(BinaryOperator::Add, second, y)
		});
		let region = region_of(&b, value);
		// The order is the copy's: first read first, a value read twice once, a literal no input.
		let order = b.ir.region_inputs(region).expect("the region is whole");
		assert_eq!(order, vec![y, x]);
		assert_eq!(b.ir.copy_region(region).expect("the region is copied").inputs, order);
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the statement lowers");
		assert_eq!(needed.len(), 1);
		let spec = spec_of(&program);
		let names: Vec<_> = spec.inputs.iter().map(|input| input.name.clone()).collect();
		assert_eq!(names, vec![unit_input_name(0), unit_input_name(1)]);
		// Each input is read, in the order the unit names them, before the instruction runs it.
		let (at, _) = instruction(&program);
		let code = &program.pipelines[0].code;
		let read: Vec<_> = spec
			.inputs
			.iter()
			.map(|input| {
				let Operand::Reg(reg) = input.value else {
					panic!("an input is read into a register");
				};
				code.iter()
					.position(|i| matches!(i, Instr::LoadParam { dst, .. } if *dst == reg))
					.expect("the input is read")
			})
			.collect();
		assert!(read.iter().all(|read| *read < at));
		let param = |at: usize| {
			let Instr::LoadParam {
				param,
				..
			} = &code[at]
			else {
				panic!("an input is read as a parameter");
			};
			program.params[*param as usize].name.clone()
		};
		assert_eq!(param(read[0]), Strand::new("y"));
		assert_eq!(param(read[1]), Strand::new("x"));
	}

	#[test]
	fn a_value_read_only_as_the_clocks_timestamp_is_handed_over_as_none() {
		let mut b = Builder::new();
		// Stands for the statement timestamp the enclosing region defines.
		let timestamp = free(&mut b, "timestamp");
		let value = subquery_of(&mut b, NestedStrategy::Apply, |b| {
			b.add(
				ExprOp::Now {
					which: NowWhich::Datetime,
				},
				&[timestamp],
			)
		});
		let region = region_of(&b, value);
		assert_eq!(b.ir.region_inputs(region).expect("the region is whole"), vec![timestamp]);
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the statement lowers");
		// The unit still takes one input, so the requester hands one over: `NONE`, not the
		// timestamp, which the program holds no operand for.
		let inputs = &spec_of(&program).inputs;
		assert_eq!(inputs.len(), 1);
		assert_eq!(inputs[0].name, unit_input_name(0));
		let Operand::Const(at) = inputs[0].value else {
			panic!("the clock's timestamp is handed over as a constant");
		};
		assert_eq!(program.consts[at as usize], Value::None);
	}

	#[test]
	fn a_unit_reads_its_inputs_as_the_parameters_its_runner_binds() {
		let mut b = Builder::new();
		let x = free(&mut b, "x");
		let mut read = None;
		let value = subquery_of(&mut b, NestedStrategy::Apply, |b| {
			let one = b.literal(Value::from(1_i64));
			let sum = b.binary(BinaryOperator::Add, x, one);
			read = Some(sum);
			sum
		});
		let region = region_of(&b, value);
		let unit = b.ir.copy_region(region).expect("the region is copied");
		assert_eq!(unit.inputs, vec![x]);
		let sum = unit.map.map_value(read.expect("the sum is built")).expect("the sum is copied");
		let count = u16::try_from(unit.inputs.len()).expect("one input");
		// A context that counts no input reads no argument of the top block.
		assert!(matches!(
			lower_in(&mut LowerCx::detached(), &unit.ir, sum, None),
			Err(IrError::Invariant {
				what: "a block argument nothing bound"
			})
		));
		let (program, _, _) =
			lower_in(&mut LowerCx::detached().with_unit_inputs(count), &unit.ir, sum, None)
				.expect("the unit lowers");
		let names: Vec<_> = program.pipelines[0]
			.code
			.iter()
			.filter_map(|i| {
				if let Instr::LoadParam {
					param,
					..
				} = i
				{
					Some(program.params[*param as usize].name.clone())
				} else {
					None
				}
			})
			.collect();
		assert_eq!(names, vec![unit_input_name(0)]);
	}

	#[test]
	fn a_statement_takes_the_document_the_code_sets_aside() {
		let mut b = Builder::new();
		let value = one(&mut b, NestedStrategy::Apply);
		let mut em = Emitter::new();
		let mut cx = LowerCx::detached();
		let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
		lowerer
			.with_statement_doc(Some(7), |lowerer| lowerer.value(value))
			.expect("the subquery lowers");
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("a program");
		let (
			_,
			Instr::Subquery {
				doc,
				..
			},
		) = instruction(&program)
		else {
			panic!("the subquery is emitted");
		};
		assert_eq!(*doc, Some(7));
	}

	#[test]
	fn a_statement_in_a_pipeline_over_a_row_is_taken_over_the_document_root_or_the_row() {
		let mut b = Builder::new();
		let value = one(&mut b, NestedStrategy::Apply);
		let row = b.literal(Value::None);
		let mut em = Emitter::new();
		let mut cx = LowerCx::detached();
		let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
		let beside = lowerer.beside();
		lowerer.bind_row(row, true);
		lowerer.value(value).expect("the subquery lowers");
		lowerer.rejoin(beside);
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("a program");
		let code = &program.pipelines[0].code;
		let root = code
			.iter()
			.find_map(|instr| {
				if let Instr::DocumentRoot {
					dst,
					..
				} = instr
				{
					Some(*dst)
				} else {
					None
				}
			})
			.expect("the document root is read");
		let (
			_,
			Instr::Subquery {
				doc,
				..
			},
		) = instruction(&program)
		else {
			panic!("the subquery is emitted");
		};
		assert_eq!(*doc, Some(root));
	}

	#[test]
	fn a_statement_is_not_in_the_folders_subset() {
		let mut b = Builder::new();
		let value = one(&mut b, NestedStrategy::Apply);
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(lowerer.value(value), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_statement_through_the_direct_entry_lowers_and_asks_for_its_unit() {
		let mut b = Builder::new();
		let value = one(&mut b, NestedStrategy::Apply);
		b.add_phys(PhysOp::Eval, &[value]);
		b.physical();
		let mut cx = LowerCx::detached();
		let lowered =
			crate::exec::vm::ir_lower::dispatch::lower_with(&mut cx, &b.ir, &FactCache::new())
				.expect("the statement lowers");
		assert_eq!(lowered.program.subqueries.len(), 1);
		assert_eq!(lowered.needs.len(), 1);
		assert!(matches!(lowered.needs[0].request, UnitRequest::Subquery { .. }));
	}

	/// The facts of a defined function whose `PERMISSIONS` clause is of kind `permission`.
	fn defined(permission: CalleePermission) -> CalleeFacts {
		CalleeFacts::Defined(crate::exec::ir::expr::ops::DefinedCallee {
			hash: 7,
			permission,
			writes: true,
			body_depth: 2,
			deepest: 3,
		})
	}

	/// A call of `fn::f` with one argument and the row name the binder passes after it, holding
	/// `callee`.
	fn user_call(b: &mut Builder, callee: Option<CalleeFacts>) -> ValueId {
		let argument = b.call("math::abs", &[]);
		let row = b.literal(Value::from(0_i64));
		b.add(
			ExprOp::Call(Box::new(CallAttrs {
				function: crate::expr::Function::Custom("f".to_owned()),
				row_names: 1,
				callee,
			})),
			&[argument, row],
		)
	}

	/// The one call of a user-defined function `program`'s one pipeline makes: the indices of its
	/// `FunctionEnter` and `FunctionCall`, the frame it enters and the arguments it passes.
	fn entered(program: &Program) -> (usize, usize, u16, &[Operand]) {
		let code = &program.pipelines[0].code;
		let (enter, entered_frame) = code
			.iter()
			.enumerate()
			.find_map(|(at, instr)| {
				if let Instr::FunctionEnter {
					frame,
					..
				} = instr
				{
					Some((at, *frame))
				} else {
					None
				}
			})
			.expect("the call enters its function");
		let (call, args) = code
			.iter()
			.enumerate()
			.find_map(|(at, instr)| {
				if let Instr::FunctionCall {
					frame,
					args,
					..
				} = instr && *frame == entered_frame
				{
					return Some((at, args.as_slice()));
				}
				None
			})
			.expect("the call runs the body in the frame it entered");
		(enter, call, entered_frame, args)
	}

	#[test]
	fn a_call_of_a_defined_function_enters_it_and_runs_its_body_unit() {
		let mut b = Builder::new();
		let value = user_call(&mut b, Some(defined(CalleePermission::Full)));
		let (program, needs, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the call lowers");
		assert_eq!(program.functions.len(), 1, "the site has an entry of the program's pool");
		let spec = &program.functions[0];
		assert_eq!(spec.name, "f");
		assert_eq!(spec.qualified, "fn::f");
		assert!(spec.defined);
		assert_eq!(spec.def_hash, DefHash(7));
		assert_eq!(spec.body, Some(UnitId(0)));
		assert_eq!(spec.unchecked_body, None);
		assert!(!spec.checked, "a call where reads are checked runs its body as bound");
		assert!(spec.guard.is_none());
		assert!(!spec.light);
		assert_eq!(
			spec.depth,
			CallDepth {
				site: 0,
				writes: true,
				body: 2,
				deepest: 3,
			}
		);
		assert_eq!(spec.frame.class_digest, 0, "the call admits any principal");
		assert!(spec.frame.checked);
		let (enter, call, frame, args) = entered(&program);
		assert!(enter < call);
		assert_eq!(frame, crate::exec::vm::program::CALL_FRAME);
		assert_eq!(args.len(), 1, "the row name the op passes is no argument");
		assert_eq!(needed.len(), 1);
		assert_eq!(
			needed[0].request,
			UnitRequest::FunctionBody {
				name: "f".to_owned(),
				unchecked: false,
				limit: None,
			}
		);
		assert_eq!(needed[0].id, Some(UnitId(0)));
		assert_eq!(needs.context, ContextLevel::Database);
		assert!(needs.keeps_row_order);
		assert_eq!(spec.body_access_mode, AccessMode::ReadWrite, "the body's text writes");
		assert_eq!(needs.access_mode, AccessMode::ReadWrite);
	}

	#[test]
	fn a_calls_arguments_are_evaluated_in_its_frame_after_it_enters() {
		let mut b = Builder::new();
		let value = user_call(&mut b, Some(defined(CalleePermission::None)));
		let (program, _, _) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the call lowers");
		let (enter, _, entered_frame, _) = entered(&program);
		let (at, frame) = program.pipelines[0]
			.code
			.iter()
			.enumerate()
			.find_map(|(at, instr)| {
				if let Instr::Call {
					frame,
					..
				} = instr
				{
					Some((at, *frame))
				} else {
					None
				}
			})
			.expect("the argument calls its builtin");
		assert!(at > enter, "the call checks what it may call before its arguments");
		assert_eq!(frame, Some(entered_frame), "under the definition's AUTH LIMIT");
	}

	#[test]
	fn a_call_after_a_catalog_change_is_planned_when_it_is_reached() {
		let mut b = Builder::new();
		let value = user_call(&mut b, Some(CalleeFacts::LateBound));
		assert!(matches!(
			lower_in(&mut LowerCx::detached(), &b.ir, value, None),
			Err(IrError::LateBind { .. })
		));
	}

	#[test]
	fn a_call_of_a_function_the_plan_found_absent_holds_no_body() {
		let mut b = Builder::new();
		let value = user_call(&mut b, Some(CalleeFacts::Absent));
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the call lowers");
		let spec = &program.functions[0];
		assert!(!spec.defined, "entering it raises what the catalog or the capabilities say");
		assert_eq!(spec.body, None);
		assert_eq!(spec.unchecked_body, None);
		assert!(needed.is_empty());
	}

	/// The physical phase states facts on every call it plans
	/// (`the_physical_phase_states_what_the_catalog_answers_of_each_callee`), so a call with none
	/// is an internal error.
	#[test]
	fn a_call_the_plan_did_not_read_is_an_internal_error() {
		let mut b = Builder::new();
		let value = user_call(&mut b, None);
		assert!(matches!(
			lower_in(&mut LowerCx::detached(), &b.ir, value, None),
			Err(IrError::Invariant { .. })
		));
	}

	/// A predicate `PERMISSIONS` whose verdict stands for `scope`.
	fn predicate(scope: Option<GuardScope>) -> CalleePermission {
		CalleePermission::Predicate {
			scope,
		}
	}

	#[test]
	fn a_predicate_judged_where_reads_are_checked_is_a_guard_unit_of_the_definition() {
		let scope = GuardScope::Statement {
			reads_data: false,
		};
		let mut b = Builder::new();
		let value = user_call(&mut b, Some(defined(predicate(Some(scope)))));
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the call lowers");
		let spec = &program.functions[0];
		assert_eq!(spec.body, Some(UnitId(0)));
		assert_eq!(
			spec.guard,
			Some((DefHash(7), GuardCode::Unit(UnitId(1)), scope)),
			"the guard is judged from the definition the body is, for as long as its scope"
		);
		let requests: Vec<_> = needed.iter().map(|needed| needed.request.clone()).collect();
		assert_eq!(
			requests,
			vec![
				UnitRequest::FunctionBody {
					name: "f".to_owned(),
					unchecked: false,
					limit: None,
				},
				UnitRequest::FunctionGuard {
					name: "f".to_owned(),
					limit: None,
				},
			]
		);
		assert!(
			!UnitRequest::FunctionGuard {
				name: "f".to_owned(),
				limit: None,
			}
			.is_relative()
		);
	}

	#[test]
	fn two_calls_of_one_guarded_function_share_its_guard_unit() {
		let mut b = Builder::new();
		let first = user_call(&mut b, Some(defined(predicate(Some(GuardScope::PerCall)))));
		let second = user_call(&mut b, Some(defined(predicate(Some(GuardScope::PerCall)))));
		let mut em = Emitter::new();
		let mut cx = LowerCx::detached();
		let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
		lowerer.value(first).expect("the first call lowers");
		lowerer.value(second).expect("the second call lowers");
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("a program");
		assert_eq!(program.functions.len(), 2);
		assert_eq!(program.functions[0].guard, program.functions[1].guard);
		assert_eq!(cx.take_needed().len(), 2, "one body and one guard");
	}

	#[test]
	fn a_predicate_the_run_raises_reading_holds_no_guard() {
		let mut b = Builder::new();
		let value = user_call(&mut b, Some(defined(predicate(None))));
		let (program, _, needed) =
			lower_in(&mut LowerCx::detached(), &b.ir, value, None).expect("the call lowers");
		assert!(program.functions[0].guard.is_none());
		assert!(
			needed
				.iter()
				.all(|needed| !matches!(needed.request, UnitRequest::FunctionGuard { .. }))
		);
	}

	#[test]
	fn a_call_where_reads_are_unchecked_asks_for_the_body_that_waives_them() {
		use crate::exec::vm::program::{FrameMode, PermissionSpec};
		use crate::expr::logical::GuardKind;
		let mut b = Builder::new();
		let frame = b.frame(EvalFrame::permission(None));
		let value = b
			.in_frame(frame, |b| user_call(b, Some(defined(predicate(Some(GuardScope::PerCall))))));
		let mut em = Emitter::new();
		let guard = em
			.permission_frame(PermissionSpec {
				mode: FrameMode::Guard(GuardKind::Read),
				auth: None,
				column: false,
			})
			.expect("a frame");
		let mut cx = LowerCx::detached();
		let mut lowerer = Lowerer::in_context(&mut em, &b.ir, &mut cx);
		lowerer.in_frame(frame, guard, |lowerer| lowerer.value(value)).expect("the call lowers");
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once).expect("the pipeline ends");
		let program = em.finish_program(vec![pipeline], AccessMode::ReadOnly).expect("a program");
		let spec = &program.functions[0];
		assert!(spec.checked, "a run that skips permissions runs the body that waives them");
		assert!(spec.body.is_some() && spec.unchecked_body.is_some());
		assert_ne!(spec.body, spec.unchecked_body);
		assert!(spec.guard.is_none(), "a frame that reads unchecked judges no predicate");
		let needed = cx.take_needed();
		assert_eq!(needed.len(), 2);
		assert!(needed.iter().any(|needed| matches!(
			&needed.request,
			UnitRequest::FunctionBody {
				unchecked: true,
				..
			}
		)));
		let outer = program.pipelines[0].code.iter().find_map(|instr| {
			if let Instr::FunctionEnter {
				outer,
				..
			} = instr
			{
				Some(*outer)
			} else {
				None
			}
		});
		assert_eq!(outer, Some(Some(guard)), "the call returns to the frame it was made in");
	}

	/// A lowerer built with no lowering context holds no frame for the units a call runs. Every
	/// lowerer that reaches a call is built in one, so a call without one is an internal
	/// error.
	#[test]
	fn a_call_lowered_with_no_lowering_context_is_an_internal_error() {
		let mut b = Builder::new();
		let value = user_call(&mut b, Some(CalleeFacts::Absent));
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::new(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(
			lowerer.value(value),
			Err(IrError::Invariant {
				what: "ir_lower::nested: a call of a user function with no lowering context",
			})
		));
	}

	#[test]
	fn a_call_is_not_in_the_folders_subset() {
		let mut b = Builder::new();
		let value = user_call(&mut b, Some(defined(CalleePermission::Full)));
		let mut em = Emitter::new();
		let mut lowerer =
			Lowerer::for_fold(&mut em, &b.ir, crate::exec::ir::analysis::cx::builtin_registry());
		assert!(matches!(lowerer.value(value), Err(IrError::Invariant { .. })));
	}

	/// A catalog that answers `gone` absent and every other function unknown.
	struct OneAbsent;

	impl crate::exec::ir::iface::catalog::SemanticCatalog for OneAbsent {
		fn function(
			&self,
			name: &str,
		) -> crate::exec::ir::iface::catalog::CatalogAnswer<
			std::sync::Arc<crate::catalog::FunctionDefinition>,
		> {
			use crate::exec::ir::iface::catalog::CatalogAnswer;
			match name {
				"gone" => CatalogAnswer::Absent,
				_ => CatalogAnswer::Unknown,
			}
		}
	}

	#[test]
	fn the_physical_phase_states_what_the_catalog_answers_of_each_callee() {
		let mut b = Builder::new();
		let call = |b: &mut Builder, name: &str| {
			b.add(ExprOp::call(crate::expr::Function::Custom(name.to_owned())), &[])
		};
		let gone = call(&mut b, "gone");
		let unknown = call(&mut b, "other");
		let builtin = b.call("math::abs", &[]);
		b.physical();
		let stated = crate::exec::ir::phys::callee::read_callees(
			&mut b.ir,
			&mut b.facts,
			&OneAbsent,
			crate::exec::ir::analysis::cx::builtin_registry(),
		)
		.expect("the callees are read");
		assert_eq!(stated, 2);
		let callee = |value: ValueId| {
			let op = crate::exec::vm::ir_lower::read::producer_of(&b.ir, value).expect("a call");
			let OpKind::Expr(ExprOp::Call(call)) = &b.ir.op(op).expect("the call").kind else {
				panic!("not a call: {:?}", b.ir.op(op).expect("the call").kind);
			};
			call.callee.clone()
		};
		assert_eq!(callee(gone), Some(CalleeFacts::Absent));
		assert_eq!(callee(unknown), Some(CalleeFacts::LateBound));
		assert_eq!(callee(builtin), None);
	}
}
