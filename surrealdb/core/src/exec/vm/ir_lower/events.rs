//! The unit that runs a table's events for one action, lowered from the container
//! [`bind_catalog_unit`](crate::exec::ir::build::stage_bundle::bind_catalog_unit) builds.
//!
//! The top region of the container is a [`WriteStage`](RegionKind::WriteStage) holding one
//! `phys.events` op and the yield that ends the region. The op states the table's event
//! definitions and the shape of each event's clauses ([`EventsAttrs`]), and hosts the clauses as
//! regions of kind [`EventFrame`](RegionKind::EventFrame): for each definition in the order the
//! catalog holds them, its `WHEN` and then each of its `THEN` clauses. An `ASYNC` event hosts no
//! `THEN` clause, as its clauses run from the stored definition when the queue drains.
//!
//! The unit is a program of host pipelines and no pipeline of its own: the write that fires the
//! events runs the pipelines the program's [`LoweredEvents`] names over the image each event is
//! about. A clause pipeline reads the image as its row, and its `$this`, `$self` and `$value` are
//! that row. `$before`, `$after`, `$event` and `$input` are read by their names through the frame
//! the run binds for the event. A clause is definer code: its pipeline is lowered in a privileged
//! frame that names no permission frame, so the caller's own bindings are never read, and the
//! frame of its ops is the one the binder states for the event's `AUTH LIMIT`.
//!
//! The definitions the unit holds are the ones the run compares against the events the written
//! table holds when a record is written, so a unit never runs against events it was not lowered
//! from.

use std::sync::Arc;

use super::dispatch::apply_needs;
use super::emit::Emitter;
use super::expr::Lowerer;
use super::pipe::region_frame;
use super::place::{Lowered, Provenance};
use super::stage::emit_clause;
use super::units::LowerCx;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::iface::ids::RegionId;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::{EventClause, EventsAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::vm::program::{Instr, Operand, ParamSpec, Pipeline};
use crate::exec::vm::write::{ClauseDepth, EventClauses, LoweredEvents};

/// Whether `ir` is the container of an events unit: its top region is a stage whose first op is a
/// `phys.events`.
pub(super) fn is_events_unit(ir: &Ir) -> bool {
	let top = ir.top();
	ir.region(top).map(|region| region.kind) == Some(RegionKind::WriteStage)
		&& ops_in(ir, top).next().is_some_and(|op| {
			matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Events(_))))
		})
}

/// Lowers the physical container `ir` of an events unit to the program that runs a table's events
/// for each record a write fires them for.
///
/// Answers [`IrError::Invariant`] for a container that is not past the physical boundary, for
/// events that state no clause to lower from, for a clause with no logical form, and for a clause
/// that holds a write no operator runs. Answers [`IrError::NotYet`] for a clause under an
/// `AUTH LIMIT` that holds a statement, a closure or a call of a function, since that clause must
/// be lowered for the narrowed principal.
pub(super) fn lower_events(cx: &mut LowerCx<'_>, ir: &Ir) -> Result<Lowered, IrError> {
	if ir.phase() < Phase::Physical {
		return Err(IrError::Invariant {
			what: "ir_lower::events: an events unit is lowered before the physical boundary",
		});
	}
	let top = ir.top();
	let ops: Vec<_> = ops_in(ir, top).collect();
	let [events, end] = <[_; 2]>::try_from(ops).map_err(|_| IrError::Invariant {
		what: "ir_lower::events: an events unit holds the events and their yield alone",
	})?;
	let data = ir.op(events).ok_or(IrError::Stale("ir_lower::events: op"))?;
	let OpKind::Phys(PhysOp::Events(attrs)) = &data.kind else {
		return Err(IrError::Invariant {
			what: "ir_lower::events: an events unit holds no events op",
		});
	};
	if !matches!(
		ir.op(end).map(|data| (&data.kind, data.operands.len())),
		Some((OpKind::Phys(PhysOp::Yield), 0))
	) {
		return Err(IrError::Invariant {
			what: "ir_lower::events: an events unit does not end in a yield of nothing",
		});
	}
	if attrs.unlowerable {
		return Err(IrError::Invariant {
			what: "ir_lower::events: an event states a clause that has no form to lower",
		});
	}
	if let Some(what) = attrs.fault() {
		return Err(IrError::Invariant {
			what,
		});
	}
	if data.regions.len() != attrs.region_kinds().len() {
		return Err(IrError::Stale("ir_lower::events: the regions of the events"));
	}

	let mut em = Emitter::new();
	let (clauses, needs) = {
		let mut lowerer = Lowerer::in_context(&mut em, ir, cx);
		let clauses = event_clauses(&mut lowerer, attrs, &mut data.regions.iter().copied())?;
		(clauses, lowerer.needs())
	};
	let mut program = em.finish_program(Vec::new(), needs.access_mode)?;
	apply_needs(&mut program, &needs);
	program.events = Some(Arc::new(LoweredEvents {
		action: attrs.action,
		events: Arc::clone(attrs.defs.definitions()),
		clauses,
	}));
	Ok(Lowered {
		program,
		provenance: Provenance::default(),
		needs: cx.take_needed(),
	})
}

/// The host pipelines of every clause of `attrs`, taking each region in turn from `regions`.
fn event_clauses(
	lowerer: &mut Lowerer<'_>,
	attrs: &EventsAttrs,
	regions: &mut impl Iterator<Item = RegionId>,
) -> Result<Vec<EventClauses>, IrError> {
	let definitions = attrs.defs.definitions();
	if definitions.len() != attrs.events.len() {
		return Err(IrError::Stale("ir_lower::events: the shapes of the events"));
	}
	let mut clauses = Vec::with_capacity(attrs.events.len());
	for (shape, definition) in attrs.events.iter().zip(definitions.iter()) {
		let when = clause(lowerer, regions, shape.when, &definition.when)?;
		let mut then = Vec::with_capacity(shape.then.len());
		let mut then_depths = Vec::with_capacity(shape.then.len());
		if shape.then.len() > definition.then.len() {
			return Err(IrError::Stale("ir_lower::events: the THEN clauses of an event"));
		}
		for (stated, source) in shape.then.iter().zip(&definition.then) {
			then.push(clause(lowerer, regions, *stated, source)?);
			then_depths.push(depth(*stated));
		}
		// Every region is lowered above, so that a clause the lowering refuses refuses the unit
		// whether or not the event is dormant. A dormant event runs no clause, so its pipelines
		// are held by the program and read by no event.
		clauses.push(match shape.dormant {
			true => EventClauses {
				when: None,
				then: Vec::new(),
				when_depth: depth(shape.when),
				then_depths: Vec::new(),
			},
			false => EventClauses {
				when: Some(when),
				then,
				when_depth: depth(shape.when),
				then_depths,
			},
		});
	}
	if regions.next().is_some() {
		return Err(IrError::Stale("ir_lower::events: a region no clause reads"));
	}
	Ok(clauses)
}

/// What evaluating `clause` spends of the computation-depth budget, as the shape states it.
fn depth(clause: EventClause) -> ClauseDepth {
	ClauseDepth {
		deepest: clause.deepest,
		write: clause.write,
	}
}

/// The index among the program's host pipelines of the pipeline of the next clause region, which
/// `stated` describes and which is lowered from `source`: the calls the clause makes spend what
/// `source` spends on the way to them ([`crate::exec::vm::depth`]).
fn clause(
	lowerer: &mut Lowerer<'_>,
	regions: &mut impl Iterator<Item = RegionId>,
	stated: EventClause,
	source: &crate::expr::Expr,
) -> Result<u16, IrError> {
	let region = regions.next().ok_or(IrError::Invariant {
		what: "ir_lower::events: the events host a region for each clause they state",
	})?;
	let sites = Arc::new(crate::exec::vm::depth::ExprDepth::of_expr(source));
	let outer = lowerer.resite(Some(sites));
	let pipeline = host(lowerer, |lowerer| clause_pipeline(lowerer, region, stated));
	lowerer.resite(outer);
	lowerer.em.host_code(pipeline?)
}

/// A pipeline, built by `build` beside the code in flight, that holds none of its registers.
fn host(
	lowerer: &mut Lowerer<'_>,
	build: impl FnOnce(&mut Lowerer<'_>) -> Result<Pipeline, IrError>,
) -> Result<Pipeline, IrError> {
	let aside = lowerer.em.set_aside();
	let beside = lowerer.beside();
	let built = build(lowerer);
	lowerer.rejoin(beside);
	lowerer.em.restore(aside);
	built
}

/// The pipeline over the image an event is about that emits the value of the clause region
/// `region`, in the privileged frame the event's clauses run in.
///
/// A statement or a call of a function the clause holds under an `AUTH LIMIT` is a unit planned
/// for the principal the limit narrows the caller to; a closure it makes there is refused with
/// [`IrError::NotYet`] ([`super::stage::refuse_narrowed`]).
fn clause_pipeline(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
	stated: EventClause,
) -> Result<Pipeline, IrError> {
	let ir = lowerer.ir;
	let container = region_frame(ir, region)?;
	super::stage::refuse_narrowed(ir, region, stated.caller_bound)?;
	bind_images(lowerer, region)?;
	lowerer.in_host_frame(container, |lowerer| {
		let value = lowerer.region_value(region)?;
		let out = lowerer.em.reg()?;
		lowerer.emit(Instr::Load {
			dst: out,
			src: value,
		});
		emit_clause(lowerer, out)
	})
}

/// Binds the arguments of the clause region `region`: after the statement timestamp, the record
/// the event is about is the row of the pipeline, an object, and `$before`, `$after`, `$event` and
/// `$input` are the parameters the run binds for the event, read where the clause reads them.
fn bind_images(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<(), IrError> {
	let ir = lowerer.ir;
	let args = lowerer.region_args(region)?;
	let [_, row, before, after, event, input] = <[_; 6]>::try_from(args).map_err(|_| {
		IrError::Stale(
			"ir_lower::events: an event clause takes the timestamp, the record, $before, $after, $event and $input",
		)
	})?;
	lowerer.bind_row(row, true);
	for (arg, name) in [(before, "before"), (after, "after"), (event, "event"), (input, "input")] {
		if ir.uses(arg).is_empty() {
			continue;
		}
		let param = lowerer.em.param(ParamSpec {
			name: surrealdb_strand::Strand::new(name),
			guard: None,
		})?;
		let dst = lowerer.em.reg()?;
		lowerer.emit(Instr::LoadParam {
			param,
			dst,
			frame: None,
		});
		lowerer.bind_arg(arg, Operand::Reg(dst));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::catalog::auth::AuthLimit;
	use crate::catalog::{EventDefinition, EventKind};
	use crate::doc::Action;
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::ids::{OpId, ValueId};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::stages::events_over;
	use crate::exec::ir::logical::verify::timestamp_arg;
	use crate::exec::ir::unit::UnitKind;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::ir_lower::units::lower_unit;
	use crate::exec::vm::program::UnitSig;
	use crate::expr::logical::EvalFrame;
	use crate::val::Value;

	fn event(name: &str, when: &str, then: &[&str], kind: EventKind) -> EventDefinition {
		EventDefinition::new(
			name.into(),
			"t".into(),
			crate::syn::expr(when).expect("a clause"),
			then.iter().map(|clause| crate::syn::expr(clause).expect("a clause")).collect(),
			kind,
			AuthLimit::new_no_limit(),
			None,
		)
	}

	fn sync(name: &str, when: &str, then: &[&str]) -> EventDefinition {
		event(name, when, then, EventKind::Sync)
	}

	fn asynchronous(name: &str, when: &str) -> EventDefinition {
		event(
			name,
			when,
			&["1"],
			EventKind::Async {
				retry: 0,
				max_depth: 0,
			},
		)
	}

	/// The block arguments of an event clause: the timestamp, the record, `$before`, `$after`,
	/// `$event` and `$input`.
	fn arguments() -> Vec<TypeKnowledge> {
		let mut args = vec![timestamp_arg()];
		args.extend(vec![TypeKnowledge::Unknown; 5]);
		args
	}

	/// The container of the events `definitions` for `Action::Update`, past the physical boundary.
	/// `clause` builds the value of the clause at a position from its block arguments, and `auth`
	/// is the narrowing the frame of every clause states.
	fn container_with(
		definitions: Vec<EventDefinition>,
		attrs: impl FnOnce(&mut crate::exec::ir::logical::ops::EventsAttrs),
		auth: Option<surrealdb_iam::AuthLimit>,
		clause: impl Fn(&mut Builder, &[ValueId], usize) -> ValueId,
	) -> Builder {
		let unit = UnitKind::Events {
			table: TableName::new("t"),
			action: Action::Update,
		};
		let mut stated = events_over(&Arc::from(definitions), Action::Update).attrs;
		attrs(&mut stated);
		let count = stated.region_kinds().len();
		let mut b = Builder::with_top(unit.top_region(), &unit.top_args());
		let op: OpId = b.add_phys(PhysOp::Events(Box::new(stated)), &[]);
		let frame = b.frame(EvalFrame::mutable_permission(auth));
		b.in_frame(frame, |b| {
			for position in 0..count {
				b.region_with_args(op, RegionKind::EventFrame, &arguments(), |b, args| {
					clause(b, args, position)
				});
			}
		});
		b.add_phys(PhysOp::Yield, &[]);
		b.physical();
		b
	}

	fn container(
		definitions: Vec<EventDefinition>,
		clause: impl Fn(&mut Builder, &[ValueId], usize) -> ValueId,
	) -> Builder {
		container_with(definitions, |_| {}, None, clause)
	}

	fn truth(b: &mut Builder, _: &[ValueId], _: usize) -> ValueId {
		b.literal(Value::Bool(true))
	}

	fn lower(b: &Builder) -> Result<Lowered, IrError> {
		lower_events(&mut LowerCx::detached(), &b.ir)
	}

	fn events_of(lowered: &Lowered) -> &LoweredEvents {
		lowered.program.events.as_deref().expect("the program holds the events")
	}

	/// A synchronous event is one host pipeline for its `WHEN` and one for each `THEN`, in the
	/// order the catalog holds the events and each event states its clauses; an `ASYNC` event is
	/// its `WHEN` alone.
	#[test]
	fn each_clause_is_one_host_pipeline_in_the_order_the_events_state_them() {
		let b = container(
			vec![
				sync("first", "true", &["1", "2"]),
				asynchronous("second", "true"),
				sync("third", "false", &[]),
			],
			truth,
		);
		let lowered = lower(&b).expect("the unit lowers");
		let events = events_of(&lowered);
		assert_eq!(events.action, Action::Update);
		assert_eq!(events.events.len(), 3);
		let shapes: Vec<(Option<u16>, Vec<u16>)> =
			events.clauses.iter().map(|clauses| (clauses.when, clauses.then.clone())).collect();
		assert_eq!(shapes, vec![(Some(0), vec![1, 2]), (Some(3), vec![]), (Some(4), vec![])]);
		assert_eq!(lowered.program.host_code.len(), 5);
		assert!(lowered.program.pipelines.is_empty(), "the unit runs no pipeline of its own");
	}

	/// A dormant event holds no clause of its own: the program still holds the pipelines every
	/// region lowers to, in the order of the regions, and the clauses of the events after it are
	/// numbered as they are without it. The depth its `WHEN` spends is kept.
	#[test]
	fn a_dormant_event_lowers_every_region_and_runs_no_clause() {
		let definitions = || {
			vec![
				sync("first", "true", &["1", "2"]),
				sync("second", "false", &["(CREATE a)"]),
				asynchronous("third", "false"),
				sync("fourth", "true", &["3"]),
			]
		};
		let live = lower(&container(definitions(), truth)).expect("the unit lowers");
		let b = container_with(
			definitions(),
			|attrs| {
				attrs.events[1].dormant = true;
				attrs.events[2].dormant = true;
			},
			None,
			truth,
		);
		let lowered = lower(&b).expect("the unit lowers");
		let events = events_of(&lowered);
		let whens: Vec<Option<u16>> = events.clauses.iter().map(|clauses| clauses.when).collect();
		let thens: Vec<Vec<u16>> =
			events.clauses.iter().map(|clauses| clauses.then.clone()).collect();
		assert_eq!(whens, vec![Some(0), None, None, Some(6)]);
		assert_eq!(thens, vec![vec![1, 2], vec![], vec![], vec![7]]);
		assert!(events.clauses[1].then_depths.is_empty());
		assert_eq!(
			events.clauses[1].when_depth,
			events_of(&live).clauses[1].when_depth,
			"the depth of the WHEN stays"
		);
		assert_eq!(lowered.program.host_code.len(), live.program.host_code.len());
	}

	/// A clause the lowering refuses refuses the unit whether or not its event is dormant. The
	/// refusal is driven with a region built directly, since no SurrealQL clause reaches the
	/// lowering's own refusal without first meeting the error of an earlier stage.
	#[test]
	fn a_dormant_event_still_refuses_what_its_regions_refuse() {
		let definitions =
			|| vec![sync("one", "true", &[]), sync("two", "false", &["(SELECT * FROM audit)"])];
		let limit = || Some(surrealdb_iam::AuthLimit::new(surrealdb_iam::Level::Root, None));
		// The `THEN` of the second event is the third region, and it makes a closure.
		let closing = |b: &mut Builder, args: &[ValueId], at: usize| match at {
			2 => b.closure(),
			_ => truth(b, args, at),
		};
		for dormant in [false, true] {
			let b = container_with(
				definitions(),
				|attrs| attrs.events[1].dormant = dormant,
				limit(),
				closing,
			);
			assert!(matches!(lower(&b), Err(IrError::NotYet { .. })), "dormant: {dormant}");
		}
	}

	/// What a clause spends of the computation-depth budget is what its shape states, and the run
	/// charges exactly that.
	#[test]
	fn a_clause_carries_the_depth_its_shape_states() {
		let b = container(vec![sync("one", "true", &["(CREATE a)", "1"])], truth);
		let lowered = lower(&b).expect("the unit lowers");
		let [clauses] = &events_of(&lowered).clauses[..] else {
			panic!("one event states one set of clauses");
		};
		assert_eq!(clauses.then_depths.len(), 2);
		assert!(
			clauses.then_depths[0].write >= clauses.then_depths[1].write,
			"a clause that writes spends at least what one that does not"
		);
	}

	/// The record is the row of a clause's pipeline, so a field of it is read from the row; and
	/// `$after` is read by its name through the frame the run binds for the event.
	#[test]
	fn the_record_is_the_row_and_the_images_are_read_through_the_frame() {
		let b = container(vec![sync("one", "true", &[])], |b, args, _| {
			b.add(
				ExprOp::Field {
					name: Strand::new("a"),
				},
				&[args[1]],
			)
		});
		let lowered = lower(&b).expect("the unit lowers");
		assert!(
			!lowered.program.host_code[0]
				.code
				.iter()
				.any(|instr| matches!(instr, Instr::LoadParam { .. })),
			"a field of the record reads no parameter"
		);

		let b = container(vec![sync("one", "true", &[])], |_, args, _| args[3]);
		let lowered = lower(&b).expect("the unit lowers");
		assert!(lowered.program.params.iter().any(|param| param.name.as_str() == "after"));
		assert!(
			lowered.program.host_code[0]
				.code
				.iter()
				.any(|instr| matches!(instr, Instr::LoadParam { .. }))
		);
	}

	/// A clause that holds a statement or a call of a function under an `AUTH LIMIT` lowers: each
	/// is a unit planned for the principal the limit narrows the caller to. A clause that makes a
	/// closure there is refused, since no unit of that principal is requested for its body. The
	/// same clauses with no limit lower.
	#[test]
	fn a_clause_that_makes_a_closure_under_an_auth_limit_is_refused() {
		let definitions = || vec![sync("one", "true", &["(SELECT * FROM audit)"])];
		let limit = || Some(surrealdb_iam::AuthLimit::new(surrealdb_iam::Level::Root, None));
		let closing = |b: &mut Builder, args: &[ValueId], at: usize| match at {
			0 => truth(b, args, at),
			_ => b.closure(),
		};
		let limited = container_with(definitions(), |_| {}, limit(), truth);
		assert!(lower(&limited).is_ok());
		let limited = container_with(definitions(), |_| {}, limit(), closing);
		assert!(matches!(lower(&limited), Err(IrError::NotYet { .. })));
		assert!(lower(&container(definitions(), truth)).is_ok());
	}

	/// An event that states a clause with no form to lower, and a shape that disagrees with the
	/// definitions it describes, are broken containers. Lowering answers [`IrError::Invariant`]
	/// for both.
	#[test]
	fn events_that_state_no_clause_to_lower_from_are_an_invariant_break() {
		let b = container_with(
			vec![sync("one", "true", &["1"])],
			|attrs| {
				attrs.unlowerable = true;
				attrs.events = Box::default();
			},
			None,
			truth,
		);
		assert!(matches!(lower(&b), Err(IrError::Invariant { .. })));
	}

	/// A table with no event is a unit with no clause, and the program holds the definitions the
	/// run compares the table's events against.
	#[test]
	fn a_table_without_events_is_a_unit_without_clauses() {
		let b = container(Vec::new(), truth);
		let lowered = lower(&b).expect("the unit lowers");
		let events = events_of(&lowered);
		assert!(events.events.is_empty() && events.clauses.is_empty());
		assert!(lowered.program.host_code.is_empty());
	}

	/// The compile driver reaches the unit through [`lower_unit`], which gives it the head of a
	/// unit.
	#[test]
	fn a_unit_lowered_through_the_driver_entry_holds_the_events_and_a_head() {
		let b = container(vec![sync("one", "true", &["1"])], truth);
		assert!(is_events_unit(&b.ir));
		let sig = UnitSig {
			params: Vec::new(),
			takes_row: false,
		};
		let lowered = lower_unit(&mut LowerCx::detached(), &b.ir, &b.facts, sig.clone())
			.expect("the unit lowers");
		assert!(lowered.program.events.is_some());
		assert_eq!(lowered.program.unit.as_ref().map(|head| head.sig.clone()), Some(sig));
	}

	/// A container that has not crossed the physical boundary is never lowered.
	#[test]
	fn a_container_before_the_physical_boundary_is_an_invariant_break() {
		let unit = UnitKind::Events {
			table: TableName::new("t"),
			action: Action::Update,
		};
		let mut b = Builder::with_top(unit.top_region(), &unit.top_args());
		let attrs = events_over(&Arc::from(Vec::new()), Action::Update).attrs;
		b.add_phys(PhysOp::Events(Box::new(attrs)), &[]);
		b.add_phys(PhysOp::Yield, &[]);
		assert!(matches!(lower(&b), Err(IrError::Invariant { .. })));
	}

	/// A container that is not an events unit is not taken for one.
	#[test]
	fn a_container_that_holds_no_events_is_not_an_events_unit() {
		assert!(!is_events_unit(&Builder::new().ir));
	}
}
