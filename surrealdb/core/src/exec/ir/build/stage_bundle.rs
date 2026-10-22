//! Containers of the units that run what a table stores, built from the definitions the catalog
//! holds for the table rather than bound from a statement (`UnitForm::Catalog`).
//!
//! # The events of a table
//!
//! The unit of [`UnitKind::Events`] holds one `logical.events` op in its top region, which is
//! the stored-code kind of a stage bundle ([`RegionKind::WriteStage`]) so that the footprint, the
//! effect walk and the callee walk see what the clauses read, write and call. The op states the
//! event definitions of the table whole and the shape of each event's clauses
//! ([`EventsAttrs`](crate::exec::ir::logical::ops::EventsAttrs)); its regions are the clauses,
//! each of kind [`RegionKind::EventFrame`], in the order the shapes state: for each definition in
//! the order the catalog holds them, its `WHEN` clause and then each of its `THEN` clauses.
//!
//! A clause is bound with permissions off and writes allowed, under the event's `AUTH LIMIT`
//! ([`events_over`]). Its row is the image the event is about, which `$this`, `$self` and `$value`
//! name, and `$before`, `$after`, `$event` and `$input` follow it as block arguments.
//!
//! The table's events are read at the version the compile is bound to. An answer the memo does
//! not hold yet leaves the table pending and the unit is bound again over what the memo loads; an
//! answer that predates a catalog change of the compile cannot describe the events a record meets
//! and answers [`IrError::LateBind`].

use std::sync::Arc;

use super::logical::bind_schema_region;
use crate::catalog::EventDefinition;
use crate::doc::Action;
use crate::exec::ir::RequestInput;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx};
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::logical::seam::{BindCx, invariant};
use crate::exec::ir::logical::stages::{TableEvents, events_over};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::unit::UnitKind;
use crate::exec::surface::Surface;

/// Binds the container of the unit `input` names, which holds no statement.
///
/// Answers [`IrError::Invariant`] for a unit that is not built from the catalog.
pub(super) fn bind_catalog_unit(
	input: &RequestInput<'_>,
	effects: &EffectCx<'_>,
	surface: Option<&'static Surface>,
) -> Result<Ir, IrError> {
	let UnitKind::Events {
		table,
		action,
	} = &input.unit
	else {
		return Err(invariant("a unit built from the catalog is the events of a table"));
	};
	if !input.statements.is_empty() {
		return Err(invariant("a unit built from the catalog holds no statement"));
	}
	let events: Arc<[EventDefinition]> = match effects.catalog.events(table) {
		CatalogAnswer::Found(events) => events,
		// A table the catalog does not define has no event, and a table the memo has yet to load
		// is bound again once it has.
		CatalogAnswer::Absent | CatalogAnswer::NotLoaded => Arc::from([]),
		CatalogAnswer::Unknown => {
			return Err(IrError::LateBind {
				what: "stage_bundle: the events of a table that a catalog change precedes",
			});
		}
	};
	bind_events(input, effects, &events, *action, surface)
}

/// The container of the events `events` of a table for `action`.
fn bind_events(
	input: &RequestInput<'_>,
	effects: &EffectCx<'_>,
	events: &Arc<[EventDefinition]>,
	action: Action,
	surface: Option<&'static Surface>,
) -> Result<Ir, IrError> {
	let TableEvents {
		attrs,
		regions,
	} = events_over(events, action);
	let mut ir =
		Ir::with_top_args(input.scope.clone(), input.unit.top_region(), &input.unit.top_args());
	let mut facts = FactCache::new();
	{
		let mut cx = BindCx::new(&mut ir, &mut facts, *effects)?;
		cx.set_surface(surface);
		cx.set_statement(input.unit.position())?;
		cx.begin_statement();
		let op = cx.emit(OpKind::Logical(LogOp::Events(Box::new(attrs))), &[], &[])?;
		for region in &regions {
			bind_schema_region(&mut cx, op, region)?;
		}
		cx.yield_logical(&[])?;
	}
	Ok(ir)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::*;
	use crate::catalog::EventKind;
	use crate::catalog::auth::AuthLimit;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::analysis::effects;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::walk::ops_in;
	use crate::exec::ir::iface::catalog::SemanticCatalog;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::ids::OpId;
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::ir::logical::ops::EventsAttrs;

	/// A catalog that answers the events of every table with one answer.
	struct Events(CatalogAnswer<Arc<[EventDefinition]>>);

	impl SemanticCatalog for Events {
		fn events(&self, _table: &TableName) -> CatalogAnswer<Arc<[EventDefinition]>> {
			self.0.clone()
		}
	}

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

	fn unit() -> UnitKind {
		UnitKind::Events {
			table: TableName::new("t"),
			action: Action::Update,
		}
	}

	/// Binds the events unit of `table` over `answer`, and answers the container.
	fn bound(answer: CatalogAnswer<Arc<[EventDefinition]>>) -> Result<Ir, IrError> {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let catalog = Events(answer);
		let effects = EffectCx {
			registry: &registry,
			catalog: &catalog,
			frames: &frames,
		};
		let input = RequestInput {
			statements: &[],
			scope: IrScope::Request {
				fragment: None,
			},
			unit: unit(),
			serves_late: false,
			late_bound: false,
		};
		bind_catalog_unit(&input, &effects, Some(&crate::exec::surface::FULL))
	}

	fn found(events: Vec<EventDefinition>) -> CatalogAnswer<Arc<[EventDefinition]>> {
		CatalogAnswer::Found(Arc::from(events))
	}

	/// The events op of the unit, which the top block holds alone before its yield.
	fn events_op(ir: &Ir) -> (OpId, EventsAttrs) {
		let ops: Vec<OpId> = ops_in(ir, ir.top()).collect();
		let [events, _yield] = ops[..] else {
			panic!("the top block holds the events op and its yield, found {} ops", ops.len());
		};
		match &ir.op(events).expect("the op exists").kind {
			OpKind::Logical(LogOp::Events(attrs)) => (events, attrs.as_ref().clone()),
			other @ (OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_)) => {
				panic!("the first op is the events op, found {other:?}")
			}
		}
	}

	fn region_kinds(ir: &Ir, op: OpId) -> Vec<RegionKind> {
		ir.op(op)
			.expect("the op exists")
			.regions
			.iter()
			.map(|region| ir.region(*region).expect("the region exists").kind)
			.collect()
	}

	/// Each definition hosts its `WHEN` and then each of its `THEN` clauses, in the order the
	/// catalog holds the definitions; an `ASYNC` event states none of its `THEN` clauses.
	#[test]
	fn each_event_hosts_when_then_its_then_clauses_in_catalog_order() {
		let ir = bound(found(vec![
			event("first", "$after.a > 1", &["1", "2"], EventKind::Sync),
			event(
				"second",
				"true",
				&["3"],
				EventKind::Async {
					retry: 0,
					max_depth: 0,
				},
			),
			event("third", "false", &[], EventKind::Sync),
		]))
		.expect("the unit binds");
		let (op, attrs) = events_op(&ir);
		assert_eq!(attrs.action, Action::Update);
		assert!(!attrs.unlowerable);
		assert_eq!(attrs.defs.definitions().len(), 3);
		let counts: Vec<usize> = attrs.events.iter().map(|shape| shape.then.len()).collect();
		assert_eq!(counts, [2, 0, 0], "an ASYNC event states no THEN clause");
		assert_eq!(attrs.fault(), None);
		// first: WHEN, THEN, THEN; second: WHEN; third: WHEN.
		assert_eq!(region_kinds(&ir, op), vec![RegionKind::EventFrame; 5]);
		assert_eq!(attrs.region_kinds(), vec![RegionKind::EventFrame; 5]);
	}

	/// A table with no event, one the catalog does not define, and one the memo has yet to load
	/// all bind the unit with no clause: the last is bound again once the memo holds the table.
	#[test]
	fn a_table_without_events_binds_a_unit_without_clauses() {
		for answer in [found(Vec::new()), CatalogAnswer::Absent, CatalogAnswer::NotLoaded] {
			let ir = bound(answer.clone()).expect("the unit binds");
			let (op, attrs) = events_op(&ir);
			assert!(attrs.events.is_empty() && !attrs.unlowerable, "{answer:?}");
			assert!(region_kinds(&ir, op).is_empty(), "{answer:?}");
		}
	}

	/// An answer that predates a catalog change of the compile cannot describe the events a record
	/// meets, so the unit is planned again when it is reached.
	#[test]
	fn an_answer_before_a_catalog_change_is_late_bound() {
		assert!(matches!(bound(CatalogAnswer::Unknown), Err(IrError::LateBind { .. })));
	}

	/// A clause that is a statement is hosted like any other. The shape states whether the clause
	/// is lowered for the principal that runs it.
	#[test]
	fn a_clause_that_is_a_statement_is_hosted_and_stated_as_caller_bound() {
		let ir = bound(found(vec![event(
			"one",
			"true",
			&["(SELECT * FROM audit)", "1"],
			EventKind::Sync,
		)]))
		.expect("the unit binds");
		let (op, attrs) = events_op(&ir);
		assert!(!attrs.unlowerable);
		assert_eq!(region_kinds(&ir, op), vec![RegionKind::EventFrame; 3]);
		let shape = &attrs.events[0];
		assert!(!shape.when.caller_bound);
		assert!(
			shape.then[0].caller_bound,
			"a statement is lowered for the principal that runs it"
		);
		assert!(!shape.then[1].caller_bound);
	}

	/// What a `THEN` calls, reads and writes is part of the unit: the effect set of the events op
	/// holds the effects of a call in a clause. The events op is a stage of the write and declares
	/// the write's set itself, so a unit whose clauses are quiet holds that set and no more.
	#[test]
	fn a_call_in_a_then_clause_is_in_the_effects_of_the_unit() {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let catalog = Events(CatalogAnswer::Absent);
		let cx = EffectCx {
			registry: &registry,
			catalog: &catalog,
			frames: &frames,
		};
		let effects_of = |events: Vec<EventDefinition>| {
			let ir = bound(found(events)).expect("the unit binds");
			let (op, _) = events_op(&ir);
			let mut facts = FactCache::new();
			effects::summary_with(&ir, &mut facts, &cx, op).set
		};
		let calling = effects_of(vec![event("one", "true", &["fn::audit()"], EventKind::Sync)]);
		assert!(calling.writes_data(), "a call of a user-defined function may write");
		let quiet = effects_of(vec![event("one", "$after.a > 1", &["1"], EventKind::Sync)]);
		assert!(calling.contains(quiet), "the call adds to what the events op declares");
	}

	/// The clauses of an event run over the image the event is about: the region binds `$this`,
	/// `$self` and `$value` to the row, and `$before`, `$after`, `$event` and `$input` follow.
	#[test]
	fn a_clause_region_takes_the_timestamp_the_row_and_the_names_of_its_frame() {
		let ir =
			bound(found(vec![event("one", "true", &[], EventKind::Sync)])).expect("the unit binds");
		let (op, _) = events_op(&ir);
		let region = ir.op(op).expect("the op exists").regions[0];
		let block = ir.region(region).expect("the region exists").blocks[0];
		let arguments = ir.block(block).expect("the block exists").args.len();
		assert_eq!(arguments, 1 + 1 + 4, "the timestamp, the row, $before, $after, $event, $input");
	}
}
