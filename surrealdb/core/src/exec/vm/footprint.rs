//! The effect summary of a unit: what running it may do, and the tables it
//! may read and write.
//!
//! A unit's summary is derived from what it was compiled from, never
//! declared: the union of the declared effects of every builtin its scalars
//! call ([`crate::exec::logical::effects_of_plan`]), the tables its sources
//! and writes name, the tables its program reads records of by id
//! ([`super::dispatch::reach_of`]), and the user-defined functions it calls,
//! which it binds late: a function's footprint is its own and the footprints
//! of the functions it names, composed by whoever reads them.
//!
//! What the derivation cannot name is the whole of its kind: a source or a
//! write target evaluated when the statement runs, a graph or reference
//! lookup, a closure invoked through a value, and any body the call site
//! cannot see ([`EffectSet::OPAQUE_BODY`]) read and write any table. A
//! summary is therefore never smaller than what the unit does; a consumer
//! deciding placement or descriptor reloads from it errs only toward the
//! slower choice.

use std::collections::BTreeSet;

use super::dispatch::Reach;
use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{EffectSet, LogicalExpr, LogicalOp, LogicalSource, SourceRef};

/// What a unit may do; see the module.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Footprint {
	/// The union of the effects its scalars declare.
	pub(crate) effects: EffectSet,
	/// The tables it may read.
	pub(crate) reads: Reach,
	/// The tables it may write.
	pub(crate) writes: Reach,
	/// The user-defined functions it calls, by name without `fn::`.
	pub(crate) calls: BTreeSet<String>,
}

impl Default for Footprint {
	fn default() -> Self {
		Self {
			effects: EffectSet::PURE,
			reads: Reach::none(),
			writes: Reach::none(),
			calls: BTreeSet::new(),
		}
	}
}

impl Footprint {
	/// The footprint of `op`, a bound statement or body, over `registry`'s
	/// builtins, with the tables `program_reach` reads records of by id.
	pub(crate) fn of(op: &LogicalOp, registry: &FunctionRegistry, program_reach: Reach) -> Self {
		let mut footprint = Footprint {
			effects: crate::exec::logical::effects_of_plan_late_bound(op, registry).set,
			..Footprint::default()
		};
		footprint.fold(op);
		footprint.reads =
			std::mem::replace(&mut footprint.reads, Reach::none()).union(program_reach);
		if footprint.effects.contains(EffectSet::OPAQUE_BODY) {
			footprint.reads = Reach::Any;
			footprint.writes = Reach::Any;
		}
		if !matches!(&footprint.writes, Reach::Tables(tables) if tables.is_empty()) {
			footprint.effects = footprint.effects.union(EffectSet::WRITES_DATA);
		}
		footprint
	}

	/// Whether the unit may write the table `table`.
	#[cfg(test)]
	pub(crate) fn may_write(&self, table: &crate::val::TableName) -> bool {
		self.writes.reaches(table)
	}

	fn read(&mut self, reach: Reach) {
		self.reads = std::mem::replace(&mut self.reads, Reach::none()).union(reach);
	}

	fn fold(&mut self, op: &LogicalOp) {
		match op {
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => self.read(match source {
				SourceRef::Table(table) => Reach::Tables(BTreeSet::from([table.clone()])),
				SourceRef::RecordId(rid) => Reach::Tables(BTreeSet::from([rid.table.clone()])),
				SourceRef::Mock(mock) => Reach::Tables(BTreeSet::from([mock.table().clone()])),
				SourceRef::Param(_) | SourceRef::Dynamic(_) => Reach::Any,
			}),
			LogicalOp::Write(write) => {
				let targets = super::lower::write_reach(write);
				// A write reads the records it writes.
				self.read(targets.clone());
				self.writes = std::mem::replace(&mut self.writes, Reach::none()).union(targets);
			}
			_ => {}
		}
		let walked: Result<(), std::convert::Infallible> = op.visit_scalars(&mut |scalar| {
			self.fold_scalar(scalar);
			Ok(())
		});
		let Ok(()) = walked;
		for child in op.children() {
			self.fold(child);
		}
	}

	fn fold_scalar(&mut self, expr: &LogicalExpr) {
		match expr {
			LogicalExpr::Call(call) => {
				if let crate::expr::Function::Custom(name) = &call.receiver {
					self.calls.insert(name.clone());
				}
			}
			LogicalExpr::Subquery(plan) => self.fold(plan),
			LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_) => self.read(Reach::Any),
			LogicalExpr::Invoke {
				..
			} => {
				self.read(Reach::Any);
				self.writes = Reach::Any;
			}
			_ => {}
		}
		expr.for_each_child(&mut |child| self.fold_scalar(child));
	}
}
