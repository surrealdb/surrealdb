//! The facts of the definitions of the user-defined functions a container calls.
//!
//! A call of a user-defined function lowers to an instruction that holds the hash of the definition
//! it was planned from, the kind of its `PERMISSIONS` clause (with how often a predicate's verdict
//! stands within a statement) and what its body spends of the computation-depth budget. Lowering
//! reads no catalog, so [`read_callees`] reads each definition here, in the physical phase, and
//! states it on the call ([`CallAttrs::callee`](super::super::expr::ops::CallAttrs::callee)).
//!
//! A call in a stored-clause region is code a statement stores and never runs, and is left as it
//! is. A call in a statement that may change the catalog as it runs is planned per call
//! ([`SemanticCatalog::calls_current`]), whether or not the memo can answer its definition. So is
//! a call that reads at a `VERSION` stamp of its statement (an op under a
//! [`Snapshot::Version`]): it runs the definition the stamp holds, and its body's calls the
//! definitions the stamp holds, which the run reads when it evaluates the stamp. A definition the
//! memo answers `Unknown` for in any other statement is one the plan cannot know (a schema
//! statement before the call's statement): the call states it is late-bound, and its statement is
//! planned again when it is reached.
//! The fill of the memo loads every function a container calls (`vm::ir_catalog::fill`), so a
//! definition answered `NotLoaded` here is an invariant break.

use super::super::expr::ops::{CalleeFacts, CalleePermission, DefinedCallee, ExprOp};
use crate::catalog::FunctionDefinition;
use crate::exec::function::FunctionRegistry;
use crate::exec::guard_verdicts::GuardScope;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::ancestors;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::{CatalogAnswer, SemanticCatalog};
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::depth::ExprDepth;
use crate::expr::Function;
use crate::expr::logical::LogicalPermissionRef;
use crate::iam::AuthLimit;
use crate::kvs::cache::ds::FunctionSnapshot;

/// States on every `expr.call` of a user-defined function in `ir` what `catalog` holds of its
/// definition, and answers how many calls it stated facts on.
///
/// `ir` is in the physical phase. A call that already states facts is read again: the answer is a
/// function of the name, the catalog version the container is bound to and `registry`, which
/// declares the effects of what a `PERMISSIONS` predicate calls. Fails with
/// [`IrError::Invariant`] for a definition the memo has not loaded.
pub(crate) fn read_callees(
	ir: &mut Ir,
	facts: &mut FactCache,
	catalog: &dyn SemanticCatalog,
	registry: &FunctionRegistry,
) -> Result<u32, IrError> {
	let mut sites = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(data) = ir.op(op)
			&& let OpKind::Expr(ExprOp::Call(call)) = &data.kind
			&& let Function::Custom(name) = &call.function
			&& !in_stored_clause(ir, op)
		{
			let versioned = matches!(ir.snapshot(data.snapshot), Some(Snapshot::Version(_)));
			sites.push((op, name.clone(), versioned));
		}
		Walk::Continue
	});
	let mut stated = 0;
	let mut rw = Rewriter::new(ir, facts);
	for (op, name, versioned) in sites {
		let per_call = versioned || catalog.calls_current();
		let callee = match catalog.function(&name) {
			CatalogAnswer::Found(definition) if per_call => {
				Some(CalleeFacts::PerCall(defined(&definition, registry)))
			}
			CatalogAnswer::Found(definition) => {
				Some(CalleeFacts::Defined(defined(&definition, registry)))
			}
			// The stamp may hold a definition the current catalog does not.
			CatalogAnswer::Absent if versioned => Some(CalleeFacts::PerCall(unknown_definition())),
			CatalogAnswer::Absent => Some(CalleeFacts::Absent),
			CatalogAnswer::Unknown if per_call => Some(CalleeFacts::PerCall(unknown_definition())),
			CatalogAnswer::Unknown => Some(CalleeFacts::LateBound),
			CatalogAnswer::NotLoaded => {
				return Err(IrError::Invariant {
					what: "phys::callee: a called function the memo has not loaded",
				});
			}
		};
		stated += 1;
		rw.update_kind(op, |kind| {
			if let OpKind::Expr(ExprOp::Call(call)) = kind {
				call.callee = callee;
			}
		})?;
	}
	Ok(stated)
}

/// What a call holds of `definition`.
///
/// The hash is the one the call compares with the definition it reads when it runs. A predicate
/// states the scope of its verdict, read from the predicate and `registry`. A `PERMISSIONS` clause
/// with no logical form, and a predicate whose `AUTH LIMIT` does not parse, are stated as a
/// predicate with no scope: the call holds no guard for it, and the run raises reading the clause
/// or the limit as it enters, before it would judge one.
pub(crate) fn defined(
	definition: &FunctionDefinition,
	registry: &FunctionRegistry,
) -> DefinedCallee {
	let permission = match definition.permissions_body.logical_ref() {
		Ok(LogicalPermissionRef::Full) => CalleePermission::Full,
		Ok(LogicalPermissionRef::None) => CalleePermission::None,
		Ok(LogicalPermissionRef::Specific(predicate)) => CalleePermission::Predicate {
			scope: AuthLimit::try_from(&definition.auth_limit)
				.is_ok()
				.then(|| GuardScope::of(predicate, registry)),
		},
		Err(_) => CalleePermission::Predicate {
			scope: None,
		},
	};
	let depth = ExprDepth::of_body(&definition.block);
	let clamp = |n: u32| u8::try_from(n).unwrap_or(u8::MAX);
	DefinedCallee {
		hash: FunctionSnapshot::hash_of(definition),
		permission,
		writes: definition.block.function_facts().direct_writes,
		body_depth: clamp(depth.charged()),
		deepest: clamp(depth.deepest),
	}
}

/// What a call planned per call holds of a definition the plan cannot read: one whose body may
/// write and that spends no depth the plan can state. The call reads the definition the
/// transaction holds when it is made, as every call planned per call does.
fn unknown_definition() -> DefinedCallee {
	DefinedCallee {
		hash: 0,
		permission: CalleePermission::Predicate {
			scope: None,
		},
		writes: true,
		body_depth: 0,
		deepest: 0,
	}
}

/// Whether `op` stands in a region that holds code the statement stores and never runs.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::analysis::cx::builtin_registry;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::location::{LocOrigin, Location};
	use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec};
	use crate::exec::ir::expr::ops::CallAttrs;
	use crate::exec::ir::iface::ids::{BlockId, FrameId, SnapshotId};
	use crate::exec::ir::iface::region_kind::StoredRole;
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::LogOp;
	use crate::exec::ir::unit::UnitKind;

	/// A catalog that answers every function as one the plan cannot know.
	struct Unknown;

	impl SemanticCatalog for Unknown {}

	fn create(rw: &mut Rewriter<'_>, block: BlockId, kind: OpKind) -> OpId {
		rw.create(
			InsertPoint::End(block),
			OpSpec {
				kind,
				operands: Default::default(),
				results: std::iter::once(TypeKnowledge::Unknown).collect(),
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				loc: Location {
					stmt: 0,
					span: None,
					origin: LocOrigin::Synthesized,
				},
			},
		)
		.expect("the op is created")
	}

	/// A container whose top region has kind `top` and whose top block holds an op that owns an
	/// event frame, where one call of `fn::f` stands; answers the container and the call.
	fn calling_in_an_event(top: RegionKind) -> (Ir, OpId) {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			top,
		);
		let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut facts = FactCache::new();
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let owner = create(&mut rw, block, OpKind::Logical(LogOp::Block));
		let (_, inner) = rw
			.create_region(owner, RegionKind::EventFrame, &[])
			.expect("the event frame is created");
		let call = create(
			&mut rw,
			inner,
			OpKind::Expr(ExprOp::Call(Box::new(CallAttrs::of(Function::Custom("f".to_owned()))))),
		);
		(ir, call)
	}

	fn callee_of(ir: &Ir, call: OpId) -> Option<CalleeFacts> {
		match &ir.op(call).expect("the call exists").kind {
			OpKind::Expr(ExprOp::Call(call)) => call.callee.clone(),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => panic!("a call"),
		}
	}

	/// A call in an event's code inside a unit that runs stored code states its callee, as a call
	/// in code the request runs does; under a stored clause, which never runs, it states none.
	#[test]
	fn a_call_in_an_event_of_a_stored_code_unit_states_its_callee() {
		let units = [
			UnitKind::Cascade,
			UnitKind::StageBundle {
				table: surrealdb_strand::TableName::new("t"),
				action: crate::doc::Action::Update,
			},
			UnitKind::Subscription {
				table: surrealdb_strand::TableName::new("t"),
			},
		];
		for unit in units {
			let (mut ir, call) = calling_in_an_event(unit.top_region());
			let mut facts = FactCache::new();
			let stated = read_callees(&mut ir, &mut facts, &Unknown, builtin_registry())
				.expect("the callees are read");
			assert_eq!(stated, 1, "{unit:?}");
			assert_eq!(callee_of(&ir, call), Some(CalleeFacts::LateBound), "{unit:?}");
		}
		let (mut ir, call) = calling_in_an_event(RegionKind::StoredClause(StoredRole::Body));
		let mut facts = FactCache::new();
		let stated = read_callees(&mut ir, &mut facts, &Unknown, builtin_registry())
			.expect("the callees are read");
		assert_eq!(stated, 0);
		assert_eq!(callee_of(&ir, call), None);
	}
}
