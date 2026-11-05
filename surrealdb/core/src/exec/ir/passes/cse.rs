//! Common-subexpression elimination over the logical dialect: ops with equal value keys within
//! one statement are merged into one.
//!
//! Ops are numbered statement by statement, in program order, through `ValueTable` and
//! `identity::key_with`. A key is built when its op is reached, after the merges of the ops
//! before it, so an op whose operands were merged is keyed by the merged values. An op is replaced
//! by the earlier op of its key only when that op's value dominates it, and the replacement
//! records `MergedInto`. Equal keys are a candidate only: the two ops are merged only when their
//! attributes are equal ([`OpKind::attrs_eq`]), so a digest collision never merges two values.
//!
//! Only expression ops with one result and no region, other than a literal over
//! [`KEYED_LITERAL_MAX_BYTES`], are candidates, and only when their transitive effects are reads of
//! the snapshot, the catalog, the row, the session or the clock.
//! Ops that are not deterministic, ops whose key is opaque, and reads separated by a write of the
//! statement are never merged. Two guards cover what the key does not state:
//!
//! - an op that owns a region is never merged, because the key does not hold its regions;
//! - in a statement that holds a catalog change, a catalog read is never merged, and in a statement
//!   that writes the session, a session read is never merged, because the key holds no catalog or
//!   session epoch. The statement is judged as a whole, since a loop runs a read written before a
//!   change after that change.
//! - an op is merged only into an op of the same error scope, so the merged op's provenance stays
//!   with the scope it was bound in; the verifier checks that binding.
//!
//! Ops outside every statement are left alone.
#![deny(clippy::wildcard_enum_match_arm)]

use super::driver::{LoCx, LoPass, PassConfig, PassReport, PassRun};
use crate::exec::ir::analysis::effects;
use crate::exec::ir::analysis::identity::key_with;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::value_table::ValueTable;
use crate::exec::ir::core::walk::{ancestors, enclosing_statement};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::bounds::{KEYED_LITERAL_MAX_BYTES, value_exceeds};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::logical::EffectSet;

/// The effects an op may have and still be merged or dropped unread: reads of the snapshot, the
/// catalog, the row, the session and the clock.
pub(super) const READS: EffectSet = EffectSet::READS_DATA
	.union(EffectSet::READS_CATALOG)
	.union(EffectSet::READS_ROW)
	.union(EffectSet::READS_SESSION)
	.union(EffectSet::CLOCK);

/// Merges the ops of a container that compute one value.
///
/// The merge is a value numbering of each statement, so one sweep is enough.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Cse;

impl LoPass for Cse {
	fn name(&self) -> &'static str {
		"cse"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.cse
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_cse(cx))
	}
}

fn run_cse(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let merged = merge_equal_values(&mut *cx.ir, &mut *cx.facts, cx.effects, Scope::Logical)?;
	Ok(PassReport {
		rewrites: merged,
		..PassReport::default()
	})
}

/// Which expression ops a merge may touch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Scope {
	/// Every expression op of the container.
	Logical,
	/// The expression ops that stand inside a region of a physical op.
	Physical,
}

/// Whether a region of a physical op holds `op`, at any depth.
pub(super) fn in_physical_region(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(_, owner)| {
		owner.and_then(|held| ir.op(held)).is_some_and(|data| matches!(data.kind, OpKind::Phys(_)))
	})
}

/// Whether `op` stands in a stored clause, whose code is bound and analysed but never rewritten.
pub(super) fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Whether `kind` is a literal over [`KEYED_LITERAL_MAX_BYTES`]. Building its key encodes the
/// whole payload, so it is never a candidate: two equal literals of that size are rare, and a
/// literal that stands once has nothing to merge.
fn is_unkeyed_literal(kind: &OpKind) -> bool {
	matches!(kind, OpKind::Expr(ExprOp::Literal(value)) if value_exceeds(value, KEYED_LITERAL_MAX_BYTES))
}

/// Whether the expression op `tag` is a value of its operands and effects alone, so that two ops
/// of equal key may be one.
///
/// A read the evaluation context supplies (a free, session or catalog parameter, the statement
/// clock) is a value of the statement, not of its operands. Each position that reads it is
/// its own evaluation, so it is never merged. A dispatch has exactly one reader, the `method`
/// or `invoke` that follows it, so it is never merged either.
fn mergeable(tag: ExprTag) -> bool {
	match tag {
		ExprTag::Literal
		| ExprTag::Array
		| ExprTag::Set
		| ExprTag::Object
		| ExprTag::RecordId
		| ExprTag::Table
		| ExprTag::Mock
		| ExprTag::Constant
		| ExprTag::Field
		| ExprTag::ComputedRead
		| ExprTag::Doc
		| ExprTag::Access
		| ExprTag::Deref
		| ExprTag::Index
		| ExprTag::First
		| ExprTag::Last
		| ExprTag::Each
		| ExprTag::Flatten
		| ExprTag::Method
		| ExprTag::Destructure
		| ExprTag::SetField
		| ExprTag::Prefix
		| ExprTag::Convert
		| ExprTag::Postfix
		| ExprTag::Binary
		| ExprTag::Call
		| ExprTag::Truthy => true,
		ExprTag::FreeParam
		| ExprTag::SessionParam
		| ExprTag::CatalogParam
		| ExprTag::Now
		| ExprTag::Optional
		| ExprTag::RepeatRecurse
		| ExprTag::Dispatch
		| ExprTag::Invoke
		| ExprTag::ShortCircuit
		| ExprTag::ClosureCreate
		| ExprTag::If
		| ExprTag::Throw
		| ExprTag::Subquery
		| ExprTag::Yield
		| ExprTag::Agg
		| ExprTag::Raise
		| ExprTag::DepthCharge
		| ExprTag::TupleRow => false,
	}
}

/// What a statement holds that bars a merge of the reads it could change.
#[derive(Clone, Copy, Debug)]
struct Guards {
	/// A catalog change stands in the statement, so no catalog read is merged.
	catalog: bool,
	/// A session write stands in the statement, so no session read is merged.
	session: bool,
}

/// Whether two of `ops` that have the shape of a candidate ([`is_candidate`]: an expression op of
/// a mergeable tag with one result and no region, and not an unkeyed literal) share their tag,
/// operands, frame and snapshot, which every pair of equal keys ([`key_with`]) does.
fn has_shape_twins(ir: &Ir, ops: &[OpId]) -> bool {
	use std::hash::{Hash, Hasher};
	if ops.len() < 2 {
		return false;
	}
	let shaped = |op: &OpId| {
		let data = ir.op(*op)?;
		let OpKind::Expr(kind) = &data.kind else {
			return None;
		};
		(data.results.len() == 1
			&& data.regions.is_empty()
			&& mergeable(kind.tag())
			&& !is_unkeyed_literal(&data.kind))
		.then_some(data)
	};
	let mut hashed: Vec<(u64, OpId)> = ops
		.iter()
		.filter_map(|op| {
			let data = shaped(op)?;
			let mut hasher = std::collections::hash_map::DefaultHasher::new();
			data.kind.tag().hash(&mut hasher);
			data.operands.hash(&mut hasher);
			data.frame.hash(&mut hasher);
			data.snapshot.hash(&mut hasher);
			Some((hasher.finish(), *op))
		})
		.collect();
	if hashed.len() < 2 {
		return false;
	}
	hashed.sort_unstable_by_key(|(hash, _)| *hash);
	let same = |a: OpId, b: OpId| match (ir.op(a), ir.op(b)) {
		(Some(a), Some(b)) => {
			a.kind.tag() == b.kind.tag()
				&& a.operands == b.operands
				&& a.frame == b.frame
				&& a.snapshot == b.snapshot
		}
		_ => false,
	};
	hashed.chunk_by(|a, b| a.0 == b.0).any(|run| {
		run.len() >= 2
			&& run
				.iter()
				.enumerate()
				.any(|(at, (_, a))| run[at + 1..].iter().any(|(_, b)| same(*a, *b)))
	})
}

/// What the statement `statement` holds: its own transitive effects, the local effects of every op
/// in it (for a physical op, the effects of the logical ops it stands for) and the catalog-change
/// attribute of a physical statement. A statement missing from the IR bars both kinds of read.
fn guards_of(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, statement: OpId) -> Guards {
	let Some(data) = ir.op(statement) else {
		return Guards {
			catalog: true,
			session: true,
		};
	};
	let mut held = effects::summary_with(ir, facts, cx, statement).set;
	for region in &data.regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
			held = held.union(effects::local(ir, facts, cx, op).set);
			Walk::Continue
		});
	}
	let physical_change = matches!(
		data.kind,
		OpKind::Phys(PhysOp::Statement {
			changes_catalog: true,
			..
		})
	);
	Guards {
		catalog: physical_change || held.contains(EffectSet::CHANGES_CATALOG),
		session: held.contains(EffectSet::WRITES_SESSION),
	}
}

/// Whether `op` may be merged into an earlier op of its key.
fn is_candidate(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	op: OpId,
	scope: Scope,
	guards: Guards,
) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let OpKind::Expr(kind) = &data.kind else {
		return false;
	};
	if data.results.len() != 1
		|| !data.regions.is_empty()
		|| !mergeable(kind.tag())
		|| is_unkeyed_literal(&data.kind)
		|| in_stored_clause(ir, op)
	{
		return false;
	}
	if scope == Scope::Physical && !in_physical_region(ir, op) {
		return false;
	}
	let answer = effects::summary_with(ir, facts, cx, op);
	if !READS.contains(answer.set) {
		return false;
	}
	let barred_catalog = guards.catalog && answer.set.contains(EffectSet::READS_CATALOG);
	let barred_session = guards.session && answer.set.intersects(EffectSet::READS_SESSION);
	!(barred_catalog || barred_session)
}

/// The ops of `ir` in program order, grouped by the statement that encloses them, the groups in
/// the order their first op is met. An op outside every statement is not listed.
fn by_statement(ir: &Ir) -> Vec<(OpId, Vec<OpId>)> {
	let mut groups: Vec<(OpId, Vec<OpId>)> = Vec::new();
	let mut index: FxHashMap<OpId, usize> = FxHashMap::default();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(statement) = enclosing_statement(ir, op) {
			let at = *index.entry(statement).or_insert_with(|| {
				groups.push((statement, Vec::new()));
				groups.len() - 1
			});
			groups[at].1.push(op);
		}
		Walk::Continue
	});
	groups
}

/// The kind of the op whose result `value` is, or `None` for a block argument or an id that names
/// no value.
fn defining_kind(ir: &Ir, value: ValueId) -> Option<&OpKind> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	ir.op(op).map(|data| &data.kind)
}

/// Whether the op defining `earlier` stands in the error scope `op_scope`, the scope being the
/// owner of the innermost `ErrorScope` region that encloses an op. The caller computes `op_scope`
/// for the op being merged as `ir.op(op).map(|data| ir.error_scope_owner(data.parent))`: the outer
/// `None` marks an op id that names no op, the inner `None` an op outside every error scope.
/// A merge hands the removed op's provenance to the definer, and the verifier requires every id of
/// a provenance to stand in the scope it was bound in, so an op is merged only into a definer of
/// its own scope. A block argument has no definer to take the provenance, so it is always in scope.
fn same_error_scope(ir: &Ir, op_scope: Option<Option<OpId>>, earlier: ValueId) -> bool {
	let definer = match ir.value(earlier).map(|value| value.def) {
		Some(ValueDef::OpResult {
			op,
			..
		}) => op,
		_ => return true,
	};
	let definer_scope = ir.op(definer).map(|data| ir.error_scope_owner(data.parent));
	matches!((op_scope, definer_scope), (Some(own), Some(other)) if own == other)
}

/// Merges, in each statement of `ir`, every candidate op into the earlier op of its key whose value
/// dominates it and whose attributes are equal, and answers how many ops went.
///
/// A key is built when its op is reached, after the merges before it. The keys of a region are
/// forgotten once the walk leaves it, so the ops of a sibling region are numbered among
/// themselves; an earlier value that still does not dominate the op leaves the op where it is.
pub(super) fn merge_equal_values(
	ir: &mut Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	scope: Scope,
) -> Result<u32, IrError> {
	let mut merged = 0;
	for (statement, ops) in by_statement(ir) {
		// Two ops merge only under equal keys, and a key names the op's tag, operands, frame and
		// snapshot: a statement where no two ops that could be candidates share all four has
		// nothing to merge.
		if !has_shape_twins(ir, &ops) {
			continue;
		}
		let guards = guards_of(ir, facts, cx, statement);
		let mut table = ValueTable::new();
		table.enter_statement(Some(statement));
		for op in ops {
			if !is_candidate(ir, facts, cx, op, scope, guards) {
				continue;
			}
			let Some(result) = ir.op(op).and_then(|data| data.results.first().copied()) else {
				continue;
			};
			let mut chain: Vec<RegionId> = ancestors(ir, op)
				.take_while(|(_, owner)| *owner != Some(statement))
				.map(|(region, _)| region)
				.collect();
			chain.reverse();
			table.enter_regions(&chain);
			let key = key_with(ir, facts, cx, op);
			let Some(kind) = ir.op(op).map(|data| &data.kind) else {
				continue;
			};
			let op_scope = ir.op(op).map(|data| ir.error_scope_owner(data.parent));
			let same = |earlier: ValueId| {
				defining_kind(ir, earlier).is_some_and(|earlier| earlier.attrs_eq(kind))
					&& same_error_scope(ir, op_scope, earlier)
			};
			let Some(earlier) = table.lookup_or_insert(key, result, same) else {
				continue;
			};
			if earlier == result || !ir.dominates(earlier, op) {
				continue;
			}
			let mut rw = Rewriter::new(&mut *ir, &mut *facts);
			match rw.replace_op(op, &[earlier]) {
				Ok(()) => merged += 1,
				Err(IrError::HasUses(_) | IrError::Refused(_)) => {}
				Err(error) => return Err(error),
			}
		}
	}
	Ok(merged)
}
