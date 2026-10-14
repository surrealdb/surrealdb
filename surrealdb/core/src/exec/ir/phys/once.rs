//! The conjuncts of a filter judged once per run of the statement.
//!
//! A conjunct of a `phys.filter`'s predicate is judged once, in a second `Clause(Where)` region of
//! the filter, where all of the following hold:
//!
//! - It reads nothing outside the ops that compute it but literals: no row, no block argument of a
//!   region around it and no other value an op outside it defines. It is more than a literal
//!   written as one: a literal a fold made of an expression is judged as that expression.
//! - Every op that computes it is an expression op of a form that reads no record and no row: no
//!   field read, document, computed read, nested statement, closure, invocation, aggregate,
//!   recursion or raise, a call only of a builtin that is not an index function, and no literal
//!   standing for a `KNN` operator no search answers, which reads the row as that operator does.
//! - Its effects are deterministic and read neither the row nor stored data.
//! - Every conjunct written before it that stays per row is total ([`conjunct::judge`]).
//! - Its tree can be rehomed beside the filter ([`can_rehome`]): every value it reads that it does
//!   not compute is visible there. A value it shares with a conjunct judged per row stays where it
//!   is, and is visible beside the filter only when it is computed outside the predicate.
//!
//! Every condition is judged before the filter is changed, so the move never fails part way.
//!
//! Such a conjunct answers the same at every row, so the filter judges it at the first row that
//! reaches the filter, before the conjuncts it judges per row, and holds the verdict. A conjunct
//! before it either is judged once too or can neither raise nor refuse anything a caller observes,
//! so the first error the predicate raises is the one the predicate judged per row raises. The
//! once conjuncts keep their written order, as do the conjuncts left per row. A predicate left
//! with nothing to judge per row is the literal `true`.
//!
//! A filter that already holds a predicate judged once, and a filter in stored code, are left as
//! they are.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;

use smallvec::{SmallVec, smallvec};
use surrealdb_types::ToSql;

use super::conjunct::{self, Site, def_op};
use super::ops::PhysOp;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::PhysCx;
use crate::exec::ir::iface::effects::Determinism;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::passes::rehome::can_rehome;
use crate::expr::Function;
use crate::expr::logical::EffectSet;

/// The most ops one conjunct's tree holds, regions included, for it to be judged once.
const TREE_BUDGET: usize = 512;

/// Moves the conjuncts of every filter of `ir` that are judged once into the filter's second
/// region, and answers the number of filters changed.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<u32, IrError> {
	let mut filters = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Filter))) {
			filters.push(op);
		}
		Walk::Continue
	});
	let mut changed = 0;
	for filter in filters {
		if in_stored_clause(ir, filter) {
			continue;
		}
		let Some(site) = conjunct::site(ir, filter) else {
			continue;
		};
		if site.once {
			continue;
		}
		let chain = conjunct::leaves(ir, &site);
		let mut once = Vec::new();
		let mut kept = Vec::new();
		// Whether every conjunct left per row so far is total.
		let mut total = true;
		for value in &chain {
			if total && row_independent(ir, facts, cx, *value) && rehomes(ir, filter, *value) {
				once.push(*value);
			} else {
				total &= conjunct::judge(ir, *value).total;
				kept.push(*value);
			}
		}
		if once.is_empty() || !movable(ir, &chain) || !conjunct::can_keep(ir, &site, &kept) {
			continue;
		}
		split(ir, facts, &site, &once, &kept)?;
		changed += 1;
	}
	Ok(changed)
}

/// Whether the tree that computes the conjunct `value` can be moved into a new region of the
/// filter `filter`: every value it reads and does not compute is visible beside the filter, which
/// is what the new region sees.
fn rehomes(ir: &Ir, filter: OpId, value: ValueId) -> bool {
	def_op(ir, value).is_some_and(|root| can_rehome(ir, root, InsertPoint::Before(filter), &[]))
}

/// Whether `op` stands in a stored-clause region, which is never rewritten.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Whether every conjunct of `chain` is an op's result read by its chain alone, so the chain can
/// be rebuilt.
fn movable(ir: &Ir, chain: &[ValueId]) -> bool {
	chain.iter().all(|value| ir.uses(*value).len() == 1 && def_op(ir, *value).is_some())
}

/// Whether the conjunct `value` answers the same at every row of a run of the statement, by the
/// rules of the module.
fn row_independent(ir: &Ir, facts: &mut FactCache, cx: &PhysCx<'_>, value: ValueId) -> bool {
	let Some(root) = def_op(ir, value) else {
		return false;
	};
	let Some(tree) = tree_of(ir, root) else {
		return false;
	};
	if tree.len() == 1 && written_literal(ir, root) {
		return false;
	}
	let inside: HashSet<OpId> = tree.iter().copied().collect();
	for op in &tree {
		let Some(data) = ir.op(*op) else {
			return false;
		};
		if !admitted_form(cx, &data.kind) || ir.knn_unanswered(*op) {
			return false;
		}
		// Every value the op reads is computed by the tree or is a literal, or is a block argument
		// of a region an op of the tree owns.
		let reads_inside =
			data.operands.iter().all(|operand| match ir.value(*operand).map(|v| v.def) {
				Some(ValueDef::OpResult {
					op,
					..
				}) => {
					inside.contains(&op)
						|| matches!(
							ir.op(op).map(|held| &held.kind),
							Some(OpKind::Expr(ExprOp::Literal(_)))
						)
				}
				Some(ValueDef::BlockArg {
					block,
					..
				}) => ir
					.block(block)
					.and_then(|held| ir.parent_op(held.parent))
					.is_some_and(|owner| inside.contains(&owner)),
				None => false,
			});
		if !reads_inside {
			return false;
		}
	}
	tree.iter().all(|op| {
		let answer = effects::of_with(ir, facts, &cx.effects, *op);
		answer.determinism == Determinism::Deterministic
			&& answer.set.is_deterministic()
			&& !answer.set.intersects(EffectSet::READS_ROW.union(EffectSet::READS_DATA))
	})
}

/// Whether `op` is a literal written as one: a literal that a fold made of an expression keeps the
/// expression's text, and is judged as the expression.
fn written_literal(ir: &Ir, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let OpKind::Expr(ExprOp::Literal(value)) = &data.kind else {
		return false;
	};
	data.loc.text().is_none_or(|text| text == value.to_sql())
}

/// The ops that compute the value `root` defines: `root`, the ops of its block that define its
/// operands, transitively, and every op inside their regions. `None` past [`TREE_BUDGET`].
fn tree_of(ir: &Ir, root: OpId) -> Option<Vec<OpId>> {
	let block = ir.op(root)?.parent;
	let mut seen: HashSet<OpId> = HashSet::new();
	let mut out = Vec::new();
	let mut pending = vec![root];
	while let Some(op) = pending.pop() {
		if !seen.insert(op) {
			continue;
		}
		if out.len() >= TREE_BUDGET {
			return None;
		}
		let data = ir.op(op)?;
		out.push(op);
		for operand in &data.operands {
			if let Some(def) = def_op(ir, *operand)
				&& ir.op(def).is_some_and(|held| held.parent == block)
			{
				pending.push(def);
			}
		}
		for region in &data.regions {
			pending.extend(ops_in(ir, *region));
		}
	}
	Some(out)
}

/// Whether an op of kind `kind` may compute a conjunct judged once.
fn admitted_form(cx: &PhysCx<'_>, kind: &OpKind) -> bool {
	let OpKind::Expr(op) = kind else {
		return false;
	};
	match op {
		ExprOp::Call(call) => match &call.function {
			Function::Normal(name) => !cx.effects.registry.is_index_function(name),
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => false,
		},
		ExprOp::Literal(_)
		| ExprOp::Array
		| ExprOp::Set
		| ExprOp::Object {
			..
		}
		| ExprOp::RecordId(_)
		| ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::Table {
			..
		}
		| ExprOp::Constant {
			..
		}
		| ExprOp::Access {
			..
		}
		| ExprOp::Index
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Method {
			..
		}
		| ExprOp::Destructure {
			..
		}
		| ExprOp::Optional
		| ExprOp::SetField {
			..
		}
		| ExprOp::Prefix(_)
		| ExprOp::Convert(_)
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Binary {
			..
		}
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::If
		| ExprOp::Yield
		| ExprOp::Truthy => true,
		ExprOp::Mock(_)
		| ExprOp::Field {
			..
		}
		| ExprOp::ComputedRead(_)
		| ExprOp::Doc
		| ExprOp::Deref {
			..
		}
		| ExprOp::Dispatch(_)
		| ExprOp::RepeatRecurse
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Invoke
		| ExprOp::ClosureCreate(_)
		| ExprOp::Throw
		| ExprOp::Subquery(_)
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::DepthCharge {
			..
		} => false,
	}
}

/// Moves the conjuncts `once` of the predicate of `site` into a new `Clause(Where)` region of its
/// filter, and leaves `kept` as the predicate judged per row, or the literal `true` when `kept` is
/// empty.
fn split(
	ir: &mut Ir,
	facts: &mut FactCache,
	site: &Site,
	once: &[ValueId],
	kept: &[ValueId],
) -> Result<(), IrError> {
	let filter = site.filter;
	let mut rw = Rewriter::new(ir, facts);
	let template = rw.ir().op(filter).ok_or(IrError::Stale("op"))?.clone();
	let (_, body) =
		rw.create_region(filter, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])?;
	let judged = rw.derived(&[filter], |rw| {
		crate::exec::ir::passes::rehome::rebuild_and_chain(rw, InsertPoint::End(body), once)
	})?;
	let close = OpSpec {
		kind: OpKind::Expr(ExprOp::Yield),
		operands: smallvec![judged],
		results: SmallVec::new(),
		frame: template.frame,
		snapshot: template.snapshot,
		loc: template.loc,
	};
	rw.derived(&[filter], |rw| rw.create(InsertPoint::End(body), close))?;
	conjunct::keep_per_row(&mut rw, site, kept)
}
