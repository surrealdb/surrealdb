//! The conjuncts of a filter judged once per run of the statement.
//!
//! A conjunct of a `phys.filter`'s predicate is judged once, in a second `Clause(Where)` region of
//! the filter, where all of the following hold:
//!
//! - It reads nothing outside the ops that compute it but literals: no row, no block argument of a
//!   region around it and no other value an op outside it defines. It is more than a literal
//!   written as one: a literal a fold made of an expression is judged as that expression, while the
//!   literal an algebraic rule decided stays as written.
//! - Every op that computes it is an expression op of a form that reads no record and no row: no
//!   field read, document, computed read, nested statement, closure, invocation, aggregate,
//!   recursion or raise, a call only of a builtin that is not an index function, and no literal
//!   standing for a `KNN` operator no search answers, which reads the row as that operator does.
//! - Its effects are deterministic and read neither the row nor stored data.
//! - Every conjunct written before it that stays per row is total ([`conjunct::judge`]), and once
//!   any earlier conjunct stays per row, the conjunct moved to `once` must itself be infallible so
//!   short-circuit failure order is preserved.
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

use smallvec::{SmallVec, smallvec};
use surrealdb_types::ToSql;

use super::conjunct::{self, Site, def_op};
use super::ops::PhysOp;
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::egraph::guards;
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
			if total
				&& row_independent(ir, facts, cx, *value, !kept.is_empty())
				&& rehomes(ir, filter, *value)
			{
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
/// rules of the module. When `must_be_infallible` holds, every op of its tree must also be
/// infallible so moving it ahead of an earlier per-row conjunct cannot raise an error on a row
/// that conjunct rejects.
fn row_independent(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	value: ValueId,
	must_be_infallible: bool,
) -> bool {
	let Some(root) = def_op(ir, value) else {
		return false;
	};
	let Some(tree) = tree_of(ir, root) else {
		return false;
	};
	if tree.len() == 1 && written_literal(ir, root) {
		return false;
	}
	let inside: FxHashSet<OpId> = tree.iter().copied().collect();
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
		let class = answer.fallibility.union(fallibility::floor(answer.set));
		answer.determinism == Determinism::Deterministic
			&& answer.set.is_deterministic()
			&& !answer.set.intersects(EffectSet::READS_ROW.union(EffectSet::READS_DATA))
			&& (!must_be_infallible || guards::infallible(class))
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
	let mut seen: FxHashSet<OpId> = FxHashSet::default();
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
		crate::exec::ir::passes::rehome::rebuild_lazy_and_chain(rw, InsertPoint::End(body), once)
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::location::{LocOrigin, Location};
	use crate::exec::ir::expr::ops::ShortCircuitOp;
	use crate::exec::ir::iface::catalog::{
		EffectCx, PhysicalCatalog, SemanticCatalog, default_config,
	};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::ids::{FrameId, SnapshotId};
	use crate::expr::BinaryOperator;
	use crate::val::Value;

	const LOC: Location = Location {
		stmt: 0,
		span: None,
		origin: LocOrigin::Synthesized,
	};

	struct EmptyCatalog;
	impl SemanticCatalog for EmptyCatalog {}
	impl PhysicalCatalog for EmptyCatalog {}

	fn build_filter_with_rhs(
		rhs_kind: ExprOp,
		rhs_operands_fn: impl FnOnce(
			&mut Rewriter<'_>,
			crate::exec::ir::iface::ids::BlockId,
		) -> SmallVec<[ValueId; 4]>,
	) -> (Ir, OpId) {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let top = ir.region(ir.top()).expect("top").blocks[0];
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let src = rw
			.create(
				InsertPoint::End(top),
				OpSpec {
					kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::Bool(true)))),
					operands: SmallVec::new(),
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("src");
		let src_val = rw.ir().op(src).expect("src").results[0];
		let filter = rw
			.create(
				InsertPoint::End(top),
				OpSpec {
					kind: OpKind::Phys(PhysOp::Filter),
					operands: smallvec![src_val],
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("filter");
		let (_, block) = rw
			.create_region(filter, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])
			.expect("where region");
		let row = rw.ir().block(block).expect("where block").args[0];
		let field_a = rw
			.create(
				InsertPoint::End(block),
				OpSpec {
					kind: OpKind::Expr(ExprOp::Field {
						name: "a".into(),
					}),
					operands: smallvec![row],
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("field");
		let field_a_val = rw.ir().op(field_a).expect("field").results[0];
		let one = rw
			.create(
				InsertPoint::End(block),
				OpSpec {
					kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1i64)))),
					operands: SmallVec::new(),
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("one");
		let one_val = rw.ir().op(one).expect("one").results[0];
		let lhs = rw
			.create(
				InsertPoint::End(block),
				OpSpec {
					kind: OpKind::Expr(ExprOp::Binary {
						op: BinaryOperator::Equal,
					}),
					operands: smallvec![field_a_val, one_val],
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("lhs");
		let lhs_val = rw.ir().op(lhs).expect("lhs").results[0];
		let and_op = rw
			.create(
				InsertPoint::End(block),
				OpSpec {
					kind: OpKind::Expr(ExprOp::ShortCircuit {
						op: ShortCircuitOp::And,
					}),
					operands: smallvec![lhs_val],
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("and");
		let (_, lazy_block) = rw.create_region(and_op, RegionKind::LazyOperand, &[]).expect("lazy");
		let rhs_operands = rhs_operands_fn(&mut rw, lazy_block);
		let rhs = rw
			.create(
				InsertPoint::End(lazy_block),
				OpSpec {
					kind: OpKind::Expr(rhs_kind),
					operands: rhs_operands,
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("rhs");
		let rhs_val = rw.ir().op(rhs).expect("rhs").results[0];
		rw.create(
			InsertPoint::End(lazy_block),
			OpSpec {
				kind: OpKind::Expr(ExprOp::Yield),
				operands: smallvec![rhs_val],
				results: SmallVec::new(),
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				loc: LOC,
			},
		)
		.expect("lazy yield");
		let and_val = rw.ir().op(and_op).expect("and").results[0];
		rw.create(
			InsertPoint::End(block),
			OpSpec {
				kind: OpKind::Expr(ExprOp::Yield),
				operands: smallvec![and_val],
				results: SmallVec::new(),
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				loc: LOC,
			},
		)
		.expect("where yield");
		(ir, filter)
	}

	#[test]
	fn once_keeps_fallible_conjunct_per_row_after_kept_conjunct() {
		let registry = FunctionRegistry::new();
		let catalog = EmptyCatalog;
		let frames = FrameTable::default();
		let cx = PhysCx {
			effects: EffectCx {
				registry: &registry,
				catalog: &catalog,
				frames: &frames,
			},
			physical: &catalog,
			config: default_config(),
		};

		// `row.a = 1 AND (1 / 0)`: `1 / 0` is row-independent but fallible, and follows `row.a =
		// 1`, so it must not move to the `once` region ahead of `row.a = 1`.
		let (mut ir, filter) = build_filter_with_rhs(
			ExprOp::Binary {
				op: BinaryOperator::Divide,
			},
			|rw, lazy_block| {
				let n1 = rw
					.create(
						InsertPoint::End(lazy_block),
						OpSpec {
							kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1i64)))),
							operands: SmallVec::new(),
							results: smallvec![TypeKnowledge::Unknown],
							frame: FrameId::new(0),
							snapshot: SnapshotId::new(0),
							loc: LOC,
						},
					)
					.expect("n1");
				let n0 = rw
					.create(
						InsertPoint::End(lazy_block),
						OpSpec {
							kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::from(0i64)))),
							operands: SmallVec::new(),
							results: smallvec![TypeKnowledge::Unknown],
							frame: FrameId::new(0),
							snapshot: SnapshotId::new(0),
							loc: LOC,
						},
					)
					.expect("n0");
				smallvec![
					rw.ir().op(n1).expect("n1").results[0],
					rw.ir().op(n0).expect("n0").results[0]
				]
			},
		);
		let mut facts = FactCache::new();
		let changed = run(&mut ir, &mut facts, &cx).expect("once runs");
		assert_eq!(changed, 0);
		assert_eq!(ir.op(filter).expect("filter").regions.len(), 1);
	}

	#[test]
	fn once_moves_infallible_conjunct_ahead_of_total_kept_conjunct() {
		let registry = FunctionRegistry::new();
		let catalog = EmptyCatalog;
		let frames = FrameTable::default();
		let cx = PhysCx {
			effects: EffectCx {
				registry: &registry,
				catalog: &catalog,
				frames: &frames,
			},
			physical: &catalog,
			config: default_config(),
		};

		// `row.a = 1 AND (1 = 2)`: `1 = 2` is row-independent and infallible, so it can move to
		// the `once` region after a total per-row conjunct.
		let (mut ir, filter) = build_filter_with_rhs(
			ExprOp::Binary {
				op: BinaryOperator::Equal,
			},
			|rw, lazy_block| {
				let n1 = rw
					.create(
						InsertPoint::End(lazy_block),
						OpSpec {
							kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1i64)))),
							operands: SmallVec::new(),
							results: smallvec![TypeKnowledge::Unknown],
							frame: FrameId::new(0),
							snapshot: SnapshotId::new(0),
							loc: LOC,
						},
					)
					.expect("n1");
				let n2 = rw
					.create(
						InsertPoint::End(lazy_block),
						OpSpec {
							kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::from(2i64)))),
							operands: SmallVec::new(),
							results: smallvec![TypeKnowledge::Unknown],
							frame: FrameId::new(0),
							snapshot: SnapshotId::new(0),
							loc: LOC,
						},
					)
					.expect("n2");
				smallvec![
					rw.ir().op(n1).expect("n1").results[0],
					rw.ir().op(n2).expect("n2").results[0]
				]
			},
		);
		let mut facts = FactCache::new();
		let changed = run(&mut ir, &mut facts, &cx).expect("once runs");
		assert_eq!(changed, 1);
		assert_eq!(ir.op(filter).expect("filter").regions.len(), 2);
	}
}
