//! Loop-invariant code motion: a value that does not vary with the loop is evaluated once, where
//! its first use stands, instead of at every row.
//!
//! - [`candidates`] selects the maximal loop-invariant subexpressions of a statement that are worth
//!   evaluating once. It judges invariance only through the row-dependence analysis and reads the
//!   container without changing it.
//! - [`Licm`] places a memo for every candidate. A memo stands at the candidate's own position, so
//!   the candidate keeps its place in evaluation order and its failure stays where it was raised. A
//!   candidate the container cannot memoise is left as it is and counted as unapplied. The count is
//!   kept in the pass report.
//! - [`HoistLeadingConjuncts`] reorders the `AND` chain of a plain `WHERE` filter or of a table
//!   permission predicate so that the loop-invariant conjuncts stand at its front. It moves an
//!   invariant conjunct past an earlier per-row conjunct only when the earlier conjunct is total
//!   (raises nothing, has no effect and reads nothing beyond the row) and the invariant conjunct
//!   itself cannot raise, so short-circuit failure semantics are preserved.
#![deny(clippy::wildcard_enum_match_arm)]

use std::cmp::Reverse;

use super::driver::{LoCx, LoPass, PassConfig, PassReport, PassRun};
use super::rehome::{and_chain, can_rehome, rebuild_lazy_and_chain};
use crate::exec::ir::analysis::multiplicity::is_loop_body;
use crate::exec::ir::analysis::row_dep::{Invariance, invariant_in, outermost_scope};
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::census::Census;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, OpList, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, enclosing_statement, ops_in};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::logical::ops::{LogOp, LogTag};
use crate::exec::ir::op_kind::{OpKind, OpTag};

/// The most memo candidates one statement offers. Past it, the candidates with the fewest ops are
/// left unmemoised.
pub(crate) const MAX_MEMOS_PER_STATEMENT: usize = 256;

/// A maximal loop-invariant subexpression worth evaluating once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MemoCandidate {
	/// The op whose result is evaluated once.
	pub(crate) root: OpId,
	/// The outermost loop the result is invariant in: the loop whose execution bounds the memo's
	/// lifetime.
	pub(crate) scope: OpId,
	/// The per-row loop bodies between `root` and `scope`, `scope`'s own body included.
	pub(crate) levels: u8,
	/// The ops the memo would hold: the root, the ops of its operand tree that nothing else reads,
	/// and every op inside their regions.
	pub(crate) ops: u32,
}

/// Every memo candidate of the statement `stmt`, largest first, at most
/// [`MAX_MEMOS_PER_STATEMENT`].
///
/// All of them are computed in one sweep over a container that is not modified, so every
/// invariance answer is given against the same container.
///
/// A candidate is an expression op in the per-row body of a loop of the statement, reached through
/// the regions of the statement's own code, such that:
///
/// - its result is invariant in that loop and some reader of it is not, a `yield` reading on behalf
///   of the op that owns its region, so it is a maximal invariant subexpression;
/// - it is worth the memo: not a literal, table, constant, session parameter, clock read or yield,
///   not a lone parameter read, and not a single infallible op;
/// - it holds no `RETURN`, `BREAK` or `CONTINUE`; a raise is allowed;
/// - it stands in no field-guard permission region and no field frame of a layer. The table
///   permission predicate, `WHERE`, projections and `FOR` bodies are open to memos.
///
/// Its scope is the outermost loop the result is invariant in. A candidate invariant in several
/// loops is reported once. A candidate may lie inside another candidate's operand tree, and its
/// scope then encloses the other's.
pub(crate) fn candidates(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	stmt: OpId,
) -> Vec<MemoCandidate> {
	let mut seen: FxHashSet<OpId> = FxHashSet::default();
	let mut found: Vec<MemoCandidate> = Vec::new();
	for scope_loop in loops_in(ir, stmt) {
		let mut sweep = Sweep {
			ir,
			facts: &mut *facts,
			cx,
			scope: scope_loop,
			roots: Vec::new(),
		};
		for body in body_regions(ir, scope_loop) {
			sweep.region(body);
		}
		for root in sweep.roots {
			if seen.insert(root)
				&& let Some(candidate) = candidate_of(ir, facts, cx, root)
			{
				found.push(candidate);
			}
		}
	}
	found.sort_by_key(|candidate| Reverse(candidate.ops));
	found.truncate(MAX_MEMOS_PER_STATEMENT);
	found
}

/// Places a memo for `c`: an `expr.memo` that takes the candidate's position, holds the
/// candidate's ops in its lazily evaluated region, and answers the candidate's value. The memo
/// evaluates the region at the first reach within an execution of `c.scope`, and every later reach
/// within that execution reads the value kept.
///
/// Answers `Ok(true)` when it placed the memo, and `Ok(false)`, changing nothing, when the
/// container has no memo op to place.
fn place_memo(_rw: &mut Rewriter<'_>, _c: &MemoCandidate) -> Result<bool, IrError> {
	Ok(false)
}

/// Memoises the loop-invariant subexpressions of every statement.
///
/// The candidates of a statement are all selected before the first memo is placed, so no
/// invariance question is asked of a container a placement has changed. A candidate for which no
/// memo can be placed is counted in [`PassReport::memo_candidates_unapplied`] and left as it is.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Licm;

impl LoPass for Licm {
	fn name(&self) -> &'static str {
		"licm"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.licm
	}

	/// [`run_licm`] takes its candidates from [`candidates`], which sweeps the bodies of the loops
	/// [`loops_in`] returns: the `logical.loop` ops outside stored code. With none, no sweep runs,
	/// no fact is read, no candidate is counted, and the rewriter it builds is not used.
	fn needs(&self, census: &Census) -> bool {
		census.has(LogTag::Loop)
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_licm(cx))
	}
}

/// Reorders the conjuncts of a `WHERE` filter and of a table permission predicate so that the
/// loop-invariant conjuncts the chain evaluates first stand at its front.
///
/// Applies to the `AND` chain of a `logical.filter` over a loop whose body holds no permission
/// filter and no projection layer, and to the predicate of every `logical.permission_filter`. A
/// conjunct is eligible when it is invariant in the enclosing loop, every conjunct written before
/// it that is not eligible is total (infallible, effect-free and reading nothing beyond the row),
/// and once any non-eligible conjunct precedes it, the invariant conjunct itself is infallible.
/// The eligible conjuncts keep their written order and move to the front, and the others follow in
/// their written order.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HoistLeadingConjuncts;

impl LoPass for HoistLeadingConjuncts {
	fn name(&self) -> &'static str {
		"hoist_leading_conjuncts"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.hoist_leading
	}

	/// [`run_hoist`] offers each op outside stored code to [`hoist_at`], which returns before any
	/// fact is read or the container touched unless [`chain_site`] accepts the op: a
	/// `logical.filter` or `logical.permission_filter` for which [`enclosing_loop`] finds a
	/// `logical.loop`. The filter is outside stored code, so the loop that encloses it is too.
	fn needs(&self, census: &Census) -> bool {
		census.has(LogTag::Loop)
			&& (census.has(LogTag::Filter) || census.has(LogTag::PermissionFilter))
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_hoist(cx))
	}
}

// ---------------------------------------------------------------------------------------------
// Container walks
// ---------------------------------------------------------------------------------------------

/// Whether `op` stands inside a stored clause, whose code is bound and analysed but never
/// rewritten.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Every op outside stored clauses, in program order: an op before the ops of its regions.
fn live_ops(ir: &Ir) -> OpList {
	ir.collect_ops(ir.top(), WalkOrder::PreOrder, |op| !in_stored_clause(ir, op))
}

/// The op that defines `value`, or `None` for a block argument and for a dangling id.
fn def_op(ir: &Ir, value: ValueId) -> Option<OpId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => ir.op(op).map(|_| op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// Whether `op` is a `logical.loop`.
fn is_loop(ir: &Ir, op: OpId) -> bool {
	ir.op(op).is_some_and(|data| data.kind.tag() == OpTag::Logical(LogTag::Loop))
}

/// The per-row body regions of the loop `loop_op`.
fn body_regions(ir: &Ir, loop_op: OpId) -> Vec<RegionId> {
	let Some(data) = ir.op(loop_op) else {
		return Vec::new();
	};
	data.regions
		.iter()
		.copied()
		.filter(|region| ir.region(*region).is_some_and(|held| is_loop_body(held.kind)))
		.collect()
}

/// The loops of the statement `stmt` outside stored clauses, innermost first.
fn loops_in(ir: &Ir, stmt: OpId) -> Vec<OpId> {
	let mut loops = Vec::new();
	let Some(data) = ir.op(stmt) else {
		return loops;
	};
	for region in data.regions.iter().copied() {
		ir.walk(region, WalkOrder::PostOrder, &mut |op| {
			if is_loop(ir, op) && !in_stored_clause(ir, op) {
				loops.push(op);
			}
			Walk::Continue
		});
	}
	loops
}

/// The statements that no other statement encloses, outside stored clauses.
fn outermost_statements(ir: &Ir) -> Vec<OpId> {
	live_ops(ir)
		.into_iter()
		.filter(|op| {
			ir.op(*op).is_some_and(|data| data.kind.tag() == OpTag::Logical(LogTag::Stmt))
				&& enclosing_statement(ir, *op).is_none()
		})
		.collect()
}

/// The nearest loop whose per-row body holds `op`.
fn enclosing_loop(ir: &Ir, op: OpId) -> Option<OpId> {
	ancestors(ir, op).find_map(|(region, owner)| {
		let owner = owner?;
		(is_loop_body(ir.region(region)?.kind) && is_loop(ir, owner)).then_some(owner)
	})
}

/// Whether `value` is invariant in the loop `scope`.
fn invariant(ir: &Ir, value: ValueId, scope: OpId) -> bool {
	matches!(invariant_in(ir, value, scope), Invariance::Invariant)
}

// ---------------------------------------------------------------------------------------------
// Memo candidates
// ---------------------------------------------------------------------------------------------

/// One sweep over the per-row body of a loop, collecting the maximal invariant expression ops.
struct Sweep<'a, 'b> {
	ir: &'a Ir,
	facts: &'a mut FactCache,
	cx: &'a EffectCx<'b>,
	scope: OpId,
	roots: Vec<OpId>,
}

impl Sweep<'_, '_> {
	fn region(&mut self, region: RegionId) {
		let ir = self.ir;
		for op in ops_in(ir, region) {
			self.op(op);
		}
	}

	/// An invariant op is judged as a whole and its regions are not entered: every op inside it is
	/// read only by ops of the same invariant subtree.
	fn op(&mut self, op: OpId) {
		let ir = self.ir;
		let Some(data) = ir.op(op) else {
			return;
		};
		if let (OpKind::Expr(_), [result]) = (&data.kind, data.results.as_slice())
			&& invariant(ir, *result, self.scope)
		{
			if self.is_maximal(*result) {
				self.roots.push(op);
			}
			return;
		}
		for region in data.regions.iter().copied() {
			if self.admits(region) {
				self.region(region);
			}
		}
	}

	/// Whether a memo may stand in `region`: not in a field frame, and not in a permission frame
	/// that is not a table's predicate.
	fn admits(&self, region: RegionId) -> bool {
		let ir = self.ir;
		let Some(held) = ir.region(region) else {
			return false;
		};
		if matches!(held.kind, RegionKind::FieldFrame(_)) {
			return false;
		}
		if held.kind == RegionKind::PermissionFrame {
			return held.parent.and_then(|owner| ir.op(owner)).is_some_and(|owner| {
				matches!(owner.kind, OpKind::Logical(LogOp::PermissionFilter { .. }))
			});
		}
		true
	}

	/// Whether some reader of `result` varies with the loop.
	fn is_maximal(&mut self, result: ValueId) -> bool {
		let ir = self.ir;
		let readers: Vec<OpId> = ir.uses(result).iter().map(|held| held.user).collect();
		readers.into_iter().any(|reader| self.varies(reader))
	}

	/// Whether the value `reader` hands on varies with the loop. A `yield` hands on the value of
	/// the op that owns its region; an op that is not an expression op with one result varies.
	fn varies(&mut self, reader: OpId) -> bool {
		let ir = self.ir;
		let Some(data) = ir.op(reader) else {
			return true;
		};
		let carrier = match &data.kind {
			OpKind::Expr(ExprOp::Yield) => match ir.parent_op(ir.enclosing_region(reader)) {
				Some(owner) => owner,
				None => return true,
			},
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => reader,
		};
		let Some(carried) = ir.op(carrier) else {
			return true;
		};
		match (&carried.kind, carried.results.as_slice()) {
			(OpKind::Expr(_), [result]) => !invariant(ir, *result, self.scope),
			(OpKind::Expr(_), _) | (OpKind::Logical(_) | OpKind::Phys(_), _) => true,
		}
	}
}

/// The candidate for `root`, or `None` when it fails a rule of [`candidates`] that is judged on
/// the op alone.
fn candidate_of(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	root: OpId,
) -> Option<MemoCandidate> {
	let data = ir.op(root)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	let [result] = data.results.as_slice() else {
		return None;
	};
	if matches!(
		kind,
		ExprOp::Literal(_)
			| ExprOp::Table { .. }
			| ExprOp::Constant { .. }
			| ExprOp::SessionParam { .. }
			| ExprOp::Now { .. }
			| ExprOp::Yield
	) {
		return None;
	}
	let ops = moved_tree_size(ir, root);
	if ops == 1
		&& (matches!(kind, ExprOp::FreeParam { .. } | ExprOp::CatalogParam { .. })
			|| guards::infallible(fallibility::of_with(ir, facts, cx, root)))
	{
		return None;
	}
	if holds_control_exit(ir, root) {
		return None;
	}
	let scope = outermost_scope(ir, *result)?;
	let levels = per_row_scopes(ir, root, scope)?;
	Some(MemoCandidate {
		root,
		scope,
		levels,
		ops: u32::try_from(ops).unwrap_or(u32::MAX),
	})
}

/// Whether a region of `op` holds a `RETURN`, `BREAK` or `CONTINUE`.
fn holds_control_exit(ir: &Ir, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let mut found = false;
	for region in data.regions.iter().copied() {
		ir.walk(region, WalkOrder::PreOrder, &mut |inner| {
			let exits = ir.op(inner).is_some_and(|held| {
				matches!(
					held.kind.tag(),
					OpTag::Logical(LogTag::Return | LogTag::Break | LogTag::Next)
				)
			});
			if exits {
				found = true;
				return Walk::Stop;
			}
			Walk::Continue
		});
	}
	found
}

/// The per-row loop bodies that enclose `op`, from the innermost out to the body of `scope`
/// inclusive, or `None` when `scope` does not enclose it or the count exceeds `u8`.
fn per_row_scopes(ir: &Ir, op: OpId, scope: OpId) -> Option<u8> {
	let mut count: u8 = 0;
	for (region, owner) in ancestors(ir, op) {
		let Some(owner) = owner else {
			break;
		};
		if is_loop_body(ir.region(region)?.kind) && is_loop(ir, owner) {
			count = count.checked_add(1)?;
			if owner == scope {
				return Some(count);
			}
		}
	}
	None
}

/// The ops a move of the tree rooted at `root` would carry: the root, the ops of its operand tree
/// that stand in its block and are read only inside the tree, and every op inside their regions.
fn moved_tree_size(ir: &Ir, root: OpId) -> usize {
	let Some(block) = ir.op(root).map(|data| data.parent) else {
		return 0;
	};
	let mut members: FxHashSet<OpId> = FxHashSet::default();
	members.insert(root);
	let mut pending = vec![root];
	while let Some(op) = pending.pop() {
		let Some(data) = ir.op(op) else {
			continue;
		};
		for operand in &data.operands {
			if let Some(defining) = def_op(ir, *operand)
				&& ir.op(defining).is_some_and(|held| held.parent == block)
				&& members.insert(defining)
			{
				pending.push(defining);
			}
		}
	}
	loop {
		let outside: Vec<OpId> = members
			.iter()
			.copied()
			.filter(|op| *op != root && read_outside(ir, *op, &members))
			.collect();
		if outside.is_empty() {
			break;
		}
		for op in outside {
			members.remove(&op);
		}
	}
	members.iter().map(|op| 1 + nested_ops(ir, *op)).sum()
}

/// Whether a result of `op` is read by an op that is neither a member nor inside one.
fn read_outside(ir: &Ir, op: OpId, members: &FxHashSet<OpId>) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	data.results.iter().any(|result| {
		ir.uses(*result).iter().any(|held| {
			!(members.contains(&held.user)
				|| ancestors(ir, held.user)
					.any(|(_, owner)| owner.is_some_and(|owner| members.contains(&owner))))
		})
	})
}

/// The ops inside the regions of `op`, at any depth.
fn nested_ops(ir: &Ir, op: OpId) -> usize {
	let mut count = 0;
	if let Some(data) = ir.op(op) {
		for region in data.regions.iter().copied() {
			ir.walk(region, WalkOrder::PreOrder, &mut |_| {
				count += 1;
				Walk::Continue
			});
		}
	}
	count
}

fn run_licm(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	let selected: Vec<Vec<MemoCandidate>> = outermost_statements(cx.ir)
		.into_iter()
		.map(|stmt| candidates(&*cx.ir, &mut *cx.facts, cx.effects, stmt))
		.collect();
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	for candidate in selected.iter().flatten() {
		if place_memo(&mut rw, candidate)? {
			report.rewrites = report.rewrites.saturating_add(1);
		} else {
			report.memo_candidates_unapplied = report.memo_candidates_unapplied.saturating_add(1);
		}
	}
	Ok(report)
}

// ---------------------------------------------------------------------------------------------
// Leading conjuncts
// ---------------------------------------------------------------------------------------------

/// A conjunct list this pass may reorder: the clause's closing yield, the value it yields and the
/// loop the clause runs in.
struct Site {
	yielded: OpId,
	predicate: ValueId,
	loop_op: OpId,
}

/// The conjunct list of `op` when it is a `WHERE` filter over a loop with no permission filter and
/// no projection layer, or a permission filter, standing in a loop.
fn chain_site(ir: &Ir, op: OpId) -> Option<Site> {
	let data = ir.op(op)?;
	let (wanted, plain) = match &data.kind {
		OpKind::Logical(LogOp::Filter) => (RegionKind::Clause(ClauseRole::Where), true),
		OpKind::Logical(LogOp::PermissionFilter {
			..
		}) => (RegionKind::PermissionFrame, false),
		OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => return None,
	};
	// The predicate is the first region; a permission filter that derives computed fields lists
	// their bodies after it, and the reordering only touches the predicate's own chain.
	let region = data.regions.first()?;
	let held = ir.region(*region)?;
	if held.kind != wanted {
		return None;
	}
	let loop_op = enclosing_loop(ir, op)?;
	if plain && body_binds_checks(ir, loop_op) {
		return None;
	}
	let [block] = held.blocks.as_slice() else {
		return None;
	};
	let yielded = *ir.block(*block)?.ops.last()?;
	let closing = ir.op(yielded)?;
	let (OpKind::Expr(ExprOp::Yield), [predicate]) = (&closing.kind, closing.operands.as_slice())
	else {
		return None;
	};
	Some(Site {
		yielded,
		predicate: *predicate,
		loop_op,
	})
}

/// Whether the per-row body of `loop_op` holds a permission filter or a projection layer, which is
/// the checked path whose conjunct order is not rewritten.
fn body_binds_checks(ir: &Ir, loop_op: OpId) -> bool {
	body_regions(ir, loop_op).into_iter().flat_map(|region| ops_in(ir, region)).any(|op| {
		ir.op(op).is_some_and(|data| match &data.kind {
			OpKind::Logical(LogOp::PermissionFilter {
				..
			}) => true,
			OpKind::Logical(LogOp::Project(attrs)) => attrs.layer,
			OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => false,
		})
	})
}

/// The ops that compute `value`, in program order: its defining op and, transitively, the ops of
/// the same block that define their operands. The ops inside their regions are not listed; an
/// analysis of one of them covers its regions.
fn computing_ops(ir: &Ir, value: ValueId) -> Vec<OpId> {
	let Some(root) = def_op(ir, value) else {
		return Vec::new();
	};
	let Some(block) = ir.op(root).map(|data| data.parent) else {
		return Vec::new();
	};
	let mut seen: FxHashSet<OpId> = FxHashSet::default();
	let mut pending = vec![root];
	while let Some(op) = pending.pop() {
		if !seen.insert(op) {
			continue;
		}
		let Some(data) = ir.op(op) else {
			continue;
		};
		for operand in &data.operands {
			if let Some(def) = def_op(ir, *operand)
				&& ir.op(def).is_some_and(|held| held.parent == block)
			{
				pending.push(def);
			}
		}
		for region in &data.regions {
			for inner in ops_in(ir, *region) {
				let Some(inner_data) = ir.op(inner) else {
					continue;
				};
				for operand in &inner_data.operands {
					if let Some(def) = def_op(ir, *operand)
						&& ir.op(def).is_some_and(|held| held.parent == block)
					{
						pending.push(def);
					}
				}
			}
		}
	}
	let Some(held) = ir.block(block) else {
		return Vec::new();
	};
	held.ops.iter().copied().filter(|op| seen.contains(op)).collect()
}

/// What evaluating a conjunct can do, over every op that computes it.
struct Verdict {
	/// No op of the conjunct can raise.
	infallible: bool,
	/// Every op of the conjunct has effects within the row alone.
	pure: bool,
}

impl Verdict {
	/// A conjunct that raises nothing, has no effect and reads nothing beyond the row.
	fn total(&self) -> bool {
		self.infallible && self.pure
	}
}

fn judge(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, value: ValueId) -> Verdict {
	let mut verdict = Verdict {
		infallible: true,
		pure: true,
	};
	for op in computing_ops(ir, value) {
		let answer = effects::of_with(ir, facts, cx, op);
		let class = answer.fallibility.union(fallibility::floor(answer.set));
		verdict.infallible &= guards::infallible(class);
		verdict.pure &= guards::pure(answer.set);
	}
	verdict
}

/// The order `chain` takes once its eligible conjuncts move to the front, or `None` when nothing
/// moves or the chain already stands in that order.
fn hoisted_order(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	site: &Site,
	chain: &[ValueId],
) -> Option<Vec<ValueId>> {
	let mut moved: Vec<usize> = Vec::new();
	let mut stays = false;
	for (position, value) in chain.iter().enumerate() {
		let verdict = judge(ir, facts, cx, *value);
		if invariant(ir, *value, site.loop_op) && (!stays || verdict.infallible) {
			moved.push(position);
		} else if verdict.total() {
			stays = true;
		} else {
			break;
		}
	}
	if moved.is_empty() || moved.iter().enumerate().all(|(slot, position)| slot == *position) {
		return None;
	}
	let front = moved.iter().map(|position| chain[*position]);
	let rest = chain
		.iter()
		.enumerate()
		.filter(|(position, _)| !moved.contains(position))
		.map(|(_, value)| *value);
	Some(front.chain(rest).collect())
}

/// Whether every conjunct of `order` can stand before the clause's yield: one at the front that is
/// already defined in the yield's block is read where it is, and every other conjunct is read by
/// the chain alone and passes the checks of a move into its lazy region.
fn can_place(ir: &Ir, site: &Site, order: &[ValueId]) -> bool {
	let Some(block) = ir.op(site.yielded).map(|data| data.parent) else {
		return false;
	};
	order.iter().enumerate().all(|(slot, value)| {
		ir.uses(*value).len() == 1
			&& def_op(ir, *value).is_none_or(|def| {
				(slot == 0 && ir.op(def).is_some_and(|data| data.parent == block))
					|| can_rehome(ir, def, InsertPoint::Before(site.yielded), &[])
			})
	})
}

/// The `AND` ops along the left side of the chain whose root is `root`, root first: the ops a
/// rebuilt chain leaves unread.
fn chain_spine(ir: &Ir, root: ValueId) -> Vec<OpId> {
	let mut spine = Vec::new();
	let mut value = root;
	while let Some(op) = def_op(ir, value) {
		let Some(data) = ir.op(op) else {
			break;
		};
		let OpKind::Expr(ExprOp::ShortCircuit {
			op: ShortCircuitOp::And,
		}) = &data.kind
		else {
			break;
		};
		spine.push(op);
		match data.operands.first() {
			Some(left) if ir.uses(*left).len() == 1 => value = *left,
			Some(_) | None => break,
		}
	}
	spine
}

/// Reorders the chain of `op` when it is a site, and answers whether the chain changed.
///
/// Everything that can refuse the rebuild is checked before the container is touched.
fn hoist_at(cx: &mut LoCx<'_>, op: OpId) -> Result<bool, IrError> {
	let Some(site) = chain_site(cx.ir, op) else {
		return Ok(false);
	};
	let chain = and_chain(cx.ir, site.predicate);
	if chain.len() < 2 {
		return Ok(false);
	};
	let Some(order) = hoisted_order(&*cx.ir, &mut *cx.facts, cx.effects, &site, &chain) else {
		return Ok(false);
	};
	if !can_place(cx.ir, &site, &order) {
		return Ok(false);
	}
	let spine = chain_spine(cx.ir, site.predicate);
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	let rebuilt = rebuild_lazy_and_chain(&mut rw, InsertPoint::Before(site.yielded), &order)?;
	rw.set_operand(site.yielded, 0, rebuilt)?;
	for gate in spine {
		if rw.ir().op(gate).is_none() {
			continue;
		}
		match rw.erase(gate, EliminationReason::DeadPure) {
			Ok(()) | Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	Ok(true)
}

fn run_hoist(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	for op in live_ops(cx.ir) {
		if cx.ir.op(op).is_some() && hoist_at(cx, op)? {
			report.rewrites = report.rewrites.saturating_add(1);
		}
	}
	Ok(report)
}
