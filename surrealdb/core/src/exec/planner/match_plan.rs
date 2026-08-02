//! Planning for GQL v2 `MATCH` queries.
//!
//! [`Planner::plan_match`] compiles a [`MatchPlan`] (the language-neutral
//! binding-table IR produced by the GQL lowering, `expr/match_plan.rs`) into a
//! tree of streaming operators. See `doc/gql/V2_DESIGN.md` §6 for the
//! normative algorithm and the worked plan trees this module pins as EXPLAIN
//! snapshots.
//!
//! # Scope
//!
//! This slice handles **multi-pattern** `MATCH` (comma-separated patterns in one
//! clause) and **sequential** `MATCH` statements (multiple clauses), joined on
//! their shared variables (equi-join) or by cartesian product (no shared
//! variables). Repeated *node* variables across patterns / clauses become join
//! keys (PR-A rejected them; that rejection flips here). The
//! per-MATCH-statement DIFFERENT EDGES default (R2) is enforced with a
//! [`DistinctEdges`] wrapper per clause.
//!
//! `OPTIONAL MATCH` is a left-outer combine against the accumulated binding
//! table (R3). The lowering tags each optional clause with `optional` + an
//! `optional_group` block id; the planner folds each block (the run of clauses
//! sharing one id) as a unit via either the OptionalExpand fast path or a
//! `HashJoin { Left }` over the block's standalone subplan — see
//! [`Planner::fold_optional`]. After the read body, the plan's
//! `mutations` are folded into a chain of write operators
//! (`UpdateBinding`/`DeleteBinding`/`InsertGraph`, see [`Planner::plan_mutations`]);
//! a leading `INSERT` with no read body is seeded by a `SingleRowScan`. Still out
//! of scope and rejected upstream in the lowering: label mutations, label
//! expressions, undirected edges.
//!
//! None of the internal bails here are `PlannerUnsupported`/`PlannerUnimplemented`:
//! a `MatchPlan` only reaches the planner because it lowered cleanly, so an
//! unexpected shape is a real internal failure with no compute fallback (the
//! `Expr::Match` compute arm hard-errors).
//!
//! # The plan shape
//!
//! Per pattern, a binder chain rooted at one of three anchors:
//!
//! ```text
//! Bound variable  → expand off the accumulator's rows (no new scan); the
//!                    far-node-only case walks the single hop in reverse.
//! Labeled node    → TableScan + Bind (single-binding conjuncts pushed into the
//!                    scan via prefix-strip), then Expand / PathExpand steps.
//! Labeled edge    → TableScan + Bind of the edge, then EndpointBind×2 (in then
//!                    out) to recover the endpoint nodes.
//! ```
//!
//! Patterns within a clause combine via `HashJoin` (Inner on shared node-binding
//! ids, Cross if none shared) — except a bound-variable pattern, which extends
//! the accumulator directly. Clauses fold left-to-right the same way. A
//! `DistinctEdges` wraps a clause's combined plan when it has ≥2 edge-ish
//! bindings whose edge-table sets are not statically disjoint. The tail
//! (Sort/Limit/Project, or Project/Distinct/Sort/Limit for DISTINCT) is
//! unchanged from the single-pattern case.

use std::sync::Arc;

use surrealdb_strand::TableName;

use super::Planner;
use crate::err::{EngineError, Error};
use crate::exec::ExecOperator;
use crate::exec::operators::{
	Aggregate, AggregateField, Bind, DeleteBinding, Distinct, DistinctEdges, DrainSink,
	EdgeBinding, EndpointBind, EndpointField, Expand, ExpandDir, FieldSelection, Filter, HashJoin,
	InsertEdgeOp, InsertGraph, InsertNodeOp, JoinType, Limit, OrderByField, PathExpand, PathMode,
	Project, ShortestPathExpand, ShortestSelector, SingleRowScan, Sort, SortDirection,
	UpdateBinding,
};
use crate::expr::match_plan::{
	BindingId, BindingKind, EdgeStep, ExpandDirection, MatchClausePlan, MatchOutput, MatchPlan,
	MatchPredicate, MatchStage, MutationStage, NodeStep, PathMode as IrPathMode, PathPrefixPlan,
	PathSearch, PatternPlan,
};
use crate::expr::{Cond, Expr, Function, Idiom, Literal, Part};

/// How a quantified step's path-search selector routes to a physical operator.
enum SearchRouting {
	/// Every matching path (`None` / `ALL`), via the `PathExpand` DFS.
	Every,
	/// A bounded path search (`ANY [k]` / the `SHORTEST` family), via the
	/// `ShortestPathExpand` BFS. `ANY [k]` rides the same per-node-bounded
	/// traversal because a shortest representative is a valid arbitrary path.
	Shortest {
		selector: ShortestSelector,
	},
}

/// Resolve a pattern's lowered path-search prefix into the executing path mode
/// and the operator routing. `None` (no prefix) is the default: every path, the
/// edge-unique `WALK` mode.
fn resolve_path_search(search: Option<PathPrefixPlan>) -> (PathMode, SearchRouting) {
	let Some(prefix) = search else {
		return (PathMode::Walk, SearchRouting::Every);
	};
	let mode = match prefix.mode {
		IrPathMode::Walk => PathMode::Walk,
		IrPathMode::Trail => PathMode::Trail,
		IrPathMode::Simple => PathMode::Simple,
		IrPathMode::Acyclic => PathMode::Acyclic,
	};
	let routing = match prefix.search {
		PathSearch::All => SearchRouting::Every,
		PathSearch::Any {
			count,
		} => SearchRouting::Shortest {
			selector: ShortestSelector::Any {
				count,
			},
		},
		PathSearch::AllShortest => SearchRouting::Shortest {
			selector: ShortestSelector::AllShortest,
		},
		PathSearch::AnyShortest => SearchRouting::Shortest {
			selector: ShortestSelector::AnyShortest,
		},
		PathSearch::ShortestCounted {
			count,
		} => SearchRouting::Shortest {
			selector: ShortestSelector::Counted(count),
		},
		PathSearch::ShortestGroups {
			count,
		} => SearchRouting::Shortest {
			selector: ShortestSelector::CountedGroups(count),
		},
	};
	(mode, routing)
}

/// A binder stage: the operator built so far, the bindings available on its
/// rows, and the current "tip" node (what the next step expands from).
struct ChainStage {
	operator: Arc<dyn ExecOperator>,
	/// Bindings available on rows leaving this stage.
	bound: Vec<BindingId>,
	/// The most recently bound node (anchor, or the prior step's target). The
	/// next step expands from this binding — tracked explicitly so a trailing
	/// path-var binding never gets mistaken for the expansion source.
	tip: BindingId,
}

/// One clause planned into a sub-tree, with the metadata the fold needs to
/// combine it with the accumulator.
struct PlannedClause {
	operator: Arc<dyn ExecOperator>,
	/// Every binding present on rows leaving this clause.
	bound: Vec<BindingId>,
	/// Clause predicates deferred to the clause-combine join (their deps span an
	/// earlier clause's bindings, so the join is the earliest stage that binds
	/// them all).
	deferred: Vec<MatchPredicate>,
}

impl<'ctx> Planner<'ctx> {
	/// Plan a GQL `MATCH` query into a streaming operator tree.
	///
	/// Folds clauses left-to-right over an accumulator `(operator, bound)`: the
	/// first clause seeds it; each subsequent (non-optional) clause is joined to
	/// the accumulator with a `HashJoin` — Inner on the node-binding ids the two
	/// sides share, or Cross when they share none (sequential `MATCH` joins
	/// exactly like comma-separated patterns, R1).
	pub(crate) async fn plan_match(&self, plan: MatchPlan) -> Result<Arc<dyn ExecOperator>, Error> {
		// Group the ordered stages into fold units: each mandatory read clause is
		// a unit; a run of read clauses sharing one `optional_group` id is one
		// OPTIONAL block unit (the all-or-nothing left-join, R3); each write stage
		// is a mutation unit. Reads and mutations interleave as written — a read
		// after a write re-scans the live (post-write) state in the same txn.
		let units = stage_units(&plan);
		let mut units = units.into_iter();

		// Seed from the first unit.
		let Some(first) = units.next() else {
			return Err(EngineError::Internal(
				"GQL planning: a MatchPlan must carry at least one stage".to_string(),
			)
			.into());
		};
		let (mut acc_op, mut acc_bound) = match first {
			StageUnit::Mandatory(clause) => {
				let planned = self.plan_clause(&plan, clause, &[]).await?;
				debug_assert!(
					planned.deferred.is_empty(),
					"GQL planning: the first clause cannot defer predicates (nothing precedes it)",
				);
				(planned.operator, planned.bound)
			}
			StageUnit::Optional(_) => {
				return Err(EngineError::Internal(
					"GQL planning: a query cannot start with OPTIONAL (the lowering must reject it)"
						.to_string(),
				).into());
			}
			StageUnit::Mutation(stage) => {
				// A leading mutation (an `INSERT` with no preceding `MATCH`): seed a
				// single empty row so it runs exactly once.
				let seed = Arc::new(SingleRowScan::new()) as Arc<dyn ExecOperator>;
				self.fold_mutation(&plan, seed, Vec::new(), stage)
			}
		};

		// Fold the remaining units.
		for unit in units {
			let (op, bound) = match unit {
				StageUnit::Mandatory(clause) => {
					let planned = self.plan_clause(&plan, clause, &acc_bound).await?;
					self.fold_mandatory(&plan, acc_op, &acc_bound, planned).await?
				}
				StageUnit::Optional(clauses) => {
					self.fold_optional(&plan, acc_op, &acc_bound, &clauses).await?
				}
				StageUnit::Mutation(stage) => self.fold_mutation(&plan, acc_op, acc_bound, stage),
			};
			acc_op = op;
			acc_bound = bound;
		}

		self.plan_match_tail(&plan, acc_op).await
	}

	/// Build one write operator over the accumulator and return it with the
	/// updated bound set. `INSERT` extends the bound set with its created node /
	/// edge bindings, so a later read can join or anchor on them; `SET`/`DELETE`
	/// leave the bound set unchanged (their target was already bound).
	fn fold_mutation(
		&self,
		plan: &MatchPlan,
		op: Arc<dyn ExecOperator>,
		mut bound: Vec<BindingId>,
		stage: &MutationStage,
	) -> (Arc<dyn ExecOperator>, Vec<BindingId>) {
		let op = match stage {
			MutationStage::Update {
				target,
				data,
			} => {
				let name = binding_name(plan, *target).to_string();
				Arc::new(UpdateBinding::new(op, name, data.clone())) as Arc<dyn ExecOperator>
			}
			MutationStage::Delete {
				target,
				detach,
			} => {
				let name = binding_name(plan, *target).to_string();
				let is_edge = matches!(
					plan.binding(*target).kind,
					BindingKind::Edge | BindingKind::EdgeGroup
				);
				Arc::new(DeleteBinding::new(op, name, *detach, is_edge)) as Arc<dyn ExecOperator>
			}
			MutationStage::Insert(insert) => {
				let nodes: Vec<InsertNodeOp> = insert
					.nodes
					.iter()
					.map(|n| InsertNodeOp {
						name: binding_name(plan, n.binding).to_string(),
						table: n.label.clone(),
						props: n.props.clone(),
					})
					.collect();
				let edges: Vec<InsertEdgeOp> = insert
					.edges
					.iter()
					.map(|e| InsertEdgeOp {
						name: binding_name(plan, e.binding).to_string(),
						table: e.label.clone(),
						from: binding_name(plan, e.from).to_string(),
						to: binding_name(plan, e.to).to_string(),
						props: e.props.clone(),
					})
					.collect();
				bound.extend(insert.nodes.iter().map(|n| n.binding));
				bound.extend(insert.edges.iter().map(|e| e.binding));
				Arc::new(InsertGraph::new(op, nodes, edges)) as Arc<dyn ExecOperator>
			}
		};
		(op, bound)
	}

	/// Combine a freshly-planned mandatory clause with the accumulator.
	///
	/// Mandatory clauses join via `HashJoin` Inner on the node-binding ids the
	/// accumulator and the clause share, or Cross when they share none. Deferred
	/// clause predicates run as a `Filter` directly above the join (the earliest
	/// stage where their deps are all bound).
	async fn fold_mandatory(
		&self,
		plan: &MatchPlan,
		acc_op: Arc<dyn ExecOperator>,
		acc_bound: &[BindingId],
		clause: PlannedClause,
	) -> Result<(Arc<dyn ExecOperator>, Vec<BindingId>), Error> {
		// Accumulator = build side; new clause = probe side.
		let shared = shared_bindings(acc_bound, &clause.bound);
		let keys = binding_names(plan, &shared);
		let join_type = if shared.is_empty() {
			JoinType::Cross
		} else {
			JoinType::Inner
		};
		let join =
			Arc::new(HashJoin::new(acc_op, clause.operator, keys, join_type, Vec::new(), None))
				as Arc<dyn ExecOperator>;

		let bound = union_bindings(acc_bound, &clause.bound);
		let op = self.place_above(join, &bound, clause.deferred).await?;
		Ok((op, bound))
	}

	/// Combine one `OPTIONAL` block (a run of clauses sharing one `optional_group`
	/// id, R3) with the accumulator as a left-outer unit.
	///
	/// Two physical shapes (V2_DESIGN §5/§6):
	///
	/// - **OptionalExpand fast path** — the block is a single clause with a single pattern that is
	///   a single, non-quantified hop whose source node is already bound in the accumulator (the
	///   dominant case, worked tree (iii)). The hop is appended directly to the accumulator as
	///   `Expand { optional: true }`: one operator, no join. The `Expand` null-fills the edge and
	///   target on a zero-survivor row, which is exactly the set of bindings this block introduces
	///   (its source comes from the accumulator), so R3 holds.
	///
	/// - **LeftJoin general path** — everything else (multi-hop, multi-pattern, multi-clause block,
	///   optional sharing a non-start variable, optional sharing no variable). The WHOLE block is
	///   planned as one standalone subplan: its clauses' patterns are anchored / inner-joined
	///   internally and its inside-predicates compile inside the subplan (so they evaluate
	///   pre-null, R3 structural). That subplan is the build side of a `HashJoin { Left }`; the
	///   accumulator is the probe side; the join keys are the node bindings the block shares with
	///   the accumulator; and the `null_template` is every binding the block introduces. A block
	///   matches all-or-nothing because the entire block is one subplan and one join unit: a
	///   partial inner match never escapes the subplan, and an accumulator row the block could not
	///   match at all is null-filled across every block binding.
	async fn fold_optional(
		&self,
		plan: &MatchPlan,
		acc_op: Arc<dyn ExecOperator>,
		acc_bound: &[BindingId],
		clauses: &[&MatchClausePlan],
	) -> Result<(Arc<dyn ExecOperator>, Vec<BindingId>), Error> {
		// The OptionalExpand fast path: a single optional clause, a single
		// single-hop pattern, source already bound in the accumulator.
		if let Some(stage) = self
			.try_optional_expand_fast_path(plan, Arc::clone(&acc_op), acc_bound, clauses)
			.await?
		{
			let bound = union_bindings(acc_bound, &stage.bound);
			return Ok((stage.operator, bound));
		}

		// The LeftJoin general path: plan the whole block as one standalone
		// subplan, then left-join it onto the accumulator.
		let subplan = self.plan_optional_block(plan, clauses).await?;

		// Keys = the node bindings the block shares with the accumulator (the
		// correlation between the optional body and the rows it extends). With a
		// shared binding this is the usual correlated left-outer join. With NO shared
		// binding the block is uncorrelated: a `Left` join with empty keys pairs the
		// one empty key on every side, so each accumulator row crosses with every
		// block row when the block matched anywhere, and falls through null-filled
		// when the block is empty — the uncorrelated OPTIONAL semantics (the
		// accumulator is always preserved, R3).
		let shared = shared_bindings(acc_bound, &subplan.bound);
		let keys = binding_names(plan, &shared);

		// The block-introduced bindings (everything the subplan binds that the
		// accumulator does not already carry) are the null template: on a miss they
		// all bind `Value::Null` together, so the whole block is all-or-nothing.
		let introduced = difference_bindings(&subplan.bound, acc_bound);
		let null_template = binding_names(plan, &introduced);

		// A block predicate that correlates an OUTER binding with the block body
		// (deferred out of the subplan, since the subplan does not carry the outer
		// binding) becomes the left-join residual (the SQL `ON` condition). It
		// gates the match-vs-null-fill decision — a probe (accumulator) row with no
		// residual-passing block row null-fills the block bindings rather than
		// being dropped (R3) — exactly as the fast path's `Expand.predicate` gates
		// its zero-survivor null-fill. The deps reference one or more outer
		// bindings (a mandatory binding, or an earlier OPTIONAL's binding that may
		// itself be `Value::Null`) plus block bindings, all present in the merged
		// row. A `Null` outer operand just makes the residual UNKNOWN under 3VL ⇒
		// no match ⇒ the block null-fills, which is the correct GQL result.
		let merged_bound = union_bindings(acc_bound, &subplan.bound);
		let mut residual_exprs = Vec::with_capacity(subplan.deferred.len());
		for predicate in subplan.deferred {
			if !deps_subset(&predicate.deps, &merged_bound) {
				return Err(EngineError::Internal(
					"GQL MATCH planning: an OPTIONAL block's deferred predicate has deps \
					 unsatisfied by the accumulator and block bindings combined"
						.to_string(),
				)
				.into());
			}
			residual_exprs.push(predicate.expr);
		}
		let residual = match conjoin(residual_exprs) {
			Some(joined) => Some(self.physical_expr(joined).await?),
			None => None,
		};

		let join = Arc::new(HashJoin::new(
			subplan.operator,
			acc_op,
			keys,
			JoinType::Left,
			null_template,
			residual,
		)) as Arc<dyn ExecOperator>;

		Ok((join, merged_bound))
	}

	/// Try the OptionalExpand fast path for an `OPTIONAL` block. Returns `Some`
	/// with the extended stage when the block is a single optional clause whose
	/// single pattern is a single, non-quantified hop whose source node (its start,
	/// or — for a reverse single hop — its far node) is already bound in
	/// `acc_bound`. Returns `None` (so the caller falls back to the LeftJoin path)
	/// for anything else.
	async fn try_optional_expand_fast_path(
		&self,
		plan: &MatchPlan,
		acc_op: Arc<dyn ExecOperator>,
		acc_bound: &[BindingId],
		clauses: &[&MatchClausePlan],
	) -> Result<Option<ChainStage>, Error> {
		// A block of exactly one clause, with exactly one pattern.
		let [clause] = clauses else {
			return Ok(None);
		};
		let [pattern] = clause.patterns.as_slice() else {
			return Ok(None);
		};
		// A single, non-quantified hop, no path variable (a path var is a third
		// introduced binding `Expand` does not null-fill).
		let [(edge, far_node)] = pattern.steps.as_slice() else {
			return Ok(None);
		};
		if edge.quantifier.is_some() || pattern.path_var.is_some() {
			return Ok(None);
		}

		// The source node must already be bound in the accumulator: either the
		// start node (forward hop) or — for a single hop — the far node (reverse).
		// The fast path null-fills only the edge and the OTHER node, so the bound
		// end must be the one we expand FROM and the introduced end the one we bind.
		let forward = acc_bound.contains(&pattern.start.binding);
		let reverse = acc_bound.contains(&far_node.binding);

		// If BOTH ends are already bound, this is a correlated existence check
		// across two bound nodes, not a single-binding extension — leave it to the
		// LeftJoin path (its key set covers both ends). If NEITHER end is bound the
		// block is uncorrelated; that is the LeftJoin path too.
		let (source_node, target_node, direction) = if forward && !reverse {
			(&pattern.start, far_node, edge.direction)
		} else if reverse && !forward {
			(far_node, &pattern.start, edge.direction.reverse())
		} else {
			return Ok(None);
		};

		// Inside-predicates ride the OptionalExpand the way they ride a mandatory
		// Expand: the conjuncts whose deps the hop satisfies attach to the
		// expand's predicate slot and so evaluate pre-null (R3). Anything the hop
		// cannot satisfy on its own is a contract violation here (a single-hop
		// optional clause owns only single-hop-scoped predicates).
		let mut pending: Vec<MatchPredicate> = clause.predicates.clone();
		let source = binding_name(plan, source_node.binding).to_string();
		let dir = expand_dir(direction);
		let edge_tables = edge.label.iter().cloned().collect::<Vec<_>>();
		let target_binding = binding_name(plan, target_node.binding).to_string();
		let target_label = target_node.label.clone();

		let step_bound = {
			let mut b = acc_bound.to_vec();
			b.push(edge.binding);
			b.push(target_node.binding);
			b
		};
		let predicates = drain_satisfiable(&mut pending, &step_bound);
		if !pending.is_empty() {
			return Err(EngineError::Internal(
				"GQL MATCH planning: an OPTIONAL single-hop clause owns a predicate its hop cannot \
				 satisfy; the lowering must scope inside-optional predicates to the clause"
					.to_string(),
			).into());
		}
		let predicate = match conjoin(predicates) {
			Some(joined) => Some(self.physical_expr(joined).await?),
			None => None,
		};

		let edge_def = plan.binding(edge.binding);
		let edge_name = edge_def.name.clone();
		let edge_binding = if edge_def.user_named {
			EdgeBinding::Full(edge_name)
		} else {
			EdgeBinding::IdOnly(edge_name)
		};

		let operator = Arc::new(Expand::new(
			acc_op,
			source,
			dir,
			edge_tables,
			edge_binding,
			target_binding,
			target_label,
			predicate,
			true,
		)) as Arc<dyn ExecOperator>;

		Ok(Some(ChainStage {
			operator,
			bound: vec![edge.binding, target_node.binding],
			tip: target_node.binding,
		}))
	}

	/// Plan a whole `OPTIONAL` block (one or more clauses sharing one
	/// `optional_group`) into a single standalone subplan — the build side of the
	/// left-outer join.
	///
	/// The block's clauses fold left-to-right exactly like mandatory clauses do
	/// (each subsequent clause inner-joins, or expands off, the block's own running
	/// accumulator), so a multi-clause block is inner-joined internally and emits
	/// only fully-matched rows. Inside-predicates are owned by their clause and
	/// compile inside this subplan, so they evaluate pre-null (R3 structural). The
	/// subplan is self-contained: it never expands off the OUTER accumulator (the
	/// fast path handles the lone correlated single-hop shape; everything reaching
	/// here is self-rootable by the lowering's optional-anchorability guarantee).
	async fn plan_optional_block(
		&self,
		plan: &MatchPlan,
		clauses: &[&MatchClausePlan],
	) -> Result<PlannedClause, Error> {
		let Some((first, rest)) = clauses.split_first() else {
			return Err(EngineError::Internal(
				"GQL MATCH planning: an OPTIONAL block must carry at least one clause".to_string(),
			)
			.into());
		};

		// The block's leading clause seeds the block accumulator with NO outer
		// bindings: the subplan stands alone, so its first clause cannot share /
		// expand off the outer accumulator (a lone correlated single-hop block is
		// the fast path; a self-rootable block anchors on its own labelled element).
		let seed = self.plan_clause(plan, first, &[]).await?;
		let mut block_op = seed.operator;
		let mut block_bound = seed.bound;
		// A predicate the lowering owns to a block clause but whose deps include an
		// OUTER binding (one not bound inside the block) cannot be placed in this
		// standalone subplan; it is collected here and surfaced (via the returned
		// `PlannedClause.deferred`) for the left-join residual in `fold_optional`,
		// NOT dropped. (Previously a `debug_assert!` masked this: it panicked in
		// debug and silently dropped the predicate in release.)
		let mut block_deferred = seed.deferred;

		// Subsequent clauses of the block EXTEND the block's running sub-tree: a
		// clause with its own labeled leading element self-roots and HashJoins in;
		// a clause whose leading pattern only reuses a within-block binding expands
		// directly off the running sub-tree. Either way the clause is planned ONTO
		// the block accumulator (one connected subplan), so the whole block is a
		// single left-join unit — never one inner clause at a time.
		for clause in rest {
			let planned = self
				.plan_clause_onto(
					plan,
					clause,
					&block_bound,
					Some((Arc::clone(&block_op), block_bound.clone())),
				)
				.await?;
			block_op = planned.operator;
			block_bound = planned.bound;
			block_deferred.extend(planned.deferred);
		}

		Ok(PlannedClause {
			operator: block_op,
			bound: block_bound,
			deferred: block_deferred,
		})
	}

	/// Plan one clause: every pattern into a chain, the chains combined via
	/// `HashJoin` Inner (shared ids) / Cross (or extended directly, for a
	/// bound-variable pattern), then wrapped in `DistinctEdges` (R2) unless the
	/// clause's edge-table sets are statically disjoint.
	///
	/// `bound_acc` is the set of bindings produced by earlier clauses; a pattern
	/// may anchor by reusing one of them (the bound-variable case).
	///
	/// This plans a clause's pattern body regardless of whether the clause is
	/// optional: the left-outer semantics of an optional clause live in
	/// [`Planner::fold_optional`] (the OptionalExpand fast path) or in the
	/// `HashJoin { Left }` that wraps the block subplan, never inside the clause
	/// body itself. An optional clause's inside-predicates are clause-owned and
	/// placed inside the body here (so they evaluate pre-null, R3).
	async fn plan_clause(
		&self,
		plan: &MatchPlan,
		clause: &MatchClausePlan,
		bound_acc: &[BindingId],
	) -> Result<PlannedClause, Error> {
		self.plan_clause_onto(plan, clause, bound_acc, None).await
	}

	/// Plan one clause's pattern body, optionally EXTENDING an existing operator.
	///
	/// `seed` is `None` for a self-rooted clause (its leading pattern must
	/// node-/edge-anchor and seed the sub-tree from a scan). When `Some((op,
	/// bound))` — the way a later clause of an OPTIONAL block extends the block's
	/// running sub-tree — the leading pattern may instead expand off `op`'s rows by
	/// reusing a node already bound in `bound`, exactly like a non-leading pattern.
	/// `bound_acc` is the bindings visible from earlier clauses (the seed's bound
	/// set is folded in when a seed is present).
	async fn plan_clause_onto(
		&self,
		plan: &MatchPlan,
		clause: &MatchClausePlan,
		bound_acc: &[BindingId],
		seed: Option<(Arc<dyn ExecOperator>, Vec<BindingId>)>,
	) -> Result<PlannedClause, Error> {
		let Some((first_pattern, rest_patterns)) = clause.patterns.split_first() else {
			return Err(EngineError::Internal(
				"GQL MATCH planning: a clause must carry at least one pattern".to_string(),
			)
			.into());
		};

		// Conjuncts owned by this clause, drained as each is placed.
		let mut pending: Vec<MatchPredicate> = clause.predicates.clone();

		// Bindings visible while planning this clause: earlier clauses' bindings
		// plus everything bound so far within it.
		let mut clause_op: Arc<dyn ExecOperator>;
		let mut clause_bound: Vec<BindingId>;

		// --- First pattern seeds (or extends) the per-clause accumulator. ---
		// A self-rooted clause (`seed == None`) plans its first pattern
		// node-/edge-anchored, draining the within-pattern conjuncts it can place
		// inline (scan pushdown / Expand.predicate, per V2_DESIGN §6). When a `seed`
		// is supplied (a later clause of an OPTIONAL block extending the block's
		// running sub-tree), a first pattern with its own labeled element still
		// self-roots and HashJoins in; one with no labeled element expands off the
		// seed by reusing a node it already binds.
		match self.plan_pattern_selfrooted(plan, first_pattern, &mut pending).await? {
			Some(stage) => match &seed {
				// Self-rooted, no seed: the stage IS the clause accumulator.
				None => {
					clause_bound = stage.bound.clone();
					clause_op = stage.operator;
				}
				// Self-rooted with a seed: HashJoin the stage into the seed on the
				// node bindings they share (Cross when none shared).
				Some((seed_op, seed_bound)) => {
					let shared = shared_bindings(seed_bound, &stage.bound);
					let keys = binding_names(plan, &shared);
					let join_type = if shared.is_empty() {
						JoinType::Cross
					} else {
						JoinType::Inner
					};
					clause_op = Arc::new(HashJoin::new(
						Arc::clone(seed_op),
						stage.operator,
						keys,
						join_type,
						Vec::new(),
						None,
					)) as Arc<dyn ExecOperator>;
					clause_bound = union_bindings(seed_bound, &stage.bound);
				}
			},
			None => match &seed {
				// A leading pattern with no labeled element extends the seed by
				// reusing a node it binds (the OPTIONAL-block continuation case).
				Some((seed_op, seed_bound))
					if shared_node_anchor(plan, first_pattern, seed_bound).is_some() =>
				{
					let stage = self
						.expand_bound_pattern(
							plan,
							first_pattern,
							seed_bound,
							Arc::clone(seed_op),
							&mut pending,
						)
						.await?;
					clause_bound = union_bindings(seed_bound, &stage.bound);
					clause_op = stage.operator;
				}
				// A first pattern with no labeled element and no expandable seed.
				// The lowering's anchorability rule forbids this for the first
				// clause; in a later (non-block) clause the bound-variable case is
				// handled by the clause-combine join, so reaching here is a contract
				// violation.
				_ => {
					return Err(EngineError::Internal(
						"GQL MATCH planning: a clause's leading pattern is unanchorable (no labeled \
						 element) and no expandable accumulator was supplied; the lowering must \
						 reject it"
							.to_string(),
					).into());
				}
			},
		}
		// Any remaining within-pattern-but-not-pushed conjuncts (whole-record
		// anchor refs, multi-step joins) land as a Filter at the earliest
		// satisfiable stage.
		clause_op = self.place_satisfiable(clause_op, &clause_bound, &mut pending).await?;

		// --- Remaining patterns. ---
		// Anchor priority (V2_DESIGN §6): a pattern with a labeled element of its
		// own is planned SELF-ROOTED and HashJoin'd into the clause — even when it
		// shares a node — matching the pinned worked tree (ii), where two
		// edge-anchored patterns sharing `b` become two independent sub-trees
		// joined on `b`. Only a pattern with NO labeled element (all nodes and its
		// edge unlabeled) falls back to the bound-variable case: it expands
		// directly off the accumulator's rows (no scan, no join), reversing the
		// hop when only its far node is the shared anchor.
		for pattern in rest_patterns {
			let visible = union_bindings(bound_acc, &clause_bound);

			if let Some(stage) = self.plan_pattern_selfrooted(plan, pattern, &mut pending).await? {
				// Self-anchored pattern: HashJoin into the accumulator on the
				// node bindings they share (Cross when none shared).
				let shared = shared_bindings(&clause_bound, &stage.bound);
				let keys = binding_names(plan, &shared);
				let join_type = if shared.is_empty() {
					JoinType::Cross
				} else {
					JoinType::Inner
				};
				clause_op = Arc::new(HashJoin::new(
					clause_op,
					stage.operator,
					keys,
					join_type,
					Vec::new(),
					None,
				)) as Arc<dyn ExecOperator>;
				clause_bound = union_bindings(&clause_bound, &stage.bound);
			} else if shared_node_anchor(plan, pattern, &visible).is_some() {
				// Bound-variable pattern: expand directly off the clause
				// accumulator. Within-pattern step conjuncts ride the expansion.
				let stage = self
					.expand_bound_pattern(
						plan,
						pattern,
						&visible,
						Arc::clone(&clause_op),
						&mut pending,
					)
					.await?;
				clause_op = stage.operator;
				clause_bound = union_bindings(&clause_bound, &stage.bound);
			} else {
				return Err(EngineError::Internal(
					"GQL MATCH planning: pattern is neither self-anchorable nor reuses a bound \
					 variable (unanchorable); the lowering must reject it"
						.to_string(),
				)
				.into());
			}

			// Cross-pattern conjuncts land here, as a Filter above the join /
			// expansion that first co-binds their deps.
			clause_op = self.place_satisfiable(clause_op, &clause_bound, &mut pending).await?;
		}

		// --- DIFFERENT EDGES (R2): one DistinctEdges per MATCH statement. ---
		clause_op = self.wrap_distinct_edges(plan, clause, clause_op);

		// Conjuncts still pending after placement reference a binding this clause
		// does not bound on its own — necessarily one from an EARLIER clause (the
		// lowering only assigns a predicate to a clause whose bindings can satisfy
		// it together with earlier ones). They defer to the clause-combine join,
		// the earliest stage that binds both sides. A predicate whose deps are all
		// within `clause_bound` yet still unplaced would be a genuine contract
		// violation (it should have been placed above its binder).
		let mut deferred = Vec::new();
		for predicate in pending {
			if deps_subset(&predicate.deps, &clause_bound) {
				return Err(EngineError::Internal(
					"GQL MATCH planning: a clause predicate was not placed despite all its \
					 dependencies being bound within the clause"
						.to_string(),
				)
				.into());
			}
			deferred.push(predicate);
		}

		Ok(PlannedClause {
			operator: clause_op,
			bound: clause_bound,
			deferred,
		})
	}

	/// Plan a pattern into a self-rooted chain (one that begins with its own
	/// scan), choosing between a labeled-node anchor and a labeled-edge anchor,
	/// draining the within-pattern conjuncts it can place inline from `pending`.
	/// Returns `None` when the pattern is only anchorable by reusing a bound
	/// variable (no labeled element of its own) — the caller then expands it off
	/// the accumulator.
	async fn plan_pattern_selfrooted(
		&self,
		plan: &MatchPlan,
		pattern: &PatternPlan,
		pending: &mut Vec<MatchPredicate>,
	) -> Result<Option<ChainStage>, Error> {
		// Labeled start node ⇒ TableScan + Bind, forward steps (PR-A).
		if pattern.start.label.is_some() {
			return Ok(Some(self.plan_node_anchored(plan, pattern, pending).await?));
		}
		// Labeled edge ⇒ edge-anchored (Bind edge + EndpointBind×2).
		if let Some(stage) = self.plan_edge_anchored(plan, pattern).await? {
			return Ok(Some(stage));
		}
		// No labeled element: only a bound-variable anchor remains.
		Ok(None)
	}

	/// Node-anchored pattern (PR-A path): the start node is labeled; build the
	/// `TableScan` + `Bind` anchor and chain the steps forward. Anchor-only
	/// conjuncts are prefix-stripped into the scan predicate (a whole-record
	/// anchor ref bails to a Filter); each step's conjuncts ride that step's
	/// binder (Expand.predicate or a Filter above PathExpand), exactly as in
	/// PR-A. Cross-pattern conjuncts are left in `pending` for the caller.
	async fn plan_node_anchored(
		&self,
		plan: &MatchPlan,
		pattern: &PatternPlan,
		pending: &mut Vec<MatchPredicate>,
	) -> Result<ChainStage, Error> {
		let Some(anchor_label) = pattern.start.label.clone() else {
			return Err(EngineError::Internal(
				"GQL MATCH planning: plan_node_anchored called on an unlabeled start node"
					.to_string(),
			)
			.into());
		};
		let anchor_binding = pattern.start.binding;

		// Anchor-only conjuncts: prefix-strip the strippable ones into the scan
		// predicate; a whole-record anchor ref runs as a Filter above the scan.
		let mut anchor_pushed: Vec<Expr> = Vec::new();
		let mut anchor_filtered: Vec<Expr> = Vec::new();
		pending.retain(|predicate| {
			if predicate.deps.as_slice() == [anchor_binding] {
				match prefix_strip(&predicate.expr, binding_name(plan, anchor_binding)) {
					Some(stripped) => anchor_pushed.push(stripped),
					None => anchor_filtered.push(predicate.expr.clone()),
				}
				false
			} else {
				true
			}
		});

		let mut stage = self.plan_anchor(anchor_binding, anchor_label, plan, anchor_pushed).await?;
		stage = self.maybe_filter(stage, anchor_filtered).await?;

		// Steps: each (edge, node) becomes Expand / PathExpand. Conjuncts whose
		// deps first become satisfiable at this step ride the step's binder.
		for (edge, node) in pattern.steps.iter() {
			let step_bound = step_bindings(&stage.bound, pattern, edge, node);
			let mut step_predicates: Vec<Expr> = Vec::new();
			pending.retain(|predicate| {
				if deps_subset(&predicate.deps, &step_bound) {
					step_predicates.push(predicate.expr.clone());
					false
				} else {
					true
				}
			});
			stage = self
				.plan_step(plan, pattern, stage, edge, node, step_predicates, edge.direction, false)
				.await?;
		}
		Ok(stage)
	}

	/// Edge-anchored pattern: no labeled node but a labeled edge. Scan the edge
	/// table, `Bind` each edge record, then stack two `EndpointBind`s (the `in`
	/// endpoint, then the `out` endpoint) to recover the endpoint nodes as full
	/// node bindings. Worked tree (ii).
	///
	/// Returns `None` when the (single) step's edge is unlabeled (no edge anchor
	/// either). Only single-hop edge-anchored patterns are in PR-B scope.
	async fn plan_edge_anchored(
		&self,
		plan: &MatchPlan,
		pattern: &PatternPlan,
	) -> Result<Option<ChainStage>, Error> {
		let [(edge, far_node)] = pattern.steps.as_slice() else {
			// Multi-hop unlabeled-node patterns aren't edge-anchorable in PR-B.
			return Ok(None);
		};
		let Some(edge_label) = edge.label.clone() else {
			return Ok(None);
		};
		if edge.quantifier.is_some() {
			// A quantified edge is not an edge anchor (there is no single edge
			// record to scan). Return `None` so the caller falls through to the
			// bound-variable path (`(bound)-[:e]->{n,m}(x)` expands via
			// `PathExpand`); a standalone quantified-edge-only pattern is already
			// rejected by the lowering (`pattern_is_realisable`).
			return Ok(None);
		}

		let edge_binding = edge.binding;
		let edge_name = binding_name(plan, edge_binding).to_string();
		let scan = self.plan_table_scan(edge_label, Vec::new()).await?;
		let bind = Arc::new(Bind::new(scan, edge_name.clone())) as Arc<dyn ExecOperator>;

		// For a forward edge `(start)-[:e]->(far)`, the edge's `in` is `start` and
		// its `out` is `far`; an `In` edge swaps the roles. EndpointBind reads the
		// edge's literal `in`/`out` field, so the field follows the pattern's
		// direction, not the node's position.
		let (in_node, out_node) = match edge.direction {
			ExpandDirection::Out => (&pattern.start, far_node),
			ExpandDirection::In => (far_node, &pattern.start),
		};

		// Bottom binds `in`, top binds `out` (worked tree (ii) ordering).
		let in_bind = Arc::new(EndpointBind::new(
			bind,
			edge_name.clone(),
			EndpointField::In,
			binding_name(plan, in_node.binding).to_string(),
			in_node.label.clone(),
		)) as Arc<dyn ExecOperator>;
		let out_bind = Arc::new(EndpointBind::new(
			in_bind,
			edge_name,
			EndpointField::Out,
			binding_name(plan, out_node.binding).to_string(),
			out_node.label.clone(),
		)) as Arc<dyn ExecOperator>;

		Ok(Some(ChainStage {
			operator: out_bind,
			bound: vec![edge_binding, in_node.binding, out_node.binding],
			tip: far_node.binding,
		}))
	}

	/// Expand a bound-variable pattern off `input` (the accumulator's rows). The
	/// pattern reuses a node already bound by `input`; the chain is a sequence of
	/// `Expand` / `PathExpand` steps applied directly to `input` — no scan, no
	/// join. When only the pattern's *far* node is the shared anchor, the single
	/// hop is walked in reverse (the edge direction is flipped and the chain runs
	/// from the far node to the start). Within-pattern step conjuncts ride each
	/// step's binder; cross-pattern ones stay in `pending` for the caller.
	async fn expand_bound_pattern(
		&self,
		plan: &MatchPlan,
		pattern: &PatternPlan,
		visible: &[BindingId],
		input: Arc<dyn ExecOperator>,
		pending: &mut Vec<MatchPredicate>,
	) -> Result<ChainStage, Error> {
		// Forward: the start node is the shared anchor — expand start → far.
		if visible.contains(&pattern.start.binding) {
			let mut stage = ChainStage {
				operator: input,
				bound: visible.to_vec(),
				tip: pattern.start.binding,
			};
			for (edge, node) in pattern.steps.iter() {
				let step_bound = step_bindings(&stage.bound, pattern, edge, node);
				let step_predicates = drain_satisfiable(pending, &step_bound);
				stage = self
					.plan_step(
						plan,
						pattern,
						stage,
						edge,
						node,
						step_predicates,
						edge.direction,
						false,
					)
					.await?;
			}
			return Ok(stage);
		}

		// Reverse: only the far node of a single-hop pattern is the shared anchor
		// — expand far → start with the edge direction reversed. A quantified hop
		// here is a reverse-anchored path search: the traversal source is the far
		// node, so `reversed` tells the operator to flip the emitted path/group.
		if let [(edge, far_node)] = pattern.steps.as_slice()
			&& visible.contains(&far_node.binding)
		{
			let stage = ChainStage {
				operator: input,
				bound: visible.to_vec(),
				tip: far_node.binding,
			};
			let step_bound = step_bindings(&stage.bound, pattern, edge, &pattern.start);
			let step_predicates = drain_satisfiable(pending, &step_bound);
			return self
				.plan_step(
					plan,
					pattern,
					stage,
					edge,
					&pattern.start,
					step_predicates,
					edge.direction.reverse(),
					true,
				)
				.await;
		}

		Err(EngineError::Internal(
			"GQL MATCH planning: bound-variable expansion found no shared anchor node \
			 (multi-hop reverse anchoring is out of PR-B scope)"
				.to_string(),
		)
		.into())
	}

	/// Place every pending predicate whose deps `bound` now satisfies as a
	/// `Filter` above `op` (the earliest stage where their deps are all bound).
	/// Drains the placed predicates from `pending`.
	async fn place_satisfiable(
		&self,
		op: Arc<dyn ExecOperator>,
		bound: &[BindingId],
		pending: &mut Vec<MatchPredicate>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let mut ready: Vec<Expr> = Vec::new();
		pending.retain(|predicate| {
			if deps_subset(&predicate.deps, bound) {
				ready.push(predicate.expr.clone());
				false
			} else {
				true
			}
		});
		self.filter_above(op, ready).await
	}

	/// Place a set of predicates as a single `Filter` above `op` (used for
	/// deferred clause predicates at the clause-combine join). Errors if any
	/// dep is unsatisfied at this stage.
	async fn place_above(
		&self,
		op: Arc<dyn ExecOperator>,
		bound: &[BindingId],
		predicates: Vec<MatchPredicate>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let mut exprs: Vec<Expr> = Vec::with_capacity(predicates.len());
		for predicate in predicates {
			if !deps_subset(&predicate.deps, bound) {
				return Err(EngineError::Internal(
					"GQL MATCH planning: a deferred clause predicate's deps are unsatisfied at \
					 the clause-combine join"
						.to_string(),
				)
				.into());
			}
			exprs.push(predicate.expr);
		}
		self.filter_above(op, exprs).await
	}

	/// Wrap a clause's combined plan in `DistinctEdges` (R2) when it has ≥2
	/// edge-ish bindings whose edge-table sets are not statically disjoint. The
	/// disjoint-label skip (worked tree (ii): `x` and `y` differ ⇒ no DistinctEdges)
	/// is implemented here.
	fn wrap_distinct_edges(
		&self,
		plan: &MatchPlan,
		clause: &MatchClausePlan,
		op: Arc<dyn ExecOperator>,
	) -> Arc<dyn ExecOperator> {
		let edges = clause_edge_bindings(plan, clause);
		if edges.len() < 2 {
			return op;
		}
		if edge_tables_pairwise_disjoint(&edges) {
			return op;
		}
		let names = edges.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
		Arc::new(DistinctEdges::new(op, names)) as Arc<dyn ExecOperator>
	}

	/// Build the anchor stage: `TableScan` (via the SELECT source machinery, so
	/// permissions / computed fields / pre-decode filter and the pushed
	/// predicate all apply) wrapped in `Bind`.
	async fn plan_anchor(
		&self,
		binding: BindingId,
		label: TableName,
		plan: &MatchPlan,
		pushed: Vec<Expr>,
	) -> Result<ChainStage, Error> {
		let scan = self.plan_table_scan(label, pushed).await?;
		let name = binding_name(plan, binding).to_string();
		let operator = Arc::new(Bind::new(scan, name)) as Arc<dyn ExecOperator>;
		Ok(ChainStage {
			operator,
			bound: vec![binding],
			tip: binding,
		})
	}

	/// Plan a `TableScan` over `label`, folding the prefix-stripped `pushed`
	/// conjuncts into the scan WHERE (the shape SELECT pushdown expects) and
	/// applying any residual as a Filter directly above the scan.
	async fn plan_table_scan(
		&self,
		label: TableName,
		pushed: Vec<Expr>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let cond = conjoin(pushed).map(Cond);
		let scan_predicate = match cond.as_ref() {
			Some(c) => Some(self.physical_expr(c.0.clone()).await?),
			None => None,
		};
		let planned = self
			.plan_source(
				Expr::Table(label),
				None,
				cond.as_ref(),
				None,
				None,
				None,
				scan_predicate,
				None,
				None,
				false,
				&super::select::TopKPushdownRequest::NotApplicable,
			)
			.await?;
		self.apply_source_filter(planned).await
	}

	/// Build one step stage from the previous stage's rows: `Expand` for an
	/// unquantified edge, `PathExpand` for a quantified one. `predicates` are the
	/// conjuncts whose deps first become satisfied at this step. An `Expand`
	/// carries them in its predicate slot; a `PathExpand` (no predicate slot)
	/// gets them as a `Filter` above the step. `direction` is passed explicitly
	/// so a reverse-anchored bound-variable chain can flip the arrow; `reversed`
	/// is `true` for that reverse-anchored case so a quantified step's path-search
	/// operator emits its path/group in the pattern's written order.
	#[allow(clippy::too_many_arguments)]
	async fn plan_step(
		&self,
		plan: &MatchPlan,
		pattern: &PatternPlan,
		prev: ChainStage,
		edge: &EdgeStep,
		node: &NodeStep,
		predicates: Vec<Expr>,
		direction: ExpandDirection,
		reversed: bool,
	) -> Result<ChainStage, Error> {
		let source = binding_name(plan, prev.tip).to_string();
		let dir = expand_dir(direction);
		let edge_tables = edge.label.iter().cloned().collect::<Vec<_>>();
		let target_binding = binding_name(plan, node.binding).to_string();
		let target_label = node.label.clone();

		let mut bound = prev.bound.clone();
		bound.push(edge.binding);
		bound.push(node.binding);

		match edge.quantifier {
			None => {
				let edge_def = plan.binding(edge.binding);
				let edge_name = edge_def.name.clone();
				let edge_binding = if edge_def.user_named {
					EdgeBinding::Full(edge_name)
				} else {
					EdgeBinding::IdOnly(edge_name)
				};
				let predicate = match conjoin(predicates) {
					Some(joined) => Some(self.physical_expr(joined).await?),
					None => None,
				};
				let operator = Arc::new(Expand::new(
					prev.operator,
					source,
					dir,
					edge_tables,
					edge_binding,
					target_binding,
					target_label,
					predicate,
					false,
				)) as Arc<dyn ExecOperator>;
				Ok(ChainStage {
					operator,
					bound,
					tip: node.binding,
				})
			}
			Some(quantifier) => {
				// Always bind the group under its name — including an anonymous
				// `__e<n>` — so the per-MATCH DIFFERENT EDGES check (R2,
				// `DistinctEdges`) can read its edge ids. Mirrors the unquantified
				// branch, which binds hidden edges as `EdgeBinding::IdOnly`. The
				// hidden name never surfaces: `Project` emits only explicit columns
				// and `RETURN *` lists user-named bindings, so a `__e<n>` group is
				// dropped before output.
				let group_binding = Some(binding_name(plan, edge.binding).to_string());
				let path_binding = pattern.path_var.map(|id| binding_name(plan, id).to_string());

				// Path-search routing (V2_DESIGN path-search): `None`/`ALL` → every
				// path (the `PathExpand` DFS); `ANY [k]` and the SHORTEST family →
				// the bounded `ShortestPathExpand` BFS.
				let (exec_mode, routing) = resolve_path_search(pattern.search);
				// A reverse-anchored selective search (`reversed`) expands from the
				// pattern's far node; per-endpoint grouping and path length are
				// symmetric so selection is unaffected, and the operator flips the
				// emitted path/group back to written order. A forward anchor has
				// `source == pattern.start`.
				debug_assert!(
					reversed || source == binding_name(plan, pattern.start.binding),
					"a forward-anchored quantified step must expand from the pattern's start",
				);
				let operator: Arc<dyn ExecOperator> = match routing {
					SearchRouting::Every => Arc::new(PathExpand::new(
						prev.operator,
						source,
						dir,
						edge_tables,
						quantifier.min,
						quantifier.max,
						target_binding,
						target_label,
						group_binding,
						path_binding,
						exec_mode,
						reversed,
					)) as Arc<dyn ExecOperator>,
					SearchRouting::Shortest {
						selector,
					} => Arc::new(ShortestPathExpand::new(
						prev.operator,
						source,
						dir,
						edge_tables,
						quantifier.min,
						quantifier.max,
						target_binding,
						target_label,
						group_binding,
						path_binding,
						exec_mode,
						selector,
						reversed,
					)) as Arc<dyn ExecOperator>,
				};
				if let Some(id) = pattern.path_var {
					bound.push(id);
				}
				self.maybe_filter(
					ChainStage {
						operator,
						bound,
						tip: node.binding,
					},
					predicates,
				)
				.await
			}
		}
	}

	/// Wrap a stage in a `Filter` for the conjoined `exprs`, or pass the stage
	/// through unchanged when there are none. Binding set and tip are preserved
	/// (a Filter neither adds nor drops bindings).
	async fn maybe_filter(&self, stage: ChainStage, exprs: Vec<Expr>) -> Result<ChainStage, Error> {
		let Some(joined) = conjoin(exprs) else {
			return Ok(stage);
		};
		let predicate = self.physical_expr(joined).await?;
		let operator = Arc::new(Filter::new(stage.operator, predicate)) as Arc<dyn ExecOperator>;
		Ok(ChainStage {
			operator,
			bound: stage.bound,
			tip: stage.tip,
		})
	}

	/// Wrap an operator in a `Filter` for the conjoined `exprs`, or pass through
	/// when there are none (operator-level variant for join placement).
	async fn filter_above(
		&self,
		op: Arc<dyn ExecOperator>,
		exprs: Vec<Expr>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let Some(joined) = conjoin(exprs) else {
			return Ok(op);
		};
		let predicate = self.physical_expr(joined).await?;
		Ok(Arc::new(Filter::new(op, predicate)) as Arc<dyn ExecOperator>)
	}

	/// Build the output tail over the binder-chain body.
	///
	/// - Aggregating (`group_by.is_some()`) ⇒ Aggregate → [Distinct] → Sort → Limit → [drop
	///   hidden]. The `Aggregate` operator emits the final projected objects (keyed by output
	///   column name), so it stands in for the Project; the lowering resolved ORDER BY to those
	///   output columns (materialising hidden sort-only columns for non-projected keys), so the
	///   Sort runs over them, and a trailing Project drops any hidden column.
	/// - DISTINCT ⇒ Project → Distinct → Sort → Limit.
	/// - Plain ⇒ Sort → Limit → Project.
	async fn plan_match_tail(
		&self,
		plan: &MatchPlan,
		body: Arc<dyn ExecOperator>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		// A mutation-only plan (no `RETURN`) carries no output spec; drive the
		// mutation chain to completion (the writes happen as its rows are pulled)
		// and emit no rows, so the query returns an empty result.
		let Some(output) = plan.output.as_ref() else {
			return Ok(Arc::new(DrainSink::new(body)) as Arc<dyn ExecOperator>);
		};
		if let Some(group_keys) = output.group_by.as_ref() {
			let mut op = self.plan_match_aggregate(body, output, group_keys).await?;
			if output.distinct {
				op = Arc::new(Distinct::new(op)) as Arc<dyn ExecOperator>;
			}
			op = self.plan_match_sort(op, output).await?;
			op = self.plan_match_limit(op, output).await?;
			// Drop any sort-only (hidden) columns the lowering materialised for a
			// non-projected ORDER BY key.
			if output.columns.iter().any(|c| c.hidden) {
				op = self.plan_match_drop_hidden(op, output).await?;
			}
			Ok(op)
		} else if output.distinct {
			let mut op = self.plan_match_project(body, output).await?;
			op = Arc::new(Distinct::new(op)) as Arc<dyn ExecOperator>;
			op = self.plan_match_sort(op, output).await?;
			op = self.plan_match_limit(op, output).await?;
			Ok(op)
		} else {
			let mut op = self.plan_match_sort(body, output).await?;
			op = self.plan_match_limit(op, output).await?;
			op = self.plan_match_project(op, output).await?;
			Ok(op)
		}
	}

	/// Build the `Aggregate` operator that folds the binding rows by the GROUP BY
	/// keys (empty keys ⇒ GROUP ALL, a single group over every row). Classifies
	/// each column (projected or hidden sort-only) three ways:
	/// - exactly a grouping key ⇒ pass it through (`is_group_key`);
	/// - contains an aggregate ⇒ [`Planner::extract_aggregate_info`] (the column carries the fold,
	///   so the helper's implicit `array::group` fallback never fires);
	/// - otherwise it is determined by the grouping keys (the lowering guarantees coverage) ⇒ emit
	///   its first value per group via `fallback_expr`, which is exact because the value is
	///   constant within a group.
	async fn plan_match_aggregate(
		&self,
		input: Arc<dyn ExecOperator>,
		output: &MatchOutput,
		group_keys: &[Expr],
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use surrealdb_types::ToSql;

		// Physical group-key expressions (empty for GROUP ALL — the operator's
		// `group_by_exprs.is_empty()` check then takes the single-group path).
		let mut group_by_exprs = Vec::with_capacity(group_keys.len());
		for key in group_keys {
			group_by_exprs.push(self.physical_expr(key.clone()).await?);
		}
		// Display idioms for EXPLAIN, kept length-aligned with the keys so the
		// operator renders `GROUP ALL` only when there are genuinely no keys.
		let group_by_idioms: Vec<Idiom> = group_keys
			.iter()
			.map(|key| match key {
				Expr::Idiom(idiom) => idiom.clone(),
				other => Idiom::field(other.to_sql()),
			})
			.collect();

		let mut aggregates = Vec::with_capacity(output.columns.len());
		for column in output.columns.iter() {
			if let Some(idx) = group_keys.iter().position(|k| *k == column.expr) {
				// A grouping key column: passed through from the group key vector.
				aggregates.push(AggregateField::new(
					column.name.clone(),
					true,
					Some(idx),
					None,
					None,
				));
			} else if expr_has_aggregate(self.function_registry(), &column.expr) {
				let (info, fallback) = self.extract_aggregate_info(column.expr.clone()).await?;
				aggregates.push(AggregateField::new(
					column.name.clone(),
					false,
					None,
					info,
					fallback,
				));
			} else {
				// Determined by the grouping keys (constant within each group): emit
				// the first value seen. The lowering guarantees coverage.
				let fallback = self.physical_expr(column.expr.clone()).await?;
				aggregates.push(AggregateField::new(
					column.name.clone(),
					false,
					None,
					None,
					Some(fallback),
				));
			}
		}

		Ok(Arc::new(Aggregate::new(input, group_by_idioms, group_by_exprs, aggregates))
			as Arc<dyn ExecOperator>)
	}

	/// Project away the hidden sort-only columns after an aggregating Sort,
	/// keeping only the user-projected columns (selected by output name, since the
	/// `Aggregate` already produced each under its name). Only built when the plan
	/// actually has hidden columns.
	async fn plan_match_drop_hidden(
		&self,
		input: Arc<dyn ExecOperator>,
		output: &MatchOutput,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let mut fields = Vec::new();
		for column in output.columns.iter().filter(|c| !c.hidden) {
			let expr = self.physical_expr(Expr::Idiom(Idiom::field(column.name.clone()))).await?;
			fields.push(FieldSelection::new(&column.name, expr));
		}
		Ok(Arc::new(Project::new(input, fields, Vec::new(), false)) as Arc<dyn ExecOperator>)
	}

	/// Build the `Sort` operator from `MatchOutput::order`, or pass through when
	/// there is no ORDER BY.
	async fn plan_match_sort(
		&self,
		input: Arc<dyn ExecOperator>,
		output: &MatchOutput,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		if output.order.is_empty() {
			return Ok(input);
		}
		let mut order_by = Vec::with_capacity(output.order.len());
		for order in output.order.iter() {
			let expr = self.physical_expr(order.expr.clone()).await?;
			order_by.push(OrderByField {
				expr,
				direction: if order.ascending {
					SortDirection::Asc
				} else {
					SortDirection::Desc
				},
				collate: false,
				numeric: false,
			});
		}
		Ok(Arc::new(Sort::new(input, order_by)) as Arc<dyn ExecOperator>)
	}

	/// Build the `Limit` operator from `MatchOutput::skip`/`limit`, or pass
	/// through when neither is present.
	async fn plan_match_limit(
		&self,
		input: Arc<dyn ExecOperator>,
		output: &MatchOutput,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let limit = match output.limit.as_ref() {
			Some(e) => Some(self.physical_expr(e.clone()).await?),
			None => None,
		};
		let offset = match output.skip.as_ref() {
			Some(e) => Some(self.physical_expr(e.clone()).await?),
			None => None,
		};
		if limit.is_none() && offset.is_none() {
			return Ok(input);
		}
		Ok(Arc::new(Limit::new(input, limit, offset)) as Arc<dyn ExecOperator>)
	}

	/// Build the `Project` operator from the IR's explicit columns. Column names
	/// are final (the lowering applied naming + dup-check), so each column is a
	/// flat `FieldSelection` aliased to its name.
	async fn plan_match_project(
		&self,
		input: Arc<dyn ExecOperator>,
		output: &MatchOutput,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let mut fields = Vec::with_capacity(output.columns.len());
		for column in output.columns.iter() {
			let expr = self.physical_expr(column.expr.clone()).await?;
			fields.push(FieldSelection::new(&column.name, expr));
		}
		Ok(Arc::new(Project::new(input, fields, Vec::new(), false)) as Arc<dyn ExecOperator>)
	}

	/// Resolve a `PlannedSource`'s residual WHERE into a Filter directly above
	/// the scan, mirroring the SELECT pipeline's `FilterAction` handling.
	async fn apply_source_filter(
		&self,
		planned: super::select::PlannedSource,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use super::select::FilterAction;
		match planned.filter_action {
			FilterAction::FullyConsumed => Ok(planned.operator),
			FilterAction::UseOriginal => Ok(planned.operator),
			FilterAction::Residual(residual) => {
				let predicate = self.physical_expr(residual.0).await?;
				Ok(Arc::new(Filter::new(planned.operator, predicate)) as Arc<dyn ExecOperator>)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// The node bindings two binding sets share, in `a`'s order. Only *node*
/// bindings join (edge/group/path reuse is rejected by the lowering), so the
/// id intersection is the join key.
fn shared_bindings(a: &[BindingId], b: &[BindingId]) -> Vec<BindingId> {
	a.iter().copied().filter(|id| b.contains(id)).collect()
}

/// Drain every predicate in `pending` whose deps `bound` now satisfies and
/// return their exprs (for inline placement on the just-built binder). Mirrors
/// the `retain`-based draining used during chain construction.
fn drain_satisfiable(pending: &mut Vec<MatchPredicate>, bound: &[BindingId]) -> Vec<Expr> {
	let mut ready = Vec::new();
	pending.retain(|predicate| {
		if deps_subset(&predicate.deps, bound) {
			ready.push(predicate.expr.clone());
			false
		} else {
			true
		}
	});
	ready
}

/// The bindings a step introduces, given the bindings already in scope: prior
/// bindings ∪ {edge, node} ∪ optional path var. Used to decide which conjuncts
/// become satisfiable at a step BEFORE the step operator is built (so an Expand
/// can carry its predicate). Mirrors the binding growth in `plan_step`.
fn step_bindings(
	prior: &[BindingId],
	pattern: &PatternPlan,
	edge: &EdgeStep,
	node: &NodeStep,
) -> Vec<BindingId> {
	let mut bound = prior.to_vec();
	bound.push(edge.binding);
	bound.push(node.binding);
	if edge.quantifier.is_some()
		&& let Some(id) = pattern.path_var
	{
		bound.push(id);
	}
	bound
}

/// Whether an expression contains a call to a registered aggregate function.
/// Used to classify an aggregating query's columns: a non-key column with an
/// aggregate folds via `extract_aggregate_info`, one without is a value
/// determined by the grouping keys (first-value). Walks on an explicit stack.
fn expr_has_aggregate(registry: &crate::exec::function::FunctionRegistry, expr: &Expr) -> bool {
	let mut stack = vec![expr];
	while let Some(e) = stack.pop() {
		match e {
			Expr::FunctionCall(call) => {
				if let Function::Normal(name) = &call.receiver
					&& registry.get_aggregate(name.as_str()).is_some()
				{
					return true;
				}
				stack.extend(call.arguments.iter());
			}
			Expr::Binary {
				left,
				right,
				..
			} => {
				stack.push(left);
				stack.push(right);
			}
			Expr::Prefix {
				expr,
				..
			}
			| Expr::Postfix {
				expr,
				..
			} => stack.push(expr),
			Expr::Literal(Literal::Array(items)) => stack.extend(items.iter()),
			Expr::Literal(Literal::Object(entries)) => {
				stack.extend(entries.iter().map(|entry| &entry.value));
			}
			_ => {}
		}
	}
	false
}

/// The union of two binding sets, preserving `a`'s order then appending the new
/// ids from `b`.
fn union_bindings(a: &[BindingId], b: &[BindingId]) -> Vec<BindingId> {
	let mut out = a.to_vec();
	for id in b {
		if !out.contains(id) {
			out.push(*id);
		}
	}
	out
}

/// The bindings in `a` that are not in `b`, in `a`'s order — the bindings a
/// clause / block *introduces* relative to a prior binding set. Drives a left
/// join's `null_template` (the introduced bindings nulled on a miss).
fn difference_bindings(a: &[BindingId], b: &[BindingId]) -> Vec<BindingId> {
	a.iter().copied().filter(|id| !b.contains(id)).collect()
}

/// One fold unit: a single mandatory clause, or a whole `OPTIONAL` block (the run
/// of consecutive clauses sharing one `optional_group` id, left-joined as a unit,
/// R3).
enum StageUnit<'p> {
	Mandatory(&'p MatchClausePlan),
	Optional(Vec<&'p MatchClausePlan>),
	Mutation(&'p MutationStage),
}

/// Group a plan's stages (textual order) into fold units: each mandatory read
/// clause is its own unit; consecutive read clauses sharing one `optional_group`
/// id collapse into one [`StageUnit::Optional`] (the all-or-nothing block); each
/// write stage is a [`StageUnit::Mutation`] and breaks any optional run. The
/// lowering mints block ids densely in textual order and an `OPTIONAL` block
/// never contains a mutation, so a block's clauses are always adjacent and a
/// simple run-grouping recovers the blocks exactly.
fn stage_units(plan: &MatchPlan) -> Vec<StageUnit<'_>> {
	let mut units: Vec<StageUnit<'_>> = Vec::new();
	let mut current_group: Option<u32> = None;
	for stage in plan.stages.iter() {
		match stage {
			MatchStage::Read(clause) => match clause.optional_group {
				Some(group) => {
					if current_group == Some(group)
						&& let Some(StageUnit::Optional(block)) = units.last_mut()
					{
						// Same block as the previous clause: extend it.
						block.push(clause);
					} else {
						// A new optional block.
						units.push(StageUnit::Optional(vec![clause]));
						current_group = Some(group);
					}
				}
				None => {
					units.push(StageUnit::Mandatory(clause));
					current_group = None;
				}
			},
			MatchStage::Mutate(mutation) => {
				units.push(StageUnit::Mutation(mutation));
				current_group = None;
			}
		}
	}
	units
}

/// The shared node binding a pattern would anchor on, if any: either its start
/// node or (single-hop only) its far node is already in `visible`. Returns the
/// shared binding id, or `None` when the pattern shares no node with `visible`
/// (so it must be self-anchored).
fn shared_node_anchor(
	_plan: &MatchPlan,
	pattern: &PatternPlan,
	visible: &[BindingId],
) -> Option<BindingId> {
	if visible.contains(&pattern.start.binding) {
		return Some(pattern.start.binding);
	}
	if let [(_, far)] = pattern.steps.as_slice()
		&& visible.contains(&far.binding)
	{
		return Some(far.binding);
	}
	None
}

/// The clause's edge-ish bindings (single edges and group variables), each with
/// its statically-known table set, in textual order. Drives DistinctEdges
/// insertion and the disjoint-label skip.
fn clause_edge_bindings(plan: &MatchPlan, clause: &MatchClausePlan) -> Vec<(String, EdgeTables)> {
	let mut edges = Vec::new();
	for pattern in clause.patterns.iter() {
		for (edge, _) in pattern.steps.iter() {
			let def = plan.binding(edge.binding);
			if matches!(def.kind, BindingKind::Edge | BindingKind::EdgeGroup) {
				let tables = match edge.label.as_ref() {
					Some(label) => EdgeTables::Known(label.clone()),
					None => EdgeTables::Any,
				};
				edges.push((def.name.clone(), tables));
			}
		}
	}
	edges
}

/// The statically-known table set of an edge binding, for the disjoint-label
/// skip.
#[derive(Clone)]
enum EdgeTables {
	/// A single known edge table (labeled edge).
	Known(TableName),
	/// Any table (unlabeled edge); never disjoint from anything.
	Any,
}

impl EdgeTables {
	/// `true` if the two table sets can never overlap (so two edges drawn from
	/// them can never share an id). [`EdgeTables::Any`] overlaps everything, so
	/// it is never disjoint.
	fn disjoint_from(&self, other: &EdgeTables) -> bool {
		match (self, other) {
			(EdgeTables::Known(a), EdgeTables::Known(b)) => a != b,
			_ => false,
		}
	}
}

/// `true` if every pair of edge bindings has a statically-disjoint table set
/// (so no two can ever share an id and R2 holds without `DistinctEdges`).
fn edge_tables_pairwise_disjoint(edges: &[(String, EdgeTables)]) -> bool {
	for i in 0..edges.len() {
		for j in (i + 1)..edges.len() {
			if !edges[i].1.disjoint_from(&edges[j].1) {
				return false;
			}
		}
	}
	true
}

/// The names of a set of bindings, in the given id order (deterministic
/// join-key rendering for EXPLAIN).
fn binding_names(plan: &MatchPlan, ids: &[BindingId]) -> Vec<String> {
	ids.iter().map(|id| binding_name(plan, *id).to_string()).collect()
}

/// The name of a binding by id. The lowering guarantees every id used in a
/// well-formed plan is in range, so this is panic-free on any plan that reached
/// the planner.
fn binding_name(plan: &MatchPlan, id: BindingId) -> &str {
	plan.binding(id).name.as_str()
}

/// Map the IR's [`ExpandDirection`] to the operator-local [`ExpandDir`].
fn expand_dir(direction: ExpandDirection) -> ExpandDir {
	match direction {
		ExpandDirection::Out => ExpandDir::Out,
		ExpandDirection::In => ExpandDir::In,
	}
}

/// `true` if every dependency in `deps` is present in `bound`.
fn deps_subset(deps: &[BindingId], bound: &[BindingId]) -> bool {
	deps.iter().all(|d| bound.contains(d))
}

/// Conjoin a list of expressions with `AND`, or `None` for an empty list.
fn conjoin(mut exprs: Vec<Expr>) -> Option<Expr> {
	let mut acc = exprs.pop()?;
	while let Some(next) = exprs.pop() {
		acc = Expr::Binary {
			left: Box::new(next),
			op: crate::expr::BinaryOperator::And,
			right: Box::new(acc),
		};
	}
	Some(acc)
}

/// Rewrite a binding-row-scoped expression into a record-row-scoped one for
/// scan pushdown: every idiom `Idiom[Field(binding), rest..]` becomes
/// `Idiom[rest..]`. Returns `None` — bailing to a Filter — if any bare
/// whole-record reference `Idiom[Field(binding)]` survives.
fn prefix_strip(expr: &Expr, binding: &str) -> Option<Expr> {
	match expr {
		Expr::Idiom(idiom) => {
			let parts = idiom.0.as_slice();
			match parts {
				[Part::Field(field)] if field.as_str() == binding => None,
				[Part::Field(field), rest @ ..] if field.as_str() == binding => {
					Some(Expr::Idiom(Idiom(rest.to_vec())))
				}
				_ => Some(expr.clone()),
			}
		}
		Expr::Binary {
			left,
			op,
			right,
		} => Some(Expr::Binary {
			left: Box::new(prefix_strip(left, binding)?),
			op: op.clone(),
			right: Box::new(prefix_strip(right, binding)?),
		}),
		Expr::Prefix {
			op,
			expr,
		} => Some(Expr::Prefix {
			op: op.clone(),
			expr: Box::new(prefix_strip(expr, binding)?),
		}),
		Expr::Postfix {
			op,
			expr,
		} => Some(Expr::Postfix {
			op: op.clone(),
			expr: Box::new(prefix_strip(expr, binding)?),
		}),
		other => Some(other.clone()),
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::*;
	use crate::ctx::Context;
	use crate::expr::match_plan::{
		BindingDef, BindingKind, EdgeQuantifier, MatchColumn, MatchOrder, MatchOutput,
	};
	use crate::expr::{BinaryOperator, Literal};
	use crate::kvs::{Datastore, TransactionType};

	// ---- shared builders ----

	/// Wrap read clauses as the plan's [`MatchStage::Read`] steps (these planner
	/// builders are all read-only).
	fn stages_of(clauses: Vec<MatchClausePlan>) -> Vec<MatchStage> {
		clauses.into_iter().map(MatchStage::Read).collect()
	}

	/// Mutable access to the read clause at step `i` (panics if it is not a read).
	fn clause_mut(plan: &mut MatchPlan, i: usize) -> &mut MatchClausePlan {
		match &mut plan.stages[i] {
			MatchStage::Read(c) => c,
			MatchStage::Mutate(_) => panic!("stage {i} is not a read clause"),
		}
	}

	fn binding(name: &str, kind: BindingKind, user_named: bool) -> BindingDef {
		BindingDef {
			name: name.to_string(),
			kind,
			user_named,
		}
	}

	fn node(name: &str) -> BindingDef {
		binding(name, BindingKind::Node, true)
	}

	fn edge(name: &str) -> BindingDef {
		binding(name, BindingKind::Edge, true)
	}

	fn hidden_edge(name: &str) -> BindingDef {
		binding(name, BindingKind::Edge, false)
	}

	/// `a.field` as a binding-row scoped idiom.
	fn field_path(b: &str, field: &str) -> Expr {
		Expr::Idiom(Idiom(vec![
			Part::Field(b.to_string().into()),
			Part::Field(field.to_string().into()),
		]))
	}

	/// `RETURN b` — a bare-binding column expression.
	fn var(b: &str) -> Expr {
		Expr::Idiom(Idiom(vec![Part::Field(b.to_string().into())]))
	}

	fn col(name: &str, expr: Expr) -> MatchColumn {
		MatchColumn {
			name: name.to_string(),
			expr,
			hidden: false,
		}
	}

	fn tbl(name: &str) -> TableName {
		TableName::new(name.to_string())
	}

	fn nodestep(b: BindingId, label: Option<&str>) -> NodeStep {
		NodeStep {
			binding: b,
			label: label.map(tbl),
		}
	}

	fn edgestep(b: BindingId, label: Option<&str>, dir: ExpandDirection) -> EdgeStep {
		EdgeStep {
			binding: b,
			label: label.map(tbl),
			direction: dir,
			quantifier: None,
		}
	}

	fn out_columns(cols: Vec<MatchColumn>) -> Option<MatchOutput> {
		Some(MatchOutput {
			columns: cols,
			distinct: false,
			group_by: None,
			order: Vec::new(),
			skip: None,
			limit: None,
		})
	}

	// ---- worked tree (i): single pattern, edge predicate ----

	fn plan_tree_i() -> MatchPlan {
		MatchPlan {
			bindings: vec![node("a"), edge("k"), node("b")],
			stages: stages_of(vec![MatchClausePlan {
				optional_group: None,
				patterns: vec![PatternPlan {
					path_var: None,
					search: None,
					start: nodestep(0, Some("person")),
					steps: vec![(
						edgestep(1, Some("knows"), ExpandDirection::Out),
						nodestep(2, Some("person")),
					)],
				}],
				predicates: vec![MatchPredicate {
					expr: Expr::Binary {
						left: Box::new(field_path("k", "since")),
						op: BinaryOperator::MoreThan,
						right: Box::new(Expr::Literal(Literal::Integer(2020))),
					},
					deps: vec![1],
				}],
			}]),
			output: out_columns(vec![
				col("a_name", field_path("a", "name")),
				col("b_name", field_path("b", "name")),
			]),
		}
	}

	// ---- worked tree (iv): quantified path + ORDER BY ----

	fn plan_tree_iv() -> MatchPlan {
		MatchPlan {
			bindings: vec![
				node("a"),
				binding("__e0", BindingKind::EdgeGroup, false),
				node("b"),
				node("p"),
			],
			stages: stages_of(vec![MatchClausePlan {
				optional_group: None,
				patterns: vec![PatternPlan {
					path_var: Some(3),
					search: None,
					start: nodestep(0, Some("person")),
					steps: vec![(
						EdgeStep {
							binding: 1,
							label: Some(tbl("knows")),
							direction: ExpandDirection::Out,
							quantifier: Some(EdgeQuantifier {
								min: 1,
								max: Some(3),
							}),
						},
						nodestep(2, Some("person")),
					)],
				}],
				predicates: Vec::new(),
			}]),
			output: Some(MatchOutput {
				columns: vec![col("p", var("p")), col("b", var("b"))],
				distinct: false,
				group_by: None,
				order: vec![MatchOrder {
					expr: field_path("a", "age"),
					ascending: true,
				}],
				skip: None,
				limit: None,
			}),
		}
	}

	// ---- worked tree (ii): edge-anchored multi-pattern, HashJoin on b ----
	// `MATCH (a)-[:x]->(b), (c)-[:y]->(b) RETURN a, c`
	fn plan_tree_ii() -> MatchPlan {
		MatchPlan {
			// 0=a, 1=__e0, 2=b, 3=c, 4=__e1
			bindings: vec![
				node("a"),
				hidden_edge("__e0"),
				node("b"),
				node("c"),
				hidden_edge("__e1"),
			],
			stages: stages_of(vec![MatchClausePlan {
				optional_group: None,
				patterns: vec![
					// (a)-[:x]->(b)
					PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, None),
						steps: vec![(
							edgestep(1, Some("x"), ExpandDirection::Out),
							nodestep(2, None),
						)],
					},
					// (c)-[:y]->(b)
					PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(3, None),
						steps: vec![(
							edgestep(4, Some("y"), ExpandDirection::Out),
							nodestep(2, None),
						)],
					},
				],
				predicates: Vec::new(),
			}]),
			output: out_columns(vec![col("a", var("a")), col("c", var("c"))]),
		}
	}

	// ---- shared-node multi-pattern, same edge table (DistinctEdges kept) ----
	// `MATCH (a:person)-[k:knows]->(b:person), (c:person)-[k2:knows]->(b) RETURN a, c`
	fn plan_shared_node_same_edge() -> MatchPlan {
		MatchPlan {
			// 0=a, 1=k, 2=b, 3=c, 4=k2
			bindings: vec![node("a"), edge("k"), node("b"), node("c"), edge("k2")],
			stages: stages_of(vec![MatchClausePlan {
				optional_group: None,
				patterns: vec![
					PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, Some("person")),
						steps: vec![(
							edgestep(1, Some("knows"), ExpandDirection::Out),
							nodestep(2, Some("person")),
						)],
					},
					PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(3, Some("person")),
						steps: vec![(
							edgestep(4, Some("knows"), ExpandDirection::Out),
							nodestep(2, None),
						)],
					},
				],
				predicates: Vec::new(),
			}]),
			output: out_columns(vec![col("a", var("a")), col("c", var("c"))]),
		}
	}

	// ---- anonymous quantified group + a second edge sharing the table ----
	// `MATCH (a:person)-[:knows]->{1,1}(b:person), (a)-[k2:knows]->(c:person) RETURN b, c`
	// The first pattern's quantified edge is ANONYMOUS (hidden `__e0`,
	// EdgeGroup). R2 still applies across the group and `k2` (same `knows`
	// table), so the planner must (a) bind the hidden group on the row via
	// `PathExpand`'s `group:` slot and (b) wrap the clause in DistinctEdges over
	// `{__e0, k2}`. Pins the regression for the anonymous-group R2 skip.
	fn plan_anon_group_second_edge() -> MatchPlan {
		MatchPlan {
			// 0=a, 1=__e0 (hidden EdgeGroup), 2=b, 3=k2, 4=c
			bindings: vec![
				node("a"),
				binding("__e0", BindingKind::EdgeGroup, false),
				node("b"),
				edge("k2"),
				node("c"),
			],
			stages: stages_of(vec![MatchClausePlan {
				optional_group: None,
				patterns: vec![
					// (a:person)-[__e0:knows]->{1,1}(b:person)
					PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, Some("person")),
						steps: vec![(
							EdgeStep {
								binding: 1,
								label: Some(tbl("knows")),
								direction: ExpandDirection::Out,
								quantifier: Some(EdgeQuantifier {
									min: 1,
									max: Some(1),
								}),
							},
							nodestep(2, Some("person")),
						)],
					},
					// (a)-[k2:knows]->(c:person)
					PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, None),
						steps: vec![(
							edgestep(3, Some("knows"), ExpandDirection::Out),
							nodestep(4, Some("person")),
						)],
					},
				],
				predicates: Vec::new(),
			}]),
			output: out_columns(vec![col("b", var("b")), col("c", var("c"))]),
		}
	}

	// ---- sequential MATCH join (two clauses, shared b) ----
	// `MATCH (a:person)-[k:knows]->(b:person) MATCH (b:person)-[k2:likes]->(c:person) RETURN a, c`
	fn plan_sequential_clauses() -> MatchPlan {
		MatchPlan {
			// 0=a, 1=k, 2=b, 3=k2, 4=c
			bindings: vec![node("a"), edge("k"), node("b"), edge("k2"), node("c")],
			stages: stages_of(vec![
				MatchClausePlan {
					optional_group: None,
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, Some("person")),
						steps: vec![(
							edgestep(1, Some("knows"), ExpandDirection::Out),
							nodestep(2, Some("person")),
						)],
					}],
					predicates: Vec::new(),
				},
				MatchClausePlan {
					optional_group: None,
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(2, Some("person")),
						steps: vec![(
							edgestep(3, Some("likes"), ExpandDirection::Out),
							nodestep(4, Some("person")),
						)],
					}],
					predicates: Vec::new(),
				},
			]),
			output: out_columns(vec![col("a", var("a")), col("c", var("c"))]),
		}
	}

	// ---- EXPLAIN snapshot harness ----

	fn render_plan(plan: &dyn ExecOperator, out: &mut String, prefix: &str) {
		use std::fmt::Write;
		let _ = write!(out, "{} [ctx: {}]", plan.name(), plan.required_context().short_name());
		let attrs = plan.attrs();
		if !attrs.is_empty() {
			let _ = write!(out, " [");
			for (i, (k, v)) in attrs.iter().enumerate() {
				if i > 0 {
					let _ = write!(out, ", ");
				}
				let _ = write!(out, "{k}: {v}");
			}
			let _ = write!(out, "]");
		}
		let _ = writeln!(out);
		let children = plan.children();
		if !children.is_empty() {
			let child_prefix = format!("{prefix}    ");
			for child in children.iter() {
				let _ = write!(out, "{child_prefix}");
				render_plan(child.as_ref(), out, &child_prefix);
			}
		}
	}

	/// Build a memory Datastore, define ns/db + the node/edge tables the plans
	/// reference, then plan `plan` and return its rendered EXPLAIN tree.
	async fn explain(plan: MatchPlan) -> String {
		let ds = Datastore::new("memory").await.expect("datastore");
		let session = crate::dbs::Session::owner().with_ns("test").with_db("test");
		ds.execute(
			"DEFINE NAMESPACE test; DEFINE DATABASE test; \
			 DEFINE TABLE person; DEFINE TABLE knows; DEFINE TABLE likes; \
			 DEFINE TABLE x; DEFINE TABLE y;",
			&session,
			None,
		)
		.await
		.expect("define schema");

		let base = ds.setup_ctx().expect("setup_ctx").freeze();
		let txn = Arc::new(ds.transaction(TransactionType::Read).await.expect("txn"));
		let mut ctx = Context::new_child(&base);
		ctx.set_transaction(Arc::clone(&txn));
		let ctx = ctx.freeze();

		let planner = Planner::with_txn(
			&ctx,
			ds.function_registry(),
			txn,
			Some("test".to_string()),
			Some("test".to_string()),
		);
		let operator = planner.plan_match(plan).await.expect("plan_match");

		let mut out = String::new();
		render_plan(operator.as_ref(), &mut out, "");
		out
	}

	// ---- pinned EXPLAIN snapshots ----

	#[tokio::test]
	async fn explain_tree_i_single_pattern_edge_predicate() {
		let rendered = explain(plan_tree_i()).await;
		let expected = "\
Project [ctx: Db]
    Expand [ctx: Db] [source: a, direction: ->, tables: knows, edge: k, target: b, target_label: person, predicate: k.since > 2020]
        Bind [ctx: Db] [binding: a]
            TableScan [ctx: Db] [table: person, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	#[tokio::test]
	async fn explain_tree_iv_quantified_path_order() {
		let rendered = explain(plan_tree_iv()).await;
		let expected = "\
Project [ctx: Db]
    Sort [ctx: Db] [order_by: a.age ASC]
        PathExpand [ctx: Db] [source: a, direction: ->, tables: knows, min: 1, max: 3, target_binding: b, target_label: person, group: __e0, path: p]
            Bind [ctx: Db] [binding: a]
                TableScan [ctx: Db] [table: person, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	#[tokio::test]
	async fn explain_shortest_routes_to_shortest_path_expand() {
		// A SHORTEST selector routes the quantified step to `ShortestPathExpand`,
		// carrying the selector and (non-default) path mode.
		let mut plan = plan_tree_iv();
		clause_mut(&mut plan, 0).patterns[0].search = Some(PathPrefixPlan {
			search: PathSearch::AllShortest,
			mode: IrPathMode::Acyclic,
		});
		let rendered = explain(plan).await;
		assert!(rendered.contains("ShortestPathExpand"), "\n--- got ---\n{rendered}");
		assert!(rendered.contains("search: all shortest"), "\n--- got ---\n{rendered}");
		assert!(rendered.contains("mode: acyclic"), "\n--- got ---\n{rendered}");
	}

	#[tokio::test]
	async fn explain_any_routes_to_shortest_path_expand() {
		// `ANY [k]` routes to the bounded `ShortestPathExpand` (a shortest
		// representative is a valid arbitrary path), labelled `search: any`.
		let mut plan = plan_tree_iv();
		clause_mut(&mut plan, 0).patterns[0].search = Some(PathPrefixPlan {
			search: PathSearch::Any {
				count: 3,
			},
			mode: IrPathMode::Walk,
		});
		let rendered = explain(plan).await;
		assert!(rendered.contains("ShortestPathExpand"), "\n--- got ---\n{rendered}");
		assert!(rendered.contains("search: any 3"), "\n--- got ---\n{rendered}");
		// Default `WALK` mode renders no `mode:` attr.
		assert!(!rendered.contains("mode:"), "\n--- got ---\n{rendered}");
	}

	/// Pin worked tree (ii): edge-anchored multi-pattern. Each pattern scans its
	/// edge table, binds the edge, then EndpointBind×2 (in then out). The two
	/// sub-trees join on the shared node `b`. DistinctEdges is ELIDED because the
	/// edge tables `x` and `y` are statically disjoint.
	#[tokio::test]
	async fn explain_tree_ii_edge_anchored_hashjoin_distinctedges_elided() {
		let rendered = explain(plan_tree_ii()).await;
		let expected = "\
Project [ctx: Db]
    HashJoin [ctx: Db] [type: Inner, keys: b]
        EndpointBind [ctx: Db] [edge: __e0, field: out, node: b]
            EndpointBind [ctx: Db] [edge: __e0, field: in, node: a]
                Bind [ctx: Db] [binding: __e0]
                    TableScan [ctx: Db] [table: x, direction: Forward]
        EndpointBind [ctx: Db] [edge: __e1, field: out, node: b]
            EndpointBind [ctx: Db] [edge: __e1, field: in, node: c]
                Bind [ctx: Db] [binding: __e1]
                    TableScan [ctx: Db] [table: y, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	/// Shared-node multi-pattern over the SAME edge table (`knows`): the two
	/// patterns are node-anchored (TableScan person + Bind + Expand), HashJoin'd
	/// on `b`, and wrapped in DistinctEdges{k, k2} since the edge tables are NOT
	/// disjoint (R2 must hold dynamically).
	#[tokio::test]
	async fn explain_shared_node_same_edge_keeps_distinctedges() {
		let rendered = explain(plan_shared_node_same_edge()).await;
		let expected = "\
Project [ctx: Db]
    DistinctEdges [ctx: Db] [edges: k, k2]
        HashJoin [ctx: Db] [type: Inner, keys: b]
            Expand [ctx: Db] [source: a, direction: ->, tables: knows, edge: k, target: b, target_label: person]
                Bind [ctx: Db] [binding: a]
                    TableScan [ctx: Db] [table: person, direction: Forward]
            Expand [ctx: Db] [source: c, direction: ->, tables: knows, edge: k2, target: b]
                Bind [ctx: Db] [binding: c]
                    TableScan [ctx: Db] [table: person, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	/// Sequential MATCH clauses joined on the shared node `b`: the second clause
	/// is planned as its own sub-tree (TableScan person + Bind b + Expand to c)
	/// and HashJoin'd Inner into the first clause's plan on `b` (R1: sequential
	/// MATCH joins exactly like comma patterns).
	#[tokio::test]
	async fn explain_sequential_clause_hashjoin_on_shared_node() {
		let rendered = explain(plan_sequential_clauses()).await;
		let expected = "\
Project [ctx: Db]
    HashJoin [ctx: Db] [type: Inner, keys: b]
        Expand [ctx: Db] [source: a, direction: ->, tables: knows, edge: k, target: b, target_label: person]
            Bind [ctx: Db] [binding: a]
                TableScan [ctx: Db] [table: person, direction: Forward]
        Expand [ctx: Db] [source: b, direction: ->, tables: likes, edge: k2, target: c, target_label: person]
            Bind [ctx: Db] [binding: b]
                TableScan [ctx: Db] [table: person, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	/// Pin the anonymous-quantified-group R2 regression: the hidden group `__e0`
	/// must be bound on the row (PathExpand `group: __e0`) AND the clause wrapped
	/// in DistinctEdges over `{__e0, k2}` (both draw from `knows`, not disjoint),
	/// so R2 can drop a row reusing one edge in both positions. Pattern 2 has a
	/// labelled single edge, so it is planned edge-anchored and HashJoin'd on the
	/// shared node `a`.
	#[tokio::test]
	async fn explain_anon_group_second_edge_binds_group_and_keeps_distinctedges() {
		let rendered = explain(plan_anon_group_second_edge()).await;
		let expected = "\
Project [ctx: Db]
    DistinctEdges [ctx: Db] [edges: __e0, k2]
        HashJoin [ctx: Db] [type: Inner, keys: a]
            PathExpand [ctx: Db] [source: a, direction: ->, tables: knows, min: 1, max: 1, target_binding: b, target_label: person, group: __e0]
                Bind [ctx: Db] [binding: a]
                    TableScan [ctx: Db] [table: person, direction: Forward]
            EndpointBind [ctx: Db] [edge: k2, field: out, node: c, target_label: person]
                EndpointBind [ctx: Db] [edge: k2, field: in, node: a]
                    Bind [ctx: Db] [binding: k2]
                        TableScan [ctx: Db] [table: knows, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	// ---- OPTIONAL: fast path (iii) ----
	// `MATCH (a:person) OPTIONAL MATCH (a)-[k:knows]->(b) RETURN a.name, b.name`
	// The optional clause is a single single-hop pattern whose source `a` is bound
	// by the mandatory clause ⇒ the OptionalExpand fast path (one operator, no
	// join), null-filling `k` and `b` on a miss.
	fn plan_optional_fast_path() -> MatchPlan {
		MatchPlan {
			// 0=a, 1=k, 2=b
			bindings: vec![node("a"), edge("k"), node("b")],
			stages: stages_of(vec![
				MatchClausePlan {
					optional_group: None,
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, Some("person")),
						steps: Vec::new(),
					}],
					predicates: Vec::new(),
				},
				MatchClausePlan {
					optional_group: Some(0),
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, None),
						steps: vec![(
							edgestep(1, Some("knows"), ExpandDirection::Out),
							nodestep(2, None),
						)],
					}],
					predicates: Vec::new(),
				},
			]),
			output: out_columns(vec![
				col("a_name", field_path("a", "name")),
				col("b_name", field_path("b", "name")),
			]),
		}
	}

	// ---- OPTIONAL: LeftJoin general path, brace block as a unit ----
	// `MATCH (a:person) OPTIONAL { MATCH (a)-[:knows]->(b:person)
	//                              MATCH (b)-[k2]->(c:person) } RETURN a`
	// The block is two clauses sharing OPTIONAL#0. The whole block is planned as
	// ONE standalone subplan (the first clause edge-anchored on `:knows`; the
	// second expands off the within-block `b` over an unlabeled edge) and
	// left-joined onto the accumulator as a unit on the shared `a`, null-filling
	// every block-introduced binding (`__e0, b, k2, c`). No DistinctEdges appears:
	// R2 (DIFFERENT EDGES) is per-MATCH-statement, and each block clause has just
	// one edge binding, so neither clause triggers the ≥2-edges wrapper.
	fn plan_optional_block_unit() -> MatchPlan {
		MatchPlan {
			// 0=a, 1=__e0 (hidden knows), 2=b, 3=k2, 4=c
			bindings: vec![node("a"), hidden_edge("__e0"), node("b"), edge("k2"), node("c")],
			stages: stages_of(vec![
				MatchClausePlan {
					optional_group: None,
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, Some("person")),
						steps: Vec::new(),
					}],
					predicates: Vec::new(),
				},
				// OPTIONAL#0, clause 1: (a)-[__e0:knows]->(b:person)
				MatchClausePlan {
					optional_group: Some(0),
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, None),
						steps: vec![(
							edgestep(1, Some("knows"), ExpandDirection::Out),
							nodestep(2, Some("person")),
						)],
					}],
					predicates: Vec::new(),
				},
				// OPTIONAL#0, clause 2: (b)-[k2]->(c:person) — unlabeled edge,
				// expands off the within-block `b`.
				MatchClausePlan {
					optional_group: Some(0),
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(2, None),
						steps: vec![(
							edgestep(3, None, ExpandDirection::Out),
							nodestep(4, Some("person")),
						)],
					}],
					predicates: Vec::new(),
				},
			]),
			output: out_columns(vec![col("a", var("a"))]),
		}
	}

	// ---- OPTIONAL: chained optionals (distinct blocks) ----
	// `MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person)
	//                   OPTIONAL MATCH (a)-[:likes]->(c:person) RETURN a`
	// Two independent blocks (#0, #1) chain left-to-right. Each is a single
	// single-hop non-quantified pattern whose source `a` is bound in the
	// accumulator, so each takes the OptionalExpand fast path; the two stack. The
	// second OptionalExpand expands off `a` on rows the first produced (where `b`
	// may already be a null-fill), so a chained optional referencing an earlier
	// accumulator binding flows through (R3, chained left-to-right).
	fn plan_chained_optionals() -> MatchPlan {
		MatchPlan {
			// 0=a, 1=__e0 knows, 2=b, 3=__e1 likes, 4=c
			bindings: vec![
				node("a"),
				hidden_edge("__e0"),
				node("b"),
				hidden_edge("__e1"),
				node("c"),
			],
			stages: stages_of(vec![
				MatchClausePlan {
					optional_group: None,
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, Some("person")),
						steps: Vec::new(),
					}],
					predicates: Vec::new(),
				},
				MatchClausePlan {
					optional_group: Some(0),
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, None),
						steps: vec![(
							edgestep(1, Some("knows"), ExpandDirection::Out),
							nodestep(2, Some("person")),
						)],
					}],
					predicates: Vec::new(),
				},
				MatchClausePlan {
					optional_group: Some(1),
					patterns: vec![PatternPlan {
						path_var: None,
						search: None,
						start: nodestep(0, None),
						steps: vec![(
							edgestep(3, Some("likes"), ExpandDirection::Out),
							nodestep(4, Some("person")),
						)],
					}],
					predicates: Vec::new(),
				},
			]),
			output: out_columns(vec![col("a", var("a"))]),
		}
	}

	// ---- OPTIONAL: multi-pattern single-clause LeftJoin ----
	// `MATCH (a:person), (z:person)
	//  OPTIONAL MATCH (a)-[:knows]->(b:person), (z)-[:likes]->(c:person) RETURN a`
	// The optional clause has TWO patterns sharing no variable ⇒ its subplan is a
	// Cross of two edge-anchored sub-trees, left-joined onto the accumulator on the
	// shared `{a, z}`. `knows`/`likes` are disjoint ⇒ DistinctEdges elided.
	fn plan_optional_multipattern() -> MatchPlan {
		MatchPlan {
			// 0=a, 1=z, 2=__e0 knows, 3=b, 4=__e1 likes, 5=c
			bindings: vec![
				node("a"),
				node("z"),
				hidden_edge("__e0"),
				node("b"),
				hidden_edge("__e1"),
				node("c"),
			],
			stages: stages_of(vec![
				MatchClausePlan {
					optional_group: None,
					patterns: vec![
						PatternPlan {
							path_var: None,
							search: None,
							start: nodestep(0, Some("person")),
							steps: Vec::new(),
						},
						PatternPlan {
							path_var: None,
							search: None,
							start: nodestep(1, Some("person")),
							steps: Vec::new(),
						},
					],
					predicates: Vec::new(),
				},
				MatchClausePlan {
					optional_group: Some(0),
					patterns: vec![
						PatternPlan {
							path_var: None,
							search: None,
							start: nodestep(0, None),
							steps: vec![(
								edgestep(2, Some("knows"), ExpandDirection::Out),
								nodestep(3, Some("person")),
							)],
						},
						PatternPlan {
							path_var: None,
							search: None,
							start: nodestep(1, None),
							steps: vec![(
								edgestep(4, Some("likes"), ExpandDirection::Out),
								nodestep(5, Some("person")),
							)],
						},
					],
					predicates: Vec::new(),
				},
			]),
			output: out_columns(vec![col("a", var("a"))]),
		}
	}

	/// Fast path (iii): OptionalExpand appended directly to the mandatory anchor —
	/// one operator, no join. The edge `k` (user-named ⇒ Full) and target `b` are
	/// null-filled on a miss (exactly the clause-introduced bindings).
	#[tokio::test]
	async fn explain_optional_fast_path_is_optionalexpand() {
		let rendered = explain(plan_optional_fast_path()).await;
		let expected = "\
Project [ctx: Db]
    OptionalExpand [ctx: Db] [source: a, direction: ->, tables: knows, edge: k, target: b]
        Bind [ctx: Db] [binding: a]
            TableScan [ctx: Db] [table: person, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	/// LeftJoin general path: the whole brace block is one standalone subplan
	/// (build side), left-joined onto the accumulator (probe) on the shared `a`,
	/// with the entire block's introduced bindings as the null_template ⇒
	/// all-or-nothing block semantics (R3).
	#[tokio::test]
	async fn explain_optional_block_is_one_leftjoin_unit() {
		let rendered = explain(plan_optional_block_unit()).await;
		let expected = "\
Project [ctx: Db]
    HashJoin [ctx: Db] [type: Left, keys: a, null_template: __e0, b, k2, c]
        Expand [ctx: Db] [source: b, direction: ->, tables: *, edge: k2, target: c, target_label: person]
            EndpointBind [ctx: Db] [edge: __e0, field: out, node: b, target_label: person]
                EndpointBind [ctx: Db] [edge: __e0, field: in, node: a]
                    Bind [ctx: Db] [binding: __e0]
                        TableScan [ctx: Db] [table: knows, direction: Forward]
        Bind [ctx: Db] [binding: a]
            TableScan [ctx: Db] [table: person, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	/// Chained optionals: two independent single-hop blocks fold left-to-right;
	/// each source `a` is bound, so each is the OptionalExpand fast path, stacked.
	/// The second OptionalExpand sees `a` on the (possibly null-filled) rows the
	/// first produced — a chained optional referencing an earlier accumulator
	/// binding flows through.
	#[tokio::test]
	async fn explain_chained_optionals_stack_optionalexpands() {
		let rendered = explain(plan_chained_optionals()).await;
		let expected = "\
Project [ctx: Db]
    OptionalExpand [ctx: Db] [source: a, direction: ->, tables: likes, edge: __e1, target: c, target_label: person]
        OptionalExpand [ctx: Db] [source: a, direction: ->, tables: knows, edge: __e0, target: b, target_label: person]
            Bind [ctx: Db] [binding: a]
                TableScan [ctx: Db] [table: person, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	/// Multi-pattern single-clause optional: the subplan is a Cross of two
	/// edge-anchored sub-trees, left-joined onto the accumulator on the shared
	/// `{a, z}`. DistinctEdges is elided (`knows`/`likes` are statically disjoint).
	#[tokio::test]
	async fn explain_optional_multipattern_is_leftjoin_over_cross() {
		let rendered = explain(plan_optional_multipattern()).await;
		let expected = "\
Project [ctx: Db]
    HashJoin [ctx: Db] [type: Left, keys: a, z, null_template: __e0, b, __e1, c]
        HashJoin [ctx: Db] [type: Cross]
            EndpointBind [ctx: Db] [edge: __e0, field: out, node: b, target_label: person]
                EndpointBind [ctx: Db] [edge: __e0, field: in, node: a]
                    Bind [ctx: Db] [binding: __e0]
                        TableScan [ctx: Db] [table: knows, direction: Forward]
            EndpointBind [ctx: Db] [edge: __e1, field: out, node: c, target_label: person]
                EndpointBind [ctx: Db] [edge: __e1, field: in, node: z]
                    Bind [ctx: Db] [binding: __e1]
                        TableScan [ctx: Db] [table: likes, direction: Forward]
        HashJoin [ctx: Db] [type: Cross]
            Bind [ctx: Db] [binding: a]
                TableScan [ctx: Db] [table: person, direction: Forward]
            Bind [ctx: Db] [binding: z]
                TableScan [ctx: Db] [table: person, direction: Forward]
";
		assert_eq!(rendered, expected, "\n--- got ---\n{rendered}");
	}

	// ---- helper unit tests ----

	#[test]
	fn shared_bindings_intersects_in_a_order() {
		assert_eq!(shared_bindings(&[0, 2, 4], &[4, 2, 9]), vec![2, 4]);
		assert!(shared_bindings(&[0, 1], &[2, 3]).is_empty());
	}

	#[test]
	fn union_bindings_appends_new_ids() {
		assert_eq!(union_bindings(&[0, 1], &[1, 2, 3]), vec![0, 1, 2, 3]);
	}

	#[test]
	fn edge_tables_disjoint_only_for_distinct_known_tables() {
		assert!(EdgeTables::Known(tbl("x")).disjoint_from(&EdgeTables::Known(tbl("y"))));
		assert!(!EdgeTables::Known(tbl("x")).disjoint_from(&EdgeTables::Known(tbl("x"))));
		// `Any` is never disjoint from anything (an unlabeled edge could match).
		assert!(!EdgeTables::Any.disjoint_from(&EdgeTables::Known(tbl("x"))));
		assert!(!EdgeTables::Known(tbl("x")).disjoint_from(&EdgeTables::Any));
		assert!(!EdgeTables::Any.disjoint_from(&EdgeTables::Any));
	}

	#[test]
	fn pairwise_disjoint_skips_distinctedges_for_distinct_tables() {
		let edges = vec![
			("e0".to_string(), EdgeTables::Known(tbl("x"))),
			("e1".to_string(), EdgeTables::Known(tbl("y"))),
		];
		assert!(edge_tables_pairwise_disjoint(&edges));
		let same = vec![
			("e0".to_string(), EdgeTables::Known(tbl("knows"))),
			("e1".to_string(), EdgeTables::Known(tbl("knows"))),
		];
		assert!(!edge_tables_pairwise_disjoint(&same));
		// Any one `Any` binding forces DistinctEdges in.
		let with_any = vec![
			("e0".to_string(), EdgeTables::Known(tbl("x"))),
			("e1".to_string(), EdgeTables::Any),
		];
		assert!(!edge_tables_pairwise_disjoint(&with_any));
	}

	#[test]
	fn shared_node_anchor_picks_start_then_far() {
		let pattern = PatternPlan {
			path_var: None,
			search: None,
			start: nodestep(0, None),
			steps: vec![(edgestep(1, Some("e"), ExpandDirection::Out), nodestep(2, None))],
		};
		let plan = MatchPlan {
			bindings: vec![node("a"), hidden_edge("e"), node("b")],
			stages: Vec::new(),
			output: out_columns(Vec::new()),
		};
		// Start node shared.
		assert_eq!(shared_node_anchor(&plan, &pattern, &[0]), Some(0));
		// Only far node shared (single hop) ⇒ reverse anchor on far.
		assert_eq!(shared_node_anchor(&plan, &pattern, &[2]), Some(2));
		// Nothing shared.
		assert_eq!(shared_node_anchor(&plan, &pattern, &[9]), None);
	}

	#[test]
	fn prefix_strip_rewrites_binding_field_path() {
		let expr = Expr::Binary {
			left: Box::new(field_path("a", "since")),
			op: BinaryOperator::MoreThan,
			right: Box::new(Expr::Literal(Literal::Integer(2020))),
		};
		let stripped = prefix_strip(&expr, "a").expect("strippable");
		let expected = Expr::Binary {
			left: Box::new(Expr::Idiom(Idiom(vec![Part::Field("since".to_string().into())]))),
			op: BinaryOperator::MoreThan,
			right: Box::new(Expr::Literal(Literal::Integer(2020))),
		};
		assert_eq!(stripped, expected);
	}

	#[test]
	fn prefix_strip_bails_on_whole_record_reference() {
		let expr = var("a");
		assert!(prefix_strip(&expr, "a").is_none());
	}

	#[test]
	fn deps_subset_checks_membership() {
		assert!(deps_subset(&[1, 2], &[0, 1, 2, 3]));
		assert!(!deps_subset(&[1, 4], &[0, 1, 2, 3]));
		assert!(deps_subset(&[], &[0]));
	}

	#[test]
	fn conjoin_builds_left_leaning_and_chain() {
		assert!(conjoin(Vec::new()).is_none());
		let one = conjoin(vec![Expr::Literal(Literal::Bool(true))]).expect("one");
		assert_eq!(one, Expr::Literal(Literal::Bool(true)));
	}

	#[test]
	fn drain_satisfiable_drains_only_ready_predicates() {
		let mut pending = vec![
			MatchPredicate {
				expr: var("a"),
				deps: vec![0],
			},
			MatchPredicate {
				expr: var("b"),
				deps: vec![5],
			},
		];
		let ready = drain_satisfiable(&mut pending, &[0, 1]);
		assert_eq!(ready, vec![var("a")]);
		assert_eq!(pending.len(), 1);
		assert_eq!(pending[0].deps, vec![5]);
	}
}
