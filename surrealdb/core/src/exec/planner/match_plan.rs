//! Planning for GQL v2 `MATCH` queries.
//!
//! `plan_match` (in `exec::vm`) compiles a [`MatchPlan`] (the language-neutral
//! binding-table IR produced by the GQL lowering, `expr/match_plan.rs`) into a
//! VM program through the fold in this module. See `doc/gql/V2_DESIGN.md` §6 for the
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
//! None of the internal bails here are `PlannerUnsupported`:
//! a `MatchPlan` only reaches the planner because it lowered cleanly, so an
//! unexpected shape is a real internal failure. There is nothing to fall back
//! to: the plan arrives already planned, and no statement stands behind it
//! to run instead.
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

use surrealdb_strand::TableName;

use super::match_backend::{
	BindingBackend, EndpointSpec, ExpandSpec, JoinSpec, PathSpec, Planning,
};
use crate::err::EngineError;
use crate::exec::operators::{
	EdgeBinding, EndpointField, ExpandDir, JoinType, PathMode, ShortestSelector,
};
use crate::expr::BinaryOperator;
use crate::expr::logical::LogicalExpr;
use crate::expr::match_plan::{
	BindingId, BindingKind, EdgeStep, ExpandDirection, MatchClausePlan, MatchOutput, MatchPlan,
	MatchPredicate, MatchStage, MutationStage, NodeStep, PathMode as IrPathMode, PathPrefixPlan,
	PathSearch, PatternPlan,
};

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

/// The fold of a [`MatchPlan`]'s stages into one node of `backend`.
pub(crate) struct MatchFold<'a, B: BindingBackend> {
	pub(crate) backend: &'a B,
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
struct ChainStage<N> {
	operator: N,
	/// Bindings available on rows leaving this stage.
	bound: Vec<BindingId>,
	/// The most recently bound node (anchor, or the prior step's target). The
	/// next step expands from this binding — tracked explicitly so a trailing
	/// path-var binding never gets mistaken for the expansion source.
	tip: BindingId,
}

/// One clause planned into a sub-tree, with the metadata the fold needs to
/// combine it with the accumulator.
struct PlannedClause<N> {
	operator: N,
	/// Every binding present on rows leaving this clause.
	bound: Vec<BindingId>,
	/// Clause predicates deferred to the clause-combine join (their deps span an
	/// earlier clause's bindings, so the join is the earliest stage that binds
	/// them all).
	deferred: Vec<MatchPredicate>,
}

impl<B: BindingBackend> MatchFold<'_, B> {
	/// Fold a GQL `MATCH` query into one node of the backend.
	///
	/// Folds clauses left-to-right over an accumulator `(operator, bound)`: the
	/// first clause seeds it; each subsequent (non-optional) clause is joined to
	/// the accumulator with a `HashJoin` — Inner on the node-binding ids the two
	/// sides share, or Cross when they share none (sequential `MATCH` joins
	/// exactly like comma-separated patterns, R1).
	pub(crate) async fn plan(&self, plan: MatchPlan) -> Result<B::Node, Planning> {
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
				let seed = self.backend.single_row()?;
				self.fold_mutation(&plan, seed, Vec::new(), stage).await?
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
				StageUnit::Mutation(stage) => {
					self.fold_mutation(&plan, acc_op, acc_bound, stage).await?
				}
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
	async fn fold_mutation(
		&self,
		plan: &MatchPlan,
		op: B::Node,
		mut bound: Vec<BindingId>,
		stage: &MutationStage,
	) -> Result<(B::Node, Vec<BindingId>), Planning> {
		let op = self.backend.mutate(plan, op, stage).await?;
		if let MutationStage::Insert(insert) = stage {
			bound.extend(insert.nodes.iter().map(|n| n.binding));
			bound.extend(insert.edges.iter().map(|e| e.binding));
		}
		Ok((op, bound))
	}

	/// Join `probe`'s rows to `build`'s: Inner on the node bindings the two
	/// sides share, Cross when they share none.
	async fn join_shared(
		&self,
		plan: &MatchPlan,
		build: B::Node,
		build_bound: &[BindingId],
		probe: B::Node,
		probe_bound: &[BindingId],
	) -> Result<B::Node, Planning> {
		let shared = shared_bindings(build_bound, probe_bound);
		let keys = binding_names(plan, &shared);
		let join_type = if shared.is_empty() {
			JoinType::Cross
		} else {
			JoinType::Inner
		};
		self.backend
			.join(
				build,
				probe,
				JoinSpec {
					keys,
					join_type,
					null_template: Vec::new(),
					residual: None,
				},
			)
			.await
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
		acc_op: B::Node,
		acc_bound: &[BindingId],
		clause: PlannedClause<B::Node>,
	) -> Result<(B::Node, Vec<BindingId>), Planning> {
		// Accumulator = build side; new clause = probe side.
		let join =
			self.join_shared(plan, acc_op, acc_bound, clause.operator, &clause.bound).await?;

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
		acc_op: B::Node,
		acc_bound: &[BindingId],
		clauses: &[&MatchClausePlan],
	) -> Result<(B::Node, Vec<BindingId>), Planning> {
		// The OptionalExpand fast path: a single optional clause, a single
		// single-hop pattern, source already bound in the accumulator.
		if let Some(stage) =
			self.try_optional_expand_fast_path(plan, acc_op.clone(), acc_bound, clauses).await?
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
		let join = self
			.backend
			.join(
				subplan.operator,
				acc_op,
				JoinSpec {
					keys,
					join_type: JoinType::Left,
					null_template,
					residual: conjoin(residual_exprs),
				},
			)
			.await?;

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
		acc_op: B::Node,
		acc_bound: &[BindingId],
		clauses: &[&MatchClausePlan],
	) -> Result<Option<ChainStage<B::Node>>, Planning> {
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
		let edge_def = plan.binding(edge.binding);
		let edge_name = edge_def.name.clone();
		let edge_binding = if edge_def.user_named {
			EdgeBinding::Full(edge_name)
		} else {
			EdgeBinding::IdOnly(edge_name)
		};

		let operator = self
			.backend
			.expand(
				acc_op,
				ExpandSpec {
					source,
					dir,
					edge_tables,
					edge_binding,
					target_binding,
					target_label,
					predicate: conjoin(predicates),
					optional: true,
				},
			)
			.await?;

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
	) -> Result<PlannedClause<B::Node>, Planning> {
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
		// NOT dropped.
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
					Some((block_op.clone(), block_bound.clone())),
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
	) -> Result<PlannedClause<B::Node>, Planning> {
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
		seed: Option<(B::Node, Vec<BindingId>)>,
	) -> Result<PlannedClause<B::Node>, Planning> {
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
		let mut clause_op: B::Node;
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
					clause_op = self
						.join_shared(
							plan,
							seed_op.clone(),
							seed_bound,
							stage.operator,
							&stage.bound,
						)
						.await?;
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
							seed_op.clone(),
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
				clause_op = self
					.join_shared(plan, clause_op, &clause_bound, stage.operator, &stage.bound)
					.await?;
				clause_bound = union_bindings(&clause_bound, &stage.bound);
			} else if shared_node_anchor(plan, pattern, &visible).is_some() {
				// Bound-variable pattern: expand directly off the clause
				// accumulator. Within-pattern step conjuncts ride the expansion.
				let stage = self
					.expand_bound_pattern(plan, pattern, &visible, clause_op.clone(), &mut pending)
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
		clause_op = self.wrap_distinct_edges(plan, clause, clause_op)?;

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
	) -> Result<Option<ChainStage<B::Node>>, Planning> {
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
	) -> Result<ChainStage<B::Node>, Planning> {
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
		let mut anchor_pushed: Vec<LogicalExpr> = Vec::new();
		let mut anchor_filtered: Vec<LogicalExpr> = Vec::new();
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
			let mut step_predicates: Vec<LogicalExpr> = Vec::new();
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
	) -> Result<Option<ChainStage<B::Node>>, Planning> {
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
		let scan = self.backend.scan(edge_label, None).await?;
		let bind = self.backend.bind(scan, edge_name.clone())?;

		// For a forward edge `(start)-[:e]->(far)`, the edge's `in` is `start` and
		// its `out` is `far`; an `In` edge swaps the roles. EndpointBind reads the
		// edge's literal `in`/`out` field, so the field follows the pattern's
		// direction, not the node's position.
		let (in_node, out_node) = match edge.direction {
			ExpandDirection::Out => (&pattern.start, far_node),
			ExpandDirection::In => (far_node, &pattern.start),
		};

		// Bottom binds `in`, top binds `out` (worked tree (ii) ordering).
		let in_bind = self.backend.endpoint(
			bind,
			EndpointSpec {
				edge: edge_name.clone(),
				field: EndpointField::In,
				name: binding_name(plan, in_node.binding).to_string(),
				label: in_node.label.clone(),
			},
		)?;
		let out_bind = self.backend.endpoint(
			in_bind,
			EndpointSpec {
				edge: edge_name,
				field: EndpointField::Out,
				name: binding_name(plan, out_node.binding).to_string(),
				label: out_node.label.clone(),
			},
		)?;

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
		input: B::Node,
		pending: &mut Vec<MatchPredicate>,
	) -> Result<ChainStage<B::Node>, Planning> {
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
		op: B::Node,
		bound: &[BindingId],
		pending: &mut Vec<MatchPredicate>,
	) -> Result<B::Node, Planning> {
		let mut ready: Vec<LogicalExpr> = Vec::new();
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
		op: B::Node,
		bound: &[BindingId],
		predicates: Vec<MatchPredicate>,
	) -> Result<B::Node, Planning> {
		let mut exprs: Vec<LogicalExpr> = Vec::with_capacity(predicates.len());
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
		op: B::Node,
	) -> Result<B::Node, Planning> {
		let edges = clause_edge_bindings(plan, clause);
		if edges.len() < 2 {
			return Ok(op);
		}
		if edge_tables_pairwise_disjoint(&edges) {
			return Ok(op);
		}
		let names = edges.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
		self.backend.distinct_edges(op, names)
	}

	/// Build the anchor stage: `TableScan` (via the SELECT source machinery, so
	/// permissions / computed fields / pre-decode filter and the pushed
	/// predicate all apply) wrapped in `Bind`.
	async fn plan_anchor(
		&self,
		binding: BindingId,
		label: TableName,
		plan: &MatchPlan,
		pushed: Vec<LogicalExpr>,
	) -> Result<ChainStage<B::Node>, Planning> {
		let scan = self.backend.scan(label, conjoin(pushed)).await?;
		let name = binding_name(plan, binding).to_string();
		let operator = self.backend.bind(scan, name)?;
		Ok(ChainStage {
			operator,
			bound: vec![binding],
			tip: binding,
		})
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
		prev: ChainStage<B::Node>,
		edge: &EdgeStep,
		node: &NodeStep,
		predicates: Vec<LogicalExpr>,
		direction: ExpandDirection,
		reversed: bool,
	) -> Result<ChainStage<B::Node>, Planning> {
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
				let operator = self
					.backend
					.expand(
						prev.operator,
						ExpandSpec {
							source,
							dir,
							edge_tables,
							edge_binding,
							target_binding,
							target_label,
							predicate: conjoin(predicates),
							optional: false,
						},
					)
					.await?;
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
				let operator = self
					.backend
					.path_expand(
						prev.operator,
						PathSpec {
							source,
							dir,
							edge_tables,
							min: quantifier.min,
							max: quantifier.max,
							target_binding,
							target_label,
							group_binding,
							path_binding,
							mode: exec_mode,
							selector: match routing {
								SearchRouting::Every => None,
								SearchRouting::Shortest {
									selector,
								} => Some(selector),
							},
							reversed,
						},
					)
					.await?;
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
	async fn maybe_filter(
		&self,
		stage: ChainStage<B::Node>,
		exprs: Vec<LogicalExpr>,
	) -> Result<ChainStage<B::Node>, Planning> {
		let Some(joined) = conjoin(exprs) else {
			return Ok(stage);
		};
		let operator = self.backend.filter(stage.operator, joined).await?;
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
		op: B::Node,
		exprs: Vec<LogicalExpr>,
	) -> Result<B::Node, Planning> {
		let Some(joined) = conjoin(exprs) else {
			return Ok(op);
		};
		self.backend.filter(op, joined).await
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
	async fn plan_match_tail(&self, plan: &MatchPlan, body: B::Node) -> Result<B::Node, Planning> {
		// A mutation-only plan (no `RETURN`) carries no output spec; drive the
		// mutation chain to completion (the writes happen as its rows are pulled)
		// and emit no rows, so the query returns an empty result.
		let Some(output) = plan.output.as_ref() else {
			return self.backend.drain(body);
		};
		if let Some(group_keys) = output.group_by.as_ref() {
			let mut op = self.backend.aggregate(body, output, group_keys).await?;
			if output.distinct {
				op = self.backend.distinct(op)?;
			}
			op = self.plan_match_sort(op, output).await?;
			op = self.plan_match_limit(op, output).await?;
			// Drop any sort-only (hidden) columns the lowering materialised for a
			// non-projected ORDER BY key.
			if output.columns.iter().any(|c| c.hidden) {
				op = self.backend.drop_hidden(op, output).await?;
			}
			Ok(op)
		} else if output.distinct {
			let mut op = self.backend.project(body, output).await?;
			op = self.backend.distinct(op)?;
			op = self.plan_match_sort(op, output).await?;
			op = self.plan_match_limit(op, output).await?;
			Ok(op)
		} else {
			let mut op = self.plan_match_sort(body, output).await?;
			op = self.plan_match_limit(op, output).await?;
			op = self.backend.project(op, output).await?;
			Ok(op)
		}
	}

	/// Order `input` by `MatchOutput::order`, or pass it through when there is
	/// no ORDER BY.
	async fn plan_match_sort(
		&self,
		input: B::Node,
		output: &MatchOutput,
	) -> Result<B::Node, Planning> {
		if output.order.is_empty() {
			return Ok(input);
		}
		self.backend.sort(input, output).await
	}

	/// Page `input` by `MatchOutput::skip`/`limit`, or pass it through when
	/// neither is present.
	async fn plan_match_limit(
		&self,
		input: B::Node,
		output: &MatchOutput,
	) -> Result<B::Node, Planning> {
		if output.limit.is_none() && output.skip.is_none() {
			return Ok(input);
		}
		self.backend.limit(input, output).await
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
fn drain_satisfiable(pending: &mut Vec<MatchPredicate>, bound: &[BindingId]) -> Vec<LogicalExpr> {
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
fn conjoin(mut exprs: Vec<LogicalExpr>) -> Option<LogicalExpr> {
	let mut acc = exprs.pop()?;
	while let Some(next) = exprs.pop() {
		acc = LogicalExpr::Binary {
			left: Box::new(next),
			op: BinaryOperator::And,
			right: Box::new(acc),
		};
	}
	Some(acc)
}

/// Rewrite a binding-row-scoped expression into a record-row-scoped one for
/// scan pushdown: every access chain `binding.rest…` becomes `rest…`. Returns
/// `None` — bailing to a Filter — if the binding is read in any other way: a
/// bare whole-record reference `binding`, or a path that reaches the binding
/// through a step other than a field access (`binding[0].x`, `binding->e`),
/// which has no record-row spelling here.
fn prefix_strip(expr: &LogicalExpr, binding: &str) -> Option<LogicalExpr> {
	if expr.is_path() {
		if let LogicalExpr::Field(root) = expr.path_root()
			&& root.as_str() == binding
		{
			return strip_binding_root(expr, binding);
		}
		return Some(expr.clone());
	}
	match expr {
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => Some(LogicalExpr::Binary {
			left: Box::new(prefix_strip(left, binding)?),
			op: op.clone(),
			right: Box::new(prefix_strip(right, binding)?),
		}),
		LogicalExpr::Prefix {
			op,
			expr,
		} => Some(LogicalExpr::Prefix {
			op: op.clone(),
			expr: Box::new(prefix_strip(expr, binding)?),
		}),
		LogicalExpr::Postfix {
			op,
			expr,
		} => Some(LogicalExpr::Postfix {
			op: op.clone(),
			expr: Box::new(prefix_strip(expr, binding)?),
		}),
		LogicalExpr::Invoke {
			target,
			args,
		} => Some(LogicalExpr::Invoke {
			target: Box::new(prefix_strip(target, binding)?),
			args: args.clone(),
		}),
		other @ (LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_)) => Some(other.clone()),
	}
}

/// `binding.f.g…` becomes `f.g…`: the access directly on the binding becomes
/// the root field read and the accesses above it are kept. `path` is a path
/// whose root is `Field(binding)`; the result is `None` unless every step
/// between the root and `path` is an [`LogicalExpr::Access`].
fn strip_binding_root(path: &LogicalExpr, binding: &str) -> Option<LogicalExpr> {
	let LogicalExpr::Access {
		base,
		field,
	} = path
	else {
		return None;
	};
	if let LogicalExpr::Field(root) = base.as_ref()
		&& root.as_str() == binding
	{
		return Some(LogicalExpr::Field(field.clone()));
	}
	Some(LogicalExpr::Access {
		base: Box::new(strip_binding_root(base, binding)?),
		field: field.clone(),
	})
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::*;
	use crate::expr::logical::LogicalLiteral;
	use crate::expr::match_plan::{BindingDef, BindingKind, MatchColumn, MatchOutput};

	// ---- shared builders ----

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

	fn hidden_edge(name: &str) -> BindingDef {
		binding(name, BindingKind::Edge, false)
	}

	/// `a.field` as a binding-row scoped path.
	fn field_path(b: &str, field: &str) -> LogicalExpr {
		LogicalExpr::Access {
			base: Box::new(var(b)),
			field: field.into(),
		}
	}

	/// `RETURN b` — a bare-binding column expression.
	fn var(b: &str) -> LogicalExpr {
		LogicalExpr::Field(b.into())
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

	// ---- worked tree (iv): quantified path + ORDER BY ----

	// ---- worked tree (ii): edge-anchored multi-pattern, HashJoin on b ----
	// `MATCH (a)-[:x]->(b), (c)-[:y]->(b) RETURN a, c`

	// ---- shared-node multi-pattern, same edge table (DistinctEdges kept) ----
	// `MATCH (a:person)-[k:knows]->(b:person), (c:person)-[k2:knows]->(b) RETURN a, c`

	// ---- anonymous quantified group + a second edge sharing the table ----
	// `MATCH (a:person)-[:knows]->{1,1}(b:person), (a)-[k2:knows]->(c:person) RETURN b, c`
	// The first pattern's quantified edge is ANONYMOUS (hidden `__e0`,
	// EdgeGroup). R2 still applies across the group and `k2` (same `knows`
	// table), so the planner must (a) bind the hidden group on the row via
	// `PathExpand`'s `group:` slot and (b) wrap the clause in DistinctEdges over
	// `{__e0, k2}`. Pins the regression for the anonymous-group R2 skip.

	// ---- sequential MATCH join (two clauses, shared b) ----
	// `MATCH (a:person)-[k:knows]->(b:person) MATCH (b:person)-[k2:likes]->(c:person) RETURN a, c`

	// ---- EXPLAIN snapshot harness ----

	// ---- pinned EXPLAIN snapshots ----

	// ---- OPTIONAL: fast path (iii) ----
	// `MATCH (a:person) OPTIONAL MATCH (a)-[k:knows]->(b) RETURN a.name, b.name`
	// The optional clause is a single single-hop pattern whose source `a` is bound
	// by the mandatory clause ⇒ the OptionalExpand fast path (one operator, no
	// join), null-filling `k` and `b` on a miss.

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

	// ---- OPTIONAL: chained optionals (distinct blocks) ----
	// `MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person)
	//                   OPTIONAL MATCH (a)-[:likes]->(c:person) RETURN a`
	// Two independent blocks (#0, #1) chain left-to-right. Each is a single
	// single-hop non-quantified pattern whose source `a` is bound in the
	// accumulator, so each takes the OptionalExpand fast path; the two stack. The
	// second OptionalExpand expands off `a` on rows the first produced (where `b`
	// may already be a null-fill), so a chained optional referencing an earlier
	// accumulator binding flows through (R3, chained left-to-right).

	// ---- OPTIONAL: multi-pattern single-clause LeftJoin ----
	// `MATCH (a:person), (z:person)
	//  OPTIONAL MATCH (a)-[:knows]->(b:person), (z)-[:likes]->(c:person) RETURN a`
	// The optional clause has TWO patterns sharing no variable ⇒ its subplan is a
	// Cross of two edge-anchored sub-trees, left-joined onto the accumulator on the
	// shared `{a, z}`. `knows`/`likes` are disjoint ⇒ DistinctEdges elided.

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
		let expr = LogicalExpr::Binary {
			left: Box::new(field_path("a", "since")),
			op: BinaryOperator::MoreThan,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(2020))),
		};
		let stripped = prefix_strip(&expr, "a").expect("strippable");
		let expected = LogicalExpr::Binary {
			left: Box::new(LogicalExpr::Field("since".into())),
			op: BinaryOperator::MoreThan,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(2020))),
		};
		assert_eq!(stripped, expected);
	}

	#[test]
	fn prefix_strip_bails_on_whole_record_reference() {
		let expr = var("a");
		assert!(prefix_strip(&expr, "a").is_none());
	}

	#[test]
	fn prefix_strip_keeps_the_accesses_above_the_binding() {
		// `a.address.city` ⇒ `address.city`; `b.name` is left as written.
		let expr = LogicalExpr::Access {
			base: Box::new(field_path("a", "address")),
			field: "city".into(),
		};
		let expected = LogicalExpr::Access {
			base: Box::new(LogicalExpr::Field("address".into())),
			field: "city".into(),
		};
		assert_eq!(prefix_strip(&expr, "a"), Some(expected));
		assert_eq!(prefix_strip(&field_path("b", "name"), "a"), Some(field_path("b", "name")));
	}

	#[test]
	fn prefix_strip_bails_on_a_non_access_step_over_the_binding() {
		// `a[0].name` reaches the binding through an index, not a field access.
		let expr = LogicalExpr::Access {
			base: Box::new(LogicalExpr::Index {
				base: Box::new(var("a")),
				index: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(0))),
			}),
			field: "name".into(),
		};
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
		let one = conjoin(vec![LogicalExpr::Literal(LogicalLiteral::Bool(true))]).expect("one");
		assert_eq!(one, LogicalExpr::Literal(LogicalLiteral::Bool(true)));
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
