//! The binding registry and the variable-resolution semantic pass.
//!
//! Implements `doc/gql/V2_DESIGN.md` §8: a parsed query's `MATCH` items —
//! both plain `MATCH` clauses and the `OPTIONAL` operands that nest them — are
//! walked once, in textual order, to discover the query's bindings: the
//! user-named pattern variables plus the hidden `__e<n>`/`__v<n>` placeholders
//! that anonymous edges and nodes need so every element is addressable in the
//! binding rows. Each binding records its [`BindingKind`] (`Node`, `Edge`,
//! `EdgeGroup` under a quantifier, or `Path` for a path variable), whether the
//! user wrote it, and its `optional_depth` — `0` for a binding first declared in
//! a mandatory clause, `> 0` for one first declared inside an `OPTIONAL`
//! operand (the nesting depth of that operand).
//!
//! The registry owns the variable-resolution rules across the WHOLE query
//! (every pattern of every clause, in textual order), tracking first
//! declaration vs reuse:
//!
//! - a **node** variable reused across patterns / clauses resolves to the SAME [`BindingId`] — the
//!   shared binding becomes the equi-join key the planner joins on (PR-A rejected this; the
//!   rejection flips here);
//! - an **edge** (or group) variable may never be reused: under DIFFERENT EDGES (R2) the join would
//!   always be empty, so it is rejected outright;
//! - a **kind-mismatched** reuse (a node variable reused as an edge, etc.) is rejected;
//! - a path variable may never be reused;
//! - the **optional-rebind** rejection (V2_DESIGN §1): a node variable first bound INSIDE an
//!   `OPTIONAL` may not be re-declared in a mandatory clause (nor a shallower optional). The
//!   optional binding can be `Value::Null` on a miss (R3), so a mandatory pattern cannot anchor /
//!   join on it. The reverse — a mandatory binding *referenced* inside an optional — is a
//!   correlated constraint and is fine (the reference resolves to the existing mandatory binding).
//!
//! Anchorability (V2_DESIGN §0) is validated per pattern: every pattern needs
//! ≥1 labelled element OR ≥1 variable already bound by an earlier pattern /
//! clause. Name validation reuses [`naming`] verbatim.

use crate::expr::match_plan::{BindingDef, BindingId, BindingKind};
use crate::gql::ast::{
	EdgePattern, Ident, MatchClause, MatchItem, NodePattern, OptionalBlock, PathPattern,
	PathSearchKind,
};
use crate::gql::lower::naming;
use crate::syn::error::{SyntaxError, bail, syntax_error};

/// One declared binding plus the bookkeeping the semantic pass needs but the
/// final [`BindingDef`] does not carry.
pub(super) struct BindingInfo {
	pub(super) name: String,
	pub(super) kind: BindingKind,
	pub(super) user_named: bool,
	/// `0` if this binding was first declared in a mandatory clause; the
	/// `OPTIONAL` nesting depth (`> 0`) if it was first declared inside an
	/// `OPTIONAL` operand. Consulted by the `nullable()` amendment (V2_DESIGN §8):
	/// a bare `Variable(v)` is nullable iff `optional_depth(v) > 0`.
	pub(super) optional_depth: u32,
}

/// The bindings of a whole MATCH query, indexed by [`BindingId`].
///
/// Built by [`analyze`] across every pattern of every clause; consumed by
/// `pattern` (deps computation) and `mod` (output spec) to address every
/// pattern element uniformly by binding name. A reused node variable keeps the
/// id of its first declaration, so it lands as one binding shared between the
/// patterns / clauses that name it — the planner's equi-join key.
pub(super) struct Registry {
	bindings: Vec<BindingInfo>,
	/// `__e<n>`/`__v<n>` counters, kept on the registry so hidden names stay
	/// unique across the whole query.
	hidden_edges: u32,
	hidden_nodes: u32,
	/// The `OPTIONAL` nesting depth currently being walked (`0` outside any
	/// `OPTIONAL`). Stamped onto each binding at first declaration so the
	/// `nullable()` amendment can tell optional-bound variables apart.
	current_depth: u32,
}

impl Registry {
	fn new() -> Self {
		Registry {
			bindings: Vec::new(),
			hidden_edges: 0,
			hidden_nodes: 0,
			current_depth: 0,
		}
	}

	/// All bindings in declaration order; the index is the [`BindingId`].
	pub(super) fn bindings(&self) -> &[BindingInfo] {
		&self.bindings
	}

	/// The [`BindingDef`]s the [`MatchPlan`](crate::expr::match_plan::MatchPlan)
	/// carries, in [`BindingId`] order.
	pub(super) fn into_defs(self) -> Vec<BindingDef> {
		self.bindings
			.into_iter()
			.map(|b| BindingDef {
				name: b.name,
				kind: b.kind,
				user_named: b.user_named,
			})
			.collect()
	}

	/// Resolves a user variable reference (in a predicate, projection or sort
	/// key) to its [`BindingId`].
	pub(super) fn resolve(&self, ident: &Ident) -> Result<BindingId, SyntaxError> {
		self.lookup(&ident.name).ok_or_else(|| {
			syntax_error!(
				"Unknown variable `{}`",
				ident.name,
				@ident.span => "variables must be declared in the MATCH pattern"
			)
		})
	}

	/// The [`BindingKind`] of a binding by id.
	pub(super) fn kind(&self, id: BindingId) -> BindingKind {
		self.bindings[id as usize].kind
	}

	/// The name of a binding by id (the user variable, or the hidden
	/// `__e<n>`/`__v<n>`).
	pub(super) fn name(&self, id: BindingId) -> &str {
		&self.bindings[id as usize].name
	}

	/// The `optional_depth` of a binding by id (`0` mandatory; `> 0`
	/// optional-bound). The `nullable()` amendment (V2_DESIGN §8) treats a bare
	/// `Variable(v)` as nullable exactly when this is `> 0`.
	pub(super) fn optional_depth(&self, id: BindingId) -> u32 {
		self.bindings[id as usize].optional_depth
	}

	fn lookup(&self, name: &str) -> Option<BindingId> {
		self.bindings.iter().position(|b| b.name == name).map(|i| i as BindingId)
	}

	/// Declares a fresh binding at the current `OPTIONAL` depth and returns its
	/// id.
	fn declare(&mut self, name: String, kind: BindingKind, user_named: bool) -> BindingId {
		let id = self.bindings.len() as BindingId;
		self.bindings.push(BindingInfo {
			name,
			kind,
			user_named,
			optional_depth: self.current_depth,
		});
		id
	}

	/// Declares a hidden node binding (`__v<n>`).
	fn declare_hidden_node(&mut self) -> BindingId {
		let name = format!("__v{}", self.hidden_nodes);
		self.hidden_nodes += 1;
		self.declare(name, BindingKind::Node, false)
	}

	/// Reserves the next `__e<n>` name and bumps the counter.
	fn next_hidden_edge_name(&mut self) -> String {
		let name = format!("__e{}", self.hidden_edges);
		self.hidden_edges += 1;
		name
	}

	/// Looks up a binding id by name if declared — the `pub(super)` form of the
	/// internal [`Registry::lookup`], used by the `INSERT` lowering to decide
	/// whether a node references a variable already bound by the read body.
	pub(super) fn try_lookup(&self, name: &str) -> Option<BindingId> {
		self.lookup(name)
	}

	/// Resolves an `INSERT` endpoint that references a variable already bound by
	/// the read body, returning its id. Errors if the variable is unknown or is
	/// not a node binding (an edge/group/path variable cannot be an endpoint).
	pub(super) fn resolve_node_ref(&self, ident: &Ident) -> Result<BindingId, SyntaxError> {
		let id = self.resolve(ident)?;
		match self.kind(id) {
			BindingKind::Node => Ok(id),
			other => Err(kind_mismatch(ident, other, BindingKind::Node)),
		}
	}

	/// Declares a NEW node binding created by an `INSERT` (a fresh user variable
	/// or an anonymous node). Errors if the user variable is already bound.
	pub(super) fn declare_new_node(
		&mut self,
		var: Option<&Ident>,
	) -> Result<BindingId, SyntaxError> {
		match var {
			None => Ok(self.declare_hidden_node()),
			Some(ident) => {
				naming::validate_var(ident)?;
				if self.lookup(&ident.name).is_some() {
					return Err(syntax_error!(
						"Variable `{}` is already bound and cannot be created by INSERT",
						ident.name,
						@ident.span => "use a fresh variable for a new node, or drop the label and \
						 properties to reference the bound one as an edge endpoint"
					));
				}
				Ok(self.declare(ident.name.clone(), BindingKind::Node, true))
			}
		}
	}

	/// Declares a NEW edge binding created by an `INSERT`. An edge is always new
	/// (never a reuse). Errors if the user variable is already bound.
	pub(super) fn declare_new_edge(
		&mut self,
		var: Option<&Ident>,
	) -> Result<BindingId, SyntaxError> {
		match var {
			None => {
				let name = self.next_hidden_edge_name();
				Ok(self.declare(name, BindingKind::Edge, false))
			}
			Some(ident) => {
				naming::validate_var(ident)?;
				if self.lookup(&ident.name).is_some() {
					return Err(syntax_error!(
						"Variable `{}` is already bound and cannot be created by INSERT",
						ident.name,
						@ident.span => "use a fresh variable for a new edge"
					));
				}
				Ok(self.declare(ident.name.clone(), BindingKind::Edge, true))
			}
		}
	}
}

/// The resolved bindings of every pattern element of a pattern, in pattern
/// order, so the caller can build the [`PatternPlan`](crate::expr::match_plan::PatternPlan)
/// without re-walking the AST.
pub(super) struct PatternBindings {
	pub(super) path_var: Option<BindingId>,
	pub(super) start: BindingId,
	/// `(edge binding, far-node binding)` per hop.
	pub(super) steps: Vec<(BindingId, BindingId)>,
	/// Implied `.id` equalities from a node variable that repeats *within this
	/// single pattern* (e.g. the self-loop `(a)-[…]->(a)`). The second and later
	/// occurrences are rewritten to fresh hidden node bindings (so neither
	/// overwrites the other on the binding row); each `(first, repeat)` pair must
	/// be enforced by an `id`-equality conjunct, because — unlike a cross-pattern
	/// reuse, which the planner equi-joins — a single chain has no join to
	/// materialise the equality (V2_DESIGN §2 IR invariant: "repeated pattern
	/// variables rewritten to hidden bindings + equality conjuncts").
	pub(super) node_equalities: Vec<(BindingId, BindingId)>,
}

/// One clause flattened out of the `MatchItem` tree in textual order, carrying
/// the OPTIONAL metadata the IR needs alongside the per-pattern binding ids.
pub(super) struct ClauseBindings<'ast> {
	/// The AST clause this entry describes (so the lowering loop pairs the right
	/// patterns / predicates without re-walking the tree).
	pub(super) clause: &'ast MatchClause,
	/// The innermost `OPTIONAL` block this clause sits directly within (R3), or
	/// `None` for a mandatory clause; every clause of one block shares the id. This
	/// is the single source of truth for "is this clause optional"
	/// (`optional_group.is_some()`). See
	/// [`MatchClausePlan::optional_group`](crate::expr::match_plan::MatchClausePlan::optional_group).
	pub(super) optional_group: Option<u32>,
	/// The binding ids of each pattern of the clause, in pattern order.
	pub(super) patterns: Vec<PatternBindings>,
}

/// Bookkeeping threaded down the `MatchItem` walk: the next fresh `OPTIONAL`
/// block id to hand out (a dense per-query counter, group order == textual
/// order).
struct GroupCounter(u32);

impl GroupCounter {
	/// Reserve the next block id.
	fn next(&mut self) -> u32 {
		let id = self.0;
		self.0 += 1;
		id
	}
}

/// Drives the binding-resolution pass one step at a time, so reads and mutations
/// can interleave in textual order. The lowering calls [`Analyzer::read`] for a
/// `MATCH`/`OPTIONAL` step and declares `INSERT` bindings via
/// [`Analyzer::registry_mut`] for a mutation step, threading the same
/// [`Registry`] (and `OPTIONAL` block counter) across the whole query. A read
/// item's anchorability is therefore checked against everything declared before
/// it — including variables an earlier `INSERT` created.
///
/// Items, a clause's patterns, and nested operands are walked in textual order,
/// so "already bound by an earlier pattern / clause / INSERT" — the
/// anchorability alternative — is exactly the set of variables declared before
/// the current pattern began.
pub(super) struct Analyzer {
	registry: Registry,
	groups: GroupCounter,
}

impl Analyzer {
	pub(super) fn new() -> Self {
		Analyzer {
			registry: Registry::new(),
			groups: GroupCounter(0),
		}
	}

	/// Declares the bindings of one read step (a `MATCH` clause or an `OPTIONAL`
	/// operand), returning one [`ClauseBindings`] per flattened clause (a plain
	/// `MATCH` yields one; an `OPTIONAL` block yields its inner clauses, each
	/// tagged with the block id). The block counter persists across calls so
	/// `OPTIONAL` block ids stay unique across the query.
	pub(super) fn read<'ast>(
		&mut self,
		item: &'ast MatchItem,
	) -> Result<Vec<ClauseBindings<'ast>>, SyntaxError> {
		let mut out = Vec::new();
		analyze_items(
			&mut self.registry,
			std::slice::from_ref(item),
			None,
			&mut self.groups,
			&mut out,
		)?;
		Ok(out)
	}

	/// The registry built so far (read by clause and output lowering).
	pub(super) fn registry(&self) -> &Registry {
		&self.registry
	}

	/// The registry, mutably — an `INSERT` step declares its new bindings here.
	pub(super) fn registry_mut(&mut self) -> &mut Registry {
		&mut self.registry
	}

	/// Consumes the analyzer, yielding the completed registry (for the output
	/// spec and the final [`BindingDef`] list).
	pub(super) fn into_registry(self) -> Registry {
		self.registry
	}
}

/// Walks a sequence of `MatchItem`s in textual order, flattening plain clauses
/// into `out` and descending into each `OPTIONAL` operand (charging the registry
/// `OPTIONAL` depth and minting a fresh block id). `group` is the innermost
/// enclosing block id (`None` at top level).
fn analyze_items<'ast>(
	registry: &mut Registry,
	items: &'ast [MatchItem],
	group: Option<u32>,
	groups: &mut GroupCounter,
	out: &mut Vec<ClauseBindings<'ast>>,
) -> Result<(), SyntaxError> {
	for item in items {
		match item {
			MatchItem::Match(clause) => {
				let optional = group.is_some();
				// The leading clause of an OPTIONAL block is the first clause emitted
				// for that block id; its leading pattern, when bound-variable
				// anchored, expands off the OUTER accumulator and the planner realises
				// it only via the single-hop OptionalExpand fast path (see
				// `AnchorContext::OptionalBlockLeading`). A later clause of the same
				// block expands off the block's own subplan accumulator instead.
				let block_leading = optional && out.last().map(|c| c.optional_group) != Some(group);
				let patterns = analyze_clause(registry, clause, optional, block_leading)?;
				out.push(ClauseBindings {
					clause,
					optional_group: group,
					patterns,
				});
			}
			MatchItem::Optional(block) => analyze_optional(registry, block, groups, out)?,
		}
	}
	Ok(())
}

/// Descends into one `OPTIONAL` operand: mints a fresh block id, bumps the
/// registry's `OPTIONAL` depth (so bindings first declared inside record an
/// `optional_depth > 0`), and walks the operand's inner items as a unit. A
/// nested `OPTIONAL` mints its own id and bumps the depth again.
fn analyze_optional<'ast>(
	registry: &mut Registry,
	block: &'ast OptionalBlock,
	groups: &mut GroupCounter,
	out: &mut Vec<ClauseBindings<'ast>>,
) -> Result<(), SyntaxError> {
	let group = groups.next();
	registry.current_depth += 1;
	let result = analyze_items(registry, &block.items, Some(group), groups, out);
	registry.current_depth -= 1;
	result
}

/// Walks one clause's patterns, declaring their bindings and validating
/// anchorability for each. `optional` is whether the clause is the body of an
/// `OPTIONAL` operand; `block_leading` is whether this clause leads its OPTIONAL
/// block (its leading pattern expands off the OUTER accumulator via the single-
/// hop OptionalExpand fast path, so its bound-variable anchor is single-hop only
/// — see [`AnchorContext`]).
fn analyze_clause(
	registry: &mut Registry,
	clause: &MatchClause,
	optional: bool,
	block_leading: bool,
) -> Result<Vec<PatternBindings>, SyntaxError> {
	if clause.patterns.is_empty() {
		return Err(syntax_error!(
			"Internal error: MATCH clause without a pattern",
			@clause.span
		));
	}
	let mut patterns = Vec::with_capacity(clause.patterns.len());
	for (index, pattern) in clause.patterns.iter().enumerate() {
		// Anchor context for this pattern (mirrors the planner's anchor selection,
		// `exec/planner/match_plan.rs`):
		// - the leading pattern of a MANDATORY clause is planned as a standalone self-rooted
		//   sub-tree — no bound-variable anchor;
		// - the leading pattern of an OPTIONAL block's LEADING clause expands off the OUTER
		//   accumulator via OptionalExpand, which is single-hop only;
		// - everything else (a later pattern in a clause, or any pattern of a later clause of an
		//   optional block) expands off the within-subplan accumulator with the full bound-variable
		//   anchor set (any chain forward, single-hop reverse).
		let context = if index == 0 && !optional {
			AnchorContext::MandatoryLeading
		} else if index == 0 && block_leading {
			AnchorContext::OptionalBlockLeading
		} else {
			AnchorContext::Inner
		};
		patterns.push(analyze_pattern(registry, pattern, context)?);
	}
	Ok(patterns)
}

/// Walks a single pattern, declaring every element binding and validating that
/// the pattern is anchorable.
///
/// Anchorability is checked against the bindings present in the registry
/// *before* this pattern is walked: a pattern is anchorable if it carries a
/// labelled element, or if it reuses a variable already bound by an earlier
/// pattern / clause. The check runs first so the generalised message fires
/// before the pattern declares its own (so-far unbound) variables.
fn analyze_pattern(
	registry: &mut Registry,
	pattern: &PathPattern,
	context: AnchorContext,
) -> Result<PatternBindings, SyntaxError> {
	if !pattern_is_anchorable(registry, pattern) {
		bail!(
			"Cannot choose a starting table for this pattern: label at least one node or reuse a \
			 variable bound by an earlier pattern",
			@pattern.start.span => "label a node: `(n:label)`, or reuse an earlier variable"
		);
	}
	// A pattern can be anchorable in the abstract (some labelled / bound element)
	// yet sit outside the shapes the planner physically realises (its anchor set is
	// the V2_DESIGN §6 priority: labelled start node, single-hop non-quantified
	// labelled edge, or a bound start / single-hop bound far node — the last two
	// restricted by the anchor context). Reject the rest cleanly here rather than
	// surfacing a planner internal error downstream (the §6 contract: a
	// cleanly-lowered plan never internal-errors).
	if !pattern_is_realisable(registry, pattern, context) {
		bail!(
			"This MATCH pattern shape is not supported yet",
			@pattern.start.span => "anchor the pattern on a labelled start node `(n:label)`, a \
			 single labelled edge, or a node variable bound by an earlier pattern used as the \
			 pattern's start"
		);
	}

	// A path-search / path-mode prefix only attaches to a single variable-length
	// segment, and the selective forms additionally reject a self-loop start==end
	// (see `validate_path_prefix`).
	validate_path_prefix(pattern)?;

	// Node bindings already placed on THIS pattern's chain, with the implied
	// `.id` equalities a repeat within the chain produces (see
	// `PatternBindings::node_equalities`).
	let mut used: Vec<BindingId> = Vec::new();
	let mut node_equalities: Vec<(BindingId, BindingId)> = Vec::new();

	let start = declare_node(registry, &pattern.start, &mut used, &mut node_equalities)?;

	let mut steps = Vec::with_capacity(pattern.steps.len());
	for step in &pattern.steps {
		let edge = declare_edge(registry, &step.edge)?;
		let node = declare_node(registry, &step.node, &mut used, &mut node_equalities)?;
		steps.push((edge, node));
	}

	// The path variable is the last binding declared, so the user-named element
	// bindings keep the ids the deps computation expects. A path variable may
	// never be reused.
	let path_var = match &pattern.path_var {
		None => None,
		Some(ident) => {
			naming::validate_var(ident)?;
			if registry.lookup(&ident.name).is_some() {
				return Err(repeated_path_variable(ident));
			}
			Some(registry.declare(ident.name.clone(), BindingKind::Path, true))
		}
	};

	Ok(PatternBindings {
		path_var,
		start,
		steps,
		node_equalities,
	})
}

/// Validate a path pattern carrying a `pathPatternPrefix`. Any prefix requires a
/// single variable-length segment (`(a)-[:e]->{m,n}(b)`) — the only shape the
/// `PathExpand` / `ShortestPathExpand` operators realise. The selective forms
/// (everything but plain `ALL` / a bare path mode) additionally reject a
/// self-loop `start == end` binding, which collapses the per-endpoint partition
/// and trips the hidden-binding + id-equality self-loop rewrite. A reverse
/// anchor (the pattern bound on its far node) IS allowed: per-endpoint grouping
/// and path length are symmetric, and the operator flips the emitted path/group
/// back to written order.
fn validate_path_prefix(pattern: &PathPattern) -> Result<(), SyntaxError> {
	let Some(prefix) = pattern.prefix.as_ref() else {
		return Ok(());
	};
	let single_segment =
		matches!(pattern.steps.as_slice(), [step] if step.edge.quantifier.is_some());
	if !single_segment {
		bail!(
			"A path search prefix requires a single variable-length segment, e.g. \
			 `(a)-[:edge]->{{1,3}}(b)`",
			@prefix.span => "quantify exactly one edge (`{{m,n}}`, `+`, `*`)"
		);
	}
	if matches!(prefix.kind, PathSearchKind::All) {
		return Ok(());
	}
	let [step] = pattern.steps.as_slice() else {
		return Ok(());
	};
	if same_node_variable(&pattern.start, &step.node) {
		bail!(
			"A path search does not support a pattern whose start and end are the same node",
			@prefix.span => "use distinct start and end nodes"
		);
	}
	Ok(())
}

/// Whether two node elements name the same user-written variable — the self-loop
/// test for a path search. Anonymous nodes are always distinct bindings, so they
/// never count as the same node.
fn same_node_variable(a: &NodePattern, b: &NodePattern) -> bool {
	match (a.var.as_ref(), b.var.as_ref()) {
		(Some(a), Some(b)) => a.name == b.name,
		_ => false,
	}
}

/// Whether a pattern is anchorable given the bindings declared before it: it
/// carries ≥1 labelled element, or it reuses ≥1 variable already in the
/// registry (bound by an earlier pattern / clause).
fn pattern_is_anchorable(registry: &Registry, pattern: &PathPattern) -> bool {
	if pattern.start.label.is_some() {
		return true;
	}
	if node_reuses_bound_var(registry, &pattern.start) {
		return true;
	}
	for step in &pattern.steps {
		if step.edge.label.is_some() || step.node.label.is_some() {
			return true;
		}
		// A reused edge variable is rejected later, but as an anchorability
		// witness an already-bound node variable counts.
		if node_reuses_bound_var(registry, &step.node) {
			return true;
		}
	}
	false
}

/// Where a pattern sits relative to the accumulator it can anchor against, which
/// fixes its realisable bound-variable anchor set (mirrors the planner's anchor
/// selection, `exec/planner/match_plan.rs`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum AnchorContext {
	/// The leading pattern of a mandatory clause: planned as a standalone
	/// self-rooted sub-tree, so no bound-variable anchor is available.
	MandatoryLeading,
	/// The leading pattern of an OPTIONAL block's leading clause: it expands off
	/// the OUTER accumulator, but only via the single-hop `OptionalExpand` fast
	/// path, so a bound-variable anchor is realisable ONLY as a single,
	/// non-quantified hop. (A self-rootable shape — labelled start, single labelled
	/// edge — is fine too; that becomes the LeftJoin general path.)
	OptionalBlockLeading,
	/// Any pattern that expands off a WITHIN-subplan accumulator (a later pattern
	/// of a clause, or any pattern of a non-leading clause of an optional block):
	/// the full bound-variable anchor set (any chain forward, single-hop reverse).
	Inner,
}

/// Whether the planner can physically realise a pattern, given the bindings
/// declared before it and its [`AnchorContext`].
///
/// Mirrors the planner's anchor selection (`exec/planner/match_plan.rs`,
/// V2_DESIGN §5/§6) exactly, so a pattern that lowers never hits a planner
/// internal error. The realisable anchors are:
///
/// - a **labelled start node** (`(n:label)…`) — node-anchored, any chain length;
/// - a **single-hop, non-quantified, labelled edge** (`(…)-[:e]->(…)`) — edge-anchored (a
///   quantified or multi-hop edge anchor is not realised);
/// - a **bound start node** (forward) / **single-hop bound far node** (reverse) expansion off the
///   accumulator — its availability and chain length depend on the context:
///   - [`AnchorContext::MandatoryLeading`]: not available (the sub-tree must be standalone);
///   - [`AnchorContext::OptionalBlockLeading`]: a SINGLE non-quantified hop only (the
///     `OptionalExpand` fast path off the outer accumulator);
///   - [`AnchorContext::Inner`]: the full set (any chain forward, single-hop reverse).
///
/// Anything else (a label only mid-chain or on a far node, a multi-hop edge-only
/// anchor, a quantified edge anchor, a multi-hop reverse bound anchor, or a
/// multi-hop / quantified bound-variable anchor leading an optional block) is
/// anchorable in the abstract but out of planner scope.
///
/// A bound-variable expansion is additionally only realisable when the nodes it
/// *introduces* are not already bound by an earlier pattern / clause: the
/// expansion would write that node from the traversal target and silently
/// overwrite the existing (shared) binding instead of constraining it, dropping
/// the implied equality and over-returning. A node that merely repeats an
/// earlier element of the *same* pattern is fine — `declare_node` rewrites it to
/// a fresh hidden binding plus an `id`-equality (the self-loop machinery).
fn pattern_is_realisable(
	registry: &Registry,
	pattern: &PathPattern,
	context: AnchorContext,
) -> bool {
	// Labelled start node ⇒ node-anchored (any chain).
	if pattern.start.label.is_some() {
		return true;
	}
	// Single-hop, non-quantified, labelled edge ⇒ edge-anchored.
	if let [step] = pattern.steps.as_slice()
		&& step.edge.label.is_some()
		&& step.edge.quantifier.is_none()
	{
		return true;
	}

	// Bound-variable anchors, restricted by context.
	//
	// `forward_ok` decides whether a bound-START forward expansion of THIS chain is
	// realisable from the bound variable alone:
	// - within a subplan ([`AnchorContext::Inner`]) the planner walks any chain off the
	//   accumulator's rows, quantified or not;
	// - an optional block's leading clause expands off the OUTER accumulator only via the
	//   single-hop, non-quantified `OptionalExpand` (a quantified hop is a `PathExpand`, which the
	//   fast path does not build, and the standalone subplan cannot self-root an unlabelled
	//   quantified start);
	// - a mandatory leading clause has no bound-variable anchor.
	let forward_ok = match context {
		AnchorContext::MandatoryLeading => false,
		AnchorContext::OptionalBlockLeading => matches!(
			pattern.steps.as_slice(),
			[step] if step.edge.quantifier.is_none()
		),
		AnchorContext::Inner => true,
	};
	// A reverse bound-far anchor is always a single hop; it is available off a
	// within-subplan accumulator and, single-hop and unquantified, off the outer
	// accumulator (OptionalExpand reverse). A mandatory leading clause has none.
	let reverse_ok = !matches!(context, AnchorContext::MandatoryLeading);

	// Bound start node ⇒ forward expansion off the accumulator. The step
	// (introduced) nodes must not already be bound.
	if forward_ok
		&& node_reuses_bound_var(registry, &pattern.start)
		&& expansion_targets_are_fresh(registry, pattern, ExpandFrom::Start)
	{
		return true;
	}
	// Single-hop bound far node ⇒ reverse expansion off the accumulator. The
	// start (introduced) node must not already be bound. A reverse anchor off the
	// outer accumulator (OptionalExpand) is also single-hop, non-quantified.
	if reverse_ok
		&& let [step] = pattern.steps.as_slice()
		&& (!matches!(context, AnchorContext::OptionalBlockLeading)
			|| step.edge.quantifier.is_none())
		&& node_reuses_bound_var(registry, &step.node)
		&& expansion_targets_are_fresh(registry, pattern, ExpandFrom::Far)
	{
		return true;
	}
	false
}

/// Which end of a single-/multi-hop pattern a bound-variable expansion starts
/// from (the other end's nodes are the ones the expansion introduces).
#[derive(Clone, Copy)]
enum ExpandFrom {
	/// Forward: anchored on the start node; the step nodes are introduced.
	Start,
	/// Reverse: anchored on the single-hop far node; the start is introduced.
	Far,
}

/// Whether every node a bound-variable expansion *introduces* is fresh — i.e.
/// the expansion will not overwrite a binding already shared with an earlier
/// pattern / clause. A node is fresh when its variable is anonymous, not yet in
/// the registry, or merely repeats an earlier element of THIS pattern (which
/// `declare_node` rewrites to a hidden binding + `id`-equality, so no overwrite
/// occurs). A node reusing an earlier *cross-pattern* binding is NOT fresh: the
/// expansion would write it from the traversal target and drop the implied
/// equality (out of PR-B scope — reject rather than over-return).
fn expansion_targets_are_fresh(
	registry: &Registry,
	pattern: &PathPattern,
	from: ExpandFrom,
) -> bool {
	// Variables that appear earlier on this chain than a given node; a repeat of
	// one is a within-pattern self-reference the self-loop rewrite handles, so it
	// does not overwrite. The anchor end is never introduced, so we walk from the
	// start and treat each node's predecessors as "earlier".
	let mut earlier: Vec<&str> = Vec::new();
	if let Some(name) = pattern.start.var.as_ref().map(|i| i.name.as_str()) {
		earlier.push(name);
	}
	match from {
		// Forward: every step node is introduced by the expansion.
		ExpandFrom::Start => {
			for step in &pattern.steps {
				if overwrites_cross_pattern_binding(registry, &step.node, &earlier) {
					return false;
				}
				if let Some(name) = step.node.var.as_ref().map(|i| i.name.as_str()) {
					earlier.push(name);
				}
			}
			true
		}
		// Reverse (single hop): only the start node is introduced. It precedes no
		// earlier element, so it is fresh iff not bound cross-pattern.
		ExpandFrom::Far => !overwrites_cross_pattern_binding(registry, &pattern.start, &[]),
	}
}

/// Whether binding `node` from a traversal target would overwrite a binding
/// already shared with an earlier pattern / clause (true ⇒ unsafe). A node that
/// only repeats a variable `earlier` on the same chain is safe (self-loop
/// rewrite); an anonymous or query-fresh node is safe.
fn overwrites_cross_pattern_binding(
	registry: &Registry,
	node: &NodePattern,
	earlier: &[&str],
) -> bool {
	node.var.as_ref().is_some_and(|ident| {
		registry.lookup(&ident.name).is_some() && !earlier.contains(&ident.name.as_str())
	})
}

/// Whether a node element names a variable already present in the registry.
fn node_reuses_bound_var(registry: &Registry, node: &NodePattern) -> bool {
	node.var.as_ref().is_some_and(|ident| registry.lookup(&ident.name).is_some())
}

/// Declares (or reuses) the binding for a node element: the user variable if
/// named, else a hidden `__v<n>`.
///
/// A node variable reused *across* patterns / clauses resolves to its first
/// declaration's id — the shared binding the planner equi-joins on; a reuse
/// against a non-node binding is a kind mismatch and is rejected.
///
/// A node variable reused *within the same pattern* (the self-loop case,
/// `(a)-[…]->(a)`) is different: there is no join to materialise the implied
/// equality, and sharing the id would let the second occurrence overwrite the
/// first on the binding row (dropping the constraint and over-returning). So the
/// repeat is rewritten to a fresh hidden node binding and an `id`-equality is
/// recorded in `node_equalities` for the predicate pass to enforce
/// (V2_DESIGN §2). `used` tracks the node ids already on this pattern's chain.
fn declare_node(
	registry: &mut Registry,
	node: &NodePattern,
	used: &mut Vec<BindingId>,
	node_equalities: &mut Vec<(BindingId, BindingId)>,
) -> Result<BindingId, SyntaxError> {
	let id = match &node.var {
		None => registry.declare_hidden_node(),
		Some(ident) => {
			naming::validate_var(ident)?;
			match registry.lookup(&ident.name) {
				Some(existing) => {
					// A reuse against a non-node binding is a kind mismatch.
					let existing = reuse_as(registry, ident, existing, BindingKind::Node)?;
					// The optional-rebind rejection (V2_DESIGN §1): a node first
					// bound INSIDE an `OPTIONAL` cannot be re-declared at a
					// shallower `OPTIONAL` depth (a mandatory clause, or a less
					// deeply nested optional). On a miss it is `Value::Null` (R3),
					// so a shallower pattern cannot anchor / equi-join on it. A
					// reuse at the same-or-deeper depth is a fine correlated /
					// shared binding; a mandatory binding reused inside an optional
					// is fine too (its depth 0 ≤ current).
					if registry.optional_depth(existing) > registry.current_depth {
						return Err(optional_rebind(ident));
					}
					if used.contains(&existing) {
						// Repeat within this pattern: rewrite to a fresh hidden
						// node + an `id`-equality constraint (no join exists here).
						let hidden = registry.declare_hidden_node();
						node_equalities.push((existing, hidden));
						hidden
					} else {
						// First occurrence on this chain of a cross-pattern share.
						existing
					}
				}
				None => registry.declare(ident.name.clone(), BindingKind::Node, true),
			}
		}
	};
	used.push(id);
	Ok(id)
}

/// Declares the binding for an edge element. A quantified edge binds an
/// [`BindingKind::EdgeGroup`] (the ordered list of traversed edges, R4); an
/// unquantified edge binds an [`BindingKind::Edge`]. Anonymous edges get a
/// hidden `__e<n>` so the row stays addressable (and so DIFFERENT-EDGES can read
/// the edge ids).
///
/// A reused edge / group variable is always rejected: under DIFFERENT EDGES
/// (R2) an edge record may never bind twice, so an equi-join on a repeated edge
/// variable would always be empty. A reuse against a non-edge binding is a kind
/// mismatch and is rejected with the kind-mismatch message.
fn declare_edge(registry: &mut Registry, edge: &EdgePattern) -> Result<BindingId, SyntaxError> {
	let kind = if edge.quantifier.is_some() {
		BindingKind::EdgeGroup
	} else {
		BindingKind::Edge
	};
	match &edge.var {
		None => {
			let name = registry.next_hidden_edge_name();
			Ok(registry.declare(name, kind, false))
		}
		Some(ident) => {
			naming::validate_var(ident)?;
			if let Some(id) = registry.lookup(&ident.name) {
				let prior = registry.kind(id);
				// A reuse against a node / path binding is a kind mismatch; a
				// reuse against another edge / group binding is the
				// repeated-edge rejection (R2).
				return match prior {
					BindingKind::Edge | BindingKind::EdgeGroup => {
						Err(repeated_edge_variable(ident))
					}
					BindingKind::Node | BindingKind::Path => Err(kind_mismatch(ident, prior, kind)),
				};
			}
			Ok(registry.declare(ident.name.clone(), kind, true))
		}
	}
}

/// Resolves a variable reuse against an existing binding of the expected
/// `kind`, returning the shared id; a kind mismatch is rejected.
fn reuse_as(
	registry: &Registry,
	ident: &Ident,
	id: BindingId,
	expected: BindingKind,
) -> Result<BindingId, SyntaxError> {
	let prior = registry.kind(id);
	if prior == expected {
		Ok(id)
	} else {
		Err(kind_mismatch(ident, prior, expected))
	}
}

/// The rejection for a repeated edge / group variable (R2). Distinct from the
/// node-variable case, which flips to a join in PR-B.
fn repeated_edge_variable(ident: &Ident) -> SyntaxError {
	syntax_error!(
		"Edge variable `{}` cannot be repeated",
		ident.name,
		@ident.span => "under DIFFERENT EDGES an edge cannot bind twice, so the join is always empty"
	)
}

/// The rejection for reusing a path variable name (paths are never joined).
fn repeated_path_variable(ident: &Ident) -> SyntaxError {
	syntax_error!(
		"Path variable `{}` is declared more than once",
		ident.name,
		@ident.span => "use a fresh name for each path variable"
	)
}

/// The optional-rebind rejection (V2_DESIGN §1): re-declaring, in a mandatory
/// clause (or a shallower `OPTIONAL`), a node variable first bound inside an
/// `OPTIONAL`. The optional binding can be `Value::Null` on a miss (R3), so a
/// mandatory pattern cannot anchor / join on it.
fn optional_rebind(ident: &Ident) -> SyntaxError {
	syntax_error!(
		"Variable `{}` was first bound inside an OPTIONAL and cannot be re-declared outside it",
		ident.name,
		@ident.span => "a variable an OPTIONAL introduces may be NULL, so a mandatory pattern cannot \
		 reuse it; name the mandatory element differently"
	)
}

/// The rejection for reusing a variable as a different kind of element (a node
/// variable reused as an edge, etc.).
fn kind_mismatch(ident: &Ident, prior: BindingKind, found: BindingKind) -> SyntaxError {
	syntax_error!(
		"Variable `{}` is already bound as {} but reused as {}",
		ident.name,
		kind_label(prior),
		kind_label(found),
		@ident.span => "use a fresh name, or reuse the variable as the same kind of element"
	)
}

/// A human-readable label for a binding kind, for the kind-mismatch message.
fn kind_label(kind: BindingKind) -> &'static str {
	match kind {
		BindingKind::Node => "a node",
		BindingKind::Edge => "an edge",
		BindingKind::EdgeGroup => "an edge group",
		BindingKind::Path => "a path",
	}
}
