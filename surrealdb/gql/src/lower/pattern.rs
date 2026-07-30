//! Pattern lowering: from a parsed MATCH clause to the declarative
//! [`MatchClausePlan`] / [`PatternPlan`].
//!
//! Implements `doc/gql/V2_DESIGN.md` §8. Every path pattern of the clause
//! (comma-separated patterns lower side by side) becomes a [`PatternPlan`] of
//! [`NodeStep`]/[`EdgeStep`] elements with its full multi-hop chain intact, and
//! the clause's predicates — the explicit `WHERE`, the inline element `WHERE`s
//! and the property-map equalities of every pattern — are merged, split into
//! NNF conjuncts (pushing `NOT` through `AND`/`OR`, never distributing ORs) and
//! lowered into a flat list of [`MatchPredicate`]s, each carrying the exact set
//! of bindings it reads (`deps`). The lowering does NOT decide joins: it only
//! declares the shared [`BindingId`]s (a reused node variable is one binding)
//! and the dependency sets; the planner owns join construction and conjunct
//! placement.
//!
//! Quantifier validation follows R6: the full set `* + ? {n} {n,m} {n,} {,m}
//! {,}` is legal; only `max < min` is rejected. A quantified edge binds an
//! edge group (R4) and may carry an inline predicate that references that edge
//! alone — a cross-variable reference is rejected because the per-path
//! traversal has no place to evaluate it.

use reblessive::Stk;
use surrealdb_strand::TableName;

use crate::expr::match_plan::{
	BindingId, EdgeQuantifier, EdgeStep, ExpandDirection, MatchClausePlan, MatchPredicate,
	NodeStep, PathMode as IrPathMode, PathPrefixPlan, PathSearch, PatternPlan,
};
use crate::expr::{BinaryOperator, Expr, Idiom, Part};
use crate::gql::ast::{
	BinaryOp, EdgeDirection, EdgePattern, ElementPredicate, GqlExpr, Ident, LabelExpr, MatchClause,
	PathMode, PathPattern, PathPatternPrefix, PathSearchKind, Quantifier, QuantifierKind, UnaryOp,
};
use crate::gql::lower::binding::{ClauseBindings, PatternBindings, Registry};
use crate::gql::lower::expr::{self, Scope};
use crate::syn::error::{SyntaxError, bail};
use crate::syn::token::Span;

/// Lowers one (flattened) MATCH clause into its [`MatchClausePlan`].
///
/// `clause_bindings` carries the AST clause, its OPTIONAL metadata (`optional` /
/// `optional_group`) and the per-pattern binding ids, all produced by
/// [`binding::analyze`](crate::gql::lower::binding::analyze); the
/// per-element binding ids let the steps and the property-map conjuncts be
/// built without re-walking the AST for resolution.
///
/// R3 conjunct ownership: a clause's predicates are owned by the clause whose
/// pattern scope introduces their bindings (the PR-B per-clause ownership
/// contract). For an `OPTIONAL` clause this is structural — a predicate written
/// inside the optional attaches here and so compiles inside the optional's own
/// subplan (pre-null), while a later clause's predicate that merely references
/// an optional binding is owned by THAT later clause (post-null). The lowering
/// only records the deps; the planner places each predicate at the earliest
/// stage that binds its deps within its owning clause's subplan.
pub(super) async fn lower_clause(
	stk: &mut Stk,
	clause_bindings: &ClauseBindings<'_>,
	registry: &Registry,
) -> Result<MatchClausePlan, SyntaxError> {
	let clause = clause_bindings.clause;
	let pattern_bindings = clause_bindings.patterns.as_slice();
	let mut patterns = Vec::with_capacity(clause.patterns.len());
	for (pattern, bindings) in clause.patterns.iter().zip(pattern_bindings.iter()) {
		patterns.push(build_pattern_plan(pattern, bindings)?);
	}
	let mut predicates = lower_predicates(stk, clause, registry, pattern_bindings).await?;
	// A node variable repeated within a single pattern (e.g. the self-loop
	// `(a)-[…]->(a)`) was rewritten to a fresh hidden binding by `binding`; emit
	// the implied `id`-equality so the planner enforces it (the chain has no join
	// to materialise it — see `PatternBindings::node_equalities`).
	for bindings in pattern_bindings {
		for &(first, repeat) in &bindings.node_equalities {
			predicates.push(node_id_equality(registry, first, repeat));
		}
	}
	// A selective path search (`ANY` / `*SHORTEST`) picks among the paths to each
	// endpoint by count/length; a predicate over its edge-group or path value
	// cannot be honoured by that per-terminal selection (it would post-filter the
	// already-chosen representative and silently drop endpoints), so reject it.
	// Property access on group/path vars is already rejected upstream; this guards
	// the residual bare-reference forms.
	reject_selective_group_predicates(clause, pattern_bindings, &predicates)?;
	Ok(MatchClausePlan {
		optional_group: clause_bindings.optional_group,
		patterns,
		predicates,
	})
}

/// Rejects a predicate that reads the edge-group or path binding of a pattern
/// carrying a selective (`ANY` / `*SHORTEST`) path search — see the call site.
fn reject_selective_group_predicates(
	clause: &MatchClause,
	pattern_bindings: &[PatternBindings],
	predicates: &[MatchPredicate],
) -> Result<(), SyntaxError> {
	for (pattern, bindings) in clause.patterns.iter().zip(pattern_bindings.iter()) {
		let Some(prefix) = pattern.prefix.as_ref() else {
			continue;
		};
		if matches!(prefix.kind, PathSearchKind::All) {
			continue;
		}
		let group_binding = bindings.steps.first().map(|&(edge, _)| edge);
		for predicate in predicates {
			let touches_group = group_binding.is_some_and(|g| predicate.deps.contains(&g));
			let touches_path = bindings.path_var.is_some_and(|p| predicate.deps.contains(&p));
			if touches_group || touches_path {
				bail!(
					"A path search does not support a predicate over its edge group or path value",
					@prefix.span => "constrain the endpoint nodes instead"
				);
			}
		}
	}
	Ok(())
}

/// Builds the `<first>.id = <repeat>.id` equality [`MatchPredicate`] for a node
/// variable that repeats within one pattern. Both bindings hold full node
/// objects, so `.id` extracts the record id (no fetch); the planner places it as
/// a `Filter` at the earliest stage that binds both.
fn node_id_equality(registry: &Registry, first: BindingId, repeat: BindingId) -> MatchPredicate {
	let id_idiom = |id: BindingId| {
		Expr::Idiom(Idiom(vec![
			Part::Field(registry.name(id).to_owned().into()),
			Part::Field("id".to_owned().into()),
		]))
	};
	let mut deps = vec![first, repeat];
	deps.sort_unstable();
	MatchPredicate {
		expr: Expr::Binary {
			left: Box::new(id_idiom(first)),
			op: BinaryOperator::Equal,
			right: Box::new(id_idiom(repeat)),
		},
		deps,
	}
}

/// Assembles the [`PatternPlan`] from the AST pattern and the pre-resolved
/// binding ids, validating edge directions, labels and quantifiers. Multi-hop
/// chains are kept whole (the planner chains the Expands).
fn build_pattern_plan(
	pattern: &PathPattern,
	pattern_bindings: &PatternBindings,
) -> Result<PatternPlan, SyntaxError> {
	let start = NodeStep {
		binding: pattern_bindings.start,
		label: label_table(&pattern.start.label)?,
	};

	let mut steps = Vec::with_capacity(pattern.steps.len());
	for (step, &(edge_binding, node_binding)) in
		pattern.steps.iter().zip(pattern_bindings.steps.iter())
	{
		let edge = &step.edge;
		let direction = edge_direction(edge)?;
		let quantifier = match &edge.quantifier {
			None => None,
			Some(quantifier) => Some(lower_quantifier(quantifier)?),
		};
		let edge_step = EdgeStep {
			binding: edge_binding,
			label: label_table(&edge.label)?,
			direction,
			quantifier,
		};
		let node_step = NodeStep {
			binding: node_binding,
			label: label_table(&step.node.label)?,
		};
		steps.push((edge_step, node_step));
	}

	Ok(PatternPlan {
		path_var: pattern_bindings.path_var,
		search: pattern.prefix.as_ref().map(lower_path_prefix),
		start,
		steps,
	})
}

/// Lowers an AST path-pattern prefix to the IR [`PathPrefixPlan`], resolving the
/// optional path/group counts (omitted ⇒ 1) and defaulting the path mode to
/// `WALK`. The structural / anchoring rules a prefix must satisfy are enforced
/// in `binding.rs` (`validate_path_prefix`).
fn lower_path_prefix(prefix: &PathPatternPrefix) -> PathPrefixPlan {
	let search = match prefix.kind {
		PathSearchKind::All => PathSearch::All,
		PathSearchKind::Any {
			count,
		} => PathSearch::Any {
			count: count.unwrap_or(1),
		},
		PathSearchKind::AllShortest => PathSearch::AllShortest,
		PathSearchKind::AnyShortest => PathSearch::AnyShortest,
		PathSearchKind::ShortestCounted {
			count,
		} => PathSearch::ShortestCounted {
			count,
		},
		PathSearchKind::ShortestGroups {
			count,
		} => PathSearch::ShortestGroups {
			count: count.unwrap_or(1),
		},
	};
	let mode = match prefix.mode {
		None | Some(PathMode::Walk) => IrPathMode::Walk,
		Some(PathMode::Trail) => IrPathMode::Trail,
		Some(PathMode::Simple) => IrPathMode::Simple,
		Some(PathMode::Acyclic) => IrPathMode::Acyclic,
	};
	PathPrefixPlan {
		search,
		mode,
	}
}

/// Maps an edge pattern's direction onto the lowered [`ExpandDirection`],
/// rejecting the undirected and multi-directional forms (out of scope).
fn edge_direction(edge: &EdgePattern) -> Result<ExpandDirection, SyntaxError> {
	match edge.direction {
		EdgeDirection::Right => Ok(ExpandDirection::Out),
		EdgeDirection::Left => Ok(ExpandDirection::In),
		EdgeDirection::Undirected
		| EdgeDirection::LeftOrUndirected
		| EdgeDirection::UndirectedOrRight
		| EdgeDirection::LeftOrRight
		| EdgeDirection::Any => bail!(
			"Undirected and multi-directional edge patterns are not supported yet",
			@edge.span => "use a directed edge: `-[…]->` or `<-[…]-`"
		),
	}
}

/// Extracts the single label name of a node or edge as a [`TableName`],
/// rejecting label expressions, which have no table mapping (out of scope).
fn label_table(label: &Option<LabelExpr>) -> Result<Option<TableName>, SyntaxError> {
	match label {
		None => Ok(None),
		Some(LabelExpr::Name(ident)) => Ok(Some(TableName::new(ident.name.clone()))),
		Some(other) => bail!(
			"Label expressions (`!`, `&`, `|`, `%`) are not supported yet",
			@other.span() => "use a single label name"
		),
	}
}

/// Lowers a graph-pattern quantifier onto an [`EdgeQuantifier`] per R6: the
/// full quantifier set is legal, with only `max < min` rejected. `*`/`+`/`?`
/// expand to their `{min,max}` equivalents.
fn lower_quantifier(quantifier: &Quantifier) -> Result<EdgeQuantifier, SyntaxError> {
	let (min, max) = match quantifier.kind {
		QuantifierKind::Star => (0, None),
		QuantifierKind::Plus => (1, None),
		QuantifierKind::Question => (0, Some(1)),
		QuantifierKind::Fixed(n) => (n, Some(n)),
		QuantifierKind::Range(min, max) => (min.unwrap_or(0), max),
	};
	if let Some(max) = max
		&& max < min
	{
		bail!(
			"The quantifier maximum must not be smaller than its minimum",
			@quantifier.span
		);
	}
	Ok(EdgeQuantifier {
		min,
		max,
	})
}

/// A single predicate conjunct awaiting lowering.
enum Conjunct<'a> {
	/// A user-written predicate, with the pending NNF negation.
	Expr {
		expr: &'a GqlExpr,
		negated: bool,
	},
	/// A property-map equality attached to a pattern element, addressed by the
	/// element's binding (id + name), so anonymous elements still lower.
	Prop {
		/// The element's binding id (for `deps`).
		binding: BindingId,
		/// The element's binding name (for the lowered idiom field).
		binding_name: &'a str,
		key: &'a Ident,
		value: &'a GqlExpr,
	},
}

/// Merges the predicate sources in contract order — the explicit clause
/// `WHERE`, then the inline node/edge `WHERE`s of every pattern, then the
/// property-map equalities of every pattern — splits each into NNF conjuncts,
/// and lowers each into a [`MatchPredicate`] with its dependency set. A
/// conjunct may reference bindings from several patterns (a cross-pattern
/// predicate); the planner places it post-join.
async fn lower_predicates(
	stk: &mut Stk,
	clause: &MatchClause,
	registry: &Registry,
	pattern_bindings: &[PatternBindings],
) -> Result<Vec<MatchPredicate>, SyntaxError> {
	let conjuncts = collect_conjuncts(
		clause.where_clause.as_ref(),
		&clause.patterns,
		pattern_bindings,
		registry,
	);
	let scope = Scope {
		registry,
		// Aggregates are not allowed in WHERE predicates or property equalities.
		allow_aggregates: false,
	};
	let mut out = Vec::with_capacity(conjuncts.len());
	for conjunct in &conjuncts {
		let deps = conjunct_deps(conjunct, registry)?;
		// Cross-variable references inside a quantified edge's inline predicate
		// have no per-path place to evaluate; reject them (R6).
		validate_quantified_edge_conjunct(conjunct, &deps, &clause.patterns, pattern_bindings)?;
		let lowered = match conjunct {
			Conjunct::Expr {
				expr: predicate,
				negated,
			} => expr::lower_predicate(stk, predicate, *negated, &scope).await?,
			Conjunct::Prop {
				binding,
				binding_name,
				key,
				value,
			} => expr::lower_prop_equality(stk, *binding, binding_name, key, value, &scope).await?,
		};
		out.push(MatchPredicate {
			// The lowering builds `sql::Expr`; the IR is binding-row `expr::Expr`.
			expr: lowered.into(),
			deps,
		});
	}
	Ok(out)
}

/// Merges and NNF-splits the clause's predicate sources into conjuncts: the
/// explicit `WHERE` first, then — pattern by pattern, in textual order — the
/// inline element `WHERE`s and finally the property-map equalities. Property
/// maps are addressed by their element's binding (id + name), recovered from
/// `pattern_bindings` and the registry, so anonymous elements lower too.
fn collect_conjuncts<'a>(
	where_clause: Option<&'a GqlExpr>,
	patterns: &'a [PathPattern],
	pattern_bindings: &[PatternBindings],
	registry: &'a Registry,
) -> Vec<Conjunct<'a>> {
	let mut out = Vec::new();
	if let Some(where_clause) = where_clause {
		split_conjuncts(where_clause, &mut out);
	}
	// Inline element WHEREs, pattern by pattern, in element order.
	for pattern in patterns {
		collect_element_where(&pattern.start.predicate, &mut out);
		for step in &pattern.steps {
			collect_element_where(&step.edge.predicate, &mut out);
			collect_element_where(&step.node.predicate, &mut out);
		}
	}
	// Property-map equalities, pattern by pattern, in element order, addressed
	// by binding.
	for (pattern, bindings) in patterns.iter().zip(pattern_bindings.iter()) {
		collect_element_props(&pattern.start.predicate, bindings.start, registry, &mut out);
		for (step, &(edge_binding, node_binding)) in pattern.steps.iter().zip(bindings.steps.iter())
		{
			collect_element_props(&step.edge.predicate, edge_binding, registry, &mut out);
			collect_element_props(&step.node.predicate, node_binding, registry, &mut out);
		}
	}
	out
}

/// Pushes an element's inline `WHERE` (if any) into the conjunct list,
/// NNF-split. Property maps are handled separately.
fn collect_element_where<'a>(predicate: &'a Option<ElementPredicate>, out: &mut Vec<Conjunct<'a>>) {
	if let Some(ElementPredicate::Where(expr)) = predicate {
		split_conjuncts(expr, out);
	}
}

/// Pushes an element's property-map equalities (if any) into the conjunct
/// list, addressed by the element's binding.
fn collect_element_props<'a>(
	predicate: &'a Option<ElementPredicate>,
	binding: BindingId,
	registry: &'a Registry,
	out: &mut Vec<Conjunct<'a>>,
) {
	if let Some(ElementPredicate::Props(props)) = predicate {
		let binding_name = registry.name(binding);
		for (key, value) in props {
			out.push(Conjunct::Prop {
				binding,
				binding_name,
				key,
				value,
			});
		}
	}
}

/// Splits an expression into top-level conjuncts, pushing `NOT` through
/// `AND`/`OR` (De Morgan) so that conjuncts hidden under negation are
/// classified independently. ORs are never distributed.
fn split_conjuncts<'a>(expr: &'a GqlExpr, out: &mut Vec<Conjunct<'a>>) {
	let mut stack: Vec<(&'a GqlExpr, bool)> = vec![(expr, false)];
	while let Some((expr, negated)) = stack.pop() {
		match expr {
			GqlExpr::Unary {
				op: UnaryOp::Not,
				expr,
				..
			} => stack.push((expr, !negated)),
			GqlExpr::Binary {
				op: BinaryOp::And,
				left,
				right,
				..
			} if !negated => {
				stack.push((right, negated));
				stack.push((left, negated));
			}
			GqlExpr::Binary {
				op: BinaryOp::Or,
				left,
				right,
				..
			} if negated => {
				stack.push((right, negated));
				stack.push((left, negated));
			}
			_ => out.push(Conjunct::Expr {
				expr,
				negated,
			}),
		}
	}
}

/// Computes the binding dependencies of a conjunct: the set of bindings every
/// variable reference in it resolves to, sorted and deduped.
fn conjunct_deps(
	conjunct: &Conjunct<'_>,
	registry: &Registry,
) -> Result<Vec<BindingId>, SyntaxError> {
	let mut deps: Vec<BindingId> = Vec::new();
	match conjunct {
		Conjunct::Expr {
			expr,
			..
		} => {
			walk_variables(expr, &mut |ident, _| {
				let id = registry.resolve(ident)?;
				if !deps.contains(&id) {
					deps.push(id);
				}
				Ok(())
			})?;
		}
		Conjunct::Prop {
			binding,
			value,
			..
		} => {
			deps.push(*binding);
			walk_variables(value, &mut |ident, _| {
				let id = registry.resolve(ident)?;
				if !deps.contains(&id) {
					deps.push(id);
				}
				Ok(())
			})?;
		}
	}
	deps.sort_unstable();
	Ok(deps)
}

/// Rejects a conjunct that references a quantified edge group together with any
/// other binding: the per-path traversal cannot evaluate such a predicate (R6).
/// A conjunct referencing only the quantified edge is permitted. Every pattern
/// of the clause is scanned for quantified edges.
fn validate_quantified_edge_conjunct(
	conjunct: &Conjunct<'_>,
	deps: &[BindingId],
	patterns: &[PathPattern],
	pattern_bindings: &[PatternBindings],
) -> Result<(), SyntaxError> {
	for (pattern, bindings) in patterns.iter().zip(pattern_bindings.iter()) {
		for (step, &(edge_binding, _)) in pattern.steps.iter().zip(bindings.steps.iter()) {
			if step.edge.quantifier.is_none() {
				continue;
			}
			if deps.contains(&edge_binding) && deps.iter().any(|d| *d != edge_binding) {
				bail!(
					"A predicate inside a quantified edge may only reference that edge",
					@conjunct_span(conjunct) => "remove the references to other variables"
				);
			}
		}
	}
	Ok(())
}

/// The source span of a conjunct, for error reporting.
fn conjunct_span(conjunct: &Conjunct<'_>) -> Span {
	match conjunct {
		Conjunct::Expr {
			expr,
			..
		} => expr.span(),
		Conjunct::Prop {
			value,
			..
		} => value.span(),
	}
}

/// Visits every variable reference in an expression, flagging whether the
/// reference is bare or the base of a property access chain. Iterative: the
/// parser builds arbitrarily deep linear chains.
fn walk_variables<'a>(
	expr: &'a GqlExpr,
	visit: &mut impl FnMut(&'a Ident, bool) -> Result<(), SyntaxError>,
) -> Result<(), SyntaxError> {
	let mut stack = vec![expr];
	while let Some(e) = stack.pop() {
		match e {
			GqlExpr::Variable(ident) => visit(ident, true)?,
			GqlExpr::Property(base, _, _) => {
				let mut base = &**base;
				while let GqlExpr::Property(inner, _, _) = base {
					base = inner;
				}
				if let GqlExpr::Variable(ident) = base {
					visit(ident, false)?;
				} else {
					stack.push(base);
				}
			}
			GqlExpr::Unary {
				expr,
				..
			}
			| GqlExpr::IsBool {
				expr,
				..
			}
			| GqlExpr::IsNull {
				expr,
				..
			} => stack.push(expr),
			GqlExpr::Binary {
				left,
				right,
				..
			} => {
				stack.push(right);
				stack.push(left);
			}
			GqlExpr::FunctionCall {
				args,
				..
			} => stack.extend(args.iter()),
			GqlExpr::List(items, _) => stack.extend(items.iter()),
			GqlExpr::Map(fields, _) => stack.extend(fields.iter().map(|(_, value)| value)),
			GqlExpr::Literal(..)
			| GqlExpr::Param {
				..
			} => {}
		}
	}
	Ok(())
}
