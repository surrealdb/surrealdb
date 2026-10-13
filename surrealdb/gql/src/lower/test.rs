//! Snapshot tests for the GQL → [`MatchPlan`] lowering.
//!
//! Each lowering test pins a deterministic, compact rendering of the lowered
//! [`MatchPlan`] (binding table, pattern, NNF-split predicates with their
//! dependency sets, and the output spec), verified against the construction
//! rules of `doc/gql/V2_DESIGN.md` §8. Predicate, column and order slots
//! render via [`Expr`]'s `ToSql` so the 3VL guard shapes stay literally
//! visible. Each rejection test pins both the error message and the source
//! slice its span covers.

use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Expr;
use crate::expr::match_plan::{
	BindingDef, BindingId, BindingKind, DetachMode, EdgeQuantifier, ExpandDirection,
	MatchClausePlan, MatchPlan, MatchStage, MutationStage, NodeStep, PatternPlan, UpdateData,
};
use crate::expr::plan::TopLevelExpr;
use crate::gql::{GqlParserSettings, lower, parse_str, parse_to_plan_with_settings};

// ------------------------------------------------------------------------
// Test harness: a compact, deterministic rendering of a `MatchPlan`.
// ------------------------------------------------------------------------

/// Parses and lowers a GQL query and returns the lowered [`MatchPlan`].
fn lower_plan(source: &str) -> MatchPlan {
	let query = match parse_str(source) {
		Ok(query) => query,
		Err(e) => panic!("failed to parse {source:?}: {:?}", e.render_on(source)),
	};
	let prepared = match lower(query) {
		Ok(prepared) => prepared,
		Err(e) => panic!("failed to lower {source:?}: {:?}", e.render_on(source)),
	};
	match prepared.0.expressions.as_slice() {
		[TopLevelExpr::Expr(Expr::Match(plan))] => (**plan).clone(),
		other => panic!("unexpected lowered plan for {source:?}: {other:?}"),
	}
}

/// Parses and lowers, returning the compact rendering of the lowered plan.
fn render(source: &str) -> String {
	render_plan(&lower_plan(source))
}

/// Renders an [`Expr`] (predicate / column / order slot) the way the IR does:
/// single-line via `ToSql`, so guard shapes stay diffable.
fn render_expr(expr: &Expr) -> String {
	let mut out = String::new();
	expr.fmt_sql(&mut out, SqlFormat::SingleLine);
	out
}

/// A compact, deterministic rendering of a [`MatchPlan`]:
///
/// ```text
/// bindings: a:Node k:Edge b:Node
/// MATCH (a:person)-[k:knows]->(b:person)
///   WHERE [k] k.since != NONE AND k.since != NULL AND k.since > 2020
/// RETURN a.name AS a_name, b.name AS b_name
/// ```
fn render_plan(plan: &MatchPlan) -> String {
	let mut out = String::new();
	out.push_str("bindings:");
	for binding in &plan.bindings {
		out.push(' ');
		out.push_str(&render_binding(binding));
	}
	out.push('\n');

	for stage in &plan.stages {
		match stage {
			MatchStage::Read(clause) => render_clause(plan, clause, &mut out),
			MatchStage::Mutate(mutation) => render_mutation(plan, mutation, &mut out),
		}
	}

	render_output(plan, &mut out);
	out
}

/// Renders one mutation stage as a compact, single-line step so interleaved
/// read/write programs stay diffable in the lowering snapshots.
fn render_mutation(plan: &MatchPlan, mutation: &MutationStage, out: &mut String) {
	let name = |id: BindingId| name_of(plan, id).to_owned();
	match mutation {
		MutationStage::Update {
			target,
			data,
		} => match data {
			UpdateData::Set(items) => {
				let assigns: Vec<String> = items
					.iter()
					.map(|(idiom, expr)| format!("{} = {}", idiom.to_sql(), render_expr(expr)))
					.collect();
				out.push_str(&format!("SET [{}] {}\n", name(*target), assigns.join(", ")));
			}
			UpdateData::Unset(paths) => {
				let paths: Vec<String> = paths.iter().map(|idiom| idiom.to_sql()).collect();
				out.push_str(&format!("REMOVE [{}] {}\n", name(*target), paths.join(", ")));
			}
			UpdateData::Content(expr) => {
				out.push_str(&format!("SET [{}] = {}\n", name(*target), render_expr(expr)));
			}
		},
		MutationStage::Delete {
			target,
			detach,
		} => {
			let mode = match detach {
				DetachMode::Detach => "DETACH ",
				DetachMode::NoDetach => "",
			};
			out.push_str(&format!("{mode}DELETE [{}]\n", name(*target)));
		}
		MutationStage::Insert(stage) => {
			let nodes: Vec<String> = stage
				.nodes
				.iter()
				.map(|node| format!("({}:{})", name(node.binding), node.label.as_str()))
				.collect();
			let edges: Vec<String> = stage
				.edges
				.iter()
				.map(|edge| {
					format!(
						"({})-[{}:{}]->({})",
						name(edge.from),
						name(edge.binding),
						edge.label.as_str(),
						name(edge.to),
					)
				})
				.collect();
			out.push_str("INSERT ");
			out.push_str(&nodes.join(" "));
			if !edges.is_empty() {
				if !nodes.is_empty() {
					out.push(' ');
				}
				out.push_str(&edges.join(" "));
			}
			out.push('\n');
		}
	}
}

/// Renders one binding as `name:Kind` (a hidden binding is suffixed `*`).
fn render_binding(binding: &BindingDef) -> String {
	let kind = match binding.kind {
		BindingKind::Node => "Node",
		BindingKind::Edge => "Edge",
		BindingKind::EdgeGroup => "EdgeGroup",
		BindingKind::Path => "Path",
	};
	let hidden = if binding.user_named {
		""
	} else {
		"*"
	};
	format!("{}:{kind}{hidden}", binding.name)
}

fn render_clause(plan: &MatchPlan, clause: &MatchClausePlan, out: &mut String) {
	// A clause is optional exactly when it carries a block id (the single source of
	// truth). Render the all-or-nothing block id alongside the OPTIONAL keyword so
	// the snapshots pin block grouping (clauses sharing an id are one left-outer
	// unit; distinct ids chain left-to-right); a mandatory clause renders neither.
	if let Some(group) = clause.optional_group {
		out.push_str(&format!("OPTIONAL#{group} "));
	}
	out.push_str("MATCH ");
	for (i, pattern) in clause.patterns.iter().enumerate() {
		if i > 0 {
			out.push_str(", ");
		}
		render_pattern(plan, pattern, out);
	}
	out.push('\n');
	for predicate in &clause.predicates {
		out.push_str("  WHERE [");
		out.push_str(&render_deps(plan, &predicate.deps));
		out.push_str("] ");
		out.push_str(&render_expr(&predicate.expr));
		out.push('\n');
	}
}

/// Renders the dependency set as space-joined binding names in id order.
fn render_deps(plan: &MatchPlan, deps: &[u32]) -> String {
	deps.iter()
		.map(|id| plan.bindings.get(*id as usize).map(|b| b.name.as_str()).unwrap_or("?"))
		.collect::<Vec<_>>()
		.join(" ")
}

fn render_pattern(plan: &MatchPlan, pattern: &PatternPlan, out: &mut String) {
	if let Some(path) = pattern.path_var {
		out.push_str(name_of(plan, path));
		out.push_str(" = ");
	}
	render_node(plan, &pattern.start, out);
	for (edge, node) in &pattern.steps {
		match edge.direction {
			ExpandDirection::Out => out.push('-'),
			ExpandDirection::In => out.push_str("<-"),
		}
		out.push('[');
		out.push_str(name_of(plan, edge.binding));
		if let Some(label) = &edge.label {
			out.push(':');
			out.push_str(label.as_str());
		}
		out.push(']');
		match edge.direction {
			ExpandDirection::Out => out.push_str("->"),
			ExpandDirection::In => out.push('-'),
		}
		if let Some(quantifier) = &edge.quantifier {
			render_quantifier(quantifier, out);
		}
		render_node(plan, node, out);
	}
}

fn render_node(plan: &MatchPlan, node: &NodeStep, out: &mut String) {
	out.push('(');
	out.push_str(name_of(plan, node.binding));
	if let Some(label) = &node.label {
		out.push(':');
		out.push_str(label.as_str());
	}
	out.push(')');
}

fn render_quantifier(quantifier: &EdgeQuantifier, out: &mut String) {
	out.push('{');
	out.push_str(&quantifier.min.to_string());
	out.push(',');
	if let Some(max) = quantifier.max {
		out.push_str(&max.to_string());
	}
	out.push('}');
}

fn render_output(plan: &MatchPlan, out: &mut String) {
	// The lowering test harness only lowers read queries, which always carry an
	// output spec.
	let output = plan.output.as_ref().expect("a read query carries a RETURN output");
	out.push_str("RETURN");
	if output.distinct {
		out.push_str(" DISTINCT");
	}
	for (i, column) in output.columns.iter().enumerate() {
		if i > 0 {
			out.push(',');
		}
		out.push(' ');
		out.push_str(&render_expr(&column.expr));
		out.push_str(" AS ");
		out.push_str(&column.name);
	}
	if let Some(keys) = &output.group_by {
		if keys.is_empty() {
			out.push_str("\nGROUP ALL");
		} else {
			out.push_str("\nGROUP BY");
			for (i, key) in keys.iter().enumerate() {
				if i > 0 {
					out.push(',');
				}
				out.push(' ');
				out.push_str(&render_expr(key));
			}
		}
	}
	if !output.order.is_empty() {
		out.push_str("\nORDER BY");
		for (i, order) in output.order.iter().enumerate() {
			if i > 0 {
				out.push(',');
			}
			out.push(' ');
			out.push_str(&render_expr(&order.expr));
			out.push_str(if order.ascending {
				" ASC"
			} else {
				" DESC"
			});
		}
	}
	if let Some(skip) = &output.skip {
		out.push_str("\nSKIP ");
		out.push_str(&render_expr(skip));
	}
	if let Some(limit) = &output.limit {
		out.push_str("\nLIMIT ");
		out.push_str(&render_expr(limit));
	}
}

fn name_of(plan: &MatchPlan, id: u32) -> &str {
	plan.bindings.get(id as usize).map(|b| b.name.as_str()).unwrap_or("?")
}

/// Parses successfully, then lowers expecting an error; returns the rendered
/// error and the source slices covered by the error's spans.
fn lower_err(source: &str) -> (String, Vec<String>) {
	let query = match parse_str(source) {
		Ok(query) => query,
		Err(e) => panic!("failed to parse {source:?}: {:?}", e.render_on(source)),
	};
	let error = lower(query).expect_err("lowering should have failed");
	let rendered = format!("{:?}", error.render_on(source));
	let mut slices = Vec::new();
	error.update_spans(|span| {
		let range = span.to_range();
		slices.push(source[range.start as usize..range.end as usize].to_owned());
	});
	(rendered, slices)
}

/// Asserts that lowering fails with a message containing `message` and a span
/// covering exactly `slice` in the source.
#[track_caller]
fn assert_rejects(source: &str, message: &str, slice: &str) {
	let (rendered, slices) = lower_err(source);
	assert!(
		rendered.contains(message),
		"error for {source:?} does not contain {message:?}: {rendered}"
	);
	assert!(
		slices.iter().any(|s| s == slice),
		"error spans for {source:?} cover {slices:?}, not {slice:?}"
	);
}

// ------------------------------------------------------------------------
// Worked examples (V2_DESIGN §8 / §6 canonical queries).
// ------------------------------------------------------------------------

#[test]
fn single_node_bare_variable() {
	assert_eq!(
		render("MATCH (n:person) RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n AS n"
	);
}

#[test]
fn one_hop_edge_predicate_dotted_columns() {
	assert_eq!(
		render(
			"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020 \
			 RETURN a.name AS a_name, b.name AS b_name"
		),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 \x20\x20WHERE [k] k.since != NONE AND k.since != NULL AND k.since > 2020\n\
		 RETURN a.name AS a_name, b.name AS b_name"
	);
}

#[test]
fn anonymous_edge_gets_hidden_binding() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(b:person) RETURN a.name, b.name"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person)\n\
		 RETURN a.name AS a.name, b.name AS b.name"
	);
}

#[test]
fn anonymous_far_node_gets_hidden_binding() {
	assert_eq!(
		render("MATCH (a:person)-[k:knows]->(:person) RETURN k"),
		"bindings: a:Node k:Edge __v0:Node*\n\
		 MATCH (a:person)-[k:knows]->(__v0:person)\n\
		 RETURN k AS k"
	);
}

#[test]
fn path_var_quantified_range() {
	assert_eq!(
		render("MATCH p = (a:person)-[:knows]->{1,3}(b:person) RETURN p, b ORDER BY a.age"),
		"bindings: a:Node __e0:EdgeGroup* b:Node p:Path\n\
		 MATCH p = (a:person)-[__e0:knows]->{1,3}(b:person)\n\
		 RETURN p AS p, b AS b\n\
		 ORDER BY a.age ASC"
	);
}

// ------------------------------------------------------------------------
// Multi-pattern, sequential MATCH, repeated-node-variable joins (PR-B).
//
// The lowering only declares the shared bindings (a reused node variable is one
// binding) and the conjunct deps; it does not decide the join. A reused node
// variable keeps the id of its first declaration, so it appears once in the
// binding table and twice in the pattern rendering.
// ------------------------------------------------------------------------

#[test]
fn multi_pattern_shared_node_is_one_binding() {
	// `b` is declared once (in the first pattern) and reused in the second, so
	// it is a single Node binding — the planner's join key on `b`.
	assert_eq!(
		render("MATCH (a:person)-[:x]->(b), (c:person)-[:y]->(b) RETURN a, c"),
		"bindings: a:Node __e0:Edge* b:Node c:Node __e1:Edge*\n\
		 MATCH (a:person)-[__e0:x]->(b), (c:person)-[__e1:y]->(b)\n\
		 RETURN a AS a, c AS c"
	);
}

#[test]
fn multi_pattern_cartesian_no_shared_variable() {
	// Two fully-disjoint patterns: no shared binding ⇒ the planner takes the
	// cartesian product. The lowering just emits both patterns.
	assert_eq!(
		render("MATCH (a:person), (b:city) RETURN a, b"),
		"bindings: a:Node b:Node\n\
		 MATCH (a:person), (b:city)\n\
		 RETURN a AS a, b AS b"
	);
}

#[test]
fn multi_pattern_three_patterns_chained_on_shared_nodes() {
	assert_eq!(
		render(
			"MATCH (a:person)-[:knows]->(b:person), (b)-[:knows]->(c:person), \
			 (c)-[:knows]->(d:person) RETURN a, d"
		),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node __e2:Edge* d:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person), (b)-[__e1:knows]->(c:person), \
		 (c)-[__e2:knows]->(d:person)\n\
		 RETURN a AS a, d AS d"
	);
}

#[test]
fn multi_hop_pattern_keeps_chain_whole() {
	// v1/PR-A rejected multi-hop; the chain now lowers whole (the planner chains
	// the Expands).
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) RETURN a, c"),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person)-[__e1:knows]->(c:person)\n\
		 RETURN a AS a, c AS c"
	);
}

#[test]
fn sequential_match_clauses_share_a_node() {
	// Two MATCH statements joined on `b` (R1: identical to a comma pattern).
	// Each clause is its own line; `b` is one Node binding shared between them.
	assert_eq!(
		render(
			"MATCH (a:person)-[k:knows]->(b:person) MATCH (b:person)-[k2:likes]->(c:person) \
			 RETURN a, c"
		),
		"bindings: a:Node k:Edge b:Node k2:Edge c:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 MATCH (b:person)-[k2:likes]->(c:person)\n\
		 RETURN a AS a, c AS c"
	);
}

#[test]
fn sequential_match_clauses_cartesian() {
	// Sequential MATCH with no shared variable: cartesian product (R1).
	assert_eq!(
		render("MATCH (a:person) MATCH (b:city) RETURN a, b"),
		"bindings: a:Node b:Node\n\
		 MATCH (a:person)\n\
		 MATCH (b:city)\n\
		 RETURN a AS a, b AS b"
	);
}

#[test]
fn repeated_node_variable_across_patterns_is_a_join_key() {
	// A repeated node variable is no longer an error; it resolves to the SAME
	// binding, which the planner equi-joins on.
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(b:person), (a)-[:likes]->(c:person) RETURN b, c"),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person), (a)-[__e1:likes]->(c:person)\n\
		 RETURN b AS b, c AS c"
	);
}

#[test]
fn repeated_node_variable_within_one_pattern_emits_id_equality() {
	// A node variable repeated WITHIN a single chain (the self-loop
	// `(a)-[:knows]->(a)`) cannot be a shared binding — there is no join to
	// materialise the equality, and reusing the id would let the target overwrite
	// the source on the row. The lowering rewrites the repeat to a fresh hidden
	// node `__v0` and emits the implied `a.id = __v0.id` equality conjunct
	// (V2_DESIGN §2). Without this, the self-loop over-returns every edge.
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(a) RETURN a.name"),
		"bindings: a:Node __e0:Edge* __v0:Node*\n\
		 MATCH (a:person)-[__e0:knows]->(__v0)\n\
		 \x20\x20WHERE [a __v0] a.id = __v0.id\n\
		 RETURN a.name AS a.name"
	);
}

#[test]
fn repeated_node_variable_thrice_within_one_pattern_chains_equalities() {
	// Three occurrences of `a` on one chain ⇒ two fresh hidden nodes and two
	// equalities, transitively forcing all three equal.
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(a)-[:knows]->(a) RETURN a.name"),
		"bindings: a:Node __e0:Edge* __v0:Node* __e1:Edge* __v1:Node*\n\
		 MATCH (a:person)-[__e0:knows]->(__v0)-[__e1:knows]->(__v1)\n\
		 \x20\x20WHERE [a __v0] a.id = __v0.id\n\
		 \x20\x20WHERE [a __v1] a.id = __v1.id\n\
		 RETURN a.name AS a.name"
	);
}

#[test]
fn cross_pattern_predicate_deps_span_both_patterns() {
	// A WHERE conjunct referencing bindings from two patterns lands as one
	// predicate whose deps span both (the planner filters it post-join).
	assert_eq!(
		render(
			"MATCH (a:person)-[:x]->(b:person), (c:person)-[:y]->(b) WHERE a.age > c.age \
			 RETURN a, c"
		),
		"bindings: a:Node __e0:Edge* b:Node c:Node __e1:Edge*\n\
		 MATCH (a:person)-[__e0:x]->(b:person), (c:person)-[__e1:y]->(b)\n\
		 \x20\x20WHERE [a c] a.age != NONE AND a.age != NULL AND c.age != NONE AND c.age != NULL \
		 AND a.age > c.age\n\
		 RETURN a AS a, c AS c"
	);
}

#[test]
fn sequential_clause_predicate_may_reference_an_earlier_clause() {
	// The second clause's WHERE references `a` (bound by the first clause): it
	// is owned by the second clause but its deps span the first. The lowering
	// just records the deps; the planner places it post-join.
	assert_eq!(
		render(
			"MATCH (a:person)-[:knows]->(b:person) MATCH (b:person)-[:likes]->(c:person) \
			 WHERE a.age > c.age RETURN a, c"
		),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person)\n\
		 MATCH (b:person)-[__e1:likes]->(c:person)\n\
		 \x20\x20WHERE [a c] a.age != NONE AND a.age != NULL AND c.age != NONE AND c.age != NULL \
		 AND a.age > c.age\n\
		 RETURN a AS a, c AS c"
	);
}

// ------------------------------------------------------------------------
// OPTIONAL MATCH (R3): left-outer clauses. The lowering tags each clause with
// `optional` and a per-block `optional_group` (rendered `OPTIONAL#<id>`); the
// planner left-joins clauses sharing one id as ONE all-or-nothing unit, and
// chains distinct ids left-to-right. Clauses sharing an id are one block; a
// plain `OPTIONAL MATCH` is a block of one. Inside-optional predicates attach
// to the optional clause (pre-null); a later clause's predicate that merely
// references an optional binding attaches to that later clause (post-null).
// ------------------------------------------------------------------------

#[test]
fn optional_clause_is_tagged_optional() {
	// Worked tree (iii): a plain `OPTIONAL MATCH` after a mandatory clause. The
	// optional clause is tagged `optional` (rendered `OPTIONAL#0`); `b`/`k` are
	// optional-bound. The leading optional pattern reuses the bound `a`.
	assert_eq!(
		render("MATCH (a:person) OPTIONAL MATCH (a)-[k:knows]->(b:person) RETURN a.name, b.name"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[k:knows]->(b:person)\n\
		 RETURN a.name AS a.name, b.name AS b.name"
	);
}

#[test]
fn optional_clause_expands_unlabeled_edge_off_bound_var() {
	// An optional clause whose only anchor is the bound `a` (no labeled element):
	// realisable as a bound-variable expansion because the clause is optional (a
	// mandatory leading clause with this shape would be rejected as unanchorable).
	assert_eq!(
		render("MATCH (a:person) OPTIONAL MATCH (a)-[k]->(b:person) RETURN a, b"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[k]->(b:person)\n\
		 RETURN a AS a, b AS b"
	);
}

#[test]
fn chained_optionals_get_distinct_block_ids() {
	// Two plain `OPTIONAL MATCH` clauses chain left-to-right: each is its own
	// block (distinct ids #0, #1), so the planner left-joins them in sequence.
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) \
			 OPTIONAL MATCH (a)-[:likes]->(c:person) RETURN a"
		),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 OPTIONAL#1 MATCH (a)-[__e1:likes]->(c:person)\n\
		 RETURN a AS a"
	);
}

#[test]
fn optional_brace_block_shares_one_block_id() {
	// A brace block `OPTIONAL { MATCH …; MATCH … }` is ONE all-or-nothing unit:
	// both inner clauses share `OPTIONAL#0`, so the planner left-joins the WHOLE
	// block subplan as a unit (not per inner clause).
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL { MATCH (a)-[:knows]->(b:person) \
			 MATCH (b)-[:knows]->(c:person) } RETURN a"
		),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 OPTIONAL#0 MATCH (b)-[__e1:knows]->(c:person)\n\
		 RETURN a AS a"
	);
}

#[test]
fn optional_paren_block_is_a_block_of_one() {
	// The paren form is the brace form's twin; a single inner clause is a block
	// of one (`OPTIONAL#0`), identical in the IR to a plain `OPTIONAL MATCH`.
	assert_eq!(
		render("MATCH (a:person) OPTIONAL ( MATCH (a)-[:knows]->(b:person) ) RETURN a"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 RETURN a AS a"
	);
}

#[test]
fn nested_optional_block_ids_are_innermost() {
	// A nested `OPTIONAL` inside a block: each block mints its own id in textual
	// order, and a clause carries the INNERMOST block it sits directly within.
	// `b` (block #0) is one unit relative to `a`; `c` (block #1) is one unit
	// relative to `a`/`b`. Distinct ids ⇒ the planner chains the two left-joins.
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL { MATCH (a)-[:knows]->(b:person) \
			 OPTIONAL MATCH (b)-[:likes]->(c:person) } RETURN a"
		),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 OPTIONAL#1 MATCH (b)-[__e1:likes]->(c:person)\n\
		 RETURN a AS a"
	);
}

#[test]
fn optional_clause_followed_by_mandatory_clause() {
	// A mandatory clause after an optional one stays mandatory (no block id); the
	// optional clause keeps its own block. Pins that `optional`/`optional_group`
	// are per-clause, not sticky.
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) \
			 MATCH (a)-[:likes]->(c:person) RETURN a"
		),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 MATCH (a)-[__e1:likes]->(c:person)\n\
		 RETURN a AS a"
	);
}

// --- OPTIONAL × guards: the nullable() amendment (V2_DESIGN §8). ---

#[test]
fn inside_optional_predicate_guards_optional_binding() {
	// A predicate WRITTEN inside the optional (pre-null, R3) is owned by the
	// optional clause. `b.age` is a property on the optional `b`, guarded as
	// before — the row is excluded when `b` missed (`b.age` → NONE).
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) WHERE b.age > 18 RETURN a"
		),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 \x20\x20WHERE [b] b.age != NONE AND b.age != NULL AND b.age > 18\n\
		 RETURN a AS a"
	);
}

#[test]
fn bare_optional_variable_is_nullable_in_ordering_comparison() {
	// THE amendment, made visible: a bare optional-bound variable `b` is now a
	// guard atom in an ordering comparison — `b != NONE AND b != NULL` — which a
	// mandatory bare variable (`a`) never gets. Without the amendment `b > a`
	// would carry no guard and a pre-null `b` (Null) would sort below `a` and
	// wrongly survive.
	assert_eq!(
		render("MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) WHERE b > a RETURN a"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 \x20\x20WHERE [a b] b != NONE AND b != NULL AND b > a\n\
		 RETURN a AS a"
	);
}

#[test]
fn both_optional_bare_equality_is_guarded() {
	// Both sides optional bare variables (chained optionals): `=` deviates only
	// when BOTH sides can be null (`NULL = NULL` is TRUE in SurrealQL but UNKNOWN
	// in GQL), and the amendment now makes both sides nullable, so the guard pair
	// is emitted and a double-miss row is excluded.
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) \
			 OPTIONAL MATCH (a)-[:likes]->(c:person) WHERE b = c RETURN a"
		),
		"bindings: a:Node __e0:Edge* b:Node __e1:Edge* c:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 OPTIONAL#1 MATCH (a)-[__e1:likes]->(c:person)\n\
		 \x20\x20WHERE [b c] b != NONE AND b != NULL AND c != NONE AND c != NULL AND b = c\n\
		 RETURN a AS a"
	);
}

#[test]
fn one_sided_optional_equality_needs_no_guard() {
	// `b = c` with `b` optional but `c` mandatory: a one-sided null already
	// compares unequal and excludes the row, so `=` needs no guard (E8c). The
	// amendment makes `nullable(b)` true, but the `Eq` rule still only guards when
	// BOTH sides are nullable — so this correctly stays guard-free. The predicate
	// is owned by the mandatory clause that introduces `c` (post-null, R3).
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) \
			 MATCH (c:person) WHERE b = c RETURN a"
		),
		"bindings: a:Node __e0:Edge* b:Node c:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 MATCH (c:person)\n\
		 \x20\x20WHERE [b c] b = c\n\
		 RETURN a AS a"
	);
}

#[test]
fn optional_binding_is_null_test() {
	// `b IS NULL` is two-valued and needs no guard; on an optional miss `b` is
	// `Value::Null`, so `b = NULL OR b = NONE` is TRUE — the unmatched rows are
	// exactly the ones it selects (R3 / the optional-miss-is-Null rule).
	assert_eq!(
		render("MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) WHERE b IS NULL RETURN a"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 \x20\x20WHERE [b] b = NULL OR b = NONE\n\
		 RETURN a AS a"
	);
}

#[test]
fn later_clause_predicate_referencing_optional_binding_is_post_null() {
	// A predicate in a LATER mandatory clause that references an optional binding
	// is owned by that later clause (post-null, R3). The deps span the optional
	// `b` and the mandatory `c`; the planner places it post-join. The property
	// `b.age` (optional `b`) keeps its guard, so a missed `b` excludes the row.
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) \
			 MATCH (c:person) WHERE c.age > b.age RETURN c"
		),
		"bindings: a:Node __e0:Edge* b:Node c:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 MATCH (c:person)\n\
		 \x20\x20WHERE [b c] c.age != NONE AND c.age != NULL AND b.age != NONE AND b.age != NULL \
		 AND c.age > b.age\n\
		 RETURN c AS c"
	);
}

// ------------------------------------------------------------------------
// Directions and labels.
// ------------------------------------------------------------------------

#[test]
fn left_direction_uses_in() {
	assert_eq!(
		render("MATCH (a:person)<-[k:knows]-(b:person) RETURN b.name"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)<-[k:knows]-(b:person)\n\
		 RETURN b.name AS b.name"
	);
}

#[test]
fn unlabeled_edge() {
	assert_eq!(
		render("MATCH (a:person)-[k]->(b) RETURN k"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k]->(b)\n\
		 RETURN k AS k"
	);
}

#[test]
fn labeled_far_node_only() {
	assert_eq!(
		render("MATCH (a:person)-[k]->(b:person) RETURN k"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k]->(b:person)\n\
		 RETURN k AS k"
	);
}

// ------------------------------------------------------------------------
// Quantifiers: the full set is legal (R6).
// ------------------------------------------------------------------------

#[test]
fn quantifier_fixed() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->{2}(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{2,2}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantifier_range() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->{1,3}(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{1,3}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantifier_star_is_zero_to_unbounded() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->*(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{0,}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantifier_plus_is_one_to_unbounded() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->+(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{1,}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantifier_question_is_zero_to_one() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->?(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{0,1}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantifier_min_two_is_legal() {
	// v1 rejected `{2}`/`{2,3}`; v2's per-path traversal makes them legal.
	assert_eq!(
		render("MATCH (a:person)-[:knows]->{2,4}(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{2,4}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantifier_zero_min_is_legal() {
	// v1 rejected `{0,3}`; v2 emits the zero-length path (R6).
	assert_eq!(
		render("MATCH (a:person)-[:knows]->{0,3}(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{0,3}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantifier_unbounded_lower_bound_is_legal() {
	// v1 rejected `{2,}`; v2 terminates the unbounded form via edge-uniqueness.
	assert_eq!(
		render("MATCH (a:person)-[:knows]->{2,}(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{2,}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantifier_missing_min_defaults_to_zero() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->{,3}(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->{0,3}(b:person)\n\
		 RETURN b AS b"
	);
}

#[test]
fn quantified_edge_with_variable_is_a_group() {
	// v1 rejected an edge variable under a quantifier; v2 binds an edge group (R4).
	assert_eq!(
		render("MATCH (a:person)-[k:knows]->{1,3}(b:person) RETURN k"),
		"bindings: a:Node k:EdgeGroup b:Node\n\
		 MATCH (a:person)-[k:knows]->{1,3}(b:person)\n\
		 RETURN k AS k"
	);
}

#[test]
fn quantified_left_direction() {
	assert_eq!(
		render("MATCH (a:person)<-[:knows]-{1,2}(b:person) RETURN b"),
		"bindings: a:Node __e0:EdgeGroup* b:Node\n\
		 MATCH (a:person)<-[__e0:knows]-{1,2}(b:person)\n\
		 RETURN b AS b"
	);
}

// ------------------------------------------------------------------------
// Predicate placement is the planner's job: the lowering emits flat conjuncts
// with their dependency sets (no anchor/edge/post classification).
// ------------------------------------------------------------------------

#[test]
fn anchor_only_conjunct_deps_anchor() {
	assert_eq!(
		render("MATCH (a:person)-[k:knows]->(b:person) WHERE a.age > 18 RETURN k"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 \x20\x20WHERE [a] a.age != NONE AND a.age != NULL AND a.age > 18\n\
		 RETURN k AS k"
	);
}

#[test]
fn cross_variable_conjunct_deps_both() {
	assert_eq!(
		render("MATCH (a:person)-[k:knows]->(b:person) WHERE a.age > b.age RETURN k"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 \x20\x20WHERE [a b] a.age != NONE AND a.age != NULL AND b.age != NONE AND b.age != NULL \
		 AND a.age > b.age\n\
		 RETURN k AS k"
	);
}

#[test]
fn bare_whole_record_reference_deps_both() {
	assert_eq!(
		render("MATCH (a:person)-[k:knows]->(b:person) WHERE b = a RETURN k"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 \x20\x20WHERE [a b] b = a\n\
		 RETURN k AS k"
	);
}

#[test]
fn and_splits_into_separate_conjuncts() {
	assert_eq!(
		render(
			"MATCH (a:person)-[k:knows]->(b:person) WHERE a.age > 18 AND k.since > 2020 RETURN b"
		),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 \x20\x20WHERE [a] a.age != NONE AND a.age != NULL AND a.age > 18\n\
		 \x20\x20WHERE [k] k.since != NONE AND k.since != NULL AND k.since > 2020\n\
		 RETURN b AS b"
	);
}

#[test]
fn or_conjunct_is_not_split() {
	assert_eq!(
		render(
			"MATCH (a:person)-[k:knows]->(b:person) WHERE a.age > 60 OR k.since > 2020 RETURN k"
		),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 \x20\x20WHERE [a k] a.age != NONE AND a.age != NULL AND a.age > 60 OR k.since != NONE \
		 AND k.since != NULL AND k.since > 2020\n\
		 RETURN k AS k"
	);
}

#[test]
fn edge_property_map() {
	assert_eq!(
		render("MATCH (a:person)-[k:knows {since: 2020}]->(b:person) RETURN k"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 \x20\x20WHERE [k] k.since = 2020\n\
		 RETURN k AS k"
	);
}

#[test]
fn far_node_property_map() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(b:person {city: 'London'}) RETURN a"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person)\n\
		 \x20\x20WHERE [b] b.city = 'London'\n\
		 RETURN a AS a"
	);
}

#[test]
fn anonymous_far_node_property_map_uses_hidden_binding() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(:person {city: 'London'}) RETURN a"),
		"bindings: a:Node __e0:Edge* __v0:Node*\n\
		 MATCH (a:person)-[__e0:knows]->(__v0:person)\n\
		 \x20\x20WHERE [__v0] __v0.city = 'London'\n\
		 RETURN a AS a"
	);
}

#[test]
fn node_property_map_no_guard_for_literal() {
	assert_eq!(
		render("MATCH (n:person {city: 'London'}) RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.city = 'London'\n\
		 RETURN n AS n"
	);
}

#[test]
fn inline_node_where() {
	assert_eq!(
		render("MATCH (n:person WHERE n.age > 18) RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND n.age > 18\n\
		 RETURN n AS n"
	);
}

#[test]
fn inline_edge_where() {
	assert_eq!(
		render("MATCH (a:person)-[k:knows WHERE k.since > 2020]->(b:person) RETURN k"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 \x20\x20WHERE [k] k.since != NONE AND k.since != NULL AND k.since > 2020\n\
		 RETURN k AS k"
	);
}

#[test]
fn inline_far_node_where_cross_variable() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(b:person WHERE b.age < a.age) RETURN a"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person)\n\
		 \x20\x20WHERE [a b] b.age != NONE AND b.age != NULL AND a.age != NONE AND a.age != NULL \
		 AND b.age < a.age\n\
		 RETURN a AS a"
	);
}

#[test]
fn predicate_source_merge_order() {
	// Explicit WHERE first, then inline element WHEREs, then property maps.
	assert_eq!(
		render("MATCH (n:person {city: 'London'}) WHERE n.age > 18 RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND n.age > 18\n\
		 \x20\x20WHERE [n] n.city = 'London'\n\
		 RETURN n AS n"
	);
}

#[test]
fn bare_anchor_equality_uniform_addressing() {
	assert_eq!(
		render("MATCH (n:person) WHERE n = $x RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n = $x\n\
		 RETURN n AS n"
	);
}

// ------------------------------------------------------------------------
// Three-valued logic guards and NNF (kept verbatim from v1).
// ------------------------------------------------------------------------

#[test]
fn ordering_guard_param_operand() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.age = $min RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND $min != NONE AND $min != NULL \
		 AND n.age = $min\n\
		 RETURN n AS n"
	);
}

#[test]
fn ordering_guard_both_params() {
	assert_eq!(
		render("MATCH (n:person) WHERE $a < $b RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [] $a != NONE AND $a != NULL AND $b != NONE AND $b != NULL AND $a < $b\n\
		 RETURN n AS n"
	);
}

#[test]
fn ordering_guard_deduplicates_atoms() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.age > n.age RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND n.age > n.age\n\
		 RETURN n AS n"
	);
}

#[test]
fn ordering_guard_arithmetic_operand() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.age + 1 > 18 RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND n.age + 1 > 18\n\
		 RETURN n AS n"
	);
}

#[test]
fn equality_with_literal_needs_no_guard() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.name = 'A' RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.name = 'A'\n\
		 RETURN n AS n"
	);
}

#[test]
fn equality_with_null_literal_is_guarded() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.age = null RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND n.age = NULL\n\
		 RETURN n AS n"
	);
}

#[test]
fn inequality_guards_one_sided() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.name <> 'A' RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.name != NONE AND n.name != NULL AND n.name != 'A'\n\
		 RETURN n AS n"
	);
}

#[test]
fn nnf_pushes_not_through_or() {
	assert_eq!(
		render("MATCH (n:person) WHERE NOT (n.age > 18 OR n.name = 'A') RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND n.age <= 18\n\
		 \x20\x20WHERE [n] n.name != NONE AND n.name != NULL AND n.name != 'A'\n\
		 RETURN n AS n"
	);
}

#[test]
fn nnf_pushes_not_through_and() {
	assert_eq!(
		render("MATCH (n:person) WHERE NOT (n.age > 18 AND n.flag) RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND n.age <= 18 OR n.flag = false\n\
		 RETURN n AS n"
	);
}

#[test]
fn nnf_double_negation() {
	assert_eq!(
		render("MATCH (n:person) WHERE NOT NOT n.flag RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.flag = true\n\
		 RETURN n AS n"
	);
}

#[test]
fn nnf_not_complements_comparison() {
	assert_eq!(
		render("MATCH (n:person) WHERE NOT n.age > 18 RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NONE AND n.age != NULL AND n.age <= 18\n\
		 RETURN n AS n"
	);
}

#[test]
fn bare_nullable_boolean_tests_true() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.flag RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.flag = true\n\
		 RETURN n AS n"
	);
}

#[test]
fn bare_param_predicate_tests_true() {
	assert_eq!(
		render("MATCH (n:person) WHERE $flag RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [] $flag = true\n\
		 RETURN n AS n"
	);
}

#[test]
fn boolean_literal_predicate() {
	assert_eq!(
		render("MATCH (n:person) WHERE true RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [] true\n\
		 RETURN n AS n"
	);
	assert_eq!(
		render("MATCH (n:person) WHERE NOT true RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [] false\n\
		 RETURN n AS n"
	);
}

#[test]
fn is_null_test() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.age IS NULL RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age = NULL OR n.age = NONE\n\
		 RETURN n AS n"
	);
}

#[test]
fn is_not_null_test() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.age IS NOT NULL RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.age != NULL AND n.age != NONE\n\
		 RETURN n AS n"
	);
}

#[test]
fn is_true_false_tests() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.flag IS TRUE RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.flag = true\n\
		 RETURN n AS n"
	);
	assert_eq!(
		render("MATCH (n:person) WHERE n.flag IS NOT FALSE RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.flag != false\n\
		 RETURN n AS n"
	);
}

#[test]
fn is_unknown_tests() {
	assert_eq!(
		render("MATCH (n:person) WHERE n.flag IS UNKNOWN RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.flag = NULL OR n.flag = NONE\n\
		 RETURN n AS n"
	);
}

#[test]
fn not_negates_truth_test() {
	assert_eq!(
		render("MATCH (n:person) WHERE NOT (n.flag IS TRUE) RETURN n"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 \x20\x20WHERE [n] n.flag != true\n\
		 RETURN n AS n"
	);
}

#[test]
fn guards_for_cross_variable_comparison() {
	// The edge-scope hazard from v1 (`out.age < $parent.age` unguarded) is now
	// uniform binding addressing, still guarded.
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(b:person) WHERE b.age < a.age RETURN a"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person)\n\
		 \x20\x20WHERE [a b] b.age != NONE AND b.age != NULL AND a.age != NONE AND a.age != NULL \
		 AND b.age < a.age\n\
		 RETURN a AS a"
	);
}

// ------------------------------------------------------------------------
// RETURN, RETURN *, columns, DISTINCT.
// ------------------------------------------------------------------------

#[test]
fn return_star_single_node() {
	assert_eq!(
		render("MATCH (n:person) RETURN *"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n AS n"
	);
}

#[test]
fn return_star_expands_alphabetically() {
	assert_eq!(
		render("MATCH (b:person)-[a:knows]->(c:person) RETURN *"),
		"bindings: b:Node a:Edge c:Node\n\
		 MATCH (b:person)-[a:knows]->(c:person)\n\
		 RETURN a AS a, b AS b, c AS c"
	);
}

#[test]
fn return_star_skips_anonymous_elements() {
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(b:person) RETURN *"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person)\n\
		 RETURN a AS a, b AS b"
	);
}

#[test]
fn return_star_includes_group_and_path_vars() {
	assert_eq!(
		render("MATCH p = (a:person)-[k:knows]->{1,2}(b:person) RETURN *"),
		"bindings: a:Node k:EdgeGroup b:Node p:Path\n\
		 MATCH p = (a:person)-[k:knows]->{1,2}(b:person)\n\
		 RETURN a AS a, b AS b, k AS k, p AS p"
	);
}

#[test]
fn concat_lowers_to_add() {
	assert_eq!(
		render("MATCH (n:person) RETURN n.name || 'x' AS y"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n.name + 'x' AS y"
	);
}

#[test]
fn arithmetic_and_sign_operators() {
	assert_eq!(
		render("MATCH (n:person) RETURN n.age * 2 - 1 AS x, -n.age AS y"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n.age * 2 - 1 AS x, -n.age AS y"
	);
}

#[test]
fn list_and_map_literals() {
	assert_eq!(
		render("MATCH (n:person) RETURN [n.age, 1] AS lst, {a: n.age} AS mp"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN [n.age, 1] AS lst, { a: n.age } AS mp"
	);
}

#[test]
fn comparison_in_value_position_is_unguarded() {
	assert_eq!(
		render("MATCH (n:person) RETURN n.age > 18 AS adult"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n.age > 18 AS adult"
	);
}

#[test]
fn distinct_flag_set() {
	assert_eq!(
		render("MATCH (n:person) RETURN DISTINCT n.name, n.age ORDER BY n.name DESC"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN DISTINCT n.name AS n.name, n.age AS n.age\n\
		 ORDER BY `n.name` DESC"
	);
}

#[test]
fn distinct_order_by_returned_column_allowed() {
	assert_eq!(
		render("MATCH (n:person) RETURN DISTINCT n.name ORDER BY n.name"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN DISTINCT n.name AS n.name\n\
		 ORDER BY `n.name` ASC"
	);
}

#[test]
fn nested_property_chains() {
	assert_eq!(
		render("MATCH (n:person) RETURN n.address.city AS city"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n.address.city AS city"
	);
}

#[test]
fn quoted_identifiers_lower_like_plain_ones() {
	assert_eq!(
		render("MATCH (\"n\":person) RETURN `n`.name AS \"the name\""),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n.name AS the name"
	);
}

// ------------------------------------------------------------------------
// ORDER BY: non-DISTINCT keys may reference any binding (R7).
// ------------------------------------------------------------------------

#[test]
fn order_by_alias() {
	// Non-DISTINCT: the key names the `name` column, so it sorts on that
	// column's underlying binding-row expression (`n.name`) pre-projection.
	assert_eq!(
		render("MATCH (n:person) RETURN n.name AS name ORDER BY name DESC"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n.name AS name\n\
		 ORDER BY n.name DESC"
	);
}

#[test]
fn order_by_returned_column_lowers_full_expr() {
	// Non-DISTINCT: the key lowers as a full binding-row expression.
	assert_eq!(
		render("MATCH (a:person)-[:knows]->(b:person) RETURN a.name ORDER BY a.name"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)-[__e0:knows]->(b:person)\n\
		 RETURN a.name AS a.name\n\
		 ORDER BY a.name ASC"
	);
}

#[test]
fn order_by_non_returned_binding_is_allowed() {
	// v1 rejected this; R7 relaxes it for non-DISTINCT queries.
	assert_eq!(
		render("MATCH (n:person) RETURN n.name ORDER BY n.age DESC"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n.name AS n.name\n\
		 ORDER BY n.age DESC"
	);
}

#[test]
fn order_by_non_returned_post_split_binding_is_allowed() {
	assert_eq!(
		render("MATCH (a:person)-[k:knows]->(b:person) RETURN a ORDER BY k.since"),
		"bindings: a:Node k:Edge b:Node\n\
		 MATCH (a:person)-[k:knows]->(b:person)\n\
		 RETURN a AS a\n\
		 ORDER BY k.since ASC"
	);
}

#[test]
fn order_by_computed_expression() {
	assert_eq!(
		render("MATCH (n:person) RETURN n.age + 1 ORDER BY n.age + 1"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n.age + 1 AS n.age + 1\n\
		 ORDER BY n.age + 1 ASC"
	);
}

// ------------------------------------------------------------------------
// SKIP / LIMIT.
// ------------------------------------------------------------------------

#[test]
fn skip_limit_parameters() {
	assert_eq!(
		render("MATCH (n:person) RETURN n SKIP $s LIMIT $l"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n AS n\n\
		 SKIP $s\n\
		 LIMIT $l"
	);
}

#[test]
fn offset_synonym_for_skip() {
	assert_eq!(
		render("MATCH (n:person) RETURN n OFFSET 2 LIMIT 3"),
		"bindings: n:Node\n\
		 MATCH (n:person)\n\
		 RETURN n AS n\n\
		 SKIP 2\n\
		 LIMIT 3"
	);
}

// ------------------------------------------------------------------------
// Public API entry points.
// ------------------------------------------------------------------------

#[test]
fn parse_to_plan_with_settings_end_to_end() {
	let prepared =
		parse_to_plan_with_settings("MATCH (n:person) RETURN n", GqlParserSettings::default())
			.expect("should lower");
	assert_eq!(prepared.to_sql(), "MATCH (n:person) RETURN n AS n");
}

#[test]
fn prepared_query_to_sql_renders_via_match_plan() {
	let prepared = parse_to_plan_with_settings(
		"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020 RETURN a.name AS x",
		GqlParserSettings::default(),
	)
	.expect("should lower");
	assert_eq!(
		prepared.to_sql(),
		"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since != NONE AND k.since != NULL AND \
		 k.since > 2020 RETURN a.name AS x"
	);
}

#[test]
fn prepared_query_debug_renders_via_match_plan() {
	let prepared =
		parse_to_plan_with_settings("MATCH (n:person) RETURN n", GqlParserSettings::default())
			.expect("should lower");
	let debug = format!("{prepared:?}");
	assert!(debug.contains("MATCH (n:person) RETURN n AS n"), "unexpected debug: {debug}");
}

#[test]
fn deep_linear_chains_lower_without_overflowing() {
	// The lowering must process linear chains (binary spines, `NOT` prefixes,
	// property postfixes) without machine-stack recursion (the reblessive
	// guarantee). Such chains exceed the default parse-time expression budget,
	// so the deep cases raise the limit explicitly. The NOT chain collapses
	// under NNF to a bounded predicate and the property chain to a flat idiom,
	// so both render fine; a deep binary spine lowers to an equally deep tree
	// whose recursive rendering is a depth shared with syn-parsed SurrealQL, so
	// it is kept modest and only the lowering (not the rendering) is asserted.
	let deep = GqlParserSettings {
		expr_recursion_limit: 100_000,
		..Default::default()
	};

	let nots = format!("MATCH (n:person) WHERE {} n.flag RETURN n", "NOT ".repeat(50_001));
	let prepared =
		parse_to_plan_with_settings(&nots, deep.clone()).expect("NOT chain should lower");
	assert!(prepared.to_sql().contains("n.flag = false"), "unexpected: {}", prepared.to_sql());

	let props = format!("MATCH (n:person) RETURN n{} AS x", ".p".repeat(50_000));
	assert!(render(&props).contains("RETURN n.p.p.p"));

	// The binary spine lowers without machine-stack recursion; rendering a
	// pathologically deep tree (the recursive `sql::Expr` → `expr::Expr`
	// conversion and the `ToSql` walk) is a depth shared with syn-parsed
	// SurrealQL and out of scope here, so the spine depth is kept modest and
	// rendered only to confirm the shape.
	let spine = format!("MATCH (n:person) RETURN 1{} AS x", " + 1".repeat(100));
	let prepared = parse_to_plan_with_settings(&spine, deep).expect("binary spine should lower");
	assert!(prepared.to_sql().contains("RETURN 1 + 1 + "));
}

// ------------------------------------------------------------------------
// Rejections — PR-A survivors (kept verbatim from v1).
// ------------------------------------------------------------------------

#[test]
fn rejects_missing_match() {
	assert_rejects("RETURN 1", "A query without a MATCH clause is not supported yet", "RETURN 1");
}

#[test]
fn rejects_undirected_edges() {
	assert_rejects(
		"MATCH (a:person)~(b:person) RETURN a",
		"Undirected and multi-directional edge patterns are not supported yet",
		"~",
	);
}

#[test]
fn rejects_any_direction_edges() {
	assert_rejects(
		"MATCH (a:person)-(b:person) RETURN a",
		"Undirected and multi-directional edge patterns are not supported yet",
		"-",
	);
}

#[test]
fn rejects_left_or_right_edges() {
	assert_rejects(
		"MATCH (a:person)<->(b:person) RETURN a",
		"Undirected and multi-directional edge patterns are not supported yet",
		"<->",
	);
}

#[test]
fn rejects_left_or_undirected_edges() {
	assert_rejects(
		"MATCH (a:person)<~(b:person) RETURN a",
		"Undirected and multi-directional edge patterns are not supported yet",
		"<~",
	);
}

#[test]
fn rejects_undirected_or_right_edges() {
	assert_rejects(
		"MATCH (a:person)~>(b:person) RETURN a",
		"Undirected and multi-directional edge patterns are not supported yet",
		"~>",
	);
}

#[test]
fn rejects_full_form_undirected_edges() {
	assert_rejects(
		"MATCH (a:person)-[k:knows]-(b:person) RETURN a",
		"Undirected and multi-directional edge patterns are not supported yet",
		"-[k:knows]-",
	);
}

#[test]
fn rejects_node_label_disjunction() {
	assert_rejects(
		"MATCH (n:person|admin) RETURN n",
		"Label expressions (`!`, `&`, `|`, `%`) are not supported yet",
		"person|admin",
	);
}

#[test]
fn rejects_node_label_wildcard() {
	assert_rejects(
		"MATCH (n:%) RETURN n",
		"Label expressions (`!`, `&`, `|`, `%`) are not supported yet",
		"%",
	);
}

#[test]
fn rejects_node_label_negation() {
	assert_rejects(
		"MATCH (n:!person) RETURN n",
		"Label expressions (`!`, `&`, `|`, `%`) are not supported yet",
		"!person",
	);
}

#[test]
fn rejects_edge_label_expression() {
	assert_rejects(
		"MATCH (a:person)-[k:knows|likes]->(b:person) RETURN a",
		"Label expressions (`!`, `&`, `|`, `%`) are not supported yet",
		"knows|likes",
	);
}

#[test]
fn rejects_unanchorable_pattern() {
	// Generalised from PR-A's leftmost-labelled-node message to the V2_DESIGN
	// wording: a pattern needs a label or a reused earlier variable.
	assert_rejects(
		"MATCH (n) RETURN n",
		"Cannot choose a starting table for this pattern: label at least one node or reuse a \
		 variable bound by an earlier pattern",
		"(n)",
	);
}

#[test]
fn rejects_unanchorable_second_pattern() {
	// The first pattern anchors on `person`; the second is wholly unlabelled
	// and shares no variable with the first, so it cannot be anchored.
	assert_rejects(
		"MATCH (a:person), (x)-[e]->(y) RETURN a",
		"Cannot choose a starting table for this pattern: label at least one node or reuse a \
		 variable bound by an earlier pattern",
		"(x)",
	);
}

#[test]
fn rejects_empty_quantifier_range() {
	assert_rejects(
		"MATCH (a:person)-[:knows]->{3,2}(b:person) RETURN a",
		"The quantifier maximum must not be smaller than its minimum",
		"{3,2}",
	);
}

#[test]
fn lowers_count_star_to_group_all() {
	// A bare `count(*)` with no GROUP BY is GROUP ALL: a zero-argument `count`
	// folded over a single group.
	let rendered = render("MATCH (n:person) RETURN count(*)");
	assert!(rendered.contains("count()"), "{rendered}");
	assert!(rendered.contains("GROUP ALL"), "{rendered}");
}

#[test]
fn lowers_group_by_with_aggregates() {
	// Each GQL aggregate maps onto its SurrealDB accumulator; the grouping key is
	// preserved and a GROUP BY clause is emitted.
	let rendered = render(
		"MATCH (n:person) RETURN n.city AS c, count(*) AS total, sum(n.age) AS s, \
		 avg(n.age) AS a, min(n.age) AS lo, max(n.age) AS hi GROUP BY n.city",
	);
	assert!(rendered.contains("GROUP BY n.city"), "{rendered}");
	assert!(rendered.contains("math::sum(n.age)"), "{rendered}");
	assert!(rendered.contains("math::mean(n.age)"), "{rendered}");
	assert!(rendered.contains("math::min(n.age)"), "{rendered}");
	assert!(rendered.contains("math::max(n.age)"), "{rendered}");
}

#[test]
fn lowers_collect_to_array_group() {
	let rendered =
		render("MATCH (n:person) RETURN n.city AS c, collect(n.name) AS names GROUP BY n.city");
	assert!(rendered.contains("array::group(n.name)"), "{rendered}");
}

#[test]
fn lowers_count_field_with_non_null_guard() {
	// GQL `count(x)` counts non-null `x`: lowered as `count(x != NONE AND x != NULL)`
	// so SurrealDB's truthy count yields the non-null count.
	let rendered = render("MATCH (n:person) RETURN count(n.age) AS c");
	assert!(rendered.contains("n.age != NONE"), "{rendered}");
	assert!(rendered.contains("n.age != NULL"), "{rendered}");
	assert!(rendered.contains("GROUP ALL"), "{rendered}");
}

#[test]
fn rejects_distinct_in_aggregate() {
	assert_rejects(
		"MATCH (n:person) RETURN count(DISTINCT n)",
		"DISTINCT/ALL inside an aggregate is not supported yet",
		"count(DISTINCT n)",
	);
}

#[test]
fn rejects_aggregate_in_where() {
	assert_rejects(
		"MATCH (n:person) WHERE count(n) > 0 RETURN n",
		"Aggregate functions are only allowed in RETURN items and ORDER BY keys",
		"count(n)",
	);
}

#[test]
fn rejects_ungrouped_non_aggregate_column() {
	// `n.age` is neither a grouping key, an aggregate, nor determined by `n.name`.
	assert_rejects(
		"MATCH (n:person) RETURN n.name, n.age GROUP BY n.name",
		"must be a GROUP BY key, an aggregate, or determined by the GROUP BY keys",
		"n.age",
	);
}

#[test]
fn lowers_functionally_dependent_column() {
	// `GROUP BY a` (the whole node) determines `a.name`, so it projects without
	// an aggregate (the planner emits its first value per group).
	let rendered = render("MATCH (a:person) RETURN a AS who, a.name AS nm GROUP BY a");
	assert!(rendered.contains("GROUP BY a"), "{rendered}");
	assert!(rendered.contains("a.name AS nm"), "{rendered}");
}

#[test]
fn rejects_uncovered_dependent_column() {
	// Grouping by `a.name` does NOT determine `a.age`.
	assert_rejects(
		"MATCH (a:person) RETURN a.name AS nm, a.age AS ag GROUP BY a.name",
		"must be a GROUP BY key, an aggregate, or determined by the GROUP BY keys",
		"a.age",
	);
}

#[test]
fn lowers_order_by_non_projected_group_key() {
	// A non-DISTINCT aggregating query may ORDER BY a grouping key it does not
	// project; the lowering materialises a hidden sort-only column.
	let rendered = render("MATCH (a:person) RETURN count(*) AS c GROUP BY a.name ORDER BY a.name");
	assert!(rendered.contains("a.name AS __order0"), "{rendered}");
	assert!(rendered.contains("ORDER BY __order0 ASC"), "{rendered}");
}

#[test]
fn rejects_distinct_order_by_non_return_item() {
	assert_rejects(
		"MATCH (n:person) RETURN DISTINCT n.name ORDER BY n.age",
		"With RETURN DISTINCT, ORDER BY may only reference returned columns",
		"n.age",
	);
}

#[test]
fn rejects_unsupported_aggregate() {
	assert_rejects(
		"MATCH (n:person) RETURN stddev_pop(n.age)",
		"Aggregate functions are not supported yet",
		"stddev_pop(n.age)",
	);
}

#[test]
fn rejects_non_aggregate_functions() {
	assert_rejects(
		"MATCH (n:person) RETURN upper(n.name)",
		"The function `upper` is not supported yet",
		"upper(n.name)",
	);
}

#[test]
fn rejects_function_calls_in_where() {
	assert_rejects(
		"MATCH (n:person) WHERE upper(n.name) = 'A' RETURN n",
		"The function `upper` is not supported yet",
		"upper(n.name)",
	);
}

#[test]
fn rejects_nulls_first() {
	assert_rejects(
		"MATCH (n:person) RETURN n.age ORDER BY n.age NULLS FIRST",
		"`NULLS FIRST`/`NULLS LAST` ordering is not supported yet",
		"n.age NULLS FIRST",
	);
}

#[test]
fn rejects_nulls_last() {
	assert_rejects(
		"MATCH (n:person) RETURN n.age ORDER BY n.age DESC NULLS LAST",
		"`NULLS FIRST`/`NULLS LAST` ordering is not supported yet",
		"n.age DESC NULLS LAST",
	);
}

#[test]
fn rejects_duplicate_verbatim_columns() {
	assert_rejects(
		"MATCH (n:person) RETURN n.name, n.name",
		"Duplicate column name `n.name`",
		"n.name",
	);
}

#[test]
fn rejects_duplicate_aliases() {
	assert_rejects(
		"MATCH (n:person) RETURN n.age AS x, n.name AS x",
		"Duplicate column name `x`",
		"x",
	);
}

#[test]
fn rejects_dunder_variables() {
	assert_rejects(
		"MATCH (__n:person) RETURN __n",
		"Variable names starting with `__` are reserved for internal use",
		"__n",
	);
}

#[test]
fn rejects_dunder_aliases() {
	assert_rejects(
		"MATCH (n:person) RETURN n.age AS __x",
		"Aliases starting with `__` are reserved for internal use",
		"__x",
	);
}

#[test]
fn rejects_dunder_parameters() {
	assert_rejects(
		"MATCH (n:person) WHERE n.age > $__p RETURN n",
		"Parameter names starting with `__` are reserved for internal use",
		"$__p",
	);
}

#[test]
fn rejects_engine_reserved_parameters() {
	assert_rejects(
		"MATCH (n:person) WHERE n.age > $parent RETURN n",
		"The parameter name `$parent` is reserved by the engine",
		"$parent",
	);
	assert_rejects(
		"MATCH (n:person) RETURN n LIMIT $auth",
		"The parameter name `$auth` is reserved by the engine",
		"$auth",
	);
}

#[test]
fn rejects_xor_in_predicates() {
	assert_rejects(
		"MATCH (n:person) WHERE n.flag XOR n.old RETURN n",
		"`XOR` is not supported yet",
		"n.flag XOR n.old",
	);
}

#[test]
fn rejects_xor_in_value_position() {
	assert_rejects(
		"MATCH (n:person) RETURN n.flag XOR true AS x",
		"`XOR` is not supported yet",
		"n.flag XOR true",
	);
}

#[test]
fn rejects_unknown_variables() {
	assert_rejects("MATCH (n:person) RETURN m", "Unknown variable `m`", "m");
	assert_rejects("MATCH (n:person) WHERE m.age > 1 RETURN n", "Unknown variable `m`", "m");
}

#[test]
fn rejects_node_variable_reused_as_edge() {
	// Reusing a node variable as an edge is a kind mismatch (it stays rejected;
	// only a node-as-node reuse flips to a join).
	assert_rejects(
		"MATCH (n:person)-[n]->(b:person) RETURN b",
		"Variable `n` is already bound as a node but reused as an edge",
		"n",
	);
}

#[test]
fn rejects_repeated_edge_variable() {
	// A repeated edge variable is rejected under DIFFERENT EDGES (R2): the
	// equi-join would always be empty.
	assert_rejects(
		"MATCH (a:person)-[k:knows]->(b:person), (c:person)-[k:knows]->(d:person) RETURN a",
		"Edge variable `k` cannot be repeated",
		"k",
	);
}

#[test]
fn rejects_edge_variable_reused_as_node() {
	assert_rejects(
		"MATCH (a:person)-[k:knows]->(b:person), (k:person) RETURN a",
		"Variable `k` is already bound as an edge but reused as a node",
		"k",
	);
}

#[test]
fn rejects_return_star_without_variables() {
	assert_rejects(
		"MATCH (:person) RETURN *",
		"RETURN * requires at least one named pattern variable",
		"RETURN *",
	);
}

// ------------------------------------------------------------------------
// Rejections — message change and new rejections (V2_DESIGN §8 ledger).
// ------------------------------------------------------------------------

#[test]
fn rejects_optional_rebind_in_mandatory_clause() {
	// `b` is first bound inside the OPTIONAL, then re-declared in a later
	// MANDATORY clause. On a miss `b` is `Value::Null` (R3), so a mandatory
	// pattern cannot anchor / join on it: the optional-rebind rejection
	// (V2_DESIGN §1). Re-declaring it INSIDE the same / a deeper optional is fine.
	assert_rejects(
		"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) MATCH (b:person) RETURN a",
		"Variable `b` was first bound inside an OPTIONAL and cannot be re-declared outside it",
		"b",
	);
}

#[test]
fn correlated_mandatory_binding_referenced_inside_optional_is_allowed() {
	// The reverse of the rebind rejection: a MANDATORY binding (`a`) reused inside
	// an OPTIONAL is a correlated constraint, not a rebind — it resolves to the
	// existing mandatory binding and lowers fine.
	assert_eq!(
		render("MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) RETURN a"),
		"bindings: a:Node __e0:Edge* b:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 RETURN a AS a"
	);
}

#[test]
fn rejects_query_leading_with_optional() {
	// `OPTIONAL` is a left-outer join against the preceding binding table, so a
	// query cannot start with one (there is nothing to join against). The planner
	// relies on the first fold unit being mandatory, so the lowering rejects this
	// cleanly rather than letting it reach a planner internal error.
	assert_rejects(
		"OPTIONAL MATCH (a:person) RETURN a",
		"A query cannot start with OPTIONAL MATCH",
		"OPTIONAL MATCH (a:person)",
	);
}

#[test]
fn rejects_optional_leading_multi_hop_bound_var_expansion() {
	// An OPTIONAL block's leading clause whose only anchor is the bound `a` (no
	// labelled element) is realised off the OUTER accumulator by the single-hop
	// `OptionalExpand` only. A MULTI-hop bound-variable expansion off the outer
	// accumulator has no operator (it is neither a self-rootable subplan nor a
	// single-hop fast path), so it is rejected at lowering rather than reaching a
	// planner internal error.
	assert_rejects(
		"MATCH (a:person) OPTIONAL MATCH (a)-[k1]->(x)-[k2]->(b) RETURN a",
		"This MATCH pattern shape is not supported yet",
		"(a)",
	);
}

#[test]
fn rejects_optional_leading_quantified_bound_var_expansion() {
	// Likewise a QUANTIFIED bound-variable expansion leading an OPTIONAL block: a
	// quantified hop is a `PathExpand` (not the single-hop `OptionalExpand` fast
	// path), and an unlabelled quantified start cannot self-root a standalone
	// subplan — so it is rejected at lowering.
	assert_rejects(
		"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->{1,3}(b:person) RETURN a",
		"This MATCH pattern shape is not supported yet",
		"(a)",
	);
}

#[test]
fn optional_block_continuation_multi_hop_expansion_lowers() {
	// The mirror of the rejection above: a MULTI-hop bound-variable expansion is
	// fine when it expands off a WITHIN-block binding (here the block's own `b`),
	// because that is a within-subplan expansion the planner realises as a chain of
	// Expands — not a correlated leading hop off the outer accumulator.
	assert_eq!(
		render(
			"MATCH (a:person) OPTIONAL { MATCH (a)-[:knows]->(b:person) \
			 MATCH (b)-[k1]->(x)-[k2]->(c) } RETURN a"
		),
		"bindings: a:Node __e0:Edge* b:Node k1:Edge x:Node k2:Edge c:Node\n\
		 MATCH (a:person)\n\
		 OPTIONAL#0 MATCH (a)-[__e0:knows]->(b:person)\n\
		 OPTIONAL#0 MATCH (b)-[k1]->(x)-[k2]->(c)\n\
		 RETURN a AS a"
	);
}

#[test]
fn rejects_property_access_on_group_variable() {
	assert_rejects(
		"MATCH (a:person)-[k:knows]->{1,3}(b:person) RETURN k.since",
		"Property access on a group or path variable is not supported yet",
		"since",
	);
}

#[test]
fn rejects_property_access_on_path_variable() {
	assert_rejects(
		"MATCH p = (a:person)-[:knows]->(b:person) RETURN p.length",
		"Property access on a group or path variable is not supported yet",
		"length",
	);
}

#[test]
fn rejects_property_map_on_quantified_edge_group() {
	// A property map on a quantified edge is property access on a group.
	assert_rejects(
		"MATCH (a:person)-[k:knows {since: 2020}]->{1,3}(b:person) RETURN b",
		"Property access on a group or path variable is not supported yet",
		"since",
	);
}

#[test]
fn rejects_cross_variable_predicate_inside_quantified_edge() {
	assert_rejects(
		"MATCH (a:person)-[k:knows WHERE k.since > a.age]->{1,3}(b:person) RETURN b",
		"A predicate inside a quantified edge may only reference that edge",
		"k.since > a.age",
	);
}
