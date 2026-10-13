//! Graph pattern tests: every full and abbreviated edge form, quantifiers,
//! label expressions, element fillers (variables, property maps, inline
//! `WHERE`), node-edge alternation enforcement and the openCypher traps that
//! are invalid (or different) in ISO GQL.

use rstest::rstest;

use super::{label_str, match_clauses, parse, parse_err};
use crate::gql::ast::{
	EdgeDirection, ElementPredicate, GqlExpr, LabelExpr, PathMode, PathPattern, PathSearchKind,
	PathStep, QuantifierKind,
};

/// Parses a query with a single MATCH clause holding a single path pattern
/// and returns the pattern.
#[track_caller]
fn single_pattern(source: &str) -> PathPattern {
	let query = parse(source);
	let mut clauses = match_clauses(&query);
	assert_eq!(clauses.len(), 1, "expected a single match clause in {source:?}");
	let mut clause = clauses.pop().expect("one match clause").clone();
	assert_eq!(clause.patterns.len(), 1, "expected a single pattern in {source:?}");
	clause.patterns.pop().expect("one pattern")
}

/// Parses `MATCH <pattern> RETURN 1` and returns the single edge-node step.
#[track_caller]
fn single_step(pattern: &str) -> PathStep {
	let source = format!("MATCH {pattern} RETURN 1");
	let mut pattern = single_pattern(&source);
	assert_eq!(pattern.steps.len(), 1, "expected a single step in {source:?}");
	pattern.steps.pop().expect("one step")
}

#[rstest]
#[case::left("(a)<-[f]-(b)", EdgeDirection::Left)]
#[case::undirected("(a)~[f]~(b)", EdgeDirection::Undirected)]
#[case::right("(a)-[f]->(b)", EdgeDirection::Right)]
#[case::left_or_undirected("(a)<~[f]~(b)", EdgeDirection::LeftOrUndirected)]
#[case::undirected_or_right("(a)~[f]~>(b)", EdgeDirection::UndirectedOrRight)]
#[case::left_or_right("(a)<-[f]->(b)", EdgeDirection::LeftOrRight)]
#[case::any("(a)-[f]-(b)", EdgeDirection::Any)]
fn full_edge_directions(#[case] pattern: &str, #[case] expected: EdgeDirection) {
	// The seven `fullEdgePattern` forms (GQL.g4:1035-1076), in grammar order.
	let step = single_step(pattern);
	assert_eq!(step.edge.direction, expected);
	assert_eq!(step.edge.var.as_ref().map(|x| x.name.as_str()), Some("f"));
	assert_eq!(step.node.var.as_ref().map(|x| x.name.as_str()), Some("b"));
}

#[rstest]
#[case::left("(a)<-(b)", EdgeDirection::Left)]
#[case::undirected("(a)~(b)", EdgeDirection::Undirected)]
#[case::right("(a)->(b)", EdgeDirection::Right)]
#[case::left_or_undirected("(a)<~(b)", EdgeDirection::LeftOrUndirected)]
#[case::undirected_or_right("(a)~>(b)", EdgeDirection::UndirectedOrRight)]
#[case::left_or_right("(a)<->(b)", EdgeDirection::LeftOrRight)]
#[case::any("(a)-(b)", EdgeDirection::Any)]
fn abbreviated_edge_directions(#[case] pattern: &str, #[case] expected: EdgeDirection) {
	// The seven `abbreviatedEdgePattern` forms (GQL.g4:1078-1086).
	let step = single_step(pattern);
	assert_eq!(step.edge.direction, expected);
	assert!(step.edge.var.is_none());
	assert!(step.edge.label.is_none());
}

#[test]
fn empty_edge_filler() {
	// `elementPatternFiller` parts are all optional: `-[]->` is valid.
	let step = single_step("()-[]->()");
	assert_eq!(step.edge.direction, EdgeDirection::Right);
	assert!(step.edge.var.is_none() && step.edge.label.is_none());
	assert!(step.node.var.is_none() && step.node.label.is_none());
}

#[rstest]
#[case::undirected_right_left("MATCH (a)<~[f]~>(b) RETURN 1", "expected `]~`")]
#[case::minus_tilde("MATCH (a)-[f]~>(b) RETURN 1", "expected `]->` or `]-`")]
#[case::tilde_minus("MATCH (a)~[f]->(b) RETURN 1", "expected `]~` or `]~>`")]
#[case::left_tilde("MATCH (a)<-[f]~(b) RETURN 1", "expected `]-` or `]->`")]
fn invalid_edge_closers_rejected(#[case] source: &str, #[case] expected: &str) {
	// Each opening bracket token admits only specific closing tokens;
	// `<~[…]~>` in particular is not a grammar form.
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[rstest]
#[case::star("*", QuantifierKind::Star)]
#[case::plus("+", QuantifierKind::Plus)]
#[case::question("?", QuantifierKind::Question)]
#[case::fixed("{3}", QuantifierKind::Fixed(3))]
#[case::range("{1,3}", QuantifierKind::Range(Some(1), Some(3)))]
#[case::lower_open_upper("{1,}", QuantifierKind::Range(Some(1), None))]
#[case::upper_only("{,3}", QuantifierKind::Range(None, Some(3)))]
#[case::both_open("{,}", QuantifierKind::Range(None, None))]
fn edge_quantifiers(#[case] quantifier: &str, #[case] expected: QuantifierKind) {
	// `graphPatternQuantifier` (GQL.g4:1125-1146), postfix on the edge.
	let step = single_step(&format!("(a)-[:knows]->{quantifier}(b)"));
	assert_eq!(step.edge.quantifier.as_ref().map(|x| x.kind), Some(expected));
}

#[test]
fn quantifier_on_abbreviated_edge() {
	let step = single_step("(a)->{2}(b)");
	assert_eq!(step.edge.quantifier.as_ref().map(|x| x.kind), Some(QuantifierKind::Fixed(2)));
	let step = single_step("(a)-*(b)");
	assert_eq!(step.edge.quantifier.as_ref().map(|x| x.kind), Some(QuantifierKind::Star));
}

#[test]
fn quantifier_bounds_accept_all_integer_radixes() {
	// Quantifier bounds are `unsignedInteger` (GQL.g4:1133), like counts.
	let step = single_step("(a)-[:knows]->{0x2,0o10}(b)");
	assert_eq!(
		step.edge.quantifier.as_ref().map(|x| x.kind),
		Some(QuantifierKind::Range(Some(2), Some(8)))
	);
}

#[rstest]
#[case::empty_braces("MATCH (a)-[k]->{}(b) RETURN 1", "an unsigned integer or `,`")]
#[case::float_bound("MATCH (a)-[k]->{1.5}(b) RETURN 1", "expected an unsigned integer")]
#[case::bound_overflow("MATCH (a)-[k]->{4294967296}(b) RETURN 1", "Quantifier bound is too large")]
#[case::star_after_node("MATCH (a)*(b) RETURN 1", "Quantifiers may only follow an edge pattern")]
#[case::braces_after_node(
	"MATCH (a)-[k]->(b){1,3} RETURN 1",
	"Quantifiers may only follow an edge pattern"
)]
#[case::question_after_node("MATCH (a)? RETURN 1", "Quantifiers may only follow an edge pattern")]
fn quantifier_errors(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[rstest]
#[case::name("(a:person)", "person")]
#[case::wildcard("(a:%)", "%")]
#[case::negation("(a:!archived)", "!(archived)")]
#[case::conjunction("(a:person&admin)", "(person&admin)")]
#[case::disjunction("(a:person|company)", "(person|company)")]
#[case::conjunction_binds_tighter("(a:w&x|y&z)", "((w&x)|(y&z))")]
#[case::negation_binds_tightest("(a:!x&y)", "(!(x)&y)")]
#[case::parenthesized("(a:(x|y)&z)", "((x|y)&z)")]
#[case::double_negation("(a:!!x)", "!(!(x))")]
#[case::negated_group("(a:!(x|y))", "!((x|y))")]
#[case::is_introducer("(a IS person)", "person")]
#[case::delimited_name("(a:\"count\")", "count")]
#[case::non_reserved_name("(a:node)", "node")]
fn node_label_expressions(#[case] pattern: &str, #[case] expected: &str) {
	// `labelExpression` (GQL.g4:1102-1109), precedence `!` > `&` > `|`;
	// labels are introduced by `:` or `IS` (`isOrColon`, GQL.g4:1024).
	let source = format!("MATCH {pattern} RETURN 1");
	let pattern = single_pattern(&source);
	let label = pattern.start.label.as_ref().expect("a label expression");
	assert_eq!(label_str(label), expected);
}

#[test]
fn edge_label_expressions() {
	let step = single_step("(a)-[:knows|likes]->(b)");
	assert_eq!(label_str(step.edge.label.as_ref().expect("a label")), "(knows|likes)");
	let step = single_step("(a)<-[k IS knows]-(b)");
	assert_eq!(label_str(step.edge.label.as_ref().expect("a label")), "knows");
}

#[test]
fn label_expression_spans() {
	// Every label expression node can produce a span, including wildcard and
	// operator chains, so lowering can reject them precisely.
	let source = "MATCH (a:!%|p) RETURN 1";
	let query = parse(source);
	let label =
		match_clauses(&query)[0].patterns[0].start.label.as_ref().expect("a label expression");
	let span = label.span();
	assert_eq!(&source[span.offset as usize..(span.offset + span.len) as usize], "!%|p");
	let LabelExpr::Disjunction(negation, _, _) = label else {
		panic!("expected a disjunction, got {label:?}");
	};
	let span = negation.span();
	assert_eq!(&source[span.offset as usize..(span.offset + span.len) as usize], "!%");
}

#[test]
fn label_reserved_word_rejected() {
	let error = parse_err("MATCH (a:count) RETURN 1");
	assert!(error.contains("`count` is a reserved word"), "{error}");
	let error = parse_err("MATCH (a:person&MATCH) RETURN 1");
	assert!(error.contains("`MATCH` is a reserved word"), "{error}");
}

#[test]
fn cypher_label_conjunction_rejected() {
	// openCypher's `:A:B` label chaining does not exist in GQL; conjunction
	// is `&` (deviation (f)4 in the reference).
	let error = parse_err("MATCH (n:A:B) RETURN 1");
	assert!(error.contains("expected the delimiter `)`"), "{error}");
}

#[test]
fn property_maps() {
	let pattern = single_pattern("MATCH (a {name: 'x', age: 30 + $base, node: true}) RETURN 1");
	let Some(ElementPredicate::Props(props)) = &pattern.start.predicate else {
		panic!("expected a property map, got {:?}", pattern.start.predicate);
	};
	let keys: Vec<_> = props.iter().map(|(key, _)| key.name.as_str()).collect();
	// `node` is a non-reserved word and thus a valid property key.
	assert_eq!(keys, vec!["name", "age", "node"]);
	// Property values are full value expressions.
	assert!(matches!(props[1].1, GqlExpr::Binary { .. }));
}

#[test]
fn property_map_delimited_key() {
	let step = single_step("(a)-[k {\"key name\": 1}]->(b)");
	let Some(ElementPredicate::Props(props)) = &step.edge.predicate else {
		panic!("expected a property map, got {:?}", step.edge.predicate);
	};
	assert_eq!(props[0].0.name, "key name");
}

#[test]
fn property_map_requires_a_pair() {
	// `propertyKeyValuePairList` (GQL.g4:1018) requires at least one pair.
	let error = parse_err("MATCH (a {}) RETURN 1");
	assert!(error.contains("expected an identifier"), "{error}");
}

#[test]
fn property_map_reserved_key_rejected() {
	let error = parse_err("MATCH (a {count: 1}) RETURN 1");
	assert!(error.contains("`count` is a reserved word"), "{error}");
}

#[test]
fn inline_where_clauses() {
	// `elementPatternWhereClause` (GQL.g4:1011) inside node and edge fillers.
	let pattern = single_pattern("MATCH (a:person WHERE a.age > 21) RETURN 1");
	assert!(matches!(pattern.start.predicate, Some(ElementPredicate::Where(_))));

	let step = single_step("(a)-[k:knows WHERE k.since > 2020]->(b)");
	let Some(ElementPredicate::Where(GqlExpr::Binary {
		..
	})) = &step.edge.predicate
	else {
		panic!("expected an inline where clause, got {:?}", step.edge.predicate);
	};
}

#[rstest]
#[case::where_then_props("MATCH (a WHERE a.x {y: 1}) RETURN 1")]
#[case::props_then_where("MATCH (a {y: 1} WHERE a.x) RETURN 1")]
#[case::edge_where_then_props("MATCH (a)-[k WHERE k.x {y: 1}]->(b) RETURN 1")]
fn where_and_props_are_mutually_exclusive(#[case] source: &str) {
	// `elementPatternPredicate` (GQL.g4:1009) is a single alternative.
	let error = parse_err(source);
	assert!(
		error.contains("may have either a WHERE clause or a property map, not both"),
		"{error}"
	);
}

#[test]
fn element_filler_forms() {
	// All filler parts are optional (`elementPatternFiller`, GQL.g4:997).
	let pattern = single_pattern("MATCH () RETURN 1");
	assert!(pattern.start.var.is_none() && pattern.start.label.is_none());
	assert!(pattern.start.predicate.is_none());

	let pattern = single_pattern("MATCH (:person) RETURN 1");
	assert!(pattern.start.var.is_none());
	assert!(pattern.start.label.is_some());

	let pattern = single_pattern("MATCH (IS person) RETURN 1");
	assert!(pattern.start.var.is_none());
	assert!(pattern.start.label.is_some());

	let pattern = single_pattern("MATCH (WHERE true) RETURN 1");
	assert!(matches!(pattern.start.predicate, Some(ElementPredicate::Where(_))));

	let pattern = single_pattern("MATCH ({k: 1}) RETURN 1");
	assert!(matches!(pattern.start.predicate, Some(ElementPredicate::Props(_))));
}

#[test]
fn pattern_variable_forms() {
	// Double-quoted tokens are delimited identifiers in variable position.
	let pattern = single_pattern("MATCH (\"my var\":person) RETURN 1");
	assert_eq!(pattern.start.var.as_ref().map(|x| x.name.as_str()), Some("my var"));

	let error = parse_err("MATCH (value) RETURN 1");
	assert!(error.contains("`value` is a reserved word"), "{error}");
	let error = parse_err("MATCH (a)-[match]->(b) RETURN 1");
	assert!(error.contains("`match` is a reserved word"), "{error}");
}

#[test]
fn path_variables() {
	let pattern = single_pattern("MATCH p = (a)-[k]->(b) RETURN 1");
	assert_eq!(pattern.path_var.as_ref().map(|x| x.name.as_str()), Some("p"));

	let pattern = single_pattern("MATCH \"my path\" = (a) RETURN 1");
	assert_eq!(pattern.path_var.as_ref().map(|x| x.name.as_str()), Some("my path"));

	// A reserved word cannot declare a path variable.
	let error = parse_err("MATCH count = (a) RETURN 1");
	assert!(error.contains("expected a node pattern"), "{error}");
}

#[test]
fn multi_step_path() {
	let pattern = single_pattern("MATCH (a)-[j]->(b)<-[k]-(c) RETURN 1");
	assert_eq!(pattern.steps.len(), 2);
	assert_eq!(pattern.steps[0].edge.direction, EdgeDirection::Right);
	assert_eq!(pattern.steps[0].node.var.as_ref().map(|x| x.name.as_str()), Some("b"));
	assert_eq!(pattern.steps[1].edge.direction, EdgeDirection::Left);
	assert_eq!(pattern.steps[1].node.var.as_ref().map(|x| x.name.as_str()), Some("c"));
}

#[test]
fn quantified_edge_with_filler() {
	let step = single_step("(a)-[k:knows]->{1,3}(b)");
	assert_eq!(step.edge.var.as_ref().map(|x| x.name.as_str()), Some("k"));
	assert_eq!(label_str(step.edge.label.as_ref().expect("a label")), "knows");
	assert_eq!(
		step.edge.quantifier.as_ref().map(|x| x.kind),
		Some(QuantifierKind::Range(Some(1), Some(3)))
	);
}

#[rstest]
#[case::node_after_node("MATCH (a)(b) RETURN 1", "expected an edge pattern between node patterns")]
#[case::full_edge_at_start("MATCH -[k]->(b) RETURN 1", "must start with a node pattern")]
#[case::abbreviated_edge_at_start("MATCH ->(b) RETURN 1", "must start with a node pattern")]
#[case::edge_at_end("MATCH (a)-[k]->", "expected a node pattern after this edge pattern")]
#[case::edge_then_return("MATCH (a)- RETURN 1", "expected a node pattern after this edge pattern")]
#[case::two_edges("MATCH (a)-[j]->-[k]->(b) RETURN 1", "expected a node pattern")]
fn node_edge_alternation_enforced(#[case] source: &str, #[case] expected: &str) {
	// `pathTerm : pathFactor+` is a flat sequence; node-edge alternation is
	// semantic (16.7) and enforced with targeted errors.
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[rstest]
#[case::subpath("MATCH ((a)-[k]->(b)) RETURN 1")]
#[case::nested_node("MATCH ((a)) RETURN 1")]
#[case::edge_inside("MATCH (-[k]->(b)) RETURN 1")]
#[case::abbreviated_inside("MATCH (<-(b)) RETURN 1")]
fn parenthesized_path_patterns_rejected(#[case] source: &str) {
	// `parenthesizedPathPatternExpression` (GQL.g4:1088).
	let error = parse_err(source);
	assert!(error.contains("Parenthesized path pattern expressions"), "{error}");
}

#[test]
fn subpath_variable_rejected() {
	let error = parse_err("MATCH (p = (a)) RETURN 1");
	assert!(error.contains("subpath variables"), "{error}");
}

#[rstest]
#[case::at_start("MATCH -/x/->(b) RETURN 1")]
#[case::minus_step("MATCH (a)-/x/->(b) RETURN 1")]
#[case::left_step("MATCH (a)<-/x/-(b) RETURN 1")]
#[case::tilde_step("MATCH (a)~/x/~(b) RETURN 1")]
#[case::left_tilde_step("MATCH (a)<~/x/~(b) RETURN 1")]
fn simplified_path_patterns_rejected(#[case] source: &str) {
	// `simplifiedPathPatternExpression` (16.12), the `-/ … /->` slash forms.
	let error = parse_err(source);
	assert!(error.contains("Simplified path pattern expressions"), "{error}");
}

#[test]
fn pattern_alternations_rejected() {
	// `pathTerm |+| pathTerm` / `pathTerm | pathTerm` (GQL.g4:966-970).
	let error = parse_err("MATCH (a)|+|(b) RETURN 1");
	assert!(error.contains("Multiset alternation (`|+|`)"), "{error}");
	let error = parse_err("MATCH (a)|(b) RETURN 1");
	assert!(error.contains("Pattern unions (`|`)"), "{error}");
}

#[rstest]
// Bare path modes (`pathModePrefix`): search defaults to ALL.
#[case::walk("WALK", PathSearchKind::All, Some(PathMode::Walk))]
#[case::trail("TRAIL", PathSearchKind::All, Some(PathMode::Trail))]
#[case::simple("SIMPLE", PathSearchKind::All, Some(PathMode::Simple))]
#[case::acyclic("ACYCLIC", PathSearchKind::All, Some(PathMode::Acyclic))]
// `allPathSearch` / `anyPathSearch`.
#[case::all("ALL", PathSearchKind::All, None)]
#[case::all_trail("ALL TRAIL", PathSearchKind::All, Some(PathMode::Trail))]
#[case::all_paths("ALL PATHS", PathSearchKind::All, None)]
#[case::any("ANY", PathSearchKind::Any { count: None }, None)]
#[case::any_count("ANY 3", PathSearchKind::Any { count: Some(3) }, None)]
#[case::any_paths("ANY 2 PATHS", PathSearchKind::Any { count: Some(2) }, None)]
#[case::any_simple("ANY SIMPLE", PathSearchKind::Any { count: None }, Some(PathMode::Simple))]
// `allShortestPathSearch` / `anyShortestPathSearch`.
#[case::all_shortest("ALL SHORTEST", PathSearchKind::AllShortest, None)]
#[case::any_shortest("ANY SHORTEST", PathSearchKind::AnyShortest, None)]
#[case::all_shortest_simple(
	"ALL SHORTEST SIMPLE",
	PathSearchKind::AllShortest,
	Some(PathMode::Simple)
)]
// `countedShortestPathSearch` / `countedShortestGroupSearch`.
#[case::shortest_k("SHORTEST 3", PathSearchKind::ShortestCounted { count: 3 }, None)]
#[case::shortest_k_trail_paths("SHORTEST 3 TRAIL PATHS", PathSearchKind::ShortestCounted { count: 3 }, Some(PathMode::Trail))]
#[case::shortest_group("SHORTEST GROUP", PathSearchKind::ShortestGroups { count: None }, None)]
#[case::shortest_k_groups("SHORTEST 2 GROUPS", PathSearchKind::ShortestGroups { count: Some(2) }, None)]
fn path_pattern_prefix_parses(
	#[case] prefix_src: &str,
	#[case] kind: PathSearchKind,
	#[case] mode: Option<PathMode>,
) {
	// `pathPatternPrefix` (GQL.g4:896-962) now parses into the AST; the lowering,
	// not the parser, rejects the combinations it does not yet execute.
	let source = format!("MATCH {prefix_src} (a)->(b) RETURN 1");
	let pattern = single_pattern(&source);
	let parsed = pattern.prefix.unwrap_or_else(|| panic!("expected a parsed prefix in {source:?}"));
	assert_eq!(parsed.kind, kind, "{source}");
	assert_eq!(parsed.mode, mode, "{source}");
}

#[rstest]
#[case::bare_shortest("MATCH SHORTEST (a)->(b) RETURN 1", "requires a path count")]
#[case::shortest_zero("MATCH SHORTEST 0 (a)->(b) RETURN 1", "must be a positive integer")]
#[case::any_zero("MATCH ANY 0 (a)->(b) RETURN 1", "must be a positive integer")]
fn path_pattern_prefix_parse_errors(#[case] source: &str, #[case] needle: &str) {
	let error = parse_err(source);
	assert!(error.contains(needle), "{error}");
}

#[test]
fn double_minus_is_a_comment() {
	// openCypher trap (deviation (f)1): `--` introduces a line comment, so
	// `(a)--(b) RETURN 1` comments out everything after `(a)`, leaving just the
	// `MATCH (a)` read body with no RETURN — which the parser now accepts (the
	// missing RETURN is enforced in lowering, not here).
	let program = crate::gql::parse_str("MATCH (a)--(b) RETURN 1")
		.expect("the comment leaves a bare MATCH (a)")
		.program;
	assert_eq!(program.steps.len(), 1);
	assert!(program.ret.is_none());

	// With the RETURN on its own line the query is valid: the pattern is
	// just `(a)` and the `(b)` is commented away.
	let pattern = single_pattern("MATCH (a)--(b)\nRETURN 1");
	assert_eq!(pattern.start.var.as_ref().map(|x| x.name.as_str()), Some("a"));
	assert!(pattern.steps.is_empty());
}

#[test]
fn cypher_variable_length_syntax_rejected() {
	// openCypher trap (deviation (f)5): GQL quantifies postfix, never inside
	// the brackets.
	let error = parse_err("MATCH (a)-[:knows*1..3]->(b) RETURN 1");
	assert!(error.contains("Unexpected token `*`"), "{error}");
}
