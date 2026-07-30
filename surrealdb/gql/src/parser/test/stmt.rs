//! Statement level tests: `MATCH`/`OPTIONAL MATCH` clauses, the `RETURN`
//! clause with `DISTINCT`/`ALL`, `*` and aliases, the trailing `ORDER BY`,
//! `OFFSET`/`SKIP` and `LIMIT` page clauses, and the targeted rejections of
//! all statement forms outside the v1 subset.

use rstest::rstest;

use super::{match_clauses, parse, parse_err, parse_return_items};
use crate::gql::ast::{GqlExpr, GqlLiteral, MatchItem, ReturnItems, SetQuantifier};
use crate::gql::{GqlParserSettings, parse_with_settings};

/// Asserts that a count specification holds the given integer literal.
#[track_caller]
fn assert_count(count: &Option<GqlExpr>, expected: i64) {
	match count {
		Some(GqlExpr::Literal(GqlLiteral::Integer(x), _)) => assert_eq!(*x, expected),
		x => panic!("expected the integer count {expected}, got {x:?}"),
	}
}

#[rstest]
#[case::none("MATCH (a) RETURN a.x", false, false, false)]
#[case::order("MATCH (a) RETURN a.x ORDER BY a.x", true, false, false)]
#[case::order_offset("MATCH (a) RETURN a.x ORDER BY a.x OFFSET 1", true, true, false)]
#[case::order_limit("MATCH (a) RETURN a.x ORDER BY a.x LIMIT 2", true, false, true)]
#[case::order_skip_limit("MATCH (a) RETURN a.x ORDER BY a.x SKIP 1 LIMIT 2", true, true, true)]
#[case::offset("MATCH (a) RETURN a.x OFFSET 1", false, true, false)]
#[case::skip_limit("MATCH (a) RETURN a.x SKIP 1 LIMIT 2", false, true, true)]
#[case::limit("MATCH (a) RETURN a.x LIMIT 2", false, false, true)]
fn page_clause_combinations(
	#[case] source: &str,
	#[case] order: bool,
	#[case] skip: bool,
	#[case] limit: bool,
) {
	// `orderByAndPageStatement` (GQL.g4:652): each later clause may appear
	// without the earlier ones, but never before them.
	let query = parse(source);
	assert_eq!(!query.ret.order_by.is_empty(), order);
	assert_eq!(query.ret.skip.is_some(), skip);
	assert_eq!(query.ret.limit.is_some(), limit);
}

#[rstest]
#[case::distinct("RETURN DISTINCT a.x", Some(SetQuantifier::Distinct))]
#[case::all("RETURN ALL a.x", Some(SetQuantifier::All))]
#[case::none("RETURN a.x", None)]
fn set_quantifiers(#[case] source: &str, #[case] expected: Option<SetQuantifier>) {
	let source = format!("MATCH (a) {source}");
	assert_eq!(parse(&source).ret.quantifier, expected);
}

#[test]
fn return_star() {
	let query = parse("MATCH (a) RETURN *");
	assert_eq!(query.ret.quantifier, None);
	assert!(matches!(query.ret.items, ReturnItems::Star));

	let query = parse("MATCH (a) RETURN DISTINCT * ORDER BY a.x LIMIT 1");
	assert_eq!(query.ret.quantifier, Some(SetQuantifier::Distinct));
	assert!(matches!(query.ret.items, ReturnItems::Star));
}

#[test]
fn return_star_takes_no_item_list() {
	// `returnStatementBody : setQuantifier? (ASTERISK | returnItemList)`
	// (GQL.g4:668): `*` and an item list are mutually exclusive.
	assert!(parse_err("MATCH (a) RETURN *, a").contains("expected the query to end"));
}

#[test]
fn return_item_aliases() {
	let items = parse_return_items(
		"MATCH (a) RETURN a.x AS y, a.y AS \"full name\", a.z AS `b c`, count(a) AS total, a.w",
	);
	let aliases: Vec<_> =
		items.iter().map(|x| x.alias.as_ref().map(|alias| alias.name.as_str())).collect();
	assert_eq!(aliases, vec![Some("y"), Some("full name"), Some("b c"), Some("total"), None]);
}

#[test]
fn return_item_text_is_verbatim() {
	// The verbatim source slice is the default column name; parenthesized
	// expressions keep their parentheses.
	let items = parse_return_items("RETURN a.x + 1, (a.x), 'str' AS s, upper( a.name )");
	let texts: Vec<_> = items.iter().map(|x| x.text.as_str()).collect();
	assert_eq!(texts, vec!["a.x + 1", "(a.x)", "'str'", "upper( a.name )"]);
}

#[test]
fn alias_reserved_word_rejected() {
	let error = parse_err("MATCH (a) RETURN a.x AS count");
	assert!(error.contains("`count` is a reserved word"), "{error}");
}

#[rstest]
#[case::skip("MATCH (a) RETURN a SKIP 5")]
#[case::offset("MATCH (a) RETURN a OFFSET 5")]
fn skip_and_offset_are_synonyms(#[case] source: &str) {
	// `offsetSynonym : OFFSET | SKIP_RESERVED_WORD` (GQL.g4:1374).
	let query = parse(source);
	assert_count(&query.ret.skip, 5);
	assert!(query.ret.limit.is_none());
}

#[rstest]
#[case::asc("ASC", Some(true))]
#[case::ascending("ASCENDING", Some(true))]
#[case::desc("DESC", Some(false))]
#[case::descending("DESCENDING", Some(false))]
#[case::unspecified("", None)]
fn order_directions(#[case] direction: &str, #[case] expected: Option<bool>) {
	let source = format!("MATCH (a) RETURN a.x ORDER BY a.x {direction}");
	let query = parse(&source);
	assert_eq!(query.ret.order_by.len(), 1);
	assert_eq!(query.ret.order_by[0].ascending, expected);
	assert_eq!(query.ret.order_by[0].nulls_first, None);
}

#[rstest]
#[case::nulls_first("NULLS FIRST", None, Some(true))]
#[case::nulls_last("NULLS LAST", None, Some(false))]
#[case::desc_nulls_last("DESC NULLS LAST", Some(false), Some(false))]
#[case::asc_nulls_first("ASCENDING NULLS FIRST", Some(true), Some(true))]
fn null_ordering(
	#[case] spec: &str,
	#[case] ascending: Option<bool>,
	#[case] nulls_first: Option<bool>,
) {
	// `sortSpecification : sortKey orderingSpecification? nullOrdering?`
	// (GQL.g4:1341).
	let source = format!("MATCH (a) RETURN a.x ORDER BY a.x {spec}");
	let query = parse(&source);
	assert_eq!(query.ret.order_by[0].ascending, ascending);
	assert_eq!(query.ret.order_by[0].nulls_first, nulls_first);
}

#[test]
fn order_by_multiple_keys() {
	let query = parse("MATCH (a) RETURN a ORDER BY a.x ASC, a.y DESC NULLS LAST, a.z + 1");
	assert_eq!(query.ret.order_by.len(), 3);
	assert_eq!(query.ret.order_by[0].ascending, Some(true));
	assert_eq!(query.ret.order_by[1].ascending, Some(false));
	assert_eq!(query.ret.order_by[1].nulls_first, Some(false));
	// The sort key is a full value expression.
	assert!(matches!(query.ret.order_by[2].expr, GqlExpr::Binary { .. }));
}

#[test]
fn null_ordering_requires_first_or_last() {
	let error = parse_err("MATCH (a) RETURN a ORDER BY a.x NULLS wrong");
	assert!(error.contains("`FIRST` or `LAST`"), "{error}");
}

#[test]
fn page_clauses_accept_parameters() {
	// `nonNegativeIntegerSpecification : unsignedInteger |
	// dynamicParameterSpecification` (GQL.g4:2268).
	let query = parse("MATCH (a) RETURN a SKIP $s LIMIT $l");
	assert!(matches!(&query.ret.skip, Some(GqlExpr::Param { name, .. }) if name == "s"));
	assert!(matches!(&query.ret.limit, Some(GqlExpr::Param { name, .. }) if name == "l"));
}

#[rstest]
#[case::hex("RETURN 1 LIMIT 0x10", 16)]
#[case::octal("RETURN 1 LIMIT 0o17", 15)]
#[case::binary("RETURN 1 LIMIT 0b101", 5)]
#[case::separated("RETURN 1 LIMIT 1_000", 1000)]
fn page_clauses_accept_all_integer_radixes(#[case] source: &str, #[case] expected: i64) {
	// `unsignedInteger` (GQL.g4:2997) covers all four integer forms.
	assert_count(&parse(source).ret.limit, expected);
}

#[rstest]
#[case::float("RETURN 1 LIMIT 1.5", "an unsigned integer or a parameter")]
#[case::negative("RETURN 1 LIMIT -1", "an unsigned integer or a parameter")]
#[case::string("RETURN 1 SKIP 'x'", "an unsigned integer or a parameter")]
#[case::substituted("RETURN 1 SKIP $$x", "Substituted parameters")]
#[case::overflow("RETURN 1 LIMIT 99999999999999999999", "Integer literal is too large")]
fn count_specification_errors(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[test]
fn multiple_match_clauses() {
	// A plain MATCH, an `OPTIONAL MATCH`, then a plain MATCH, interleaved.
	let query = parse("MATCH (a) WHERE a.x OPTIONAL MATCH (b) WHERE b.y MATCH (c) RETURN 1");
	assert_eq!(query.items.len(), 3);

	let MatchItem::Match(first) = &query.items[0] else {
		panic!("expected a plain MATCH item, got {:?}", query.items[0]);
	};
	assert!(first.where_clause.is_some());

	let MatchItem::Optional(block) = &query.items[1] else {
		panic!("expected an OPTIONAL item, got {:?}", query.items[1]);
	};
	assert_eq!(block.items.len(), 1);
	let MatchItem::Match(optional_clause) = &block.items[0] else {
		panic!("expected a MATCH clause inside the OPTIONAL block");
	};
	assert!(optional_clause.where_clause.is_some());

	let MatchItem::Match(third) = &query.items[2] else {
		panic!("expected a plain MATCH item, got {:?}", query.items[2]);
	};
	assert!(third.where_clause.is_none());
}

#[test]
fn return_without_match() {
	let query = parse("RETURN 1");
	assert!(query.items.is_empty());
}

#[test]
fn multiple_patterns_per_match() {
	// `pathPatternList : pathPattern (COMMA pathPattern)*` (GQL.g4:830).
	let query = parse("MATCH (a), (b)-[k]->(c), p = (d) RETURN 1");
	let clauses = match_clauses(&query);
	assert_eq!(clauses.len(), 1);
	assert_eq!(clauses[0].patterns.len(), 3);
	assert_eq!(clauses[0].patterns[2].path_var.as_ref().map(|x| x.name.as_str()), Some("p"));
}

#[test]
fn keywords_are_case_insensitive() {
	let query = parse("match (a) where a.x return distinct a.x order by a.x desc skip 1 limit 2");
	assert_eq!(query.ret.quantifier, Some(SetQuantifier::Distinct));
	assert_eq!(query.ret.order_by[0].ascending, Some(false));
	assert_count(&query.ret.skip, 1);
	assert_count(&query.ret.limit, 2);
}

#[test]
fn match_without_return_parses() {
	// `RETURN` is now optional at the parser level (a linear data-modifying
	// statement may end without a result statement); a read-only query that
	// omits it is rejected later, in lowering, not here.
	let program = crate::gql::parse_str("MATCH (a)").expect("MATCH without RETURN parses").program;
	assert_eq!(program.steps.len(), 1);
	assert!(matches!(program.steps[0], crate::gql::ast::GqlStep::Read(_)));
	assert!(program.ret.is_none());
}

#[test]
fn query_must_end_after_page_clauses() {
	assert!(parse_err("RETURN 1 RETURN 2").contains("expected the query to end"));
}

#[rstest]
#[case::group_after_items("MATCH (a) RETURN a.x GROUP BY a.x")]
#[case::group_after_star("MATCH (a) RETURN * GROUP BY a.x")]
#[case::group_multiple_keys("MATCH (a) RETURN a.x GROUP BY a.x, a.y")]
fn group_by_parses(#[case] source: &str) {
	// A `groupByClause` (GQL.g4:671) parses directly after the return items; the
	// lowering enforces the GQL shape (valid keys, aggregates, etc.).
	parse(source);
}

#[rstest]
#[case::group_after_order("MATCH (a) RETURN a.x ORDER BY a.x GROUP BY a.x")]
#[case::group_after_limit("MATCH (a) RETURN a.x LIMIT 1 GROUP BY a.x")]
fn group_by_after_page_clauses_rejected(#[case] source: &str) {
	// GROUP BY must directly follow the return items; after a page clause it is
	// an out-of-order error.
	let error = parse_err(source);
	assert!(error.contains("Unexpected `GROUP` clause"), "{error}");
}

#[test]
fn finish_rejected() {
	// `primitiveResultStatement : … | FINISH` (GQL.g4:662).
	let error = parse_err("MATCH (a) FINISH");
	assert!(error.contains("FINISH statements are not supported yet"), "{error}");
}

#[rstest]
#[case::union("MATCH (a) RETURN a UNION MATCH (b) RETURN b")]
#[case::except("MATCH (a) RETURN a EXCEPT MATCH (b) RETURN b")]
#[case::intersect("MATCH (a) RETURN a INTERSECT MATCH (b) RETURN b")]
#[case::otherwise("MATCH (a) RETURN a OTHERWISE MATCH (b) RETURN b")]
fn composite_queries_rejected(#[case] source: &str) {
	// `compositeQueryExpression` (GQL.g4:504).
	let error = parse_err(source);
	assert!(error.contains("Composite queries"), "{error}");
}

#[rstest]
#[case::let_stmt("LET x = 1 RETURN x", "`LET` statements")]
#[case::for_stmt("FOR x IN [1] RETURN x", "`FOR` statements")]
#[case::filter_stmt("FILTER a.x > 1 RETURN a", "`FILTER` statements")]
#[case::use_stmt("USE g MATCH (a) RETURN a", "`USE` statements")]
#[case::select("SELECT a.x FROM a", "`SELECT` statements")]
#[case::call("CALL { MATCH (a) RETURN a }", "`CALL` statements")]
fn unsupported_statements_rejected(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
	assert!(error.contains("not supported yet"), "{error}");
}

#[rstest]
#[case::insert("INSERT (a:person {name: 'x'})")]
#[case::insert_return("INSERT (a:person) RETURN 1")]
#[case::set("MATCH (a) SET a.x = 1 RETURN 1")]
#[case::set_all("MATCH (a) SET a = {x: 1}")]
#[case::remove("MATCH (a) REMOVE a.x RETURN 1")]
#[case::delete("MATCH (a) DELETE a")]
#[case::detach("MATCH (a) DETACH DELETE a")]
#[case::nodetach("MATCH (a) NODETACH DELETE a")]
fn mutation_statements_parse(#[case] source: &str) {
	// The four ISO data-modifying statements now parse; their semantics (and
	// any rejections, e.g. label mutations) are enforced in lowering.
	crate::gql::parse_str(source)
		.unwrap_or_else(|e| panic!("expected {source:?} to parse: {:?}", e.render_on(source)));
}

#[rstest]
#[case::create("CREATE GRAPH g RETURN 1", "`CREATE` statements")]
#[case::drop("DROP GRAPH g", "`DROP` statements")]
fn catalog_statements_rejected(#[case] source: &str, #[case] expected: &str) {
	// `CREATE GRAPH` / `DROP GRAPH` are ISO catalog statements, outside the
	// supported data-modifying subset.
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
	assert!(error.contains("not supported yet"), "{error}");
}

#[rstest]
#[case::order_after_limit("MATCH (a) RETURN a LIMIT 1 ORDER BY a", "Unexpected `ORDER` clause")]
#[case::offset_after_limit("MATCH (a) RETURN a LIMIT 1 OFFSET 2", "Unexpected `OFFSET` clause")]
#[case::order_after_skip("MATCH (a) RETURN a SKIP 1 ORDER BY a", "Unexpected `ORDER` clause")]
#[case::duplicate_limit("MATCH (a) RETURN a LIMIT 1 LIMIT 2", "Unexpected `LIMIT` clause")]
#[case::skip_after_offset("MATCH (a) RETURN a OFFSET 1 SKIP 2", "Unexpected `SKIP` clause")]
#[case::duplicate_order("MATCH (a) RETURN a ORDER BY a ORDER BY a", "Unexpected `ORDER` clause")]
fn page_clause_order_violations(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
	assert!(error.contains("at most once each, in that order"), "{error}");
}

#[rstest]
#[case::order("MATCH (a) ORDER BY a.x MATCH (b) RETURN 1", "Standalone `ORDER` statements")]
#[case::offset("MATCH (a) OFFSET 1 MATCH (b) RETURN 1", "Standalone `OFFSET` statements")]
#[case::skip("MATCH (a) SKIP 1 RETURN 1", "Standalone `SKIP` statements")]
#[case::limit("MATCH (a) LIMIT 1 RETURN 1", "Standalone `LIMIT` statements")]
fn standalone_page_statements_rejected(#[case] source: &str, #[case] expected: &str) {
	// `orderByAndPageStatement` is also a standalone
	// `primitiveQueryStatement` (GQL.g4:568), only supported post-RETURN.
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

/// Asserts an item is a plain MATCH clause and returns it.
#[track_caller]
fn as_match(item: &MatchItem) -> &crate::gql::ast::MatchClause {
	match item {
		MatchItem::Match(clause) => clause,
		MatchItem::Optional(_) => panic!("expected a plain MATCH item, got {item:?}"),
	}
}

/// Asserts an item is an OPTIONAL operand and returns its block.
#[track_caller]
fn as_optional(item: &MatchItem) -> &crate::gql::ast::OptionalBlock {
	match item {
		MatchItem::Optional(block) => block,
		MatchItem::Match(_) => panic!("expected an OPTIONAL item, got {item:?}"),
	}
}

#[test]
fn optional_plain_match() {
	// `optionalOperand : simpleMatchStatement` (GQL.g4:592): a plain
	// `OPTIONAL MATCH` is represented as a block of exactly one inner clause.
	let query = parse("OPTIONAL MATCH (a)-[:knows]->(b) WHERE b.x RETURN 1");
	assert_eq!(query.items.len(), 1);
	let block = as_optional(&query.items[0]);
	assert_eq!(block.items.len(), 1);
	let clause = as_match(&block.items[0]);
	assert_eq!(clause.patterns.len(), 1);
	assert!(clause.where_clause.is_some());
}

#[test]
fn optional_brace_block_multi_clause() {
	// `OPTIONAL LEFT_BRACE matchStatementBlock RIGHT_BRACE` (GQL.g4:593) with
	// several inner MATCH statements forming one all-or-nothing unit.
	let query = parse("MATCH (a) OPTIONAL { MATCH (a)-[:r]->(b) MATCH (b)-[:s]->(c) } RETURN 1");
	assert_eq!(query.items.len(), 2);
	as_match(&query.items[0]);
	let block = as_optional(&query.items[1]);
	assert_eq!(block.items.len(), 2);
	for inner in &block.items {
		assert_eq!(as_match(inner).patterns.len(), 1);
	}
}

#[test]
fn optional_paren_block() {
	// `OPTIONAL LEFT_PAREN matchStatementBlock RIGHT_PAREN` (GQL.g4:594).
	let query = parse("MATCH (a) OPTIONAL ( MATCH (a)-[:r]->(b) ) RETURN 1");
	assert_eq!(query.items.len(), 2);
	let block = as_optional(&query.items[1]);
	assert_eq!(block.items.len(), 1);
	as_match(&block.items[0]);
}

#[rstest]
#[case::brace_in_brace("MATCH (a) OPTIONAL { OPTIONAL { MATCH (a)-[:r]->(b) } } RETURN 1")]
#[case::paren_in_brace("MATCH (a) OPTIONAL { OPTIONAL ( MATCH (a)-[:r]->(b) ) } RETURN 1")]
#[case::brace_in_paren("MATCH (a) OPTIONAL ( OPTIONAL { MATCH (a)-[:r]->(b) } ) RETURN 1")]
fn optional_nested_blocks(#[case] source: &str) {
	// `matchStatementBlock : matchStatement+` (GQL.g4:597), and a
	// `matchStatement` may itself be an `optionalMatchStatement`, so OPTIONAL
	// blocks nest.
	let query = parse(source);
	let outer = as_optional(&query.items[1]);
	assert_eq!(outer.items.len(), 1);
	let inner = as_optional(&outer.items[0]);
	assert_eq!(inner.items.len(), 1);
	as_match(&inner.items[0]);
}

#[test]
fn optional_interleaved_with_plain_match() {
	// Plain and OPTIONAL items mix freely in the leading `matchStatement+`.
	let query = parse(
		"MATCH (a) OPTIONAL MATCH (a)-[:r]->(b) MATCH (b)-[:s]->(c) OPTIONAL { MATCH (c)-[:t]->(d) } \
		 RETURN 1",
	);
	assert_eq!(query.items.len(), 4);
	as_match(&query.items[0]);
	assert_eq!(as_optional(&query.items[1]).items.len(), 1);
	as_match(&query.items[2]);
	assert_eq!(as_optional(&query.items[3]).items.len(), 1);
}

#[test]
fn optional_block_with_inner_optional_and_plain() {
	// A block whose `matchStatement+` mixes a plain MATCH and a nested OPTIONAL.
	let query =
		parse("MATCH (a) OPTIONAL { MATCH (a)-[:r]->(b) OPTIONAL MATCH (b)-[:s]->(c) } RETURN 1");
	let block = as_optional(&query.items[1]);
	assert_eq!(block.items.len(), 2);
	as_match(&block.items[0]);
	assert_eq!(as_optional(&block.items[1]).items.len(), 1);
}

#[test]
fn deeply_nested_optional_blocks_hit_the_recursion_limit() {
	// Each OPTIONAL block delimiter charges the object-recursion budget, so a
	// pathological stack of nested OPTIONAL blocks trips the depth limit rather
	// than overflowing the machine stack. With a budget of 3 the fourth nested
	// block delimiter exceeds it.
	let settings = GqlParserSettings {
		object_recursion_limit: 3,
		..Default::default()
	};
	let source = "OPTIONAL { OPTIONAL { OPTIONAL { OPTIONAL { MATCH (a) } } } } RETURN 1";
	let error = parse_with_settings(source, settings).expect_err("should exceed the limit");
	let rendered = format!("{:?}", error.render_on(source));
	assert!(rendered.contains("Exceeded query expression nesting depth limit"), "{rendered}");
}

#[test]
fn pathological_nested_optional_blocks_fail_fast() {
	// 100k unclosed OPTIONAL braces: the depth limit trips after the default
	// budget; the parser must error without overflowing the machine stack.
	let source = format!("{}RETURN 1", "OPTIONAL { ".repeat(100_000));
	let error = parse_err(&source);
	assert!(error.contains("Exceeded query expression nesting depth limit"), "{error}");
}

#[test]
fn optional_requires_match_or_block() {
	// `optionalOperand` is a MATCH, a brace block or a paren block; anything
	// else after OPTIONAL is rejected.
	let error = parse_err("OPTIONAL RETURN 1");
	assert!(error.contains("expected `MATCH`, `{` or `(`"), "{error}");
}

#[rstest]
#[case::empty_brace("OPTIONAL { } RETURN 1")]
#[case::empty_paren("OPTIONAL ( ) RETURN 1")]
fn optional_empty_block_rejected(#[case] source: &str) {
	// `matchStatementBlock : matchStatement+` (GQL.g4:597) requires at least
	// one inner statement.
	let error = parse_err(source);
	assert!(error.contains("expected `MATCH` or `OPTIONAL`"), "{error}");
}

#[test]
fn optional_unclosed_brace_block_rejected() {
	// The inner block parses the MATCH clause, then the closing `}` is missing:
	// `RETURN` ends the block loop and the close-delimiter check rejects it,
	// pointing back at the opening `{`.
	let error = parse_err("OPTIONAL { MATCH (a) RETURN 1");
	assert!(error.contains("expected the delimiter `}`"), "{error}");
}

#[rstest]
#[case::repeatable_elements("MATCH REPEATABLE ELEMENTS (a) RETURN 1", "REPEATABLE ELEMENTS")]
#[case::repeatable_element("MATCH REPEATABLE ELEMENT (a) RETURN 1", "REPEATABLE ELEMENTS")]
#[case::different_edges("MATCH DIFFERENT EDGES (a) RETURN 1", "DIFFERENT EDGES")]
#[case::different_edge("MATCH DIFFERENT EDGE (a) RETURN 1", "DIFFERENT EDGES")]
#[case::different_relationships("MATCH DIFFERENT RELATIONSHIPS (a) RETURN 1", "DIFFERENT EDGES")]
fn match_modes_rejected(#[case] source: &str, #[case] expected: &str) {
	// `matchMode` (GQL.g4:807-828).
	let error = parse_err(source);
	assert!(error.contains("Match modes"), "{error}");
	assert!(error.contains(expected), "{error}");
}

#[rstest]
#[case::repeatable("MATCH repeatable = (a) RETURN 1", "repeatable")]
#[case::different("MATCH different = (a) RETURN 1", "different")]
fn match_mode_words_are_valid_path_variables(#[case] source: &str, #[case] expected: &str) {
	// `REPEATABLE` and `DIFFERENT` are non-reserved words; without their
	// element/edge synonym they are ordinary identifiers.
	let query = parse(source);
	assert_eq!(
		match_clauses(&query)[0].patterns[0].path_var.as_ref().map(|x| x.name.as_str()),
		Some(expected)
	);
}

#[test]
fn keep_clause_rejected() {
	// `keepClause` (GQL.g4:844).
	let error = parse_err("MATCH (a) KEEP TRAIL RETURN 1");
	assert!(error.contains("KEEP clauses are not supported yet"), "{error}");
}

#[test]
fn yield_clause_rejected() {
	// `graphPatternYieldClause` (GQL.g4:597).
	let error = parse_err("MATCH (a) YIELD x RETURN x");
	assert!(error.contains("YIELD clauses are not supported yet"), "{error}");
}
