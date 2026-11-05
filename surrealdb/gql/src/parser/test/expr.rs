//! Value expression tests: the full precedence table of
//! `doc/gql/REFERENCE.md` section (e) pinned via AST-shape assertions,
//! the `IS [NOT] NULL` primary restriction, literals (strings with all escape
//! forms, every numeric notation, booleans, `UNKNOWN`), parameters,
//! identifier/keyword classification and the targeted rejections of
//! operators GQL does not have.

use rstest::rstest;

use super::{parse_err, parse_expr_str, parse_return_expr, parse_return_items};
use crate::gql::ast::{GqlExpr, GqlLiteral};

#[track_caller]
fn assert_string_literal(expr: &GqlExpr, expected: &str) {
	let GqlExpr::Literal(GqlLiteral::String(x), _) = expr else {
		panic!("expected the string literal {expected:?}, got {expr:?}");
	};
	assert_eq!(x, expected);
}

#[rstest]
// `OR`/`XOR` (level 1, `#disjunctiveExprAlt`) bind loosest; `AND` (level 2,
// `#conjunctiveExprAlt`) binds tighter.
#[case::or_and("a OR b AND c", "(a OR (b AND c))")]
#[case::and_or("a AND b OR c", "((a AND b) OR c)")]
// `XOR` shares level 1 with `OR`, left associative.
#[case::or_xor("a OR b XOR c", "((a OR b) XOR c)")]
#[case::xor_or("a XOR b OR c", "((a XOR b) OR c)")]
// `NOT` (level 4, `#notExprAlt`) binds looser than comparisons (level 6,
// `#comparisonExprAlt`): `NOT a = b` negates the whole comparison.
#[case::not_comparison("NOT a = b", "(NOT (a = b))")]
#[case::not_not("NOT NOT a", "(NOT (NOT a))")]
// `AND` (level 2) binds looser than `NOT` (level 4).
#[case::not_and("NOT a AND b", "((NOT a) AND b)")]
// `IS [NOT] TRUE|FALSE|UNKNOWN` (level 3, `#isNotExprAlt`) binds looser than
// `NOT` (level 4) but tighter than `AND` (level 2).
#[case::not_is_true("NOT a IS TRUE", "((NOT a) IS TRUE)")]
#[case::is_true_and("a IS TRUE AND b", "((a IS TRUE) AND b)")]
#[case::chained_is("a IS TRUE IS NOT FALSE", "((a IS TRUE) IS NOT FALSE)")]
// `||` (level 7, `#concatenationExprAlt`) binds tighter than comparisons.
#[case::eq_concat("a = b || c", "(a = (b || c))")]
// `+`/`-` (level 8) bind tighter than `||` (level 7).
#[case::concat_add("a || b + c", "(a || (b + c))")]
#[case::add_concat("a + b || c", "((a + b) || c)")]
// `*`/`/` (level 9) bind tighter than `+`/`-` (level 8).
#[case::add_mul("1 + 2 * 3", "(1 + (2 * 3))")]
#[case::mul_add("1 * 2 + 3", "((1 * 2) + 3)")]
// Left associativity within a level.
#[case::sub_assoc("a - b - c", "((a - b) - c)")]
#[case::div_mul_assoc("a / b * c", "((a / b) * c)")]
#[case::concat_assoc("a || b || c", "((a || b) || c)")]
// Unary `+`/`-` (level 10, `#signedExprAlt`) bind tighter than `*`.
#[case::neg_mul("-a * b", "((-a) * b)")]
#[case::sub_neg("a - -b", "(a - (-b))")]
#[case::neg_property("-a.x", "(-a.x)")]
#[case::unary_plus("+5", "(+5)")]
// Parentheses override precedence; the AST drops the paren node.
#[case::parenthesized("(a OR b) AND c", "((a OR b) AND c)")]
// Property access binds tightest (level 11, `valueExpressionPrimary`).
#[case::property_chain("a.b.c.d", "a.b.c.d")]
// `IS [NOT] NULL` is a primary postfix (19.5): `NOT` applies on top of it.
#[case::not_is_null("NOT a IS NULL", "(NOT (a IS NULL))")]
#[case::is_not_null_and("x IS NOT NULL AND y > 1", "((x IS NOT NULL) AND (y > 1))")]
fn expression_precedence(#[case] source: &str, #[case] expected: &str) {
	assert_eq!(parse_expr_str(source), expected, "for {source:?}");
}

#[rstest]
#[case::eq("a = b", "(a = b)")]
#[case::neq("a <> b", "(a <> b)")]
#[case::lt("a < b", "(a < b)")]
#[case::lte("a <= b", "(a <= b)")]
#[case::gt("a > b", "(a > b)")]
#[case::gte("a >= b", "(a >= b)")]
fn comparison_operators(#[case] source: &str, #[case] expected: &str) {
	// `compOp` (GQL.g4:2025): `=`, `<>`, `<`, `>`, `<=`, `>=`.
	assert_eq!(parse_expr_str(source), expected);
}

#[rstest]
#[case::eq_eq("RETURN a = b = c")]
#[case::lt_gt("RETURN a < b > c")]
#[case::lte_neq("RETURN 1 <= 2 <> 3")]
fn chained_comparisons_rejected(#[case] source: &str) {
	let error = parse_err(source);
	assert!(error.contains("Comparison operators cannot be chained"), "{error}");
	assert!(error.contains("use AND"), "{error}");
}

#[rstest]
#[case::bare("RETURN a != b")]
#[case::after_comparison("RETURN a = b != c")]
#[case::in_where("MATCH (a) WHERE a.x != 1 RETURN a")]
fn not_equals_rejected(#[case] source: &str) {
	// Deviation (f)2: GQL only has `<>`.
	let error = parse_err(source);
	assert!(error.contains("GQL uses `<>` for inequality"), "{error}");
}

#[rstest]
#[case::membership("RETURN x IN [1, 2]", "GQL has no `IN` membership operator")]
#[case::like("RETURN a LIKE 'x%'", "GQL has no `LIKE` operator")]
#[case::starts_with("RETURN a STARTS WITH 'x'", "GQL has no `STARTS WITH` operator")]
#[case::ends_with("RETURN a ENDS WITH 'x'", "GQL has no `ENDS WITH` operator")]
#[case::contains("RETURN a CONTAINS 'x'", "GQL has no `CONTAINS` operator")]
fn foreign_operators_rejected(#[case] source: &str, #[case] expected: &str) {
	// Deviations (f)2 and (f)3: these operators do not exist in GQL.
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[rstest]
#[case::after_add("RETURN a.x + 1 IS NULL")]
#[case::after_mul("RETURN a * b IS NULL")]
#[case::after_concat("RETURN a || b IS NULL")]
#[case::after_unary("RETURN - b IS NULL")]
#[case::after_comparison("RETURN a = b IS NULL")]
#[case::after_is_bool("RETURN a IS TRUE IS NULL")]
fn null_test_requires_a_primary_operand(#[case] source: &str) {
	// `nullPredicate : valueExpressionPrimary IS NOT? NULL` (19.5,
	// GQL.g4:2042): the operand must be a primary.
	let error = parse_err(source);
	assert!(error.contains("`IS NULL` may only directly follow a simple expression"), "{error}");
	assert!(error.contains("parentheses"), "{error}");
}

#[rstest]
#[case::simple("a IS NULL", "(a IS NULL)")]
#[case::negated("a.x IS NOT NULL", "(a.x IS NOT NULL)")]
#[case::parenthesized("(a.x + 1) IS NULL", "((a.x + 1) IS NULL)")]
#[case::function_call("f(x) IS NULL", "(f(x) IS NULL)")]
#[case::parameter("$p IS NOT NULL", "($p IS NOT NULL)")]
// A parenthesized null test is an ordinary operand; lowering type-checks it.
#[case::parenthesized_operand("a + (b IS NULL)", "(a + (b IS NULL))")]
// A predicate is a primary alternative of `valueExpression` (the head of the
// left recursion), so it may appear as a *left* operand per the grammar.
#[case::left_operand("a IS NULL = b", "((a IS NULL) = b)")]
fn null_tests(#[case] source: &str, #[case] expected: &str) {
	assert_eq!(parse_expr_str(source), expected, "for {source:?}");
}

#[rstest]
#[case::is_true("a IS TRUE", "(a IS TRUE)")]
#[case::is_not_true("a IS NOT TRUE", "(a IS NOT TRUE)")]
#[case::is_false("a IS FALSE", "(a IS FALSE)")]
#[case::is_not_false("a IS NOT FALSE", "(a IS NOT FALSE)")]
#[case::is_unknown("a IS UNKNOWN", "(a IS UNKNOWN)")]
#[case::is_not_unknown("a IS NOT UNKNOWN", "(a IS NOT UNKNOWN)")]
fn boolean_tests(#[case] source: &str, #[case] expected: &str) {
	// `IS [NOT] truthValue` (`#isNotExprAlt`, `truthValue` GQL.g4:2536).
	assert_eq!(parse_expr_str(source), expected);
}

#[rstest]
#[case::typed("RETURN a IS TYPED INT", "`IS [NOT] TYPED`")]
#[case::not_typed("RETURN a IS NOT TYPED INT", "`IS [NOT] TYPED`")]
#[case::normalized("RETURN a IS NORMALIZED", "`IS [NOT] NORMALIZED`")]
#[case::nfc_normalized("RETURN a IS NFC NORMALIZED", "`IS [NOT] NORMALIZED`")]
#[case::labeled("RETURN a IS LABELED l", "`IS [NOT] LABELED`")]
#[case::directed("RETURN a IS DIRECTED", "`IS [NOT] DIRECTED`")]
#[case::source("RETURN a IS SOURCE OF k", "`IS [NOT] SOURCE/DESTINATION OF`")]
#[case::destination("RETURN a IS DESTINATION OF k", "`IS [NOT] SOURCE/DESTINATION OF`")]
fn other_is_predicates_rejected(#[case] source: &str, #[case] expected: &str) {
	// The remaining `predicate` alternatives (19.2) parse-and-reject.
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[test]
fn cast_rejected() {
	let error = parse_err("RETURN CAST(1 AS FLOAT)");
	assert!(error.contains("`CAST` expressions are not supported yet"), "{error}");
}

#[rstest]
// Typed temporal literals (`temporalLiteral`) are recognised as literals and
// rejected, not misdiagnosed as a reserved word used as a variable.
#[case::date("RETURN DATE '2024-01-01'", "Typed temporal literals (`DATE '…'`)")]
#[case::time("RETURN TIME '12:00:00'", "Typed temporal literals (`TIME '…'`)")]
#[case::datetime(
	"RETURN DATETIME '2024-01-01T12:00:00'",
	"Typed temporal literals (`DATETIME '…'`)"
)]
#[case::timestamp(
	"RETURN TIMESTAMP '2024-01-01 12:00:00'",
	"Typed temporal literals (`TIMESTAMP '…'`)"
)]
#[case::duration("RETURN DURATION 'PT1H'", "Typed temporal literals (`DURATION '…'`)")]
// `SESSION_USER` is a grammatical primary (`generalValueSpecification`).
#[case::session_user("RETURN SESSION_USER", "`SESSION_USER` value specification is not supported")]
fn unsupported_primaries_rejected(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
	// The misleading delimited-identifier hint must not appear: the user did
	// not write a variable.
	assert!(!error.contains("delimited identifier"), "{error}");
}

#[test]
fn temporal_keyword_without_string_is_a_reserved_word() {
	// Without a following string there is no temporal literal; the reserved
	// word diagnosis applies.
	let error = parse_err("RETURN DATE");
	assert!(error.contains("`DATE` is a reserved word"), "{error}");
}

#[rstest]
#[case::simple("'simple'", "simple")]
#[case::quote_doubling("'it''s'", "it's")]
#[case::double_quoted("\"say \\\"hi\\\"\"", "say \"hi\"")]
#[case::double_quote_doubling("\"a\"\"b\"", "a\"b")]
#[case::tab_escape(r"'a\tb'", "a\tb")]
#[case::newline_escape(r"'line\nbreak'", "line\nbreak")]
#[case::backslash_escape(r"'back\\slash'", "back\\slash")]
#[case::unicode_4(r"'\u0041'", "A")]
#[case::unicode_6(r"'\U01F600'", "\u{1F600}")]
#[case::no_escape_mode(r"@'C:\dir\new'", r"C:\dir\new")]
#[case::no_escape_doubling("@'no''escape'", "no'escape")]
#[case::empty("''", "")]
fn string_literals(#[case] source: &str, #[case] expected: &str) {
	// `characterStringLiteral` (GQL.g4:2972) with `ESCAPED_CHARACTER`
	// (GQL.g4:3157-3185) and the `@` `NO_ESCAPE` prefix (GQL.g4:3129).
	assert_string_literal(&parse_return_expr(source), expected);
}

#[rstest]
// The escape letters are case-sensitive: `\N` is not a newline escape.
#[case::uppercase_letter(r"RETURN '\N'", "Invalid escape sequence")]
#[case::unknown_letter(r"RETURN '\q'", "Invalid escape sequence")]
#[case::short_unicode(r"RETURN '\u12'", "Invalid unicode escape sequence")]
#[case::surrogate_unicode(r"RETURN '\uD800'", "not a valid unicode code point")]
#[case::raw_newline("RETURN 'a\nb'", "may not contain raw newline characters")]
#[case::unterminated("RETURN 'abc", "expected the quoted sequence to end")]
fn string_literal_errors(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[rstest]
#[case::zero("0", 0)]
#[case::decimal("123", 123)]
#[case::separated("1_000_000", 1_000_000)]
#[case::hex("0x10", 16)]
#[case::hex_separated("0xdead_beef", 0xdead_beef)]
#[case::hex_leading_separator("0x_1", 1)]
#[case::octal("0o777", 511)]
#[case::binary("0b1010", 10)]
#[case::binary_separated("0b1_0", 2)]
#[case::max("9223372036854775807", i64::MAX)]
#[case::exact_suffix("42M", 42)]
fn integer_literals(#[case] source: &str, #[case] expected: i64) {
	// `unsignedNumericLiteral` (GQL.g4:2977-3002): decimal with `_` digit
	// separators, lowercase-only `0x`/`0o`/`0b` prefixes, `M` exact suffix.
	let expr = parse_return_expr(source);
	let GqlExpr::Literal(GqlLiteral::Integer(x), _) = expr else {
		panic!("expected the integer literal {expected}, got {expr:?}");
	};
	assert_eq!(x, expected);
}

#[rstest]
#[case::trailing_period("123.", 123.0)]
#[case::common("0.5", 0.5)]
#[case::leading_period(".456", 0.456)]
#[case::scientific("1.5e10", 1.5e10)]
#[case::scientific_upper("2E-3", 2e-3)]
#[case::scientific_integer_mantissa("1e3", 1e3)]
#[case::scientific_plus("12e+2", 12e2)]
#[case::separated_float("1_0.2_5", 10.25)]
// The `M` exact suffix on a fractional literal is approximated as f64: the
// AST has no exact decimal representation.
#[case::exact_suffix("1.5M", 1.5)]
#[case::float_suffix("2.0F", 2.0)]
#[case::double_suffix_integer("3D", 3.0)]
#[case::double_suffix("2.5d", 2.5)]
fn float_literals(#[case] source: &str, #[case] expected: f64) {
	let expr = parse_return_expr(source);
	let GqlExpr::Literal(GqlLiteral::Float(x), _) = expr else {
		panic!("expected the float literal {expected}, got {expr:?}");
	};
	assert_eq!(x, expected);
}

#[rstest]
#[case::overflow("RETURN 9223372036854775808", "Integer literal is too large")]
#[case::hex_overflow("RETURN 0xffffffffffffffff", "Integer literal is too large")]
#[case::double_separator("RETURN 1__2", "underscore digit separators")]
#[case::trailing_separator("RETURN 1_", "underscore digit separators")]
#[case::dangling_prefix_separator("RETURN 0x_", "underscore digit separators")]
// `0X` is not a hex introducer (the prefix is lowercase only): `0X10` lexes
// as the number `0` followed by the identifier `X10`.
#[case::uppercase_hex_prefix("RETURN 0X10", "expected the query to end")]
fn numeric_literal_errors(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[rstest]
#[case::true_literal("TRUE", "true")]
#[case::false_lower("false", "false")]
#[case::null_literal("NULL", "null")]
// `UNKNOWN` is the null value of the boolean type (ISO three-valued logic).
#[case::unknown_literal("UNKNOWN", "null")]
fn boolean_and_null_literals(#[case] source: &str, #[case] expected: &str) {
	assert_eq!(parse_expr_str(source), expected);
}

#[test]
fn non_reserved_words_are_variables() {
	// `regularIdentifier : REGULAR_IDENTIFIER | nonReservedWords`.
	let items = parse_return_items("RETURN node, type, first, last, source");
	let names: Vec<_> = items.iter().map(|x| x.text.as_str()).collect();
	assert_eq!(names, vec!["node", "type", "first", "last", "source"]);
	for item in &items {
		assert!(matches!(item.expr, GqlExpr::Variable(_)), "{:?}", item.expr);
	}
}

#[rstest]
#[case::count("RETURN count", "`count` is a reserved word")]
#[case::value("RETURN value", "`value` is a reserved word")]
#[case::limit("RETURN limit", "`limit` is a reserved word")]
#[case::match_kw("RETURN match", "`match` is a reserved word")]
// The error preserves the original casing.
#[case::cased("RETURN Count", "`Count` is a reserved word")]
// Prereserved words are not identifiers either.
#[case::prereserved("RETURN current_user", "`current_user` is a reserved word")]
#[case::prereserved_abstract("RETURN abstract", "`abstract` is a reserved word")]
fn reserved_words_are_not_variables(#[case] source: &str, #[case] expected: &str) {
	// Deviation (f)14: the reserved list is huge; common Cypher variable
	// names are reserved in GQL.
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
	assert!(error.contains("delimited identifier"), "{error}");
}

#[rstest]
#[case::keyword_lower("count(a)", "count(a)")]
#[case::keyword_upper("COUNT(a)", "COUNT(a)")]
#[case::keyword_abs("ABS(-1)", "ABS((-1))")]
#[case::multiple_args("coalesce(a, b, 1)", "coalesce(a, b, 1)")]
#[case::no_args("foo()", "foo()")]
#[case::nested_arg("abs(a - b)", "abs((a - b))")]
#[case::nested_call("upper(lower(a))", "upper(lower(a))")]
#[case::non_reserved_name("first(a)", "first(a)")]
// Aggregate argument forms (20.9): `count(*)` (GQL.g4:2381) and a leading
// `DISTINCT`/`ALL` set quantifier (GQL.g4:2387) parse into the AST so that
// lowering can reject aggregates with a targeted error.
#[case::count_star("count(*)", "count(*)")]
#[case::count_star_upper("COUNT(*)", "COUNT(*)")]
#[case::count_distinct("count(DISTINCT a)", "count(DISTINCT a)")]
#[case::sum_all("sum(ALL a.x)", "sum(ALL a.x)")]
fn function_calls(#[case] source: &str, #[case] expected: &str) {
	// Any keyword directly followed by `(` is accepted as a function name
	// (with its original casing); lowering validates the name.
	assert_eq!(parse_expr_str(source), expected);
}

#[rstest]
// `*` is only an argument as the sole `count(*)` form; it is not an
// expression.
#[case::star_with_args("RETURN count(*, a)", "Unexpected token `*`, expected an expression")]
#[case::star_in_expression("RETURN count(* + 1)", "Unexpected token `*`, expected an expression")]
// A set quantifier must be followed by an expression.
#[case::dangling_distinct("RETURN count(DISTINCT)", "expected an expression")]
fn aggregate_argument_errors(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[test]
fn not_is_an_operator_even_before_parens() {
	// `NOT(x)` is the NOT operator applied to `(x)`, not a function call.
	assert_eq!(parse_expr_str("NOT(x)"), "(NOT x)");
}

#[test]
fn literals_are_not_function_names() {
	// `TRUE` parses as a boolean literal; a call-like suffix is an error.
	let error = parse_err("RETURN TRUE(1)");
	assert!(error.contains("expected the query to end"), "{error}");
}

#[test]
fn property_names() {
	// Property names are identifiers: non-reserved words and delimited
	// identifiers are valid, reserved words are not.
	assert_eq!(parse_expr_str("a.node.type"), "a.node.type");
	assert_eq!(parse_expr_str("a.\"b c\".d"), "a.b c.d");
	let error = parse_err("RETURN a.count");
	assert!(error.contains("`count` is a reserved word"), "{error}");
	assert_eq!(parse_expr_str("a.\"count\""), "a.count");
}

#[rstest]
#[case::list("[1, 2, 3]", "[1, 2, 3]")]
#[case::empty_list("[]", "[]")]
#[case::nested_list("[[1], [2]]", "[[1], [2]]")]
#[case::list_of_expressions("[a + 1, 'x']", "[(a + 1), 'x']")]
#[case::map("{a: 1, \"b c\": 2}", "{a: 1, b c: 2}")]
#[case::empty_map("{}", "{}")]
#[case::non_reserved_key("{node: 1}", "{node: 1}")]
#[case::nested_map("{a: {b: [1]}}", "{a: {b: [1]}}")]
fn list_and_map_literals(#[case] source: &str, #[case] expected: &str) {
	assert_eq!(parse_expr_str(source), expected);
}

#[rstest]
#[case::trailing_comma("RETURN [1,]", "expected an expression")]
#[case::missing_colon("RETURN {a}", "expected `:`")]
#[case::reserved_key("RETURN {count: 1}", "`count` is a reserved word")]
fn list_and_map_errors(#[case] source: &str, #[case] expected: &str) {
	let error = parse_err(source);
	assert!(error.contains(expected), "{error}");
}

#[rstest]
#[case::simple("$x", "x")]
#[case::digit_leading("$1", "1")]
#[case::underscore("$_y", "_y")]
#[case::delimited("$\"weird name\"", "weird name")]
#[case::accent_delimited("$`q`", "q")]
fn parameters(#[case] source: &str, #[case] expected: &str) {
	// `PARAMETER_NAME : SEPARATED_IDENTIFIER` (GQL.g4:3055): the name may
	// start with a digit or be a delimited identifier.
	let expr = parse_return_expr(source);
	let GqlExpr::Param {
		name,
		..
	} = &expr
	else {
		panic!("expected a parameter, got {expr:?}");
	};
	assert_eq!(name, expected);
}

#[test]
fn parameter_errors() {
	// `$$name` substituted parameters are recognised and rejected.
	let error = parse_err("RETURN $$x");
	assert!(error.contains("Substituted parameters (`$$name`)"), "{error}");
	// A `$` without a name is a lexical error.
	let error = parse_err("RETURN $ x");
	assert!(error.contains("expected a parameter name"), "{error}");
}

#[test]
fn double_quoted_disambiguation() {
	// Deviation (f)8: `"…"` is a string literal in expression position and a
	// delimited identifier in identifier positions, decided by the parser.
	let mut items = parse_return_items("RETURN \"lit\" AS \"col\"");
	let item = items.pop().expect("one item");
	assert_string_literal(&item.expr, "lit");
	assert_eq!(item.alias.as_ref().map(|x| x.name.as_str()), Some("col"));
}

#[test]
fn accent_quoted_is_always_an_identifier() {
	// In expression position an accent-quoted token is a variable.
	let expr = parse_return_expr("`my var`");
	let GqlExpr::Variable(ident) = &expr else {
		panic!("expected a variable, got {expr:?}");
	};
	assert_eq!(ident.name, "my var");
}

#[test]
fn double_minus_is_a_comment_in_expressions() {
	// Deviation (f)1: `--` introduces a line comment even in expression
	// position, so a doubled unary minus needs separating whitespace.
	let error = parse_err("RETURN --1");
	assert!(error.contains("Unexpected end of file, expected an expression"), "{error}");
	assert_eq!(parse_expr_str("- -1"), "(-(-1))");
}
