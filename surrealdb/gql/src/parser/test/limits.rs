//! Recursion limit and pathological input tests: deeply nested expressions
//! must hit the configurable depth limit (never the machine stack), linear
//! constructs must be stack-safe via the reblessive stack, and adversarial
//! inputs must fail fast.
//!
//! Operator chains (binary spines, `NOT`/sign prefixes, `IS` postfixes) are
//! bounded by `expr_recursion_limit` (default 128, mirroring `syn`), because
//! they lower one-to-one onto `sql::Expr` spines whose downstream recursive
//! walks (drop, formatting, `sql → expr` conversion) must stay within the
//! machine stack. Property-access and label chains are deliberately *not*
//! charged: properties lower to flat `sql::Idiom` part vectors and labels
//! never reach `sql::Expr`, so their ASTs can be arbitrarily deep; the manual
//! iterative [`Drop`] of `GqlExpr` and `LabelExpr` must release them without
//! overflowing the machine stack either. The `*_drop_safe`/`*_is_iterative`
//! tests below use depths far beyond what recursive drop glue survives on a
//! 2MiB test thread stack (which overflows somewhere between 20k and 40k
//! levels).
//!
//! The 4GB input length guard in [`crate::gql::parse_with_settings`] is
//! not tested here: exercising it requires allocating a `u32::MAX`-byte
//! string.

use rstest::rstest;

use super::{parse, parse_err};
use crate::gql::{GqlParserSettings, parse_with_settings};

/// Each nesting construct counts one level against the default
/// `object_recursion_limit` of 100, plus one level for the innermost
/// primary: 99 levels of nesting parse, 100 do not.
#[rstest]
#[case::parens_within("(", "1", ")", 99, true)]
#[case::parens_beyond("(", "1", ")", 100, false)]
#[case::lists_within("[", "1", "]", 99, true)]
#[case::lists_beyond("[", "1", "]", 100, false)]
#[case::maps_within("{a: ", "1", "}", 99, true)]
#[case::maps_beyond("{a: ", "1", "}", 100, false)]
#[case::calls_within("f(", "1", ")", 99, true)]
#[case::calls_beyond("f(", "1", ")", 100, false)]
fn nesting_depth_limit(
	#[case] open: &str,
	#[case] inner: &str,
	#[case] close: &str,
	#[case] depth: usize,
	#[case] ok: bool,
) {
	let source = format!("RETURN {}{}{}", open.repeat(depth), inner, close.repeat(depth));
	if ok {
		parse(&source);
	} else {
		let error = parse_err(&source);
		assert!(error.contains("Exceeded query expression nesting depth limit"), "{error}");
	}
}

#[test]
fn label_expression_parens_count_against_the_limit() {
	let source = format!("MATCH (a:{}L{}) RETURN 1", "(".repeat(150), ")".repeat(150));
	let error = parse_err(&source);
	assert!(error.contains("Exceeded query expression nesting depth limit"), "{error}");
}

#[rstest]
#[case::at_limit("RETURN ((((1))))", true)]
#[case::beyond_limit("RETURN (((((1)))))", false)]
fn custom_recursion_limit(#[case] source: &str, #[case] ok: bool) {
	// With a limit of 5: four parens plus the innermost primary consume
	// exactly the budget; a fifth paren exceeds it.
	let settings = GqlParserSettings {
		object_recursion_limit: 5,
		..Default::default()
	};
	let result = parse_with_settings(source, settings);
	if ok {
		result.expect("should parse within the limit");
	} else {
		let error = result.expect_err("should exceed the limit");
		let rendered = format!("{:?}", error.render_on(source));
		assert!(rendered.contains("Exceeded query expression nesting depth limit"), "{rendered}");
	}
}

#[test]
fn pathological_open_parens_fail_fast() {
	// 100k unclosed parens: the depth limit trips after 100 of them; the
	// parser must error without scanning further or overflowing the stack.
	let source = format!("RETURN {}", "(".repeat(100_000));
	let error = parse_err(&source);
	assert!(error.contains("Exceeded query expression nesting depth limit"), "{error}");
}

#[test]
fn pathological_open_brackets_fail_fast() {
	let source = format!("RETURN {}", "[".repeat(100_000));
	let error = parse_err(&source);
	assert!(error.contains("Exceeded query expression nesting depth limit"), "{error}");
}

/// Flat operator spines and prefix chains are bounded by the expression
/// depth budget: parsing must fail fast with a clean error (never build the
/// tree, never touch the machine stack), exactly like `syn` after its
/// expression-depth limit. 200k-term inputs double as fail-fast checks.
#[rstest]
#[case::or_spine(format!("RETURN a{}", " OR a".repeat(200_000)))]
#[case::and_spine(format!("RETURN a{}", " AND a".repeat(200_000)))]
#[case::add_spine(format!("RETURN 1{}", " + 1".repeat(200_000)))]
#[case::mul_spine(format!("RETURN 1{}", " * 1".repeat(200_000)))]
#[case::concat_spine(format!("RETURN a{}", " || a".repeat(200_000)))]
#[case::is_chain(format!("RETURN a{}", " IS NOT FALSE".repeat(200_000)))]
#[case::not_chain(format!("RETURN {}1", "NOT ".repeat(200_000)))]
// The minus signs need separating whitespace: `--` introduces a line comment.
#[case::unary_minus_chain(format!("RETURN {}1", "- ".repeat(200_000)))]
fn deep_operator_chains_hit_the_expression_depth_limit(#[case] source: String) {
	let error = parse_err(&source);
	assert!(error.contains("Exceeded expression recursion depth limit"), "{error}");
}

/// The budget is charged per operator with the default of 128: one level for
/// the expression entry plus one per spine operator, so 126 operators fit
/// and the cap stays comfortably above any realistic query.
#[test]
fn operator_spines_within_the_default_budget_parse() {
	let source = format!("RETURN 1{}", " + 1".repeat(126));
	parse(&source);
}

/// Sibling expressions must not be charged for one another: the budget is
/// restored when each expression entry returns, so many parallel spines each
/// near the limit all parse.
#[test]
fn sibling_expressions_do_not_accumulate_depth() {
	let element = format!("1{}", " + 1".repeat(120));
	let source = format!("RETURN [{}]", vec![element; 16].join(", "));
	parse(&source);
}

/// Every operator family charges the same budget; with a tiny custom limit
/// the boundary is exact (one entry level + N operators).
#[rstest]
#[case::add_at_limit("RETURN 1 + 1 + 1 + 1 + 1", true)]
#[case::add_beyond_limit("RETURN 1 + 1 + 1 + 1 + 1 + 1", false)]
#[case::or_beyond_limit("RETURN a OR a OR a OR a OR a OR a", false)]
#[case::not_beyond_limit("RETURN NOT NOT NOT NOT NOT 1", false)]
#[case::is_beyond_limit("RETURN a IS TRUE IS TRUE IS TRUE IS TRUE IS TRUE", false)]
fn custom_expression_depth_limit(#[case] source: &str, #[case] ok: bool) {
	let settings = GqlParserSettings {
		expr_recursion_limit: 5,
		..Default::default()
	};
	let result = parse_with_settings(source, settings);
	if ok {
		result.expect("should parse within the limit");
	} else {
		let error = result.expect_err("should exceed the limit");
		let rendered = format!("{:?}", error.render_on(source));
		assert!(rendered.contains("Exceeded expression recursion depth limit"), "{rendered}");
	}
}

#[test]
fn long_property_chain_is_parse_and_drop_safe() {
	let source = format!("RETURN a{}", ".b".repeat(200_000));
	parse(&source);
}

#[test]
fn deep_is_null_operand_chain_is_drop_safe() {
	// Property chains are not charged against the expression budget; the
	// trailing null test is consumed by the primary itself.
	let source = format!("RETURN a{} IS NOT NULL", ".b".repeat(200_000));
	parse(&source);
}

#[test]
fn deep_label_negation_chain_is_parse_and_drop_safe() {
	let source = format!("MATCH (a:{}L) RETURN 1", "!".repeat(200_000));
	parse(&source);
}

#[test]
fn long_label_operator_chain_is_parse_and_drop_safe() {
	// `&`/`|` label chains are parsed iteratively but the AST is left-deep.
	let source = format!("MATCH (a:L{}) RETURN 1", "&L|L".repeat(100_000));
	parse(&source);
}

#[test]
fn long_function_argument_and_list_trees_are_drop_safe() {
	// Wide-and-deep mixes: every list element is itself a deep chain.
	let element = format!("a{}", ".b".repeat(10_000));
	let source = format!("RETURN [{}]", vec![element; 20].join(", "));
	parse(&source);
	// Operator chains inside argument positions are charged like any other
	// expression entry.
	let source = format!("RETURN f({}1)", "NOT ".repeat(200_000));
	let error = parse_err(&source);
	assert!(error.contains("Exceeded expression recursion depth limit"), "{error}");
}

#[test]
fn long_comment_runs_are_iterative() {
	// Consecutive hidden tokens are skipped iteratively in the lexer.
	let source = format!("{}RETURN 1", "//x\n".repeat(100_000));
	parse(&source);
	let source = format!("{}RETURN 1", "--x\n".repeat(100_000));
	parse(&source);
}
