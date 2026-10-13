use reblessive::Stack;
use rstest::rstest;

use crate::syn::parser::{Parser, ParserSettings};

#[rstest]
#[case::object_depth(
	ParserSettings { object_recursion_limit: 5, ..Default::default() },
	r#"
	RETURN {
		a: {
			b: {
				c: {
					d: {
						e: 1
					}
				}
			}
		}
	}
	"#,
	true
)]
#[case::object_depth_fail(
	ParserSettings { object_recursion_limit: 5, ..Default::default() },
	r#"
	RETURN {
		a: {
			b: {
				c: {
					d: {
						e: {
							f: 1
						}
					}
				}
			}
		}
	}
	"#,
	false
)]
#[case::array_depth(
	ParserSettings { object_recursion_limit: 5, ..Default::default() },
	"RETURN [ [ [ [ [ ] ] ] ] ]",
	true
)]
#[case::array_depth_fail(
	ParserSettings { object_recursion_limit: 5, ..Default::default() },
	"RETURN [ [ [ [ [ [ ] ] ] ] ] ]",
	false
)]
#[case::set_depth(
	ParserSettings { object_recursion_limit: 5, ..Default::default() },
	"RETURN { { { { { } } } } }",
	true
)]
#[case::query_depth_subquery(
	ParserSettings { query_recursion_limit: 5, ..Default::default() },
	"RETURN select (select foo from bar ) from bar",
	true
)]
#[case::query_depth_subquery_fail(
	ParserSettings { query_recursion_limit: 5, ..Default::default() },
	"RETURN select (select (select (select foo from bar) from bar ) from bar) from bar",
	false
)]
#[case::query_depth_block(
	ParserSettings { query_recursion_limit: 5, ..Default::default() },
	r#"
	{
		{
			{
				RETURN "foo";
			}
		}
	}
	"#,
	true
)]
#[case::query_depth_block_fail(
	ParserSettings { query_recursion_limit: 5, ..Default::default() },
	r#"
	{
		{
			{
				{
					{
						RETURN "foo";
					}
				}
			}
		}
	}
	"#,
	false
)]
#[case::query_depth_if(
	ParserSettings { query_recursion_limit: 5, ..Default::default() },
	"IF IF IF IF IF true THEN false END { false } { false } { false } { false }",
	true
)]
#[case::query_depth_if_fail(
	ParserSettings { query_recursion_limit: 5, ..Default::default() },
	"IF IF IF IF IF IF true THEN false END { false } { false } { false } { false } { false }",
	false
)]
fn test_parse_depth(
	#[case] parser_settings: ParserSettings,
	#[case] source: &str,
	#[case] expected: bool,
) {
	let mut stack = Stack::new();

	let mut parser = Parser::new_with_settings(source.as_bytes(), parser_settings);
	let result = stack.enter(|stk| parser.parse_query(stk)).finish();
	assert_eq!(result.is_ok(), expected);
}

#[rstest]
#[case::value_array_depth(5, "[[[[[null]]]]]", true)]
#[case::value_array_depth_fail(5, "[[[[[[null]]]]]]", false)]
#[case::value_object_depth(5, r#"{"a":{"b":{"c":{"d":{"e":null}}}}}"#, true)]
#[case::value_object_depth_fail(5, r#"{"a":{"b":{"c":{"d":{"e":{"f":null}}}}}}"#, false)]
#[case::value_paren_depth(5, "(((((null)))))", true)]
#[case::value_paren_depth_fail(5, "((((((null))))))", false)]
#[case::value_set_depth(5, "{ { { { { null, }, }, }, }, }", true)]
#[case::value_set_depth_fail(5, "{ { { { { { null, }, }, }, }, }, }", false)]
#[case::value_record_id_object_depth(5, r#"table:{"a":{"b":{"c":{"d":{"e":null}}}}}"#, true)]
#[case::value_record_id_object_depth_fail(
	5,
	r#"table:{"a":{"b":{"c":{"d":{"e":{"f":null}}}}}}"#,
	false
)]
#[case::value_record_id_array_depth(5, "table:[[[[[null]]]]]", true)]
#[case::value_record_id_array_depth_fail(5, "table:[[[[[[null]]]]]]", false)]
#[case::value_range_depth(5, "..=..=..=..=..=null", true)]
#[case::value_range_depth_fail(5, "..=..=..=..=..=..=null", false)]
fn test_parse_value_depth(#[case] limit: usize, #[case] source: &str, #[case] expected: bool) {
	use crate::syn;
	let settings = ParserSettings {
		object_recursion_limit: limit,
		..Default::default()
	};
	let result = syn::parse_with_settings(source.as_bytes(), settings, async |parser, stk| {
		parser.parse_value(stk).await
	});
	assert_eq!(result.is_ok(), expected);
}

#[rstest]
#[case::json_array_depth(5, "[[[[[null]]]]]", true)]
#[case::json_array_depth_fail(5, "[[[[[[null]]]]]]", false)]
#[case::json_object_depth(5, r#"{"a":{"b":{"c":{"d":{"e":null}}}}}"#, true)]
#[case::json_object_depth_fail(5, r#"{"a":{"b":{"c":{"d":{"e":{"f":null}}}}}}"#, false)]
fn test_parse_json_depth(#[case] limit: usize, #[case] source: &str, #[case] expected: bool) {
	use crate::syn;
	let settings = ParserSettings {
		object_recursion_limit: limit,
		..Default::default()
	};
	let result = syn::parse_with_settings(source.as_bytes(), settings, async |parser, stk| {
		parser.parse_json(stk).await
	});
	assert_eq!(result.is_ok(), expected);
}

/// Generate `RETURN <array<option<array<option<...int...>>>>>0;`
fn nested_cast_kind(depth: usize) -> String {
	let mut s = String::with_capacity(depth * 8 + 20);
	s.push_str("RETURN <");
	for i in 0..depth {
		if i % 2 == 0 {
			s.push_str("array<");
		} else {
			s.push_str("option<");
		}
	}
	s.push_str("int");
	for _ in 0..depth {
		s.push('>');
	}
	s.push_str(">0;");
	s
}

/// Generate `DEFINE FIELD x ON t TYPE array<option<array<...int...>>>;`
fn nested_define_field_kind(depth: usize) -> String {
	let mut s = String::with_capacity(depth * 8 + 40);
	s.push_str("DEFINE FIELD x ON t TYPE ");
	for i in 0..depth {
		if i % 2 == 0 {
			s.push_str("array<");
		} else {
			s.push_str("option<");
		}
	}
	s.push_str("int");
	for _ in 0..depth {
		s.push('>');
	}
	s.push(';');
	s
}

#[rstest]
#[case::cast_kind_depth(5, nested_cast_kind(4), true)]
#[case::cast_kind_depth_fail(5, nested_cast_kind(5), false)]
#[case::define_field_kind_depth(5, nested_define_field_kind(4), true)]
#[case::define_field_kind_depth_fail(5, nested_define_field_kind(5), false)]
fn test_parse_kind_depth(#[case] limit: usize, #[case] source: String, #[case] expected: bool) {
	let settings = ParserSettings {
		object_recursion_limit: limit,
		..Default::default()
	};
	let mut parser = Parser::new_with_settings(source.as_bytes(), settings);
	let mut stack = Stack::new();
	let result = stack.enter(|stk| parser.parse_query(stk)).finish();
	assert_eq!(result.is_ok(), expected);
}

/// `1` followed by `n` ` + 1` terms: a left-associative infix spine `n` levels
/// deep that consumes neither the query nor object recursion budget.
fn linear_infix(n: usize) -> String {
	let mut s = String::with_capacity(n * 4 + 1);
	s.push('1');
	for _ in 0..n {
		s.push_str(" + 1");
	}
	s
}

/// `n` `!` prefix operators applied to `true`.
fn prefix_chain(n: usize) -> String {
	let mut s = String::with_capacity(n + 4);
	for _ in 0..n {
		s.push('!');
	}
	s.push_str("true");
	s
}

/// Operator spines (infix) and prefix chains both deepen the `Expr` tree once
/// per operator while consuming neither the query nor the object recursion
/// budget, so they are bounded only by `expr_recursion_limit`.
#[rstest]
#[case::infix_ok(8, linear_infix(3), true)]
#[case::infix_fail(8, linear_infix(30), false)]
#[case::prefix_ok(8, prefix_chain(3), true)]
#[case::prefix_fail(8, prefix_chain(30), false)]
fn test_parse_expr_depth(#[case] limit: usize, #[case] source: String, #[case] expected: bool) {
	let settings = ParserSettings {
		expr_recursion_limit: limit,
		..Default::default()
	};
	let mut parser = Parser::new_with_settings(source.as_bytes(), settings);
	let mut stack = Stack::new();
	let result = stack.enter(|stk| parser.parse_query(stk)).finish();
	assert_eq!(result.is_ok(), expected);
}

/// A flat operator spine like `1 + 1 + 1 + ...` consumes neither the query nor
/// the object recursion budget (it is built iteratively by the pratt loop), so
/// before the `expr_recursion_limit` guard such a chain would build an
/// arbitrarily deep `Expr` tree from a small amount of query text. The
/// resulting tree is walked recursively when it is dropped, formatted, or
/// lowered, overflowing the call stack — a denial of service reachable from
/// query text alone. With the guard in place the parser must instead reject the
/// chain with a syntax error.
///
/// Runs on an explicit 2 MiB stack — the conservative default worker-thread
/// size — so the test is meaningful regardless of `RUST_MIN_STACK`.
#[test]
fn expr_depth_rejects_pathological_chain_without_overflow() {
	std::thread::Builder::new()
		.stack_size(2 * 1024 * 1024)
		.spawn(|| {
			// ~50k operators, ~200 KiB of text: well under any query size limit,
			// and (before the fix) more than enough to overflow the stack.
			let src = format!("RETURN 1{}", " + 1".repeat(50_000));
			let err = crate::syn::parse(&src)
				.expect_err("a 50k-term operator chain must be rejected, not parsed");
			assert!(
				err.to_string().contains("expression recursion depth limit"),
				"unexpected error: {err}"
			);
		})
		.unwrap()
		.join()
		.unwrap();
}

/// Confirms the default `expr_recursion_limit` is small enough that the deepest
/// `Expr` tree the parser will accept can still be lowered, formatted, and
/// dropped — all of which recurse once per node — without overflowing a 2 MiB
/// stack (the conservative default worker-thread size). If the default limit
/// were raised past the safe ceiling this test would abort the process,
/// catching the regression.
#[test]
fn expr_depth_default_limit_is_stack_safe() {
	std::thread::Builder::new()
		.stack_size(2 * 1024 * 1024)
		.spawn(|| {
			// ~110 operators, just under the default limit, so the accepted tree
			// is close to as deep as the parser allows and exercises every
			// recursive consumer near the worst-case depth.
			let expr = crate::syn::expr(&linear_infix(110)).expect("a near-limit chain parses");
			// `ToSql` recurses once per node.
			let formatted = surrealdb_types::ToSql::to_sql(&expr);
			assert!(formatted.contains('+'));
			// `sql::Expr -> expr::Expr` lowering recurses once per node.
			let lowered = crate::expr::Expr::from(expr);
			// Dropping the lowered tree recurses once per node.
			drop(lowered);
		})
		.unwrap()
		.join()
		.unwrap();
}
