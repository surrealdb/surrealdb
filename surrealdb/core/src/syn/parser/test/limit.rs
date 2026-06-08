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
