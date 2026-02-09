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
