use reblessive::Stack;

use crate::syn::parser::{Parser, ParserSettings};

#[test]
fn object_depth() {
	let mut stack = Stack::new();

	let source = r#"
		RETURN {
			a: {
				b: {
					c: {
						d: {
						}
					}
				}
			}
		}
	"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			object_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect("recursion limit of 5 couldn't parse 5 deep object");

	let source = r#"
		RETURN {
			a: {
				b: {
					c: {
						d: {
							e: {
							}
						}
					}
				}
			}
		}
	"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			object_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect_err("recursion limit of 5 didn't trigger on 6 deep object");
}

#[test]
fn array_depth() {
	let mut stack = Stack::new();

	let source = r#"
		RETURN [ [ [ [ [ ] ] ] ] ]
	"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			object_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect("recursion limit of 5 couldn't parse 5 deep object");

	let source = r#"
		RETURN [ [ [ [ [ [ ] ] ] ] ] ]
	"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			object_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect_err("recursion limit of 5 didn't trigger on 6 deep object");
}

#[test]
fn object_depth_succeed_then_fail() {
	let mut stack = Stack::new();
	let source = r#"
		RETURN {
			a: {
				b: {
					c: {
						d: {
						}
					}
				}
			}
		};
	RETURN {
		a: {
			b: {
				c: {
					d: {
					}
				}
			}
		}
	};
	"#;

	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			object_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect("recursion limit of 5 couldn't parse 5 deep object");

	let mut stack = Stack::new();
	let source = r#"
		RETURN {
			a: {
				b: {
					c: {
						d: {
						}
					}
				}
			}
		};
	RETURN {
		a: {
			b: {
				c: {
					d: {
						e: {
						}
					}
				}
			}
		}
	};
	"#;

	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			object_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect_err("recursion limit of 5 didn't trigger on 6 deep object");
}

#[test]
fn query_depth_subquery() {
	let mut stack = Stack::new();

	let source = r#"
		RETURN select (select foo from bar ) from bar
		"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			query_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect("recursion limit of 5 couldn't parse 5 deep query");

	let source = r#"
		RETURN select (select (select (select foo from bar) from bar ) from bar) from bar
		"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			query_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect_err("recursion limit of 5 didn't trigger on 6 deep query");
}

#[test]
fn query_depth_block() {
	let mut stack = Stack::new();

	let source = r#"
	{
		{
			{
				{
					RETURN "foo";
				}
			}
		}
	}
	"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			query_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect("recursion limit of 5 couldn't parse 5 deep query");

	let source = r#"
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
	"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			query_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect_err("recursion limit of 5 didn't trigger on 6 deep query");
}

#[test]
fn query_depth_if() {
	let mut stack = Stack::new();

	let source = r#"
		IF IF IF IF IF true THEN false END { false } { false } { false } { false }
	"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			query_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect("recursion limit of 5 couldn't parse 5 deep query");

	let source = r#"
		IF IF IF IF IF IF true THEN false END { false } { false } { false } { false } { false }
	"#;
	let mut parser = Parser::new_with_settings(
		source.as_bytes(),
		ParserSettings {
			query_recursion_limit: 5,
			..Default::default()
		},
	);
	stack
		.enter(|stk| parser.parse_query(stk))
		.finish()
		.expect_err("recursion limit of 5 didn't trigger on 6 deep query");
}
