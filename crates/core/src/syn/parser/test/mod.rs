use super::Parser;
use crate::{sql, syn};

mod json;
mod limit;
mod stmt;
mod streaming;
mod value;

#[test]
fn parse_large_test_file() {
	syn::parse_with(include_bytes!("../../../../test.surql"), async |parser, stk| {
		parser.parse_query(stk).await
	})
	.unwrap();
}

#[test]
fn less_then_idiom() {
	let src = r#"
		if ($param.foo < 2){
			return 1
		}
	"#;
	syn::parse_with(src.as_bytes(), async |parser, stk| parser.parse_query(stk).await).unwrap();
}

#[test]
fn ident_is_field() {
	let mut field =
		syn::parse_with(r#"foo"#.as_bytes(), async |parser, stk| parser.parse_query(stk).await)
			.unwrap();

	let exprs = field.expressions.pop().unwrap();

	assert_eq!(
		exprs,
		sql::TopLevelExpr::Expr(sql::Expr::Idiom(sql::Idiom(vec![sql::Part::Field(
			sql::Ident::new("foo".to_string()).unwrap()
		)])))
	);
}

#[test]
fn parse_immediate_insert_subquery() {
	syn::parse_with(
		r#"LET $insert = INSERT INTO t (SELECT true FROM 1);"#.as_bytes(),
		async |parser, stk| parser.parse_query(stk).await,
	)
	.unwrap();
}

#[test]
fn test_non_valid_utf8() {
	let mut src = "SELECT * FROM foo;".as_bytes().to_vec();
	src.push(0xff);

	let mut parser = Parser::new(&src);
	let mut stack = reblessive::Stack::new();
	stack.enter(|ctx| parser.parse_query(ctx)).finish().unwrap_err();
}
