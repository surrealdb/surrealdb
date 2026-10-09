use surrealdb_sql as sql;
use surrealdb_strand::Strand;

use super::Parser;

mod stack;
mod stmt;
mod streaming;
mod value;

#[test]
fn parse_large_test_file() {
	crate::parse_with(include_bytes!("../../../test.surql"), |parser| parser.parse_query())
		.unwrap();
}

#[test]
fn less_then_idiom() {
	let src = r#"
		if ($param.foo < 2){
			return 1
		}
	"#;
	crate::parse_with(src.as_bytes(), |parser| parser.parse_query()).unwrap();
}

#[test]
fn ident_is_field() {
	let mut field = crate::parse_with(r#"foo"#.as_bytes(), |parser| parser.parse_query()).unwrap();

	let exprs = field.expressions.pop().unwrap();

	assert_eq!(
		exprs,
		sql::TopLevelExpr::Expr(sql::Expr::Idiom(sql::Idiom(vec![sql::Part::Field(
			Strand::new_static("foo")
		)])))
	);
}

#[test]
fn parse_immediate_insert_subquery() {
	crate::parse_with(
		r#"LET $insert = INSERT INTO t (SELECT true FROM 1);"#.as_bytes(),
		|parser| parser.parse_query(),
	)
	.unwrap();
}

#[test]
fn test_non_valid_utf8() {
	let mut src = "SELECT * FROM foo;".as_bytes().to_vec();
	src.push(0xff);

	let mut parser = Parser::new(&src);
	parser.parse_query().unwrap_err();
}
