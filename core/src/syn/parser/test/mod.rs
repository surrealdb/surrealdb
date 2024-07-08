use crate::{sql, syn::parser::mac::test_parse};

mod limit;
mod stmt;
mod streaming;
mod value;

#[test]
fn multiple_semicolons() {
	let res = test_parse!(parse_query, r#";;"#).unwrap();
	let expected = sql::Query(sql::Statements(vec![]));
	assert_eq!(res, expected);
}
