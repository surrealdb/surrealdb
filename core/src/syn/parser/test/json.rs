use crate::syn::parser::mac::test_parse;

#[test]
fn object_with_negative() {
	test_parse!(parse_json, r#"{"foo": -1 }"#).unwrap();
}

#[test]
fn object_with_trailing_whitespace() {
	test_parse!(parse_json, r#"{"foo": -1 }\n"#).unwrap();
}

#[test]
fn array_with_negative() {
	test_parse!(parse_json, r#"[-1]"#).unwrap();
}
