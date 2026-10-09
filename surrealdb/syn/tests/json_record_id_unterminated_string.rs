//! `parse_json` on a record id whose key holds an unterminated string must
//! return an error, not panic: the input reaches it from unauthenticated HTTP
//! bodies. Issue #1529.

fn parse_json(input: &str) -> bool {
	surrealdb_syn::parse_with_settings(input.as_bytes(), Default::default(), |p| p.parse_json())
		.is_ok()
}

#[test]
#[ignore = "panics until #1529 is fixed"]
fn unterminated_string_in_record_id_key_is_an_error() {
	for input in ["a:{'b", "a: { '0hAS} }]"] {
		let res = std::panic::catch_unwind(|| parse_json(input));
		assert_eq!(res.ok(), Some(false), "{input:?}");
	}
}
