use std::collections::BTreeMap;

use crate::{
	sql::{Array, Id, Object, Strand, Thing, Value},
	syn::v2::parser::mac::test_parse,
};

#[test]
fn parse_recursive_record_string() {
	let res = test_parse!(parse_value, r#" r"a:[r"b:{c: r"d:1"}"]" "#).unwrap();
	assert_eq!(
		res,
		Value::Thing(Thing {
			tb: "a".to_owned(),
			id: Id::Array(Array(vec![Value::Thing(Thing {
				tb: "b".to_owned(),
				id: Id::Object(Object(BTreeMap::from([(
					"c".to_owned(),
					Value::Thing(Thing {
						tb: "d".to_owned(),
						id: Id::Number(1)
					})
				)])))
			})]))
		})
	)
}

#[test]
fn parse_record_string_2() {
	let res = test_parse!(parse_value, r#" r'a:["foo"]' "#).unwrap();
	assert_eq!(
		res,
		Value::Thing(Thing {
			tb: "a".to_owned(),
			id: Id::Array(Array(vec![Value::Strand(Strand("foo".to_owned()))]))
		})
	)
}
