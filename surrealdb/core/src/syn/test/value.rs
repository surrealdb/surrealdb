use surrealdb_types::Table as PublicTable;

use crate::syn;
use crate::types::{PublicRecordIdKey, PublicValue};

/// A record id read from a string is the whole string: the callers that read
/// one (casts, token claims, CBOR record ids) never act on a prefix of it.
#[test]
fn a_record_id_is_the_whole_input() {
	let rid = syn::record_id("value:⟨focus:outline-none⟩").unwrap();
	assert_eq!(rid.table.as_str(), "value");
	assert_eq!(rid.key, PublicRecordIdKey::String("focus:outline-none".to_owned()));
	assert!(syn::record_id("user:john ").is_ok());

	for text in ["value:focus:outline-none", "user:john extra", "user:1..3", "user:x;"] {
		assert!(syn::record_id(text).is_err(), "{text} must not read as a record id");
	}
}

/// An identifier that ends a value is a table, the form a `table` value renders
/// as; one followed by `:` begins a record id.
#[test]
fn a_bare_identifier_is_a_table() {
	assert_eq!(syn::value("hi").unwrap(), PublicValue::Table(PublicTable::new("hi")));
	assert_eq!(
		syn::value("[hi, ⟨a b⟩]").unwrap(),
		PublicValue::Array(
			vec![
				PublicValue::Table(PublicTable::new("hi")),
				PublicValue::Table(PublicTable::new("a b")),
			]
			.into()
		)
	);
	assert!(matches!(syn::value("hi:1").unwrap(), PublicValue::RecordId(_)));
	// Anything else after the identifier is still an error, at the top level
	// and within a record id's key alike.
	assert!(syn::value("hi there").is_err());
	assert!(syn::value("hi)").is_err());
	assert!(syn::value("a:[hi]").is_err());
}
