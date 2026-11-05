//! What a plain string becomes under `legacy_strands`, the profile the JSON
//! RPC transports read their values with.

use surrealdb_expr::parser::{ParserConfig, value_legacy_strand};
use surrealdb_types::Value;

fn read(text: &str) -> Value {
	value_legacy_strand(&format!("\"{text}\""), &ParserConfig::default())
		.expect("a quoted string is a value")
}

/// A string is a uuid or a datetime only when the whole of it reads as one.
/// A string that merely begins with one keeps the rest of its text.
#[test]
fn a_string_that_begins_with_a_uuid_or_datetime_stays_a_string() {
	const UUID: &str = "3bb7beb4-128e-488f-aeab-974dd3a2df39";
	for text in [
		format!("{UUID}__8b0e61e5-02fb-48f7-9e07-89f3cb7c4966__18"),
		format!("{UUID}x"),
		format!("{UUID} "),
		"2024-01-01T00:00:00Zjunk".to_owned(),
		"2024-01-01T00:00:00+01:00:00".to_owned(),
		"2024-01-01 and more".to_owned(),
	] {
		assert_eq!(read(&text), Value::String(text.clone()), "{text}");
	}
}

#[test]
fn a_string_that_is_wholly_a_uuid_or_datetime_is_read_as_one() {
	assert!(matches!(read("3bb7beb4-128e-488f-aeab-974dd3a2df39"), Value::Uuid(_)));
	assert!(matches!(read("2024-01-01T00:00:00Z"), Value::Datetime(_)));
	assert!(matches!(read("2024-01-01T00:00:00.123456789+01:00"), Value::Datetime(_)));
	assert!(matches!(read("2024-01-01"), Value::Datetime(_)));
}
