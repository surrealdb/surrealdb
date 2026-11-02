//! Frozen bytes for the enum catalog entry and the enum kinds.
//!
//! A stored enum definition is written by [`StoredEnumDefinition`]'s stored encoding at the key of
//! the enum, and `Kind::Enum` joins `Kind` revision 2 as variant 38 over a revisioned `EnumRef`.
//! Both are permanent: the bytes below decode to the values they were captured from, and the
//! current encoder writes each value as exactly these bytes.
//!
//! Never edit the frozen bytes and never regenerate them to make a failing test pass.

use revision::{DeserializeRevisioned, SerializeRevisioned};
use surrealdb_strand::Strand;

use crate::catalog::{
	EnumDefinition, EnumId, EnumLabel, FromStored, Permission, StoredEnumDefinition,
};
use crate::expr::{EnumRef, Kind};
use crate::key::KVValue;

fn definition() -> EnumDefinition {
	EnumDefinition {
		id: EnumId(7),
		name: Strand::new("mood"),
		labels: vec![
			EnumLabel {
				label: "sad".to_owned(),
				order: 1 << 32,
				ident: 1,
			},
			EnumLabel {
				label: "ok".to_owned(),
				order: 3 << 31,
				ident: 3,
			},
			EnumLabel {
				label: "happy".to_owned(),
				order: 2 << 32,
				ident: 2,
			},
		],
		permissions: Permission::Full,
		comment: Some("how it feels".to_owned()),
	}
}

fn kinds() -> Vec<(&'static str, Kind)> {
	vec![
		("named", Kind::Enum(EnumRef::Named("mood".to_owned()))),
		(
			"inline",
			Kind::Enum(EnumRef::Inline(vec!["a".to_owned(), "it's".to_owned(), String::new()])),
		),
	]
}

#[test]
fn the_frozen_definition_decodes_and_is_written_as_it_was() {
	let stored: StoredEnumDefinition = KVValue::kv_decode_value(DEFINITION, ())
		.unwrap_or_else(|e| panic!("BACKWARDS COMPATIBILITY BROKEN: enum definition: {e}"));
	let decoded = EnumDefinition::from_stored(&stored).expect("compiles");
	assert_eq!(decoded, definition());
	assert_eq!(
		definition().to_stored().kv_encode_value().expect("encodes"),
		DEFINITION,
		"an enum definition is written differently"
	);
}

#[test]
fn the_frozen_kinds_decode_and_are_written_as_they_were() {
	for (name, kind) in kinds() {
		let bytes = frozen_kind(name);
		let decoded = Kind::deserialize_revisioned(&mut &bytes[..])
			.unwrap_or_else(|e| panic!("BACKWARDS COMPATIBILITY BROKEN: kind {name}: {e}"));
		assert_eq!(decoded, kind, "{name}");
		let mut written = Vec::new();
		kind.serialize_revisioned(&mut written).expect("encodes");
		assert_eq!(written, bytes, "kind {name} is written differently");
	}
}

fn frozen_kind(name: &str) -> &'static [u8] {
	KINDS
		.iter()
		.find(|(n, _)| *n == name)
		.map(|(_, bytes)| *bytes)
		.unwrap_or_else(|| panic!("no frozen bytes for kind {name}"))
}

/// Prints [`DEFINITION`] and [`KINDS`]. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::enum_definition::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixtures; run by hand when a case is added"]
fn generate() {
	println!("{:?}", definition().to_stored().kv_encode_value().expect("encodes"));
	for (name, kind) in kinds() {
		let mut written = Vec::new();
		kind.serialize_revisioned(&mut written).expect("encodes");
		println!("{name}: {written:?}");
	}
}

/// FROZEN. The stored encoding of [`definition`].
#[rustfmt::skip]
const DEFINITION: &[u8] = &[
	1, 1, 7, 4, 109, 111, 111, 100, 3, 1, 3, 115, 97, 100, 253, 0,
	0, 0, 0, 1, 0, 0, 0, 1, 1, 2, 111, 107, 253, 0, 0, 0,
	128, 1, 0, 0, 0, 3, 1, 5, 104, 97, 112, 112, 121, 253, 0, 0,
	0, 0, 2, 0, 0, 0, 2, 1, 1, 1, 12, 104, 111, 119, 32, 105,
	116, 32, 102, 101, 101, 108, 115,
];

/// FROZEN. The `revision` bytes of the enum kinds.
const KINDS: &[(&str, &[u8])] = &[
	("named", &[2, 38, 1, 0, 4, 109, 111, 111, 100]),
	("inline", &[2, 38, 1, 1, 3, 1, 97, 4, 105, 116, 39, 115, 0]),
];
