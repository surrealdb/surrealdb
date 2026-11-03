//! Frozen bytes for the type catalog entry and the kinds that name types.
//!
//! A stored type definition is written by [`StoredTypeDefinition`]'s stored encoding at the key of
//! the type. `Kind::Enum` is variant 38 of `Kind` revision 2, holding its labels, and
//! `Kind::Named` is variant 40, holding the name of a type. Both are permanent: the bytes below
//! decode to the values they were captured from, and the current encoder writes each value as
//! exactly these bytes.
//!
//! Never edit the frozen bytes and never regenerate them to make a failing test pass.

use revision::{DeserializeRevisioned, SerializeRevisioned};
use surrealdb_strand::Strand;

use crate::catalog::{
	EnumLabel, EnumLabels, FromStored, Permission, StoredTypeDefinition, TypeBody, TypeDefinition,
	TypeId,
};
use crate::expr::statements::define::TypeAssert;
use crate::expr::{Expr, Kind, Literal};
use crate::key::KVValue;

/// A union of labels, one added after the others and one renamed.
fn union() -> TypeDefinition {
	TypeDefinition {
		id: TypeId(7),
		name: Strand::new("mood"),
		body: TypeBody::Union(EnumLabels {
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
		}),
		asserts: Vec::new(),
		default: None,
		sql_type: None,
		permissions: Permission::Full,
		comment: Some("how it feels".to_owned()),
	}
}

/// A type over a kind, with a named and an unnamed check, a default and a PostgreSQL type.
fn constrained() -> TypeDefinition {
	TypeDefinition {
		id: TypeId(8),
		name: Strand::new("percent"),
		body: TypeBody::Kind(Kind::Int),
		asserts: vec![
			TypeAssert {
				name: Some("in_range".to_owned()),
				check: crate::syn::expr("$value >= 0 AND $value <= 100").expect("parses"),
			},
			TypeAssert {
				name: None,
				check: crate::syn::expr("$value != 13").expect("parses"),
			},
		],
		default: Some(Expr::Literal(Literal::Integer(50))),
		sql_type: Some("integer".to_owned()),
		permissions: Permission::None,
		comment: None,
	}
}

fn definitions() -> Vec<(&'static str, TypeDefinition)> {
	vec![("union", union()), ("constrained", constrained())]
}

fn kinds() -> Vec<(&'static str, Kind)> {
	vec![
		("named", Kind::Named("mood".to_owned())),
		("inline", Kind::Enum(vec!["a".to_owned(), "it's".to_owned(), String::new()])),
	]
}

#[test]
fn the_frozen_definitions_decode_and_are_written_as_they_were() {
	for (name, definition) in definitions() {
		let bytes = frozen(DEFINITIONS, name);
		let stored: StoredTypeDefinition = KVValue::kv_decode_value(bytes, ())
			.unwrap_or_else(|e| panic!("BACKWARDS COMPATIBILITY BROKEN: type {name}: {e}"));
		let decoded = TypeDefinition::from_stored(&stored).expect("compiles");
		assert_eq!(decoded, definition, "{name}");
		assert_eq!(
			definition.to_stored().kv_encode_value().expect("encodes"),
			bytes,
			"the type definition {name} is written differently"
		);
	}
}

#[test]
fn the_frozen_kinds_decode_and_are_written_as_they_were() {
	for (name, kind) in kinds() {
		let bytes = frozen(KINDS, name);
		let decoded = Kind::deserialize_revisioned(&mut &bytes[..])
			.unwrap_or_else(|e| panic!("BACKWARDS COMPATIBILITY BROKEN: kind {name}: {e}"));
		assert_eq!(decoded, kind, "{name}");
		let mut written = Vec::new();
		kind.serialize_revisioned(&mut written).expect("encodes");
		assert_eq!(written, bytes, "kind {name} is written differently");
	}
}

fn frozen(table: &[(&'static str, &'static [u8])], name: &str) -> &'static [u8] {
	table
		.iter()
		.find(|(n, _)| *n == name)
		.map(|(_, bytes)| *bytes)
		.unwrap_or_else(|| panic!("no frozen bytes for {name}"))
}

/// Prints [`DEFINITIONS`] and [`KINDS`]. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::type_definition::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixtures; run by hand when a case is added"]
fn generate() {
	for (name, definition) in definitions() {
		println!("{name}: {:?}", definition.to_stored().kv_encode_value().expect("encodes"));
	}
	for (name, kind) in kinds() {
		let mut written = Vec::new();
		kind.serialize_revisioned(&mut written).expect("encodes");
		println!("{name}: {written:?}");
	}
}

/// FROZEN. The stored encoding of each of [`definitions`].
#[rustfmt::skip]
const DEFINITIONS: &[(&str, &[u8])] = &[
	("union", &[
		1, 1, 7, 4, 109, 111, 111, 100, 1, 1, 3, 1, 3, 115, 97, 100, 253, 0, 0, 0, 0, 1, 0, 0, 0,
		1, 1, 2, 111, 107, 253, 0, 0, 0, 128, 1, 0, 0, 0, 3, 1, 5, 104, 97, 112, 112, 121, 253, 0,
		0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 1, 1, 12, 104, 111, 119, 32, 105, 116, 32, 102, 101,
		101, 108, 115,
	]),
	("constrained", &[
		1, 1, 8, 7, 112, 101, 114, 99, 101, 110, 116, 1, 0, 3, 105, 110, 116, 2, 1, 1, 8, 105,
		110, 95, 114, 97, 110, 103, 101, 29, 36, 118, 97, 108, 117, 101, 32, 62, 61, 32, 48, 32,
		65, 78, 68, 32, 36, 118, 97, 108, 117, 101, 32, 60, 61, 32, 49, 48, 48, 1, 0, 12, 36, 118,
		97, 108, 117, 101, 32, 33, 61, 32, 49, 51, 1, 2, 53, 48, 1, 7, 105, 110, 116, 101, 103,
		101, 114, 1, 0, 0,
	]),
];

/// FROZEN. The `revision` bytes of the kinds that name or list a type's labels.
const KINDS: &[(&str, &[u8])] = &[
	("named", &[2, 40, 4, 109, 111, 111, 100]),
	("inline", &[2, 38, 3, 1, 97, 4, 105, 116, 39, 115, 0]),
];
