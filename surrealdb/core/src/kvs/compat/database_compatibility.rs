//! Backwards-compatibility coverage for a database's compatibility level.
//!
//! A database definition written in `revision` stores no level and reads at level 3; a database
//! at level 4 is written only in the stored grammar, which a store at the Legacy feature level
//! refuses. The bytes below are frozen: a failure means the current code can no longer read what
//! an earlier build wrote. Never edit them by hand and never regenerate them to make a failing
//! test pass.

use surrealdb_kvs::EncodeContext;

use crate::catalog::{DatabaseCompatibility, DatabaseDefinition, DatabaseId, NamespaceId};
use crate::key::KVValue;
use crate::val::Value;
use crate::val::stored::schema::Unrevisioned;

/// The database the frozen bytes were captured from.
fn database(compatibility: DatabaseCompatibility) -> DatabaseDefinition {
	DatabaseDefinition {
		namespace_id: NamespaceId(1),
		database_id: DatabaseId(1),
		name: "test".into(),
		comment: None,
		changefeed: None,
		strict: false,
		compatibility: Unrevisioned(compatibility),
	}
}

/// FROZEN. The database in `revision`, which stores no level.
#[rustfmt::skip]
const DATABASE_REVISION: &[u8] = &[1, 1, 1, 4, 116, 101, 115, 116, 0, 0, 0];

/// The level the bytecode VM's codec reads from `bytes`.
fn vm_level(bytes: &[u8]) -> Value {
	let Value::Object(fields) = crate::exec::vm::codec::decode(
		&crate::exec::vm::codec::FULL_CODEC,
		"DatabaseDefinition",
		bytes,
	)
	.unwrap() else {
		panic!("a database decodes to an object");
	};
	fields.get("compatibility").cloned().unwrap_or(Value::None)
}

#[test]
fn a_revision_database_reads_at_level_3() {
	let decoded = DatabaseDefinition::kv_decode_value(DATABASE_REVISION, ()).unwrap();
	assert_eq!(decoded, database(DatabaseCompatibility::LEVEL_3));
	assert!(!decoded.enforces_migrations());
	assert_eq!(vm_level(DATABASE_REVISION), Value::from(3i64));
}

#[test]
fn a_database_at_level_3_is_written_in_revision() {
	let level_3 = database(DatabaseCompatibility::LEVEL_3);
	assert_eq!(level_3.kv_encode_value().unwrap(), DATABASE_REVISION);
	let at_legacy = level_3.kv_encode_value_at(&EncodeContext::LEGACY).unwrap();
	assert_eq!(at_legacy.bytes, DATABASE_REVISION);
	assert_eq!(at_legacy.floor, None);
}

#[test]
fn a_database_at_level_4_is_written_in_the_stored_grammar_only() {
	let level_4 = database(DatabaseCompatibility::LEVEL_4);
	assert!(level_4.kv_encode_value().is_err(), "revision would drop the level");
	assert!(level_4.kv_encode_value_at(&EncodeContext::LEGACY).is_err());
	let stored = level_4.kv_encode_value_at(&EncodeContext::CURRENT).unwrap();
	assert!(stored.floor.is_some());
	let decoded = DatabaseDefinition::kv_decode_value(&stored.bytes, ()).unwrap();
	assert_eq!(decoded, level_4);
	assert!(decoded.enforces_migrations());
	assert_eq!(vm_level(&stored.bytes), Value::from(4i64));
}

#[test]
fn a_stored_level_3_database_reads_as_the_revision_one_does() {
	let level_3 = database(DatabaseCompatibility::LEVEL_3);
	let stored = level_3.kv_encode_value_at(&EncodeContext::CURRENT).unwrap();
	let decoded = DatabaseDefinition::kv_decode_value(&stored.bytes, ()).unwrap();
	assert_eq!(decoded, DatabaseDefinition::kv_decode_value(DATABASE_REVISION, ()).unwrap());
}
