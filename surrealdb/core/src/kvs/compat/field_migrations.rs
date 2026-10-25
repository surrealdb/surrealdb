//! Backwards-compatibility coverage for the catalog state field migrations persist: the
//! migration a table generation's descriptor carries at `!lg{n}`, and a table's record of the
//! migrations it has run at `!mn`.
//!
//! No released snapshot holds either, so this lives outside the `fixtures.rs`/`vX_Y_Z.rs`
//! machinery. The bytes below are frozen: a failure means the current code can no longer read
//! what an earlier build of it wrote. Never edit them by hand and never regenerate them to make
//! a failing test pass.

use surrealdb_kvs::EncodeContext;

use crate::catalog::{
	ExprText, Generation, IdiomText, KindText, LayoutDescriptor, Migration, MigrationRun,
	MigrationStep, MigrationTest, RowLayout,
};
use crate::expr::Kind;
use crate::key::KVValue;
use crate::val::{TableName, Value};

/// The migration the frozen bytes were captured from: a rewrite with a fallback and a test.
fn rewrite() -> Migration {
	Migration {
		name: "age_to_int".into(),
		step: MigrationStep::Rewrite {
			path: IdiomText::from_raw("age"),
			using: ExprText::from_raw("<int> $value"),
			fallback: Some(Value::from(0i64)),
			test: MigrationTest {
				kind: Some(KindText::new(&Kind::Int)),
				schemafull: true,
				flexible: false,
				assert: Some(ExprText::from_raw("$value >= 0")),
			},
		},
	}
}

/// One of each other step.
fn others() -> [Migration; 2] {
	[
		Migration {
			name: "drop_legacy".into(),
			step: MigrationStep::Remove {
				path: IdiomText::from_raw("legacy"),
			},
		},
		Migration {
			name: "tidy".into(),
			step: MigrationStep::RemoveUndeclared {
				declared: vec!["age".into(), "name".into()],
			},
		},
	]
}

fn descriptor(migration: Option<Migration>, migrated: Option<u16>) -> LayoutDescriptor {
	LayoutDescriptor {
		table: TableName::from("person"),
		layout: RowLayout::Document,
		migration,
		migrated: migrated.map(|n| Generation::new(n).unwrap()),
		verified: None,
	}
}

/// FROZEN. A descriptor that carries no migration, as one written before migrations existed.
#[rustfmt::skip]
const DESCRIPTOR_PLAIN: &[u8] = &[254, 0, 128, 128, 16, 38, 2, 0, 8, 2, 7, 112, 101, 114, 115, 111, 110, 4, 38, 1, 0, 1, 0];

/// FROZEN. A descriptor minted by a migration.
#[rustfmt::skip]
const DESCRIPTOR_REWRITE: &[u8] = &[254, 0, 128, 128, 16, 38, 4, 0, 8, 14, 92, 2, 7, 112, 101, 114, 115, 111, 110, 4, 38, 1, 0, 1, 0, 6, 38, 2, 0, 12, 2, 7, 97, 103, 101, 95, 116, 111, 95, 105, 110, 116, 4, 38, 1, 0, 1, 38, 4, 0, 5, 19, 21, 2, 7, 97, 103, 101, 4, 7, 60, 105, 110, 116, 62, 32, 36, 118, 97, 108, 117, 101, 6, 128, 8, 38, 4, 0, 5, 7, 9, 2, 7, 105, 110, 116, 4, 3, 6, 2, 8, 7, 36, 118, 97, 108, 117, 101, 32, 62, 61, 32, 48, 8, 131];

/// FROZEN. A table's record of a run migration.
#[rustfmt::skip]
const MIGRATION_RUN: &[u8] = &[254, 0, 128, 128, 16, 38, 2, 0, 2, 2, 131, 4, 7, 97, 103, 101];

#[test]
fn a_descriptor_without_a_migration_round_trips_its_frozen_bytes() {
	let plain = descriptor(None, None);
	let encoded = plain.kv_encode_value_at(&EncodeContext::CURRENT).unwrap();
	assert_eq!(encoded.bytes, DESCRIPTOR_PLAIN, "{:?}", encoded.bytes);
	assert_eq!(LayoutDescriptor::kv_decode_value(DESCRIPTOR_PLAIN, ()).unwrap(), plain);
}

#[test]
fn a_descriptor_minted_by_a_migration_round_trips_its_frozen_bytes() {
	let minted = descriptor(Some(rewrite()), Some(3));
	let encoded = minted.kv_encode_value_at(&EncodeContext::CURRENT).unwrap();
	assert_eq!(encoded.bytes, DESCRIPTOR_REWRITE, "{:?}", encoded.bytes);
	assert_eq!(LayoutDescriptor::kv_decode_value(DESCRIPTOR_REWRITE, ()).unwrap(), minted);
}

#[test]
fn every_step_survives_a_descriptor() {
	for migration in std::iter::once(rewrite()).chain(others()) {
		let minted = descriptor(Some(migration), Some(4));
		let encoded = minted.kv_encode_value_at(&EncodeContext::CURRENT).unwrap();
		assert_eq!(LayoutDescriptor::kv_decode_value(&encoded.bytes, ()).unwrap(), minted);
	}
}

#[test]
fn a_migration_is_refused_at_the_legacy_level() {
	let minted = descriptor(Some(rewrite()), Some(3));
	assert!(minted.kv_encode_value_at(&EncodeContext::LEGACY).is_err());
}

#[test]
fn a_run_migration_round_trips_its_frozen_bytes() {
	let run = MigrationRun {
		generation: Generation::new(3).unwrap(),
		field: Some("age".into()),
	};
	let encoded = run.kv_encode_value_at(&EncodeContext::CURRENT).unwrap();
	assert_eq!(encoded.bytes, MIGRATION_RUN, "{:?}", encoded.bytes);
	assert_eq!(MigrationRun::kv_decode_value(MIGRATION_RUN, ()).unwrap(), run);
}
