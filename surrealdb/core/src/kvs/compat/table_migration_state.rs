//! Backwards-compatibility coverage for the table migration job's progress at `!tm`
//! ([`TableMigrationState`]), which gained the check for undeclared keys (`undeclared`) at
//! revision 2 and the holds of removals under way (`holds`) at revision 3.
//!
//! The progress is written by the node that holds the job's lease and read by whichever node
//! holds it next, so an entry of revision 1 must still decode, with no check queued, and one of
//! revision 2 with no hold. No released
//! snapshot holds an entry, so this lives outside the `fixtures.rs`/`vX_Y_Z.rs` machinery. The
//! bytes below are frozen: a failure means the current code can no longer read what an earlier
//! build wrote. Never edit them by hand and never regenerate them to make a failing test pass.

use revision::revisioned;

use crate::key::KVValue;
use crate::key::table_migration::{TableMigrationState, UndeclaredKeys, VerifiedField};

/// `TableMigrationState` exactly as revision 1 declared it: the current struct without the
/// `undeclared` field that `#[revision(start = 2)]` added.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
struct TableMigrationStateV1 {
	lap: u64,
	target: u16,
	cursor: Option<Vec<u8>>,
	scanned: u64,
	rewritten: u64,
	fallbacks: u64,
	rewrite: bool,
	verify: Vec<VerifiedField>,
	settled: bool,
}

/// The value the frozen bytes were captured from: a lap part-way through a rewrite and a
/// verification, so a decode break in any field shows up.
fn state_v1() -> TableMigrationStateV1 {
	TableMigrationStateV1 {
		lap: 3,
		target: 7,
		cursor: Some(b"/*cursor".to_vec()),
		scanned: 512,
		rewritten: 300,
		fallbacks: 2,
		rewrite: true,
		verify: vec![
			VerifiedField {
				name: "age".into(),
				definition: -42,
				failures: 1,
				finished: false,
			},
			VerifiedField {
				name: "name".into(),
				definition: 9,
				failures: 4,
				finished: true,
			},
		],
		settled: true,
	}
}

/// FROZEN. Revision-1 bytes as a build before revision 2 wrote them under `!tm`.
#[rustfmt::skip]
const STATE_V1: &[u8] = &[
	1, 3, 7, 1, 8, 47, 42, 99, 117, 114, 115, 111, 114, 251, 0, 2,
	251, 44, 1, 2, 1, 2, 1, 3, 97, 103, 101, 83, 1, 0, 1, 4,
	110, 97, 109, 101, 18, 4, 1, 1,
];

/// Revision-1 bytes still decode, every field as it was written, with no check for undeclared
/// keys queued.
#[test]
fn a_revision_1_entry_decodes_with_no_check_for_undeclared_keys() {
	let decoded = TableMigrationState::kv_decode_value(STATE_V1, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-1 TableMigrationState bytes no longer \
			 decode.\nError: {e}\n\nA node that cannot decode a queue entry skips its table, so \
			 the rewrite and verification it holds never finish."
		)
	});
	let expected = state_v1();
	assert_eq!(
		decoded,
		TableMigrationState {
			lap: expected.lap,
			target: expected.target,
			cursor: expected.cursor,
			scanned: expected.scanned,
			rewritten: expected.rewritten,
			fallbacks: expected.fallbacks,
			rewrite: expected.rewrite,
			verify: expected.verify,
			settled: expected.settled,
			undeclared: None,
			holds: Vec::new(),
		}
	);
}

/// An entry decoded from revision 1 is written back at the current revision, and reads back
/// unchanged.
#[test]
fn a_revision_1_entry_is_written_back_at_the_current_revision() {
	let decoded = TableMigrationState::kv_decode_value(STATE_V1, ()).expect("decode");
	let encoded = decoded.kv_encode_value().expect("encode");
	assert_eq!(STATE_V1.first(), Some(&1), "the frozen fixture is revision 1");
	assert_eq!(encoded.first(), Some(&3), "the entry is written at revision 3");
	assert_eq!(TableMigrationState::kv_decode_value(&encoded, ()).expect("re-decode"), decoded);
}

/// `TableMigrationState` exactly as revision 2 declared it: the current struct without the
/// `holds` field that `#[revision(start = 3)]` added.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, PartialEq)]
struct TableMigrationStateV2 {
	lap: u64,
	target: u16,
	cursor: Option<Vec<u8>>,
	scanned: u64,
	rewritten: u64,
	fallbacks: u64,
	rewrite: bool,
	verify: Vec<VerifiedField>,
	settled: bool,
	undeclared: Option<UndeclaredKeys>,
}

/// The value the revision-2 bytes were captured from: the revision-1 value with a check for
/// undeclared keys under way.
fn state_v2() -> TableMigrationStateV2 {
	let v1 = state_v1();
	TableMigrationStateV2 {
		lap: v1.lap,
		target: v1.target,
		cursor: v1.cursor,
		scanned: v1.scanned,
		rewritten: v1.rewritten,
		fallbacks: v1.fallbacks,
		rewrite: v1.rewrite,
		verify: v1.verify,
		settled: v1.settled,
		undeclared: Some(UndeclaredKeys {
			declared: vec!["a".into(), "b".into()],
			failures: 3,
			finished: false,
		}),
	}
}

/// FROZEN. Revision-2 bytes as a build before revision 3 wrote them under `!tm`.
#[rustfmt::skip]
const STATE_V2: &[u8] = &[
	2, 3, 7, 1, 8, 47, 42, 99, 117, 114, 115, 111, 114, 251, 0, 2,
	251, 44, 1, 2, 1, 2, 1, 3, 97, 103, 101, 83, 1, 0, 1, 4,
	110, 97, 109, 101, 18, 4, 1, 1, 1, 1, 2, 1, 97, 1, 98, 3,
	0,
];

/// Revision-2 bytes still decode, every field as it was written, with no removal holding the
/// job off the table.
#[test]
fn a_revision_2_entry_decodes_with_no_removal_hold() {
	let decoded = TableMigrationState::kv_decode_value(STATE_V2, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-2 TableMigrationState bytes no longer \
			 decode.\nError: {e}\n\nA node that cannot decode a queue entry skips its table, so \
			 the rewrite and verification it holds never finish."
		)
	});
	let expected = state_v2();
	assert_eq!(STATE_V2.first(), Some(&2), "the frozen fixture is revision 2");
	assert_eq!(
		decoded,
		TableMigrationState {
			lap: expected.lap,
			target: expected.target,
			cursor: expected.cursor,
			scanned: expected.scanned,
			rewritten: expected.rewritten,
			fallbacks: expected.fallbacks,
			rewrite: expected.rewrite,
			verify: expected.verify,
			settled: expected.settled,
			undeclared: expected.undeclared,
			holds: Vec::new(),
		}
	);
}

/// Regenerate the frozen constant. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::table_migration_state::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixture; run by hand when the revision-1 shape is restated"]
fn generate() {
	let bytes = revision::to_vec(&state_v1()).expect("encode");
	let body = bytes
		.chunks(16)
		.map(|row| {
			let cells: Vec<String> = row.iter().map(|b| b.to_string()).collect();
			format!("\t{},", cells.join(", "))
		})
		.collect::<Vec<_>>()
		.join("\n");
	println!("const STATE_V1: &[u8] = &[\n{body}\n];");
}

/// Regenerate the frozen revision-2 constant, as [`generate`] does the revision-1 one:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::table_migration_state::generate_v2 -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixture; run by hand when the revision-2 shape is restated"]
fn generate_v2() {
	let bytes = revision::to_vec(&state_v2()).expect("encode");
	let body = bytes
		.chunks(16)
		.map(|row| {
			let cells: Vec<String> = row.iter().map(|b| b.to_string()).collect();
			format!("\t{},", cells.join(", "))
		})
		.collect::<Vec<_>>()
		.join("\n");
	println!("const STATE_V2: &[u8] = &[\n{body}\n];");
}
