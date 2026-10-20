//! Backwards-compatibility coverage for the index build state's revision bump.
//!
//! [`IndexBuildState`] gained `basis` at revision 5. Every `!bs` key an older node wrote is
//! revision 4 or earlier, and the new code reads it on every write admission and compare-and-swaps
//! it on every build transition: if the added field's encoding were wrong in any way, no index an
//! older release built could be written to, rebuilt or resumed — and `revision.lock` would not
//! catch it, because it records that the shape changed, not that old bytes still decode.
//!
//! This module holds frozen revision-4 bytes, and asserts on the decoded value's fields. Regenerate
//! the constant with the ignored test at the bottom if — and only if — the revision-4 *shape* is
//! ever restated; the bytes themselves must never be edited by hand.

use chrono::{DateTime, TimeZone, Utc};
use revision::revisioned;
use surrealdb_datastore::values::index_build::{
	IndexBuildPhase, IndexBuildReportStatus, IndexBuildState,
};
use surrealdb_expr::val::RecordIdKey;
use uuid::Uuid;

use crate::key::KVValue;

/// `IndexBuildState` exactly as revision 4 declared it: the current struct minus the `basis` field
/// that `#[revision(start = 5)]` added.
#[revisioned(revision = 4)]
#[derive(Clone, Debug, PartialEq)]
struct IndexBuildStateV4 {
	generation: u64,
	phase: IndexBuildPhase,
	owner: Option<Uuid>,
	next_ticket: u64,
	initial_complete: bool,
	updated_at: DateTime<Utc>,
	#[revision(start = 3)]
	owner_heartbeat_at: Option<DateTime<Utc>>,
	#[revision(start = 2)]
	error: Option<String>,
	#[revision(start = 3)]
	report_status: Option<IndexBuildReportStatus>,
	#[revision(start = 3)]
	initial: Option<u64>,
	#[revision(start = 3)]
	updated: Option<u64>,
	#[revision(start = 3)]
	pending: Option<u64>,
	#[revision(start = 4)]
	initial_cursor: Option<RecordIdKey>,
}

/// The value the frozen bytes were captured from: an owned `Online` generation with every field
/// set, so a decode break in any of them shows up.
fn state_v4() -> IndexBuildStateV4 {
	let at = Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap();
	IndexBuildStateV4 {
		generation: 3,
		phase: IndexBuildPhase::Online,
		owner: Some(Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)),
		next_ticket: 9,
		initial_complete: true,
		updated_at: at,
		owner_heartbeat_at: Some(at),
		error: None,
		report_status: Some(IndexBuildReportStatus::Ready),
		initial: Some(2000),
		updated: Some(4),
		pending: Some(0),
		initial_cursor: Some(RecordIdKey::Number(1999)),
	}
}

/// FROZEN. Revision-4 bytes as an older node wrote them under `!bs`. Never edit these by hand and
/// never regenerate them to make a failing test pass: a failure here means the current code can no
/// longer read what a previous version wrote.
#[rustfmt::skip]
const INDEX_BUILD_STATE_V4: &[u8] = &[
	4, 3, 1, 2, 1, 1, 35, 69, 103, 137, 171, 205, 239, 1, 35, 69, 103, 137, 171, 205, 239, 9, 1,
	252, 128, 175, 174, 213, 0, 1, 252, 128, 175, 174, 213, 0, 0, 1, 1, 3, 1, 251, 208, 7, 1, 4, 1,
	0, 1, 1, 0, 251, 158, 15,
];

/// Revision-4 bytes must still decode, and the field `#[revision(start = 5)]` added must default to
/// `None`: what a generation an older release recorded was built against is unknown, so it is
/// never withdrawn on that account.
#[test]
fn v4_index_build_state_decodes_with_no_basis() {
	let state = IndexBuildState::kv_decode_value(INDEX_BUILD_STATE_V4, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-4 IndexBuildState bytes no longer decode.\n\
			 Error: {e}\n\nEvery `!bs` key written by a previous version is revision 4 or earlier. \
			 A node that cannot decode it cannot admit writes to, rebuild or resume the index."
		)
	});
	let expected = state_v4();
	assert_eq!(state.generation, expected.generation);
	assert_eq!(state.phase, expected.phase);
	assert_eq!(state.owner, expected.owner);
	assert_eq!(state.next_ticket, expected.next_ticket);
	assert_eq!(state.initial_complete, expected.initial_complete);
	assert_eq!(state.updated_at, expected.updated_at);
	assert_eq!(state.owner_heartbeat_at, expected.owner_heartbeat_at);
	assert_eq!(state.error, expected.error);
	assert_eq!(state.report_status, expected.report_status);
	assert_eq!(state.initial, expected.initial);
	assert_eq!(state.updated, expected.updated);
	assert_eq!(state.pending, expected.pending);
	assert_eq!(state.initial_cursor, expected.initial_cursor);
	assert_eq!(state.basis, None, "the field added at revision 5 must default to None");
}

/// Decoding revision-4 bytes and writing them back produces revision-5 bytes, and decoding those
/// again is lossless. The revision byte moving 4 -> 5 is the intended cost of the bump, asserted so
/// that a change which silently kept writing revision 4 fails here.
#[test]
fn re_encoding_an_upgraded_index_build_state_writes_revision_5_losslessly() {
	let decoded = IndexBuildState::kv_decode_value(INDEX_BUILD_STATE_V4, ()).expect("decode");
	let re_encoded = decoded.kv_encode_value().expect("encode");
	assert_eq!(INDEX_BUILD_STATE_V4.first(), Some(&4), "the frozen fixture must be revision 4");
	assert_eq!(re_encoded.first(), Some(&5), "an upgraded node must write revision 5");
	let round_tripped = IndexBuildState::kv_decode_value(&re_encoded, ()).expect("re-decode");
	assert_eq!(round_tripped, decoded);
}

/// Regenerate the frozen constant. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::index_build_state::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixture; run by hand when the revision-4 shape is restated"]
fn generate() {
	let bytes = revision::to_vec(&state_v4()).expect("encode");
	let cells: Vec<String> = bytes.iter().map(|b| b.to_string()).collect();
	println!("const INDEX_BUILD_STATE_V4: &[u8] = &[\n\t{},\n];", cells.join(", "));
}
