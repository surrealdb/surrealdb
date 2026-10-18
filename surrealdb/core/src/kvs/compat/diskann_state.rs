//! Backwards-compatibility coverage for the DiskANN graph state's revision bump.
//!
//! [`DiskAnnState`] gained `start_points` at revision 2. Every `!ds` key an older node wrote is
//! revision 1, and the new code reads it before every search and rewrites it with every
//! compaction: if the added field's encoding were wrong in any way, every DiskANN index an older
//! release built would fail to search or compact — and `revision.lock` would not catch it,
//! because it records that the shape changed, not that old bytes still decode.
//!
//! This module holds frozen revision-1 bytes, and asserts on the decoded value's fields. Regenerate
//! the constant with the ignored test at the bottom if — and only if — the revision-1 *shape* is
//! ever restated; the bytes themselves must never be edited by hand.

use revision::revisioned;
use surrealdb_datastore::values::diskann::DiskAnnState;

use crate::key::KVValue;

/// `DiskAnnState` exactly as revision 1 declared it: the current struct minus the `start_points`
/// field that `#[revision(start = 2)]` added.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
struct DiskAnnStateV1 {
	enter_point: Option<u64>,
	next_element_id: u64,
}

/// The value the frozen bytes were captured from: a graph with an entry point and allocated ids,
/// so a decode break in either field shows up.
fn state_v1() -> DiskAnnStateV1 {
	DiskAnnStateV1 {
		enter_point: Some(7),
		next_element_id: 300,
	}
}

/// FROZEN. Revision-1 bytes as an older node wrote them under `!ds`. Never edit these by hand
/// and never regenerate them to make a failing test pass: a failure here means the current code
/// can no longer read what a previous version wrote.
#[rustfmt::skip]
const DISKANN_STATE_V1: &[u8] = &[
	1, 1, 7, 251, 44, 1,
];

/// Revision-1 bytes must still decode, and the field `#[revision(start = 2)]` added must default
/// to empty: a graph an older release seeded keeps no start points beside its entry point.
#[test]
fn v1_diskann_state_decodes_with_no_start_points() {
	let state = DiskAnnState::kv_decode_value(DISKANN_STATE_V1, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-1 DiskAnnState bytes no longer decode.\n\
			 Error: {e}\n\nEvery `!ds` key written by a previous version is revision 1. A node \
			 that cannot decode it cannot search or compact the index."
		)
	});
	let expected = state_v1();
	assert_eq!(state.enter_point, expected.enter_point);
	assert_eq!(state.next_element_id, expected.next_element_id);
	assert!(state.start_points.is_empty(), "the field added at revision 2 must default to empty");
}

/// Decoding revision-1 bytes and writing them back produces revision-2 bytes, and decoding those
/// again is lossless. The revision byte moving 1 -> 2 is the intended cost of the bump, asserted
/// so that a change which silently kept writing revision 1 — and dropped every start point —
/// fails here.
#[test]
fn re_encoding_an_upgraded_diskann_state_writes_revision_2_losslessly() {
	let decoded = DiskAnnState::kv_decode_value(DISKANN_STATE_V1, ()).expect("decode");
	let re_encoded = decoded.kv_encode_value().expect("encode");
	assert_eq!(DISKANN_STATE_V1.first(), Some(&1), "the frozen fixture must be revision 1");
	assert_eq!(re_encoded.first(), Some(&2), "an upgraded node must write revision 2");
	assert_eq!(
		re_encoded.len(),
		DISKANN_STATE_V1.len() + 1,
		"the only added byte is the empty start points' length"
	);
	let round_tripped = DiskAnnState::kv_decode_value(&re_encoded, ()).expect("re-decode");
	assert_eq!(round_tripped.enter_point, decoded.enter_point);
	assert_eq!(round_tripped.next_element_id, decoded.next_element_id);
	assert!(round_tripped.start_points.is_empty());
}

/// Regenerate the frozen constant. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::diskann_state::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixture; run by hand when the revision-1 shape is restated"]
fn generate() {
	let bytes = revision::to_vec(&state_v1()).expect("encode");
	let cells: Vec<String> = bytes.iter().map(|b| b.to_string()).collect();
	println!("const DISKANN_STATE_V1: &[u8] = &[\n\t{},\n];", cells.join(", "));
}
