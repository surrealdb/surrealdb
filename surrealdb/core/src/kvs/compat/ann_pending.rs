//! Backwards-compatibility coverage for the ANN pending values' revision bump.
//!
//! [`HnswRecordPendingUpdate`] and [`DiskAnnRecordPendingUpdate`] gained `id`
//! (the record the entry belongs to) at revision 2. Every `!hr`/`!dr`/`!dw`
//! entry an older node wrote is revision 1, and a rolling upgrade decodes those
//! with the new code: if the added field's encoding were wrong in any way,
//! `kv_decode_value` would fail for every queued write on every node — and
//! `revision.lock` would not catch it, because it records that the shape
//! changed, not that old bytes still decode.
//!
//! The tests beside the types construct the value at revision 2 and round-trip
//! it through revision-2 code, which by construction cannot see that break.
//! This module holds frozen revision-1 bytes instead, and asserts on the decoded
//! value's fields; it does not exercise either read path.
//!
//! This lives outside the `fixtures.rs`/`vX_Y_Z.rs` machinery next door because
//! that machinery snapshots a whole catalog at a released version, and no
//! released snapshot contains either pending value. Regenerate the constants
//! with the ignored test at the bottom if — and only if — a revision-1 *shape*
//! is ever restated; the bytes themselves must never be edited by hand.

use revision::revisioned;
use surrealdb_datastore::values::diskann::DiskAnnRecordPendingUpdate;
use surrealdb_datastore::values::hnsw::HnswRecordPendingUpdate;
use surrealdb_datastore::values::ids::DocId;
use surrealdb_datastore::values::vector::SerializedVector;

use crate::key::KVValue;

/// `HnswRecordPendingUpdate` exactly as revision 1 declared it: the current
/// struct minus the `id` field that `#[revision(start = 2)]` added.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
struct HnswRecordPendingUpdateV1 {
	doc_id: Option<DocId>,
	old_vectors: Vec<SerializedVector>,
	new_vectors: Vec<SerializedVector>,
}

/// `DiskAnnRecordPendingUpdate` at revision 1, for the same reason.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
struct DiskAnnRecordPendingUpdateV1 {
	doc_id: Option<DocId>,
	old_vectors: Vec<SerializedVector>,
	new_vectors: Vec<SerializedVector>,
}

/// The value the frozen bytes were captured from: an update that both replaces
/// a vector and carries a doc-ID, so a decode break in any field shows up.
fn hnsw_v1() -> HnswRecordPendingUpdateV1 {
	HnswRecordPendingUpdateV1 {
		doc_id: Some(42),
		old_vectors: vec![SerializedVector::F32(vec![1.0, 2.0, 3.0, 4.0])],
		new_vectors: vec![SerializedVector::F32(vec![5.0, 6.0, 7.0, 8.0])],
	}
}

fn diskann_v1() -> DiskAnnRecordPendingUpdateV1 {
	DiskAnnRecordPendingUpdateV1 {
		doc_id: Some(42),
		old_vectors: vec![SerializedVector::F32(vec![1.0, 2.0, 3.0, 4.0])],
		new_vectors: vec![SerializedVector::F32(vec![5.0, 6.0, 7.0, 8.0])],
	}
}

/// FROZEN. Revision-1 bytes as an older node wrote them under `!hr`. Never edit
/// these by hand and never regenerate them to make a failing test pass: a
/// failure here means the current code can no longer read what a previous
/// version wrote.
#[rustfmt::skip]
const HNSW_PENDING_V1: &[u8] = &[
	1, 1, 42, 1, 2, 1, 4, 0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64, 0, 0, 128, 64, 1, 2, 1, 4, 0,
	0, 160, 64, 0, 0, 192, 64, 0, 0, 224, 64, 0, 0, 0, 65,
];

/// FROZEN. Revision-1 bytes as an older node wrote them under `!dr`/`!dw`. Never edit
/// these by hand and never regenerate them to make a failing test pass: a
/// failure here means the current code can no longer read what a previous
/// version wrote.
#[rustfmt::skip]
const DISKANN_PENDING_V1: &[u8] = &[
	1, 1, 42, 1, 2, 1, 4, 0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64, 0, 0, 128, 64, 1, 2, 1, 4, 0,
	0, 160, 64, 0, 0, 192, 64, 0, 0, 224, 64, 0, 0, 0, 65,
];

/// Revision-1 bytes must still decode, and the field `#[revision(start = 2)]`
/// added must default to `None` — an entry queued before the value carried the
/// record id. That default is what sends the read path to the key's lossy copy,
/// so it is the behaviour the fallback arm exists to serve.
#[test]
fn v1_ann_pending_entries_decode_with_no_record_id() {
	let hnsw = HnswRecordPendingUpdate::kv_decode_value(HNSW_PENDING_V1, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-1 HnswRecordPendingUpdate bytes no longer \
			 decode.\nError: {e}\n\nEvery `!hr` entry written by a previous version is revision \
			 1. A node that cannot decode them cannot compact the backlog it inherits."
		)
	});
	let diskann = DiskAnnRecordPendingUpdate::kv_decode_value(DISKANN_PENDING_V1, ())
		.unwrap_or_else(|e| {
			panic!(
				"BACKWARDS COMPATIBILITY BROKEN: revision-1 DiskAnnRecordPendingUpdate bytes no \
				 longer decode.\nError: {e}\n\nEvery `!dr`/`!dw` entry written by a previous \
				 version is revision 1."
			)
		});

	let expected_hnsw = hnsw_v1();
	assert_eq!(hnsw.id, None, "the field added at revision 2 must default to None");
	assert_eq!(hnsw.doc_id, expected_hnsw.doc_id);
	assert_eq!(hnsw.old_vectors, expected_hnsw.old_vectors);
	assert_eq!(hnsw.new_vectors, expected_hnsw.new_vectors);

	let expected_diskann = diskann_v1();
	assert_eq!(diskann.id, None, "the field added at revision 2 must default to None");
	assert_eq!(diskann.doc_id, expected_diskann.doc_id);
	assert_eq!(diskann.old_vectors, expected_diskann.old_vectors);
	assert_eq!(diskann.new_vectors, expected_diskann.new_vectors);
}

/// Decoding revision-1 bytes and writing them back produces revision-2 bytes,
/// and decoding those again is lossless.
///
/// The revision byte moving 1 -> 2 is the normal, intended cost of the bump. It
/// is asserted rather than assumed so that a change which silently kept writing
/// revision 1 — and therefore silently dropped every stamped id, reinstating the
/// defect this field exists to fix — fails here.
#[test]
fn re_encoding_an_upgraded_pending_entry_writes_revision_2_losslessly() {
	let decoded = HnswRecordPendingUpdate::kv_decode_value(HNSW_PENDING_V1, ()).expect("decode");
	let re_encoded = decoded.kv_encode_value().expect("encode");

	assert_eq!(HNSW_PENDING_V1.first(), Some(&1), "the frozen fixture must be revision 1");
	assert_eq!(re_encoded.first(), Some(&2), "an upgraded node must write revision 2");
	assert_eq!(
		re_encoded.len(),
		HNSW_PENDING_V1.len() + 1,
		"the only added byte is the absent id's `None` marker"
	);

	let round_tripped =
		HnswRecordPendingUpdate::kv_decode_value(&re_encoded, ()).expect("re-decode");
	assert_eq!(round_tripped.id, None);
	assert_eq!(round_tripped.doc_id, decoded.doc_id);
	assert_eq!(round_tripped.new_vectors, decoded.new_vectors);

	// The same property for DiskANN. It is a property of each `revisioned`
	// declaration rather than of a read path, so one struct passing says nothing
	// about the other.
	let decoded =
		DiskAnnRecordPendingUpdate::kv_decode_value(DISKANN_PENDING_V1, ()).expect("decode");
	let re_encoded = decoded.kv_encode_value().expect("encode");

	assert_eq!(DISKANN_PENDING_V1.first(), Some(&1), "the frozen fixture must be revision 1");
	assert_eq!(re_encoded.first(), Some(&2), "an upgraded node must write revision 2");
	assert_eq!(
		re_encoded.len(),
		DISKANN_PENDING_V1.len() + 1,
		"the only added byte is the absent id's `None` marker"
	);

	let round_tripped =
		DiskAnnRecordPendingUpdate::kv_decode_value(&re_encoded, ()).expect("re-decode");
	assert_eq!(round_tripped.id, None);
	assert_eq!(round_tripped.doc_id, decoded.doc_id);
	assert_eq!(round_tripped.new_vectors, decoded.new_vectors);
}

/// Regenerate the frozen constants. Ignored so it never runs in CI:
///
/// ```text
/// cargo test -p surrealdb-core --lib kvs::compat::ann_pending::generate -- --ignored --nocapture
/// ```
#[test]
#[ignore = "prints the frozen fixtures; run by hand when a revision-1 shape is restated"]
fn generate() {
	for (name, bytes) in [
		("HNSW_PENDING_V1", revision::to_vec(&hnsw_v1()).expect("encode")),
		("DISKANN_PENDING_V1", revision::to_vec(&diskann_v1()).expect("encode")),
	] {
		let body = bytes
			.chunks(16)
			.map(|row| {
				let cells: Vec<String> = row.iter().map(|b| b.to_string()).collect();
				format!("\t{},", cells.join(", "))
			})
			.collect::<Vec<_>>()
			.join("\n");
		println!("const {name}: &[u8] = &[\n{body}\n];");
	}
}
