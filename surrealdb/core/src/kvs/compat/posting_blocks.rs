//! Backwards-compatibility coverage for a full-text index's posting blocks.
//!
//! An index stamped at the posting-block format keeps each term's compacted
//! documents in [`PostingBlock`]s (`!tp`) listed in a [`TermBlocks`] (`!tk`),
//! and an addition to it carries its documents' postings in its change
//! ([`ChangeBatch`], `!tx`). Every node of a cluster reads what another
//! wrote, and a later release reads what an earlier one wrote, so these bytes
//! must keep decoding to the same values. `revision.lock` records that a shape changed, not that
//! bytes written before still decode.
//!
//! This lives outside the `fixtures.rs`/`vX_Y_Z.rs` machinery because no
//! released snapshot holds these values. A change without postings is a bare
//! bitmap, the `!tx` value every release has written. Regenerate the constants
//! with the ignored test at the bottom if — and only if — a *shape* is ever
//! restated; the bytes themselves must never be edited by hand.

use roaring::RoaringTreemap;
use surrealdb_datastore::values::fulltext::{
	BlockSummary, ChangeBatch, ChangePostings, PostingBlock, TermBlocks,
};

use crate::key::KVValue;

/// A block whose gaps, frequencies and lengths each need more than one byte
/// somewhere, so a decode break in any field shows up.
fn block() -> PostingBlock {
	PostingBlock::from_entries([(5, 1, 12), (300, 2, 7), (70_000, 300, 40_000)])
}

/// The list naming that block and one after it.
fn blocks() -> TermBlocks {
	TermBlocks {
		blocks: vec![
			BlockSummary {
				start: 0,
				count: 3,
				max_frequency: 300,
				min_length: 7,
			},
			BlockSummary {
				start: 70_001,
				count: 1,
				max_frequency: 1,
				min_length: u64::MAX,
			},
		],
	}
}

/// A change naming documents in two of a treemap's bitmaps, and no postings.
fn bare_change() -> ChangeBatch {
	ChangeBatch {
		docs: RoaringTreemap::from_iter([3u64, 70_000, (1 << 32) + 5]),
		postings: None,
	}
}

/// The same change as an addition carrying a posting per document, whose
/// frequencies and lengths each need more than one byte somewhere.
fn carried_change() -> ChangeBatch {
	ChangeBatch {
		postings: Some(ChangePostings::from_postings([(1, 12), (300, 7), (2, 40_000)])),
		..bare_change()
	}
}

/// FROZEN. Revision-1 bytes of [`block`] as a node writes them under `!tp`.
/// Never edit these by hand and never regenerate them to make a failing test
/// pass: a failure here means the current code can no longer read what a
/// previous version wrote.
#[rustfmt::skip]
const POSTING_BLOCK_V1: &[u8] = &[
	1, 3, 6, 5, 167, 2, 196, 160, 4, 4, 1, 2, 172, 2, 5, 12,
	7, 192, 184, 2,
];

/// FROZEN. Revision-1 bytes of [`blocks`] as a node writes them under `!tk`.
/// Never edit these by hand and never regenerate them to make a failing test
/// pass.
#[rustfmt::skip]
const TERM_BLOCKS_V1: &[u8] = &[
	1, 2, 1, 0, 3, 251, 44, 1, 7, 1, 252, 113, 17, 1, 0, 1,
	1, 253, 255, 255, 255, 255, 255, 255, 255, 255,
];

/// FROZEN. The bytes of [`bare_change`] as a node writes them under `!tx`.
/// Never edit these by hand and never regenerate them to make a failing test
/// pass.
#[rustfmt::skip]
const BARE_CHANGE: &[u8] = &[
	2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 58, 48, 0, 0,
	2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 0, 0, 0,
	26, 0, 0, 0, 3, 0, 112, 17, 1, 0, 0, 0, 58, 48, 0, 0,
	1, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 5, 0,
];

/// FROZEN. The bytes of [`carried_change`] as a node writes them under `!tx`.
/// Never edit these by hand and never regenerate them to make a failing test
/// pass.
#[rustfmt::skip]
const CARRIED_CHANGE: &[u8] = &[
	2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 58, 48, 0, 0,
	2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 0, 0, 0,
	26, 0, 0, 0, 3, 0, 112, 17, 1, 0, 0, 0, 58, 48, 0, 0,
	1, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 5, 0, 1, 3,
	1, 12, 172, 2, 7, 2, 192, 184, 2,
];

/// Revision-1 bytes decode to the values they were captured from, and those
/// values encode to the same bytes, so a node of either side of a change reads
/// the other's blocks.
#[test]
fn v1_posting_blocks_decode_and_reencode() {
	let decoded = PostingBlock::kv_decode_value(POSTING_BLOCK_V1, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-1 PostingBlock bytes no longer decode.\n\
			 Error: {e}\n\nEvery `!tp` block a previous version compacted is revision 1."
		)
	});
	assert_eq!(decoded, block());
	assert_eq!(
		decoded.entries().expect("the block's entries"),
		[(5, 1, 12), (300, 2, 7), (70_000, 300, 40_000)]
	);
	assert_eq!(block().kv_encode_value().unwrap(), POSTING_BLOCK_V1);

	let decoded = TermBlocks::kv_decode_value(TERM_BLOCKS_V1, ()).unwrap_or_else(|e| {
		panic!(
			"BACKWARDS COMPATIBILITY BROKEN: revision-1 TermBlocks bytes no longer decode.\n\
			 Error: {e}\n\nEvery `!tk` list a previous version compacted is revision 1."
		)
	});
	assert_eq!(decoded, blocks());
	assert_eq!(blocks().kv_encode_value().unwrap(), TERM_BLOCKS_V1);
}

/// A change's bytes decode to the change they were captured from, and that
/// change encodes to the same bytes. A change without postings is its bitmap's
/// serialization alone, as every release writes a `!tx` value.
#[test]
fn change_batches_decode_and_reencode() {
	for (name, bytes, change) in
		[("bare", BARE_CHANGE, bare_change()), ("carried", CARRIED_CHANGE, carried_change())]
	{
		let decoded = ChangeBatch::kv_decode_value(bytes, ()).unwrap_or_else(|e| {
			panic!(
				"BACKWARDS COMPATIBILITY BROKEN: {name} `!tx` change bytes no longer decode.\n\
				 Error: {e}"
			)
		});
		assert_eq!(decoded, change, "{name}");
		assert_eq!(change.kv_encode_value().unwrap(), bytes, "{name}");
	}
	let mut bitmap = Vec::new();
	bare_change().docs.serialize_into(&mut bitmap).unwrap();
	assert_eq!(BARE_CHANGE, bitmap, "a bare change is its bitmap");
}

/// Prints the constants above. Run once when a shape is first frozen.
#[test]
#[ignore = "generates frozen bytes"]
fn generate_posting_block_bytes() {
	println!("POSTING_BLOCK_V1 = {:?}", block().kv_encode_value().unwrap());
	println!("TERM_BLOCKS_V1 = {:?}", blocks().kv_encode_value().unwrap());
	println!("BARE_CHANGE = {:?}", bare_change().kv_encode_value().unwrap());
	println!("CARRIED_CHANGE = {:?}", carried_change().kv_encode_value().unwrap());
}
