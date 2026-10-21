//! Backwards-compatibility coverage for a full-text index's posting blocks.
//!
//! An index stamped at the posting-block format keeps each term's compacted
//! documents in [`PostingBlock`]s (`!tp`) listed in a [`TermBlocks`] (`!tk`).
//! Every node of a cluster reads what another wrote, and a later release reads
//! what an earlier one wrote, so these bytes must keep decoding to the same
//! values. `revision.lock` records that a shape changed, not that bytes written
//! before still decode.
//!
//! This lives outside the `fixtures.rs`/`vX_Y_Z.rs` machinery because no
//! released snapshot holds either value. Regenerate the constants with the
//! ignored test at the bottom if — and only if — a revision-1 *shape* is ever
//! restated; the bytes themselves must never be edited by hand.

use surrealdb_datastore::values::fulltext::{BlockSummary, PostingBlock, TermBlocks};

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

/// Prints the constants above. Run once when a shape is first frozen.
#[test]
#[ignore = "generates frozen bytes"]
fn generate_posting_block_bytes() {
	println!("POSTING_BLOCK_V1 = {:?}", block().kv_encode_value().unwrap());
	println!("TERM_BLOCKS_V1 = {:?}", blocks().kv_encode_value().unwrap());
}
