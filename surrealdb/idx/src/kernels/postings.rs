//! Posting-block kernels: a term's compacted postings as a full-text index
//! stamped at [`crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION`] or
//! later keeps them.
//!
//! A term's compacted documents are split into blocks (`!tp`), each covering
//! the document ids from its start up to the next block's, and listed in the
//! term's block list (`!tk`). [`TermPostings`] is what a query makes of a
//! term's blocks: its document set, and each document's frequency and length.
//! [`BlockWindow`] is what compaction makes of them: the block list, the
//! blocks a round's changes fall in, and the edits that round makes, with the
//! writes they come to.
//!
//! The first block of a term starts at zero, so every document id has exactly
//! one block: the last whose start is at or below it. A block is split once it
//! holds more than twice [`POSTING_BLOCK_SIZE`] documents, and merged with a
//! neighbour once it holds fewer than a quarter of it. A merge keeps the lower
//! block's start, so the first block keeps starting at zero.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context as _, Result};
use roaring::RoaringTreemap;

use crate::docids::DocId;
use crate::ft::fulltext::{BlockSummary, PostingBlock, TermBlocks};
use crate::ft::{DocLength, TermFrequency};

/// How many documents a posting block holds once split.
pub const POSTING_BLOCK_SIZE: usize = 256;

/// The most documents a block holds before it is split.
const SPLIT_ABOVE: usize = POSTING_BLOCK_SIZE * 2;

/// The fewest documents a block holds before it is merged with a neighbour.
const MERGE_BELOW: usize = POSTING_BLOCK_SIZE / 4;

/// One document's entry in a term's postings: its frequency of the term and
/// its length.
type Entry = (TermFrequency, DocLength);

/// A term's compacted postings, from all of its blocks: the documents carrying
/// it, and each one's frequency and length.
#[derive(Debug, Default, PartialEq)]
pub struct TermPostings {
	docs: RoaringTreemap,
	/// `(document, frequency, length)`, in ascending document order.
	entries: Vec<(DocId, TermFrequency, DocLength)>,
	/// Per block, in order: its first document, and where its entries start in
	/// `entries`.
	blocks: Vec<(DocId, usize)>,
	/// Each block's impacts, block after block, with where each block's start:
	/// the `(frequency, length)` pairs of its entries no other entry of the block
	/// has at least as high a frequency and at most as short a length as, an
	/// entry of frequency zero left out ([`impacts`]). Worked out the first time
	/// a query bounds the term's score, and kept with the postings after that.
	impacts: std::sync::OnceLock<(Vec<usize>, Vec<Entry>)>,
}

impl TermPostings {
	/// The postings `blocks` hold, given in ascending order of start, as a
	/// range read of the term's blocks returns them. Fails on a block whose
	/// bytes do not decode to its entries.
	pub fn from_blocks<'a>(blocks: impl IntoIterator<Item = &'a PostingBlock>) -> Result<Self> {
		let mut entries = Vec::new();
		let mut starts = Vec::new();
		for block in blocks {
			let held = decoded(block)?;
			if let Some((first, ..)) = held.first() {
				starts.push((*first, entries.len()));
			}
			entries.extend(held);
		}
		let docs = entries.iter().map(|(doc_id, ..)| *doc_id).collect();
		Ok(Self {
			docs,
			entries,
			blocks: starts,
			impacts: std::sync::OnceLock::new(),
		})
	}

	/// The documents carrying the term.
	pub fn docs(&self) -> &RoaringTreemap {
		&self.docs
	}

	/// `doc_id`'s frequency of the term and its length, when the term's blocks
	/// hold it.
	pub fn entry(&self, doc_id: DocId) -> Option<Entry> {
		let at = self.entries.binary_search_by_key(&doc_id, |(id, ..)| *id).ok()?;
		let (_, frequency, length) = self.entries[at];
		Some((frequency, length))
	}

	/// [`Self::entry`] of `doc_id`, found by a cursor `at` over the entries
	/// that a caller asking in ascending doc-id order moves forward: the search
	/// gallops from `at`, and leaves it at the first entry not below `doc_id`.
	/// A doc id below the cursor's entry is searched for from the start.
	pub fn entry_at(&self, at: &mut usize, doc_id: DocId) -> Option<Entry> {
		let from = match self.entries.get(*at) {
			Some((first, ..)) if *first <= doc_id => *at,
			Some(_) => 0,
			None if self.entries.last().is_some_and(|(last, ..)| *last < doc_id) => {
				*at = self.entries.len();
				return None;
			}
			None => 0,
		};
		// The first window past `from`, doubling, whose last entry is not below
		// `doc_id` holds it, if any entry does.
		let mut end = from + 1;
		let mut step = 1;
		while end < self.entries.len() && self.entries[end - 1].0 < doc_id {
			step *= 2;
			end = (end + step).min(self.entries.len());
		}
		let end = end.min(self.entries.len());
		*at = from + self.entries[from..end].partition_point(|(id, ..)| *id < doc_id);
		match self.entries.get(*at) {
			Some((id, frequency, length)) if *id == doc_id => Some((*frequency, *length)),
			Some(_) | None => None,
		}
	}

	/// Each block, in order, as its first document and its impacts. A score that
	/// rises with frequency and does not rise with length is highest, over a
	/// block's documents, at one of its impacts.
	pub fn block_impacts(&self) -> impl Iterator<Item = (DocId, &[Entry])> + '_ {
		let (starts, held) = self.impacts.get_or_init(|| {
			let mut starts = Vec::with_capacity(self.blocks.len());
			let mut held = Vec::new();
			for (at, (_, start)) in self.blocks.iter().enumerate() {
				let end = self.blocks.get(at + 1).map_or(self.entries.len(), |(_, end)| *end);
				starts.push(held.len());
				held.extend(impacts(self.entries[*start..end].iter().map(|(_, f, l)| (*f, *l))));
			}
			(starts, held)
		});
		self.blocks.iter().zip(starts).enumerate().map(move |(at, ((first, _), start))| {
			let end = starts.get(at + 1).copied().unwrap_or(held.len());
			(*first, &held[*start..end])
		})
	}

	/// The bytes the postings hold, as a cache weighs them: the impacts a query
	/// works out later, a fraction of the entries, are not counted.
	pub fn weight(&self) -> u64 {
		(self.docs.serialized_size()
			+ self.entries.len() * std::mem::size_of::<(DocId, TermFrequency, DocLength)>()
			+ self.blocks.len() * std::mem::size_of::<(DocId, usize)>()) as u64
	}
}

/// One block as a round edits it: its entries by document.
type Block = BTreeMap<DocId, Entry>;

/// A term's block list and the blocks one compaction round loaded, with the
/// edits the round makes to them.
///
/// The round names the documents its changes touch ([`Self::starts_for`]),
/// loads the blocks those fall in ([`Self::load`]), puts or removes each
/// document's entry, loads the neighbours an under-full block merges with
/// ([`Self::merge_partners`]), and takes the writes ([`Self::finish`]). A
/// block the round does not load is neither read nor written.
#[derive(Debug, Default)]
pub struct BlockWindow {
	/// Every block of the term, by start, with its summary as stored.
	summaries: BTreeMap<DocId, BlockSummary>,
	/// The blocks loaded, by start.
	loaded: BTreeMap<DocId, Block>,
	/// The starts of loaded blocks the round changed.
	dirty: BTreeSet<DocId>,
	/// The starts of stored blocks the round removed.
	removed: BTreeSet<DocId>,
}

impl BlockWindow {
	/// The window over a term whose block list is `blocks`, absent for a term
	/// with none.
	pub fn new(blocks: Option<TermBlocks>) -> Self {
		Self {
			summaries: blocks
				.unwrap_or_default()
				.blocks
				.into_iter()
				.map(|summary| (summary.start, summary))
				.collect(),
			..Self::default()
		}
	}

	/// The start of the block `doc_id` falls in, when the term has a block.
	fn covering(&self, doc_id: DocId) -> Option<DocId> {
		self.summaries.range(..=doc_id).next_back().map(|(start, _)| *start)
	}

	/// The starts of the stored blocks `doc_ids` fall in, which the round
	/// loads before it edits them.
	pub fn starts_for(&self, doc_ids: impl IntoIterator<Item = DocId>) -> BTreeSet<DocId> {
		doc_ids.into_iter().filter_map(|doc_id| self.covering(doc_id)).collect()
	}

	/// Takes in the stored block starting at `start`. Fails on a block whose
	/// bytes do not decode to its entries.
	pub fn load(&mut self, start: DocId, block: &PostingBlock) -> Result<()> {
		let entries = decoded(block)?.into_iter().map(|(doc_id, f, l)| (doc_id, (f, l))).collect();
		self.loaded.insert(start, entries);
		Ok(())
	}

	/// The block `doc_id` falls in, which must be loaded, or a first block
	/// starting at zero for a term with none.
	fn block_mut(&mut self, doc_id: DocId) -> Option<(DocId, &mut Block)> {
		let start = match self.covering(doc_id) {
			Some(start) => start,
			None => {
				self.summaries.insert(0, BlockSummary::default());
				self.loaded.insert(0, Block::new());
				0
			}
		};
		self.loaded.get_mut(&start).map(|block| (start, block))
	}

	/// Whether the term's blocks hold `doc_id`. Its block must be loaded.
	pub fn contains(&self, doc_id: DocId) -> bool {
		self.covering(doc_id)
			.and_then(|start| self.loaded.get(&start))
			.is_some_and(|block| block.contains_key(&doc_id))
	}

	/// Puts `doc_id`'s entry, replacing the one its block holds. Its block must
	/// be loaded.
	pub fn put(&mut self, doc_id: DocId, frequency: TermFrequency, length: DocLength) {
		match self.block_mut(doc_id) {
			Some((start, block)) => {
				block.insert(doc_id, (frequency, length));
				self.dirty.insert(start);
			}
			None => debug_assert!(false, "a put into an unloaded block"),
		}
	}

	/// Removes `doc_id`'s entry. Its block must be loaded.
	pub fn remove(&mut self, doc_id: DocId) {
		let Some(start) = self.covering(doc_id) else {
			return;
		};
		match self.loaded.get_mut(&start) {
			Some(block) => {
				if block.remove(&doc_id).is_some() {
					self.dirty.insert(start);
				}
			}
			None => debug_assert!(false, "a removal from an unloaded block"),
		}
	}

	/// The neighbour an under-full block merges with: the next block, or the
	/// previous one for the last block.
	fn partner(&self, start: DocId) -> Option<DocId> {
		self.summaries
			.range(start + 1..)
			.next()
			.or_else(|| self.summaries.range(..start).next_back())
			.map(|(start, _)| *start)
	}

	/// Drops every changed block above the first that the round left empty.
	/// It holds no document, so dropping it only widens the block before it,
	/// which is neither read nor rewritten for it. The first block is kept, so
	/// the term's blocks still start at zero; it is merged instead.
	fn drop_empty(&mut self) {
		let empty: Vec<DocId> = self
			.dirty
			.iter()
			.copied()
			.filter(|start| *start > 0 && self.loaded.get(start).is_some_and(Block::is_empty))
			.collect();
		for start in empty {
			self.loaded.remove(&start);
			self.summaries.remove(&start);
			self.dirty.remove(&start);
			self.removed.insert(start);
		}
	}

	/// The starts of the stored blocks the round must load before
	/// [`Self::finish`], so every changed block left under-full can be merged.
	///
	/// Called once the round's edits are made. Every block above the first
	/// that they emptied is dropped first, so a block an under-full one merges
	/// with is never empty.
	pub fn merge_partners(&mut self) -> BTreeSet<DocId> {
		self.drop_empty();
		self.dirty
			.iter()
			.filter(|start| self.loaded.get(start).is_some_and(|block| block.len() < MERGE_BELOW))
			.filter_map(|start| self.partner(*start))
			.filter(|start| !self.loaded.contains_key(start))
			.collect()
	}

	/// Merges every changed block left under-full into its neighbour, keeping
	/// the lower block's start, where the neighbour is loaded.
	fn merge(&mut self) {
		self.drop_empty();
		let underfull: Vec<DocId> = self
			.dirty
			.iter()
			.copied()
			.filter(|start| self.loaded.get(start).is_some_and(|block| block.len() < MERGE_BELOW))
			.collect();
		for start in underfull {
			// An earlier merge may have taken this block already.
			if !self.summaries.contains_key(&start) {
				continue;
			}
			match self.partner(start) {
				Some(partner) if self.loaded.contains_key(&partner) => {
					let (low, high) = if partner > start {
						(start, partner)
					} else {
						(partner, start)
					};
					let taken = self.loaded.remove(&high).unwrap_or_default();
					self.loaded.entry(low).or_default().extend(taken);
					self.summaries.remove(&high);
					self.dirty.remove(&high);
					self.removed.insert(high);
					self.dirty.insert(low);
				}
				// The term's only block: dropped once empty.
				None if self.loaded.get(&start).is_some_and(Block::is_empty) => {
					self.loaded.remove(&start);
					self.summaries.remove(&start);
					self.dirty.remove(&start);
					self.removed.insert(start);
				}
				_ => {}
			}
		}
	}

	/// Splits every changed block holding more than it may into blocks of
	/// [`POSTING_BLOCK_SIZE`] or so, each but the first starting at its first
	/// document.
	fn split(&mut self) {
		let overfull: Vec<DocId> = self
			.dirty
			.iter()
			.copied()
			.filter(|start| self.loaded.get(start).is_some_and(|block| block.len() > SPLIT_ABOVE))
			.collect();
		for start in overfull {
			let Some(block) = self.loaded.remove(&start) else {
				continue;
			};
			let parts = block.len().div_ceil(POSTING_BLOCK_SIZE);
			let per_part = block.len().div_ceil(parts);
			let entries: Vec<_> = block.into_iter().collect();
			for (at, chunk) in entries.chunks(per_part).enumerate() {
				let part_start = if at == 0 {
					start
				} else {
					chunk[0].0
				};
				self.loaded.insert(part_start, chunk.iter().copied().collect());
				self.summaries.insert(part_start, BlockSummary::default());
				self.removed.remove(&part_start);
				self.dirty.insert(part_start);
			}
		}
	}

	/// The writes the round's edits come to: each changed block, with `None`
	/// for one to delete, and the term's block list, with `None` where the term
	/// has no block left.
	pub fn finish(mut self) -> (Vec<(DocId, Option<PostingBlock>)>, Option<TermBlocks>) {
		self.merge();
		self.split();
		let mut writes = Vec::new();
		for start in &self.removed {
			writes.push((*start, None));
		}
		for start in &self.dirty {
			let Some(block) = self.loaded.get(start) else {
				continue;
			};
			let summary = summarise(*start, block);
			self.summaries.insert(*start, summary);
			writes.push((
				*start,
				Some(PostingBlock::from_entries(
					block.iter().map(|(doc_id, (f, l))| (*doc_id, *f, *l)),
				)),
			));
		}
		let blocks = (!self.summaries.is_empty()).then(|| TermBlocks {
			blocks: self.summaries.into_values().collect(),
		});
		(writes, blocks)
	}
}

/// The impacts of `entries`, `(frequency, length)` pairs: those no other pair has at
/// least as high a frequency and at most as short a length as, by descending
/// frequency. A pair of frequency zero is left out: it stands for a document
/// a reader scores from its stored postings.
fn impacts(entries: impl Iterator<Item = Entry>) -> Vec<Entry> {
	let mut pairs: Vec<Entry> = entries.filter(|(frequency, _)| *frequency > 0).collect();
	pairs.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
	let mut kept: Vec<Entry> = Vec::new();
	for pair in pairs {
		if kept.last().is_none_or(|(_, shortest)| pair.1 < *shortest) {
			kept.push(pair);
		}
	}
	kept
}

/// The entries `block` holds, or an error naming a block whose bytes do not
/// decode to them.
fn decoded(block: &PostingBlock) -> Result<Vec<(DocId, TermFrequency, DocLength)>> {
	block.entries().context("a posting block whose columns do not hold its entries")
}

/// The summary of the block starting at `start` that holds `block`.
///
/// An entry of frequency zero is one compaction placed for a document whose
/// stored postings no longer carry the term, which a reader scores from those
/// postings instead; it raises neither bound.
fn summarise(start: DocId, block: &Block) -> BlockSummary {
	let mut summary = BlockSummary {
		start,
		count: block.len() as u64,
		max_frequency: 0,
		min_length: DocLength::MAX,
	};
	for (frequency, length) in block.values() {
		if *frequency > 0 {
			summary.max_frequency = summary.max_frequency.max(*frequency);
			summary.min_length = summary.min_length.min(*length);
		}
	}
	summary
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A stored term: its block list and its blocks, as a round would read them.
	#[derive(Default)]
	struct Stored {
		blocks: Option<TermBlocks>,
		data: BTreeMap<DocId, PostingBlock>,
	}

	impl Stored {
		/// Runs one round over the stored term: loads what the edits touch,
		/// applies them, loads merge partners, and writes the result back.
		fn round(&mut self, edits: &[(DocId, Option<Entry>)]) {
			let mut window = BlockWindow::new(self.blocks.clone());
			for start in window.starts_for(edits.iter().map(|(doc_id, _)| *doc_id)) {
				window.load(start, &self.data[&start]).unwrap();
			}
			for (doc_id, entry) in edits {
				match entry {
					Some((f, l)) => window.put(*doc_id, *f, *l),
					None => window.remove(*doc_id),
				}
			}
			for start in window.merge_partners() {
				window.load(start, &self.data[&start]).unwrap();
			}
			let (writes, blocks) = window.finish();
			for (start, block) in writes {
				match block {
					Some(block) => self.data.insert(start, block),
					None => self.data.remove(&start),
				};
			}
			self.blocks = blocks;
		}

		/// Checks the stored blocks against `expected` and the block invariants.
		fn check(&self, expected: &BTreeMap<DocId, Entry>) {
			let postings = TermPostings::from_blocks(self.data.values()).unwrap();
			let held: BTreeMap<_, _> =
				postings.entries.iter().map(|(doc_id, f, l)| (*doc_id, (*f, *l))).collect();
			assert_eq!(&held, expected, "the postings the blocks hold");
			let Some(blocks) = &self.blocks else {
				assert!(self.data.is_empty(), "blocks without a block list");
				assert!(expected.is_empty(), "no block list for a term with documents");
				return;
			};
			let starts: Vec<_> = blocks.blocks.iter().map(|summary| summary.start).collect();
			assert_eq!(starts, self.data.keys().copied().collect::<Vec<_>>(), "listed blocks");
			assert_eq!(starts.first(), Some(&0), "the first block starts at zero");
			for (at, summary) in blocks.blocks.iter().enumerate() {
				let block = &self.data[&summary.start];
				assert!(!block.is_empty(), "an empty block is kept");
				assert!(block.len() <= SPLIT_ABOVE as u64, "a block over the split size");
				let end = blocks.blocks.get(at + 1).map_or(DocId::MAX, |next| next.start);
				for (doc_id, f, l) in block.entries().unwrap() {
					assert!(doc_id >= summary.start && doc_id < end, "{doc_id} outside its block");
					assert!(f <= summary.max_frequency && l >= summary.min_length, "a loose bound");
				}
				assert_eq!(summary.count, block.len(), "the listed count");
			}
		}
	}

	fn entry(doc_id: DocId) -> Entry {
		(1 + doc_id % 7, 10 + doc_id % 13)
	}

	#[test]
	fn appended_documents_split_into_blocks() {
		let mut stored = Stored::default();
		let mut expected = BTreeMap::new();
		for round in 0..12u64 {
			let edits: Vec<_> = (round * 200..(round + 1) * 200)
				.map(|doc_id| (doc_id, Some(entry(doc_id))))
				.collect();
			expected.extend(edits.iter().map(|(doc_id, e)| (*doc_id, e.unwrap())));
			stored.round(&edits);
			stored.check(&expected);
		}
		assert!(stored.data.len() > 4, "2400 documents kept in {} blocks", stored.data.len());
	}

	#[test]
	fn removals_merge_blocks_and_the_last_removal_drops_the_term() {
		let mut stored = Stored::default();
		let mut expected: BTreeMap<_, _> =
			(0..2000).map(|doc_id| (doc_id, entry(doc_id))).collect();
		stored.round(&expected.iter().map(|(doc_id, e)| (*doc_id, Some(*e))).collect::<Vec<_>>());
		stored.check(&expected);
		// Thin the term out from both ends and the middle, round by round.
		for step in 0..20u64 {
			let edits: Vec<_> = expected
				.keys()
				.copied()
				.filter(|doc_id| (doc_id + step) % 3 == 0 || *doc_id < step * 60)
				.take(150)
				.map(|doc_id| (doc_id, None))
				.collect();
			for (doc_id, _) in &edits {
				expected.remove(doc_id);
			}
			stored.round(&edits);
			stored.check(&expected);
		}
		let rest: Vec<_> = expected.keys().map(|doc_id| (*doc_id, None)).collect();
		expected.clear();
		stored.round(&rest);
		stored.check(&expected);
		assert!(stored.blocks.is_none(), "a term with no document keeps a block list");
	}

	#[test]
	fn a_document_below_every_block_falls_in_the_first() {
		let mut stored = Stored::default();
		let mut expected: BTreeMap<_, _> =
			(1000..2200).map(|doc_id| (doc_id, entry(doc_id))).collect();
		stored.round(&expected.iter().map(|(doc_id, e)| (*doc_id, Some(*e))).collect::<Vec<_>>());
		for doc_id in [3, 999, 1500] {
			expected.insert(doc_id, (9, 9));
		}
		stored.round(&[(3, Some((9, 9))), (999, Some((9, 9))), (1500, Some((9, 9)))]);
		stored.check(&expected);
	}

	/// A term of three blocks, starting at 0, 300 and 600, each holding a
	/// hundred documents from its start.
	fn three_blocks() -> (Stored, BTreeMap<DocId, Entry>) {
		let mut stored = Stored::default();
		let mut expected = BTreeMap::new();
		let mut blocks = TermBlocks::default();
		for start in [0, 300, 600] {
			let entries: Vec<_> = (start..start + 100).map(|d| (d, entry(d))).collect();
			expected.extend(entries.iter().copied());
			let block = PostingBlock::from_entries(entries.iter().map(|(d, (f, l))| (*d, *f, *l)));
			blocks.blocks.push(summarise(start, &entries.into_iter().collect()));
			stored.data.insert(start, block);
		}
		stored.blocks = Some(blocks);
		stored.check(&expected);
		(stored, expected)
	}

	#[test]
	fn emptying_two_neighbouring_blocks_in_one_round_keeps_no_empty_block() {
		for emptied in [[0, 300], [300, 600], [0, 600]] {
			let (mut stored, mut expected) = three_blocks();
			let edits: Vec<_> = emptied
				.iter()
				.flat_map(|start| *start..*start + 100)
				.map(|doc_id| (doc_id, None))
				.collect();
			for (doc_id, _) in &edits {
				expected.remove(doc_id);
			}
			stored.round(&edits);
			stored.check(&expected);
		}
	}

	#[test]
	fn the_postings_answer_each_document_once() {
		let blocks = [
			PostingBlock::from_entries([(0, 1, 5), (7, 2, 6)]),
			PostingBlock::from_entries([(300, 3, 7)]),
		];
		let postings = TermPostings::from_blocks(&blocks).unwrap();
		assert_eq!(postings.docs().iter().collect::<Vec<_>>(), [0, 7, 300]);
		assert_eq!(postings.entry(7), Some((2, 6)));
		assert_eq!(postings.entry(8), None);
	}

	/// A cursor finds every entry [`TermPostings::entry`] finds, whether the doc
	/// ids it is asked for ascend, skip ahead, repeat or go back.
	#[test]
	fn a_cursor_finds_what_a_search_finds() {
		let entries: Vec<_> =
			(0..3000u64).filter(|d| d % 3 != 1).map(|d| (d * 2, d + 1, 9)).collect();
		let postings = TermPostings::from_blocks(&[
			PostingBlock::from_entries(entries[..700].iter().copied()),
			PostingBlock::from_entries(entries[700..].iter().copied()),
		])
		.unwrap();
		let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
		let mut next = |bound: u64| {
			seed ^= seed << 13;
			seed ^= seed >> 7;
			seed ^= seed << 17;
			seed % bound
		};
		for _ in 0..50 {
			let mut at = 0;
			let mut doc_id = 0;
			for _ in 0..400 {
				doc_id = match next(10) {
					0 => next(7000),
					1 => doc_id,
					2 => doc_id + next(2000),
					_ => doc_id + next(4),
				};
				assert_eq!(postings.entry_at(&mut at, doc_id), postings.entry(doc_id), "{doc_id}");
			}
		}
		let empty = TermPostings::default();
		assert_eq!(empty.entry_at(&mut 0, 5), None);
	}

	#[test]
	fn impacts_keep_the_pairs_no_other_dominates() {
		let held =
			impacts([(1, 10), (3, 40), (3, 20), (2, 5), (0, 1), (1, 30), (2, 25)].into_iter());
		assert_eq!(held, [(3, 20), (2, 5)]);
		for (f, l) in [(1, 10), (3, 40), (3, 20), (2, 5), (1, 30), (2, 25)] {
			assert!(
				held.iter().any(|(hf, hl)| *hf >= f && *hl <= l),
				"({f}, {l}) is not dominated"
			);
		}
	}

	#[test]
	fn a_placeholder_entry_raises_no_bound() {
		let block: Block = [(1, (0, 1)), (2, (4, 50))].into_iter().collect();
		let summary = summarise(0, &block);
		assert_eq!((summary.max_frequency, summary.min_length, summary.count), (4, 50, 2));
	}
}
