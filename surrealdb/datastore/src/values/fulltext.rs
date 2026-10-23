//! What a full-text index persists.
//!
//! A full-text index stores, for each document, a posting per term it carries —
//! how often the term occurs and where — plus the corpus statistics BM25 needs to
//! weigh those postings against the collection as a whole. Both are keyspace
//! values, so their shapes are declared here.
//!
//! The analyzer that produces the offsets, the indexer that maintains the counts
//! and the scorer that reads them all stay above: they need the document, the
//! transaction and the analyzer definitions.

use revision::revisioned;
use roaring::RoaringTreemap;
use surrealdb_collections::VecMap;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_kvs::value::KVValue;
use surrealdb_strand::Strand;

use crate::values::ids::DocId;

/// A character position within an analysed document.
pub type Position = u32;

/// The number of terms in one document, as counted by the index's analyzer.
pub type DocLength = u64;

/// How many times a term occurs in one document.
pub type TermFrequency = u64;

/// Where one occurrence of a term sits in the document it was found in.
///
/// Both the original and the generated span are kept: an analyzer may rewrite a
/// term (stemming, ascii-folding), and a highlighter has to point at the text the
/// user actually wrote, not at what the term became.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub struct Offset {
	pub index: u32,
	// Start position of the original term
	pub start: Position,
	// Start position of the generated term
	pub gen_start: Position,
	// End position of the original term
	pub end: Position,
}

impl Offset {
	pub fn new(index: u32, start: Position, gen_start: Position, end: Position) -> Self {
		Self {
			index,
			start,
			gen_start,
			end,
		}
	}
}

/// Represents a term occurrence within a document
#[revisioned(revision = 1)]
#[derive(Debug, Default, PartialEq)]
pub struct TermDocument {
	/// The frequency of the term in the document
	pub f: TermFrequency,
	/// The offsets of the term occurrences in the document
	pub o: Vec<Offset>,
}

impl_kv_value_revisioned!(TermDocument);

/// Every term one document carries, with that document's posting for each.
///
/// Stored under a single key per document. A term absent from the map does not
/// occur in the document, which is the same answer a missing per-term posting
/// gave.
#[revisioned(revision = 1)]
#[derive(Debug, Default, PartialEq)]
pub struct DocumentTerms {
	/// Postings by term.
	///
	/// A `VecMap` rather than a `BTreeMap`: the map is built once when a
	/// document is indexed, never mutated afterwards, and then read to take a
	/// handful of terms' postings from it. So it wants one allocation and a
	/// binary search, not a node per term — a document carrying hundreds of
	/// terms would otherwise allocate that many nodes on every read that scores
	/// it. Ordering also makes the encoding deterministic for a given set of
	/// terms, which keeps a re-index of unchanged content from rewriting a
	/// different value.
	///
	/// Keyed by `Strand` rather than `String` because a term almost always fits
	/// `Strand`'s inline capacity, so the keys of a document carrying hundreds of
	/// terms cost no allocations at all rather than one each.
	///
	/// The wire format is unaffected by both choices — `VecMap` encodes
	/// length-prefixed entries as `BTreeMap` does, and `Strand` encodes as `str`
	/// does — so this decides representation only, not what is stored.
	pub terms: VecMap<Strand, TermDocument>,
}

impl_kv_value_revisioned!(DocumentTerms);

/// What a term's compacted document set could not hold.
///
/// Uncompacted changes to a term are signed counts — one per addition, minus one
/// per removal — and a document belongs to the term when its compacted
/// membership plus those counts comes to one or more. The compacted side is a
/// membership bitmap, so it can only carry a total of zero or one. A total
/// outside that range is kept here instead of being clamped into the bitmap and
/// lost.
///
/// That is what makes a term's compacted set independent of how its changes were
/// grouped: each round preserves `membership + residual + remaining counts` per
/// document, and that sum is what every reader resolves.
///
/// A positive count belongs to a document the bitmap holds and a negative one to
/// a document it does not, so a residual written beside its own bitmap decides
/// nothing on its own. A reader still has to fold it: a compactor that does not
/// know this family can move the bitmap without clearing the residual, and the
/// total is then the only thing that says where the document belongs.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TermDocsResidual {
	/// Per-document totals, excluding the one the bitmap already carries. Zero
	/// only on an index that keeps its postings in blocks, for a document whose
	/// changes the round that stopped inside the term folded to nothing: the
	/// next round reads such a document's postings rather than taking an older
	/// change's. Otherwise a document whose total the bitmap holds exactly is
	/// absent.
	pub counts: VecMap<DocId, i64>,
}

impl TermDocsResidual {
	/// Builds a residual from per-document totals in any order.
	pub fn new(counts: impl IntoIterator<Item = (DocId, i64)>) -> Self {
		Self {
			counts: counts.into_iter().collect(),
		}
	}
}

impl_kv_value_revisioned!(TermDocsResidual);

/// Tracks document length and count statistics for the index
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DocLengthAndCount {
	/// The total length of all documents in the index
	pub total_docs_length: i128,
	/// The total number of documents in the index
	pub doc_count: i64,
}

impl_kv_value_revisioned!(DocLengthAndCount);

/// One block of a term's compacted postings: the documents carrying the term
/// whose ids fall in the block's range, in ascending order, each with how often
/// it carries the term and its length.
///
/// The range starts at the block's start and ends at the next block's start
/// ([`TermBlocks`]). Each of the three columns is a run of unsigned LEB128
/// varints, one per document: the documents as gaps (the first is the
/// document's id, each later one the distance from the document before it),
/// their frequencies and their lengths. A block decodes without its key.
///
/// An entry's frequency and length are the document's `!dt` and `!dl` as the
/// compaction that placed it read them. A document rewritten since holds an
/// uncompacted change for every term it carries before and after the rewrite,
/// and a reader takes such a document's frequency and length from `!dt` and
/// `!dl` rather than from here.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostingBlock {
	/// How many documents the block holds.
	count: u64,
	/// The documents, as gaps.
	gaps: Vec<u8>,
	/// Each document's frequency of the term.
	frequencies: Vec<u8>,
	/// Each document's length.
	lengths: Vec<u8>,
}

/// Appends `value` to `out` as an unsigned LEB128 varint.
fn put_varint(out: &mut Vec<u8>, mut value: u64) {
	loop {
		let byte = (value & 0x7f) as u8;
		value >>= 7;
		if value == 0 {
			out.push(byte);
			return;
		}
		out.push(byte | 0x80);
	}
}

/// The unsigned LEB128 varint at `*at` in `bytes`, advancing `*at` past it, or
/// `None` where the bytes end inside it or it does not fit a `u64`.
fn take_varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
	let mut value = 0u64;
	for shift in (0..64).step_by(7) {
		let byte = *bytes.get(*at)?;
		*at += 1;
		// The tenth byte holds the 64th bit alone; anything above it overflows.
		if shift == 63 && byte & 0x7f > 1 {
			return None;
		}
		value |= u64::from(byte & 0x7f) << shift;
		if byte & 0x80 == 0 {
			return Some(value);
		}
	}
	None
}

impl PostingBlock {
	/// A block of `entries`, `(document, frequency, length)` in ascending
	/// document order.
	pub fn from_entries(
		entries: impl IntoIterator<Item = (DocId, TermFrequency, DocLength)>,
	) -> Self {
		let mut block = Self::default();
		let mut previous = 0;
		for (doc_id, frequency, length) in entries {
			let gap = if block.count == 0 {
				doc_id
			} else {
				doc_id - previous
			};
			put_varint(&mut block.gaps, gap);
			put_varint(&mut block.frequencies, frequency);
			put_varint(&mut block.lengths, length);
			previous = doc_id;
			block.count += 1;
		}
		block
	}

	/// The entries, `(document, frequency, length)` in ascending document
	/// order, or `None` where the columns do not hold the block's count of
	/// varints each, exactly.
	pub fn entries(&self) -> Option<Vec<(DocId, TermFrequency, DocLength)>> {
		let mut entries = Vec::with_capacity(usize::try_from(self.count).ok()?);
		let (mut g, mut f, mut l) = (0, 0, 0);
		let mut doc_id: DocId = 0;
		for _ in 0..self.count {
			doc_id = doc_id.checked_add(take_varint(&self.gaps, &mut g)?)?;
			let frequency = take_varint(&self.frequencies, &mut f)?;
			let length = take_varint(&self.lengths, &mut l)?;
			entries.push((doc_id, frequency, length));
		}
		let whole = g == self.gaps.len() && f == self.frequencies.len() && l == self.lengths.len();
		whole.then_some(entries)
	}

	/// How many documents the block holds.
	pub fn len(&self) -> u64 {
		self.count
	}

	/// Whether the block holds no document.
	pub fn is_empty(&self) -> bool {
		self.count == 0
	}
}

impl_kv_value_revisioned!(PostingBlock);

/// The frequency of a term in, and the length of, each document one transaction
/// added to the term, carried by the `!tx` addition that names those documents
/// ([`ChangeBatch`]).
///
/// The documents are not repeated: the postings are in ascending document order,
/// the order the addition's bitmap iterates in, and pair with its documents one
/// for one. Each posting is two unsigned LEB128 varints, the frequency then the
/// length. The encoding is versioned by its first byte.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangePostings {
	/// How many postings the value holds.
	count: u64,
	/// The postings, each a frequency and a length.
	columns: Vec<u8>,
}

/// The only encoding of [`ChangePostings`] there is.
const CHANGE_POSTINGS_V1: u8 = 1;

impl ChangePostings {
	/// The postings of the documents of an addition, in ascending document
	/// order.
	pub fn from_postings(postings: impl IntoIterator<Item = (TermFrequency, DocLength)>) -> Self {
		let mut value = Self::default();
		for (frequency, length) in postings {
			put_varint(&mut value.columns, frequency);
			put_varint(&mut value.columns, length);
			value.count += 1;
		}
		value
	}

	/// The postings, in ascending document order, or `None` where the bytes do
	/// not hold the value's count of them, exactly.
	pub fn postings(&self) -> Option<Vec<(TermFrequency, DocLength)>> {
		let mut postings = Vec::with_capacity(usize::try_from(self.count).ok()?);
		let mut at = 0;
		for _ in 0..self.count {
			let frequency = take_varint(&self.columns, &mut at)?;
			let length = take_varint(&self.columns, &mut at)?;
			postings.push((frequency, length));
		}
		(at == self.columns.len()).then_some(postings)
	}

	/// How many postings the value holds.
	pub fn len(&self) -> u64 {
		self.count
	}

	/// Whether the value holds no posting.
	pub fn is_empty(&self) -> bool {
		self.count == 0
	}

	/// At most how many bytes [`Self::encode_into`] appends: the encoding
	/// byte, the count's varint and the postings.
	fn encoded_len(&self) -> usize {
		1 + 10 + self.columns.len()
	}

	/// Appends the value's encoding to `bytes`.
	fn encode_into(&self, bytes: &mut Vec<u8>) {
		bytes.push(CHANGE_POSTINGS_V1);
		put_varint(bytes, self.count);
		bytes.extend_from_slice(&self.columns);
	}
}

impl KVValue for ChangePostings {
	type KeyContext = ();

	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut bytes = Vec::with_capacity(self.encoded_len());
		self.encode_into(&mut bytes);
		Ok(bytes)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		match bytes.first() {
			Some(&CHANGE_POSTINGS_V1) => {
				let mut at = 1;
				let count = take_varint(bytes, &mut at)
					.ok_or_else(|| anyhow::anyhow!("a change's postings end inside their count"))?;
				Ok(Self {
					count,
					columns: bytes[at..].to_vec(),
				})
			}
			Some(version) => anyhow::bail!("change postings in unknown encoding {version}"),
			None => anyhow::bail!("change postings with no bytes"),
		}
	}
}

/// One transaction's change to a term in one direction (`!tx`): the documents it
/// added to or removed from the term, and, for an addition to an index that keeps
/// its postings in blocks, each document's posting.
///
/// The value is the documents' bitmap in its own serialization, followed by the
/// postings ([`ChangePostings`]) where there are any. A bitmap with nothing after
/// it carries no postings: every change of an index without posting blocks,
/// every removal, and an addition none of whose documents holds a posting.
/// Compaction reads each document of a change without postings from what it
/// stores, and so it does for postings that do not decode, which the value
/// reads as none rather than failing every reader of the term.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChangeBatch {
	/// The documents the change adds or removes.
	pub docs: RoaringTreemap,
	/// Each added document's posting, in the order `docs` iterates in.
	pub postings: Option<ChangePostings>,
}

impl KVValue for ChangeBatch {
	type KeyContext = ();

	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut bytes = Vec::with_capacity(
			self.docs.serialized_size()
				+ self.postings.as_ref().map_or(0, ChangePostings::encoded_len),
		);
		self.docs.serialize_into(&mut bytes)?;
		if let Some(postings) = &self.postings {
			postings.encode_into(&mut bytes);
		}
		Ok(bytes)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		let mut rest = bytes;
		// The bitmap reads exactly its own serialization, leaving the postings.
		let docs = RoaringTreemap::deserialize_from(&mut rest)?;
		let postings = if rest.is_empty() {
			None
		} else {
			match ChangePostings::kv_decode_value(rest, ()) {
				Ok(postings) => Some(postings),
				Err(error) => {
					tracing::warn!(
						target: "surrealdb::datastore::fulltext",
						"A full-text change carries postings that do not decode ({error}); its \
						 documents are read from what they store"
					);
					None
				}
			}
		};
		Ok(Self {
			docs,
			postings,
		})
	}
}

/// What one posting block of a term holds, as its term's block list
/// ([`TermBlocks`]) records it.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockSummary {
	/// The lowest document id the block covers, which is its key.
	pub start: DocId,
	/// How many documents it holds.
	pub count: u64,
	/// The highest frequency of the term among them.
	pub max_frequency: TermFrequency,
	/// The shortest length among them.
	pub min_length: DocLength,
}

/// A term's posting blocks, in ascending order of start.
///
/// The first block starts at zero, so every document id falls in exactly one
/// block: the last whose start is at or below it. A term no document carries
/// once compacted has no block list.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TermBlocks {
	/// One summary per block.
	pub blocks: Vec<BlockSummary>,
}

impl_kv_value_revisioned!(TermBlocks);

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_posting_block_holds_its_entries_in_varints() {
		let entries =
			vec![(0, 1, 0), (127, 128, 16_383), (16_511, 3, 16_384), (u64::MAX, u64::MAX, 7)];
		let block = PostingBlock::from_entries(entries.clone());
		assert_eq!(block.entries(), Some(entries));
		assert_eq!(block.len(), 4);
		// A dense block of short documents takes a few bytes per entry.
		let dense = PostingBlock::from_entries((1000..1256).map(|d| (d, 1 + d % 5, 20 + d % 90)));
		let bytes = dense.gaps.len() + dense.frequencies.len() + dense.lengths.len();
		assert!(bytes <= 256 * 4, "{bytes} bytes for 256 entries");
	}

	#[test]
	fn change_postings_hold_their_postings_in_varints() {
		let postings = vec![(1, 0), (128, 16_383), (3, 16_384), (u64::MAX, 7)];
		let value = ChangePostings::from_postings(postings.clone());
		assert_eq!(value.postings(), Some(postings.clone()));
		let bytes = value.kv_encode_value().unwrap();
		let decoded = ChangePostings::kv_decode_value(&bytes, ()).unwrap();
		assert_eq!(decoded, value);
		assert_eq!(decoded.postings(), Some(postings));
		// Each posting of a short document is two or three bytes.
		let dense = ChangePostings::from_postings((0..256u64).map(|d| (1 + d % 5, 20 + d % 90)));
		assert!(dense.columns.len() <= 256 * 3, "{} bytes", dense.columns.len());
	}

	#[test]
	fn change_postings_that_disagree_with_their_count_do_not_decode() {
		let mut value = ChangePostings::from_postings([(1, 12), (2, 7)]);
		value.count = 3;
		assert_eq!(value.postings(), None, "the bytes end before the count");
		let mut value = ChangePostings::from_postings([(1, 12)]);
		value.columns.push(1);
		assert_eq!(value.postings(), None, "the bytes hold more than the count");
		assert!(ChangePostings::kv_decode_value(&[], ()).is_err(), "no bytes");
		assert!(ChangePostings::kv_decode_value(&[2, 0], ()).is_err(), "an unknown encoding");
	}

	#[test]
	fn a_change_batch_carries_its_postings_after_its_documents() {
		let docs: RoaringTreemap = [3, 70_000, u64::from(u32::MAX) + 5].into_iter().collect();
		let bare = ChangeBatch {
			docs: docs.clone(),
			postings: None,
		};
		let bytes = bare.kv_encode_value().unwrap();
		// A change with no postings is the bitmap alone, and each reads as the
		// other.
		let mut bitmap = Vec::new();
		docs.serialize_into(&mut bitmap).unwrap();
		assert_eq!(bytes, bitmap);
		assert_eq!(ChangeBatch::kv_decode_value(&bitmap, ()).unwrap(), bare);
		let carried = ChangeBatch {
			docs,
			postings: Some(ChangePostings::from_postings([(1, 12), (2, 7), (5, 300)])),
		};
		let bytes = carried.kv_encode_value().unwrap();
		assert_eq!(ChangeBatch::kv_decode_value(&bytes, ()).unwrap(), carried);
		// Postings that do not decode read as none, with the documents intact.
		for trailer in [&[2u8, 0][..], &[0], &[1]] {
			assert_eq!(
				ChangeBatch::kv_decode_value(&[&bitmap[..], trailer].concat(), ()).unwrap(),
				bare,
				"trailer {trailer:?}"
			);
		}
	}

	#[test]
	fn a_posting_block_whose_columns_disagree_does_not_decode() {
		let mut block = PostingBlock::from_entries([(5, 1, 12), (300, 2, 7)]);
		block.count = 3;
		assert_eq!(block.entries(), None, "a column ends before the count");
		let mut block = PostingBlock::from_entries([(5, 1, 12), (300, 2, 7)]);
		block.lengths.push(1);
		assert_eq!(block.entries(), None, "a column holds more than the count");
		let mut block = PostingBlock::from_entries([(5, 1, 12)]);
		block.gaps = vec![0x80];
		assert_eq!(block.entries(), None, "a varint the bytes end inside");
		let mut block = PostingBlock::from_entries([(5, 1, 12)]);
		block.gaps = [vec![0x80; 9], vec![0x02]].concat();
		assert_eq!(block.entries(), None, "a varint past u64::MAX");
		block.gaps = [vec![0xff; 9], vec![0x01]].concat();
		assert_eq!(block.entries(), Some(vec![(u64::MAX, 1, 12)]), "u64::MAX itself");
	}
}
