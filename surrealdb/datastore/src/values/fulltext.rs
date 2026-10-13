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
use surrealdb_collections::VecMap;
use surrealdb_kvs::impl_kv_value_revisioned;
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
	/// Per-document totals, excluding the one the bitmap already carries.
	/// Never zero: a document whose total the bitmap holds exactly is absent.
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
