//! What a full-text index persists.
//!
//! A full-text index stores one posting per (term, document) pair — how often the
//! term occurs and where — plus the corpus statistics BM25 needs to weigh those
//! postings against the collection as a whole. Both are keyspace values, so their
//! shapes are declared here.
//!
//! The analyzer that produces the offsets, the indexer that maintains the counts
//! and the scorer that reads them all stay above: they need the document, the
//! transaction and the analyzer definitions.

use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;

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
