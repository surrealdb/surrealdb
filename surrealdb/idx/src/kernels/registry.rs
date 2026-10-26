//! The kernels by name.
//!
//! A [`Kernel`] is the stable name of one kernel. A name is what a program or a
//! capability manifest records; [`Kernel::named`] resolves it, and a name that
//! no kernel answers to is refused rather than guessed at. Names are dotted,
//! the first segment naming the index kind the kernel belongs to.

macro_rules! kernels {
	($($(#[$doc:meta])* $variant:ident = $name:literal;)*) => {
		/// A kernel, by its stable name.
		#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
		#[non_exhaustive]
		pub enum Kernel {
			$($(#[$doc])* $variant,)*
		}

		impl Kernel {
			/// Every kernel, in declaration order.
			pub const ALL: &'static [Kernel] = &[$(Kernel::$variant,)*];

			/// The kernel's stable name.
			pub const fn name(self) -> &'static str {
				match self {
					$(Kernel::$variant => $name,)*
				}
			}

			/// The kernel named `name`, if there is one.
			pub fn named(name: &str) -> Option<Kernel> {
				match name {
					$($name => Some(Kernel::$variant),)*
					_ => None,
				}
			}

		}
	};
}

kernels! {
	/// The key a B-tree or `UNIQUE` index entry is stored at, from the
	/// entry's values and its record's id.
	BTreeEntryKey = "btree.entry_key";
	/// The key range a B-tree or `UNIQUE` index scan reads for an equality,
	/// a range or a compound prefix.
	BTreeBounds = "btree.bounds";
	/// The record id, and the doc-id when it carries one, an index entry's
	/// stored value names.
	BTreeDecodeEntry = "btree.decode_entry";
	/// The key a doc-ID's reverse mapping is stored at, in its table.
	DocIdRecordKey = "docids.record_key";
	/// The record key a reverse mapping's stored bytes name.
	DocIdDecodeRecordKey = "docids.decode_record_key";
	/// The keys and ranges a query reads of one term.
	FullTextTermReads = "fulltext.term_reads";
	/// A term's signed per-document changes from its change keys and batches.
	FullTextTermDeltas = "fulltext.term_deltas";
	/// A term's residual added to its per-document changes.
	FullTextAddResidual = "fulltext.add_residual";
	/// The changes the reading transaction buffered to a term added to its
	/// per-document changes.
	FullTextAddPending = "fulltext.add_pending";
	/// Whether a document carries a term, from its compacted membership and
	/// its total change.
	FullTextCarries = "fulltext.carries";
	/// A term's document set from its bitmap, residual and changes.
	FullTextTermDocs = "fulltext.term_docs";
	/// The documents matching a query from its terms' document sets under a
	/// boolean operator.
	FullTextCombine = "fulltext.combine";
	/// A term's compacted documents, with each one's frequency and length,
	/// from its posting blocks.
	FullTextPostings = "fulltext.postings";
	/// The posting-block writes one compaction round's edits to a term come to.
	FullTextBlockEdit = "fulltext.block_edit";
	/// The BM25 score of one term in one document.
	FullTextBm25 = "fulltext.bm25";
	/// A document's field with the occurrences of a query's terms wrapped.
	FullTextHighlight = "fulltext.highlight";
	/// The positions of the occurrences of a query's terms in a document.
	FullTextOffsets = "fulltext.offsets";
	/// A query vector in an index's vector type.
	VectorQuery = "vector.query";
	/// The distance of a record's raw vector from a query vector.
	VectorScore = "vector.score";
	/// The distance between two vectors of numbers.
	VectorDistance = "vector.distance";
}

impl std::fmt::Display for Kernel {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(self.name())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn every_kernel_resolves_by_its_own_name() {
		for kernel in Kernel::ALL {
			assert_eq!(Kernel::named(kernel.name()), Some(*kernel), "{kernel}");
		}
	}

	#[test]
	fn names_are_unique_and_dotted() {
		let mut names: Vec<_> = Kernel::ALL.iter().map(|k| k.name()).collect();
		assert!(names.iter().all(|n| n.contains('.')), "{names:?}");
		names.sort_unstable();
		let before = names.len();
		names.dedup();
		assert_eq!(names.len(), before, "a name is claimed twice");
	}

	#[test]
	fn an_unknown_name_is_refused() {
		assert_eq!(Kernel::named("btree.nope"), None);
		assert_eq!(Kernel::named(""), None);
	}
}
