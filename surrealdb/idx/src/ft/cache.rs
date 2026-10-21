//! The process-wide cache of full-text indexes' compacted term state.
//!
//! A query reads, for each of its terms, the term's compacted documents (its
//! `!td` bitmap, or its `!tp` posting blocks on an index that keeps them) and
//! its residual (`!tr`), then folds the term's uncompacted deltas on top. The
//! compacted pair is written only by term-docs compaction, which
//! advances the index's `!tv` generation in the same transaction, and by an
//! index wipe, which advances it too. So every snapshot that reads one `!tv`
//! generation of one index reads the same pair for a term, and the pair can be
//! served from memory to every such snapshot on the node. The deltas, and the
//! changes the reading transaction buffered, are always read afresh.
//!
//! # What an entry is keyed by
//!
//! [`TermScope`] pins the state one snapshot reads:
//!
//! - the namespace, database, table and index ids;
//! - the table's catalog version, which every catalog write to the table, its fields or its indexes
//!   stamps afresh. Index ids are reused once released, and an index removed and defined again
//!   restarts its `!tv` generation, so the ids and the generation alone could name an earlier
//!   incarnation's state;
//! - the `!tv` generation.
//!
//! A scope exists only where every part of it is known: a table no catalog
//! write has stamped, and an index whose terms no compaction has folded yet
//! (which holds no compacted state to serve), are read uncached.
//!
//! # Who may read and fill it
//!
//! Only a read-only transaction. A write transaction may hold a compaction of
//! its own that has advanced `!tv` and rewritten the pair without committing:
//! if it rolled back, another commit could take the same generation for a
//! different pair. A read-only transaction reads only committed state, which
//! no rollback can take back.
//!
//! The cache is per node and holds no state another node must invalidate:
//! another node's compaction or wipe changes the generation a snapshot reads,
//! and another node's DDL changes the catalog version.
//!
//! # Documents
//!
//! The cache also holds, per document, its length (`!dl`) and its postings
//! (`!dt`), which scoring reads for every hit. Those are rewritten by the
//! write that re-indexes the document, not by compaction, so an entry under
//! one generation is served only for a document the reading query's terms
//! hold no uncompacted state for ([`crate::ft::fulltext::QueryTerms::uncompacted`]).
//! That is sound because a write that re-indexes a document records a change
//! for every term the document carries before and after it, and that change
//! stays uncompacted until a compaction advances the generation: a document
//! a query scores carries one of its terms, so a rewrite of it since the
//! generation began shows in the query's own uncompacted state. A document no
//! write has touched since then reads the same in every snapshot of the
//! generation. A change to how a re-index records its term changes must keep
//! that per-document signal, or this cache must be invalidated another way;
//! `a_rewritten_document_is_scored_as_stored` in the core full-text tests
//! fails otherwise.

use std::sync::Arc;

use anyhow::Result;
use quick_cache::sync::Cache;
use quick_cache::{OptionsBuilder, Weighter};
use surrealdb_datastore::Transaction;
use uuid::Uuid;

use crate::IndexKeyBase;
use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::docids::DocId;
use crate::ft::DocLength;
use crate::ft::fulltext::{Compacted, DocumentTerms, TermDocsResidual, TermDocument};
use crate::ft::offset::Offset;
use crate::val::TableName;

/// The state of one index one read-only snapshot reads, which a cached term
/// is served to.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TermScope {
	ns: NamespaceId,
	db: DatabaseId,
	table: TableName,
	ix: IndexId,
	/// The table's catalog version as the snapshot reads it.
	catalog: Uuid,
	/// The index's `!tv` generation as the snapshot reads it.
	generation: u64,
}

impl TermScope {
	/// The scope `tx` reads index `ikb` at, or `None` where its state is read
	/// uncached: `tx` may write, the table carries no catalog version, or no
	/// compaction has folded the index's terms yet.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::idx::fulltext", skip_all)
	)]
	pub async fn of(tx: &Transaction, ikb: &IndexKeyBase) -> Result<Option<Self>> {
		if tx.writeable() {
			return Ok(None);
		}
		let Some(catalog) = tx.catalog_version(ikb.ns(), ikb.db(), ikb.table()).await? else {
			return Ok(None);
		};
		let Some(generation) = crate::read_compaction_generation(tx, &ikb.new_tv_key()).await?
		else {
			return Ok(None);
		};
		Ok(Some(Self {
			ns: ikb.ns(),
			db: ikb.db(),
			table: ikb.table().clone(),
			ix: ikb.index(),
			catalog,
			generation,
		}))
	}
}

/// One term's compacted state: its documents and its residual, either absent
/// where the index holds none.
pub struct CompactedTerm {
	pub compacted: Compacted,
	pub residual: Option<TermDocsResidual>,
}

/// One document's stored state as scoring reads it: its length and its
/// postings, either absent where the index holds none.
pub struct StoredDocument {
	pub length: Option<DocLength>,
	pub terms: Option<DocumentTerms>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Key {
	Term(TermScope, String),
	Document(TermScope, DocId),
}

#[derive(Clone)]
enum Entry {
	Term(Arc<CompactedTerm>),
	Document(Arc<StoredDocument>),
}

/// An entry and what it holds, measured once when it is cached.
#[derive(Clone)]
struct Weighed {
	entry: Entry,
	weight: u64,
}

#[derive(Clone)]
struct EntryWeighter;

impl Weighter<Key, Weighed> for EntryWeighter {
	fn weight(&self, key: &Key, value: &Weighed) -> u64 {
		// The fixed key and entry fields, what the key names and the measured state.
		let named = match key {
			Key::Term(scope, term) => scope.table.as_str().len() + term.len(),
			Key::Document(scope, _) => scope.table.as_str().len(),
		};
		(std::mem::size_of::<Key>() + std::mem::size_of::<Weighed>() + 32 + named) as u64
			+ value.weight
	}
}

/// The least a shard of the cache holds.
///
/// The cache is split in shards, and refuses an entry heavier than its shard
/// can hold. Split as `quick_cache` splits it by default, four shards a core,
/// a node with many cores would hold a few hundred KiB a shard and refuse the
/// postings of every common term, which are the entries worth most. So the
/// cache keeps fewer shards, each at least this large.
const MIN_SHARD_WEIGHT: u64 = 8 * 1024 * 1024;

/// The process-wide cache of full-text indexes' compacted terms and stored
/// documents, bounded by the bytes its entries hold. A size of zero caches
/// nothing.
pub struct FullTextCache {
	entries: Option<Cache<Key, Weighed, EntryWeighter>>,
}

impl FullTextCache {
	/// A cache bounded to roughly `size` bytes; `0` disables it.
	pub fn new(size: u64) -> Self {
		let entries = (size > 0).then(|| {
			let items = (size / 256).clamp(16, u64::MAX) as usize;
			let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
			let shards =
				usize::try_from(size / MIN_SHARD_WEIGHT).unwrap_or(usize::MAX).clamp(1, cores * 4);
			// `quick_cache` rounds a shard count up to a power of two, which would
			// leave a shard below the minimum, so it is rounded down here.
			let shards = 1 << shards.ilog2();
			match OptionsBuilder::new()
				.estimated_items_capacity(items)
				.weight_capacity(size)
				.shards(shards)
				.build()
			{
				Ok(options) => Cache::with_options(
					options,
					EntryWeighter,
					Default::default(),
					Default::default(),
				),
				Err(_) => Cache::with_weighter(items, size, EntryWeighter),
			}
		});
		Self {
			entries,
		}
	}

	/// Whether the cache holds anything at all, so a caller can skip working
	/// out a scope it would never read.
	pub fn enabled(&self) -> bool {
		self.entries.is_some()
	}

	/// How many documents' stored state the cache holds now.
	pub fn documents(&self) -> usize {
		self.entries.as_ref().map_or(0, |entries| {
			entries.iter().filter(|(key, _)| matches!(key, Key::Document(..))).count()
		})
	}

	/// The compacted state of `term` in `scope`, when cached.
	pub fn term(&self, scope: &TermScope, term: &str) -> Option<Arc<CompactedTerm>> {
		match self.entries.as_ref()?.get(&Key::Term(scope.clone(), term.to_owned()))?.entry {
			Entry::Term(term) => Some(term),
			Entry::Document(_) => None,
		}
	}

	/// Caches the compacted state of `term` in `scope`.
	pub fn insert_term(
		&self,
		scope: &TermScope,
		term: &str,
		compacted: Compacted,
		residual: Option<TermDocsResidual>,
	) {
		let Some(entries) = &self.entries else {
			return;
		};
		let held = match &compacted {
			Compacted::Absent => 0,
			Compacted::Bitmap(bitmap) => bitmap.serialized_size() as u64,
			Compacted::Postings(postings) => postings.weight(),
		};
		let weight = held
			+ residual.as_ref().map_or(0, |residual| {
				(residual.counts.len() * std::mem::size_of::<(u64, i64)>()) as u64
			});
		entries.insert(
			Key::Term(scope.clone(), term.to_owned()),
			Weighed {
				entry: Entry::Term(Arc::new(CompactedTerm {
					compacted,
					residual,
				})),
				weight,
			},
		);
	}

	/// The stored state of document `doc_id` in `scope`, when cached.
	pub fn document(&self, scope: &TermScope, doc_id: DocId) -> Option<Arc<StoredDocument>> {
		match self.entries.as_ref()?.get(&Key::Document(scope.clone(), doc_id))?.entry {
			Entry::Document(document) => Some(document),
			Entry::Term(_) => None,
		}
	}

	/// Caches the stored state of document `doc_id` in `scope`, weighed by
	/// everything its postings hold: every term it carries, and every offset
	/// where the index highlights.
	pub fn insert_document(&self, scope: &TermScope, doc_id: DocId, document: Arc<StoredDocument>) {
		let Some(entries) = &self.entries else {
			return;
		};
		let weight = document.terms.as_ref().map_or(0, |terms| {
			terms
				.terms
				.iter()
				.map(|(term, posting)| {
					(term.as_str().len()
						+ std::mem::size_of::<TermDocument>()
						+ posting.o.len() * std::mem::size_of::<Offset>()) as u64
				})
				.sum()
		});
		entries.insert(
			Key::Document(scope.clone(), doc_id),
			Weighed {
				entry: Entry::Document(document),
				weight,
			},
		);
	}
}

#[cfg(test)]
mod tests {
	use roaring::RoaringTreemap;

	use super::*;

	fn scope(generation: u64, catalog: u128) -> TermScope {
		TermScope {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			table: "t".into(),
			ix: IndexId(3),
			catalog: Uuid::from_u128(catalog),
			generation,
		}
	}

	/// An entry is served to the scope it was cached under, and to no other
	/// generation or catalog version of the index.
	#[test]
	fn an_entry_answers_its_own_scope_only() {
		let cache = FullTextCache::new(1024 * 1024);
		let bitmap: RoaringTreemap = [1u64, 2, 3].into_iter().collect();
		cache.insert_term(&scope(5, 9), "hello", Compacted::Bitmap(bitmap.clone()), None);
		let hit = cache.term(&scope(5, 9), "hello").expect("the entry is cached");
		assert!(matches!(&hit.compacted, Compacted::Bitmap(cached) if *cached == bitmap));
		assert!(cache.term(&scope(6, 9), "hello").is_none(), "another generation");
		assert!(cache.term(&scope(5, 10), "hello").is_none(), "another incarnation");
		assert!(cache.term(&scope(5, 9), "world").is_none(), "another term");
	}

	/// A document's stored state is served to the scope it was cached under
	/// only, and never as a term's.
	#[test]
	fn a_document_answers_its_own_scope_only() {
		let cache = FullTextCache::new(1024 * 1024);
		let document = Arc::new(StoredDocument {
			length: Some(3),
			terms: None,
		});
		cache.insert_document(&scope(5, 9), 42, document);
		assert_eq!(cache.document(&scope(5, 9), 42).and_then(|d| d.length), Some(3));
		assert!(cache.document(&scope(6, 9), 42).is_none(), "another generation");
		assert!(cache.document(&scope(5, 9), 43).is_none(), "another document");
	}

	/// An entry as heavy as the postings of a term most of a large index's
	/// documents carry is kept, whatever the number of cores and whether or not
	/// the size is a power-of-two multiple of the minimum shard.
	#[test]
	fn a_heavy_term_is_kept() {
		let blocks: Vec<_> = (0..1000u64)
			.map(|block| {
				crate::ft::fulltext::PostingBlock::from_entries(
					(0..256u64).map(|at| (block * 256 + at, 1 + at % 5, 10 + at % 30)),
				)
			})
			.collect();
		let postings = crate::kernels::postings::TermPostings::from_blocks(&blocks).unwrap();
		assert!(postings.weight() > 4 * 1024 * 1024, "{} bytes", postings.weight());
		let postings = Arc::new(postings);
		for mib in [128, 24, 40] {
			let cache = FullTextCache::new(mib * 1024 * 1024);
			cache.insert_term(
				&scope(1, 1),
				"common",
				Compacted::Postings(Arc::clone(&postings)),
				None,
			);
			assert!(cache.term(&scope(1, 1), "common").is_some(), "a {mib} MiB cache refused it");
		}
	}

	/// A cache of size zero holds nothing.
	#[test]
	fn a_zero_sized_cache_holds_nothing() {
		let cache = FullTextCache::new(0);
		assert!(!cache.enabled());
		cache.insert_term(&scope(1, 1), "hello", Compacted::Bitmap(RoaringTreemap::new()), None);
		assert!(cache.term(&scope(1, 1), "hello").is_none());
	}
}
