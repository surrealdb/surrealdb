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
use quick_cache::sync::{Cache, DefaultLifecycle, PlaceholderGuard};
use quick_cache::{DefaultHashBuilder, OptionsBuilder, Weighter};
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
	/// A term's documents alone, for a reader that scores none of them.
	TermDocs(TermScope, String),
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
			Key::Term(scope, term) | Key::TermDocs(scope, term) => {
				scope.table.as_str().len() + term.len()
			}
			Key::Document(scope, _) => scope.table.as_str().len(),
		};
		(std::mem::size_of::<Key>() + std::mem::size_of::<Weighed>() + 32 + named) as u64
			+ value.weight
	}
}

/// A term one reader reads and caches while every other reader asking for it
/// waits ([`FullTextCache::term_or_fill`]).
///
/// The first queries to read a term after a build or a compaction would each
/// read and decode the whole of it, a cost that grows with the documents the
/// term carries; one of them reads it and the others are handed what it
/// read. A fill dropped without [`Self::finish`], by a reader that failed or
/// was cancelled, lets the next waiter read the term in its place.
pub struct TermFill<'a> {
	guard: PlaceholderGuard<
		'a,
		Key,
		Weighed,
		EntryWeighter,
		DefaultHashBuilder,
		DefaultLifecycle<Key, Weighed>,
	>,
	/// Whether the readers waiting score none of the documents: they are
	/// handed the term's documents alone.
	docs_only: bool,
}

impl TermFill<'_> {
	/// Caches `term`'s compacted state at `scope` in `cache`, as
	/// [`FullTextCache::insert_term`] does, and hands it to the readers
	/// waiting for it.
	pub fn finish(
		self,
		cache: &FullTextCache,
		scope: &TermScope,
		term: &str,
		compacted: Compacted,
		residual: Option<TermDocsResidual>,
	) {
		let docs = docs_entry(&compacted, &residual);
		let whole = term_entry(compacted, residual);
		let (handed, other) = match (self.docs_only, docs) {
			(true, Some(docs)) => (docs, Some((Key::Term(scope.clone(), term.to_owned()), whole))),
			(false, Some(docs)) => {
				(whole, Some((Key::TermDocs(scope.clone(), term.to_owned()), docs)))
			}
			// A term whose whole state is its documents serves both readers.
			(true, None) => {
				(whole.clone(), Some((Key::Term(scope.clone(), term.to_owned()), whole)))
			}
			(false, None) => (whole, None),
		};
		// A placeholder an insert of another reader replaced hands its waiters
		// that reader's value instead.
		let _ = self.guard.insert(handed);
		if let (Some(entries), Some((key, value))) = (&cache.entries, other) {
			entries.insert(key, value);
		}
	}
}

/// The cache entry of a term's whole compacted state.
fn term_entry(compacted: Compacted, residual: Option<TermDocsResidual>) -> Weighed {
	let held = match &compacted {
		Compacted::Absent => 0,
		Compacted::Bitmap(bitmap) => bitmap.serialized_size() as u64,
		Compacted::Postings(postings) => postings.weight(),
	};
	Weighed {
		weight: held + residual_weight(&residual),
		entry: Entry::Term(Arc::new(CompactedTerm {
			compacted,
			residual,
		})),
	}
}

/// The cache entry of the documents alone of postings a term keeps in blocks;
/// `None` for a term whose whole state is its documents.
fn docs_entry(compacted: &Compacted, residual: &Option<TermDocsResidual>) -> Option<Weighed> {
	let Compacted::Postings(postings) = compacted else {
		return None;
	};
	let docs = postings.docs().clone();
	Some(Weighed {
		weight: docs.serialized_size() as u64 + residual_weight(residual),
		entry: Entry::Term(Arc::new(CompactedTerm {
			compacted: Compacted::Bitmap(docs),
			residual: residual.clone(),
		})),
	})
}

/// What a term's residual holds.
fn residual_weight(residual: &Option<TermDocsResidual>) -> u64 {
	residual
		.as_ref()
		.map_or(0, |residual| (residual.counts.len() * std::mem::size_of::<(u64, i64)>()) as u64)
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

	/// The documents of `term` in `scope` with its residual, when cached:
	/// its whole compacted state, or the documents alone where the postings a
	/// term keeps in blocks are not cached
	/// ([`crate::ft::fulltext::Compacted::Bitmap`]). For a reader that scores
	/// no document from the term's postings.
	pub fn term_docs(&self, scope: &TermScope, term: &str) -> Option<Arc<CompactedTerm>> {
		if let Some(term) = self.term(scope, term) {
			return Some(term);
		}
		match self.entries.as_ref()?.get(&Key::TermDocs(scope.clone(), term.to_owned()))?.entry {
			Entry::Term(term) => Some(term),
			Entry::Document(_) => None,
		}
	}

	/// Caches the compacted state of `term` in `scope`. Postings are also
	/// cached as their documents alone ([`Self::term_docs`]): the documents of
	/// a term most documents carry fit in a fraction of what its postings
	/// hold, which may be more than the cache admits.
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
		if let Some(docs) = docs_entry(&compacted, &residual) {
			entries.insert(Key::TermDocs(scope.clone(), term.to_owned()), docs);
		}
		entries.insert(Key::Term(scope.clone(), term.to_owned()), term_entry(compacted, residual));
	}

	/// The compacted state of `term` in `scope` as [`Self::term`] serves it,
	/// or as [`Self::term_docs`] does where `docs_only`; where the cache holds
	/// none, the fill this reader is to complete once it has read the term
	/// ([`TermFill`]), while another reader filling it is waited for and its
	/// state taken. `Err(None)` for a cache that holds nothing.
	pub async fn term_or_fill(
		&self,
		scope: &TermScope,
		term: &str,
		docs_only: bool,
	) -> Result<Arc<CompactedTerm>, Option<TermFill<'_>>> {
		let entries = self.entries.as_ref().ok_or(None)?;
		let key = if docs_only {
			if let Some(term) = self.term(scope, term) {
				return Ok(term);
			}
			Key::TermDocs(scope.clone(), term.to_owned())
		} else {
			Key::Term(scope.clone(), term.to_owned())
		};
		match entries.get_value_or_guard_async(&key).await {
			Ok(Weighed {
				entry: Entry::Term(term),
				..
			}) => Ok(term),
			Ok(Weighed {
				entry: Entry::Document(_),
				..
			}) => Err(None),
			Err(guard) => Err(Some(TermFill {
				guard,
				docs_only,
			})),
		}
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

	/// The postings of a term heavier than the cache admits are not kept, but
	/// their documents are, and served to a reader of the documents alone; a
	/// reader of the whole state finds nothing.
	#[test]
	fn the_documents_of_postings_too_heavy_to_keep_are_kept() {
		let blocks: Vec<_> = (0..1000u64)
			.map(|block| {
				crate::ft::fulltext::PostingBlock::from_entries(
					(0..256u64).map(|at| (block * 256 + at, 1 + at % 5, 10 + at % 30)),
				)
			})
			.collect();
		let postings =
			Arc::new(crate::kernels::postings::TermPostings::from_blocks(&blocks).unwrap());
		let cache = FullTextCache::new(2 * 1024 * 1024);
		assert!(postings.weight() > 2 * 1024 * 1024, "{} bytes", postings.weight());
		cache.insert_term(&scope(1, 1), "common", Compacted::Postings(Arc::clone(&postings)), None);
		assert!(cache.term(&scope(1, 1), "common").is_none(), "the postings are refused");
		let docs = cache.term_docs(&scope(1, 1), "common").expect("the documents are kept");
		assert!(matches!(&docs.compacted, Compacted::Bitmap(cached) if cached == postings.docs()));
		assert!(cache.term_docs(&scope(2, 1), "common").is_none(), "another generation");
		// A term whose whole state is kept answers a reader of its documents too.
		let bitmap: RoaringTreemap = [1u64, 2].into_iter().collect();
		cache.insert_term(&scope(1, 1), "rare", Compacted::Bitmap(bitmap.clone()), None);
		let docs = cache.term_docs(&scope(1, 1), "rare").expect("the state is kept");
		assert!(matches!(&docs.compacted, Compacted::Bitmap(cached) if *cached == bitmap));
	}

	/// One reader fills a term the cache lacks while another reader of it
	/// waits, and is handed what the first read: the documents alone to a
	/// reader that scores none, though the postings were too heavy to keep.
	/// A fill dropped unfinished leaves the term to the next reader.
	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn a_term_is_filled_by_one_reader_while_the_others_wait() {
		let blocks: Vec<_> = (0..1000u64)
			.map(|block| {
				crate::ft::fulltext::PostingBlock::from_entries(
					(0..256u64).map(|at| (block * 256 + at, 1 + at % 5, 10 + at % 30)),
				)
			})
			.collect();
		let postings =
			Arc::new(crate::kernels::postings::TermPostings::from_blocks(&blocks).unwrap());
		let cache = Arc::new(FullTextCache::new(2 * 1024 * 1024));
		// A fill dropped unfinished hands the term to the next reader.
		let Err(Some(abandoned)) = cache.term_or_fill(&scope(1, 1), "common", true).await else {
			panic!("the first reader fills the term");
		};
		drop(abandoned);
		let Err(Some(fill)) = cache.term_or_fill(&scope(1, 1), "common", true).await else {
			panic!("the next reader fills the term");
		};
		let waiter = {
			let cache = Arc::clone(&cache);
			tokio::spawn(async move {
				match cache.term_or_fill(&scope(1, 1), "common", true).await {
					Ok(term) => term,
					Err(_) => panic!("a reader waits for the fill under way"),
				}
			})
		};
		tokio::time::sleep(std::time::Duration::from_millis(50)).await;
		assert!(!waiter.is_finished(), "the waiter waits for the fill");
		fill.finish(
			&cache,
			&scope(1, 1),
			"common",
			Compacted::Postings(Arc::clone(&postings)),
			None,
		);
		let handed = waiter.await.unwrap();
		assert!(matches!(&handed.compacted, Compacted::Bitmap(docs) if docs == postings.docs()));
		assert!(cache.term_docs(&scope(1, 1), "common").is_some(), "the documents are kept");
		// A term whose whole state is its documents, filled for a reader of its
		// documents, answers a reader of its whole state too.
		let Err(Some(fill)) = cache.term_or_fill(&scope(1, 1), "bitmap", true).await else {
			panic!("the reader fills the term");
		};
		let bitmap: RoaringTreemap = [7u64, 8].into_iter().collect();
		fill.finish(&cache, &scope(1, 1), "bitmap", Compacted::Bitmap(bitmap.clone()), None);
		let whole = cache.term(&scope(1, 1), "bitmap").expect("the whole state is kept");
		assert!(matches!(&whole.compacted, Compacted::Bitmap(cached) if *cached == bitmap));
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
