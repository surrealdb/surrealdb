//! The persisted doc-ID → elements mapping of a vector index.
//!
//! HNSW and DiskANN store vector → element + doc-IDs (`!hv`/`!hh`,
//! `!dq`/`!dh`). Exact scoring of an allow-list of doc-IDs needs the reverse
//! direction, kept under `!hk` / `!dk`: for each document, the set of elements
//! holding its vectors (a record indexing several distinct vectors maps to
//! several elements).
//!
//! The mapping is changed by [`link`] and [`unlink`] in the same transaction,
//! and at the same point, as every change to a document set of the forward
//! mapping, so the two agree in every snapshot. Only indexes stamped with
//! [`crate::catalog::VECTOR_DOC_ELEMENTS_FORMAT_VERSION`] or later keep it —
//! see [`crate::catalog::keeps_vector_doc_elements`]; an older index has
//! none of these keys until it is rebuilt.

use std::fmt::Debug;
use std::future::Future;

use anyhow::{Result, ensure};
use surrealdb_datastore::Transaction;

use crate::docids::DocId;
use crate::env::{IndexEnv, expect_not_done};
use crate::key::KVKey;
use crate::trees::hnsw::{ElementId, VectorId};
use crate::trees::knn::{Ids64, KnnResultBuilder};

/// Allow-list members whose doc-ID → elements entries one multi-get reads.
const MEMBER_BATCH: usize = 256;

/// Elements whose vectors one read fetches. A chunk closes at the first
/// document boundary at or past this, so it is exceeded only by the elements of
/// a single document.
const ELEMENT_BATCH: usize = 1024;

/// The point reads and writes of the doc-ID → elements mapping, through
/// whatever the caller reaches KV by: a transaction, or an accessor that checks
/// its caller's cancellation before each operation.
pub(crate) trait ElementsKv: Sync {
	fn get_elements<K>(&self, key: &K) -> impl Future<Output = Result<Option<Ids64>>> + Send
	where
		K: KVKey<Value = Ids64> + Debug + Sync;

	fn set_elements<K>(&self, key: &K, elements: &Ids64) -> impl Future<Output = Result<()>> + Send
	where
		K: KVKey<Value = Ids64> + Debug + Sync;

	fn del_elements<K>(&self, key: &K) -> impl Future<Output = Result<()>> + Send
	where
		K: KVKey<Value = Ids64> + Debug + Sync;
}

impl ElementsKv for Transaction {
	async fn get_elements<K>(&self, key: &K) -> Result<Option<Ids64>>
	where
		K: KVKey<Value = Ids64> + Debug + Sync,
	{
		self.get_key(key, None).await
	}

	async fn set_elements<K>(&self, key: &K, elements: &Ids64) -> Result<()>
	where
		K: KVKey<Value = Ids64> + Debug + Sync,
	{
		self.set_key(key, elements).await
	}

	async fn del_elements<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey<Value = Ids64> + Debug + Sync,
	{
		self.del_key(key).await
	}
}

impl ElementsKv for std::sync::Arc<Transaction> {
	async fn get_elements<K>(&self, key: &K) -> Result<Option<Ids64>>
	where
		K: KVKey<Value = Ids64> + Debug + Sync,
	{
		self.as_ref().get_elements(key).await
	}

	async fn set_elements<K>(&self, key: &K, elements: &Ids64) -> Result<()>
	where
		K: KVKey<Value = Ids64> + Debug + Sync,
	{
		self.as_ref().set_elements(key, elements).await
	}

	async fn del_elements<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey<Value = Ids64> + Debug + Sync,
	{
		self.as_ref().del_elements(key).await
	}
}

/// Records that the document addressed by `key` holds a vector in `element`,
/// writing the entry only when that adds the element.
pub(crate) async fn link<K>(kv: &impl ElementsKv, key: &K, element: ElementId) -> Result<()>
where
	K: KVKey<Value = Ids64> + Debug + Sync,
{
	let mut elements = kv.get_elements(key).await?.unwrap_or(Ids64::Empty);
	if let Some(grown) = elements.insert(element) {
		kv.set_elements(key, &grown).await?;
	}
	Ok(())
}

/// Records that the document addressed by `key` no longer holds a vector in
/// `element`, deleting the entry once the document holds none, and writing
/// nothing when the element was not linked.
pub(crate) async fn unlink<K>(kv: &impl ElementsKv, key: &K, element: ElementId) -> Result<()>
where
	K: KVKey<Value = Ids64> + Debug + Sync,
{
	let Some(mut elements) = kv.get_elements(key).await? else {
		return Ok(());
	};
	match elements.remove(element) {
		Some(shrunk) if shrunk.is_empty() => kv.del_elements(key).await,
		Some(shrunk) => kv.set_elements(key, &shrunk).await,
		None => Ok(()),
	}
}

/// Scores `members` into `builder`, each at the distance of its nearest
/// vector, through the doc-ID → elements mapping.
///
/// `elements_of` reads the element sets of a batch of documents, in input
/// order, and `distances` the query distances of a chunk of elements, in input
/// order, `None` for an element the index no longer holds. A document with no
/// element set, or none still held, takes no slot. `members` must be ascending
/// and free of duplicates, as an allow-list's iterator is.
///
/// Each callback takes its batch by value and hands it back with its answer,
/// so the buffers are reused across batches and a callback's future borrows
/// nothing from this function: it is `Send` wherever its captures are.
pub(crate) async fn score_members<M, MF, D, DF>(
	env: &dyn IndexEnv,
	mut members: impl Iterator<Item = DocId>,
	mut elements_of: M,
	mut distances: D,
	builder: &mut KnnResultBuilder,
) -> Result<()>
where
	M: FnMut(Vec<DocId>) -> MF,
	MF: Future<Output = Result<(Vec<DocId>, Vec<Option<Ids64>>)>>,
	D: FnMut(Vec<ElementId>) -> DF,
	DF: Future<Output = Result<(Vec<ElementId>, Vec<Option<f64>>)>>,
{
	let mut batch = Vec::with_capacity(MEMBER_BATCH);
	let mut docs = Vec::with_capacity(ELEMENT_BATCH);
	let mut elements = Vec::with_capacity(ELEMENT_BATCH);
	let mut checkpoints = 0usize;
	loop {
		batch.clear();
		batch.extend(members.by_ref().take(MEMBER_BATCH));
		if batch.is_empty() {
			break;
		}
		checkpoints += batch.len();
		expect_not_done(env, Some(checkpoints)).await?;
		let (returned, sets) = elements_of(std::mem::take(&mut batch)).await?;
		batch = returned;
		// A short answer would leave the trailing documents unscored, silently.
		ensure!(
			sets.len() == batch.len(),
			"{} element sets for {} documents",
			sets.len(),
			batch.len()
		);
		for (doc, set) in batch.iter().zip(sets) {
			for element in set.iter().flat_map(Ids64::iter) {
				docs.push(*doc);
				elements.push(element);
			}
			if elements.len() >= ELEMENT_BATCH {
				// A batch of documents holding many vectors each scores several
				// chunks, so cancellation is checked before each of them too.
				checkpoints += elements.len();
				expect_not_done(env, Some(checkpoints)).await?;
				score_chunk(&docs, &mut elements, &mut distances, builder).await?;
				docs.clear();
				elements.clear();
			}
		}
	}
	// Chunks accumulate across member batches; the last one closes here,
	// checked for cancellation like every other chunk.
	if !elements.is_empty() {
		checkpoints += elements.len();
		expect_not_done(env, Some(checkpoints)).await?;
		score_chunk(&docs, &mut elements, &mut distances, builder).await?;
	}
	Ok(())
}

/// Scores one chunk of elements, whose documents are adjacent and complete,
/// offering each document once at its nearest distance.
async fn score_chunk<D, DF>(
	docs: &[DocId],
	elements: &mut Vec<ElementId>,
	distances: &mut D,
	builder: &mut KnnResultBuilder,
) -> Result<()>
where
	D: FnMut(Vec<ElementId>) -> DF,
	DF: Future<Output = Result<(Vec<ElementId>, Vec<Option<f64>>)>>,
{
	let mut run: Option<(DocId, f64)> = None;
	let (returned, scored) = distances(std::mem::take(elements)).await?;
	*elements = returned;
	// A short answer would leave the trailing documents unscored, silently.
	ensure!(
		scored.len() == elements.len(),
		"{} distances for {} elements",
		scored.len(),
		elements.len()
	);
	for (doc, d) in docs.iter().copied().zip(scored) {
		let Some(d) = d else {
			continue;
		};
		match &mut run {
			Some((current, nearest)) if *current == doc => *nearest = nearest.min(d),
			_ => {
				if let Some((doc, d)) = run.replace((doc, d)) {
					offer(builder, doc, d);
				}
			}
		}
	}
	if let Some((doc, d)) = run {
		offer(builder, doc, d);
	}
	Ok(())
}

/// Offers a document's nearest distance to the result.
fn offer(builder: &mut KnnResultBuilder, doc: DocId, d: f64) {
	if builder.check_add(d) {
		builder.add_vector_id_result(d, VectorId::DocId(doc));
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;

	use common::EngineError;
	use surrealdb_kvs::TransactionType;

	use super::*;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId};
	use crate::key::schema::HnswDocElementsKey;
	use crate::test_env::{CancelAtCheckpoint, TestIndexStore};

	/// A callback answering synchronously, in the by-value shape
	/// [`score_members`] takes.
	fn owned<R>(
		mut f: impl FnMut(&[u64]) -> Result<R>,
	) -> impl FnMut(Vec<u64>) -> std::future::Ready<Result<(Vec<u64>, R)>> {
		move |batch| {
			let answer = f(&batch);
			std::future::ready(answer.map(|answer| (batch, answer)))
		}
	}

	fn key(doc_id: u64) -> HnswDocElementsKey<'static> {
		HnswDocElementsKey {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: Cow::Owned("tb".into()),
			ix: IndexId(3),
			doc_id,
		}
	}

	/// Single-vector documents accumulate across member batches: 2,000 of them
	/// read their vectors in chunks of `ELEMENT_BATCH`, not one per member
	/// batch.
	#[tokio::test]
	async fn score_members_fills_element_chunks_across_member_batches() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let env = ds.env(TransactionType::Read).await;
		let mut reads = Vec::new();
		let mut builder = KnnResultBuilder::new(10);
		score_members(
			&env,
			0..2_000u64,
			owned(|docs: &[u64]| {
				Ok(docs
					.iter()
					.map(|d| {
						let mut set = Ids64::Empty;
						if let Some(grown) = set.insert(*d) {
							set = grown;
						}
						Some(set)
					})
					.collect())
			}),
			owned(|elements: &[u64]| {
				reads.push(elements.len());
				Ok(elements.iter().map(|e| Some(*e as f64)).collect())
			}),
			&mut builder,
		)
		.await?;
		env.tx().cancel().await?;
		assert_eq!(reads, [ELEMENT_BATCH, 2_000 - ELEMENT_BATCH]);
		let got: Vec<u64> = builder
			.collect()
			.into_iter()
			.map(|(_, id)| match id {
				VectorId::DocId(doc) => doc,
				other => panic!("unexpected id {other:?}"),
			})
			.collect();
		assert_eq!(got, (0..10).collect::<Vec<_>>());
		Ok(())
	}

	/// A deadline that passes while members are scored is reported as a
	/// timeout, not as a cancellation.
	#[tokio::test]
	async fn a_deadline_while_scoring_is_reported_as_a_timeout() -> Result<()> {
		let timeout = std::time::Duration::from_millis(250);
		let ds = TestIndexStore::new().await;
		let env = CancelAtCheckpoint::timing_out(ds.env(TransactionType::Read).await, 1, timeout);
		let mut builder = KnnResultBuilder::new(10);
		let err = score_members(
			&env,
			0..10u64,
			owned(|docs: &[u64]| Ok(docs.iter().map(|_| None).collect())),
			owned(|elements: &[u64]| Ok(elements.iter().map(|_| None).collect())),
			&mut builder,
		)
		.await
		.expect_err("the deadline stops the scoring");
		env.tx().cancel().await?;
		match err.downcast_ref::<EngineError>() {
			Some(EngineError::QueryTimedout(d)) => assert_eq!(*d, timeout),
			_ => panic!("expected a timeout, got {err:#}"),
		}
		Ok(())
	}

	/// The trailing chunk, scored after the last member batch, is checked
	/// for cancellation too: a deadline expiring between the batch and that
	/// chunk stops the scoring before any distance is read.
	#[tokio::test]
	async fn a_deadline_before_the_trailing_chunk_is_reported_as_a_timeout() -> Result<()> {
		let timeout = std::time::Duration::from_millis(250);
		let ds = TestIndexStore::new().await;
		// The first counted check covers the member batch, the second the
		// trailing chunk its ten elements fill.
		let env = CancelAtCheckpoint::timing_out(ds.env(TransactionType::Read).await, 2, timeout);
		let mut reads = 0usize;
		let mut builder = KnnResultBuilder::new(10);
		let err = score_members(
			&env,
			0..10u64,
			owned(|docs: &[u64]| {
				Ok(docs
					.iter()
					.map(|d| {
						let mut set = Ids64::Empty;
						if let Some(grown) = set.insert(*d) {
							set = grown;
						}
						Some(set)
					})
					.collect())
			}),
			owned(|elements: &[u64]| {
				reads += elements.len();
				Ok(elements.iter().map(|e| Some(*e as f64)).collect())
			}),
			&mut builder,
		)
		.await
		.expect_err("the deadline stops the trailing chunk");
		env.tx().cancel().await?;
		assert_eq!(reads, 0, "no distance is read past the deadline");
		match err.downcast_ref::<EngineError>() {
			Some(EngineError::QueryTimedout(d)) => assert_eq!(*d, timeout),
			_ => panic!("expected a timeout, got {err:#}"),
		}
		Ok(())
	}

	/// An element-sets callback that answers fewer documents than it was asked
	/// for fails the scoring rather than leaving the trailing documents
	/// unscored.
	#[tokio::test]
	async fn score_members_refuses_a_short_element_sets_answer() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let env = ds.env(TransactionType::Read).await;
		let mut builder = KnnResultBuilder::new(10);
		let res = score_members(
			&env,
			[1u64, 2, 3].into_iter(),
			owned(|docs: &[u64]| {
				Ok(docs
					.iter()
					.skip(1)
					.map(|d| {
						let mut set = Ids64::Empty;
						if let Some(grown) = set.insert(*d) {
							set = grown;
						}
						Some(set)
					})
					.collect())
			}),
			owned(|elements: &[u64]| Ok(elements.iter().map(|e| Some(*e as f64)).collect())),
			&mut builder,
		)
		.await;
		env.tx().cancel().await?;
		let err = res.expect_err("a short answer must fail");
		assert!(err.to_string().contains("2 element sets for 3 documents"), "{err}");
		Ok(())
	}

	/// A distances callback that answers fewer elements than it was asked
	/// for fails the scoring rather than leaving the trailing documents
	/// unscored.
	#[tokio::test]
	async fn score_members_refuses_a_short_distance_answer() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let env = ds.env(TransactionType::Read).await;
		let mut builder = KnnResultBuilder::new(10);
		let res = score_members(
			&env,
			[1u64, 2, 3].into_iter(),
			owned(|docs: &[u64]| {
				Ok(docs
					.iter()
					.map(|d| {
						let mut set = Ids64::Empty;
						if let Some(grown) = set.insert(*d) {
							set = grown;
						}
						Some(set)
					})
					.collect())
			}),
			owned(|elements: &[u64]| {
				Ok(elements.iter().skip(1).map(|e| Some(*e as f64)).collect())
			}),
			&mut builder,
		)
		.await;
		env.tx().cancel().await?;
		let err = res.expect_err("a short answer must fail");
		assert!(err.to_string().contains("2 distances for 3 elements"), "{err}");
		Ok(())
	}

	/// Every member is offered once, at the distance of its nearest held
	/// element, however its elements fall across member batches and element
	/// chunks: here documents with up to 600 elements each, so chunks close
	/// past the element budget, alongside documents with no entry and
	/// elements the index no longer holds.
	#[tokio::test]
	async fn score_members_offers_each_document_at_its_nearest_element() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let env = ds.env(TransactionType::Read).await;
		// Document `d` holds `d * 37 % 601` elements; element `e` of document
		// `d` is `d * 1000 + e`, at distance `(d * 7 + e * 13) % 97`, and every
		// fifth element is no longer held.
		let count = |d: u64| d * 37 % 601;
		let distance = |e: u64| ((e / 1000) * 7 + (e % 1000) * 13) % 97;
		let members: Vec<u64> = (0..400).collect();
		let mut reads = Vec::new();
		let mut builder = KnnResultBuilder::new(1_000);
		score_members(
			&env,
			members.iter().copied(),
			owned(|docs: &[u64]| {
				Ok(docs
					.iter()
					.map(|d| {
						(count(*d) > 0).then(|| {
							let mut set = Ids64::Empty;
							for e in 0..count(*d) {
								if let Some(grown) = set.insert(d * 1000 + e) {
									set = grown;
								}
							}
							set
						})
					})
					.collect())
			}),
			owned(|elements: &[u64]| {
				reads.push(elements.len());
				Ok(elements.iter().map(|e| (e % 5 != 0).then(|| distance(*e) as f64)).collect())
			}),
			&mut builder,
		)
		.await?;
		env.tx().cancel().await?;
		let got: Vec<(u64, f64)> = builder
			.collect()
			.into_iter()
			.map(|(d, id)| match id {
				VectorId::DocId(doc) => (doc, f64::from(d)),
				other => panic!("unexpected id {other:?}"),
			})
			.collect();
		let mut want: Vec<(u64, f64)> = members
			.iter()
			.filter_map(|d| {
				(0..count(*d))
					.map(|e| d * 1000 + e)
					.filter(|e| e % 5 != 0)
					.map(|e| distance(e) as f64)
					.reduce(f64::min)
					.map(|nearest| (*d, nearest))
			})
			.collect();
		want.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
		assert_eq!(got, want);
		// A chunk only runs past the budget by one document's elements.
		assert!(reads.iter().all(|n| *n < ELEMENT_BATCH + 601), "{reads:?}");
		assert!(reads.len() > 1, "the members must span several chunks");
		Ok(())
	}

	/// Linking and unlinking past the size at which the set becomes a bitmap
	/// persists every change, and the last unlink deletes the entry.
	#[tokio::test]
	async fn link_and_unlink_persist_through_every_set_size() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let k = key(7);
		for e in 0..20u64 {
			let env = ds.env(TransactionType::Write).await;
			link(&env.tx(), &k, e).await?;
			// Linking twice changes nothing.
			link(&env.tx(), &k, e).await?;
			env.tx().commit().await?;
			let env = ds.env(TransactionType::Read).await;
			let got: Vec<u64> = env.tx().get_key(&k, None).await?.expect("linked").iter().collect();
			env.tx().cancel().await?;
			assert_eq!(got, (0..=e).collect::<Vec<_>>());
		}
		for e in (0..20u64).rev() {
			let env = ds.env(TransactionType::Write).await;
			unlink(&env.tx(), &k, e).await?;
			// Unlinking twice, or an element never linked, changes nothing.
			unlink(&env.tx(), &k, e).await?;
			unlink(&env.tx(), &k, 1_000).await?;
			env.tx().commit().await?;
			let env = ds.env(TransactionType::Read).await;
			let got = env.tx().get_key(&k, None).await?;
			env.tx().cancel().await?;
			match got {
				None => assert_eq!(e, 0, "deleted before the last element was unlinked"),
				Some(set) => assert_eq!(set.iter().collect::<Vec<_>>(), (0..e).collect::<Vec<_>>()),
			}
		}
		Ok(())
	}
}
