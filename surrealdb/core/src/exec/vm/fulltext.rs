//! The candidates of a full-text query, as a program reads them.
//!
//! The index is opened through [`Services::open_full_text`] for its analyzer
//! and its doc-id layout, and the query is analyzed into its terms
//! (`FullTextIndex::plan_query`), which names what each term reads. Every one
//! of those reads is a plain read of the keyspace — the change keys and change
//! batches through [`Services::read_range`], the residuals and compacted
//! bitmaps through [`Services::read_values`] — and the changes the transaction
//! buffered come from [`Services::pending_term_changes`]. The full-text kernels
//! (`idx::kernels::fulltext`) fold what was read into each term's document set
//! and combine the sets under the query's operator; the hits are the records
//! the combined set names, resolved a batch at a time.

use std::sync::Arc;

use roaring::RoaringTreemap;

use crate::catalog::IndexId;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::function::MatchContext;
use crate::exec::index::access_path::IndexRef;
use crate::exec::index::iterator::btree::INDEX_BATCH_SIZE;
use crate::exec::ir::phys::ops::ScoreSum;
use crate::exec::operators::SortDirection;
use crate::exec::operators::scan::FullTextHits;
use crate::exec::vm::interp::ScanMatch;
use crate::exec::vm::program::TopKScan;
use crate::exec::vm::services::Services;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::operator::{MatchRef, MatchesOperator};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::docids::DocId;
use crate::idx::ft::fulltext::{
	ChangeBatch, Compacted, FullTextIndex, PostingBlock, QueryTerms, TermDocsResidual, TermState,
};
use crate::idx::kernels::fulltext::CompactedReads;
use crate::key::{KVValue, RawRange};
use crate::kvs::Direction;
use crate::val::{Number, RecordId, TableName, Value};

/// The hits of `query` against full-text index `index` of `table` under
/// `operator`, or `None` when no record can match.
///
/// Where `shared` is the context of the clause the scan answers
/// ([`super::interp::State::scan_match_context`]), the index and the query's
/// terms are the context's: read here unless a call of the run already read
/// them, and read by no call after.
pub(super) async fn candidates(
	ctx: &ExecutionContext,
	services: &impl Services,
	table: &TableName,
	index: &IndexRef,
	query: &str,
	operator: &MatchesOperator,
	shared: Option<&MatchContext>,
) -> FlowResult<Option<FullTextHits>> {
	if let Some(shared) = shared
		&& let Some(ft) = shared
			.resources_for_scan(ctx, index.definition().index_id, || {
				query_terms(ctx, services, table, index, query, false)
			})
			.await?
	{
		return Ok(ft.index.new_hits_iterator(&ft.terms, operator.operator).map(FullTextHits::new));
	}
	// No call of the program scores from these terms: the scan lists their
	// documents, and any score is the shared context's.
	let (fti, terms) = query_terms(ctx, services, table, index, query, true).await?;
	Ok(fti.new_hits_iterator(&terms, operator.operator).map(FullTextHits::new))
}

/// Full-text index `index` of `table`, opened, and `query`'s terms with the
/// documents each carries; the documents alone where `docs_only`, for a
/// caller that scores none of them from the terms
/// ([`FullTextIndex::cached_states`]).
async fn query_terms(
	ctx: &ExecutionContext,
	services: &impl Services,
	table: &TableName,
	index: &IndexRef,
	query: &str,
	docs_only: bool,
) -> FlowResult<(FullTextIndex, QueryTerms)> {
	#[cfg(test)]
	crate::exec::function::TERM_READS.with(|reads| reads.set(reads.get() + 1));
	let fti = services.open_full_text(table, index).await?;
	let az_fn = StreamingAnalyzerFunction::new(ctx);
	let plan =
		fti.plan_query(&az_fn, query.to_owned()).await.context("Failed to extract query terms")?;
	// Every term's change ranges, and the residual and bitmap point keys every
	// term names, are read at once: an engine whose transaction serves reads
	// concurrently overlaps their round trips, and one that does not serves
	// them in turn.
	// A term whose compacted state the node has cached for the generation this
	// snapshot reads takes it from memory; only the others' are read. The
	// cache is keyed by the table's catalog version, which the run reads as
	// it reads the version a compiled unit is kept under.
	let txn = ctx.txn();
	let scope = crate::exec::compiles::seam(fti.term_scope(&txn)).await?;
	// The first reader of a term the cache lacks reads it; the others wait for
	// what it reads.
	let (cached, fills) = fti.load_states(scope.as_ref(), &plan, docs_only).await;
	// An uncached term reads its residual, and its bitmap where the index
	// keeps one, as point keys; its posting blocks, where the index keeps
	// those, are a range read beside its changes.
	let stored = plan
		.reads()
		.iter()
		.zip(&cached)
		.filter(|(_, cached)| cached.is_none())
		.flat_map(|(r, _)| {
			let bitmap = match &r.compacted {
				CompactedReads::Bitmap(key) => Some(key.clone()),
				CompactedReads::Blocks(_) => None,
			};
			std::iter::once(r.residual.clone()).chain(bitmap)
		})
		.collect();
	let ranges = futures::future::try_join_all(plan.reads().iter().zip(&cached).map(
		|(reads, cached)| async move {
			let change_keys = read_all_keys(ctx, services, reads.changes.clone().into()).await?;
			let batches = read_all(ctx, services, reads.batches.clone().into())
				.await?
				.into_iter()
				.map(|(key, value)| Ok((key, decode::<ChangeBatch>(&value)?)))
				.collect::<FlowResult<Vec<_>>>()?;
			let blocks = match (&reads.compacted, cached) {
				(CompactedReads::Blocks(range), None) => Some(
					read_all(ctx, services, range.clone().into())
						.await?
						.iter()
						.map(|(_, value)| decode::<PostingBlock>(value))
						.collect::<FlowResult<Vec<_>>>()?,
				),
				_ => None,
			};
			FlowResult::Ok((change_keys, batches, blocks))
		},
	));
	let (ranges, values) = futures::future::try_join(ranges, services.read_values(stored)).await?;
	let mut states = Vec::with_capacity(plan.terms().len());
	let mut blocks = Vec::with_capacity(plan.terms().len());
	for (term, (change_keys, batches, term_blocks)) in plan.terms().iter().zip(ranges) {
		let (pending_added, pending_removed) = services.pending_term_changes(&fti, term);
		states.push(TermState {
			change_keys,
			batches,
			pending_added,
			pending_removed,
			residual: None,
			compacted: Compacted::Absent,
		});
		blocks.push(term_blocks);
	}
	// Each uncached term's residual and compacted documents, decoded from the
	// values read in the order they were asked for.
	let mut values = values.into_iter();
	let mut read = Vec::new();
	for ((reads, term_blocks), cached) in plan.reads().iter().zip(blocks).zip(&cached) {
		if cached.is_some() {
			continue;
		}
		#[cfg(test)]
		crate::exec::function::TERM_STATES_READ.with(|read| read.set(read.get() + 1));
		let residual = values.next().context("a residual the read did not answer")?;
		let residual = residual.as_deref().map(decode::<TermDocsResidual>).transpose()?;
		let compacted = match &reads.compacted {
			CompactedReads::Bitmap(_) => {
				let bitmap = values.next().context("a bitmap the read did not answer")?;
				Compacted::from_bitmap(bitmap.as_deref().map(decode::<RoaringTreemap>).transpose()?)
			}
			CompactedReads::Blocks(_) => Compacted::from_blocks(
				&term_blocks.context("posting blocks the read did not answer")?,
			)?,
		};
		read.push((compacted, residual));
	}
	fti.settle_states(scope.as_ref(), &plan, &mut states, cached, fills, read)
		.context("Failed to extract query terms")?;
	FullTextIndex::record_pending_read(&txn, &states);
	let terms = plan.resolve(states).context("Failed to extract query terms")?.with_scope(scope);
	Ok((fti, terms))
}

#[cfg(test)]
thread_local! {
	/// How many full-text hits this thread's scans handed on to have their
	/// records read, for tests that must know a threshold passed hits over.
	pub(super) static HITS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many full-text hits this thread's scans passed over by their posting
	/// blocks' bounds before scoring them, for tests that must know the bounds
	/// were used.
	pub(super) static BLOCK_SKIPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many full-text hits this thread's index union branches cut by the
	/// ceilings of a sum's clauses, for tests that must know the cut was made.
	pub(super) static UNION_CUTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// What a full-text listing rejects a hit by before reading its record, with the
/// threshold the sort above has published.
pub(super) enum Threshold<'a> {
	/// The score of the clause the listing answers ([`Source::FullText`]).
	///
	/// [`Source::FullText`]: super::program::Source::FullText
	Single(&'a TopKScan, Arc<Value>),
	/// A sum of clause scores an index union's branches compare
	/// ([`UnionScores`]), with the clause the listing's branch answers.
	Sum(&'a UnionScores, Arc<Value>, Option<MatchRef>),
}

/// What the full-text branches of an index union reject hits by
/// ([`super::program::UnionTopK`]): the threshold the sort above publishes, and
/// the sum of clause scores its first key is, each clause scored in the
/// context the program's calls on it share.
pub(super) struct UnionScores {
	pub(super) cell: Arc<crate::exec::topk_pushdown::TopKThresholdCell>,
	pub(super) direction: SortDirection,
	pub(super) single_key: bool,
	pub(super) sum: ScoreSum,
	pub(super) clauses: Vec<UnionClause>,
}

/// One clause of a [`UnionScores`] sum: the branch answering it, and its
/// context.
pub(super) struct UnionClause {
	pub(super) clause: MatchRef,
	pub(super) table: TableName,
	pub(super) index: IndexRef,
	pub(super) query: String,
	pub(super) context: Arc<MatchContext>,
}

impl UnionScores {
	/// Reads every clause's terms into its context, as the scan of its branch
	/// would when it opens, so a hit another branch lists can be scored under
	/// it before that branch is reached.
	pub(super) async fn prepare(
		&self,
		ctx: &ExecutionContext,
		services: &impl Services,
	) -> FlowResult<()> {
		for clause in &self.clauses {
			clause
				.context
				.resources_for_scan(ctx, clause.index.definition().index_id, || {
					query_terms(ctx, services, &clause.table, &clause.index, &clause.query, false)
				})
				.await?;
		}
		Ok(())
	}

	/// The value of the sort's first key for each of `doc_ids`, in order: the
	/// sum of the hit's clause scores, each a float as `search::score` returns
	/// it and added as the key adds them. `None` where a clause's context
	/// cannot score the batch, which then rejects no hit.
	async fn sums(
		&self,
		ctx: &ExecutionContext,
		doc_ids: &[DocId],
	) -> FlowResult<Option<Vec<Value>>> {
		let mut scored = Vec::with_capacity(self.clauses.len());
		for clause in &self.clauses {
			let index_id = clause.index.definition().index_id;
			let Some(scores) = clause.context.scores(ctx, index_id, doc_ids).await? else {
				return Ok(None);
			};
			scored.push((clause.clause, scores));
		}
		let mut sums = Vec::with_capacity(doc_ids.len());
		for at in 0..doc_ids.len() {
			match sum_of(&self.sum, &scored, at) {
				Some(sum) => sums.push(sum),
				None => return Ok(None),
			}
		}
		Ok(Some(sums))
	}
}

impl UnionScores {
	/// The doc ids of `doc_ids` a sort keeping the highest sums may still keep:
	/// every one but those whose sum, bounded clause by clause by the clauses'
	/// posting blocks, cannot beat `threshold`.
	///
	/// A bound is at or above its clause's score, a document carrying none of a
	/// clause's terms scores zero under it, and the sum only adds, so a sum of
	/// bounds is at or above the hit's sum. All clauses read one table's doc ids,
	/// as only an index keeping its postings in blocks bounds a score. A hit
	/// a clause does not bound, and every hit where a clause bounds nothing, is
	/// kept.
	async fn within_bounds(
		&self,
		ctx: &ExecutionContext,
		doc_ids: Vec<DocId>,
		threshold: &Value,
	) -> FlowResult<Vec<DocId>> {
		if self.direction != SortDirection::Desc {
			return Ok(doc_ids);
		}
		let mut bounded = Vec::with_capacity(self.clauses.len());
		for clause in &self.clauses {
			let index_id = clause.index.definition().index_id;
			let Some(bounds) = clause.context.score_bounds(ctx, index_id, &doc_ids).await? else {
				return Ok(doc_ids);
			};
			// A score no bound holds is taken as unbounded, so its sum beats any
			// threshold.
			let bounds = bounds.into_iter().map(|bound| bound.unwrap_or(f32::INFINITY)).collect();
			bounded.push((clause.clause, bounds));
		}
		Ok(doc_ids
			.into_iter()
			.enumerate()
			.filter(|(at, _)| {
				sum_of(&self.sum, &bounded, *at).is_none_or(|bound| {
					!crate::exec::topk_pushdown::cannot_beat(
						threshold,
						&bound,
						self.direction,
						self.single_key,
					)
				})
			})
			.map(|(_, doc_id)| doc_id)
			.collect())
	}
}

impl UnionScores {
	/// Cuts the hits not yet listed by the branch answering clause `clause` to
	/// those a sort keeping the highest sums may still keep by the clauses'
	/// ceilings alone, and answers how many were cut.
	///
	/// A clause scores a document its terms hold no uncompacted state for at
	/// most its ceiling ([`MatchContext::score_ceiling`]), and zero where the
	/// document carries none of its terms. The most clauses, taken lowest
	/// ceiling first, whose ceilings, added as the sum adds the clauses'
	/// scores with zero for every other clause, cannot beat `threshold`, bound
	/// the sum of every such document carrying none of the other clauses'
	/// terms. Where the branch's own clause is among them, only the hits
	/// carrying another clause's terms, and those any clause holds uncompacted
	/// state for, may still be kept; as the threshold only tightens, the rest
	/// are cut for good. Nothing is cut for a branch whose clause the sum does
	/// not add, or where a clause bounds nothing.
	async fn cut(
		&self,
		ctx: &ExecutionContext,
		listing: &mut FullTextHits,
		clause: MatchRef,
		threshold: &Value,
	) -> FlowResult<u64> {
		// Each clause once, however many times the sum adds it.
		let mut clauses: Vec<&UnionClause> = Vec::with_capacity(self.clauses.len());
		for held in &self.clauses {
			if !clauses.iter().any(|seen| seen.clause == held.clause) {
				clauses.push(held);
			}
		}
		let Some(own) = clauses.iter().position(|held| held.clause == clause) else {
			return Ok(0);
		};
		if self.direction != SortDirection::Desc || clauses.len() < 2 {
			return Ok(0);
		}
		let mut ceilings = Vec::with_capacity(clauses.len());
		for held in &clauses {
			let index_id = held.index.definition().index_id;
			let Some(ceiling) = held.context.score_ceiling(ctx, index_id).await? else {
				return Ok(0);
			};
			ceilings.push(ceiling);
		}
		let mut order: Vec<usize> = (0..ceilings.len()).collect();
		order.sort_by(|a, b| ceilings[*a].total_cmp(&ceilings[*b]));
		let mut passed = vec![false; ceilings.len()];
		for at in order {
			passed[at] = true;
			let bounded: Vec<(MatchRef, Vec<f32>)> = clauses
				.iter()
				.zip(&ceilings)
				.zip(&passed)
				.map(|((held, ceiling), passed)| {
					(
						held.clause,
						vec![if *passed {
							*ceiling
						} else {
							0.0
						}],
					)
				})
				.collect();
			let beats = sum_of(&self.sum, &bounded, 0).is_none_or(|bound| {
				!crate::exec::topk_pushdown::cannot_beat(
					threshold,
					&bound,
					self.direction,
					self.single_key,
				)
			});
			if beats {
				passed[at] = false;
				break;
			}
		}
		let count = passed.iter().filter(|passed| **passed).count();
		if !passed[own] || count <= listing.passed_clauses {
			return Ok(0);
		}
		let mut kept = RoaringTreemap::new();
		for (held, passed) in clauses.iter().zip(&passed) {
			let Some((carriers, uncompacted)) =
				held.context.carriers(held.index.definition().index_id)
			else {
				return Ok(0);
			};
			kept |= uncompacted;
			if !passed {
				kept |= carriers;
			}
		}
		listing.passed_clauses = count;
		Ok(listing.hits_mut().retain(&kept))
	}
}

/// The value `sum` comes to for the hit at `at`, its clauses' scores taken from
/// `scored`; `None` where a clause has no score or an addition is refused.
fn sum_of(sum: &ScoreSum, scored: &[(MatchRef, Vec<f32>)], at: usize) -> Option<Value> {
	match sum {
		ScoreSum::Clause(clause) => {
			let (_, scores) = scored.iter().find(|(held, _)| held == clause)?;
			Some(Value::Number(Number::Float(f64::from(*scores.get(at)?))))
		}
		ScoreSum::Add(left, right) => {
			let left = sum_of(left, scored, at)?;
			let right = sum_of(right, scored, at)?;
			crate::fnc::operate::add(left, right).ok()
		}
	}
}

/// The records of the next batch of hits a full-text scan of index `index_id`
/// hands on, or `None` once the hits have run out.
///
/// A batch is up to `limit` doc ids. Where the program scores
/// the clause the scan answers (`shared`), they are scored first, and where a
/// sort above keeps the best scores, those that cannot beat its `threshold`
/// are passed over ([`admitted`]) before anything else is read of them. Only
/// the rest are resolved to their records, in one batched read, and remembered
/// in the clause's context with their doc ids and scores. A batch every hit of
/// which is passed over hands on no record.
pub(super) async fn next_records(
	ctx: &ExecutionContext,
	listing: &mut FullTextHits,
	limit: usize,
	shared: Option<&ScanMatch>,
	index_id: IndexId,
	threshold: Option<Threshold<'_>>,
) -> FlowResult<Option<Vec<RecordId>>> {
	// Where the sort keeps the best sums of clause scores, the hits carrying only
	// the terms of clauses whose ceilings together cannot beat its threshold are
	// cut before any is taken.
	if let Some(Threshold::Sum(union, threshold, Some(clause))) = &threshold {
		let cut = union.cut(ctx, listing, *clause, threshold).await?;
		#[cfg(test)]
		{
			BLOCK_SKIPS.with(|skips| skips.set(skips.get() + cut as usize));
			UNION_CUTS.with(|cuts| cuts.set(cuts.get() + cut as usize));
		}
		#[cfg(not(test))]
		let _ = cut;
	}
	// Where the sort keeps the best scores of the clause the scan answers, the
	// hits whose posting blocks bound their score below its threshold are
	// passed over a range of blocks at a time, before they are taken.
	let bounded = match (&threshold, shared) {
		(Some(Threshold::Single(topk, threshold)), Some(shared))
			if shared.scored && topk.direction == SortDirection::Desc =>
		{
			shared
				.context
				.take_within_bounds(
					ctx,
					index_id,
					listing.hits_mut(),
					limit,
					threshold,
					topk.single_key,
				)
				.await?
		}
		(Some(Threshold::Single(..) | Threshold::Sum(..)) | None, Some(_) | None) => None,
	};
	let doc_ids = match bounded {
		Some((doc_ids, passed)) => {
			#[cfg(test)]
			BLOCK_SKIPS.with(|skips| skips.set(skips.get() + passed as usize));
			#[cfg(not(test))]
			let _ = passed;
			doc_ids
		}
		None => listing.take_doc_ids(limit),
	};
	if doc_ids.is_empty() {
		return Ok(None);
	}
	super::fuel::tick(ctx, super::fuel::units(doc_ids.len())).await?;
	// Where the sort keeps the best sums of clause scores, the hits whose sum
	// every clause's posting blocks bound below its threshold are passed over
	// before they are scored.
	let doc_ids = match &threshold {
		// A sum over several clauses is bounded by every clause's blocks at once.
		Some(Threshold::Sum(union, threshold, _)) if union.direction == SortDirection::Desc => {
			#[cfg(test)]
			let taken = doc_ids.len();
			let kept = union.within_bounds(ctx, doc_ids, threshold).await?;
			#[cfg(test)]
			BLOCK_SKIPS.with(|skips| skips.set(skips.get() + taken - kept.len()));
			kept
		}
		Some(Threshold::Sum(..) | Threshold::Single(..)) | None => doc_ids,
	};
	if doc_ids.is_empty() {
		return Ok(Some(Vec::new()));
	}
	let scores = match shared {
		Some(shared) if shared.scored => shared.context.scores(ctx, index_id, &doc_ids).await?,
		Some(_) | None => None,
	};
	let (doc_ids, scores) = match (threshold, scores) {
		(Some(Threshold::Single(topk, threshold)), Some(scores)) => {
			let (doc_ids, scores) =
				admitted(doc_ids, scores, &threshold, topk.direction, topk.single_key);
			(doc_ids, Some(scores))
		}
		(Some(Threshold::Sum(union, threshold, _)), scores) => {
			match union.sums(ctx, &doc_ids).await? {
				Some(sums) => {
					let keep: Vec<bool> = sums
						.iter()
						.map(|sum| {
							!crate::exec::topk_pushdown::cannot_beat(
								&threshold,
								sum,
								union.direction,
								union.single_key,
							)
						})
						.collect();
					let scores = scores.map(|scores| {
						scores
							.into_iter()
							.zip(&keep)
							.filter(|(_, keep)| **keep)
							.map(|(s, _)| s)
							.collect()
					});
					let doc_ids = doc_ids
						.into_iter()
						.zip(&keep)
						.filter(|(_, keep)| **keep)
						.map(|(d, _)| d)
						.collect();
					(doc_ids, scores)
				}
				None => (doc_ids, scores),
			}
		}
		(Some(Threshold::Single(..)) | None, scores) => (doc_ids, scores),
	};
	if doc_ids.is_empty() {
		return Ok(Some(Vec::new()));
	}
	let records = listing.records(&ctx.txn(), &doc_ids).await?;
	let mut hits = Vec::with_capacity(doc_ids.len());
	for (at, (doc_id, rid)) in doc_ids.into_iter().zip(records).enumerate() {
		if let Some(rid) = rid {
			hits.push((rid, doc_id, scores.as_ref().and_then(|scores| scores.get(at).copied())));
		}
	}
	if let Some(shared) = shared {
		shared.context.remember(index_id, &hits);
	}
	#[cfg(test)]
	HITS_READ.with(|read| read.set(read.get() + hits.len()));
	Ok(Some(hits.into_iter().map(|(rid, _, _)| rid).collect()))
}

/// The doc ids of `doc_ids` a sort keeping the best rows in `direction` may
/// still keep, with their scores: every one but those whose score, `scores` at
/// the same position, cannot beat `threshold`, the first key of the worst row
/// the sort keeps.
pub(super) fn admitted(
	doc_ids: Vec<DocId>,
	scores: Vec<f32>,
	threshold: &Value,
	direction: SortDirection,
	single_key: bool,
) -> (Vec<DocId>, Vec<f32>) {
	doc_ids
		.into_iter()
		.zip(scores)
		.filter(|(_, score)| {
			let score = Value::Number(Number::Float(f64::from(*score)));
			!crate::exec::topk_pushdown::cannot_beat(threshold, &score, direction, single_key)
		})
		.unzip()
}

/// Every entry of `range`, read a batch at a time.
///
/// Each read asks for no more entries than the fuel left allows
/// ([`super::fuel::batch`]) and charges the entries it returns. A read that
/// answers fewer entries than it asked for has drained the range, so the
/// loop ends on it rather than on a further read that answers none.
pub(super) async fn read_all(
	ctx: &ExecutionContext,
	services: &impl Services,
	mut range: RawRange,
) -> FlowResult<Vec<(Vec<u8>, Vec<u8>)>> {
	let mut out = Vec::new();
	loop {
		let most = super::fuel::batch(INDEX_BATCH_SIZE);
		let batch = services.read_range(&mut range, Direction::Forward, most).await?;
		let read = batch.len();
		out.extend(batch);
		super::fuel::tick(ctx, super::fuel::units(read)).await?;
		if read < most as usize {
			return Ok(out);
		}
	}
}

/// Every key of `range`, read a batch at a time with no value read, as
/// [`read_all`] reads its entries.
pub(super) async fn read_all_keys(
	ctx: &ExecutionContext,
	services: &impl Services,
	mut range: RawRange,
) -> FlowResult<Vec<Vec<u8>>> {
	let mut out = Vec::new();
	loop {
		let most = super::fuel::batch(INDEX_BATCH_SIZE);
		let batch = services.read_range_keys(&mut range, Direction::Forward, most).await?;
		let read = batch.len();
		out.extend(batch);
		super::fuel::tick(ctx, super::fuel::units(read)).await?;
		if read < most as usize {
			return Ok(out);
		}
	}
}

fn decode<V: KVValue<KeyContext = ()>>(bytes: &[u8]) -> FlowResult<V> {
	V::kv_decode_value(bytes, ()).map_err(ControlFlow::Err)
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::*;
	use crate::catalog::providers::TableProvider;
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::vm::services::EngineServices;

	/// More documents carrying the term than one read takes, each written by a
	/// statement of its own, so each leaves the term a change batch of its own.
	const DOCS: usize = INDEX_BATCH_SIZE as usize + 100;

	/// A term with more uncompacted change batches than one read takes is
	/// read whole: every batch once, in key order, by both the entry and the
	/// key read, and the match answers every document.
	#[tokio::test]
	async fn a_term_read_crosses_batches_and_misses_none() {
		let mut setup = String::from(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25;",
		);
		for i in 0..DOCS {
			setup.push_str(&format!(" CREATE d:{i} SET body = 'hello world';"));
		}
		// No maintenance task runs, so no compaction folds the batches.
		let db = TestDb::quiescent(&setup, false).await;
		let ctx = db.exec_ctx().await;
		let db_ctx = ctx.database().expect("a database context");
		let table = TableName::from("d");
		let def = ctx
			.txn()
			.expect_tb_index(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, &table, "d_body")
			.await
			.expect("the index");
		let index = IndexRef::new(std::slice::from_ref(def.as_ref()), 0);
		let services = EngineServices::new(&ctx);
		let fti = services.open_full_text(&table, &index).await.expect("the index opens");
		let plan = fti
			.plan_query(&StreamingAnalyzerFunction::new(&ctx), "hello".to_owned())
			.await
			.expect("the query plans");
		let reads = &plan.reads()[0];

		let entries = read_all(&ctx, &services, reads.batches.clone().into()).await.expect("read");
		let keys =
			read_all_keys(&ctx, &services, reads.batches.clone().into()).await.expect("read");
		assert_eq!(entries.len(), DOCS, "one change batch per committed statement");
		assert!(entries.windows(2).all(|pair| pair[0].0 < pair[1].0), "in key order, once each");
		let entry_keys: Vec<_> = entries.into_iter().map(|(key, _)| key).collect();
		assert_eq!(keys, entry_keys, "the key read answers the entry read's keys");
		let none =
			read_all_keys(&ctx, &services, reads.changes.clone().into()).await.expect("read");
		assert!(none.is_empty(), "no per-document change key is written");

		let ds = db.datastore();
		let session = TestDb::owner();
		let count = |sql: &'static str| {
			let session = &session;
			async move {
				let mut responses = ds.execute(sql, session, None).await.expect("the query runs");
				responses.remove(0).result.expect("a count")
			}
		};
		assert_eq!(
			count("SELECT count() FROM d WHERE body @@ 'hello' GROUP ALL").await,
			count("SELECT count() FROM d GROUP ALL").await,
			"the match answers every document"
		);
	}
	/// A statement that scores, orders by and highlights the clause its scan
	/// answers reads the query's terms once: the scan reads them, and every
	/// call reads the scan's. Nor does a call read a row's doc id or score it:
	/// it takes the doc id the scan listed the row with, and the score the scan
	/// computed for the row's batch.
	#[tokio::test]
	async fn a_scored_scan_reads_its_terms_once() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS; \
			 CREATE d:1 SET body = 'hello world'; \
			 CREATE d:2 SET body = 'hello hello there'; \
			 CREATE d:3 SET body = 'goodbye';",
			false,
		)
		.await;
		let sql = "SELECT id, search::score(1) AS s, search::highlight('<b>', '</b>', 1) AS h \
			FROM d WHERE body @1@ 'hello' ORDER BY s DESC";
		crate::exec::function::TERM_READS.with(|reads| reads.set(0));
		crate::exec::function::DOC_ID_READS.with(|reads| reads.set(0));
		crate::exec::function::ROW_SCORES.with(|scores| scores.set(0));
		let mut responses =
			db.datastore().execute(sql, &TestDb::owner(), None).await.expect("the query runs");
		let rows = responses.remove(0).result.expect("rows");
		assert_eq!(crate::exec::function::TERM_READS.with(|reads| reads.get()), 1);
		assert_eq!(
			crate::exec::function::DOC_ID_READS.with(|reads| reads.get()),
			0,
			"every call takes the doc id the scan listed its row with"
		);
		assert_eq!(
			crate::exec::function::ROW_SCORES.with(|scores| scores.get()),
			0,
			"every score is the one the scan computed with its row's batch"
		);
		let surrealdb_types::Value::Array(rows) = rows else {
			panic!("the statement answers an array of rows: {rows:?}");
		};
		assert_eq!(rows.len(), 2, "two documents carry the term: {rows:?}");
		for row in rows.iter() {
			let surrealdb_types::Value::Object(row) = row else {
				panic!("each row is an object: {row:?}");
			};
			assert!(
				matches!(row.get("s"), Some(surrealdb_types::Value::Number(_))),
				"each row is scored: {row:?}"
			);
			assert!(
				matches!(row.get("h"), Some(surrealdb_types::Value::String(h)) if h.contains("<b>hello</b>")),
				"each row is highlighted: {row:?}"
			);
		}
	}
	/// The scores a scan computes with its batches are the scores each row
	/// gets scored alone: the same statement with the scan taken out of the
	/// plan (`WITH NOINDEX`) scores every row in `search::score` itself.
	#[tokio::test]
	async fn batch_scores_are_the_scores_rows_get_alone() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25; \
			 CREATE d:1 SET body = 'hello world'; \
			 CREATE d:2 SET body = 'hello hello there'; \
			 CREATE d:3 SET body = 'world without end'; \
			 CREATE d:4 SET body = 'goodbye'; \
			 CREATE d:5 SET body = 'hello world hello world hello';",
			false,
		)
		.await;
		let run = |sql: &'static str| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				responses.remove(0).result.expect("rows")
			}
		};
		crate::exec::function::ROW_SCORES.with(|scores| scores.set(0));
		let scanned = run("SELECT id, search::score(1) AS s FROM d \
			WHERE body @1@ 'hello world' ORDER BY id")
		.await;
		assert_eq!(crate::exec::function::ROW_SCORES.with(|scores| scores.get()), 0);
		let alone = run("SELECT id, search::score(1) AS s FROM d WITH NOINDEX \
			WHERE body @1@ 'hello world' ORDER BY id")
		.await;
		assert!(crate::exec::function::ROW_SCORES.with(|scores| scores.get()) > 0);
		assert_eq!(scanned, alone);
	}
	/// A row no scan scored is scored once however many calls score it: the
	/// first remembers its score for the rest.
	#[tokio::test]
	async fn a_row_is_scored_once_by_the_calls_that_score_it() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25; \
			 CREATE d:1 SET body = 'hello world'; \
			 CREATE d:2 SET body = 'hello there'; \
			 CREATE d:3 SET body = 'goodbye';",
			false,
		)
		.await;
		crate::exec::function::ROW_SCORES.with(|scores| scores.set(0));
		let mut responses = db
			.datastore()
			.execute(
				"SELECT search::score(1) AS a, search::score(1) * 2 AS b FROM d WITH NOINDEX \
				 WHERE body @1@ 'hello'",
				&TestDb::owner(),
				None,
			)
			.await
			.expect("runs");
		let rows = responses.remove(0).result.expect("rows");
		let surrealdb_types::Value::Array(rows) = rows else {
			panic!("the statement answers an array of rows: {rows:?}");
		};
		assert_eq!(rows.len(), 2, "{rows:?}");
		assert_eq!(
			crate::exec::function::ROW_SCORES.with(|scores| scores.get()),
			2,
			"one score per row, not per call"
		);
	}
	/// A sort under a literal `LIMIT` ordered first by the score of the clause
	/// the full-text scan answers has the scan reject hits by its threshold;
	/// any other order, no limit, or an op a rejected hit would skip that may
	/// raise leaves the scan as it is.
	#[tokio::test]
	async fn a_bounded_sort_by_the_clause_score_pushes_its_threshold_into_the_scan() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25; \
			 CREATE d:1 SET body = 'hello world', n = 1;",
			false,
		)
		.await;
		let explain = |sql: &'static str| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				format!("{:?}", responses.remove(0).result.expect("a plan"))
			}
		};
		let pushed = [
			"EXPLAIN SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello' \
			 ORDER BY s DESC LIMIT 2",
			"EXPLAIN SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello' \
			 ORDER BY s DESC, n LIMIT 2 START 1",
		];
		for sql in pushed {
			let plan = explain(sql).await;
			assert!(plan.contains("topk_pushdown: score"), "{sql}\n{plan}");
		}
		let kept = [
			"EXPLAIN SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello' \
			 ORDER BY s DESC",
			"EXPLAIN SELECT id, search::score(1) * 2 AS s FROM d WHERE body @1@ 'hello' \
			 ORDER BY s DESC LIMIT 2",
			"EXPLAIN SELECT id, n FROM d WHERE body @1@ 'hello' ORDER BY n DESC LIMIT 2",
			// A residual conjunct that may raise would go unjudged for a hit rejected unread.
			"EXPLAIN SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello' \
			 AND <int> n > 0 ORDER BY s DESC LIMIT 2",
			// A call with more arguments than the clause raises however it is answered.
			"EXPLAIN SELECT id, search::score(1, 2) AS s FROM d WHERE body @1@ 'hello' \
			 ORDER BY s DESC LIMIT 2",
			// So would a later sort key that may raise.
			"EXPLAIN SELECT id, search::score(1) AS s, <int> n AS m FROM d WHERE body @1@ 'hello' \
			 ORDER BY s DESC, m LIMIT 2",
		];
		for sql in kept {
			let plan = explain(sql).await;
			assert!(plan.contains("FullTextScan"), "{sql}\n{plan}");
			assert!(!plan.contains("topk_pushdown: score"), "{sql}\n{plan}");
		}
	}
	/// A hit is passed over only when its score cannot beat the threshold: a
	/// tie is passed over only when the order has a single key, and a better
	/// score is kept with its doc id.
	#[test]
	fn a_threshold_passes_over_only_hits_that_cannot_beat_it() {
		let doc_ids = vec![1, 2, 3];
		let scores = vec![1.0, 2.0, 3.0];
		let threshold = Value::Number(Number::Float(2.0));
		assert_eq!(
			admitted(doc_ids.clone(), scores.clone(), &threshold, SortDirection::Desc, true),
			(vec![3], vec![3.0]),
			"worse and tied are passed over"
		);
		assert_eq!(
			admitted(doc_ids.clone(), scores.clone(), &threshold, SortDirection::Desc, false),
			(vec![2, 3], vec![2.0, 3.0]),
			"a tie may still win on a later key"
		);
		assert_eq!(
			admitted(doc_ids, scores, &threshold, SortDirection::Asc, true),
			(vec![1], vec![1.0]),
			"ascending keeps the lower scores"
		);
	}

	/// The setup of a table of `count` documents each carrying `hello` once in
	/// a body whose length, and so whose score, is its own, in an order that is
	/// not the order of the scores; `n` numbers them and `keep` flags every
	/// other one. As many documents again and a half carry no `hello`, so the
	/// term is rare enough to weigh well above the scores' rounding: a term
	/// every document carries weighs next to nothing, and its scores would
	/// differ by less than a threshold can tell apart.
	fn scored_corpus(count: usize) -> String {
		let mut setup = String::from(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25;",
		);
		// Each document's padding, and so its score's rank, shuffled with a
		// fixed seed: the order the scan reads the hits in has nothing to do
		// with the order of their scores.
		let mut paddings: Vec<usize> = (0..count).collect();
		let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
		for at in (1..paddings.len()).rev() {
			seed ^= seed << 13;
			seed ^= seed >> 7;
			seed ^= seed << 17;
			paddings.swap(at, (seed % (at as u64 + 1)) as usize);
		}
		for (i, padding) in paddings.into_iter().enumerate() {
			let body = format!("hello{}", " x".repeat(padding));
			setup.push_str(&format!(
				" CREATE d:{i} SET body = '{body}', n = {i}, keep = {};",
				i % 2 == 0
			));
		}
		for j in 0..count + count / 2 {
			setup.push_str(&format!(
				" CREATE d:{} SET body = 'filler', n = -1, keep = false;",
				count + j
			));
		}
		setup
	}

	/// A bounded sort by the clause's score answers what the same statement
	/// answers with no index (where no threshold reaches a scan), while the
	/// scan reads far fewer records than the clause matches.
	///
	/// The scan's pipeline runs serially, so each row reaches the sort as it is
	/// read and the sort publishes its threshold once it holds the rows it
	/// keeps.
	#[tokio::test]
	async fn a_score_threshold_keeps_the_answer_and_reads_fewer_records() {
		const COUNT: usize = 300;
		let db = TestDb::quiescent(&scored_corpus(COUNT), false).await;
		let run = |sql: String| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(&sql, &TestDb::owner(), None).await.expect("runs");
				responses.remove(0).result.expect("rows")
			}
		};
		for (clause, tail) in
			[("", "LIMIT 5"), ("", "LIMIT 5 START 3"), (" AND keep = true", "LIMIT 5")]
		{
			let indexed = format!(
				"SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello'{clause} \
				 ORDER BY s DESC {tail}"
			);
			let unindexed = format!(
				"SELECT id, search::score(1) AS s FROM d WITH NOINDEX \
				 WHERE body @1@ 'hello'{clause} ORDER BY s DESC {tail}"
			);
			HITS_READ.with(|read| read.set(0));
			let answer = run(indexed.clone()).await;
			let read = HITS_READ.with(|read| read.get());
			assert_eq!(answer, run(unindexed).await, "{indexed}");
			if clause.is_empty() {
				assert!(read < COUNT / 4, "{indexed}: {read} of {COUNT} records read");
			}
		}
	}

	/// A bounded sort by a sum of clause scores over an `OR` of clauses, a lone
	/// clause's score included, has the index union of the clauses' full-text
	/// scans reject hits by it, the `OR` the union answers dropped; one whose key
	/// is not such a sum, or reads a clause the union does not answer, does not.
	#[tokio::test]
	async fn a_bounded_sort_by_a_sum_of_clause_scores_pushes_its_threshold_into_the_union() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25; \
			 DEFINE INDEX d_title ON d FIELDS title FULLTEXT ANALYZER simple BM25; \
			 CREATE d:1 SET body = 'hello world', title = 'hello', n = 1;",
			false,
		)
		.await;
		let explain = |sql: &'static str| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				format!("{:?}", responses.remove(0).result.expect("a plan"))
			}
		};
		let pushed = [
			"EXPLAIN SELECT id, search::score(1) + search::score(2) AS s FROM d \
			 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC LIMIT 2",
			"EXPLAIN SELECT id, search::score(1) + search::score(2) AS s FROM d \
			 WHERE body @1@ 'hello' OR title @2@ 'hello' ORDER BY s DESC LIMIT 2",
			"EXPLAIN SELECT id, search::score(2) AS s FROM d \
			 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC LIMIT 2",
			"EXPLAIN SELECT id, (search::score(2) + search::score(1)) + search::score(2) AS s FROM d \
			 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC LIMIT 2",
		];
		for sql in pushed {
			let plan = explain(sql).await;
			assert!(plan.contains("UnionIndexScan"), "{sql}\n{plan}");
			assert!(plan.contains("topk_pushdown: score"), "{sql}\n{plan}");
			assert!(!plan.contains("Filter"), "{sql}\n{plan}");
		}
		let kept = [
			"EXPLAIN SELECT id, search::score(1) + search::score(2) AS s FROM d \
			 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC",
			"EXPLAIN SELECT id, search::score(1) + 1 AS s FROM d \
			 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC LIMIT 2",
			"EXPLAIN SELECT id, search::score(1) - search::score(2) AS s FROM d \
			 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC LIMIT 2",
			"EXPLAIN SELECT id, search::score(1) + search::score(3) AS s FROM d \
			 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC LIMIT 2",
			"EXPLAIN SELECT id, search::score(1) + search::score(2) AS s, <int> n AS m FROM d \
			 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC, m LIMIT 2",
		];
		for sql in kept {
			let plan = explain(sql).await;
			assert!(!plan.contains("topk_pushdown: score"), "{sql}\n{plan}");
		}
	}

	/// A bounded sort by the sum of two clauses' scores over their `OR` answers
	/// what the same statement answers with no index, while the union's
	/// branches read far fewer records than the clauses match.
	#[tokio::test]
	async fn a_summed_score_threshold_keeps_the_answer_and_reads_fewer_records() {
		const COUNT: usize = 300;
		let mut setup = scored_corpus(COUNT);
		// Every third document carries `world` too, at a frequency of its own,
		// and as many again carry `world` alone.
		for i in (0..COUNT).step_by(3) {
			setup.push_str(&format!(
				" UPDATE d:{i} SET body = string::concat(body, '{}');",
				" world".repeat(1 + i % 4)
			));
		}
		for j in 0..COUNT / 3 {
			setup.push_str(&format!(
				" CREATE d:{} SET body = 'world{}', n = -2, keep = false;",
				10_000 + j,
				" y".repeat(j % 11)
			));
		}
		let db = TestDb::quiescent(&setup, false).await;
		let run = |sql: String| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(&sql, &TestDb::owner(), None).await.expect("runs");
				responses.remove(0).result.expect("rows")
			}
		};
		// A lone clause's score is zero for every hit of the other branch, so its
		// threshold rises only as the first branch meets hits of its clause.
		let matched = COUNT + COUNT / 3;
		for (sum, tail, bound) in [
			("search::score(1) + search::score(2)", "LIMIT 5", matched / 4),
			("search::score(2) + search::score(1)", "LIMIT 5 START 3", matched / 4),
			("search::score(2)", "LIMIT 5", matched / 2),
		] {
			let indexed = format!(
				"SELECT id, {sum} AS s FROM d WHERE body @1@ 'hello' OR body @2@ 'world' \
				 ORDER BY s DESC {tail}"
			);
			let unindexed = format!(
				"SELECT id, {sum} AS s FROM d WITH NOINDEX \
				 WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC {tail}"
			);
			HITS_READ.with(|read| read.set(0));
			let answer = run(indexed.clone()).await;
			let read = HITS_READ.with(|read| read.get());
			assert_eq!(answer, run(unindexed).await, "{indexed}");
			assert!(read < bound, "{indexed}: {read} of {matched} records read");
		}
	}

	/// A document both clauses match reaches the top of a sum that neither of
	/// its scores alone would reach: each is below a document matching one
	/// clause, listed first by its branch. Both branches judge it by the sum,
	/// so neither passes it over; judged by its own clause's score, both would.
	#[tokio::test]
	async fn a_hit_both_clauses_match_is_judged_by_its_sum() {
		let mut setup = String::from(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25; \
			 CREATE d:p SET body = 'apple apple apple apple'; \
			 CREATE d:q SET body = 'berry berry berry berry'; \
			 CREATE d:x SET body = 'apple berry';",
		);
		for i in 0..20 {
			setup.push_str(&format!(" CREATE d:f{i} SET body = 'filler';"));
		}
		let db = TestDb::quiescent(&setup, false).await;
		let run = |sql: &'static str| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				responses.remove(0).result.expect("rows")
			}
		};
		// The shape the test relies on: x scores below p under the first clause
		// and below q under the second, and above both summed.
		let scores = run("SELECT id, search::score(1) AS a, search::score(2) AS b FROM d \
			 WHERE body @1@ 'apple' OR body @2@ 'berry' ORDER BY id")
		.await;
		let score = |id: &str, clause: &str| -> f64 {
			let surrealdb_types::Value::Array(rows) = &scores else {
				panic!("rows: {scores:?}");
			};
			rows.iter()
				.find_map(|row| {
					let surrealdb_types::Value::Object(row) = row else {
						return None;
					};
					(format!("{:?}", row.get("id")).contains(id))
						.then(|| match row.get(clause) {
							Some(surrealdb_types::Value::Number(n)) => n.to_f64(),
							other => panic!("{id}.{clause}: {other:?}"),
						})
						.flatten()
				})
				.unwrap_or_else(|| panic!("no score for {id}.{clause} in {scores:?}"))
		};
		let (xa, xb, pa, qb) =
			(score("\"x\"", "a"), score("\"x\"", "b"), score("\"p\"", "a"), score("\"q\"", "b"));
		assert!(xa < pa && xb < qb && xa + xb > pa.max(qb), "x {xa} + {xb}, p {pa}, q {qb}");

		// p is listed first and sets the threshold; x then passes it by its sum
		// alone, and q and x's second listing do not, so two records are read.
		HITS_READ.with(|read| read.set(0));
		let top = run("SELECT id, search::score(1) + search::score(2) AS s \
			 FROM d WHERE body @1@ 'apple' OR body @2@ 'berry' ORDER BY s DESC LIMIT 1")
		.await;
		let read = HITS_READ.with(|read| read.get());
		assert!(format!("{top:?}").contains("\"x\""), "the top row: {top:?}");
		assert_eq!(read, 2, "records read for {top:?}");
	}

	/// A reference written on several `MATCHES` resolves to the last of them, so
	/// its score may not be the score of the scan or branch carrying the
	/// reference: no scan rejects hits by it, and the answer is the one the
	/// statement gives with no index.
	#[tokio::test]
	async fn a_reused_clause_reference_pushes_no_threshold() {
		let mut setup = String::from(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25; \
			 DEFINE INDEX d_title ON d FIELDS title FULLTEXT ANALYZER simple BM25;",
		);
		for i in 0..40 {
			let body = match i % 4 {
				0 => format!("apple{}", " apple".repeat(i % 5)),
				1 => format!("apple berry{}", " berry".repeat(i % 7)),
				2 => format!("berry{}", " x".repeat(i % 3)),
				_ => "filler".to_string(),
			};
			let title = if i % 3 == 0 {
				format!("apple{}", " y".repeat(i % 4))
			} else {
				"berry".to_string()
			};
			setup.push_str(&format!(" CREATE d:{i} SET body = '{body}', title = '{title}';"));
		}
		let db = TestDb::quiescent(&setup, false).await;
		let run = |sql: String| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(&sql, &TestDb::owner(), None).await.expect("runs");
				format!("{:?}", responses.remove(0).result.expect("rows"))
			}
		};
		for (key, filter) in [
			("search::score(1)", "body @1@ 'apple' OR body @1@ 'berry'"),
			("search::score(1)", "body @1@ 'berry' OR title @1@ 'apple'"),
			(
				"search::score(1) + search::score(2)",
				"body @1@ 'apple' OR body @2@ 'berry' OR title @2@ 'apple'",
			),
			("search::score(1)", "body @1@ 'apple' AND (body @1@ 'berry' OR body @2@ 'apple')"),
		] {
			let indexed =
				format!("SELECT id, {key} AS s FROM d WHERE {filter} ORDER BY s DESC LIMIT 3");
			let plan = run(format!("EXPLAIN {indexed}")).await;
			assert!(!plan.contains("topk_pushdown: score"), "{indexed}\n{plan}");
			let unindexed = format!(
				"SELECT id, {key} AS s FROM d WITH NOINDEX WHERE {filter} ORDER BY s DESC LIMIT 3"
			);
			assert_eq!(run(indexed.clone()).await, run(unindexed).await, "{indexed}");
		}
	}

	/// The scores of the rows `rows` holds, as `f32` bits, in order.
	fn scores(rows: surrealdb_types::Value) -> Vec<u64> {
		let surrealdb_types::Value::Array(rows) = rows else {
			panic!("rows: {rows:?}");
		};
		rows.iter()
			.map(|row| {
				if let surrealdb_types::Value::Object(row) = row {
					if let Some(surrealdb_types::Value::Number(n)) = row.get("s") {
						n.to_f64().expect("a float").to_bits()
					} else {
						panic!("a score: {:?}", row.get("s"));
					}
				} else {
					panic!("a row: {row:?}");
				}
			})
			.collect()
	}
	/// A scan passes hits over by their posting blocks' bounds only where no
	/// hit the bound covers could beat the sort's threshold: the best scores
	/// are those the statement gives with no index, for one term, for an `AND`
	/// and an `OR` of terms, for several `k` and length weights `b`, with
	/// documents rewritten and indexed since the compaction among the best. The
	/// bounds pass hits over in each case.
	#[tokio::test]
	async fn block_bounds_pass_over_only_hits_that_cannot_beat_the_threshold() {
		let words = ["apple", "berry", "cherry"];
		for b in ["0", "0.75", "1"] {
			let mut setup = format!(
				"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
				 DEFINE TABLE d SCHEMALESS; \
				 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25(1.2, {b});"
			);
			let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
			let mut next = |bound: u64| {
				seed ^= seed << 13;
				seed ^= seed >> 7;
				seed ^= seed << 17;
				seed % bound
			};
			for i in 0..1600 {
				let mut body = Vec::new();
				for _ in 0..1 + next(24) {
					body.push(match next(20) {
						0..=5 => words[0],
						6..=8 => words[1],
						9 => words[2],
						_ => "filler",
					});
				}
				setup.push_str(&format!(" CREATE d:{i} SET body = '{}';", body.join(" ")));
			}
			let db = TestDb::quiescent(&setup, false).await;
			crate::kvs::Datastore::index_compaction(
				std::sync::Arc::clone(db.datastore()),
				std::time::Duration::from_secs(1),
				tokio_util::sync::CancellationToken::new(),
			)
			.await
			.expect("compaction runs");
			let run = |sql: String| {
				let db = &db;
				async move {
					let mut responses =
						db.datastore().execute(&sql, &TestDb::owner(), None).await.expect("runs");
					responses.remove(0).result.expect("rows")
				}
			};
			// Rewritten and new documents the compaction has not folded, each carrying a
			// term far more often than any compacted document: they are the best, and
			// the blocks their old postings sit in bound them far below what they score.
			// Their doc ids are among the last a scan lists, so it reaches them with the
			// threshold set by the compacted documents before them.
			let many = |word: &str| vec![word; 40].join(" ");
			run(format!(
				"UPDATE d:1590 SET body = '{} berry'; UPDATE d:1595 SET body = '{} apple'; \
				 CREATE d:5000 SET body = '{} apple cherry';",
				many("apple"),
				many("cherry"),
				many("berry")
			))
			.await;
			BLOCK_SKIPS.with(|skips| skips.set(0));
			for (op, query) in
				[("@1@", "apple"), ("@1@", "apple berry"), ("@1,OR@", "berry cherry")]
			{
				for k in [1, 10, 100] {
					let indexed = format!(
						"SELECT id, search::score(1) AS s FROM d WHERE body {op} '{query}' \
						 ORDER BY s DESC LIMIT {k}"
					);
					let unindexed = format!(
						"SELECT id, search::score(1) AS s FROM d WITH NOINDEX WHERE body {op} '{query}' \
						 ORDER BY s DESC LIMIT {k}"
					);
					assert_eq!(
						scores(run(indexed.clone()).await),
						scores(run(unindexed).await),
						"BM25(1.2, {b}): {indexed}"
					);
				}
			}
			let skipped = BLOCK_SKIPS.with(|skips| skips.get());
			assert!(skipped > 0, "BM25(1.2, {b}): no hit was passed over by a block bound");
		}
	}

	/// An index union whose sort keeps the highest sum of clause scores passes
	/// hits over by the sum of every clause's block bounds only where no hit the
	/// bounds cover could beat the sort's threshold. The answer is the one the
	/// statement gives with no index, row for row, for two clauses on one column
	/// and on two columns, three clauses, single- and multi-key orders, several
	/// `k` and BM25 parameters, with documents rewritten and indexed since the
	/// compaction among the best. The bounds pass hits over for each shape.
	#[tokio::test]
	async fn block_bounds_pass_over_only_hits_whose_sum_cannot_beat_the_threshold() {
		let words = ["apple", "berry", "cherry"];
		for (k1, b) in [("1.2", "0"), ("1.2", "0.75"), ("2", "1")] {
			let mut setup = format!(
				"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
				 DEFINE TABLE d SCHEMALESS; \
				 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25({k1}, {b}); \
				 DEFINE INDEX d_title ON d FIELDS title FULLTEXT ANALYZER simple BM25({k1}, {b});"
			);
			let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
			let mut next = |bound: u64| {
				seed ^= seed << 13;
				seed ^= seed >> 7;
				seed ^= seed << 17;
				seed % bound
			};
			let mut text = |len: u64| {
				let mut text = Vec::new();
				for _ in 0..1 + next(len) {
					text.push(match next(20) {
						0..=5 => words[0],
						6..=8 => words[1],
						9 => words[2],
						_ => "filler",
					});
				}
				text.join(" ")
			};
			for i in 0..1600 {
				let (body, title) = (text(24), text(8));
				setup.push_str(&format!(" CREATE d:{i} SET body = '{body}', title = '{title}';"));
			}
			let db = TestDb::quiescent(&setup, false).await;
			crate::kvs::Datastore::index_compaction(
				std::sync::Arc::clone(db.datastore()),
				std::time::Duration::from_secs(1),
				tokio_util::sync::CancellationToken::new(),
			)
			.await
			.expect("compaction runs");
			let run = |sql: String| {
				let db = &db;
				async move {
					let mut responses =
						db.datastore().execute(&sql, &TestDb::owner(), None).await.expect("runs");
					responses.remove(0).result.expect("rows")
				}
			};
			// Rewritten and new documents the compaction has not folded, carrying
			// the terms far more often than any compacted document, in one clause
			// or both: they are the best, and the blocks their old postings sit in
			// bound them far below what they score.
			let many = |word: &str| vec![word; 40].join(" ");
			run(format!(
				"UPDATE d:1590 SET body = '{} berry'; \
				 UPDATE d:1595 SET body = 'apple', title = '{}'; \
				 CREATE d:5000 SET body = '{}', title = '{}';",
				many("apple"),
				many("cherry"),
				many("berry"),
				many("berry")
			))
			.await;
			for (sum, condition) in [
				("search::score(1) + search::score(2)", "body @1@ 'apple' OR body @2@ 'berry'"),
				("search::score(1) + search::score(2)", "body @1@ 'apple' OR title @2@ 'cherry'"),
				(
					"search::score(1) + search::score(2) + search::score(3)",
					"body @1@ 'apple' OR body @2@ 'berry' OR title @3@ 'cherry'",
				),
			] {
				BLOCK_SKIPS.with(|skips| skips.set(0));
				for order in ["s DESC", "s DESC, id ASC"] {
					for k in [1, 10, 100] {
						let indexed = format!(
							"SELECT id, {sum} AS s FROM d WHERE {condition} \
							 ORDER BY {order} LIMIT {k}"
						);
						let unindexed = format!(
							"SELECT id, {sum} AS s FROM d WITH NOINDEX WHERE {condition} \
							 ORDER BY {order} LIMIT {k}"
						);
						let (indexed_rows, unindexed_rows) =
							(run(indexed.clone()).await, run(unindexed).await);
						if order == "s DESC" {
							// Rows tied on the only key may come in either order.
							assert_eq!(
								scores(indexed_rows),
								scores(unindexed_rows),
								"BM25({k1}, {b}): {indexed}"
							);
						} else {
							assert_eq!(indexed_rows, unindexed_rows, "BM25({k1}, {b}): {indexed}");
						}
					}
				}
				let skipped = BLOCK_SKIPS.with(|skips| skips.get());
				assert!(skipped > 0, "BM25({k1}, {b}): no hit of `{condition}` was passed over");
			}
		}
	}

	/// Ranked full-text queries answer exactly as exhaustive scoring does, over
	/// random corpora whose terms span several posting blocks: the rows and
	/// scores of a query with no index, for one term, an `AND` of terms, an `OR`
	/// of terms (one a rare term, one repeated), and a sum of clauses combined
	/// with `OR`, for `k` of 1, 10 and 100, BM25 `b` of 0, 0.75 and 1, single-
	/// and multi-key orders, with documents tied on their score, documents
	/// rewritten and indexed since the compaction among the best, and a share of
	/// the index left uncompacted. Hits are passed over in each corpus.
	#[tokio::test]
	async fn ranked_queries_answer_as_exhaustive_scoring() {
		for (round, (k1, b)) in [("1.2", "0"), ("1.2", "0.75"), ("2", "1")].into_iter().enumerate()
		{
			let mut setup = format!(
				"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
				 DEFINE TABLE d SCHEMALESS; \
				 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25({k1}, {b});"
			);
			let mut seed: u64 =
				0x9e37_79b9_7f4a_7c15 ^ (round as u64 + 1).wrapping_mul(0x2545_f491);
			let mut next = |bound: u64| {
				seed ^= seed << 13;
				seed ^= seed >> 7;
				seed ^= seed << 17;
				seed % bound
			};
			let text = |next: &mut dyn FnMut(u64) -> u64| {
				let mut text = Vec::new();
				for _ in 0..1 + next(30) {
					text.push(match next(40) {
						0..=11 => "common",
						12..=16 => "mid",
						17 => "rare",
						_ => "filler",
					});
				}
				text.join(" ")
			};
			let mut last = String::new();
			for i in 0..2400 {
				// Every so often a document repeats the one before it, so the two tie.
				if i % 37 != 0 {
					last = text(&mut next);
				}
				setup.push_str(&format!(" CREATE d:{i} SET body = '{last}';"));
			}
			let db = TestDb::quiescent(&setup, false).await;
			crate::kvs::Datastore::index_compaction(
				std::sync::Arc::clone(db.datastore()),
				std::time::Duration::from_secs(1),
				tokio_util::sync::CancellationToken::new(),
			)
			.await
			.expect("compaction runs");
			let run = |sql: String| {
				let db = &db;
				async move {
					let mut responses =
						db.datastore().execute(&sql, &TestDb::owner(), None).await.expect("runs");
					responses.remove(0).result.expect("rows")
				}
			};
			// Documents written since the compaction: a share of the index left
			// uncompacted, and rewritten and new documents carrying the terms far
			// more often than any compacted one, which are the best.
			let mut writes = String::new();
			for i in 2400..2700 {
				writes.push_str(&format!(" CREATE d:{i} SET body = '{}';", text(&mut next)));
			}
			let many = |word: &str| vec![word; 40].join(" ");
			writes.push_str(&format!(
				" UPDATE d:2390 SET body = '{} rare'; UPDATE d:5 SET body = '{} mid'; \
				 CREATE d:5000 SET body = '{} common'; CREATE d:5001 SET body = '{} common';",
				many("common"),
				many("rare"),
				many("mid"),
				many("mid"),
			));
			run(writes).await;
			let (mut skipped, mut cut) = (0, 0);
			for (score, condition) in [
				("search::score(1)", "body @1@ 'common'"),
				("search::score(1)", "body @1@ 'rare'"),
				("search::score(1)", "body @1@ 'common mid'"),
				("search::score(1)", "body @1,OR@ 'common rare'"),
				("search::score(1)", "body @1,OR@ 'mid common mid rare'"),
				("search::score(1) + search::score(2)", "body @1@ 'common' OR body @2@ 'rare'"),
				(
					"search::score(1) + search::score(2) + search::score(3)",
					"body @1@ 'mid' OR body @2@ 'common rare' OR body @3@ 'rare'",
				),
			] {
				for order in ["s DESC", "s DESC, id ASC"] {
					for k in [1, 10, 100] {
						let indexed = format!(
							"SELECT id, {score} AS s FROM d WHERE {condition} \
							 ORDER BY {order} LIMIT {k}"
						);
						let unindexed = format!(
							"SELECT id, {score} AS s FROM d WITH NOINDEX WHERE {condition} \
							 ORDER BY {order} LIMIT {k}"
						);
						BLOCK_SKIPS.with(|skips| skips.set(0));
						UNION_CUTS.with(|cuts| cuts.set(0));
						let indexed_rows = run(indexed.clone()).await;
						skipped += BLOCK_SKIPS.with(|skips| skips.get());
						cut += UNION_CUTS.with(|cuts| cuts.get());
						let unindexed_rows = run(unindexed).await;
						if order == "s DESC" {
							// Rows tied on the only key may come in either order.
							assert_eq!(
								scores(indexed_rows),
								scores(unindexed_rows),
								"BM25({k1}, {b}): {indexed}"
							);
						} else {
							assert_eq!(indexed_rows, unindexed_rows, "BM25({k1}, {b}): {indexed}");
						}
					}
				}
			}
			assert!(skipped > 0, "BM25({k1}, {b}): no hit was passed over");
			assert!(cut > 0, "BM25({k1}, {b}): no union branch was cut by its clauses' ceilings");
		}
	}

	/// A branch of an index union cut to the hits carrying another clause's
	/// terms, once its own clause's ceiling cannot beat the threshold, still
	/// lists the documents written since the compaction: one carrying only the
	/// cut clause's term, far more often than any compacted document does, is
	/// among the best, as it is with no index.
	#[tokio::test]
	async fn a_cut_union_branch_keeps_its_uncompacted_documents() {
		let mut setup = String::from(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25(1.2, 0);",
		);
		// `apple` and `berry` each in a tenth of the documents, never together,
		// once each but for one `berry` document.
		for i in 0..2000 {
			let body = match i % 10 {
				0 => "apple filler",
				5 if i == 1005 => "berry berry berry berry berry",
				5 => "berry filler",
				_ => "filler",
			};
			setup.push_str(&format!(" CREATE d:{i} SET body = '{body}';"));
		}
		let db = TestDb::quiescent(&setup, false).await;
		crate::kvs::Datastore::index_compaction(
			std::sync::Arc::clone(db.datastore()),
			std::time::Duration::from_secs(1),
			tokio_util::sync::CancellationToken::new(),
		)
		.await
		.expect("compaction runs");
		let run = |sql: String| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(&sql, &TestDb::owner(), None).await.expect("runs");
				responses.remove(0).result.expect("rows")
			}
		};
		// Early documents that raise the threshold above `apple`'s ceiling, and a
		// later one that beats them.
		run("UPDATE d:0 SET body = 'apple apple apple'; UPDATE d:10 SET body = 'apple apple apple'; \
			 UPDATE d:1990 SET body = 'apple apple apple apple apple apple'"
			.to_owned())
		.await;
		let sql = |noindex: &str| {
			format!(
				"SELECT id, search::score(1) + search::score(2) AS s FROM d {noindex} \
				 WHERE body @1@ 'apple' OR body @2@ 'berry' ORDER BY s DESC LIMIT 3"
			)
		};
		UNION_CUTS.with(|cuts| cuts.set(0));
		let indexed = run(sql("")).await;
		assert!(UNION_CUTS.with(|cuts| cuts.get()) > 0, "the `apple` branch is cut");
		let unindexed = run(sql("WITH NOINDEX")).await;
		assert!(format!("{indexed:?}").contains("1990"), "{indexed:?}");
		assert_eq!(scores(indexed), scores(unindexed));
	}

	/// Where scores tie, the threshold passes over no tie of a multi-key order,
	/// so the later key still decides between the tied rows.
	#[tokio::test]
	async fn tied_scores_are_left_to_the_later_key() {
		let mut setup = String::from(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25;",
		);
		for i in 0..120 {
			let body = format!("hello{}", " x".repeat(i % 3));
			setup.push_str(&format!(" CREATE d:{i} SET body = '{body}', n = {};", 1000 - i));
		}
		let db = TestDb::quiescent(&setup, false).await;
		let run = |sql: &'static str| {
			let db = &db;
			async move {
				let mut responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				responses.remove(0).result.expect("rows")
			}
		};
		assert_eq!(
			run("SELECT id, search::score(1) AS s, n FROM d WHERE body @1@ 'hello' \
				 ORDER BY s DESC, n LIMIT 7")
			.await,
			run("SELECT id, search::score(1) AS s, n FROM d WITH NOINDEX WHERE body @1@ 'hello' \
				 ORDER BY s DESC, n LIMIT 7")
			.await,
		);
	}
	/// A read-only query serves the compacted state of terms a compaction has
	/// folded from the node's cache, keyed by the generation the compaction
	/// left: a second query reads none of it again, a write after it is still
	/// seen through the deltas read beside it, a compaction moves the
	/// generation and the next query reads afresh, and a query in a write
	/// transaction neither reads nor fills the cache.
	#[tokio::test]
	async fn compacted_terms_are_served_from_the_cache_of_their_generation() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25; \
			 CREATE d:1 SET body = 'hello world'; \
			 CREATE d:2 SET body = 'hello there'; \
			 CREATE d:3 SET body = 'goodbye';",
			false,
		)
		.await;
		let compact = || async {
			crate::kvs::Datastore::index_compaction(
				std::sync::Arc::clone(db.datastore()),
				std::time::Duration::from_secs(1),
				tokio_util::sync::CancellationToken::new(),
			)
			.await
			.expect("compaction runs");
		};
		let query = |sql: &'static str| {
			let db = &db;
			async move {
				crate::exec::function::TERM_STATES_READ.with(|read| read.set(0));
				let mut responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				let rows = responses.pop().expect("a response").result.expect("rows");
				(rows, crate::exec::function::TERM_STATES_READ.with(|read| read.get()))
			}
		};
		const IDS: &str = "SELECT VALUE id FROM d WHERE body @1@ 'hello world' ORDER BY id";
		compact().await;
		let (first, read) = query(IDS).await;
		assert_eq!(read, 2, "the first query reads both terms");
		let (second, read) = query(IDS).await;
		assert_eq!(read, 0, "the second is served from the cache");
		assert_eq!(first, second);

		query("CREATE d:4 SET body = 'hello world again'").await;
		let (after_write, read) = query(IDS).await;
		assert_eq!(read, 0, "no compaction ran, so the generation is the same");
		assert_ne!(after_write, second, "the new document is seen through its delta");

		compact().await;
		let (after_compaction, read) = query(IDS).await;
		assert_eq!(read, 2, "the compaction moved the generation");
		assert_eq!(after_compaction, after_write);

		let (_, read) = query(
			"BEGIN; CREATE d:5 SET body = 'hello'; \
			 SELECT VALUE id FROM d WHERE body @1@ 'hello world' ORDER BY id; COMMIT;",
		)
		.await;
		assert_eq!(read, 2, "a write transaction reads around the cache");
	}
	/// A document a scored query read, rewritten with the same length and the
	/// same query term at another frequency, is scored as it is stored now,
	/// before and after a compaction, whether the index keeps its terms in
	/// bitmaps or in posting blocks.
	///
	/// The rewrite records a change for every term the document carries. On a
	/// bitmap index that keeps the document's cached postings from being
	/// served while the change is uncompacted; on a block index it keeps the
	/// document's block entries from being scored until a compaction puts them
	/// afresh. A re-index that recorded no change for a term it keeps would
	/// leave the old frequency in place, and this test would fail.
	///
	/// A write transaction on a block index scores from the blocks too, so the
	/// block index's scores are also held to the bitmap index's, which takes
	/// every frequency from the document's postings.
	#[tokio::test]
	async fn a_rewritten_document_is_scored_as_stored() {
		let bitmaps = rewritten_document_is_scored_as_stored(false).await;
		let blocks = rewritten_document_is_scored_as_stored(true).await;
		assert_eq!(blocks, bitmaps, "the block index scored as the bitmap index");
	}

	/// A statement that writes scores a record as it holds it when the score is
	/// read: a row that rewrites its record before scoring it gets the score of
	/// what it wrote, not the score the scan gave the record it listed.
	#[tokio::test]
	async fn a_write_returns_the_score_of_what_it_wrote() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25;",
			false,
		)
		.await;
		let run = |sql: &'static str| {
			let db = &db;
			async move {
				let responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				responses.into_iter().last().expect("a response").result.expect("rows")
			}
		};
		run("CREATE d:1 SET body = 'hello hello world'; \
			 CREATE d:2 SET body = 'hello there now'; \
			 FOR $i IN 3..9 { CREATE type::record('d', $i) SET body = 'some other words' };")
		.await;
		crate::kvs::Datastore::index_compaction(
			std::sync::Arc::clone(db.datastore()),
			std::time::Duration::from_secs(1),
			tokio_util::sync::CancellationToken::new(),
		)
		.await
		.expect("compaction runs");
		let written = run("SELECT VALUE [\
				(UPDATE ONLY $parent.id SET body = 'goodbye world' RETURN NONE), \
				search::score(1)\
			][1] FROM d WHERE body @1@ 'hello'")
		.await;
		let surrealdb_types::Value::Array(scores) = written else {
			panic!("the update's rows: {written:?}");
		};
		assert_eq!(scores.len(), 2, "both documents carrying the term were rewritten");
		for score in scores.iter() {
			assert_eq!(
				*score,
				surrealdb_types::Value::Number(surrealdb_types::Number::Float(0.0)),
				"a document that no longer carries the term scores nothing: {scores:?}"
			);
		}
	}

	/// In a statement that writes, a call reads the index as the writes before
	/// it left it, rather than as another call of the same clause read it before
	/// them: the only row, admitted by another clause and rewritten to carry the
	/// term between a score and an offsets call, carries no term for the first
	/// and is located by the second.
	#[tokio::test]
	async fn a_call_after_a_write_reads_what_it_wrote() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS; \
			 CREATE d:1 SET body = 'world'; \
			 FOR $i IN 3..9 { CREATE type::record('d', $i) SET body = 'some other words' };",
			false,
		)
		.await;
		let run = |sql: &'static str| {
			let db = &db;
			async move {
				let responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				responses.into_iter().last().expect("a response").result.expect("rows")
			}
		};
		let written = run("SELECT VALUE [\
				search::score(1), \
				(UPDATE ONLY $parent.id SET body = 'hello world' RETURN NONE), \
				search::offsets(1)\
			] FROM d WHERE body @1@ 'hello' OR body @2@ 'world'")
		.await;
		let expected = run("RETURN [[0f, NONE, { 0: [{ e: 5, s: 0 }] }]]").await;
		assert_eq!(written, expected, "the offsets call reads the rewritten document");
	}

	/// In a statement that writes, each row's calls read the index as the
	/// writes before them left it, not as an earlier row's call read it: every
	/// row, rewritten to carry the term before it is located, is located.
	#[tokio::test]
	async fn each_row_of_a_write_reads_what_it_wrote() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS; \
			 CREATE d:1 SET body = 'hello'; \
			 CREATE d:2 SET body = 'world'; \
			 FOR $i IN 3..9 { CREATE type::record('d', $i) SET body = 'some other words' };",
			false,
		)
		.await;
		let run = |sql: &'static str| {
			let db = &db;
			async move {
				let responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				responses.into_iter().last().expect("a response").result.expect("rows")
			}
		};
		let written = run("SELECT VALUE [\
				(UPDATE ONLY $parent.id SET body = 'hello world' RETURN NONE), \
				search::offsets(1)\
			] FROM d WHERE body @1@ 'hello' OR body @2@ 'world'")
		.await;
		let located = run("RETURN [NONE, { 0: [{ e: 5, s: 0 }] }]").await;
		assert_eq!(
			written,
			surrealdb_types::Value::Array(vec![located.clone(), located].into()),
			"every row is located in what it wrote"
		);
	}

	/// A statement that writes reads the query's terms once per run, not once
	/// per row: scoring, highlighting and locating every record a bulk write
	/// rewrites reads the terms as often, and reads about as many bytes per
	/// row, at two table sizes. Reading the terms per row reads each term's
	/// compacted postings per row, which grow with the table.
	#[tokio::test]
	async fn a_scored_write_reads_its_terms_once_per_run() {
		const STATEMENTS: [&str; 4] = [
			"UPDATE d SET n = 1 WHERE body @1@ 'hello' RETURN search::score(1) AS s",
			"UPDATE d SET s = search::score(1) WHERE body @1@ 'hello' RETURN NONE",
			"UPDATE d SET n = 1 WHERE body @1@ 'hello' \
			 RETURN search::highlight('<b>', '</b>', 1) AS h, search::offsets(1) AS o",
			"UPDATE d SET body = 'hello there' WHERE body @1@ 'hello' \
			 RETURN search::score(1) AS s, search::offsets(1) AS o",
		];
		const SMALL: usize = 100;
		const LARGE: usize = 400;
		let mut measured = Vec::new();
		for sql in STATEMENTS {
			let (small_terms, small) = reads_of_a_write(SMALL, sql).await;
			let (large_terms, large) = reads_of_a_write(LARGE, sql).await;
			println!(
				"{sql}\n  {SMALL} rows: {small_terms} term reads, {} keys, {} bytes, {} gets, {} scans\
				 \n  {LARGE} rows: {large_terms} term reads, {} keys, {} bytes, {} gets, {} scans",
				small.keys_read,
				small.total_bytes_read,
				small.ops_get,
				small.ops_scan,
				large.keys_read,
				large.total_bytes_read,
				large.ops_get,
				large.ops_scan,
			);
			measured.push((sql, small_terms, small, large_terms, large));
		}
		for (sql, small_terms, small, large_terms, large) in measured {
			// The scan reads the terms once, and the calls naming the clause once
			// between them.
			assert_eq!(small_terms, 2, "{sql}: the terms are read once by the calls");
			assert_eq!(large_terms, 2, "{sql}: the terms are read once by the calls");
			// Each row reads its own record and index entries, so the bytes read
			// grow with the rows by about what a row reads at the smaller size.
			let per_row = small.total_bytes_read as f64 / SMALL as f64;
			let per_added_row =
				(large.total_bytes_read - small.total_bytes_read) as f64 / (LARGE - SMALL) as f64;
			assert!(
				per_added_row <= per_row * 1.25,
				"{sql}: each row reads a bounded number of bytes: {per_row:.0} per row at \
				 {SMALL} rows, {per_added_row:.0} per added row"
			);
		}
	}

	/// How many times the query's terms are read, and what the transaction
	/// counts, running the write `sql` in a transaction of its own over `rows`
	/// compacted documents of `d` carrying `hello` and as many carrying none of
	/// it.
	async fn reads_of_a_write(
		rows: usize,
		sql: &'static str,
	) -> (usize, surrealdb_observe::TransactionMetricsSnapshot) {
		let db = TestDb::quiescent(
			&format!(
				"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
				 DEFINE TABLE d SCHEMALESS; \
				 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS; \
				 FOR $i IN 0..{rows} {{ \
					CREATE type::record('d', $i) SET body = 'hello world' RETURN NONE; \
					CREATE type::record('d', $i + {rows}) SET body = 'other words' RETURN NONE; \
				 }};"
			),
			false,
		)
		.await;
		crate::kvs::Datastore::index_compaction(
			std::sync::Arc::clone(db.datastore()),
			std::time::Duration::from_secs(1),
			tokio_util::sync::CancellationToken::new(),
		)
		.await
		.expect("compaction runs");
		let txn = std::sync::Arc::new(
			db.datastore()
				.transaction(crate::kvs::TransactionType::Write)
				.await
				.expect("a transaction"),
		);
		let session = TestDb::owner();
		let request = crate::kvs::request::QueryRequest::new(sql, &session)
			.with_transaction(std::sync::Arc::clone(&txn));
		crate::exec::function::TERM_READS.with(|reads| reads.set(0));
		let responses = db.datastore().run(request).await.expect("the request runs");
		for response in responses {
			response.result.expect("the write succeeds");
		}
		let terms = crate::exec::function::TERM_READS.with(|reads| reads.get());
		let reads = txn.metrics_snapshot_for_test();
		txn.cancel().await.expect("the transaction ends");
		(terms, reads)
	}

	/// On an index keeping its own doc ids (format 0), a rewrite gives the
	/// record a new doc id, so the id the scan listed a record with no longer
	/// names it once the record is rewritten. A statement that writes reads each
	/// record's doc id from its mapping when a call needs it, rather than taking
	/// the scan's.
	#[tokio::test]
	async fn a_write_reads_each_doc_id_from_the_mapping() {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;",
			false,
		)
		.await;
		stamp(db.datastore(), crate::catalog::DOC_IDS_FORMAT_VERSION - 1).await;
		db.datastore()
			.execute(
				"CREATE d:1 SET body = 'hello world'; \
				 CREATE d:2 SET body = 'hello there now'; \
				 FOR $i IN 3..9 { CREATE type::record('d', $i) SET body = 'some other words' };",
				&TestDb::owner(),
				None,
			)
			.await
			.expect("runs");
		crate::exec::function::DOC_ID_READS.with(|reads| reads.set(0));
		let responses = db
			.datastore()
			.execute(
				"SELECT VALUE [\
					(UPDATE ONLY $parent.id SET body = 'goodbye hello' RETURN NONE), \
					search::offsets(1)\
				][1] FROM d WHERE body @1@ 'hello'",
				&TestDb::owner(),
				None,
			)
			.await
			.expect("runs");
		let rows = responses.into_iter().last().expect("a response").result.expect("rows");
		let surrealdb_types::Value::Array(rows) = rows else {
			panic!("the rows: {rows:?}");
		};
		assert_eq!(rows.len(), 2, "both documents carrying the term were rewritten");
		assert_eq!(
			crate::exec::function::DOC_ID_READS.with(|reads| reads.get()),
			2,
			"each call read its record's doc id after the rewrite"
		);
	}

	/// [`a_rewritten_document_is_scored_as_stored`] on an index stamped with
	/// the posting-block format where `blocks`, and with the format before it
	/// otherwise: the scores before the rewrite, after it, and once compacted.
	async fn rewritten_document_is_scored_as_stored(blocks: bool) -> Vec<surrealdb_types::Value> {
		let db = TestDb::quiescent(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25;",
			false,
		)
		.await;
		if !blocks {
			stamp(db.datastore(), crate::catalog::FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION - 1).await;
		}
		let compact = || async {
			crate::kvs::Datastore::index_compaction(
				std::sync::Arc::clone(db.datastore()),
				std::time::Duration::from_secs(1),
				tokio_util::sync::CancellationToken::new(),
			)
			.await
			.expect("compaction runs");
		};
		let run = |sql: &'static str| {
			let db = &db;
			async move {
				let responses =
					db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
				responses
					.into_iter()
					.map(|response| response.result.expect("rows"))
					.filter(|rows| matches!(rows, surrealdb_types::Value::Array(_)))
					.last()
					.expect("the select's rows")
			}
		};
		run("CREATE d:1 SET body = 'hello hello world'; \
			 CREATE d:2 SET body = 'hello there now'; \
			 FOR $i IN 3..9 { CREATE type::record('d', $i) SET body = 'some other words' };")
		.await;
		const SCORED: &str = "SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello' \
			ORDER BY id";
		// A write transaction reads around the cache, so it scores what is stored.
		const STORED: &str = "BEGIN; UPSERT x:1; \
			SELECT id, search::score(1) AS s FROM d WHERE body @1@ 'hello' ORDER BY id; COMMIT;";

		compact().await;
		let before = run(SCORED).await;
		assert_eq!(before, run(STORED).await);
		// A bitmap index scores from each document's postings, which the
		// query caches; a block index scores compacted documents from their
		// blocks and reads no document.
		let cached = db.datastore().index_store().fulltext_cache().documents();
		if blocks {
			assert_eq!(cached, 0, "the scored query read documents");
		} else {
			assert!(cached > 0, "the scored query cached its documents");
		}

		run("UPDATE d:1 SET body = 'hello world world'").await;
		let rewritten = run(SCORED).await;
		assert_ne!(rewritten, before, "the rewrite changed the document's score");
		assert_eq!(rewritten, run(STORED).await, "uncompacted");

		compact().await;
		let compacted = run(SCORED).await;
		assert_eq!(compacted, run(STORED).await, "compacted");
		vec![before, rewritten, compacted]
	}

	/// Stamps the `d_body` index on `d` with `format_version` before it holds
	/// any document, as a binary that wrote that format would have.
	async fn stamp(ds: &crate::kvs::Datastore, format_version: u16) {
		use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
		let tx = ds.transaction(crate::kvs::TransactionType::Write).await.expect("a transaction");
		let ns = tx.get_ns_by_name("test", None).await.expect("a read").expect("the namespace");
		let db =
			tx.get_db_by_name("test", "test", None).await.expect("a read").expect("the database");
		let tb: crate::val::TableName = "d".into();
		let def = tx
			.get_tb_index(ns.namespace_id, db.database_id, &tb, "d_body", None)
			.await
			.expect("a read")
			.expect("the index");
		let mut stamped = (*def).clone();
		stamped.format_version = format_version;
		tx.put_tb_index(ns.namespace_id, db.database_id, &tb, &stamped).await.expect("a write");
		tx.commit().await.expect("the commit");
	}
}
