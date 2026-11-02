//! Index function system for the engine.
//!
//! This module provides traits for functions that are bound to WHERE clause
//! predicates via index infrastructure. Unlike scalar functions which operate
//! purely on their arguments, index functions reference a specific predicate
//! (e.g., a MATCHES clause or a KNN operator) in the WHERE condition and need
//! access to the associated index at evaluation time.
//!
//! Each index function declares what kind of index context it requires via
//! [`IndexContextKind`]. The planner uses this to resolve the appropriate
//! [`IndexContext`] at plan time:
//!
//! - **FullText**: The `index_ref_arg_index()` argument (e.g., the `1` in `search::highlight('<b>',
//!   '</b>', 1)`) is extracted and resolved to a [`MatchContext`] via the WHERE clause's MATCHES
//!   operators.
//!
//! - **Knn**: A [`KnnContext`] is created from the KNN operator in the WHERE clause. The KNN scan
//!   operator populates it with per-row distances at execution time.
//!
//! Examples: search::highlight, search::score, search::offsets, vector::distance::knn

// Some items have no caller in every build, so dead-code lints are allowed.
#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;

use super::Signature;
use crate::catalog::Index;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::ir::iface::Fallibility;
use crate::exec::{BoxFut, ContextLevel, EvalContext};
use crate::expr::Kind;
use crate::expr::idiom::Idiom;
use crate::expr::logical::{FieldPath, LogicalExpr};
use crate::idx::IndexKeyBase;
use crate::idx::ft::MatchRef;
use crate::idx::ft::fulltext::{
	BlockBounds, FullTextHitsIterator, FullTextIndex, QueryTerms, Scorer,
};
use crate::kvs::index::filter_online_indexes;
use crate::val::{Number, RecordId, RecordIdentity, TableName, Value};

// =========================================================================
// IndexContextKind - what kind of index context a function requires
// =========================================================================

/// What kind of index context an [`IndexFunction`] requires.
///
/// The planner uses this to resolve the appropriate [`IndexContext`] at plan
/// time without needing to know about specific function names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexContextKind {
	/// Full-text search context (resolved from MATCHES clauses). Only
	/// `search::*` reads it.
	#[cfg_attr(not(feature = "search"), allow(dead_code))]
	FullText,
	/// KNN distance context (resolved from KNN operators).
	Knn,
}

// =========================================================================
// IndexContext - resolved context enum passed to index functions
// =========================================================================

/// Resolved index context, created at plan time and passed to index functions
/// at evaluation time.
///
/// Each variant carries the context appropriate for its index type.
/// Cloning is cheap (all variants are `Arc`-wrapped).
#[derive(Clone)]
pub enum IndexContext {
	/// Full-text search context with lazy access to FT index resources.
	FullText(Arc<MatchContext>),
	/// KNN distance context populated by the KNN scan operator.
	Knn(Arc<KnnContext>),
}

impl Debug for IndexContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::FullText(ctx) => f.debug_tuple("IndexContext::FullText").field(ctx).finish(),
			Self::Knn(ctx) => f.debug_tuple("IndexContext::Knn").field(ctx).finish(),
		}
	}
}

// =========================================================================
// IndexFunction trait
// =========================================================================

/// A function that is bound to a WHERE clause predicate via index infrastructure.
///
/// Index functions differ from scalar functions in that they:
/// - Are associated with a specific WHERE clause predicate (MATCHES, KNN, etc.)
/// - Need access to index infrastructure at evaluation time
/// - May have a reference argument extracted at plan time (not passed at runtime)
///
/// The planner dispatches generically based on [`index_context_kind()`] to
/// resolve the appropriate [`IndexContext`], without hardcoding function names.
pub trait IndexFunction: Send + Sync + Debug {
	/// The fully qualified function name (e.g., "search::highlight", "search::score")
	fn name(&self) -> &'static str;

	/// The function signature describing arguments and return type.
	#[allow(unused)]
	fn signature(&self) -> Signature;

	/// Infer the return type given the argument types.
	///
	/// The default implementation returns the signature's return type.
	#[allow(unused)]
	fn return_type(&self, _arg_types: &[Kind]) -> Result<Kind> {
		Ok(self.signature().returns)
	}

	/// What kind of index context this function requires.
	///
	/// The planner uses this to determine how to resolve the [`IndexContext`]:
	/// - [`IndexContextKind::FullText`]: resolve via MATCHES clause match_ref
	/// - [`IndexContextKind::Knn`]: resolve via KNN operator context
	fn index_context_kind(&self) -> IndexContextKind;

	/// Which argument position contains the index reference number.
	///
	/// When `Some(idx)`, this argument is extracted at plan time by the planner
	/// and is NOT passed to `invoke_async` as a runtime argument. For full-text
	/// functions, this is the match_ref that identifies a MATCHES clause.
	///
	/// Returns `None` if no reference argument is needed (e.g., KNN functions
	/// that use a single KNN operator from the WHERE clause).
	fn index_ref_arg_index(&self) -> Option<usize>;

	/// The minimum context level required to execute this function.
	///
	/// Index functions typically need root context for transaction and
	/// index store access.
	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	/// What a call's failure can depend on: the failure classes of the body
	/// for arguments of the signature's kinds, the index context included.
	/// Argument coercion (arity and kind) is the call's `VALUE`, not the
	/// function's.
	///
	/// A class is a superset when unsure. Every implementor declares a
	/// class; there is no default, so an undeclared one does not compile.
	#[allow(unused)]
	fn fallibility(&self) -> Fallibility;

	/// Async invocation with index context.
	///
	/// # Arguments
	/// * `ctx` - The evaluation context with access to current row and parameters
	/// * `index_ctx` - The resolved index context (FullText or Knn)
	/// * `args` - The evaluated function arguments, WITHOUT any plan-time extracted arguments
	///
	/// # Returns
	/// The computed value
	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		index_ctx: &'a IndexContext,
		args: Vec<Value>,
	) -> BoxFut<'a, Result<Value>>;
}

#[cfg(test)]
thread_local! {
	/// How many times this thread has read a full-text clause's query terms,
	/// through a scan or a [`MatchContext`], for tests that must know a run
	/// reads them once.
	pub(crate) static TERM_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many times this thread has read a record's doc id from its id
	/// mapping for an index call, rather than taking the one the scan listed.
	pub(crate) static DOC_ID_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many times this thread has scored a row in `search::score` itself,
	/// reading its length and postings, rather than taking the score the scan
	/// computed with its batch.
	pub(crate) static ROW_SCORES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many terms' compacted state this thread's full-text scans read
	/// from storage rather than from the node's cache.
	pub(crate) static TERM_STATES_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// =========================================================================
// MatchContext - resolved context for a single MATCHES clause
// =========================================================================

/// Resolved context for a single MATCHES clause, shared by every index
/// function call of a run that names the clause and by the full-text scan
/// answering it.
///
/// This captures the field and query string from a `WHERE field @N@ 'query'`
/// expression and provides lazy access to the associated full-text index
/// infrastructure. The FullTextIndex and QueryTerms are resolved once, by the
/// scan or by the first call to ask, and the Scorer once, by the first call
/// that scores; each is then cached for all subsequent rows.
pub struct MatchContext {
	/// The declared position ([`LogicalExpr::field_path`]) of the field read
	/// on the left side of the MATCHES operator, which names an index column
	/// when it is that column; `None` for a field that reads no declared
	/// position.
	pub path: Option<FieldPath>,
	/// The search query string from the right side of the MATCHES operator.
	pub query: String,
	/// The other queries that share this clause's match reference and field. The
	/// highlight and offsets of the reference cover the terms of all of them.
	pub extra_queries: Vec<String>,
	/// The table name for index lookup.
	pub table: TableName,
	/// Lazily initialized full-text index resources. The inner `Option` is
	/// `None` when no full-text index on `table` covers `field` — e.g. a
	/// MATCHES traversing a record link to an index on another table, or a
	/// field that reads no declared position — in which case
	/// score/highlight/offsets yield `NONE`.
	ft_cache: tokio::sync::OnceCell<Option<FullTextResources>>,
	/// The BM25 scorer, `None` when the index scores nothing. Built apart from
	/// [`Self::ft_cache`] because it reads the index's document statistics,
	/// which only a call that scores needs.
	scorer: tokio::sync::OnceCell<Option<Scorer>>,
	/// The bounds the terms' posting blocks put on the clause's score
	/// ([`QueryTerms::block_bounds`]), `None` where they bound nothing. Built
	/// with the scorer, once, by a scan rejecting hits by the clause's score.
	bounds: std::sync::OnceLock<Option<BlockBounds>>,
	/// The documents carrying any of the terms ([`Self::carriers`]), built once,
	/// by the first branch of an index union to pass hits over by them.
	carriers: std::sync::OnceLock<roaring::RoaringTreemap>,
	/// The doc ids, and the scores where the scan scored them, of the hits the
	/// scan answering the clause listed last, so a call on a row the scan
	/// produced finds its document without reading the record's id mapping
	/// ([`Self::doc_id`]), and its score without reading its postings
	/// ([`Self::remembered_score`]).
	recent: parking_lot::Mutex<RecentDocIds>,
	/// Whether the statement is read-only. Only then does a record hold what
	/// the clause's terms read of it for as long as the statement runs, so its
	/// doc id and score are kept and served again ([`Self::doc_id`],
	/// [`Self::remembered_score`]) and its posting blocks score it. In a
	/// statement that writes, a record may be rewritten between the read of
	/// the terms and a call on it, which reads what the record holds by then:
	/// its doc id from its mapping, since a rewrite of a record in an index's
	/// private doc-id space gives it a new one, and its score from its stored
	/// postings ([`QueryTerms::scored_as_stored`]).
	///
	/// A statement that writes reads the terms once too, to judge documents
	/// ([`FullTextIndex::extract_judging_terms`]), and every call judges against
	/// the index as the statement has left it when the call runs: which of the
	/// terms the record carries, how many documents carry each (the idf), and
	/// the index's document count and total length (the average length), each
	/// brought up to date from the changes the transaction has buffered since
	/// the terms were read ([`Scorer::score`]). A call so answers what terms
	/// read afresh for it would, and scores rows the statement wrote earlier
	/// against the corpus those writes left.
	read_only: bool,
}

/// What a full-text scan resolved for one record it listed: its doc id, and
/// its score where the scan scored the batch it was in.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Remembered {
	doc_id: crate::idx::docids::DocId,
	score: Option<f32>,
}

/// What a full-text scan resolved for the records it listed most recently,
/// in two generations of at most [`RecentDocIds::GENERATION`] entries each.
///
/// Bounded, so it holds a constant amount however many records the scan
/// lists: a row is called on soon after its batch is listed, and one called
/// on later — after a sort, say — misses and reads its mapping instead.
#[derive(Default)]
struct RecentDocIds {
	current: HashMap<RecordIdentity, Remembered>,
	previous: HashMap<RecordIdentity, Remembered>,
}

impl RecentDocIds {
	/// The most entries one generation holds.
	const GENERATION: usize = 1024;

	fn insert(&mut self, rid: RecordIdentity, remembered: Remembered) {
		// The generation let go hands its allocation to the next one.
		if self.current.len() >= Self::GENERATION {
			std::mem::swap(&mut self.current, &mut self.previous);
			self.current.clear();
		}
		self.current.insert(rid, remembered);
	}

	fn get(&self, rid: &RecordIdentity) -> Option<Remembered> {
		self.current.get(rid).or_else(|| self.previous.get(rid)).copied()
	}
}

/// The full-text resources a [`MatchContext`] resolves once per statement.
#[cfg_attr(not(feature = "search"), allow(dead_code))]
pub struct FullTextResources {
	pub index: FullTextIndex,
	pub terms: QueryTerms,
	/// The terms of [`MatchContext::extra_queries`], in the same order.
	pub extra_terms: Vec<QueryTerms>,
	/// The indexed column the MATCHES field named, as the catalog declares it.
	/// Highlighting reads the document through this path.
	pub column: Idiom,
	/// The catalog id of the index these resources read.
	pub index_id: crate::catalog::IndexId,
}

impl MatchContext {
	/// Create a new MatchContext from resolved MATCHES clause info. It keeps
	/// no score, since nothing says the statement it serves is read-only.
	pub fn new(field: &LogicalExpr, query: String, table: TableName) -> Self {
		Self::of_path(field.field_path(), query, Vec::new(), table, false)
	}

	/// A MatchContext for a MATCHES clause whose field reads the declared
	/// position `path`, as [`Self::new`] makes one for the field itself.
	/// `extra_queries` are the other queries sharing the clause's match
	/// reference. `read_only` is whether the statement is read-only, so a record
	/// holds what the terms read of it while the statement runs.
	pub fn of_path(
		path: Option<FieldPath>,
		query: String,
		extra_queries: Vec<String>,
		table: TableName,
		read_only: bool,
	) -> Self {
		Self {
			path,
			query,
			extra_queries,
			table,
			ft_cache: tokio::sync::OnceCell::new(),
			scorer: tokio::sync::OnceCell::new(),
			bounds: std::sync::OnceLock::new(),
			carriers: std::sync::OnceLock::new(),
			recent: parking_lot::Mutex::new(RecentDocIds::default()),
			read_only,
		}
	}

	/// The full-text index the clause is answered from, with the column of it
	/// the field reads: the first queryable `FULLTEXT` index on the table with a
	/// column whose declared position the field reads. `None` when no index
	/// covers the field.
	async fn covering_index(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
	) -> Result<Option<(crate::catalog::IndexDefinition, Idiom)>> {
		use crate::catalog::providers::TableProvider;

		let Some(path) = &self.path else {
			return Ok(None);
		};
		let tx = exec_ctx.txn();
		let db_ctx = exec_ctx
			.database()
			.map_err(|e| anyhow::anyhow!("IndexFunction requires database context: {}", e))?;
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let indexes = crate::exec::vm::descriptor::object(tx.all_tb_indexes(
			ns_id,
			db_id,
			&self.table,
			exec_ctx.version_stamp(),
		))
		.await?;
		let indexes = if exec_ctx.version_stamp().is_none() {
			// MATCHES and scoring only open indexes that durable build
			// state marks as queryable. That state is catalog data,
			// loaded together with the index list.
			crate::exec::vm::descriptor::object(filter_online_indexes(
				tx.as_ref(),
				ns_id,
				db_id,
				indexes,
			))
			.await?
		} else {
			indexes
		};
		Ok(indexes.iter().find_map(|idx| {
			if !matches!(&idx.index, Index::FullText(_)) {
				return None;
			}
			idx.cols
				.iter()
				.find(|c| FieldPath::from_idiom(c).as_ref() == Some(path))
				.map(|c| (idx.clone(), c.clone()))
		}))
	}

	/// Get or lazily initialize the full-text index resources.
	///
	/// On first call, this looks up the full-text index definition for the
	/// table/field, opens the FullTextIndex and extracts QueryTerms, unless the
	/// scan answering the clause already resolved them
	/// ([`Self::resources_for_scan`]). Subsequent calls return the cached
	/// result. Returns `Ok(None)` when no full-text index on the table
	/// covers the field (see the `ft_cache` field docs).
	#[cfg_attr(not(feature = "search"), allow(dead_code))]
	pub async fn ft_resources(&self, ctx: &EvalContext<'_>) -> Result<&Option<FullTextResources>> {
		self.ft_cache
			.get_or_try_init(|| async {
				let Some((index_def, column)) = self.covering_index(ctx.exec_ctx).await? else {
					// No index on this table covers the field (e.g. the
					// MATCHES traverses a record link) → score/highlight/
					// offsets yield NONE.
					return Ok(None);
				};
				let Index::FullText(ft_params) = &index_def.index else {
					unreachable!("covering_index answers a full-text index");
				};
				let frozen = ctx.exec_ctx.ctx();
				let db_ctx = ctx.exec_ctx.database()?;
				let ikb = IndexKeyBase::new(
					db_ctx.ns_ctx.ns.namespace_id,
					db_ctx.db.database_id,
					self.table.clone(),
					index_def.index_id,
				);
				let fti = crate::exec::vm::descriptor::object(FullTextIndex::new(
					frozen.get_index_stores(),
					ctx.txn().as_ref(),
					ikb,
					ft_params,
					&frozen.config.idx.file_allowlist,
					index_def.format_version,
				))
				.await?;
				#[cfg(test)]
				TERM_READS.with(|reads| reads.set(reads.get() + 1));
				let terms = self.read_terms(ctx.exec_ctx, &fti, self.query.clone()).await?;
				let extra_terms = self.extra_terms(ctx.exec_ctx, &fti).await?;
				Ok(Some(FullTextResources {
					index: fti,
					terms: self.as_held(terms),
					extra_terms,
					column,
					index_id: index_def.index_id,
				}))
			})
			.await
	}

	/// The terms of [`Self::extra_queries`] as `fti` reads them, in the same
	/// order.
	async fn extra_terms(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
		fti: &FullTextIndex,
	) -> Result<Vec<QueryTerms>> {
		let mut terms = Vec::with_capacity(self.extra_queries.len());
		for query in &self.extra_queries {
			terms.push(self.read_terms(exec_ctx, fti, query.clone()).await?);
		}
		Ok(terms)
	}

	/// The terms of `query` as `fti` reads them. In a statement that writes
	/// ([`Self::read_only`]) they are read to judge documents
	/// ([`FullTextIndex::extract_judging_terms`]), so every call reading them
	/// judges and scores a record as the statement has left it by then.
	async fn read_terms(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
		fti: &FullTextIndex,
		query: String,
	) -> Result<QueryTerms> {
		let frozen = exec_ctx.ctx();
		let az_fn = StreamingAnalyzerFunction::new(exec_ctx);
		// The terms are read through the node's cache, keyed by the table's
		// catalog version, which the run reads as it reads the version a
		// compiled unit is kept under.
		if self.read_only {
			crate::exec::compiles::seam(fti.extract_querying_terms(frozen, &az_fn, query)).await
		} else {
			crate::exec::compiles::seam(fti.extract_judging_terms(frozen, &az_fn, query)).await
		}
	}

	/// `terms` as the context holds them: scoring each record from what it
	/// stores where the statement may rewrite it ([`Self::read_only`]).
	fn as_held(&self, terms: QueryTerms) -> QueryTerms {
		if self.read_only {
			terms
		} else {
			terms.scored_as_stored()
		}
	}

	/// The resources the full-text scan of index `index_id` answers the clause
	/// with, made by `init` unless an earlier caller already resolved them, so
	/// that the calls naming the clause read the terms the scan read.
	///
	/// `None` where the clause's calls are answered from another index than
	/// the scan's, or from none ([`Self::covering_index`]), or where the
	/// statement writes ([`Self::read_only`]), whose calls judge each record
	/// from terms read to judge documents, which the scan's are not: the scan
	/// then keeps the terms `init` would make to itself, and the calls resolve
	/// their own, as they would with no scan.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::exec::fulltext", skip_all)
	)]
	pub async fn resources_for_scan<F, Fut, E>(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
		index_id: crate::catalog::IndexId,
		init: F,
	) -> std::result::Result<Option<&FullTextResources>, E>
	where
		F: FnOnce() -> Fut,
		Fut: std::future::Future<Output = std::result::Result<(FullTextIndex, QueryTerms), E>>,
		E: From<anyhow::Error>,
	{
		if !self.read_only {
			return Ok(None);
		}
		if let Some(resolved) = self.ft_cache.get() {
			return Ok(resolved.as_ref().filter(|r| r.index_id == index_id));
		}
		let Some((index_def, column)) = self.covering_index(exec_ctx).await? else {
			return Ok(None);
		};
		if index_def.index_id != index_id {
			return Ok(None);
		}
		let resolved = self
			.ft_cache
			.get_or_try_init(|| async {
				let (index, terms) = init().await?;
				let extra_terms = self.extra_terms(exec_ctx, &index).await?;
				Ok::<_, E>(Some(FullTextResources {
					index,
					terms: self.as_held(terms),
					extra_terms,
					column,
					index_id,
				}))
			})
			.await?;
		Ok(resolved.as_ref().filter(|r| r.index_id == index_id))
	}

	/// The BM25 score of each of `doc_ids`, in order, read with the batch's
	/// lengths and postings in two batched reads ([`Scorer::score_batch`]), for
	/// a scan of index `index_id` whose program scores the clause.
	///
	/// `None` where the scan's index is not the one the context's resources
	/// read, or the index scores nothing: a call on such a row computes its
	/// score itself.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::exec::fulltext", skip_all, fields(docs = doc_ids.len()))
	)]
	pub async fn scores(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
		index_id: crate::catalog::IndexId,
		doc_ids: &[crate::idx::docids::DocId],
	) -> Result<Option<Vec<f32>>> {
		let Some(ft) = self.answering(index_id) else {
			return Ok(None);
		};
		let Some(scorer) = self.scorer_in(exec_ctx, ft).await? else {
			return Ok(None);
		};
		Ok(Some(scorer.score_batch(&ft.index, &exec_ctx.txn(), &ft.terms, doc_ids).await?))
	}

	/// Upper bounds on the clause's score for each of `doc_ids`, in order, for
	/// a scan of index `index_id`, from its terms' posting blocks: zero for a
	/// document carrying none of the terms, which scores zero, and `None` for one
	/// the blocks do not bound, as it holds uncompacted state. `None` overall
	/// where the context bounds no score: the scan's index is not the one its
	/// resources read, the index scores nothing, or its terms bound nothing
	/// ([`QueryTerms::block_bounds`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::exec::fulltext", skip_all, fields(docs = doc_ids.len()))
	)]
	pub async fn score_bounds(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
		index_id: crate::catalog::IndexId,
		doc_ids: &[crate::idx::docids::DocId],
	) -> Result<Option<Vec<Option<f32>>>> {
		let Some(ft) = self.answering(index_id) else {
			return Ok(None);
		};
		let Some(scorer) = self.scorer_in(exec_ctx, ft).await? else {
			return Ok(None);
		};
		let Some(bounds) = self.bounds.get_or_init(|| ft.terms.block_bounds(scorer)) else {
			return Ok(None);
		};
		Ok(Some(
			doc_ids
				.iter()
				.map(|doc_id| {
					if !ft.terms.carries_any(*doc_id) {
						Some(0.0)
					} else if ft.terms.is_uncompacted(*doc_id) {
						None
					} else {
						bounds.bound(*doc_id)
					}
				})
				.collect(),
		))
	}

	/// The highest score the clause gives a document its terms hold no
	/// uncompacted state for, for a scan of index `index_id`, from its terms'
	/// posting blocks ([`BlockBounds::ceiling`]): zero where no such document
	/// carries a term. `None` where the context bounds no score, as for
	/// [`Self::score_bounds`].
	pub async fn score_ceiling(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
		index_id: crate::catalog::IndexId,
	) -> Result<Option<f32>> {
		let Some(ft) = self.answering(index_id) else {
			return Ok(None);
		};
		let Some(scorer) = self.scorer_in(exec_ctx, ft).await? else {
			return Ok(None);
		};
		let Some(bounds) = self.bounds.get_or_init(|| ft.terms.block_bounds(scorer)) else {
			return Ok(None);
		};
		Ok(Some(bounds.ceiling().unwrap_or(0.0)))
	}

	/// The documents carrying any of the clause's terms, which it scores above
	/// zero, and those its terms hold uncompacted state for, which no bound
	/// covers, where the context's resources read index `index_id`.
	pub fn carriers(
		&self,
		index_id: crate::catalog::IndexId,
	) -> Option<(&roaring::RoaringTreemap, &roaring::RoaringTreemap)> {
		let ft = self.answering(index_id)?;
		Some((self.carriers.get_or_init(|| ft.terms.carriers()), ft.terms.uncompacted()))
	}

	/// The doc ids of up to `limit` of the hits `hits` has not yet taken whose
	/// score may still beat `threshold`, the first key of the worst row a sort
	/// ordering by the clause's score in descending order keeps, for a scan of
	/// index `index_id`, with how many hits were passed over on the way
	/// ([`FullTextHitsIterator::take_doc_ids_within`]): every hit but those the
	/// terms hold no uncompacted state for whose block bound cannot beat it
	/// ([`QueryTerms::block_bounds`]), passed over a range of blocks at a time.
	/// `None`, with no hit taken, where the context's resources read another
	/// index, the index scores nothing, or its blocks bound nothing.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::exec::fulltext", skip_all)
	)]
	pub async fn take_within_bounds(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
		index_id: crate::catalog::IndexId,
		hits: &mut FullTextHitsIterator,
		limit: usize,
		threshold: &crate::val::Value,
		single_key: bool,
	) -> Result<Option<(Vec<crate::idx::docids::DocId>, u64)>> {
		let Some(ft) = self.answering(index_id) else {
			return Ok(None);
		};
		let Some(scorer) = self.scorer_in(exec_ctx, ft).await? else {
			return Ok(None);
		};
		let Some(bounds) = self.bounds.get_or_init(|| ft.terms.block_bounds(scorer)) else {
			return Ok(None);
		};
		// A score's threshold is a float, compared as the sort compares it: a bound
		// below it cannot beat it, nor can one equal to it where the order has a
		// single key. Any other threshold is compared as a value.
		let beats = |bound: f32| {
			if let crate::val::Value::Number(crate::val::Number::Float(threshold)) = threshold {
				let bound = f64::from(bound);
				bound > *threshold || (bound == *threshold && !single_key)
			} else {
				let bound = crate::val::Value::Number(crate::val::Number::Float(f64::from(bound)));
				!crate::exec::topk_pushdown::cannot_beat(
					threshold,
					&bound,
					crate::exec::operators::SortDirection::Desc,
					single_key,
				)
			}
		};
		Ok(Some(hits.take_doc_ids_within(limit, &ft.terms, bounds, beats)))
	}

	/// Records what a scan of index `index_id` resolved for the hits it hands
	/// on: each record with its doc id and, where the scan scored it, its
	/// score, for [`Self::doc_id`] and [`Self::remembered_score`] to answer
	/// from.
	///
	/// Kept only where the scan's index is the one the context's resources
	/// read: a legacy index numbers its documents in a space of its own, so
	/// another index's doc id would name another document.
	pub fn remember(
		&self,
		index_id: crate::catalog::IndexId,
		hits: &[(RecordId, crate::idx::docids::DocId, Option<f32>)],
	) {
		if !self.read_only || self.answering(index_id).is_none() {
			return;
		}
		let mut recent = self.recent.lock();
		for (rid, doc_id, score) in hits {
			if rid.table == self.table {
				recent.insert(
					RecordIdentity(rid.key.clone()),
					Remembered {
						doc_id: *doc_id,
						score: *score,
					},
				);
			}
		}
	}

	/// The context's resources, where they read the index `index_id`.
	fn answering(&self, index_id: crate::catalog::IndexId) -> Option<&FullTextResources> {
		self.ft_cache.get()?.as_ref().filter(|ft| ft.index_id == index_id)
	}

	/// Remembers `score` as the score of record `rid`, document `doc_id`, so
	/// another call scoring the same row takes it rather than reading the
	/// document again. A statement that writes keeps nothing.
	#[cfg_attr(not(feature = "search"), allow(dead_code))]
	pub fn remember_score(&self, rid: &RecordId, doc_id: crate::idx::docids::DocId, score: f32) {
		if self.read_only && rid.table == self.table {
			self.recent.lock().insert(
				RecordIdentity(rid.key.clone()),
				Remembered {
					doc_id,
					score: Some(score),
				},
			);
		}
	}

	/// The score of record `rid` when it is remembered: computed by the scan
	/// with the batch that listed it, or by an earlier call on the row.
	#[cfg_attr(not(feature = "search"), allow(dead_code))]
	pub fn remembered_score(&self, rid: &RecordId) -> Option<f32> {
		if rid.table != self.table {
			return None;
		}
		self.recent.lock().get(&RecordIdentity(rid.key.clone()))?.score
	}

	/// The doc id of record `rid` in the index `ft` reads: the one the scan
	/// listed it with when it is still remembered, read from the record's id
	/// mapping otherwise. `None` for a record the index does not hold.
	#[cfg_attr(not(feature = "search"), allow(dead_code))]
	pub async fn doc_id(
		&self,
		ctx: &EvalContext<'_>,
		ft: &FullTextResources,
		rid: &RecordId,
	) -> Result<Option<crate::idx::docids::DocId>> {
		if self.read_only
			&& rid.table == self.table
			&& let Some(remembered) = self.recent.lock().get(&RecordIdentity(rid.key.clone()))
		{
			return Ok(Some(remembered.doc_id));
		}
		#[cfg(test)]
		DOC_ID_READS.with(|reads| reads.set(reads.get() + 1));
		let doc_id = ft.index.get_doc_id(&ctx.txn(), rid).await?;
		// Remembered, so the next call on the same row — another of the
		// clause's functions, after a sort — takes it rather than reading it
		// again.
		if let Some(doc_id) = doc_id
			&& self.read_only
			&& rid.table == self.table
		{
			self.recent.lock().insert(
				RecordIdentity(rid.key.clone()),
				Remembered {
					doc_id,
					score: None,
				},
			);
		}
		Ok(doc_id)
	}

	/// The BM25 scorer of the index `ft` reads, built on first use; `None`
	/// when the index declares no BM25 scoring.
	#[cfg_attr(not(feature = "search"), allow(dead_code))]
	pub async fn scorer(
		&self,
		ctx: &EvalContext<'_>,
		ft: &FullTextResources,
	) -> Result<Option<&Scorer>> {
		self.scorer_in(ctx.exec_ctx, ft).await
	}

	async fn scorer_in(
		&self,
		exec_ctx: &crate::exec::ExecutionContext,
		ft: &FullTextResources,
	) -> Result<Option<&Scorer>> {
		let scorer = self
			.scorer
			.get_or_try_init(|| async { ft.index.new_scorer(exec_ctx.ctx()).await })
			.await?;
		Ok(scorer.as_ref())
	}
}

impl Debug for MatchContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MatchContext")
			.field("path", &self.path)
			.field("query", &self.query)
			.field("extra_queries", &self.extra_queries)
			.field("table", &self.table)
			.field("initialized", &self.ft_cache.initialized())
			.finish()
	}
}

// =========================================================================
// KnnContext - distance context populated by KNN scan operators
// =========================================================================

/// KNN distance context, populated by KnnScan at execution time.
///
/// Created at plan time and shared (via `Arc`) between the KNN scan operator
/// and the `vector::distance::knn()` index function. The scan operator writes
/// per-row distances after the HNSW search completes; the function reads them
/// during projection evaluation.
///
/// This is the KNN equivalent of [`MatchContext`] -- same lifecycle pattern
/// (plan-time creation, shared via `Arc`, deferred population at execution time).
pub struct KnnContext {
	/// Per-row distances keyed by the record each row is, populated by the KNN
	/// scan operator.
	///
	/// The key compares record ids by the record they address, not with
	/// `RecordId`'s own equality, which compares numbers across variants: under
	/// it `k:[1]` and `k:[1dec]` are one key, and the two records would share
	/// whichever distance was written last.
	///
	/// Uses `tokio::sync::RwLock` so lock acquisition is async and cannot
	/// block the tokio runtime.
	distances: tokio::sync::RwLock<HashMap<(TableName, RecordIdentity), Number>>,
}

impl KnnContext {
	/// Create a new empty KnnContext.
	pub fn new() -> Self {
		Self {
			distances: tokio::sync::RwLock::new(HashMap::new()),
		}
	}

	/// Record the distance of every record in `entries`, under one acquisition
	/// of the lock. Called by the KNN operators once their results are known.
	pub async fn insert_all(&self, entries: impl IntoIterator<Item = (RecordId, Number)>) {
		let mut distances = self.distances.write().await;
		for (rid, dist) in entries {
			distances.insert((rid.table, RecordIdentity(rid.key)), dist);
		}
	}

	/// Look up the distance for a record. Called by vector::distance::knn().
	pub async fn get(&self, rid: &RecordId) -> Option<Number> {
		let key = (rid.table.clone(), RecordIdentity(rid.key.clone()));
		self.distances.read().await.get(&key).cloned()
	}
}

impl Default for KnnContext {
	fn default() -> Self {
		Self::new()
	}
}

impl Debug for KnnContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self.distances.try_read() {
			Ok(guard) => f.debug_struct("KnnContext").field("entries", &guard.len()).finish(),
			Err(_) => f.debug_struct("KnnContext").field("entries", &"<locked>").finish(),
		}
	}
}

// =========================================================================
// MatchesContext - planning-time map of all MATCHES clauses
// =========================================================================

/// Information about a single MATCHES clause extracted from the WHERE condition.
#[derive(Debug, Clone)]
pub struct MatchInfo {
	/// The field read on the left side of the MATCHES operator, as written: a
	/// path expression ([`LogicalExpr::is_path`]).
	pub field: LogicalExpr,
	/// The search query string from the right side of the MATCHES operator.
	pub query: String,
}

/// Planning-time context mapping match_ref numbers to MATCHES clause info.
///
/// Built by analyzing the WHERE clause AST during query planning. Each entry
/// maps a match_ref number (e.g., `1` from `@1@`) to the field and query
/// string of the corresponding MATCHES operator.
#[derive(Debug, Clone)]
pub struct MatchesContext {
	matches: HashMap<MatchRef, MatchInfo>,
	/// The table name from the FROM clause, set during planning.
	table: Option<TableName>,
}

impl MatchesContext {
	/// Create a new empty MatchesContext.
	pub fn new() -> Self {
		Self {
			matches: HashMap::new(),
			table: None,
		}
	}

	/// Get the table name.
	pub fn table(&self) -> Option<&TableName> {
		self.table.as_ref()
	}

	/// Insert a MATCHES clause entry.
	pub fn insert(&mut self, match_ref: MatchRef, info: MatchInfo) {
		self.matches.insert(match_ref, info);
	}

	/// Look up a MATCHES clause by its match_ref number.
	pub fn get(&self, match_ref: MatchRef) -> Option<&MatchInfo> {
		self.matches.get(&match_ref)
	}

	/// Get the first available MatchInfo (for when there's only one MATCHES clause).
	pub fn first(&self) -> Option<(MatchRef, &MatchInfo)> {
		self.matches.iter().next().map(|(&k, v)| (k, v))
	}

	/// The clauses held, by match reference ascending.
	pub fn entries(&self) -> Vec<(MatchRef, &MatchInfo)> {
		let mut entries: Vec<_> = self.matches.iter().map(|(rf, info)| (*rf, info)).collect();
		entries.sort_by_key(|(rf, _)| *rf);
		entries
	}

	/// Create a MatchContext for a given match_ref, resolving against this context.
	///
	/// If the match_ref is found, creates a MatchContext with the resolved
	/// field, query, and table name. If not found and there's exactly one
	/// entry, falls back to that entry (common case: single MATCHES clause).
	pub fn resolve(&self, match_ref: MatchRef, table: TableName) -> Result<Arc<MatchContext>> {
		let info = self.get(match_ref).or_else(|| {
			// Fall back to the single entry if there's only one
			if self.matches.len() == 1 {
				self.first().map(|(_, info)| info)
			} else {
				None
			}
		});

		match info {
			Some(info) => Ok(Arc::new(MatchContext::new(&info.field, info.query.clone(), table))),
			None => {
				// If there are no MATCHES clauses at all, provide a clear error
				if self.matches.is_empty() {
					Err(anyhow::anyhow!("no MATCHES clause found in WHERE condition"))
				} else {
					Err(anyhow::anyhow!(
						"no MATCHES clause found for match_ref {} (available: {:?})",
						match_ref,
						self.matches.keys().collect::<Vec<_>>()
					))
				}
			}
		}
	}
}

impl Default for MatchesContext {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod recent_doc_ids_tests {
	use super::RecentDocIds;
	use crate::val::{RecordIdKey, RecordIdentity};

	fn rid(n: i64) -> RecordIdentity {
		RecordIdentity(RecordIdKey::Number(n))
	}

	/// The window holds the last two generations and nothing older, so it
	/// stays bounded however many records a scan lists.
	#[test]
	fn the_window_keeps_two_generations() {
		let mut recent = RecentDocIds::default();
		let total = RecentDocIds::GENERATION * 3;
		for n in 0..total {
			recent.insert(
				rid(n as i64),
				super::Remembered {
					doc_id: n as u64,
					score: None,
				},
			);
		}
		assert!(recent.current.len() + recent.previous.len() <= 2 * RecentDocIds::GENERATION);
		let doc_id = |n: usize| recent.get(&rid(n as i64)).map(|r| r.doc_id);
		assert_eq!(doc_id(total - 1), Some(total as u64 - 1), "the newest");
		let oldest_kept = total - 2 * RecentDocIds::GENERATION + 1;
		assert_eq!(doc_id(oldest_kept), Some(oldest_kept as u64));
		assert_eq!(doc_id(0), None, "the first generation was let go");
	}
}
