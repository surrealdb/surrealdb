//! The search of a KNN source: which records an ANN index lists as nearest to
//! a vector once the table's permission and the statement's conditions narrow
//! the candidates.
//!
//! A search gates each candidate on the table's permission and then on a
//! condition, and may be restricted to a bitmap allow-list of the records
//! that satisfy the conjuncts it covers. The records the search returns are
//! the `k` nearest of the candidates that pass, so a search cut to `k` and
//! filtered afterwards holds other rows. The gates are pipelines of the
//! program ([`KnnGate`]); the search runs each on a candidate record's data
//! and takes a row emitted as a pass.
//!
//! The allow-list decides the strategy ([`plan_prefilter`]): a list small
//! enough, or one whose members cost less to score exactly than the graph
//! traversal they would gate, is scored exactly, without the graph; a larger
//! one gates the graph traversal on membership. A list that overflows its
//! branch budget, an index with no shared doc-IDs or a versioned read search
//! the graph with the full condition judged on every candidate.

use std::borrow::Cow;
use std::sync::Arc;

use roaring::RoaringTreemap;

use super::batch::{BatchSizer, read_sized};
use super::host::{out_of_line, run_cow_row, statement_state};
use super::program::{KnnGate, Program};
use super::services::{EngineServices, Services};
use crate::catalog::{Index, Record};
use crate::exe::FlowResultExt as _;
use crate::exec::field_path_convert::field_path_from_idiom;
use crate::exec::index::access_path::IndexRef;
use crate::exec::operators::KnnTopKHeap;
use crate::exec::operators::scan::{
	AnnTarget, PrefilterSettings, PrefilterTier, ann_graph_shape, exact_from_index_vectors,
	knn_allow_list, plan_prefilter,
};
use crate::exec::permission::validate_record_user_access;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::kernels::vector::{query as typed_query_vector, score as score_raw_values};
use crate::idx::trees::KnnCondFilter;
use crate::idx::trees::gate::{
	BoxGateFut, CachedTableSelect, CandidateCondition, CandidateFetchCounter,
};
use crate::kvs::CachePolicy;
use crate::val::{Number, RecordId, TableName, Value};

/// The most members of an allow-list the exact tier resolves and reads at a
/// time.
const EXACT_BATCH_SIZE: usize = 1000;

/// The records ANN index `index` of `table` lists as nearest to `vector`, at
/// most `k`, nearest first, each with its distance, among the candidates
/// `gate` admits; every record the index holds is a candidate without one.
#[expect(clippy::too_many_arguments)]
pub(super) async fn search(
	program: &Program,
	ctx: &ExecutionContext,
	table: &TableName,
	index: &IndexRef,
	vector: &[Number],
	k: u32,
	ef: u32,
	version: Option<u64>,
	gate: Option<&KnnGate>,
) -> FlowResult<Vec<(RecordId, f64)>> {
	let services = EngineServices::new(ctx);
	let Some(gate) = gate else {
		return services.knn_candidates(table, index, vector, k, ef, version, None, None).await;
	};
	let index_def = index.definition();
	// The permission is judged inside the search only for a caller whose reads
	// are checked.
	let judges_permission = gate.permission.is_some() && !ctx.root().skip_fetch_perms;

	// Pre-filtered search: the covered conjuncts become an allow-list of
	// doc-IDs, and the strategy follows its cardinality and, where the graph
	// can be weighed, its cost. When the list cannot be built, the full
	// condition is judged on every candidate, as it is for a versioned read,
	// which no allow-list serves.
	let mut allow: Option<RoaringTreemap> = None;
	let mut in_search = gate.residual;
	if let Some(prefilter) = gate.prefilter.as_ref().filter(|_| index_def.uses_shared_doc_ids())
		&& version.is_none()
		&& let Some(list) = knn_allow_list(ctx, table, &prefilter.root, ef, None).await?
	{
		in_search = prefilter.residual;
		allow = Some(list);
	}
	// The condition that holds the `MATCHES` clauses supersedes the residual
	// whenever no allow-list restricts the search, and stays beside one that
	// could not cover a `MATCHES` conjunct.
	let uncovered_matches =
		gate.prefilter.as_ref().is_some_and(|p| p.uncovered_matches) && allow.is_some();
	let matches_gate = if allow.is_none() || uncovered_matches {
		gate.matches_condition
	} else {
		None
	};
	if matches_gate.is_some() {
		in_search = None;
	}

	// What the exact tier judges on each member it reads; with nothing to
	// judge, an index that keeps its doc-ID → elements mapping scores the
	// members from the vectors it stores, which is the scoring the cost model
	// weighs against the traversal.
	let per_row = matches_gate
		.or(gate.prefilter.as_ref().and_then(|p| p.residual))
		.or(judges_permission.then_some(gate.permission).flatten());
	let decision = match &allow {
		Some(list) => {
			Some(decide_prefilter(ctx, table, index_def, list.len(), ef, per_row.is_some()).await?)
		}
		None => None,
	};
	// A walk restricted to the allow-list is gated on it at the width the
	// triage chose; a search with no allow-list (none was planned, or it could
	// not be built) walks the graph at the requested width.
	let (ef, search_allow) = match decision {
		Some(PrefilterDecision::Exact {
			ids,
		}) => {
			let allow = allow.as_ref().expect("an exact decision holds an allow-list");
			return exact(program, ctx, table, index, vector, k, allow, per_row, ids).await;
		}
		Some(PrefilterDecision::Walk {
			ef,
			..
		}) => (ef, allow.as_ref()),
		None => (ef, None),
	};
	// A search with no condition to judge still judges the permission of each
	// candidate, whether the allow-list admits it or the graph order does, so
	// a record the caller may not see takes no slot and the `k` records
	// returned are `k` the caller may see.
	let judged =
		matches_gate.or(in_search).or(judges_permission.then_some(gate.permission).flatten());
	let cond_filter = judged.map(|pipeline| KnnCondFilter {
		select_gate: CachedTableSelect::Skip,
		cond: Some(Arc::new(Judge {
			program,
			pipeline,
			ctx,
		}) as Arc<dyn CandidateCondition + '_>),
		// Under `EXPLAIN ANALYZE`, a candidate the search fetches to judge is
		// counted against the plan.
		metrics: ctx
			.vm_metrics()
			.filter(|metrics| metrics.is_enabled())
			.map(|metrics| Arc::clone(metrics) as Arc<dyn CandidateFetchCounter>),
		// Each candidate is read at the stamp the statement reads at.
		version,
	});
	services.knn_candidates(table, index, vector, k, ef, version, cond_filter, search_allow).await
}

/// Exact scoring of the members of `allow`, without the graph, into a
/// bounded heap, nearest first.
///
/// When the index keeps its doc-ID → elements mapping, the members are scored
/// from the vectors the index stores first. With nothing to judge per record
/// (`per_row` is `None`), no record is read here: the caller reads only the
/// `k` winners. With a `per_row` judge, the scored candidates are read and
/// judged in nearest-first batches until `k` winners pass, so records beyond
/// the `k`th passing neighbour are never read or judged. An index that keeps
/// no doc-ID → elements mapping reads, judges and scores each member from its
/// record.
///
/// `ids` are the table id and key base the index is opened with, where the
/// caller has read them already.
#[expect(clippy::too_many_arguments)]
async fn exact(
	program: &Program,
	ctx: &ExecutionContext,
	table: &TableName,
	index: &IndexRef,
	vector: &[Number],
	k: u32,
	allow: &RoaringTreemap,
	per_row: Option<u16>,
	ids: Option<TargetIds>,
) -> FlowResult<Vec<(RecordId, f64)>> {
	validate_record_user_access(ctx.database()?)?;
	let services = EngineServices::new(ctx);
	let index_def = index.definition();
	let (distance, vector_type, dimension) = match &index_def.index {
		Index::Hnsw(p) => (p.distance.clone(), p.vector_type, p.dimension as usize),
		#[cfg(diskann)]
		Index::DiskAnn(p) => (p.distance.clone(), p.vector_type, p.dimension as usize),
		#[cfg(not(diskann))]
		Index::DiskAnn(_) => {
			return Err(ControlFlow::Err(anyhow::anyhow!(
				"DISKANN indexes require a 64-bit, non-WASM platform"
			)));
		}
		_ => {
			return Err(ControlFlow::Err(anyhow::anyhow!(
				"Index '{}' is not an ANN index",
				index_def.name
			)));
		}
	};
	let vector_cap = match per_row {
		None => k,
		Some(_) if k == 0 => 0,
		Some(_) => u32::try_from(allow.len()).unwrap_or(u32::MAX),
	};
	if let Some(scored) =
		from_index_vectors(ctx, table, index_def, vector, vector_cap, allow, ids).await?
	{
		return match per_row {
			None => Ok(scored),
			Some(pipeline) => screen_scored_candidates(program, ctx, pipeline, k, scored).await,
		};
	}
	// The indexed column is a stored field, read off each record as a pure
	// path.
	let field = match index_def.cols.first() {
		Some(column) => {
			field_path_from_idiom(column).map_err(|e| ControlFlow::Err(anyhow::anyhow!(e)))?
		}
		None => {
			return Err(ControlFlow::Err(anyhow::anyhow!(
				"Index '{}' has no indexed column",
				index_def.name
			)));
		}
	};
	let query = typed_query_vector(vector_type, dimension, vector)
		.map_err(|e| ControlFlow::Err(e.context("Invalid KNN query vector")))?;
	// No member can be offered: answered before any record is read, once the
	// query vector has been checked.
	if allow.is_empty() || k == 0 {
		return Ok(Vec::new());
	}
	let mut heap: KnnTopKHeap<RecordId> = KnnTopKHeap::new(k as usize);
	let mut members = allow.iter();
	let mut sizer = BatchSizer::new(ctx, EXACT_BATCH_SIZE);
	loop {
		let chunk: Vec<u64> = members.by_ref().take(sizer.len()).collect();
		if chunk.is_empty() {
			break;
		}
		// A unit per member resolved, read and ranked.
		super::fuel::tick(ctx, super::fuel::units(chunk.len())).await?;
		let rids = services.resolve_doc_ids(table, &chunk).await?;
		// The fetch does not populate the transaction record cache: a cached
		// record is shared, so each row read would be a deep copy of it rather
		// than a move, while only the k winners are read again.
		let rows = read_sized(ctx, &mut sizer, &rids, None, CachePolicy::ReadOnly).await?;
		for row in rows {
			// The record's vector is scored in place before the per-row
			// condition is judged: once the heap holds `k` admitted records,
			// any candidate whose vector distance cannot enter the heap is
			// rejected without running the condition pipeline.
			let scored = if let Value::Array(record) = field.extract(&row).as_ref() {
				score_raw_values(&distance, vector_type, dimension, &query, &record.0)
			} else {
				None
			};
			let Some(scored) = scored else {
				continue;
			};
			let scored = Number::Float(scored);
			// A candidate the full heap would turn away takes no entry.
			if !heap.admits(&scored) {
				continue;
			}
			if let Some(pipeline) = per_row
				&& !admits(program, pipeline, ctx, &row).await.catch_return()?.is_truthy()
			{
				continue;
			}
			let Value::Object(mut object) = row else {
				continue;
			};
			let Some(Value::RecordId(rid)) = object.remove("id") else {
				continue;
			};
			heap.offer(scored, rid);
		}
	}
	// Sized by the neighbours found, not by `k`, which is the user's.
	Ok(heap.into_sorted_nearest_first().map(|(distance, rid)| (rid, distance.to_float())).collect())
}

/// Screen `candidates` — already scored from the index's stored vectors and
/// ordered nearest-first — through `pipeline` in bounded record batches,
/// stopping as soon as `k` candidates pass.
async fn screen_scored_candidates(
	program: &Program,
	ctx: &ExecutionContext,
	pipeline: u16,
	k: u32,
	candidates: Vec<(RecordId, f64)>,
) -> FlowResult<Vec<(RecordId, f64)>> {
	let want = k as usize;
	if want == 0 || candidates.is_empty() {
		return Ok(Vec::new());
	}
	let mut winners = Vec::with_capacity(want.min(candidates.len()));
	let mut iter = candidates.into_iter();
	let mut sizer = BatchSizer::new(ctx, EXACT_BATCH_SIZE);
	let mut first_batch = true;
	while winners.len() < want {
		let needed = want - winners.len();
		let limit = if first_batch {
			first_batch = false;
			sizer.len().min(needed.saturating_mul(2).max(8))
		} else {
			sizer.len()
		};
		let chunk: Vec<(RecordId, f64)> = iter.by_ref().take(limit).collect();
		if chunk.is_empty() {
			break;
		}
		super::fuel::tick(ctx, super::fuel::units(chunk.len())).await?;
		let rids: Vec<RecordId> = chunk.iter().map(|(rid, _)| rid.clone()).collect();
		let rows = read_sized(ctx, &mut sizer, &rids, None, CachePolicy::ReadOnly).await?;
		// `read_sized` yields records ordered by storage key (`RecordId`), whereas
		// `chunk` is ordered nearest-first by distance. Re-index the fetched rows
		// back into `chunk`'s nearest-first order before screening so that `admits`
		// evaluation and early stopping at `want` winners strictly preserve distance
		// rank regardless of `RecordId` order.
		let mut ordered_rows: Vec<Option<Value>> = (0..chunk.len()).map(|_| None).collect();
		for row in rows {
			let Value::Object(object) = &row else {
				continue;
			};
			let Some(Value::RecordId(rid)) = object.get("id") else {
				continue;
			};
			if let Some(slot) =
				chunk.iter().zip(ordered_rows.iter_mut()).find_map(|((cand_rid, _), slot)| {
					(slot.is_none() && cand_rid == rid).then_some(slot)
				}) {
				*slot = Some(row);
			}
		}
		for ((rid, distance), row) in chunk.into_iter().zip(ordered_rows) {
			let Some(row) = row else {
				continue;
			};
			if admits(program, pipeline, ctx, &row).await.catch_return()?.is_truthy() {
				winners.push((rid, distance));
				if winners.len() == want {
					break;
				}
			}
		}
	}
	Ok(winners)
}

/// The table id and key base an [`AnnTarget`] opens an ANN index with.
type TargetIds = (crate::catalog::TableId, crate::idx::IndexKeyBase);

/// What a search restricted to an allow-list does with it
/// ([`decide_prefilter`]).
pub(super) enum PrefilterDecision {
	/// Score the members exactly, without the graph. `ids` are the table id
	/// and key base to open the index with, where the decision read them.
	Exact {
		ids: Option<TargetIds>,
	},
	/// Walk the graph, admitting members only, `ef` wide: `tier` is
	/// [`PrefilterTier::Graph`] or [`PrefilterTier::GraphUnboosted`].
	Walk {
		tier: PrefilterTier,
		ef: u32,
	},
}

impl PrefilterDecision {
	/// The tier, as `EXPLAIN ANALYZE` reports it.
	pub(super) fn tier(&self) -> PrefilterTier {
		match self {
			Self::Exact {
				..
			} => PrefilterTier::Exact,
			Self::Walk {
				tier,
				..
			} => *tier,
		}
	}

	/// The width of the walk, `None` for the exact tier.
	pub(super) fn walk_ef(&self) -> Option<u32> {
		match self {
			Self::Exact {
				..
			} => None,
			Self::Walk {
				ef,
				..
			} => Some(*ef),
		}
	}
}

/// What a search of ANN index `index_def` of `table`, restricted to an
/// allow-list of `allow_len` members and requested with `ef`, does with the
/// list ([`plan_prefilter`]), where `per_row` is whether the exact tier would
/// judge each member it reads.
///
/// The graph's shape is read only where the decision weighs it: an HNSW index,
/// above the exact threshold, that the exact tier scores from the vectors it
/// stores, with nothing judged per member. Elsewhere neither the table nor the
/// graph is read.
pub(super) async fn decide_prefilter(
	ctx: &ExecutionContext,
	table: &TableName,
	index_def: &crate::catalog::IndexDefinition,
	allow_len: u64,
	ef: u32,
	per_row: bool,
) -> FlowResult<PrefilterDecision> {
	let settings = PrefilterSettings::configured(&index_def.index);
	let weighed = matches!(index_def.index, Index::Hnsw(_))
		&& settings.weighs_graph(allow_len)
		&& !per_row
		&& crate::catalog::keeps_vector_doc_elements(index_def.format_version);
	let (graph, ids) = if weighed {
		let (table_id, ikb) = target_ids(ctx, table, index_def).await?;
		let graph = ann_graph_shape(&AnnTarget {
			frozen_ctx: &ctx.root().ctx,
			index_def,
			ikb: &ikb,
			table_id,
		})
		.await?;
		(graph, Some((table_id, ikb)))
	} else {
		(None, None)
	};
	Ok(match plan_prefilter(allow_len, ef, graph, &settings) {
		(PrefilterTier::Exact, _) => PrefilterDecision::Exact {
			ids,
		},
		(tier, ef) => PrefilterDecision::Walk {
			tier,
			ef,
		},
	})
}

/// The table id and key base an [`AnnTarget`] opens ANN index `index_def` of
/// `table` with, the table's definition read at the current version.
async fn target_ids(
	ctx: &ExecutionContext,
	table: &TableName,
	index_def: &crate::catalog::IndexDefinition,
) -> FlowResult<TargetIds> {
	let db_ctx = ctx.database()?;
	let Some(table_def) = db_ctx.get_table_def(table, None).await.context("Failed to get table")?
	else {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::catalog::Error::TbNotFound {
			name: table.clone(),
		})));
	};
	let ikb = crate::idx::IndexKeyBase::new(
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		index_def.table_name.clone(),
		index_def.index_id,
	);
	Ok((table_def.table_id, ikb))
}

/// The members of `allow` nearest `vector` as [`exact`] answers them, scored
/// from the vectors ANN index `index_def` stores; `None` where the index keeps
/// no doc-ID → elements mapping and the members' records must be scored.
/// `ids` are the table id and key base to open the index with, where the
/// caller has read them already.
async fn from_index_vectors(
	ctx: &ExecutionContext,
	table: &TableName,
	index_def: &crate::catalog::IndexDefinition,
	vector: &[Number],
	k: u32,
	allow: &RoaringTreemap,
	ids: Option<TargetIds>,
) -> FlowResult<Option<Vec<(RecordId, f64)>>> {
	if !crate::catalog::keeps_vector_doc_elements(index_def.format_version) {
		return Ok(None);
	}
	let (table_id, ikb) = match ids {
		Some(ids) => ids,
		None => target_ids(ctx, table, index_def).await?,
	};
	let scored = exact_from_index_vectors(
		&AnnTarget {
			frozen_ctx: &ctx.root().ctx,
			index_def,
			ikb: &ikb,
			table_id,
		},
		vector,
		k,
		allow,
	)
	.await?;
	Ok(scored.map(|results| {
		results
			.into_iter()
			.map(|(rid, distance, _)| (Arc::unwrap_or_clone(rid), distance))
			.collect()
	}))
}

/// The condition a search judges each candidate on: pipeline `pipeline` of
/// the program, run on the candidate's data.
struct Judge<'a> {
	program: &'a Program,
	pipeline: u16,
	ctx: &'a ExecutionContext,
}

impl CandidateCondition for Judge<'_> {
	fn matches<'a>(&'a self, _rid: &'a Arc<RecordId>, record: &'a Arc<Record>) -> BoxGateFut<'a> {
		Box::pin(async move {
			let verdict = admits(self.program, self.pipeline, self.ctx, &record.data).await;
			Ok(verdict.catch_return()?.is_truthy())
		})
	}
}

/// Whether host pipeline `pipeline` of the program emits a row for `row`:
/// `true` for a record that passes what it judges, as a boolean value.
async fn admits(
	program: &Program,
	pipeline: u16,
	ctx: &ExecutionContext,
	row: &Value,
) -> FlowResult<Value> {
	let mut state = statement_state(program, ctx)?;
	state.rowless = false;
	let code = &program.host_code[pipeline as usize];
	out_of_line(|| run_cow_row(program, code, Cow::Borrowed(row), &mut state, ctx)).await?;
	Ok(Value::Bool(!state.output.is_empty()))
}
