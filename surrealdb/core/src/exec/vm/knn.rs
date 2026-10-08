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
//! The allow-list decides the strategy by its cardinality
//! ([`choose_prefilter_tier`]): a small list is scored exactly, without the
//! graph; a larger one gates the graph traversal on membership, with a wider
//! search for the mid tier. A list that overflows its branch budget, an index
//! with no shared doc-IDs or a versioned read search the graph with the full
//! condition judged on every candidate.

use std::borrow::Cow;
use std::sync::Arc;

use reblessive::tree::Stk;
use roaring::RoaringTreemap;

use super::batch::{BatchSizer, read_sized};
use super::host::{out_of_line, run_cow_row, statement_state};
use super::program::{KnnGate, Program};
use super::services::{EngineServices, Services};
use crate::catalog::{Index, Record};
use crate::exe::FlowResultExt as _;
use crate::exec::field_path_convert::field_path_from_idiom;
use crate::exec::index::access_path::IndexRef;
use crate::exec::operators::scan::{
	PrefilterTier, boosted_ef, choose_prefilter_tier, knn_allow_list,
};
use crate::exec::operators::{KnnTopKHeap, extract_vector};
use crate::exec::permission::validate_record_user_access;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::idx::trees::KnnCondFilter;
use crate::idx::trees::gate::{
	BoxGateFut, CachedTableSelect, CandidateCondition, CandidateFetchCounter,
};
use crate::idx::trees::vector::{score_raw_vector, typed_query_vector};
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
	// doc-IDs, and the strategy follows its cardinality. When the list cannot
	// be built, the full condition is judged on every candidate.
	let mut tier = None;
	let mut allow: Option<RoaringTreemap> = None;
	let mut in_search = gate.residual;
	if let Some(prefilter) = gate.prefilter.as_ref().filter(|_| index_def.uses_shared_doc_ids()) {
		if version.is_some() {
			tier = Some(PrefilterTier::Fallback);
		} else {
			match knn_allow_list(ctx, table, &prefilter.root).await? {
				Some(list) => {
					tier = Some(choose_prefilter_tier(
						list.len(),
						*surrealdb_cnf::KNN_PREFILTER_EXACT_THRESHOLD,
						*surrealdb_cnf::KNN_PREFILTER_EF_BOOST_THRESHOLD,
					));
					in_search = prefilter.residual;
					allow = Some(list);
				}
				None => tier = Some(PrefilterTier::Fallback),
			}
		}
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

	if matches!(tier, Some(PrefilterTier::Exact)) {
		let allow = allow.as_ref().expect("the exact tier holds an allow-list");
		let per_row = matches_gate
			.or(gate.prefilter.as_ref().and_then(|p| p.residual))
			.or(judges_permission.then_some(gate.permission).flatten());
		return exact(program, ctx, table, index, vector, k, allow, per_row).await;
	}

	// The graph tiers: the allow-list gates admission, with a wider search
	// on the mid-selectivity tier.
	let (ef, search_allow) = match tier {
		Some(PrefilterTier::Graph) => (
			boosted_ef(
				ef,
				*surrealdb_cnf::KNN_PREFILTER_EF_BOOST,
				*surrealdb_cnf::KNN_PREFILTER_EF_MAX,
			),
			allow.as_ref(),
		),
		Some(PrefilterTier::GraphUnboosted) => (ef, allow.as_ref()),
		_ => (ef, None),
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

/// Exact scoring of the members of `allow`, without the graph: each is read,
/// judged by `per_row` where there is one, and scored against `vector` into a
/// bounded heap, so a record that is rejected takes no slot.
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
		let rows = read_sized(ctx, &mut sizer, &rids, None, CachePolicy::ReadWrite).await?;
		for row in rows {
			if let Some(pipeline) = per_row
				&& !admits(program, pipeline, ctx, &row).await.catch_return()?.is_truthy()
			{
				continue;
			}
			let Some(record) = extract_vector(&row, &field) else {
				continue;
			};
			let Some(scored) = score_raw_vector(&distance, vector_type, dimension, &query, &record)
			else {
				continue;
			};
			let Value::Object(object) = &row else {
				continue;
			};
			let Some(Value::RecordId(rid)) = object.get("id") else {
				continue;
			};
			heap.offer(Number::Float(scored), rid.clone());
		}
	}
	Ok(heap
		.into_sorted_nearest_first()
		.into_iter()
		.map(|(distance, rid)| (rid, distance.to_float()))
		.collect())
}

/// The condition a search judges each candidate on: pipeline `pipeline` of
/// the program, run on the candidate's data.
struct Judge<'a> {
	program: &'a Program,
	pipeline: u16,
	ctx: &'a ExecutionContext,
}

impl CandidateCondition for Judge<'_> {
	fn matches<'a>(
		&'a self,
		_stk: &'a mut Stk,
		_rid: &'a Arc<RecordId>,
		record: &'a Arc<Record>,
	) -> BoxGateFut<'a> {
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
