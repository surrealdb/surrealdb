//! KNN scan operator for ANN index-backed vector search.
//!
//! This operator performs approximate nearest-neighbor search using an ANN
//! index. It retrieves the top-K records closest to a query vector, ordered by
//! distance (nearest first).

use std::collections::VecDeque;
use std::sync::Arc;

use roaring::RoaringTreemap;

use crate::catalog::{Error, Index, keeps_vector_doc_elements};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::trees::{AnnGraphShape, KnnCondFilter, KnnIteratorResult};
use crate::val::{Number, RecordId, TableName};

/// Execution strategy chosen by the KNN prefilter triage (#548), keyed on
/// the allow-list cardinality.
///
/// Stored as a `u8` on the operator after execution so EXPLAIN ANALYZE can
/// render the tier actually taken (`0` = no prefilter ran).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PrefilterTier {
	/// The allow-list build overflowed its branch budget (or a versioned
	/// execution reached the operator): run the pre-prefilter path with the
	/// full residual evaluated in-traversal.
	Fallback = 1,
	/// Graph-free exact scoring over the allow-list members.
	Exact = 2,
	/// Allow-list-gated graph traversal with boosted ef.
	Graph = 3,
	/// Allow-list-gated graph traversal without ef boost: above the boost
	/// threshold the allow-list barely rejects candidates on typical tables,
	/// so no recall compensation is needed — and when the list IS still
	/// selective relative to a huge table, gated traversal stays correct and
	/// merely walks further. (An unrestricted-search-plus-post-check tier was
	/// deliberately rejected: an absolute cardinality says nothing about how
	/// near-universe the predicate is, and post-checking truncates the result
	/// set when the nearest overall neighbours are excluded.)
	GraphUnboosted = 4,
}

impl PrefilterTier {
	/// EXPLAIN label for a stored tier code (`0` = not run ⇒ `None`).
	pub(crate) fn label(code: u8) -> Option<&'static str> {
		match code {
			1 => Some("fallback"),
			2 => Some("exact"),
			3 => Some("graph"),
			4 => Some("graph_unboosted"),
			_ => None,
		}
	}
}

/// Pick the execution strategy from the allow-list cardinality (#548).
///
/// Thresholds are passed in (rather than read from the process-global cnf
/// statics) so the decision table is unit-testable.
pub(crate) fn choose_prefilter_tier(allow_len: u64, t_exact: u64, t_boost: u64) -> PrefilterTier {
	// Includes the empty allow-list: the exact tier over zero members
	// returns no rows without touching the graph — correct, since no record
	// satisfies the covered conjuncts.
	if allow_len <= t_exact {
		return PrefilterTier::Exact;
	}
	if allow_len <= t_boost {
		PrefilterTier::Graph
	} else {
		PrefilterTier::GraphUnboosted
	}
}

/// The settings the prefilter triage weighs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PrefilterSettings {
	/// Allow-list size at or below which the exact tier is always taken.
	pub(crate) exact_threshold: u64,
	/// Allow-list size above which the gated traversal is not widened.
	pub(crate) boost_threshold: u64,
	/// The `ef` multiplier of the widened traversal.
	pub(crate) boost: u32,
	/// The ceiling of the widened `ef`.
	pub(crate) ef_max: u32,
	/// The cost of scoring one member exactly, in traversal distance
	/// computations.
	pub(crate) exact_cost: u32,
}

impl PrefilterSettings {
	/// Whether [`plan_prefilter`] weighs the graph for an allow-list of
	/// `allow_len` members: it does not at or below the exact threshold, where
	/// the exact tier is taken whatever the graph, so a caller need not read
	/// the graph's shape there.
	pub(crate) fn weighs_graph(&self, allow_len: u64) -> bool {
		allow_len > self.exact_threshold
	}

	/// The settings the configuration sets for a search of `index`: an HNSW
	/// traversal and a DiskANN search are widened by settings of their own,
	/// since only DiskANN's admits members from a fixed-width candidate list.
	pub(crate) fn configured(index: &Index) -> Self {
		Self {
			exact_threshold: *surrealdb_cnf::KNN_PREFILTER_EXACT_THRESHOLD,
			boost_threshold: *surrealdb_cnf::KNN_PREFILTER_EF_BOOST_THRESHOLD,
			boost: if let Index::Hnsw(_) = index {
				*surrealdb_cnf::KNN_PREFILTER_HNSW_EF_BOOST
			} else {
				*surrealdb_cnf::KNN_PREFILTER_EF_BOOST
			},
			ef_max: *surrealdb_cnf::KNN_PREFILTER_EF_MAX,
			exact_cost: *surrealdb_cnf::KNN_PREFILTER_EXACT_COST,
		}
	}
}

/// The tier and the traversal `ef` of a search restricted to an allow-list of
/// `allow_len` members, requested with `ef`.
///
/// The cardinality decides first ([`choose_prefilter_tier`]). Where `graph`
/// describes the graph the traversal would walk — an index that scores the
/// members from the vectors it stores, with nothing to judge per record — a
/// graph tier gives way to the exact tier wherever scoring the members costs
/// no more than the traversal, in distance computations:
///
/// - the exact tier scores every member: `allow_len × exact_cost`;
/// - the traversal admits a share `s = allow_len / nodes` of the candidates it meets, so it expands
///   about `ef / s` nodes of `degree` neighbours each: `ef × degree / s`.
///
/// Multiplying both by `allow_len`, the exact tier is taken wherever
/// `allow_len² × exact_cost ≤ ef × degree × nodes`. The share is capped at
/// one: an allow-list naming more records than the graph holds nodes —
/// records sharing a vector, or vectors not yet compacted into the graph —
/// admits about every candidate, so `nodes` counts as at least `allow_len`.
/// The exact tier's `ef` is the requested one, which it does not use.
pub(crate) fn plan_prefilter(
	allow_len: u64,
	ef: u32,
	graph: Option<AnnGraphShape>,
	settings: &PrefilterSettings,
) -> (PrefilterTier, u32) {
	let tier = choose_prefilter_tier(allow_len, settings.exact_threshold, settings.boost_threshold);
	let walk_ef = match tier {
		PrefilterTier::Exact => return (tier, ef),
		PrefilterTier::Graph => boosted_ef(ef, settings.boost, settings.ef_max),
		PrefilterTier::Fallback | PrefilterTier::GraphUnboosted => ef,
	};
	if let Some(graph) = graph {
		let members = u128::from(allow_len);
		let nodes = u128::from(graph.nodes.max(allow_len));
		let exact = members.saturating_mul(members).saturating_mul(u128::from(settings.exact_cost));
		let walk = u128::from(walk_ef) * u128::from(graph.degree) * nodes;
		if exact <= walk {
			return (PrefilterTier::Exact, ef);
		}
	}
	(tier, walk_ef)
}

/// The shape of the graph ANN index `target` searches, where the prefilter's
/// cost model can weigh it: an HNSW graph, brought level with the snapshot as
/// a search would be. `None` for any other index.
pub(crate) async fn ann_graph_shape(
	target: &AnnTarget<'_>,
) -> Result<Option<AnnGraphShape>, ControlFlow> {
	let AnnTarget {
		frozen_ctx,
		index_def,
		ikb,
		table_id,
	} = *target;
	let Index::Hnsw(hnsw_params) = &index_def.index else {
		return Ok(None);
	};
	let hnsw_index = frozen_ctx
		.get_index_stores()
		.get_index_hnsw(frozen_ctx, table_id, ikb, hnsw_params, index_def.format_version)
		.await
		.context("Failed to get HNSW index")?;
	let shape =
		hnsw_index.graph_shape(frozen_ctx).await.context("Failed to read the HNSW graph")?;
	Ok(Some(shape))
}

/// Boosted ef for allow-list-gated traversal: `ef·boost` capped at `ef_max`,
/// never below the user-requested `ef` (#548). A selective allow-list rejects
/// most candidates, effectively thinning the graph; widening the search
/// counteracts the recall loss.
pub(crate) fn boosted_ef(ef: u32, boost: u32, ef_max: u32) -> u32 {
	ef.saturating_mul(boost).min(ef_max).max(ef)
}

/// The ANN index [`ann_search`] searches and what it is opened with.
#[derive(Clone, Copy)]
pub(crate) struct AnnTarget<'a> {
	pub(crate) frozen_ctx: &'a crate::ctx::FrozenContext,
	pub(crate) index_def: &'a crate::catalog::IndexDefinition,
	pub(crate) ikb: &'a crate::idx::IndexKeyBase,
	pub(crate) table_id: crate::catalog::TableId,
}

/// Search the ANN index of `target` for the `k` nearest neighbours of
/// `vector`, exploring `ef` candidates, nearest first.
///
/// `cond_filter` gates each candidate inside the traversal on the table's
/// permission and a pushed-down condition; `allow` restricts admission to a
/// bitmap of doc-IDs. With neither, the search ranks every record of the
/// index and applies no permission, which is the caller's to apply to what it
/// returns.
pub(crate) async fn ann_search(
	target: &AnnTarget<'_>,
	vector: &[Number],
	k: u32,
	ef: u32,
	cond_filter: Option<KnnCondFilter<'_>>,
	allow: Option<&RoaringTreemap>,
) -> Result<VecDeque<KnnIteratorResult>, ControlFlow> {
	let AnnTarget {
		frozen_ctx,
		index_def,
		ikb,
		table_id,
	} = *target;
	Ok(match &index_def.index {
		Index::Hnsw(hnsw_params) => {
			// Obtain the shared HNSW index
			let hnsw_index = frozen_ctx
				.get_index_stores()
				.get_index_hnsw(frozen_ctx, table_id, ikb, hnsw_params, index_def.format_version)
				.await
				.context("Failed to get HNSW index")?;

			// The search brings the graph level with this transaction's snapshot
			// itself, under the guard it walks the graph with.
			{
				let hnsw_index = &hnsw_index;
				async move {
					hnsw_index
						.knn_search(frozen_ctx, vector, k as usize, ef as usize, cond_filter, allow)
						.await
				}
			}
			.await
			.context("HNSW KNN search failed")?
		}
		#[cfg(diskann)]
		Index::DiskAnn(diskann_params) => {
			let diskann_index = frozen_ctx
				.get_index_stores()
				.get_index_diskann(index_def.format_version, table_id, ikb, diskann_params)
				.await
				.context("Failed to get DiskANN index")?;

			diskann_index.check_state().await.context("Failed to check DiskANN index state")?;

			{
				let diskann_index = &diskann_index;
				async move {
					diskann_index
						.knn_search(frozen_ctx, vector, k as usize, ef as usize, cond_filter, allow)
						.await
				}
			}
			.await
			.context("DiskANN KNN search failed")?
		}
		#[cfg(not(diskann))]
		Index::DiskAnn(_) => {
			Err(ControlFlow::Err(anyhow::anyhow!(
				"DISKANN indexes require a 64-bit, non-WASM platform"
			)))?;
			unreachable!()
		}
		_ => {
			Err(ControlFlow::Err(anyhow::anyhow!(
				"Index '{}' is not an ANN index",
				index_def.name
			)))?;
			unreachable!()
		}
	})
}

/// The `k` members of `allow` nearest `vector`, nearest first, each with its
/// distance, scored from the vectors ANN index `target` stores: no member's
/// record is read, and no permission or condition is judged, so this answers
/// only a search with nothing to evaluate per record.
///
/// `None` where the index keeps no doc-ID → elements mapping (an index
/// stamped before it kept one): the members' records are scored instead. Such
/// an index is not opened, so its store is not loaded for a search it would
/// decline.
pub(crate) async fn exact_from_index_vectors(
	target: &AnnTarget<'_>,
	vector: &[Number],
	k: u32,
	allow: &RoaringTreemap,
) -> Result<Option<VecDeque<KnnIteratorResult>>, ControlFlow> {
	let AnnTarget {
		frozen_ctx,
		index_def,
		ikb,
		table_id,
	} = *target;
	if !keeps_vector_doc_elements(index_def.format_version) {
		return Ok(None);
	}
	Ok(match &index_def.index {
		Index::Hnsw(hnsw_params) => {
			let hnsw_index = frozen_ctx
				.get_index_stores()
				.get_index_hnsw(frozen_ctx, table_id, ikb, hnsw_params, index_def.format_version)
				.await
				.context("Failed to get HNSW index")?;
			hnsw_index
				.exact_knn_search(frozen_ctx, vector, k as usize, allow)
				.await
				.context("HNSW exact KNN search failed")?
		}
		#[cfg(diskann)]
		Index::DiskAnn(diskann_params) => {
			let diskann_index = frozen_ctx
				.get_index_stores()
				.get_index_diskann(index_def.format_version, table_id, ikb, diskann_params)
				.await
				.context("Failed to get DiskANN index")?;
			diskann_index
				.exact_knn_search(frozen_ctx, vector, k as usize, allow)
				.await
				.context("DiskANN exact KNN search failed")?
		}
		_ => None,
	})
}

/// The records ANN index `index_def` of `table` ranks nearest to `vector`,
/// at most `k`, nearest first, each with its distance; the table's definition
/// is read as of `version`.
///
/// Without `cond_filter` and `allow` the ranking is the index's own, over
/// every record the index holds: no table permission, condition or allow-list
/// narrows the candidates, so the caller applies the table's permission to
/// the records it reads for them. `cond_filter` gates each candidate inside
/// the search on the table's permission and then a condition, and `allow`
/// restricts admission to a bitmap of doc-IDs.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn knn_candidates(
	ctx: &ExecutionContext,
	table: &TableName,
	index_def: &crate::catalog::IndexDefinition,
	vector: &[Number],
	k: u32,
	ef: u32,
	version: Option<u64>,
	cond_filter: Option<KnnCondFilter<'_>>,
	allow: Option<&RoaringTreemap>,
) -> FlowResult<Vec<(RecordId, f64)>> {
	let db_ctx = ctx.database().context("a KNN scan requires database context")?;
	let table_def = db_ctx.get_table_def(table, version).await.context("Failed to get table")?;
	let Some(table_def) = table_def else {
		return Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
			name: table.clone(),
		})));
	};
	let ikb = crate::idx::IndexKeyBase::new(
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		index_def.table_name.clone(),
		index_def.index_id,
	);
	let root = ctx.root();
	let results = ann_search(
		&AnnTarget {
			frozen_ctx: &root.ctx,
			index_def,
			ikb: &ikb,
			table_id: table_def.table_id,
		},
		vector,
		k,
		ef,
		cond_filter,
		allow,
	)
	.await?;
	// A record id the search alone holds is moved out, not cloned.
	Ok(results
		.into_iter()
		.map(|(rid, distance, _)| (Arc::unwrap_or_clone(rid), distance))
		.collect())
}

#[cfg(test)]
mod tests {

	use super::{
		AnnGraphShape, PrefilterSettings, PrefilterTier, boosted_ef, choose_prefilter_tier,
		plan_prefilter,
	};

	/// The full decision table of the prefilter triage (#548): every tier is
	/// allow-list-gated, differing only in graph use and ef boosting.
	#[test]
	fn prefilter_tier_decision_table() {
		use PrefilterTier::*;
		const T_EXACT: u64 = 2_000;
		const T_BOOST: u64 = 100_000;
		let choose = |len| choose_prefilter_tier(len, T_EXACT, T_BOOST);
		// Empty allow-list short-circuits through the exact tier.
		assert_eq!(choose(0), Exact);
		// Boundaries are inclusive on the lower tier.
		assert_eq!(choose(T_EXACT), Exact);
		assert_eq!(choose(T_EXACT + 1), Graph);
		assert_eq!(choose(T_BOOST), Graph);
		// Above the boost threshold, gated traversal continues without ef
		// boost — never an unrestricted search (post-checking would truncate
		// results whenever the nearest overall neighbours are excluded).
		assert_eq!(choose(T_BOOST + 1), GraphUnboosted);
		assert_eq!(choose(u64::MAX), GraphUnboosted);
	}

	/// Where the graph can be weighed, the exact tier is taken wherever scoring
	/// the members costs no more than the gated traversal, so the crossover
	/// grows with the square root of the graph; the boost widens only a graph
	/// tier, and the traversal is weighed at the width it would search.
	#[test]
	fn prefilter_cost_model() {
		use PrefilterTier::*;
		let settings = PrefilterSettings {
			exact_threshold: 2_000,
			boost_threshold: 100_000,
			boost: 1,
			ef_max: 1_024,
			exact_cost: 5,
		};
		let graph = |nodes| {
			Some(AnnGraphShape {
				nodes,
				degree: 32,
			})
		};
		let plan = |len, nodes| plan_prefilter(len, 64, graph(nodes), &settings);
		// The exact tier holds while len² × 5 ≤ 64 × 32 × nodes: up to 4_525
		// members of 50_000 nodes (√20_480_000 ≈ 4_525.4).
		assert_eq!(plan(2_500, 50_000), (Exact, 64));
		assert_eq!(plan(4_525, 50_000), (Exact, 64));
		assert_eq!(plan(4_526, 50_000), (Graph, 64));
		// Four times the nodes, twice the crossover (√81_920_000 ≈ 9_050.97).
		assert_eq!(plan(9_050, 200_000), (Exact, 64));
		assert_eq!(plan(9_051, 200_000), (Graph, 64));
		// At or below the threshold the exact tier holds whatever the graph,
		// and the graph is not weighed.
		assert_eq!(plan(2_000, 1), (Exact, 64));
		assert!(!settings.weighs_graph(0));
		assert!(!settings.weighs_graph(2_000));
		assert!(settings.weighs_graph(2_001));
		// No graph to weigh: the cardinality alone decides.
		assert_eq!(plan_prefilter(4_000, 64, None, &settings), (Graph, 64));
		assert_eq!(plan_prefilter(200_000, 64, None, &settings), (GraphUnboosted, 64));
		// A boost widens a graph tier, and the traversal is weighed at that
		// width: 256 × 32 × 50_000 / 5 has its crossover at 9_050.
		let boosted = PrefilterSettings {
			boost: 4,
			..settings
		};
		assert_eq!(plan_prefilter(9_050, 64, graph(50_000), &boosted), (Exact, 64));
		assert_eq!(plan_prefilter(9_051, 64, graph(50_000), &boosted), (Graph, 256));
		// Above the boost threshold the traversal is not widened.
		assert_eq!(plan_prefilter(200_000, 64, graph(1_000_000), &boosted), (GraphUnboosted, 64));
		// A graph holding fewer nodes than the allow-list names (none at all
		// before its first compaction, or records sharing vectors) admits
		// about every candidate: the share is capped at one, so the traversal
		// is weighed at `ef × degree` per member, here 2_048 × 2_500 against
		// 2_500² × 5 for the exact tier.
		assert_eq!(plan(2_500, 0), (Graph, 64));
		assert_eq!(plan(2_500, 1_000), (Graph, 64));
		// Below `ef × degree / exact_cost` (409) members the exact tier would
		// win at any share, which only a threshold below that reaches.
		let low = PrefilterSettings {
			exact_threshold: 0,
			..settings
		};
		assert_eq!(plan_prefilter(409, 64, graph(0), &low), (Exact, 64));
		assert_eq!(plan_prefilter(410, 64, graph(0), &low), (Graph, 64));
		// Extreme sizes do not overflow the comparison.
		assert_eq!(plan_prefilter(u64::MAX, u32::MAX, graph(u64::MAX), &boosted).0, GraphUnboosted);
	}

	/// An HNSW traversal is widened by its own setting, any other index's
	/// search by the general one; unset, they widen HNSW not at all and DiskANN
	/// fourfold.
	#[test]
	fn prefilter_boost_follows_the_index_kind() {
		use crate::catalog::{DiskAnnParams, Distance, HnswParams, Index, VectorType};
		use crate::val::Number;
		let hnsw = Index::Hnsw(HnswParams {
			dimension: 2,
			distance: Distance::Euclidean,
			vector_type: VectorType::F32,
			m: 8,
			m0: 16,
			ml: Number::Float(0.5),
			ef_construction: 40,
			extend_candidates: false,
			keep_pruned_connections: false,
			use_hashed_vector: false,
		});
		let diskann = Index::DiskAnn(DiskAnnParams {
			dimension: 2,
			distance: Distance::Euclidean,
			vector_type: VectorType::F32,
			degree: 16,
			l_build: 40,
			alpha: Number::Float(1.2),
			use_hashed_vector: false,
		});
		let boost = |index: &Index| PrefilterSettings::configured(index).boost;
		assert_eq!(boost(&hnsw), *surrealdb_cnf::KNN_PREFILTER_HNSW_EF_BOOST);
		assert_eq!(boost(&diskann), *surrealdb_cnf::KNN_PREFILTER_EF_BOOST);
		if std::env::var_os("SURREAL_KNN_PREFILTER_HNSW_EF_BOOST").is_none() {
			assert_eq!(boost(&hnsw), 1);
		}
		if std::env::var_os("SURREAL_KNN_PREFILTER_EF_BOOST").is_none() {
			assert_eq!(boost(&diskann), 4);
		}
	}

	/// ef boosting saturates, respects the cap, and never clamps below the
	/// user-requested ef.
	#[test]
	fn boosted_ef_bounds() {
		// Standard boost.
		assert_eq!(boosted_ef(40, 4, 1_024), 160);
		// Cap applies.
		assert_eq!(boosted_ef(400, 4, 1_024), 1_024);
		// The cap never reduces a user ef already above it.
		assert_eq!(boosted_ef(2_000, 4, 1_024), 2_000);
		// A zero boost disables widening but keeps the user ef.
		assert_eq!(boosted_ef(40, 0, 1_024), 40);
		// Saturating multiply.
		assert_eq!(boosted_ef(u32::MAX, 2, u32::MAX), u32::MAX);
	}

	/// Tier codes round-trip through the EXPLAIN label mapping.
	#[test]
	fn prefilter_tier_labels() {
		assert_eq!(PrefilterTier::label(0), None);
		assert_eq!(PrefilterTier::label(PrefilterTier::Fallback as u8), Some("fallback"));
		assert_eq!(PrefilterTier::label(PrefilterTier::Exact as u8), Some("exact"));
		assert_eq!(PrefilterTier::label(PrefilterTier::Graph as u8), Some("graph"));
		assert_eq!(
			PrefilterTier::label(PrefilterTier::GraphUnboosted as u8),
			Some("graph_unboosted")
		);
		assert_eq!(PrefilterTier::label(5), None);
	}
}
