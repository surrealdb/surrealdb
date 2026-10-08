//! Scan operators — operators that perform storage I/O and check permissions.
//!
//! All operators in this module read data from the underlying key-value store
//! (table scans, index scans, full-text search, KNN, graph traversals, etc.)
//! and handle table/field-level permissions.

pub(crate) mod bitmap;
pub(crate) mod common;
mod count;
mod dynamic;
mod empty;
pub(crate) mod fetch;
mod fulltext;
mod graph;
pub(crate) mod graph_keys;
#[cfg(test)]
mod index;
pub(crate) mod index_count;
pub(crate) mod key_matcher;
mod knn;
pub(crate) mod lightweight;
pub(crate) mod pipeline;
mod record_id;
mod reference;
pub(crate) mod resolved;
mod table;
mod union_index;

#[cfg(test)]
pub use bitmap::BitmapResolve;
pub(crate) use bitmap::{
	BitmapNode, FusedCandidates, RESOLVE_BATCH_SIZE, evaluate_fusion_plan,
	exact_bitmap_cardinality, knn_allow_list, resolve_doc_ids,
};
pub use count::CountOutput;
#[cfg(test)]
pub use count::CountScan;
pub(crate) use count::record_key_range;
#[cfg(test)]
pub use dynamic::DynamicScan;
#[cfg(test)]
pub use empty::EmptyScan;
#[cfg(test)]
pub use fulltext::FullTextScan;
pub(crate) use fulltext::{FULL_TEXT_BATCH_SIZE, FullTextHits, open_full_text_hits};
pub use graph::{EdgeTableSpec, GraphEdgeScan, GraphScanOutput};
pub(crate) use graph::{PayloadPredicateSpec, PayloadTableSpec, payload_fields};
#[cfg(test)]
pub use index::IndexScan;
#[cfg(test)]
pub(crate) use knn::KnnPrefilter;
#[cfg(test)]
pub use knn::KnnScan;
pub(crate) use knn::{PrefilterTier, boosted_ef, choose_prefilter_tier, knn_candidates};
#[cfg(test)]
pub use lightweight::LightweightEdgeScan;
#[cfg(test)]
pub use record_id::RecordIdScan;
pub(crate) use record_id::execute_record_lookup;
pub(crate) use reference::ref_key_range;
pub use reference::{ReferenceScan, ReferenceScanOutput};
#[cfg(test)]
pub use table::TableScan;
pub(crate) use union_index::MergeMode;
#[cfg(test)]
pub use union_index::UnionIndexScan;
#[cfg(test)]
pub(crate) use union_index::extract_rid;
