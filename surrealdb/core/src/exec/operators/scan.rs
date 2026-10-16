//! The storage reads the VM's host performs for a program: record decoding and
//! the per-record pipeline, index and full-text iterators, KNN and bitmap
//! helpers, graph and reference adjacency, and counts. Each reads the
//! underlying key-value store and applies table/field-level permissions.

pub(crate) mod bitmap;
pub(crate) mod common;
mod count;
pub(crate) mod fetch;
mod fulltext;
mod graph;
pub(crate) mod graph_keys;
pub(crate) mod id_range;
pub(crate) mod index_count;
pub(crate) mod key_matcher;
pub(crate) mod key_range;
mod knn;
pub(crate) mod lightweight;
pub(crate) mod pipeline;
mod record_id;
mod reference;
pub(crate) mod resolved;
mod union_index;

pub(crate) use bitmap::{
	BitmapTally, FusedCandidates, RESOLVE_BATCH_SIZE, evaluate_fusion_plan,
	exact_bitmap_cardinality, knn_allow_list,
};
pub use count::CountOutput;
pub(crate) use count::record_key_range;
pub(crate) use fulltext::{FULL_TEXT_BATCH_SIZE, FullTextHits, open_full_text_index};
pub use graph::EdgeTableSpec;
pub(crate) use graph::{PayloadTableSpec, payload_fields};
pub(crate) use knn::{
	AnnTarget, PrefilterSettings, PrefilterTier, ann_graph_shape, exact_from_index_vectors,
	knn_candidates, plan_prefilter,
};
pub(crate) use record_id::execute_record_lookup;
pub(crate) use reference::ref_key_range;
pub(crate) use union_index::MergeMode;
