//! Plan-time utility helpers.
//!
//! Organised into focused submodules; this `mod.rs` re-exports the
//! complete public surface so `use super::util::{…}` keeps working
//! unchanged in callers (`select.rs`, `source.rs`, `aggregate.rs`).
//!
//! - [`literals`] — pure value/expression conversion and the constant folder used to reduce
//!   deterministic WHERE expressions to literals.
//! - [`conditions`] — predicate analysis (top-level OR, KNN/FTS detection), condition stripping
//!   after a scan operator has consumed a predicate, record-ID point-lookup detection, and MATCHES
//!   context collection.
//! - [`params`] — plan-time bind-parameter resolution and projection function-to-idiom rewriting.
//! - [`fields`] — field-name and field-path derivation, plus GROUP BY row-scope validation.
//! - [`optimization`] — fast-path eligibility (CountScan/IndexCountScan, ORDER BY pushdown,
//!   sort-elimination, VERSION extraction, LIMIT helpers).

mod conditions;
mod fields;
mod literals;
mod optimization;
mod params;

pub(crate) use conditions::{
	all_value_sources, extract_bruteforce_knn, extract_matches_context,
	extract_record_id_point_lookup, extract_table_from_context, has_knn_k_operator,
	has_knn_ktree_operator, has_knn_operator, has_top_level_or, strip_fts_condition,
	strip_index_conditions, strip_knn_from_condition, strip_union_index_conditions,
};
pub(crate) use fields::{
	check_forbidden_group_by_params, derive_field_name, idiom_to_field_name, idiom_to_field_path,
};
pub(crate) use literals::{
	fold_condition_expressions, key_lit_to_expr, literal_to_value, try_literal_to_value,
};
pub(crate) use optimization::{
	extract_count_field_names, extract_version, get_effective_limit_literal, index_covers_ordering,
	is_bounded_topk_downstream, is_count_all_eligible, is_indexed_count_eligible,
	order_is_scan_compatible,
};
pub(crate) use params::{
	SELECT_ITERATION_PARAMS, resolve_condition_params, resolve_param_value,
	resolve_projection_field_idioms,
};
