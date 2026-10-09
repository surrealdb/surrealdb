//! Plan-time utility helpers over the logical plan's scalar types.
//!
//! Organised into focused submodules; this `mod.rs` re-exports the complete
//! surface, so callers (`select/`, `source.rs`, `aggregate.rs`, the binding
//! passes, the dynamic scan) name one path, `super::util::{…}`.
//!
//! - [`literals`] — conversion between runtime values and logical literals in both directions, and
//!   the constant folder that reduces deterministic WHERE expressions to literals.
//! - [`conditions`] — predicate analysis (top-level OR, KNN/FTS detection), condition stripping
//!   after a scan operator has consumed a predicate, record-ID point-lookup detection, MATCHES
//!   context collection, and the comparison of logical idioms against catalog index columns.
//! - [`params`] — plan-time bind-parameter resolution and projection function-to-field rewriting.
//! - [`fields`] — field-name and field-path derivation, plus GROUP BY row-scope validation.
//! - [`optimization`] — ORDER BY pushdown eligibility, sort-elimination, VERSION extraction, LIMIT
//!   helpers.
//! - [`evaluates_against_row`] — the calls whose reads of the row their arguments do not show.

mod conditions;
mod fields;
mod literals;
mod optimization;
mod params;

pub(crate) use conditions::{
	BruteForceKnnParams, BruteForceKnnVector, exact_knn_by, expr_contains_matches,
	extract_bruteforce_knn, extract_matches_context, extract_record_id_point_lookup,
	extract_table_from_matches, has_knn_ktree_operator, has_knn_operator, idiom_matches_column,
	idiom_matches_containment, strip_fts_probes, strip_index_conditions,
	strip_knn_and_matches_from_condition, strip_knn_from_condition,
};
pub(crate) use fields::{
	check_forbidden_group_by_params, field_path_from_expr, idiom_is_id, path_steps,
};
pub(crate) use literals::{
	RestrictedFields, fold_expressions, key_lit_to_expr, source_field_reads_are_inert,
	substitution_is_lossy, try_literal_to_value, try_value_to_logical_expr, value_to_logical_expr,
};
pub(crate) use optimization::{index_covers_ordering, order_is_scan_compatible};
pub(crate) use params::{
	ParamBinding, SELECT_ITERATION_PARAMS, bind_param, resolve_param_value,
	resolve_projection_field_idioms,
};

/// Whether a call is evaluated against the row in a way its arguments do not
/// show: a projection function reads the row's fields by the names its
/// arguments evaluate to, and a user-defined function, a script or a module
/// function runs its body with the row as the current value. Which fields
/// such a call reads cannot be told from the expression, so an analysis of
/// what an expression reads off the row takes it as reading the row whole.
pub(crate) fn evaluates_against_row(
	call: &crate::expr::logical::LogicalCall,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	use crate::expr::Function;
	match &call.receiver {
		Function::Normal(name) => registry.is_projection(name),
		Function::Model(_) => false,
		Function::Custom(_)
		| Function::Script(_)
		| Function::Module(..)
		| Function::Silo {
			..
		} => true,
	}
}
