//! Per-MATCH-statement edge-uniqueness (R2 DIFFERENT EDGES).
//!
//! The default GQL match mode is **DIFFERENT EDGES** (`doc/gql/V2_DESIGN.md`
//! R2): within one MATCH statement no edge *record* may bind twice (nodes repeat
//! freely). [`row_has_distinct_edges`] enforces that across a clause's edge-ish bindings —
//! the named/hidden single edges and the group variables produced by quantified
//! edges. Intra-group uniqueness (an edge repeating *within* a single quantified
//! traversal) is already guaranteed by path expansion's within-path edge check; this
//! check covers the *across-binding* case the plan cannot fold into a single
//! traversal.
//!
//! Per input row it collects the record id of every edge each binding holds and
//! drops the row unless those ids are pairwise distinct:
//!
//! | `row[name]`           | contributes                                  |
//! |-----------------------|----------------------------------------------|
//! | `Object` (full edge)  | its `.id` (a `RecordId`)                      |
//! | `RecordId` (hidden)   | itself                                        |
//! | `Array` (EdgeGroup)   | each element's id (`Object`→`.id`, `RecordId`→self) |
//! | `Null` (optional miss, PR-C) | nothing (skipped)                      |
//!
//! The collected count `k` is small (typically 2–4 edge bindings, each a single
//! edge or a short group), so the pairwise check is a plain `O(k²)` scan over a
//! `Vec` — no hashing overhead, no per-row allocation beyond that `Vec`.
//!
//! The plan decides *when* to apply this check: only when a clause has ≥2
//! edge-ish bindings, and it is skipped entirely when the edge-table sets are
//! statically disjoint (two edges from different tables can never share an id).
//! The placement logic lives in `exec/planner/match_plan.rs`. The check is
//! order-preserving and 1:≤1 (each input row yields at most one output row).

use crate::val::{RecordId, Value};

/// Decide whether `row` keeps all its edge bindings distinct.
///
/// Collects the record id of every edge the `edge_bindings` reference (flattening
/// group bindings) and returns `true` iff they are pairwise distinct. A row that
/// is not an object, or that binds nothing edge-ish, trivially passes.
pub(crate) fn row_has_distinct_edges(row: &Value, edge_bindings: &[String]) -> bool {
	let Value::Object(obj) = row else {
		// Binding rows are always objects; a non-object row carries no edge
		// bindings to compare, so it passes unchanged.
		return true;
	};
	// `k` is small (2–4 single edges or short groups), so a flat `Vec` with an
	// `O(k²)` pairwise scan beats a hash set on both allocation and constant
	// factors.
	let mut ids: Vec<RecordId> = Vec::new();
	for name in edge_bindings {
		for rid in slot_edge_ids(obj.get(name.as_str())) {
			if !push_distinct(&mut ids, rid) {
				return false;
			}
		}
	}
	true
}

/// The edge record ids a binding slot contributes to the R2 distinctness check,
/// flattening a group binding's element list:
///
/// - a full edge `Object` ⇒ its `.id`;
/// - a bare `RecordId` (hidden edge) ⇒ itself;
/// - an `Array` (EdgeGroup) ⇒ each element's id (`Object`→`.id`, `RecordId`→self);
/// - a `Null` (optional miss, PR-C) or any other / missing value ⇒ nothing.
fn slot_edge_ids(slot: Option<&Value>) -> Vec<RecordId> {
	match slot {
		// Full edge record: its id lives at `.id`.
		Some(Value::Object(edge)) => edge_id_from_object(edge).into_iter().collect(),
		// Hidden edge binding: a bare record id.
		Some(Value::RecordId(rid)) => vec![rid.clone()],
		// Group variable (quantified edge): an ordered list of edges.
		Some(Value::Array(group)) => group
			.iter()
			.filter_map(|elem| {
				if let Value::Object(edge) = elem {
					edge_id_from_object(edge)
				} else if let Value::RecordId(rid) = elem {
					Some(rid.clone())
				} else {
					// Non-edge group element: nothing to compare.
					None
				}
			})
			.collect(),
		// Optional miss (PR-C) or a missing/non-edge binding: contributes no id.
		_ => Vec::new(),
	}
}

/// Read the `id` field of an edge object as a `RecordId`, if present.
fn edge_id_from_object(edge: &crate::val::Object) -> Option<RecordId> {
	match edge.get("id") {
		Some(Value::RecordId(rid)) => Some(rid.clone()),
		_ => None,
	}
}

/// Append `rid` to `ids` unless an equal id is already present. Returns `false`
/// when `rid` duplicates an existing id (the row must be dropped), `true`
/// otherwise.
fn push_distinct(ids: &mut Vec<RecordId>, rid: RecordId) -> bool {
	if ids.contains(&rid) {
		return false;
	}
	ids.push(rid);
	true
}
