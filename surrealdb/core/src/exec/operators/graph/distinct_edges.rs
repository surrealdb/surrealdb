//! `DistinctEdges` — per-MATCH-statement edge-uniqueness (R2 DIFFERENT EDGES).
//!
//! The default GQL match mode is **DIFFERENT EDGES** (`doc/gql/V2_DESIGN.md`
//! R2): within one MATCH statement no edge *record* may bind twice (nodes repeat
//! freely). This operator enforces that across a clause's edge-ish bindings —
//! the named/hidden single edges and the group variables produced by quantified
//! edges. Intra-group uniqueness (an edge repeating *within* a single quantified
//! traversal) is already guaranteed by `PathExpand`'s within-path edge check; this
//! operator covers the *across-binding* case the planner cannot fold into a single
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
//! The planner decides *when* to insert this operator: only when a clause has ≥2
//! edge-ish bindings, and it is skipped entirely when the edge-table sets are
//! statically disjoint (two edges from different tables can never share an id).
//! This module is only the operator; the placement logic lives in
//! `exec/planner/match_plan.rs`. The transform is order-preserving and 1:≤1 (each
//! input row yields at most one output row), so it propagates the input ordering.

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;

use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	OutputOrdering, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::val::{RecordId, Value};

/// Drops rows in which any two of the clause's edge bindings reference the same
/// edge record, enforcing the per-MATCH DIFFERENT EDGES default (R2).
#[derive(Debug, Clone)]
pub struct DistinctEdges {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The clause's edge-ish binding names (single edges and group variables).
	pub(crate) edge_bindings: Vec<String>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DistinctEdges {
	/// Create a new `DistinctEdges` over `input`, enforcing pairwise-distinct
	/// edge ids across `edge_bindings`.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, edge_bindings: Vec<String>) -> Self {
		Self {
			input,
			edge_bindings,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for DistinctEdges {
	fn name(&self) -> &'static str {
		"DistinctEdges"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("edges".to_string(), self.edge_bindings.join(", "))]
	}

	fn output_ordering(&self) -> OutputOrdering {
		// Rows are only dropped, never reordered, so any ordering the input
		// guarantees is preserved.
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let edge_bindings = self.edge_bindings.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			while let Some(batch_result) = input_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				let mut values = Vec::new();
				for value in batch.into_values() {
					if row_has_distinct_edges(&value, &edge_bindings) {
						values.push(value);
					}
				}
				if !values.is_empty() {
					yielder.emit(ValueBatch::new(values)).await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "DistinctEdges", &self.metrics))
	}
}

/// Decide whether `row` keeps all its edge bindings distinct.
///
/// Collects the record id of every edge the `edge_bindings` reference (flattening
/// group bindings) and returns `true` iff they are pairwise distinct. A row that
/// is not an object, or that binds nothing edge-ish, trivially passes.
fn row_has_distinct_edges(row: &Value, edge_bindings: &[String]) -> bool {
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
			.filter_map(|elem| match elem {
				Value::Object(edge) => edge_id_from_object(edge),
				Value::RecordId(rid) => Some(rid.clone()),
				// Non-edge group element: nothing to compare.
				_ => None,
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{ValuesOperator, collect, root_ctx};
	use crate::val::{Array, Object, RecordId, RecordIdKey, TableName, Value};

	/// Build a record id `table:key`.
	fn rid(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table.to_string()),
			key: RecordIdKey::from(key.to_string()),
		}
	}

	/// Wrap an `id` record id into a full edge object `{ id: <rid> }`.
	fn edge_obj(table: &str, key: &str) -> Value {
		let mut o = Object::default();
		o.insert("id".to_string(), Value::RecordId(rid(table, key)));
		Value::Object(o)
	}

	/// Build a binding row object from `(name, value)` pairs.
	fn row(fields: &[(&str, Value)]) -> Value {
		let mut o = Object::default();
		for (k, v) in fields {
			o.insert(k.to_string(), v.clone());
		}
		Value::Object(o)
	}

	fn names(ns: &[&str]) -> Vec<String> {
		ns.iter().map(|s| s.to_string()).collect()
	}

	#[tokio::test]
	async fn passes_row_with_distinct_edges() {
		// Two distinct full edge objects across two bindings.
		let r = row(&[("k1", edge_obj("knows", "a")), ("k2", edge_obj("knows", "b"))]);
		let input = ValuesOperator::new(vec![r.clone()]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["k1", "k2"])));
		let ctx = root_ctx();
		assert_eq!(collect(&op, &ctx).await, vec![r]);
	}

	#[tokio::test]
	async fn drops_row_with_duplicate_edge() {
		// The same edge id bound twice across two bindings → dropped.
		let r = row(&[("k1", edge_obj("knows", "a")), ("k2", edge_obj("knows", "a"))]);
		let input = ValuesOperator::new(vec![r]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["k1", "k2"])));
		let ctx = root_ctx();
		assert!(collect(&op, &ctx).await.is_empty());
	}

	#[tokio::test]
	async fn drops_row_with_duplicate_hidden_record_id_edges() {
		// Hidden (id-only) edge bindings: bare record ids, same id twice.
		let r = row(&[
			("__e0", Value::RecordId(rid("knows", "x"))),
			("__e1", Value::RecordId(rid("knows", "x"))),
		]);
		let input = ValuesOperator::new(vec![r]);
		let op: Arc<dyn ExecOperator> =
			Arc::new(DistinctEdges::new(input, names(&["__e0", "__e1"])));
		let ctx = root_ctx();
		assert!(collect(&op, &ctx).await.is_empty());
	}

	#[tokio::test]
	async fn flattens_group_binding_and_drops_internal_duplicate() {
		// A single group (EdgeGroup) binding containing a repeated edge id is
		// dropped by flattening then comparing pairwise.
		let group = Value::Array(Array(vec![edge_obj("knows", "a"), edge_obj("knows", "a")]));
		let r = row(&[("g", group)]);
		let input = ValuesOperator::new(vec![r]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["g"])));
		let ctx = root_ctx();
		assert!(collect(&op, &ctx).await.is_empty());
	}

	#[tokio::test]
	async fn passes_group_binding_with_distinct_edges() {
		// A group of two distinct edges passes.
		let group = Value::Array(Array(vec![edge_obj("knows", "a"), edge_obj("knows", "b")]));
		let r = row(&[("g", group)]);
		let input = ValuesOperator::new(vec![r.clone()]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["g"])));
		let ctx = root_ctx();
		assert_eq!(collect(&op, &ctx).await, vec![r]);
	}

	#[tokio::test]
	async fn drops_when_single_edge_collides_with_group_element() {
		// Mixed group + single edge: the single edge id also appears inside the
		// group → dropped (cross-binding duplicate).
		let group = Value::Array(Array(vec![edge_obj("knows", "a"), edge_obj("knows", "b")]));
		let r = row(&[("g", group), ("k", edge_obj("knows", "a"))]);
		let input = ValuesOperator::new(vec![r]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["g", "k"])));
		let ctx = root_ctx();
		assert!(collect(&op, &ctx).await.is_empty());
	}

	#[tokio::test]
	async fn passes_mixed_group_and_distinct_single_edge() {
		// Mixed group + single edge, all distinct → passes.
		let group = Value::Array(Array(vec![edge_obj("knows", "a"), edge_obj("knows", "b")]));
		let r = row(&[("g", group), ("k", edge_obj("knows", "c"))]);
		let input = ValuesOperator::new(vec![r.clone()]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["g", "k"])));
		let ctx = root_ctx();
		assert_eq!(collect(&op, &ctx).await, vec![r]);
	}

	#[tokio::test]
	async fn null_binding_is_skipped() {
		// An optional-miss Null binding contributes no id; the surviving single
		// edge keeps the row.
		let r = row(&[("k", edge_obj("knows", "a")), ("opt", Value::Null)]);
		let input = ValuesOperator::new(vec![r.clone()]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["k", "opt"])));
		let ctx = root_ctx();
		assert_eq!(collect(&op, &ctx).await, vec![r]);
	}

	#[tokio::test]
	async fn empty_group_passes() {
		// A zero-length path's group binding is `[]`; it contributes nothing.
		let r = row(&[("g", Value::Array(Array(Vec::new()))), ("k", edge_obj("knows", "a"))]);
		let input = ValuesOperator::new(vec![r.clone()]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["g", "k"])));
		let ctx = root_ctx();
		assert_eq!(collect(&op, &ctx).await, vec![r]);
	}

	#[tokio::test]
	async fn preserves_order_dropping_only_offenders() {
		// Stream of three rows; the middle one has a duplicate edge and is
		// dropped, the survivors keep their relative order.
		let keep1 = row(&[("k1", edge_obj("knows", "a")), ("k2", edge_obj("knows", "b"))]);
		let drop = row(&[("k1", edge_obj("knows", "c")), ("k2", edge_obj("knows", "c"))]);
		let keep2 = row(&[("k1", edge_obj("knows", "d")), ("k2", edge_obj("knows", "e"))]);
		let input = ValuesOperator::new(vec![keep1.clone(), drop, keep2.clone()]);
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["k1", "k2"])));
		let ctx = root_ctx();
		assert_eq!(collect(&op, &ctx).await, vec![keep1, keep2]);
	}

	#[tokio::test]
	async fn empty_input_yields_nothing() {
		let input = ValuesOperator::new(Vec::new());
		let op: Arc<dyn ExecOperator> = Arc::new(DistinctEdges::new(input, names(&["k1", "k2"])));
		let ctx = root_ctx();
		assert!(collect(&op, &ctx).await.is_empty());
	}

	#[test]
	fn reports_name_and_edges_attr() {
		let op = DistinctEdges::new(ValuesOperator::new(Vec::new()), names(&["k1", "k2"]));
		assert_eq!(op.name(), "DistinctEdges");
		assert_eq!(op.attrs(), vec![("edges".to_string(), "k1, k2".to_string())]);
	}
}
