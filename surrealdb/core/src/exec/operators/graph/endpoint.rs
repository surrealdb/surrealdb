//! The `EndpointBind` operator — bind a node from a bound edge's endpoint.
//!
//! `EndpointBind` is the companion of [`Expand`](super::expand::Expand) for
//! *edge-anchored* MATCH patterns. When a pattern is anchored on a labeled edge
//! (`(a)-[:x]->(b)` with no labeled node), the planner scans the edge table and
//! [`Bind`](crate::exec::operators::bind::Bind)s each edge record under a binding name, then
//! stacks two `EndpointBind`s on top to recover the two endpoint nodes — the
//! `in` endpoint and the `out` endpoint — as full node bindings. See the worked
//! plan tree (ii) in `doc/gql/V2_DESIGN.md` §6:
//!
//! ```text
//! EndpointBind [edge: __e0, field: out, node: b]
//!     EndpointBind [edge: __e0, field: in, node: a]
//!         Bind [binding: __e0]  ← TableScan [table: x]
//! ```
//!
//! For every input row it:
//!
//! 1. reads `row[edge]` — a full edge record object bound by a prior `Bind` on an edge table (see
//!    the edge-extraction rules below);
//! 2. reads that edge's `in`/`out` field (per `field`), which holds the endpoint vertex
//!    [`RecordId`];
//! 3. applies `target_label` (an endpoint whose table differs is dropped);
//! 4. batch-fetches the endpoint node through the FieldState-aware
//!    [`fetch`](crate::exec::operators::scan::fetch) helper (a miss or permission denial drops the
//!    row);
//! 5. emits `input ∪ {target_binding: node_obj}`.
//!
//! The mapping is **1:≤1** and **order-preserving**: each input row produces at
//! most one output row (the endpoint resolves to a single vertex), and rows are
//! emitted in input order. Cardinality and ordering are therefore delegated to
//! the input.
//!
//! ## Edge extraction
//!
//! - `row[edge]` is a full edge object ⇒ its `field` (`in`/`out`) value is read; a missing or
//!   non-record `field` value drops the row (`debug!` log, never a panic);
//! - `row[edge]` is itself a `Value::RecordId` ⇒ the row is dropped: an id-only edge carries no
//!   `in`/`out` to read, and the planner only stacks `EndpointBind` above a `Bind` that holds the
//!   full edge record. (Defensive; unreachable on valid plans.)
//! - `row[edge]` is `Value::Null` (a prior optional miss), is missing, or holds any other value ⇒
//!   the row is dropped with a `debug!` log.
//!
//! ## Security
//!
//! The endpoint node binding is produced by the FieldState-aware
//! [`fetch::resolve_with_field_state`] helper, exactly as `Expand` resolves its
//! targets: table-level SELECT permission, computed fields, and field-level
//! SELECT permissions are all applied, so the binding contents are exactly what
//! a `SELECT` on that table would return for the caller. A permission denial is
//! indistinguishable from a missing record — both drop the row.

use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use tracing::debug;

use crate::exec::operators::scan::fetch::{FetchFieldStateCache, resolve_with_field_state};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, OutputOrdering, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::val::{Object, RecordId, TableName, Value};

/// Which endpoint of the bound edge to bind as the target node.
///
/// Operator-local mirror of the edge `in`/`out` field selection; kept here so
/// this operator compiles without the `gql` feature — matching the
/// `graph/expand.rs` precedent for [`ExpandDir`](super::expand::ExpandDir).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointField {
	/// Bind the edge's `in` endpoint.
	In,
	/// Bind the edge's `out` endpoint.
	Out,
}

impl EndpointField {
	/// The edge-record field holding this endpoint's vertex.
	fn field(self) -> &'static str {
		match self {
			EndpointField::In => "in",
			EndpointField::Out => "out",
		}
	}
}

/// Bind a node from a bound edge's `in`/`out` endpoint.
///
/// See the module docs for the per-row algorithm and the edge-extraction rules.
#[derive(Debug, Clone)]
pub struct EndpointBind {
	/// Child operator producing input binding rows.
	pub(crate) input: Arc<dyn ExecOperator>,
	/// Name of the bound edge binding to read the endpoint from. The bound value
	/// must be a full edge record object (produced by a prior `Bind` on an edge
	/// table).
	pub(crate) edge: String,
	/// Which endpoint (`in`/`out`) to bind.
	pub(crate) field: EndpointField,
	/// Name of the target node binding to introduce.
	pub(crate) target_binding: String,
	/// Optional label filter on the endpoint vertex's table.
	pub(crate) target_label: Option<TableName>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl EndpointBind {
	/// Create a new `EndpointBind` operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		edge: String,
		field: EndpointField,
		target_binding: String,
		target_label: Option<TableName>,
	) -> Self {
		Self {
			input,
			edge,
			field,
			target_binding,
			target_label,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for EndpointBind {
	fn name(&self) -> &'static str {
		"EndpointBind"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![
			("edge".to_string(), self.edge.clone()),
			("field".to_string(), self.field.field().to_string()),
			("node".to_string(), self.target_binding.clone()),
		];
		if let Some(label) = self.target_label.as_ref() {
			attrs.push(("target_label".to_string(), label.as_str().to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Endpoint fetches need database context; never demote below the input.
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		// 1:≤1 — each input row produces at most one output row, so the input's
		// cardinality is an upper bound and is preserved.
		self.input.cardinality_hint()
	}

	fn output_ordering(&self) -> OutputOrdering {
		// Rows are emitted in input order (filtered, never reordered).
		self.input.output_ordering()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// SECURITY: the endpoint node binding is produced by the FieldState
		// helper ([`resolve_with_field_state`]), which resolves whether SELECT
		// permissions must be enforced (once, internally) and applies table +
		// field SELECT permissions and computed fields per record.
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);

		let edge = self.edge.clone();
		let field = self.field;
		let target_binding = self.target_binding.clone();
		let target_label = self.target_label.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// One FieldState cache for the endpoint table, reused across batches
			// so each table's permission + FieldState are resolved at most once.
			let mut target_cache = FetchFieldStateCache::new();

			while let Some(batch_result) = input_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;

				// Collect the endpoint rids to fetch for this batch, keeping a
				// parallel record of which input row each resolved endpoint maps
				// back to. Rows whose edge binding is missing / not a record /
				// label-mismatched never enter `targets_to_fetch` and so are
				// dropped here.
				let mut targets_to_fetch: Vec<RecordId> = Vec::with_capacity(batch.values.len());
				let mut row_positions: Vec<usize> = Vec::with_capacity(batch.values.len());

				for (i, row) in batch.values.iter().enumerate() {
					match extract_endpoint_id(row, &edge, field) {
						Some(rid) => {
							// Apply the target_label filter before fetching.
							if let Some(label) = target_label.as_ref()
								&& &rid.table != label
							{
								continue;
							}
							targets_to_fetch.push(rid);
							row_positions.push(i);
						}
						None => {
							// Malformed plan, optional miss, or an edge with no
							// readable endpoint: drop the row without panicking.
							// The lowering guarantees a full edge object here, so
							// this is unreachable on valid plans.
							debug!(
								binding = %edge,
								"EndpointBind: edge binding missing, not a full edge record, or has no endpoint; dropping row"
							);
						}
					}
				}

				// Batch-fetch the endpoint nodes (FieldState-aware). Positional
				// with `targets_to_fetch`; `None` ⇒ missing or permission-denied
				// ⇒ drop.
				let target_values =
					resolve_with_field_state(&ctx, &mut target_cache, &targets_to_fetch).await?;

				let mut out: Vec<Value> = Vec::with_capacity(target_values.len());
				for (slot, target_value) in target_values.into_iter().enumerate() {
					let Some(target_value) = target_value else {
						continue;
					};
					let input_row = &batch.values[row_positions[slot]];
					out.push(bound_row(input_row, &target_binding, target_value));
				}

				if !out.is_empty() {
					yielder
						.emit(ValueBatch {
							values: out,
						})
						.await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "EndpointBind", &self.metrics))
	}
}

/// Read the endpoint vertex [`RecordId`] from `row[edge]`'s `in`/`out` field per
/// the edge-extraction rules (module docs). Returns `None` (the row drops) for a
/// missing / non-object / non-record-id edge, an id-only edge, an optional-miss
/// `Null`, or an edge whose endpoint field is missing or not a record id.
fn extract_endpoint_id(row: &Value, edge: &str, field: EndpointField) -> Option<RecordId> {
	let Value::Object(obj) = row else {
		return None;
	};
	let Some(Value::Object(edge_obj)) = obj.get(edge) else {
		// Missing binding, an id-only edge (bare RecordId, no endpoints to read),
		// an optional-miss Null, or any other non-object value: drop.
		return None;
	};
	match edge_obj.get(field.field()) {
		Some(Value::RecordId(rid)) => Some(rid.clone()),
		_ => None,
	}
}

/// Assemble the output row: clone the input object and insert the target node
/// binding.
fn bound_row(input: &Value, target_binding: &str, target: Value) -> Value {
	let mut obj = match input {
		Value::Object(o) => o.clone(),
		// `extract_endpoint_id` only yields `Some` for object rows, so this is
		// unreachable in practice; default to an empty object defensively.
		_ => Object::default(),
	};
	obj.insert(target_binding, target);
	Value::Object(obj)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::ValuesOperator;

	fn obj_row(pairs: &[(&str, Value)]) -> Value {
		let mut o = Object::default();
		for (k, v) in pairs {
			o.insert(*k, v.clone());
		}
		Value::Object(o)
	}

	fn rid(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table.to_string()),
			key: crate::val::RecordIdKey::from(key.to_string()),
		}
	}

	/// A full edge record object with `in`/`out` endpoints.
	fn edge_obj(table: &str, key: &str, in_rid: RecordId, out_rid: RecordId) -> Value {
		obj_row(&[
			("id", Value::RecordId(rid(table, key))),
			("in", Value::RecordId(in_rid)),
			("out", Value::RecordId(out_rid)),
		])
	}

	fn sample(field: EndpointField, target_label: Option<&str>) -> EndpointBind {
		EndpointBind::new(
			ValuesOperator::new(Vec::new()),
			"__e0".to_string(),
			field,
			"a".to_string(),
			target_label.map(|l| TableName::new(l.to_string())),
		)
	}

	#[test]
	fn name_is_endpoint_bind() {
		assert_eq!(sample(EndpointField::In, None).name(), "EndpointBind");
	}

	#[test]
	fn attrs_render_edge_field_node_and_label() {
		let attrs = sample(EndpointField::Out, Some("person")).attrs();
		assert!(attrs.iter().any(|(k, v)| k == "edge" && v == "__e0"));
		assert!(attrs.iter().any(|(k, v)| k == "field" && v == "out"));
		assert!(attrs.iter().any(|(k, v)| k == "node" && v == "a"));
		assert!(attrs.iter().any(|(k, v)| k == "target_label" && v == "person"));
	}

	#[test]
	fn attrs_omit_label_when_unset() {
		let attrs = sample(EndpointField::In, None).attrs();
		assert!(attrs.iter().any(|(k, v)| k == "field" && v == "in"));
		assert!(!attrs.iter().any(|(k, _)| k == "target_label"));
	}

	#[test]
	fn required_context_is_at_least_database() {
		assert_eq!(sample(EndpointField::In, None).required_context(), ContextLevel::Database);
	}

	#[test]
	fn field_maps_to_edge_field_name() {
		assert_eq!(EndpointField::In.field(), "in");
		assert_eq!(EndpointField::Out.field(), "out");
	}

	#[test]
	fn extract_endpoint_id_reads_correct_field() {
		let row = obj_row(&[(
			"__e0",
			edge_obj("knows", "e1", rid("person", "alice"), rid("person", "bob")),
		)]);
		assert_eq!(
			extract_endpoint_id(&row, "__e0", EndpointField::In),
			Some(rid("person", "alice"))
		);
		assert_eq!(
			extract_endpoint_id(&row, "__e0", EndpointField::Out),
			Some(rid("person", "bob"))
		);
	}

	#[test]
	fn extract_endpoint_id_drops_missing_binding() {
		let row = obj_row(&[("__e1", edge_obj("knows", "e1", rid("p", "a"), rid("p", "b")))]);
		assert_eq!(extract_endpoint_id(&row, "__e0", EndpointField::In), None);
	}

	#[test]
	fn extract_endpoint_id_drops_id_only_edge() {
		// A bare RecordId edge (id-only) has no endpoints to read.
		let row = obj_row(&[("__e0", Value::RecordId(rid("knows", "e1")))]);
		assert_eq!(extract_endpoint_id(&row, "__e0", EndpointField::Out), None);
	}

	#[test]
	fn extract_endpoint_id_drops_optional_miss_and_bad_values() {
		// Optional-miss Null.
		let null_row = obj_row(&[("__e0", Value::Null)]);
		assert_eq!(extract_endpoint_id(&null_row, "__e0", EndpointField::In), None);
		// Non-record edge value.
		let bad_row = obj_row(&[("__e0", Value::Bool(true))]);
		assert_eq!(extract_endpoint_id(&bad_row, "__e0", EndpointField::In), None);
		// Edge object whose endpoint field is missing.
		let no_field =
			obj_row(&[("__e0", obj_row(&[("id", Value::RecordId(rid("knows", "e1")))]))]);
		assert_eq!(extract_endpoint_id(&no_field, "__e0", EndpointField::Out), None);
		// Edge object whose endpoint field is not a record id.
		let bad_field =
			obj_row(&[("__e0", obj_row(&[("in", Value::Bool(true)), ("out", Value::Bool(true))]))]);
		assert_eq!(extract_endpoint_id(&bad_field, "__e0", EndpointField::In), None);
		// A non-object row drops.
		assert_eq!(extract_endpoint_id(&Value::Bool(true), "__e0", EndpointField::In), None);
	}

	#[test]
	fn bound_row_inserts_target_preserving_input() {
		let input = obj_row(&[(
			"__e0",
			edge_obj("knows", "e1", rid("person", "alice"), rid("person", "bob")),
		)]);
		let node = obj_row(&[
			("id", Value::RecordId(rid("person", "alice"))),
			("name", Value::from("Alice")),
		]);
		let row = bound_row(&input, "a", node.clone());
		let Value::Object(o) = row else {
			panic!("expected object");
		};
		// Input edge binding preserved.
		assert!(o.get("__e0").is_some());
		// Target node bound under its name.
		assert_eq!(o.get("a"), Some(&node));
	}

	#[test]
	fn bound_row_overwrites_only_target_binding() {
		// The target binding is inserted even if a value already exists; the rest
		// of the row is untouched.
		let input = obj_row(&[("x", Value::from(1)), ("a", Value::from(0))]);
		let row = bound_row(&input, "a", Value::from(42));
		let Value::Object(o) = row else {
			panic!("expected object");
		};
		assert_eq!(o.get("x"), Some(&Value::from(1)));
		assert_eq!(o.get("a"), Some(&Value::from(42)));
	}
}
