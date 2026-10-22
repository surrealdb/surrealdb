//! Binding a node from a bound edge's endpoint.
//!
//! [`bind_endpoints`] is the companion of [`expand`](super::expand) for
//! *edge-anchored* MATCH patterns. When a pattern is anchored on a labeled edge
//! (`(a)-[:x]->(b)` with no labeled node), the plan scans the edge table and binds
//! each edge record under a binding name, then runs two endpoint steps to recover
//! the two endpoint nodes — the `in` endpoint and the `out` endpoint — as full
//! node bindings, read from the full edge object that the earlier scan step bound.
//!
//! For every input row it:
//!
//! 1. reads `row[edge]` — a full edge record object bound by a prior step on an edge table (see the
//!    edge-extraction rules below);
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
//!   `in`/`out` to read, and the plan only runs an endpoint step above a binding that holds the
//!   full edge record. (Defensive; unreachable on valid plans.)
//! - `row[edge]` is `Value::Null` (a prior optional miss), is missing, or holds any other value ⇒
//!   the row is dropped with a `debug!` log.
//!
//! ## Security
//!
//! The endpoint node binding is produced by the FieldState-aware
//! [`fetch::resolve_with_field_state`] helper, exactly as [`expand`](super::expand) resolves its
//! targets: table-level SELECT permission, computed fields, and field-level
//! SELECT permissions are all applied, so the binding contents are exactly what
//! a `SELECT` on that table would return for the caller. A permission denial is
//! indistinguishable from a missing record — both drop the row.

use tracing::debug;

use crate::exec::operators::scan::fetch::{FetchFieldStateCache, resolve_with_field_state};
use crate::exec::{ExecutionContext, FlowResult};
use crate::val::{Object, RecordId, TableName, Value};

/// Which endpoint of the bound edge to bind as the target node.
///
/// Mirror of the edge `in`/`out` field selection, kept local so this operator
/// compiles without the `gql` feature. It has the same shape as
/// [`ExpandDir`](super::expand::ExpandDir).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndpointField {
	/// Bind the edge's `in` endpoint.
	In,
	/// Bind the edge's `out` endpoint.
	Out,
}

impl EndpointField {
	/// The edge-record field holding this endpoint's vertex.
	pub(crate) fn field(self) -> &'static str {
		match self {
			EndpointField::In => "in",
			EndpointField::Out => "out",
		}
	}
}

/// The rows of `batch` extended with the endpoint node of the edge each binds
/// at `edge`, bound at `target_binding`, in order; the nodes are fetched as one
/// batch.
///
/// A row whose edge binding is missing, not a record, an optional miss, of a
/// label other than `target_label`, or whose endpoint is missing or denied to
/// the caller, is dropped.
pub(crate) async fn bind_endpoints(
	ctx: &ExecutionContext,
	cache: &mut FetchFieldStateCache,
	edge: &str,
	field: EndpointField,
	target_binding: &str,
	target_label: Option<&TableName>,
	batch: &[Value],
) -> FlowResult<Vec<Value>> {
	// Collect the endpoint rids to fetch for this batch, keeping a parallel
	// record of which input row each resolved endpoint maps back to. Rows
	// whose edge binding is missing / not a record / label-mismatched never
	// enter `targets_to_fetch` and so are dropped here.
	let mut targets_to_fetch: Vec<RecordId> = Vec::with_capacity(batch.len());
	let mut row_positions: Vec<usize> = Vec::with_capacity(batch.len());

	for (i, row) in batch.iter().enumerate() {
		match extract_endpoint_id(row, edge, field) {
			Some(rid) => {
				// Apply the target_label filter before fetching.
				if let Some(label) = target_label
					&& &rid.table != label
				{
					continue;
				}
				targets_to_fetch.push(rid);
				row_positions.push(i);
			}
			None => {
				// Malformed plan, optional miss, or an edge with no readable
				// endpoint: drop the row without panicking. Valid plans bind
				// a full edge object here, so the malformed-plan case should
				// not occur.
				debug!(
					binding = %edge,
					"EndpointBind: edge binding missing, not a full edge record, or has no endpoint; dropping row"
				);
			}
		}
	}

	// Batch-fetch the endpoint nodes (FieldState-aware). Positional with
	// `targets_to_fetch`; `None` => missing or permission-denied => drop.
	let target_values = resolve_with_field_state(ctx, cache, &targets_to_fetch).await?;

	let mut out: Vec<Value> = Vec::with_capacity(target_values.len());
	for (slot, target_value) in target_values.into_iter().enumerate() {
		let Some(target_value) = target_value else {
			continue;
		};
		out.push(bound_row(&batch[row_positions[slot]], target_binding, target_value));
	}
	Ok(out)
}

/// Read the endpoint vertex [`RecordId`] from `row[edge]`'s `in`/`out` field per
/// the edge-extraction rules (module docs). Returns `None` (the row drops) for a
/// missing / non-object / non-record-id edge, an id-only edge, an optional-miss
/// `Null`, or an edge whose endpoint field is missing or not a record id.
pub(crate) fn extract_endpoint_id(
	row: &Value,
	edge: &str,
	field: EndpointField,
) -> Option<RecordId> {
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
pub(crate) fn bound_row(input: &Value, target_binding: &str, target: Value) -> Value {
	let mut obj = if let Value::Object(o) = input {
		o.clone()
	} else {
		// `extract_endpoint_id` only yields `Some` for object rows, so this is
		// unreachable in practice; default to an empty object defensively.
		Object::default()
	};
	obj.insert(target_binding, target);
	Value::Object(obj)
}

#[cfg(test)]
mod tests {
	use super::*;

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
