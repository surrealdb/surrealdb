//! Single-hop graph traversal for GQL v2 `MATCH`.
//!
//! [`expand_input_row`] is the core of binding-table execution. Given a
//! binding row (a `Value::Object` keyed by binding name), it expands one edge
//! step from a bound source node, binding the traversed edge and the reached
//! target node into each surviving row.
//!
//! For every input row it:
//!
//! 1. reads `row[source].id` (the source node's record id) — see the source-extraction rules below;
//! 2. enumerates the source vertex's adjacency in `direction`, restricted to `edge_tables` (empty ⇒
//!    all edge tables), via the shared [`graph_keys`](super::super::scan::graph_keys) machinery;
//! 3. resolves the edge binding — `Full` fetches the edge record through the FieldState-aware
//!    [`fetch`](crate::exec::operators::scan::fetch) helper; `IdOnly` carries just the edge
//!    `Value::RecordId` with no fetch;
//! 4. determines the target vertex id (see the target-rid decision below);
//! 5. applies `target_label` (a candidate whose target table differs is dropped);
//! 6. batch-fetches the target node through the FieldState-aware helper (a miss or permission
//!    denial drops the candidate);
//! 7. assembles `input ∪ {edge_binding, target_binding}` and judges the optional predicate against
//!    that candidate row ([`CandidateJudge`]);
//! 8. emits the survivors.
//!
//! ## Source extraction
//!
//! - `row[source]` is a full record object ⇒ its `id` field (a `Value::RecordId`) is the source id;
//! - `row[source]` is itself a `Value::RecordId` ⇒ used directly (defensive; node bindings normally
//!   hold full objects);
//! - `row[source]` is `Value::Null` ⇒ the source is an optional miss: when `optional` the row
//!   passes through with `edge`/`target` bound to `Null`, otherwise it is skipped;
//! - the binding is missing, or holds any other (non-record, non-null) value ⇒ the row is dropped
//!   with a `debug!` log (never a panic). This only happens on a malformed plan; the planner
//!   guarantees the source binding is a bound node.
//!
//! ## `IdOnly` edges and the target-rid decision
//!
//! The adjacency keys scanned here are *pointer keys* (vertex-side adjacency):
//! [`DecodedGraph::edge`](crate::key::schema::DecodedGraph) is the
//! **edge record id**, and `DecodedGraph::target` is **`Some` only for
//! new-format keys**, carrying the far endpoint vertex directly.
//!
//! The target vertex id is therefore resolved in priority order:
//!
//! 1. `decoded.target` when present — covers new-format keys for *both* edge modes with zero extra
//!    work;
//! 2. otherwise the fetched edge object's `out` (for `Out`) / `in` (for `In`) field — available for
//!    free in `Full` mode on legacy keys;
//! 3. otherwise (`IdOnly` mode on a legacy key, where the edge is not fetched) a minimal edge fetch
//!    through the FieldState helper to read its `out`/`in`.
//!
//! `IdOnly` binds the edge as a bare `Value::RecordId` (no record content
//! surfaces), but still needs the target id. New-format keys supply it during
//! decode. Only legacy-format keys fall back to a minimal fetch, so the common
//! path stays fetch-free and legacy storage stays correct.
//!
//! ## Edge SELECT-permission gating (security)
//!
//! Binding the edge id-only must not let a user *traverse* an edge table they
//! cannot SELECT. Otherwise `->edge` would enumerate relationships that stay
//! hidden, even though no edge content is returned. When permissions are
//! enforced (`check_perms`), `IdOnly` edges are still resolved through the
//! FieldState-aware [`fetch`](crate::exec::operators::scan::fetch) helper, but
//! only to gate traversal. A denied or missing edge yields `None`, and its
//! candidate target is dropped. The bound value stays the bare
//! `Value::RecordId`. The gating fetch also supplies `out`/`in` for legacy keys,
//! so it adds at most one batched read on the secured path. For root and owner
//! sessions (`check_perms == false`) the gate is skipped and the fetch-free path
//! is used.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::debug;

use crate::exec::operators::scan::fetch::{FetchFieldStateCache, resolve_with_field_state};
use crate::exec::operators::scan::graph_keys::{EdgeTableSpec, collect_adjacency};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, Dir};
use crate::idx::adjacency::VertexAdjacency;
use crate::val::{Object, RecordId, TableName, Value};

/// Direction of a single `Expand` hop.
///
/// GQL `MATCH` patterns only traverse a directed edge: `->` (`Out`) or `<-`
/// (`In`). There is no undirected hop at the operator level. This is a local
/// mirror of the `match_plan::ExpandDirection` enum. The planner, which sits
/// behind the `gql` feature, translates between the two. Keeping this copy
/// local lets the operator build without that feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExpandDir {
	/// Outgoing edge `->`: target is the edge's `out` endpoint.
	Out,
	/// Incoming edge `<-`: target is the edge's `in` endpoint.
	In,
}

impl ExpandDir {
	/// Map to the storage-layer [`Dir`] used for adjacency range computation.
	fn as_dir(self) -> Dir {
		match self {
			ExpandDir::Out => Dir::Out,
			ExpandDir::In => Dir::In,
		}
	}

	/// The edge-record field holding the target vertex for this direction.
	fn target_field(self) -> &'static str {
		match self {
			ExpandDir::Out => "out",
			ExpandDir::In => "in",
		}
	}
}

/// How the traversed edge is bound into each surviving row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EdgeBinding {
	/// Bind the full edge record (FieldState-aware fetch) under this name.
	Full(String),
	/// Bind only the edge `Value::RecordId` under this name (no fetch).
	IdOnly(String),
}

impl EdgeBinding {
	#[cfg(test)]
	/// The binding name regardless of mode.
	fn name(&self) -> &str {
		match self {
			EdgeBinding::Full(n) | EdgeBinding::IdOnly(n) => n,
		}
	}

	#[cfg(test)]
	/// Whether the edge record must be fetched.
	fn fetch_full(&self) -> bool {
		matches!(self, EdgeBinding::Full(_))
	}
}

/// The edge-table specs, with unbounded ranges, of `tables`. No tables is the
/// all-edges wildcard scan.
pub(crate) fn edge_specs_for(tables: &[TableName]) -> Vec<EdgeTableSpec> {
	tables
		.iter()
		.cloned()
		.map(|table| EdgeTableSpec {
			table,
			range_start: std::ops::Bound::Unbounded,
			range_end: std::ops::Bound::Unbounded,
		})
		.collect()
}

/// What judges an assembled candidate row of an expansion.
pub(crate) trait CandidateJudge {
	/// Whether `candidate` — the input row with the edge and the target bound —
	/// survives.
	fn passes<'a>(
		&'a mut self,
		candidate: &'a Value,
	) -> impl std::future::Future<Output = FlowResult<bool>> + Send + 'a;
}

/// What one expansion is parameterised by, fixed for the whole run.
pub(crate) struct ExpandParams {
	pub(crate) direction: ExpandDir,
	pub(crate) edge_specs: Vec<EdgeTableSpec>,
	pub(crate) edge_name: String,
	pub(crate) fetch_full_edge: bool,
	pub(crate) target_binding: String,
	pub(crate) target_label: Option<TableName>,
	pub(crate) optional: bool,
	pub(crate) scan_batch_size: usize,
	pub(crate) max_output_rows: usize,
}

/// What an expansion keeps across the input rows of one run.
///
/// The FieldState caches are keyed by table internally, so each table's
/// permission and FieldState are resolved at most once. `emitted` is the
/// cumulative count of survivors across all input rows: it bounds a
/// high-fan-out expansion (a single dense source vertex, or a long input,
/// can fan out far past memory). The per-source optional null-fills are
/// one per row and bounded by the input, so only survivors are counted.
pub(crate) struct ExpandCaches {
	edge_cache: FetchFieldStateCache,
	target_cache: FetchFieldStateCache,
	/// Per-table adjacency classifications (source rids can span tables).
	adjacency_memo: HashMap<TableName, VertexAdjacency>,
	emitted: usize,
}

impl ExpandCaches {
	pub(crate) fn new() -> Self {
		Self {
			edge_cache: FetchFieldStateCache::new(),
			target_cache: FetchFieldStateCache::new(),
			adjacency_memo: HashMap::new(),
			emitted: 0,
		}
	}
}

/// Expand one input binding row from the node bound at `source`, appending the
/// rows it yields to `out`.
pub(crate) async fn expand_input_row<J: CandidateJudge>(
	ctx: &ExecutionContext,
	params: &ExpandParams,
	source: &str,
	caches: &mut ExpandCaches,
	judge: &mut J,
	row: &Value,
	out: &mut Vec<Value>,
) -> Result<(), ControlFlow> {
	match extract_source_id(row, source) {
		SourceId::Found(rid) => {
			let db_ctx = ctx.database()?;
			let txn = ctx.txn();
			expand_row(
				ctx,
				&txn,
				db_ctx.ns_ctx.ns.namespace_id,
				db_ctx.db.database_id,
				ctx.version_stamp(),
				row,
				&rid,
				params,
				caches,
				judge,
				out,
			)
			.await
		}
		SourceId::OptionalMiss => {
			// Source is a prior optional miss (Null). Left-outer
			// propagation: keep the row, nulling this hop's
			// bindings when this expand is itself optional;
			// otherwise an inner expand from a null source
			// produces nothing.
			if params.optional {
				out.push(null_filled_row(row, &params.edge_name, &params.target_binding));
			}
			Ok(())
		}
		SourceId::Drop => {
			// Malformed plan: the source binding is missing or
			// not a record. The planner guarantees a bound node
			// here, so this is unreachable on valid plans; drop
			// without panicking.
			debug!(
				binding = %source,
				"Expand: source binding is missing or not a record id; dropping row"
			);
			Ok(())
		}
	}
}

/// Result of locating the source record id within an input binding row.
enum SourceId {
	/// A concrete source record id was found.
	Found(RecordId),
	/// The source binding is `Value::Null` (a prior optional miss).
	OptionalMiss,
	/// The binding is missing or not a record id; the row must be dropped.
	Drop,
}

/// Locate `row[source].id` per the source-extraction rules (module docs).
fn extract_source_id(row: &Value, source: &str) -> SourceId {
	let Value::Object(obj) = row else {
		return SourceId::Drop;
	};
	match obj.get(source) {
		Some(Value::Object(node)) => match node.get("id") {
			Some(Value::RecordId(rid)) => SourceId::Found(rid.clone()),
			_ => SourceId::Drop,
		},
		// Defensive: a node binding that is itself a bare record id.
		Some(Value::RecordId(rid)) => SourceId::Found(rid.clone()),
		Some(Value::Null) => SourceId::OptionalMiss,
		// Missing binding, or a non-record/non-null value.
		_ => SourceId::Drop,
	}
}

/// Assemble a candidate binding row: clone the input object and insert the
/// edge and target bindings.
fn candidate_row(
	input: &Value,
	edge_name: &str,
	edge: Value,
	target_binding: &str,
	target: Value,
) -> Value {
	let mut obj = if let Value::Object(o) = input {
		o.clone()
	} else {
		// `extract_source_id` only yields `Found` for object rows, so this is
		// unreachable in practice; default to an empty object defensively.
		Object::default()
	};
	obj.insert(edge_name, edge);
	obj.insert(target_binding, target);
	Value::Object(obj)
}

/// Build a row for an optional miss: the input row with the edge and target
/// bindings set to `Value::Null`.
fn null_filled_row(input: &Value, edge_name: &str, target_binding: &str) -> Value {
	candidate_row(input, edge_name, Value::Null, target_binding, Value::Null)
}

/// Read a `RecordId` from an edge object's `out`/`in` field for `direction`.
fn target_from_edge_obj(edge: &Value, direction: ExpandDir) -> Option<RecordId> {
	let Value::Object(obj) = edge else {
		return None;
	};
	match obj.get(direction.target_field()) {
		Some(Value::RecordId(rid)) => Some(rid.clone()),
		_ => None,
	}
}

/// Enumerate one input row's adjacency and emit surviving candidate rows.
#[allow(clippy::too_many_arguments)]
async fn expand_row<J: CandidateJudge>(
	ctx: &ExecutionContext,
	txn: &Arc<crate::kvs::Transaction>,
	ns_id: crate::catalog::NamespaceId,
	db_id: crate::catalog::DatabaseId,
	version: Option<u64>,
	input_row: &Value,
	source_rid: &RecordId,
	params: &ExpandParams,
	caches: &mut ExpandCaches,
	judge: &mut J,
	out: &mut Vec<Value>,
) -> Result<(), ControlFlow> {
	let ExpandParams {
		direction,
		edge_specs,
		edge_name,
		fetch_full_edge,
		target_binding,
		optional,
		scan_batch_size,
		max_output_rows,
		..
	} = params;
	let (direction, fetch_full_edge, optional, scan_batch_size, max_output_rows) =
		(*direction, *fetch_full_edge, *optional, *scan_batch_size, *max_output_rows);
	let target_label = params.target_label.as_ref();
	let dir = direction.as_dir();
	let ExpandCaches {
		edge_cache,
		target_cache,
		adjacency_memo,
		emitted,
	} = caches;
	// Enumerate the source vertex's adjacency — packed blocks merged with
	// per-edge pointer keys — retaining only (edge rid, optional decoded
	// target) pairs per item: the key and props are dropped as each edge
	// streams by, never buffered for the whole fan-out. `edge` is the edge
	// record id; `target` is the far vertex on new-format entries.
	let mut edge_rids: Vec<RecordId> = Vec::new();
	let mut decoded_targets: Vec<Option<RecordId>> = Vec::new();
	collect_adjacency(
		txn,
		ns_id,
		db_id,
		source_rid,
		dir,
		edge_specs,
		ctx,
		version,
		adjacency_memo,
		|item| {
			edge_rids.push(item.edge);
			decoded_targets.push(item.target);
			Ok(())
		},
	)
	.await?;

	if edge_rids.is_empty() {
		if optional {
			out.push(null_filled_row(input_row, edge_name, target_binding));
		}
		return Ok(());
	}

	// SECURITY: an id-only (hidden or anonymous) edge binds only its record id.
	// Without a gate, a user denied SELECT on the edge table could still traverse
	// `->edge` to reach the far node. The common new-format path decodes the target
	// straight from the adjacency key and never reads the edge record, so nothing
	// would check that permission. `GraphEdgeScan` (scan/graph.rs) applies the same
	// edge-permission gate to its id-only output. When permissions are enforced,
	// every id-only edge is therefore resolved through the FieldState helper, only
	// to gate traversal. A denied or missing edge (`None`) drops the candidate. For
	// root and owner sessions (`check_perms == false`) the gate is a no-op and the
	// fetch-free fast path is kept.
	let gate_id_only_edges = !fetch_full_edge && edge_cache.check_perms(ctx)?;

	// Survivor count for this input row, so OptionalExpand can emit its
	// null-filled fallback only when nothing matched.
	let before = out.len();

	// Process in batches bounded by `scan_batch_size` so high-fanout sources
	// don't buffer an unbounded number of edges at once.
	let mut start = 0;
	while start < edge_rids.len() {
		let end = (start + scan_batch_size).min(edge_rids.len());
		// A unit per candidate the batch resolves and judges.
		crate::exec::vm::fuel::tick(ctx, crate::exec::vm::fuel::units(end - start)).await?;
		let edge_slice = &edge_rids[start..end];
		let decoded_slice = &decoded_targets[start..end];

		// Resolve the edge bindings. In `Full` mode every edge is fetched through the
		// FieldState helper, and its perm gating is the `Some`/`None` of
		// `edge_values[i]`. In `IdOnly` mode the bound value is always the bare record
		// id, with no record content. `edge_perm` records whether the edge passed the
		// table and field SELECT permission, computed by a gating fetch when
		// permissions are enforced (see `gate_id_only_edges`). Both vectors are
		// positional with `edge_slice`.
		let (edge_values, edge_perm): (Vec<Option<Value>>, Vec<Option<Value>>) = if fetch_full_edge
		{
			let fetched = resolve_with_field_state(ctx, edge_cache, edge_slice).await?;
			// In Full mode the binding value IS the fetched object; the gate
			// reuses it directly (no second fetch).
			(fetched.clone(), fetched)
		} else if gate_id_only_edges {
			// IdOnly with permissions enforced: bind the bare id, but gate on a
			// FieldState fetch of the edge record. The fetched object also
			// supplies `out`/`in` for legacy keys for free.
			let gated = resolve_with_field_state(ctx, edge_cache, edge_slice).await?;
			let values = edge_slice.iter().map(|rid| Some(Value::RecordId(rid.clone()))).collect();
			(values, gated)
		} else {
			// IdOnly fast path (no permission enforcement): bind the bare id,
			// no edge fetch and no gate (every edge is allowed).
			let values: Vec<Option<Value>> =
				edge_slice.iter().map(|rid| Some(Value::RecordId(rid.clone()))).collect();
			let perm = vec![None; edge_slice.len()];
			(values, perm)
		};

		// Determine the target rid for each adjacency entry (priority order:
		// decoded target ⇒ fetched/gating edge `out`/`in` ⇒ minimal edge fetch),
		// and collect a parallel batch of target rids to resolve.
		// `legacy_positions` records which entries still need a minimal edge
		// fetch (legacy IdOnly keys with no perm-gating fetch already in hand).
		let mut target_rids: Vec<Option<RecordId>> = Vec::with_capacity(edge_slice.len());
		let mut legacy_edge_fetch: Vec<RecordId> = Vec::new();
		let mut legacy_positions: Vec<usize> = Vec::new();

		for (i, edge_rid) in edge_slice.iter().enumerate() {
			// An edge denied by the SELECT permission cannot let its target
			// surface; drop the candidate and never read the denied edge again.
			// In Full mode `edge_values[i]` is the gate; in gated IdOnly mode
			// `edge_perm[i]` is. (Fast-path IdOnly leaves both permissive.)
			let edge_denied = (fetch_full_edge && edge_values[i].is_none())
				|| (gate_id_only_edges && edge_perm[i].is_none());
			if edge_denied {
				target_rids.push(None);
			} else if let Some(target) = decoded_slice[i].clone() {
				// (1) New-format key: target carried in the adjacency. No edge
				// read needed.
				target_rids.push(Some(target));
			} else if let Some(edge_val) = edge_perm[i].as_ref() {
				// (2) Legacy key with the edge record already in hand (Full mode,
				// or gated IdOnly): read the endpoint from its `out`/`in`.
				target_rids.push(target_from_edge_obj(edge_val, direction));
			} else {
				// (3) Legacy key, fast-path IdOnly: the edge was not fetched, so
				// defer a minimal FieldState fetch purely to recover `out`/`in`.
				target_rids.push(None);
				legacy_edge_fetch.push(edge_rid.clone());
				legacy_positions.push(i);
			}
		}

		// Minimal edge fetch for legacy fast-path IdOnly entries: read the edge
		// records (FieldState-aware) purely to recover their `out`/`in`
		// endpoint.
		if !legacy_edge_fetch.is_empty() {
			let fetched = resolve_with_field_state(ctx, edge_cache, &legacy_edge_fetch).await?;
			for (pos, edge_val) in legacy_positions.iter().zip(fetched) {
				if let Some(edge_val) = edge_val {
					target_rids[*pos] = target_from_edge_obj(&edge_val, direction);
				}
			}
		}

		// Apply the target_label filter and gather the target rids to fetch.
		// `candidate_positions` maps each fetched-target slot back to its
		// adjacency index so we can pair it with the right edge value.
		let mut targets_to_fetch: Vec<RecordId> = Vec::new();
		let mut candidate_positions: Vec<usize> = Vec::new();
		for (i, target) in target_rids.iter().enumerate() {
			let Some(target) = target else {
				continue;
			};
			if let Some(label) = target_label
				&& &target.table != label
			{
				continue;
			}
			targets_to_fetch.push(target.clone());
			candidate_positions.push(i);
		}

		// Batch-fetch the target nodes (FieldState-aware). Positional with
		// `targets_to_fetch`; `None` ⇒ missing or permission-denied ⇒ drop.
		let target_values = resolve_with_field_state(ctx, target_cache, &targets_to_fetch).await?;

		for (slot, target_value) in target_values.into_iter().enumerate() {
			let Some(target_value) = target_value else {
				continue;
			};
			let i = candidate_positions[slot];

			// The edge binding value. Permission-denied edges (Full or gated
			// IdOnly) were already excluded above (their `target_rids` slot was
			// `None`, so they are absent from `candidate_positions`); this `None`
			// skip is an extra guard for Full mode, where the dropped
			// object would otherwise leak the denied edge's existence.
			let edge_value = match edge_values[i].clone() {
				Some(v) => v,
				None => continue,
			};

			let candidate =
				candidate_row(input_row, edge_name, edge_value, target_binding, target_value);

			// The predicate, evaluated against the assembled candidate row.
			if !judge.passes(&candidate).await? {
				continue;
			}

			*emitted += 1;
			if *emitted > max_output_rows {
				return Err(crate::exec::operators::gql_output_rows_exceeded(max_output_rows));
			}
			out.push(candidate);
		}

		start = end;
	}

	// OptionalExpand: zero survivors for this input row ⇒ emit one null-filled
	// row (left-outer semantics).
	if optional && out.len() == before {
		out.push(null_filled_row(input_row, edge_name, target_binding));
	}

	Ok(())
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

	#[test]
	fn dir_mapping_and_target_field() {
		assert_eq!(ExpandDir::Out.as_dir(), Dir::Out);
		assert_eq!(ExpandDir::In.as_dir(), Dir::In);
		assert_eq!(ExpandDir::Out.target_field(), "out");
		assert_eq!(ExpandDir::In.target_field(), "in");
	}

	#[test]
	fn edge_binding_name_and_fetch_flag() {
		assert_eq!(EdgeBinding::Full("k".to_string()).name(), "k");
		assert_eq!(EdgeBinding::IdOnly("k".to_string()).name(), "k");
		assert!(EdgeBinding::Full("k".to_string()).fetch_full());
		assert!(!EdgeBinding::IdOnly("k".to_string()).fetch_full());
	}

	#[test]
	fn extract_source_id_from_full_object() {
		let row = obj_row(&[("a", obj_row(&[("id", Value::RecordId(rid("person", "alice")))]))]);
		let SourceId::Found(r) = extract_source_id(&row, "a") else {
			panic!("expected Found");
		};
		assert_eq!(r, rid("person", "alice"));
	}

	#[test]
	fn extract_source_id_from_bare_record_id() {
		let row = obj_row(&[("a", Value::RecordId(rid("person", "bob")))]);
		let SourceId::Found(r) = extract_source_id(&row, "a") else {
			panic!("expected Found");
		};
		assert_eq!(r, rid("person", "bob"));
	}

	#[test]
	fn extract_source_id_null_is_optional_miss() {
		let row = obj_row(&[("a", Value::Null)]);
		assert!(matches!(extract_source_id(&row, "a"), SourceId::OptionalMiss));
	}

	#[test]
	fn extract_source_id_missing_or_bad_is_drop() {
		let missing = obj_row(&[("z", Value::Bool(true))]);
		assert!(matches!(extract_source_id(&missing, "a"), SourceId::Drop));
		let bad = obj_row(&[("a", Value::Bool(true))]);
		assert!(matches!(extract_source_id(&bad, "a"), SourceId::Drop));
		// A node object missing its `id` field is also a drop.
		let no_id = obj_row(&[("a", obj_row(&[("name", Value::Bool(true))]))]);
		assert!(matches!(extract_source_id(&no_id, "a"), SourceId::Drop));
		// A non-object row drops.
		assert!(matches!(extract_source_id(&Value::Bool(true), "a"), SourceId::Drop));
	}

	#[test]
	fn candidate_row_inserts_edge_and_target() {
		let input = obj_row(&[("a", Value::RecordId(rid("person", "alice")))]);
		let row = candidate_row(
			&input,
			"k",
			Value::RecordId(rid("knows", "e1")),
			"b",
			Value::RecordId(rid("person", "bob")),
		);
		let Value::Object(o) = row else {
			panic!("expected object");
		};
		assert_eq!(o.get("a"), Some(&Value::RecordId(rid("person", "alice"))));
		assert_eq!(o.get("k"), Some(&Value::RecordId(rid("knows", "e1"))));
		assert_eq!(o.get("b"), Some(&Value::RecordId(rid("person", "bob"))));
	}

	#[test]
	fn null_filled_row_nulls_edge_and_target() {
		let input = obj_row(&[("a", Value::RecordId(rid("person", "alice")))]);
		let row = null_filled_row(&input, "k", "b");
		let Value::Object(o) = row else {
			panic!("expected object");
		};
		assert_eq!(o.get("a"), Some(&Value::RecordId(rid("person", "alice"))));
		assert_eq!(o.get("k"), Some(&Value::Null));
		assert_eq!(o.get("b"), Some(&Value::Null));
	}

	#[test]
	fn target_from_edge_obj_reads_correct_field() {
		let edge = obj_row(&[
			("in", Value::RecordId(rid("person", "alice"))),
			("out", Value::RecordId(rid("person", "bob"))),
		]);
		assert_eq!(target_from_edge_obj(&edge, ExpandDir::Out), Some(rid("person", "bob")));
		assert_eq!(target_from_edge_obj(&edge, ExpandDir::In), Some(rid("person", "alice")));
		// Missing endpoint field ⇒ None.
		let partial = obj_row(&[("in", Value::RecordId(rid("person", "alice")))]);
		assert_eq!(target_from_edge_obj(&partial, ExpandDir::Out), None);
		// Non-object ⇒ None.
		assert_eq!(target_from_edge_obj(&Value::Null, ExpandDir::Out), None);
	}
}
