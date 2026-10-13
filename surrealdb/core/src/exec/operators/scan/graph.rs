//! Graph edge scanning operator for the engine.
//!
//! This operator scans graph edges based on a source record, direction, and
//! target edge tables. It is used to implement graph traversal idioms like
//! `person:alice->knows->person`.

// Items here are built by the planned execution path, so dead-code warnings are allowed.
#![allow(dead_code)]

use std::sync::Arc;

// Re-exported so callers can import `EdgeTableSpec` from this module.
// The type itself is defined in `graph_keys`.
pub use super::graph_keys::EdgeTableSpec;
use crate::val::{TableName, Value};

/// Plan-time snapshot of the inline-field state a payload-resident
/// predicate was admitted under: per edge table, the inline generation and
/// the complete inline field set in canonical (payload) order.
///
/// The snapshot is the trust anchor at read time: a payload whose stamped
/// generation or value count differs was written under a different field
/// set and falls back to the record — the predicate is never evaluated
/// against values that mean something else than the plan assumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadPredicateSpec {
	pub(crate) tables: std::collections::HashMap<TableName, PayloadTableSpec>,
	/// Whether the predicate references the synthetic `id` / `in` / `out`
	/// fields. The synthesized candidate carries only what the predicate
	/// reads — every skipped field is a per-edge record-id clone or value
	/// insert saved on the scan's hottest path.
	pub(crate) needs_id: bool,
	pub(crate) needs_in: bool,
	pub(crate) needs_out: bool,
	/// A key-resident predicate: every field it reads is `id`, `in` or
	/// `out`, all of which the adjacency entry itself carries — no inline
	/// payload is needed, so evaluation prunes non-matching edges before
	/// any fetch on any table, with no `INLINE` fields declared and no
	/// per-table generation to validate. Pruning on key material is
	/// permission-safe: an edge the predicate drops would have been
	/// dropped by the same predicate after the fetch, so the visible
	/// result set is unchanged, and every surviving edge still resolves
	/// through the permission-checked record path.
	pub(crate) key_only: bool,
	/// A comparison-shaped key-resident predicate additionally compiles
	/// to a synchronous matcher (see [`super::key_matcher`]); when set,
	/// the scan tests each entry directly instead of synthesizing a
	/// candidate and running the evaluator. The matcher covers the whole
	/// predicate or is absent — never a partial residue.
	pub(crate) matcher: Option<Arc<super::key_matcher::KeyPredicateMatcher>>,
}

/// One table's inline-field snapshot inside a [`PayloadPredicateSpec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PayloadTableSpec {
	/// The table's inline generation at plan time.
	pub(crate) generation: u32,
	/// The raw names of every inline field, in the canonical payload order.
	pub(crate) fields: Vec<String>,
	/// Which of `fields` the predicate references, in the same order. The
	/// full list is still what stamps the payload's value count; only the
	/// referenced ones land in the synthesized candidate.
	pub(crate) referenced: Vec<bool>,
}

/// The referenced inline fields an adjacency entry's inline-property section
/// `props` carries for an edge of the table `table_spec` snapshots, by name;
/// `None` when the section is absent, undecodable, or written under another
/// generation or field count than the snapshot's, which leaves the edge
/// record the only source of truth.
pub(crate) fn payload_fields(
	table_spec: &PayloadTableSpec,
	props: Option<&[u8]>,
) -> Option<Vec<(String, Value)>> {
	use surrealdb_datastore::values::graph::InlineProps;
	let props = InlineProps::decode(props?).ok()?;
	if props.generation != table_spec.generation {
		return None;
	}
	let values = props.values?;
	if values.len() != table_spec.fields.len() {
		return None;
	}
	Some(
		table_spec
			.fields
			.iter()
			.zip(&table_spec.referenced)
			.zip(values)
			.filter(|((_, referenced), _)| **referenced)
			.map(|((name, _), value)| (name.clone(), value))
			.collect(),
	)
}
