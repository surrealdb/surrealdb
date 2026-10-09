//! The record and its edge keys, written to storage.
//!
//! The stages that put bytes in the KV store or take them out: the record, its
//! edge keys and its inline edge-property payloads.
//!
//! These are the only stages that may set
//! [`WriteRow::mutated`](super::super::row::WriteRow), and only after a real
//! write completes — the statement's affected-row total is that flag.

use surrealdb_types::ToSql;

use crate::catalog::{Relation, TableDefinition, TableType};
use crate::doc::Error as DocError;
use crate::err::EngineError;
use crate::expr::ControlFlow;
use crate::val::{RecordId, Value};

/// One endpoint read off the before-image, which is what the stored pointer
/// keys were derived from.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "an edge's endpoint is a record id, or the before-image is not an edge"
)]
pub(crate) fn endpoint(
	initial: &Value,
	path: &[crate::expr::Part],
	which: &'static str,
) -> Result<RecordId, ControlFlow> {
	match initial.pick(path) {
		Value::RecordId(id) => Ok(id),
		_ => Err(ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(format!(
			"Expected a record id for the `{which}` field of an edge"
		))))),
	}
}

/// Refuse a write that moved one of an edge's endpoints.
///
/// The record and the adjacency about to be restamped must keep naming the
/// same endpoints, or a later rewrite would derive pointer keys from a record
/// that no longer matches them.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "every shape but the endpoint the adjacency was written under is the same refusal"
)]
pub(crate) fn unmoved(
	current: &Value,
	path: &[crate::expr::Part],
	expected: &RecordId,
	refuse: impl FnOnce(String) -> DocError,
) -> Result<(), ControlFlow> {
	match current.pick(path) {
		Value::RecordId(ref id) if id == expected => Ok(()),
		moved => Err(ControlFlow::Err(anyhow::Error::new(refuse(moved.to_sql())))),
	}
}

/// Whether the table stores each of its edges as its two vertex-side adjacency
/// keys alone. A lightweight relation's records are synthesized on read from
/// the canonical `[in, out]` id, so there is no record to store.
pub(super) fn table_is_lightweight(table: &TableDefinition) -> bool {
	matches!(
		&table.table_type,
		TableType::Relation(Relation {
			lightweight: true,
			..
		})
	)
}
