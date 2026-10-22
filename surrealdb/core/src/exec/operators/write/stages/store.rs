//! The record and its edge keys, written to storage.
//!
//! The stages that put bytes in the KV store or take them out: the record, its
//! edge keys and its inline edge-property payloads.
//!
//! These are the only stages that may set
//! [`WriteRow::mutated`](super::super::row::WriteRow), and only after a real
//! write completes — the statement's affected-row total is that flag.

use crate::catalog::{Relation, TableDefinition, TableType};
use crate::doc::Error as DocError;
use crate::err::EngineError;
use crate::expr::ControlFlow;
use crate::val::{RecordId, Value};

/// One endpoint read off the before-image, which is what the stored pointer
/// keys were derived from.
pub(crate) fn endpoint(
	initial: &Value,
	path: &[crate::expr::Part],
	which: &'static str,
) -> Result<RecordId, ControlFlow> {
	if let Value::RecordId(id) = initial.pick(path) {
		Ok(id)
	} else {
		Err(ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(format!(
			"Expected a record id for the `{which}` field of an edge"
		)))))
	}
}

/// Whether the record's endpoint at `path` in `image` differs from the one the
/// stored edge was written under.
///
/// The record and the adjacency about to be restamped must keep naming the same
/// endpoints, or a later rewrite would derive pointer keys from a record that no
/// longer matches them; a difference is refused by [`endpoint_override`].
pub(crate) fn endpoint_moved(
	image: &Value,
	path: &[crate::expr::Part],
	expected: &RecordId,
) -> bool {
	!matches!(image.pick(path), Value::RecordId(id) if id == *expected)
}

/// The failure a write that would move one of an edge's endpoints raises, or
/// `None` when neither endpoint moved.
///
/// `in_moved` and `out_moved` say which of the supplied endpoints differs from
/// the one the stored edge holds; an edge's endpoints are immutable, so either
/// is a refusal. When the caller's reads are permission-checked (`checked`), the
/// refusal names neither endpoint ([`DocError::EndpointOverride`]): were the
/// `in` and `out` cases distinguished, a caller with write but not read access
/// to the edge could learn a hidden endpoint from which refusal came back - a
/// wrong `in` guess failing differently from a correct `in` with a wrong `out` -
/// one query at a time. A caller whose reads are not permission-checked (a
/// definer frame, a system actor) hides nothing, so it is given the specific
/// `in`/`out` refusal, `in` taking precedence as the comparisons are ordered.
pub(crate) fn endpoint_override(
	checked: bool,
	in_moved: bool,
	out_moved: bool,
) -> Option<DocError> {
	if !in_moved && !out_moved {
		None
	} else if checked {
		Some(DocError::EndpointOverride)
	} else if in_moved {
		Some(DocError::InOverride)
	} else {
		Some(DocError::OutOverride)
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
