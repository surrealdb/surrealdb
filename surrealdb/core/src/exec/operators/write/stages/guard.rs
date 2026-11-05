//! The structural guards.
//!
//! Every stage that asks whether the *table* admits this write at all — its storage mode, its
//! adjacency shape, its declared record type, and whether the record exists.
//!
//! A guard reads the catalog and the record and nothing the statement wrote,
//! which is what separates it from [`super::data`].
//!
//! # Where these sit, and why it matters
//!
//! [`CheckRecordExists`] heads the updating orders, ahead of the permission
//! gate, because a record that is not there is not a record the gate has an
//! opinion about; it skips the record rather than failing the statement.
//!
//! [`CheckTableNotView`] and [`CheckTableNotLightweight`] follow the order's
//! permission gate wherever they appear, so an actor without permission gets
//! the permission outcome — a silent skip — rather than a failure that
//! discloses the table's storage mode.
//!
//! [`CheckTableType`] does not share that property, and must not be read as if
//! it did. It *precedes* the permission gate on `UPSERT`'s two orders,
//! `INSERT`'s two and `RELATE`'s updating one, so a caller those gates would
//! have refused is told the table's declared record type instead; only
//! `CREATE`'s order and `RELATE`'s creating order place it after the gate.
//!
//! The positions are fixed in [`plan`](crate::exec::operators::write::plan);
//! nothing in this module may reorder them.
//!
//! A guard that refuses fails the statement. Only the existence check skips a
//! record, because only it is answering "is there anything here to write".

use surrealdb_types::ToSql;

use crate::catalog::{TableDefinition, TableType};
use crate::doc::Error as DocError;
use crate::exec::Error as ExecError;
use crate::exec::operators::write::row::WriteRow;
use crate::expr::ControlFlow;

/// The record a failure names.
///
/// A table-type guard that refuses names the record id unconditionally, and an
/// order whose guard precedes the id derivation — `UPSERT`'s and `INSERT`'s —
/// can arrive without one. That combination is internal-error territory; the
/// guard's own refusal is reached only where an id exists.
fn record_label(row: &WriteRow) -> Result<String, ControlFlow> {
	match &row.id {
		Some(id) => Ok(id.to_sql()),
		None => Err(ControlFlow::Err(anyhow::Error::new(crate::err::Error::unreachable(
			"Expected a document id to be present",
		)))),
	}
}

/// Whether the table's edges are vertex-side adjacency keys alone, synthesized
/// on read from their canonical `[in, out]` ids.
pub(crate) fn table_is_lightweight(table: &TableDefinition) -> bool {
	matches!(&table.table_type, TableType::Relation(rel) if rel.lightweight)
}

/// Refuse a mutation of a lightweight relation's edges, naming `action` as the
/// thing they cannot be.
///
/// Their whole state is the canonical id: there is nothing to set, and readers
/// synthesize the record, so a stored mutation could never be observed
/// consistently.
pub(crate) fn refuse_lightweight(table: &TableDefinition, action: &str) -> Result<(), ControlFlow> {
	if table_is_lightweight(table) {
		return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::Thrown(format!(
			"a LIGHTWEIGHT relation's edges cannot be {action}: they carry no data beyond `id`, `in` and `out`"
		)))));
	}
	Ok(())
}

/// Refuse a write whose record type the table does not admit.
///
/// `relation` is what the write is, not what the table is: it selects the
/// phrasing of the failure and is paired with the predicate that was tested.
pub(crate) fn refuse_table_type(
	row: &WriteRow,
	table: &TableDefinition,
	relation: bool,
) -> Result<(), ControlFlow> {
	Err(ControlFlow::Err(anyhow::anyhow!(DocError::TableCheck {
		record: record_label(row)?,
		relation,
		target_type: table.table_type.to_sql()
	})))
}
