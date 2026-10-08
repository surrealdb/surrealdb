//! Removing the reference keys a field no longer writes.
//!
//! Reference keys are written by the record writes of a `REFERENCE` field,
//! under the record they name. A schema change that leaves a target table
//! unreachable by the field strands the keys written for it, so the statement
//! that makes the change removes them.

use std::borrow::Cow;

use anyhow::Result;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, DatabaseId, NamespaceId};
use crate::expr::Kind;
use crate::key::KVKeyDecode;
use crate::key::schema::{RefCachePrefix, ReferenceKey, ReferencePrefix};
use crate::kvs::{Direction, NORMAL_BATCH_SIZE, Transaction};
use crate::val::TableName;

/// Purge the reference keys a field wrote under target tables it no longer
/// references after a schema change (`REMOVE FIELD`, `ALTER FIELD`, or
/// `DEFINE FIELD ... OVERWRITE`).
///
/// Reference keys are stored under the *referenced* (target) record's range,
/// keyed by the referencing `(table, field)` rather than under the field's own
/// definition (see `Document::process_reference_clause`). So when a field's
/// `REFERENCE` clause is dropped, or its record kind is narrowed or changed,
/// the keys it wrote for the now-unreachable target tables are not removed by
/// rewriting the field definition. Left behind they are orphaned: a later
/// `DELETE` of a referenced record skips the purge scan (because
/// `table_may_have_incoming_references` reports that no current field can
/// target that table), so on record-id reuse or re-definition the stale key
/// could resurface in a `<~` reference lookup or drive the wrong `ON DELETE`
/// action. Cleaning them at the schema change keeps that DELETE purge gate
/// sound: "no current reference field can target this table" then truly
/// implies "no reference keys exist for it".
///
/// `old` is the field definition before the change; `new` is the definition
/// after it (`None` when the field is being removed). Only target tables that
/// `old` could reference but `new` cannot are scanned. This runs on rare DDL,
/// so scanning the candidate target ranges is acceptable and avoids
/// maintaining a reverse index.
pub(crate) async fn purge_dropped_reference_keys(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ft: &TableName,
	old: &catalog::FieldDefinition,
	new: Option<&catalog::FieldDefinition>,
) -> Result<()> {
	// Only a field that previously declared a REFERENCE wrote reference keys.
	if old.reference.is_none() {
		return Ok(());
	}
	// `ff` is the referencing field name exactly as `process_reference_clause`
	// encoded it into each key's `ff` slot.
	let ff = old.name.to_sql();
	let old_kind: Option<&Kind> = old.field_kind.as_ref();
	let new_kind: Option<&Kind> = new.and_then(|n| n.field_kind.as_ref());
	for target in txn.all_tb(ns, db, None).await?.iter() {
		let target = target.name.clone();
		let target = &target;
		// Reference keys live under their target table, so only tables the old
		// kind could hold a record of can carry this field's keys (an untyped
		// `record` could target any table).
		let old_can_target = old_kind.is_none_or(|k| k.reference_can_target(target));
		if !old_can_target {
			continue;
		}
		// Keep the keys the new definition still references.
		let new_can_target = new.is_some_and(|n| {
			n.reference.is_some() && new_kind.is_none_or(|k| k.reference_can_target(target))
		});
		if new_can_target {
			continue;
		}
		// Collect the matching keys first, then delete them, so the range is
		// never mutated while the cursor is still scanning it.
		let range = ReferencePrefix {
			ns,
			db,
			tb: Cow::Borrowed(target),
		}
		.range()?;
		let mut orphaned: Vec<Vec<u8>> = Vec::new();
		let mut cursor = txn.open_keys_cursor(range, Direction::Forward, 0, None).await?;
		loop {
			let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for raw in batch.iter() {
				let key = ReferenceKey::decode_key(raw)?;
				if key.foreign_table.as_ref() == ft && key.foreign_field.as_ref() == ff.as_str() {
					orphaned.push(raw.to_vec());
				}
			}
		}
		drop(cursor);
		for raw in &orphaned {
			let key = ReferenceKey::decode_key(raw)?;
			txn.del_key(&key).await?;
		}
		// The deletions above happen behind the target table's inline
		// reference caches, so those caches — if the table maintains any —
		// are now stale. Drop the table's whole cache subspace in this
		// transaction; caches re-materialise lazily. Invalidate-to-missing
		// here is one range delete, where cache-accurate maintenance would
		// be a read-modify-write per orphaned key.
		if !orphaned.is_empty() {
			let key = RefCachePrefix {
				ns,
				db,
				tb: Cow::Borrowed(target),
			};
			txn.del_prefix_key(&key).await?;
		}
	}
	Ok(())
}
