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
	purge_dropped_reference_keys_with(txn, ns, db, ft, old, new, false).await
}

/// Like [`purge_dropped_reference_keys`], erasing earlier versions of each key
/// as well when `expunge` is `true`.
pub(crate) async fn purge_dropped_reference_keys_with(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ft: &TableName,
	old: &catalog::FieldDefinition,
	new: Option<&catalog::FieldDefinition>,
	expunge: bool,
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
	let mut targets: std::collections::HashSet<TableName> =
		txn.all_tb(ns, db, None).await?.iter().map(|tb| tb.name.clone()).collect();
	if let Some(kind) = old_kind {
		kind.collect_reference_target_tables(&mut targets);
	}
	for target in &targets {
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
			if expunge {
				txn.clr_key(&key).await?;
			} else {
				txn.del_key(&key).await?;
			}
		}
		// The deletions above happen behind the target table's inline
		// reference caches, so those caches — if the table maintains any —
		// are now stale. Drop the table's whole cache subspace in this
		// transaction; caches re-materialise lazily. Invalidate-to-missing
		// here is one range delete, where cache-accurate maintenance would
		// be a read-modify-write per orphaned key. Under `EXPUNGE` the
		// caches' history goes too, since an older cache entry still names
		// the expunged table, field and records. That history outlives the
		// field's live keys — a reference unset before the removal leaves
		// none to find — so it is cleared for every table the field could
		// reference, whether or not a live key was found.
		let key = RefCachePrefix {
			ns,
			db,
			tb: Cow::Borrowed(target),
		};
		if expunge {
			txn.clr_prefix_key(&key).await?;
		} else if !orphaned.is_empty() {
			txn.del_prefix_key(&key).await?;
		}
	}
	Ok(())
}

/// Bring the reference keys of field `new` of table `ft` in line with a schema
/// change from `old` (`None` for a field defined fresh): purge the keys `old`
/// wrote under target tables `new` no longer references, then, when `new`
/// starts a `REFERENCE` or changes its record kind, write the keys of the links
/// the stored records hold that `new` references ([`backfill_reference_keys`]).
/// Every statement that changes a field definition outside an import calls
/// this, so the keys stay those a write of every stored record would produce.
pub(crate) async fn reconcile_reference_keys(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	ft: &TableName,
	old: Option<&catalog::FieldDefinition>,
	new: &catalog::FieldDefinition,
) -> Result<()> {
	let txn = ctx.txn();
	if let Some(old) = old {
		purge_dropped_reference_keys(&txn, ns, db, ft, old, Some(new)).await?;
	}
	let starts_or_retargets = new.reference.is_some()
		&& old.is_none_or(|old| old.reference.is_none() || old.field_kind != new.field_kind);
	if starts_or_retargets {
		backfill_reference_keys(ctx, &txn, ns, db, ft, new).await?;
	}
	Ok(())
}

/// Write the reference keys a field declares for the records its table
/// already holds, when a schema change starts or widens the field's
/// `REFERENCE` (`DEFINE FIELD ... OVERWRITE` adding the clause, or changing
/// the record kind).
///
/// Record writes keep a field's reference keys in step with its value by
/// comparing the value before and after the write, so a record that already
/// holds a link when the clause arrives has no key for it and a write that
/// leaves the link unchanged writes none. This scans the table once, replays
/// any pending migration steps over each record so links produced by an
/// un-rewritten migration are seen as reads see them, and writes the key of
/// every link a record holds under the field to a table the field's record
/// kind can target, the same keys and inline caches a write of that record
/// would have produced. A view's rows skip its field definitions and write no
/// reference keys. A link to a table the kind no longer admits (a stored value
/// a narrowed kind left behind) gets none. Writing a key that exists is a
/// no-op, so the scan may cover links the previous definition already tracked.
/// A collection (array or set) contributes a key per record it holds, as a
/// write of it does.
pub(crate) async fn backfill_reference_keys(
	ctx: &crate::exec::ExecutionContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ft: &TableName,
	field: &catalog::FieldDefinition,
) -> Result<()> {
	use crate::exec::vm::codec;
	use crate::exec::vm::codec::key::KeyField;
	use crate::key::schema::{RecordKey, RecordPrefix};
	use crate::val::{RecordId, Value};

	if field.reference.is_none() {
		return Ok(());
	}
	// A view's rows skip its field definitions and write no reference keys.
	if txn.get_tb(ns, db, ft, None).await?.is_some_and(|tb| tb.view.is_some()) {
		return Ok(());
	}
	// `ff` is the referencing field name exactly as the record writes encode it
	// into each key's `ff` slot.
	let ff = field.name.to_sql();
	let layouts = txn.table_layouts(ns, db, ft, None).await?;
	let replay = crate::exec::migrate::Replay::of(&layouts, ns, db);
	let range = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(ft),
	}
	.range()?;
	let mut cursor = txn.open_vals_cursor_raw(range, Direction::Forward, 0, None).await?;
	loop {
		// The rows of one batch of records, decoded first so the cursor's range
		// is never mutated while it is scanning.
		let mut rows: Vec<(RecordId, Value, Option<catalog::Generation>)> = Vec::new();
		let mut refused: Option<anyhow::Error> = None;
		let stats = cursor
			.for_each(NORMAL_BATCH_SIZE, &mut |raw_key, value| {
				let mut read = || -> Result<()> {
					let record_key = RecordKey::decode_key(raw_key)?;
					let rid = RecordId {
						table: ft.clone(),
						key: record_key.id.as_ref().clone(),
					};
					let stamp = crate::exec::migrate::stamp_of(value);
					let record =
						catalog::Record::decode_admitted(value, Some(&layouts), rid.clone())?;
					rows.push((rid, record.data, stamp));
					Ok(())
				};
				match read() {
					Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
					Err(error) => {
						refused = Some(error);
						Ok(std::ops::ControlFlow::Break(()))
					}
				}
			})
			.await?;
		if let Some(error) = refused {
			return Err(error);
		}
		let mut links: Vec<(RecordId, RecordId)> = Vec::new();
		for (rid, mut data, stamp) in rows {
			if let Some(replay) = &replay {
				replay.apply(ctx, &mut data, stamp).await.map_err(|flow| {
					flow.into_error_or(|_| {
						anyhow::anyhow!("migration replay signalled control flow")
					})
				})?;
			}
			for (_, found) in data.walk(&field.name) {
				if let Value::RecordId(target) = &found {
					links.push((rid.clone(), target.clone()));
				} else if let Value::Array(items) = &found {
					links.extend(items.iter().filter_map(|item| {
						item.as_record().map(|target| (rid.clone(), target.clone()))
					}));
				} else if let Value::Set(items) = &found {
					links.extend(items.iter().filter_map(|item| {
						item.as_record().map(|target| (rid.clone(), target.clone()))
					}));
				}
			}
		}
		for (rid, target) in &links {
			if !field
				.field_kind
				.as_ref()
				.is_none_or(|kind| kind.reference_can_target(&target.table))
			{
				continue;
			}
			let key = codec::key::encode_fields(
				"ReferenceKey",
				&[
					("ns", KeyField::Int(ns.0 as i64)),
					("db", KeyField::Int(db.0 as i64)),
					("tb", KeyField::Str(target.table.as_str())),
					("id", KeyField::Id(&target.key)),
					("foreign_table", KeyField::Str(rid.table.as_str())),
					("foreign_field", KeyField::Str(ff.as_str())),
					("foreign_key", KeyField::Id(&rid.key)),
				],
			)?;
			txn.set(key.into(), Vec::new()).await?;
			crate::idx::inline_cache::record_ref_write(
				txn, ns, db, target, &rid.table, &ff, &rid.key,
			)
			.await?;
		}
		if stats.rows == 0 {
			return Ok(());
		}
	}
}
