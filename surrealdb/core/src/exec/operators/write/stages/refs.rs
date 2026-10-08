//! The reference keys the record's fields declare.
//!
//! The keys a `REFERENCE` field maintains, and their release.
//!
//! The release runs on `DELETE` before the record is cleared, because it walks
//! the post-image the clear blanks.
//!
//! A reference key is the back-link a referenced record is found by: it lives
//! under the *target* record's range and names the record and field that point
//! at it. Both stages below maintain the same key set, and differ only in the
//! value they judge the record's fields against — the field's own value on a
//! write, and `NONE` on a delete, which releases every link the field holds.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::catalog::FieldDefinition;
use crate::err::EngineError;
use crate::exec::BoxFut;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::stage::{StageId, StageOutcome, WriteStage};
use crate::expr::ControlFlow;
use crate::key::schema::ReferenceKey;
use crate::kvs::Transaction;
use crate::val::{RecordId, Value};

/// This module's stages, by id.
///
/// `None` is a stage whose body has not landed:
/// [`WritePlan::resolve`](crate::exec::operators::write::plan::WritePlan)
/// refuses to bind a plan that names one, rather than running a partial order.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "a stage this module does not own is not this module's to name, and `super::lookup` \
	          routes none of them here"
)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::CleanupTableReferences => Some(Arc::new(CleanupTableReferences)),
		StageId::ProcessTableReferences => Some(Arc::new(ProcessTableReferences)),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// Maintain the reference keys the record's fields declare.
///
/// Sits after the write and its permission gate on every order that writes a
/// record, so a link is published only for a write the caller was allowed to
/// make. It is not conditional on the write having reached storage: the store
/// skips a record no mutation phase changed, a `DROP` table's record and a
/// lightweight relation's edge, and each of those publishes its links all the
/// same.
///
/// Nor is it conditional on the write being an import. A replay skips the
/// state a record's *own* clauses would derive — field `TYPE`, `VALUE` and
/// `ASSERT` — because the exported record already carries their result. A
/// reference key is not that: it lives under the *referenced* record's range,
/// so no exported record carries it and nothing else in the replay writes it.
/// Skipping it here drops every backlink in the restored database, silently,
/// with the referencing field itself intact.
///
/// Each `REFERENCE` field is judged one place at a time — a field whose path
/// spans an array yields one place per element — against the same place in the
/// before-image, so a record whose links are unchanged writes no keys.
#[derive(Debug)]
struct ProcessTableReferences;

impl WriteStage for ProcessTableReferences {
	fn name(&self) -> &'static str {
		StageId::ProcessTableReferences.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			#[cfg(test)]
			REFERENCE_BODIES.with(|runs| runs.set(runs.get() + 1));
			let rid = record_id(row)?;
			let txn = ctx.txn();
			for fd in reference_fields(ctx) {
				for (place, value) in row.current.walk(&fd.name) {
					let previous = row.initial.pick(&place);
					update_reference_keys(ctx, &txn, &rid, fd, &previous, &value).await?;
				}
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// Release the outgoing references the record holds.
///
/// `DELETE` only, and ahead of the clear: it reads the post-image, which the
/// clear blanks, and treats every link it finds there as removed. The keys it
/// deletes are the ones this record wrote under other records' ranges; the
/// links *into* this record are the purge's to answer for.
#[derive(Debug)]
struct CleanupTableReferences;

impl WriteStage for CleanupTableReferences {
	fn name(&self) -> &'static str {
		StageId::CleanupTableReferences.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			#[cfg(test)]
			REFERENCE_BODIES.with(|runs| runs.set(runs.get() + 1));
			let rid = record_id(row)?;
			let txn = ctx.txn();
			for fd in reference_fields(ctx) {
				for (_, value) in row.current.walk(&fd.name) {
					// A place holding nothing links to nothing, and the diff
					// below would be empty for it anyway.
					if value.is_none() || value.is_empty_array() {
						continue;
					}
					update_reference_keys(ctx, &txn, &rid, fd, &value, &Value::None).await?;
				}
			}
			Ok(StageOutcome::Continue)
		})
	}
}

#[cfg(test)]
thread_local! {
	/// How many times this thread ran a reference stage's body over a record, for
	/// tests that must know a write kept its reference keys as instructions.
	pub(crate) static REFERENCE_BODIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The table's fields that declare a `REFERENCE`.
fn reference_fields(ctx: &WriteCtx) -> impl Iterator<Item = &FieldDefinition> {
	ctx.fields.iter().filter(|fd| fd.reference.is_some())
}

/// Bring the reference keys of one field place from `previous` to `current`.
///
/// The keys are a set: the links dropped between the two values are deleted and
/// the links gained are written, so a place whose links are unchanged — in any
/// order, and however the value around them changed — writes nothing. Each key
/// is mirrored in the referenced record's inline cache, written after the key
/// so a first-write backfill's scan already includes it.
async fn update_reference_keys(
	ctx: &WriteCtx,
	txn: &Transaction,
	rid: &RecordId,
	fd: &FieldDefinition,
	previous: &Value,
	current: &Value,
) -> Result<(), ControlFlow> {
	if previous == current {
		return Ok(());
	}
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let previous = referenced_ids(previous);
	let current = referenced_ids(current);
	// The field as the key names it, which is what the reference scan and the
	// delete strategies match against.
	let field = fd.name.to_sql();
	for target in previous.difference(&current) {
		let key = reference_key(ctx, target, rid, &field);
		txn.del_key(&key).await?;
		crate::idx::inline_cache::record_ref_delete(
			txn, ns, db, target, &rid.table, &field, &rid.key,
		)
		.await?;
	}
	for target in current.difference(&previous) {
		let key = reference_key(ctx, target, rid, &field);
		txn.set_key(&key, &()).await?;
		crate::idx::inline_cache::record_ref_write(
			txn, ns, db, target, &rid.table, &field, &rid.key,
		)
		.await?;
	}
	Ok(())
}

/// The back-link key stored under `target` for the field `field` of `rid`.
fn reference_key<'a>(
	ctx: &WriteCtx,
	target: &'a RecordId,
	rid: &'a RecordId,
	field: &'a str,
) -> ReferenceKey<'a> {
	ReferenceKey {
		ns: ctx.ns.namespace_id,
		db: ctx.db.database_id,
		tb: Cow::Borrowed(&target.table),
		id: Cow::Borrowed(&target.key),
		foreign_table: Cow::Borrowed(&rid.table),
		foreign_field: Cow::Borrowed(field),
		foreign_key: Cow::Borrowed(&rid.key),
	}
}

/// The records a field's value links to.
///
/// A collection links to each record it holds and to nothing else, so a value
/// that is not a record and holds none links to nothing.
fn referenced_ids(value: &Value) -> HashSet<&RecordId> {
	if let Some(rid) = value.as_record() {
		return HashSet::from([rid]);
	}
	if let Value::Array(array) = value {
		return array.iter().filter_map(|v| v.as_record()).collect();
	}
	if let Value::Set(set) = value {
		return set.iter().filter_map(|v| v.as_record()).collect();
	}
	HashSet::new()
}

/// The record id the reference keys are written under.
///
/// Every order derives or carries one long before a reference stage runs, so a
/// row without one is a broken order rather than a rejected record.
fn record_id(row: &WriteRow) -> Result<RecordId, ControlFlow> {
	row.id.clone().ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(EngineError::unreachable(
			"Expected a document id to be present",
		)))
	})
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::operators::write::clauses::WriteClauses;
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::kvs::TransactionType;
	use crate::val::{Object, RecordIdKey, TableName};

	/// A table whose `friend` field publishes a back-link on every record it
	/// points at.
	const SCHEMA: &str = "DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD friend ON person TYPE option<record<person>> REFERENCE;";

	/// The context one table's stages share, over a fresh write transaction.
	async fn ctx_for(db: &TestDb) -> WriteCtx {
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		WriteCtx::resolve(
			&exec,
			&TableName::new("person"),
			TableExistence::Required,
			None,
			Arc::new(WriteClauses::default()),
		)
		.await
		.unwrap()
	}

	fn person(key: &str) -> RecordId {
		RecordId {
			table: TableName::new("person"),
			key: RecordIdKey::String(key.into()),
		}
	}

	/// A record whose `friend` field links to `target`.
	fn links_to(target: &RecordId) -> Value {
		Value::Object(Object::from_iter([("friend".to_string(), Value::RecordId(target.clone()))]))
	}

	/// The row a write carries for `person:a`, with the images given.
	fn row_for(initial: Value, current: Value) -> WriteRow {
		let mut row = WriteRow::for_target(Value::RecordId(person("a"))).unwrap();
		row.initial = initial;
		row.current = current;
		row
	}

	/// Whether `target` holds a back-link from `person:a`'s `friend` field.
	async fn linked(ctx: &WriteCtx, target: &RecordId) -> bool {
		let from = person("a");
		let key = reference_key(ctx, target, &from, "friend");
		ctx.txn().exists_key(&key, None).await.unwrap()
	}

	/// A link the record gained publishes a key under the record it points at.
	#[tokio::test]
	async fn a_gained_link_is_published() {
		let db = TestDb::new(SCHEMA).await;
		let ctx = ctx_for(&db).await;
		let mut row = row_for(Value::None, links_to(&person("b")));
		let outcome = ProcessTableReferences.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(outcome, StageOutcome::Continue);
		assert!(linked(&ctx, &person("b")).await, "the referenced record holds the back-link");
	}

	/// A link the record already held is left alone: the keys are a set, and a
	/// write that does not change it writes nothing.
	#[tokio::test]
	async fn an_unchanged_link_writes_nothing() {
		let db = TestDb::new(SCHEMA).await;
		let ctx = ctx_for(&db).await;
		let mut row = row_for(links_to(&person("b")), links_to(&person("b")));
		ProcessTableReferences.apply(&mut row, &ctx).await.unwrap();
		assert!(
			!linked(&ctx, &person("b")).await,
			"a link present in both images is neither rewritten nor re-derived"
		);
	}

	/// Moving a link releases the record it left and publishes the one it
	/// reached, in that order.
	#[tokio::test]
	async fn a_moved_link_is_released_and_republished() {
		let db = TestDb::new(SCHEMA).await;
		let ctx = ctx_for(&db).await;
		let mut row = row_for(Value::None, links_to(&person("b")));
		ProcessTableReferences.apply(&mut row, &ctx).await.unwrap();

		let mut row = row_for(links_to(&person("b")), links_to(&person("c")));
		ProcessTableReferences.apply(&mut row, &ctx).await.unwrap();
		assert!(!linked(&ctx, &person("b")).await, "the record left holds no back-link");
		assert!(linked(&ctx, &person("c")).await, "the record reached holds one");
	}

	/// The cleanup releases every link the post-image holds, which is why it
	/// runs before the clear blanks that image.
	#[tokio::test]
	async fn cleanup_releases_the_links_the_post_image_holds() {
		let db = TestDb::new(SCHEMA).await;
		let ctx = ctx_for(&db).await;
		let mut row = row_for(Value::None, links_to(&person("b")));
		ProcessTableReferences.apply(&mut row, &ctx).await.unwrap();
		assert!(linked(&ctx, &person("b")).await);

		let mut row = row_for(links_to(&person("b")), links_to(&person("b")));
		let outcome = CleanupTableReferences.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(outcome, StageOutcome::Continue);
		assert!(!linked(&ctx, &person("b")).await, "the delete releases the link");
	}
}
