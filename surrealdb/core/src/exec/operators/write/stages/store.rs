//! The record and its edge keys, written to storage.
//!
//! The stages that put bytes in the KV store or take them out: the record, its
//! edge keys and its inline edge-property payloads.
//!
//! These are the only stages that may set
//! [`WriteRow::mutated`](super::super::row::WriteRow), and only after a real
//! write completes — the statement's affected-row total is that flag.

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_datastore::values::graph::AdjacencyValue;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{LATEST_EDGE_VARIANT, Record, Relation, TableDefinition, TableType};
use crate::doc::{Error as DocError, encode_inline_section, inline_fields};
use crate::err::EngineError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::metadata::{edge_variant, is_edge};
use crate::exec::operators::write::row::{WriteExtras, WriteRow};
use crate::exec::operators::write::stage::{StageId, StageOutcome, WriteStage};
use crate::exec::{BoxFut, Error as ExecError};
use crate::expr::paths::{IN, OUT};
use crate::expr::{ControlFlow, Dir};
use crate::key::schema::{GraphKey, GraphPointerKey};
use crate::kvs::storage_error;
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
		StageId::ClearRecordData => Some(Arc::new(ClearRecordData)),
		StageId::StoreRecordData => Some(Arc::new(StoreRecordData)),
		StageId::StoreEdgesData => Some(Arc::new(StoreEdgesData)),
		StageId::StoreInlineAdjacencyData => Some(Arc::new(StoreInlineAdjacencyData)),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// Empty the post-image.
///
/// `DELETE` only. It sits after the reference cleanup, which walks the image
/// this blanks, and before the index maintenance and the purge, which read the
/// emptied image as the record's new state.
///
/// The record-type marker goes with the data: the post-image of a deleted
/// record is a record of no type. The before-image and the marker it carried
/// are left intact, because the purge dispatches its adjacency-key deletions on
/// them and the statement's output gate is shown them.
///
/// [`WriteRow::fields_computed`] is left untouched. Emptying the record does
/// not settle whether the record's observers are shown derived values: that is
/// the materialisation stage's decision, and it runs later in the order.
#[derive(Debug)]
struct ClearRecordData;

impl WriteStage for ClearRecordData {
	fn name(&self) -> &'static str {
		StageId::ClearRecordData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		_ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			row.current = Value::None;
			row.metadata = None;
			Ok(StageOutcome::Continue)
		})
	}
}

/// Write the record.
///
/// Sets [`WriteRow::mutated`] once the KV write has completed, and only then:
/// the statement's affected-row total is that flag, so a stage that set it
/// ahead of the write would report a record the transaction never received.
///
/// Whether the record is stored anew or overwritten is keyed on the row's
/// before-image — [`WriteRow::is_iteration_initial`] — and not on which order
/// selected this stage. A creating order whose data clause put an existing
/// record's id into the row still writes create-only, so a caller holding
/// `create` permission alone cannot overwrite a record it may not update: the
/// write fails as a duplicate naming the record, which is also how a form that
/// retries a conflict as an update recognises one.
///
/// Nothing is written for three subjects: a record no mutation phase changed, a
/// record of a `DROP` table, and an edge of a lightweight relation, whose whole
/// state is its adjacency keys.
#[derive(Debug)]
struct StoreRecordData;

impl WriteStage for StoreRecordData {
	fn name(&self) -> &'static str {
		StageId::StoreRecordData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if !row.is_modified() || ctx.table.drop || table_is_lightweight(&ctx.table) {
				return Ok(StageOutcome::Continue);
			}
			let rid = record_id(row)?;
			let ns = ctx.ns.namespace_id;
			let db = ctx.db.database_id;
			// A stored record is the post-image plus the record-type marker
			// the stages stamped on it, so the marker an edge was written
			// under always matches the adjacency keys of the same write.
			let record = Arc::new(Record {
				metadata: row.metadata.clone(),
				data: row.current.clone(),
			});
			let txn = ctx.txn();
			if row.is_iteration_initial() {
				txn.put_record(ns, db, &rid.table, &rid.key, record).await.map_err(|err| {
					if matches!(
						storage_error(&err),
						Some(crate::kvs::Error::TransactionKeyAlreadyExists)
					) {
						ControlFlow::Err(anyhow::Error::new(DocError::RecordExists {
							record: rid.clone(),
						}))
					} else {
						ControlFlow::Err(err)
					}
				})?;
			} else {
				txn.set_record(ns, db, &rid.table, &rid.key, record).await?;
			}
			row.mutated = true;
			Ok(StageOutcome::Continue)
		})
	}
}

/// Write an edge's four adjacency keys.
///
/// `RELATE` and `INSERT RELATION` only: the stage stands down for every other
/// subject, because the endpoints it keys on are the statement's, carried on
/// [`WriteExtras::Relate`], and not something the record body may name.
///
/// The four keys model one relation. `ltr` and `rtl` are vertex-side pointer
/// keys, stored on the `in` and `out` vertices with the opposite endpoint
/// embedded, so a `->edge->vertex` scan resolves the far vertex without reading
/// the edge record. `etl` and `etr` are edge-side keys, whose adjacency already
/// names the vertex, so they carry no embedded target and are the same across
/// layout generations. A lightweight relation writes the two pointer keys
/// alone: its records are synthesized from the canonical `[in, out]` id, so
/// there is no edge side to point at.
///
/// The layout to supersede is read from the **before-image's** marker. The data
/// clause has already advanced the post-image's stamp to the latest generation,
/// so asking the post-image would never find an older layout to clean up and
/// would strand a first-generation edge's vertex-side keys.
#[derive(Debug)]
struct StoreEdgesData;

impl WriteStage for StoreEdgesData {
	fn name(&self) -> &'static str {
		StageId::StoreEdgesData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if ctx.table.drop {
				return Ok(StageOutcome::Continue);
			}
			let WriteExtras::Relate(l, r, _) = &row.extras else {
				return Ok(StageOutcome::Continue);
			};
			let (l, r) = (l.clone(), r.clone());
			let rid = record_id(row)?;
			let ns = ctx.ns.namespace_id;
			let db = ctx.db.database_id;
			let txn = ctx.txn();

			// An enforced relation admits an edge only between endpoints that
			// exist — except while replaying an export, where an edge table
			// whose name sorts earlier is restored before the vertex tables it
			// points at. Enforcement is an admission check on the write path
			// and not an invariant any reader depends on, so deferring it for
			// the length of a restore costs nothing a query can observe. Every
			// sibling observer defers on the same flag for the same reason.
			if !is_import(ctx)
				&& matches!(
					ctx.table.table_type,
					TableType::Relation(Relation {
						enforced: true,
						..
					})
				) {
				for endpoint in [&l, &r] {
					let exists = txn
						.record_exists(
							ns,
							db,
							&endpoint.table,
							&endpoint.key,
							ctx.exec().version_stamp(),
						)
						.await
						.map_err(ControlFlow::Err)?;
					if !exists {
						return Err(ControlFlow::Err(anyhow::Error::new(DocError::IdNotFound {
							rid: endpoint.to_sql(),
						})));
					}
				}
			}

			// A lightweight relation cannot itself be a graph endpoint: its
			// records are synthesized from adjacency, so hanging classic edges
			// off one would write pointer keys into the lightweight table's own
			// graph subspace — the state its emptiness probes and record-less
			// scans define as impossible.
			for endpoint in [&l, &r] {
				let def =
					txn.get_tb(ns, db, &endpoint.table, None).await.map_err(ControlFlow::Err)?;
				if def.is_some_and(|def| table_is_lightweight(&def)) {
					return Err(ControlFlow::Err(anyhow::Error::new(ExecError::Thrown(format!(
						"a LIGHTWEIGHT relation's edges cannot be graph endpoints: {}",
						endpoint.to_sql()
					)))));
				}
			}

			let lightweight = table_is_lightweight(&ctx.table);
			let etl = GraphKey {
				ns,
				db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				dir: Dir::In,
				foreign_table: Cow::Borrowed(&l.table),
				foreign_key: Cow::Borrowed(&l.key),
			};
			let etr = GraphKey {
				ns,
				db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				dir: Dir::Out,
				foreign_table: Cow::Borrowed(&r.table),
				foreign_key: Cow::Borrowed(&r.key),
			};
			// A lightweight edge only ever exists in the latest layout, so the
			// supersede pass is for stored edges alone.
			if !lightweight
				&& edge_variant(&row.initial_metadata).unwrap_or(LATEST_EDGE_VARIANT) == 1
			{
				let ltr_legacy = GraphKey {
					ns,
					db,
					tb: Cow::Borrowed(&l.table),
					id: Cow::Borrowed(&l.key),
					dir: Dir::Out,
					foreign_table: Cow::Borrowed(&rid.table),
					foreign_key: Cow::Borrowed(&rid.key),
				};
				let rtl_legacy = GraphKey {
					ns,
					db,
					tb: Cow::Borrowed(&r.table),
					id: Cow::Borrowed(&r.key),
					dir: Dir::In,
					foreign_table: Cow::Borrowed(&rid.table),
					foreign_key: Cow::Borrowed(&rid.key),
				};
				futures::try_join!(txn.del_key(&ltr_legacy), txn.del_key(&rtl_legacy))
					.map_err(ControlFlow::Err)?;
			}
			let ltr = pointer_key(ns, db, &l, Dir::Out, &rid, &r);
			let rtl = pointer_key(ns, db, &r, Dir::In, &rid, &l);

			let live = inline_value(row, ctx, lightweight)?;
			if lightweight {
				futures::try_join!(txn.set_key(&ltr, &live), txn.set_key(&rtl, &live))
					.map_err(ControlFlow::Err)?;
			} else {
				futures::try_join!(
					txn.set_key(&ltr, &live),
					txn.set_key(&etl, &()),
					txn.set_key(&etr, &()),
					txn.set_key(&rtl, &live),
				)
				.map_err(ControlFlow::Err)?;
			}
			// Fold the edge into each endpoint's inline adjacency cache after
			// the key writes, so a first-write backfill's scan — which runs in
			// this transaction — already includes them.
			let resolve = Some(ctx.exec().root().ctx.get_index_stores().adjacency_resolve());
			crate::idx::inline_cache::record_edge_write(
				&txn,
				ns,
				db,
				&l,
				Dir::Out,
				&rid,
				&r,
				resolve,
			)
			.await
			.map_err(ControlFlow::Err)?;
			crate::idx::inline_cache::record_edge_write(
				&txn,
				ns,
				db,
				&r,
				Dir::In,
				&rid,
				&l,
				resolve,
			)
			.await
			.map_err(ControlFlow::Err)?;
			// Reset `in`/`out` to the canonical endpoints, so a user-supplied
			// body cannot override the endpoints the adjacency was written
			// under.
			row.current.put(&IN, Value::RecordId(l));
			row.current.put(&OUT, Value::RecordId(r));
			Ok(StageOutcome::Continue)
		})
	}
}

/// Restamp a stored edge's two pointer values with the current inline payload.
///
/// Every update-shaped order but `RELATE`'s, which embeds the payload in the
/// pointer keys it writes. The rewrite is unconditional — even where the inline
/// projection did not change — because it is also the lazy repair for a payload
/// written under a superseded generation.
///
/// The pointer keys are derived from the **committed** edge's endpoints, and a
/// write that moves `in` or `out` on the record is refused: an edge's endpoints
/// are fixed at `RELATE`, and the record must keep naming the endpoints its
/// adjacency was written under, or a later rewrite would derive keys from a
/// record that no longer matches them.
#[derive(Debug)]
struct StoreInlineAdjacencyData;

impl WriteStage for StoreInlineAdjacencyData {
	fn name(&self) -> &'static str {
		StageId::StoreInlineAdjacencyData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if matches!(row.extras, WriteExtras::Relate(_, _, _)) {
				return Ok(StageOutcome::Continue);
			}
			// Only a stored edge in the current layout carries pointer values:
			// a superseded variant has no pointer keys to stamp, and a
			// lightweight edge is immutable and payload-less by definition.
			if !is_edge(&row.initial_metadata)
				|| edge_variant(&row.initial_metadata) != Some(LATEST_EDGE_VARIANT)
			{
				return Ok(StageOutcome::Continue);
			}
			if ctx.table.drop || table_is_lightweight(&ctx.table) {
				return Ok(StageOutcome::Continue);
			}
			let inline = inline_fields(&ctx.fields);
			if inline.is_empty() {
				return Ok(StageOutcome::Continue);
			}
			let (l, r) = (endpoint(&row.initial, &IN, "in")?, endpoint(&row.initial, &OUT, "out")?);
			unmoved(&row.current, &IN, &l, |value| DocError::InOverride {
				value,
			})?;
			unmoved(&row.current, &OUT, &r, |value| DocError::OutOverride {
				value,
			})?;
			let rid = record_id(row)?;
			let ns = ctx.ns.namespace_id;
			let db = ctx.db.database_id;
			let cap = ctx.exec().root().ctx.config.idx.graph_inline_props_cap;
			let section =
				encode_inline_section(&row.current, &inline, ctx.table.graph_inline_gen, cap)
					.map_err(ControlFlow::Err)?;
			let value = AdjacencyValue {
				tombstone: false,
				sections: vec![section],
			};
			let txn = ctx.txn();
			let ltr = pointer_key(ns, db, &l, Dir::Out, &rid, &r);
			let rtl = pointer_key(ns, db, &r, Dir::In, &rid, &l);
			futures::try_join!(txn.set_key(&ltr, &value), txn.set_key(&rtl, &value))
				.map_err(ControlFlow::Err)?;
			Ok(StageOutcome::Continue)
		})
	}
}

/// One vertex-side pointer key: stored on `vertex` in `dir`, naming the edge,
/// with the opposite endpoint embedded so a traversal resolves the far vertex
/// without reading the edge record.
fn pointer_key<'a>(
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	vertex: &'a RecordId,
	dir: Dir,
	edge: &'a RecordId,
	target: &'a RecordId,
) -> GraphPointerKey<'a> {
	GraphPointerKey {
		ns,
		db,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir,
		foreign_table: Cow::Borrowed(&edge.table),
		foreign_key: Cow::Borrowed(&edge.key),
		target_table: Cow::Borrowed(&target.table),
		target_key: Cow::Borrowed(&target.key),
	}
}

/// The value both pointer keys carry: the table's `INLINE` field values read
/// off the post-image, or the empty live marker where the table declares none.
///
/// A lightweight relation carries no payload — it has no record to project
/// from — so it always takes the empty marker.
fn inline_value(
	row: &WriteRow,
	ctx: &WriteCtx,
	lightweight: bool,
) -> Result<AdjacencyValue, ControlFlow> {
	if lightweight {
		return Ok(AdjacencyValue::live());
	}
	let inline = inline_fields(&ctx.fields);
	if inline.is_empty() {
		return Ok(AdjacencyValue::live());
	}
	let section = encode_inline_section(
		&row.current,
		&inline,
		ctx.table.graph_inline_gen,
		ctx.exec().root().ctx.config.idx.graph_inline_props_cap,
	)
	.map_err(ControlFlow::Err)?;
	Ok(AdjacencyValue {
		tombstone: false,
		sections: vec![section],
	})
}

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

/// An import restores state that was written under enforcement once already.
fn is_import(ctx: &WriteCtx) -> bool {
	ctx.exec().options().is_some_and(|options| options.import)
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

/// The record id a storage write is keyed on.
///
/// Every order derives or carries one before reaching a storage stage, so a row
/// without one is a broken order rather than a rejected record.
pub(super) fn record_id(row: &WriteRow) -> Result<RecordId, ControlFlow> {
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

	/// The context one table's stages share, over a fresh write transaction.
	async fn ctx_for(db: &TestDb, table: &str) -> WriteCtx {
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		WriteCtx::resolve(
			&exec,
			&TableName::new(table),
			TableExistence::DefinedOnDemand,
			None,
			Arc::new(WriteClauses::default()),
		)
		.await
		.unwrap()
	}

	fn id(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table),
			key: RecordIdKey::String(key.into()),
		}
	}

	fn object(field: &str, value: i64) -> Value {
		Value::Object(Object::from_iter([(field.to_string(), Value::from(value))]))
	}

	/// One field of a record, or `NONE` when the record does not hold it.
	fn field(value: &Value, name: &str) -> Value {
		let Value::Object(obj) = value else {
			return Value::None;
		};
		obj.get(name).cloned().unwrap_or(Value::None)
	}

	/// The row a creating order carries: a named record id, and no
	/// before-image.
	fn creating(rid: RecordId, current: Value) -> WriteRow {
		let mut row = WriteRow::for_target(Value::RecordId(rid)).unwrap();
		row.current = current;
		row
	}

	/// What storage holds for a record. A record that is not there answers
	/// `NONE`, as a missing key does.
	async fn stored(ctx: &WriteCtx, rid: &RecordId) -> Value {
		ctx.txn()
			.get_record(ctx.ns.namespace_id, ctx.db.database_id, &rid.table, &rid.key, None)
			.await
			.unwrap()
			.data
			.clone()
	}

	/// The row an updating order carries for a subject a row source read: the
	/// record's value alone, with no marker, exactly as a batch yields it.
	fn sourced(rid: &RecordId, fields: impl IntoIterator<Item = (String, Value)>) -> WriteRow {
		let mut obj = Object::from_iter(fields);
		obj.insert("id".to_string(), Value::RecordId(rid.clone()));
		WriteRow::for_record(Value::Object(obj)).unwrap()
	}

	/// How many adjacency keys one record carries, in both directions.
	///
	/// The count alone distinguishes every case here: a vertex of a stored
	/// relation carries one pointer key per edge, and the edge record carries
	/// one edge-side key per direction.
	async fn adjacency(ctx: &WriteCtx, record: &RecordId) -> usize {
		let range = crate::key::schema::GraphIdPrefix {
			ns: ctx.ns.namespace_id,
			db: ctx.db.database_id,
			tb: Cow::Borrowed(&record.table),
			id: Cow::Borrowed(&record.key),
		}
		.range()
		.unwrap();
		ctx.txn().keys_raw(range, 1000, 0, None).await.unwrap().len()
	}

	/// A `RELATE` writes four keys: one pointer key on each vertex, and one
	/// edge-side key in each direction on the edge record itself.
	#[tokio::test]
	async fn a_relate_writes_all_four_adjacency_keys() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE likes TYPE RELATION SCHEMALESS;",
		)
		.await;
		let ctx = ctx_for(&db, "likes").await;
		let (l, r, edge) = (id("person", "a"), id("person", "b"), id("likes", "one"));
		let mut row = creating(edge.clone(), object("n", 1));
		row.extras = WriteExtras::Relate(l.clone(), r.clone(), None);

		assert_eq!(StoreEdgesData.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);

		assert_eq!(adjacency(&ctx, &l).await, 1, "a pointer key on the `in` vertex");
		assert_eq!(adjacency(&ctx, &r).await, 1, "a pointer key on the `out` vertex");
		assert_eq!(adjacency(&ctx, &edge).await, 2, "one edge-side key per direction");
		// The endpoints are restamped onto the record, so a user-supplied body
		// cannot override the endpoints the adjacency was written under.
		assert_eq!(field(&row.current, "in"), Value::RecordId(l));
		assert_eq!(field(&row.current, "out"), Value::RecordId(r));
	}

	/// A lightweight relation stores the two vertex-side pointer keys alone:
	/// its records are synthesized from the canonical `[in, out]` id, so there
	/// is no edge side to point at.
	#[tokio::test]
	async fn a_lightweight_relate_writes_the_vertex_keys_alone() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE likes TYPE RELATION IN person OUT person LIGHTWEIGHT SCHEMALESS;
			 CREATE person:a; CREATE person:b;",
		)
		.await;
		let ctx = ctx_for(&db, "likes").await;
		let (l, r, edge) = (id("person", "a"), id("person", "b"), id("likes", "one"));
		let mut row = creating(edge.clone(), Value::empty_object());
		row.extras = WriteExtras::Relate(l.clone(), r.clone(), None);

		assert_eq!(StoreEdgesData.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);

		assert_eq!(adjacency(&ctx, &l).await, 1, "a pointer key on the `in` vertex");
		assert_eq!(adjacency(&ctx, &r).await, 1, "a pointer key on the `out` vertex");
		assert_eq!(adjacency(&ctx, &edge).await, 0, "and no edge-side key at all");
	}

	/// An enforced relation admits an edge only between endpoints that exist.
	#[tokio::test]
	async fn an_enforced_relation_refuses_a_missing_endpoint() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE likes TYPE RELATION IN person OUT person ENFORCED SCHEMALESS;",
		)
		.await;
		let ctx = ctx_for(&db, "likes").await;
		let (l, r) = (id("person", "a"), id("person", "b"));
		let mut row = creating(id("likes", "one"), Value::empty_object());
		row.extras = WriteExtras::Relate(l, r, None);
		assert!(
			StoreEdgesData.apply(&mut row, &ctx).await.is_err(),
			"neither endpoint was ever written"
		);
	}

	/// The inline restamp stands down for a subject whose before-image names
	/// no edge — which is every ordinary record, and which is also what a row
	/// whose marker never reached it would look like.
	#[tokio::test]
	async fn the_inline_restamp_stands_down_for_a_plain_record() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE likes TYPE RELATION SCHEMALESS;
			 DEFINE FIELD n ON likes INLINE;",
		)
		.await;
		let ctx = ctx_for(&db, "likes").await;
		let mut row = sourced(&id("likes", "one"), [("n".to_string(), Value::from(1))]);
		assert_eq!(
			StoreInlineAdjacencyData.apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue
		);
	}

	/// A write that moves an edge's endpoints is refused: the record and the
	/// adjacency about to be restamped must keep naming the same endpoints, or
	/// a later rewrite would derive pointer keys from a record that no longer
	/// matches them.
	#[tokio::test]
	async fn the_inline_restamp_refuses_an_endpoint_override() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE likes TYPE RELATION SCHEMALESS;
			 DEFINE FIELD n ON likes INLINE;",
		)
		.await;
		let ctx = ctx_for(&db, "likes").await;
		let (l, r) = (id("person", "a"), id("person", "b"));
		let edge = |to: &RecordId| {
			[
				("in".to_string(), Value::RecordId(l.clone())),
				("out".to_string(), Value::RecordId(to.clone())),
				("n".to_string(), Value::from(1)),
			]
		};
		let rid = id("likes", "one");
		let mut row = sourced(&rid, edge(&r));
		row.initial_metadata = Some(crate::catalog::Metadata {
			record_type: crate::catalog::RecordType::Edge {
				variant: LATEST_EDGE_VARIANT,
			},
			aggregation_stats: Vec::new(),
		});
		row.current = Value::Object(Object::from_iter(edge(&id("person", "c"))));
		assert!(
			StoreInlineAdjacencyData.apply(&mut row, &ctx).await.is_err(),
			"an `out` the write moved is refused"
		);
	}

	/// A row a row source produced carries no record-type marker, and picks it
	/// up from storage before any stage dispatches on it.
	///
	/// Without this the three stages that read the marker all see `None`: an
	/// edge deleted through a row-sourced `DELETE` would keep its four
	/// adjacency keys, pointing at a record that is gone.
	#[tokio::test]
	async fn a_sourced_row_loads_its_record_type_marker() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE likes TYPE RELATION SCHEMALESS;",
		)
		.await;
		let ctx = ctx_for(&db, "likes").await;
		let rid = id("likes", "one");
		let marker = crate::catalog::Metadata {
			record_type: crate::catalog::RecordType::Edge {
				variant: LATEST_EDGE_VARIANT,
			},
			aggregation_stats: Vec::new(),
		};
		ctx.txn()
			.set_record(
				ctx.ns.namespace_id,
				ctx.db.database_id,
				&rid.table,
				&rid.key,
				Arc::new(Record {
					metadata: Some(marker.clone()),
					data: object("n", 1),
				}),
			)
			.await
			.unwrap();

		let mut row = sourced(&rid, [("n".to_string(), Value::from(1))]);
		assert!(row.initial_metadata.is_none(), "a batch carries no marker");
		row.load_stored_metadata(&ctx).await.unwrap();
		assert!(is_edge(&row.initial_metadata), "the before-image's marker is filled");
		assert!(is_edge(&row.metadata), "and so is the post-image's");
	}

	/// A record is its marker and its value, so a write that advances an
	/// edge's adjacency-key layout without touching a field is a write.
	///
	/// Comparing the images alone leaves it looking untouched, and the record
	/// then keeps claiming the superseded layout while the adjacency stages
	/// write the current one — a disagreement between a record and its own
	/// keys that nothing later reconciles.
	#[tokio::test]
	async fn a_record_whose_marker_moved_is_modified() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE likes TYPE RELATION SCHEMALESS;",
		)
		.await;
		let ctx = ctx_for(&db, "likes").await;
		let rid = id("likes", "one");
		let marker = |variant: u16| {
			Some(crate::catalog::Metadata {
				record_type: crate::catalog::RecordType::Edge {
					variant,
				},
				aggregation_stats: Vec::new(),
			})
		};

		let mut row = sourced(&rid, [("n".to_string(), Value::from(1))]);
		row.initial_metadata = marker(1);
		row.metadata = marker(LATEST_EDGE_VARIANT);
		assert_ne!(row.initial_metadata, row.metadata, "the layout advanced");
		assert_eq!(row.initial, row.current, "and nothing else did");
		assert!(row.is_modified());

		assert_eq!(StoreRecordData.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
		assert!(row.mutated, "the record is rewritten under its new marker");

		// The control: neither half moved, so neither did the record.
		let mut unchanged = sourced(&rid, [("n".to_string(), Value::from(1))]);
		unchanged.initial_metadata = marker(LATEST_EDGE_VARIANT);
		unchanged.metadata = marker(LATEST_EDGE_VARIANT);
		assert!(!unchanged.is_modified());
	}

	/// A record that reached storage is written verbatim and marked, which is
	/// what the statement counts as affected.
	#[tokio::test]
	async fn a_stored_record_is_written_and_marked() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = ctx_for(&db, "t").await;
		let rid = id("t", "one");
		let mut row = creating(rid.clone(), object("a", 1));
		let outcome = StoreRecordData.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(outcome, StageOutcome::Continue);
		assert!(row.mutated, "a completed write marks the row");
		assert_eq!(field(&stored(&ctx, &rid).await, "a"), Value::from(1));
	}

	/// A record no mutation phase changed is not rewritten, and does not count
	/// towards the statement's total.
	#[tokio::test]
	async fn an_unchanged_record_is_not_written() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = ctx_for(&db, "t").await;
		let rid = id("t", "one");
		let mut row = creating(rid.clone(), object("a", 1));
		row.initial = object("a", 1);
		let outcome = StoreRecordData.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(outcome, StageOutcome::Continue);
		assert!(!row.mutated, "an unchanged record affects nothing");
		assert_eq!(stored(&ctx, &rid).await, Value::None);
	}

	/// A `DROP` table keeps no records, so the write is skipped rather than
	/// performed and immediately undone — and nothing is counted.
	#[tokio::test]
	async fn a_drop_table_stores_nothing() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS DROP;").await;
		let ctx = ctx_for(&db, "t").await;
		let rid = id("t", "one");
		let mut row = creating(rid.clone(), object("a", 1));
		let outcome = StoreRecordData.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(outcome, StageOutcome::Continue);
		assert!(!row.mutated);
		assert_eq!(stored(&ctx, &rid).await, Value::None);
	}

	/// A creating order that reaches an id already taken fails as a duplicate
	/// naming the record, rather than overwriting it. This is the write a
	/// caller holding `create` permission alone must not be able to make, and
	/// the failure a form that retries a conflict as an update recognises.
	#[tokio::test]
	async fn a_create_over_an_existing_record_is_refused() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:one SET a = 1;").await;
		let ctx = ctx_for(&db, "t").await;
		let rid = id("t", "one");
		let mut row = creating(rid.clone(), object("a", 2));
		let Err(ControlFlow::Err(err)) = StoreRecordData.apply(&mut row, &ctx).await else {
			panic!("a create over an existing record must fail");
		};
		assert!(
			matches!(err.downcast_ref::<DocError>(), Some(DocError::RecordExists { .. })),
			"the failure must name the existing record, got: {err}"
		);
		assert!(!row.mutated, "a refused write affects nothing");
		assert_eq!(field(&stored(&ctx, &rid).await, "a"), Value::from(1));
	}

	/// A row carrying a before-image is an update, and overwrites.
	#[tokio::test]
	async fn a_row_with_a_before_image_overwrites() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:one SET a = 1;").await;
		let ctx = ctx_for(&db, "t").await;
		let rid = id("t", "one");
		let mut row = creating(rid.clone(), object("a", 2));
		row.initial = object("a", 1);
		StoreRecordData.apply(&mut row, &ctx).await.unwrap();
		assert!(row.mutated);
		assert_eq!(field(&stored(&ctx, &rid).await, "a"), Value::from(2));
	}

	/// A lightweight relation's whole state is its two vertex-side adjacency
	/// keys, so nothing about one of its edges is a record and none is written.
	/// The edge stages that emit those keys answer to the same table type.
	#[tokio::test]
	async fn a_lightweight_relation_stores_no_record() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;",
		)
		.await;
		let ctx = ctx_for(&db, "knows").await;
		let rid = id("knows", "one");
		let mut row = creating(rid.clone(), object("a", 1));
		let outcome = StoreRecordData.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(outcome, StageOutcome::Continue);
		assert!(!row.mutated, "an edge that is not a record affects no record");
		assert_eq!(stored(&ctx, &rid).await, Value::None);
	}

	/// Clearing empties the post-image and the marker it carries, and leaves
	/// the before-image the purge and the output gate read.
	#[tokio::test]
	async fn clearing_empties_the_post_image_alone() {
		use crate::catalog::{Metadata, RecordType};

		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = ctx_for(&db, "t").await;
		let mut row = creating(id("t", "one"), object("a", 1));
		row.initial = object("a", 1);
		row.initial_metadata = Some(Metadata {
			record_type: RecordType::Edge {
				variant: 2,
			},
			aggregation_stats: Vec::new(),
		});
		row.metadata = row.initial_metadata.clone();
		ClearRecordData.apply(&mut row, &ctx).await.unwrap();
		assert_eq!(row.current, Value::None);
		assert!(row.metadata.is_none(), "the post-image carries no record type once cleared");
		assert_eq!(row.initial, object("a", 1));
		assert!(row.initial_metadata.is_some(), "the before-image keeps its marker");
	}
}
