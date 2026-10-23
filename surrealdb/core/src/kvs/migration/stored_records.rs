//! Marks a datastore as opened by a build that reads the v1 record envelope.
//!
//! The migration does no work. Its ledger entry (id 6) is what it is for: every 4.0 build records
//! it at startup, and a build whose registry lacks it refuses to start on a datastore that carries
//! it (`check_downgrade`). Every build that cannot read the v1 envelope lacks it, so once a build
//! of this release has opened a datastore, such a build refuses the datastore rather than failing
//! on the first record it reads.
//!
//! The entry says that a reader of the envelope has opened the datastore, not that the datastore
//! holds the envelope. Whether a record is written in the v1 envelope or in the `revision`
//! encoding is the feature level's decision. The first stored-grammar bytes, or the raise of the
//! feature level that permits them, record id 7 (`RECORD_FORMAT_V1`), which marks the datastore as
//! holding them. Nothing is rewritten: a record stays in the encoding it was written in until it
//! is next written, and a reader dispatches on each record's own first bytes.
//!
//! Id 5 belonged to an earlier envelope that no release reads. It is retired and absent from the
//! registry, so a datastore marked with it is refused, and a build of that envelope refuses a
//! datastore marked with id 6.
//!
//! # Which nodes read the envelope
//!
//! Only builds that register this migration read the v1 envelope. A node of an earlier release
//! that is already running when the first build of this release starts does not re-check the
//! ledger, and fails to read each v1 record this release writes, so an upgrade that mixes the two
//! is not rolling-safe unless the earlier release carries the reader and registers this migration
//! too. The feature level keeps v1 records unwritten until it is raised, which is what makes a
//! rolling upgrade safe.

use anyhow::Result;

use crate::kvs::ds::Datastore;

/// Records nothing beyond the ledger entry the driver writes.
pub(super) async fn mark_records_stored_value_encoded(_ds: &Datastore) -> Result<()> {
	Ok(())
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod tests {
	use std::borrow::Cow;
	use std::sync::Arc;

	use surrealdb_strand::Strand;

	use crate::catalog::aggregation::AggregationStat;
	use crate::catalog::providers::DatabaseProvider;
	use crate::catalog::{Metadata, Record, RecordType, StoredRecord, TableLayouts, ViewState};
	use crate::dbs::Session;
	use crate::key::schema::{RecordKey, ViewStateKey};
	use crate::key::{KVKey, KVValue, Key};
	use crate::kvs::TransactionType;
	use crate::kvs::ds::Datastore;
	use crate::val::convert_public::convert_public_value_to_internal;
	use crate::val::{Array, Object, RecordId, RecordIdKey, TableName, Value};

	/// Runs `sql` in namespace and database `test`, answering what its last
	/// statement returned, and fails on a statement that fails.
	async fn run(ds: &Datastore, sql: &str) -> Value {
		let session = Session::owner().with_ns("test").with_db("test");
		let mut last = Value::None;
		for response in ds.execute(sql, &session, None).await.unwrap() {
			last = convert_public_value_to_internal(response.result.unwrap());
		}
		last
	}

	fn rid(table: &str, key: i64) -> RecordId {
		RecordId {
			table: TableName::from(table),
			key: RecordIdKey::Number(key),
		}
	}

	fn object(fields: &[(&str, Value)]) -> Value {
		let mut object = Object::default();
		for (name, value) in fields {
			object.0.insert(Strand::new(name), value.clone());
		}
		Value::Object(object)
	}

	/// The key of record `rid` in namespace and database `test`.
	async fn key_of(ds: &Datastore, rid: &RecordId) -> RecordKey<'static> {
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let db = txn.expect_db_by_name("test", "test").await.unwrap();
		txn.cancel().await.unwrap();
		RecordKey {
			ns: db.namespace_id,
			db: db.database_id,
			tb: Cow::Owned(rid.table.clone()),
			id: Cow::Owned(rid.key.clone()),
		}
	}

	/// The layouts of table `table` in namespace and database `test`, which a
	/// record stamped with one of the table's generations is decoded through.
	async fn layouts(ds: &Datastore, table: &str) -> Arc<TableLayouts> {
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let db = txn.expect_db_by_name("test", "test").await.unwrap();
		let layouts = txn
			.table_layouts(db.namespace_id, db.database_id, &TableName::from(table), None)
			.await
			.unwrap();
		txn.cancel().await.unwrap();
		layouts
	}

	/// The bytes stored at record `rid`'s key.
	async fn stored(ds: &Datastore, rid: &RecordId) -> Vec<u8> {
		let key = key_of(ds, rid).await;
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let bytes = txn.get_key_raw(&key, None).await.unwrap().expect("the record is stored");
		txn.cancel().await.unwrap();
		bytes
	}

	/// Every record a statement writes is written in the stored-value
	/// encoding: a record and an edge with their `id` left to the key, and a
	/// view row whole, its aggregation state at a key of its own. Each reads back as written.
	#[tokio::test]
	async fn a_write_stores_the_stored_value_encoding() {
		let ds = Datastore::new("memory").await.unwrap();
		run(
			&ds,
			"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test; USE DB test; \
			 DEFINE TABLE t; DEFINE TABLE v AS SELECT count() AS c FROM t GROUP ALL; \
			 CREATE t:1 SET a = 1; CREATE t:2 SET a = 2; RELATE t:1->e:1->t:2 SET w = 3;",
		)
		.await;

		let bytes = stored(&ds, &rid("t", 1)).await;
		let record = StoredRecord::parse(&bytes).unwrap().expect("stored-value encoded");
		assert!(record.is_keyed() && !record.is_edge());
		assert_eq!(
			Record::decode_stored(&bytes, Some(&*layouts(&ds, "t").await), rid("t", 1))
				.unwrap()
				.data,
			object(&[("a", Value::from(1i64)), ("id", Value::RecordId(rid("t", 1)))])
		);

		let bytes = stored(&ds, &rid("e", 1)).await;
		let edge = StoredRecord::parse(&bytes).unwrap().expect("stored-value encoded");
		assert!(edge.is_keyed() && edge.is_edge());
		assert_eq!(
			Record::decode_stored(&bytes, Some(&*layouts(&ds, "e").await), rid("e", 1))
				.unwrap()
				.data,
			object(&[
				("id", Value::RecordId(rid("e", 1))),
				("in", Value::RecordId(rid("t", 1))),
				("out", Value::RecordId(rid("t", 2))),
				("w", Value::from(3i64)),
			])
		);
		assert_eq!(run(&ds, "SELECT VALUE w FROM ONLY e:1").await, Value::from(3i64));

		let Value::Array(ids) = run(&ds, "SELECT VALUE id FROM v").await else {
			panic!("the view's rows are listed");
		};
		let [Value::RecordId(row)] = ids.0.as_slice() else {
			panic!("the view holds one row: {ids:?}");
		};
		let bytes = stored(&ds, row).await;
		assert!(
			StoredRecord::parse(&bytes).unwrap().is_some(),
			"a view row is a v1 record: its aggregation state has a key of its own"
		);
		let view_row =
			Record::decode_as_written(&bytes, Some(&*layouts(&ds, "v").await), || row.clone())
				.unwrap();
		assert!(view_row.metadata.is_none(), "a view row carries no state of its own");
		assert_eq!(run(&ds, "SELECT VALUE c FROM v").await, Value::from(vec![Value::from(2i64)]));
	}

	/// The key of the state of group `group` of the view `table`, in namespace
	/// and database `test`.
	async fn state_key_of(ds: &Datastore, table: &str, group: &RecordIdKey) -> Key<'static> {
		let row = key_of(
			ds,
			&RecordId {
				table: TableName::from(table),
				key: group.clone(),
			},
		)
		.await;
		ViewStateKey {
			ns: row.ns,
			db: row.db,
			tb: row.tb,
			id: row.id,
		}
		.encode_key()
		.unwrap()
	}

	/// The statistics stored at `state`, or `None` for a group with no state.
	async fn state_stats(ds: &Datastore, state: &Key<'static>) -> Option<Vec<AggregationStat>> {
		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let bytes = txn.get(state.clone(), None).await.unwrap();
		txn.cancel().await.unwrap();
		bytes.map(|bytes| ViewState::kv_decode_value(&bytes, ()).unwrap().into_stats())
	}

	/// The single row of the view `table`.
	async fn only_row(ds: &Datastore, table: &str) -> RecordId {
		let Value::Array(ids) = run(ds, &format!("SELECT VALUE id FROM {table}")).await else {
			panic!("the view's rows are listed");
		};
		let [Value::RecordId(row)] = ids.0.as_slice() else {
			panic!("the view holds one row: {ids:?}");
		};
		row.clone()
	}

	/// The row of the view `table` under `group`, decoded as written.
	async fn view_row(ds: &Datastore, table: &str, group: &RecordIdKey) -> (Vec<u8>, Record) {
		let rid = RecordId {
			table: TableName::from(table),
			key: group.clone(),
		};
		let bytes = stored(ds, &rid).await;
		let layouts = layouts(ds, table).await;
		let record = Record::decode_as_written(&bytes, Some(&*layouts), || rid.clone()).unwrap();
		(bytes, record)
	}

	/// Rewrites the group `group` of the view `table` as a release that keeps a
	/// group's statistics inline writes it: the row in `revision` with the
	/// statistics in its metadata, and no state.
	async fn inline_group(ds: &Datastore, table: &str, group: &RecordIdKey) {
		let state = state_key_of(ds, table, group).await;
		let stats = state_stats(ds, &state).await.expect("the group has a state");
		let (_, row) = view_row(ds, table, group).await;
		let inline = Record {
			data: row.data,
			metadata: Some(Metadata {
				record_type: RecordType::Table,
				aggregation_stats: stats,
			}),
		};
		let row = key_of(
			ds,
			&RecordId {
				table: TableName::from(table),
				key: group.clone(),
			},
		)
		.await;
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		txn.set(KVKey::encode_key(&row).unwrap(), revision::to_vec(&inline).unwrap())
			.await
			.unwrap();
		txn.del(state).await.unwrap();
		txn.commit().await.unwrap();
	}

	/// The record count a group's statistics hold.
	fn count_of(stats: &[AggregationStat]) -> Option<i64> {
		AggregationStat::get_count(stats)
	}

	/// A group whose statistics are inline, as a release before the stored
	/// format writes it, is read as it is; the first write that touches it
	/// moves the statistics to the group's own key and writes the row without
	/// them, and later writes adjust that state.
	#[tokio::test]
	async fn an_inline_group_moves_its_stats_to_its_own_key_when_next_written() {
		let ds = Datastore::new("memory").await.unwrap();
		run(
			&ds,
			"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test; USE DB test; \
			 DEFINE TABLE t; \
			 DEFINE TABLE v AS SELECT g, count() AS c, math::sum(a) AS s, math::max(a) AS m \
			 FROM t GROUP BY g; \
			 CREATE t:1 SET g = 'x', a = 1; CREATE t:2 SET g = 'x', a = 2;",
		)
		.await;
		let group = only_row(&ds, "v").await.key;
		inline_group(&ds, "v", &group).await;
		let state = state_key_of(&ds, "v", &group).await;
		assert_eq!(state_stats(&ds, &state).await, None);
		let (bytes, row) = view_row(&ds, "v", &group).await;
		assert!(StoredRecord::parse(&bytes).unwrap().is_none(), "the row is in revision");
		assert!(row.metadata.is_some());
		assert_eq!(
			run(&ds, "SELECT VALUE [c, s, m] FROM ONLY v:['x']").await,
			Value::from(vec![Value::from(2i64), Value::from(3i64), Value::from(2i64)])
		);

		run(&ds, "CREATE t:3 SET g = 'x', a = 4").await;
		let stats = state_stats(&ds, &state).await.expect("the write stored the group's state");
		assert_eq!(count_of(&stats), Some(3));
		let (bytes, row) = view_row(&ds, "v", &group).await;
		assert!(StoredRecord::parse(&bytes).unwrap().is_some(), "the row is a v1 record");
		assert!(row.metadata.is_none(), "the row carries no statistics");
		assert_eq!(
			run(&ds, "SELECT VALUE [c, s, m] FROM ONLY v:['x']").await,
			Value::from(vec![Value::from(3i64), Value::from(7i64), Value::from(4i64)])
		);

		run(&ds, "DELETE t:3").await;
		assert_eq!(count_of(&state_stats(&ds, &state).await.unwrap()), Some(2));
		assert_eq!(
			run(&ds, "SELECT VALUE [c, s, m] FROM ONLY v:['x']").await,
			Value::from(vec![Value::from(2i64), Value::from(3i64), Value::from(2i64)])
		);
		run(&ds, "DELETE t:1, t:2").await;
		assert_eq!(state_stats(&ds, &state).await, None, "the last record leaves no state");
		assert_eq!(run(&ds, "SELECT VALUE id FROM v").await, Value::from(Vec::<Value>::new()));
	}

	/// A node whose cached feature level still reads `Legacy` after the store
	/// was raised writes a group new to it inline, and keeps a group that has a
	/// state in its state rather than refusing it. Writing that state reads the
	/// raised level, so the node's next write moves the inline group's
	/// statistics to its own key.
	#[tokio::test]
	async fn a_node_that_reads_the_level_as_legacy_keeps_a_stated_group_stated() {
		let ds = Datastore::new("memory").await.unwrap();
		run(
			&ds,
			"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test; USE DB test; \
			 DEFINE TABLE t; \
			 DEFINE TABLE v AS SELECT g, count() AS c FROM t GROUP BY g; \
			 CREATE t:1 SET g = 'x';",
		)
		.await;
		let x = only_row(&ds, "v").await.key;
		let x_state = state_key_of(&ds, "v", &x).await;
		assert_eq!(count_of(&state_stats(&ds, &x_state).await.unwrap()), Some(1));

		ds.feature_cell().observe(None);
		let keyed = || async {
			let txn = ds.transaction(TransactionType::Read).await.unwrap();
			let keyed = txn.keyed_view_state();
			txn.cancel().await.unwrap();
			keyed
		};
		assert!(!keyed().await, "this node reads the store as Legacy");

		run(&ds, "CREATE t:2 SET g = 'y'").await;
		let y = RecordIdKey::Array(Array(vec![Value::from("y")]));
		let y_state = state_key_of(&ds, "v", &y).await;
		assert_eq!(state_stats(&ds, &y_state).await, None, "a new group is written inline");
		let (bytes, row) = view_row(&ds, "v", &y).await;
		assert!(StoredRecord::parse(&bytes).unwrap().is_none());
		assert_eq!(
			count_of(&row.metadata.expect("the stats are inline").aggregation_stats),
			Some(1)
		);

		run(&ds, "CREATE t:3 SET g = 'x'").await;
		assert_eq!(count_of(&state_stats(&ds, &x_state).await.unwrap()), Some(2));
		assert!(view_row(&ds, "v", &x).await.1.metadata.is_none());
		assert!(keyed().await, "writing the state read the raised level");

		run(&ds, "CREATE t:4 SET g = 'y'").await;
		assert_eq!(count_of(&state_stats(&ds, &y_state).await.unwrap()), Some(2));
		assert!(view_row(&ds, "v", &y).await.1.metadata.is_none());
		assert_eq!(
			run(&ds, "SELECT VALUE c FROM v ORDER BY g").await,
			Value::from(vec![Value::from(2i64), Value::from(2i64)])
		);
	}

	/// A record written in the `revision` encoding is read as it is, beside
	/// records in the stored-value encoding, and is written in the stored-value
	/// encoding when it is next written.
	#[tokio::test]
	async fn a_revision_record_stays_as_written_until_it_is_written() {
		let ds = Datastore::new("memory").await.unwrap();
		run(
			&ds,
			"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test; USE DB test; \
			 CREATE t:1 SET a = 1; CREATE t:2 SET a = 2;",
		)
		.await;
		let key = key_of(&ds, &rid("t", 3)).await;
		let revision = revision::to_vec(&Record::new(object(&[("a", Value::from(3i64))]))).unwrap();
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		txn.set(KVKey::encode_key(&key).unwrap(), revision.clone()).await.unwrap();
		txn.commit().await.unwrap();

		assert_eq!(run(&ds, "SELECT VALUE a FROM ONLY t:3").await, Value::from(3i64));
		assert_eq!(
			run(&ds, "SELECT VALUE id FROM t WHERE a >= 2").await,
			Value::from(vec![Value::RecordId(rid("t", 2)), Value::RecordId(rid("t", 3))])
		);
		assert_eq!(stored(&ds, &rid("t", 3)).await, revision, "a read rewrites nothing");

		run(&ds, "UPDATE t:3 SET b = 4").await;
		let bytes = stored(&ds, &rid("t", 3)).await;
		let record = StoredRecord::parse(&bytes).unwrap().expect("stored-value encoded");
		assert!(record.is_keyed());
		assert_eq!(
			Record::decode_stored(&bytes, Some(&*layouts(&ds, "t").await), rid("t", 3))
				.unwrap()
				.data,
			object(&[
				("a", Value::from(3i64)),
				("b", Value::from(4i64)),
				("id", Value::RecordId(rid("t", 3))),
			])
		);
	}
}
