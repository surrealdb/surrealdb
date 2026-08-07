//! The change feed's write path, end to end.
//!
//! Buffering a record change and flushing it at commit both happen inside the
//! transaction, one crate down. What these cover is the property that spans
//! both crates: a change buffered here is readable through [`super::read`]
//! afterwards, in commit order.

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::time::Duration;

	use surrealdb_strand::Strand;

	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
	use crate::catalog::{
		DatabaseDefinition, DatabaseId, FromStored, NamespaceDefinition, NamespaceId, Record,
		StoredTableDefinition, TableDefinition, TableId,
	};
	use crate::cf::ChangeSet;
	use crate::expr::changefeed::ChangeFeed;
	use crate::expr::statements::show::ShowSince;
	use crate::kvs::TransactionType::*;
	use crate::kvs::{Datastore, Transaction};
	use crate::val::{RecordId, RecordIdKey, TableName, Value};

	const DONT_STORE_PREVIOUS: bool = false;

	const NS: &str = "myns";
	const DB: &str = "mydb";
	const TB: &str = "mytb";

	#[tokio::test]
	async fn changefeed_read_write() {
		let ds = init(false).await;

		//
		// Write records to the table.
		//

		let tx = ds.transaction(Write).await.unwrap();
		let tb_name = TableName::new(TB.to_owned());
		let tb = tx.expect_tb_by_name(NS, DB, &tb_name).await.unwrap();
		tx.commit().await.unwrap();

		let tx1 = ds.transaction(Write).await.unwrap();
		let record_a = RecordId {
			table: tb_name.clone(),
			key: RecordIdKey::String(Strand::new_static("A")),
		};
		let value_a: Value = "a".into();
		let previous = Value::None;
		tx1.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb_name,
			&record_a,
			Record::new(previous.clone()).into_read_only(),
			Record::new(value_a).into_read_only(),
			DONT_STORE_PREVIOUS,
		);
		tx1.commit().await.unwrap();

		let tx2 = ds.transaction(Write).await.unwrap();
		let record_c = RecordId {
			table: tb_name.clone(),
			key: RecordIdKey::String(Strand::new_static("C")),
		};
		let value_c: Value = "c".into();
		tx2.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb_name,
			&record_c,
			Record::new(previous.clone()).into_read_only(),
			Record::new(value_c).into_read_only(),
			DONT_STORE_PREVIOUS,
		);
		tx2.commit().await.unwrap();

		let tx3 = ds.transaction(Write).await.unwrap();
		let record_b = RecordId {
			table: tb_name.clone(),
			key: RecordIdKey::String(Strand::new_static("B")),
		};
		let value_b: Value = "b".into();
		tx3.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb_name,
			&record_b,
			Record::new(previous.clone()).into_read_only(),
			Record::new(value_b).into_read_only(),
			DONT_STORE_PREVIOUS,
		);
		let record_c2 = RecordId {
			table: tb_name.clone(),
			key: RecordIdKey::String(Strand::new_static("C")),
		};
		let value_c2: Value = "c2".into();
		tx3.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb_name,
			&record_c2,
			Record::new(previous.clone()).into_read_only(),
			Record::new(value_c2).into_read_only(),
			DONT_STORE_PREVIOUS,
		);
		tx3.commit().await.unwrap();

		// Note that we committed tx1, tx2, and tx3 in this order so far.
		// Therefore, the change feeds should give us
		// the mutations in the commit order, which is tx1, tx3, then tx2.

		let start: u64 = 0;

		let tx4 = ds.transaction(Write).await.unwrap();
		let r = crate::cf::read(
			&tx4,
			tb.namespace_id,
			tb.database_id,
			Some(&tb_name),
			ShowSince::Versionstamp(start),
			Some(10),
		)
		.await
		.unwrap();
		tx4.commit().await.unwrap();

		// Verify we got 3 changesets
		assert_eq!(r.len(), 3);

		// Verify the contents of each changeset
		assert_eq!(r[0].1.0.len(), 1); // First changeset has 1 table mutation
		assert_eq!(r[0].1.0[0].1.len(), 1); // With 1 record mutation

		assert_eq!(r[1].1.0.len(), 1); // Second changeset has 1 table mutation
		assert_eq!(r[1].1.0[0].1.len(), 1); // With 1 record mutation

		assert_eq!(r[2].1.0.len(), 1); // Third changeset has 1 table mutation
		assert_eq!(r[2].1.0[0].1.len(), 2); // With 2 record mutations

		// Verify versionstamps are monotonically increasing
		assert!(r[0].0 < r[1].0, "Versionstamps should be monotonically increasing");
		assert!(r[1].0 < r[2].0, "Versionstamps should be monotonically increasing");
	}

	#[test_log::test(tokio::test)]
	async fn scan_picks_up_from_offset() {
		// Given we have 2 entries in change feeds
		let ds = init(false).await;

		let tx = ds.transaction(Write).await.unwrap();
		let tb_name = TableName::new(TB.to_owned());
		let tb = tx.expect_tb_by_name(NS, DB, &tb_name).await.unwrap();
		tx.commit().await.unwrap();

		// Record first change with timestamp ~5
		let _id1 = record_change_feed_entry(
			ds.transaction(Write).await.unwrap(),
			&tb,
			"First".to_string(),
		)
		.await;

		// Record second change with timestamp ~10 (or later)
		let _id2 = record_change_feed_entry(
			ds.transaction(Write).await.unwrap(),
			&tb,
			"Second".to_string(),
		)
		.await;

		// When we scan from timestamp 0 we should see both changes
		let r = change_feed_ts(ds.transaction(Write).await.unwrap(), &tb, 0).await;
		assert_eq!(r.len(), 2);

		// When we scan from a timestamp after the first change, we should only see the second
		let r = change_feed_ts(ds.transaction(Write).await.unwrap(), &tb, r[0].0 as u64 + 1).await;
		assert_eq!(r.len(), 1);
	}

	async fn change_feed_ts(tx: Transaction, tb: &TableDefinition, ts: u64) -> Vec<ChangeSet> {
		let r = crate::cf::read(
			&tx,
			tb.namespace_id,
			tb.database_id,
			Some(&tb.name.clone()),
			ShowSince::Versionstamp(ts),
			Some(10),
		)
		.await
		.unwrap();
		tx.cancel().await.unwrap();
		r
	}

	async fn record_change_feed_entry(
		tx: Transaction,
		tb: &TableDefinition,
		id: String,
	) -> RecordId {
		let tb_name = tb.name.clone();
		let record_id = RecordId {
			table: tb_name.clone(),
			key: RecordIdKey::String(id.into()),
		};
		let value_a: Value = "a".into();
		let previous = Value::None;
		tx.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb_name,
			&record_id,
			Record::new(previous).into_read_only(),
			Record::new(value_a).into_read_only(),
			DONT_STORE_PREVIOUS,
		);
		tx.commit().await.unwrap();
		record_id
	}

	async fn init(store_diff: bool) -> Arc<Datastore> {
		let namespace_id = NamespaceId(1);
		let database_id = DatabaseId(2);
		let table_id = TableId(3);
		let ns_def = NamespaceDefinition {
			namespace_id,
			name: NS.into(),
			comment: None,
		};
		let db_def = DatabaseDefinition {
			namespace_id,
			database_id,
			name: DB.into(),
			changefeed: Some(ChangeFeed {
				expiry: Duration::from_secs(10),
				store_diff,
			}),
			comment: None,
			strict: false,
		};
		let mut tb_def = TableDefinition::from_stored(&StoredTableDefinition::new(
			namespace_id,
			database_id,
			table_id,
			TableName::new(TB.to_owned()),
		))
		.unwrap();
		tb_def.changefeed = Some(ChangeFeed {
			expiry: Duration::from_secs(10 * 60),
			store_diff,
		});

		let ds = Datastore::new("memory").await.unwrap();

		//
		// Create the ns, db, and tb to let the GC and the timestamp-to-versionstamp
		// conversion work.
		//

		let tx = ds.transaction(Write).await.unwrap();

		tx.put_ns(ns_def).await.unwrap();
		tx.put_db(NS, db_def).await.unwrap();
		tx.put_tb(NS, DB, &tb_def).await.unwrap();

		tx.commit().await.unwrap();
		ds
	}
}
