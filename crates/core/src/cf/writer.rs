use anyhow::Result;
use dashmap::DashMap;

use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::cf::{TableMutation, TableMutations};
use crate::doc::CursorRecord;
use crate::kvs::KVValue;
use crate::val::RecordId;

// PreparedWrite is a tuple of (namespace, database, table, serialized table mutations).
// The timestamp will be provided at commit time via Transaction::current_timestamp().
type PreparedWrite = (NamespaceId, DatabaseId, String, crate::kvs::Val);

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct ChangeKey {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: String,
}

/// Writer is a helper for writing table mutations to a transaction.
pub struct Writer {
	/// The buffer of table mutations to be written to the database.
	buffer: DashMap<ChangeKey, TableMutations>,
}

// Writer is a helper for writing table mutations to a transaction.
impl Writer {
	/// Create a new changefeed writer
	pub(crate) fn new() -> Self {
		Self {
			buffer: DashMap::new(),
		}
	}

	/// Record a table definition modification
	pub(crate) fn changefeed_buffer_table_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		dt: &TableDefinition,
	) {
		// Get or create the entry for the change key
		let mut entry = self
			.buffer
			.entry(ChangeKey {
				ns,
				db,
				tb: tb.to_string(),
			})
			.or_insert_with(|| TableMutations::new(tb.to_string()));
		// Push the define table mutation to the entry
		entry.1.push(TableMutation::Def(dt.to_owned()));
	}

	/// Record a record modification or deletion
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn changefeed_buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: RecordId,
		previous: CursorRecord,
		current: CursorRecord,
		store_difference: bool,
	) {
		// Get or create the entry for the change key
		let mut entry = self
			.buffer
			.entry(ChangeKey {
				ns,
				db,
				tb: tb.to_string(),
			})
			.or_insert_with(|| TableMutations::new(tb.to_string()));
		// Check if this is a delete operation
		if current.as_ref().is_nullish() {
			// Push the delete mutation to the entry
			entry.1.push(match store_difference {
				true => TableMutation::DelWithOriginal(id, previous.into_owned()),
				false => TableMutation::Del(id),
			});
		} else {
			// Push the set mutation to the entry
			entry.1.push(match store_difference {
				true => {
					if previous.as_ref().is_none() {
						TableMutation::Set(id, current.into_owned())
					} else {
						// We intentionally record the patches in reverse (current -> previous)
						// because we cannot otherwise resolve operations such as "replace" and
						// "remove".
						let patches_to_create_previous = current.as_ref().diff(previous.as_ref());
						TableMutation::SetWithDiff(
							id,
							current.into_owned(),
							patches_to_create_previous,
						)
					}
				}
				false => TableMutation::Set(id, current.into_owned()),
			});
		}
	}

	// get returns all the mutations buffered for this transaction.
	// The timestamp will be provided at commit time.
	pub(crate) fn changes(&self) -> Result<Vec<PreparedWrite>> {
		// Create a new change result set
		let mut res = Vec::with_capacity(self.buffer.len());
		// Iterate over the buffered mutations
		for entry in self.buffer.iter() {
			// Deconstruct the change key
			let ChangeKey {
				ns,
				db,
				tb,
			} = entry.key();
			// Encode the value
			let value = entry.value().kv_encode_value()?;
			// Push the prepared write to the result (timestamp will be added at commit time)
			res.push((*ns, *db, tb.clone(), value))
		}
		// Return the prepared writes
		Ok(res)
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use crate::catalog::providers::{
		CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider,
	};
	use crate::catalog::{
		DatabaseDefinition, DatabaseId, NamespaceDefinition, NamespaceId, TableDefinition, TableId,
	};
	use crate::cf::ChangeSet;
	use crate::expr::changefeed::ChangeFeed;
	use crate::expr::statements::show::ShowSince;
	use crate::kvs::LockType::*;
	use crate::kvs::TransactionType::*;
	use crate::kvs::{Datastore, Transaction};
	use crate::val::{RecordId, RecordIdKey, Value};

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

		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		let tb = tx.ensure_ns_db_tb(None, NS, DB, TB).await.unwrap();
		tx.commit().await.unwrap();

		let tx1 = ds.transaction(Write, Optimistic).await.unwrap();
		let record_a = RecordId {
			table: TB.to_owned(),
			key: RecordIdKey::String("A".to_owned()),
		};
		let value_a: Value = "a".into();
		let previous = Value::None;
		tx1.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb.name,
			&record_a,
			previous.clone().into(),
			value_a.into(),
			DONT_STORE_PREVIOUS,
		);
		tx1.complete_changes().await.unwrap();
		tx1.commit().await.unwrap();

		let tx2 = ds.transaction(Write, Optimistic).await.unwrap();
		let record_c = RecordId {
			table: TB.to_owned(),
			key: RecordIdKey::String("C".to_owned()),
		};
		let value_c: Value = "c".into();
		tx2.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb.name,
			&record_c,
			previous.clone().into(),
			value_c.into(),
			DONT_STORE_PREVIOUS,
		);
		tx2.complete_changes().await.unwrap();
		tx2.commit().await.unwrap();

		let tx3 = ds.transaction(Write, Optimistic).await.unwrap();
		let record_b = RecordId {
			table: TB.to_owned(),
			key: RecordIdKey::String("B".to_owned()),
		};
		let value_b: Value = "b".into();
		tx3.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb.name,
			&record_b,
			previous.clone().into(),
			value_b.into(),
			DONT_STORE_PREVIOUS,
		);
		let record_c2 = RecordId {
			table: TB.to_owned(),
			key: RecordIdKey::String("C".to_owned()),
		};
		let value_c2: Value = "c2".into();
		tx3.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb.name,
			&record_c2,
			previous.clone().into(),
			value_c2.into(),
			DONT_STORE_PREVIOUS,
		);
		tx3.complete_changes().await.unwrap();
		tx3.commit().await.unwrap();

		// Note that we committed tx1, tx2, and tx3 in this order so far.
		// Therefore, the change feeds should give us
		// the mutations in the commit order, which is tx1, tx3, then tx2.

		let start: u64 = 0;

		let tx4 = ds.transaction(Write, Optimistic).await.unwrap();
		let r = crate::cf::read(
			&tx4,
			tb.namespace_id,
			tb.database_id,
			Some(&tb.name),
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

		// Store the last timestamp for cleanup
		let last_ts = r[2].0.into_u64_lossy();

		let tx5 = ds.transaction(Write, Optimistic).await.unwrap();
		// gc_all needs to be committed before we can read the changes
		crate::cf::gc_range(&tx5, tb.namespace_id, tb.database_id, last_ts).await.unwrap();
		// We now commit tx5, which should persist the gc_all results
		tx5.commit().await.unwrap();

		// Now we should see the gc_all results
		let tx6 = ds.transaction(Write, Optimistic).await.unwrap();
		let r = crate::cf::read(
			&tx6,
			tb.namespace_id,
			tb.database_id,
			Some(&tb.name),
			ShowSince::Versionstamp(start),
			Some(10),
		)
		.await
		.unwrap();
		tx6.commit().await.unwrap();

		// After GC, we should only see the last changeset
		assert_eq!(r.len(), 1);
		assert_eq!(r[0].1.0.len(), 1);
		assert_eq!(r[0].1.0[0].1.len(), 2); // With 2 record mutations
	}

	#[test_log::test(tokio::test)]
	async fn scan_picks_up_from_offset() {
		// Given we have 2 entries in change feeds
		let ds = init(false).await;

		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		let tb = tx.ensure_ns_db_tb(None, NS, DB, TB).await.unwrap();
		tx.commit().await.unwrap();

		// Record first change with timestamp ~5
		let _id1 = record_change_feed_entry(
			ds.transaction(Write, Optimistic).await.unwrap(),
			&tb,
			"First".to_string(),
		)
		.await;

		// Record second change with timestamp ~10 (or later)
		let _id2 = record_change_feed_entry(
			ds.transaction(Write, Optimistic).await.unwrap(),
			&tb,
			"Second".to_string(),
		)
		.await;

		// When we scan from timestamp 0 we should see both changes
		let r = change_feed_ts(ds.transaction(Write, Optimistic).await.unwrap(), &tb, 0).await;
		assert_eq!(r.len(), 2);

		// When we scan from a timestamp after the first change, we should only see the second
		let r = change_feed_ts(
			ds.transaction(Write, Optimistic).await.unwrap(),
			&tb,
			r[0].0.into_u64_lossy() + 1,
		)
		.await;
		assert_eq!(r.len(), 1);
	}

	async fn change_feed_ts(tx: Transaction, tb: &TableDefinition, ts: u64) -> Vec<ChangeSet> {
		let r = crate::cf::read(
			&tx,
			tb.namespace_id,
			tb.database_id,
			Some(&tb.name),
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
		let record_id = RecordId {
			table: tb.name.clone(),
			key: RecordIdKey::String(id),
		};
		let value_a: Value = "a".into();
		let previous = Value::None.into();
		tx.changefeed_buffer_record_change(
			tb.namespace_id,
			tb.database_id,
			&tb.name,
			&record_id,
			previous,
			value_a.into(),
			DONT_STORE_PREVIOUS,
		);
		tx.complete_changes().await.unwrap();
		tx.commit().await.unwrap();
		record_id
	}

	async fn init(store_diff: bool) -> Datastore {
		let namespace_id = NamespaceId(1);
		let database_id = DatabaseId(2);
		let table_id = TableId(3);
		let ns_def = NamespaceDefinition {
			namespace_id,
			name: NS.to_string(),
			comment: None,
		};
		let db_def = DatabaseDefinition {
			namespace_id,
			database_id,
			name: DB.to_string(),
			changefeed: Some(ChangeFeed {
				expiry: Duration::from_secs(10),
				store_diff,
			}),
			comment: None,
			strict: false,
		};
		let mut tb_def = TableDefinition::new(namespace_id, database_id, table_id, TB.to_string());
		tb_def.changefeed = Some(ChangeFeed {
			expiry: Duration::from_secs(10 * 60),
			store_diff,
		});

		let ds = Datastore::new("memory").await.unwrap();

		//
		// Create the ns, db, and tb to let the GC and the timestamp-to-versionstamp
		// conversion work.
		//

		let tx = ds.transaction(Write, Optimistic).await.unwrap();

		tx.put_ns(ns_def).await.unwrap();
		tx.put_db(NS, db_def).await.unwrap();
		tx.put_tb(NS, DB, &tb_def).await.unwrap();

		tx.commit().await.unwrap();
		ds
	}
}
