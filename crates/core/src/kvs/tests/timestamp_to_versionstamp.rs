use std::sync::Arc;

use uuid::Uuid;

use super::CreateDs;
use crate::dbs::Session;
use crate::dbs::node::Timestamp;
use crate::kvs::KVKey;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::kvs::clock::{FakeClock, SizedClock};

// Timestamp to versionstamp tests
// This translation mechanism is currently used by the garbage collector to
// determine which change feed entries to delete.
//
// FAQ:
// Q: What’s the difference between database TS and database VS?
// A: Timestamps are basically seconds since the unix epoch.
//    Versionstamps can be anything that is provided by our TSO.
// Q: Why do we need to translate timestamps to versionstamps?
// A: The garbage collector needs to know which change feed entries to delete.
//    However our SQL syntax `DEFINE DATABASE foo CHANGEFEED 1h` let the user
// specify the expiration in a duration, not a delta in the versionstamp.
//    We need to translate the timestamp to the versionstamp due to that; `now -
// 1h` to a key suffixed by the versionstamp.
pub async fn timestamp_to_versionstamp(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("A905CA25-56ED-49FB-B759-696AEA87C342").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Give the current versionstamp a timestamp of 0
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	let db = tx.ensure_ns_db("myns", "mydb", false).await.unwrap();

	let mut tr = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tr.set_timestamp_for_versionstamp(0, db.namespace_id, db.database_id).await.unwrap();
	tr.commit().await.unwrap();
	// Get the versionstamp for timestamp 0
	let mut tr = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs1 = tr
		.get_versionstamp_from_timestamp(0, db.namespace_id, db.database_id)
		.await
		.unwrap()
		.unwrap();
	tr.commit().await.unwrap();
	// Give the current versionstamp a timestamp of 1
	let mut tr = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tr.set_timestamp_for_versionstamp(1, db.namespace_id, db.database_id).await.unwrap();
	tr.commit().await.unwrap();
	// Get the versionstamp for timestamp 1
	let mut tr = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs2 = tr
		.get_versionstamp_from_timestamp(1, db.namespace_id, db.database_id)
		.await
		.unwrap()
		.unwrap();
	tr.commit().await.unwrap();
	// Give the current versionstamp a timestamp of 2
	let mut tr = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tr.set_timestamp_for_versionstamp(2, db.namespace_id, db.database_id).await.unwrap();
	tr.commit().await.unwrap();
	// Get the versionstamp for timestamp 2
	let mut tr = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs3 = tr
		.get_versionstamp_from_timestamp(2, db.namespace_id, db.database_id)
		.await
		.unwrap()
		.unwrap();
	tr.commit().await.unwrap();
	assert!(vs1 < vs2);
	assert!(vs2 < vs3);
}

pub async fn writing_ts_again_results_in_following_ts(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("A905CA25-56ED-49FB-B759-696AEA87C342").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;

	// Declare ns/db
	ds.execute("USE NS myns; USE DB mydb; CREATE record", &Session::owner(), None).await.unwrap();

	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	let db = tx.get_or_add_db("myns", "mydb", false).await.unwrap();

	// Give the current versionstamp a timestamp of 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(0, db.namespace_id, db.database_id).await.unwrap();
	tx.commit().await.unwrap();

	// Get the versionstamp for timestamp 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs1 = tx
		.get_versionstamp_from_timestamp(0, db.namespace_id, db.database_id)
		.await
		.unwrap()
		.unwrap();
	tx.commit().await.unwrap();

	// Give the current versionstamp a timestamp of 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(1, db.namespace_id, db.database_id).await.unwrap();
	tx.commit().await.unwrap();

	// Get the versionstamp for timestamp 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs2 = tx
		.get_versionstamp_from_timestamp(1, db.namespace_id, db.database_id)
		.await
		.unwrap()
		.unwrap();
	tx.commit().await.unwrap();

	assert!(vs1 < vs2);

	// Scan range
	let start =
		crate::key::database::ts::new(db.namespace_id, db.database_id, 0).encode_key().unwrap();
	let end = crate::key::database::ts::new(db.namespace_id, db.database_id, u64::MAX)
		.encode_key()
		.unwrap();
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let scanned = tx.scan(start.clone()..end.clone(), u32::MAX, None).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(scanned.len(), 2);
	assert_eq!(
		scanned[0].0,
		crate::key::database::ts::new(db.namespace_id, db.database_id, 0).encode_key().unwrap()
	);
	assert_eq!(
		scanned[1].0,
		crate::key::database::ts::new(db.namespace_id, db.database_id, 1).encode_key().unwrap()
	);

	// Repeating tick
	ds.changefeed_process_at(None, 1).await.unwrap();

	// Validate
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let scanned = tx.scan(start..end, u32::MAX, None).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(scanned.len(), 3);
	assert_eq!(
		scanned[0].0,
		crate::key::database::ts::new(db.namespace_id, db.database_id, 0).encode_key().unwrap()
	);
	assert_eq!(
		scanned[1].0,
		crate::key::database::ts::new(db.namespace_id, db.database_id, 1).encode_key().unwrap()
	);
	assert_eq!(
		scanned[2].0,
		crate::key::database::ts::new(db.namespace_id, db.database_id, 2).encode_key().unwrap()
	);
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn timestamp_to_versionstamp() {
			super::timestamp_to_versionstamp::timestamp_to_versionstamp($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn writing_ts_again_results_in_following_ts() {
			super::timestamp_to_versionstamp::writing_ts_again_results_in_following_ts($new_ds)
				.await;
		}
	};
}
pub(crate) use define_tests;
