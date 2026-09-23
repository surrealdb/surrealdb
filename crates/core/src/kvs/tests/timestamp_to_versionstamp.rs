use super::CreateDs;
use crate::kvs::KeyEncode;
use std::sync::Arc;
use uuid::Uuid;

use crate::{
	dbs::{node::Timestamp, Session},
	kvs::{
		clock::{FakeClock, SizedClock},
		LockType::*,
		TransactionType::*,
	},
	vs::VersionStamp,
};

// Timestamp to versionstamp tests
// This translation mechanism is currently used by the garbage collector to determine which change feed entries to delete.
//
// FAQ:
// Q: What’s the difference between database TS and database VS?
// A: Timestamps are basically seconds since the unix epoch.
//    Versionstamps can be anything that is provided by our TSO.
// Q: Why do we need to translate timestamps to versionstamps?
// A: The garbage collector needs to know which change feed entries to delete.
//    However our SQL syntax `DEFINE DATABASE foo CHANGEFEED 1h` let the user specify the expiration in a duration, not a delta in the versionstamp.
//    We need to translate the timestamp to the versionstamp due to that; `now - 1h` to a key suffixed by the versionstamp.
pub async fn timestamp_to_versionstamp(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("A905CA25-56ED-49FB-B759-696AEA87C342").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Give the current versionstamp a timestamp of 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(0, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();
	// Get the versionstamp for timestamp 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs1 = tx.get_versionstamp_from_timestamp(0, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();
	// Give the current versionstamp a timestamp of 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(1, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();
	// Get the versionstamp for timestamp 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs2 = tx.get_versionstamp_from_timestamp(1, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();
	// Give the current versionstamp a timestamp of 2
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(2, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();
	// Get the versionstamp for timestamp 2
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs3 = tx.get_versionstamp_from_timestamp(2, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();
	assert!(vs1 < vs2);
	assert!(vs2 < vs3);
}

pub async fn writing_ts_again_results_in_following_ts(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("A905CA25-56ED-49FB-B759-696AEA87C342").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;

	// Declare ns/db. A `ts` key is only maintained for a database which has a
	// changefeed, so the database defines one for the tick below to advance.
	ds.execute(
		"USE NS myns; USE DB mydb; DEFINE DATABASE mydb CHANGEFEED 1h; CREATE record",
		&Session::owner(),
		None,
	)
	.await
	.unwrap();

	// Give the current versionstamp a timestamp of 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(0, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();

	// Get the versionstamp for timestamp 0
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs1 = tx.get_versionstamp_from_timestamp(0, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();

	// Give the current versionstamp a timestamp of 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(1, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();

	// Get the versionstamp for timestamp 1
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let vs2 = tx.get_versionstamp_from_timestamp(1, "myns", "mydb").await.unwrap().unwrap();
	tx.commit().await.unwrap();

	assert!(vs1 < vs2);

	// Scan range
	let start = crate::key::database::ts::new("myns", "mydb", 0).encode_owned().unwrap();
	let end = crate::key::database::ts::new("myns", "mydb", u64::MAX).encode_owned().unwrap();
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let scanned = tx.scan(start.clone()..end.clone(), u32::MAX, None).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(scanned.len(), 2);
	assert_eq!(scanned[0].0, crate::key::database::ts::new("myns", "mydb", 0).encode().unwrap());
	assert_eq!(scanned[1].0, crate::key::database::ts::new("myns", "mydb", 1).encode().unwrap());

	// Repeating tick
	ds.changefeed_process_at(1).await.unwrap();

	// Validate
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let scanned = tx.scan(start..end, u32::MAX, None).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(scanned.len(), 3);
	assert_eq!(scanned[0].0, crate::key::database::ts::new("myns", "mydb", 0).encode().unwrap());
	assert_eq!(scanned[1].0, crate::key::database::ts::new("myns", "mydb", 1).encode().unwrap());
	assert_eq!(scanned[2].0, crate::key::database::ts::new("myns", "mydb", 2).encode().unwrap());
}

// A backend without a reverse scan walks the `ts` range in batches of
// `NORMAL_FETCH_SIZE`. A range whose size is an exact multiple of that ends in
// an empty batch, and the walk must still report the last key it saw.
pub async fn the_latest_entry_survives_an_exact_batch_boundary(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("A905CA25-56ED-49FB-B759-696AEA87C342").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Fill the range with exactly one batch worth of entries
	let count = *crate::cnf::NORMAL_FETCH_SIZE as u64;
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	for ts in 0..count {
		let key = crate::key::database::ts::new("myns", "mydb", ts);
		tx.replace(key, VersionStamp::from_u64(ts + 1).as_bytes()).await.unwrap();
	}
	tx.commit().await.unwrap();
	// The newest entry at or before the end of the range is the last one written
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	let found = tx.get_timestamp_entry(count, "myns", "mydb").await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(found, Some((count - 1, VersionStamp::from_u64(count))));
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

		#[tokio::test]
		#[serial_test::serial]
		async fn the_latest_entry_survives_an_exact_batch_boundary() {
			super::timestamp_to_versionstamp::the_latest_entry_survives_an_exact_batch_boundary(
				$new_ds,
			)
			.await;
		}
	};
}
pub(crate) use define_tests;
