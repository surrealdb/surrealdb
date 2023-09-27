use crate::key::table::nt;
use crate::sql::uuid::Uuid as sqlUuid;

#[tokio::test]
#[serial]
#[cfg(feature = "kv-mem")]
async fn can_scan_notifications() {
	let namespace = "testns";
	let database = "testdb";
	let table = "testtb";
	let node_id = sql::uuid::Uuid::try_from("10e59cba-98bd-42b1-b60d-6ab32d989b65").unwrap();
	let notifications: Vec<(nt::Nt, Notification)> = vec![
		create_nt_tuple(
			namespace,
			database,
			table,
			node_id.clone(),
			sqlUuid::try_from("69b5840c-d05f-4b58-8a64-606ad12689c1").unwrap(),
			sqlUuid::try_from("74c37cb1-c329-4ec1-acd9-26d312e6f259").unwrap(),
		),
		create_nt_tuple(
			namespace,
			database,
			table,
			node_id.clone(),
			sqlUuid::try_from("cda21a4f-3493-4934-bb87-b81070dedba0").unwrap(),
			sqlUuid::try_from("2bd9c065-4301-4cdd-9df7-4ce1a50fd08b").unwrap(),
		),
	];

	let clock_override =
		Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let ds = Datastore::new_full("memory", Some(clock_override))
		.await
		.unwrap()
		.with_node_id(node_id.clone());

	// Create all the data
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	for pair in notifications.clone() {
		tx.putc_tbnt(
			pair.0.ns,
			pair.0.db,
			pair.0.tb,
			sql::Uuid(pair.0.lq),
			pair.0.ts,
			sql::Uuid(pair.0.nt),
			pair.1.clone(),
			None,
		)
		.await
		.unwrap();
	}
	tx.commit().await.unwrap();

	// Read all the data
	for pair in notifications {
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let scanned_notifications =
			tx.scan_tbnt(pair.0.ns, pair.0.db, pair.0.tb, sqlUuid(pair.0.lq), 1000).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(scanned_notifications, vec![pair.1]);
	}
}

fn create_nt_tuple<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	node_id: sqlUuid,
	live_id: sqlUuid,
	not_id: sqlUuid,
) -> (nt::Nt<'a>, Notification) {
	let result = Value::Strand(Strand::from("teststrand result"));
	let timestamp = Timestamp {
		value: 123,
	};
	let not = Notification {
		live_id: live_id.clone(),
		node_id: node_id.clone(),
		notification_id: not_id.clone(),
		action: Action::Create,
		result,
		timestamp: timestamp.clone(),
	};
	let nt = nt::Nt::new(ns, db, tb, live_id, timestamp, not_id);
	return (nt, not);
}
