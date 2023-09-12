use crate::key::table::nt;

#[tokio::test]
#[serial]
#[cfg(feature = "kv-mem")]
async fn can_scan_notifications() {
	let namespace = "testns";
	let database = "testdb";
	let table = "testtb";
	let notifications: Vec<(nt::Nt, Notification)> = vec![
		create_nt_tuple(
			namespace,
			database,
			table,
			sql::uuid::Uuid::try_from("69b5840c-d05f-4b58-8a64-606ad12689c1").unwrap(),
		),
		create_nt_tuple(
			namespace,
			database,
			table,
			sql::uuid::Uuid::try_from("cda21a4f-3493-4934-bb87-b81070dedba0").unwrap(),
		),
	];

	let clock_override =
		Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let ds = Datastore::new_full("memory", Some(clock_override))
		.await
		.unwrap()
		.with_node_id(crate::sql::Uuid::new());

	// Create all the data
	let mut tx = ds.transaction(true, false).await.unwrap();
	for pair in notifications.clone() {
		tx.putc_tbnt(
			pair.0.ns,
			pair.0.db,
			pair.0.tb,
			sql::Uuid(pair.0.lq),
			pair.0.ts,
			sql::Uuid(pair.0.id),
			pair.1.clone(),
			None,
		)
		.await
		.unwrap();
	}
	tx.commit().await.unwrap();

	// Read all the data
	for pair in notifications {
		let mut tx = ds.transaction(true, false).await.unwrap();
		let scanned_notifications = tx
			.scan_tbnt(pair.0.ns, pair.0.db, pair.0.tb, sql::Uuid(pair.0.id), 1000)
			.await
			.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(scanned_notifications, vec![pair.1]);
	}
}

fn create_nt_tuple<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	live_id: sql::uuid::Uuid,
) -> (nt::Nt<'a>, Notification) {
	let node_id = sql::uuid::Uuid::new_v4();
	let notification_id = sql::uuid::Uuid::new_v4();
	let result = Value::Strand(Strand::from("teststrand result"));
	let timestamp = Timestamp {
		value: 123,
	};
	let not = Notification {
		live_id: live_id.clone(),
		node_id: node_id.clone(),
		notification_id: notification_id.clone(),
		action: Action::Create,
		result,
		timestamp: timestamp.clone(),
	};
	let nt = nt::Nt::new(ns, db, tb, live_id, timestamp, notification_id);
	return (nt, not);
}
