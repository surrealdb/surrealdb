use crate::key::table::nt;
use crate::key::table::nt::Nt;
use crate::sql::uuid::Uuid as sqlUuid;

#[tokio::test]
#[serial]
async fn can_scan_notifications() {
	let namespace = "testns";
	let database = "testdb";
	let table = "testtb";
	let node_id = sql::uuid::Uuid::try_from("10e59cba-98bd-42b1-b60d-6ab32d989b65").unwrap();
	let notifications: Vec<(Nt, KvsNotification)> = vec![
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
	let ds = Datastore::new_full("memory", Some(clock_override)).await.unwrap();

	// Create all the data
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	for pair in notifications.clone() {
		let key = Nt::new(
			pair.0.ns,
			pair.0.db,
			pair.0.tb,
			sql::Uuid(pair.0.lq),
			pair.0.ts,
			sql::Uuid(pair.0.nt),
		);
		tx.putc_tbnt(key, pair.1.clone(), None).await.unwrap();
	}
	tx.commit().await.unwrap();

	// Read all the data
	for pair in notifications {
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let scanned_notifications =
			tx.scan_tbnt(pair.0.ns, pair.0.db, pair.0.tb, sqlUuid(pair.0.lq), 1000).await.unwrap();
		let scanned_no_limit = tx
			.scan_tbnt(pair.0.ns, pair.0.db, pair.0.tb, sqlUuid(pair.0.lq), NO_LIMIT)
			.await
			.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(scanned_notifications, vec![pair.1]);
		assert_eq!(scanned_notifications, scanned_no_limit);
	}
}

#[tokio::test]
#[serial]
async fn can_delete_notifications() {
	let node_id = sql::uuid::Uuid::try_from("fed046f3-05a2-4dc9-8ce0-7fa92ceb7ec2").unwrap();
	let clock_override =
		Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let ds = Datastore::new_full("memory", Some(clock_override)).await.unwrap();
	let ns = "testns";
	let db = "testdb";
	let tb = "testtb";
	let ts = Timestamp {
		value: 123456,
	};
	let not_id = sql::uuid::Uuid::try_from("7719f939-e901-416d-89ff-5e6d97e2a49d").unwrap();
	let live_id = sql::uuid::Uuid::try_from("cfaea67b-6cca-436e-8bf0-819c2277100e").unwrap();
	let not = KvsNotification {
		live_id: live_id.clone(),
		node_id: node_id.clone(),
		notification_id: not_id.clone(),
		action: KvsAction::Create,
		result: Value::Strand(Strand("this would be an object".to_string())),
		timestamp: ts.clone(),
	};
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	let key = Nt::new(ns, db, tb, live_id.clone(), ts.clone(), not_id.clone());
	tx.putc_tbnt(key, not.clone(), None).await.unwrap();
	tx.commit().await.unwrap();

	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	let res = tx.scan_tbnt(ns, db, tb, live_id.clone(), 0).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(res, vec![not.clone()]);

	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	let key_nt = nt::Nt::new(ns, db, tb, live_id.clone(), ts.clone(), not_id.clone());
	tx.del(key_nt).await.unwrap();
	tx.commit().await.unwrap();

	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	let res = tx.scan_tbnt(ns, db, tb, live_id.clone(), 0).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(res, vec![]);
}

#[tokio::test]
#[serial]
async fn putc_tbnt_sanity_checks_key_with_value() {
	let node_id = sql::uuid::Uuid::try_from("5225d016-efad-40dc-8385-4340606894fc").unwrap();
	let clock_override =
		Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let ds = Datastore::new_full("memory", Some(clock_override)).await.unwrap();

	// Test truths
	let ns = "testns";
	let db = "testdb";
	let tb = "testtb";
	let live_id = sql::uuid::Uuid::try_from("04c7197a-a4af-4b1a-a663-8affe9a2b7b1").unwrap();
	let ts = Timestamp {
		value: 0x123456,
	};
	let not_id = sql::uuid::Uuid::try_from("bb4be42a-e04c-4245-8cee-55263bd19eeb").unwrap();

	// Test erroneous data
	let not_bad_ts = KvsNotification {
		live_id: live_id.clone(),
		node_id: node_id.clone(),
		notification_id: not_id.clone(),
		action: KvsAction::Create,
		result: Value::None,
		timestamp: Timestamp {
			value: 0x0bad,
		},
	};
	let not_bad_lq = KvsNotification {
		live_id: Default::default(),
		node_id: node_id.clone(),
		notification_id: not_id.clone(),
		action: KvsAction::Create,
		result: Value::None,
		timestamp: ts.clone(),
	};
	let not_bad_nt = KvsNotification {
		live_id: live_id.clone(),
		node_id: node_id.clone(),
		notification_id: Default::default(),
		action: KvsAction::Create,
		result: Value::None,
		timestamp: ts.clone(),
	};

	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	let key = Nt::new(ns, db, tb, live_id.clone(), ts.clone(), not_id.clone());
	let res = tx.putc_tbnt(key, not_bad_ts, None).await;
	assert!(res.is_err());
	let key = Nt::new(ns, db, tb, live_id.clone(), ts.clone(), not_id.clone());
	let res = tx.putc_tbnt(key, not_bad_lq, None).await;
	assert!(res.is_err());
	let key = Nt::new(ns, db, tb, live_id.clone(), ts.clone(), not_id.clone());
	let res = tx.putc_tbnt(key, not_bad_nt, None).await;
	assert!(res.is_err());
	tx.commit().await.unwrap();
}

fn create_nt_tuple<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	node_id: sqlUuid,
	live_id: sqlUuid,
	not_id: sqlUuid,
) -> (Nt<'a>, KvsNotification) {
	let result = Value::Strand(Strand::from("teststrand result"));
	let timestamp = Timestamp {
		value: 123,
	};
	let not = KvsNotification {
		live_id: live_id.clone(),
		node_id: node_id.clone(),
		notification_id: not_id.clone(),
		action: KvsAction::Create,
		result,
		timestamp: timestamp.clone(),
	};
	let nt = Nt::new(ns, db, tb, live_id, timestamp, not_id);
	return (nt, not);
}
