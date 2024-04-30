use crate::fflags::FFLAGS;
use crate::kvs::lq_structs::{LqIndexKey, LqIndexValue, LqSelector};
use uuid::Uuid;

#[tokio::test]
#[serial]
async fn scan_node_lq() {
	let node_id = Uuid::parse_str("63bb5c1a-b14e-4075-a7f8-680267fbe136").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let namespace = "test_namespace";
	let database = "test_database";
	let live_query_id = Uuid::from_bytes([
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
		0x1F,
	]);
	let key = crate::key::node::lq::new(node_id, live_query_id, namespace, database);
	trace!(
		"Inserting key: {}",
		key.encode()
			.unwrap()
			.iter()
			.flat_map(|byte| std::ascii::escape_default(*byte))
			.map(|byte| byte as char)
			.collect::<String>()
	);
	tx.putc(key, "value", None).await.unwrap();
	tx.commit().await.unwrap();
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();

	let res = tx.scan_ndlq(&node_id, 100).await.unwrap();
	assert_eq!(res.len(), 1);
	for val in res {
		assert_eq!(val.nd.0, node_id.clone());
		assert_eq!(val.ns, namespace);
		assert_eq!(val.db, database);
		assert_eq!(val.lq.0, live_query_id.clone());
	}

	tx.commit().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn live_params_are_evaluated() {
	if !FFLAGS.change_feed_live_queries.enabled() {
		return;
	}
	let node_id = Uuid::parse_str("9cb22db9-1851-4781-8847-d781a3f373ae").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let test = init(node_id, clock).await.unwrap();

	let sess = Session::owner().with_ns("test_namespace").with_db("test_database");
	let params = map! {
		"expected_table".to_string() => Value::Table(sql::Table("test_table".to_string())),
	};
	test.db.execute("DEFINE TABLE expected_table CHANGEFEED 10m INCLUDE ORIGINAL; LIVE SELECT * FROM $expected_table", &sess, Some(params)).await.unwrap();
	let mut res = test.db.lq_cf_store.read().await.live_queries_for_selector(&LqSelector {
		ns: "test_namespace".to_string(),
		db: "test_database".to_string(),
		tb: "test_table".to_string(),
	});
	assert_eq!(res.len(), 1);
	// We remove the unknown value
	res[0].0.lq = Default::default();
	assert_eq!(
		res,
		vec![(
			LqIndexKey {
				selector: LqSelector {
					ns: "test_namespace".to_string(),
					db: "test_database".to_string(),
					tb: "test_table".to_string(),
				},
				lq: Default::default(),
			},
			LqIndexValue {
				stm: Default::default(),
				vs: [0; 10],
				ts: Default::default(),
			}
		)]
	)
}
