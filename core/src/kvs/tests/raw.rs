use crate::key::error::KeyCategory::Unknown;

#[tokio::test]
#[serial]
async fn initialise() {
	let mut tx = new_tx(Write, Optimistic).await;
	assert!(tx.put(Unknown, "test", "ok").await.is_ok());
	tx.commit().await.unwrap();
}

#[tokio::test]
#[serial]
async fn exi() {
	// Create a new datastore
	let node_id = Uuid::parse_str("463a5008-ee1d-43db-9662-5e752b6ea3f9").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test", "ok").await.is_ok());
	tx.commit().await.unwrap();

	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.exi("test").await.unwrap();
	assert!(val);
	let val = tx.exi("none").await.unwrap();
	assert!(!val);
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn get() {
	// Create a new datastore
	let node_id = Uuid::parse_str("477e2895-8c98-4606-a827-0add82eb466b").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test", "ok").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"ok")));
	let val = tx.get("none").await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn set() {
	// Create a new datastore
	let node_id = Uuid::parse_str("32b80d8b-dd16-4f6f-a687-1192f6cfc6f1").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.set("test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.set("test", "two").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn put() {
	// Create a new datastore
	let node_id = Uuid::parse_str("80149655-db34-451c-8711-6fa662a44b70").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test", "two").await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn del() {
	// Create a new datastore
	let node_id = Uuid::parse_str("e0acb360-9187-401f-8192-f870b09e2c9e").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.del("test").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn putc() {
	// Create a new datastore
	let node_id = Uuid::parse_str("705bb520-bc2b-4d52-8e64-d1214397e408").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.putc("test", "two", Some("one")).await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.putc("test", "tre", Some("one")).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn delc() {
	// Create a new datastore
	let node_id = Uuid::parse_str("0985488e-cf2f-417a-bd10-7f4aa9c99c15").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.delc("test", Some("two")).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.delc("test", Some("one")).await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(val.as_deref().is_none());
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn scan() {
	// Create a new datastore
	let node_id = Uuid::parse_str("83b81cc2-9609-4533-bede-c170ab9f7bbe").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test1", "1").await.is_ok());
	assert!(tx.put(Unknown, "test2", "2").await.is_ok());
	assert!(tx.put(Unknown, "test3", "3").await.is_ok());
	assert!(tx.put(Unknown, "test4", "4").await.is_ok());
	assert!(tx.put(Unknown, "test5", "5").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scan("test1".."test9", u32::MAX).await.unwrap();
	assert_eq!(val.len(), 5);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	assert_eq!(val[2].0, b"test3");
	assert_eq!(val[2].1, b"3");
	assert_eq!(val[3].0, b"test4");
	assert_eq!(val[3].1, b"4");
	assert_eq!(val[4].0, b"test5");
	assert_eq!(val[4].1, b"5");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scan("test2".."test4", u32::MAX).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scan("test1".."test9", 2).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn scan_paged() {
	// Create a new datastore
	let node_id = Uuid::parse_str("6572a13c-a7a0-4e19-be62-18acb4e854f5").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds(node_id, clock).await;
	// Create a writeable transaction
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx.put(Unknown, "test1", "1").await.is_ok());
	assert!(tx.put(Unknown, "test2", "2").await.is_ok());
	assert!(tx.put(Unknown, "test3", "3").await.is_ok());
	assert!(tx.put(Unknown, "test4", "4").await.is_ok());
	assert!(tx.put(Unknown, "test5", "5").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val =
		tx.scan_paged(ScanPage::from("test1".into().."test9".into()), u32::MAX).await.unwrap();
	let val = val.values;
	assert_eq!(val.len(), 5);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	assert_eq!(val[2].0, b"test3");
	assert_eq!(val[2].1, b"3");
	assert_eq!(val[3].0, b"test4");
	assert_eq!(val[3].1, b"4");
	assert_eq!(val[4].0, b"test5");
	assert_eq!(val[4].1, b"5");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val =
		tx.scan_paged(ScanPage::from("test2".into().."test4".into()), u32::MAX).await.unwrap();
	let val = val.values;
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.scan_paged(ScanPage::from("test1".into().."test9".into()), 2).await.unwrap();
	let val = val.values;
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	tx.cancel().await.unwrap();
}
