#[tokio::test]
#[serial]
async fn initialise() {
	let mut tx = new_tx(true, false).await;
	assert!(tx.put("test", "ok").await.is_ok());
	tx.commit().await.unwrap();
}

#[tokio::test]
#[serial]
async fn exi() {
	// Create a new datastore
	let ds = new_ds().await;
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.put("test", "ok").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.exi("test").await.unwrap();
	assert_eq!(val, true);
	let val = tx.exi("none").await.unwrap();
	assert_eq!(val, false);
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn get() {
	// Create a new datastore
	let ds = new_ds().await;
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.put("test", "ok").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"ok")));
	let val = tx.get("none").await.unwrap();
	assert!(matches!(val.as_deref(), None));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn set() {
	// Create a new datastore
	let ds = new_ds().await;
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.set("test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.set("test", "two").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn put() {
	// Create a new datastore
	let ds = new_ds().await;
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.put("test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.put("test", "two").await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn del() {
	// Create a new datastore
	let ds = new_ds().await;
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.put("test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.del("test").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), None));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn putc() {
	// Create a new datastore
	let ds = new_ds().await;
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.put("test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.putc("test", "two", Some("one")).await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.putc("test", "tre", Some("one")).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"two")));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn delc() {
	// Create a new datastore
	let ds = new_ds().await;
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.put("test", "one").await.is_ok());
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.delc("test", Some("two")).await.is_err());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), Some(b"one")));
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.delc("test", Some("one")).await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.get("test").await.unwrap();
	assert!(matches!(val.as_deref(), None));
	tx.cancel().await.unwrap();
}

#[tokio::test]
#[serial]
async fn scan() {
	// Create a new datastore
	let ds = new_ds().await;
	// Create a writeable transaction
	let mut tx = ds.transaction(true, false).await.unwrap();
	assert!(tx.put("test1", "1").await.is_ok());
	assert!(tx.put("test2", "2").await.is_ok());
	assert!(tx.put("test3", "3").await.is_ok());
	assert!(tx.put("test4", "4").await.is_ok());
	assert!(tx.put("test5", "5").await.is_ok());
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
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
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.scan("test2".."test4", u32::MAX).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test2");
	assert_eq!(val[0].1, b"2");
	assert_eq!(val[1].0, b"test3");
	assert_eq!(val[1].1, b"3");
	tx.cancel().await.unwrap();
	// Create a readonly transaction
	let mut tx = ds.transaction(false, false).await.unwrap();
	let val = tx.scan("test1".."test9", 2).await.unwrap();
	assert_eq!(val.len(), 2);
	assert_eq!(val[0].0, b"test1");
	assert_eq!(val[0].1, b"1");
	assert_eq!(val[1].0, b"test2");
	assert_eq!(val[1].1, b"2");
	tx.cancel().await.unwrap();
}
