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
