#[tokio::test]
#[serial]
async fn tx_drop_cancels_transaction() {
	let test = init().await.unwrap();
	{
		let mut tx = test.db.transaction(true, false).await.unwrap();
		tx.set("some_key", "some value").await.unwrap();
		// intentionally don't commit
	}
	let mut tx = test.db.transaction(false, false).await.unwrap();
	let val = tx.get("some_key").await;
	assert!(!val.is_err());
	assert!(val.unwrap().is_none());
}
