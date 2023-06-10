#[tokio::test]
#[serial]
async fn initialise() {
	let mut tx = new_tx(true, false).await;
	tx.put("test", "ok").await.unwrap();
}
