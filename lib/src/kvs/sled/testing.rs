#[cfg(test)]
mod tests {

	use crate::kvs::tx::Transaction;
	use crate::kvs::{Datastore, Key, Val};
	use std::fs;
	use std::path::PathBuf;
	use std::sync::atomic::{AtomicU16, Ordering};

	/// This value is automatically incremented for each test
	/// so that each test has a dedicated id
	static TEST_ID: AtomicU16 = AtomicU16::new(1);

	pub fn next_test_id() -> usize {
		TEST_ID.fetch_add(1, Ordering::SeqCst) as usize
	}

	pub fn new_tmp_path(path: &str, delete_existing: bool) -> PathBuf {
		let mut path_buf = PathBuf::from("/tmp");
		if !path_buf.exists() {
			fs::create_dir(path_buf.as_path()).unwrap();
		}
		path_buf.push(path);
		if delete_existing && path_buf.exists() {
			if path_buf.is_dir() {
				fs::remove_dir_all(&path_buf).unwrap();
			} else if path_buf.is_file() {
				fs::remove_file(&path_buf).unwrap()
			}
		}
		path_buf
	}

	fn new_store_path() -> String {
		let store_path = format!("/tmp/sled.{}", next_test_id());
		new_tmp_path(&store_path, true);
		store_path
	}

	async fn get_transaction(store_path: &str) -> Transaction {
		let datastore = Datastore::new(&format!("sled:{}", store_path)).await.unwrap();
		datastore.transaction(true, false).await.unwrap()
	}

	#[tokio::test]
	async fn test_transaction_sled_put_exi_get() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			// The key should not exist
			assert_eq!(tx.exi("flip").await.unwrap(), false);
			assert_eq!(tx.get("flip").await.unwrap(), None);
			tx.put("flip", "flop").await.unwrap();
			// Check existence against memory
			assert_eq!(tx.exi("flip").await.unwrap(), true);
			// Read from memory
			assert_eq!(tx.get("flip").await.unwrap(), Some("flop".as_bytes().to_vec()));
			// Commit in storage
			tx.commit().await.unwrap();
		}
		{
			// New transaction with the committed data
			let mut tx = get_transaction(&store_path).await;
			// The key exists
			assert_eq!(tx.exi("flip").await.unwrap(), true);
			// And the value can be retrieved
			assert_eq!(tx.get("flip").await.unwrap(), Some("flop".as_bytes().to_vec()));
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_putc_err() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("flip", "flop").await.unwrap();
			assert_eq!(
				tx.putc("flip", "flap", Some("nada")).await.err().unwrap().to_string(),
				"Value being checked was not correct"
			);
			// Checked the value did not change in memory
			assert_eq!(tx.get("flip").await.unwrap(), Some("flop".as_bytes().to_vec()));
			// Commit in storage
			tx.commit().await.unwrap();
		}
		{
			// New transaction with the committed data
			let mut tx = get_transaction(&store_path).await;
			// Check the value did not change on storage
			assert_eq!(tx.get("flip").await.unwrap(), Some("flop".as_bytes().to_vec()));
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_putc_ok() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("flip", "flop").await.unwrap();
			tx.putc("flip", "flap", Some("flop")).await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			// New transaction with the committed data
			let mut tx = get_transaction(&store_path).await;
			assert_eq!(tx.get("flip").await.unwrap(), Some("flap".as_bytes().to_vec()));
		}
	}

	fn check_scan_result(result: Vec<(Key, Val)>, expected: Vec<(&'static str, &'static str)>) {
		assert_eq!(result.len(), expected.len());
		let mut i = 0;
		for (key, val) in result {
			let (expected_key, expected_value) = expected.get(i).unwrap();
			assert_eq!(&String::from_utf8(key).unwrap(), *expected_key);
			assert_eq!(&String::from_utf8(val).unwrap(), *expected_value);
			i += 1;
		}
	}

	async fn scan_suite_checks(tx: &mut Transaction) {
		// I can retrieve the key/values with using scan with several ranges
		check_scan_result(
			tx.scan("k1".."k9", 100).await.unwrap(),
			vec![("k1", "v1"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4"), ("k5", "v5")],
		);
		check_scan_result(tx.scan("k1".."k2", 100).await.unwrap(), vec![("k1", "v1")]);
		check_scan_result(
			tx.scan("k1".."k3", 100).await.unwrap(),
			vec![("k1", "v1"), ("k2", "v2")],
		);
		check_scan_result(tx.scan("k2".."k3", 100).await.unwrap(), vec![("k2", "v2")]);
		check_scan_result(tx.scan("k3".."k4", 100).await.unwrap(), vec![("k3", "v3")]);
		check_scan_result(tx.scan("k4".."k5", 100).await.unwrap(), vec![("k4", "v4")]);
		check_scan_result(
			tx.scan("k4".."k6", 100).await.unwrap(),
			vec![("k4", "v4"), ("k5", "v5")],
		);
		check_scan_result(tx.scan("k5".."k7", 100).await.unwrap(), vec![("k5", "v5")]);
		check_scan_result(tx.scan("k2".."k1", 100).await.unwrap(), vec![]);

		// I can retrieve the key/values using scan with several limits
		check_scan_result(tx.scan("k1".."k9", 0).await.unwrap(), vec![]);
		check_scan_result(tx.scan("k1".."k9", 1).await.unwrap(), vec![("k1", "v1")]);
		check_scan_result(tx.scan("k2".."k9", 1).await.unwrap(), vec![("k2", "v2")]);
		check_scan_result(tx.scan("k5".."k9", 1).await.unwrap(), vec![("k5", "v5")]);
		check_scan_result(tx.scan("k6".."k9", 1).await.unwrap(), vec![]);
		check_scan_result(tx.scan("k1".."k4", 2).await.unwrap(), vec![("k1", "v1"), ("k2", "v2")]);
		check_scan_result(tx.scan("k2".."k4", 2).await.unwrap(), vec![("k2", "v2"), ("k3", "v3")]);
		check_scan_result(tx.scan("k3".."k4", 2).await.unwrap(), vec![("k3", "v3")]);
	}

	#[tokio::test]
	async fn test_transaction_sled_scan_in_transaction() {
		let store_path = new_store_path();
		{
			// Given a set of key/values added in a transaction
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.put("k1", "v1").await.unwrap();
			tx.put("k5", "v5").await.unwrap();
			tx.put("k4", "v4").await.unwrap();

			// Then, I can successfully use the range method on in memory key/values
			scan_suite_checks(&mut tx).await;
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_scan_in_storage() {
		let store_path = new_store_path();
		{
			// Given three key/values added in the transaction...
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.put("k1", "v1").await.unwrap();
			tx.put("k5", "v5").await.unwrap();
			tx.put("k4", "v4").await.unwrap();
			// ... and stored
			tx.commit().await.unwrap();
		}
		{
			// Then, I can successfully use the range method on stored key/values
			let mut tx = get_transaction(&store_path).await;
			scan_suite_checks(&mut tx).await;
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_scan_mixed() {
		let store_path = new_store_path();
		{
			// Given three key/values added in the transaction and stored
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.put("k5", "v5").await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			// then, given two key/values added in the transaction
			let mut tx = get_transaction(&store_path).await;
			tx.put("k1", "v1").await.unwrap();
			tx.put("k4", "v4").await.unwrap();

			// Then, I can successfully use the range method on mixed key/values
			scan_suite_checks(&mut tx).await;
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_scan_mixed_with_deletion() {
		let store_path = new_store_path();
		{
			// Given three key/values added in the transaction and stored
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.put("k5", "v5").await.unwrap();
			tx.put("k6", "v6").await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			// then, given two key/values added in the transaction
			let mut tx = get_transaction(&store_path).await;
			tx.del("k6").await.unwrap();
			tx.put("k1", "v1").await.unwrap();
			tx.put("k4", "v4").await.unwrap();

			// Then, I can successfully use the range method on mixed key/values
			scan_suite_checks(&mut tx).await;
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_del_in_transaction() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("k1", "v1").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.del("k1").await.unwrap();
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k1").await.unwrap(), None);
			tx.commit().await.unwrap();
		}
		{
			let mut tx = get_transaction(&store_path).await;
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k2").await.unwrap(), Some("v2".as_bytes().to_vec()));
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_del_in_storage() {
		let store_path = new_store_path();
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("k1", "v1").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			let mut tx = get_transaction(&store_path).await;
			tx.del("k1").await.unwrap();
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k1").await.unwrap(), None);
			tx.commit().await.unwrap();
		}
		{
			let mut tx = get_transaction(&store_path).await;
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k2").await.unwrap(), Some("v2".as_bytes().to_vec()));
		}
	}

	#[tokio::test]
	async fn test_transaction_sled_cancel_put_and_del() {
		let store_path = new_store_path();
		// Given a store with two keys
		{
			let mut tx = get_transaction(&store_path).await;
			tx.put("k1", "v1").await.unwrap();
			tx.put("k2", "v2").await.unwrap();
			tx.commit().await.unwrap();
		}
		{
			// When cancelling a transaction adding k3 and deleting k1
			let mut tx = get_transaction(&store_path).await;
			tx.put("k3", "v3").await.unwrap();
			tx.del("k1").await.unwrap();
			assert_eq!(tx.exi("k1").await.unwrap(), false);
			assert_eq!(tx.get("k3").await.unwrap(), Some("v3".as_bytes().to_vec()));

			tx.cancel().await.unwrap();
		}
		{
			// Then k3 has not been added, and k1 as not been deleted
			let mut tx = get_transaction(&store_path).await;
			assert_eq!(tx.exi("k3").await.unwrap(), false);
			assert_eq!(tx.get("k1").await.unwrap(), Some("v1".as_bytes().to_vec()));
			assert_eq!(tx.get("k2").await.unwrap(), Some("v2".as_bytes().to_vec()));
		}
	}
}
