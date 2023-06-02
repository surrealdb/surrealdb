#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use crate::kvs::tests::helper::helper;
	use uuid::Uuid;

	#[tokio::test]
	async fn scan_node_lq() {
		let test = helper::init().await.unwrap();
		let mut tx = test.db.transaction(true, true).await.unwrap();
		let node_id = Uuid::from_bytes([
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
			0x0E, 0x0F,
		]);
		let namespace = "test_namespace";
		let database = "test_database";
		let live_query_id = Uuid::from_bytes([
			0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
			0x1E, 0x1F,
		]);
		let key = crate::key::lq::new(&node_id, namespace, database, &live_query_id);
		trace!(
			"Inserting key: {}",
			key.encode()
				.unwrap()
				.iter()
				.flat_map(|byte| std::ascii::escape_default(byte.clone()))
				.map(|byte| byte as char)
				.collect::<String>()
		);
		let _ = tx.putc(key, "value", None).await.unwrap();
		tx.commit().await.unwrap();
		let mut tx = test.db.transaction(true, true).await.unwrap();

		let res = tx.scan_lq(&node_id, 100).await.unwrap();
		assert_eq!(res.len(), 1);
		for val in res {
			assert_eq!(val.cl, node_id);
			assert_eq!(val.ns, namespace);
			assert_eq!(val.db, database);
			assert_eq!(val.lq, live_query_id);
		}

		tx.commit().await.unwrap();
	}

	#[tokio::test]
	async fn scan_table_lq() {
		let test = helper::init().await.unwrap();
		let mut tx = test.db.transaction(true, true).await.unwrap();
		tx.commit().await.unwrap();
		assert_eq!(0, 1)
	}
}
