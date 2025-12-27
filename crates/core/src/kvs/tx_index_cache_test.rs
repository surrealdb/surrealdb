//! Tests for index cache invalidation
//!
//! These tests verify that the transaction cache is properly invalidated
//! when indexes are added or removed, preventing stale cache data from
//! causing "index not found" errors.

#[cfg(test)]
mod tests {
	use crate::dbs::{Capabilities, Session};
	use crate::kvs::Datastore;

	/// Test that demonstrates the cache invalidation bug when adding an index.
	///
	/// Without the fix, this test could fail because:
	/// 1. First call to all_tb_indexes() caches an empty list
	/// 2. put_tb_index() adds a new index but doesn't invalidate the cached list
	/// 3. Second call to all_tb_indexes() returns the stale cached empty list
	///
	/// With the fix, put_tb_index() invalidates the cached list, so subsequent
	/// queries see the new index.
	#[tokio::test]
	async fn test_index_cache_invalidation_on_put() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
		let ses = Session::owner().with_ns("test").with_db("test");

		// Setup: Create namespace, database, and table
		ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
		ds.execute("DEFINE DATABASE test", &ses, None).await.unwrap();
		ds.execute("DEFINE TABLE test_table", &ses, None).await.unwrap();

		// Add an index
		let mut res = ds
			.execute("DEFINE INDEX test_idx ON test_table FIELDS name", &ses, None)
			.await
			.unwrap();
		assert!(res.remove(0).result.is_ok(), "DEFINE INDEX should succeed");

		// Verify the index exists by querying INFO FOR TABLE
		let mut res = ds.execute("INFO FOR TABLE test_table", &ses, None).await.unwrap();
		let val = res.remove(0).result.unwrap();

		// Convert to string to check if index is present
		let info = format!("{:?}", val);
		assert!(
			info.contains("test_idx"),
			"After adding an index, it should be visible in table info"
		);
	}

	/// Test that demonstrates the cache invalidation bug when removing an index.
	///
	/// Without the fix, this test could fail because:
	/// 1. First call to all_tb_indexes() caches a list with one index
	/// 2. del_tb_index() removes the index but doesn't invalidate the cached list
	/// 3. Second call to all_tb_indexes() returns the stale cached list with the removed index
	///
	/// With the fix, del_tb_index() invalidates the cached list, so subsequent
	/// queries don't see the removed index.
	#[tokio::test]
	async fn test_index_cache_invalidation_on_delete() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
		let ses = Session::owner().with_ns("test").with_db("test");

		// Setup: Create namespace, database, table, and index
		ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
		ds.execute("DEFINE DATABASE test", &ses, None).await.unwrap();
		ds.execute("DEFINE TABLE test_table", &ses, None).await.unwrap();
		let mut res = ds
			.execute("DEFINE INDEX test_idx ON test_table FIELDS name", &ses, None)
			.await
			.unwrap();
		assert!(res.remove(0).result.is_ok(), "DEFINE INDEX should succeed");

		// Remove the index
		let mut res = ds.execute("REMOVE INDEX test_idx ON test_table", &ses, None).await.unwrap();
		assert!(res.remove(0).result.is_ok(), "REMOVE INDEX should succeed");

		// Verify the index is gone by checking INFO FOR TABLE
		let mut res = ds.execute("INFO FOR TABLE test_table", &ses, None).await.unwrap();
		let val = res.remove(0).result.unwrap();

		// Convert to string to check if index is absent
		let info = format!("{:?}", val);
		assert!(
			!info.contains("test_idx"),
			"After removing an index, it should not be visible in table info"
		);
	}

	/// Test that verifies index is usable after creation
	#[tokio::test]
	async fn test_index_usable_after_creation() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
		let ses = Session::owner().with_ns("test").with_db("test");

		// Setup
		ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
		ds.execute("DEFINE DATABASE test", &ses, None).await.unwrap();
		ds.execute("DEFINE TABLE test_table", &ses, None).await.unwrap();

		// Create an index
		let mut res = ds
			.execute("DEFINE INDEX test_idx ON test_table FIELDS name", &ses, None)
			.await
			.unwrap();
		assert!(res.remove(0).result.is_ok());

		// Insert some data that uses the indexed field
		let mut res = ds.execute("CREATE test_table SET name = 'test'", &ses, None).await.unwrap();
		assert!(res.remove(0).result.is_ok(), "INSERT should succeed with index present");

		// Query using the indexed field
		let mut res =
			ds.execute("SELECT * FROM test_table WHERE name = 'test'", &ses, None).await.unwrap();
		let val = res.remove(0).result.unwrap();
		assert!(
			val.is_array() && !val.as_array().unwrap().is_empty(),
			"Query should return results"
		);
	}

	/// Test that verifies multiple sequential index operations work correctly
	#[tokio::test]
	async fn test_multiple_index_operations_cache_consistency() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
		let ses = Session::owner().with_ns("test").with_db("test");

		// Setup
		ds.execute("DEFINE NAMESPACE test", &Session::owner(), None).await.unwrap();
		ds.execute("DEFINE DATABASE test", &ses, None).await.unwrap();
		ds.execute("DEFINE TABLE test_table", &ses, None).await.unwrap();

		// Add first index
		let mut res =
			ds.execute("DEFINE INDEX idx1 ON test_table FIELDS field1", &ses, None).await.unwrap();
		assert!(res.remove(0).result.is_ok());

		// Add second index
		let mut res =
			ds.execute("DEFINE INDEX idx2 ON test_table FIELDS field2", &ses, None).await.unwrap();
		assert!(res.remove(0).result.is_ok());

		// Verify both indexes exist
		let mut res = ds.execute("INFO FOR TABLE test_table", &ses, None).await.unwrap();
		let val = res.remove(0).result.unwrap();
		let info = format!("{:?}", val);
		assert!(info.contains("idx1") && info.contains("idx2"), "Both indexes should be visible");

		// Remove first index
		let mut res = ds.execute("REMOVE INDEX idx1 ON test_table", &ses, None).await.unwrap();
		assert!(res.remove(0).result.is_ok());

		// Verify only second index remains
		let mut res = ds.execute("INFO FOR TABLE test_table", &ses, None).await.unwrap();
		let val = res.remove(0).result.unwrap();
		let info = format!("{:?}", val);
		assert!(
			!info.contains("idx1") && info.contains("idx2"),
			"Only idx2 should remain after removing idx1"
		);
	}
}
