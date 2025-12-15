//! Integration tests for session isolation features
//!
//! These tests verify that cloned clients maintain independent session state including:
//! - Namespace and database selection
//! - Authentication state
//! - Variables (SET/UNSET)
//! - Transactions (BEGIN/COMMIT/CANCEL)

use std::iter;

use surrealdb::opt::Config;
use surrealdb::opt::auth::{Database, Namespace};
use surrealdb::types::RecordId;
use surrealdb_types::SurrealValue;
use ulid::Ulid;

use super::CreateDb;

#[derive(Debug, Clone, PartialEq, SurrealValue)]
struct TestRecord {
	id: RecordId,
	value: i32,
}

/// Test that cloning creates a new session with independent namespace/database selection
pub async fn clone_creates_new_session(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	// Set namespace and database on original client
	let ns1 = Ulid::new().to_string();
	let db1_name = Ulid::new().to_string();
	db.use_ns(&ns1).use_db(&db1_name).await.unwrap();

	// Create a record in the first database
	let table = format!("t{}", Ulid::new());
	db.query(format!("CREATE {table}:test SET value = 1")).await.unwrap().check().unwrap();

	// Clone the client - should create a new session
	let db2 = db.clone();

	// Set different namespace/database on cloned client
	let ns2 = Ulid::new().to_string();
	let db2_name = Ulid::new().to_string();
	db2.use_ns(&ns2).use_db(&db2_name).await.unwrap();

	// Query on cloned client should return nothing (different database)
	let records: Vec<TestRecord> =
		db2.query(format!("SELECT * FROM {table}:test")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 0, "Cloned client should see empty database");

	// Original client should still see the record
	let records: Vec<TestRecord> =
		db.query(format!("SELECT * FROM {table}:test")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 1, "Original client should see its record");
	assert_eq!(records[0].value, 1);

	drop(permit);
}

/// Test that multiple clients can use different namespaces/databases simultaneously
pub async fn multiple_namespaces_databases(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	// Client 1: namespace1/database1
	let client1 = db.clone();
	let ns1 = Ulid::new().to_string();
	let db1 = Ulid::new().to_string();
	client1.use_ns(&ns1).use_db(&db1).await.unwrap();

	// Client 2: namespace2/database2
	let client2 = db.clone();
	let ns2 = Ulid::new().to_string();
	let db2 = Ulid::new().to_string();
	client2.use_ns(&ns2).use_db(&db2).await.unwrap();

	// Client 3: namespace1/database2 (same ns as client1, different db)
	let client3 = db.clone();
	let db3 = Ulid::new().to_string();
	client3.use_ns(&ns1).use_db(&db3).await.unwrap();

	let table = format!("t{}", Ulid::new());

	// Create different records in each database
	client1.query(format!("CREATE {table}:test SET value = 100")).await.unwrap().check().unwrap();
	client2.query(format!("CREATE {table}:test SET value = 200")).await.unwrap().check().unwrap();
	client3.query(format!("CREATE {table}:test SET value = 300")).await.unwrap().check().unwrap();

	// Verify each client sees only its own data
	let records: Vec<TestRecord> =
		client1.query(format!("SELECT * FROM {table}:test")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 1);
	assert_eq!(records[0].value, 100, "Client 1 should see value 100");

	let records: Vec<TestRecord> =
		client2.query(format!("SELECT * FROM {table}:test")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 1);
	assert_eq!(records[0].value, 200, "Client 2 should see value 200");

	let records: Vec<TestRecord> =
		client3.query(format!("SELECT * FROM {table}:test")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 1);
	assert_eq!(records[0].value, 300, "Client 3 should see value 300");

	drop(permit);
}

/// Test that session-specific variables (SET/UNSET) are isolated
pub async fn session_variables_isolated(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	let ns = Ulid::new().to_string();
	let db_name = Ulid::new().to_string();

	// Set up two clients with same namespace/database
	let client1 = db.clone();
	client1.use_ns(&ns).use_db(&db_name).await.unwrap();

	let client2 = db.clone();
	client2.use_ns(&ns).use_db(&db_name).await.unwrap();

	// Set different variables on each client
	client1.set("my_var", 111).await.unwrap();
	client2.set("my_var", 222).await.unwrap();

	let table = format!("t{}", Ulid::new());

	// Create records using the variables - should use different values
	client1
		.query(format!("CREATE {table}:client1 SET value = $my_var"))
		.await
		.unwrap()
		.check()
		.unwrap();
	client2
		.query(format!("CREATE {table}:client2 SET value = $my_var"))
		.await
		.unwrap()
		.check()
		.unwrap();

	// Verify each client used its own variable value
	let records: Vec<TestRecord> =
		client1.query(format!("SELECT * FROM {table}:client1")).await.unwrap().take(0).unwrap();
	assert_eq!(records[0].value, 111, "Client 1 should use its own variable");

	let records: Vec<TestRecord> =
		client2.query(format!("SELECT * FROM {table}:client2")).await.unwrap().take(0).unwrap();
	assert_eq!(records[0].value, 222, "Client 2 should use its own variable");

	// UNSET on one client shouldn't affect the other
	client1.unset("my_var").await.unwrap();

	// Client1 should no longer have the variable (use OR to coalesce NONE to 0)
	client1
		.query(format!("CREATE {table}:test1 SET value = $my_var OR 0"))
		.await
		.unwrap()
		.check()
		.unwrap();
	let records: Vec<TestRecord> =
		client1.query(format!("SELECT * FROM {table}:test1")).await.unwrap().take(0).unwrap();
	assert_eq!(records[0].value, 0, "Client 1's variable should be unset (NONE coalesces to 0)");

	// Client2 should still have its variable
	client2
		.query(format!("CREATE {table}:test2 SET value = $my_var"))
		.await
		.unwrap()
		.check()
		.unwrap();
	let records: Vec<TestRecord> =
		client2.query(format!("SELECT * FROM {table}:test2")).await.unwrap().take(0).unwrap();
	assert_eq!(records[0].value, 222, "Client 2 should still have its variable");

	drop(permit);
}

/// Test that transactions are isolated per session
#[cfg(not(feature = "protocol-http"))]
pub async fn session_transactions_isolated(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	let ns = Ulid::new().to_string();
	let db_name = Ulid::new().to_string();

	// Set up two clients with same namespace/database
	let client1 = db.clone();
	client1.use_ns(&ns).use_db(&db_name).await.unwrap();

	let client2 = db.clone();
	client2.use_ns(&ns).use_db(&db_name).await.unwrap();

	let table = format!("t{}", Ulid::new());

	// Start transaction on client1 (consumes client1)
	let tx1 = client1.begin().await.unwrap();

	// Create record in transaction on client1 - call query on the transaction object
	tx1.query(format!("CREATE {table}:tx1 SET value = 100")).await.unwrap().check().unwrap();

	// Client2 should NOT see the uncommitted record (it's in client1's transaction)
	let records: Vec<TestRecord> =
		client2.query(format!("SELECT * FROM {table}:tx1")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 0, "Client 2 should not see uncommitted transaction data");

	// Commit client1's transaction (returns client1)
	let client1 = tx1.commit().await.unwrap();

	// Now client2 should see client1's committed data
	let records: Vec<TestRecord> =
		client2.query(format!("SELECT * FROM {table}:tx1")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 1, "Client 2 should see committed transaction data");
	assert_eq!(records[0].value, 100);

	// Client2 can start its own independent transaction (consumes client2)
	let tx2 = client2.begin().await.unwrap();

	// Create record in transaction on client2 - call query on the transaction object
	tx2.query(format!("CREATE {table}:tx2 SET value = 200")).await.unwrap().check().unwrap();

	// Client1 should NOT see the uncommitted record from client2's transaction
	let records: Vec<TestRecord> =
		client1.query(format!("SELECT * FROM {table}:tx2")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 0, "Client 1 should not see uncommitted transaction data");

	// Cancel client2's transaction (returns client2)
	let _client2 = tx2.cancel().await.unwrap();

	// Client1 should NOT see client2's cancelled transaction data
	let records: Vec<TestRecord> =
		client1.query(format!("SELECT * FROM {table}:tx2")).await.unwrap().take(0).unwrap();
	assert_eq!(records.len(), 0, "Client 1 should not see cancelled transaction data");

	drop(permit);
}

/// Test that different authentication states are isolated per session
pub async fn session_authentication_isolated(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	let ns = Ulid::new().to_string();
	let db_name = Ulid::new().to_string();

	let table = format!("t{}", Ulid::new());

	// Set up namespace and database with authentication
	db.use_ns(&ns).use_db(&db_name).await.unwrap();
	db.query(format!(
		"
		DEFINE TABLE {table};
        DEFINE USER ns_user ON NAMESPACE PASSWORD 'ns_pass' ROLES OWNER;
        DEFINE USER db_user ON DATABASE PASSWORD 'db_pass' ROLES OWNER;
    ",
	))
	.await
	.unwrap()
	.check()
	.unwrap();

	// Client1: Authenticate as namespace user
	let client1 = db.clone();
	client1
		.signin(Namespace {
			namespace: ns.clone(),
			username: "ns_user".to_string(),
			password: "ns_pass".to_string(),
		})
		.await
		.unwrap();

	// Client2: Authenticate as database user
	let client2 = db.clone();
	client2
		.signin(Database {
			namespace: ns.clone(),
			database: db_name.clone(),
			username: "db_user".to_string(),
			password: "db_pass".to_string(),
		})
		.await
		.unwrap();

	// Both should be able to operate in their authenticated context
	// (specific permission testing would require more complex setup)
	client1.use_ns(&ns).use_db(&db_name).await.unwrap();
	client2.use_ns(&ns).use_db(&db_name).await.unwrap();

	// Both clients should be able to create records
	client1
		.query(format!("CREATE {table}:from_ns_user SET value = 1"))
		.await
		.unwrap()
		.check()
		.unwrap();

	client2
		.query(format!("CREATE {table}:from_db_user SET value = 2"))
		.await
		.unwrap()
		.check()
		.unwrap();

	// Invalidate client1's session
	client1.invalidate().await.unwrap();

	// Client1 should no longer be able to create records (not authenticated)
	let result = client1.query(format!("CREATE {table}:test SET value = 3; RETURN $auth")).await;
	assert!(
		result.is_err() || result.unwrap().check().is_err(),
		"Client 1 should be unauthenticated after invalidate"
	);

	// Client2 should still be authenticated and functional
	client2
		.query(format!("CREATE {table}:still_works SET value = 4"))
		.await
		.unwrap()
		.check()
		.unwrap();

	drop(permit);
}

/// Test mixed operations across multiple sessions
pub async fn mixed_session_operations(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	// Create 3 clients with different configurations
	let client1 = db.clone();
	let ns1 = Ulid::new().to_string();
	let db1 = Ulid::new().to_string();
	client1.use_ns(&ns1).use_db(&db1).await.unwrap();
	client1.set("multiplier", 10).await.unwrap();

	let client2 = db.clone();
	let ns2 = Ulid::new().to_string();
	let db2 = Ulid::new().to_string();
	client2.use_ns(&ns2).use_db(&db2).await.unwrap();
	client2.set("multiplier", 20).await.unwrap();

	let client3 = db.clone();
	client3.use_ns(&ns1).use_db(&db1).await.unwrap(); // Same as client1
	client3.set("multiplier", 30).await.unwrap();

	let table = format!("t{}", Ulid::new());

	// Define the tables upfront to avoid race conditions
	client1.query(format!("DEFINE TABLE {table}")).await.unwrap().check().unwrap();
	client2.query(format!("DEFINE TABLE {table}")).await.unwrap().check().unwrap();

	// Perform operations simultaneously
	// Note: We clone clients to move into tasks, each gets a fresh session
	let h1 = tokio::spawn({
		let client = client1.clone(); // Clone to move into task
		let table = table.clone();
		async move {
			for i in 1..=5 {
				client
					.query(format!("CREATE {table}:{i} SET value = {i} * $multiplier"))
					.await
					.unwrap()
					.check()
					.unwrap();
			}
		}
	});

	let h2 = tokio::spawn({
		let client = client2.clone(); // Clone to move into task
		let table = table.clone();
		async move {
			for i in 1..=5 {
				client
					.query(format!("CREATE {table}:{i} SET value = {i} * $multiplier"))
					.await
					.unwrap()
					.check()
					.unwrap();
			}
		}
	});

	let h3 = tokio::spawn({
		let client = client3.clone(); // Clone to move into task
		let table = table.clone();
		async move {
			for i in 6..=10 {
				client
					.query(format!("CREATE {table}:{i} SET value = {i} * $multiplier"))
					.await
					.unwrap()
					.check()
					.unwrap();
			}
		}
	});

	tokio::try_join!(h1, h2, h3).unwrap();

	// Verify client1's data (multiplier=10)
	let records: Vec<TestRecord> = client1
		.query(format!("SELECT * FROM {table} ORDER BY value"))
		.await
		.unwrap()
		.take(0)
		.unwrap();
	assert_eq!(records.len(), 10); // 5 from client1 + 5 from client3 (same DB)
	// Client1 records: 1*10=10, 2*10=20, 3*10=30, 4*10=40, 5*10=50
	// Client3 records: 6*30=180, 7*30=210, 8*30=240, 9*30=270, 10*30=300
	assert_eq!(records[0].value, 10);
	assert_eq!(records[4].value, 50);
	assert_eq!(records[5].value, 180);
	assert_eq!(records[9].value, 300);

	// Verify client2's data (multiplier=20, different namespace/database)
	let records: Vec<TestRecord> = client2
		.query(format!("SELECT * FROM {table} ORDER BY value"))
		.await
		.unwrap()
		.take(0)
		.unwrap();
	assert_eq!(records.len(), 5);
	assert_eq!(records[0].value, 20); // 1*20
	assert_eq!(records[4].value, 100); // 5*20

	drop(permit);
}

/// Test that query variables work correctly with session isolation
pub async fn query_variables_with_session_vars(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	let ns = Ulid::new().to_string();
	let db_name = Ulid::new().to_string();

	let client1 = db.clone();
	client1.use_ns(&ns).use_db(&db_name).await.unwrap();
	client1.set("session_value", 100).await.unwrap();

	let client2 = db.clone();
	client2.use_ns(&ns).use_db(&db_name).await.unwrap();
	client2.set("session_value", 200).await.unwrap();

	let table = format!("t{}", Ulid::new());

	// Query with both session variables and query-specific variables
	// Query-specific variables should take precedence
	#[derive(Debug, SurrealValue)]
	struct RecordWithVars {
		id: RecordId,
		session: i32,
		query: i32,
	}

	let records: Vec<RecordWithVars> = client1
		.query(format!("CREATE {table}:test1 SET session = $session_value, query = $query_value"))
		.bind(("query_value", 111))
		.await
		.unwrap()
		.take(0)
		.unwrap();
	assert_eq!(records[0].session, 100, "Should use client1's session variable");
	assert_eq!(records[0].query, 111, "Should use query-specific variable");

	let records: Vec<RecordWithVars> = client2
		.query(format!("CREATE {table}:test2 SET session = $session_value, query = $query_value"))
		.bind(("query_value", 222))
		.await
		.unwrap()
		.take(0)
		.unwrap();
	assert_eq!(records[0].session, 200, "Should use client2's session variable");
	assert_eq!(records[0].query, 222, "Should use query-specific variable");

	drop(permit);
}

/// Test session isolation with the same underlying connection
pub async fn shared_connection_isolated_sessions(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	// Create multiple clones from the same root client
	let clients: Vec<_> = iter::repeat_n(db, 5).collect();

	// Set up each client with unique namespace/database/variable
	let mut handles = vec![];

	for (i, client) in clients.iter().enumerate() {
		let client = client.clone();
		let handle = tokio::spawn(async move {
			let ns = Ulid::new().to_string();
			let db_name = Ulid::new().to_string();
			let table = format!("t{}", Ulid::new());

			client.use_ns(&ns).use_db(&db_name).await.unwrap();
			client.set("client_id", i as i32).await.unwrap();

			// Create records
			for j in 0..10 {
				client
					.query(format!("CREATE {table}:{j} SET value = {j} + $client_id"))
					.await
					.unwrap()
					.check()
					.unwrap();
			}

			// Verify data
			let records: Vec<TestRecord> = client
				.query(format!("SELECT * FROM {table} ORDER BY value"))
				.await
				.unwrap()
				.take(0)
				.unwrap();
			assert_eq!(records.len(), 10);
			assert_eq!(records[0].value, i as i32); // 0 + client_id

			(ns, db_name, table)
		});
		handles.push(handle);
	}

	// Wait for all operations to complete
	for handle in handles {
		handle.await.unwrap();
	}

	drop(permit);
}

define_include_tests!(
	session_isolation => {
		#[test_log::test(tokio::test)]
		clone_creates_new_session,
		#[test_log::test(tokio::test)]
		multiple_namespaces_databases,
		#[test_log::test(tokio::test)]
		session_variables_isolated,
		#[test_log::test(tokio::test)]
		#[cfg(not(feature = "protocol-http"))]
		session_transactions_isolated,
		#[test_log::test(tokio::test)]
		session_authentication_isolated,
		#[test_log::test(tokio::test)]
		mixed_session_operations,
		#[test_log::test(tokio::test)]
		query_variables_with_session_vars,
		#[test_log::test(tokio::test)]
		shared_connection_isolated_sessions,
	}
);
