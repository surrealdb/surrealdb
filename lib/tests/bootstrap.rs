/// The tests in this file are checking that bootstrapping of the database works correctly
/// They are testing edge cases that may accidentally occur with bugs - we wan't to make sure
/// the system can recover in light of these issues.
///
/// We may want to move these tests to another suite, as they aren't testing the statements like
/// the other tests are.
mod helpers;
mod parse;

use helpers::new_ds;
use surrealdb::err::Error;
use uuid::Uuid;

#[tokio::test]
async fn bootstrap_removes_unreachable_nodes() -> Result<(), Error> {
	// Create the datastore
	let dbs = new_ds().await?;

	// Introduce missing nodes (without heartbeats)
	let tx = dbs.transaction(true, false).await.unwrap().enclose();
	let bad_node =
		Uuid::from(uuid::Uuid::parse_str("9d8e16e4-9f6a-4704-8cf1-7cd55b937c5b").unwrap());
	tx.lock().await.set_nd(bad_node).await.unwrap();
	tx.lock().await.commit().await.unwrap();

	// Bootstrap
	dbs.bootstrap().await.unwrap();

	// Verify the incorrect node is deleted, but self is inserted
	let tx = dbs.transaction(true, false).await.unwrap().enclose();
	let res = tx.lock().await.scan_cl(1000).await.unwrap();
	tx.lock().await.cancel().await.unwrap();
	assert_eq!(res.len(), 1);
	let cluster_membership = res.get(0).unwrap();
	assert_ne!(cluster_membership.name, bad_node.to_string());
	Ok(())
}
#[tokio::test]
async fn bootstrap_removes_unreachable_node_live_queries() -> Result<(), Error> {
	// Create the datastore

	// Introduce a valid heartbeat

	// Introduce a valid node

	// Introduce an invalid node live query

	// Bootstrap

	// Verify node live query is deleted
	Ok(())
}

#[tokio::test]
async fn bootstrap_removes_unreachable_table_live_queries() -> Result<(), Error> {
	// Create the datastore

	// Introduce a valid heartbeat

	// Introduce a valid node

	// Introduce a valid node live query

	// Introduce an invalid table live query

	// Introduce a valid table live query for coherency

	// Bootstrap

	// Verify invalid table live query is deleted
	Ok(())
}

async fn bootstrap_removes_unreachable_live_query_notifications() -> Result<(), Error> {
	Ok(())
}
