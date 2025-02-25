mod helpers;

use helpers::*;
use std::sync::Arc;

use hashbrown::HashSet;
use surrealdb_core::dbs::Session;
use surrealdb_core::err::Error;
use surrealdb_core::kvs::Datastore;

async fn concurrent_task(ds: Arc<Datastore>, count: usize) -> HashSet<i64> {
	let mut set = HashSet::new();
	let ses = Session::owner().with_ns("test").with_db("test");
	for _ in 0..count {
		let res = &mut ds.execute("RETURN sequence::nextval('sq');", &ses, None).await.unwrap();
		let val = res.remove(0).result.unwrap().coerce_to_i64().unwrap();
		set.insert(val);
	}
	set
}
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_sequence_next_val() -> Result<(), Error> {
	let ds = Arc::new(new_ds().await?);
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create the sequence
	let res = &mut ds.execute("DEFINE SEQUENCE sq", &ses, None).await?;
	skip_ok(res, 1)?;

	let count = 1000;
	// Run 3 tasks collecting the next value of the sequence
	let task1 = tokio::spawn(concurrent_task(ds.clone(), count));
	let task2 = tokio::spawn(concurrent_task(ds.clone(), count));
	let task3 = tokio::spawn(concurrent_task(ds.clone(), count));
	let (mut set1, set2, set3) = tokio::try_join!(task1, task2, task3).expect("Tasks failed");

	// Check that each set has the expected number of unique numbers
	assert_eq!(set1.len(), count);
	assert_eq!(set2.len(), count);
	assert_eq!(set3.len(), count);

	// Check that merged sets have unique numbers
	set1.extend(set2);
	set1.extend(set3);
	assert_eq!(set1.len(), count * 3);

	Ok(())
}
