mod helpers;

use helpers::*;
use std::collections::BTreeSet;
use std::sync::Arc;

use hashbrown::HashSet;
use surrealdb_core::dbs::Session;
use surrealdb_core::err::Error;
use surrealdb_core::kvs::Datastore;

async fn concurrent_task(ds: Arc<Datastore>, seq: &str, count: usize) -> HashSet<i64> {
	let mut set = HashSet::new();
	let ses = Session::owner().with_ns("test").with_db("test");
	let sql = format!("RETURN sequence::nextval('{seq}');");
	for _ in 0..count {
		let res = &mut ds.execute(&sql, &ses, None).await.unwrap();
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
	let res = &mut ds
		.execute(
			"DEFINE SEQUENCE sq1 START -250; DEFINE SEQUENCE sq2 BATCH 50; DEFINE SEQUENCE sq3 BATCH 10 START 1000;",
			&ses,
			None,
		)
		.await?;
	skip_ok(res, 3)?;

	// The number of unique id each task will collect
	let count = 1000;

	// Run 3 tasks collecting the next value of the sequence
	let task11 = tokio::spawn(concurrent_task(ds.clone(), "sq1", count));
	let task12 = tokio::spawn(concurrent_task(ds.clone(), "sq1", count));
	let task13 = tokio::spawn(concurrent_task(ds.clone(), "sq1", count));
	let task21 = tokio::spawn(concurrent_task(ds.clone(), "sq2", count));
	let task22 = tokio::spawn(concurrent_task(ds.clone(), "sq2", count));
	let task31 = tokio::spawn(concurrent_task(ds.clone(), "sq3", count));
	let (set11, set12, set13, set21, set22, set31) =
		tokio::try_join!(task11, task12, task13, task21, task22, task31).expect("Tasks failed");

	// Check that each set has the expected number of unique numbers
	assert_eq!(set11.len(), count);
	assert_eq!(set12.len(), count);
	assert_eq!(set13.len(), count);
	assert_eq!(set11.len(), count);
	assert_eq!(set12.len(), count);
	assert_eq!(set13.len(), count);

	// Check that merged sets have unique numbers
	let set1: BTreeSet<i64> =
		set11.into_iter().chain(set12.into_iter()).chain(set13.into_iter()).collect();
	assert_eq!(set1.len(), count * 3);
	assert_eq!(set1.first().cloned(), Some(-250));
	assert_eq!(set1.last().cloned(), Some(2749));

	let set2: BTreeSet<i64> = set21.into_iter().chain(set22.into_iter()).collect();
	assert_eq!(set2.len(), count * 2);
	assert_eq!(set2.first().cloned(), Some(0));
	assert_eq!(set2.last().cloned(), Some(1999));

	let set3: BTreeSet<i64> = set31.into_iter().collect();
	assert_eq!(set3.len(), count);
	assert_eq!(set3.first().cloned(), Some(1000));
	assert_eq!(set3.last().cloned(), Some(1999));

	Ok(())
}
