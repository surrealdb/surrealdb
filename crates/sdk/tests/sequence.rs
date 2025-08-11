mod helpers;

use std::collections::BTreeSet;
use std::sync::Arc;

use hashbrown::HashSet;
use helpers::*;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;

async fn concurrent_task(ds: &Datastore, seq: &str, count: usize) -> HashSet<i64> {
	let mut set = HashSet::new();
	let ses = Session::owner().with_ns("test").with_db("test");
	let sql = format!("RETURN sequence::nextval('{seq}');");
	for _ in 0..count {
		let res = &mut ds.execute(&sql, &ses, None).await.unwrap();
		let val = res.remove(0).result.unwrap().coerce_to().unwrap();
		set.insert(val);
	}
	set
}

async fn concurrent_task_asc(ds: Arc<Datastore>, seq: &str, count: usize) -> HashSet<i64> {
	concurrent_task(&ds, seq, count).await
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_sequence_next_val() -> Result<()> {
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

	// The number of unique IDs each task will collect
	let count = 1000;

	// Run 3 tasks collecting the next value of the sequence
	let task11 = tokio::spawn(concurrent_task_asc(ds.clone(), "sq1", count));
	let task12 = tokio::spawn(concurrent_task_asc(ds.clone(), "sq1", count));
	let task13 = tokio::spawn(concurrent_task_asc(ds.clone(), "sq1", count));
	let task21 = tokio::spawn(concurrent_task_asc(ds.clone(), "sq2", count));
	let task22 = tokio::spawn(concurrent_task_asc(ds.clone(), "sq2", count));
	let task31 = tokio::spawn(concurrent_task_asc(ds.clone(), "sq3", count));
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
	assert_eq!(set1.first().copied(), Some(-250));
	assert_eq!(set1.last().copied(), Some(2749));

	let set2: BTreeSet<i64> = set21.into_iter().chain(set22.into_iter()).collect();
	assert_eq!(set2.len(), count * 2);
	assert_eq!(set2.first().copied(), Some(0));
	assert_eq!(set2.last().copied(), Some(1999));

	let set3: BTreeSet<i64> = set31.into_iter().collect();
	assert_eq!(set3.len(), count);
	assert_eq!(set3.first().copied(), Some(1000));
	assert_eq!(set3.last().copied(), Some(1999));

	Ok(())
}

#[tokio::test]
async fn sequence_next_val_after_restart() -> Result<()> {
	let ds = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create the sequence
	let res = &mut ds.execute("DEFINE SEQUENCE sq;", &ses, None).await?;
	skip_ok(res, 1)?;

	// Run 1000 sequence::nextval()
	let set1 = concurrent_task(&ds, "sq", 1000).await;

	// Restart the datastore
	let ds = ds.restart();

	// Run again 1000 sequence::nextval()
	let set2 = concurrent_task(&ds, "sq", 1000).await;

	// Let's merge the 2 sets
	let set: BTreeSet<i64> = set1.into_iter().chain(set2.into_iter()).collect();
	// We should have 2000 unique numbers
	assert_eq!(set.len(), 2000);
	// They should be consecutive
	assert_eq!(set.first().copied(), Some(0));
	assert_eq!(set.last().copied(), Some(1999));

	Ok(())
}
