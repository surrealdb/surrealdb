use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;

use crate::helpers::*;

async fn concurrent_task(ds: &Datastore, seq: &str, count: usize) -> HashSet<i64> {
	let mut set = HashSet::new();
	let ses = Session::owner().with_ns("test").with_db("test");
	let sql = format!("RETURN sequence::nextval('{seq}');");
	for _ in 0..count {
		let res = &mut ds.execute(&sql, &ses, None).await.unwrap();
		let val = res.remove(0).result.unwrap().into_int().unwrap();
		set.insert(val);
	}
	set
}

async fn concurrent_task_asc(ds: Arc<Datastore>, seq: &str, count: usize) -> HashSet<i64> {
	concurrent_task(&ds, seq, count).await
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_sequence_next_val() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create the sequence
	let res = &mut ds
		.execute("DEFINE SEQUENCE sq1 START -250; DEFINE SEQUENCE sq2 BATCH 50; DEFINE SEQUENCE sq3 BATCH 10 START 1000;",
			&ses, None)
		.await?;
	skip_ok(res, 3)?;

	// The number of unique IDs each task will collect
	let count = 1000;

	// Run 3 tasks collecting the next value of the sequence
	let task11 = tokio::spawn(concurrent_task_asc(Arc::clone(&ds), "sq1", count));
	let task12 = tokio::spawn(concurrent_task_asc(Arc::clone(&ds), "sq1", count));
	let task13 = tokio::spawn(concurrent_task_asc(Arc::clone(&ds), "sq1", count));
	let task21 = tokio::spawn(concurrent_task_asc(Arc::clone(&ds), "sq2", count));
	let task22 = tokio::spawn(concurrent_task_asc(Arc::clone(&ds), "sq2", count));
	let task31 = tokio::spawn(concurrent_task_asc(Arc::clone(&ds), "sq3", count));
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
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Create the sequence
	let res = &mut ds.execute("DEFINE SEQUENCE sq;", &ses, None).await?;
	skip_ok(res, 1)?;

	// Run 1000 sequence::nextval()
	let set1 = concurrent_task(&ds, "sq", 1000).await;

	// Restart the datastore
	let ds = Arc::new(Arc::into_inner(ds).expect("sole owner of the datastore").restart());

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

/// `sequence::nextval` is dense, so a restart *inside* an allocated window has
/// to resume on the id it stopped at rather than skipping the window's tail.
///
/// The sibling `sequence_next_val_after_restart` takes exactly one batch's
/// worth of values, so it reloads on a window boundary and never exercises
/// this. Here the batch is far larger than the run.
#[tokio::test]
async fn sequence_next_val_after_mid_batch_restart() -> Result<()> {
	const BEFORE: usize = 30;
	const AFTER: usize = 30;

	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let res = &mut ds.execute("DEFINE SEQUENCE sq BATCH 1000;", &ses, None).await?;
	skip_ok(res, 1)?;

	let set1 = concurrent_task(&ds, "sq", BEFORE).await;

	// `restart` keeps the node id, which is what makes the persisted state
	// reachable again: it is keyed per node.
	let ds = Arc::new(Arc::into_inner(ds).expect("sole owner of the datastore").restart());

	let set2 = concurrent_task(&ds, "sq", AFTER).await;

	let set: BTreeSet<i64> = set1.into_iter().chain(set2.into_iter()).collect();
	assert_eq!(set.len(), BEFORE + AFTER, "no id may be handed out twice");
	assert_eq!(set.first().copied(), Some(0));
	assert_eq!(
		set.last().copied(),
		Some((BEFORE + AFTER - 1) as i64),
		"a dense sequence must not skip its window's tail across a restart"
	);

	Ok(())
}

/// `DEFINE SEQUENCE OVERWRITE` repositions a sequence that is already serving.
///
/// The statement resets the sequence on disk, but a node that has served
/// `sequence::nextval` also holds an allocator in memory that owns a reserved
/// window and its own cursor, and the cached path never re-reads `start`. If
/// that allocator is not dropped the statement has no visible effect until the
/// process restarts — which is silent, and leaves `DEFINE SEQUENCE OVERWRITE`
/// unable to repair a sequence that has drifted from the data it feeds.
#[tokio::test(flavor = "multi_thread")]
async fn overwrite_repositions_a_live_sequence() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE SEQUENCE sq BATCH 1 START 1", &ses, None).await?.remove(0).result?;
	// Serve from it, so the allocator is loaded and cached.
	for expected in 1..=2 {
		let got = ds
			.execute("RETURN sequence::nextval('sq')", &ses, None)
			.await?
			.remove(0)
			.result?
			.into_int()
			.unwrap();
		assert_eq!(got, expected);
	}

	ds.execute("DEFINE SEQUENCE OVERWRITE sq BATCH 1 START 500", &ses, None)
		.await?
		.remove(0)
		.result?;

	let got = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(got, 500, "the overwritten start must take effect without a restart");
	Ok(())
}

/// The same repositioning, driven the way an operator repairs a sequence after a
/// restore: raise it past the ids already in the table, then keep writing.
#[tokio::test(flavor = "multi_thread")]
async fn overwrite_repairs_a_sequence_that_collides_with_existing_rows() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	ds.execute(
		"DEFINE SEQUENCE order_id BATCH 1 START 1;
		 DEFINE TABLE thing SCHEMAFULL;
		 DEFINE FIELD id ON thing TYPE int DEFAULT sequence::nextval('order_id');
		 CREATE thing:1 CONTENT {}; CREATE thing:2 CONTENT {}; CREATE thing:3 CONTENT {};",
		&ses,
		None,
	)
	.await?
	.drain(..)
	.try_for_each(|r| r.result.map(|_| ()))?;

	// The sequence still stands at its start, so the next write collides.
	let err = ds.execute("CREATE thing CONTENT {}", &ses, None).await?.remove(0).result;
	assert!(err.is_err(), "the fixture must start from a colliding sequence");

	ds.execute("DEFINE SEQUENCE OVERWRITE order_id BATCH 1 START 4", &ses, None)
		.await?
		.remove(0)
		.result?;

	ds.execute("CREATE thing CONTENT {}", &ses, None)
		.await?
		.remove(0)
		.result
		.map_err(|e| anyhow::anyhow!("repositioned sequence still collided: {e}"))?;
	Ok(())
}

/// `INFO FOR DB STRUCTURE` reports the value a sequence can be restarted from,
/// not just where it was declared to begin.
///
/// `resume` is the lowest value no node has issued, which is what repositioning
/// a sequence needs; `start` keeps meaning what the definition says.
#[tokio::test(flavor = "multi_thread")]
async fn info_structure_reports_the_sequence_position() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE SEQUENCE sq BATCH 1 START 10", &ses, None).await?.remove(0).result?;

	let structure = |ds: Arc<Datastore>, ses: Session| async move {
		let v = ds
			.execute("INFO FOR DB STRUCTURE", &ses, None)
			.await?
			.remove(0)
			.result?
			.into_object()
			.unwrap();
		anyhow::Ok(
			v.get("sequences").unwrap().clone().into_array().unwrap()[0]
				.clone()
				.into_object()
				.unwrap(),
		)
	};

	// Untouched: the bound is the declared start.
	let sq = structure(Arc::clone(&ds), ses.clone()).await?;
	assert_eq!(sq.get("start").unwrap().clone().into_string().unwrap(), "10");
	assert_eq!(sq.get("resume").unwrap().clone().into_string().unwrap(), "10");

	for _ in 0..3 {
		ds.execute("RETURN sequence::nextval('sq')", &ses, None).await?.remove(0).result?;
	}

	// Advanced: `start` still records the declaration, `resume` moves.
	let sq = structure(Arc::clone(&ds), ses.clone()).await?;
	assert_eq!(sq.get("start").unwrap().clone().into_string().unwrap(), "10");
	assert_eq!(sq.get("resume").unwrap().clone().into_string().unwrap(), "13");
	Ok(())
}

/// A sequence can be defined and used inside one transaction.
///
/// The allocator runs its own transactions, so it cannot see what the caller
/// has written and not yet committed. What the caller asked for still has to
/// reach it, or defining a sequence and drawing from it in the same block fails
/// outright.
#[tokio::test(flavor = "multi_thread")]
async fn a_sequence_defined_in_a_transaction_can_be_used_in_it() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let mut res = ds
		.execute(
			"BEGIN; DEFINE SEQUENCE sq BATCH 10 START 7; RETURN sequence::nextval('sq'); COMMIT;",
			&ses,
			None,
		)
		.await?;
	// BEGIN, DEFINE, the allocation, COMMIT.
	let got = res.remove(2).result?.into_int().unwrap();
	assert_eq!(got, 7, "a sequence defined in the same transaction must be usable in it");
	Ok(())
}

/// An `OVERWRITE` of a sequence already in use works inside a transaction.
///
/// The statement clears the allocator's rows and the draw writes its own; both
/// now happen in the one transaction, so there is nothing for them to contend
/// over. Before that they were separate transactions writing the same rows, and
/// the caller's commit lost every time — while the draw, having committed
/// already, consumed an id on each attempt.
#[tokio::test(flavor = "multi_thread")]
async fn an_overwrite_of_a_used_sequence_works_in_a_transaction() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE SEQUENCE sq BATCH 10 START 1;", &ses, None)
		.await?
		.drain(..)
		.try_for_each(|r| r.result.map(|_| ()))?;
	let first = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(first, 1, "the sequence has been drawn from");

	let mut res = ds
		.execute(
			"BEGIN; DEFINE SEQUENCE OVERWRITE sq BATCH 10 START 900; \
			 RETURN sequence::nextval('sq'); COMMIT;",
			&ses,
			None,
		)
		.await?;
	// BEGIN, DEFINE, the allocation, COMMIT.
	let inside = res.remove(2).result?.into_int().unwrap();
	assert_eq!(inside, 900, "the draw comes from the definition the statement wrote");

	let after = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(after, 901, "and the committed sequence carries on from it");
	Ok(())
}

/// A cancelled `OVERWRITE` leaves the sequence exactly where it was.
///
/// What the statement drew is as provisional as the definition it drew from:
/// the cursor and the claim are written in the cancelled transaction, so they
/// go with it. The sequence carries on from the run that is still committed,
/// having lost nothing to a definition that never existed.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_overwrite_leaves_the_sequence_untouched() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE SEQUENCE sq BATCH 10 START 1;", &ses, None)
		.await?
		.drain(..)
		.try_for_each(|r| r.result.map(|_| ()))?;
	let first = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(first, 1);

	ds.execute(
		"BEGIN; DEFINE SEQUENCE OVERWRITE sq BATCH 10 START 5000; \
		 RETURN sequence::nextval('sq'); CANCEL;",
		&ses,
		None,
	)
	.await?;

	let after = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(after, 2, "the cancelled definition left no position behind it");
	Ok(())
}

/// A sequence defined and drawn from in a transaction that cancels leaves
/// nothing behind.
///
/// Nothing committed ever described this sequence, so neither the definition
/// nor the values drawn from it may outlive the statement that wrote it.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_definition_leaves_no_sequence_behind() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	ds.execute(
		"BEGIN; DEFINE SEQUENCE sq BATCH 10 START 7; \
		 RETURN sequence::nextval('sq'); CANCEL;",
		&ses,
		None,
	)
	.await?;

	let info = ds.execute("INFO FOR DB", &ses, None).await?.remove(0).result?;
	assert!(
		!format!("{info:?}").contains("sq"),
		"a cancelled definition left the sequence behind: {info:?}"
	);

	// And drawing from it fails, rather than being served from rows the
	// cancelled statement left in the store.
	let drawn = ds.execute("RETURN sequence::nextval('sq')", &ses, None).await?.remove(0).result;
	assert!(drawn.is_err(), "a cancelled definition still served a value: {drawn:?}");
	Ok(())
}

/// A draw from a definition the transaction commits is visible afterwards,
/// exactly as one from a committed definition is.
///
/// The complement of `a_cancelled_definition_leaves_no_sequence_behind`: what
/// rides the transaction has to arrive when the transaction arrives, or a
/// sequence created and used in one statement would start again from `START`
/// for the next caller and re-issue what it already served.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_definition_keeps_what_it_drew() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let mut res = ds
		.execute(
			"BEGIN; DEFINE SEQUENCE sq BATCH 10 START 7; \
			 RETURN sequence::nextval('sq'); COMMIT;",
			&ses,
			None,
		)
		.await?;
	// BEGIN, DEFINE, the allocation, COMMIT.
	let inside = res.remove(2).result?.into_int().unwrap();
	assert_eq!(inside, 7);

	let after = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(after, 8, "the committed draw was not carried forward");
	Ok(())
}

/// An `OVERWRITE` inside a transaction is what the same transaction allocates
/// from, on a sequence nothing has drawn from yet.
///
/// The allocator reads definitions through transactions of its own, so it
/// cannot see the write; from the store the uncommitted overwrite is
/// indistinguishable from a definition somebody else replaced. Taking the
/// store's answer would serve the run the statement is replacing, silently and
/// with the value the caller asked not to get. So the draw is served by the
/// transaction, from the definition that transaction holds.
///
/// The sibling case — the same statement on a sequence already in use — is
/// `an_overwrite_of_a_used_sequence_works_in_a_transaction`. This one carries
/// the path where there is no cursor to resume from.
#[tokio::test(flavor = "multi_thread")]
async fn an_overwrite_in_a_transaction_is_what_it_allocates_from() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE SEQUENCE sq BATCH 10 START 1;", &ses, None)
		.await?
		.drain(..)
		.try_for_each(|r| r.result.map(|_| ()))?;

	let mut res = ds
		.execute(
			"BEGIN; DEFINE SEQUENCE OVERWRITE sq BATCH 10 START 900; \
			 RETURN sequence::nextval('sq'); COMMIT;",
			&ses,
			None,
		)
		.await?;
	// BEGIN, DEFINE, the allocation, COMMIT.
	let got = res.remove(2).result?.into_int().unwrap();
	assert_eq!(got, 900, "the allocation came from the definition being replaced");

	let after = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(after, 901, "the committed transaction left its allocation consumed");
	Ok(())
}

/// An identical `OVERWRITE` resets a sequence whose window is exhausted, not
/// just one mid-window.
///
/// With `BATCH 1` every value ends a window, so the next call claims before it
/// writes a cursor. The claim has to notice the reset itself: if it commits
/// first, it leaves a window starting at the old position that the rebuild then
/// resumes into, and the sequence carries on from where it was instead of from
/// `START`.
#[tokio::test(flavor = "multi_thread")]
async fn an_identical_overwrite_resets_an_exhausted_window() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	ds.execute("DEFINE SEQUENCE sq BATCH 1 START 1;", &ses, None)
		.await?
		.drain(..)
		.try_for_each(|r| r.result.map(|_| ()))?;
	let first = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(first, 1);

	// Byte-for-byte what it already had, which still clears every row.
	ds.execute("DEFINE SEQUENCE OVERWRITE sq BATCH 1 START 1;", &ses, None)
		.await?
		.remove(0)
		.result?;

	let after = ds
		.execute("RETURN sequence::nextval('sq')", &ses, None)
		.await?
		.remove(0)
		.result?
		.into_int()
		.unwrap();
	assert_eq!(after, 1, "the reset did not take effect; the sequence continued");
	Ok(())
}

/// A savepoint rollback inside the defining transaction must leave a provisional
/// allocator in step with the rows backing it.
///
/// `UPSERT` attempts a create inside a document savepoint and, on a unique-index
/// conflict, rolls it back and retries as an update. A provisional allocator
/// writes through the caller's transaction, so the rollback reaches the rows it
/// wrote — and restores the allocator with them, so the retry draws again from
/// where the savepoint began.
#[tokio::test(flavor = "multi_thread")]
async fn a_savepoint_rollback_does_not_strand_a_provisional_allocator() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let res = &mut ds
		.execute(
			"DEFINE TABLE t SCHEMALESS; DEFINE INDEX uniq ON t FIELDS k UNIQUE; \
			 INSERT INTO t { k: 'dup', n: 0 };",
			&ses,
			None,
		)
		.await?;
	skip_ok(res, 3)?;

	// The create attempt collides on `k` and is retried as an update, which
	// recomputes the data clause and so draws a second time.
	let res = &mut ds
		.execute(
			"BEGIN; \
			 DEFINE SEQUENCE sq BATCH 10 START 1; \
			 UPSERT t SET k = 'dup', n = sequence::nextval('sq'); \
			 COMMIT;",
			&ses,
			None,
		)
		.await?;
	for r in res.iter() {
		if let Err(e) = &r.result {
			panic!("the transaction failed: {e}");
		}
	}

	// The draw the rolled-back create made is undone with it, and the retry
	// draws again. What matters is that the sequence stands above what was
	// stored.
	let res = &mut ds
		.execute("SELECT VALUE n FROM t; RETURN sequence::nextval('sq');", &ses, None)
		.await?;
	let stored = res.remove(0).result?.into_array()?.first().cloned().unwrap().into_int()?;
	let next = res.remove(0).result?.into_int()?;
	assert_eq!(next, stored + 1, "the sequence would reissue the value the record already holds");
	Ok(())
}

/// A retained draw must still be a value the sequence has moved past.
///
/// `ON DUPLICATE KEY UPDATE ... $input` reuses the value computed for the
/// create attempt. `INSERT` computes its data before that attempt's savepoint
/// opens, so the draw is not undone when the attempt rolls back — unlike a
/// draw made inside the savepoint — and the value it returned stays issued.
#[tokio::test(flavor = "multi_thread")]
async fn a_retained_draw_is_not_reissued() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut ds
		.execute(
			"DEFINE TABLE t SCHEMALESS; DEFINE INDEX uniq ON t FIELDS k UNIQUE; \
			 INSERT INTO t { k: 'dup', n: 0 };",
			&ses,
			None,
		)
		.await?;
	skip_ok(res, 3)?;
	let res = &mut ds
		.execute(
			"BEGIN; \
			 DEFINE SEQUENCE sq BATCH 10 START 1; \
			 INSERT INTO t { k: 'dup', n: sequence::nextval('sq') } \
			   ON DUPLICATE KEY UPDATE n = $input.n; \
			 COMMIT;",
			&ses,
			None,
		)
		.await?;
	for r in res.iter() {
		if let Err(e) = &r.result {
			panic!("the transaction failed: {e}");
		}
	}
	let res = &mut ds
		.execute("SELECT VALUE n FROM t; RETURN sequence::nextval('sq');", &ses, None)
		.await?;
	let stored = res.remove(0).result?.into_array()?.first().cloned().unwrap().into_int()?;
	let next = res.remove(0).result?.into_int()?;
	assert_eq!(next, stored + 1, "the sequence would reissue the value the record already holds");
	Ok(())
}

/// A provisional claim that cannot succeed must fail, not spin.
///
/// The batch-allocation retry waits for state another transaction holds to
/// change. A provisional claim runs in the caller's own transaction, so a limit
/// that transaction has already crossed is the same on every attempt: with no
/// query timeout to stop it, retrying never ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_provisional_claim_that_cannot_succeed_fails_rather_than_spinning() -> Result<()> {
	use std::time::Duration;

	// A budget the `DEFINE SEQUENCE` clears — it writes the definition and
	// deletes the two stale ranges — and the claim that follows does not, so
	// the failure lands inside the batch-allocation retry.
	let ds = Datastore::builder()
		.with_capabilities(surrealdb_rpc::capabilities::Capabilities::all())
		.with_config(
			surrealdb_cnf::ConfigMap::empty().with_key_value("transaction_max_write_keys", "4"),
		)
		.build_with_path("memory")
		.await?;
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test;", &Session::owner(), None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let outcome = tokio::time::timeout(
		Duration::from_secs(20),
		ds.execute(
			"BEGIN; DEFINE SEQUENCE sq BATCH 10 START 1; RETURN sequence::nextval('sq'); COMMIT;",
			&ses,
			None,
		),
	)
	.await;
	let Ok(res) = outcome else {
		panic!("a provisional claim with no budget left retried without end");
	};
	// The limit is reported, rather than being retried until something else
	// stops the query.
	let reported = res?
		.iter()
		.filter_map(|r| r.result.as_ref().err().map(|e| e.to_string()))
		.any(|e| e.contains("maximum number of key writes"));
	assert!(reported, "the write-key limit was not surfaced to the caller");
	Ok(())
}

/// The same rollback, with a window the rolled-back draw exhausted.
///
/// `BATCH 1` sends the retry back through `find_batch_allocation`, so the claim
/// path runs again, against the state the rollback restored.
#[tokio::test(flavor = "multi_thread")]
async fn a_savepoint_rollback_does_not_strand_a_provisional_window_rollover() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut ds
		.execute(
			"DEFINE TABLE t SCHEMALESS; DEFINE INDEX uniq ON t FIELDS k UNIQUE; \
			 INSERT INTO t { k: 'dup', n: 0 };",
			&ses,
			None,
		)
		.await?;
	skip_ok(res, 3)?;
	let res = &mut ds
		.execute(
			"BEGIN; \
			 DEFINE SEQUENCE sq BATCH 1 START 1; \
			 UPSERT t SET k = 'dup', n = sequence::nextval('sq'); \
			 COMMIT;",
			&ses,
			None,
		)
		.await?;
	for r in res.iter() {
		if let Err(e) = &r.result {
			panic!("the transaction failed: {e}");
		}
	}
	let res = &mut ds
		.execute("SELECT VALUE n FROM t; RETURN sequence::nextval('sq');", &ses, None)
		.await?;
	let stored = res.remove(0).result?.into_array()?.first().cloned().unwrap().into_int()?;
	let next = res.remove(0).result?.into_int()?;
	assert_eq!(next, stored + 1, "the sequence would reissue the value the record already holds");
	Ok(())
}

/// Draws that a single statement evaluates together must each get their own
/// value, and the first of them must build the allocator alone.
///
/// A function call computes its arguments with `try_join_all`, so two draws
/// from a sequence this transaction has just defined can both find the slot
/// empty. Building claims a window and writes it through the transaction, and
/// a second claim would collide on the row the first created.
#[tokio::test(flavor = "multi_thread")]
async fn draws_evaluated_together_do_not_both_build_the_allocator() -> Result<()> {
	let (_, ds) = new_ds("test", "test", false).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut ds
		.execute(
			"BEGIN; \
			 DEFINE SEQUENCE sq BATCH 10 START 1; \
			 RETURN array::union([sequence::nextval('sq')], [sequence::nextval('sq')]); \
			 COMMIT;",
			&ses,
			None,
		)
		.await?;
	let mut drawn = None;
	for r in res.iter() {
		match &r.result {
			Err(e) => panic!("the transaction failed: {e}"),
			Ok(v) => {
				if let Ok(a) = v.clone().into_array() {
					drawn = Some(a);
				}
			}
		}
	}
	let drawn = drawn.expect("the statement returned no values");
	let mut values: Vec<i64> =
		drawn.into_iter().map(|v| v.into_int().expect("an integer")).collect();
	values.sort_unstable();
	assert_eq!(values, vec![1, 2], "the two draws did not each get their own value");
	Ok(())
}
