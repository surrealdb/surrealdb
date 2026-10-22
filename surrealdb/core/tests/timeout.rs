#![recursion_limit = "256"]

mod helpers;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use helpers::Test;
use surrealdb_core::dbs::{Capabilities, Session};
#[allow(unused_imports)]
use surrealdb_core::kvs::Datastore;

/// A writer that appends everything written to it into a shared buffer, so
/// a test can install it as a `tracing` subscriber's output and assert on
/// what got logged.
#[derive(Clone)]
struct VecWriter(Arc<Mutex<Vec<u8>>>);

impl Write for VecWriter {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		self.0.lock().expect("VecWriter mutex poisoned").extend_from_slice(buf);
		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for VecWriter {
	type Writer = Self;

	fn make_writer(&'a self) -> Self::Writer {
		self.clone()
	}
}

#[tokio::test]
async fn statement_timeouts() -> Result<()> {
	let sql = "
		CREATE ONLY person:ok CONTENT { test: true };
		CREATE person:test CONTENT { test: true } TIMEOUT 0s;
		UPSERT person:test CONTENT { test: true } TIMEOUT 0s;
		UPDATE person:test CONTENT { test: true } TIMEOUT 0s;
		INSERT { id: person:test, test: true } TIMEOUT 0s;
		RELATE person:test->know->person:ok TIMEOUT 0s;
		LET $temp = SELECT * FROM person TIMEOUT 0s;
		SELECT * FROM person TIMEOUT 0s;
		DELETE person:test TIMEOUT 0s;
	";
	let error = "The query was not executed because it exceeded the timeout: 0ns";
	Test::new(sql)
		.await?
		.expect_val("{ id: person:ok, test: true }")?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?
		.expect_error(error)?;

	Ok(())
}

async fn new_ds_with_timeout(query: Option<Duration>, transaction: Option<Duration>) -> Datastore {
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_query_timeout(query)
		.with_transaction_timeout(transaction)
		.build_with_path("memory")
		.await
		.expect("success");
	ds.execute("DEFINE NS `test`", &Session::owner(), None).await.expect("success");
	let sess = Session::owner().with_ns("test");
	ds.execute("DEFINE DB `test`", &sess, None).await.expect("success");
	ds
}

#[tokio::test]
async fn query_timeout() -> Result<()> {
	let sql = "
		FOR $i in 0..1000000000{
			FOR $i in 0..1000000000{
				FOR $i in 0..1000000000{
				}
			}
		}
	";
	let ds = new_ds_with_timeout(Some(Duration::from_millis(500)), None).await;
	let session = Session::owner();
	let before = Instant::now();
	let mut res = ds.execute(sql, &session, None).await.unwrap();
	if before.elapsed() > Duration::from_millis(7050) {
		panic!("Query did not properly timeout");
	}
	res.pop().unwrap().result.unwrap_err();

	Ok(())
}

#[tokio::test]
async fn transaction_timeout() -> Result<()> {
	let ds = new_ds_with_timeout(None, Some(Duration::from_millis(500))).await;
	let session = Session::owner().with_ns("test").with_db("test");

	let before = Instant::now();
	let mut res = ds.execute("SLEEP 10s", &session, None).await.unwrap();
	let elapsed = before.elapsed();
	assert!(
		elapsed < Duration::from_secs(5),
		"Transaction timeout was not enforced, took {elapsed:?}"
	);

	let result = res.pop().unwrap().result;
	let err = result.unwrap_err().to_string();
	assert!(err.contains("exceeded the timeout"), "Expected transaction timeout error, got: {err}");

	Ok(())
}

#[tokio::test]
async fn transaction_timeout_begin_commit() -> Result<()> {
	let ds = new_ds_with_timeout(None, Some(Duration::from_millis(500))).await;
	let session = Session::owner().with_ns("test").with_db("test");

	let before = Instant::now();
	let res = ds
		.execute("BEGIN; CREATE person:1 SET name = 'a'; SLEEP 10s; COMMIT;", &session, None)
		.await
		.unwrap();
	let elapsed = before.elapsed();
	assert!(
		elapsed < Duration::from_secs(5),
		"Transaction timeout was not enforced in BEGIN block, took {elapsed:?}"
	);

	let has_timeout_err = res.iter().any(|r| {
		r.result.as_ref().is_err_and(|e| {
			e.to_string().contains("exceeded the timeout") || e.to_string().contains("timed out")
		})
	});
	assert!(has_timeout_err, "Expected transaction timeout error in BEGIN block results: {res:?}");

	Ok(())
}

/// When a batch of statements collectively hits the overall query timeout,
/// the statements that never got a chance to run are correctly reported as
/// timed out, but nothing in the batch individually exceeds the slow-log
/// threshold on its own. The timeout itself is "slow" by definition, so it
/// should still produce exactly one slow-log entry for the batch, not zero
/// (the bug) and not one per unexecuted statement (log spam).
#[tokio::test]
async fn query_timeout_batch_emits_single_slow_log() -> Result<()> {
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_query_timeout(Some(Duration::from_millis(800)))
		.with_slow_log(Duration::from_millis(500), vec![], vec![])
		.build_with_path("memory")
		.await
		.expect("success");
	let session = Session::owner();

	let buf = Arc::new(Mutex::new(Vec::new()));
	let subscriber = tracing_subscriber::fmt().with_writer(VecWriter(Arc::clone(&buf))).finish();
	let guard = tracing::subscriber::set_default(subscriber);

	// Each statement takes 10ms nominally, far below the 500ms slow-log
	// threshold, so none of them trips it on its own even under heavy
	// scheduling jitter. A hundred of them back to back exceed the 800ms
	// query timeout partway through, which is itself comfortably below
	// the slow-log threshold plus the elapsed time at cancellation, so the
	// test only depends on the relative ordering of these three values,
	// not on hitting a precise millisecond boundary.
	let sql = "SLEEP 10ms;".repeat(100);
	let res = ds.execute(&sql, &session, None).await.unwrap();

	drop(guard);

	let timed_out = res
		.iter()
		.filter(|r| {
			r.result.as_ref().is_err_and(|e| e.to_string().contains("exceeded the timeout"))
		})
		.count();
	assert!(timed_out > 0, "expected at least one statement to be timed out, got: {res:?}");

	let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
	let slow_log_lines = output.lines().filter(|l| l.contains("Slow query detected")).count();
	assert_eq!(
		slow_log_lines, 1,
		"expected exactly one slow-log line for the whole batch, got {slow_log_lines}: {output}"
	);
	assert!(
		output.contains("batch did not complete"),
		"expected the batch-cancellation slow-log message, got: {output}"
	);

	Ok(())
}
