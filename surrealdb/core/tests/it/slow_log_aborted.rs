//! A statement the datastore stops before it finishes is still slow-logged
//! once it has run past the threshold: when the query timeout aborts it, and
//! when the caller cancels it. A batch whose statements each finish under the
//! threshold, but which is stopped part-way once the batch as a whole has run
//! past it, is slow-logged once for the batch.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};

const SLOW: Duration = Duration::from_millis(300);

#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl CapturedLog {
	fn contents(&self) -> String {
		String::from_utf8_lossy(&self.0.lock().expect("the log lock is never poisoned"))
			.into_owned()
	}
}

impl std::io::Write for CapturedLog {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		self.0.lock().expect("the log lock is never poisoned").extend_from_slice(buf);
		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
	type Writer = CapturedLog;

	fn make_writer(&'a self) -> Self::Writer {
		self.clone()
	}
}

fn capture() -> (CapturedLog, tracing::subscriber::DefaultGuard) {
	let log = CapturedLog::default();
	let subscriber = tracing_subscriber::fmt()
		.with_writer(log.clone())
		.with_max_level(tracing::Level::WARN)
		.with_ansi(false)
		.finish();
	(log, tracing::subscriber::set_default(subscriber))
}

async fn datastore(timeout: Option<Duration>) -> Result<Arc<Datastore>> {
	Datastore::builder()
		.without_maintenance_tasks()
		.with_slow_log(SLOW, Vec::new(), Vec::new())
		.with_query_timeout(timeout)
		.build_with_path("memory")
		.await
}

/// On a current-thread runtime, so the subscriber this test installs for its
/// own thread sees the statement wherever the runtime polls it.
#[tokio::test]
async fn a_statement_aborted_by_the_query_timeout_is_slow_logged() -> Result<()> {
	let (log, _default) = capture();
	let ds = datastore(Some(Duration::from_secs(1))).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = ds.execute("SLEEP 5s", &ses, None).await?;
	assert!(res.iter().all(|r| r.result.is_err()), "the statement is aborted by the timeout");
	let log = log.contents();
	assert!(log.contains("Slow query detected") && log.contains("SLEEP"), "{log}");
	Ok(())
}

#[tokio::test]
async fn a_statement_cancelled_by_its_caller_is_slow_logged() -> Result<()> {
	let (log, _default) = capture();
	let ds = datastore(None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let cancel = CancelHandle::new();
	let tripper = cancel.clone();
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_secs(1)).await;
		tripper.trip();
	});
	let req = QueryRequest::new("SLEEP 5s", &ses).with_cancel(cancel);
	let res = ds.run(req).await?;
	assert!(res.iter().all(|r| r.result.is_err()), "the statement is cancelled");
	let log = log.contents();
	assert!(log.contains("Slow query detected") && log.contains("SLEEP"), "{log}");
	Ok(())
}

/// The lines of `log` that are slow-log entries.
fn entries(log: &str) -> Vec<&str> {
	log.lines().filter(|line| line.contains("Slow query detected")).collect()
}

/// The duration an entry reports in its `time: …` field.
fn entry_time(entry: &str) -> Option<Duration> {
	let rest = entry.split("time: ").nth(1)?;
	let value = rest.split(' ').next()?;
	let (number, scale) = if let Some(ms) = value.strip_suffix("ms") {
		(ms, 1e-3)
	} else if let Some(us) = value.strip_suffix("µs") {
		(us, 1e-6)
	} else if let Some(ns) = value.strip_suffix("ns") {
		(ns, 1e-9)
	} else {
		(value.strip_suffix('s')?, 1.0)
	};
	Some(Duration::from_secs_f64(number.parse::<f64>().ok()? * scale))
}

/// The batch entries of `log`, after checking that every other entry is for a
/// statement that itself ran past the threshold. A statement that should take
/// a fraction of the threshold can still overrun it on a starved scheduler, and
/// its entry is then correct, so only the batch entry is counted.
fn batch_entries(log: &str) -> Vec<&str> {
	let (batch, statements): (Vec<&str>, Vec<&str>) =
		entries(log).into_iter().partition(|entry| entry.contains("batch did not complete"));
	for entry in statements {
		assert!(entry_time(entry).is_some_and(|time| time >= SLOW), "{log}");
	}
	batch
}

/// Each statement takes a fraction of the threshold, so none is slow-logged
/// on its own unless the scheduler starves it (see [`batch_entries`]), and
/// together they run well past the timeout, which itself is past the
/// threshold. The test depends only on that ordering of the three
/// durations, not on where in the batch the timeout lands.
fn sleeps(n: usize) -> String {
	"SLEEP 20ms;".repeat(n)
}

#[tokio::test]
async fn a_batch_cut_short_by_the_query_timeout_is_slow_logged_once() -> Result<()> {
	let (log, _default) = capture();
	let ds = datastore(Some(Duration::from_secs(1))).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = ds.execute(&sleeps(100), &ses, None).await?;
	assert!(res.iter().any(|r| r.result.is_ok()), "the first statements run");
	assert!(res.last().is_some_and(|r| r.result.is_err()), "the last statement is timed out");
	let log = log.contents();
	let entries = batch_entries(&log);
	assert_eq!(entries.len(), 1, "{log}");
	assert!(entries[0].contains("batch did not complete (timed out after 1s)"), "{log}");
	assert!(entries[0].contains("query: SLEEP 20ms"), "{log}");
	Ok(())
}

#[tokio::test]
async fn a_transaction_block_cut_short_by_the_query_timeout_is_slow_logged_once() -> Result<()> {
	let (log, _default) = capture();
	let ds = datastore(Some(Duration::from_secs(1))).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = ds.execute(&format!("BEGIN; {} COMMIT;", sleeps(100)), &ses, None).await?;
	assert!(res.iter().skip(1).all(|r| r.result.is_err()), "the block is rolled back");
	let log = log.contents();
	let entries = batch_entries(&log);
	assert_eq!(entries.len(), 1, "{log}");
	assert!(entries[0].contains("batch did not complete (timed out after 1s)"), "{log}");
	Ok(())
}

#[tokio::test]
async fn a_batch_cancelled_by_its_caller_is_slow_logged_once() -> Result<()> {
	let (log, _default) = capture();
	let ds = datastore(None).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let cancel = CancelHandle::new();
	let tripper = cancel.clone();
	tokio::spawn(async move {
		tokio::time::sleep(Duration::from_secs(1)).await;
		tripper.trip();
	});
	let sql = sleeps(100);
	let req = QueryRequest::new(sql.as_str(), &ses).with_cancel(cancel);
	let res = ds.run(req).await?;
	assert!(res.last().is_some_and(|r| r.result.is_err()), "the last statement is cancelled");
	let log = log.contents();
	let entries = batch_entries(&log);
	assert_eq!(entries.len(), 1, "{log}");
	assert!(entries[0].contains("batch did not complete (cancelled)"), "{log}");
	Ok(())
}

/// The statement the timeout cut short already has an entry of its own, so
/// the statements after it add none for the batch.
#[tokio::test]
async fn a_slow_logged_statement_cut_short_is_not_logged_again_for_its_batch() -> Result<()> {
	let (log, _default) = capture();
	let ds = datastore(Some(Duration::from_secs(1))).await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = ds.execute("SLEEP 5s; SLEEP 1ms; SLEEP 1ms;", &ses, None).await?;
	assert!(res.iter().all(|r| r.result.is_err()), "every statement is timed out");
	let log = log.contents();
	let entries = entries(&log);
	assert_eq!(entries.len(), 1, "{log}");
	assert!(entries[0].contains("query: SLEEP 5s"), "{log}");
	Ok(())
}
