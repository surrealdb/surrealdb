//! A statement the datastore stops before it finishes is still slow-logged
//! once it has run past the threshold: when the query timeout aborts it, and
//! when the caller cancels it.

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
