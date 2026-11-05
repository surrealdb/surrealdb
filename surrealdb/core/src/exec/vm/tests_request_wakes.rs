//! A request whose statement is served from the statement cache runs to its
//! end in one poll: nothing on its path wakes the request's own task while it
//! is being polled, and nothing returns `Pending`, unless tokio's cooperative
//! budget or the VM's fuel runs out.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use surrealdb_types::{RecordId, Variables};

use crate::exec::operators::test_util::TestDb;

/// Wakes of the task, split by whether the task was being polled.
#[derive(Default)]
struct Wakes {
	polling: AtomicBool,
	during_poll: AtomicUsize,
	outside_poll: AtomicUsize,
}

struct Counting(Arc<Wakes>, Waker);

impl Wake for Counting {
	fn wake(self: Arc<Self>) {
		self.wake_by_ref()
	}

	fn wake_by_ref(self: &Arc<Self>) {
		match self.0.polling.load(Ordering::Relaxed) {
			true => self.0.during_poll.fetch_add(1, Ordering::Relaxed),
			false => self.0.outside_poll.fetch_add(1, Ordering::Relaxed),
		};
		self.1.wake_by_ref()
	}
}

/// Polls `inner` under a waker that counts into `wakes`, and counts the polls
/// that answered `Pending`.
struct Probe<'a, F> {
	inner: Pin<Box<F>>,
	wakes: Arc<Wakes>,
	pending: &'a AtomicUsize,
}

impl<F: Future> Future for Probe<'_, F> {
	type Output = F::Output;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
		let waker = Waker::from(Arc::new(Counting(Arc::clone(&self.wakes), cx.waker().clone())));
		self.wakes.polling.store(true, Ordering::Relaxed);
		let polled = self.inner.as_mut().poll(&mut Context::from_waker(&waker));
		self.wakes.polling.store(false, Ordering::Relaxed);
		if polled.is_pending() {
			self.pending.fetch_add(1, Ordering::Relaxed);
		}
		polled
	}
}

#[tokio::test]
async fn a_cached_point_read_never_wakes_itself() {
	let db = TestDb::new(
		"DEFINE TABLE pk SCHEMAFULL;
		 DEFINE FIELD name ON pk TYPE string;
		 FOR $i IN 0..20 { CREATE type::record('pk', $i) SET name = 'n' };",
	)
	.await;
	let (ds, owner) = (db.datastore(), TestDb::owner());
	let read = |n: i64| {
		let mut vars = Variables::new();
		vars.insert("id", RecordId::new("pk", n));
		ds.execute("SELECT * FROM $id", &owner, Some(vars))
	};
	// The first request compiles the statement; the rest are served it.
	read(0).await.expect("the request runs");
	let before = ds.statement_cache_stats();
	let (wakes, pending) = (Arc::new(Wakes::default()), AtomicUsize::new(0));
	for n in 1..20 {
		// A full quantum, so the VM has no reason to yield.
		crate::exec::vm::fuel::refill(4096);
		let probe = Probe {
			inner: Box::pin(read(n)),
			wakes: Arc::clone(&wakes),
			pending: &pending,
		};
		let results = tokio::task::unconstrained(probe).await.expect("the request runs");
		let row = results.into_iter().next().expect("one result").result.expect("a row");
		assert!(!row.is_none(), "pk:{n} is read");
	}
	assert_eq!(ds.statement_cache_stats().hits - before.hits, 19, "every request is a cache hit");
	assert_eq!(wakes.during_poll.load(Ordering::Relaxed), 0, "no request wakes itself");
	assert_eq!(pending.load(Ordering::Relaxed), 0, "every request ends in its first poll");
	assert_eq!(wakes.outside_poll.load(Ordering::Relaxed), 0, "nothing wakes a finished request");
}
