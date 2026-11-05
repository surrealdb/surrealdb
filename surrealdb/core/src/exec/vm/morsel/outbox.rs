//! The rows a running Range morsel hands the coordinator before it ends.
//!
//! A morsel whose pipeline emits rows to the statement's output (`Combine::Concat`) puts the rows
//! of each chunk it reads into its outbox, a bounded single-producer queue of the chunk buffers
//! themselves. The coordinator takes the chunks of the morsel at the front of the scan, or of any
//! running morsel where the merge takes results as morsels finish, as they are produced, so the
//! rows flow to the reader while the morsel runs. A morsel that is not taken from stops filling its
//! outbox once the bytes it holds reach the window and parks, exactly as a morsel that holds its
//! output until it ends does.
//!
//! The coordinator hands each emptied buffer back ([`Outbox::recycle`]), and the morsel reads its
//! next chunk into it, so streaming allocates no buffer per row and holds at most the window
//! plus one chunk of rows per morsel.
//!
//! A chunk waits with the [`Grant`] that charges its bytes to the run's fan-out lease, and the
//! grant is released with the chunk: when the coordinator takes it, when the morsel ends and
//! [`Outbox::finish`] hands what is left on as the morsel's output, and when the outbox drops with
//! chunks in it. A chunk the lease refused waits without one.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::lease::Grant;
use crate::val::Value;

/// The chunks a morsel has produced that the coordinator has not taken, oldest first.
#[derive(Default)]
pub(super) struct Outbox {
	queue: Mutex<Queue>,
	/// The bytes the chunks in the queue weigh, which the morsel holds against its window.
	held: AtomicUsize,
	/// Rung when the coordinator takes chunks, for a producer that waits for room in the window.
	taken: tokio::sync::Notify,
}

#[derive(Default)]
struct Queue {
	/// The chunks, each with the bytes it weighs and the grant that charges them to the lease.
	chunks: VecDeque<(Vec<Value>, usize, Option<Grant>)>,
	/// An emptied buffer the coordinator returned for the morsel's next chunk.
	spare: Option<Vec<Value>>,
}

impl Outbox {
	fn queue(&self) -> std::sync::MutexGuard<'_, Queue> {
		self.queue.lock().unwrap_or_else(|e| e.into_inner())
	}

	/// The bytes the morsel's outbox holds for the coordinator to take.
	pub(super) fn held(&self) -> usize {
		self.held.load(Ordering::Acquire)
	}

	/// A buffer for the morsel's next chunk: the one the coordinator returned, or a new one.
	pub(super) fn buffer(&self) -> Vec<Value> {
		self.queue().spare.take().unwrap_or_default()
	}

	/// Put the rows of a chunk, which weigh `bytes` and are charged to the lease by `grant`, at the
	/// back of the queue.
	pub(super) fn put(&self, rows: Vec<Value>, bytes: usize, grant: Option<Grant>) {
		if rows.is_empty() {
			self.recycle(rows);
			return;
		}
		self.queue().chunks.push_back((rows, bytes, grant));
		self.held.fetch_add(bytes, Ordering::Release);
	}

	/// The oldest chunk, if the queue holds one, whose grant is released.
	pub(super) fn take(&self) -> Option<Vec<Value>> {
		let (chunk, bytes, grant) = self.queue().chunks.pop_front()?;
		self.held.fetch_sub(bytes, Ordering::Release);
		drop(grant);
		self.taken.notify_waiters();
		Some(chunk)
	}

	/// Every row still queued, in order, followed by `last`: what a morsel that ends hands the
	/// merge as its output. The grants of the chunks are released.
	pub(super) fn finish(&self, mut last: Vec<Value>) -> Vec<Value> {
		let mut queue = self.queue();
		if queue.chunks.is_empty() {
			return last;
		}
		let rows: usize =
			queue.chunks.iter().map(|(chunk, ..)| chunk.len()).sum::<usize>() + last.len();
		let mut all = Vec::with_capacity(rows);
		for (mut chunk, _, _grant) in queue.chunks.drain(..) {
			all.append(&mut chunk);
		}
		all.append(&mut last);
		self.held.store(0, Ordering::Release);
		self.taken.notify_waiters();
		all
	}

	/// The signal that the coordinator has taken chunks, to be enabled before [`Self::held`] is
	/// read so that a take in between is not missed.
	pub(super) fn taken(&self) -> tokio::sync::futures::Notified<'_> {
		self.taken.notified()
	}

	/// Return an emptied chunk buffer for the morsel's next chunk.
	pub(super) fn recycle(&self, mut buffer: Vec<Value>) {
		buffer.clear();
		let mut queue = self.queue();
		if queue.spare.as_ref().is_none_or(|spare| spare.capacity() < buffer.capacity()) {
			queue.spare = Some(buffer);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn rows(n: i64) -> Vec<Value> {
		(0..n).map(Value::from).collect()
	}

	/// Chunks leave in the order they were put, the bytes held follow them, and what is left when
	/// the morsel ends comes out in order ahead of its last rows.
	#[test]
	fn chunks_are_taken_in_order_and_the_rest_ends_the_morsel_output() {
		let outbox = Outbox::default();
		let weigh = |rows: &[Value]| rows.iter().map(crate::exec::memory::size::value_bytes).sum();
		let (a, b, c) = (rows(3), rows(2), rows(4));
		let (wa, wb, wc): (usize, usize, usize) = (weigh(&a), weigh(&b), weigh(&c));
		outbox.put(a, wa, None);
		outbox.put(b, wb, None);
		assert_eq!(outbox.held(), wa + wb);
		let first = outbox.take().expect("a chunk");
		assert_eq!(first.len(), 3);
		assert_eq!(outbox.held(), wb);
		outbox.recycle(first);
		assert!(outbox.buffer().capacity() >= 3, "the emptied buffer comes back");
		outbox.put(c, wc, None);
		let all = outbox.finish(rows(1));
		assert_eq!(all.len(), 2 + 4 + 1);
		assert_eq!(outbox.held(), 0);
		assert!(outbox.take().is_none());
	}

	/// A query's memory over a node of `limit` bytes, whose lease the grants are drawn from.
	fn memory(limit: usize) -> std::sync::Arc<crate::exec::memory::QueryMemory> {
		use crate::exec::memory::{FairGovernor, MemoryHost, MemorySettings};
		MemoryHost {
			governor: std::sync::Arc::new(FairGovernor::new(limit)),
			store: None,
			settings: MemorySettings {
				floor: 100,
				fan_in: 2,
				chunk: 64,
				wait: std::time::Duration::ZERO,
				max: usize::MAX,
			},
		}
		.query()
	}

	/// What a chunk of the outbox holds of the lease is held while it waits and returned the way
	/// it leaves: taken by the coordinator, handed on as the output of a morsel that ends, or
	/// dropped with the outbox.
	#[tokio::test]
	async fn a_chunk_holds_its_grant_until_it_leaves_the_outbox() {
		use super::super::lease::FanoutLease;
		let ctx = crate::exec::operators::test_util::root_ctx();
		let memory = memory(1 << 20);
		let lease = FanoutLease::new(Some(std::sync::Arc::clone(&memory)));
		let queue = |outbox: &Outbox, bytes: usize| {
			let grant = lease.grant_queued(bytes, &ctx).expect("within the node's limit");
			outbox.put(rows(2), bytes, Some(grant));
		};
		let pinned = || memory.lease().pinned();

		let outbox = Outbox::default();
		queue(&outbox, 1000);
		queue(&outbox, 2000);
		assert_eq!((lease.held(), pinned()), (3000, 3000));
		assert!(outbox.take().is_some());
		assert_eq!((lease.held(), pinned()), (2000, 2000), "a taken chunk is returned");
		queue(&outbox, 500);
		assert_eq!(outbox.finish(rows(1)).len(), 5);
		assert_eq!((lease.held(), pinned()), (0, 0), "the chunks handed on as output are returned");

		queue(&outbox, 700);
		queue(&outbox, 800);
		assert_eq!(lease.held(), 1500);
		drop(outbox);
		assert_eq!((lease.held(), pinned()), (0, 0), "an outbox that drops returns what it holds");
		assert!(!lease.revoked());
	}

	/// A chunk the lease cannot hold within its grant is refused, which revokes the lease; the
	/// chunk still reaches the coordinator, without a grant, and nothing is held for it.
	#[tokio::test]
	async fn a_refused_chunk_waits_without_a_grant_and_revokes_the_lease() {
		use super::super::lease::FanoutLease;
		let ctx = crate::exec::operators::test_util::root_ctx();
		let memory = memory(10_000);
		let lease = FanoutLease::new(Some(std::sync::Arc::clone(&memory)));
		let outbox = Outbox::default();
		let grant = lease.grant_queued(1 << 20, &ctx);
		assert!(grant.is_none(), "past the node's limit");
		assert!(lease.revoked());
		outbox.put(rows(3), 1 << 20, grant);
		assert_eq!((lease.held(), memory.lease().pinned()), (0, 0));
		assert_eq!(outbox.held(), 1 << 20, "the window still counts the chunk");
		assert_eq!(outbox.take().expect("the chunk").len(), 3);
	}
}
