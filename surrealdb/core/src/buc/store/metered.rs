//! Metered object store wrapper.
//!
//! Wraps any [`ObjectStore`] and reports each operation to the datastore's
//! [`ExecutionObserver`] as a [`BucketOperationEvent`], so the server can
//! maintain `surrealdb.bucket.*` traffic counters (bytes in/out and operation
//! counts, labelled by backend/op/outcome). When no observer is attached the
//! wrapper is a thin pass-through — the recording is skipped via
//! [`ExecutionObserver::is_noop`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll};

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use futures::{Stream, TryStreamExt};

use super::{
	ByteRange, ListEntry, ListOptions, ObjectKey, ObjectMeta, ObjectStore, ObjectStream, PutError,
	PutOptions, PutResult,
};
use crate::observe::{
	BucketOp, BucketOperationEvent, BucketOperationEventCtx, BucketOperationEventSafe,
	ExecutionObserver, Outcome,
};

/// An [`ObjectStore`] decorator that records per-operation traffic metrics.
pub(crate) struct MeteredObjectStore {
	inner: Arc<dyn ObjectStore>,
	observer: Arc<dyn ExecutionObserver>,
	/// Fixed backend label (`s3` / `gcs` / `azure` / `file` / `memory` / …).
	backend: &'static str,
}

impl MeteredObjectStore {
	/// Wrap `inner`, reporting operations to `observer` under the `backend` label.
	pub(crate) fn new(
		inner: Arc<dyn ObjectStore>,
		observer: Arc<dyn ExecutionObserver>,
		backend: &'static str,
	) -> Self {
		Self {
			inner,
			observer,
			backend,
		}
	}

	/// Emit a bucket-operation event, unless the observer is a no-op.
	fn record(&self, op: BucketOp, outcome: Outcome, sent: u64, received: u64) {
		emit(&*self.observer, self.backend, op, outcome, sent, received);
	}
}

/// Emit a bucket-operation event, unless the observer is a no-op.
///
/// A free function rather than only a method, because [`MeteredStream`] outlives
/// the borrow of the store it came from and so cannot call through `&self`.
fn emit(
	observer: &dyn ExecutionObserver,
	backend: &'static str,
	op: BucketOp,
	outcome: Outcome,
	sent: u64,
	received: u64,
) {
	if observer.is_noop() {
		return;
	}
	observer.on_bucket_operation(&BucketOperationEvent {
		safe: BucketOperationEventSafe {
			backend,
			op,
			outcome,
			sent,
			received,
		},
		ctx: BucketOperationEventCtx::default(),
	});
}

/// Counts a streamed read and reports one event when it ends.
///
/// A streamed read reports at the end of the transfer, not at the call that
/// began it: the byte count is unknown in advance. The event carries the bytes
/// that actually moved, so a transfer that fails or is abandoned part way
/// through is still counted for what it moved, with [`Outcome::Error`].
///
/// The `Drop` arm is not redundant with the terminal arm of `poll_next`. A
/// client that disconnects mid-download leaves the stream dropped without ever
/// yielding `None`, and that is the common way a large transfer ends.
struct MeteredStream {
	inner: BoxStream<'static, Result<Bytes, String>>,
	observer: Arc<dyn ExecutionObserver>,
	backend: &'static str,
	op: BucketOp,
	count: u64,
	/// `false` until the stream yields `None`, so a drop before that is an
	/// abandoned transfer rather than a completed one.
	completed: bool,
	/// Guards against reporting twice when a stream is polled to its end and
	/// then dropped.
	reported: bool,
}

impl MeteredStream {
	fn report(&mut self) {
		if self.reported {
			return;
		}
		self.reported = true;

		let outcome = if self.completed {
			Outcome::Success
		} else {
			Outcome::Error
		};
		emit(&*self.observer, self.backend, self.op, outcome, 0, self.count);
	}
}

/// Accounting for a streamed write, reported once however the write ends.
///
/// The counter is shared with the stream that feeds the store, so the event
/// carries what actually moved even when the commit then fails, and even when
/// nothing returns at all because the future was dropped.
struct MeteredWrite {
	observer: Arc<dyn ExecutionObserver>,
	backend: &'static str,
	counter: Arc<AtomicU64>,
	/// The outcome to report. `Error` until the write returns, so a dropped
	/// future reports the failure it is.
	outcome: Outcome,
	reported: bool,
}

impl MeteredWrite {
	fn report(&mut self) {
		if self.reported {
			return;
		}
		self.reported = true;
		emit(
			&*self.observer,
			self.backend,
			BucketOp::Put,
			self.outcome,
			self.counter.load(Ordering::Relaxed),
			0,
		);
	}
}

impl Drop for MeteredWrite {
	fn drop(&mut self) {
		self.report();
	}
}

impl Stream for MeteredStream {
	type Item = Result<Bytes, String>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();
		match this.inner.as_mut().poll_next(cx) {
			Poll::Ready(Some(Ok(bytes))) => {
				this.count += bytes.len() as u64;
				Poll::Ready(Some(Ok(bytes)))
			}
			Poll::Ready(Some(Err(e))) => {
				this.report();
				Poll::Ready(Some(Err(e)))
			}
			Poll::Ready(None) => {
				this.completed = true;
				this.report();
				Poll::Ready(None)
			}
			Poll::Pending => Poll::Pending,
		}
	}
}

impl Drop for MeteredStream {
	fn drop(&mut self) {
		self.report();
	}
}

/// Map a `Result` to its [`Outcome`].
fn outcome_of<T, E>(res: &Result<T, E>) -> Outcome {
	match res {
		Ok(_) => Outcome::Success,
		Err(_) => Outcome::Error,
	}
}

impl ObjectStore for MeteredObjectStore {
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let sent = data.len() as u64;
		Box::pin(async move {
			let res = self.inner.put(key, data).await;
			self.record(
				BucketOp::Put,
				outcome_of(&res),
				if res.is_ok() {
					sent
				} else {
					0
				},
				0,
			);
			res
		})
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		let sent = data.len() as u64;
		Box::pin(async move {
			let res = self.inner.put_if_not_exists(key, data).await;
			self.record(
				BucketOp::PutIfNotExists,
				outcome_of(&res),
				if res.is_ok() {
					sent
				} else {
					0
				},
				0,
			);
			res
		})
	}

	fn supports_content_type(&self) -> bool {
		self.inner.supports_content_type()
	}

	fn put_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: BoxStream<'static, Result<Bytes, String>>,
		opts: PutOptions,
	) -> Pin<Box<dyn Future<Output = Result<PutResult, PutError>> + Send + 'a>> {
		// A write reports from the future rather than from the stream, unlike a
		// read: the store can consume every byte and still fail to commit them,
		// so the stream reaching its end is not the outcome. The counter is
		// shared so the event carries what actually moved even when the commit
		// then fails.
		let counter = Arc::new(AtomicU64::new(0));
		let counted = {
			let counter = Arc::clone(&counter);
			data.inspect_ok(move |chunk| {
				counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
			})
			.boxed()
		};

		Box::pin(async move {
			// Reported from a guard rather than after the await, for the same
			// reason `MeteredStream` carries one: this future is dropped when a
			// client disconnects mid-upload or the node shuts down, and that is
			// a common way a large transfer ends. Reporting only on the path
			// that returns would leave those transfers uncounted entirely --
			// neither the bytes nor the failure.
			let mut report = MeteredWrite {
				observer: Arc::clone(&self.observer),
				backend: self.backend,
				counter: Arc::clone(&counter),
				outcome: Outcome::Error,
				reported: false,
			};
			let res = self.inner.put_streamed(key, counted, opts).await;
			report.outcome = outcome_of(&res);
			report.report();
			res
		})
	}

	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.get(key).await;
			let received = match &res {
				Ok(Some(bytes)) => bytes.len() as u64,
				_ => 0,
			};
			self.record(BucketOp::Get, outcome_of(&res), 0, received);
			res
		})
	}

	fn get_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		range: Option<ByteRange>,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectStream>, String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.get_streamed(key, range).await;

			// Only a failure to *open* the object is reported here. Once a
			// stream exists, `MeteredStream` owns the reporting, so that the
			// event carries the bytes that actually moved.
			let Some(mut stream) = (match res {
				Ok(v) => v,
				Err(e) => {
					self.record(BucketOp::Get, Outcome::Error, 0, 0);
					return Err(e);
				}
			}) else {
				self.record(BucketOp::Get, Outcome::Success, 0, 0);
				return Ok(None);
			};

			stream.stream = MeteredStream {
				inner: stream.stream,
				observer: Arc::clone(&self.observer),
				backend: self.backend,
				op: BucketOp::Get,
				count: 0,
				completed: false,
				reported: false,
			}
			.boxed();

			Ok(Some(stream))
		})
	}

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.head(key).await;
			self.record(BucketOp::Head, outcome_of(&res), 0, 0);
			res
		})
	}

	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.delete(key).await;
			self.record(BucketOp::Delete, outcome_of(&res), 0, 0);
			res
		})
	}

	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.exists(key).await;
			self.record(BucketOp::Exists, outcome_of(&res), 0, 0);
			res
		})
	}

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.copy(key, target).await;
			self.record(BucketOp::Copy, outcome_of(&res), 0, 0);
			res
		})
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.copy_if_not_exists(key, target).await;
			self.record(BucketOp::CopyIfNotExists, outcome_of(&res), 0, 0);
			res
		})
	}

	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.rename(key, target).await;
			self.record(BucketOp::Rename, outcome_of(&res), 0, 0);
			res
		})
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.rename_if_not_exists(key, target).await;
			self.record(BucketOp::RenameIfNotExists, outcome_of(&res), 0, 0);
			res
		})
	}

	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ListEntry>, String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.list(opts).await;
			self.record(BucketOp::List, outcome_of(&res), 0, 0);
			res
		})
	}
}
