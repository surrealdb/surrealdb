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

use bytes::Bytes;

use super::{ListOptions, ObjectKey, ObjectMeta, ObjectStore};
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
		if self.observer.is_noop() {
			return;
		}
		self.observer.on_bucket_operation(&BucketOperationEvent {
			safe: BucketOperationEventSafe {
				backend: self.backend,
				op,
				outcome,
				sent,
				received,
			},
			ctx: BucketOperationEventCtx::default(),
		});
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
	) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let res = self.inner.list(opts).await;
			self.record(BucketOp::List, outcome_of(&res), 0, 0);
			res
		})
	}
}
