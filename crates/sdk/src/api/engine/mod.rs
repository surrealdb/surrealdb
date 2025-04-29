//! Different embedded and remote database engines

pub mod any;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-fdb-7_1",
	feature = "kv-fdb-7_3",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
))]
pub mod local;
pub mod proto;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;
#[doc(hidden)]
pub mod tasks;

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use surrealdb_core::sql::Values as CoreValues;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Interval;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::Interval;

use super::opt::{Resource, Table};
use crate::Value;

// used in http and all local engines.
pub(crate) fn resource_to_values(r: Resource) -> CoreValues {
	let mut res = CoreValues::default();
	match r {
		Resource::Table(x) => {
			res.0 = vec![Table(x).into_core().into()];
		}
		Resource::RecordId(x) => res.0 = vec![x.into_inner().into()],
		Resource::Object(x) => res.0 = vec![x.into_inner().into()],
		Resource::Array(x) => res.0 = Value::array_to_core(x),
		Resource::Edge(x) => res.0 = vec![x.into_inner().into()],
		Resource::Range(x) => res.0 = vec![x.into_inner().into()],
		Resource::Unspecified => {}
	}
	res
}

struct IntervalStream {
	inner: Interval,
}

impl IntervalStream {
	fn new(interval: Interval) -> Self {
		Self {
			inner: interval,
		}
	}
}

impl Stream for IntervalStream {
	type Item = Instant;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Instant>> {
		self.inner.poll_tick(cx).map(Some)
	}
}
