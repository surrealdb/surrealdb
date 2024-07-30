//! Different embedded and remote database engines

pub mod any;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
))]
pub mod local;
pub mod proto;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;
#[doc(hidden)]
pub mod tasks;

use futures::Stream;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use surrealdb_core::sql::Values as CoreValues;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Instant;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Interval;
#[cfg(target_arch = "wasm32")]
use wasmtimer::std::Instant;
#[cfg(target_arch = "wasm32")]
use wasmtimer::tokio::Interval;

use super::opt::Resource;
use super::opt::Table;
use super::value::ToCore;

// used in http and all local engines.
#[allow(dead_code)]
fn resource_to_values(r: Resource) -> CoreValues {
	let mut res = CoreValues::default();
	match r {
		Resource::Table(x) => {
			res.0 = vec![Table(x).to_core().into()];
		}
		Resource::RecordId(x) => res.0 = vec![x.to_core().into()],
		Resource::Object(x) => res.0 = vec![x.to_core().into()],
		Resource::Array(x) => res.0 = x.to_core().0,
		Resource::Edge(x) => res.0 = vec![x.to_core().into()],
		Resource::Range(x) => res.0 = vec![x.to_core().into()],
	}
	res
}

struct IntervalStream {
	inner: Interval,
}

impl IntervalStream {
	#[allow(unused)]
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
