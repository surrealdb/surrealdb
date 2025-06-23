//! Different embedded and remote database engines

pub mod any;
#[cfg(any(
	kv_fdb,
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-surrealkv",
))]
pub mod local;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;
#[doc(hidden)]
pub mod tasks;

use futures::Stream;
use surrealdb_core::expr::Array;
use surrealdb_core::expr::Value;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use surrealdb_core::expr::Values as Values;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Interval;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::Interval;

use super::opt::Resource;
use super::opt::Table;

// // used in http and all local engines.
// #[allow(dead_code)]
// pub(crate) fn resource_to_sql_values(r: Resource) -> SqlValues {
// 	let mut res = SqlValues::default();
// 	match r {
// 		Resource::Table(x) => {
// 			res.0 = vec![Table(x).into_core().into()];
// 		}
// 		Resource::RecordId(x) => res.0 = vec![SqlThing::from(x).into()],
// 		Resource::Object(x) => res.0 = vec![SqlObject::from(x).into()],
// 		Resource::Array(x) => res.0 = Value::Array(Array(x)),
// 		Resource::Edge(x) => res.0 = vec![SqlEdges::from(x.into_inner()).into()],
// 		Resource::Range(x) => res.0 = vec![SqlThing::from(x.into_inner()).into()],
// 		Resource::Unspecified => {}
// 	}
// 	res
// }

// // used in http and all local engines.
// #[allow(dead_code)]
// pub(crate) fn resource_to_values(r: Resource) -> Values {
// 	let mut res = Values::default();
// 	match r {
// 		Resource::Table(x) => {
// 			res.0 = vec![Table(x).into_core().into()];
// 		}
// 		Resource::RecordId(x) => res.0 = vec![x.into()],
// 		Resource::Object(x) => res.0 = vec![x.into()],
// 		Resource::Array(x) => res.0 = x,
// 		Resource::Edge(x) => res.0 = vec![x.into_inner().into()],
// 		Resource::Range(x) => res.0 = vec![x.into_inner().into()],
// 		Resource::Unspecified => {}
// 	}
// 	res
// }

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
