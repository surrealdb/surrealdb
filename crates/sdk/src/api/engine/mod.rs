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
pub(crate) mod proto;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;
#[doc(hidden)]
pub mod tasks;

use futures::Stream;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use surrealdb_core::expr::Values as CoreValues;
use surrealdb_core::sql::SqlValues as CoreSqlValues;
use surrealdb_core::sql::{Edges as CoreSqlEdges, Object as CoreSqlObject, Thing as CoreSqlThing};
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Interval;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::Interval;

use crate::Value;

use super::opt::Resource;
use super::opt::Table;

// used in http and all local engines.
#[allow(dead_code)]
pub(crate) fn resource_to_sql_values(r: Resource) -> CoreSqlValues {
	let mut res = CoreSqlValues::default();
	match r {
		Resource::Table(x) => {
			res.0 = vec![Table(x).into_core_sql().into()];
		}
		Resource::RecordId(x) => res.0 = vec![CoreSqlThing::from(x.into_inner()).into()],
		Resource::Object(x) => res.0 = vec![CoreSqlObject::from(x.into_inner()).into()],
		Resource::Array(x) => res.0 = Value::array_to_core(x).into_iter().map(Into::into).collect(),
		Resource::Edge(x) => res.0 = vec![CoreSqlEdges::from(x.into_inner()).into()],
		Resource::Range(x) => res.0 = vec![CoreSqlThing::from(x.into_inner()).into()],
		Resource::Unspecified => {}
	}
	res
}

// used in http and all local engines.
#[allow(dead_code)]
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
