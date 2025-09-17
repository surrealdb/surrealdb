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

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Interval;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::Interval;

use super::opt::Resource;
use crate::core::expr;

// used in http and all local engines.
#[allow(dead_code)]
pub(crate) fn resource_to_exprs(r: Resource) -> Vec<expr::Expr> {
	match r {
		Resource::Table(x) => {
			// TODO: Null byte validity
			vec![expr::Expr::Table(unsafe { expr::Ident::new_unchecked(x) })]
		}
		Resource::RecordId(x) => {
			vec![expr::Expr::Literal(expr::Literal::RecordId(x.into_inner().into_literal()))]
		}
		Resource::Object(x) => {
			vec![expr::Expr::Literal(expr::Literal::Object(x.into_inner().into_literal()))]
		}
		Resource::Array(x) => x.into_iter().map(|x| x.into_inner().into_literal()).collect(),
		Resource::Range(x) => {
			vec![expr::Expr::Literal(expr::Literal::RecordId(x.into_inner().into_literal()))]
		}
		Resource::Unspecified => vec![expr::Expr::Literal(expr::Literal::None)],
	}
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
