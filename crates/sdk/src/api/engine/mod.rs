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
use surrealdb_types::{RecordId, RecordIdKey};
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
	todo!("STU")
	// match r {
	// 	Resource::Table(x) => {
	// 		// TODO: Null byte validity
	// 		vec![expr::Expr::Table(unsafe { expr::Ident::new_unchecked(x) })]
	// 	}
	// 	Resource::RecordId(x) => {
	// 		vec![record_id_to_expr(x)]
	// 	}
	// 	Resource::Object(x) => {
	// 		vec![expr::Expr::Literal(expr::Literal::Object(x.into_literal()))]
	// 	}
	// 	Resource::Array(x) => x.into_iter().map(|x| x.into_inner().into_literal()).collect(),
	// 	Resource::Range(x) => {
	// 		vec![expr::Expr::Literal(expr::Literal::RecordId(x.into_inner().into_literal()))]
	// 	}
	// 	Resource::Unspecified => vec![expr::Expr::Literal(expr::Literal::None)],
	// }
}

fn record_id_to_expr(x: RecordId) -> expr::Expr {
	expr::Expr::Literal(expr::Literal::RecordId(expr::RecordIdLit {
		table: x.table,
		key: public_record_id_key_to_literal(x.key),
	}))
}

fn public_record_id_key_to_literal(x: RecordIdKey) -> expr::RecordIdKeyLit {
	todo!("STU")
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
