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
use surrealdb_types::{RecordIdKey, Value};
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Interval;
#[cfg(target_family = "wasm")]
use wasmtimer::std::Instant;
#[cfg(target_family = "wasm")]
use wasmtimer::tokio::Interval;

use super::opt::{QueryRange, Resource};
use crate::core::expr;

// used in http and all local engines.
#[allow(dead_code)]
pub(crate) fn resource_to_exprs(r: Resource) -> Vec<expr::Expr> {
	match r {
		Resource::Table(x) => {
			vec![expr::Expr::Table(x)]
		}
		Resource::RecordId(x) => {
			vec![expr::Expr::from_public_value(Value::RecordId(x))]
		}
		Resource::Object(x) => {
			vec![expr::Expr::from_public_value(Value::Object(x))]
		}
		Resource::Array(x) => x.into_iter().map(expr::Expr::from_public_value).collect(),
		Resource::Range(x) => {
			let QueryRange(record_id) = x;
			vec![expr::Expr::Literal(expr::Literal::RecordId(expr::RecordIdLit {
				table: record_id.table,
				key: public_record_id_key_to_literal(record_id.key),
			}))]
		}
		Resource::Unspecified => vec![expr::Expr::Literal(expr::Literal::None)],
	}
}

fn public_record_id_key_to_literal(x: RecordIdKey) -> expr::RecordIdKeyLit {
	match x {
		RecordIdKey::Number(n) => expr::RecordIdKeyLit::Number(n),
		RecordIdKey::String(s) => expr::RecordIdKeyLit::String(s),
		RecordIdKey::Uuid(u) => expr::RecordIdKeyLit::Uuid(u.into()),
		RecordIdKey::Array(a) => expr::RecordIdKeyLit::Array(
			a.inner().iter().cloned().map(expr::Expr::from_public_value).collect(),
		),
		RecordIdKey::Object(o) => {
			use crate::core::expr::ObjectEntry;
			expr::RecordIdKeyLit::Object(
				o.inner()
					.iter()
					.map(|(k, v)| ObjectEntry {
						key: k.clone(),
						value: expr::Expr::from_public_value(v.clone()),
					})
					.collect(),
			)
		}
		RecordIdKey::Range(_range) => {
			// Ranges within RecordIdKey are not yet fully supported in the SDK
			// For now, default to a Number(0) key
			expr::RecordIdKeyLit::Number(0)
		}
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
