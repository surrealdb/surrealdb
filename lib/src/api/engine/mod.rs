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
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;
#[doc(hidden)]
pub mod tasks;

use crate::sql::statements::CreateStatement;
use crate::sql::statements::DeleteStatement;
use crate::sql::statements::InsertStatement;
use crate::sql::statements::SelectStatement;
use crate::sql::statements::UpdateStatement;
use crate::sql::statements::UpsertStatement;
use crate::sql::Data;
use crate::sql::Field;
use crate::sql::Output;
use crate::value::ToCore;
use crate::Value;
use futures::Stream;
use std::mem;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Instant;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::Interval;
#[cfg(target_arch = "wasm32")]
use wasmtimer::std::Instant;
#[cfg(target_arch = "wasm32")]
use wasmtimer::tokio::Interval;

#[allow(dead_code)] // used by the the embedded database and `http`
fn split_params(params: &mut [Value]) -> (bool, Vec<Value>, Value) {
	let (what, data) = match params {
		[what] => (mem::take(what), Value::None),
		[what, data] => (mem::take(what), mem::take(data)),
		_ => unreachable!(),
	};
	let one = what.is_record_id();
	let what = match what {
		Value::Array(vec) => vec,
		value => {
			vec![value]
		}
	};
	(one, what, data)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn create_statement(params: &mut [Value]) -> CreateStatement {
	let (_, what, data) = split_params(params);
	let data = match data {
		Value::None => None,
		value => Some(Data::ContentExpression(value.to_core())),
	};
	let mut stmt = CreateStatement::default();
	stmt.what.0 = what.into_iter().map(|x| x.to_core()).collect();
	stmt.data = data;
	stmt.output = Some(Output::After);
	stmt
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn upsert_statement(params: &mut [Value]) -> (bool, UpsertStatement) {
	let (one, what, data) = split_params(params);
	let data = match data {
		Value::None => None,
		value => Some(Data::ContentExpression(value.to_core())),
	};
	let mut stmt = UpsertStatement::default();
	stmt.what.0 = what.into_iter().map(|x| x.to_core()).collect();
	stmt.data = data;
	stmt.output = Some(Output::After);
	(one, stmt)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn update_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
	let (one, what, data) = split_params(params);
	let data = match data {
		Value::None => None,
		value => Some(Data::ContentExpression(value.to_core())),
	};
	let mut stmt = UpdateStatement::default();
	stmt.what.0 = what.into_iter().map(|x| x.to_core()).collect();
	stmt.data = data;
	stmt.output = Some(Output::After);
	(one, stmt)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn insert_statement(params: &mut [Value]) -> (bool, InsertStatement) {
	let (what, data) = match params {
		[what, data] => (mem::take(what), mem::take(data)),
		_ => unreachable!(),
	};
	let one = !data.is_array();
	let mut stmt = InsertStatement::default();
	stmt.into = match what {
		Value::None => None,
		what => Some(what.to_core()),
	};
	stmt.data = Data::SingleExpression(data.to_core());
	stmt.output = Some(Output::After);
	(one, stmt)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn patch_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
	let (one, what, data) = split_params(params);
	let data = match data {
		Value::None => None,
		value => Some(Data::PatchExpression(value.to_core())),
	};
	let mut stmt = UpdateStatement::default();
	stmt.what.0 = what.into_iter().map(|x| x.to_core()).collect();
	stmt.data = data;
	stmt.output = Some(Output::After);
	(one, stmt)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn merge_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
	let (one, what, data) = split_params(params);
	let data = match data {
		Value::None => None,
		value => Some(Data::MergeExpression(value.to_core())),
	};
	let mut stmt = UpdateStatement::default();
	stmt.what.0 = what.into_iter().map(|x| x.to_core()).collect();
	stmt.data = data;
	stmt.output = Some(Output::After);
	(one, stmt)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn select_statement(params: &mut [Value]) -> (bool, SelectStatement) {
	let (one, what, _) = split_params(params);
	let mut stmt = SelectStatement::default();
	stmt.what.0 = what.into_iter().map(|x| x.to_core()).collect();
	stmt.expr.0 = vec![Field::All];
	(one, stmt)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn delete_statement(params: &mut [Value]) -> (bool, DeleteStatement) {
	let (one, what, _) = split_params(params);
	let mut stmt = DeleteStatement::default();
	stmt.what.0 = what.into_iter().map(|x| x.to_core()).collect();
	stmt.output = Some(Output::Before);
	(one, stmt)
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
