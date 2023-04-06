//! Different embedded and remote database engines

pub mod any;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
pub mod local;
#[cfg(any(feature = "protocol-http", feature = "protocol-ws"))]
pub mod remote;

use crate::sql::statements::CreateStatement;
use crate::sql::statements::DeleteStatement;
use crate::sql::statements::SelectStatement;
use crate::sql::statements::UpdateStatement;
use crate::sql::Array;
use crate::sql::Data;
use crate::sql::Field;
use crate::sql::Fields;
use crate::sql::Output;
use crate::sql::Value;
use crate::sql::Values;
use std::mem;

#[allow(dead_code)] // used by the the embedded database and `http`
fn split_params(params: &mut [Value]) -> (bool, Values, Value) {
	let (what, data) = match params {
		[what] => (mem::take(what), Value::None),
		[what, data] => (mem::take(what), mem::take(data)),
		_ => unreachable!(),
	};
	let one = what.is_thing();
	let what = match what {
		Value::Array(Array(vec)) => Values(vec),
		value => Values(vec![value]),
	};
	(one, what, data)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn create_statement(params: &mut [Value]) -> CreateStatement {
	let (_, what, data) = split_params(params);
	let data = match data {
		Value::None | Value::Null => None,
		value => Some(Data::ContentExpression(value)),
	};
	CreateStatement {
		what,
		data,
		output: Some(Output::After),
		..Default::default()
	}
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn update_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
	let (one, what, data) = split_params(params);
	let data = match data {
		Value::None | Value::Null => None,
		value => Some(Data::ContentExpression(value)),
	};
	(
		one,
		UpdateStatement {
			what,
			data,
			output: Some(Output::After),
			..Default::default()
		},
	)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn patch_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
	let (one, what, data) = split_params(params);
	let data = match data {
		Value::None | Value::Null => None,
		value => Some(Data::PatchExpression(value)),
	};
	(
		one,
		UpdateStatement {
			what,
			data,
			output: Some(Output::Diff),
			..Default::default()
		},
	)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn merge_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
	let (one, what, data) = split_params(params);
	let data = match data {
		Value::None | Value::Null => None,
		value => Some(Data::MergeExpression(value)),
	};
	(
		one,
		UpdateStatement {
			what,
			data,
			output: Some(Output::After),
			..Default::default()
		},
	)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn select_statement(params: &mut [Value]) -> (bool, SelectStatement) {
	let (one, what, _) = split_params(params);
	(
		one,
		SelectStatement {
			what,
			expr: Fields(vec![Field::All], false),
			..Default::default()
		},
	)
}

#[allow(dead_code)] // used by the the embedded database and `http`
fn delete_statement(params: &mut [Value]) -> (bool, DeleteStatement) {
	let (one, what, _) = split_params(params);
	(
		one,
		DeleteStatement {
			what,
			output: Some(Output::Before),
			..Default::default()
		},
	)
}
