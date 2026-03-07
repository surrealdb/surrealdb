use std::sync::Arc;

use futures::stream;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use surrealdb_core::dbs::QueryResult;
use surrealdb_types::Value;

pub fn query_results_to_response(results: Vec<QueryResult>) -> PgWireResult<Vec<Response>> {
	let mut responses = Vec::new();

	for qr in results {
		match qr.result {
			Ok(value) => {
				let response = value_to_response(value)?;
				responses.push(response);
			}
			Err(e) => {
				return Err(PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
					"ERROR".to_owned(),
					"XX000".to_owned(),
					e.to_string(),
				))));
			}
		}
	}

	Ok(responses)
}

fn value_to_response(value: Value) -> PgWireResult<Response> {
	match &value {
		Value::Array(arr) => array_to_response(arr),
		Value::Object(obj) => {
			let fields: Vec<FieldInfo> = obj
				.keys()
				.map(|k| FieldInfo::new(k.to_string(), None, None, Type::TEXT, FieldFormat::Text))
				.collect();
			let schema = Arc::new(fields);
			let mut encoder = DataRowEncoder::new(schema.clone());
			for (_key, val) in obj.iter() {
				encode_value(&mut encoder, val)?;
			}
			let rows: Vec<PgWireResult<DataRow>> = vec![Ok(encoder.take_row())];
			Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))))
		}
		Value::None | Value::Null => Ok(Response::Execution(Tag::new("OK"))),
		_ => {
			let schema = Arc::new(vec![FieldInfo::new(
				"result".to_string(),
				None,
				None,
				value_to_pg_type(&value),
				FieldFormat::Text,
			)]);
			let mut encoder = DataRowEncoder::new(schema.clone());
			encode_value(&mut encoder, &value)?;
			let rows: Vec<PgWireResult<DataRow>> = vec![Ok(encoder.take_row())];
			Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))))
		}
	}
}

fn array_to_response(arr: &surrealdb_types::Array) -> PgWireResult<Response> {
	if arr.is_empty() {
		let schema = Arc::new(Vec::new());
		return Ok(Response::Query(QueryResponse::new(
			schema,
			stream::iter(Vec::<PgWireResult<DataRow>>::new()),
		)));
	}

	let first = arr.first().unwrap();
	let schema = match first {
		Value::Object(obj) => Arc::new(
			obj.keys()
				.map(|k| {
					let val = obj.get(k).unwrap_or(&Value::None);
					FieldInfo::new(
						k.to_string(),
						None,
						None,
						value_to_pg_type(val),
						FieldFormat::Text,
					)
				})
				.collect::<Vec<_>>(),
		),
		_ => Arc::new(vec![FieldInfo::new(
			"result".to_string(),
			None,
			None,
			value_to_pg_type(first),
			FieldFormat::Text,
		)]),
	};

	let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
	for item in arr.iter() {
		let mut encoder = DataRowEncoder::new(schema.clone());
		match item {
			Value::Object(obj) => {
				for fi in schema.iter() {
					let val = obj.get(fi.name()).unwrap_or(&Value::None);
					encode_value(&mut encoder, val)?;
				}
			}
			other => {
				encode_value(&mut encoder, other)?;
			}
		}
		rows.push(Ok(encoder.take_row()));
	}

	Ok(Response::Query(QueryResponse::new(schema, stream::iter(rows))))
}

fn into_api_err(e: impl std::fmt::Display) -> PgWireError {
	PgWireError::ApiError(Box::new(std::io::Error::other(e.to_string())))
}

fn encode_value(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
	let err = into_api_err;
	match value {
		Value::None | Value::Null => {
			encoder.encode_field(&None::<String>).map_err(err)?;
		}
		Value::Bool(b) => {
			encoder.encode_field(b).map_err(err)?;
		}
		Value::Number(n) => match n {
			surrealdb_types::Number::Int(i) => {
				encoder.encode_field(i).map_err(err)?;
			}
			surrealdb_types::Number::Float(f) => {
				encoder.encode_field(f).map_err(err)?;
			}
			surrealdb_types::Number::Decimal(d) => {
				encoder.encode_field(&d.to_string()).map_err(err)?;
			}
		},
		Value::String(s) => {
			encoder.encode_field(&s.as_str()).map_err(err)?;
		}
		_ => {
			let s = format!("{value:?}");
			encoder.encode_field(&s.as_str()).map_err(err)?;
		}
	}
	Ok(())
}

fn value_to_pg_type(value: &Value) -> Type {
	match value {
		Value::Bool(_) => Type::BOOL,
		Value::Number(surrealdb_types::Number::Int(_)) => Type::INT8,
		Value::Number(surrealdb_types::Number::Float(_)) => Type::FLOAT8,
		Value::Number(surrealdb_types::Number::Decimal(_)) => Type::NUMERIC,
		Value::String(_) => Type::TEXT,
		Value::Datetime(_) => Type::TIMESTAMPTZ,
		Value::Object(_) => Type::JSONB,
		Value::Array(_) => Type::JSONB,
		Value::Uuid(_) => Type::UUID,
		Value::Bytes(_) => Type::BYTEA,
		_ => Type::TEXT,
	}
}
