use crate::dbs::DB;
use crate::err::Error;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use crate::net::params::Params;
use axum::extract::{DefaultBodyLimit, Path};
use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router, TypedHeader};
use axum_extra::extract::Query;
use bytes::Bytes;
use http_body::Body as HttpBody;
use surrealdb::dbs::Session;
use surrealdb::iam::check::check_ns_db;
use surrealdb::sql::Value;
use tower_http::limit::RequestBodyLimitLayer;

use super::headers::Accept;

const MAX: usize = 1024 * 16; // 16 KiB

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	B::Data: Send,
	B::Error: std::error::Error + Send + Sync + 'static,
	S: Clone + Send + Sync + 'static,
{
    Router::new()
		.route(
			"/relate/:table",
			options(|| async {})
				.post(create_relation_with_table),
		)
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(MAX))
		.merge(
			Router::new()
				.route(
					"/relate/:table/:key",
					options(|| async {})
						.post(create_relation_with_id),
				)
				.route_layer(DefaultBodyLimit::disable())
				.layer(RequestBodyLimitLayer::new(MAX)),
		)
}

async fn create_relation_with_table(
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Path(table): Path<String>,
	Query(params): Query<Params>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = DB.get().unwrap();
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session)?;
	// Convert the HTTP request body
	let data = bytes_to_utf8(&body)?;
	// Parse the request body as JSON
	match surrealdb::sql::value(data) {
		Ok(data) => {
			// Specify the request statement
			// TODO : be able to use type::table (table as variable)
			let sql = format!("RELATE ($data.ins)->{0}->($data.outs) CONTENT $data.content || {{}}", table);
			// Specify the request variables
			let vars = map! {
				String::from("table") => Value::from(table),
				String::from("data") => data,
				=> params.parse()
			};
			// Execute the query and return the result
			match db.execute(&sql, &session, Some(vars)).await {
				Ok(res) => match accept.as_deref() {
					// Simple serialization
					Some(Accept::ApplicationJson) => Ok(output::json(&output::simplify(res))),
					Some(Accept::ApplicationCbor) => Ok(output::cbor(&output::simplify(res))),
					Some(Accept::ApplicationPack) => Ok(output::pack(&output::simplify(res))),
					// Internal serialization
					Some(Accept::Surrealdb) => Ok(output::full(&res)),
					// An incorrect content-type was requested
					_ => Err(Error::InvalidType),
				},
				// There was an error when executing the query
				Err(err) => Err(Error::from(err)),
			}
		}
		Err(_) => Err(Error::Request),
	}
}

async fn create_relation_with_id(
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Query(params): Query<Params>,
	Path((table, id)): Path<(String, String)>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = DB.get().unwrap();
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session)?;
	// Convert the HTTP request body
	let data = bytes_to_utf8(&body)?;
	// Parse the Record ID as a SurrealQL value
	let rid = match surrealdb::sql::json(&id) {
		Ok(id) => id,
		Err(_) => Value::from(id),
	};
	// Parse the request body as JSON
	match surrealdb::sql::value(data) {
		Ok(data) => {
			// Specify the request statement
			// TODO : be able to use type::thing (table and id as variable)
			let sql = format!("RELATE ($data.in)->{0}:{1}->($data.out) CONTENT $data.content || {{}}", table, rid.to_raw_string());
			// Specify the request variables
			let vars = map! {
				String::from("table") => Value::from(table),
				String::from("id") => rid,
				String::from("data") => data,
				=> params.parse()
			};
			// Execute the query and return the result
			match db.execute(&sql, &session, Some(vars)).await {
				Ok(res) => match accept.as_deref() {
					// Simple serialization
					Some(Accept::ApplicationJson) => Ok(output::json(&output::simplify(res))),
					Some(Accept::ApplicationCbor) => Ok(output::cbor(&output::simplify(res))),
					Some(Accept::ApplicationPack) => Ok(output::pack(&output::simplify(res))),
					// Internal serialization
					Some(Accept::Surrealdb) => Ok(output::full(&res)),
					// An incorrect content-type was requested
					_ => Err(Error::InvalidType),
				},
				// There was an error when executing the query
				Err(err) => Err(Error::from(err)),
			}
		}
		Err(_) => Err(Error::Request),
	}
}