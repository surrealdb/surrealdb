use std::str;

use anyhow::Context as _;
use axum::extract::{DefaultBodyLimit, Path};
use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use axum_extra::extract::Query;
use bytes::Bytes;
use serde::Deserialize;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_core::iam::check::check_ns_db;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::{map, syn};
use surrealdb_types::{Array, SurrealValue, Value, Variables, vars};
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::ntw::error::Error as NetError;
use crate::ntw::input::bytes_to_utf8;
use crate::ntw::params::Params;

#[derive(Default, Deserialize, Debug, Clone)]
struct QueryOptions {
	pub limit: Option<i64>,
	pub start: Option<i64>,
	pub fields: Option<Vec<String>>,
}

pub fn router<S>(max_body_size: usize) -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route(
			"/key/{table}",
			options(|| async {})
				.get(select_all)
				.post(create_all)
				.put(update_all)
				.patch(modify_all)
				.delete(delete_all),
		)
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(max_body_size))
		.merge(
			Router::new()
				.route(
					"/key/{table}/{key}",
					options(|| async {})
						.get(select_one)
						.post(create_one)
						.put(update_one)
						.patch(modify_one)
						.delete(delete_one),
				)
				.route_layer(DefaultBodyLimit::disable())
				.layer(RequestBodyLimitLayer::new(max_body_size)),
		)
}

async fn execute_and_return(
	db: &Datastore,
	sql: &str,
	session: &Session,
	mut vars: Variables,
	accept: Option<&Accept>,
	expr: Option<String>,
) -> Result<Output, anyhow::Error> {
	let vars = if let Some(expr) = expr {
		let mut value = db.execute(&expr, session, Some(vars.clone())).await?;
		if let Some(resp) = value.pop() {
			vars.insert("data".to_owned(), resp.result?);
		}
		vars
	} else {
		vars
	};

	match db.execute(sql, session, Some(vars)).await {
		Ok(res) => match accept {
			// Simple serialization
			None | Some(Accept::ApplicationJson) => {
				let v = Value::Array(Array::from(
					res.into_iter().map(|x| x.into_value()).collect::<Vec<Value>>(),
				));
				Ok(Output::json_value(&v))
			}
			Some(Accept::ApplicationCbor) => {
				let v = Value::Array(Array::from(
					res.into_iter().map(|x| x.into_value()).collect::<Vec<Value>>(),
				));
				Ok(Output::cbor(v))
			}
			// Internal serialization
			Some(Accept::ApplicationFlatbuffers) => {
				let v = Value::Array(Array::from(
					res.into_iter().map(|x| x.into_value()).collect::<Vec<Value>>(),
				));
				Ok(Output::flatbuffers(&v))
			}
			// An unsupported content-type was requested
			Some(_) => Err(NetError::InvalidType.into()),
		},
		// There was an error when executing the query
		Err(err) => Err(err.into()),
	}
}

fn assert_capabilities(db: &Datastore, session: &Session) -> Result<(), anyhow::Error> {
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Key) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Key);
		return Err(NetError::ForbiddenRoute(RouteTarget::Key.to_string()).into());
	}
	// Check if the user is allowed to query
	if !db.allows_query_by_subject(session.au.as_ref()) {
		return Err(NetError::ForbiddenRoute(RouteTarget::Key.to_string()).into());
	}
	Ok(())
}

// ------------------------------
// Routes for a table
// ------------------------------

async fn select_all(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Path(table): Path<String>,
	Query(query): Query<QueryOptions>,
) -> Result<impl IntoResponse, ResponseError> {
	// Get the datastore reference
	let ds = &state.datastore;
	assert_capabilities(ds, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;

	// Specify the request statement
	let sql = match query.fields {
		None => "SELECT * FROM type::table($table) LIMIT $limit START $start",
		_ => "SELECT type::fields($fields) FROM type::table($table) LIMIT $limit START $start",
	};
	// Specify the request variables
	let vars = vars! {
		"table": Value::Table(table.into()),
		"start": query.start.unwrap_or(0),
		"limit": query.limit.unwrap_or(100),
		"fields": Value::Array(Array::from(query.fields.unwrap_or_default().into_iter().map(SurrealValue::into_value).collect::<Vec<Value>>())),
	};
	execute_and_return(ds, sql, &session, vars, accept.as_deref(), None)
		.await
		.map_err(ResponseError)
}

async fn create_all(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Path(table): Path<String>,
	Query(params): Query<Params>,
	body: Bytes,
) -> Result<impl IntoResponse, ResponseError> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Convert the HTTP request body
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Specify the request statement
	let sql = "CREATE type::table($table) CONTENT $data";
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		=> params.parse()
	});

	execute_and_return(db, sql, &session, vars, accept.as_deref(), Some(data.to_string()))
		.await
		.map_err(ResponseError)
}

async fn update_all(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Path(table): Path<String>,
	Query(params): Query<Params>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Convert the HTTP request body
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Specify the request statement
	let sql = "UPDATE type::table($table) CONTENT $data";
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		=> params.parse()
	});
	execute_and_return(db, sql, &session, vars, accept.as_deref(), Some(data.to_string()))
		.await
		.map_err(ResponseError)
}

async fn modify_all(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Path(table): Path<String>,
	Query(params): Query<Params>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Convert the HTTP request body
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Specify the request statement
	let sql = "UPDATE type::table($table) MERGE $data";
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		=> params.parse()
	});
	execute_and_return(db, sql, &session, vars, accept.as_deref(), Some(data.to_string()))
		.await
		.map_err(ResponseError)
}

async fn delete_all(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Path(table): Path<String>,
	Query(params): Query<Params>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Specify the request statement
	let sql = "DELETE type::table($table) RETURN BEFORE";
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		=> params.parse()
	});
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref(), None)
		.await
		.map_err(ResponseError)
}

// ------------------------------
// Routes for a thing
// ------------------------------

async fn select_one(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Path((table, id)): Path<(String, String)>,
	Query(query): Query<QueryOptions>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Specify the request statement
	let sql = match query.fields {
		None => "SELECT * FROM type::record($table, $id)",
		_ => "SELECT type::fields($fields) FROM type::record($table, $id)",
	};
	// Parse the Record ID as a SurrealQL value
	let rid = match syn::json(&id) {
		Ok(id) => id,
		Err(_) => Value::String(id),
	};
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		String::from("id") => rid,
		String::from("fields") => Value::Array(Array::from(query.fields.unwrap_or_default().into_iter().map(SurrealValue::into_value).collect::<Vec<Value>>())),
	});
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref(), None)
		.await
		.map_err(ResponseError)
}

async fn create_one(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Query(params): Query<Params>,
	Path((table, id)): Path<(String, String)>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Convert the HTTP request body
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Parse the Record ID as a SurrealQL value
	let rid = match syn::json(&id) {
		Ok(id) => id,
		Err(_) => Value::String(id),
	};

	// Specify the request statement
	let sql = "CREATE type::record($table, $id) CONTENT $data";
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		String::from("id") => rid,
		=> params.parse()
	});
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref(), Some(data.to_string()))
		.await
		.map_err(ResponseError)
}

async fn update_one(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Query(params): Query<Params>,
	Path((table, id)): Path<(String, String)>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Convert the HTTP request body
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Parse the Record ID as a SurrealQL value
	let rid = match syn::json(&id) {
		Ok(id) => id,
		Err(_) => Value::String(id),
	};

	// Specify the request statement
	let sql = "UPSERT type::record($table, $id) CONTENT $data";
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		String::from("id") => rid,
		=> params.parse()
	});
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref(), Some(data.to_string()))
		.await
		.map_err(ResponseError)
}

async fn modify_one(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Query(params): Query<Params>,
	Path((table, id)): Path<(String, String)>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Convert the HTTP request body
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Parse the Record ID as a SurrealQL value
	let rid = match syn::json(&id) {
		Ok(id) => id,
		Err(_) => Value::String(id),
	};

	// Specify the request statement
	let sql = "UPSERT type::record($table, $id) MERGE $data";
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		String::from("id") => rid,
		=> params.parse()
	});
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref(), Some(data.to_string()))
		.await
		.map_err(ResponseError)
}

async fn delete_one(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	Path((table, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Specify the request statement
	let sql = "DELETE type::record($table, $id) RETURN BEFORE";
	// Parse the Record ID as a SurrealQL value
	let rid = match syn::json(&id) {
		Ok(id) => id,
		Err(_) => Value::String(id),
	};
	// Specify the request variables
	let vars = Variables::from(map! {
		String::from("table") => Value::String(table),
		String::from("id") => rid,
	});
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref(), None)
		.await
		.map_err(ResponseError)
}
