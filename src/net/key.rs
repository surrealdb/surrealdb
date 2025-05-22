use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::cnf::HTTP_MAX_KEY_BODY_SIZE;
use crate::net::error::Error as NetError;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use crate::net::params::Params;
use anyhow::Context as _;
use axum::Extension;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Path};
use axum::response::IntoResponse;
use axum::routing::options;
use axum_extra::TypedHeader;
use axum_extra::extract::Query;
use bytes::Bytes;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::str;
use surrealdb::dbs::Session;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::expr::Value;
use surrealdb::iam::check::check_ns_db;
use surrealdb_core::kvs::Datastore;
use tower_http::limit::RequestBodyLimitLayer;

#[derive(Default, Deserialize, Debug, Clone)]
struct QueryOptions {
	pub limit: Option<i64>,
	pub start: Option<i64>,
	pub fields: Option<Vec<String>>,
}

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route(
			"/key/:table",
			options(|| async {})
				.get(select_all)
				.post(create_all)
				.put(update_all)
				.patch(modify_all)
				.delete(delete_all),
		)
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_KEY_BODY_SIZE))
		.merge(
			Router::new()
				.route(
					"/key/:table/:key",
					options(|| async {})
						.get(select_one)
						.post(create_one)
						.put(update_one)
						.patch(modify_one)
						.delete(delete_one),
				)
				.route_layer(DefaultBodyLimit::disable())
				.layer(RequestBodyLimitLayer::new(*HTTP_MAX_KEY_BODY_SIZE)),
		)
}

async fn execute_and_return(
	db: &Datastore,
	sql: &str,
	session: &Session,
	vars: BTreeMap<String, Value>,
	accept: Option<&Accept>,
) -> Result<Output, anyhow::Error> {
	match db.execute(sql, session, Some(vars)).await {
		Ok(res) => match accept {
			// Simple serialization
			Some(Accept::ApplicationJson) => Ok(Output::json(&output::simplify(res)?)),
			Some(Accept::ApplicationCbor) => Ok(Output::cbor(&output::simplify(res)?)),
			// Internal serialization
			// TODO: remove format in 2.0.0
			Some(Accept::Surrealdb) => Ok(Output::full(&res)),
			// An incorrect content-type was requested
			_ => Err(anyhow::Error::new(NetError::InvalidType)),
		},
		// There was an error when executing the query
		Err(err) => Err(err),
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
	let db = &state.datastore;
	assert_capabilities(db, &session).map_err(ResponseError)?;
	// Ensure a NS and DB are set
	let _ = check_ns_db(&session).map_err(ResponseError)?;
	// Specify the request statement
	let sql = match query.fields {
		None => "SELECT * FROM type::table($table) LIMIT $limit START $start",
		_ => "SELECT type::fields($fields) FROM type::table($table) LIMIT $limit START $start",
	};
	// Specify the request variables
	let vars = map! {
		String::from("table") => Value::from(table),
		String::from("start") => Value::from(query.start.unwrap_or(0)),
		String::from("limit") => Value::from(query.limit.unwrap_or(100)),
		String::from("fields") => Value::from(query.fields.unwrap_or_default()),
	};
	execute_and_return(db, sql, &session, vars, accept.as_deref()).await.map_err(ResponseError)
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
	// Parse the request body as JSON
	match surrealdb::sql::value(data) {
		Ok(data) => {
			// Specify the request statement
			let sql = "CREATE type::table($table) CONTENT $data";
			// Specify the request variables
			let vars = map! {
				String::from("table") => Value::from(table),
				String::from("data") => Value::from(data),
				=> params.parse()
			};

			execute_and_return(db, sql, &session, vars, accept.as_deref())
				.await
				.map_err(ResponseError)
		}
		Err(_) => Err(NetError::Request.into()),
	}
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
	// Parse the request body as JSON
	match surrealdb::sql::value(data) {
		Ok(data) => {
			// Specify the request statement
			let sql = "UPDATE type::table($table) CONTENT $data";
			// Specify the request variables
			let vars = map! {
				String::from("table") => Value::from(table),
				String::from("data") => Value::from(data),
				=> params.parse()
			};
			execute_and_return(db, sql, &session, vars, accept.as_deref())
				.await
				.map_err(ResponseError)
		}
		Err(_) => Err(NetError::Request.into()),
	}
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
	// Parse the request body as JSON
	match surrealdb::sql::value(data) {
		Ok(data) => {
			// Specify the request statement
			let sql = "UPDATE type::table($table) MERGE $data";
			// Specify the request variables
			let vars = map! {
				String::from("table") => table.into(),
				String::from("data") => data.into(),
				=> params.parse()
			};
			execute_and_return(db, sql, &session, vars, accept.as_deref())
				.await
				.map_err(ResponseError)
		}
		Err(_) => Err(NetError::Request.into()),
	}
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
	let vars = map! {
		String::from("table") => Value::from(table),
		=> params.parse()
	};
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref()).await.map_err(ResponseError)
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
		None => "SELECT * FROM type::thing($table, $id)",
		_ => "SELECT type::fields($fields) FROM type::thing($table, $id)",
	};
	// Parse the Record ID as a SurrealQL value
	let rid = match surrealdb::sql::json(&id) {
		Ok(id) => id.into(),
		Err(_) => id.into(),
	};
	// Specify the request variables
	let vars = map! {
		String::from("table") => Value::from(table),
		String::from("id") => rid,
		String::from("fields") => Value::from(query.fields.unwrap_or_default()),
	};
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref()).await.map_err(ResponseError)
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
	let rid = match surrealdb::sql::json(&id) {
		Ok(id) => id.into(),
		Err(_) => id.into(),
	};
	// Parse the request body as JSON
	match surrealdb::sql::value(data) {
		Ok(data) => {
			// Specify the request statement
			let sql = "CREATE type::thing($table, $id) CONTENT $data";
			// Specify the request variables
			let vars = map! {
				String::from("table") => Value::from(table),
				String::from("id") => rid,
				String::from("data") => data.into(),
				=> params.parse()
			};
			// Execute the query and return the result
			execute_and_return(db, sql, &session, vars, accept.as_deref())
				.await
				.map_err(ResponseError)
		}
		Err(_) => Err(NetError::Request.into()),
	}
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
	let rid = match surrealdb::sql::json(&id) {
		Ok(id) => id.into(),
		Err(_) => id.into(),
	};
	// Parse the request body as JSON
	match surrealdb::sql::value(data) {
		Ok(data) => {
			// Specify the request statement
			let sql = "UPSERT type::thing($table, $id) CONTENT $data";
			// Specify the request variables
			let vars = map! {
				String::from("table") => Value::from(table),
				String::from("id") => rid,
				String::from("data") => data.into(),
				=> params.parse()
			};
			// Execute the query and return the result
			execute_and_return(db, sql, &session, vars, accept.as_deref())
				.await
				.map_err(ResponseError)
		}
		Err(_) => Err(NetError::Request.into()),
	}
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
	let rid = match surrealdb::sql::json(&id) {
		Ok(id) => id.into(),
		Err(_) => id.into(),
	};
	// Parse the request body as JSON
	match surrealdb::sql::value(data) {
		Ok(data) => {
			// Specify the request statement
			let sql = "UPSERT type::thing($table, $id) MERGE $data";
			// Specify the request variables
			let vars = map! {
				String::from("table") => Value::from(table),
				String::from("id") => rid,
				String::from("data") => data.into(),
				=> params.parse()
			};
			// Execute the query and return the result
			execute_and_return(db, sql, &session, vars, accept.as_deref())
				.await
				.map_err(ResponseError)
		}
		Err(_) => Err(NetError::Request.into()),
	}
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
	let sql = "DELETE type::thing($table, $id) RETURN BEFORE";
	// Parse the Record ID as a SurrealQL value
	let rid = match surrealdb::sql::json(&id) {
		Ok(id) => id.into(),
		Err(_) => id.into(),
	};
	// Specify the request variables
	let vars = map! {
		String::from("table") => Value::from(table),
		String::from("id") => rid,
	};
	// Execute the query and return the result
	execute_and_return(db, sql, &session, vars, accept.as_deref()).await.map_err(ResponseError)
}
