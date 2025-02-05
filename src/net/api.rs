use std::collections::BTreeMap;
use std::sync::Arc;

use super::params::Params;
use super::AppState;
use crate::cnf::HTTP_MAX_API_BODY_SIZE;
use crate::err::Error;
use crate::net::output;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::http::Method;
use axum::response::IntoResponse;
use axum::routing::any;
use axum::Extension;
use axum::Router;
use http::HeaderMap;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::dbs::Session;
use surrealdb::kvs::LockType;
use surrealdb::kvs::TransactionType;
use surrealdb::sql::statements::FindApi;
use surrealdb::sql::Object;
use surrealdb::sql::Value;
use surrealdb::{ApiBody, ApiInvocation, ApiMethod};
use tower_http::limit::RequestBodyLimitLayer;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/api/:ns/:db/*path", any(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_API_BODY_SIZE))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	Path((ns, db, path)): Path<(String, String, String)>,
	headers: HeaderMap,
	Query(query): Query<Params>,
	method: Method,
	body: Body,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Format the full URL
	let url = format!("/api/{ns}/{db}/{path}");
	// Get a database reference
	let ds = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !ds.allows_http_route(&RouteTarget::Api) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Sql);
		return Err(Error::ForbiddenRoute(RouteTarget::Api.to_string()));
	}

	let headers = {
		let mut x = Object::default();

		for (key, value) in headers.iter() {
			let value =
				value.to_str().map_err(|e| Error::InvalidHeader(key.to_owned(), e.to_string()))?;

			x.insert(key.as_str().to_string(), Value::from(value));
		}

		x
	};

	let query: Object = query
		.inner
		.into_iter()
		.map(|(k, v)| (k, Value::from(v)))
		.collect::<BTreeMap<String, Value>>()
		.into();

	let method = match method {
		Method::DELETE => ApiMethod::Delete,
		Method::GET => ApiMethod::Get,
		Method::PATCH => ApiMethod::Patch,
		Method::POST => ApiMethod::Post,
		Method::PUT => ApiMethod::Put,
		Method::TRACE => ApiMethod::Trace,
		_ => return Err(Error::NotFound(url)),
	};

	let tx = Arc::new(
		ds.transaction(TransactionType::Write, LockType::Optimistic).await.map_err(Error::from)?,
	);
	let apis = tx.all_db_apis(&ns, &db).await.map_err(Error::from)?;
	let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

	let res = if let Some((api, params)) = apis.as_ref().find_api(segments, method) {
		let invocation = ApiInvocation {
			params,
			method,
			query,
			headers,
			session: Some(session),
			values: vec![],
		};

		match invocation
			.invoke_with_transaction(
				ns,
				db,
				tx.clone(),
				ds.clone(),
				api,
				ApiBody::from_stream(body.into_data_stream()),
			)
			.await
		{
			Ok(Some(v)) => v,
			Err(e) => return Err(Error::from(e)),
			_ => return Err(Error::NotFound(url)),
		}
	} else {
		return Err(Error::NotFound(url));
	};

	// Commit the transaction
	tx.commit().await.map_err(Error::from)?;

	// // Convert the received sql query
	// let sql = bytes_to_utf8(&sql)?;
	// Execute the received sql query
	Ok(output::json(&res))
	// match None {
	// 	// Simple serialization
	// 	None | Some(Accept::ApplicationJson) => Ok(output::json(&res)),
	// 	Some(Accept::ApplicationCbor) => Ok(output::cbor(&res)),
	// 	Some(Accept::ApplicationPack) => Ok(output::pack(&res)),
	// 	// Internal serialization
	// 	Some(Accept::Surrealdb) => Ok(output::full(&res)),
	// 	// An incorrect content-type was requested
	// 	_ => Err(Error::InvalidType),
	// }
}
