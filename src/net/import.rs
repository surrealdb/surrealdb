use super::headers::Accept;
use super::AppState;
use crate::cnf::HTTP_MAX_IMPORT_BODY_SIZE;
use crate::err::Error;
use crate::net::output;
use axum::extract::DefaultBodyLimit;
use axum::extract::Request;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Extension;
use axum::Router;
use axum_extra::TypedHeader;
use futures::TryStreamExt;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::dbs::Session;
use surrealdb::iam::Action::Edit;
use surrealdb::iam::ResourceKind::Any;
use tower_http::limit::RequestBodyLimitLayer;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/import", post(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_IMPORT_BODY_SIZE))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	request: Request,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Import) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Import);
		return Err(Error::ForbiddenRoute(RouteTarget::Import.to_string()));
	}
	// Check the permissions level
	db.check(&session, Edit, Any.on_level(session.au.level().to_owned()))?;

	let body_stream = request
		.into_body()
		.into_data_stream()
		.map_err(|e| surrealdb_core::err::Error::QueryStream(e.to_string()));

	// Execute the sql query in the database
	match db.import_stream(&session, body_stream).await {
		Ok(res) => {
			match accept.as_deref() {
				// Simple serialization
				Some(Accept::ApplicationJson) => Ok(output::json(&output::simplify(res))),
				Some(Accept::ApplicationCbor) => Ok(output::cbor(&output::simplify(res))),
				Some(Accept::ApplicationPack) => Ok(output::pack(&output::simplify(res))),
				// Return nothing
				Some(Accept::ApplicationOctetStream) => Ok(output::none()),
				// Internal serialization
				Some(Accept::Surrealdb) => Ok(output::full(&res)),
				// An incorrect content-type was requested
				_ => Err(Error::InvalidType),
			}
		}
		// There was an error when executing the query
		Err(err) => Err(Error::from(err)),
	}
}
