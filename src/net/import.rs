use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use crate::cnf::HTTP_MAX_IMPORT_BODY_SIZE;
use crate::net::error::Error as NetError;
use crate::net::output::{self, Output};
use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use futures::TryStreamExt;
use surrealdb::dbs::Session;
use surrealdb::dbs::capabilities::RouteTarget;
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
) -> Result<impl IntoResponse, ResponseError> {
	// Get the datastore reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Import) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Import);
		return Err(NetError::ForbiddenRoute(RouteTarget::Import.to_string()).into());
	}
	// Check the permissions level
	db.check(&session, Edit, Any.on_level(session.au.level().to_owned())).map_err(ResponseError)?;

	let body_stream = request.into_body().into_data_stream().map_err(anyhow::Error::new);

	// Execute the sql query in the database
	match db.import_stream(&session, body_stream).await {
		Ok(res) => {
			match accept.as_deref() {
				// Simple serialization
				Some(Accept::ApplicationJson) => {
					Ok(Output::json(&output::simplify(res).map_err(ResponseError)?))
				}
				Some(Accept::ApplicationCbor) => {
					Ok(Output::cbor(&output::simplify(res).map_err(ResponseError)?))
				}
				// Return nothing
				Some(Accept::ApplicationOctetStream) => Ok(Output::None),
				// Internal serialization
				Some(Accept::Surrealdb) => Ok(Output::full(&res)),
				// An incorrect content-type was requested
				_ => Err(NetError::InvalidType.into()),
			}
		}
		// There was an error when executing the query
		Err(err) => Err(ResponseError(err)),
	}
}
