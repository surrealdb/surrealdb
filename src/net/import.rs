use axum::extract::{DefaultBodyLimit, Request};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use futures::TryStreamExt;
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use crate::cnf::HTTP_MAX_IMPORT_BODY_SIZE;
use crate::core::dbs::Session;
use crate::core::dbs::capabilities::RouteTarget;
use crate::core::iam::Action::Edit;
use crate::core::iam::ResourceKind::Any;
use crate::core::val::Value;
use crate::net::error::Error as NetError;
use crate::net::output::Output;

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
					// TODO(3.0): This code here is using the wrong serialization method which might
					// result in some values of the code being serialized wrong.
					//
					// this will serialize structs differently then they should.
					let res = res.into_iter().map(|x| x.into_value()).collect::<Value>();
					Ok(Output::json_value(&res))
				}
				Some(Accept::ApplicationCbor) => {
					// TODO(3.0): This code here is using the wrong serialization method which might
					// result in some values of the code being serialized wrong.
					let res = res.into_iter().map(|x| x.into_value()).collect::<Value>();
					Ok(Output::cbor(&res))
				}
				// Return nothing
				Some(Accept::ApplicationOctetStream) => Ok(Output::None),
				// Internal serialization
				Some(Accept::Surrealdb) => Ok(Output::bincode(&res)),
				// An incorrect content-type was requested
				_ => Err(NetError::InvalidType.into()),
			}
		}
		// There was an error when executing the query
		Err(err) => Err(ResponseError(err)),
	}
}
