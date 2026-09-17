use axum::extract::{DefaultBodyLimit, Request};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use futures::TryStreamExt;
use http::StatusCode;
use surrealdb_core::dbs::Session;
use surrealdb_iam::Action::Edit;
use surrealdb_iam::ResourceKind::Any;
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_types::SurrealValue;
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use crate::cnf::HTTP_MAX_IMPORT_BODY_SIZE;
use crate::ntw::error::Error as NetError;
use crate::ntw::output::Output;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/import", post(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_IMPORT_BODY_SIZE))
}

/// Imports a SurrealQL dump into the session's namespace and database.
///
/// The import is not transactional: each statement commits on its own and
/// execution carries on past a statement that fails. The response therefore
/// states two things.
///
/// - The status says whether the whole dump applied. A 2xx means every statement applied. 422 means
///   the dump was executed and at least one statement did not apply, so the database holds a
///   partially imported dump. A request rejected before execution - an unroutable or unauthorised
///   caller, a body that is not a dump - is answered with its own 4xx and leaves no state behind.
/// - The body reports the statements that did not apply, in the negotiated format, and is empty on
///   a complete import. Every statement the body does not name was applied, which is what tells a
///   caller how much of a failed import is now in the database. JSON, CBOR and flatbuffers carry
///   the array of results; `application/octet-stream` has no structure to carry it in, so it gets
///   the failure messages as UTF-8 text, one per line.
async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	request: Request,
) -> Result<Response, ResponseError> {
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
			// The import path suppresses the results of statements that
			// succeeded, so `res` holds one entry per failed statement. The
			// verdict is read from the entries themselves rather than from the
			// length of `res`, so a successful result arriving here could never
			// be mistaken for a complete import.
			let failed = res.iter().any(|r| r.result.is_err());
			let output = match accept.as_deref() {
				// Simple serialization
				None | Some(Accept::ApplicationJson) => {
					let res = res.into_value();
					Output::json_value(&res)
				}
				Some(Accept::ApplicationCbor) => {
					let res = res.into_value();
					Output::cbor(res)
				}
				// Return nothing for a complete import. A failure is still
				// reported in full: this content type asks to be spared the
				// result of an import that worked, not to be left without the
				// reason one did not — and it is reported in the format the
				// request negotiated, because a client that dispatches on the
				// response's content type cannot decode a report that arrives
				// as something else.
				Some(Accept::ApplicationOctetStream) => {
					if failed {
						// `application/octet-stream` carries no structure, so
						// the failures go back as the one thing a client of it
						// can decode: their messages, one per line, in UTF-8.
						let report = res
							.iter()
							.filter_map(|r| r.result.as_ref().err())
							.map(ToString::to_string)
							.collect::<Vec<_>>()
							.join("\n");
						Output::OctetStream(report.into_bytes())
					} else {
						Output::None
					}
				}
				// Internal serialization
				Some(Accept::ApplicationFlatbuffers) => {
					let res = res.into_value();
					Output::flatbuffers(&res)
				}
				// An unsupported content-type was requested
				Some(_) => return Err(NetError::InvalidType.into()),
			};
			let mut response = output.into_response();
			// The request was well formed and was executed, but the
			// instructions it carried could not all be applied. Failing to
			// render the report of that is a server fault and keeps its own
			// 5xx, which must not be restated as a fault of the request.
			if failed && response.status().is_success() {
				*response.status_mut() = StatusCode::UNPROCESSABLE_ENTITY;
			}
			Ok(response)
		}
		// There was an error when executing the query
		Err(err) => Err(ResponseError(err)),
	}
}
