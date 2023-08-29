use crate::dbs::DB;
use crate::err::Error;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use axum::extract::DefaultBodyLimit;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Extension;
use axum::Router;
use axum::TypedHeader;
use bytes::Bytes;
use http_body::Body as HttpBody;
use surrealdb::dbs::Session;
use tower_http::limit::RequestBodyLimitLayer;
use surrealml::storage::surml_file::SurMlFile;

use super::headers::Accept;

const MAX: usize = 1024 * 1024 * 1024 * 4; // 4 GiB


pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	B::Data: Send,
	B::Error: std::error::Error + Send + Sync + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/import", post(import))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(MAX))
}


async fn import(
	Extension(session): Extension<Session>,
	maybe_output: Option<TypedHeader<Accept>>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {

	let file = SurMlFile::from_bytes(body.to_vec()).map_err(|_| Error::Request)?;
	let id = format!("{}-{}", file.header.name.to_string(), file.header.version.to_string());

	let bytes = file.to_bytes();
	let byte_string = String::from_utf8(bytes).map_err(|_| Error::Request)?;

	let id_value = surrealdb::sql::value(&id)?;
	let data_value = surrealdb::sql::value(&byte_string)?;

	// Get the datastore reference
	let db = DB.get().unwrap();
	let sql = "DEFINE_MODEL $id CONTENT $data";
	let vars = map!{
		String::from("id") => id_value,
		String::from("data") => data_value
	};
	match db.execute(sql, &session, Some(vars)).await {
		Ok(_) => match maybe_output.as_deref() {
			// Return nothing
			Some(Accept::ApplicationOctetStream) => Ok(output::none()),
			// An incorrect content-type was requested
			_ => Err(Error::InvalidType),
		},
		// There was an error when executing the query
		Err(err) => Err(Error::from(err)),
	}
}