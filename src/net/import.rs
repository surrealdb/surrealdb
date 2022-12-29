use crate::cli::CF;
use crate::dbs::DB;
use crate::err::Error;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use crate::net::session;
use bytes::Bytes;
use surrealdb::dbs::Session;
use warp::http;
use warp::Filter;

const MAX: u64 = 1024 * 1024 * 1024 * 4; // 4 GiB

#[allow(opaque_hidden_inferred_bound)]
pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	warp::path("import")
		.and(warp::path::end())
		.and(warp::post())
		.and(warp::header::<String>(http::header::ACCEPT.as_str()))
		.and(warp::body::content_length_limit(MAX))
		.and(warp::body::bytes())
		.and(session::build())
		.and_then(handler)
}

async fn handler(
	output: String,
	sql: Bytes,
	session: Session,
) -> Result<impl warp::Reply, warp::Rejection> {
	// Check the permissions
	match session.au.is_db() {
		true => {
			// Get the datastore reference
			let db = DB.get().unwrap();
			// Get local copy of options
			let opt = CF.get().unwrap();
			// Convert the body to a byte slice
			let sql = bytes_to_utf8(&sql)?;
			// Execute the sql query in the database
			match db.execute(sql, &session, None, opt.strict).await {
				Ok(res) => match output.as_ref() {
					"application/json" => Ok(output::json(&res)),
					"application/cbor" => Ok(output::cbor(&res)),
					"application/msgpack" => Ok(output::pack(&res)),
					"application/octet-stream" => Ok(output::none()),
					// An incorrect content-type was requested
					_ => Err(warp::reject::custom(Error::InvalidType)),
				},
				// There was an error when executing the query
				Err(err) => Err(warp::reject::custom(Error::from(err))),
			}
		}
		_ => Err(warp::reject::custom(Error::InvalidAuth)),
	}
}
