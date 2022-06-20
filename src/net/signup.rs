use crate::cnf::SERVER_NAME;
use crate::err::Error;
use crate::net::head;
use crate::net::jwt::{Claims, HEADER};
use crate::net::DB;
use bytes::Bytes;
use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey};
use std::str;
use surrealdb::sql::Object;
use surrealdb::sql::Value;
use surrealdb::Session;
use warp::http::Response;
use warp::Filter;

const MAX: u64 = 1024; // 1 KiB

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("signup").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base
		.and(warp::post())
		.and(warp::body::content_length_limit(MAX))
		.and(warp::body::bytes())
		.and_then(handler);
	// Specify route
	opts.or(post).with(head::cors())
}

async fn handler(body: Bytes) -> Result<impl warp::Reply, warp::Rejection> {
	//
	let data = str::from_utf8(&body).unwrap();
	//
	match surrealdb::sql::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => {
			// Parse the speficied variables
			let ns = vars.get("NS").or_else(|| vars.get("ns"));
			let db = vars.get("DB").or_else(|| vars.get("db"));
			let sc = vars.get("SC").or_else(|| vars.get("sc"));
			// Match the authentication type
			match (ns, db, sc) {
				(Some(ns), Some(db), Some(sc)) => {
					// Process the provided values
					let ns = ns.to_strand().as_string();
					let db = db.to_strand().as_string();
					let sc = sc.to_strand().as_string();
					// Attempt to signin to specified scope
					match signup_sc(ns, db, sc, vars).await {
						// Namespace authentication was successful
						Ok(v) => Ok(Response::builder().body(v)),
						// There was an error with authentication
						Err(e) => Err(warp::reject::custom(e)),
					}
				}
				// No NS, DB, or SC keys were specified
				_ => Err(warp::reject::custom(Error::InvalidAuth)),
			}
		}
		// The provided value was not an object
		_ => Err(warp::reject::custom(Error::Request)),
	}
}

async fn signup_sc(ns: String, db: String, sc: String, vars: Object) -> Result<String, Error> {
	// Get a database reference
	let kvs = DB.get().unwrap();
	// Create a new readonly transaction
	let mut tx = kvs.transaction(false, false).await?;
	// Check if the supplied NS Login exists
	match tx.get_sc(&ns, &db, &sc).await {
		Ok(sv) => {
			match sv.signup {
				// This scope allows signin
				Some(val) => {
					// Setup the query params
					let vars = Some(vars.0);
					// Setup the query session
					let sess = Session::for_db(&ns, &db);
					// Compute the value with the params
					match kvs.compute(val, &sess, vars).await {
						// The signin value succeeded
						Ok(val) => match val.rid() {
							// There is a record returned
							Some(rid) => {
								// Create the authentication key
								let key = EncodingKey::from_secret(sv.code.as_ref());
								// Create the authentication claim
								let val = Claims {
									iss: SERVER_NAME.to_owned(),
									iat: Utc::now().timestamp(),
									nbf: Utc::now().timestamp(),
									exp: match sv.session {
										Some(v) => Utc::now() + Duration::from_std(v.0).unwrap(),
										_ => Utc::now() + Duration::hours(1),
									}
									.timestamp(),
									ns: Some(ns),
									db: Some(db),
									sc: Some(sc),
									id: Some(rid.to_raw()),
									..Claims::default()
								};
								// Create the authentication token
								match encode(&*HEADER, &val, &key) {
									// The auth token was created successfully
									Ok(tk) => Ok(tk),
									// There was an error creating the token
									_ => Err(Error::InvalidAuth),
								}
							}
							// No record was returned
							_ => Err(Error::InvalidAuth),
						},
						// The signin query failed
						_ => Err(Error::InvalidAuth),
					}
				}
				// This scope does not allow signin
				_ => Err(Error::InvalidAuth),
			}
		}
		// The scope does not exists
		_ => Err(Error::InvalidAuth),
	}
}
