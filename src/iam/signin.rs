use crate::cli::CF;
use crate::cnf::SERVER_NAME;
use crate::dbs::DB;
use crate::err::Error;
use crate::iam::token::{Claims, HEADER};
use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey};
use std::sync::Arc;
use surrealdb::dbs::Auth;
use surrealdb::dbs::Session;
use surrealdb::sql::Object;
use surrealdb::sql::Value;

use super::verify::verify_creds;

pub async fn signin(session: &mut Session, vars: Object) -> Result<Option<String>, Error> {
	// Parse the specified variables
	let ns = vars.get("NS").or_else(|| vars.get("ns"));
	let db = vars.get("DB").or_else(|| vars.get("db"));
	let sc = vars.get("SC").or_else(|| vars.get("sc"));
	// Check if the parameters exist
	match (ns, db, sc) {
		// SCOPE signin
		(Some(ns), Some(db), Some(sc)) => {
			// Process the provided values
			let ns = ns.to_raw_string();
			let db = db.to_raw_string();
			let sc = sc.to_raw_string();
			// Attempt to signin to specified scope
			super::signin::sc(session, ns, db, sc, vars).await
		}
		// DB signin
		(Some(ns), Some(db), None) => {
			// Get the provided user and pass
			let user = vars.get("user");
			let pass = vars.get("pass");
			// Validate the user and pass
			match (user, pass) {
				// There is a username and password
				(Some(user), Some(pass)) => {
					// Process the provided values
					let ns = ns.to_raw_string();
					let db = db.to_raw_string();
					let user = user.to_raw_string();
					let pass = pass.to_raw_string();
					// Attempt to signin to database
					super::signin::db(session, ns, db, user, pass).await
				}
				// There is no username or password
				_ => Err(Error::InvalidAuth),
			}
		}
		// NS signin
		(Some(ns), None, None) => {
			// Get the provided user and pass
			let user = vars.get("user");
			let pass = vars.get("pass");
			// Validate the user and pass
			match (user, pass) {
				// There is a username and password
				(Some(user), Some(pass)) => {
					// Process the provided values
					let ns = ns.to_raw_string();
					let user = user.to_raw_string();
					let pass = pass.to_raw_string();
					// Attempt to signin to namespace
					super::signin::ns(session, ns, user, pass).await
				}
				// There is no username or password
				_ => Err(Error::InvalidAuth),
			}
		}
		// KV signin
		(None, None, None) => {
			// Get the provided user and pass
			let user = vars.get("user");
			let pass = vars.get("pass");
			// Validate the user and pass
			match (user, pass) {
				// There is a username and password
				(Some(user), Some(pass)) => {
					// Process the provided values
					let user = user.to_raw_string();
					let pass = pass.to_raw_string();
					// Attempt to signin to KV
					super::signin::kv(session, user, pass).await
				}
				// There is no username or password
				_ => Err(Error::InvalidAuth),
			}
		}
		_ => Err(Error::InvalidAuth),
	}
}

pub async fn sc(
	session: &mut Session,
	ns: String,
	db: String,
	sc: String,
	vars: Object,
) -> Result<Option<String>, Error> {
	// Get a database reference
	let kvs = DB.get().unwrap();
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Create a new readonly transaction
	let mut tx = kvs.transaction(false, false).await?;
	// Check if the supplied DB Scope exists
	match tx.get_sc(&ns, &db, &sc).await {
		Ok(sv) => {
			match sv.signin {
				// This scope allows signin
				Some(val) => {
					// Setup the query params
					let vars = Some(vars.0);
					// Setup the query session
					let sess = Session::for_db(&ns, &db);
					// Compute the value with the params
					match kvs.compute(val, &sess, vars, opt.strict).await {
						// The signin value succeeded
						Ok(val) => match val.record() {
							// There is a record returned
							Some(rid) => {
								// Create the authentication key
								let key = EncodingKey::from_secret(sv.code.as_ref());
								// Create the authentication claim
								let val = Claims {
									iss: Some(SERVER_NAME.to_owned()),
									iat: Some(Utc::now().timestamp()),
									nbf: Some(Utc::now().timestamp()),
									exp: Some(
										match sv.session {
											Some(v) => {
												Utc::now() + Duration::from_std(v.0).unwrap()
											}
											_ => Utc::now() + Duration::hours(1),
										}
										.timestamp(),
									),
									ns: Some(ns.to_owned()),
									db: Some(db.to_owned()),
									sc: Some(sc.to_owned()),
									id: Some(rid.to_raw()),
									..Claims::default()
								};
								// Create the authentication token
								let enc = encode(&HEADER, &val, &key);
								// Set the authentication on the session
								session.tk = Some(val.into());
								session.ns = Some(ns.to_owned());
								session.db = Some(db.to_owned());
								session.sc = Some(sc.to_owned());
								session.sd = Some(Value::from(rid));
								session.au = Arc::new(Auth::Sc(ns, db, sc));
								// Check the authentication token
								match enc {
									// The auth token was created successfully
									Ok(tk) => Ok(Some(tk)),
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

pub async fn db(
	session: &mut Session,
	ns: String,
	db: String,
	user: String,
	pass: String,
) -> Result<Option<String>, Error> {
	match verify_creds(DB.get().unwrap(), Some(&ns), Some(&db), &user, &pass).await {
		Ok((au, u)) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some(ns.to_owned()),
				db: Some(db.to_owned()),
				id: Some(user),
				..Claims::default()
			};
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);
			// Set the authentication on the session
			session.tk = Some(val.into());
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Some(tk)),
				// There was an error creating the token
				_ => Err(Error::InvalidAuth),
			}
		}
		// The password did not verify
		_ => Err(Error::InvalidAuth),
	}
}

pub async fn ns(
	session: &mut Session,
	ns: String,
	user: String,
	pass: String,
) -> Result<Option<String>, Error> {
	match verify_creds(DB.get().unwrap(), Some(&ns), None, &user, &pass).await {
		Ok((au, u)) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some(ns.to_owned()),
				id: Some(user),
				..Claims::default()
			};
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);
			// Set the authentication on the session
			session.tk = Some(val.into());
			session.ns = Some(ns.to_owned());
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Some(tk)),
				// There was an error creating the token
				_ => Err(Error::InvalidAuth),
			}
		}
		Err(e) => Err(e),
	}
}

pub async fn kv(
	session: &mut Session,
	user: String,
	pass: String,
) -> Result<Option<String>, Error> {
	match verify_creds(DB.get().unwrap(), None, None, &user, &pass).await {
		Ok((au, u)) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				id: Some(user),
				..Claims::default()
			};
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);
			// Set the authentication on the session
			session.tk = Some(val.into());
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Some(tk)),
				// There was an error creating the token
				_ => Err(Error::InvalidAuth),
			}
		}
		Err(e) => Err(e),
	}
}
