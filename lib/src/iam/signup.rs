use crate::cnf::{INSECURE_FORWARD_SCOPE_ERRORS, SERVER_NAME};
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::token::{Claims, HEADER};
use crate::iam::Auth;
use crate::iam::{Actor, Level};
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::Object;
use crate::sql::Value;
use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey};
use std::sync::Arc;

pub async fn signup(
	kvs: &Datastore,
	session: &mut Session,
	vars: Object,
) -> Result<Option<String>, Error> {
	// Parse the specified variables
	let ns = vars.get("NS").or_else(|| vars.get("ns"));
	let db = vars.get("DB").or_else(|| vars.get("db"));
	let sc = vars.get("SC").or_else(|| vars.get("sc"));
	// Check if the parameters exist
	match (ns, db, sc) {
		(Some(ns), Some(db), Some(sc)) => {
			// Process the provided values
			let ns = ns.to_raw_string();
			let db = db.to_raw_string();
			let sc = sc.to_raw_string();
			// Attempt to signup to specified scope
			super::signup::sc(kvs, session, ns, db, sc, vars).await
		}
		_ => Err(Error::InvalidSignup),
	}
}

pub async fn sc(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	db: String,
	sc: String,
	vars: Object,
) -> Result<Option<String>, Error> {
	// Create a new readonly transaction
	let mut tx = kvs.transaction(Read, Optimistic).await?;
	// Fetch the specified scope from storage
	let scope = tx.get_sc(&ns, &db, &sc).await;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Check if the supplied Scope login exists
	match scope {
		Ok(sv) => {
			match sv.signup {
				// This scope allows signup
				Some(val) => {
					// Setup the query params
					let vars = Some(vars.0);
					// Setup the system session for creating the signup record
					let mut sess = Session::editor().with_ns(&ns).with_db(&db);
					sess.ip = session.ip.clone();
					sess.or = session.or.clone();
					// Compute the value with the params
					match kvs.evaluate(val, &sess, vars).await {
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
								// Log the authenticated scope info
								trace!("Signing up to scope `{}`", sc);
								// Create the authentication token
								let enc = encode(&HEADER, &val, &key);
								// Set the authentication on the session
								session.tk = Some(val.into());
								session.ns = Some(ns.to_owned());
								session.db = Some(db.to_owned());
								session.sc = Some(sc.to_owned());
								session.sd = Some(Value::from(rid.to_owned()));
								session.au = Arc::new(Auth::new(Actor::new(
									rid.to_string(),
									Default::default(),
									Level::Scope(ns, db, sc),
								)));
								// Create the authentication token
								match enc {
									// The auth token was created successfully
									Ok(tk) => Ok(Some(tk)),
									_ => Err(Error::TokenMakingFailed),
								}
							}
							_ => Err(Error::NoRecordFound),
						},
						Err(e) => match e {
							Error::Thrown(_) => Err(e),
							e if *INSECURE_FORWARD_SCOPE_ERRORS => Err(e),
							_ => Err(Error::SignupQueryFailed),
						},
					}
				}
				_ => Err(Error::ScopeNoSignup),
			}
		}
		_ => Err(Error::NoScopeFound),
	}
}
