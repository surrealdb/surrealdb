use crate::cnf::{INSECURE_FORWARD_SCOPE_ERRORS, SERVER_NAME};
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::token::{Claims, HEADER};
use crate::iam::Auth;
use crate::iam::{Actor, Level};
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::AccessType;
use crate::sql::Object;
use crate::sql::Value;
use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey};
use std::sync::Arc;
use uuid::Uuid;

pub async fn signup(
	kvs: &Datastore,
	session: &mut Session,
	vars: Object,
) -> Result<Option<String>, Error> {
	// Parse the specified variables
	let ns = vars.get("NS").or_else(|| vars.get("ns"));
	let db = vars.get("DB").or_else(|| vars.get("db"));
	let ac = vars.get("AC").or_else(|| vars.get("ac"));
	// Check if the parameters exist
	match (ns, db, ac) {
		(Some(ns), Some(db), Some(ac)) => {
			// Process the provided values
			let ns = ns.to_raw_string();
			let db = db.to_raw_string();
			let ac = ac.to_raw_string();
			// Attempt to signup using specified access method
			// Currently, signup is only supported at the database level
			super::signup::db(kvs, session, ns, db, ac, vars).await
		}
		_ => Err(Error::InvalidSignup),
	}
}

pub async fn db(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	db: String,
	ac: String,
	vars: Object,
) -> Result<Option<String>, Error> {
	// Create a new readonly transaction
	let mut tx = kvs.transaction(Read, Optimistic).await?;
	// Fetch the specified access method from storage
	let access = tx.get_db_access(&ns, &db, &ac).await;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Check the provided access method exists
	match access {
		Ok(av) => {
			// Check the access method type
			// Currently, only the record access method supports signup
			match av.kind {
				AccessType::Record(at) => {
					match at.signup {
						// This record access allows signup
						Some(val) => {
							// Setup the query params
							let vars = Some(vars.0);
							// Setup the system session for finding the signup record
							let mut sess = Session::editor().with_ns(&ns).with_db(&db);
							sess.ip.clone_from(&session.ip);
							sess.or.clone_from(&session.or);
							// Compute the value with the params
							match kvs.evaluate(val, &sess, vars).await {
								// The signin value succeeded
								Ok(val) => {
									match val.record() {
										// There is a record returned
										Some(rid) => {
											// Create the authentication key
											let key = EncodingKey::from_secret(av.key.as_ref());
											// Create the authentication claim
											let exp =
												Some(
													match at.duration {
														Some(v) => {
															// The defined session duration must be valid
															match Duration::from_std(v.0) {
														// The resulting session expiration must be valid
														Ok(d) => match Utc::now().checked_add_signed(d) {
															Some(exp) => exp,
															None => {
																return Err(Error::InvalidSessionExpiration)
															}
														},
														Err(_) => {
															return Err(Error::InvalidSessionDuration)
														}
													}
														}
														_ => Utc::now() + Duration::hours(1),
													}
													.timestamp(),
												);
											let val = Claims {
												iss: Some(SERVER_NAME.to_owned()),
												iat: Some(Utc::now().timestamp()),
												nbf: Some(Utc::now().timestamp()),
												exp,
												jti: Some(Uuid::new_v4().to_string()),
												ns: Some(ns.to_owned()),
												db: Some(db.to_owned()),
												ac: Some(ac.to_owned()),
												id: Some(rid.to_raw()),
												..Claims::default()
											};
											// Log the authenticated access method info
											trace!("Signing up with access method `{}`", ac);
											// Create the authentication token
											let enc = encode(&HEADER, &val, &key);
											// Set the authentication on the session
											session.tk = Some(val.into());
											session.ns = Some(ns.to_owned());
											session.db = Some(db.to_owned());
											session.ac = Some(ac.to_owned());
											session.sd = Some(Value::from(rid.to_owned()));
											session.exp = exp;
											session.au = Arc::new(Auth::new(Actor::new(
												rid.to_string(),
												Default::default(),
												Level::Record(ns, db, rid.to_string()),
											)));
											// Check the authentication token
											match enc {
												// The auth token was created successfully
												Ok(tk) => Ok(Some(tk)),
												_ => Err(Error::TokenMakingFailed),
											}
										}
										_ => Err(Error::NoRecordFound),
									}
								}
								Err(e) => match e {
									Error::Thrown(_) => Err(e),
									e if *INSECURE_FORWARD_SCOPE_ERRORS => Err(e),
									_ => Err(Error::AccessRecordSignupQueryFailed),
								},
							}
						}
						_ => Err(Error::AccessRecordNoSignup),
					}
				}
				_ => Err(Error::AccessMethodMismatch),
			}
		}
		_ => Err(Error::AccessNotFound),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::iam::Role;
	use std::collections::HashMap;

	#[tokio::test]
	async fn test_record_signup() {
		// Test with valid parameters
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD DURATION 1h
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
					)
					SIGNUP (
						CREATE user CONTENT {
							name: $user,
							pass: crypto::argon2::generate($pass)
						}
					);
				"#,
				&sess,
				None,
			)
			.await
			.unwrap();

			// Signin with the user
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("user", "user".into());
			vars.insert("pass", "pass".into());
			let res = db(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
			)
			.await;

			assert!(res.is_ok(), "Failed to signup: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert!(sess.au.id().starts_with("user:"));
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			// Record users should not have roles.
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should always be set for tokens issued by SurrealDB
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::hours(1) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(1) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow scope duration"
			);
		}

		// Test with invalid parameters
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD DURATION 1h
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
					)
					SIGNUP (
						CREATE user CONTENT {
							name: $user,
							pass: crypto::argon2::generate($pass)
						}
					);
				"#,
				&sess,
				None,
			)
			.await
			.unwrap();

			// Signin with the user
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			// Password is missing
			vars.insert("user", "user".into());
			let res = db(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
			)
			.await;

			assert!(res.is_err(), "Unexpected successful signup: {:?}", res);
		}
	}
}
