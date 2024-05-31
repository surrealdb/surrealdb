use super::verify::{verify_db_creds, verify_ns_creds, verify_root_creds};
use super::{Actor, Level};
use crate::cnf::{INSECURE_FORWARD_RECORD_ACCESS_ERRORS, SERVER_NAME};
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::issue::{config, expiration};
use crate::iam::token::{Claims, HEADER};
use crate::iam::Auth;
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::AccessType;
use crate::sql::Object;
use crate::sql::Value;
use chrono::{Duration, Utc};
use jsonwebtoken::{encode, EncodingKey, Header};
use std::sync::Arc;
use uuid::Uuid;

pub async fn signin(
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
		// DB signin with access method
		(Some(ns), Some(db), Some(ac)) => {
			// Process the provided values
			let ns = ns.to_raw_string();
			let db = db.to_raw_string();
			let ac = ac.to_raw_string();
			// Attempt to signin using specified access method
			super::signin::db_access(kvs, session, ns, db, ac, vars).await
		}
		// DB signin with user credentials
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
					super::signin::db_user(kvs, session, ns, db, user, pass).await
				}
				_ => Err(Error::MissingUserOrPass),
			}
		}
		// NS signin with user credentials
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
					super::signin::ns_user(kvs, session, ns, user, pass).await
				}
				_ => Err(Error::MissingUserOrPass),
			}
		}
		// ROOT signin with user credentials
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
					// Attempt to signin to root
					super::signin::root_user(kvs, session, user, pass).await
				}
				_ => Err(Error::MissingUserOrPass),
			}
		}
		_ => Err(Error::NoSigninTarget),
	}
}

pub async fn db_access(
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
			// All access method types are supported except for JWT
			// The JWT access method is the one that is internal to SurrealDB
			// The equivalent of signing in with JWT is to authenticate it
			match av.kind {
				AccessType::Record(at) => {
					// Check if the record access method supports issuing tokens
					let iss = match at.jwt.issue {
						Some(iss) => iss,
						_ => return Err(Error::AccessMethodMismatch),
					};
					match at.signin {
						// This record access allows signin
						Some(val) => {
							// Setup the query params
							let vars = Some(vars.0);
							// Setup the system session for finding the signin record
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
											let key = config(iss.alg, iss.key)?;
											// Create the authentication claim
											let val = Claims {
												iss: Some(SERVER_NAME.to_owned()),
												iat: Some(Utc::now().timestamp()),
												nbf: Some(Utc::now().timestamp()),
												exp: expiration(av.duration.token)?,
												jti: Some(Uuid::new_v4().to_string()),
												ns: Some(ns.to_owned()),
												db: Some(db.to_owned()),
												ac: Some(ac.to_owned()),
												id: Some(rid.to_raw()),
												..Claims::default()
											};
											// Log the authenticated access method info
											trace!("Signing in with access method `{}`", ac);
											// Create the authentication token
											let enc =
												encode(&Header::new(iss.alg.into()), &val, &key);
											// Set the authentication on the session
											session.tk = Some(val.into());
											session.ns = Some(ns.to_owned());
											session.db = Some(db.to_owned());
											session.ac = Some(ac.to_owned());
											session.rd = Some(Value::from(rid.to_owned()));
											session.exp = expiration(av.duration.session)?;
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
									e if *INSECURE_FORWARD_RECORD_ACCESS_ERRORS => Err(e),
									_ => Err(Error::AccessRecordSigninQueryFailed),
								},
							}
						}
						_ => Err(Error::AccessRecordNoSignin),
					}
				}
				_ => Err(Error::AccessMethodMismatch),
			}
		}
		_ => Err(Error::AccessNotFound),
	}
}

pub async fn db_user(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	db: String,
	user: String,
	pass: String,
) -> Result<Option<String>, Error> {
	match verify_db_creds(kvs, &ns, &db, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let exp = Some((Utc::now() + Duration::hours(1)).timestamp());
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp,
				jti: Some(Uuid::new_v4().to_string()),
				ns: Some(ns.to_owned()),
				db: Some(db.to_owned()),
				id: Some(user),
				..Claims::default()
			};
			// Log the authenticated database info
			trace!("Signing in to database `{}`", db);
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);
			// Set the authentication on the session
			session.tk = Some(val.into());
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.exp = expiration(u.session)?;
			session.au = Arc::new((&u, Level::Database(ns.to_owned(), db.to_owned())).into());
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Some(tk)),
				_ => Err(Error::TokenMakingFailed),
			}
		}
		_ => Err(Error::InvalidAuth),
	}
}

pub async fn ns_user(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	user: String,
	pass: String,
) -> Result<Option<String>, Error> {
	match verify_ns_creds(kvs, &ns, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let exp = Some((Utc::now() + Duration::hours(1)).timestamp());
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp,
				jti: Some(Uuid::new_v4().to_string()),
				ns: Some(ns.to_owned()),
				id: Some(user),
				..Claims::default()
			};
			// Log the authenticated namespace info
			trace!("Signing in to namespace `{}`", ns);
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);
			// Set the authentication on the session
			session.tk = Some(val.into());
			session.ns = Some(ns.to_owned());
			session.exp = expiration(u.session)?;
			session.au = Arc::new((&u, Level::Namespace(ns.to_owned())).into());
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Some(tk)),
				_ => Err(Error::TokenMakingFailed),
			}
		}
		// The password did not verify
		_ => Err(Error::InvalidAuth),
	}
}

pub async fn root_user(
	kvs: &Datastore,
	session: &mut Session,
	user: String,
	pass: String,
) -> Result<Option<String>, Error> {
	match verify_root_creds(kvs, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let exp = Some((Utc::now() + Duration::hours(1)).timestamp());
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp,
				jti: Some(Uuid::new_v4().to_string()),
				id: Some(user),
				..Claims::default()
			};
			// Log the authenticated root info
			trace!("Signing in as root");
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);
			// Set the authentication on the session
			session.tk = Some(val.into());
			session.exp = expiration(u.session)?;
			session.au = Arc::new((&u, Level::Root).into());
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Some(tk)),
				_ => Err(Error::TokenMakingFailed),
			}
		}
		// The password did not verify
		_ => Err(Error::InvalidAuth),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::iam::Role;
	use std::collections::HashMap;

	#[tokio::test]
	async fn test_signin_record() {
		// Test with correct credentials
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
					)
					SIGNUP (
						CREATE user CONTENT {
							name: $user,
							pass: crypto::argon2::generate($pass)
						}
					)
					DURATION FOR SESSION 2h
				;

				CREATE user:test CONTENT {
					name: 'user',
					pass: crypto::argon2::generate('pass')
				}
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
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
			)
			.await;

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:test"));
			// Record users should not have roles
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
		}

		// Test with incorrect credentials
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
					)
					SIGNUP (
						CREATE user CONTENT {
							name: $user,
							pass: crypto::argon2::generate($pass)
						}
					)
					DURATION FOR SESSION 2h
				;

				CREATE user:test CONTENT {
					name: 'user',
					pass: crypto::argon2::generate('pass')
				}
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
			vars.insert("pass", "incorrect".into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
			)
			.await;

			assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_signin_record_with_jwt_issuer() {
		use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
		// Test with correct credentials
		{
			let public_key = r#"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu1SU1LfVLPHCozMxH2Mo
4lgOEePzNm0tRgeLezV6ffAt0gunVTLw7onLRnrq0/IzW7yWR7QkrmBL7jTKEn5u
+qKhbwKfBstIs+bMY2Zkp18gnTxKLxoS2tFczGkPLPgizskuemMghRniWaoLcyeh
kd3qqGElvW/VDL5AaWTg0nLVkjRo9z+40RQzuVaE8AkAFmxZzow3x+VJYKdjykkJ
0iT9wCS0DRTXu269V264Vf/3jvredZiKRkgwlL9xNAwxXFg0x/XFw005UWVRIkdg
cKWTjpBP2dPwVZ4WWC+9aGVd+Gyn1o0CLelf4rEjGoXbAAEgAqeGUxrcIlbjXfbc
mwIDAQAB
-----END PUBLIC KEY-----"#;
			let private_key = r#"-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC7VJTUt9Us8cKj
MzEfYyjiWA4R4/M2bS1GB4t7NXp98C3SC6dVMvDuictGeurT8jNbvJZHtCSuYEvu
NMoSfm76oqFvAp8Gy0iz5sxjZmSnXyCdPEovGhLa0VzMaQ8s+CLOyS56YyCFGeJZ
qgtzJ6GR3eqoYSW9b9UMvkBpZODSctWSNGj3P7jRFDO5VoTwCQAWbFnOjDfH5Ulg
p2PKSQnSJP3AJLQNFNe7br1XbrhV//eO+t51mIpGSDCUv3E0DDFcWDTH9cXDTTlR
ZVEiR2BwpZOOkE/Z0/BVnhZYL71oZV34bKfWjQIt6V/isSMahdsAASACp4ZTGtwi
VuNd9tybAgMBAAECggEBAKTmjaS6tkK8BlPXClTQ2vpz/N6uxDeS35mXpqasqskV
laAidgg/sWqpjXDbXr93otIMLlWsM+X0CqMDgSXKejLS2jx4GDjI1ZTXg++0AMJ8
sJ74pWzVDOfmCEQ/7wXs3+cbnXhKriO8Z036q92Qc1+N87SI38nkGa0ABH9CN83H
mQqt4fB7UdHzuIRe/me2PGhIq5ZBzj6h3BpoPGzEP+x3l9YmK8t/1cN0pqI+dQwY
dgfGjackLu/2qH80MCF7IyQaseZUOJyKrCLtSD/Iixv/hzDEUPfOCjFDgTpzf3cw
ta8+oE4wHCo1iI1/4TlPkwmXx4qSXtmw4aQPz7IDQvECgYEA8KNThCO2gsC2I9PQ
DM/8Cw0O983WCDY+oi+7JPiNAJwv5DYBqEZB1QYdj06YD16XlC/HAZMsMku1na2T
N0driwenQQWzoev3g2S7gRDoS/FCJSI3jJ+kjgtaA7Qmzlgk1TxODN+G1H91HW7t
0l7VnL27IWyYo2qRRK3jzxqUiPUCgYEAx0oQs2reBQGMVZnApD1jeq7n4MvNLcPv
t8b/eU9iUv6Y4Mj0Suo/AU8lYZXm8ubbqAlwz2VSVunD2tOplHyMUrtCtObAfVDU
AhCndKaA9gApgfb3xw1IKbuQ1u4IF1FJl3VtumfQn//LiH1B3rXhcdyo3/vIttEk
48RakUKClU8CgYEAzV7W3COOlDDcQd935DdtKBFRAPRPAlspQUnzMi5eSHMD/ISL
DY5IiQHbIH83D4bvXq0X7qQoSBSNP7Dvv3HYuqMhf0DaegrlBuJllFVVq9qPVRnK
xt1Il2HgxOBvbhOT+9in1BzA+YJ99UzC85O0Qz06A+CmtHEy4aZ2kj5hHjECgYEA
mNS4+A8Fkss8Js1RieK2LniBxMgmYml3pfVLKGnzmng7H2+cwPLhPIzIuwytXywh
2bzbsYEfYx3EoEVgMEpPhoarQnYPukrJO4gwE2o5Te6T5mJSZGlQJQj9q4ZB2Dfz
et6INsK0oG8XVGXSpQvQh3RUYekCZQkBBFcpqWpbIEsCgYAnM3DQf3FJoSnXaMhr
VBIovic5l0xFkEHskAjFTevO86Fsz1C2aSeRKSqGFoOQ0tmJzBEs1R6KqnHInicD
TQrKhArgLXX4v3CddjfTRJkFWDbE/CkvKZNOrcf1nhaGCPspRJj2KUkj1Fhl9Cnc
dn/RsYEONbwQSjIfMPkvxF+8HQ==
-----END PRIVATE KEY-----"#;
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				&format!(
					r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
					)
					SIGNUP (
						CREATE user CONTENT {{
							name: $user,
							pass: crypto::argon2::generate($pass)
						}}
					)
				    WITH JWT ALGORITHM RS256 KEY '{public_key}'
				        WITH ISSUER KEY '{private_key}'
					DURATION FOR SESSION 2h, FOR TOKEN 15m
				;

				CREATE user:test CONTENT {{
					name: 'user',
					pass: crypto::argon2::generate('pass')
				}}
				"#
				),
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
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
			)
			.await;
			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:test"));
			// Record users should not have roles
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_sess_exp =
				(Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_sess_exp =
				(Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_sess_exp && exp < max_sess_exp,
				"Session expiration is expected to follow the defined duration"
			);

			// Decode token and check that it has been issued as intended
			if let Ok(Some(tk)) = res {
				// Check that token can be verified with the defined algorithm
				let val = Validation::new(Algorithm::RS256);
				// Check that token can be verified with the defined public key
				let token_data = decode::<Claims>(
					&tk,
					&DecodingKey::from_rsa_pem(public_key.as_ref()).unwrap(),
					&val,
				)
				.unwrap();
				// Check that token has been issued with the defined algorithm
				assert_eq!(token_data.header.alg, Algorithm::RS256);
				// Check that token expiration matches the defined duration
				// Expiration should match the current time plus token duration with some margin
				let exp = match token_data.claims.exp {
					Some(exp) => exp,
					_ => panic!("Token is missing expiration claim"),
				};
				let min_tk_exp =
					(Utc::now() + Duration::minutes(15) - Duration::seconds(10)).timestamp();
				let max_tk_exp =
					(Utc::now() + Duration::minutes(15) + Duration::seconds(10)).timestamp();
				assert!(
					exp > min_tk_exp && exp < max_tk_exp,
					"Token expiration is expected to follow the defined duration"
				);
				// Check required token claims
				assert_eq!(token_data.claims.ns, Some("test".to_string()));
				assert_eq!(token_data.claims.db, Some("test".to_string()));
				assert_eq!(token_data.claims.id, Some("user:test".to_string()));
				assert_eq!(token_data.claims.ac, Some("user".to_string()));
			} else {
				panic!("Token could not be extracted from result")
			}
		}
	}

	#[tokio::test]
	async fn test_signin_db_user() {
		//
		// Test without roles defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON DB PASSWORD 'pass'", &sess, None).await.unwrap();

			// Signin with the user
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let res = db_user(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				"pass".to_string(),
			)
			.await;

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_db());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			assert_eq!(sess.exp, None, "Default system user expiration is expected to be None");
		}

		//
		// Test with roles defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON DB PASSWORD 'pass' ROLES EDITOR, OWNER", &sess, None)
				.await
				.unwrap();

			// Signin with the user
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let res = db_user(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				"pass".to_string(),
			)
			.await;

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_db());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			assert_eq!(sess.exp, None, "Default system user expiration is expected to be None");
		}

		// Test invalid password
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON DB PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				..Default::default()
			};
			let res = db_user(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				"invalid".to_string(),
			)
			.await;

			assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_signin_ns_user() {
		//
		// Test without roles defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test");
			ds.execute("DEFINE USER user ON NS PASSWORD 'pass'", &sess, None).await.unwrap();

			// Signin with the user
			let mut sess = Session {
				ns: Some("test".to_string()),
				..Default::default()
			};
			let res =
				ns_user(&ds, &mut sess, "test".to_string(), "user".to_string(), "pass".to_string())
					.await;

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_ns());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			assert_eq!(sess.exp, None, "Default system user expiration is expected to be None");
		}

		//
		// Test with roles defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test");
			ds.execute("DEFINE USER user ON NS PASSWORD 'pass' ROLES EDITOR, OWNER", &sess, None)
				.await
				.unwrap();

			// Signin with the user
			let mut sess = Session {
				ns: Some("test".to_string()),
				..Default::default()
			};
			let res =
				ns_user(&ds, &mut sess, "test".to_string(), "user".to_string(), "pass".to_string())
					.await;

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_ns());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			assert_eq!(sess.exp, None, "Default system user expiration is expected to be None");
		}

		// Test invalid password
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test");
			ds.execute("DEFINE USER user ON NS PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				..Default::default()
			};
			let res = ns_user(
				&ds,
				&mut sess,
				"test".to_string(),
				"user".to_string(),
				"invalid".to_string(),
			)
			.await;

			assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_signin_root_user() {
		//
		// Test without roles defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner();
			ds.execute("DEFINE USER user ON ROOT PASSWORD 'pass'", &sess, None).await.unwrap();

			// Signin with the user
			let mut sess = Session {
				..Default::default()
			};
			let res = root_user(&ds, &mut sess, "user".to_string(), "pass".to_string()).await;

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_root());
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			assert_eq!(sess.exp, None, "Default system user expiration is expected to be None");
		}

		//
		// Test with roles defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner();
			ds.execute("DEFINE USER user ON ROOT PASSWORD 'pass' ROLES EDITOR, OWNER", &sess, None)
				.await
				.unwrap();

			// Signin with the user
			let mut sess = Session {
				..Default::default()
			};
			let res = root_user(&ds, &mut sess, "user".to_string(), "pass".to_string()).await;

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_root());
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			assert_eq!(sess.exp, None, "Default system user expiration is expected to be None");
		}

		// Test invalid password
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test");
			ds.execute("DEFINE USER user ON ROOT PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				..Default::default()
			};
			let res = root_user(&ds, &mut sess, "user".to_string(), "invalid".to_string()).await;

			assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
		}
	}
}
