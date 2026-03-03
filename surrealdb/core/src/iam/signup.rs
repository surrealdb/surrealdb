use std::sync::Arc;

use anyhow::{Result, bail};
use chrono::Utc;
use jsonwebtoken::{Header, encode};
use surrealdb_types::ToSql;
use uuid::Uuid;

use super::access::{authenticate_record, create_refresh_token_record};
use crate::catalog;
use crate::catalog::providers::{AuthorisationProvider, DatabaseProvider};
use crate::cnf::SERVER_NAME;
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::issue::{config, expiration};
use crate::iam::token::{Claims, Token};
use crate::iam::{Actor, Auth, Level, Role, algorithm_to_jwt_algorithm};
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::types::PublicVariables;
use crate::val::Value;

/// Registers a new user and returns an authentication token.
///
/// This function handles user registration for SurrealDB and returns a token
/// that can be used for subsequent API requests. The token may include both
/// access and refresh tokens depending on the registration method and
/// configuration.
///
/// # Parameters
///
/// - `kvs`: The datastore instance for database operations
/// - `session`: The current session context
/// - `vars`: Public variables containing registration credentials
///
/// # Returns
///
/// Returns a `Token` that can be either:
/// - An access token only (legacy mode)
/// - An access token with an optional refresh token (modern mode)
///
/// # Registration Methods
///
/// The function supports multiple registration methods based on the provided variables:
/// - **Database access method**: When `NS`, `DB`, and `AC` are provided
/// - **Database user credentials**: When `NS`, `DB`, `user`, and `pass` are provided
/// - **Namespace user credentials**: When `NS`, `user`, and `pass` are provided
/// - **Root user credentials**: When `user` and `pass` are provided
///
/// # Examples
///
/// ```rust
/// use surrealdb_core::iam::signup;
/// use surrealdb_core::kvs::Datastore;
/// use surrealdb_core::dbs::Session;
/// use surrealdb_core::types::PublicVariables;
///
/// // Database access method registration
/// let vars = PublicVariables::from([
///     ("NS".to_string(), "test_namespace".into()),
///     ("DB".to_string(), "test_database".into()),
///     ("AC".to_string(), "my_access_method".into()),
///     ("email".to_string(), "user@example.com".into()),
///     ("pass".to_string(), "password123".into()),
/// ]);
///
/// let token = signup(&kvs, &mut session, vars).await?;
/// match token {
///     Token::Access(access_token) => {
///         // Use access token for API requests
///     }
///     Token::WithRefresh { access, refresh } => {
///         // Use access token for API requests
///         // Store refresh token for token renewal
///     }
/// }
/// ```
pub async fn signup(
	kvs: &Datastore,
	session: &mut Session,
	vars: PublicVariables,
) -> Result<Token> {
	// Parse the specified variables
	let ns = vars.get("NS").or_else(|| vars.get("ns")).cloned();
	let db = vars.get("DB").or_else(|| vars.get("db")).cloned();
	let ac = vars.get("AC").or_else(|| vars.get("ac")).cloned();
	// Check if the parameters exist
	match (ns, db, ac) {
		(Some(ns), Some(db), Some(ac)) => {
			// Process the provided values
			let ns = ns.into_string()?;
			let db = db.into_string()?;
			let ac = ac.into_string()?;
			// Attempt to signup using specified access method
			// Currently, signup is only supported at the database level
			super::signup::db_access(kvs, session, ns, db, ac, vars).await
		}
		_ => Err(anyhow::Error::new(Error::InvalidSignup)),
	}
}

/// Registers a new user using a database access method.
///
/// This function handles user registration for users who will be granted access
/// through a specific access method defined on the database. It supports both
/// traditional access tokens and refresh token flows.
///
/// # Parameters
///
/// - `kvs`: The datastore instance for database operations
/// - `session`: The current session context
/// - `ns`: The namespace name
/// - `db`: The database name
/// - `ac`: The access method name
/// - `vars`: Public variables containing registration parameters
///
/// # Returns
///
/// Returns a `Token` that may include both access and refresh tokens
/// depending on the access method configuration.
///
/// # Access Method Configuration
///
/// The access method must be defined with the `DEFINE ACCESS` statement
/// and can include refresh token support with the `WITH REFRESH` clause:
///
/// ```sql
/// DEFINE ACCESS my_access ON DATABASE TYPE RECORD
/// SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
/// SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
/// WITH REFRESH
/// DURATION FOR SESSION 1d FOR TOKEN 15m
/// ```
///
/// # Examples
///
/// ```rust
/// use surrealdb_core::iam::signup::db_access;
/// use surrealdb_core::kvs::Datastore;
/// use surrealdb_core::dbs::Session;
/// use surrealdb_core::types::PublicVariables;
///
/// let vars = PublicVariables::from([
///     ("email".to_string(), "user@example.com".into()),
///     ("pass".to_string(), "password123".into()),
/// ]);
///
/// let token = db_access(
///     &kvs,
///     &mut session,
///     "test_namespace".to_string(),
///     "test_database".to_string(),
///     "my_access".to_string(),
///     vars
/// ).await?;
///
/// match token {
///     Token::Access(access_token) => {
///         // Traditional access token
///     }
///     Token::WithRefresh { access, refresh } => {
///         // Access token with refresh capability
///     }
/// }
/// ```
pub async fn db_access(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	db: String,
	ac: String,
	vars: PublicVariables,
) -> Result<Token> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read, Optimistic).await?;
	let db_def = match catch!(tx, tx.get_db_by_name(&ns, &db).await) {
		Some(db) => db,
		None => {
			let _ = tx.cancel().await;
			return Err(Error::DbNotFound {
				name: db,
			}
			.into());
		}
	};
	// Fetch the specified access method from storage
	let Some(av) = catch!(tx, tx.get_db_access(db_def.namespace_id, db_def.database_id, &ac).await)
	else {
		let _ = tx.cancel().await;
		bail!(Error::AccessNotFound);
	};
	// Ensure that the transaction is cancelled
	tx.cancel().await?;

	// Check the access method type
	// Currently, only the record access method supports signup
	let catalog::AccessType::Record(ref at) = av.access_type else {
		bail!(Error::AccessMethodMismatch)
	};

	// Check if the record access method supports issuing tokens
	let Some(iss) = &at.jwt.issue else {
		bail!(Error::AccessMethodMismatch)
	};

	let Some(val) = &at.signup else {
		bail!(Error::AccessRecordNoSignup);
	};
	// Setup the query params
	// Setup the system session for finding the signup record
	let mut sess = Session::for_level(Level::Database(ns.clone(), db.clone()), Role::Editor);
	sess.ip.clone_from(&session.ip);
	sess.or.clone_from(&session.or);
	// Compute the value with the params
	match kvs.evaluate(val, &sess, Some(vars)).await {
		// The signup value succeeded
		Ok(val) => {
			// There is a record returned
			let Ok(mut rid) = val.into_record() else {
				bail!(Error::NoRecordFound)
			};
			// Create the authentication key
			let key = config(iss.alg, &iss.key)?;
			// Create the authentication claim
			let claims = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: expiration(av.token_duration)?,
				jti: Some(Uuid::new_v4().to_string()),
				ns: Some(ns.clone()),
				db: Some(db.clone()),
				ac: Some(ac.clone()),
				id: Some(rid.to_sql()),
				..Claims::default()
			};
			// AUTHENTICATE clause
			if let Some(au) = &av.authenticate {
				// Setup the system session for finding the signin record
				let mut sess =
					Session::for_level(Level::Database(ns.clone(), db.clone()), Role::Editor);
				sess.rd = Some(
					crate::val::convert_value_to_public_value(Value::RecordId(rid.clone().into()))
						.expect("record id conversion should succeed"),
				);
				sess.tk = Some(
					crate::val::convert_value_to_public_value(
						claims.clone().into_claims_object().into(),
					)
					.expect("claims conversion should succeed"),
				);
				sess.ip.clone_from(&session.ip);
				sess.or.clone_from(&session.or);
				rid = authenticate_record(kvs, &sess, au).await?;
			}
			// Create refresh token if defined for the record access method
			let refresh = match &at.bearer {
				Some(_) => Some(
					create_refresh_token_record(kvs, av.name.clone(), &ns, &db, rid.clone().into())
						.await?,
				),
				None => None,
			};
			// Log the authenticated access method info
			trace!("Signing up with access method `{}`", ac);
			// Create the authentication token
			let enc = encode(&Header::new(algorithm_to_jwt_algorithm(iss.alg)), &claims, &key);
			// Set the authentication on the session
			session.tk = Some(
				crate::val::convert_value_to_public_value(claims.into_claims_object().into())
					.expect("claims conversion should succeed"),
			);
			session.ns = Some(ns.clone());
			session.db = Some(db.clone());
			session.ac = Some(ac.clone());
			session.rd = Some(
				crate::val::convert_value_to_public_value(Value::RecordId(rid.clone().into()))
					.expect("record id conversion should succeed"),
			);
			session.exp = expiration(av.session_duration)?;
			session.au = Arc::new(Auth::new(Actor::new(
				rid.to_sql(),
				Default::default(),
				Level::Record(ns, db, rid.to_sql()),
			)));
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(token) => Ok(match refresh {
					Some(refresh) => Token::WithRefresh {
						access: token,
						refresh,
					},
					None => Token::Access(token),
				}),
				_ => Err(anyhow::Error::new(Error::TokenMakingFailed)),
			}
		}
		Err(e) => match e.downcast_ref() {
			// If the SIGNUP clause throws a specific error, authentication fails with that error
			Some(Error::Thrown(_)) => Err(e),
			// If the SIGNUP clause failed due to an unexpected error, be more specific
			// This allows clients to handle these errors, which may be retryable
			Some(Error::Kvs(kvs_err)) if kvs_err.is_retryable() => {
				debug!("Unexpected error found while executing a SIGNUP clause: {e}");
				Err(anyhow::Error::new(Error::UnexpectedAuth))
			}
			// Otherwise, return a generic error unless it should be forwarded
			_ => {
				if kvs.config().security.insecure_forward_access_errors {
					Err(e)
				} else {
					Err(anyhow::Error::new(Error::AccessRecordSignupQueryFailed))
				}
			}
		},
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use chrono::Duration;

	use super::*;
	use crate::iam::Role;

	#[tokio::test]
	async fn test_record_signup() {
		// Test with valid parameters
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
			let mut vars = PublicVariables::new();
			vars.insert("user", "user");
			vars.insert("pass", "pass");
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
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
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some
			// margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		// Test with invalid parameters
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
			let mut vars = PublicVariables::new();
			// Password is missing
			vars.insert("user", "user");
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;

			assert!(res.is_err(), "Unexpected successful signup: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_signup_record_with_refresh() {
		use crate::iam::{Token, signin};

		// Test without refresh
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP (
						CREATE user CONTENT {
							name: $user,
							pass: crypto::argon2::generate($pass)
						}
					)
					DURATION FOR GRANT 1w, FOR SESSION 2h
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

			// Signup with the user
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars = PublicVariables::new();
			vars.insert("user", "user");
			vars.insert("pass", "pass");
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;

			match res {
				Ok(data) => {
					assert!(
						!matches!(data, Token::WithRefresh { .. }),
						"Refresh token was unexpectedly returned"
					)
				}
				Err(e) => panic!("Failed to signup with credentials: {e}"),
			}
		}
		// Test with refresh
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP (
						CREATE user CONTENT {
							name: $user,
							pass: crypto::argon2::generate($pass)
						}
					)
					WITH REFRESH
					DURATION FOR GRANT 1w, FOR SESSION 2h
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

			// Signup with the user
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars = PublicVariables::new();
			vars.insert("user", "user");
			vars.insert("pass", "pass");
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;

			assert!(res.is_ok(), "Failed to signup with credentials: {:?}", res);
			let refresh = match res {
				Ok(data) => match data {
					Token::WithRefresh {
						refresh,
						..
					} => refresh,
					Token::Access(_) => panic!("Refresh token was not returned"),
				},
				Err(e) => panic!("Failed to signup with credentials: {e}"),
			};
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert!(sess.au.id().starts_with("user:"));
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some
			// margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
			// Signin with the refresh token
			let mut vars = PublicVariables::new();
			vars.insert("refresh", refresh.clone());
			let res = signin::db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;
			// Authentication should be identical as with user credentials
			match res {
				Ok(data) => match data {
					Token::WithRefresh {
						refresh: new_refresh,
						..
					} => assert!(
						new_refresh != refresh,
						"New refresh token is identical to used one"
					),
					Token::Access(_) => panic!("Refresh token was not returned"),
				},
				Err(e) => panic!("Failed to signin with credentials: {e}"),
			};
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert!(sess.au.id().starts_with("user:"));
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some
			// margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
			// Attempt to sign in with the original refresh token
			let mut vars = PublicVariables::new();
			vars.insert("refresh", refresh);
			let res = signin::db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;
			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
	}

	#[tokio::test]
	async fn test_record_signup_with_jwt_issuer() {
		use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
		// Test with valid parameters
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
			let mut vars = PublicVariables::new();
			vars.insert("user", "user");
			vars.insert("pass", "pass");
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
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
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration should always be set for tokens issued by SurrealDB
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some
			// margin
			let min_sess_exp =
				(Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_sess_exp =
				(Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_sess_exp && exp < max_sess_exp,
				"Session expiration is expected to follow access method duration"
			);

			// Decode token and check that it has been issued as intended
			if let Ok(sd) = res {
				let token = match &sd {
					Token::Access(token) => token,
					Token::WithRefresh {
						access: token,
						..
					} => token,
				};
				// Check that token can be verified with the defined algorithm
				let val = Validation::new(Algorithm::RS256);
				// Check that token can be verified with the defined public key
				let token_data = decode::<Claims>(
					token,
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
					"Token expiration is expected to follow issuer duration"
				);
				// Check required token claims
				assert_eq!(token_data.claims.ns, Some("test".to_string()));
				assert_eq!(token_data.claims.db, Some("test".to_string()));
				assert!(token_data.claims.id.unwrap().starts_with("user:"));
				assert_eq!(token_data.claims.ac, Some("user".to_string()));
			} else {
				panic!("Token could not be extracted from result")
			}
		}
	}

	#[tokio::test]
	async fn test_signup_record_and_authenticate_clause() {
		// Test with correct credentials
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP (
						CREATE type::record('user', $id)
					)
					AUTHENTICATE (
						-- Simple example increasing the record identifier by one
					    SELECT * FROM type::record('user', record::id($auth) + 1)
					)
					DURATION FOR SESSION 2h
				;

				CREATE user:2;
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
			let mut vars = PublicVariables::new();
			vars.insert("id", 1);
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;

			assert!(res.is_ok(), "Failed to signup with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("user".to_string()));
			assert_eq!(sess.au.id(), "user:2");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:2"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some
			// margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
		}

		// Test with correct credentials and "realistic" scenario
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS owner ON DATABASE TYPE RECORD
					SIGNUP (
						-- Allow anyone to sign up as a new company
						-- This automatically creates an owner with the same credentials
						CREATE company CONTENT {
							email: $email,
							pass: crypto::argon2::generate($pass),
							owner: (CREATE employee CONTENT {
								email: $email,
								pass: $pass,
							}),
						}
					)
					SIGNIN (
						-- Allow company owners to log in directly with the company account
						SELECT * FROM company WHERE email = $email AND crypto::argon2::compare(pass, $pass)
					)
					AUTHENTICATE (
						-- If logging in with a company account, the session will be authenticated as the first owner
						IF record::tb($auth) = "company" {
							RETURN SELECT VALUE owner FROM company WHERE id = $auth
						}
					)
					DURATION FOR SESSION 2h
				;

				CREATE company:1 CONTENT {
					email: "info@example.com",
					pass: crypto::argon2::generate("company-password"),
					owner: employee:2,
				};
				CREATE employee:1 CONTENT {
					email: "member@example.com",
					pass: crypto::argon2::generate("member-password"),
				};
				CREATE employee:2 CONTENT {
					email: "owner@example.com",
					pass: crypto::argon2::generate("owner-password"),
				};
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
			let mut vars = PublicVariables::new();
			vars.insert("email", "info@example.com");
			vars.insert("pass", "company-password");
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"owner".to_string(),
				vars,
			)
			.await;

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("owner".to_string()));
			assert!(sess.au.id().starts_with("employee:"));
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(sess.au.level().id().unwrap().starts_with("employee:"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some
			// margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
		}

		// Test being able to fail authentication
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP (
					   CREATE type::record('user', $id)
					)
					AUTHENTICATE {
					    -- Not just signin, this clause runs across signin, signup and authenticate, which makes it a nice place to centralize logic
					    IF !$auth.enabled {
							THROW "This user is not enabled";
						};

						-- Always need to return the user id back, otherwise auth generically fails
						RETURN $auth;
					}
					DURATION FOR SESSION 2h
				;
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
			let mut vars = PublicVariables::new();
			vars.insert("id", 1);
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::Thrown(e) => assert_eq!(e, "This user is not enabled"),
				e => panic!("Unexpected error, expected Thrown found {e:?}"),
			}
		}

		// Test AUTHENTICATE clause not returning a value
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP (
					   CREATE type::record('user', $id)
					)
					AUTHENTICATE {}
					DURATION FOR SESSION 2h
				;
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
			let mut vars = PublicVariables::new();
			vars.insert("id", 1);
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
	}

	#[tokio::test]
	#[ignore = "flaky"]
	async fn test_signup_record_transaction_conflict() {
		// Test SIGNUP failing due to datastore transaction conflict
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
					)
					SIGNUP {
						-- Concurrently write to the same document
						UPSERT count:1 SET count += 1;
						-- Increase the duration of the transaction
						sleep(500ms);
						-- Continue with authentication
						RETURN (CREATE user CONTENT {
							name: $user,
							pass: crypto::argon2::generate($pass)
						})
					}
					DURATION FOR SESSION 2h
				;
				"#,
				&sess,
				None,
			)
			.await
			.unwrap();

			// Sign up with the user twice at the same time
			let mut sess1 = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut sess2 = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars = PublicVariables::new();
			vars.insert("user", "user");
			vars.insert("pass", "pass");

			let (res1, res2) = tokio::join!(
				db_access(
					&ds,
					&mut sess1,
					"test".to_string(),
					"test".to_string(),
					"user".to_string(),
					vars.clone(),
				),
				db_access(
					&ds,
					&mut sess2,
					"test".to_string(),
					"test".to_string(),
					"user".to_string(),
					vars,
				)
			);

			match (res1, res2) {
				(Ok(r1), Ok(r2)) => panic!(
					"Expected authentication to fail in one instance, but instead received: {:?} and {:?}",
					r1, r2
				),
				(Err(e1), Err(e2)) => panic!(
					"Expected authentication to fail in one instance, but instead received: {:?} and {:?}",
					e1, e2
				),
				(Err(e1), Ok(_)) => match e1.downcast().expect("Unexpected error kind") {
					Error::UnexpectedAuth => {}
					e => panic!("Unexpected error, expected UnexpectedAuth found {e}"),
				},
				(Ok(_), Err(e2)) => match e2.downcast().expect("Unexpected error kind") {
					Error::UnexpectedAuth => {}
					e => panic!("Unexpected error, expected UnexpectedAuth found {e}"),
				},
			}
		}

		// Test AUTHENTICATE failing due to datastore transaction conflict
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNUP (
						CREATE type::record('user', $id)
					)
					AUTHENTICATE {
						-- Concurrently write to the same document
						UPSERT count:1 SET count += 1;
						-- Increase the duration of the transaction
						sleep(500ms);
						-- Continue with authentication
						$auth.id
					}
					DURATION FOR SESSION 2h
				;
				"#,
				&sess,
				None,
			)
			.await
			.unwrap();

			// Sign up with the user twice at the same time
			let mut sess1 = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut sess2 = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars = PublicVariables::new();
			vars.insert("id", 1);

			let (res1, res2) = tokio::join!(
				db_access(
					&ds,
					&mut sess1,
					"test".to_string(),
					"test".to_string(),
					"user".to_string(),
					vars.clone(),
				),
				db_access(
					&ds,
					&mut sess2,
					"test".to_string(),
					"test".to_string(),
					"user".to_string(),
					vars,
				)
			);

			match (res1, res2) {
				(Ok(r1), Ok(r2)) => panic!(
					"Expected authentication to fail in one instance, but instead received: {:?} and {:?}",
					r1, r2
				),
				(Err(e1), Err(e2)) => panic!(
					"Expected authentication to fail in one instance, but instead received: {:?} and {:?}",
					e1, e2
				),
				(Err(e1), Ok(_)) => match e1.downcast().expect("Unexpected error kind") {
					Error::UnexpectedAuth => {}
					e => panic!("Unexpected error, expected UnexpectedAuth found {e}"),
				},
				(Ok(_), Err(e2)) => match e2.downcast().expect("Unexpected error kind") {
					Error::UnexpectedAuth => {}
					e => panic!("Unexpected error, expected UnexpectedAuth found {e}"),
				},
			}
		}
	}
}
