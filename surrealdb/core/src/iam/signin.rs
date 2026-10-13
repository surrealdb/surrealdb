use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use chrono::Utc;
use jsonwebtoken::{EncodingKey, Header, encode};
use md5::Digest;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use surrealdb_cnf::SERVER_NAME;
use surrealdb_rpc::Token;
use surrealdb_types::ToSql;
use uuid::Uuid;

use super::access::{
	access_session, authenticate_generic, authenticate_record, create_refresh_token_record,
	reset_previous_principal, revoke_refresh_token_record, session_data_for_record,
};
use super::verify::{verify_db_creds, verify_ns_creds, verify_root_creds};
use super::{Actor, Level, Role};
use crate::catalog;
use crate::catalog::providers::{
	AuthorisationProvider, DatabaseProvider, NamespaceProvider, UserProvider,
};
use crate::catalog::{DatabaseDefinition, NamespaceDefinition};
use crate::dbs::Session;
use crate::err::{Error, exec_error};
use crate::exec::Error as ExecError;
use crate::expr::access_type;
use crate::expr::statements::access;
use crate::iam::issue::{config, expiration};
use crate::iam::token::{Audience, Claims, HEADER};
use crate::iam::{self, Auth, Error as AuthError, algorithm_to_jwt_algorithm};
use crate::kvs::TransactionType::*;
use crate::kvs::{Datastore, is_retryable_transaction_conflict};
use crate::types::{PublicValue, PublicVariables};
use crate::val::{Datetime, Value};

/// Authenticates a user and returns an authentication token.
///
/// This function handles user authentication for SurrealDB and returns a token
/// that can be used for subsequent API requests. The token may include both
/// access and refresh tokens depending on the authentication method and
/// configuration.
///
/// # Parameters
///
/// - `kvs`: The datastore instance for database operations
/// - `session`: The current session context
/// - `vars`: Public variables containing authentication credentials
///
/// # Returns
///
/// Returns a `Token` that can be either:
/// - An access token only
/// - An access token and a refresh token
///
/// # Authentication Methods
///
/// The function supports multiple authentication methods based on the provided variables:
/// - **Database access method**: When `NS`, `DB`, and `AC` are provided
/// - **Database user credentials**: When `NS`, `DB`, `user`, and `pass` are provided
/// - **Namespace user credentials**: When `NS`, `user`, and `pass` are provided
/// - **Root user credentials**: When `user` and `pass` are provided
///
/// # Examples
///
/// ```rust
/// use surrealdb_core::iam::signin;
/// use surrealdb_core::kvs::Datastore;
/// use surrealdb_core::dbs::Session;
/// use surrealdb_core::types::PublicVariables;
///
/// // Database access method
/// let vars = PublicVariables::from([
///     ("NS".to_string(), "test_namespace".into()),
///     ("DB".to_string(), "test_database".into()),
///     ("AC".to_string(), "my_access_method".into()),
///     // ... other access method parameters
/// ]);
///
/// let token = signin(&kvs, &mut session, vars).await?;
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
pub async fn signin(
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
		// DB signin with access method
		(Some(ns), Some(db), Some(ac)) => {
			// Process the provided values
			let ns = ns.into_string()?;
			let db = db.into_string()?;
			let ac = ac.into_string()?;
			// Attempt to signin using specified access method
			super::signin::db_access(kvs, session, ns, db, ac, vars).await
		}
		// DB signin with user credentials
		(Some(ns), Some(db), None) => {
			// Get the provided user and pass
			let user = vars.get("user").cloned();
			let pass = vars.get("pass").cloned();
			// Validate the user and pass
			match (user, pass) {
				// There is a username and password
				(Some(user), Some(pass)) => {
					// Process the provided values
					let ns = ns.into_string()?;
					let db = db.into_string()?;
					let user = user.into_string()?;
					let pass = pass.into_string()?;
					// Attempt to signin to database
					super::signin::db_user(kvs, session, ns, db, user, pass).await
				}
				_ => Err(anyhow::Error::new(AuthError::MissingUserOrPass)),
			}
		}
		// NS signin with access method
		(Some(ns), None, Some(ac)) => {
			// Process the provided values
			let ns = ns.into_string()?;
			let ac = ac.into_string()?;
			// Attempt to signin using specified access method
			super::signin::ns_access(kvs, session, ns, ac, vars).await
		}
		// NS signin with user credentials
		(Some(ns), None, None) => {
			// Get the provided user and pass
			let user = vars.get("user").cloned();
			let pass = vars.get("pass").cloned();
			// Validate the user and pass
			match (user, pass) {
				// There is a username and password
				(Some(user), Some(pass)) => {
					// Process the provided values
					let ns = ns.into_string()?;
					let user = user.into_string()?;
					let pass = pass.into_string()?;
					// Attempt to signin to namespace
					super::signin::ns_user(kvs, session, ns, user, pass).await
				}
				_ => Err(anyhow::Error::new(AuthError::MissingUserOrPass)),
			}
		}
		// ROOT signin with access method
		(None, None, Some(ac)) => {
			// Process the provided values
			let ac = ac.into_string()?;
			// Attempt to signin using the specified access method
			super::signin::root_access(kvs, session, ac, vars).await
		}
		// ROOT signin with user credentials
		(None, None, None) => {
			// Get the provided user and pass
			let user = vars.get("user").cloned();
			let pass = vars.get("pass").cloned();
			// Validate the user and pass
			match (user, pass) {
				// There is a username and password
				(Some(user), Some(pass)) => {
					// Process the provided values
					let user = user.into_string()?;
					let pass = pass.into_string()?;
					// Attempt to signin to root
					super::signin::root_user(kvs, session, user, pass).await
				}
				_ => Err(anyhow::Error::new(AuthError::MissingUserOrPass)),
			}
		}
		_ => Err(anyhow::Error::new(AuthError::NoSigninTarget)),
	}
}

/// Authenticates a user using a database access method.
///
/// This function handles authentication for users who have been granted access
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
/// - `vars`: Public variables containing authentication parameters
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
/// DURATION FOR SESSION 1d FOR TOKEN 15s
/// ```
///
/// # Examples
///
/// ```rust
/// use surrealdb_core::iam::signin::db_access;
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
	let tx = kvs.transaction(Read).await?;

	let ns_def = catch!(tx, tx.expect_ns_by_name(&ns).await);
	let db_def = catch!(tx, tx.expect_db_by_name(&ns, &db).await);

	// Fetch the specified access method from storage
	let Some(av) =
		catch!(tx, tx.get_db_access(db_def.namespace_id, db_def.database_id, &ac, None).await)
	else {
		let _ = tx.cancel().await;
		bail!(AuthError::AccessNotFound);
	};
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Check the provided access method exists

	// Check the access method type
	// All access method types are supported except for JWT
	// The equivalent of signing in with JWT is to authenticate it
	match av.access_type.clone() {
		catalog::AccessType::Record(at) => {
			// Check if the record access method supports issuing tokens
			let iss = match &at.jwt.issue {
				Some(iss) => iss.clone(),
				_ => bail!(AuthError::AccessMethodMismatch),
			};
			// Check if a refresh token is defined
			if let Some(bearer) = &at.bearer {
				// Check if a refresh token is being used to authenticate
				if let Some(key) = vars.get("refresh") {
					// Perform bearer access using the refresh token as the bearer key
					return signin_bearer(
						kvs,
						session,
						Some(&ns_def),
						Some(&db_def),
						av,
						bearer,
						key.as_string().ok_or(AuthError::InvalidAuth)?.clone(),
					)
					.await;
				}
			};
			match &av.signin {
				// This record access allows signin
				Some(val) => {
					// Setup the system session for finding the signin record
					let sess = access_session(&av, Some(ns.as_str()), Some(db.as_str()), session)?;
					// Compute the value with the params
					match kvs.evaluate(val, &sess, Some(vars)).await {
						// The signin value succeeded
						Ok(val) => {
							match val.into_record() {
								// There is a record returned
								Ok(mut rid) => {
									// Create the authentication key
									let key = iam::issue::config(iss.alg, &iss.key)?;
									// Create the authentication claim
									let claims = Claims {
										iss: Some(SERVER_NAME.to_owned()),
										iat: Some(Utc::now().timestamp()),
										nbf: Some(Utc::now().timestamp()),
										exp: iam::issue::expiration(av.token_duration)?,
										jti: Some(Uuid::new_v4().to_string()),
										aud: at
											.jwt
											.audience
											.as_ref()
											.map(|a| Audience::from_configured(a)),
										ns: Some(ns.clone()),
										db: Some(db.clone()),
										ac: Some(ac.clone()),
										id: Some(rid.to_sql()),
										..Claims::default()
									};
									// AUTHENTICATE clause
									if let Some(au) = &av.authenticate {
										// Setup the system session for finding the signin
										// record
										let mut sess = access_session(
											&av,
											Some(ns.as_str()),
											Some(db.as_str()),
											session,
										)?;
										sess.rd = Some(
											crate::val::convert_value_to_public_value(
												Value::RecordId(rid.clone().into()),
											)
											.expect("record id conversion should succeed"),
										);
										sess.tk = Some(
											crate::val::convert_value_to_public_value(
												claims.clone().into_claims_object().into(),
											)
											.expect("claims conversion should succeed"),
										);
										rid = authenticate_record(kvs, &sess, au).await?;
									}
									// CONTEXT clause. Evaluated once, after the
									// identity has settled, so `$auth` is the
									// final record. Independent of AUTHENTICATE.
									let data = session_data_for_record(
										kvs,
										&av,
										ns.as_str(),
										db.as_str(),
										PublicValue::RecordId(rid.clone()),
										&claims,
										session,
									)
									.await?;
									// Create refresh token if defined for the record access
									// method
									let refresh = match &at.bearer {
										Some(_) => Some(
											create_refresh_token_record(
												kvs,
												av.name.to_string(),
												&ns,
												&db,
												rid.clone().into(),
											)
											.await?,
										),
										None => None,
									};
									// Log the authenticated access method info
									trace!("Signing in to database with access method `{}`", ac);
									// Create the authentication token
									let enc = encode(
										&Header::new(algorithm_to_jwt_algorithm(iss.alg)),
										&claims,
										&key,
									);
									// Every fallible step is taken before the
									// session is touched: a partial failure
									// must leave the session in its
									// pre-authentication state.
									let session_exp = iam::issue::expiration(av.session_duration)?;
									// Set the authentication on the session
									reset_previous_principal(session);
									session.tk = Some(
										crate::val::convert_value_to_public_value(
											claims.into_claims_object().into(),
										)
										.expect("claims conversion should succeed"),
									);
									session.ns = Some(ns.clone());
									session.db = Some(db.clone());
									session.ac = Some(ac.clone());
									session.rd = Some(
										crate::val::convert_value_to_public_value(Value::RecordId(
											rid.clone().into(),
										))
										.expect("record id conversion should succeed"),
									);
									session.data = data;
									session.exp = session_exp;
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
										_ => Err(anyhow::Error::new(AuthError::TokenMakingFailed)),
									}
								}
								_ => Err(anyhow::Error::new(AuthError::NoRecordFound)),
							}
						}
						// If the SIGNIN clause throws a specific error, authentication
						// fails with that error
						Err(e) if matches!(exec_error(&e), Some(ExecError::Thrown(_))) => Err(e),
						Err(e) => {
							// If the SIGNIN clause failed due to an unexpected error, be
							// more specific This allows clients to handle these
							// errors, which may be retryable
							if is_retryable_transaction_conflict(&e) {
								debug!(
									"Unexpected error found while executing a SIGNIN clause: {e}"
								);
								Err(anyhow::Error::new(AuthError::UnexpectedAuth))
							} else {
								// Otherwise, return a generic error unless it should be
								// forwarded
								debug!("Record user signin query failed: {e}");
								if kvs.config().iam.insecure_forward_access_errors {
									Err(e)
								} else {
									Err(anyhow::Error::new(
										AuthError::AccessRecordSigninQueryFailed,
									))
								}
							}
						}
					}
				}
				_ => Err(anyhow::Error::new(AuthError::AccessRecordNoSignin)),
			}
		}
		catalog::AccessType::Bearer(at) => {
			// Extract key identifier and key from the provided variables.
			let key = match vars.get("key") {
				Some(key) => key.as_string().ok_or(AuthError::InvalidAuth)?.clone(),
				None => return Err(anyhow::Error::new(AuthError::AccessBearerMissingKey)),
			};

			signin_bearer(kvs, session, Some(&ns_def), Some(&db_def), av, &at, key).await
		}
		_ => Err(anyhow::Error::new(AuthError::AccessMethodMismatch)),
	}
}

fn auth_from_level_user(level: Level, user: &catalog::UserDefinition) -> Result<Auth> {
	let roles = user
		.roles
		.iter()
		.map(|x| Role::from_str(x))
		.collect::<Result<Vec<_>, _>>()
		.map_err(Error::from)?;
	let actor = Actor::new(user.name.to_string(), roles, level);
	Ok(Auth::new(actor))
}

pub async fn db_user(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	db: String,
	user: String,
	pass: String,
) -> Result<Token> {
	match verify_db_creds(kvs, &ns, &db, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: expiration(u.token_duration)?,
				jti: Some(Uuid::new_v4().to_string()),
				ns: Some(ns.clone()),
				db: Some(db.clone()),
				id: Some(user),
				..Claims::default()
			};
			// Log the authenticated database info
			trace!("Signing in to database `{ns}/{db}`");
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);

			let au = auth_from_level_user(Level::Database(ns.clone(), db.clone()), &u)?;
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(u.session_duration)?;

			// Set the authentication on the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(val.into_claims_object().into())
					.expect("claims conversion should succeed"),
			);
			session.ns = Some(ns.clone());
			session.db = Some(db.clone());
			session.exp = session_exp;
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Token::Access(tk)),
				_ => Err(anyhow::Error::new(AuthError::TokenMakingFailed)),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!(
				"Failed to verify signin credentials for user `{user}` in database `{ns}/{db}`: {e}"
			);
			Err(anyhow::Error::new(AuthError::InvalidAuth))
		}
	}
}

pub async fn ns_access(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	ac: String,
	vars: PublicVariables,
) -> Result<Token> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read).await?;
	let ns_def = catch!(tx, tx.expect_ns_by_name(&ns).await);
	// Fetch the specified access method from storage
	let Some(av) = catch!(tx, tx.get_ns_access(ns_def.namespace_id, &ac, None).await) else {
		let _ = tx.cancel().await;
		bail!(AuthError::AccessNotFound);
	};
	// Ensure that the transaction is cancelled
	tx.cancel().await?;

	// Check the access method type
	match av.access_type.clone() {
		catalog::AccessType::Bearer(at) => {
			// Extract key identifier and key from the provided variables.
			let key = match vars.get("key") {
				Some(key) => key.as_string().ok_or(AuthError::InvalidAuth)?.clone(),
				None => bail!(AuthError::AccessBearerMissingKey),
			};

			signin_bearer(kvs, session, Some(&ns_def), None, av, &at, key).await
		}
		_ => Err(anyhow::Error::new(AuthError::AccessMethodMismatch)),
	}
}

pub async fn ns_user(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	user: String,
	pass: String,
) -> Result<Token> {
	match verify_ns_creds(kvs, &ns, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: expiration(u.token_duration)?,

				jti: Some(Uuid::new_v4().to_string()),
				ns: Some(ns.clone()),
				id: Some(user),
				..Claims::default()
			};
			// Log the authenticated namespace info
			trace!("Signing in to namespace `{ns}`");
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);

			let au = auth_from_level_user(Level::Namespace(ns.clone()), &u)?;
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(u.session_duration)?;

			// Set the authentication on the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(val.into_claims_object().into())
					.expect("claims conversion should succeed"),
			);
			session.ns = Some(ns.clone());
			session.exp = session_exp;
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Token::Access(tk)),
				_ => Err(anyhow::Error::new(AuthError::TokenMakingFailed)),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!(
				"Failed to verify signin credentials for user `{user}` in namespace `{ns}`: {e}"
			);
			Err(anyhow::Error::new(AuthError::InvalidAuth))
		}
	}
}

pub async fn root_user(
	kvs: &Datastore,
	session: &mut Session,
	user: String,
	pass: String,
) -> Result<Token> {
	match verify_root_creds(kvs, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: expiration(u.token_duration)?,
				jti: Some(Uuid::new_v4().to_string()),
				id: Some(user),
				..Claims::default()
			};
			// Log the authenticated root info
			trace!("Signing in as root");
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);

			let au = auth_from_level_user(Level::Root, &u)?;
			// Every fallible step is taken before the session is touched: a partial
			// failure must leave the session in its pre-authentication state.
			let session_exp = expiration(u.session_duration)?;

			// Set the authentication on the session
			reset_previous_principal(session);
			session.tk = Some(
				crate::val::convert_value_to_public_value(val.into_claims_object().into())
					.expect("claims conversion should succeed"),
			);
			session.exp = session_exp;
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(Token::Access(tk)),
				_ => Err(anyhow::Error::new(AuthError::TokenMakingFailed)),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!("Failed to verify signin credentials for user `{user}` in root: {e}");
			Err(anyhow::Error::new(AuthError::InvalidAuth))
		}
	}
}

pub async fn root_access(
	kvs: &Datastore,
	session: &mut Session,
	ac: String,
	vars: PublicVariables,
) -> Result<Token> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read).await?;
	// Fetch the specified access method from storage
	let Some(av) = catch!(tx, tx.get_root_access(&ac, None).await) else {
		let _ = tx.cancel().await;
		bail!(AuthError::AccessNotFound);
	};

	// Ensure that the transaction is cancelled
	tx.cancel().await?;

	// Check the access method type
	match av.access_type.clone() {
		catalog::AccessType::Bearer(at) => {
			// Extract key identifier and key from the provided variables.
			let key = match vars.get("key") {
				Some(PublicValue::String(key)) => key.clone(),
				_ => return Err(anyhow::Error::new(AuthError::AccessBearerMissingKey)),
			};

			signin_bearer(kvs, session, None, None, av, &at, key).await
		}
		_ => Err(anyhow::Error::new(AuthError::AccessMethodMismatch)),
	}
}

/// Authenticates a user using bearer token authentication (refresh token flow).
///
/// This function handles authentication for refresh tokens, which are long-lived
/// bearer tokens that can be used to obtain new access tokens without re-authentication.
/// This is the core function that validates refresh tokens and issues new token pairs.
///
/// # Parameters
///
/// - `kvs`: The datastore instance for database operations
/// - `session`: The current session context to be updated with authentication state
/// - `ns`: Optional namespace definition for scoped authentication
/// - `db`: Optional database definition for scoped authentication
/// - `av`: The access method definition
/// - `at`: The bearer access configuration
/// - `key`: The bearer token (refresh token) to validate
///
/// # Returns
///
/// Returns a new `Token` containing both access and refresh tokens.
///
/// # Security
///
/// This function implements several security measures:
/// - Validates the bearer token format and extracts the grant identifier
/// - Checks if the grant exists and hasn't been revoked or expired
/// - Uses constant-time comparison to prevent timing attacks
/// - Revokes the old refresh token after successful authentication (single-use model)
/// - Issues a new refresh token for the next refresh cycle
///
/// # Errors
///
/// Returns an error if:
/// - The bearer access method doesn't support issuing tokens
/// - The grant identifier is invalid
/// - The grant doesn't exist, is revoked, or is expired
/// - The bearer token doesn't match the stored grant
/// - User roles cannot be retrieved (for system user grants)
pub(crate) async fn signin_bearer(
	kvs: &Datastore,
	session: &mut Session,
	ns: Option<&NamespaceDefinition>,
	db: Option<&DatabaseDefinition>,
	av: Arc<catalog::AccessDefinition>,
	at: &catalog::BearerAccess,
	key: String,
) -> Result<Token> {
	// Check if the bearer access method supports issuing tokens.
	let iss = match &at.jwt.issue {
		Some(iss) => iss.clone(),
		_ => bail!(AuthError::AccessMethodMismatch),
	};
	// Extract key identifier and key from the provided key.
	let kid = validate_grant_bearer(&key)?;

	// Create a new readonly transaction
	let tx = kvs.transaction(Read).await?;
	// Fetch the specified access grant from storage
	let gr = match (&ns, &db) {
		(Some(ns), Some(db)) => {
			catch!(
				tx,
				tx.get_db_access_grant(ns.namespace_id, db.database_id, &av.name, &kid, None).await
			)
		}
		(Some(ns), None) => {
			catch!(tx, tx.get_ns_access_grant(ns.namespace_id, &av.name, &kid, None).await)
		}
		(None, None) => catch!(tx, tx.get_root_access_grant(&av.name, &kid, None).await),
		(None, Some(_)) => {
			let _ = tx.cancel().await;
			bail!(ExecError::NsEmpty)
		}
	};

	let gr = match gr {
		Some(gr) => gr,
		None => {
			let _ = tx.cancel().await;
			bail!(AuthError::InvalidAuth);
		}
	};

	// Ensure that the transaction is cancelled.
	tx.cancel().await?;
	// Authenticate bearer key against stored grant.
	verify_grant_bearer(&gr, key)?;

	// If the subject of the grant is a system user, get their roles.
	let roles = if let catalog::Subject::User(user) = &gr.subject {
		// Create a new readonly transaction.
		let tx = kvs.transaction(Read).await?;
		// Fetch the specified user from storage.

		let user = match (&ns, &db) {
			(Some(ns), Some(db)) => {
				let res = catch!(
					tx,
					tx.get_db_user(ns.namespace_id, db.database_id, user, None).await.map_err(
						|e| {
							debug!(
								"Error retrieving user for bearer access to database `{}/{}`: {}",
								ns.name, db.name, e
							);
							// Return opaque error to avoid leaking grant subject existence.
							anyhow::Error::new(AuthError::InvalidAuth)
						}
					)
				);
				match res {
					Some(v) => v,
					None => {
						let _ = tx.cancel().await;
						bail!(AuthError::InvalidAuth);
					}
				}
			}
			(Some(ns), None) => {
				let res = catch!(
					tx,
					tx.get_ns_user(ns.namespace_id, user, None).await.map_err(|e| {
						debug!(
							"Error retrieving user for bearer access to namespace `{}`: {}",
							ns.name, e
						);
						// Return opaque error to avoid leaking grant subject existence.
						anyhow::Error::new(AuthError::InvalidAuth)
					})
				);
				match res {
					Some(v) => v,
					None => {
						let _ = tx.cancel().await;
						bail!(AuthError::InvalidAuth);
					}
				}
			}
			(None, None) => {
				let res = catch!(
					tx,
					tx.get_root_user(user, None).await.map_err(|e| {
						debug!("Error retrieving user for bearer access to root: {e}");
						// Return opaque error to avoid leaking grant subject existence.
						anyhow::Error::new(AuthError::InvalidAuth)
					})
				);
				match res {
					Some(v) => v,
					None => {
						let _ = tx.cancel().await;
						bail!(AuthError::InvalidAuth);
					}
				}
			}
			(None, Some(_)) => {
				let _ = tx.cancel().await;
				bail!(ExecError::NsEmpty)
			}
		};
		// Ensure that the transaction is cancelled.
		tx.cancel().await?;
		user.roles.clone()
	} else {
		vec![]
	};
	// Create the authentication key.
	let key = config(iss.alg, &iss.key)?;
	// Create the authentication claim.
	let claims = Claims {
		iss: Some(SERVER_NAME.to_owned()),
		iat: Some(Utc::now().timestamp()),
		nbf: Some(Utc::now().timestamp()),
		exp: expiration(av.token_duration)?,
		jti: Some(Uuid::new_v4().to_string()),
		aud: at.jwt.audience.as_ref().map(|a| Audience::from_configured(a)),
		ns: ns.map(|ns| ns.name.to_string()),
		db: db.map(|db| db.name.to_string()),
		ac: Some(av.name.to_string()),
		id: match &gr.subject {
			catalog::Subject::User(user) => Some(user.clone()),
			catalog::Subject::Record(rid) => Some(rid.to_sql()),
		},
		roles: match &gr.subject {
			catalog::Subject::User(_) => Some(roles.clone()),
			catalog::Subject::Record(_) => Default::default(),
		},
		..Claims::default()
	};
	// AUTHENTICATE clause
	if let Some(au) = &av.authenticate {
		// Setup the system session for executing the clause.
		let mut sess = access_session(
			&av,
			ns.map(|ns| ns.name.as_str()),
			db.map(|db| db.name.as_str()),
			session,
		)?;
		sess.tk = Some(
			crate::val::convert_value_to_public_value(claims.clone().into_claims_object().into())
				.expect("claims conversion should succeed"),
		);
		authenticate_generic(kvs, &sess, au).await?;
	}
	// CONTEXT clause. A bearer grant whose subject is a record settles on an
	// identity here just as signin/signup/token verification do, so it gets the
	// same treatment. This is also the refresh-token rotation path: refresh
	// tokens rotate without bound, so recomputing here is what stops a session
	// pinning the grant it was first issued with, forever.
	//
	// A grant whose subject is a system user has no record identity for `$auth`
	// to name, which is why `CONTEXT` is rejected at definition time for every
	// access method that cannot authenticate a record.
	let data = match (&gr.subject, &ns, &db) {
		(catalog::Subject::Record(rid), Some(ns), Some(db)) => {
			session_data_for_record(
				kvs,
				&av,
				&ns.name,
				&db.name,
				crate::val::convert_value_to_public_value(Value::from(rid.clone()))
					.expect("record id conversion should succeed"),
				&claims,
				session,
			)
			.await?
		}
		_ => None,
	};
	// If the bearer grant is a refresh token.
	let refresh = match at.kind {
		catalog::BearerAccessType::Refresh => {
			match &gr.subject {
				catalog::Subject::Record(rid) => {
					if let (Some(ns), Some(db)) = (&ns, &db) {
						// Revoke the used refresh token.
						revoke_refresh_token_record(
							kvs,
							gr.id.clone(),
							gr.ac.clone(),
							&ns.name,
							&db.name,
						)
						.await?;
						// Create a new refresh token to replace it.
						let refresh = create_refresh_token_record(
							kvs,
							gr.ac.clone(),
							&ns.name,
							&db.name,
							rid.clone(),
						)
						.await?;
						Some(refresh)
					} else {
						debug!(
							"Invalid attempt to authenticate as a record without a namespace and database"
						);
						bail!(AuthError::InvalidAuth);
					}
				}
				catalog::Subject::User(_) => {
					debug!(
						"Invalid attempt to authenticatea as a system user with a refresh token"
					);
					bail!(AuthError::InvalidAuth);
				}
			}
		}
		_ => None,
	};
	// Log the authenticated access method information.
	trace!("Signing in to database with bearer access method `{}`", av.name);
	// Create the authentication token.
	let enc = encode(&Header::new(algorithm_to_jwt_algorithm(iss.alg)), &claims, &key);
	// Resolve the new principal BEFORE any of it is written to the session.
	//
	// A root-level `BEARER FOR RECORD` grant reaches the Record arm below with
	// no namespace or database and bails; `expiration` can bail on an invalid
	// duration; a stored role that no longer parses can bail too. Computed
	// here, all three leave the session in its pre-authentication state
	// instead of a failed signin extending its expiry and scrambling its
	// scope.
	let session_exp = expiration(av.session_duration)?;
	let (au, rd) = match &gr.subject {
		catalog::Subject::User(user) => {
			let level = match (ns, db) {
				(Some(ns), Some(db)) => Level::Database(ns.name.to_string(), db.name.to_string()),
				(Some(ns), None) => Level::Namespace(ns.name.to_string()),
				(None, None) => Level::Root,
				(None, Some(_)) => bail!(ExecError::NsEmpty),
			};
			let au = Auth::new(Actor::new(
				user.clone(),
				roles
					.iter()
					.map(|e| Role::from_str(e))
					.collect::<Result<_, _>>()
					.map_err(Error::from)?,
				level,
			));
			(au, None)
		}
		catalog::Subject::Record(rid) => {
			let (Some(ns), Some(db)) = (ns, db) else {
				debug!(
					"Invalid attempt to authenticate as a record without a namespace and database"
				);
				bail!(AuthError::InvalidAuth);
			};
			let au = Auth::new(Actor::new(
				rid.to_sql(),
				Default::default(),
				Level::Record(ns.name.to_string(), db.name.to_string(), rid.to_sql()),
			));
			let rd = crate::val::convert_value_to_public_value(Value::from(rid.clone()))
				.expect("value conversion should succeed");
			(au, Some(rd))
		}
	};
	// Set the authentication on the session.
	//
	// IMPORTANT: These assignments overwrite the session's working context (ns, db, ac).
	// This is intentional behavior, especially during token refresh operations.
	// When refreshing, the session is restored to the original authentication scope
	// from the bearer grant (which comes from the expired access token's claims),
	// not the current session's working context that may have been modified by USE commands.
	// This maintains proper authentication boundaries and prevents scope confusion.
	reset_previous_principal(session);
	session.tk = Some(
		crate::val::convert_value_to_public_value(claims.into_claims_object().into())
			.expect("claims conversion should succeed"),
	);
	session.ns.clone_from(&ns.map(|ns| ns.name.to_string()));
	session.db.clone_from(&db.map(|db| db.name.to_string()));
	session.ac = Some(av.name.to_string());
	session.data = data;
	session.exp = session_exp;
	session.au = Arc::new(au);
	session.rd = rd;
	// Return the authentication token.
	match enc {
		Ok(token) => Ok(match refresh {
			Some(refresh) => Token::WithRefresh {
				access: token,
				refresh,
			},
			None => Token::Access(token),
		}),
		_ => Err(anyhow::Error::new(AuthError::TokenMakingFailed)),
	}
}

/// Validates a bearer token and extracts the grant identifier.
///
/// This function parses and validates the structure of a bearer token (refresh token)
/// and returns the grant identifier that can be used to look up the grant record
/// in the database.
///
/// # Bearer Token Format
///
/// Bearer tokens follow this format: `{prefix}-{type}-{identifier}-{key}`
///
/// - `prefix`: Always "sdb" for SurrealDB tokens
/// - `type`: The bearer access type (e.g., "refresh")
/// - `identifier`: A unique identifier for the grant (used as database key)
/// - `key`: The actual secret key value
///
/// # Parameters
///
/// - `key`: The bearer token string to validate
///
/// # Returns
///
/// Returns the grant identifier on success, which can be used to fetch
/// the grant record from the database.
///
/// # Errors
///
/// Returns an error if:
/// - The token doesn't have exactly 4 parts separated by "-"
/// - The bearer access type is invalid
/// - The identifier length doesn't match the expected length
/// - The key length doesn't match the expected length
///
/// # Examples
///
/// ```ignore
/// let grant_id = validate_grant_bearer("sdb-refresh-abc123def456-xyz789uvw012")?;
/// // grant_id == "abc123def456"
/// ```
pub fn validate_grant_bearer(key: &str) -> Result<String> {
	let parts: Vec<&str> = key.split("-").collect();
	ensure!(parts.len() == 4, AuthError::AccessGrantBearerInvalid);
	// Check that the prefix type exists.
	parse_bearer_access_type(parts[1])?;
	// Retrieve the key identifier from the provided key.
	let kid = parts[2];
	// Check the length of the key identifier.
	ensure!(kid.len() == access::GRANT_BEARER_ID_LENGTH, AuthError::AccessGrantBearerInvalid);
	// Retrieve the key from the provided key.
	let key = parts[3];
	// Check the length of the key.
	ensure!(key.len() == access::GRANT_BEARER_KEY_LENGTH, AuthError::AccessGrantBearerInvalid);

	Ok(kid.to_string())
}

pub(crate) fn verify_grant_bearer(
	gr: &Arc<catalog::AccessGrant>,
	key: String,
) -> Result<&catalog::GrantBearer> {
	// Check if the grant is revoked or expired.

	match (&gr.expiration, &gr.revocation) {
		(None, None) => {}
		(Some(exp), None) => {
			if exp < &Datetime::now() {
				// Return opaque error to avoid leaking revocation status.
				debug!("Bearer access grant `{}` for method `{}` is expired", gr.id, gr.ac);

				bail!(AuthError::InvalidAuth);
			}
		}
		(_, Some(_)) => {
			debug!("Bearer access grant `{}` for method `{}` is revoked", gr.id, gr.ac);
			bail!(AuthError::InvalidAuth);
		}
	}
	// Check if the provided key matches the bearer key in the grant.
	// We use time-constant comparison to prevent timing attacks.
	match &gr.grant {
		catalog::Grant::Bearer(bearer) => {
			// Hash provided signin bearer key.

			let mut hasher = Sha256::new();
			hasher.update(key);
			let hash = hasher.finalize();
			let hash_hex = format!("{hash:x}");
			// Compare hashed key to stored bearer key.
			let signin_key_bytes: &[u8] = hash_hex.as_bytes();
			let bearer_key_bytes: &[u8] = bearer.key.as_bytes();
			let ok: bool = bearer_key_bytes.ct_eq(signin_key_bytes).into();

			if ok {
				Ok(bearer)
			} else {
				debug!("Bearer access grant `{}` for method `{}` is invalid", gr.id, gr.ac);
				Err(anyhow::Error::new(AuthError::InvalidAuth))
			}
		}
		_ => Err(anyhow::Error::new(AuthError::AccessMethodMismatch)),
	}
}

/// Parses the access-type segment of a bearer grant key.
fn parse_bearer_access_type(s: &str) -> Result<access_type::BearerAccessType, crate::iam::Error> {
	match s.to_ascii_lowercase().as_str() {
		"bearer" => Ok(access_type::BearerAccessType::Bearer),
		"refresh" => Ok(access_type::BearerAccessType::Refresh),
		_ => Err(crate::iam::Error::AccessGrantBearerInvalid),
	}
}

#[cfg(test)]
mod tests {
	use chrono::Duration;
	use jsonwebtoken::{Algorithm, DecodingKey, Validation, dangerous, decode};
	use regex::Regex;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::iam::Role;
	use crate::sql::statements::define::DefineKind;
	use crate::sql::statements::define::user::PassType;
	use crate::sql::{Ast, Expr, Literal, TopLevelExpr};
	use crate::types::PublicObject;

	struct TestLevel {
		level: &'static str,
		ns: Option<&'static str>,
		db: Option<&'static str>,
	}

	const AVAILABLE_ROLES: [Role; 3] = [Role::Viewer, Role::Editor, Role::Owner];

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

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:test"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
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
			let mut vars = PublicVariables::new();
			vars.insert("user", "user");
			vars.insert("pass", "incorrect");
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;

			assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_signin_record_with_refresh() {
		// Test without refresh
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
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

			match res {
				Ok(data) => {
					assert!(
						!matches!(data, Token::WithRefresh { .. }),
						"Refresh token was unexpectedly returned"
					)
				}
				Err(e) => panic!("Failed to signin with credentials: {e}"),
			}
		}
		// Test with refresh
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
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

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			let refresh = match res {
				Ok(data) => match data {
					Token::WithRefresh {
						refresh,
						..
					} => refresh,
					Token::Access(_) => panic!("Refresh token was not returned"),
				},
				Err(e) => panic!("Failed to signin with credentials: {e}"),
			};
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:test"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
			// Signin with the refresh token
			let mut vars = PublicVariables::new();
			vars.insert("refresh", refresh.clone());
			let res = db_access(
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
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:test"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Expiration should match the defined duration
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to follow the defined duration"
			);
			// Attempt to sign in with the original refresh token
			let mut vars = PublicVariables::new();
			vars.insert("refresh", refresh);
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
				AuthError::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
		// Test with expired refresh
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
					)
					WITH REFRESH
					DURATION FOR GRANT 1s, FOR SESSION 2h
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
			let refresh = match res {
				Ok(data) => match data {
					Token::WithRefresh {
						refresh,
						..
					} => refresh,
					Token::Access(_) => panic!("Refresh token was not returned"),
				},
				Err(e) => panic!("Failed to signin with credentials: {e}"),
			};
			// Wait for the refresh token to expire
			std::thread::sleep(Duration::seconds(2).to_std().unwrap());
			// Signin with the refresh token
			let mut vars = PublicVariables::new();
			vars.insert("refresh", refresh.clone());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars,
			)
			.await;
			// Should fail due to the refresh token being expired
			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				AuthError::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
		// Test that only the hash of the refresh token is stored
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
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

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			let refresh = match res {
				Ok(data) => match data {
					Token::WithRefresh {
						refresh,
						..
					} => refresh,
					Token::Access(_) => panic!("Refresh token was not returned"),
				},
				Err(e) => panic!("Failed to signin with credentials: {e}"),
			};

			// Extract grant identifier from refresh token
			let id = refresh.split("-").collect::<Vec<&str>>()[2];

			// Test that returned refresh token is in plain text
			let ok = Regex::new(r"surreal-refresh-[a-zA-Z0-9]{12}-[a-zA-Z0-9]{24}").unwrap();
			assert!(ok.is_match(&refresh), "Output '{}' doesn't match regex '{}'", refresh, ok);

			// Get the stored bearer key representing the refresh token
			let tx = ds.transaction(Read).await.unwrap().enclose();
			let grant = tx
				.get_db_access_grant(NamespaceId(0), DatabaseId(0), "user", id, None)
				.await
				.unwrap()
				.unwrap();
			let key = match &grant.grant {
				catalog::Grant::Bearer(grant) => grant.key.clone(),
				_ => panic!("Incorrect grant type returned, expected a bearer grant"),
			};
			tx.cancel().await.unwrap();

			// Test that the returned key is a SHA-256 hash
			let ok = Regex::new(r"[0-9a-f]{64}").unwrap();
			assert!(ok.is_match(&key), "Output '{}' doesn't match regex '{}'", key, ok);
		}
	}

	#[tokio::test]
	async fn test_signin_record_with_jwt_issuer() {
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
			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:test"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
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
	async fn test_signin_record_with_audience() {
		// Single audience: issued tokens carry the `aud` claim and verify.
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
					WITH JWT ALGORITHM HS512 KEY 'secret' AUDIENCE "surrealdb-test"
					DURATION FOR SESSION 2h, FOR TOKEN 15m
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
			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			let sd = res.unwrap();
			let token = match &sd {
				Token::Access(token) => token,
				Token::WithRefresh {
					access: token,
					..
				} => token,
			};

			// The issued token carries the configured audience
			let mut val = Validation::new(Algorithm::HS512);
			val.set_audience(&["surrealdb-test"]);
			let token_data =
				decode::<Claims>(token, &DecodingKey::from_secret("secret".as_ref()), &val)
					.unwrap();
			match token_data.claims.aud {
				Some(Audience::Single(aud)) => assert_eq!(aud, "surrealdb-test"),
				aud => panic!("Expected a single audience claim, got {aud:?}"),
			}

			// The issued token round-trips through token verification
			let mut sess = Session::default();
			let res = crate::iam::verify::token(&ds, &mut sess, token).await;
			assert!(res.is_ok(), "Failed to verify issued token: {:?}", res);
			assert_eq!(sess.au.id(), "user:test");
		}
		// Several audiences: the `aud` claim is issued as an array.
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
					SIGNIN (
						SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass)
					)
					WITH JWT ALGORITHM HS512 KEY 'secret' AUDIENCE "aud1", "aud2"
					DURATION FOR SESSION 2h, FOR TOKEN 15m
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
			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
			let sd = res.unwrap();
			let token = match &sd {
				Token::Access(token) => token,
				Token::WithRefresh {
					access: token,
					..
				} => token,
			};

			// The issued token carries the configured audiences as an array
			let mut val = Validation::new(Algorithm::HS512);
			val.set_audience(&["aud1"]);
			let token_data =
				decode::<Claims>(token, &DecodingKey::from_secret("secret".as_ref()), &val)
					.unwrap();
			match token_data.claims.aud {
				Some(Audience::Multiple(aud)) => {
					assert_eq!(aud, vec!["aud1".to_string(), "aud2".to_string()])
				}
				aud => panic!("Expected an audience array claim, got {aud:?}"),
			}

			// The issued token round-trips through token verification
			let mut sess = Session::default();
			let res = crate::iam::verify::token(&ds, &mut sess, token).await;
			assert!(res.is_ok(), "Failed to verify issued token: {:?}", res);
			assert_eq!(sess.au.id(), "user:test");
		}
	}

	#[tokio::test]
	async fn test_signin_user() {
		#[derive(Debug)]
		struct TestCase {
			title: &'static str,
			password: &'static str,
			roles: Vec<Role>,
			token_expiration: Option<Duration>,
			session_expiration: Option<Duration>,
			expect_ok: bool,
		}

		let test_cases = vec![
			TestCase {
				title: "without roles or expiration",
				password: "pass",
				roles: vec![Role::Viewer],
				token_expiration: None,
				session_expiration: None,
				expect_ok: true,
			},
			TestCase {
				title: "with roles and expiration",
				password: "pass",
				roles: vec![Role::Editor, Role::Owner],
				token_expiration: Some(Duration::days(365)),
				session_expiration: Some(Duration::days(1)),
				expect_ok: true,
			},
			TestCase {
				title: "with invalid password",
				password: "invalid",
				roles: vec![],
				token_expiration: None,
				session_expiration: None,
				expect_ok: false,
			},
		];

		let test_levels = vec![
			TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			},
		];

		for level in &test_levels {
			for case in &test_cases {
				println!("Test case: {} level {}", level.level, case.title);
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");

				let roles_clause = if case.roles.is_empty() {
					String::new()
				} else {
					let roles: Vec<&str> = case
						.roles
						.iter()
						.map(|r| match r {
							Role::Viewer => "VIEWER",
							Role::Editor => "EDITOR",
							Role::Owner => "OWNER",
							_ => unreachable!("unknown role"),
						})
						.collect();
					format!("ROLES {}", roles.join(", "))
				};

				let mut duration_clause = String::new();
				if case.token_expiration.is_some() || case.session_expiration.is_some() {
					duration_clause = "DURATION".to_owned()
				}
				if let Some(duration) = case.token_expiration {
					duration_clause =
						format!("{} FOR TOKEN {}s", duration_clause, duration.num_seconds())
				}
				if let Some(duration) = case.session_expiration {
					duration_clause =
						format!("{} FOR SESSION {}s", duration_clause, duration.num_seconds())
				}

				let define_user_query = format!(
					"DEFINE USER user ON {} PASSWORD 'pass' {} {}",
					level.level, roles_clause, duration_clause,
				);

				ds.execute(&define_user_query, &sess, None).await.unwrap();

				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};

				let res = match level.level {
					"ROOT" => {
						root_user(&ds, &mut sess, "user".to_string(), case.password.to_string())
							.await
					}
					"NS" => {
						ns_user(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"user".to_string(),
							case.password.to_string(),
						)
						.await
					}
					"DB" => {
						db_user(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"user".to_string(),
							case.password.to_string(),
						)
						.await
					}
					_ => panic!("Unsupported level"),
				};

				if case.expect_ok {
					assert!(res.is_ok(), "Failed to signin: {:?}", res);
					assert_eq!(sess.ns, level.ns.map(|s| s.to_string()));
					assert_eq!(sess.db, level.db.map(|s| s.to_string()));
					assert_eq!(sess.au.level().ns(), level.ns);
					assert_eq!(sess.au.level().db(), level.db);
					assert_eq!(sess.au.id(), "user");

					// Check auth level
					match level.level {
						"ROOT" => assert!(sess.au.is_root()),
						"NS" => assert!(sess.au.is_ns()),
						"DB" => assert!(sess.au.is_db()),
						_ => panic!("Unsupported level"),
					}

					// Check roles
					for role in AVAILABLE_ROLES {
						let has_role = sess.au.has_role(role);
						let should_have_role = case.roles.contains(&role);
						assert_eq!(has_role, should_have_role, "Role {:?} check failed", role);
					}

					// Check session expiration
					if let Some(exp_duration) = case.session_expiration {
						let exp = sess.exp.unwrap();
						let min_exp =
							(Utc::now() + exp_duration - Duration::seconds(10)).timestamp();
						let max_exp =
							(Utc::now() + exp_duration + Duration::seconds(10)).timestamp();
						assert!(
							exp > min_exp && exp < max_exp,
							"Session expiration is expected to match the defined duration"
						);
					} else {
						assert_eq!(sess.exp, None, "Session expiration is expected to be None");
					}

					// Check issued token
					if let Ok(sd) = res {
						let token = match &sd {
							Token::Access(token) => token,
							Token::WithRefresh {
								access: token,
								..
							} => token,
						};
						// Decode token without validation
						let token_data = dangerous::insecure_decode::<Claims>(token).unwrap();

						// Check session expiration
						if let Some(exp_duration) = case.token_expiration {
							let exp = match token_data.claims.exp {
								Some(exp) => exp,
								_ => panic!("Token is missing expiration claim"),
							};
							let min_exp =
								(Utc::now() + exp_duration - Duration::seconds(10)).timestamp();
							let max_exp =
								(Utc::now() + exp_duration + Duration::seconds(10)).timestamp();
							assert!(
								exp > min_exp && exp < max_exp,
								"Session expiration is expected to match the defined duration"
							);
						} else {
							assert_eq!(sess.exp, None, "Session expiration is expected to be None");
						}

						// Check required token claims
						assert_eq!(token_data.claims.ns, level.ns.map(|s| s.to_string()));
						assert_eq!(token_data.claims.db, level.db.map(|s| s.to_string()));
						assert_eq!(token_data.claims.id, Some("user".to_string()));
					} else {
						panic!("Token could not be extracted from result")
					}
				} else {
					assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
				}
			}
		}
	}

	#[tokio::test]
	async fn test_signin_record_and_authenticate_clause() {
		// Test with correct credentials
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM type::record('user', $id)
					)
					AUTHENTICATE (
						-- Simple example increasing the record identifier by one
					    SELECT * FROM type::record('user', record::id($auth) + 1)
					)
					DURATION FOR SESSION 2h
				;

				CREATE user:1, user:2;
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

			assert!(res.is_ok(), "Failed to signin with credentials: {:?}", res);
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
			// Expiration should match the current time plus session duration with some margin
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
			assert_eq!(sess.au.id(), "employee:2");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("employee:2"));
			// Record users should not have roles
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
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

		// Test being able to fail authentication
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
                r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM type::record('user', $id)
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

				CREATE user:1 SET enabled = false;
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
				ExecError::Thrown(e) => assert_eq!(e, "This user is not enabled"),
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
					SIGNIN (
					   SELECT * FROM type::record('user', $id)
					)
					AUTHENTICATE {}
					DURATION FOR SESSION 2h
				;

				CREATE user:1;
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
				AuthError::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
	}

	#[tokio::test]
	#[ignore = "flaky"]
	async fn test_signin_record_transaction_conflict() {
		// Test SIGNIN failing due to datastore transaction conflict
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN {
						-- Concurrently write to the same document
						UPSERT count:1 SET count += 1;
						-- Increase the duration of the transaction
						sleep(500ms);
						-- Continue with authentication
						RETURN (SELECT * FROM user WHERE name = $user AND crypto::argon2::compare(pass, $pass))
					}
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

			// Sign in with the user twice at the same time
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
					AuthError::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e}"),
				},
				(Ok(_), Err(e2)) => match e2.downcast().expect("Unexpected error kind") {
					AuthError::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e}"),
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
				SIGNIN (
					SELECT * FROM type::record('user', $id)
				)
				AUTHENTICATE {
					-- Concurrently write to the same document
						UPSERT count:1 SET count += 1;
					-- Increase the duration of the transaction
						sleep(500ms);
					-- Continue with authentication
						$auth.id -- Continue with authentication
				}
				DURATION FOR SESSION 2h
				;

				CREATE user:1;
				"#,
				&sess,
				None,
			)
			.await
			.unwrap();

			// Sign in with the user twice at the same time
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
					AuthError::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e:?}"),
				},
				(Ok(_), Err(e2)) => match e2.downcast().expect("Unexpected error kind") {
					AuthError::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e:?}"),
				},
			}
		}
	}

	#[tokio::test]
	async fn test_signin_dispatch_bearer_above_database() {
		// Bearer signin must route correctly through the public `signin()`
		// entrypoint for namespace- and root-level access, not only through the
		// level-specific helpers. Root bearer has its own dispatch arm, so drive
		// `signin()` directly to guard that routing.
		for (level, ns) in [("ROOT", None), ("NS", Some("test"))] {
			let ds = Datastore::new("memory").await.unwrap();
			let setup = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					&format!(
						r#"
						DEFINE ACCESS api ON {level} TYPE BEARER FOR USER DURATION FOR SESSION 2h;
						DEFINE USER tobie ON {level} ROLES EDITOR;
						ACCESS api ON {level} GRANT FOR USER tobie;
						"#
					),
					&setup,
					None,
				)
				.await
				.unwrap();
			let result = res.last().unwrap().result.clone().unwrap();
			let grant = result.get("grant").clone().into_object().unwrap();
			let key = grant.get("key").unwrap().as_string().unwrap().clone();

			// Route through signin() with only the params a correct client sends:
			// { [ns], ac, key } and no database key.
			let mut sess = Session::default();
			let mut vars = PublicVariables::new();
			if let Some(ns) = ns {
				vars.insert("ns", ns);
			}
			vars.insert("ac", "api");
			vars.insert("key", key);
			let res = signin(&ds, &mut sess, vars).await;

			assert!(res.is_ok(), "{level} bearer signin failed via dispatch: {res:?}");
			assert_eq!(sess.ns.as_deref(), ns);
			assert_eq!(sess.db, None);
			match level {
				"ROOT" => assert!(sess.au.is_root(), "expected root auth"),
				"NS" => assert!(sess.au.is_ns(), "expected namespace auth"),
				_ => unreachable!(),
			}
			assert!(sess.au.has_role(Role::Editor));
			assert!(!sess.au.has_role(Role::Owner));
			assert!(sess.exp.is_some(), "session must carry a finite expiry");
		}

		// A root access method that does not exist is rejected, not silently
		// accepted: the dispatch reaches root_access, which errors.
		let ds = Datastore::new("memory").await.unwrap();
		let mut sess = Session::default();
		let mut vars = PublicVariables::new();
		vars.insert("ac", "nonexistent");
		vars.insert("key", "surreal-bearer-000000000000-000000000000000000000000");
		assert!(signin(&ds, &mut sess, vars).await.is_err());
	}

	/// The database-level [`TestLevel`] targeting the `test`/`test` namespace
	/// and database, as used by the record-based bearer access tests.
	const RECORD_DB_LEVEL: TestLevel = TestLevel {
		level: "DB",
		ns: Some("test"),
		db: Some("test"),
	};

	/// Bearer-for-record setup granting a key for `user:test` with a 2h
	/// session duration.
	const BEARER_RECORD_SETUP: &str = r#"
		DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
		DURATION FOR SESSION 2h
		;
		ACCESS api ON DATABASE GRANT FOR RECORD user:test;
		"#;

	/// Bearer-for-record setup granting a key for `user:test` that expires
	/// after one second.
	const BEARER_RECORD_SETUP_GRANT_1S: &str = r#"
		DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
		DURATION FOR GRANT 1s FOR SESSION 2h
		;
		ACCESS api ON DATABASE GRANT FOR RECORD user:test;
		"#;

	/// Runs `setup` as the owner of the `test`/`test` namespace and database
	/// and returns the grant object produced by its final statement, which
	/// must be an `ACCESS ... GRANT` statement.
	async fn bearer_grant_object(ds: &Datastore, setup: &str) -> PublicObject {
		let sess = Session::owner().with_ns("test").with_db("test");
		let res = ds.execute(setup, &sess, None).await.unwrap();
		let result = if let Ok(res) = &res.last().unwrap().result {
			res.clone()
		} else {
			panic!("Unable to retrieve bearer key grant");
		};
		result.get("grant").clone().into_object().unwrap()
	}

	/// Returns the plain-text bearer key of a grant object.
	fn grant_key(grant: &PublicObject) -> String {
		grant.get("key").unwrap().as_string().unwrap().clone()
	}

	/// Builds the setup statements for a bearer-for-user access method with
	/// the given extra clauses at the test level, together with a `tobie`
	/// user holding the EDITOR role and a bearer grant for it.
	fn bearer_user_setup(level: &TestLevel, clauses: &str) -> String {
		format!(
			r#"
			DEFINE ACCESS api ON {0} TYPE BEARER FOR USER
			{1}
			;
			DEFINE USER tobie ON {0} ROLES EDITOR;
			ACCESS api ON {0} GRANT FOR USER tobie;
			"#,
			level.level, clauses
		)
	}

	fn key_vars(key: String) -> PublicVariables {
		let mut vars = PublicVariables::new();
		vars.insert("key", key);
		vars
	}

	/// Dispatches a bearer signin with the given variables through the access
	/// function matching the test level, returning the result together with
	/// the session it authenticated.
	async fn signin_bearer_at_level(
		ds: &Datastore,
		level: &TestLevel,
		vars: PublicVariables,
	) -> (Result<Token>, Session) {
		let mut sess = Session {
			ns: level.ns.map(String::from),
			db: level.db.map(String::from),
			..Default::default()
		};
		let res = match level.level {
			"DB" => {
				db_access(
					ds,
					&mut sess,
					level.ns.unwrap().to_string(),
					level.db.unwrap().to_string(),
					"api".to_string(),
					vars,
				)
				.await
			}
			"NS" => {
				ns_access(ds, &mut sess, level.ns.unwrap().to_string(), "api".to_string(), vars)
					.await
			}
			"ROOT" => root_access(ds, &mut sess, "api".to_string(), vars).await,
			_ => panic!("Unsupported level"),
		};
		(res, sess)
	}

	/// Defines a bearer access method from `setup`, signs in with the granted
	/// key at the test level, and hands the authenticated session to `check`.
	async fn bearer_signin_ok_scenario(
		level: &TestLevel,
		setup: &str,
		check: impl FnOnce(&Session),
	) {
		let ds = Datastore::new("memory").await.unwrap();
		let grant = bearer_grant_object(&ds, setup).await;
		let (res, sess) = signin_bearer_at_level(&ds, level, key_vars(grant_key(&grant))).await;
		assert!(res.is_ok(), "Failed to sign in with bearer key: {:?}", res);
		check(&sess);
	}

	/// Defines a bearer access method from `setup`, signs in at the test
	/// level with the variables built from the granted key, and hands the
	/// resulting error to `check`.
	async fn bearer_signin_err_scenario(
		level: &TestLevel,
		setup: &str,
		vars: impl FnOnce(String) -> PublicVariables,
		check: impl FnOnce(anyhow::Error),
	) {
		let ds = Datastore::new("memory").await.unwrap();
		let grant = bearer_grant_object(&ds, setup).await;
		let (res, _) = signin_bearer_at_level(&ds, level, vars(grant_key(&grant))).await;
		check(res.unwrap_err());
	}

	/// Defines a bearer access method from `setup`, waits for the granted key
	/// to expire, and asserts that signing in with it is rejected.
	async fn bearer_signin_expired_scenario(level: &TestLevel, setup: &str) {
		let ds = Datastore::new("memory").await.unwrap();
		let grant = bearer_grant_object(&ds, setup).await;
		let key = grant_key(&grant);
		// Wait for the grant to expire
		std::thread::sleep(Duration::seconds(2).to_std().unwrap());
		let (res, _) = signin_bearer_at_level(&ds, level, key_vars(key)).await;
		expect_invalid_auth(res.unwrap_err());
	}

	/// Defines a bearer access method from `setup`, runs the statement built
	/// by `action` from the granted key as the owner, then signs in with the
	/// key at the test level and hands the resulting error to `check`.
	async fn bearer_signin_after_action_scenario(
		level: &TestLevel,
		setup: &str,
		action: impl FnOnce(&str) -> String,
		check: impl FnOnce(anyhow::Error),
	) {
		let ds = Datastore::new("memory").await.unwrap();
		let grant = bearer_grant_object(&ds, setup).await;
		let key = grant_key(&grant);
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(&action(&key), &sess, None).await.unwrap();
		let (res, _) = signin_bearer_at_level(&ds, level, key_vars(key)).await;
		check(res.unwrap_err());
	}

	/// Replaces the key character at `index` with one that is invalid in a
	/// bearer key.
	fn corrupt_key_at(valid_key: &str, index: usize) -> String {
		let mut invalid_key: Vec<char> = valid_key.chars().collect();
		invalid_key[index] = '_';
		invalid_key.into_iter().collect()
	}

	/// Removes the last character from the key.
	fn truncate_key(valid_key: &str) -> String {
		let mut invalid_key: Vec<char> = valid_key.chars().collect();
		invalid_key.truncate(invalid_key.len() - 1);
		invalid_key.into_iter().collect()
	}

	fn expect_invalid_auth(e: anyhow::Error) {
		match e.downcast().expect("Unexpected error kind") {
			AuthError::InvalidAuth => {}
			e => panic!("Unexpected error, expected InvalidAuth found {e}"),
		}
	}

	fn expect_access_not_found(e: anyhow::Error) {
		match e.downcast().expect("Unexpected error kind") {
			AuthError::AccessNotFound => {}
			e => panic!("Unexpected error, expected AccessNotFound found {e}"),
		}
	}

	fn expect_access_bearer_missing_key(e: anyhow::Error) {
		match e.downcast().expect("Unexpected error kind") {
			AuthError::AccessBearerMissingKey => {}
			e => panic!("Unexpected error, expected AccessBearerMissingKey found {e}"),
		}
	}

	fn expect_access_grant_bearer_invalid(e: anyhow::Error) {
		match e.downcast().expect("Unexpected error kind") {
			AuthError::AccessGrantBearerInvalid => {}
			e => panic!("Unexpected error, expected AccessGrantBearerInvalid found {e}"),
		}
	}

	fn expect_thrown_authentication_error(e: anyhow::Error) {
		match e.downcast().expect("Unexpected error kind") {
			ExecError::Thrown(e) => assert_eq!(e, "Test authentication error"),
			e => panic!("Unexpected error, expected Thrown found {e:?}"),
		}
	}

	/// Asserts the session state produced by a successful bearer-for-user
	/// signin at the test level: EDITOR role only and a 2h session expiry.
	fn assert_bearer_user_session(sess: &Session, level: &TestLevel) {
		assert_eq!(sess.ns, level.ns.map(|s| s.to_string()));
		assert_eq!(sess.db, level.db.map(|s| s.to_string()));
		match level.level {
			"ROOT" => assert!(sess.au.is_root()),
			"NS" => assert!(sess.au.is_ns()),
			"DB" => assert!(sess.au.is_db()),
			_ => panic!("Unsupported level"),
		}
		assert_eq!(sess.au.level().ns(), level.ns);
		assert_eq!(sess.au.level().db(), level.db);
		assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
		// User is defined with this role only
		assert!(sess.au.has_role(Role::Editor), "Auth user expected to have Editor role");
		assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
		let exp = sess.exp.unwrap();
		let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
		let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
		assert!(
			exp > min_exp && exp < max_exp,
			"Session expiration is expected to match the defined duration",
		);
	}

	/// Asserts the session state produced by a successful bearer-for-record
	/// signin as `user:test`: record-level auth without roles and a 2h
	/// session expiry.
	fn assert_bearer_record_session(sess: &Session) {
		assert_eq!(sess.ns, Some("test".to_string()));
		assert_eq!(sess.db, Some("test".to_string()));
		assert_eq!(sess.au.id(), "user:test");
		assert!(sess.au.is_record());
		assert_eq!(sess.au.level().ns(), Some("test"));
		assert_eq!(sess.au.level().db(), Some("test"));
		assert_eq!(sess.au.level().id(), Some("user:test"));
		// Record users should not have roles
		assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
		assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
		assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
		// Expiration should match the current time plus session duration with some margin
		let exp = sess.exp.unwrap();
		let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
		let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
		assert!(
			exp > min_exp && exp < max_exp,
			"Session expiration is expected to follow the defined duration"
		);
	}

	/// Verifies that a bearer-for-user grant returns its key in plain text
	/// exactly once while only a SHA-256 hash of it is stored.
	async fn bearer_user_key_stored_hashed(level: &TestLevel) {
		let ds = Datastore::new("memory").await.unwrap();
		let grant =
			bearer_grant_object(&ds, &bearer_user_setup(level, "DURATION FOR SESSION 2h")).await;
		let id = grant.get("id").unwrap().as_string().unwrap().clone();
		let key = grant_key(&grant);

		// Test that returned key is in plain text
		let plain_text_regex =
			Regex::new("surreal-bearer-[a-zA-Z0-9]{12}-[a-zA-Z0-9]{24}").unwrap();
		assert!(
			plain_text_regex.is_match(&key),
			"Output '{}' doesn't match regex '{}'",
			key,
			plain_text_regex
		);

		// Get the stored bearer grant
		let tx = ds.transaction(Read).await.unwrap().enclose();
		let grant = match level.level {
			"DB" => {
				let db = tx.expect_db_by_name(level.ns.unwrap(), level.db.unwrap()).await.unwrap();
				tx.get_db_access_grant(db.namespace_id, db.database_id, "api", &id, None)
					.await
					.unwrap()
			}
			"NS" => {
				let ns = tx.expect_ns_by_name(level.ns.unwrap()).await.unwrap();
				tx.get_ns_access_grant(ns.namespace_id, "api", &id, None).await.unwrap()
			}
			"ROOT" => tx.get_root_access_grant("api", &id, None).await.unwrap(),
			_ => panic!("Unsupported level"),
		}
		.unwrap();
		let key = match &grant.grant {
			catalog::Grant::Bearer(grant) => grant.key.clone(),
			_ => panic!("Incorrect grant type returned, expected a bearer grant"),
		};
		tx.cancel().await.unwrap();

		// Test that the returned key is a SHA-256 hash
		let sha_256_regex = Regex::new(r"[0-9a-f]{64}").unwrap();
		assert!(
			sha_256_regex.is_match(&key),
			"Output '{}' doesn't match regex '{}'",
			key,
			sha_256_regex
		);
	}

	/// Verifies that a bearer-for-record grant returns its key in plain text
	/// exactly once while only a SHA-256 hash of it is stored.
	async fn bearer_record_key_stored_hashed(setup: &str) {
		let ds = Datastore::new("memory").await.unwrap();
		let grant = bearer_grant_object(&ds, setup).await;
		let id = grant.get("id").unwrap().as_string().unwrap().clone();
		let key = grant_key(&grant);

		// Test that returned key is in plain text
		let ok = Regex::new(r"surreal-bearer-[a-zA-Z0-9]{12}-[a-zA-Z0-9]{24}").unwrap();
		assert!(ok.is_match(&key), "Output '{}' doesn't match regex '{}'", key, ok);

		// Get the stored bearer grant
		let tx = ds.transaction(Read).await.unwrap().enclose();
		let grant = tx
			.get_db_access_grant(NamespaceId(0), DatabaseId(0), "api", &id, None)
			.await
			.unwrap()
			.unwrap();
		let key = match &grant.grant {
			catalog::Grant::Bearer(grant) => grant.key.clone(),
			_ => panic!("Incorrect grant type returned, expected a bearer grant"),
		};
		tx.cancel().await.unwrap();

		// Test that the returned key is a SHA-256 hash
		let ok = Regex::new(r"[0-9a-f]{64}").unwrap();
		assert!(ok.is_match(&key), "Output '{}' doesn't match regex '{}'", key, ok);
	}

	#[tokio::test]
	async fn test_signin_bearer_for_user() {
		let test_levels = vec![
			TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			},
		];

		for level in &test_levels {
			println!("Test level: {}", level.level);

			// Test with correct bearer key
			bearer_signin_ok_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR SESSION 2h"),
				|sess| assert_bearer_user_session(sess, level),
			)
			.await;

			// Test with correct bearer key and AUTHENTICATE clause succeeding
			bearer_signin_ok_scenario(
				level,
				&bearer_user_setup(
					level,
					r#"AUTHENTICATE {
						RETURN NONE
					}
					DURATION FOR SESSION 2h"#,
				),
				|sess| assert_bearer_user_session(sess, level),
			)
			.await;

			// Test with correct bearer key and AUTHENTICATE clause failing
			bearer_signin_err_scenario(
				level,
				&bearer_user_setup(
					level,
					r#"AUTHENTICATE {
						THROW "Test authentication error";
					}
					DURATION FOR SESSION 2h"#,
				),
				key_vars,
				expect_thrown_authentication_error,
			)
			.await;

			// Test with expired grant
			bearer_signin_expired_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR GRANT 1s FOR SESSION 2h"),
			)
			.await;

			// Test with revoked grant
			bearer_signin_after_action_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR GRANT 1s FOR SESSION 2h"),
				|key| {
					// Get grant identifier from key
					let kid = key.split("-").collect::<Vec<&str>>()[2];
					format!("ACCESS api ON {} REVOKE GRANT {kid}", level.level)
				},
				expect_invalid_auth,
			)
			.await;

			// Test with removed access method
			bearer_signin_after_action_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR GRANT 1s FOR SESSION 2h"),
				|_| format!("REMOVE ACCESS api ON {}", level.level),
				expect_access_not_found,
			)
			.await;

			// Test with missing key
			bearer_signin_err_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR SESSION 2h"),
				// The key parameter is not inserted:
				|_| PublicVariables::new(),
				expect_access_bearer_missing_key,
			)
			.await;

			// Test with incorrect bearer key prefix part
			bearer_signin_err_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR SESSION 2h"),
				|key| key_vars(corrupt_key_at(&key, "surreal-".len() + 2)),
				expect_access_grant_bearer_invalid,
			)
			.await;

			// Test with incorrect bearer key length
			bearer_signin_err_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR SESSION 2h"),
				|key| key_vars(truncate_key(&key)),
				expect_access_grant_bearer_invalid,
			)
			.await;

			// Test with incorrect bearer key identifier part
			bearer_signin_err_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR SESSION 2h"),
				|key| {
					key_vars(corrupt_key_at(
						&key,
						access_type::BearerAccessType::Bearer.prefix().len() + 2,
					))
				},
				expect_invalid_auth,
			)
			.await;

			// Test with incorrect bearer key value
			bearer_signin_err_scenario(
				level,
				&bearer_user_setup(level, "DURATION FOR SESSION 2h"),
				|key| key_vars(corrupt_key_at(&key, key.len() - 2)),
				expect_invalid_auth,
			)
			.await;

			// Test that only the key hash is stored
			bearer_user_key_stored_hashed(level).await;
		}
	}

	#[tokio::test]
	async fn test_signin_bearer_for_record() {
		// Test with correct bearer key and existing record
		bearer_signin_ok_scenario(
			&RECORD_DB_LEVEL,
			r#"
			DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
			DURATION FOR SESSION 2h
			;
			CREATE user:test;
			ACCESS api ON DATABASE GRANT FOR RECORD user:test;
			"#,
			assert_bearer_record_session,
		)
		.await;

		// Test with correct bearer key and non-existing record
		bearer_signin_ok_scenario(
			&RECORD_DB_LEVEL,
			BEARER_RECORD_SETUP,
			assert_bearer_record_session,
		)
		.await;

		// Test with correct bearer key and AUTHENTICATE clause succeeding
		bearer_signin_ok_scenario(
			&RECORD_DB_LEVEL,
			r#"
			DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
			AUTHENTICATE {{
				RETURN NONE
			}}
			DURATION FOR SESSION 2h
			;
			ACCESS api ON DATABASE GRANT FOR RECORD user:test;
			"#,
			assert_bearer_record_session,
		)
		.await;

		// Test with correct bearer key and AUTHENTICATE clause failing
		bearer_signin_err_scenario(
			&RECORD_DB_LEVEL,
			r#"
			DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
			AUTHENTICATE {{
				THROW "Test authentication error";
			}}
			DURATION FOR SESSION 2h
			;
			ACCESS api ON DATABASE GRANT FOR RECORD user:test;
			"#,
			key_vars,
			expect_thrown_authentication_error,
		)
		.await;

		// Test with expired grant
		bearer_signin_expired_scenario(&RECORD_DB_LEVEL, BEARER_RECORD_SETUP_GRANT_1S).await;

		// Test with revoked grant
		bearer_signin_after_action_scenario(
			&RECORD_DB_LEVEL,
			BEARER_RECORD_SETUP_GRANT_1S,
			|key| {
				// Get grant identifier from key
				let kid = key.split("-").collect::<Vec<&str>>()[2];
				format!("ACCESS api ON DATABASE REVOKE GRANT {kid}")
			},
			expect_invalid_auth,
		)
		.await;

		// Test with removed access method
		bearer_signin_after_action_scenario(
			&RECORD_DB_LEVEL,
			BEARER_RECORD_SETUP_GRANT_1S,
			|_| "REMOVE ACCESS api ON DATABASE".to_string(),
			expect_access_not_found,
		)
		.await;

		// Test with missing key
		bearer_signin_err_scenario(
			&RECORD_DB_LEVEL,
			BEARER_RECORD_SETUP,
			// The key parameter is not inserted:
			|_| PublicVariables::new(),
			expect_access_bearer_missing_key,
		)
		.await;

		// Test with incorrect bearer key prefix part
		bearer_signin_err_scenario(
			&RECORD_DB_LEVEL,
			BEARER_RECORD_SETUP,
			|key| key_vars(corrupt_key_at(&key, "surreal-".len() + 2)),
			expect_access_grant_bearer_invalid,
		)
		.await;

		// Test with incorrect bearer key length
		bearer_signin_err_scenario(
			&RECORD_DB_LEVEL,
			BEARER_RECORD_SETUP,
			|key| key_vars(truncate_key(&key)),
			expect_access_grant_bearer_invalid,
		)
		.await;

		// Test with incorrect bearer key identifier part
		bearer_signin_err_scenario(
			&RECORD_DB_LEVEL,
			BEARER_RECORD_SETUP,
			|key| {
				key_vars(corrupt_key_at(
					&key,
					access_type::BearerAccessType::Bearer.prefix().len() + 2,
				))
			},
			expect_invalid_auth,
		)
		.await;

		// Test with incorrect bearer key value
		bearer_signin_err_scenario(
			&RECORD_DB_LEVEL,
			BEARER_RECORD_SETUP,
			|key| key_vars(corrupt_key_at(&key, key.len() - 2)),
			expect_invalid_auth,
		)
		.await;

		// Test that only the key hash is stored
		bearer_record_key_stored_hashed(BEARER_RECORD_SETUP).await;
	}

	#[tokio::test]
	async fn test_signin_nonexistent_role() {
		use crate::iam::PolicyError;
		use crate::sql::Base;
		use crate::sql::statements::define::{DefineStatement, DefineUserStatement};
		let test_levels = vec![
			TestLevel {
				level: "ROOT",
				ns: None,
				db: None,
			},
			TestLevel {
				level: "NS",
				ns: Some("test"),
				db: None,
			},
			TestLevel {
				level: "DB",
				ns: Some("test"),
				db: Some("test"),
			},
		];

		for level in &test_levels {
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");

			let base = match level.level {
				"ROOT" => Base::Root,
				"NS" => Base::Ns,
				"DB" => Base::Db,
				_ => panic!("Unsupported level"),
			};

			let user = DefineUserStatement {
				kind: DefineKind::Default,
				base,
				name: crate::sql::Expr::Idiom(crate::sql::Idiom::field("user".to_string())),
				// This is the Argon2id hash for "pass" with a random salt.
				pass_type: PassType::Hash(
					"$argon2id$v=19$m=16,t=2,p=1$VUlHTHVOYjc5d0I1dGE3OQ$sVtmRNH+Xtiijk0uXL2+4w"
						.to_string(),
				),
				scram: None,
				roles: vec!["nonexistent".to_owned()],
				session_duration: Expr::Literal(Literal::None),
				token_duration: Expr::Literal(Literal::None),
				comment: Expr::Literal(Literal::None),
			};

			let ast = Ast {
				expressions: vec![TopLevelExpr::Expr(Expr::Define(Box::new(
					DefineStatement::User(user),
				)))],
			};

			// Use pre-parsed definition, which bypasses the existent role check during parsing.
			ds.process(ast, &sess, None).await.unwrap();

			let mut sess = Session {
				ns: level.ns.map(String::from),
				db: level.db.map(String::from),
				..Default::default()
			};

			// Sign in using the newly defined user.
			let res = match level.level {
				"ROOT" => root_user(&ds, &mut sess, "user".to_string(), "pass".to_string()).await,
				"NS" => {
					ns_user(
						&ds,
						&mut sess,
						level.ns.unwrap().to_string(),
						"user".to_string(),
						"pass".to_string(),
					)
					.await
				}
				"DB" => {
					db_user(
						&ds,
						&mut sess,
						level.ns.unwrap().to_string(),
						level.db.unwrap().to_string(),
						"user".to_string(),
						"pass".to_string(),
					)
					.await
				}
				_ => panic!("Unsupported level"),
			};

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::IamError(PolicyError::InvalidRole(_)) => {}
				e => panic!(
					"Unexpected error, expected IamError(PolicyError::InvalidRole) found {e}"
				),
			}
		}
	}
}

/// Tests for the `CONTEXT` clause on `DEFINE ACCESS`.
///
/// The clause is evaluated once, at authentication time, with `$auth` already
/// bound to the settled identity, and its whole result — no `into_record()` —
/// is stored on the session and read back as `$session.data`.
#[cfg(test)]
mod context_clause_tests {
	use surrealdb_types::Value as PV;

	use super::*;
	use crate::dbs::Session;
	use crate::types::PublicVariables;

	/// `DEFINE ACCESS ... CONTEXT ...` used by most of these tests: `user:1`
	/// owns two `grant` rows, and the clause projects just their `org` values.
	const SETUP: &str = r#"
		DEFINE ACCESS user ON DATABASE TYPE RECORD
			SIGNIN ( SELECT * FROM type::record('user', $id) )
			CONTEXT ( SELECT VALUE org FROM grant WHERE owner = $auth )
			DURATION FOR SESSION 2h
		;
		DEFINE TABLE grant SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
		CREATE user:1, user:2;
		CREATE grant:a SET owner = user:1, org = "acme";
		CREATE grant:b SET owner = user:1, org = "globex";
		CREATE grant:c SET owner = user:2, org = "initech";
	"#;

	async fn signin_as(ds: &Datastore, id: i64) -> (Session, Result<Token>) {
		let mut sess = Session {
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			..Default::default()
		};
		let mut vars = PublicVariables::new();
		vars.insert("id", id);
		let res = db_access(
			ds,
			&mut sess,
			"test".to_string(),
			"test".to_string(),
			"user".to_string(),
			vars,
		)
		.await;
		(sess, res)
	}

	async fn setup(sql: &str) -> std::sync::Arc<Datastore> {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		let res = ds.execute(sql, &owner, None).await.unwrap();
		for r in res {
			r.result.unwrap();
		}
		ds
	}

	/// The clause parses, evaluates once with `$auth` bound to the settled
	/// identity, and its whole result is readable back as `$session.data`.
	#[tokio::test]
	async fn context_is_evaluated_with_auth_bound_and_readable_as_session_data() {
		let ds = setup(SETUP).await;
		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");

		// The clause saw `$auth` — it returned only user:1's grants.
		let data = sess.data.as_ref().expect("CONTEXT should have populated session data");
		let mut orgs = data.public().clone().into_array().unwrap().into_vec();
		orgs.sort_by_key(|v| v.clone().into_string().unwrap());
		assert_eq!(orgs, vec![PV::String("acme".to_owned()), PV::String("globex".to_owned())]);

		// And the same value is what a query reads back.
		let res = ds.execute("RETURN $session.data", &sess, None).await.unwrap();
		let mut got =
			res.into_iter().next().unwrap().result.unwrap().into_array().unwrap().into_vec();
		got.sort_by_key(|v| v.clone().into_string().unwrap());
		assert_eq!(got, vec![PV::String("acme".to_owned()), PV::String("globex".to_owned())]);

		// `$auth` is untouched: still a bare record link, not the projection.
		let res = ds.execute("RETURN $auth", &sess, None).await.unwrap();
		assert_eq!(
			res.into_iter().next().unwrap().result.unwrap(),
			PV::RecordId(surrealdb_types::RecordId::new("user", 1))
		);
	}

	/// The result is stored WHOLE. A record-shaped projection stays an object
	/// with all its fields, rather than being reduced to a link the way
	/// `authenticate_record` reduces the AUTHENTICATE clause's result.
	#[tokio::test]
	async fn context_result_is_stored_whole() {
		let ds = setup(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM type::record('user', $id) )
				CONTEXT ( SELECT * FROM ONLY $auth )
				DURATION FOR SESSION 2h
			;
			DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
			CREATE user:1 SET name = "tobie", tier = "gold";
			"#,
		)
		.await;
		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");

		let res = ds.execute("RETURN $session.data.tier", &sess, None).await.unwrap();
		assert_eq!(res.into_iter().next().unwrap().result.unwrap(), PV::String("gold".to_owned()));
	}

	/// A permission clause can read the materialised payload.
	#[tokio::test]
	async fn context_data_is_readable_from_a_permission() {
		let ds = setup(&format!(
			r#"
			{SETUP}
			DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE org IN $session.data;
			CREATE doc:1 SET org = "acme";
			CREATE doc:2 SET org = "globex";
			CREATE doc:3 SET org = "initech";
			"#
		))
		.await;
		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");

		let res = ds.execute("SELECT VALUE id FROM doc ORDER BY id", &sess, None).await.unwrap();
		let rows = res.into_iter().next().unwrap().result.unwrap().into_array().unwrap().into_vec();
		assert_eq!(
			rows,
			vec![
				PV::RecordId(surrealdb_types::RecordId::new("doc", 1)),
				PV::RecordId(surrealdb_types::RecordId::new("doc", 2)),
			],
			"only the rows the CONTEXT payload grants should be visible"
		);
	}

	/// `LET $session = ...` is refused, by any of the three routes a client
	/// has to it.
	///
	/// That is the whole of the claim, and it is worth being precise about
	/// what it is not. `$session.data` is exactly as trustworthy as `$auth` —
	/// no better, no worse. `$session` being in `PROTECTED_PARAM_NAMES` closes
	/// `LET`, `db.set` and the RPC `let`; it does not close every route. A
	/// `FOR $session IN [...] { ... }` loop rebinds the name for its body, and
	/// so does a closure parameter, and a permission clause evaluated inside
	/// one reads the rebound value. `FOR $auth IN [user:2] { ... }` does the
	/// same to `$auth`, which every record permission written before this
	/// clause existed already depends on.
	///
	/// That is a pre-existing, engine-wide property of parameter binding, not
	/// something this clause introduces, and it is being addressed separately.
	/// `$session.data` simply inherits the trust boundary `$auth` already had:
	/// a permission that is safe to write against `$auth` is exactly as safe
	/// written against `$session.data`.
	#[tokio::test]
	async fn let_session_is_refused_by_every_client_route() {
		let ds = setup(SETUP).await;
		let (mut sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");

		// A top-level LET.
		let res = ds.execute(r#"LET $session = { data: ["initech"] }"#, &sess, None).await.unwrap();
		assert!(res.into_iter().next().unwrap().result.is_err(), "LET $session must be refused");

		// The same inside a block, which is a different execution path.
		let res = ds
			.execute(
				r#"{ LET $session = { data: ["initech"] }; RETURN $session.data }"#,
				&sess,
				None,
			)
			.await
			.unwrap();
		assert!(
			res.into_iter().next().unwrap().result.is_err(),
			"LET $session inside a block must be refused"
		);

		// And as a session variable, the `db.set` / RPC `let` route.
		sess.variables.insert("session", PV::String("forged".to_owned()));
		let res = ds.execute("RETURN $session.data", &sess, None).await;
		let refused = match res {
			Err(_) => true,
			Ok(res) => res.into_iter().next().unwrap().result.is_err(),
		};
		assert!(refused, "a session variable named `session` must be refused");
	}

	/// The CONTEXT clause runs as the authenticated record, not privileged, so
	/// it cannot materialise anything the session could not have selected for
	/// itself. This is what makes exposing the result to the client safe.
	#[tokio::test]
	async fn context_is_evaluated_with_the_records_own_permissions() {
		let ds = setup(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM type::record('user', $id) )
				CONTEXT ( SELECT VALUE secret FROM vault )
				DURATION FOR SESSION 2h
			;
			DEFINE TABLE vault SCHEMALESS PERMISSIONS NONE;
			CREATE user:1;
			CREATE vault:1 SET secret = "hunter2";
			"#,
		)
		.await;
		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");
		let data = sess.data.as_ref().expect("CONTEXT populates data even when it reads nothing");
		assert_eq!(
			data.public().clone().into_array().unwrap().into_vec(),
			Vec::<PV>::new(),
			"a table the record cannot read must not leak through CONTEXT"
		);
	}

	/// An access method without the clause behaves exactly as before: no `data`
	/// on the session, and `$session` carries the same keys it always did.
	#[tokio::test]
	async fn access_without_the_clause_is_unchanged() {
		let ds = setup(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM type::record('user', $id) )
				DURATION FOR SESSION 2h
			;
			CREATE user:1;
			"#,
		)
		.await;
		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");
		assert!(sess.data.is_none(), "no CONTEXT clause means no session data");

		let res = ds.execute("RETURN $session.data", &sess, None).await.unwrap();
		assert_eq!(res.into_iter().next().unwrap().result.unwrap(), PV::None);

		// The `$session` object itself is byte-identical to what it was before
		// the clause existed: exactly the nine pre-existing keys.
		let res = ds.execute("RETURN $session", &sess, None).await.unwrap();
		let obj = res.into_iter().next().unwrap().result.unwrap().into_object().unwrap();
		let mut keys: Vec<_> = obj.keys().cloned().collect();
		keys.sort();
		assert_eq!(keys, vec!["ac", "db", "exp", "id", "ip", "ns", "or", "rd", "tk"]);
	}

	/// The clause is independent of AUTHENTICATE: it must fire whether or not
	/// one is defined, and when one is, it must see the identity AUTHENTICATE
	/// settled on rather than the one signin produced.
	#[tokio::test]
	async fn context_sees_the_identity_authenticate_settled_on() {
		let ds = setup(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM type::record('user', $id) )
				AUTHENTICATE ( SELECT * FROM ONLY type::record('user', record::id($auth) + 1) )
				CONTEXT ( RETURN $auth )
				DURATION FOR SESSION 2h
			;
			CREATE user:1, user:2;
			"#,
		)
		.await;
		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");
		assert_eq!(
			sess.data.as_ref().unwrap().public(),
			&PV::RecordId(surrealdb_types::RecordId::new("user", 2)),
			"CONTEXT must run after AUTHENTICATE has settled the identity"
		);
	}

	/// `$token.ID` and `$auth` name the same record inside the clause.
	///
	/// The `id` claim is minted before AUTHENTICATE runs and still names the
	/// record SIGNIN found. CONTEXT is the first clause to receive both the
	/// claims and the settled identity at once, so without the rebuild a
	/// clause keyed on `$token.ID` would resolve against the record SIGNIN
	/// found rather than the one AUTHENTICATE settled on — the exact
	/// substitution AUTHENTICATE exists to perform.
	#[tokio::test]
	async fn context_token_id_names_the_identity_authenticate_settled_on() {
		let ds = setup(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM type::record('user', $id) )
				AUTHENTICATE ( SELECT * FROM ONLY type::record('user', record::id($auth) + 1) )
				CONTEXT ( { auth: $auth, token: $token.ID } )
				DURATION FOR SESSION 2h
			;
			CREATE user:1, user:2;
			"#,
		)
		.await;
		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");
		let data = sess.data.as_ref().unwrap().public().clone().into_object().unwrap();
		assert_eq!(
			data.get("auth").unwrap(),
			&PV::RecordId(surrealdb_types::RecordId::new("user", 2)),
			"$auth is the settled identity"
		);
		assert_eq!(
			data.get("token").unwrap(),
			&PV::String("user:2".to_owned()),
			"$token.ID must name the same record as $auth"
		);
	}

	/// A signin that can still fail must not have committed session state.
	///
	/// `signin_bearer` assigned `tk`, `ns`, `db`, `ac` and `data` and only
	/// then computed the session expiry, which can bail on a duration that
	/// overflows. The failed attempt therefore installed a new token, a new
	/// access method and a CONTEXT payload on a session whose `au` still named
	/// the previous principal.
	#[tokio::test]
	async fn a_failed_bearer_signin_leaves_the_session_untouched() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		let res = ds
			.execute(
				r#"
				DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					CONTEXT ( RETURN 'granted' )
					DURATION FOR TOKEN 1h, FOR SESSION 100000000w;
				CREATE user:1;
				ACCESS api ON DATABASE GRANT FOR RECORD user:1;
				"#,
				&owner,
				None,
			)
			.await
			.unwrap();
		let grant = res
			.into_iter()
			.next_back()
			.unwrap()
			.result
			.unwrap()
			.get("grant")
			.clone()
			.into_object()
			.unwrap();
		let key = grant.get("key").unwrap().as_string().unwrap().clone();

		// A session already authenticated as something else.
		let before = Session {
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("previous".to_string()),
			exp: Some(1),
			..Default::default()
		};
		let mut sess = before.clone();

		let mut vars = PublicVariables::new();
		vars.insert("key", key);
		let res = db_access(
			&ds,
			&mut sess,
			"test".to_string(),
			"test".to_string(),
			"api".to_string(),
			vars,
		)
		.await;
		assert!(res.is_err(), "an unrepresentable session expiry must fail the signin");

		assert_eq!(sess.ac, before.ac, "a failed signin must not change the access method");
		assert_eq!(sess.exp, before.exp, "a failed signin must not extend the expiry");
		assert!(sess.tk.is_none(), "a failed signin must not install a token");
		assert!(sess.data.is_none(), "a failed signin must not install a CONTEXT payload");
	}

	/// Invalidating or resetting a session drops the payload: it is an
	/// authorisation grant, and a reset session must be maximally restrictive.
	#[tokio::test]
	async fn reset_and_clear_drop_the_payload() {
		let ds = setup(SETUP).await;

		let (mut sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");
		assert!(sess.data.is_some());
		crate::iam::clear::clear(&mut sess).unwrap();
		assert!(sess.data.is_none(), "invalidate must drop the CONTEXT payload");

		let (mut sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");
		assert!(sess.data.is_some());
		crate::iam::reset::reset(&mut sess);
		assert!(sess.data.is_none(), "reset must drop the CONTEXT payload");
	}

	/// A CONTEXT clause that writes is refused at definition time — it would
	/// otherwise fire a write on every HTTP request, which re-authenticates.
	#[tokio::test]
	async fn writing_context_is_refused_by_define_and_alter() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");

		let res = ds
			.execute(
				r#"DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN ( SELECT * FROM user )
					CONTEXT ( CREATE audit SET at = time::now() )
					DURATION FOR SESSION 2h;"#,
				&owner,
				None,
			)
			.await
			.unwrap();
		let err = res.into_iter().next().unwrap().result.unwrap_err();
		assert!(err.to_string().contains("read-only"), "unexpected error: {err}");

		// The same expression must not be installable through ALTER either.
		ds.execute(
			r#"DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM user )
				DURATION FOR SESSION 2h;"#,
			&owner,
			None,
		)
		.await
		.unwrap()
		.into_iter()
		.next()
		.unwrap()
		.result
		.unwrap();

		let res = ds
			.execute(
				r#"ALTER ACCESS user ON DATABASE CONTEXT ( CREATE audit SET at = time::now() );"#,
				&owner,
				None,
			)
			.await
			.unwrap();
		let err = res.into_iter().next().unwrap().result.unwrap_err();
		assert!(err.to_string().contains("read-only"), "unexpected error: {err}");
	}

	/// CONTEXT is scoped to access methods that authenticate a record, because
	/// that record's own permissions are what the clause is evaluated under.
	#[tokio::test]
	async fn context_is_refused_on_access_without_a_record_identity() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		let res = ds
			.execute(
				r#"DEFINE ACCESS tok ON DATABASE TYPE JWT ALGORITHM HS512 KEY "secret"
					CONTEXT ( RETURN 1 ) DURATION FOR SESSION 2h;"#,
				&owner,
				None,
			)
			.await
			.unwrap();
		let err = res.into_iter().next().unwrap().result.unwrap_err();
		assert!(err.to_string().contains("authenticates a record"), "unexpected error: {err}");
	}

	/// The accept case the scoping rule exists to allow: `TYPE BEARER FOR
	/// RECORD` settles a record identity through the key exchange, so it takes
	/// a CONTEXT clause and gets a payload.
	///
	/// It also pins what happens to the JWT that exchange issues.
	/// `signin_bearer` always fills the `id` claim from the grant's subject,
	/// so presenting that JWT back matches `verify::token`'s record-access arm
	/// — which requires the definition's access type to be `RECORD` and
	/// rejects a `BEARER` one outright. Re-verification is a hard
	/// AccessMethodMismatch, not a downgrade to a session with neither `$auth`
	/// nor `$session.data`.
	#[tokio::test]
	async fn context_is_accepted_on_bearer_for_record() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		let res = ds
			.execute(
				r#"
				DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					CONTEXT ( SELECT VALUE org FROM grant WHERE owner = $auth )
					DURATION FOR TOKEN 1h, FOR SESSION 2h;
				DEFINE TABLE grant SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
				CREATE user:1;
				CREATE grant:a SET owner = user:1, org = "acme";
				ACCESS api ON DATABASE GRANT FOR RECORD user:1;
				"#,
				&owner,
				None,
			)
			.await
			.unwrap();
		let grant = res
			.into_iter()
			.next_back()
			.unwrap()
			.result
			.unwrap()
			.get("grant")
			.clone()
			.into_object()
			.unwrap();
		let key = grant.get("key").unwrap().as_string().unwrap().clone();

		let mut sess = Session {
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			..Default::default()
		};
		let mut vars = PublicVariables::new();
		vars.insert("key", key);
		let issued = db_access(
			&ds,
			&mut sess,
			"test".to_string(),
			"test".to_string(),
			"api".to_string(),
			vars,
		)
		.await
		.expect("bearer signin failed");

		assert_eq!(
			sess.data.as_ref().expect("data").public().clone().into_array().unwrap().into_vec(),
			vec![PV::String("acme".to_owned())],
			"a BEARER FOR RECORD exchange settles an identity, so it gets a payload"
		);

		let access = match issued {
			Token::Access(t) => t,
			Token::WithRefresh {
				access,
				..
			} => access,
		};
		let mut other = Session::default();
		let err = crate::iam::verify::token(&ds, &mut other, &access).await.unwrap_err();
		assert!(
			err.to_string().contains("cannot be used in the requested operation"),
			"presenting the issued JWT back must be a hard mismatch, got: {err}"
		);
		assert!(other.data.is_none());
	}

	/// The reject cases either side of it: a bearer method whose subject is a
	/// system user has no record identity for `$auth` to name, and a
	/// definition outside a database could never run the clause at all.
	#[tokio::test]
	async fn context_is_refused_on_bearer_for_user_and_outside_a_database() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");

		for stmt in [
			r#"DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER
				CONTEXT ( RETURN 1 ) DURATION FOR SESSION 2h;"#,
			r#"DEFINE ACCESS api ON NAMESPACE TYPE BEARER FOR USER
				CONTEXT ( RETURN 1 ) DURATION FOR SESSION 2h;"#,
			r#"DEFINE ACCESS api ON ROOT TYPE BEARER FOR USER
				CONTEXT ( RETURN 1 ) DURATION FOR SESSION 2h;"#,
		] {
			let res = ds.execute(stmt, &owner, None).await.unwrap();
			let err = res.into_iter().next().unwrap().result.unwrap_err();
			assert!(
				err.to_string().contains("authenticates a record"),
				"unexpected error for `{stmt}`: {err}"
			);
		}
	}

	/// The clause survives a definition round-trip through the catalog: it is
	/// rendered by `INFO FOR DB`, and `ALTER ... DROP CONTEXT` removes it.
	#[tokio::test]
	async fn context_round_trips_through_info_and_alter() {
		let ds = setup(SETUP).await;
		let owner = Session::owner().with_ns("test").with_db("test");

		let res = ds.execute("INFO FOR DB", &owner, None).await.unwrap();
		let info = res.into_iter().next().unwrap().result.unwrap();
		let accesses = info.get("accesses").clone().into_object().unwrap();
		let def = accesses.get("user").unwrap().as_string().unwrap().clone();
		assert!(def.contains("CONTEXT"), "INFO FOR DB should render the clause: {def}");

		ds.execute("ALTER ACCESS user ON DATABASE DROP CONTEXT", &owner, None)
			.await
			.unwrap()
			.into_iter()
			.next()
			.unwrap()
			.result
			.unwrap();

		let res = ds.execute("INFO FOR DB", &owner, None).await.unwrap();
		let info = res.into_iter().next().unwrap().result.unwrap();
		let accesses = info.get("accesses").clone().into_object().unwrap();
		let def = accesses.get("user").unwrap().as_string().unwrap().clone();
		assert!(!def.contains("CONTEXT"), "DROP CONTEXT should remove the clause: {def}");

		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");
		assert!(sess.data.is_none(), "a dropped clause leaves no session data");
	}

	/// A refresh-token rotation recomputes the payload, so a grant revoked in
	/// the table stops being carried by the session on the next rotation.
	#[tokio::test]
	async fn refresh_token_rotation_recomputes_the_payload() {
		let ds = setup(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM type::record('user', $id) )
				WITH REFRESH
				CONTEXT ( SELECT VALUE org FROM grant WHERE owner = $auth )
				DURATION FOR TOKEN 1h, FOR SESSION 2h
			;
			DEFINE TABLE grant SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
			CREATE user:1;
			CREATE grant:a SET owner = user:1, org = "acme";
			"#,
		)
		.await;

		let (sess, res) = signin_as(&ds, 1).await;
		let refresh = match res.expect("signin failed") {
			Token::WithRefresh {
				refresh,
				..
			} => refresh,
			other => panic!("expected a refresh token, got {other:?}"),
		};
		assert_eq!(
			sess.data.as_ref().unwrap().public().clone().into_array().unwrap().into_vec(),
			vec![PV::String("acme".to_owned())]
		);

		// Revoke the grant, then rotate the refresh token.
		let owner = Session::owner().with_ns("test").with_db("test");
		ds.execute("DELETE grant:a", &owner, None).await.unwrap();

		let mut sess = Session {
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			..Default::default()
		};
		let mut vars = PublicVariables::new();
		vars.insert("refresh", refresh);
		db_access(&ds, &mut sess, "test".to_string(), "test".to_string(), "user".to_string(), vars)
			.await
			.expect("refresh rotation failed");

		assert_eq!(
			sess.data.as_ref().unwrap().public().clone().into_array().unwrap().into_vec(),
			Vec::<PV>::new(),
			"rotating a refresh token must recompute the CONTEXT payload"
		);
	}

	/// The plan-time fold of `$session.<constant path>` must not reach record
	/// links: `$auth.tier` stays a fetch of the stored row, so it tracks a
	/// later UPDATE, while `$session.data` is the snapshot the clause froze at
	/// authentication time and deliberately does not.
	#[tokio::test]
	async fn auth_still_fetches_while_session_data_stays_frozen() {
		let ds = setup(
			r#"
			DEFINE ACCESS user ON DATABASE TYPE RECORD
				SIGNIN ( SELECT * FROM type::record('user', $id) )
				CONTEXT ( SELECT VALUE tier FROM ONLY $auth )
				DURATION FOR SESSION 2h
			;
			DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth;
			CREATE user:1 SET tier = "gold";
			"#,
		)
		.await;
		let (sess, res) = signin_as(&ds, 1).await;
		assert!(res.is_ok(), "signin failed: {res:?}");

		let owner = Session::owner().with_ns("test").with_db("test");
		ds.execute("UPDATE user:1 SET tier = 'silver'", &owner, None).await.unwrap();

		// `$auth.tier` re-reads the row.
		let res = ds.execute("RETURN $auth.tier", &sess, None).await.unwrap();
		assert_eq!(
			res.into_iter().next().unwrap().result.unwrap(),
			PV::String("silver".to_owned()),
			"$auth must keep fetch semantics"
		);

		// `$session.data` is the login-time snapshot.
		let res = ds.execute("RETURN $session.data", &sess, None).await.unwrap();
		assert_eq!(
			res.into_iter().next().unwrap().result.unwrap(),
			PV::String("gold".to_owned()),
			"the CONTEXT payload is a snapshot, bounded by DURATION FOR SESSION"
		);
	}
}
