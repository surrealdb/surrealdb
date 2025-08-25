use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use chrono::Utc;
use jsonwebtoken::{EncodingKey, Header, encode};
use md5::Digest;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use uuid::Uuid;

use super::access::{
	authenticate_generic, authenticate_record, create_refresh_token_record,
	revoke_refresh_token_record,
};
use super::verify::{verify_db_creds, verify_ns_creds, verify_root_creds};
use super::{Actor, Level, Role};
use crate::catalog;
use crate::catalog::{DatabaseDefinition, NamespaceDefinition};
use crate::cnf::{INSECURE_FORWARD_ACCESS_ERRORS, SERVER_NAME};
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::{Session, Variables};
use crate::err::Error;
use crate::expr::statements::access;
use crate::expr::{Ident, access_type};
use crate::iam::issue::{config, expiration};
use crate::iam::token::{Claims, HEADER};
use crate::iam::{self, Auth, algorithm_to_jwt_algorithm};
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::val::{Datetime, Object, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SigninData {
	pub token: String,
	pub refresh: Option<String>,
}

pub async fn signin(kvs: &Datastore, session: &mut Session, vars: Object) -> Result<SigninData> {
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
				_ => Err(anyhow::Error::new(Error::MissingUserOrPass)),
			}
		}
		// NS signin with access method
		(Some(ns), None, Some(ac)) => {
			// Process the provided values
			let ns = ns.to_raw_string();
			let ac = ac.to_raw_string();
			// Attempt to signin using specified access method
			super::signin::ns_access(kvs, session, ns, ac, vars).await
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
				_ => Err(anyhow::Error::new(Error::MissingUserOrPass)),
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
				_ => Err(anyhow::Error::new(Error::MissingUserOrPass)),
			}
		}
		_ => Err(anyhow::Error::new(Error::NoSigninTarget)),
	}
}

pub async fn db_access(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	db: String,
	ac: String,
	vars: Object,
) -> Result<SigninData> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read, Optimistic).await?;

	let ns_def = tx.expect_ns_by_name(&ns).await?;
	let db_def = tx.expect_db_by_name(&ns, &db).await?;

	// Fetch the specified access method from storage
	let access = tx.get_db_access(db_def.namespace_id, db_def.database_id, &ac).await;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Check the provided access method exists

	match access {
		Ok(Some(av)) => {
			// Check the access method type
			// All access method types are supported except for JWT
			// The JWT access method is the one that is internal to SurrealDB
			// The equivalent of signing in with JWT is to authenticate it
			match av.access_type.clone() {
				catalog::AccessType::Record(at) => {
					// Check if the record access method supports issuing tokens
					let iss = match &at.jwt.issue {
						Some(iss) => iss.clone(),
						_ => bail!(Error::AccessMethodMismatch),
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
								key.to_raw_string(),
							)
							.await;
						}
					};
					match &at.signin {
						// This record access allows signin
						Some(val) => {
							// Setup the query params
							let vars = Some(Variables::from(vars));
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
										Some(mut rid) => {
											// Create the authentication key
											let key = iam::issue::config(iss.alg, &iss.key)?;
											// Create the authentication claim
											let claims = Claims {
												iss: Some(SERVER_NAME.to_owned()),
												iat: Some(Utc::now().timestamp()),
												nbf: Some(Utc::now().timestamp()),
												exp: iam::issue::expiration(av.token_duration)?,
												jti: Some(Uuid::new_v4().to_string()),
												ns: Some(ns.clone()),
												db: Some(db.clone()),
												ac: Some(ac.clone()),
												id: Some(rid.to_string()),
												..Claims::default()
											};
											// AUTHENTICATE clause
											if let Some(au) = &av.authenticate {
												// Setup the system session for finding the signin
												// record
												let mut sess =
													Session::editor().with_ns(&ns).with_db(&db);
												sess.rd = Some(rid.clone().into());
												sess.tk = Some(
													claims.clone().into_claims_object().into(),
												);
												sess.ip.clone_from(&session.ip);
												sess.or.clone_from(&session.or);
												rid = authenticate_record(kvs, &sess, au).await?;
											}
											// Create refresh token if defined for the record access
											// method
											let refresh = match &at.bearer {
												Some(_) => {
													// TODO(gguillemas): Remove this once bearer
													// access is no longer experimental
													if !kvs.get_capabilities().allows_experimental(
														&ExperimentalTarget::BearerAccess,
													) {
														debug!(
															"Will not create refresh token with disabled bearer access feature"
														);
														None
													} else {
														Some(
															create_refresh_token_record(
																kvs,
																Ident::new(av.name.clone())
																	.unwrap(),
																&ns,
																&db,
																rid.clone(),
															)
															.await?,
														)
													}
												}
												None => None,
											};
											// Log the authenticated access method info
											trace!(
												"Signing in to database with access method `{}`",
												ac
											);
											// Create the authentication token
											let enc = encode(
												&Header::new(algorithm_to_jwt_algorithm(iss.alg)),
												&claims,
												&key,
											);
											// Set the authentication on the session
											session.tk = Some(claims.into_claims_object().into());
											session.ns = Some(ns.clone());
											session.db = Some(db.clone());
											session.ac = Some(ac.clone());
											session.rd = Some(Value::from(rid.clone()));
											session.exp =
												iam::issue::expiration(av.session_duration)?;
											session.au = Arc::new(Auth::new(Actor::new(
												rid.to_string(),
												Default::default(),
												Level::Record(ns, db, rid.to_string()),
											)));
											// Check the authentication token
											match enc {
												// The auth token was created successfully
												Ok(token) => Ok(SigninData {
													token,
													refresh,
												}),
												_ => Err(anyhow::Error::new(
													Error::TokenMakingFailed,
												)),
											}
										}
										_ => Err(anyhow::Error::new(Error::NoRecordFound)),
									}
								}
								Err(e) => match e.downcast_ref() {
									// If the SIGNIN clause throws a specific error, authentication
									// fails with that error
									Some(Error::Thrown(_)) => Err(e),
									// If the SIGNIN clause failed due to an unexpected error, be
									// more specific This allows clients to handle these
									// errors, which may be retryable
									Some(Error::Tx(_) | Error::TxFailure | Error::TxRetryable) => {
										debug!(
											"Unexpected error found while executing a SIGNIN clause: {e}"
										);
										Err(anyhow::Error::new(Error::UnexpectedAuth))
									}
									// Otherwise, return a generic error unless it should be
									// forwarded
									_ => {
										debug!("Record user signin query failed: {e}");
										if *INSECURE_FORWARD_ACCESS_ERRORS {
											Err(e)
										} else {
											Err(anyhow::Error::new(
												Error::AccessRecordSigninQueryFailed,
											))
										}
									}
								},
							}
						}
						_ => Err(anyhow::Error::new(Error::AccessRecordNoSignin)),
					}
				}
				catalog::AccessType::Bearer(at) => {
					// Extract key identifier and key from the provided variables.
					let key = match vars.get("key") {
						Some(key) => key.to_raw_string(),
						None => return Err(anyhow::Error::new(Error::AccessBearerMissingKey)),
					};

					signin_bearer(kvs, session, Some(&ns_def), Some(&db_def), av, &at, key).await
				}
				_ => Err(anyhow::Error::new(Error::AccessMethodMismatch)),
			}
		}
		Ok(None) => Err(anyhow::Error::new(Error::AccessNotFound)),
		_ => Err(anyhow::Error::new(Error::AccessNotFound)),
	}
}

fn auth_from_level_user(level: Level, user: &catalog::UserDefinition) -> Result<Auth> {
	let roles = user
		.roles
		.iter()
		.map(|x| Role::from_str(x))
		.collect::<Result<Vec<_>, _>>()
		.map_err(Error::from)?;
	let actor = Actor::new(user.name.clone(), roles, level);
	Ok(Auth::new(actor))
}

pub async fn db_user(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	db: String,
	user: String,
	pass: String,
) -> Result<SigninData> {
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

			// Set the authentication on the session
			session.tk = Some(val.into_claims_object().into());
			session.ns = Some(ns.clone());
			session.db = Some(db.clone());
			session.exp = expiration(u.session_duration)?;
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(SigninData {
					token: tk,
					refresh: None,
				}),
				_ => Err(anyhow::Error::new(Error::TokenMakingFailed)),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!(
				"Failed to verify signin credentials for user `{user}` in database `{ns}/{db}`: {e}"
			);
			Err(anyhow::Error::new(Error::InvalidAuth))
		}
	}
}

pub async fn ns_access(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	ac: String,
	vars: Object,
) -> Result<SigninData> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read, Optimistic).await?;
	let ns_def = tx.expect_ns_by_name(&ns).await?;
	// Fetch the specified access method from storage
	let access = tx.get_ns_access(ns_def.namespace_id, &ac).await;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Check the provided access method exists
	match access {
		Ok(Some(av)) => {
			// Check the access method type
			match av.access_type.clone() {
				catalog::AccessType::Bearer(at) => {
					// Extract key identifier and key from the provided variables.
					let key = match vars.get("key") {
						Some(key) => key.to_raw_string(),
						None => bail!(Error::AccessBearerMissingKey),
					};

					signin_bearer(kvs, session, Some(&ns_def), None, av, &at, key).await
				}
				_ => Err(anyhow::Error::new(Error::AccessMethodMismatch)),
			}
		}
		Ok(None) => Err(anyhow::Error::new(Error::AccessNotFound)),
		_ => Err(anyhow::Error::new(Error::AccessNotFound)),
	}
}

pub async fn ns_user(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	user: String,
	pass: String,
) -> Result<SigninData> {
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

			// Set the authentication on the session
			session.tk = Some(val.into_claims_object().into());
			session.ns = Some(ns.clone());
			session.exp = expiration(u.session_duration)?;
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(SigninData {
					token: tk,
					refresh: None,
				}),
				_ => Err(anyhow::Error::new(Error::TokenMakingFailed)),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!(
				"Failed to verify signin credentials for user `{user}` in namespace `{ns}`: {e}"
			);
			Err(anyhow::Error::new(Error::InvalidAuth))
		}
	}
}

pub async fn root_user(
	kvs: &Datastore,
	session: &mut Session,
	user: String,
	pass: String,
) -> Result<SigninData> {
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

			// Set the authentication on the session
			session.tk = Some(val.into_claims_object().into());
			session.exp = expiration(u.session_duration)?;
			session.au = Arc::new(au);
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(SigninData {
					token: tk,
					refresh: None,
				}),
				_ => Err(anyhow::Error::new(Error::TokenMakingFailed)),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!("Failed to verify signin credentials for user `{user}` in root: {e}");
			Err(anyhow::Error::new(Error::InvalidAuth))
		}
	}
}

pub async fn root_access(
	kvs: &Datastore,
	session: &mut Session,
	ac: String,
	vars: Object,
) -> Result<SigninData> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read, Optimistic).await?;
	// Fetch the specified access method from storage
	let access = tx.get_root_access(&ac).await;

	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Check the provided access method exists
	match access {
		Ok(Some(av)) => {
			// Check the access method type
			match av.access_type.clone() {
				catalog::AccessType::Bearer(at) => {
					// Extract key identifier and key from the provided variables.
					let key = match vars.get("key") {
						Some(key) => key.to_raw_string(),
						None => return Err(anyhow::Error::new(Error::AccessBearerMissingKey)),
					};

					signin_bearer(kvs, session, None, None, av, &at, key).await
				}
				_ => Err(anyhow::Error::new(Error::AccessMethodMismatch)),
			}
		}
		Ok(None) => Err(anyhow::Error::new(Error::AccessNotFound)),
		_ => Err(anyhow::Error::new(Error::AccessNotFound)),
	}
}

pub async fn signin_bearer(
	kvs: &Datastore,
	session: &mut Session,
	ns: Option<&NamespaceDefinition>,
	db: Option<&DatabaseDefinition>,
	av: Arc<catalog::AccessDefinition>,
	at: &catalog::BearerAccess,
	key: String,
) -> Result<SigninData> {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	if !kvs.get_capabilities().allows_experimental(&ExperimentalTarget::BearerAccess) {
		// Return opaque error to avoid leaking the existence of the feature.
		debug!("Error attempting to authenticate with disabled bearer access feature");
		bail!(Error::InvalidAuth);
	}
	// Check if the bearer access method supports issuing tokens.
	let iss = match &at.jwt.issue {
		Some(iss) => iss.clone(),
		_ => bail!(Error::AccessMethodMismatch),
	};
	// Extract key identifier and key from the provided key.
	let kid = validate_grant_bearer(&key)?;

	// Create a new readonly transaction
	let tx = kvs.transaction(Read, Optimistic).await?;
	// Fetch the specified access grant from storage
	let gr = match (&ns, &db) {
		(Some(ns), Some(db)) => {
			tx.get_db_access_grant(ns.namespace_id, db.database_id, &av.name, &kid).await?
		}
		(Some(ns), None) => tx.get_ns_access_grant(ns.namespace_id, &av.name, &kid).await?,
		(None, None) => tx.get_root_access_grant(&av.name, &kid).await?,
		(None, Some(_)) => bail!(Error::NsEmpty),
	}
	.ok_or(Error::InvalidAuth)?;

	// Ensure that the transaction is cancelled.
	tx.cancel().await?;
	// Authenticate bearer key against stored grant.
	verify_grant_bearer(&gr, key)?;

	// If the subject of the grant is a system user, get their roles.
	let roles = if let catalog::Subject::User(user) = &gr.subject {
		// Create a new readonly transaction.
		let tx = kvs.transaction(Read, Optimistic).await?;
		// Fetch the specified user from storage.

		let user = match (&ns, &db) {
			(Some(ns), Some(db)) => tx
				.get_db_user(ns.namespace_id, db.database_id, user)
				.await
				.map_err(|e| {
					debug!(
						"Error retrieving user for bearer access to database `{}/{}`: {}",
						ns.name, db.name, e
					);
					// Return opaque error to avoid leaking grant subject existence.
					Error::InvalidAuth
				})?
				.ok_or(Error::InvalidAuth)?,
			(Some(ns), None) => tx
				.get_ns_user(ns.namespace_id, user)
				.await
				.map_err(|e| {
					debug!(
						"Error retrieving user for bearer access to namespace `{}`: {}",
						ns.name, e
					);
					// Return opaque error to avoid leaking grant subject existence.
					Error::InvalidAuth
				})?
				.ok_or(Error::InvalidAuth)?,
			(None, None) => tx
				.get_root_user(user)
				.await
				.map_err(|e| {
					debug!("Error retrieving user for bearer access to root: {e}");
					// Return opaque error to avoid leaking grant subject existence.
					Error::InvalidAuth
				})?
				.ok_or(Error::InvalidAuth)?,
			(None, Some(_)) => bail!(Error::NsEmpty),
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
		ns: ns.map(|ns| ns.name.clone()),
		db: db.map(|db| db.name.clone()),
		ac: Some(av.name.to_string()),
		id: match &gr.subject {
			catalog::Subject::User(user) => Some(user.clone()),
			catalog::Subject::Record(rid) => Some(rid.to_string()),
		},
		roles: match &gr.subject {
			catalog::Subject::User(_) => Some(roles.iter().map(|v| v.to_string()).collect()),
			catalog::Subject::Record(_) => Default::default(),
		},
		..Claims::default()
	};
	// AUTHENTICATE clause
	if let Some(au) = &av.authenticate {
		// Setup the system session for executing the clause.
		let mut sess = match (&ns, &db) {
			(Some(ns), Some(db)) => Session::editor().with_ns(&ns.name).with_db(&db.name),
			(Some(ns), None) => Session::editor().with_ns(&ns.name),
			(None, None) => Session::editor(),
			(None, Some(_)) => bail!(Error::NsEmpty),
		};
		sess.tk = Some(claims.clone().into_claims_object().into());
		sess.ip.clone_from(&session.ip);
		sess.or.clone_from(&session.or);
		authenticate_generic(kvs, &sess, au).await?;
	}
	// If the bearer grant is a refresh token.
	let refresh = match at.kind {
		catalog::BearerAccessType::Refresh => {
			match &gr.subject {
				catalog::Subject::Record(rid) => {
					if let (Some(ns), Some(db)) = (&ns, &db) {
						// Revoke the used refresh token.
						revoke_refresh_token_record(
							kvs,
							Ident::new(gr.id.clone()).unwrap(),
							Ident::new(gr.ac.clone()).unwrap(),
							&ns.name,
							&db.name,
						)
						.await?;
						// Create a new refresh token to replace it.
						let refresh = create_refresh_token_record(
							kvs,
							Ident::new(gr.ac.clone()).unwrap(),
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
						bail!(Error::InvalidAuth);
					}
				}
				catalog::Subject::User(_) => {
					debug!(
						"Invalid attempt to authenticatea as a system user with a refresh token"
					);
					bail!(Error::InvalidAuth);
				}
			}
		}
		_ => None,
	};
	// Log the authenticated access method information.
	trace!("Signing in to database with bearer access method `{}`", av.name);
	// Create the authentication token.
	let enc = encode(&Header::new(algorithm_to_jwt_algorithm(iss.alg)), &claims, &key);
	// Set the authentication on the session.
	session.tk = Some(claims.into_claims_object().into());
	session.ns.clone_from(&ns.map(|ns| ns.name.clone()));
	session.db.clone_from(&db.map(|db| db.name.clone()));
	session.ac = Some(av.name.to_string());
	session.exp = expiration(av.session_duration)?;
	match &gr.subject {
		catalog::Subject::User(user) => {
			session.au = Arc::new(Auth::new(Actor::new(
				user.to_string(),
				roles
					.iter()
					.map(|e| Role::from_str(e))
					.collect::<Result<_, _>>()
					.map_err(Error::from)?,
				match (ns, db) {
					(Some(ns), Some(db)) => Level::Database(ns.name.clone(), db.name.clone()),
					(Some(ns), None) => Level::Namespace(ns.name.clone()),
					(None, None) => Level::Root,
					(None, Some(_)) => bail!(Error::NsEmpty),
				},
			)));
		}
		catalog::Subject::Record(rid) => {
			session.au = Arc::new(Auth::new(Actor::new(
				rid.to_string(),
				Default::default(),
				if let (Some(ns), Some(db)) = (ns, db) {
					Level::Record(ns.name.clone(), db.name.clone(), rid.to_string())
				} else {
					debug!(
						"Invalid attempt to authenticate as a record without a namespace and database"
					);
					bail!(Error::InvalidAuth);
				},
			)));
			session.rd = Some(Value::from(rid.clone()));
		}
	};
	// Return the authentication token.
	match enc {
		Ok(token) => Ok(SigninData {
			token,
			refresh,
		}),
		_ => Err(anyhow::Error::new(Error::TokenMakingFailed)),
	}
}

pub fn validate_grant_bearer(key: &str) -> Result<String> {
	let parts: Vec<&str> = key.split("-").collect();
	ensure!(parts.len() == 4, Error::AccessGrantBearerInvalid);
	// Check that the prefix type exists.
	access_type::BearerAccessType::from_str(parts[1])?;
	// Retrieve the key identifier from the provided key.
	let kid = parts[2];
	// Check the length of the key identifier.
	ensure!(kid.len() == access::GRANT_BEARER_ID_LENGTH, Error::AccessGrantBearerInvalid);
	// Retrieve the key from the provided key.
	let key = parts[3];
	// Check the length of the key.
	ensure!(key.len() == access::GRANT_BEARER_KEY_LENGTH, Error::AccessGrantBearerInvalid);

	Ok(kid.to_string())
}

pub fn verify_grant_bearer(
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

				bail!(Error::InvalidAuth);
			}
		}
		(_, Some(_)) => {
			debug!("Bearer access grant `{}` for method `{}` is revoked", gr.id, gr.ac);
			bail!(Error::InvalidAuth);
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
				Err(anyhow::Error::new(Error::InvalidAuth))
			}
		}
		_ => Err(anyhow::Error::new(Error::AccessMethodMismatch)),
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use chrono::Duration;
	use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
	use regex::Regex;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::dbs::Capabilities;
	use crate::iam::Role;
	use crate::sql::statements::define::DefineKind;
	use crate::sql::statements::define::user::PassType;
	use crate::sql::{Ast, Expr, Ident, TopLevelExpr};

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

			match res {
				Ok(data) => {
					assert!(data.refresh.is_none(), "Refresh token was unexpectedly returned")
				}
				Err(e) => panic!("Failed to signin with credentials: {e}"),
			}
		}
		// Test with refresh
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
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
			let refresh = match res {
				Ok(data) => match data.refresh {
					Some(refresh) => refresh,
					None => panic!("Refresh token was not returned"),
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
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("refresh", refresh.clone().into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
			)
			.await;
			// Authentication should be identical as with user credentials
			match res {
				Ok(data) => match data.refresh {
					Some(new_refresh) => assert!(
						new_refresh != refresh,
						"New refresh token is identical to used one"
					),
					None => panic!("Refresh token was not returned"),
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
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("refresh", refresh.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
		// Test with expired refresh
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
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
			let refresh = match res {
				Ok(data) => match data.refresh {
					Some(refresh) => refresh,
					None => panic!("Refresh token was not returned"),
				},
				Err(e) => panic!("Failed to signin with credentials: {e}"),
			};
			// Wait for the refresh token to expire
			std::thread::sleep(Duration::seconds(2).to_std().unwrap());
			// Signin with the refresh token
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("refresh", refresh.clone().into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
			)
			.await;
			// Should fail due to the refresh token being expired
			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
		// Test that only the hash of the refresh token is stored
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
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
			let refresh = match res {
				Ok(data) => match data.refresh {
					Some(refresh) => refresh,
					None => panic!("Refresh token was not returned"),
				},
				Err(e) => panic!("Failed to signin with credentials: {e}"),
			};

			// Extract grant identifier from refresh token
			let id = refresh.split("-").collect::<Vec<&str>>()[2];

			// Test that returned refresh token is in plain text
			let ok = Regex::new(r"surreal-refresh-[a-zA-Z0-9]{12}-[a-zA-Z0-9]{24}").unwrap();
			assert!(ok.is_match(&refresh), "Output '{}' doesn't match regex '{}'", refresh, ok);

			// Get the stored bearer key representing the refresh token
			let tx = ds.transaction(Read, Optimistic).await.unwrap().enclose();
			let grant = tx
				.get_db_access_grant(NamespaceId(0), DatabaseId(0), "user", id)
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
				// Check that token can be verified with the defined algorithm
				let val = Validation::new(Algorithm::RS256);
				// Check that token can be verified with the defined public key
				let token_data = decode::<Claims>(
					&sd.token,
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
						// Decode token without validation
						let token_data =
							decode::<Claims>(&sd.token, &DecodingKey::from_secret(&[]), &{
								let mut validation =
									Validation::new(jsonwebtoken::Algorithm::HS256);
								validation.insecure_disable_signature_validation();
								validation.validate_nbf = false;
								validation.validate_exp = false;
								validation
							})
							.unwrap();

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
						SELECT * FROM type::thing('user', $id)
					)
					AUTHENTICATE (
						-- Simple example increasing the record identifier by one
					    SELECT * FROM type::thing('user', record::id($auth) + 1)
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
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("id", 1.into());
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
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("email", "info@example.com".into());
			vars.insert("pass", "company-password".into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"owner".to_string(),
				vars.into(),
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
						SELECT * FROM type::thing('user', $id)
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
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("id", 1.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
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
					SIGNIN (
					   SELECT * FROM type::thing('user', $id)
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
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("id", 1.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"user".to_string(),
				vars.into(),
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
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("user", "user".into());
			vars.insert("pass", "pass".into());

			let (res1, res2) = tokio::join!(
				db_access(
					&ds,
					&mut sess1,
					"test".to_string(),
					"test".to_string(),
					"user".to_string(),
					vars.clone().into(),
				),
				db_access(
					&ds,
					&mut sess2,
					"test".to_string(),
					"test".to_string(),
					"user".to_string(),
					vars.into(),
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
					Error::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e}"),
				},
				(Ok(_), Err(e2)) => match e2.downcast().expect("Unexpected error kind") {
					Error::InvalidAuth => {}
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
					SELECT * FROM type::thing('user', $id)
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
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("id", 1.into());

			let (res1, res2) = tokio::join!(
				db_access(
					&ds,
					&mut sess1,
					"test".to_string(),
					"test".to_string(),
					"user".to_string(),
					vars.clone().into(),
				),
				db_access(
					&ds,
					&mut sess2,
					"test".to_string(),
					"test".to_string(),
					"user".to_string(),
					vars.into(),
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
					Error::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e:?}"),
				},
				(Ok(_), Err(e2)) => match e2.downcast().expect("Unexpected error kind") {
					Error::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e:?}"),
				},
			}
		}
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

		let plain_text_regex =
			Regex::new("surreal-bearer-[a-zA-Z0-9]{12}-[a-zA-Z0-9]{24}").unwrap();
		let sha_256_regex = Regex::new(r"[0-9a-f]{64}").unwrap();

		for level in &test_levels {
			println!("Test level: {}", level.level);

			// Test with correct bearer key
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_raw_string();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				assert!(res.is_ok(), "Failed to sign in with bearer key: {:?}", res);
				assert_eq!(sess.ns, level.ns.map(|s| s.to_string()));
				assert_eq!(sess.db, level.db.map(|s| s.to_string()));

				// Check auth level
				match level.level {
					"ROOT" => assert!(sess.au.is_root()),
					"NS" => assert!(sess.au.is_ns()),
					"DB" => assert!(sess.au.is_db()),
					_ => panic!("Unsupported level"),
				}
				assert_eq!(sess.au.level().ns(), level.ns);
				assert_eq!(sess.au.level().db(), level.db);

				// Check roles
				assert!(
					!sess.au.has_role(Role::Viewer),
					"Auth user expected to not have Viewer role"
				);
				assert!(
					// User is defined with this role only
					sess.au.has_role(Role::Editor),
					"Auth user expected to have Editor role"
				);
				assert!(
					!sess.au.has_role(Role::Owner),
					"Auth user expected to not have Owner role"
				);

				// Check expiration
				let exp = sess.exp.unwrap();
				let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
				let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
				assert!(
					exp > min_exp && exp < max_exp,
					"Session expiration is expected to match the defined duration",
				);
			}

			// Test with correct bearer key and AUTHENTICATE clause succeeding
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							AUTHENTICATE {{
								RETURN NONE
							}}
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_raw_string();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				assert!(res.is_ok(), "Failed to sign in with bearer key: {:?}", res);
				assert_eq!(sess.ns, level.ns.map(|s| s.to_string()));
				assert_eq!(sess.db, level.db.map(|s| s.to_string()));

				// Check auth level
				match level.level {
					"ROOT" => assert!(sess.au.is_root()),
					"NS" => assert!(sess.au.is_ns()),
					"DB" => assert!(sess.au.is_db()),
					_ => panic!("Unsupported level"),
				}
				assert_eq!(sess.au.level().ns(), level.ns);
				assert_eq!(sess.au.level().db(), level.db);

				// Check roles
				assert!(
					!sess.au.has_role(Role::Viewer),
					"Auth user expected to not have Viewer role"
				);
				assert!(
					// User is defined with this role only
					sess.au.has_role(Role::Editor),
					"Auth user expected to have Editor role"
				);
				assert!(
					!sess.au.has_role(Role::Owner),
					"Auth user expected to not have Owner role"
				);

				// Check expiration
				let exp = sess.exp.unwrap();
				let min_exp = (Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
				let max_exp = (Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
				assert!(
					exp > min_exp && exp < max_exp,
					"Session expiration is expected to match the defined duration",
				);
			}

			// Test with correct bearer key and AUTHENTICATE clause failing
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							AUTHENTICATE {{
								THROW "Test authentication error";
							}}
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level,
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_raw_string();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::Thrown(e) => assert_eq!(e, "Test authentication error"),
					e => panic!("Unexpected error, expected Thrown found {e:?}"),
				}
			}

			// Test with expired grant
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR GRANT 1s FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_raw_string();

				// Wait for the grant to expire
				std::thread::sleep(Duration::seconds(2).to_std().unwrap());

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e}"),
				}
			}

			// Test with revoked grant
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR GRANT 1s FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_raw_string();

				// Get grant identifier from key
				let kid = key.split("-").collect::<Vec<&str>>()[2];

				// Revoke grant
				ds.execute(
					&format!("ACCESS api ON {} REVOKE GRANT {kid}", level.level),
					&sess,
					None,
				)
				.await
				.unwrap();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e}"),
				}
			}

			// Test with removed access method
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR GRANT 1s FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_raw_string();

				// Remove bearer access method
				ds.execute(format!("REMOVE ACCESS api ON {}", level.level).as_str(), &sess, None)
					.await
					.unwrap();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::AccessNotFound => {}
					e => panic!("Unexpected error, expected AccessNotFound found {e}"),
				}
			}

			// Test with missing key
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let _key = grant.get("key").unwrap().clone().as_raw_string();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};

				// The key parameter is not inserted:
				let vars: HashMap<&str, Value> = HashMap::new();
				// vars.insert("key", key.into());

				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::AccessBearerMissingKey => {}
					e => panic!("Unexpected error, expected AccessBearerMissingKey found {e}"),
				}
			}

			// Test with incorrect bearer key prefix part
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let valid_key = grant.get("key").unwrap().clone().as_raw_string();

				// Replace a character from the key prefix
				let mut invalid_key: Vec<char> = valid_key.chars().collect();
				invalid_key["surreal-".len() + 2] = '_';
				let key: String = invalid_key.into_iter().collect();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::AccessGrantBearerInvalid => {}
					e => panic!("Unexpected error, expected AccessGrantBearerInvalid found {e}"),
				}
			}

			// Test with incorrect bearer key length
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let valid_key = grant.get("key").unwrap().clone().as_raw_string();

				// Remove a character from the bearer key
				let mut invalid_key: Vec<char> = valid_key.chars().collect();
				invalid_key.truncate(invalid_key.len() - 1);
				let key: String = invalid_key.into_iter().collect();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::AccessGrantBearerInvalid => {}
					e => panic!("Unexpected error, expected AccessGrantBearerInvalid found {e}"),
				}
			}

			// Test with incorrect bearer key identifier part
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let valid_key = grant.get("key").unwrap().clone().as_raw_string();

				// Replace a character from the key identifier
				let mut invalid_key: Vec<char> = valid_key.chars().collect();
				invalid_key[access_type::BearerAccessType::Bearer.prefix().len() + 2] = '_';
				let key: String = invalid_key.into_iter().collect();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e}"),
				}
			}

			// Test with incorrect bearer key value
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let valid_key = grant.get("key").unwrap().clone().as_raw_string();

				// Replace a character from the key value
				let mut invalid_key: Vec<char> = valid_key.chars().collect();
				invalid_key[valid_key.len() - 2] = '_';
				let key: String = invalid_key.into_iter().collect();

				// Sign in with the bearer key
				let mut sess = Session {
					ns: level.ns.map(String::from),
					db: level.db.map(String::from),
					..Default::default()
				};
				let mut vars: HashMap<&str, Value> = HashMap::new();
				vars.insert("key", key.into());
				let res = match level.level {
					"DB" => {
						db_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							level.db.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"NS" => {
						ns_access(
							&ds,
							&mut sess,
							level.ns.unwrap().to_string(),
							"api".to_string(),
							vars.into(),
						)
						.await
					}
					"ROOT" => root_access(&ds, &mut sess, "api".to_string(), vars.into()).await,
					_ => panic!("Unsupported level"),
				};

				let e = res.unwrap_err();
				match e.downcast().expect("Unexpected error kind") {
					Error::InvalidAuth => {}
					e => panic!("Unexpected error, expected InvalidAuth found {e}"),
				}
			}

			// Test that only the key hash is stored
			{
				let ds = Datastore::new("memory").await.unwrap().with_capabilities(
					Capabilities::default()
						.with_experimental(ExperimentalTarget::BearerAccess.into()),
				);
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
							DEFINE ACCESS api ON {} TYPE BEARER FOR USER
							DURATION FOR SESSION 2h
							;
							DEFINE USER tobie ON {} ROLES EDITOR;
							ACCESS api ON {} GRANT FOR USER tobie;
							"#,
							level.level, level.level, level.level
						),
						&sess,
						None,
					)
					.await
					.unwrap();

				// Get the bearer key from grant
				let result = if let Ok(res) = &res.last().unwrap().result {
					res.clone()
				} else {
					panic!("Unable to retrieve bearer key grant");
				};
				let grant = result
					.coerce_to::<Object>()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to::<Object>()
					.unwrap();
				let id = grant.get("id").unwrap().clone().as_raw_string();
				let key = grant.get("key").unwrap().clone().as_raw_string();

				// Test that returned key is in plain text
				assert!(
					plain_text_regex.is_match(&key),
					"Output '{}' doesn't match regex '{}'",
					key,
					plain_text_regex
				);

				// Get the stored bearer grant
				let tx = ds.transaction(Read, Optimistic).await.unwrap().enclose();
				let grant = match level.level {
					"DB" => {
						let db = tx
							.expect_db_by_name(level.ns.unwrap(), level.db.unwrap())
							.await
							.unwrap();
						tx.get_db_access_grant(db.namespace_id, db.database_id, "api", &id)
							.await
							.unwrap()
					}
					"NS" => {
						let ns = tx.expect_ns_by_name(level.ns.unwrap()).await.unwrap();
						tx.get_ns_access_grant(ns.namespace_id, "api", &id).await.unwrap()
					}
					"ROOT" => tx.get_root_access_grant("api", &id).await.unwrap(),
					_ => panic!("Unsupported level"),
				}
				.unwrap();
				let key = match &grant.grant {
					catalog::Grant::Bearer(grant) => grant.key.clone(),
					_ => panic!("Incorrect grant type returned, expected a bearer grant"),
				};
				tx.cancel().await.unwrap();

				// Test that the returned key is a SHA-256 hash
				assert!(
					sha_256_regex.is_match(&key),
					"Output '{}' doesn't match regex '{}'",
					key,
					sha_256_regex
				);
			}
		}
	}

	#[tokio::test]
	async fn test_signin_bearer_for_record() {
		// Test with correct bearer key and existing record
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR SESSION 2h
					;
					CREATE user:test;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let key = grant.get("key").unwrap().clone().as_raw_string();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;
			assert!(res.is_ok(), "Failed to sign in with bearer key: {:?}", res);
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
		// Test with correct bearer key and non-existing record
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let key = grant.get("key").unwrap().clone().as_raw_string();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			assert!(res.is_ok(), "Failed to sign in with bearer key: {:?}", res);
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
		// Test with correct bearer key and AUTHENTICATE clause succeeding
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					AUTHENTICATE {{
						RETURN NONE
					}}
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let key = grant.get("key").unwrap().clone().as_raw_string();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			assert!(res.is_ok(), "Failed to sign in with bearer key: {:?}", res);
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

		// Test with correct bearer key and AUTHENTICATE clause failing
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					AUTHENTICATE {{
						THROW "Test authentication error";
					}}
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let key = grant.get("key").unwrap().clone().as_raw_string();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::Thrown(e) => assert_eq!(e, "Test authentication error"),
				e => panic!("Unexpected error, expected Thrown found {e:?}"),
			}
		}

		// Test with expired grant
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR GRANT 1s FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let key = grant.get("key").unwrap().clone().as_raw_string();

			// Wait for the grant to expire
			std::thread::sleep(Duration::seconds(2).to_std().unwrap());

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}

		// Test with revoked grant
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR GRANT 1s FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let key = grant.get("key").unwrap().clone().as_raw_string();

			// Get grant identifier from key
			let kid = key.split("-").collect::<Vec<&str>>()[2];

			// Revoke grant
			ds.execute(&format!("ACCESS api ON DATABASE REVOKE GRANT {kid}"), &sess, None)
				.await
				.unwrap();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}

		// Test with removed access method
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR GRANT 1s FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let key = grant.get("key").unwrap().clone().as_raw_string();

			// Remove bearer access method
			ds.execute("REMOVE ACCESS api ON DATABASE", &sess, None).await.unwrap();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::AccessNotFound => {}
				e => panic!("Unexpected error, expected AccessNotFound found {e}"),
			}
		}

		// Test with missing key
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let _key = grant.get("key").unwrap().clone().as_raw_string();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			// The key parameter is not inserted:
			let vars: HashMap<&str, Value> = HashMap::new();
			// vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::AccessBearerMissingKey => {}
				e => panic!("Unexpected error, expected AccessBearerMissingKey found {e}"),
			}
		}

		// Test with incorrect bearer key prefix part
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let valid_key = grant.get("key").unwrap().clone().as_raw_string();

			// Replace a character from the key prefix
			let mut invalid_key: Vec<char> = valid_key.chars().collect();
			invalid_key["surreal-".len() + 2] = '_';
			let key: String = invalid_key.into_iter().collect();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::AccessGrantBearerInvalid => {}
				e => panic!("Unexpected error, expected AccessGrantBearerInvalid found {e}"),
			}
		}

		// Test with incorrect bearer key length
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let valid_key = grant.get("key").unwrap().clone().as_raw_string();

			// Remove a character from the bearer key
			let mut invalid_key: Vec<char> = valid_key.chars().collect();
			invalid_key.truncate(invalid_key.len() - 1);
			let key: String = invalid_key.into_iter().collect();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::AccessGrantBearerInvalid => {}
				e => panic!("Unexpected error, expected AccessGrantBearerInvalid found {e}"),
			}
		}

		// Test with incorrect bearer key identifier part
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let valid_key = grant.get("key").unwrap().clone().as_raw_string();

			// Replace a character from the key identifier
			let mut invalid_key: Vec<char> = valid_key.chars().collect();
			invalid_key[access_type::BearerAccessType::Bearer.prefix().len() + 2] = '_';
			let key: String = invalid_key.into_iter().collect();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}

		// Test with incorrect bearer key value
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let valid_key = grant.get("key").unwrap().clone().as_raw_string();

			// Replace a character from the key value
			let mut invalid_key: Vec<char> = valid_key.chars().collect();
			invalid_key[valid_key.len() - 2] = '_';
			let key: String = invalid_key.into_iter().collect();

			// Sign in with the bearer key
			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let mut vars: HashMap<&str, Value> = HashMap::new();
			vars.insert("key", key.into());
			let res = db_access(
				&ds,
				&mut sess,
				"test".to_string(),
				"test".to_string(),
				"api".to_string(),
				vars.into(),
			)
			.await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}

		// Test that only the key hash is stored
		{
			let ds = Datastore::new("memory").await.unwrap().with_capabilities(
				Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
			);
			let sess = Session::owner().with_ns("test").with_db("test");
			let res = ds
				.execute(
					r#"
					DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD
					DURATION FOR SESSION 2h
					;
					ACCESS api ON DATABASE GRANT FOR RECORD user:test;
					"#,
					&sess,
					None,
				)
				.await
				.unwrap();

			// Get the bearer key from grant
			let result = if let Ok(res) = &res.last().unwrap().result {
				res.clone()
			} else {
				panic!("Unable to retrieve bearer key grant");
			};
			let grant = result
				.coerce_to::<Object>()
				.unwrap()
				.get("grant")
				.unwrap()
				.clone()
				.coerce_to::<Object>()
				.unwrap();
			let id = grant.get("id").unwrap().clone().as_raw_string();
			let key = grant.get("key").unwrap().clone().as_raw_string();

			// Test that returned key is in plain text
			let ok = Regex::new(r"surreal-bearer-[a-zA-Z0-9]{12}-[a-zA-Z0-9]{24}").unwrap();
			assert!(ok.is_match(&key), "Output '{}' doesn't match regex '{}'", key, ok);

			// Get the stored bearer grant
			let tx = ds.transaction(Read, Optimistic).await.unwrap().enclose();
			let grant = tx
				.get_db_access_grant(NamespaceId(0), DatabaseId(0), "api", &id)
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
	async fn test_signin_nonexistent_role() {
		use crate::iam::Error as IamError;
		use crate::sql::Base;
		use crate::sql::statements::DefineUserStatement;
		use crate::sql::statements::define::DefineStatement;
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
				name: Ident::new("user".to_owned()).unwrap(),
				// This is the Argon2id hash for "pass" with a random salt.
				pass_type: PassType::Hash(
					"$argon2id$v=19$m=16,t=2,p=1$VUlHTHVOYjc5d0I1dGE3OQ$sVtmRNH+Xtiijk0uXL2+4w"
						.to_string(),
				),
				roles: vec![Ident::new("nonexistent".to_owned()).unwrap()],
				session_duration: None,
				token_duration: None,
				comment: None,
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
				Error::IamError(IamError::InvalidRole(_)) => {}
				e => panic!("Unexpected error, expected IamError(InvalidRole) found {e}"),
			}
		}
	}
}
