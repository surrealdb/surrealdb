use super::verify::{
	authenticate_generic, authenticate_record, verify_db_creds, verify_ns_creds, verify_root_creds,
};
use super::{Actor, Level, Role};
use crate::cnf::{EXPERIMENTAL_BEARER_ACCESS, INSECURE_FORWARD_ACCESS_ERRORS, SERVER_NAME};
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::issue::{config, expiration};
use crate::iam::token::{Claims, HEADER};
use crate::iam::Auth;
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::statements::{access, AccessGrant};
use crate::sql::AccessType;
use crate::sql::Datetime;
use crate::sql::Object;
use crate::sql::Value;
use chrono::Utc;
use jsonwebtoken::{encode, EncodingKey, Header};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use uuid::Uuid;

pub async fn signin(kvs: &Datastore, session: &mut Session, vars: Object) -> Result<String, Error> {
	// Check vars contains only computed values
	vars.validate_computed()?;
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
) -> Result<String, Error> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read, Optimistic).await?;
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
			match &av.kind {
				AccessType::Record(at) => {
					// Check if the record access method supports issuing tokens
					let iss = match &at.jwt.issue {
						Some(iss) => iss.clone(),
						_ => return Err(Error::AccessMethodMismatch),
					};
					match &at.signin {
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
										Some(mut rid) => {
											// Create the authentication key
											let key = config(iss.alg, &iss.key)?;
											// Create the authentication claim
											let claims = Claims {
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
											// AUTHENTICATE clause
											if let Some(au) = &av.authenticate {
												// Setup the system session for finding the signin record
												let mut sess =
													Session::editor().with_ns(&ns).with_db(&db);
												sess.rd = Some(rid.clone().into());
												sess.tk = Some((&claims).into());
												sess.ip.clone_from(&session.ip);
												sess.or.clone_from(&session.or);
												rid = authenticate_record(kvs, &sess, au).await?;
											}
											// Log the authenticated access method info
											trace!(
												"Signing in to database with access method `{}`",
												ac
											);
											// Create the authentication token
											let enc =
												encode(&Header::new(iss.alg.into()), &claims, &key);
											// Set the authentication on the session
											session.tk = Some((&claims).into());
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
												Ok(tk) => Ok(tk),
												_ => Err(Error::TokenMakingFailed),
											}
										}
										_ => Err(Error::NoRecordFound),
									}
								}
								Err(e) => match e {
									Error::Thrown(_) => Err(e),
									e => {
										debug!("Record user signin query failed: {e}");
										if *INSECURE_FORWARD_ACCESS_ERRORS {
											Err(e)
										} else {
											Err(Error::AccessRecordSigninQueryFailed)
										}
									}
								},
							}
						}
						_ => Err(Error::AccessRecordNoSignin),
					}
				}
				AccessType::Bearer(at) => {
					// TODO(gguillemas): Remove this once bearer access is no longer experimental.
					if !*EXPERIMENTAL_BEARER_ACCESS {
						// Return opaque error to avoid leaking the existence of the feature.
						debug!(
							"Error attempting to authenticate with disabled bearer access feature"
						);
						return Err(Error::InvalidAuth);
					}
					// Check if the bearer access method supports issuing tokens.
					let iss = match &at.jwt.issue {
						Some(iss) => iss.clone(),
						_ => return Err(Error::AccessMethodMismatch),
					};
					// Extract key identifier and key from the provided variables.
					let (kid, key) = validate_grant_bearer(vars)?;
					// Create a new readonly transaction
					let tx = kvs.transaction(Read, Optimistic).await?;
					// Fetch the specified access grant from storage
					let gr = match tx.get_db_access_grant(&ns, &db, &ac, &kid).await {
						Ok(gr) => gr,
						// Return opaque error to avoid leaking existence of the grant.
						Err(e) => {
							debug!("Error retrieving bearer access grant: {e}");
							return Err(Error::InvalidAuth);
						}
					};
					// Ensure that the transaction is cancelled.
					tx.cancel().await?;
					// Authenticate bearer key against stored grant.
					verify_grant_bearer(&gr, key)?;
					// If the subject of the grant is a system user, get their roles.
					let roles = if let Some(access::Subject::User(user)) = &gr.subject {
						// Create a new readonly transaction.
						let tx = kvs.transaction(Read, Optimistic).await?;
						// Fetch the specified user from storage.
						let user = tx.get_db_user(&ns, &db, user).await.map_err(|e| {
							debug!("Error retrieving user for bearer access to database `{ns}/{db}`: {e}");
							// Return opaque error to avoid leaking grant subject existence.
							Error::InvalidAuth
						})?;
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
						exp: expiration(av.duration.token)?,
						jti: Some(Uuid::new_v4().to_string()),
						ns: Some(ns.to_owned()),
						db: Some(db.to_owned()),
						ac: Some(ac.to_owned()),
						id: match &gr.subject {
							Some(access::Subject::User(user)) => Some(user.to_raw()),
							Some(access::Subject::Record(rid)) => Some(rid.to_raw()),
							// Return opaque error as this code should not be reachable.
							None => return Err(Error::InvalidAuth),
						},
						roles: match &gr.subject {
							Some(access::Subject::User(_)) => {
								Some(roles.iter().map(|v| v.to_string()).collect())
							}
							Some(access::Subject::Record(_)) => Default::default(),
							// Return opaque error as this code should not be reachable.
							None => return Err(Error::InvalidAuth),
						},
						..Claims::default()
					};
					// AUTHENTICATE clause
					if let Some(au) = &av.authenticate {
						// Setup the system session for executing the clause
						let mut sess = Session::editor().with_ns(&ns).with_db(&db);
						sess.tk = Some((&claims).into());
						sess.ip.clone_from(&session.ip);
						sess.or.clone_from(&session.or);
						authenticate_generic(kvs, &sess, au).await?;
					}
					// Log the authenticated access method information.
					trace!("Signing in to database with bearer access method `{}`", ac);
					// Create the authentication token.
					let enc = encode(&Header::new(iss.alg.into()), &claims, &key);
					// Set the authentication on the session.
					session.tk = Some((&claims).into());
					session.ns = Some(ns.to_owned());
					session.db = Some(db.to_owned());
					session.ac = Some(ac.to_owned());
					session.exp = expiration(av.duration.session)?;
					match &gr.subject {
						Some(access::Subject::User(user)) => {
							session.au = Arc::new(Auth::new(Actor::new(
								user.to_string(),
								roles.iter().map(Role::from).collect(),
								Level::Database(ns, db),
							)));
						}
						Some(access::Subject::Record(rid)) => {
							session.au = Arc::new(Auth::new(Actor::new(
								rid.to_string(),
								Default::default(),
								Level::Record(ns, db, rid.to_string()),
							)));
							session.rd = Some(Value::from(rid.to_owned()));
						}
						// Return opaque error as this code should not be reachable.
						None => return Err(Error::InvalidAuth),
					};
					// Check the authentication token.
					match enc {
						// The authentication token was created successfully.
						Ok(tk) => Ok(tk),
						_ => Err(Error::TokenMakingFailed),
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
) -> Result<String, Error> {
	match verify_db_creds(kvs, &ns, &db, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: expiration(u.duration.token)?,
				jti: Some(Uuid::new_v4().to_string()),
				ns: Some(ns.to_owned()),
				db: Some(db.to_owned()),
				id: Some(user),
				..Claims::default()
			};
			// Log the authenticated database info
			trace!("Signing in to database `{ns}/{db}`");
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);
			// Set the authentication on the session
			session.tk = Some((&val).into());
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.exp = expiration(u.duration.session)?;
			session.au = Arc::new((&u, Level::Database(ns.to_owned(), db.to_owned())).into());
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(tk),
				_ => Err(Error::TokenMakingFailed),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!("Failed to verify signin credentials for user `{user}` in database `{ns}/{db}`: {e}");
			Err(Error::InvalidAuth)
		}
	}
}

pub async fn ns_access(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	ac: String,
	vars: Object,
) -> Result<String, Error> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read, Optimistic).await?;
	// Fetch the specified access method from storage
	let access = tx.get_ns_access(&ns, &ac).await;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Check the provided access method exists
	match access {
		Ok(av) => {
			// Check the access method type
			match &av.kind {
				AccessType::Bearer(at) => {
					// TODO(gguillemas): Remove this once bearer access is no longer experimental.
					if !*EXPERIMENTAL_BEARER_ACCESS {
						// Return opaque error to avoid leaking the existence of the feature.
						debug!(
							"Error attempting to authenticate with disabled bearer access feature"
						);
						return Err(Error::InvalidAuth);
					}
					// Check if the bearer access method supports issuing tokens.
					let iss = match &at.jwt.issue {
						Some(iss) => iss.clone(),
						_ => return Err(Error::AccessMethodMismatch),
					};
					// Extract key identifier and key from the provided variables.
					let (kid, key) = validate_grant_bearer(vars)?;
					// Create a new readonly transaction
					let tx = kvs.transaction(Read, Optimistic).await?;
					// Fetch the specified access grant from storage
					let gr = match tx.get_ns_access_grant(&ns, &ac, &kid).await {
						Ok(gr) => gr,
						// Return opaque error to avoid leaking existence of the grant.
						Err(e) => {
							debug!("Error retrieving bearer access grant: {e}");
							return Err(Error::InvalidAuth);
						}
					};
					// Ensure that the transaction is cancelled.
					tx.cancel().await?;
					// Authenticate bearer key against stored grant.
					verify_grant_bearer(&gr, key)?;
					// If the subject of the grant is a system user, get their roles.
					let roles = if let Some(access::Subject::User(user)) = &gr.subject {
						// Create a new readonly transaction.
						let tx = kvs.transaction(Read, Optimistic).await?;
						// Fetch the specified user from storage.
						let user =
							tx.get_ns_user(&ns, user).await.map_err(|e| {
								debug!("Error retrieving user for bearer access to namespace `{ns}`: {e}");
								// Return opaque error to avoid leaking grant subject existence.
								Error::InvalidAuth
							})?;
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
						exp: expiration(av.duration.token)?,
						jti: Some(Uuid::new_v4().to_string()),
						ns: Some(ns.to_owned()),
						ac: Some(ac.to_owned()),
						id: match &gr.subject {
							Some(access::Subject::User(user)) => Some(user.to_raw()),
							// Return opaque error as this code should not be reachable.
							_ => return Err(Error::InvalidAuth),
						},
						roles: match &gr.subject {
							Some(access::Subject::User(_)) => {
								Some(roles.iter().map(|v| v.to_string()).collect())
							}
							// Return opaque error as this code should not be reachable.
							_ => return Err(Error::InvalidAuth),
						},
						..Claims::default()
					};
					// AUTHENTICATE clause
					if let Some(au) = &av.authenticate {
						// Setup the system session for executing the clause
						let mut sess = Session::editor().with_ns(&ns);
						sess.tk = Some((&claims).into());
						sess.ip.clone_from(&session.ip);
						sess.or.clone_from(&session.or);
						authenticate_generic(kvs, &sess, au).await?;
					}
					// Log the authenticated access method information.
					trace!("Signing in to database with bearer access method `{}`", ac);
					// Create the authentication token.
					let enc = encode(&Header::new(iss.alg.into()), &claims, &key);
					// Set the authentication on the session.
					session.tk = Some((&claims).into());
					session.ns = Some(ns.to_owned());
					session.ac = Some(ac.to_owned());
					session.exp = expiration(av.duration.session)?;
					match &gr.subject {
						Some(access::Subject::User(user)) => {
							session.au = Arc::new(Auth::new(Actor::new(
								user.to_string(),
								roles.iter().map(Role::from).collect(),
								Level::Namespace(ns),
							)));
						}
						// Return opaque error as this code should not be reachable.
						_ => return Err(Error::InvalidAuth),
					};
					// Check the authentication token.
					match enc {
						// The authentication token was created successfully.
						Ok(tk) => Ok(tk),
						_ => Err(Error::TokenMakingFailed),
					}
				}
				_ => Err(Error::AccessMethodMismatch),
			}
		}
		_ => Err(Error::AccessNotFound),
	}
}

pub async fn ns_user(
	kvs: &Datastore,
	session: &mut Session,
	ns: String,
	user: String,
	pass: String,
) -> Result<String, Error> {
	match verify_ns_creds(kvs, &ns, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: expiration(u.duration.token)?,
				jti: Some(Uuid::new_v4().to_string()),
				ns: Some(ns.to_owned()),
				id: Some(user),
				..Claims::default()
			};
			// Log the authenticated namespace info
			trace!("Signing in to namespace `{ns}`");
			// Create the authentication token
			let enc = encode(&HEADER, &val, &key);
			// Set the authentication on the session
			session.tk = Some((&val).into());
			session.ns = Some(ns.to_owned());
			session.exp = expiration(u.duration.session)?;
			session.au = Arc::new((&u, Level::Namespace(ns.to_owned())).into());
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(tk),
				_ => Err(Error::TokenMakingFailed),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!(
				"Failed to verify signin credentials for user `{user}` in namespace `{ns}`: {e}"
			);
			Err(Error::InvalidAuth)
		}
	}
}

pub async fn root_user(
	kvs: &Datastore,
	session: &mut Session,
	user: String,
	pass: String,
) -> Result<String, Error> {
	match verify_root_creds(kvs, &user, &pass).await {
		Ok(u) => {
			// Create the authentication key
			let key = EncodingKey::from_secret(u.code.as_ref());
			// Create the authentication claim
			let val = Claims {
				iss: Some(SERVER_NAME.to_owned()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: expiration(u.duration.token)?,
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
			session.exp = expiration(u.duration.session)?;
			session.au = Arc::new((&u, Level::Root).into());
			// Check the authentication token
			match enc {
				// The auth token was created successfully
				Ok(tk) => Ok(tk),
				_ => Err(Error::TokenMakingFailed),
			}
		}
		// The password did not verify
		Err(e) => {
			debug!("Failed to verify signin credentials for user `{user}` in root: {e}");
			Err(Error::InvalidAuth)
		}
	}
}

pub async fn root_access(
	kvs: &Datastore,
	session: &mut Session,
	ac: String,
	vars: Object,
) -> Result<String, Error> {
	// Create a new readonly transaction
	let tx = kvs.transaction(Read, Optimistic).await?;
	// Fetch the specified access method from storage
	let access = tx.get_root_access(&ac).await;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Check the provided access method exists
	match access {
		Ok(av) => {
			// Check the access method type
			match &av.kind {
				AccessType::Bearer(at) => {
					// TODO(gguillemas): Remove this once bearer access is no longer experimental.
					if !*EXPERIMENTAL_BEARER_ACCESS {
						// Return opaque error to avoid leaking the existence of the feature.
						debug!(
							"Error attempting to authenticate with disabled bearer access feature"
						);
						return Err(Error::InvalidAuth);
					}
					// Check if the bearer access method supports issuing tokens.
					let iss = match &at.jwt.issue {
						Some(iss) => iss.clone(),
						_ => return Err(Error::AccessMethodMismatch),
					};
					// Extract key identifier and key from the provided variables.
					let (kid, key) = validate_grant_bearer(vars)?;
					// Create a new readonly transaction
					let tx = kvs.transaction(Read, Optimistic).await?;
					// Fetch the specified access grant from storage
					let gr = match tx.get_root_access_grant(&ac, &kid).await {
						Ok(gr) => gr,
						// Return opaque error to avoid leaking existence of the grant.
						Err(e) => {
							debug!("Error retrieving bearer access grant: {e}");
							return Err(Error::InvalidAuth);
						}
					};
					// Ensure that the transaction is cancelled.
					tx.cancel().await?;
					// Authenticate bearer key against stored grant.
					verify_grant_bearer(&gr, key)?;
					// If the subject of the grant is a system user, get their roles.
					let roles = if let Some(access::Subject::User(user)) = &gr.subject {
						// Create a new readonly transaction.
						let tx = kvs.transaction(Read, Optimistic).await?;
						// Fetch the specified user from storage.
						let user = tx.get_root_user(user).await.map_err(|e| {
							debug!("Error retrieving user for bearer access to root: {e}");
							// Return opaque error to avoid leaking grant subject existence.
							Error::InvalidAuth
						})?;
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
						exp: expiration(av.duration.token)?,
						jti: Some(Uuid::new_v4().to_string()),
						ac: Some(ac.to_owned()),
						id: match &gr.subject {
							Some(access::Subject::User(user)) => Some(user.to_raw()),
							// Return opaque error as this code should not be reachable.
							_ => return Err(Error::InvalidAuth),
						},
						roles: match &gr.subject {
							Some(access::Subject::User(_)) => {
								Some(roles.iter().map(|v| v.to_string()).collect())
							}
							// Return opaque error as this code should not be reachable.
							_ => return Err(Error::InvalidAuth),
						},
						..Claims::default()
					};
					// AUTHENTICATE clause
					if let Some(au) = &av.authenticate {
						// Setup the system session for executing the clause
						let mut sess = Session::editor();
						sess.tk = Some((&claims).into());
						sess.ip.clone_from(&session.ip);
						sess.or.clone_from(&session.or);
						authenticate_generic(kvs, &sess, au).await?;
					}
					// Log the authenticated access method information.
					trace!("Signing in to database with bearer access method `{}`", ac);
					// Create the authentication token.
					let enc = encode(&Header::new(iss.alg.into()), &claims, &key);
					// Set the authentication on the session.
					session.tk = Some(claims.into());
					session.ac = Some(ac.to_owned());
					session.exp = expiration(av.duration.session)?;
					match &gr.subject {
						Some(access::Subject::User(user)) => {
							session.au = Arc::new(Auth::new(Actor::new(
								user.to_string(),
								roles.iter().map(Role::from).collect(),
								Level::Root,
							)));
						}
						// Return opaque error as this code should not be reachable.
						_ => return Err(Error::InvalidAuth),
					};
					// Check the authentication token.
					match enc {
						// The authentication token was created successfully.
						Ok(tk) => Ok(tk),
						_ => Err(Error::TokenMakingFailed),
					}
				}
				_ => Err(Error::AccessMethodMismatch),
			}
		}
		_ => Err(Error::AccessNotFound),
	}
}

pub fn validate_grant_bearer(vars: Object) -> Result<(String, String), Error> {
	// Extract the provided key.
	let key = match vars.get("key") {
		Some(key) => key.to_raw_string(),
		None => return Err(Error::AccessBearerMissingKey),
	};
	if key.len() != access::GRANT_BEARER_LENGTH {
		return Err(Error::AccessGrantBearerInvalid);
	}
	// Retrieve the prefix from the provided key.
	let prefix: String = key.chars().take(access::GRANT_BEARER_PREFIX.len()).collect();
	// Check the length of the key prefix.
	if prefix != access::GRANT_BEARER_PREFIX {
		return Err(Error::AccessGrantBearerInvalid);
	}
	// Retrieve the key identifier from the provided key.
	let kid: String = key
		.chars()
		.skip(access::GRANT_BEARER_PREFIX.len() + 1)
		.take(access::GRANT_BEARER_ID_LENGTH)
		.collect();
	// Check the length of the key identifier.
	if kid.len() != access::GRANT_BEARER_ID_LENGTH {
		return Err(Error::AccessGrantBearerInvalid);
	};

	Ok((kid, key))
}

pub fn verify_grant_bearer(gr: &Arc<AccessGrant>, key: String) -> Result<(), Error> {
	// Check if the grant is revoked or expired.
	match (&gr.expiration, &gr.revocation) {
		(None, None) => {}
		(Some(exp), None) => {
			if exp < &Datetime::default() {
				// Return opaque error to avoid leaking revocation status.
				debug!("Bearer access grant `{}` for method `{}` is expired", gr.id, gr.ac);
				return Err(Error::InvalidAuth);
			}
		}
		(_, Some(_)) => {
			debug!("Bearer access grant `{}` for method `{}` is revoked", gr.id, gr.ac);
			return Err(Error::InvalidAuth);
		}
	}
	// Check if the provided key matches the bearer key in the grant.
	// We use time-constant comparison to prevent timing attacks.
	if let access::Grant::Bearer(grant) = &gr.grant {
		let grant_key_bytes: &[u8] = grant.key.as_bytes();
		let signin_key_bytes: &[u8] = key.as_bytes();
		let ok: bool = grant_key_bytes.ct_eq(signin_key_bytes).into();
		if !ok {
			debug!("Bearer access grant `{}` for method `{}` is invalid", gr.id, gr.ac);
			return Err(Error::InvalidAuth);
		}
	};

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::iam::Role;
	use chrono::Duration;
	use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
	use std::collections::HashMap;

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
			if let Ok(tk) = res {
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
					for role in &AVAILABLE_ROLES {
						let has_role = sess.au.has_role(role);
						let should_have_role = case.roles.contains(role);
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
					if let Ok(tk) = res {
						// Decode token without validation
						let token_data = decode::<Claims>(&tk, &DecodingKey::from_secret(&[]), &{
							let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
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

			match res {
				Err(Error::Thrown(e)) if e == "This user is not enabled" => {} // ok
				res => panic!(
				    "Expected authentication to failed due to user not being enabled, but instead received: {:?}",
					res
				),
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

			match res {
				Err(Error::InvalidAuth) => {} // ok
				res => panic!(
					"Expected authentication to generally fail, but instead received: {:?}",
					res
				),
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

		for level in &test_levels {
			println!("Test level: {}", level.level);

			// Test with correct bearer key
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_string();

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
					!sess.au.has_role(&Role::Viewer),
					"Auth user expected to not have Viewer role"
				);
				assert!(
					// User is defined with this role only
					sess.au.has_role(&Role::Editor),
					"Auth user expected to have Editor role"
				);
				assert!(
					!sess.au.has_role(&Role::Owner),
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
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_string();

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
					!sess.au.has_role(&Role::Viewer),
					"Auth user expected to not have Viewer role"
				);
				assert!(
					// User is defined with this role only
					sess.au.has_role(&Role::Editor),
					"Auth user expected to have Editor role"
				);
				assert!(
					!sess.au.has_role(&Role::Owner),
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
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_string();

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

				match res {
					Err(Error::Thrown(e)) => {
						assert_eq!(e, "Test authentication error")
					}
					res => panic!(
						"Expected a thrown authentication error, but instead received: {:?}",
						res
					),
				}
			}

			// Test with expired grant
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR GRANT 1s FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_string();

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

				match res {
					Err(Error::InvalidAuth) => {} // ok
					res => panic!(
						"Expected a generic authentication error, but instead received: {:?}",
						res
					),
				}
			}

			// Test with revoked grant
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR GRANT 1s FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_string();

				// Get grant identifier from key
				let kid = key.split("-").collect::<Vec<&str>>()[2];

				// Revoke grant
				ds.execute(
					&format!(
						r#"
					ACCESS api ON {} REVOKE `{kid}`;
					"#,
						level.level
					),
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

				match res {
					Err(Error::InvalidAuth) => {} // ok
					res => panic!(
						"Expected a generic authentication error, but instead received: {:?}",
						res
					),
				}
			}

			// Test with removed access method
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR GRANT 1s FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let key = grant.get("key").unwrap().clone().as_string();

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

				match res {
					Err(Error::AccessNotFound) => {} // ok
					res => panic!(
						"Expected an access method not found error, but instead received: {:?}",
						res
					),
				}
			}

			// Test with missing key
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let _key = grant.get("key").unwrap().clone().as_string();

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

				match res {
					Err(Error::AccessBearerMissingKey) => {} // ok
					res => panic!(
						"Expected a missing key authentication error, but instead received: {:?}",
						res
					),
				}
			}

			// Test with incorrect bearer key prefix part
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let valid_key = grant.get("key").unwrap().clone().as_string();

				// Replace a character from the key prefix
				let mut invalid_key: Vec<char> = valid_key.chars().collect();
				invalid_key[access::GRANT_BEARER_PREFIX.len() - 2] = '_';
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

				match res {
					Err(Error::AccessGrantBearerInvalid) => {} // ok
					res => panic!(
						"Expected an invalid key authentication error, but instead received: {:?}",
						res
					),
				}
			}

			// Test with incorrect bearer key length
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let valid_key = grant.get("key").unwrap().clone().as_string();

				// Remove a character from the bearer key
				let mut invalid_key: Vec<char> = valid_key.chars().collect();
				invalid_key.truncate(access::GRANT_BEARER_LENGTH - 1);
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

				match res {
					Err(Error::AccessGrantBearerInvalid) => {} // ok
					res => panic!(
						"Expected an invalid key authentication error, but instead received: {:?}",
						res
					),
				}
			}

			// Test with incorrect bearer key identifier part
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let valid_key = grant.get("key").unwrap().clone().as_string();

				// Replace a character from the key identifier
				let mut invalid_key: Vec<char> = valid_key.chars().collect();
				invalid_key[access::GRANT_BEARER_PREFIX.len() + 2] = '_';
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

				match res {
					Err(Error::InvalidAuth) => {} // ok
					res => panic!(
						"Expected a generic authentication error, but instead received: {:?}",
						res
					),
				}
			}

			// Test with incorrect bearer key value
			{
				let ds = Datastore::new("memory").await.unwrap();
				let sess = Session::owner().with_ns("test").with_db("test");
				let res = ds
					.execute(
						&format!(
							r#"
					DEFINE ACCESS api ON {} TYPE BEARER DURATION FOR SESSION 2h;
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
					.coerce_to_object()
					.unwrap()
					.get("grant")
					.unwrap()
					.clone()
					.coerce_to_object()
					.unwrap();
				let valid_key = grant.get("key").unwrap().clone().as_string();

				// Replace a character from the key value
				let mut invalid_key: Vec<char> = valid_key.chars().collect();
				invalid_key
					[access::GRANT_BEARER_PREFIX.len() + 1 + access::GRANT_BEARER_ID_LENGTH + 2] = '_';
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

				match res {
					Err(Error::InvalidAuth) => {} // ok
					res => panic!(
						"Expected a generic authentication error, but instead received: {:?}",
						res
					),
				}
			}
		}
	}
}
