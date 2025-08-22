use std::str::{self, FromStr};
use std::sync::{Arc, LazyLock};

use anyhow::{Result, bail};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use chrono::Utc;
use jsonwebtoken::{DecodingKey, Validation, decode};

use crate::dbs::Session;
use crate::err::Error;
use crate::iam::access::{authenticate_generic, authenticate_record};
use crate::iam::issue::expiration;
#[cfg(feature = "jwks")]
use crate::iam::jwks;
use crate::iam::token::Claims;
use crate::iam::{self, Actor, Auth, Level, Role};
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::val::Value;
use crate::{catalog, syn};

/// Returns the decoding key as wel as the method by which to verify the key against
fn decode_key(alg: catalog::Algorithm, key: &[u8]) -> Result<(DecodingKey, Validation)> {
	let (dec, mut val) = match alg {
		catalog::Algorithm::Hs256 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS256))
		}
		catalog::Algorithm::Hs384 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS384))
		}
		catalog::Algorithm::Hs512 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS512))
		}
		catalog::Algorithm::EdDSA => {
			(DecodingKey::from_ed_pem(key)?, Validation::new(jsonwebtoken::Algorithm::EdDSA))
		}
		catalog::Algorithm::Es256 => {
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES256))
		}
		catalog::Algorithm::Es384 => {
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES384))
		}
		catalog::Algorithm::Es512 => {
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES384))
		}
		catalog::Algorithm::Ps256 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS256))
		}
		catalog::Algorithm::Ps384 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS384))
		}
		catalog::Algorithm::Ps512 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS512))
		}
		catalog::Algorithm::Rs256 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::RS256))
		}
		catalog::Algorithm::Rs384 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::RS384))
		}
		catalog::Algorithm::Rs512 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::RS512))
		}
	};

	// TODO(gguillemas): This keeps the existing behavior as of SurrealDB 2.0.0-alpha.9.
	// Up to that point, a fork of the "jsonwebtoken" crate in version 8.3.0 was being used.
	// Now that the audience claim is validated by default, we could allow users to leverage this.
	// This will most likely involve defining an audience string via "DEFINE ACCESS ... TYPE JWT".
	val.validate_aud = false;

	Ok((dec, val))
}

static KEY: LazyLock<DecodingKey> = LazyLock::new(|| DecodingKey::from_secret(&[]));

static DUD: LazyLock<Validation> = LazyLock::new(|| {
	let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
	validation.insecure_disable_signature_validation();
	validation.validate_nbf = false;
	validation.validate_exp = false;
	validation.validate_aud = false;
	validation
});

pub async fn basic(
	kvs: &Datastore,
	session: &mut Session,
	user: &str,
	pass: &str,
	ns: Option<&str>,
	db: Option<&str>,
) -> Result<()> {
	// Log the authentication type
	trace!("Attempting basic authentication");
	// Check if the parameters exist
	match (ns, db) {
		// DB signin
		(Some(ns), Some(db)) => match verify_db_creds(kvs, ns, db, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as database user '{}'", user);
				session.exp = expiration(u.session_duration)?;
				let au = Auth::new(Actor::from_role_names(
					u.name.clone(),
					&u.roles,
					Level::Database(ns.to_owned(), db.to_owned()),
				)?);

				session.au = Arc::new(au);
				Ok(())
			}
			Err(err) => Err(err),
		},
		// NS signin
		(Some(ns), None) => match verify_ns_creds(kvs, ns, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as namespace user '{}'", user);
				session.exp = expiration(u.session_duration)?;
				let au = Auth::new(Actor::from_role_names(
					u.name.clone(),
					&u.roles,
					Level::Namespace(ns.to_owned()),
				)?);

				session.au = Arc::new(au);
				Ok(())
			}
			Err(err) => Err(err),
		},
		// Root signin
		(None, None) => match verify_root_creds(kvs, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as root user '{}'", user);
				session.exp = expiration(u.session_duration)?;
				let au = Auth::new(Actor::from_role_names(u.name.clone(), &u.roles, Level::Root)?);

				session.au = Arc::new(au);
				Ok(())
			}
			Err(err) => Err(err),
		},
		(None, Some(db)) => {
			debug!(
				"Attempted basic authentication in database '{db}' without specifying a namespace"
			);
			Err(anyhow::Error::new(Error::InvalidAuth))
		}
	}
}

pub async fn token(kvs: &Datastore, session: &mut Session, token: &str) -> Result<()> {
	// Log the authentication type
	trace!("Attempting token authentication");
	// Decode the token without verifying
	let token_data = decode::<Claims>(token, &KEY, &DUD)?;
	// Convert the token to a SurrealQL object value
	let value = Value::from(token_data.claims.clone().into_claims_object());
	// Check if the auth token can be used
	if let Some(nbf) = token_data.claims.nbf {
		if nbf > Utc::now().timestamp() {
			debug!("Token verification failed due to the 'nbf' claim containing a future time");
			bail!(Error::InvalidAuth);
		}
	}
	// Check if the auth token has expired
	if let Some(exp) = token_data.claims.exp {
		if exp < Utc::now().timestamp() {
			debug!("Token verification failed due to the 'exp' claim containing a past time");
			bail!(Error::ExpiredToken);
		}
	}
	// Check the token authentication claims
	match &token_data.claims {
		// Check if this is record access
		Claims {
			ns: Some(ns),
			db: Some(db),
			ac: Some(ac),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating with record access method `{}`", ac);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read, Optimistic).await?;
			let db_def = match tx.get_db_by_name(ns, db).await? {
				Some(db) => db,
				None => {
					return Err(Error::DbNotFound {
						name: db.to_string(),
					}
					.into());
				}
			};
			// Parse the record id
			let mut rid = syn::record_id(id)?;
			// Get the database access method
			let Some(de) = tx.get_db_access(db_def.namespace_id, db_def.database_id, ac).await?
			else {
				return Err(Error::AccessDbNotFound {
					ac: ac.to_string(),
					ns: ns.to_string(),
					db: db.to_string(),
				}
				.into());
			};
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.access_type {
				catalog::AccessType::Record(at) => match &at.jwt.verify {
					catalog::JwtAccessVerify::Key(key) => {
						iam::verify::decode_key(key.alg, key.key.as_bytes())
					}
					#[cfg(feature = "jwks")]
					catalog::JwtAccessVerify::Jwks(jwks) => {
						if let Some(kid) = token_data.header.kid {
							jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
						} else {
							Err(anyhow::Error::new(Error::MissingTokenHeader("kid".to_string())))
						}
					}
					#[cfg(not(feature = "jwks"))]
					_ => bail!(Error::AccessMethodMismatch),
				}?,
				_ => bail!(Error::AccessMethodMismatch),
			};
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for finding the signin record
				let mut sess = Session::editor().with_ns(ns).with_db(db);
				sess.rd = Some(rid.clone().into());
				sess.tk = Some(token_data.claims.clone().into_claims_object().into());
				sess.ip.clone_from(&session.ip);
				sess.or.clone_from(&session.or);
				rid = authenticate_record(kvs, &sess, au).await?;
			}
			// Log the success
			debug!("Authenticated with record access method `{}`", ac);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.ac = Some(ac.to_owned());
			session.rd = Some(Value::from(rid.clone()));
			session.exp = expiration(de.session_duration)?;
			session.au = Arc::new(Auth::new(Actor::new(
				rid.to_string(),
				Default::default(),
				Level::Record(ns.to_string(), db.to_string(), rid.to_string()),
			)));
			Ok(())
		}
		// Check if this is database access
		// This can also be record access with an authenticate clause
		Claims {
			ns: Some(ns),
			db: Some(db),
			ac: Some(ac),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to database `{}` with access method `{}`", db, ac);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read, Optimistic).await?;
			let db_def = match tx.get_db_by_name(ns, db).await? {
				Some(db) => db,
				None => {
					return Err(Error::DbNotFound {
						name: db.to_string(),
					}
					.into());
				}
			};

			// Get the database access method
			let de = tx.get_db_access(db_def.namespace_id, db_def.database_id, ac).await?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;

			let Some(de) = de else {
				return Err(Error::AccessDbNotFound {
					ac: ac.to_string(),
					ns: ns.to_string(),
					db: db.to_string(),
				}
				.into());
			};

			// Obtain the configuration to verify the token based on the access method
			match &de.access_type {
				// If the access type is Jwt or Bearer, this is database access
				catalog::AccessType::Jwt(jwt)
				| catalog::AccessType::Bearer(catalog::BearerAccess {
					jwt,
					..
				}) => {
					let cf = match &jwt.verify {
						catalog::JwtAccessVerify::Key(key) => {
							decode_key(key.alg, key.key.as_bytes())
						}
						#[cfg(feature = "jwks")]
						catalog::JwtAccessVerify::Jwks(jwks) => {
							if let Some(kid) = token_data.header.kid {
								jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
							} else {
								Err(anyhow::Error::new(Error::MissingTokenHeader(
									"kid".to_string(),
								)))
							}
						}
						#[cfg(not(feature = "jwks"))]
						_ => bail!(Error::AccessMethodMismatch),
					}?;
					// Verify the token
					verify_token(token, &cf.0, &cf.1)?;
					// AUTHENTICATE clause
					if let Some(au) = &de.authenticate {
						// Setup the system session for executing the clause
						let mut sess = Session::editor().with_ns(ns).with_db(db);
						sess.tk = Some(token_data.claims.clone().into_claims_object().into());
						sess.ip.clone_from(&session.ip);
						sess.or.clone_from(&session.or);
						authenticate_generic(kvs, &sess, au).await?;
					}
					// Parse the roles
					let roles = match &token_data.claims.roles {
						// If no role is provided, grant the viewer role
						None => vec![Role::Viewer],
						// If roles are provided, parse them
						Some(roles) => roles
							.iter()
							.map(|r| -> Result<Role> {
								Role::from_str(r.as_str())
									.map_err(Error::IamError)
									.map_err(anyhow::Error::new)
							})
							.collect::<Result<Vec<_>, _>>()?,
					};
					// Log the success
					debug!("Authenticated to database `{}` with access method `{}`", db, ac);
					// Set the session
					session.tk = Some(value);
					session.ns = Some(ns.to_owned());
					session.db = Some(db.to_owned());
					session.ac = Some(ac.to_owned());
					session.exp = expiration(de.session_duration)?;
					session.au = Arc::new(Auth::new(Actor::new(
						de.name.to_string(),
						roles,
						Level::Database(ns.to_string(), db.to_string()),
					)));
				}
				// If the access type is Record, this is record access
				// Record access without an "id" claim is only possible if there is an AUTHENTICATE
				// clause The clause can make up for the missing "id" claim by resolving other
				// claims to a specific record
				catalog::AccessType::Record(at) => match &de.authenticate {
					Some(au) => {
						trace!("Access method `{}` is record access with AUTHENTICATE clause", ac);
						let cf = match &at.jwt.verify {
							catalog::JwtAccessVerify::Key(key) => {
								decode_key(key.alg, key.key.as_bytes())
							}
							#[cfg(feature = "jwks")]
							catalog::JwtAccessVerify::Jwks(jwks) => {
								if let Some(kid) = token_data.header.kid {
									jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
								} else {
									Err(anyhow::Error::new(Error::MissingTokenHeader(
										"kid".to_string(),
									)))
								}
							}
							#[cfg(not(feature = "jwks"))]
							_ => bail!(Error::AccessMethodMismatch),
						}?;

						// Verify the token
						verify_token(token, &cf.0, &cf.1)?;
						// AUTHENTICATE clause
						// Setup the system session for finding the signin record
						let mut sess = Session::editor().with_ns(ns).with_db(db);
						sess.tk = Some(token_data.claims.clone().into_claims_object().into());
						sess.ip.clone_from(&session.ip);
						sess.or.clone_from(&session.or);
						let rid = authenticate_record(kvs, &sess, au).await?;
						// Log the success
						debug!("Authenticated with record access method `{}`", ac);
						// Set the session
						session.tk = Some(value);
						session.ns = Some(ns.to_owned());
						session.db = Some(db.to_owned());
						session.ac = Some(ac.to_owned());
						session.rd = Some(Value::from(rid.clone()));
						session.exp = expiration(de.session_duration)?;
						session.au = Arc::new(Auth::new(Actor::new(
							rid.to_string(),
							Default::default(),
							Level::Record(ns.to_string(), db.to_string(), rid.to_string()),
						)));
					}
					_ => bail!(Error::AccessMethodMismatch),
				},
			};
			Ok(())
		}
		// Check if this is database authentication with user credentials
		Claims {
			ns: Some(ns),
			db: Some(db),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to database `{}` with user `{}`", db, id);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read, Optimistic).await?;
			let db_def = match tx.get_db_by_name(ns, db).await? {
				Some(db) => db,
				None => {
					return Err(Error::DbNotFound {
						name: db.to_string(),
					}
					.into());
				}
			};

			// Get the database user
			let de = match tx
				.get_db_user(db_def.namespace_id, db_def.database_id, id)
				.await
				.map_err(|e| {
					debug!("Error while authenticating to database `{db}`: {e}");
					Error::InvalidAuth
				})? {
				Some(de) => de,
				None => return Err(Error::InvalidAuth.into()),
			};
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = decode_key(catalog::Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to database `{}` with user `{}` using token", db, id);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.exp = expiration(de.session_duration)?;
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				de.roles
					.iter()
					.map(|e| Role::from_str(e).map_err(Error::from))
					.collect::<Result<_, _>>()?,
				Level::Database(ns.to_string(), db.to_string()),
			)));
			Ok(())
		}
		// Check if this is namespace access
		Claims {
			ns: Some(ns),
			ac: Some(ac),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to namespace `{}` with access method `{}`", ns, ac);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read, Optimistic).await?;
			let ns_def = match tx.get_ns_by_name(ns).await? {
				Some(ns) => ns,
				None => {
					return Err(Error::NsNotFound {
						name: ns.to_string(),
					}
					.into());
				}
			};

			// Get the namespace access method
			let de = tx.get_ns_access(ns_def.namespace_id, ac).await?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;

			let Some(de) = de else {
				return Err(Error::AccessNsNotFound {
					ac: ac.to_string(),
					ns: ns.to_string(),
				}
				.into());
			};

			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.access_type {
				catalog::AccessType::Jwt(jwt)
				| catalog::AccessType::Bearer(catalog::BearerAccess {
					jwt,
					..
				}) => match &jwt.verify {
					catalog::JwtAccessVerify::Key(key) => decode_key(key.alg, key.key.as_bytes()),
					#[cfg(feature = "jwks")]
					catalog::JwtAccessVerify::Jwks(jwks) => {
						if let Some(kid) = token_data.header.kid {
							jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
						} else {
							bail!(Error::MissingTokenHeader("kid".to_string()))
						}
					}
					#[cfg(not(feature = "jwks"))]
					_ => bail!(Error::AccessMethodMismatch),
				},
				_ => bail!(Error::AccessMethodMismatch),
			}?;
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for executing the clause
				let mut sess = Session::editor().with_ns(ns);
				sess.tk = Some(token_data.claims.clone().into_claims_object().into());
				sess.ip.clone_from(&session.ip);
				sess.or.clone_from(&session.or);
				authenticate_generic(kvs, &sess, au).await?;
			}
			// Parse the roles
			let roles = match &token_data.claims.roles {
				// If no role is provided, grant the viewer role
				None => vec![Role::Viewer],
				// If roles are provided, parse them
				Some(roles) => roles
					.iter()
					.map(|r| -> Result<Role> {
						Role::from_str(r.as_str())
							.map_err(Error::IamError)
							.map_err(anyhow::Error::new)
					})
					.collect::<Result<Vec<_>, _>>()?,
			};
			// Log the success
			debug!("Authenticated to namespace `{}` with access method `{}`", ns, ac);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.ac = Some(ac.to_owned());
			session.exp = expiration(de.session_duration)?;
			session.au = Arc::new(Auth::new(Actor::new(
				de.name.to_string(),
				roles,
				Level::Namespace(ns.to_string()),
			)));
			Ok(())
		}
		// Check if this is namespace authentication with user credentials
		Claims {
			ns: Some(ns),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to namespace `{}` with user `{}`", ns, id);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read, Optimistic).await?;
			let ns_def = match tx.get_ns_by_name(ns).await? {
				Some(ns) => ns,
				None => {
					return Err(Error::NsNotFound {
						name: ns.to_string(),
					}
					.into());
				}
			};
			// Get the namespace user
			let de = tx
				.get_ns_user(ns_def.namespace_id, id)
				.await
				.map_err(|e| {
					debug!("Error while authenticating to namespace `{ns}`: {e}");
					Error::InvalidAuth
				})?
				.ok_or(Error::InvalidAuth)?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = decode_key(catalog::Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to namespace `{}` with user `{}` using token", ns, id);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.exp = expiration(de.session_duration)?;
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				de.roles
					.iter()
					.map(|e| Role::from_str(e).map_err(Error::from))
					.collect::<Result<_, _>>()?,
				Level::Namespace(ns.to_string()),
			)));
			Ok(())
		}
		// Check if this is root access
		Claims {
			ac: Some(ac),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to root with access method `{}`", ac);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read, Optimistic).await?;
			// Get the namespace access method
			let de = tx.get_root_access(ac).await?;

			// Ensure that the transaction is cancelled
			tx.cancel().await?;

			let Some(de) = de else {
				return Err(Error::AccessRootNotFound {
					ac: ac.to_string(),
				}
				.into());
			};

			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.access_type {
				catalog::AccessType::Jwt(jwt)
				| catalog::AccessType::Bearer(catalog::BearerAccess {
					jwt,
					..
				}) => match &jwt.verify {
					catalog::JwtAccessVerify::Key(key) => decode_key(key.alg, key.key.as_bytes()),
					#[cfg(feature = "jwks")]
					catalog::JwtAccessVerify::Jwks(jwks) => {
						if let Some(kid) = token_data.header.kid {
							jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
						} else {
							bail!(Error::MissingTokenHeader("kid".to_string()))
						}
					}
					#[cfg(not(feature = "jwks"))]
					_ => bail!(Error::AccessMethodMismatch),
				},
				_ => bail!(Error::AccessMethodMismatch),
			}?;
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for executing the clause
				let mut sess = Session::editor();
				sess.tk = Some(token_data.claims.clone().into_claims_object().into());
				sess.ip.clone_from(&session.ip);
				sess.or.clone_from(&session.or);
				authenticate_generic(kvs, &sess, au).await?;
			}
			// Parse the roles
			let roles = match &token_data.claims.roles {
				// If no role is provided, grant the viewer role
				None => vec![Role::Viewer],
				// If roles are provided, parse them
				Some(roles) => roles
					.iter()
					.map(|r| -> Result<Role> {
						Role::from_str(r.as_str())
							.map_err(Error::IamError)
							.map_err(anyhow::Error::new)
					})
					.collect::<Result<Vec<_>, _>>()?,
			};
			// Log the success
			debug!("Authenticated to root with access method `{}`", ac);
			// Set the session
			session.tk = Some(value);
			session.ac = Some(ac.to_owned());
			session.exp = expiration(de.session_duration)?;
			session.au = Arc::new(Auth::new(Actor::new(de.name.to_string(), roles, Level::Root)));
			Ok(())
		}
		// Check if this is root authentication with user credentials
		Claims {
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to root level with user `{}`", id);
			// Create a new readonly transaction
			let tx = kvs.transaction(Read, Optimistic).await?;
			// Get the namespace user
			let de = tx.expect_root_user(id).await.map_err(|e| {
				debug!("Error while authenticating to root: {e}");
				Error::InvalidAuth
			})?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = decode_key(catalog::Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			verify_token(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to root level with user `{}` using token", id);
			// Set the session
			session.tk = Some(value);
			session.exp = expiration(de.session_duration)?;
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				de.roles
					.iter()
					.map(|e| Role::from_str(e).map_err(Error::from))
					.collect::<Result<_, _>>()?,
				Level::Root,
			)));
			Ok(())
		}
		// There was an auth error
		_ => Err(anyhow::Error::new(Error::InvalidAuth)),
	}
}

pub async fn verify_root_creds(
	ds: &Datastore,
	user: &str,
	pass: &str,
) -> Result<catalog::UserDefinition> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read, Optimistic).await?;
	// Fetch the specified user from storage
	let user = tx.expect_root_user(user).await.map_err(|e| {
		debug!("Error retrieving user for authentication to root: {e}");

		Error::InvalidAuth
	})?;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
	// Clone the cached user object
	let user = (*user).clone();
	// Return the verified user object
	Ok(user)
}

pub async fn verify_ns_creds(
	ds: &Datastore,
	ns: &str,
	user: &str,
	pass: &str,
) -> Result<catalog::UserDefinition> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read, Optimistic).await?;
	let ns_def = match tx.get_ns_by_name(ns).await? {
		Some(ns) => ns,
		None => {
			return Err(Error::NsNotFound {
				name: ns.to_string(),
			}
			.into());
		}
	};

	// Fetch the specified user from storage
	let user = tx
		.get_ns_user(ns_def.namespace_id, user)
		.await
		.map_err(|e| {
			debug!("Error retrieving user for authentication to namespace `{ns}`: {e}");
			Error::InvalidAuth
		})?
		.ok_or(Error::InvalidAuth)?;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
	// Clone the cached user object
	let user = (*user).clone();
	// Return the verified user object
	Ok(user)
}

pub async fn verify_db_creds(
	ds: &Datastore,
	ns: &str,
	db: &str,
	user: &str,
	pass: &str,
) -> Result<catalog::UserDefinition> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read, Optimistic).await?;
	let db_def = match tx.get_db_by_name(ns, db).await? {
		Some(db) => db,
		None => {
			return Err(Error::DbNotFound {
				name: db.to_string(),
			}
			.into());
		}
	};
	// Fetch the specified user from storage
	let user = tx
		.get_db_user(db_def.namespace_id, db_def.database_id, user)
		.await
		.map_err(|e| {
			debug!("Error retrieving user for authentication to database `{ns}/{db}`: {e}");
			Error::InvalidAuth
		})?
		.ok_or(Error::InvalidAuth)?;
	// Ensure that the transaction is cancelled
	tx.cancel().await?;
	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
	// Clone the cached user object
	let user = (*user).clone();
	// Return the verified user object
	Ok(user)
}

fn verify_pass(pass: &str, hash: &str) -> Result<()> {
	// Compute the hash and verify the password
	let hash = PasswordHash::new(hash).unwrap();
	// Attempt to verify the password using Argon2
	match Argon2::default().verify_password(pass.as_ref(), &hash) {
		Ok(_) => Ok(()),
		_ => Err(anyhow::Error::new(Error::InvalidPass)),
	}
}

fn verify_token(token: &str, key: &DecodingKey, validation: &Validation) -> Result<()> {
	match decode::<Claims>(token, key, validation) {
		Ok(_) => Ok(()),
		Err(err) => {
			// Only transparently return certain token verification errors
			match err.kind() {
				jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
					Err(anyhow::Error::new(Error::ExpiredToken))
				}
				_ => {
					debug!("Error verifying authentication token: {err}");
					Err(anyhow::Error::new(Error::InvalidAuth))
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use argon2::password_hash::{PasswordHasher, SaltString};
	use chrono::Duration;
	use jsonwebtoken::{EncodingKey, encode};

	use super::*;
	use crate::iam::token::{Audience, HEADER};
	use crate::sql::statements::define::DefineKind;
	use crate::sql::statements::define::user::PassType;
	use crate::sql::{Ast, Ident};

	struct TestLevel {
		level: &'static str,
		ns: Option<&'static str>,
		db: Option<&'static str>,
	}

	const AVAILABLE_ROLES: [Role; 3] = [Role::Viewer, Role::Editor, Role::Owner];

	#[tokio::test]
	async fn test_basic() {
		#[derive(Debug)]
		struct TestCase {
			title: &'static str,
			password: &'static str,
			roles: Vec<Role>,
			expiration: Option<Duration>,
			expect_ok: bool,
		}

		let test_cases = vec![
			TestCase {
				title: "without roles or expiration",
				password: "pass",
				roles: vec![Role::Viewer],
				expiration: None,
				expect_ok: true,
			},
			TestCase {
				title: "with roles and expiration",
				password: "pass",
				roles: vec![Role::Editor, Role::Owner],
				expiration: Some(Duration::days(1)),
				expect_ok: true,
			},
			TestCase {
				title: "with invalid password",
				password: "invalid",
				roles: vec![],
				expiration: None,
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

				let duration_clause = if let Some(duration) = case.expiration {
					format!("DURATION FOR SESSION {}s", duration.num_seconds())
				} else {
					String::new()
				};

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

				let res = basic(&ds, &mut sess, "user", case.password, level.ns, level.db).await;

				if case.expect_ok {
					assert!(res.is_ok(), "Failed to signin: {:?}", res);
					assert_eq!(sess.au.id(), "user");

					// Check auth level
					assert_eq!(sess.au.level().ns(), level.ns);
					assert_eq!(sess.au.level().db(), level.db);
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

					// Check expiration
					if let Some(exp_duration) = case.expiration {
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
						assert_eq!(sess.exp, None, "Expiration is expected to be None");
					}
				} else {
					assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
				}
			}
		}
	}

	#[tokio::test]
	async fn test_basic_nonexistent_role() {
		use crate::iam::Error as IamError;
		use crate::sql::statements::DefineUserStatement;
		use crate::sql::statements::define::DefineStatement;
		use crate::sql::{Base, Expr, TopLevelExpr};
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
				name: Ident::new("user".to_string()).unwrap(),
				// This is the Argon2id hash for "pass" with a random salt.
				pass_type: PassType::Hash(
					"$argon2id$v=19$m=16,t=2,p=1$VUlHTHVOYjc5d0I1dGE3OQ$sVtmRNH+Xtiijk0uXL2+4w"
						.to_string(),
				),
				roles: vec![Ident::new("nonexistent".to_owned()).unwrap()],
				token_duration: None,
				session_duration: None,
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

			// Basic authentication using the newly defined user.
			let res = basic(&ds, &mut sess, "user", "pass", level.ns, level.db).await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				IamError::InvalidRole(_) => {}
				e => panic!("Unexpected error, expected IamError(InvalidRole) found {e}"),
			}
		}
	}

	#[tokio::test]
	async fn test_token() {
		#[derive(Debug)]
		struct TestCase {
			title: &'static str,
			roles: Option<Vec<&'static str>>,
			key: &'static str,
			expect_roles: Vec<Role>,
			expect_error: bool,
		}

		let test_cases = vec![
			TestCase {
				title: "with no roles",
				roles: None,
				key: "secret",
				expect_roles: vec![Role::Viewer],
				expect_error: false,
			},
			TestCase {
				title: "with roles",
				roles: Some(vec!["editor", "owner"]),
				key: "secret",
				expect_roles: vec![Role::Editor, Role::Owner],
				expect_error: false,
			},
			TestCase {
				title: "with nonexistent roles",
				roles: Some(vec!["viewer", "nonexistent"]),
				key: "secret",
				expect_roles: vec![],
				expect_error: true,
			},
			TestCase {
				title: "with invalid token signature",
				roles: None,
				key: "invalid",
				expect_roles: vec![],
				expect_error: true,
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

		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ac: Some("token".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");

		for level in &test_levels {
			// Define the access token for that level
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS token ON {} TYPE JWT
						ALGORITHM HS512 KEY 'secret' DURATION FOR SESSION 30d
					;
				"#,
					level.level
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			for case in &test_cases {
				println!("Test case: {} level {}", level.level, case.title);

				// Prepare the claims object
				let mut claims = claims.clone();
				claims.ns = level.ns.map(|s| s.to_string());
				claims.db = level.db.map(|s| s.to_string());
				claims.roles =
					case.roles.clone().map(|roles| roles.into_iter().map(String::from).collect());

				// Create the token
				let key = EncodingKey::from_secret(case.key.as_ref());
				let enc = encode(&HEADER, &claims, &key).unwrap();

				// Authenticate with the token
				let mut sess = Session::default();
				let res = token(&ds, &mut sess, &enc).await;

				if case.expect_error {
					assert!(res.is_err(), "Unexpected success for case: {:?}", case);
				} else {
					assert!(res.is_ok(), "Failed to sign in with token for case: {:?}", case);
					assert_eq!(sess.ns, level.ns.map(|s| s.to_string()));
					assert_eq!(sess.db, level.db.map(|s| s.to_string()));
					assert_eq!(sess.au.id(), "token");

					// Check roles
					for role in AVAILABLE_ROLES {
						let has_role = sess.au.has_role(role);
						let should_have_role = case.expect_roles.contains(&role);
						assert_eq!(has_role, should_have_role, "Role {:?} check failed", role);
					}

					// Ensure that the expiration is set correctly
					let exp = sess.exp.unwrap();
					let min_exp =
						(Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
					let max_exp =
						(Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
					assert!(
						exp > min_exp && exp < max_exp,
						"Session expiration is expected to match the defined duration in case: {:?}",
						case
					);
				}
			}
		}
	}

	#[tokio::test]
	async fn test_token_record() {
		#[derive(Debug)]
		struct TestCase {
			title: &'static str,
			ids: Vec<&'static str>,
			roles: Option<Vec<&'static str>>,
			key: &'static str,
			expect_error: bool,
		}

		let test_cases = vec![
			TestCase {
				title: "with no roles",
				ids: vec!["user:test"],
				roles: None,
				key: "secret",
				expect_error: false,
			},
			TestCase {
				title: "with roles",
				ids: vec!["user:test"],
				roles: Some(vec!["editor", "owner"]),
				key: "secret",
				expect_error: false,
			},
			TestCase {
				title: "with invalid token signature",
				ids: vec!["user:test"],
				roles: None,
				key: "invalid",
				expect_error: true,
			},
			TestCase {
				title: "with invalid id",
				ids: vec!["invalid"],
				roles: None,
				key: "invalid",
				expect_error: true,
			},
			TestCase {
				title: "with generic id",
				ids: vec!["user:2k9qnabxuxh8k4d5gfto"],
				roles: None,
				key: "secret",
				expect_error: false,
			},
			TestCase {
				title: "with numeric ids",
				ids: vec!["user:1", "user:2", "user:100", "user:10000000"],
				roles: None,
				key: "secret",
				expect_error: false,
			},
			TestCase {
				title: "with alphanumeric ids",
				ids: vec!["user:username", "user:username1", "user:username10", "user:username100"],
				roles: None,
				key: "secret",
				expect_error: false,
			},
			TestCase {
				title: "with ids including special characters",
				ids: vec![
					"user:⟨user.name⟩",
					"user:⟨user.name1⟩",
					"user:⟨user.name10⟩",
					"user:⟨user.name100⟩",
				],
				roles: None,
				key: "secret",
				expect_error: false,
			},
			TestCase {
				title: "with UUID ids",
				ids: vec!["user:⟨83149446-95f5-4c0d-9f42-136e7b272456⟩"],
				roles: None,
				key: "secret",
				expect_error: false,
			},
		];

		let secret = "secret";
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("token".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!(
				r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY '{secret}'
				DURATION FOR SESSION 30d;

			CREATE user:test;
			"#
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		for case in &test_cases {
			println!("Test case: {}", case.title);

			for id in &case.ids {
				// Prepare the claims object
				let mut claims = claims.clone();
				claims.id = Some((*id).to_string());
				claims.roles =
					case.roles.clone().map(|roles| roles.into_iter().map(String::from).collect());

				// Create the token
				let key = EncodingKey::from_secret(case.key.as_ref());
				let enc = encode(&HEADER, &claims, &key).unwrap();

				// Authenticate with the token
				let mut sess = Session::default();
				let res = token(&ds, &mut sess, &enc).await;

				if case.expect_error {
					assert!(res.is_err(), "Unexpected success for case: {:?}", case);
				} else {
					assert!(res.is_ok(), "Failed to sign in with token for case: {:?}", case);
					assert_eq!(sess.ns, Some("test".to_string()));
					assert_eq!(sess.db, Some("test".to_string()));
					assert_eq!(sess.au.id(), *id);

					// Ensure record users do not have roles
					for role in AVAILABLE_ROLES {
						assert!(
							!sess.au.has_role(role),
							"Auth user expected to not have role {:?} in case: {:?}",
							role,
							case
						);
					}

					// Ensure that the expiration is set correctly
					let exp = sess.exp.unwrap();
					let min_exp =
						(Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
					let max_exp =
						(Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
					assert!(
						exp > min_exp && exp < max_exp,
						"Session expiration is expected to match the defined duration in case: {:?}",
						case
					);
				}
			}
		}
	}

	#[tokio::test]
	async fn test_token_record_custom_claims() {
		use std::collections::HashMap;

		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!(
				r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT ALGORITHM HS512 KEY '{secret}'
				DURATION FOR SESSION 30d;

			CREATE user:test;
			"#
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		//
		// Token with valid custom claims of different types
		//
		let now = Utc::now().timestamp();
		let later = (Utc::now() + Duration::hours(1)).timestamp();
		{
			let claims_json = format!(
				r#"
				{{
					"iss": "surrealdb-test",
					"iat": {now},
					"nbf": {now},
					"exp": {later},
					"ns": "test",
					"db": "test",
					"ac": "token",
					"id": "user:test",

					"string_claim": "test",
					"bool_claim": true,
					"int_claim": 123456,
					"float_claim": 123.456,
					"array_claim": [
						"test_1",
						"test_2"
					],
					"object_claim": {{
						"test_1": "value_1",
						"test_2": {{
							"test_2_1": "value_2_1",
							"test_2_2": "value_2_2"
						}}
					}}
				}}
				"#
			);
			let claims = serde_json::from_str::<Claims>(&claims_json).unwrap();
			// Create the token
			let enc = match encode(&HEADER, &claims, &key) {
				Ok(enc) => enc,
				Err(err) => panic!("Failed to encode token: {:?}", err),
			};
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("token".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
			let tk = match sess.tk {
				Some(Value::Object(tk)) => tk,
				_ => panic!("Session token is not an object"),
			};
			let string_claim = tk.get("string_claim").unwrap();
			assert_eq!(*string_claim, Value::Strand("test".into()));
			let bool_claim = tk.get("bool_claim").unwrap();
			assert_eq!(*bool_claim, Value::Bool(true));
			let int_claim = tk.get("int_claim").unwrap();
			assert_eq!(*int_claim, Value::Number(123456.into()));
			let float_claim = tk.get("float_claim").unwrap();
			assert_eq!(*float_claim, Value::Number(123.456.into()));
			let array_claim = tk.get("array_claim").unwrap();
			assert_eq!(*array_claim, Value::Array(vec!["test_1", "test_2"].into()));
			let object_claim = tk.get("object_claim").unwrap();
			let mut test_object: HashMap<String, Value> = HashMap::new();
			test_object.insert("test_1".to_string(), Value::Strand("value_1".into()));
			let mut test_object_child = HashMap::new();
			test_object_child.insert("test_2_1".to_string(), Value::Strand("value_2_1".into()));
			test_object_child.insert("test_2_2".to_string(), Value::Strand("value_2_2".into()));
			test_object.insert("test_2".to_string(), Value::Object(test_object_child.into()));
			assert_eq!(*object_claim, Value::Object(test_object.into()));
		}
	}

	#[cfg(feature = "jwks")]
	#[tokio::test]
	async fn test_token_record_jwks() {
		use base64::Engine;
		use base64::engine::general_purpose::STANDARD_NO_PAD;
		use jsonwebtoken::jwk::{Jwk, JwkSet};
		use rand::Rng;
		use rand::distributions::Alphanumeric;
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

		use crate::dbs::capabilities::{Capabilities, NetTarget, Targets};

		// Use unique path to prevent accidental cache reuse
		fn random_path() -> String {
			let rng = rand::thread_rng();
			rng.sample_iter(&Alphanumeric).take(8).map(char::from).collect()
		}

		// Key identifier used in both JWT and JWT
		let kid = "test_kid";
		// Secret used to both sign and verify with HMAC
		let secret = "jwt_secret";

		// JWKS object with single JWK object providing the HS512 secret used to verify
		let jwks = JwkSet {
			keys: vec![Jwk {
				common: jsonwebtoken::jwk::CommonParameters {
					public_key_use: None,
					key_operations: None,
					key_algorithm: Some(jsonwebtoken::jwk::KeyAlgorithm::HS512),
					key_id: Some(kid.to_string()),
					x509_url: None,
					x509_chain: None,
					x509_sha1_fingerprint: None,
					x509_sha256_fingerprint: None,
				},
				algorithm: jsonwebtoken::jwk::AlgorithmParameters::OctetKey(
					jsonwebtoken::jwk::OctetKeyParameters {
						key_type: jsonwebtoken::jwk::OctetKeyType::Octet,
						value: STANDARD_NO_PAD.encode(secret),
					},
				),
			}],
		};

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.expect(1)
			.mount(&mock_server)
			.await;
		let server_url = mock_server.uri();

		// We allow requests to the local server serving the JWKS object
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);

		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!(
				r#"
			DEFINE ACCESS token ON DATABASE TYPE RECORD
				WITH JWT URL '{server_url}/{jwks_path}';

			CREATE user:test;
			"#
			)
			.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		// Use custom JWT header that includes the key identifier
		let header_with_kid = jsonwebtoken::Header {
			kid: Some(kid.to_string()),
			alg: jsonwebtoken::Algorithm::HS512,
			..jsonwebtoken::Header::default()
		};

		// Sign the JWT with the same secret specified in the JWK
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			aud: Some(Audience::Single("surrealdb-test".to_string())),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("token".to_string()),
			id: Some("user:test".to_string()),
			..Claims::default()
		};

		//
		// Test without roles defined
		// Roles should be ignored in record access
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&header_with_kid, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("token".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(Role::Owner), "Auth user expected to not have Owner role");
			assert_eq!(sess.exp, None, "Default session expiration is expected to be None");
		}

		//
		// Test with invalid signature
		//
		{
			// Prepare the claims object
			let claims = claims.clone();
			// Create the token
			let key = EncodingKey::from_secret("invalid".as_ref());
			let enc = encode(&header_with_kid, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}
	}

	#[test]
	fn test_verify_pass() {
		let salt = SaltString::generate(&mut rand::thread_rng());
		let hash = Argon2::default().hash_password("test".as_bytes(), &salt).unwrap().to_string();

		// Verify with the matching password
		verify_pass("test", &hash).unwrap();

		// Verify with a non matching password
		assert!(verify_pass("nonmatching", &hash).is_err());
	}

	#[tokio::test]
	async fn test_verify_creds_invalid() {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = "N".to_string();
		let db = "D".to_string();

		// Reject invalid ROOT credentials
		{
			assert!(verify_root_creds(&ds, "test", "test").await.is_err());
		}

		// Reject invalid NS credentials
		{
			assert!(verify_ns_creds(&ds, &ns, "test", "test").await.is_err());
		}

		// Reject invalid DB credentials
		{
			assert!(verify_db_creds(&ds, &ns, &db, "test", "test").await.is_err());
		}
	}

	#[tokio::test]
	async fn test_verify_creds_valid() {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = "N".to_string();
		let db = "D".to_string();

		// Define users
		{
			let sess = Session::owner();

			let sql = "DEFINE USER root ON ROOT PASSWORD 'root'";
			ds.execute(sql, &sess, None).await.unwrap();

			let sql = "USE NS N; DEFINE USER ns ON NS PASSWORD 'ns'";
			ds.execute(sql, &sess, None).await.unwrap();

			let sql = "USE NS N DB D; DEFINE USER db ON DB PASSWORD 'db'";
			ds.execute(sql, &sess, None).await.unwrap();
		}

		// Accept ROOT user
		{
			let res = verify_root_creds(&ds, "root", "root").await;
			res.unwrap();
		}

		// Accept NS user
		{
			let res = verify_ns_creds(&ds, &ns, "ns", "ns").await;
			res.unwrap();
		}

		// Accept DB user
		{
			let res = verify_db_creds(&ds, &ns, &db, "db", "db").await;
			res.unwrap();
		}
	}

	#[tokio::test]
	async fn test_expired_token() {
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			// Token was issued two hours ago and expired one hour ago
			iat: Some((Utc::now() - Duration::hours(2)).timestamp()),
			nbf: Some((Utc::now() - Duration::hours(2)).timestamp()),
			exp: Some((Utc::now() - Duration::hours(1)).timestamp()),
			ac: Some("token".to_string()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!("DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM HS512 KEY '{secret}' DURATION FOR SESSION 30d, FOR TOKEN 30d")
				.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		// Prepare the claims object
		let mut claims = claims.clone();
		claims.roles = None;
		// Create the token
		let enc = encode(&HEADER, &claims, &key).unwrap();
		// Signin with the token
		let mut sess = Session::default();
		let res = token(&ds, &mut sess, &enc).await;

		let e = res.unwrap_err();
		match e.downcast().expect("Unexpected error kind") {
			Error::ExpiredToken => {}
			e => panic!("Unexpected error, expected ExpiredToken found {e}"),
		}
	}

	#[tokio::test]
	async fn test_token_authenticate_clause() {
		#[derive(Debug)]
		struct TestCase {
			title: &'static str,
			iss_claim: Option<&'static str>,
			aud_claim: Option<Audience>,
			error_statement: &'static str,
			expected_error: Option<Error>,
		}

		let test_cases = vec![
			TestCase {
				title: "with correct 'iss' and 'aud' claims",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Single("surrealdb-test".to_string())),
				error_statement: "THROW",
				expected_error: None,
			},
			TestCase {
				title: "with correct 'iss' and 'aud' claims, multiple audiences",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Multiple(vec![
					"invalid".to_string(),
					"surrealdb-test".to_string(),
				])),
				error_statement: "THROW",
				expected_error: None,
			},
			TestCase {
				title: "with correct 'iss' claim but invalid 'aud' claim",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Single("invalid".to_string())),
				error_statement: "THROW",
				expected_error: Some(Error::Thrown("Invalid token audience string".to_string())),
			},
			TestCase {
				title: "with correct 'iss' claim but invalid 'aud' claim, multiple audiences",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Multiple(vec![
					"invalid".to_string(),
					"surrealdb-test-different".to_string(),
				])),
				error_statement: "THROW",
				expected_error: Some(Error::Thrown("Invalid token audience array".to_string())),
			},
			TestCase {
				title: "with correct 'iss' claim but invalid 'aud' claim, generic error",
				iss_claim: Some("surrealdb-test"),
				aud_claim: Some(Audience::Single("invalid".to_string())),
				error_statement: "RETURN",
				expected_error: Some(Error::InvalidAuth),
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

		let secret = "secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ac: Some("user".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");

		for level in &test_levels {
			for case in &test_cases {
				println!("Test case: {} level {}", level.level, case.title);

				ds.execute(
					format!(
						r#"
						REMOVE ACCESS IF EXISTS user ON {0};
						DEFINE ACCESS user ON {0} TYPE JWT
							ALGORITHM HS512 KEY '{1}'
							AUTHENTICATE {{
								IF $token.iss != "surrealdb-test" {{ {2} "Invalid token issuer" }};
								IF type::is::array($token.aud) {{
									IF "surrealdb-test" NOT IN $token.aud {{ {2} "Invalid token audience array" }}
								}} ELSE {{
									IF $token.aud IS NOT "surrealdb-test" {{ {2} "Invalid token audience string" }}
								}};
							}}
							DURATION FOR SESSION 2h
						;
					"#,
						level.level, secret, case.error_statement,
					)
					.as_str(),
					&sess,
					None,
				)
				.await
				.unwrap();

				// Prepare the claims object
				let mut claims = claims.clone();
				claims.ns = level.ns.map(|s| s.to_string());
				claims.db = level.db.map(|s| s.to_string());
				claims.iss = case.iss_claim.map(|s| s.to_string());
				claims.aud = case.aud_claim.clone();

				// Create the token
				let enc = encode(&HEADER, &claims, &key).unwrap();

				// Signin with the token
				let mut sess = Session::default();
				let res = token(&ds, &mut sess, &enc).await;

				if let Some(expected_err) = &case.expected_error {
					assert!(res.is_err(), "Unexpected success for case: {:?}", case);
					let err = res.unwrap_err().downcast().expect("Unexpected error type");
					match (expected_err, &err) {
						(Error::InvalidAuth, Error::InvalidAuth) => {}
						(Error::Thrown(expected_msg), Error::Thrown(msg))
							if expected_msg == msg => {}
						_ => panic!("Unexpected error for case: {:?}, got: {:?}", case, err),
					}
				} else {
					assert!(res.is_ok(), "Failed to sign in with token for case: {:?}", case);
					assert_eq!(sess.ns, level.ns.map(|s| s.to_string()));
					assert_eq!(sess.db, level.db.map(|s| s.to_string()));
					assert_eq!(sess.ac, Some("user".to_string()));
					assert_eq!(sess.au.id(), "user");

					// Check auth level
					assert_eq!(sess.au.level().ns(), level.ns);
					assert_eq!(sess.au.level().db(), level.db);
					match level.level {
						"ROOT" => assert!(sess.au.is_root()),
						"NS" => assert!(sess.au.is_ns()),
						"DB" => assert!(sess.au.is_db()),
						_ => panic!("Unsupported level"),
					}

					// Check roles
					assert!(
						sess.au.has_role(Role::Viewer),
						"Auth user expected to have Viewer role"
					);
					assert!(
						!sess.au.has_role(Role::Editor),
						"Auth user expected to not have Editor role"
					);
					assert!(
						!sess.au.has_role(Role::Owner),
						"Auth user expected to not have Owner role"
					);

					// Check expiration
					let exp = sess.exp.unwrap();
					let min_exp =
						(Utc::now() + Duration::hours(2) - Duration::seconds(10)).timestamp();
					let max_exp =
						(Utc::now() + Duration::hours(2) + Duration::seconds(10)).timestamp();
					assert!(
						exp > min_exp && exp < max_exp,
						"Session expiration is expected to match the defined duration in case: {:?}",
						case
					);
				}
			}
		}
	}

	#[tokio::test]
	async fn test_token_record_and_authenticate_clause() {
		// Test with an "id" claim
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				id: Some("user:1".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON DATABASE TYPE RECORD
 				        WITH JWT ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE (
							-- Simple example increasing the record identifier by one
							SELECT * FROM type::thing('user', record::id($auth) + 1)
    					)
    					DURATION FOR SESSION 2h
    				;

    				CREATE user:1, user:2;
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
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

		// Test without an "id" claim
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
				DEFINE ACCESS user ON DATABASE TYPE RECORD
					SIGNIN (
						SELECT * FROM type::thing('user', $id)
					)
					WITH JWT ALGORITHM HS512 KEY '{secret}'
					AUTHENTICATE (
					    SELECT id FROM user WHERE email = $token.email
					)
					DURATION FOR SESSION 2h
				;

				CREATE user:1 SET email = "info@surrealdb.com";
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			let now = Utc::now().timestamp();
			let later = (Utc::now() + Duration::hours(1)).timestamp();
			let claims_json = format!(
				r#"
				{{
					"iss": "surrealdb-test",
					"iat": {now},
					"nbf": {now},
					"exp": {later},
					"ns": "test",
					"db": "test",
					"ac": "user",
					"email": "info@surrealdb.com"
				}}
				"#
			);
			let claims = serde_json::from_str::<Claims>(&claims_json).unwrap();
			// Create the token
			let enc = match encode(&HEADER, &claims, &key) {
				Ok(enc) => enc,
				Err(err) => panic!("Failed to encode token: {:?}", err),
			};
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("user".to_string()));
			assert_eq!(sess.au.id(), "user:1");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert_eq!(sess.au.level().id(), Some("user:1"));
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
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				id: Some("user:1".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(r#"
    				DEFINE ACCESS user ON DATABASE TYPE RECORD
                        WITH JWT ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
    					    -- Not just signin, this clause runs across signin, signup and authenticate, which makes it a nice place to centralize logic
    					    IF !$auth.enabled {{
    							THROW "This user is not enabled";
    						}};

    						-- Always need to return the user id back, otherwise auth generically fails
    						RETURN $auth;
    					}}
    					DURATION FOR SESSION 2h
    				;

    				CREATE user:1 SET enabled = false;
				"#).as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::Thrown(e) => assert_eq!(e, "This user is not enabled"),
				e => panic!("Unexpected error, expected Thrown found {e:?}"),
			}
		}

		// Test AUTHENTICATE clause not returning a value
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				id: Some("user:test".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON DATABASE TYPE RECORD
    				    WITH JWT ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{}}
    					DURATION FOR SESSION 2h
    				;

    				CREATE user:1;
				"#
				)
				.as_str(),
				&sess,
				None,
			)
			.await
			.unwrap();

			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			let e = res.unwrap_err();
			match e.downcast().expect("Unexpected error kind") {
				Error::InvalidAuth => {}
				e => panic!("Unexpected error, expected InvalidAuth found {e}"),
			}
		}
	}
}
