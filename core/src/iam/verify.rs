use crate::cnf::INSECURE_FORWARD_ACCESS_ERRORS;
use crate::dbs::Session;
use crate::err::Error;
#[cfg(feature = "jwks")]
use crate::iam::jwks;
use crate::iam::{issue::expiration, token::Claims, Actor, Auth, Level, Role};
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::access_type::{AccessType, Jwt, JwtAccessVerify};
use crate::sql::{statements::DefineUserStatement, Algorithm, Thing, Value};
use crate::syn;
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use chrono::Utc;
use jsonwebtoken::{decode, DecodingKey, Validation};
use std::str::{self, FromStr};
use std::sync::Arc;
use std::sync::LazyLock;

fn config(alg: Algorithm, key: &[u8]) -> Result<(DecodingKey, Validation), Error> {
	let (dec, mut val) = match alg {
		Algorithm::Hs256 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS256))
		}
		Algorithm::Hs384 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS384))
		}
		Algorithm::Hs512 => {
			(DecodingKey::from_secret(key), Validation::new(jsonwebtoken::Algorithm::HS512))
		}
		Algorithm::EdDSA => {
			(DecodingKey::from_ed_pem(key)?, Validation::new(jsonwebtoken::Algorithm::EdDSA))
		}
		Algorithm::Es256 => {
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES256))
		}
		Algorithm::Es384 => {
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES384))
		}
		Algorithm::Es512 => {
			(DecodingKey::from_ec_pem(key)?, Validation::new(jsonwebtoken::Algorithm::ES384))
		}
		Algorithm::Ps256 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS256))
		}
		Algorithm::Ps384 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS384))
		}
		Algorithm::Ps512 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::PS512))
		}
		Algorithm::Rs256 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::RS256))
		}
		Algorithm::Rs384 => {
			(DecodingKey::from_rsa_pem(key)?, Validation::new(jsonwebtoken::Algorithm::RS384))
		}
		Algorithm::Rs512 => {
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
) -> Result<(), Error> {
	// Log the authentication type
	trace!("Attempting basic authentication");
	// Check if the parameters exist
	match (ns, db) {
		// DB signin
		(Some(ns), Some(db)) => match verify_db_creds(kvs, ns, db, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as database user '{}'", user);
				session.exp = expiration(u.duration.session)?;
				session.au = Arc::new((&u, Level::Database(ns.to_owned(), db.to_owned())).into());
				Ok(())
			}
			Err(err) => Err(err),
		},
		// NS signin
		(Some(ns), None) => match verify_ns_creds(kvs, ns, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as namespace user '{}'", user);
				session.exp = expiration(u.duration.session)?;
				session.au = Arc::new((&u, Level::Namespace(ns.to_owned())).into());
				Ok(())
			}
			Err(err) => Err(err),
		},
		// Root signin
		(None, None) => match verify_root_creds(kvs, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as root user '{}'", user);
				session.exp = expiration(u.duration.session)?;
				session.au = Arc::new((&u, Level::Root).into());
				Ok(())
			}
			Err(err) => Err(err),
		},
		(None, Some(_)) => Err(Error::InvalidAuth),
	}
}

pub async fn token(kvs: &Datastore, session: &mut Session, token: &str) -> Result<(), Error> {
	// Log the authentication type
	trace!("Attempting token authentication");
	// Decode the token without verifying
	let token_data = decode::<Claims>(token, &KEY, &DUD)?;
	// Convert the token to a SurrealQL object value
	let value = (&token_data.claims).into();
	// Check if the auth token can be used
	if let Some(nbf) = token_data.claims.nbf {
		if nbf > Utc::now().timestamp() {
			trace!("The 'nbf' field in the authentication token was invalid");
			return Err(Error::InvalidAuth);
		}
	}
	// Check if the auth token has expired
	if let Some(exp) = token_data.claims.exp {
		if exp < Utc::now().timestamp() {
			trace!("The 'exp' field in the authentication token was invalid");
			return Err(Error::InvalidAuth);
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
			// Parse the record id
			let mut rid = syn::thing(id)?;
			// Get the database access method
			let de = tx.get_db_access(ns, db, ac).await?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.kind {
				AccessType::Record(at) => match &at.jwt.verify {
					JwtAccessVerify::Key(key) => config(key.alg, key.key.as_bytes()),
					#[cfg(feature = "jwks")]
					JwtAccessVerify::Jwks(jwks) => {
						if let Some(kid) = token_data.header.kid {
							jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
						} else {
							Err(Error::MissingTokenHeader("kid".to_string()))
						}
					}
					#[cfg(not(feature = "jwks"))]
					_ => return Err(Error::AccessMethodMismatch),
				}?,
				_ => return Err(Error::AccessMethodMismatch),
			};
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for finding the signin record
				let mut sess = Session::editor().with_ns(ns).with_db(db);
				sess.rd = Some(rid.clone().into());
				sess.tk = Some((&token_data.claims).into());
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
			session.rd = Some(Value::from(rid.to_owned()));
			session.exp = expiration(de.duration.session)?;
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
			// Get the database access method
			let de = tx.get_db_access(ns, db, ac).await?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Obtain the configuration to verify the token based on the access method
			match &de.kind {
				// If the access type is Jwt or Bearer, this is database access
				AccessType::Jwt(_) | AccessType::Bearer(_) => {
					let cf = match &de.kind.jwt().verify {
						JwtAccessVerify::Key(key) => config(key.alg, key.key.as_bytes()),
						#[cfg(feature = "jwks")]
						JwtAccessVerify::Jwks(jwks) => {
							if let Some(kid) = token_data.header.kid {
								jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
							} else {
								Err(Error::MissingTokenHeader("kid".to_string()))
							}
						}
						#[cfg(not(feature = "jwks"))]
						_ => return Err(Error::AccessMethodMismatch),
					}?;
					// Verify the token
					decode::<Claims>(token, &cf.0, &cf.1)?;
					// AUTHENTICATE clause
					if let Some(au) = &de.authenticate {
						// Setup the system session for executing the clause
						let mut sess = Session::editor().with_ns(ns).with_db(db);
						sess.tk = Some((&token_data.claims).into());
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
							.map(|r| -> Result<Role, Error> {
								Role::from_str(r.as_str()).map_err(Error::IamError)
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
					session.exp = expiration(de.duration.session)?;
					session.au = Arc::new(Auth::new(Actor::new(
						de.name.to_string(),
						roles,
						Level::Database(ns.to_string(), db.to_string()),
					)));
				}
				// If the access type is Record, this is record access
				// Record access without an "id" claim is only possible if there is an AUTHENTICATE clause
				// The clause can make up for the missing "id" claim by resolving other claims to a specific record
				AccessType::Record(at) => match &de.authenticate {
					Some(au) => {
						trace!("Access method `{}` is record access with authenticate clause", ac);
						let cf = match &at.jwt.verify {
							JwtAccessVerify::Key(key) => config(key.alg, key.key.as_bytes()),
							#[cfg(feature = "jwks")]
							JwtAccessVerify::Jwks(jwks) => {
								if let Some(kid) = token_data.header.kid {
									jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
								} else {
									Err(Error::MissingTokenHeader("kid".to_string()))
								}
							}
							#[cfg(not(feature = "jwks"))]
							_ => return Err(Error::AccessMethodMismatch),
						}?;

						// Verify the token
						decode::<Claims>(token, &cf.0, &cf.1)?;
						// AUTHENTICATE clause
						// Setup the system session for finding the signin record
						let mut sess = Session::editor().with_ns(ns).with_db(db);
						sess.tk = Some((&token_data.claims).into());
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
						session.rd = Some(Value::from(rid.to_owned()));
						session.exp = expiration(de.duration.session)?;
						session.au = Arc::new(Auth::new(Actor::new(
							rid.to_string(),
							Default::default(),
							Level::Record(ns.to_string(), db.to_string(), rid.to_string()),
						)));
					}
					_ => return Err(Error::AccessMethodMismatch),
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
			// Get the database user
			let de = tx.get_db_user(ns, db, id).await.map_err(|e| {
				trace!("Error while authenticating to database `{db}`: {e}");
				Error::InvalidAuth
			})?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = config(Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to database `{}` with user `{}`", db, id);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.exp = expiration(de.duration.session)?;
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				de.roles.iter().map(|r| r.into()).collect(),
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
			// Get the namespace access method
			let de = tx.get_ns_access(ns, ac).await?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.kind {
				AccessType::Jwt(_) | AccessType::Bearer(_) => match &de.kind.jwt().verify {
					JwtAccessVerify::Key(key) => config(key.alg, key.key.as_bytes()),
					#[cfg(feature = "jwks")]
					JwtAccessVerify::Jwks(jwks) => {
						if let Some(kid) = token_data.header.kid {
							jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
						} else {
							Err(Error::MissingTokenHeader("kid".to_string()))
						}
					}
					#[cfg(not(feature = "jwks"))]
					_ => return Err(Error::AccessMethodMismatch),
				},
				_ => return Err(Error::AccessMethodMismatch),
			}?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for executing the clause
				let mut sess = Session::editor().with_ns(ns);
				sess.tk = Some((&token_data.claims).into());
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
					.map(|r| -> Result<Role, Error> {
						Role::from_str(r.as_str()).map_err(Error::IamError)
					})
					.collect::<Result<Vec<_>, _>>()?,
			};
			// Log the success
			trace!("Authenticated to namespace `{}` with access method `{}`", ns, ac);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.ac = Some(ac.to_owned());
			session.exp = expiration(de.duration.session)?;
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
			// Get the namespace user
			let de = tx.get_ns_user(ns, id).await.map_err(|e| {
				trace!("Error while authenticating to namespace `{ns}`: {e}");
				Error::InvalidAuth
			})?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = config(Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Log the success
			trace!("Authenticated to namespace `{}` with user `{}`", ns, id);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.exp = expiration(de.duration.session)?;
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				de.roles.iter().map(|r| r.into()).collect(),
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
			// Obtain the configuration to verify the token based on the access method
			let cf = match &de.kind {
				AccessType::Jwt(_) | AccessType::Bearer(_) => match &de.kind.jwt().verify {
					JwtAccessVerify::Key(key) => config(key.alg, key.key.as_bytes()),
					#[cfg(feature = "jwks")]
					JwtAccessVerify::Jwks(jwks) => {
						if let Some(kid) = token_data.header.kid {
							jwks::config(kvs, &kid, &jwks.url, token_data.header.alg).await
						} else {
							Err(Error::MissingTokenHeader("kid".to_string()))
						}
					}
					#[cfg(not(feature = "jwks"))]
					_ => return Err(Error::AccessMethodMismatch),
				},
				_ => return Err(Error::AccessMethodMismatch),
			}?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// AUTHENTICATE clause
			if let Some(au) = &de.authenticate {
				// Setup the system session for executing the clause
				let mut sess = Session::editor();
				sess.tk = Some((&token_data.claims).into());
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
					.map(|r| -> Result<Role, Error> {
						Role::from_str(r.as_str()).map_err(Error::IamError)
					})
					.collect::<Result<Vec<_>, _>>()?,
			};
			// Log the success
			trace!("Authenticated to root with access method `{}`", ac);
			// Set the session
			session.tk = Some(value);
			session.ac = Some(ac.to_owned());
			session.exp = expiration(de.duration.session)?;
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
			let de = tx.get_root_user(id).await.map_err(|e| {
				trace!("Error while authenticating to root: {e}");
				Error::InvalidAuth
			})?;
			// Ensure that the transaction is cancelled
			tx.cancel().await?;
			// Check the algorithm
			let cf = config(Algorithm::Hs512, de.code.as_bytes())?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Log the success
			trace!("Authenticated to root level with user `{}`", id);
			// Set the session
			session.tk = Some(value);
			session.exp = expiration(de.duration.session)?;
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				de.roles.iter().map(|r| r.into()).collect(),
				Level::Root,
			)));
			Ok(())
		}
		// There was an auth error
		_ => Err(Error::InvalidAuth),
	}
}

pub async fn verify_root_creds(
	ds: &Datastore,
	user: &str,
	pass: &str,
) -> Result<DefineUserStatement, Error> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read, Optimistic).await?;
	// Fetch the specified user from storage
	let user = tx.get_root_user(user).await.map_err(|e| {
		trace!("Error while authenticating to root: {e}");
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
) -> Result<DefineUserStatement, Error> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read, Optimistic).await?;
	// Fetch the specified user from storage
	let user = tx.get_ns_user(ns, user).await.map_err(|e| {
		trace!("Error while authenticating to namespace `{ns}`: {e}");
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

pub async fn verify_db_creds(
	ds: &Datastore,
	ns: &str,
	db: &str,
	user: &str,
	pass: &str,
) -> Result<DefineUserStatement, Error> {
	// Create a new readonly transaction
	let tx = ds.transaction(Read, Optimistic).await?;
	// Fetch the specified user from storage
	let user = tx.get_db_user(ns, db, user).await.map_err(|e| {
		trace!("Error while authenticating to database `{ns}/{db}`: {e}");
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

fn verify_pass(pass: &str, hash: &str) -> Result<(), Error> {
	// Compute the hash and verify the password
	let hash = PasswordHash::new(hash).unwrap();
	// Attempt to verify the password using Argon2
	match Argon2::default().verify_password(pass.as_ref(), &hash) {
		Ok(_) => Ok(()),
		_ => Err(Error::InvalidPass),
	}
}

// Execute the AUTHENTICATE clause for a Record access method
pub async fn authenticate_record(
	kvs: &Datastore,
	session: &Session,
	authenticate: &Value,
) -> Result<Thing, Error> {
	match kvs.evaluate(authenticate, session, None).await {
		Ok(val) => match val.record() {
			// If the AUTHENTICATE clause returns a record, authentication continues with that record
			Some(id) => Ok(id),
			// If the AUTHENTICATE clause returns anything else, authentication fails generically
			_ => Err(Error::InvalidAuth),
		},
		Err(e) => match e {
			// If the AUTHENTICATE clause throws a specific error, authentication fails with that error
			Error::Thrown(_) => Err(e),
			e if *INSECURE_FORWARD_ACCESS_ERRORS => Err(e),
			_ => Err(Error::InvalidAuth),
		},
	}
}

// Execute the AUTHENTICATE clause for any other access method
pub async fn authenticate_generic(
	kvs: &Datastore,
	session: &Session,
	authenticate: &Value,
) -> Result<(), Error> {
	match kvs.evaluate(authenticate, session, None).await {
		Ok(val) => {
			match val {
				// If the AUTHENTICATE clause returns nothing, authentication continues
				Value::None => Ok(()),
				// If the AUTHENTICATE clause returns anything else, authentication fails generically
				_ => Err(Error::InvalidAuth),
			}
		}
		Err(e) => match e {
			// If the AUTHENTICATE clause throws a specific error, authentication fails with that error
			Error::Thrown(_) => Err(e),
			e if *INSECURE_FORWARD_ACCESS_ERRORS => Err(e),
			_ => Err(Error::InvalidAuth),
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::iam::token::{Audience, HEADER};
	use argon2::password_hash::{PasswordHasher, SaltString};
	use chrono::Duration;
	use jsonwebtoken::{encode, EncodingKey};

	#[tokio::test]
	async fn test_basic_root() {
		//
		// Test without roles or expiration defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON ROOT PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "pass", None, None).await;

			assert!(res.is_ok(), "Failed to signin with ROOT user: {:?}", res);
			assert_eq!(sess.ns, None);
			assert_eq!(sess.db, None);
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_root());
			assert_eq!(sess.au.level().ns(), None);
			assert_eq!(sess.au.level().db(), None);
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			assert_eq!(sess.exp, None, "Default system user expiration is expected to be None");
		}

		//
		// Test with roles and expiration defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				"DEFINE USER user ON ROOT PASSWORD 'pass' ROLES EDITOR, OWNER DURATION FOR SESSION 1d",
				&sess,
				None,
			)
			.await
			.unwrap();

			let mut sess = Session {
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "pass", None, None).await;

			assert!(res.is_ok(), "Failed to signin with ROOT user: {:?}", res);
			assert_eq!(sess.ns, None);
			assert_eq!(sess.db, None);
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_root());
			assert_eq!(sess.au.level().ns(), None);
			assert_eq!(sess.au.level().db(), None);
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(1) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(1) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		// Test invalid password
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON ROOT PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "invalid", None, None).await;

			assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_basic_ns() {
		//
		// Test without roles or expiration defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON NS PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				ns: Some("test".to_string()),
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "pass", Some("test"), None).await;

			assert!(res.is_ok(), "Failed to signin with ROOT user: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, None);
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_ns());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), None);
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			assert_eq!(sess.exp, None, "Default system user expiration is expected to be None");
		}

		//
		// Test with roles and expiration defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				"DEFINE USER user ON NS PASSWORD 'pass' ROLES EDITOR, OWNER DURATION FOR SESSION 1d",
				&sess,
				None,
			)
			.await
			.unwrap();

			let mut sess = Session {
				ns: Some("test".to_string()),
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "pass", Some("test"), None).await;

			assert!(res.is_ok(), "Failed to signin with ROOT user: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, None);
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_ns());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), None);
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			// Expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(1) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(1) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		// Test invalid password
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON NS PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "invalid", Some("test"), None).await;

			assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_basic_db() {
		//
		// Test without roles or expiration defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON DB PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "pass", Some("test"), Some("test")).await;

			assert!(res.is_ok(), "Failed to signin with ROOT user: {:?}", res);
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
		// Test with roles and expiration defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				"DEFINE USER user ON DB PASSWORD 'pass' ROLES EDITOR, OWNER DURATION FOR SESSION 1d",
				&sess,
				None,
			)
			.await
			.unwrap();

			let mut sess = Session {
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "pass", Some("test"), Some("test")).await;

			assert!(res.is_ok(), "Failed to signin with ROOT user: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "user");
			assert!(sess.au.is_db());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			// Expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(1) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(1) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		// Test invalid password
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON DB PASSWORD 'pass'", &sess, None).await.unwrap();

			let mut sess = Session {
				..Default::default()
			};
			let res = basic(&ds, &mut sess, "user", "invalid", Some("test"), Some("test")).await;

			assert!(res.is_err(), "Unexpected successful signin: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_token_root() {
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
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
		ds.execute(
			format!("DEFINE ACCESS token ON ROOT TYPE JWT ALGORITHM HS512 KEY '{secret}' DURATION FOR SESSION 30d").as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		//
		// Test without roles defined
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = None;
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, None);
			assert_eq!(sess.db, None);
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_root());
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		//
		// Test with roles defined
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = Some(vec!["editor".to_string(), "owner".to_string()]);
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, None);
			assert_eq!(sess.db, None);
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_root());
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		//
		// Test with invalid signature
		//
		{
			// Prepare the claims object
			let claims = claims.clone();
			// Create the token
			let key = EncodingKey::from_secret("invalid".as_ref());
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_token_ns() {
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ac: Some("token".to_string()),
			ns: Some("test".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!("DEFINE ACCESS token ON NS TYPE JWT ALGORITHM HS512 KEY '{secret}' DURATION FOR SESSION 30d").as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		//
		// Test without roles defined
		//
		{
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
			assert_eq!(sess.db, None);
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_ns());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		//
		// Test with roles defined
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = Some(vec!["editor".to_string(), "owner".to_string()]);
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, None);
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_ns());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		//
		// Test with invalid signature
		//
		{
			// Prepare the claims object
			let claims = claims.clone();
			// Create the token
			let key = EncodingKey::from_secret("invalid".as_ref());
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}

		//
		// Test with valid token invalid ns
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.ns = Some("invalid".to_string());
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_token_db() {
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ac: Some("token".to_string()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!("DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM HS512 KEY '{secret}' DURATION FOR SESSION 30d")
				.as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		//
		// Test without roles defined
		//
		{
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
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_db());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		//
		// Test with roles defined
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = Some(vec!["editor".to_string(), "owner".to_string()]);
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_db());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(sess.au.has_role(&Role::Editor), "Auth user expected to have Editor role");
			assert!(sess.au.has_role(&Role::Owner), "Auth user expected to have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		//
		// Test with invalid signature
		//
		{
			// Prepare the claims object
			let claims = claims.clone();
			// Create the token
			let key = EncodingKey::from_secret("invalid".as_ref());
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}

		//
		// Test with valid token invalid db
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.db = Some("invalid".to_string());
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}
	}

	#[tokio::test]
	async fn test_token_db_record() {
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			ac: Some("token".to_string()),
			id: Some("user:test".to_string()),
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

		//
		// Test without roles defined
		// Roles should be ignored in record access
		//
		{
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
			assert_eq!(sess.ac, Some("token".to_string()));
			assert_eq!(sess.au.id(), "user:test");
			assert!(sess.au.is_record());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		//
		// Test with roles defined
		// Roles should be ignored in record access
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.roles = Some(vec!["editor".to_string(), "owner".to_string()]);
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
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
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
			// Session expiration has been set explicitly
			let exp = sess.exp.unwrap();
			// Expiration should match the current time plus session duration with some margin
			let min_exp = (Utc::now() + Duration::days(30) - Duration::seconds(10)).timestamp();
			let max_exp = (Utc::now() + Duration::days(30) + Duration::seconds(10)).timestamp();
			assert!(
				exp > min_exp && exp < max_exp,
				"Session expiration is expected to match the defined duration"
			);
		}

		//
		// Test with invalid signature
		//
		{
			// Prepare the claims object
			let claims = claims.clone();
			// Create the token
			let key = EncodingKey::from_secret("invalid".as_ref());
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}

		//
		// Test with valid token invalid access method
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.ac = Some("invalid".to_string());
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}

		//
		// Test with invalid id
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.id = Some("##_INVALID_##".to_string());
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_err(), "Unexpected success signing in with token: {:?}", res);
		}

		//
		// Test with generic user identifier
		//
		{
			let resource_id = "user:2k9qnabxuxh8k4d5gfto".to_string();
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.id = Some(resource_id.clone());
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("token".to_string()));
			assert_eq!(sess.au.id(), resource_id);
			assert!(sess.au.is_record());
			let user_id = syn::thing(&resource_id).unwrap();
			assert_eq!(sess.rd, Some(Value::from(user_id)));
		}

		//
		// Test with custom user numeric identifiers of varying sizes
		//
		{
			let ids = ["1", "2", "100", "10000000"];
			for id in ids.iter() {
				let resource_id = format!("user:{id}");
				// Prepare the claims object
				let mut claims = claims.clone();
				claims.id = Some(resource_id.clone());
				// Create the token
				let enc = encode(&HEADER, &claims, &key).unwrap();
				// Signin with the token
				let mut sess = Session::default();
				let res = token(&ds, &mut sess, &enc).await;

				assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
				assert_eq!(sess.ns, Some("test".to_string()));
				assert_eq!(sess.db, Some("test".to_string()));
				assert_eq!(sess.ac, Some("token".to_string()));
				assert_eq!(sess.au.id(), resource_id);
				assert!(sess.au.is_record());
				let user_id = syn::thing(&resource_id).unwrap();
				assert_eq!(sess.rd, Some(Value::from(user_id)));
			}
		}

		//
		// Test with custom user string identifiers of varying lengths
		//
		{
			let ids = ["username", "username1", "username10", "username100"];
			for id in ids.iter() {
				let resource_id = format!("user:{id}");
				// Prepare the claims object
				let mut claims = claims.clone();
				claims.id = Some(resource_id.clone());
				// Create the token
				let enc = encode(&HEADER, &claims, &key).unwrap();
				// Signin with the token
				let mut sess = Session::default();
				let res = token(&ds, &mut sess, &enc).await;

				assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
				assert_eq!(sess.ns, Some("test".to_string()));
				assert_eq!(sess.db, Some("test".to_string()));
				assert_eq!(sess.ac, Some("token".to_string()));
				assert_eq!(sess.au.id(), resource_id);
				assert!(sess.au.is_record());
				let user_id = syn::thing(&resource_id).unwrap();
				assert_eq!(sess.rd, Some(Value::from(user_id)));
			}
		}

		//
		// Test with custom user string identifiers of varying lengths with special characters
		//
		{
			let ids = ["user.name", "user.name1", "user.name10", "user.name100"];
			for id in ids.iter() {
				// Enclose special characters in "⟨brackets⟩"
				let resource_id = format!("user:⟨{id}⟩");
				// Prepare the claims object
				let mut claims = claims.clone();
				claims.id = Some(resource_id.clone());
				// Create the token
				let enc = encode(&HEADER, &claims, &key).unwrap();
				// Signin with the token
				let mut sess = Session::default();
				let res = token(&ds, &mut sess, &enc).await;

				assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
				assert_eq!(sess.ns, Some("test".to_string()));
				assert_eq!(sess.db, Some("test".to_string()));
				assert_eq!(sess.ac, Some("token".to_string()));
				assert_eq!(sess.au.id(), resource_id);
				assert!(sess.au.is_record());
				let user_id = syn::thing(&resource_id).unwrap();
				assert_eq!(sess.rd, Some(Value::from(user_id)));
			}
		}

		//
		// Test with custom UUID user identifier
		//
		{
			let id = "83149446-95f5-4c0d-9f42-136e7b272456";
			// Enclose special characters in "⟨brackets⟩"
			let resource_id = format!("user:⟨{id}⟩");
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.id = Some(resource_id.clone());
			// Create the token
			let enc = encode(&HEADER, &claims, &key).unwrap();
			// Signin with the token
			let mut sess = Session::default();
			let res = token(&ds, &mut sess, &enc).await;

			assert!(res.is_ok(), "Failed to signin with token: {:?}", res);
			assert_eq!(sess.ns, Some("test".to_string()));
			assert_eq!(sess.db, Some("test".to_string()));
			assert_eq!(sess.ac, Some("token".to_string()));
			assert_eq!(sess.au.id(), resource_id);
			assert!(sess.au.is_record());
			let user_id = syn::thing(&resource_id).unwrap();
			assert_eq!(sess.rd, Some(Value::from(user_id)));
		}
	}

	#[tokio::test]
	async fn test_token_db_record_custom_claims() {
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
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
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
	async fn test_token_db_record_jwks() {
		use crate::dbs::capabilities::{Capabilities, NetTarget, Targets};
		use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
		use jsonwebtoken::jwk::{Jwk, JwkSet};
		use rand::{distributions::Alphanumeric, Rng};
		use wiremock::matchers::{method, path};
		use wiremock::{Mock, MockServer, ResponseTemplate};

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
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
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
		assert!(verify_pass("test", &hash).is_ok());

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
			assert!(res.is_ok());
		}

		// Accept NS user
		{
			let res = verify_ns_creds(&ds, &ns, "ns", "ns").await;
			assert!(res.is_ok());
		}

		// Accept DB user
		{
			let res = verify_db_creds(&ds, &ns, &db, "db", "db").await;
			assert!(res.is_ok());
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

		assert!(res.is_err(), "Unexpected success signing in with expired token: {:?}", res);
	}

	#[tokio::test]
	async fn test_token_db_record_and_authenticate_clause() {
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
	async fn test_token_db_authenticate_clause() {
		// Test with correct "iss" and "aud" claims
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("surrealdb-test".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON DATABASE TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
					;
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
			assert!(sess.au.is_db());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			// Record users should not have roles
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
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

		// Test with correct "iss" and "aud" claims, with multiple audiences
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Multiple(vec![
					"invalid".to_string(),
					"surrealdb-test".to_string(),
				])),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON DATABASE TYPE JWT
				        ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
    				;
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
			assert!(sess.au.is_db());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			// Record users should not have roles
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
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

		// Test with correct "iss" claim but incorrect "aud" claim
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("invalid".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON DATABASE TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
    					DURATION FOR SESSION 2h
					;
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

			match res {
				Err(Error::Thrown(e)) if e == "Invalid token audience string" => {} // ok
				res => panic!(
				    "Expected authentication to failed due to invalid token audience, but instead received: {:?}",
					res
				),
			}
		}

		// Test with correct "iss" claim but incorrect "aud" claim, with multiple audiences
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Multiple(vec![
					"surrealdb-test-different".to_string(),
					"invalid".to_string(),
				])),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON DATABASE TYPE JWT
				        ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
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

			match res {
				Err(Error::Thrown(e)) if e == "Invalid token audience array" => {} // ok
				res => panic!(
				    "Expected authentication to failed due to invalid token audience array, but instead received: {:?}",
					res
				),
			}
		}

		// Test with correct "iss" claim but incorrect "aud" claim
		// In this case, something is returned by the clause, which returns a generic error
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("invalid".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				db: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON DATABASE TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ RETURN "FAIL" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ RETURN "FAIL" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ RETURN "FAIL" }}
							}};
						}}
    					DURATION FOR SESSION 2h
    				;
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

			match res {
				Err(Error::InvalidAuth) => {} // ok
				res => panic!(
					"Expected a generic authentication error, but instead received: {:?}",
					res
				),
			}
		}
	}

	#[tokio::test]
	async fn test_token_ns_authenticate_clause() {
		// Test with correct "iss" and "aud" claims
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("surrealdb-test".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON NAMESPACE TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
					;
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
			assert_eq!(sess.ac, Some("user".to_string()));
			assert!(sess.au.is_ns());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), None);
			// Record users should not have roles
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
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

		// Test with correct "iss" and "aud" claims, with multiple audiences
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Multiple(vec![
					"invalid".to_string(),
					"surrealdb-test".to_string(),
				])),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test");
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON NAMESPACE TYPE JWT
				        ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
    				;
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
			assert_eq!(sess.db, None);
			assert_eq!(sess.ac, Some("user".to_string()));
			assert!(sess.au.is_ns());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), None);
			// Record users should not have roles
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
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

		// Test with correct "iss" claim but incorrect "aud" claim
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("invalid".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON NAMESPACE TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
    				;
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

			match res {
				Err(Error::Thrown(e)) if e == "Invalid token audience string" => {} // ok
				res => panic!(
				    "Expected authentication to failed due to invalid token audience, but instead received: {:?}",
					res
				),
			}
		}

		// Test with correct "iss" claim but incorrect "aud" claim, with multiple audiences
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Multiple(vec![
					"surrealdb-test-different".to_string(),
					"invalid".to_string(),
				])),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test");
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON NAMESPACE TYPE JWT
				        ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
					;
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

			match res {
				Err(Error::Thrown(e)) if e == "Invalid token audience array" => {} // ok
				res => panic!(
				    "Expected authentication to failed due to invalid token audience array, but instead received: {:?}",
					res
				),
			}
		}

		// Test with correct "iss" claim but incorrect "aud" claim
		// In this case, something is returned by the clause, which returns a generic error
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("invalid".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ns: Some("test".to_string()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON NAMESPACE TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ RETURN "FAIL" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ RETURN "FAIL" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ RETURN "FAIL" }}
							}};
						}}
    					DURATION FOR SESSION 2h
    				;
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

			match res {
				Err(Error::InvalidAuth) => {} // ok
				res => panic!(
					"Expected a generic authentication error, but instead received: {:?}",
					res
				),
			}
		}
	}

	#[tokio::test]
	async fn test_token_root_authenticate_clause() {
		// Test with correct "iss" and "aud" claims
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("surrealdb-test".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON ROOT TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
    					DURATION FOR SESSION 2h
    				;
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
			assert_eq!(sess.ac, Some("user".to_string()));
			assert!(sess.au.is_root());
			assert_eq!(sess.au.level().ns(), None);
			assert_eq!(sess.au.level().db(), None);
			// Record users should not have roles
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
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

		// Test with correct "iss" and "aud" claims, with multiple audiences
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Multiple(vec![
					"invalid".to_string(),
					"surrealdb-test".to_string(),
				])),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner();
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON ROOT TYPE JWT
				        ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
					;
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
			assert_eq!(sess.ns, None);
			assert_eq!(sess.db, None);
			assert_eq!(sess.ac, Some("user".to_string()));
			assert!(sess.au.is_root());
			assert_eq!(sess.au.level().ns(), None);
			assert_eq!(sess.au.level().db(), None);
			// Record users should not have roles
			assert!(sess.au.has_role(&Role::Viewer), "Auth user expected to have Viewer role");
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

		// Test with correct "iss" claim but incorrect "aud" claim
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("invalid".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON ROOT TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
    					DURATION FOR SESSION 2h
    				;
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

			match res {
				Err(Error::Thrown(e)) if e == "Invalid token audience string" => {} // ok
				res => panic!(
				    "Expected authentication to failed due to invalid token audience, but instead received: {:?}",
					res
				),
			}
		}

		// Test with correct "iss" claim but incorrect "aud" claim, with multiple audiences
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Multiple(vec![
					"surrealdb-test-different".to_string(),
					"invalid".to_string(),
				])),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner();
			ds.execute(
				format!(
					r#"
					DEFINE ACCESS user ON ROOT TYPE JWT
				        ALGORITHM HS512 KEY '{secret}'
						AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ THROW "Invalid token issuer" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ THROW "Invalid token audience array" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ THROW "Invalid token audience string" }}
							}};
						}}
						DURATION FOR SESSION 2h
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

			match res {
				Err(Error::Thrown(e)) if e == "Invalid token audience array" => {} // ok
				res => panic!(
				    "Expected authentication to failed due to invalid token audience array, but instead received: {:?}",
					res
				),
			}
		}

		// Test with correct "iss" claim but incorrect "aud" claim
		// In this case, something is returned by the clause, which returns a generic error
		{
			let secret = "jwt_secret";
			let key = EncodingKey::from_secret(secret.as_ref());
			let claims = Claims {
				iss: Some("surrealdb-test".to_string()),
				aud: Some(Audience::Single("invalid".to_string())),
				iat: Some(Utc::now().timestamp()),
				nbf: Some(Utc::now().timestamp()),
				exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
				ac: Some("user".to_string()),
				..Claims::default()
			};

			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute(
				format!(
					r#"
    				DEFINE ACCESS user ON ROOT TYPE JWT
 				        ALGORITHM HS512 KEY '{secret}'
    					AUTHENTICATE {{
							IF $token.iss != "surrealdb-test" {{ RETURN "FAIL" }};
							IF type::is::array($token.aud) {{
								IF "surrealdb-test" NOT IN $token.aud {{ RETURN "FAIL" }}
							}} ELSE {{
								IF $token.aud IS NOT "surrealdb-test" {{ RETURN "FAIL" }}
							}};
						}}
    					DURATION FOR SESSION 2h
    				;
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
