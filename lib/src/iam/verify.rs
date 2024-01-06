use crate::dbs::Session;
use crate::err::Error;
use crate::iam::{token::Claims, Actor, Auth, Level, Role};
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::{statements::DefineUserStatement, Algorithm, Value};
use crate::syn;
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use chrono::Utc;
use jsonwebtoken::{decode, DecodingKey, Validation};
use once_cell::sync::Lazy;
use std::str::{self, FromStr};
use std::sync::Arc;

fn config(algo: Algorithm, code: String) -> Result<(DecodingKey, Validation), Error> {
	match algo {
		Algorithm::Hs256 => Ok((
			DecodingKey::from_secret(code.as_ref()),
			Validation::new(jsonwebtoken::Algorithm::HS256),
		)),
		Algorithm::Hs384 => Ok((
			DecodingKey::from_secret(code.as_ref()),
			Validation::new(jsonwebtoken::Algorithm::HS384),
		)),
		Algorithm::Hs512 => Ok((
			DecodingKey::from_secret(code.as_ref()),
			Validation::new(jsonwebtoken::Algorithm::HS512),
		)),
		Algorithm::EdDSA => Ok((
			DecodingKey::from_ed_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::EdDSA),
		)),
		Algorithm::Es256 => Ok((
			DecodingKey::from_ec_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::ES256),
		)),
		Algorithm::Es384 => Ok((
			DecodingKey::from_ec_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::ES384),
		)),
		Algorithm::Es512 => Ok((
			DecodingKey::from_ec_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::ES384),
		)),
		Algorithm::Ps256 => Ok((
			DecodingKey::from_rsa_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::PS256),
		)),
		Algorithm::Ps384 => Ok((
			DecodingKey::from_rsa_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::PS384),
		)),
		Algorithm::Ps512 => Ok((
			DecodingKey::from_rsa_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::PS512),
		)),
		Algorithm::Rs256 => Ok((
			DecodingKey::from_rsa_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::RS256),
		)),
		Algorithm::Rs384 => Ok((
			DecodingKey::from_rsa_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::RS384),
		)),
		Algorithm::Rs512 => Ok((
			DecodingKey::from_rsa_pem(code.as_ref())?,
			Validation::new(jsonwebtoken::Algorithm::RS512),
		)),
	}
}

static KEY: Lazy<DecodingKey> = Lazy::new(|| DecodingKey::from_secret(&[]));

static DUD: Lazy<Validation> = Lazy::new(|| {
	let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
	validation.insecure_disable_signature_validation();
	validation.validate_nbf = false;
	validation.validate_exp = false;
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
				session.au = Arc::new((&u, Level::Database(ns.to_owned(), db.to_owned())).into());
				Ok(())
			}
			Err(err) => Err(err),
		},
		// NS signin
		(Some(ns), None) => match verify_ns_creds(kvs, ns, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as namespace user '{}'", user);
				session.au = Arc::new((&u, Level::Namespace(ns.to_owned())).into());
				Ok(())
			}
			Err(err) => Err(err),
		},
		// Root signin
		(None, None) => match verify_root_creds(kvs, user, pass).await {
			Ok(u) => {
				debug!("Authenticated as root user '{}'", user);
				session.au = Arc::new((&u, Level::Root).into());
				Ok(())
			}
			Err(err) => Err(err),
		},
		(None, Some(_)) => Err(Error::InvalidAuth),
	}
}

// TODO(gguillemas): Remove this method once the legacy authentication is deprecated in v2.0.0
pub async fn basic_legacy(
	kvs: &Datastore,
	session: &mut Session,
	user: &str,
	pass: &str,
) -> Result<(), Error> {
	// Log the authentication type
	trace!("Attempting legacy basic authentication");

	match verify_creds_legacy(kvs, session.ns.as_ref(), session.db.as_ref(), user, pass).await {
		Ok((au, _)) if au.is_root() => {
			debug!("Authenticated as root user '{}'", user);
			session.au = Arc::new(au);
			Ok(())
		}
		Ok((au, _)) if au.is_ns() => {
			debug!("Authenticated as namespace user '{}'", user);
			session.au = Arc::new(au);
			Ok(())
		}
		Ok((au, _)) if au.is_db() => {
			debug!("Authenticated as database user '{}'", user);
			session.au = Arc::new(au);
			Ok(())
		}
		Ok(_) => Err(Error::InvalidAuth),
		Err(e) => Err(e),
	}
}

pub async fn token(kvs: &Datastore, session: &mut Session, token: &str) -> Result<(), Error> {
	// Log the authentication type
	trace!("Attempting token authentication");
	// Decode the token without verifying
	let token_data = decode::<Claims>(token, &KEY, &DUD)?;
	// Convert the token to a SurrealQL object value
	let value = token_data.claims.clone().into();
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
	match token_data.claims {
		// Check if this is scope token authentication
		Claims {
			ns: Some(ns),
			db: Some(db),
			sc: Some(sc),
			tk: Some(tk),
			id,
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to scope `{}` with token `{}`", sc, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(Read, Optimistic).await?;
			// Parse the record id
			let id = match id {
				Some(id) => syn::thing(&id)?.into(),
				None => Value::None,
			};
			// Get the scope token
			let de = tx.get_sc_token(&ns, &db, &sc, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to scope `{}` with token `{}`", sc, tk);
			// Set the session
			session.sd = Some(id);
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.sc = Some(sc.to_owned());
			session.au = Arc::new(Auth::new(Actor::new(
				de.name.to_string(),
				Default::default(),
				Level::Scope(ns, db, sc),
			)));
			Ok(())
		}
		// Check if this is scope authentication
		Claims {
			ns: Some(ns),
			db: Some(db),
			sc: Some(sc),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to scope `{}`", sc);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(Read, Optimistic).await?;
			// Parse the record id
			let id = syn::thing(&id)?;
			// Get the scope
			let de = tx.get_sc(&ns, &db, &sc).await?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to scope `{}`", sc);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.sc = Some(sc.to_owned());
			session.sd = Some(Value::from(id.to_owned()));
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				Default::default(),
				Level::Scope(ns, db, sc),
			)));
			Ok(())
		}
		// Check if this is database token authentication
		Claims {
			ns: Some(ns),
			db: Some(db),
			tk: Some(tk),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to database `{}` with token `{}`", db, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(Read, Optimistic).await?;
			// Get the database token
			let de = tx.get_db_token(&ns, &db, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Parse the roles
			let roles = match token_data.claims.roles {
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
			debug!("Authenticated to database `{}` with token `{}`", db, tk);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.au = Arc::new(Auth::new(Actor::new(
				de.name.to_string(),
				roles,
				Level::Database(ns, db),
			)));
			Ok(())
		}
		// Check if this is database authentication
		Claims {
			ns: Some(ns),
			db: Some(db),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to database `{}` with user `{}`", db, id);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(Read, Optimistic).await?;
			// Get the database user
			let de = tx.get_db_user(&ns, &db, &id).await.map_err(|e| {
				trace!("Error while authenticating to database `{db}`: {e}");
				Error::InvalidAuth
			})?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to database `{}` with user `{}`", db, id);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				de.roles.iter().map(|r| r.into()).collect(),
				Level::Database(ns, db),
			)));
			Ok(())
		}
		// Check if this is namespace token authentication
		Claims {
			ns: Some(ns),
			tk: Some(tk),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to namespace `{}` with token `{}`", ns, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(Read, Optimistic).await?;
			// Get the namespace token
			let de = tx.get_ns_token(&ns, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Parse the roles
			let roles = match token_data.claims.roles {
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
			trace!("Authenticated to namespace `{}` with token `{}`", ns, tk);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.au =
				Arc::new(Auth::new(Actor::new(de.name.to_string(), roles, Level::Namespace(ns))));
			Ok(())
		}
		// Check if this is namespace authentication
		Claims {
			ns: Some(ns),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to namespace `{}` with user `{}`", ns, id);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(Read, Optimistic).await?;
			// Get the namespace user
			let de = tx.get_ns_user(&ns, &id).await.map_err(|e| {
				trace!("Error while authenticating to namespace `{ns}`: {e}");
				Error::InvalidAuth
			})?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Log the success
			trace!("Authenticated to namespace `{}` with user `{}`", ns, id);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.au = Arc::new(Auth::new(Actor::new(
				id.to_string(),
				de.roles.iter().map(|r| r.into()).collect(),
				Level::Namespace(ns),
			)));
			Ok(())
		}
		// Check if this is root level authentication
		Claims {
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!("Authenticating to root level with user `{}`", id);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(Read, Optimistic).await?;
			// Get the namespace user
			let de = tx.get_root_user(&id).await.map_err(|e| {
				trace!("Error while authenticating to root: {e}");
				Error::InvalidAuth
			})?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(token, &cf.0, &cf.1)?;
			// Log the success
			trace!("Authenticated to root level with user `{}`", id);
			// Set the session
			session.tk = Some(value);
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
	let mut tx = ds.transaction(Read, Optimistic).await?;
	// Fetch the specified user from storage
	let user = tx.get_root_user(user).await.map_err(|e| {
		trace!("Error while authenticating to root: {e}");
		Error::InvalidAuth
	})?;
	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
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
	let mut tx = ds.transaction(Read, Optimistic).await?;
	// Fetch the specified user from storage
	let user = tx.get_ns_user(ns, user).await.map_err(|e| {
		trace!("Error while authenticating to namespace `{ns}`: {e}");
		Error::InvalidAuth
	})?;
	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
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
	let mut tx = ds.transaction(Read, Optimistic).await?;
	// Fetch the specified user from storage
	let user = tx.get_db_user(ns, db, user).await.map_err(|e| {
		trace!("Error while authenticating to database `{ns}/{db}`: {e}");
		Error::InvalidAuth
	})?;
	// Verify the specified password for the user
	verify_pass(pass, user.hash.as_ref())?;
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

// TODO(gguillemas): Remove this method once the legacy authentication is deprecated in v2.0.0
pub async fn verify_creds_legacy(
	ds: &Datastore,
	ns: Option<&String>,
	db: Option<&String>,
	user: &str,
	pass: &str,
) -> Result<(Auth, DefineUserStatement), Error> {
	if user.is_empty() || pass.is_empty() {
		return Err(Error::InvalidAuth);
	}

	// Try to authenticate as a ROOT user
	match verify_root_creds(ds, user, pass).await {
		Ok(u) => Ok(((&u, Level::Root).into(), u)),
		Err(_) => {
			// Try to authenticate as a NS user
			match ns {
				Some(ns) => {
					match verify_ns_creds(ds, ns, user, pass).await {
						Ok(u) => Ok(((&u, Level::Namespace(ns.to_owned())).into(), u)),
						Err(_) => {
							// Try to authenticate as a DB user
							match db {
								Some(db) => match verify_db_creds(ds, ns, db, user, pass).await {
									Ok(u) => Ok((
										(&u, Level::Database(ns.to_owned(), db.to_owned())).into(),
										u,
									)),
									Err(_) => Err(Error::InvalidAuth),
								},
								None => Err(Error::InvalidAuth),
							}
						}
					}
				}
				None => Err(Error::InvalidAuth),
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{iam::token::HEADER, kvs::Datastore};
	use argon2::password_hash::{PasswordHasher, SaltString};
	use chrono::Duration;
	use jsonwebtoken::{encode, EncodingKey};

	#[tokio::test]
	async fn test_basic_root() {
		//
		// Test without roles defined
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
		}

		//
		// Test with roles defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON ROOT PASSWORD 'pass' ROLES EDITOR, OWNER", &sess, None)
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
		// Test without roles defined
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
		}

		//
		// Test with roles defined
		//
		{
			let ds = Datastore::new("memory").await.unwrap();
			let sess = Session::owner().with_ns("test").with_db("test");
			ds.execute("DEFINE USER user ON NS PASSWORD 'pass' ROLES EDITOR, OWNER", &sess, None)
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
		// Test without roles defined
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
	async fn test_token_ns() {
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			tk: Some("token".to_string()),
			ns: Some("test".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!("DEFINE TOKEN token ON NS TYPE HS512 VALUE '{secret}'").as_str(),
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
			tk: Some("token".to_string()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!("DEFINE TOKEN token ON DB TYPE HS512 VALUE '{secret}'").as_str(),
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
	async fn test_token_scope() {
		let secret = "jwt_secret";
		let key = EncodingKey::from_secret(secret.as_ref());
		let claims = Claims {
			iss: Some("surrealdb-test".to_string()),
			iat: Some(Utc::now().timestamp()),
			nbf: Some(Utc::now().timestamp()),
			exp: Some((Utc::now() + Duration::hours(1)).timestamp()),
			tk: Some("token".to_string()),
			ns: Some("test".to_string()),
			db: Some("test".to_string()),
			sc: Some("test".to_string()),
			..Claims::default()
		};

		let ds = Datastore::new("memory").await.unwrap();
		let sess = Session::owner().with_ns("test").with_db("test");
		ds.execute(
			format!("DEFINE TOKEN token ON SCOPE test TYPE HS512 VALUE '{secret}';").as_str(),
			&sess,
			None,
		)
		.await
		.unwrap();

		//
		// Test without roles defined
		// Roles should be ignored in scope authentication
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
			assert_eq!(sess.sc, Some("test".to_string()));
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_scope());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
		}

		//
		// Test with roles defined
		// Roles should be ignored in scope authentication
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
			assert_eq!(sess.sc, Some("test".to_string()));
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_scope());
			assert_eq!(sess.au.level().ns(), Some("test"));
			assert_eq!(sess.au.level().db(), Some("test"));
			assert!(!sess.au.has_role(&Role::Viewer), "Auth user expected to not have Viewer role");
			assert!(!sess.au.has_role(&Role::Editor), "Auth user expected to not have Editor role");
			assert!(!sess.au.has_role(&Role::Owner), "Auth user expected to not have Owner role");
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
		// Test with valid token invalid sc
		//
		{
			// Prepare the claims object
			let mut claims = claims.clone();
			claims.sc = Some("invalid".to_string());
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
			assert_eq!(sess.sc, Some("test".to_string()));
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_scope());
			let user_id = syn::thing(&resource_id).unwrap();
			assert_eq!(sess.sd, Some(Value::from(user_id)));
		}

		//
		// Test with custom user numeric identifiers of varying sizes
		//
		{
			let ids = vec!["1", "2", "100", "10000000"];
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
				assert_eq!(sess.sc, Some("test".to_string()));
				assert_eq!(sess.au.id(), "token");
				assert!(sess.au.is_scope());
				let user_id = syn::thing(&resource_id).unwrap();
				assert_eq!(sess.sd, Some(Value::from(user_id)));
			}
		}

		//
		// Test with custom user string identifiers of varying lengths
		//
		{
			let ids = vec!["username", "username1", "username10", "username100"];
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
				assert_eq!(sess.sc, Some("test".to_string()));
				assert_eq!(sess.au.id(), "token");
				assert!(sess.au.is_scope());
				let user_id = syn::thing(&resource_id).unwrap();
				assert_eq!(sess.sd, Some(Value::from(user_id)));
			}
		}

		//
		// Test with custom user string identifiers of varying lengths with special characters
		//
		{
			let ids = vec!["user.name", "user.name1", "user.name10", "user.name100"];
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
				assert_eq!(sess.sc, Some("test".to_string()));
				assert_eq!(sess.au.id(), "token");
				assert!(sess.au.is_scope());
				let user_id = syn::thing(&resource_id).unwrap();
				assert_eq!(sess.sd, Some(Value::from(user_id)));
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
			assert_eq!(sess.sc, Some("test".to_string()));
			assert_eq!(sess.au.id(), "token");
			assert!(sess.au.is_scope());
			let user_id = syn::thing(&resource_id).unwrap();
			assert_eq!(sess.sd, Some(Value::from(user_id)));
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
}
