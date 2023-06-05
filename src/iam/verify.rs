use crate::dbs::DB;
use crate::err::Error;
use crate::iam::base::{Engine, BASE64};
use crate::iam::token::Claims;
use crate::iam::BASIC;
use crate::iam::LOG;
use crate::iam::TOKEN;
use argon2::password_hash::{PasswordHash, PasswordVerifier};
use argon2::Argon2;
use chrono::Utc;
use jsonwebtoken::{decode, DecodingKey, Validation};
use once_cell::sync::Lazy;
use std::sync::Arc;
use surrealdb::dbs::Auth;
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use surrealdb::sql::statements::DefineUserStatement;
use surrealdb::sql::Algorithm;
use surrealdb::sql::Value;

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

pub async fn basic(session: &mut Session, auth: String) -> Result<(), Error> {
	// Log the authentication type
	trace!(target: LOG, "Attempting basic authentication");

	// Retrieve just the auth data
	let auth = auth.trim_start_matches(BASIC).trim();
	// Decode the encoded auth data
	let auth = BASE64.decode(auth)?;
	// Convert the auth data to String
	let auth = String::from_utf8(auth)?;
	// Split the auth data into user and pass
	match auth.split_once(':') {
		Some((user, pass)) => {
			let ns = session.ns.to_owned();
			let db = session.db.to_owned();

			match verify_creds(DB.get().unwrap(), ns.as_ref(), db.as_ref(), user, pass).await {
				Ok((au, _)) if au.is_kv() => {
					debug!(target: LOG, "Authenticated as root user '{}'", user);
					session.au = Arc::new(au);
					Ok(())
				}
				Ok((au, _)) if au.is_ns() => {
					debug!(target: LOG, "Authenticated as namespace user '{}'", user);
					session.au = Arc::new(au);
					Ok(())
				}
				Ok((au, _)) if au.is_db() => {
					debug!(target: LOG, "Authenticated as database user '{}'", user);
					session.au = Arc::new(au);
					Ok(())
				}
				Ok(_) => Err(Error::InvalidAuth),
				Err(e) => Err(e),
			}
		}
		_ => {
			// Couldn't parse the auth data info
			Err(Error::InvalidAuth)
		}
	}
}

pub async fn token(session: &mut Session, auth: String) -> Result<(), Error> {
	// Log the authentication type
	trace!(target: LOG, "Attempting token authentication");
	// Retrieve just the auth data
	let auth = auth.trim_start_matches(TOKEN).trim();
	// Get a database reference
	let kvs = DB.get().unwrap();
	// Decode the token without verifying
	let token = decode::<Claims>(auth, &KEY, &DUD)?;
	// Parse the token and catch any errors
	let value = super::parse::parse(auth)?;
	// Check if the auth token can be used
	if let Some(nbf) = token.claims.nbf {
		if nbf > Utc::now().timestamp() {
			trace!(target: LOG, "The 'nbf' field in the authentication token was invalid");
			return Err(Error::InvalidAuth);
		}
	}
	// Check if the auth token has expired
	if let Some(exp) = token.claims.exp {
		if exp < Utc::now().timestamp() {
			trace!(target: LOG, "The 'exp' field in the authentication token was invalid");
			return Err(Error::InvalidAuth);
		}
	}
	// Check the token authentication claims
	match token.claims {
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
			trace!(target: LOG, "Authenticating to scope `{}` with token `{}`", sc, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Parse the record id
			let id = match id {
				Some(id) => surrealdb::sql::thing(&id)?.into(),
				None => Value::None,
			};
			// Get the scope token
			let de = tx.get_st(&ns, &db, &sc, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			debug!(target: LOG, "Authenticated to scope `{}` with token `{}`", sc, tk);
			// Set the session
			session.sd = Some(id);
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.sc = Some(sc.to_owned());
			session.au = Arc::new(Auth::Sc(ns, db, sc));
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
			trace!(target: LOG, "Authenticating to scope `{}`", sc);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Parse the record id
			let id = surrealdb::sql::thing(&id)?;
			// Get the scope
			let de = tx.get_sc(&ns, &db, &sc).await?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			debug!(target: LOG, "Authenticated to scope `{}`", sc);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.sc = Some(sc.to_owned());
			session.sd = Some(Value::from(id));
			session.au = Arc::new(Auth::Sc(ns, db, sc));
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
			trace!(target: LOG, "Authenticating to database `{}` with token `{}`", db, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Get the database token
			let de = tx.get_dt(&ns, &db, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			debug!(target: LOG, "Authenticated to database `{}` with token `{}`", db, tk);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.au = Arc::new(Auth::Db(ns, db));
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
			trace!(target: LOG, "Authenticating to database `{}` with user `{}`", db, id);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Get the database user
			let de = tx.get_db_user(&ns, &db, &id).await?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			debug!(target: LOG, "Authenticated to database `{}` with user `{}`", db, id);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.db = Some(db.to_owned());
			session.au = Arc::new(Auth::Db(ns, db));
			Ok(())
		}
		// Check if this is namespace token authentication
		Claims {
			ns: Some(ns),
			tk: Some(tk),
			..
		} => {
			// Log the decoded authentication claims
			trace!(target: LOG, "Authenticating to namespace `{}` with token `{}`", ns, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Get the namespace token
			let de = tx.get_nt(&ns, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			trace!(target: LOG, "Authenticated to namespace `{}` with token `{}`", ns, tk);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.au = Arc::new(Auth::Ns(ns));
			Ok(())
		}
		// Check if this is namespace authentication
		Claims {
			ns: Some(ns),
			id: Some(id),
			..
		} => {
			// Log the decoded authentication claims
			trace!(target: LOG, "Authenticating to namespace `{}` with user `{}`", ns, id);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Get the namespace user
			let de = tx.get_ns_user(&ns, &id).await?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			trace!(target: LOG, "Authenticated to namespace `{}` with user `{}`", ns, id);
			// Set the session
			session.tk = Some(value);
			session.ns = Some(ns.to_owned());
			session.au = Arc::new(Auth::Ns(ns));
			Ok(())
		}
		// There was an auth error
		_ => Err(Error::InvalidAuth),
	}
}

pub async fn verify_creds(
	ds: &Datastore,
	ns: Option<&String>,
	db: Option<&String>,
	user: &str,
	pass: &str,
) -> Result<(Auth, DefineUserStatement), Error> {
	if user.is_empty() || pass.is_empty() {
		return Err(Error::InvalidAuth);
	}

	// TODO(sgirones): Keep the same behaviour as before, where it would try to authenticate as a KV first, then NS and then DB.
	// In the future, we want the client to specify the type of user it wants to authenticate as, so we can remove this chain.

	// Try to authenticate as a KV user
	match verify_kv_creds(ds, user, pass).await {
		Ok(u) => Ok((Auth::Kv, u)),
		Err(_) => {
			// Try to authenticate as a NS user
			match ns {
				Some(ns) => {
					match verify_ns_creds(ds, ns, user, pass).await {
						Ok(u) => Ok((Auth::Ns(ns.to_owned()), u)),
						Err(_) => {
							// Try to authenticate as a DB user
							match db {
								Some(db) => match verify_db_creds(ds, ns, db, user, pass).await {
									Ok(u) => Ok((Auth::Db(ns.to_owned(), db.to_owned()), u)),
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

async fn verify_kv_creds(
	ds: &Datastore,
	user: &str,
	pass: &str,
) -> Result<DefineUserStatement, Error> {
	let mut tx = ds.transaction(false, false).await?;
	let user_res = tx.get_kv_user(user).await?;

	verify_pass(pass, user_res.hash.as_ref())?;

	Ok(user_res)
}

async fn verify_ns_creds(
	ds: &Datastore,
	ns: &str,
	user: &str,
	pass: &str,
) -> Result<DefineUserStatement, Error> {
	let mut tx = ds.transaction(false, false).await?;

	let user_res = match tx.get_ns_user(ns, user).await {
		Ok(u) => Ok(u),
		Err(surrealdb::error::Db::UserNsNotFound {
			ns: _,
			value: _,
		}) => match tx.get_nl(ns, user).await {
			Ok(u) => Ok(DefineUserStatement {
				base: u.base,
				name: u.name,
				hash: u.hash,
				code: u.code,
			}),
			Err(e) => Err(e),
		},
		Err(e) => Err(e),
	}?;

	verify_pass(pass, user_res.hash.as_ref())?;

	Ok(user_res)
}

async fn verify_db_creds(
	ds: &Datastore,
	ns: &str,
	db: &str,
	user: &str,
	pass: &str,
) -> Result<DefineUserStatement, Error> {
	let mut tx = ds.transaction(false, false).await?;

	let user_res = match tx.get_db_user(ns, db, user).await {
		Ok(u) => Ok(u),
		Err(surrealdb::error::Db::UserDbNotFound {
			ns: _,
			db: _,
			value: _,
		}) => match tx.get_dl(ns, db, user).await {
			Ok(u) => Ok(DefineUserStatement {
				base: u.base,
				name: u.name,
				hash: u.hash,
				code: u.code,
			}),
			Err(e) => Err(e),
		},
		Err(e) => Err(e),
	}?;

	verify_pass(pass, user_res.hash.as_ref())?;

	Ok(user_res)
}

fn verify_pass(pass: &str, hash: &str) -> Result<(), Error> {
	// Compute the hash and verify the password
	let hash = PasswordHash::new(hash.as_ref()).unwrap();
	// Attempt to verify the password using Argon2
	match Argon2::default().verify_password(pass.as_ref(), &hash) {
		Ok(_) => Ok(()),
		// The password did not verify
		_ => Err(Error::InvalidAuth),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use argon2::password_hash::{PasswordHasher, SaltString};
	use surrealdb::{kvs::Datastore, sql::Base};

	#[tokio::test]
	async fn basic() {}

	#[tokio::test]
	async fn token() {}

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

		// Reject empty username or password
		{
			assert!(verify_creds(&ds, None, None, "", "").await.is_err());
			assert!(verify_creds(&ds, None, None, "test", "").await.is_err());
			assert!(verify_creds(&ds, None, None, "", "test").await.is_err());
		}

		// Reject invalid KV credentials
		{
			assert!(verify_creds(&ds, None, None, "test", "test").await.is_err());
		}

		// Reject invalid NS credentials
		{
			assert!(verify_creds(&ds, Some(&ns), None, "test", "test").await.is_err());
		}

		// Reject invalid DB credentials
		{
			assert!(verify_creds(&ds, Some(&ns), Some(&db), "test", "test").await.is_err());
		}
	}

	#[tokio::test]
	async fn test_verify_creds_valid() {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = "N".to_string();
		let db = "D".to_string();

		// Define users
		{
			let sess = Session::for_kv();

			let sql = "DEFINE USER kv ON KV PASSWORD 'kv'";
			ds.execute(&sql, &sess, None, false).await.unwrap();

			let sql = "USE NS N; DEFINE USER ns ON NS PASSWORD 'ns'";
			ds.execute(&sql, &sess, None, false).await.unwrap();

			let sql = "USE NS N DB D; DEFINE USER db ON DB PASSWORD 'db'";
			ds.execute(&sql, &sess, None, false).await.unwrap();
		}

		// Accept KV user
		{
			let res = verify_creds(&ds, None, None, "kv", "kv").await;
			assert!(res.is_ok());

			let (auth, user) = res.unwrap();
			assert_eq!(auth, Auth::Kv);
			assert_eq!(user.base, Base::Kv);
			assert_eq!(user.name.to_string(), "kv");
		}

		// Accept NS user
		{
			let res = verify_creds(&ds, Some(&ns), None, "ns", "ns").await;
			assert!(res.is_ok());

			let (auth, user) = res.unwrap();
			assert_eq!(auth, Auth::Ns(ns.to_owned()));
			assert_eq!(user.base, Base::Ns);
			assert_eq!(user.name.to_string(), "ns");
		}

		// Accept DB user
		{
			let res = verify_creds(&ds, Some(&ns), Some(&db), "db", "db").await;
			assert!(res.is_ok());

			let (auth, user) = res.unwrap();
			assert_eq!(auth, Auth::Db(ns.to_owned(), db.to_owned()));
			assert_eq!(user.base, Base::Db);
			assert_eq!(user.name.to_string(), "db");
		}
	}

	#[tokio::test]
	async fn test_verify_creds_chain() {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = "N".to_string();
		let db = "D".to_string();

		// Define users
		{
			let sess = Session::for_kv();

			let sql = "DEFINE USER kv ON KV PASSWORD 'kv'";
			ds.execute(&sql, &sess, None, false).await.unwrap();

			let sql = "USE NS N; DEFINE USER ns ON NS PASSWORD 'ns'";
			ds.execute(&sql, &sess, None, false).await.unwrap();

			let sql = "USE NS N DB D; DEFINE USER db ON DB PASSWORD 'db'";
			ds.execute(&sql, &sess, None, false).await.unwrap();
		}

		// Accept KV user even with NS and DB defined
		{
			let res = verify_creds(&ds, Some(&ns), Some(&db), "kv", "kv").await;
			assert!(res.is_ok());

			let (auth, user) = res.unwrap();
			assert_eq!(auth, Auth::Kv);
			assert_eq!(user.base, Base::Kv);
			assert_eq!(user.name.to_string(), "kv");
		}

		// Accept NS user even with DB defined
		{
			let res = verify_creds(&ds, Some(&ns), Some(&db), "ns", "ns").await;
			assert!(res.is_ok());

			let (auth, user) = res.unwrap();
			assert_eq!(auth, Auth::Ns(ns.to_owned()));
			assert_eq!(user.base, Base::Ns);
			assert_eq!(user.name.to_string(), "ns");
		}

		// Accept DB user
		{
			let res = verify_creds(&ds, Some(&ns), Some(&db), "db", "db").await;
			assert!(res.is_ok());

			let (auth, user) = res.unwrap();
			assert_eq!(auth, Auth::Db(ns.to_owned(), db.to_owned()));
			assert_eq!(user.base, Base::Db);
			assert_eq!(user.name.to_string(), "db");
		}
	}
}
