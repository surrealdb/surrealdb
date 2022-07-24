use crate::cli::CF;
use crate::dbs::DB;
use crate::err::Error;
use crate::iam::token::Claims;
use argon2::password_hash::{PasswordHash, PasswordVerifier};
use argon2::Argon2;
use jsonwebtoken::{decode, DecodingKey, Validation};
use once_cell::sync::Lazy;
use std::sync::Arc;
use surrealdb::sql::Algorithm;
use surrealdb::sql::Value;
use surrealdb::Auth;
use surrealdb::Session;

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
	validation.validate_nbf = true;
	validation.validate_exp = true;
	validation
});

pub async fn basic(session: &mut Session, auth: String) -> Result<(), Error> {
	// Retrieve just the auth data
	if let Some((_, auth)) = auth.split_once(' ') {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Get the config options
		let opts = CF.get().unwrap();
		// Decode the encoded auth data
		let auth = base64::decode(auth)?;
		// Convert the auth data to String
		let auth = String::from_utf8(auth)?;
		// Split the auth data into user and pass
		if let Some((user, pass)) = auth.split_once(':') {
			// Check that the details are not empty
			if user.is_empty() || pass.is_empty() {
				return Err(Error::InvalidAuth);
			}
			// Check if this is root authentication
			if let Some(root) = &opts.pass {
				if user == opts.user && pass == root {
					session.au = Arc::new(Auth::Kv);
					return Ok(());
				}
			}
			// Check if this is NS authentication
			if let Some(ns) = &session.ns {
				// Create a new readonly transaction
				let mut tx = kvs.transaction(false, false).await?;
				// Check if the supplied NS Login exists
				if let Ok(nl) = tx.get_nl(ns, user).await {
					// Compute the hash and verify the password
					let hash = PasswordHash::new(&nl.hash).unwrap();
					if Argon2::default().verify_password(pass.as_ref(), &hash).is_ok() {
						session.au = Arc::new(Auth::Ns(ns.to_owned()));
						return Ok(());
					}
				};
				// Check if this is DB authentication
				if let Some(db) = &session.db {
					// Check if the supplied DB Login exists
					if let Ok(dl) = tx.get_dl(ns, db, user).await {
						// Compute the hash and verify the password
						let hash = PasswordHash::new(&dl.hash).unwrap();
						if Argon2::default().verify_password(pass.as_ref(), &hash).is_ok() {
							session.au = Arc::new(Auth::Db(ns.to_owned(), db.to_owned()));
							return Ok(());
						}
					};
				}
			}
		}
	}
	// There was an auth error
	Err(Error::InvalidAuth)
}

pub async fn token(session: &mut Session, auth: String) -> Result<(), Error> {
	// Retrieve just the auth data
	if let Some((_, auth)) = auth.split_once(' ') {
		// Get a database reference
		let kvs = DB.get().unwrap();
		// Decode the token without verifying
		let token = decode::<Claims>(auth, &KEY, &DUD)?;
		// Check the token authentication claims
		match token.claims {
			// Check if this is scope token authentication
			Claims {
				ns: Some(ns),
				db: Some(db),
				sc: Some(sc),
				tk: Some(tk),
				id: Some(id),
				..
			} => {
				// Create a new readonly transaction
				let mut tx = kvs.transaction(false, false).await?;
				// Parse the record id
				let id = surrealdb::sql::thing(&id)?;
				// Get the scope token
				let de = tx.get_st(&ns, &db, &sc, &tk).await?;
				let cf = config(de.kind, de.code)?;
				// Verify the token
				decode::<Claims>(auth, &cf.0, &cf.1)?;
				// Set the session
				session.ns = Some(ns.to_owned());
				session.db = Some(db.to_owned());
				session.sc = Some(sc.to_owned());
				session.sd = Some(Value::from(id));
				session.au = Arc::new(Auth::Sc(ns, db, sc));
				return Ok(());
			}
			// Check if this is scope authentication
			Claims {
				ns: Some(ns),
				db: Some(db),
				sc: Some(sc),
				id: Some(id),
				..
			} => {
				// Create a new readonly transaction
				let mut tx = kvs.transaction(false, false).await?;
				// Parse the record id
				let id = surrealdb::sql::thing(&id)?;
				// Get the scope
				let de = tx.get_sc(&ns, &db, &sc).await?;
				let cf = config(Algorithm::Hs512, de.code)?;
				// Verify the token
				decode::<Claims>(auth, &cf.0, &cf.1)?;
				// Set the session
				session.ns = Some(ns.to_owned());
				session.db = Some(db.to_owned());
				session.sc = Some(sc.to_owned());
				session.sd = Some(Value::from(id));
				session.au = Arc::new(Auth::Sc(ns, db, sc));
				return Ok(());
			}
			// Check if this is database token authentication
			Claims {
				ns: Some(ns),
				db: Some(db),
				tk: Some(tk),
				..
			} => {
				// Create a new readonly transaction
				let mut tx = kvs.transaction(false, false).await?;
				// Get the database token
				let de = tx.get_dt(&ns, &db, &tk).await?;
				let cf = config(de.kind, de.code)?;
				// Verify the token
				decode::<Claims>(auth, &cf.0, &cf.1)?;
				// Set the session
				session.ns = Some(ns.to_owned());
				session.db = Some(db.to_owned());
				session.au = Arc::new(Auth::Db(ns, db));
				return Ok(());
			}
			// Check if this is database authentication
			Claims {
				ns: Some(ns),
				db: Some(db),
				id: Some(id),
				..
			} => {
				// Create a new readonly transaction
				let mut tx = kvs.transaction(false, false).await?;
				// Get the database login
				let de = tx.get_dl(&ns, &db, &id).await?;
				let cf = config(Algorithm::Hs512, de.code)?;
				// Verify the token
				decode::<Claims>(auth, &cf.0, &cf.1)?;
				// Set the session
				session.ns = Some(ns.to_owned());
				session.db = Some(db.to_owned());
				session.au = Arc::new(Auth::Db(ns, db));
				return Ok(());
			}
			// Check if this is namespace token authentication
			Claims {
				ns: Some(ns),
				tk: Some(tk),
				..
			} => {
				// Create a new readonly transaction
				let mut tx = kvs.transaction(false, false).await?;
				// Get the namespace token
				let de = tx.get_nt(&ns, &tk).await?;
				let cf = config(de.kind, de.code)?;
				// Verify the token
				decode::<Claims>(auth, &cf.0, &cf.1)?;
				// Set the session
				session.ns = Some(ns.to_owned());
				session.au = Arc::new(Auth::Ns(ns));
				return Ok(());
			}
			// Check if this is namespace authentication
			Claims {
				ns: Some(ns),
				id: Some(id),
				..
			} => {
				// Create a new readonly transaction
				let mut tx = kvs.transaction(false, false).await?;
				// Get the namespace login
				let de = tx.get_nl(&ns, &id).await?;
				let cf = config(Algorithm::Hs512, de.code)?;
				// Verify the token
				decode::<Claims>(auth, &cf.0, &cf.1)?;
				// Set the session
				session.ns = Some(ns.to_owned());
				session.au = Arc::new(Auth::Ns(ns));
				return Ok(());
			}
			// There was an auth error
			_ => return Err(Error::InvalidAuth),
		};
	}
	// There was an auth error
	Err(Error::InvalidAuth)
}
