use crate::dbs::Auth;
use crate::dbs::Session;
use crate::err::Error;
use crate::iam::token::Claims;
use crate::iam::TOKEN;
use crate::kvs::Datastore;
use crate::sql::Algorithm;
use crate::sql::Value;
use chrono::Utc;
use jsonwebtoken::{decode, DecodingKey, Validation};
use once_cell::sync::Lazy;
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

pub async fn token(kvs: &Datastore, session: &mut Session, auth: String) -> Result<(), Error> {
	// Log the authentication type
	trace!("Attempting token authentication");
	// Retrieve just the auth data
	let auth = auth.trim_start_matches(TOKEN).trim();
	// Decode the token without verifying
	let token = decode::<Claims>(auth, &KEY, &DUD)?;
	// Parse the token and catch any errors
	let value = super::parse::parse(auth)?;
	// Check if the auth token can be used
	if let Some(nbf) = token.claims.nbf {
		if nbf > Utc::now().timestamp() {
			trace!("The 'nbf' field in the authentication token was invalid");
			return Err(Error::InvalidAuth);
		}
	}
	// Check if the auth token has expired
	if let Some(exp) = token.claims.exp {
		if exp < Utc::now().timestamp() {
			trace!("The 'exp' field in the authentication token was invalid");
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
			trace!("Authenticating to scope `{}` with token `{}`", sc, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Parse the record id
			let id = match id {
				Some(id) => crate::sql::thing(&id)?.into(),
				None => Value::None,
			};
			// Get the scope token
			let de = tx.get_st(&ns, &db, &sc, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to scope `{}` with token `{}`", sc, tk);
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
			trace!("Authenticating to scope `{}`", sc);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Parse the record id
			let id = crate::sql::thing(&id)?;
			// Get the scope
			let de = tx.get_sc(&ns, &db, &sc).await?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to scope `{}`", sc);
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
			trace!("Authenticating to database `{}` with token `{}`", db, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Get the database token
			let de = tx.get_dt(&ns, &db, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to database `{}` with token `{}`", db, tk);
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
			trace!("Authenticating to database `{}` with login `{}`", db, id);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Get the database login
			let de = tx.get_dl(&ns, &db, &id).await?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			debug!("Authenticated to database `{}` with login `{}`", db, id);
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
			trace!("Authenticating to namespace `{}` with token `{}`", ns, tk);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Get the namespace token
			let de = tx.get_nt(&ns, &tk).await?;
			let cf = config(de.kind, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			trace!("Authenticated to namespace `{}` with token `{}`", ns, tk);
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
			trace!("Authenticating to namespace `{}` with login `{}`", ns, id);
			// Create a new readonly transaction
			let mut tx = kvs.transaction(false, false).await?;
			// Get the namespace login
			let de = tx.get_nl(&ns, &id).await?;
			let cf = config(Algorithm::Hs512, de.code)?;
			// Verify the token
			decode::<Claims>(auth, &cf.0, &cf.1)?;
			// Log the success
			trace!("Authenticated to namespace `{}` with login `{}`", ns, id);
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
