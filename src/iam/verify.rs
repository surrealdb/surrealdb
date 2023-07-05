use crate::cli::CF;
use crate::dbs::DB;
use crate::err::Error;
use crate::iam::BASIC;
use argon2::password_hash::{PasswordHash, PasswordVerifier};
use argon2::Argon2;
use std::sync::Arc;
use surrealdb::dbs::Auth;
use surrealdb::dbs::Session;
use surrealdb::iam::base::{Engine, BASE64};

pub async fn basic(session: &mut Session, auth: String) -> Result<(), Error> {
	// Log the authentication type
	trace!("Attempting basic authentication");
	// Retrieve just the auth data
	let auth = auth.trim_start_matches(BASIC).trim();
	// Get a database reference
	let kvs = DB.get().unwrap();
	// Get the config options
	let opts = CF.get().unwrap();
	// Decode the encoded auth data
	let auth = BASE64.decode(auth)?;
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
				// Log the authentication type
				debug!("Authenticated as super user");
				// Store the authentication data
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
					// Log the successful namespace authentication
					debug!("Authenticated as namespace user: {}", user);
					// Store the authentication data
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
						// Log the successful namespace authentication
						debug!("Authenticated as database user: {}", user);
						// Store the authentication data
						session.au = Arc::new(Auth::Db(ns.to_owned(), db.to_owned()));
						return Ok(());
					}
				};
			}
		}
	}
	// There was an auth error
	Err(Error::InvalidAuth)
}
