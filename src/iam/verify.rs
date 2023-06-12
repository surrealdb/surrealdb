use crate::dbs::DB;
use crate::iam::BASIC;
use std::sync::Arc;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::base::{Engine, BASE64};
use surrealdb::iam::verify::verify_creds;
use surrealdb::iam::LOG;

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
