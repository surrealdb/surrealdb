use crate::cnf::INSECURE_FORWARD_ACCESS_ERRORS;
use crate::ctx::MutableContext;
use crate::dbs::Session;
use crate::err::Error;
use crate::kvs::{Datastore, LockType::*, TransactionType::*};
use crate::sql::statements::access;
use crate::sql::{Base, Ident, Thing, Value};
use reblessive::tree::Stk;

// Execute the AUTHENTICATE clause for a record access method
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
			_ => {
				debug!("Authentication attempt as record user rejected by AUTHENTICATE clause");
				Err(Error::InvalidAuth)
			}
		},
		Err(e) => {
			match e {
				// If the AUTHENTICATE clause throws a specific error, authentication fails with that error
				Error::Thrown(_) => Err(e),
				// If the AUTHENTICATE clause failed due to an unexpected error, be more specific
				// This allows clients to handle these errors, which may be retryable
				Error::Tx(_) | Error::TxFailure => {
					debug!("Unexpected error found while executing AUTHENTICATE clause: {e}");
					Err(Error::UnexpectedAuth)
				}
				// Otherwise, return a generic error unless it should be forwarded
				e => {
					debug!("Authentication attempt failed due to an error in the AUTHENTICATE clause: {e}");
					if *INSECURE_FORWARD_ACCESS_ERRORS {
						Err(e)
					} else {
						Err(Error::InvalidAuth)
					}
				}
			}
		}
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
				_ => {
					debug!("Authentication attempt as system user rejected by AUTHENTICATE clause");
					Err(Error::InvalidAuth)
				}
			}
		}
		Err(e) => {
			match e {
				// If the AUTHENTICATE clause throws a specific error, authentication fails with that error
				Error::Thrown(_) => Err(e),
				// If the AUTHENTICATE clause failed due to an unexpected error, be more specific
				// This allows clients to handle these errors, which may be retryable
				Error::Tx(_) | Error::TxFailure => {
					debug!("Unexpected error found while executing an AUTHENTICATE clause: {e}");
					Err(Error::UnexpectedAuth)
				}
				// Otherwise, return a generic error unless it should be forwarded
				e => {
					debug!("Authentication attempt failed due to an error in the AUTHENTICATE clause: {e}");
					if *INSECURE_FORWARD_ACCESS_ERRORS {
						Err(e)
					} else {
						Err(Error::InvalidAuth)
					}
				}
			}
		}
	}
}


// Create a bearer key to act as refresh token for a record user
pub async fn create_refresh_token_record(
	kvs: &Datastore,
	ac: Ident,
	ns: &str,
	db: &str,
	rid: Thing,
) -> Result<String, Error> {
	let stmt = access::AccessStatementGrant {
		ac,
		base: Some(Base::Db),
		subject: access::Subject::Record(rid),
	};
	let sess = Session::owner().with_ns(ns).with_db(db);
	let opt = kvs.setup_options(&sess);
	// Create a new context with a writeable transaction
	let mut ctx = MutableContext::background();
	let tx = kvs.transaction(Write, Optimistic).await?.enclose();
	ctx.set_transaction(tx.clone());
	let ctx = ctx.freeze();
	// Create a bearer grant to act as the refresh token 
	let grant = access::create_grant(&stmt, &ctx, &opt).await.map_err(|e| {
		warn!("Unexpected error when attempting to create a refresh token: {e}");
		Error::UnexpectedAuth
	})?;
	tx.cancel().await?;
	// Return the key string from the bearer grant
	match grant.grant {
		access::Grant::Bearer(bearer) => Ok(bearer.key.as_string()),
		_ => Err(Error::AccessMethodMismatch),
	}
}

// Revoke a bearer key that acted as a refresh token for a record user
pub async fn revoke_refresh_token_record(
	kvs: &Datastore,
	gr: Ident,
	ac: Ident,
	ns: &str,
	db: &str,
) -> Result<(), Error> {
	let stmt = access::AccessStatementRevoke {
		ac,
		base: Some(Base::Db),
		gr: Some(gr),
		cond: None,
	};
	let sess = Session::owner().with_ns(ns).with_db(db);
	let opt = kvs.setup_options(&sess);
	// Create a new context with a writeable transaction
	let mut ctx = MutableContext::background();
	let tx = kvs.transaction(Write, Optimistic).await?.enclose();
	ctx.set_transaction(tx.clone());
	let ctx = ctx.freeze();
	// Create a bearer grant to act as the refresh token 
	Stk::enter_scope(|stk| stk.run(|stk| access::revoke_grant(&stmt, stk, &ctx, &opt)))
		.await
		.map_err(|e| {
			warn!("Unexpected error when attempting to revoke a refresh token: {e}");
			Error::UnexpectedAuth
		})?;
	tx.cancel().await?;
	Ok(())
}
