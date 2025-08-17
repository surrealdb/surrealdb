use anyhow::Result;
use reblessive;

use crate::catalog;
use crate::cnf::INSECURE_FORWARD_ACCESS_ERRORS;
use crate::ctx::MutableContext;
use crate::dbs::Session;
use crate::err::Error;
use crate::expr::statements::access;
use crate::expr::{Base, Expr, Ident};
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::val::{RecordId, Value};

// Execute the AUTHENTICATE clause for a record access method
pub async fn authenticate_record(
	kvs: &Datastore,
	session: &Session,
	authenticate: &Expr,
) -> Result<RecordId> {
	match kvs.evaluate(authenticate, session, None).await {
		Ok(val) => match val.record() {
			// If the AUTHENTICATE clause returns a record, authentication continues with that
			// record
			Some(id) => Ok(id),
			// If the AUTHENTICATE clause returns anything else, authentication fails generically
			_ => {
				debug!("Authentication attempt as record user rejected by AUTHENTICATE clause");
				Err(anyhow::Error::new(Error::InvalidAuth))
			}
		},
		Err(e) => {
			match e.downcast_ref() {
				// If the AUTHENTICATE clause throws a specific error, authentication fails with
				// that error
				Some(Error::Thrown(_)) => Err(e),
				// If the AUTHENTICATE clause failed due to an unexpected error, be more specific
				// This allows clients to handle these errors, which may be retryable
				Some(Error::Tx(_) | Error::TxFailure | Error::TxRetryable) => {
					debug!("Unexpected error found while executing AUTHENTICATE clause: {e}");
					Err(anyhow::Error::new(Error::UnexpectedAuth))
				}
				// Otherwise, return a generic error unless it should be forwarded
				_ => {
					debug!(
						"Authentication attempt failed due to an error in the AUTHENTICATE clause: {e}"
					);
					if *INSECURE_FORWARD_ACCESS_ERRORS {
						Err(e)
					} else {
						Err(anyhow::Error::new(Error::InvalidAuth))
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
	authenticate: &Expr,
) -> Result<()> {
	match kvs.evaluate(authenticate, session, None).await {
		Ok(val) => {
			match val {
				// If the AUTHENTICATE clause returns nothing, authentication continues
				Value::None => Ok(()),
				// If the AUTHENTICATE clause returns anything else, authentication fails
				// generically
				_ => {
					debug!("Authentication attempt as system user rejected by AUTHENTICATE clause");
					Err(anyhow::Error::new(Error::InvalidAuth))
				}
			}
		}
		Err(e) => {
			match e.downcast_ref() {
				// If the AUTHENTICATE clause throws a specific error, authentication fails with
				// that error
				Some(Error::Thrown(_)) => Err(e),
				// If the AUTHENTICATE clause failed due to an unexpected error, be more specific
				// This allows clients to handle these errors, which may be retryable
				Some(Error::Tx(_) | Error::TxFailure | Error::TxRetryable) => {
					debug!("Unexpected error found while executing an AUTHENTICATE clause: {e}");
					Err(anyhow::Error::new(Error::UnexpectedAuth))
				}
				// Otherwise, return a generic error unless it should be forwarded
				_ => {
					debug!(
						"Authentication attempt failed due to an error in the AUTHENTICATE clause: {e}"
					);
					if *INSECURE_FORWARD_ACCESS_ERRORS {
						Err(e)
					} else {
						Err(anyhow::Error::new(Error::InvalidAuth))
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
	rid: RecordId,
) -> Result<String> {
	let sess = Session::owner().with_ns(ns).with_db(db);
	let opt = kvs.setup_options(&sess);
	// Create a new context with a writeable transaction
	let mut ctx = MutableContext::background();
	let tx = kvs.transaction(Write, Optimistic).await?.enclose();
	ctx.set_transaction(tx.clone());
	let ctx = ctx.freeze();
	// Create a bearer grant to act as the refresh token
	let grant = access::create_grant(ac, Some(Base::Db), catalog::Subject::Record(rid), &ctx, &opt)
		.await
		.map_err(|e| {
			warn!("Unexpected error when attempting to create a refresh token: {e}");
			Error::UnexpectedAuth
		})?;
	tx.commit().await?;
	// Return the key string from the bearer grant
	match grant.grant {
		catalog::Grant::Bearer(bearer) => Ok(bearer.key),
		_ => Err(anyhow::Error::new(Error::AccessMethodMismatch)),
	}
}

// Revoke a bearer key that acted as a refresh token for a record user
pub async fn revoke_refresh_token_record(
	kvs: &Datastore,
	gr: Ident,
	ac: Ident,
	ns: &str,
	db: &str,
) -> Result<()> {
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
	let mut stack = reblessive::tree::TreeStack::new();
	stack
		.enter(|stk| async {
			access::revoke_grant(&stmt, stk, &ctx, &opt).await.map_err(|e| {
				warn!("Unexpected error when attempting to revoke a refresh token: {e}");
				Error::UnexpectedAuth
			})
		})
		.finish()
		.await?;
	tx.commit().await?;
	Ok(())
}
