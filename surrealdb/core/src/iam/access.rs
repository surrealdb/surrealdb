use std::sync::Arc;

use anyhow::Result;
use reblessive;

use crate::catalog;
use crate::dbs::Session;
use crate::err::exec_error;
use crate::exec::Error as ExecError;
use crate::expr::statements::access;
use crate::expr::{Base, Expr};
use crate::iam::Error as AuthError;
use crate::kvs::TransactionType::*;
use crate::kvs::{Datastore, is_retryable_transaction_conflict};
use crate::types::{PublicRecordId, PublicValue};
use crate::val::RecordId;

// Execute the AUTHENTICATE clause for a record access method
pub(crate) async fn authenticate_record(
	kvs: &Datastore,
	session: &Session,
	authenticate: &Expr,
) -> Result<PublicRecordId> {
	match kvs.evaluate(authenticate, session, None).await {
		Ok(val) => match val.into_record() {
			// If the AUTHENTICATE clause returns a record, authentication continues with that
			// record
			Ok(id) => Ok(id),
			// If the AUTHENTICATE clause returns anything else, authentication fails generically
			_ => {
				debug!("Authentication attempt as record user rejected by AUTHENTICATE clause");
				Err(anyhow::Error::new(AuthError::InvalidAuth))
			}
		},
		// If the AUTHENTICATE clause throws a specific error, authentication fails with
		// that error
		Err(e) if matches!(exec_error(&e), Some(ExecError::Thrown(_))) => Err(e),
		Err(e) => {
			// If the AUTHENTICATE clause failed due to an unexpected error, be more specific
			// This allows clients to handle these errors, which may be retryable
			if is_retryable_transaction_conflict(&e) {
				debug!("Unexpected error found while executing AUTHENTICATE clause: {e}");
				Err(anyhow::Error::new(AuthError::UnexpectedAuth))
			} else {
				// Otherwise, return a generic error unless it should be forwarded
				debug!(
					"Authentication attempt failed due to an error in the AUTHENTICATE clause: {e}"
				);
				if kvs.config().insecure_forward_access_errors {
					Err(e)
				} else {
					Err(anyhow::Error::new(AuthError::InvalidAuth))
				}
			}
		}
	}
}

// Execute the AUTHENTICATE clause for any other access method
pub(crate) async fn authenticate_generic(
	kvs: &Datastore,
	session: &Session,
	authenticate: &Expr,
) -> Result<()> {
	match kvs.evaluate(authenticate, session, None).await {
		Ok(val) => {
			match val {
				// If the AUTHENTICATE clause returns nothing, authentication continues
				PublicValue::None => Ok(()),
				// If the AUTHENTICATE clause returns anything else, authentication fails
				// generically
				_ => {
					debug!("Authentication attempt as system user rejected by AUTHENTICATE clause");
					Err(anyhow::Error::new(AuthError::InvalidAuth))
				}
			}
		}
		// If the AUTHENTICATE clause throws a specific error, authentication fails with
		// that error
		Err(e) if matches!(exec_error(&e), Some(ExecError::Thrown(_))) => Err(e),
		Err(e) => {
			// If the AUTHENTICATE clause failed due to an unexpected error, be more specific
			// This allows clients to handle these errors, which may be retryable
			if is_retryable_transaction_conflict(&e) {
				debug!("Unexpected error found while executing an AUTHENTICATE clause: {e}");
				Err(anyhow::Error::new(AuthError::UnexpectedAuth))
			} else {
				// Otherwise, return a generic error unless it should be forwarded
				debug!(
					"Authentication attempt failed due to an error in the AUTHENTICATE clause: {e}"
				);
				if kvs.config().insecure_forward_access_errors {
					Err(e)
				} else {
					Err(anyhow::Error::new(AuthError::InvalidAuth))
				}
			}
		}
	}
}

// Create a bearer key to act as refresh token for a record user
pub(crate) async fn create_refresh_token_record(
	kvs: &Datastore,
	ac: String,
	ns: &str,
	db: &str,
	rid: RecordId,
) -> Result<String> {
	let sess = Session::owner().with_ns(ns).with_db(db);
	let opt = kvs.setup_options(&sess);
	// Create a new context with a writeable transaction
	let mut ctx = kvs.setup_ctx()?;
	let tx = kvs.transaction(Write).await?.enclose();
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	// Create a bearer grant to act as the refresh token
	let grant = run!(
		tx,
		crate::legacy::create_grant(ac, Some(Base::Db), catalog::Subject::Record(rid), &ctx, &opt)
			.await
			.map_err(|e| {
				warn!("Unexpected error when attempting to create a refresh token: {e}");
				anyhow::Error::new(AuthError::UnexpectedAuth)
			})
	)?;
	// Return the key string from the bearer grant
	match grant.grant {
		catalog::Grant::Bearer(bearer) => Ok(bearer.key),
		_ => Err(anyhow::Error::new(AuthError::AccessMethodMismatch)),
	}
}

// Revoke a bearer key that acted as a refresh token for a record user
pub async fn revoke_refresh_token_record(
	kvs: &Datastore,
	gr: String,
	ac: String,
	ns: &str,
	db: &str,
) -> Result<()> {
	let stmt = access::AccessStatementRevoke {
		ac: ac.into(),
		base: Some(Base::Db),
		gr: Some(gr.into()),
		cond: None,
	};
	let sess = Session::owner().with_ns(ns).with_db(db);
	let opt = kvs.setup_options(&sess);
	// Create a new context with a writeable transaction
	let mut ctx = kvs.setup_ctx()?;
	let tx = kvs.transaction(Write).await?.enclose();
	ctx.set_transaction(Arc::clone(&tx));
	let ctx = ctx.freeze();
	// Create a bearer grant to act as the refresh token
	let mut stack = reblessive::tree::TreeStack::new();
	run!(
		tx,
		stack
			.enter(|stk| async {
				crate::legacy::revoke_grant(&stmt, stk, &ctx, &opt).await.map_err(|e| {
					warn!("Unexpected error when attempting to revoke a refresh token: {e}");
					anyhow::Error::new(AuthError::UnexpectedAuth)
				})
			})
			.finish()
			.await
	)?;
	Ok(())
}
