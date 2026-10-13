use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive;
use surrealdb_types::ToSql;

use crate::catalog;
use crate::catalog::base::Base as AccessBase;
use crate::catalog::providers::{AuthorisationProvider, DatabaseProvider};
use crate::dbs::{Session, SessionData};
use crate::err::exec_error;
use crate::exec::Error as ExecError;
use crate::expr::statements::access;
use crate::expr::{Base, Expr};
use crate::iam::token::Claims;
use crate::iam::{Error as AuthError, Level, Role};
use crate::kvs::TransactionType::*;
use crate::kvs::{Datastore, is_retryable_transaction_conflict};
use crate::types::{PublicRecordId, PublicValue};
use crate::val::RecordId;

// Drop every piece of session state that names the principal a session was
// previously authenticated as.
//
// `au`, `tk` and `exp` are overwritten unconditionally by every authentication
// path. `ac`, `rd` and `data` are not: they are only assigned by the paths that
// have something to put in them, so re-authenticating as a different principal
// used to leave the previous one's access method, record identity and CONTEXT
// payload in place. `data` in particular is an authorisation grant — the same
// grounds on which `clear` and `reset` drop it — and a session that signed in
// as a record and then authenticated as something else kept carrying it into
// every permission it evaluated.
//
// Call this from the point where a path commits its new session state, not on
// entry: a failed authentication attempt must leave the session exactly as it
// found it.
pub(crate) fn reset_previous_principal(session: &mut Session) {
	session.ac = None;
	session.rd = None;
	session.data = None;
}

// Create the system session used to evaluate an access method's SIGNUP, SIGNIN or
// AUTHENTICATE clause. The session is an Editor scoped to the level implied by the
// definition's base and the namespace and database it was resolved in. A mismatch
// between the two fails authentication rather than widening the scope.
pub(crate) fn access_session(
	de: &catalog::AccessDefinition,
	ns: Option<&str>,
	db: Option<&str>,
	caller: &Session,
) -> Result<Session> {
	let level = match (&de.base, ns, db) {
		(AccessBase::Root, None, None) => Level::Root,
		(AccessBase::Ns, Some(ns), None) => Level::Namespace(ns.to_owned()),
		(AccessBase::Db, Some(ns), Some(db)) => Level::Database(ns.to_owned(), db.to_owned()),
		(base, ns, db) => {
			warn!(
				"Access method `{}` is defined at {base} level but was resolved with namespace {ns:?} and database {db:?}",
				de.name
			);
			bail!(AuthError::InvalidAuth)
		}
	};
	let mut sess = Session::for_level(level, Role::Editor);
	sess.ip.clone_from(&caller.ip);
	sess.or.clone_from(&caller.or);
	Ok(sess)
}

// Create the session used to evaluate an access method's CONTEXT clause.
//
// SECURITY: this deliberately does NOT reuse `access_session`. The result of a
// SIGNIN, SIGNUP or AUTHENTICATE clause is either discarded or reduced to a
// record id (`authenticate_record` below calls `into_record`), so evaluating
// those with an Editor session leaks nothing. The whole result of a CONTEXT
// clause, by contrast, is stored on the session and handed straight back to
// the client as `$session.data`. Evaluated privileged, every CONTEXT clause
// would be a permission-bypassing read whose output the record user can print.
// So it runs as the authenticated record itself, under exactly the permissions
// that record already has: CONTEXT can only ever materialise what the session
// could have selected for itself, which is what makes exposing it safe.
//
// This is also what SECURITY_GUIDE.md's "minimum necessary privileges" rule for
// access-method clauses asks for.
pub(crate) fn context_session(
	de: &catalog::AccessDefinition,
	ns: &str,
	db: &str,
	rid: PublicValue,
	token: PublicValue,
	caller: &Session,
) -> Session {
	let mut sess = Session::for_record(ns, db, de.name.as_str(), rid);
	sess.tk = Some(token);
	sess.ip.clone_from(&caller.ip);
	sess.or.clone_from(&caller.or);
	sess
}

// Execute the CONTEXT clause for an access method.
//
// Unlike `authenticate_record`, the returned value is kept WHOLE: no
// `into_record()`. That is the entire point of the clause — an expression that
// projects whatever the definition wants frozen for the life of the session,
// evaluated once, rather than re-derived per row inside a permission.
pub(crate) async fn compute_access_context(
	kvs: &Datastore,
	session: &Session,
	context: &Expr,
) -> Result<PublicValue> {
	match kvs.evaluate(context, session, None).await {
		Ok(val) => Ok(val),
		// If the CONTEXT clause throws a specific error, authentication fails with
		// that error
		Err(e) if matches!(exec_error(&e), Some(ExecError::Thrown(_))) => Err(e),
		Err(e) => {
			// If the CONTEXT clause failed due to an unexpected error, be more specific
			// This allows clients to handle these errors, which may be retryable
			if is_retryable_transaction_conflict(&e) {
				debug!("Unexpected error found while executing a CONTEXT clause: {e}");
				Err(anyhow::Error::new(AuthError::UnexpectedAuth))
			} else {
				// Otherwise, return a generic error unless it should be forwarded
				debug!("Authentication attempt failed due to an error in the CONTEXT clause: {e}");
				if kvs.config().iam.insecure_forward_access_errors {
					Err(e)
				} else {
					Err(anyhow::Error::new(AuthError::InvalidAuth))
				}
			}
		}
	}
}

// Compute the access method's CONTEXT clause, if it defines one, for a session
// that has just settled on `rid` as its identity.
//
// Returns `None` when the access method has no CONTEXT clause, which is what
// leaves an access method defined without one behaving exactly as before.
// Deliberately not gated on the presence of an AUTHENTICATE clause: CONTEXT is
// independent of it and must fire with or without one.
pub(crate) async fn session_data_for_record(
	kvs: &Datastore,
	de: &catalog::AccessDefinition,
	ns: &str,
	db: &str,
	rid: PublicValue,
	claims: &Claims,
	caller: &Session,
) -> Result<Option<SessionData>> {
	// Every argument this function needs beyond the definition itself is
	// derived AFTER this early return, on purpose. Building the `$token`
	// object means deep-cloning the claims (`custom_claims` is a `HashMap` of
	// `serde_json::Value`) and walking the whole thing through
	// `convert_value_to_public_value`. An access method with no CONTEXT clause
	// — which is every access method that existed before this clause did —
	// must not pay for any of it.
	let Some(context) = &de.context else {
		return Ok(None);
	};
	// SECURITY: backstop for the read-only guarantee.
	//
	// `validate_access_context` runs in the DEFINE and ALTER handlers and
	// checks the *statement's AST*. What is stored is canonical text
	// (`ExprText::new`), and what runs here is `ExprText::compile()` — a
	// re-parse under `ParserSettings::STORED_TEXT`, the full grammar with
	// unbounded limits, deliberately independent of the datastore's live
	// capabilities. Nothing in between re-checks the compiled form.
	//
	// That matters because `Datastore::evaluate` opens a *write* transaction
	// for anything `read_only()` reports false for, and commits it. HTTP
	// re-authenticates on every request, so any render/parse asymmetry — or
	// catalog bytes written by a differently-versioned binary — would become
	// an unguarded write firing once per request from any token holder.
	// `alter_access_statement_apply` already re-validates the round-tripped
	// definition for the same reason; this re-validates the form that
	// actually runs.
	if !context.read_only() {
		warn!(
			"The stored CONTEXT clause of access method `{}` is not read-only and will not be evaluated",
			de.name
		);
		bail!(AuthError::InvalidAuth);
	}
	// `$token` inside the clause names the identity the session settled on.
	//
	// The `id` claim was minted before AUTHENTICATE ran and still names the
	// record SIGNIN found; `rid` is what AUTHENTICATE settled on, and is what
	// `$auth` is bound to below. CONTEXT is the first clause to see both at
	// once, so it is the first place they can disagree. Overriding the claim
	// here keeps `$token.ID` and `$auth` naming the same record, which is the
	// substitution AUTHENTICATE exists to perform. The issued JWT is not
	// touched: it keeps the claims it was signed with, so re-verifying it
	// re-runs AUTHENTICATE and arrives here the same way.
	let mut claims = claims.clone();
	claims.id = Some(rid.to_sql());
	let token = crate::val::convert_value_to_public_value(claims.into_claims_object().into())?;
	let sess = context_session(de, ns, db, rid, token, caller);
	let val = compute_access_context(kvs, &sess, context).await?;
	Ok(Some(SessionData::new(val)))
}

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
				if kvs.config().iam.insecure_forward_access_errors {
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
				if kvs.config().iam.insecure_forward_access_errors {
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
/// Revokes the refresh-token grant named by `gr`, but only once the caller has
/// proved they hold its key.
///
/// This is the entry point for the externally reachable `revoke` operation.
/// That request carries no session, and the grant id it names is readable by
/// any principal holding `Action::View` on the access method, so possession of
/// the key is the only thing that distinguishes the token's owner from anyone
/// who has merely seen its id. Verifying it here is what authorises the
/// revocation; [`revoke_refresh_token_record`] itself runs as owner and
/// performs no such check, which is correct for the callers that have already
/// authenticated the grant.
///
/// Every failure is reported as `InvalidAuth` so a caller cannot distinguish a
/// wrong key from a grant that does not exist, is expired, or is already
/// revoked.
pub async fn verify_and_revoke_refresh_token_record(
	kvs: &Datastore,
	gr: String,
	ac: String,
	refresh: String,
	ns: &str,
	db: &str,
) -> Result<()> {
	let tx = kvs.transaction(Read).await?;
	let db_def = match tx.get_db_by_name(ns, db, None).await {
		Ok(Some(db_def)) => db_def,
		Ok(None) => {
			let _ = tx.cancel().await;
			debug!("Refresh token revocation rejected: database `{ns}/{db}` not found");
			return Err(anyhow::Error::new(AuthError::InvalidAuth));
		}
		Err(e) => {
			let _ = tx.cancel().await;
			return Err(e);
		}
	};
	let grant = match tx
		.get_db_access_grant(db_def.namespace_id, db_def.database_id, &ac, &gr, None)
		.await
	{
		Ok(Some(grant)) => grant,
		Ok(None) => {
			let _ = tx.cancel().await;
			debug!("Refresh token revocation rejected: grant `{gr}` for method `{ac}` not found");
			return Err(anyhow::Error::new(AuthError::InvalidAuth));
		}
		Err(e) => {
			let _ = tx.cancel().await;
			return Err(e);
		}
	};
	tx.cancel().await?;
	// Constant-time comparison of the presented key against the stored grant.
	crate::iam::signin::verify_grant_bearer(&grant, refresh)?;
	revoke_refresh_token_record(kvs, gr, ac, ns, db).await
}

/// Revokes the refresh-token grant named by `gr`, as owner and without
/// verifying possession of its key.
///
/// Callers must have already established that the revocation is authorised —
/// `signin_bearer` does so by authenticating the grant before rotating it. A
/// caller that has not must use [`verify_and_revoke_refresh_token_record`].
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

#[cfg(test)]
mod read_only_backstop_tests {
	use catalog::ExprText;

	use super::*;
	use crate::iam::token::Claims;

	fn definition(context: &str) -> catalog::AccessDefinition {
		catalog::AccessDefinition {
			name: "api".into(),
			base: AccessBase::Db,
			access_type: catalog::AccessType::Record(catalog::RecordAccess {
				signup: None,
				signin: None,
				jwt: catalog::JwtAccess {
					verify: catalog::JwtAccessVerify::Key(catalog::JwtAccessVerifyKey {
						alg: catalog::Algorithm::Hs512,
						key: "secret".to_owned(),
					}),
					issue: None,
					audience: None,
				},
				bearer: None,
			}),
			authenticate: None,
			context: Some(ExprText::from_raw(context).compile().unwrap()),
			signup: None,
			signin: None,
			grant_duration: None,
			token_duration: None,
			session_duration: None,
			comment: None,
		}
	}

	/// A stored CONTEXT clause that is not read-only must not be evaluated.
	///
	/// DEFINE and ALTER both refuse to install one, so the only way to reach
	/// this is a render/parse asymmetry or catalog bytes written by another
	/// binary — which is exactly what the backstop is for. Without it,
	/// `Datastore::evaluate` would open a write transaction and commit it,
	/// once per authentication.
	#[tokio::test]
	async fn a_stored_clause_that_writes_is_refused_at_evaluation() {
		let ds = Datastore::new("memory").await.unwrap();
		let owner = Session::owner().with_ns("test").with_db("test");
		for res in ds
			.execute(
				"DEFINE NAMESPACE test; DEFINE DATABASE test; DEFINE TABLE audit SCHEMALESS PERMISSIONS FULL",
				&owner,
				None,
			)
			.await
			.unwrap()
		{
			res.result.unwrap();
		}

		let de = definition("CREATE audit SET at = 1");
		let rid = PublicValue::RecordId(surrealdb_types::RecordId::new("user", 1));
		let err = session_data_for_record(
			&ds,
			&de,
			"test",
			"test",
			rid,
			&Claims::default(),
			&Session::default(),
		)
		.await
		.unwrap_err();
		assert!(
			err.downcast_ref::<AuthError>().is_some(),
			"a non-read-only stored clause must fail authentication, got: {err}"
		);

		let res = ds.execute("SELECT VALUE id FROM audit", &owner, None).await.unwrap();
		let rows = res.into_iter().next().unwrap().result.unwrap().into_array().unwrap().into_vec();
		assert!(rows.is_empty(), "the refused clause must not have written anything");
	}

	/// The read-only clause beside it still evaluates.
	#[tokio::test]
	async fn a_stored_read_only_clause_still_evaluates() {
		let ds = Datastore::new("memory").await.unwrap();
		let de = definition("RETURN 'granted'");
		let rid = PublicValue::RecordId(surrealdb_types::RecordId::new("user", 1));
		let data = session_data_for_record(
			&ds,
			&de,
			"test",
			"test",
			rid,
			&Claims::default(),
			&Session::default(),
		)
		.await
		.unwrap()
		.expect("a read-only clause produces a payload");
		assert_eq!(data.public(), &PublicValue::String("granted".to_owned()));
	}
}
