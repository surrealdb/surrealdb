//! The grants an access method issues.
//!
//! `ACCESS ... GRANT`, `SHOW`, `REVOKE` and `PURGE` read and write the grants
//! held under an access method. These statements are executed directly, not
//! planned. Their expressions (a grant's subject and a `WHERE` clause over the
//! grants) are resolved through [`crate::ddl::scalar`] in the frame that
//! [`crate::ddl::frame`] builds.
//!
//! Every grant a clause is shown has been redacted first. A `WHERE` that could
//! read the plaintext key would hand it back through the statements that exist
//! to withhold it.

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use rand::Rng;

use crate::catalog::Error as CatalogError;
use crate::catalog::providers::{
	AuthorisationProvider, CatalogProvider, NamespaceProvider, UserProvider,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exe::FlowResultExt as _;
use crate::exec::{Clause, Error as ExecError, EvalContext, ExecutionContext};
use crate::expr::statements::access::{
	AccessStatement, AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke,
	AccessStatementShow, GRANT_BEARER_CHARACTER_POOL, GRANT_BEARER_ID_LENGTH,
	GRANT_BEARER_KEY_LENGTH, PurgeKind, Subject,
};
use crate::expr::{Base, Cond, Expr, Literal};
use crate::iam::{Action, Error as AuthError, ResourceKind};
use crate::key::schema::{DbGrantKey, NsGrantKey, RootGrantKey};
use crate::kvs::storage_error;
use crate::val::{Array, Datetime, Object, RecordId, Value};
use crate::{catalog, val};

/// The `WHERE` clause of an access statement, resolved once for the statement.
///
/// The clause is checked against every grant the statement reached, so it is
/// compiled once here, not per grant. The compiled form comes from the compile
/// cache ([`crate::exec::vm::serve::expr`]) and is only compiled on a cache
/// miss. A clause that cannot be lowered is kept as written and evaluated by
/// [`crate::ddl::scalar`].
enum GrantFilter {
	/// No clause: every grant the statement reached.
	Every,
	Compiled(Clause),
	Uncompiled(Expr),
}

impl GrantFilter {
	async fn resolve(cond: Option<&Cond>, exec: &ExecutionContext) -> Result<Self> {
		let Some(cond) = cond else {
			return Ok(Self::Every);
		};
		let Ok(logical) = crate::expr::logical::lower_expr(cond.0.clone()) else {
			return Ok(Self::Uncompiled(cond.0.clone()));
		};
		let compiled =
			crate::exec::vm::serve::expr(exec, logical, Some(Arc::clone(&exec.root().auth)), 0)
				.await?;
		Ok(Self::Compiled(compiled))
	}

	/// Whether the clause accepts `grant`, which is the redacted grant the
	/// statement would otherwise report.
	async fn admits(&self, grant: &Value, exec: &ExecutionContext) -> Result<bool> {
		match self {
			Self::Every => Ok(true),
			Self::Compiled(expr) => {
				let frame = EvalContext::from_exec_ctx(exec).with_value_and_doc(grant);
				Ok(expr.evaluate(frame).await.catch_return()?.is_truthy())
			}
			Self::Uncompiled(expr) => {
				let exec = exec.with_current_row(Arc::new(grant.clone()));
				Ok(crate::ddl::scalar::scalar(&exec, expr).await?.is_truthy())
			}
		}
	}
}

/// Resolve the subject a `GRANT` is issued to.
///
/// A record subject is written as a record-id literal, whose key may itself be
/// an expression, so it is evaluated rather than read: `GRANT FOR RECORD
/// user:$id` names the record `$id` holds.
#[cfg_attr(
	feature = "observability",
	tracing::instrument(level = "trace", name = "Subject::compute", skip_all)
)]
pub(crate) async fn subject_compute(
	this: &Subject,
	exec: &ExecutionContext,
) -> Result<catalog::Subject> {
	match this {
		Subject::Record(record_id_lit) => {
			let expr = Expr::Literal(Literal::RecordId(record_id_lit.clone()));
			let value = crate::ddl::scalar::scalar(exec, &expr).await?;
			record_subject(value)
		}
		Subject::User(ident) => Ok(catalog::Subject::User(ident.to_string())),
	}
}

/// The subject of a grant issued to the record `value` names; a value that names no record is
/// refused.
pub(crate) fn record_subject(value: Value) -> Result<catalog::Subject> {
	let rid: RecordId = value.coerce_to().map_err(|_| {
		anyhow::Error::new(ExecError::InvalidStatement(
			"A grant subject must name a record".to_string(),
		))
	})?;
	Ok(catalog::Subject::Record(rid))
}

/// Resolve the access method named by an access statement under the given base.
///
/// The namespace and database of the base are resolved first, so a missing
/// namespace or database is reported before a missing access method. Every
/// access statement must name an access method that exists under its base.
async fn expect_access(
	base: Base,
	ac: &str,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Arc<catalog::AccessDefinition>> {
	let txn = ctx.tx();
	match base {
		Base::Root => txn.expect_root_access(ac).await,
		Base::Ns => {
			let ns = ctx.expect_ns_id(opt).await?;
			txn.get_ns_access(ns, ac, None).await?.ok_or_else(|| {
				CatalogError::AccessNsNotFound {
					ac: ac.to_owned(),
					// The namespace is expected above
					ns: opt.ns.as_deref().expect("namespace validated by expect_ns_id").to_owned(),
				}
				.into()
			})
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			txn.get_db_access(ns, db, ac, None).await?.ok_or_else(|| {
				CatalogError::AccessDbNotFound {
					ac: ac.to_owned(),
					// The namespace and database are expected above
					ns: opt
						.ns
						.as_deref()
						.expect("namespace validated by expect_ns_db_ids")
						.to_owned(),
					db: opt
						.db
						.as_deref()
						.expect("database validated by expect_ns_db_ids")
						.to_owned(),
				}
				.into()
			})
		}
	}
}

pub(crate) async fn create_grant(
	access: String,
	base: Option<Base>,
	subject: catalog::Subject,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<catalog::AccessGrant> {
	let base = match &base {
		Some(base) => *base,
		None => opt.selected_base()?,
	};
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Access, base)?;
	// Get the transaction.
	let txn = ctx.tx();
	// Clear the cache.
	txn.clear_cache();

	// Read the access definition.
	let ac = expect_access(base, &access, ctx, opt).await?;

	// Verify the access type.
	match &ac.access_type {
		catalog::AccessType::Jwt(_) => {
			Err(anyhow::Error::new(ExecError::Unimplemented(format!("Grants for JWT on {base}"))))
		}
		catalog::AccessType::Record(at) => {
			match &subject {
				catalog::Subject::User(_) => {
					bail!(ExecError::AccessGrantInvalidSubject);
				}
				catalog::Subject::Record(_) => {
					// If the grant is being created for a record, a database must be selected.
					ensure!(matches!(base, Base::Db), ExecError::DbEmpty);
				}
			};
			// The record access type must allow issuing bearer grants.
			let atb = match &at.bearer {
				Some(bearer) => bearer,
				None => bail!(AuthError::AccessMethodMismatch),
			};
			// Create a new bearer key.
			let grant = new_grant_bearer(atb.kind);

			let expiration = ac.grant_duration.map(|d| val::Duration(d) + Datetime::now());

			let gr = catalog::AccessGrant {
				ac: ac.name.to_string(),
				// Unique grant identifier.
				// In the case of bearer grants, the key identifier.
				id: grant.id.clone(),
				// Current time.
				creation: Datetime::now(),
				// Current time plus grant duration. Only if set.
				expiration,
				// The grant is initially not revoked.
				revocation: None,
				// Subject associated with the grant.
				subject,
				// The contents of the grant.
				grant: catalog::Grant::Bearer(grant.clone()),
			};

			// Create the grant.
			// On the very unlikely event of a collision, "put" will return an error.
			if !matches!(base, Base::Db) {
				bail!(ExecError::AccessLevelMismatch);
			}

			// Create a hashed version of the grant for storage.
			let mut gr_store = gr.clone();
			gr_store.grant = catalog::Grant::Bearer(grant.hashed());

			let (ns, db) = ctx.get_ns_db_ids(opt).await?;
			let key = DbGrantKey {
				ns,
				db,
				ac: Cow::Borrowed(&gr.ac),
				gr: Cow::Borrowed(&gr.id),
			};
			let res = txn.put_key(&key, &gr_store).await;

			// Check if a collision was found in order to log a specific error on the server.
			// For an access method with a billion grants, this chance is of only one in 295
			// billion.
			match res {
				Ok(_) => {}
				Err(e) => {
					if matches!(
						storage_error(&e),
						Some(crate::kvs::Error::TransactionKeyAlreadyExists)
					) {
						error!(
							"A collision was found when attempting to create a new grant. Purging inactive grants is advised"
						)
					}
					return Err(e);
				}
			}

			info!(
				"Access method '{}' was used to create grant '{}' of type '{}' for '{}' by '{}'",
				gr.ac,
				gr.id,
				gr.grant.variant(),
				gr.subject.id(),
				opt.auth.id()
			);

			// Return the original version of the grant.
			// This is the only time the plaintext key is returned.
			Ok(gr)
		}
		catalog::AccessType::Bearer(at) => {
			match &subject {
				catalog::Subject::User(user) => {
					// Grant subject must match access method subject.
					ensure!(
						matches!(&at.subject, catalog::BearerAccessSubject::User),
						ExecError::AccessGrantInvalidSubject
					);

					// If the grant is being created for a user, the user must exist.
					match base {
						Base::Root => txn.expect_root_user(user).await?,
						Base::Ns => {
							let ns_id = ctx.get_ns_id(opt).await?;
							txn.get_ns_user(ns_id, user, None).await?.ok_or_else(|| {
								CatalogError::UserNsNotFound {
									name: user.clone(),
									// The namespace id was resolved by get_ns_id above
									ns: opt
										.ns()
										.expect("namespace validated by get_ns_id")
										.to_owned(),
								}
							})?
						}
						Base::Db => {
							let (ns_id, db_id) = ctx.expect_ns_db_ids(opt).await?;
							txn.get_db_user(ns_id, db_id, user, None).await?.ok_or_else(|| {
								CatalogError::UserDbNotFound {
									name: user.clone(),
									// The namespace and database ids were resolved above
									ns: opt
										.ns()
										.expect("namespace validated by expect_ns_db_ids")
										.to_owned(),
									db: opt
										.db()
										.expect("database validated by expect_ns_db_ids")
										.to_owned(),
								}
							})?
						}
					};
				}
				catalog::Subject::Record(_) => {
					// If the grant is being created for a record, a database must be selected.
					ensure!(matches!(base, Base::Db), ExecError::DbEmpty);
					// Grant subject must match access method subject.
					ensure!(
						matches!(&at.subject, catalog::BearerAccessSubject::Record),
						ExecError::AccessGrantInvalidSubject
					);
					// A grant can be created for a record that does not exist yet.
				}
			};
			// Create a new bearer key.
			let grant = new_grant_bearer(at.kind);
			let gr = catalog::AccessGrant {
				ac: ac.name.to_string(),
				// Unique grant identifier.
				// In the case of bearer grants, the key identifier.
				id: grant.id.clone(),
				// Current time.
				creation: Datetime::now(),
				// Current time plus grant duration. Only if set.
				expiration: ac.grant_duration.map(|d| val::Duration(d) + Datetime::now()),
				// The grant is initially not revoked.
				revocation: None,
				// Subject associated with the grant.
				subject,
				// The contents of the grant.
				grant: catalog::Grant::Bearer(grant.clone()),
			};

			// Create the grant.
			// On the very unlikely event of a collision, "put" will return an error.
			// Create a hashed version of the grant for storage.
			let mut gr_store = gr.clone();
			gr_store.grant = catalog::Grant::Bearer(grant.hashed());
			let res = match base {
				Base::Root => {
					let key = RootGrantKey {
						ac: Cow::Borrowed(&gr.ac),
						gr: Cow::Borrowed(&gr.id),
					};
					txn.put_key(&key, &gr_store).await
				}
				Base::Ns => {
					let ns = txn.get_or_add_ns(Some(ctx), opt.ns()?).await?;
					let key = NsGrantKey {
						ns: ns.namespace_id,
						ac: Cow::Borrowed(&gr.ac),
						gr: Cow::Borrowed(&gr.id),
					};
					txn.put_key(&key, &gr_store).await
				}
				Base::Db => {
					let (ns, db) = opt.ns_db()?;
					let db = txn.get_or_add_db(Some(ctx), ns, db).await?;
					let key = DbGrantKey {
						ns: db.namespace_id,
						db: db.database_id,
						ac: Cow::Borrowed(&gr.ac),
						gr: Cow::Borrowed(&gr.id),
					};

					txn.put_key(&key, &gr_store).await
				}
			};

			// Check if a collision was found in order to log a specific error on the server.
			// For an access method with a billion grants, this chance is of only one in 295
			// billion.
			match res {
				Ok(_) => {}
				Err(e) => {
					if matches!(
						storage_error(&e),
						Some(crate::kvs::Error::TransactionKeyAlreadyExists)
					) {
						error!(
							"A collision was found when attempting to create a new grant. Purging inactive grants is advised"
						)
					}
					return Err(e);
				}
			}

			info!(
				"Access method '{}' was used to create grant '{}' of type '{}' for '{}' by '{}'",
				gr.ac,
				gr.id,
				gr.grant.variant(),
				gr.subject.id(),
				opt.auth.id()
			);

			// Return the original version of the grant.
			// This is the only time the plaintext key is returned.
			Ok(gr)
		}
	}
}

pub(crate) async fn compute_grant(
	stmt: &AccessStatementGrant,
	ctx: &FrozenContext,
	opt: &Options,
	exec: &ExecutionContext,
) -> Result<Value> {
	let subject = subject_compute(&stmt.subject, exec).await?;

	let grant = create_grant(stmt.ac.to_string(), stmt.base, subject, ctx, opt).await?;

	Ok(Value::Object(access_object_from_grant(&grant)))
}

pub(crate) async fn compute_show(
	stmt: &AccessStatementShow,
	ctx: &FrozenContext,
	opt: &Options,
	exec: &ExecutionContext,
) -> Result<Value> {
	let base = access_method_base(stmt.base, stmt.ac.as_str(), Action::View, ctx, opt).await?;
	match &stmt.gr {
		Some(gr) => shown_grant(base, stmt.ac.as_str(), gr.as_str(), ctx, opt).await,
		None => {
			let grs = grants_at(base, stmt.ac.as_str(), ctx, opt).await?;
			let filter = GrantFilter::resolve(stmt.cond.as_ref(), exec).await?;
			let mut show = Vec::new();
			for gr in grs.iter() {
				let redacted_gr = shown(gr);
				if !filter.admits(&redacted_gr, exec).await? {
					continue;
				}
				show.push(redacted_gr);
			}
			Ok(Value::Array(show.into()))
		}
	}
}

/// Resolves the base an `ACCESS ... SHOW`, `REVOKE` or `PURGE` statement addresses. The checks run
/// in this order: the base `base` names (or the session's selected base), the session's permission
/// to take `action` at that base, and that access method `ac` exists under it. The catalog cache is
/// cleared before the last check.
pub(crate) async fn access_method_base(
	base: Option<Base>,
	ac: &str,
	action: Action,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Base> {
	let base = match base {
		Some(base) => base,
		None => opt.selected_base()?,
	};
	// Allowed to run?
	ctx.is_allowed(opt, action, ResourceKind::Access, base)?;
	// Clear the cache.
	ctx.tx().clear_cache();
	// Check if the access method exists.
	expect_access(base, ac, ctx, opt).await?;
	Ok(base)
}

/// Every grant access method `ac` holds at `base`, in key order.
pub(crate) async fn grants_at(
	base: Base,
	ac: &str,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Arc<[catalog::AccessGrant]>> {
	let txn = ctx.tx();
	match base {
		Base::Root => txn.all_root_access_grants(ac, None).await,
		Base::Ns => {
			let ns = ctx.expect_ns_id(opt).await?;
			txn.all_ns_access_grants(ns, ac, None).await
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			txn.all_db_access_grants(ns, db, ac, None).await
		}
	}
}

/// Grant `grant` as a statement shows it: redacted, so its plaintext key is never shown.
pub(crate) fn shown(grant: &catalog::AccessGrant) -> Value {
	Value::Object(access_object_from_grant(&grant.clone().redacted()))
}

/// Grant `gr` of access method `ac` at `base`, shown; a grant that does not exist is the error
/// naming it at that base.
pub(crate) async fn shown_grant(
	base: Base,
	ac: &str,
	gr: &str,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let txn = ctx.tx();
	let grant = match base {
		Base::Root => match txn.get_root_access_grant(ac, gr, None).await? {
			Some(val) => val,
			None => bail!(CatalogError::AccessGrantRootNotFound {
				ac: ac.to_owned(),
				gr: gr.to_owned(),
			}),
		},
		Base::Ns => {
			let ns = ctx.expect_ns_id(opt).await?;
			match txn.get_ns_access_grant(ns, ac, gr, None).await? {
				Some(val) => val,
				None => bail!(CatalogError::AccessGrantNsNotFound {
					ac: ac.to_owned(),
					gr: gr.to_owned(),
					ns: ns.to_string(),
				}),
			}
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			match txn.get_db_access_grant(ns, db, ac, gr, None).await? {
				Some(val) => val,
				None => bail!(CatalogError::AccessGrantDbNotFound {
					ac: ac.to_owned(),
					gr: gr.to_owned(),
					ns: ns.to_string(),
					db: db.to_string(),
				}),
			}
		}
	};
	Ok(shown(&grant))
}

pub(crate) async fn revoke_grant(
	stmt: &AccessStatementRevoke,
	ctx: &FrozenContext,
	opt: &Options,
	exec: &ExecutionContext,
) -> Result<Value> {
	let base = access_method_base(stmt.base, stmt.ac.as_str(), Action::Edit, ctx, opt).await?;
	// Get the grants to revoke.
	let mut revoked = Vec::new();
	match &stmt.gr {
		Some(gr) => {
			revoked.push(revoke_named(base, stmt.ac.as_str(), gr.as_str(), ctx, opt).await?)
		}
		None => {
			let grs = grants_at(base, stmt.ac.as_str(), ctx, opt).await?;
			let filter = GrantFilter::resolve(stmt.cond.as_ref(), exec).await?;
			for gr in grs.iter() {
				// If the grant is already revoked, it cannot be revoked again.
				if gr.revocation.is_some() {
					continue;
				}
				if !filter.admits(&shown(gr), exec).await? {
					continue;
				}
				revoked.push(revoke_held(base, stmt.ac.as_str(), gr, ctx, opt).await?);
			}
		}
	}

	// Return revoked grants.
	Ok(Value::Array(revoked.into()))
}

/// Revokes grant `gr` of access method `ac` at `base`, answering it shown as revoked. A grant
/// that does not exist is the error naming it at that base, and one already revoked is
/// [`ExecError::AccessGrantRevoked`].
pub(crate) async fn revoke_named(
	base: Base,
	ac: &str,
	gr: &str,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let txn = ctx.tx();
	let mut revoke = match base {
		Base::Root => match txn.get_root_access_grant(ac, gr, None).await? {
			Some(val) => (*val).clone(),
			None => bail!(CatalogError::AccessGrantRootNotFound {
				ac: ac.to_owned(),
				gr: gr.to_owned(),
			}),
		},
		Base::Ns => {
			let ns = ctx.expect_ns_id(opt).await?;
			match txn.get_ns_access_grant(ns, ac, gr, None).await? {
				Some(val) => (*val).clone(),
				None => {
					let ns = opt.ns()?;
					bail!(CatalogError::AccessGrantNsNotFound {
						ac: ac.to_owned(),
						gr: gr.to_owned(),
						ns: ns.to_string(),
					})
				}
			}
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			match txn.get_db_access_grant(ns, db, ac, gr, None).await? {
				Some(val) => (*val).clone(),
				None => {
					let (ns, db) = opt.ns_db()?;
					bail!(CatalogError::AccessGrantDbNotFound {
						ac: ac.to_owned(),
						gr: gr.to_owned(),
						ns: ns.to_string(),
						db: db.to_string(),
					})
				}
			}
		}
	};
	ensure!(revoke.revocation.is_none(), ExecError::AccessGrantRevoked);
	revoke.revocation = Some(Datetime::now());
	store_revoked(base, ac, &revoke, ctx, opt).await?;
	Ok(shown(&revoke))
}

/// Revokes `grant`, one of the grants access method `ac` holds at `base` and not yet revoked,
/// answering it shown as revoked.
pub(crate) async fn revoke_held(
	base: Base,
	ac: &str,
	grant: &catalog::AccessGrant,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let mut gr = grant.clone();
	gr.revocation = Some(Datetime::now());
	store_revoked(base, ac, &gr, ctx, opt).await?;
	Ok(shown(&gr))
}

/// Stores `gr`, which carries its revocation, under access method `ac` at `base`, and logs the
/// revocation.
async fn store_revoked(
	base: Base,
	ac: &str,
	gr: &catalog::AccessGrant,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<()> {
	let txn = ctx.tx();
	match base {
		Base::Root => {
			let key = RootGrantKey {
				ac: Cow::Borrowed(ac),
				gr: Cow::Borrowed(&gr.id),
			};
			txn.set_key(&key, gr).await?;
		}
		Base::Ns => {
			let ns = txn.get_or_add_ns(Some(ctx), opt.ns()?).await?;
			let key = NsGrantKey {
				ns: ns.namespace_id,
				ac: Cow::Borrowed(ac),
				gr: Cow::Borrowed(&gr.id),
			};
			txn.set_key(&key, gr).await?;
		}
		Base::Db => {
			let (ns, db) = opt.ns_db()?;
			let db = txn.get_or_add_db(Some(ctx), ns, db).await?;
			let key = DbGrantKey {
				ns: db.namespace_id,
				db: db.database_id,
				ac: Cow::Borrowed(ac),
				gr: Cow::Borrowed(&gr.id),
			};
			txn.set_key(&key, gr).await?;
		}
	};

	info!(
		"Access method '{}' was used to revoke grant '{}' of type '{}' for '{}' by '{}'",
		gr.ac,
		gr.id,
		gr.grant.variant(),
		gr.subject.id(),
		opt.auth.id()
	);
	Ok(())
}

pub(crate) async fn compute_revoke(
	stmt: &AccessStatementRevoke,
	ctx: &FrozenContext,
	opt: &Options,
	exec: &ExecutionContext,
) -> Result<Value> {
	let revoked = revoke_grant(stmt, ctx, opt, exec).await?;
	Ok(Value::Array(vec![revoked].into()))
}

pub(crate) async fn compute_purge(
	stmt: &AccessStatementPurge,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	purge_grants(stmt.base, stmt.ac.as_str(), &stmt.kind, stmt.grace.0, ctx, opt).await
}

/// Deletes the grants of access method `ac` at `base` (or the session's selected base) that
/// expired, were revoked, or both as `kind` says, more than `grace` before now, answering each
/// deleted grant shown. A grant expired or revoked at a later time, or within the grace, is kept.
pub(crate) async fn purge_grants(
	base: Option<Base>,
	ac: &str,
	kind: &PurgeKind,
	grace: std::time::Duration,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let base = access_method_base(base, ac, Action::Edit, ctx, opt).await?;
	let txn = ctx.tx();
	// Get all grants to purge.
	let mut purged = Array::new();
	let grs = grants_at(base, ac, ctx, opt).await?;
	for gr in grs.iter() {
		// Decide whether the grant should be purged, based on expiration or revocation.
		let now = Datetime::now();
		// Saturating subtraction keeps the cast to unsigned non-negative.
		// Revocation times should never exceed the current time.
		// Grants expired or revoked at a future time will not be purged.
		// Grants expired or revoked at exactly the current second will not be purged.
		let purge_expired = matches!(kind, PurgeKind::Expired | PurgeKind::Both)
			&& gr.expiration.as_ref().is_some_and(|exp| {
				                 now.timestamp() >= exp.timestamp() // Prevent saturating when not expired yet.
				                     && (now.timestamp().saturating_sub(exp.timestamp()) as u64) > grace.as_secs()
				             });
		let purge_revoked = matches!(kind, PurgeKind::Revoked | PurgeKind::Both)
			&& gr.revocation.as_ref().is_some_and(|rev| {
				                 now.timestamp() >= rev.timestamp() // Prevent saturating when not revoked yet.
				                     && (now.timestamp().saturating_sub(rev.timestamp()) as u64) > grace.as_secs()
				             });
		// Delete the grant and append its redacted form to the result.
		if purge_expired || purge_revoked {
			match base {
				Base::Root => {
					let key = RootGrantKey {
						ac: Cow::Borrowed(ac),
						gr: Cow::Borrowed(&gr.id),
					};
					txn.del_key(&key).await?
				}
				Base::Ns => {
					let ns = ctx.expect_ns_id(opt).await?;
					let key = NsGrantKey {
						ns,
						ac: Cow::Borrowed(ac),
						gr: Cow::Borrowed(&gr.id),
					};

					txn.del_key(&key).await?
				}
				Base::Db => {
					let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
					let key = DbGrantKey {
						ns,
						db,
						ac: Cow::Borrowed(ac),
						gr: Cow::Borrowed(&gr.id),
					};

					txn.del_key(&key).await?
				}
			};

			info!(
				"Access method '{}' was used to purge grant '{}' of type '{}' for '{}' by '{}'",
				gr.ac,
				gr.id,
				gr.grant.variant(),
				gr.subject.id(),
				opt.auth.id()
			);

			purged.push(shown(gr));
		}
	}

	Ok(Value::Array(purged))
}

/// Run one `ACCESS` statement.
///
/// The statement is top-level only, so the frame it resolves its expressions
/// in encloses no row. The frame is built once and shared by every expression
/// the statement resolves.
pub(crate) async fn access_statement_compute(
	this: &AccessStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let exec = crate::ddl::frame(ctx, opt, None).await;
	match this {
		AccessStatement::Grant(stmt) => compute_grant(stmt, ctx, opt, &exec).await,
		AccessStatement::Show(stmt) => compute_show(stmt, ctx, opt, &exec).await,
		AccessStatement::Revoke(stmt) => compute_revoke(stmt, ctx, opt, &exec).await,
		AccessStatement::Purge(stmt) => compute_purge(stmt, ctx, opt).await,
	}
}

pub(crate) fn random_string(length: usize, pool: &[u8]) -> String {
	let mut rng = rand::rng();
	let string: String = (0..length)
		.map(|_| {
			let i = rng.random_range(0..pool.len());
			pool[i] as char
		})
		.collect();
	string
}

pub(crate) fn new_grant_bearer(ty: catalog::BearerAccessType) -> catalog::GrantBearer {
	let id = format!(
		"{}{}",
		// The pool for the first character of the key identifier excludes digits.
		random_string(1, &GRANT_BEARER_CHARACTER_POOL[10..]),
		random_string(GRANT_BEARER_ID_LENGTH - 1, GRANT_BEARER_CHARACTER_POOL)
	);
	let secret = random_string(GRANT_BEARER_KEY_LENGTH, GRANT_BEARER_CHARACTER_POOL);
	let prefix = match ty {
		catalog::BearerAccessType::Bearer => "surreal-bearer",
		catalog::BearerAccessType::Refresh => "surreal-refresh",
	};

	let key = format!("{prefix}-{id}-{secret}");

	catalog::GrantBearer {
		id,
		key,
	}
}

/// Returns the SurrealQL object form of the access grant.
pub(crate) fn access_object_from_grant(grant: &catalog::AccessGrant) -> Object {
	let mut res = Object::default();
	res.insert("id".to_owned(), Value::from(grant.id.clone()));
	res.insert("ac".to_owned(), Value::from(grant.ac.clone()));
	res.insert("type".to_owned(), Value::from(grant.grant.variant()));
	res.insert("creation".to_owned(), Value::from(grant.creation));
	res.insert("expiration".to_owned(), grant.expiration.map(Value::from).unwrap_or(Value::None));
	res.insert("revocation".to_owned(), grant.revocation.map(Value::from).unwrap_or(Value::None));
	let mut sub = Object::default();
	match &grant.subject {
		catalog::Subject::Record(id) => sub.insert("record".to_owned(), Value::from(id.clone())),
		catalog::Subject::User(name) => sub.insert("user".to_owned(), Value::from(name.clone())),
	};
	res.insert("subject".to_owned(), Value::from(sub));

	let mut gr = Object::default();
	match &grant.grant {
		catalog::Grant::Jwt(jg) => {
			gr.insert("jti".to_owned(), Value::from(val::Uuid(jg.jti)));
			if let Some(token) = &jg.token {
				gr.insert("token".to_owned(), Value::from(token.clone()));
			}
		}
		catalog::Grant::Record(rg) => {
			gr.insert("rid".to_owned(), Value::from(val::Uuid(rg.rid)));
			gr.insert("jti".to_owned(), Value::from(val::Uuid(rg.jti)));
			if let Some(token) = &rg.token {
				gr.insert("token".to_owned(), Value::from(token.clone()));
			}
		}
		catalog::Grant::Bearer(bg) => {
			gr.insert("id".to_owned(), Value::from(bg.id.clone()));
			gr.insert("key".to_owned(), Value::from(bg.key.clone()));
		}
	};
	res.insert("grant".to_owned(), Value::from(gr));

	res
}
