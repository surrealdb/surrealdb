use std::borrow::Cow;

use anyhow::{Result, bail, ensure};
use rand::Rng;
use reblessive::tree::Stk;

use crate::catalog::Error as CatalogError;
use crate::catalog::providers::{
	AuthorisationProvider, CatalogProvider, NamespaceProvider, UserProvider,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::statements::access::{
	AccessStatement, AccessStatementGrant, AccessStatementPurge, AccessStatementRevoke,
	AccessStatementShow, GRANT_BEARER_CHARACTER_POOL, GRANT_BEARER_ID_LENGTH,
	GRANT_BEARER_KEY_LENGTH, PurgeKind, Subject,
};
use crate::expr::{Base, ControlFlow, FlowResult};
use crate::iam::{Action, Error as AuthError, ResourceKind};
use crate::key::schema::{DbGrantKey, NsGrantKey, RootGrantKey};
use crate::val::{Array, Datetime, Object, Value};
use crate::{catalog, val};

#[instrument(level = "trace", name = "Subject::compute", skip_all)]
pub(crate) async fn subject_compute(
	this: &Subject,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<catalog::Subject> {
	match this {
		Subject::Record(record_id_lit) => Ok(catalog::Subject::Record(
			crate::legacy::record_id_lit_compute(record_id_lit, stk, ctx, opt, doc).await?,
		)),
		Subject::User(ident) => Ok(catalog::Subject::User(ident.to_string())),
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
	let ac = match base {
		Base::Root => txn.expect_root_access(&access).await?,
		Base::Ns => {
			let ns = ctx.expect_ns_id(opt).await?;
			txn.get_ns_access(ns, &access, None).await?.ok_or_else(|| {
				CatalogError::AccessNsNotFound {
					ac: access.clone(),
					// The namespace is expected above
					ns: opt.ns.as_deref().expect("namespace validated by expect_ns_id").to_owned(),
				}
			})?
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			txn.get_db_access(ns, db, &access, None).await?.ok_or_else(|| {
				CatalogError::AccessDbNotFound {
					ac: access.clone(),
					// The namespace and database is expected above
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
			})?
		}
	};

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
			let res = match base {
				Base::Db => {
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
					txn.put_key(&key, &gr_store).await
				}
				_ => bail!(ExecError::AccessLevelMismatch),
			};

			// Check if a collision was found in order to log a specific error on the server.
			// For an access method with a billion grants, this chance is of only one in 295
			// billion.
			match res {
				Ok(_) => {}
				Err(e) => {
					if matches!(
						e.downcast_ref(),
						Some(Error::Kvs(crate::kvs::Error::TransactionKeyAlreadyExists))
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
									// We just retrieved the ns_id above
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
									// We just retrieved the ns_id and db_id above
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
						e.downcast_ref(),
						Some(Error::Kvs(crate::kvs::Error::TransactionKeyAlreadyExists))
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
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	let subject = crate::legacy::subject_compute(&stmt.subject, stk, ctx, opt, doc).await?;

	let grant = create_grant(stmt.ac.to_string(), stmt.base, subject, ctx, opt).await?;

	Ok(Value::Object(access_object_from_grant(&grant)))
}

pub(crate) async fn compute_show(
	stmt: &AccessStatementShow,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value> {
	let base = match &stmt.base {
		Some(base) => *base,
		None => opt.selected_base()?,
	};
	// Allowed to run?
	ctx.is_allowed(opt, Action::View, ResourceKind::Access, base)?;
	// Get the transaction.
	let txn = ctx.tx();
	// Clear the cache.
	txn.clear_cache();
	// Check if the access method exists.
	match base {
		Base::Root => {
			txn.expect_root_access(stmt.ac.as_str()).await?;
		}
		Base::Ns => {
			let ns = ctx.expect_ns_id(opt).await?;
			if txn.get_ns_access(ns, stmt.ac.as_str(), None).await?.is_none() {
				bail!(CatalogError::AccessNsNotFound {
					ac: stmt.ac.to_string(),
					// We expected a namespace above
					ns: opt.ns.as_deref().expect("namespace validated by expect_ns_id").to_owned(),
				});
			}
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			// We expected a namespace above
			if txn.get_db_access(ns, db, stmt.ac.as_str(), None).await?.is_none() {
				bail!(CatalogError::AccessDbNotFound {
					ac: stmt.ac.to_string(),
					// We expected a namespace and database above
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
				});
			}
		}
	};

	// Get the grants to show.
	match &stmt.gr {
		Some(gr) => {
			let grant = match base {
				Base::Root => {
					match txn.get_root_access_grant(stmt.ac.as_str(), gr.as_str(), None).await? {
						Some(val) => val,
						None => bail!(CatalogError::AccessGrantRootNotFound {
							ac: stmt.ac.to_string(),
							gr: gr.to_string(),
						}),
					}
				}
				Base::Ns => {
					let ns = ctx.expect_ns_id(opt).await?;
					match txn.get_ns_access_grant(ns, stmt.ac.as_str(), gr.as_str(), None).await? {
						Some(val) => val,
						None => bail!(CatalogError::AccessGrantNsNotFound {
							ac: stmt.ac.to_string(),
							gr: gr.to_string(),
							ns: ns.to_string(),
						}),
					}
				}
				Base::Db => {
					let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
					match txn
						.get_db_access_grant(ns, db, stmt.ac.as_str(), gr.as_str(), None)
						.await?
					{
						Some(val) => val,
						None => bail!(CatalogError::AccessGrantDbNotFound {
							ac: stmt.ac.to_string(),
							gr: gr.to_string(),
							ns: ns.to_string(),
							db: db.to_string(),
						}),
					}
				}
			};

			Ok(Value::Object(access_object_from_grant(&(*grant).clone().redacted())))
		}
		None => {
			// Get all grants.
			let grs = match base {
				Base::Root => txn.all_root_access_grants(stmt.ac.as_str(), None).await?,
				Base::Ns => {
					let ns = ctx.expect_ns_id(opt).await?;
					txn.all_ns_access_grants(ns, stmt.ac.as_str(), None).await?
				}
				Base::Db => {
					let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
					txn.all_db_access_grants(ns, db, stmt.ac.as_str(), None).await?
				}
			};

			let mut show = Vec::new();
			for gr in grs.iter() {
				let redacted_gr = Value::Object(access_object_from_grant(&gr.clone().redacted()));
				// If provided, check if grant matches conditions.
				if let Some(cond) = &stmt.cond {
					// Redact grant before evaluating conditions.
					if !stk
						.run(|stk| async {
							crate::legacy::expr_compute(
								&cond.0,
								stk,
								ctx,
								opt,
								Some(&CursorDoc {
									rid: None,
									ir: None,
									doc: redacted_gr.clone().into(),
									fields_computed: false,
								}),
							)
							.await
						})
						.await
						.catch_return()?
						.is_truthy()
					{
						// Skip grant if it does not match the provided conditions.
						continue;
					}
				}

				// Store revoked version of the redacted grant.
				show.push(redacted_gr);
			}

			Ok(Value::Array(show.into()))
		}
	}
}

pub(crate) async fn revoke_grant(
	stmt: &AccessStatementRevoke,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let base = match &stmt.base {
		Some(base) => *base,
		None => opt.selected_base()?,
	};
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Access, base)?;
	// Get the transaction
	let txn = ctx.tx();
	// Clear the cache
	txn.clear_cache();
	// Check if the access method exists.
	match base {
		Base::Root => txn.get_root_access(stmt.ac.as_str(), None).await?,
		Base::Ns => {
			let ns = ctx.expect_ns_id(opt).await?;
			txn.get_ns_access(ns, stmt.ac.as_str(), None).await?
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			txn.get_db_access(ns, db, stmt.ac.as_str(), None).await?
		}
	};

	// Get the grants to revoke.
	let mut revoked = Vec::new();
	match &stmt.gr {
		Some(gr) => {
			let mut revoke = match base {
				Base::Root => {
					match txn.get_root_access_grant(stmt.ac.as_str(), gr.as_str(), None).await? {
						Some(val) => (*val).clone(),
						None => bail!(CatalogError::AccessGrantRootNotFound {
							ac: stmt.ac.to_string(),
							gr: gr.to_string(),
						}),
					}
				}
				Base::Ns => {
					let ns = ctx.expect_ns_id(opt).await?;
					match txn.get_ns_access_grant(ns, stmt.ac.as_str(), gr.as_str(), None).await? {
						Some(val) => (*val).clone(),
						None => {
							let ns = opt.ns()?;
							bail!(CatalogError::AccessGrantNsNotFound {
								ac: stmt.ac.to_string(),
								gr: gr.to_string(),
								ns: ns.to_string(),
							})
						}
					}
				}
				Base::Db => {
					let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
					match txn
						.get_db_access_grant(ns, db, stmt.ac.as_str(), gr.as_str(), None)
						.await?
					{
						Some(val) => (*val).clone(),
						None => {
							let (ns, db) = opt.ns_db()?;
							bail!(CatalogError::AccessGrantDbNotFound {
								ac: stmt.ac.to_string(),
								gr: gr.to_string(),
								ns: ns.to_string(),
								db: db.to_string(),
							})
						}
					}
				}
			};
			ensure!(revoke.revocation.is_none(), ExecError::AccessGrantRevoked);
			revoke.revocation = Some(Datetime::now());

			// Revoke the grant.
			match base {
				Base::Root => {
					let key = RootGrantKey {
						ac: Cow::Borrowed(&stmt.ac),
						gr: Cow::Borrowed(gr),
					};
					txn.set_key(&key, &revoke).await?;
				}
				Base::Ns => {
					let ns = txn.get_or_add_ns(Some(ctx), opt.ns()?).await?;
					let key = NsGrantKey {
						ns: ns.namespace_id,
						ac: Cow::Borrowed(&stmt.ac),
						gr: Cow::Borrowed(gr),
					};
					txn.set_key(&key, &revoke).await?;
				}
				Base::Db => {
					let (ns, db) = opt.ns_db()?;
					let db = txn.get_or_add_db(Some(ctx), ns, db).await?;

					let key = DbGrantKey {
						ns: db.namespace_id,
						db: db.database_id,
						ac: Cow::Borrowed(&stmt.ac),
						gr: Cow::Borrowed(gr),
					};
					txn.set_key(&key, &revoke).await?;
				}
			};

			info!(
				"Access method '{}' was used to revoke grant '{}' of type '{}' for '{}' by '{}'",
				revoke.ac,
				revoke.id,
				revoke.grant.variant(),
				revoke.subject.id(),
				opt.auth.id()
			);

			revoked.push(Value::Object(access_object_from_grant(&revoke.redacted())));
		}
		None => {
			// Get all grants.
			let grs = match base {
				Base::Root => txn.all_root_access_grants(stmt.ac.as_str(), None).await?,
				Base::Ns => {
					let ns = ctx.expect_ns_id(opt).await?;
					txn.all_ns_access_grants(ns, stmt.ac.as_str(), None).await?
				}
				Base::Db => {
					let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
					txn.all_db_access_grants(ns, db, stmt.ac.as_str(), None).await?
				}
			};

			for gr in grs.iter() {
				// If the grant is already revoked, it cannot be revoked again.
				if gr.revocation.is_some() {
					continue;
				}

				let redacted_gr = Value::Object(access_object_from_grant(&gr.clone().redacted()));
				// If provided, check if grant matches conditions.
				if let Some(cond) = &stmt.cond {
					// Redact grant before evaluating conditions.
					if !stk
						.run(|stk| async {
							crate::legacy::expr_compute(
								&cond.0,
								stk,
								ctx,
								opt,
								Some(&CursorDoc {
									rid: None,
									ir: None,
									doc: redacted_gr.into(),
									fields_computed: false,
								}),
							)
							.await
						})
						.await
						.catch_return()?
						.is_truthy()
					{
						// Skip grant if it does not match the provided conditions.
						continue;
					}
				}

				let mut gr = gr.clone();
				gr.revocation = Some(Datetime::now());
				// recreate now that the revocation is set.
				let redacted_gr = Value::Object(access_object_from_grant(&gr.clone().redacted()));

				// Revoke the grant.
				match base {
					Base::Root => {
						let key = RootGrantKey {
							ac: Cow::Borrowed(&stmt.ac),
							gr: Cow::Borrowed(&gr.id),
						};
						txn.set_key(&key, &gr).await?;
					}
					Base::Ns => {
						let ns = txn.get_or_add_ns(Some(ctx), opt.ns()?).await?;
						let key = NsGrantKey {
							ns: ns.namespace_id,
							ac: Cow::Borrowed(&stmt.ac),
							gr: Cow::Borrowed(&gr.id),
						};
						txn.set_key(&key, &gr).await?;
					}
					Base::Db => {
						let (ns, db) = opt.ns_db()?;
						let db = txn.get_or_add_db(Some(ctx), ns, db).await?;

						let key = DbGrantKey {
							ns: db.namespace_id,
							db: db.database_id,
							ac: Cow::Borrowed(&stmt.ac),
							gr: Cow::Borrowed(&gr.id),
						};
						txn.set_key(&key, &gr).await?;
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

				// Store revoked version of the redacted grant.
				revoked.push(redacted_gr);
			}
		}
	}

	// Return revoked grants.
	Ok(Value::Array(revoked.into()))
}

pub(crate) async fn compute_revoke(
	stmt: &AccessStatementRevoke,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value> {
	let revoked = revoke_grant(stmt, stk, ctx, opt).await?;
	Ok(Value::Array(vec![revoked].into()))
}

pub(crate) async fn compute_purge(
	stmt: &AccessStatementPurge,
	ctx: &FrozenContext,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value> {
	let base = match &stmt.base {
		Some(base) => *base,
		None => opt.selected_base()?,
	};
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Access, base)?;
	// Get the transaction.
	let txn = ctx.tx();
	// Clear the cache.
	txn.clear_cache();
	// Check if the access method exists.
	match base {
		Base::Root => txn.get_root_access(stmt.ac.as_str(), None).await?,
		Base::Ns => {
			let ns = ctx.get_ns_id(opt).await?;
			txn.get_ns_access(ns, stmt.ac.as_str(), None).await?
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			txn.get_db_access(ns, db, stmt.ac.as_str(), None).await?
		}
	};
	// Get all grants to purge.
	let mut purged = Array::new();
	let grs = match base {
		Base::Root => txn.all_root_access_grants(stmt.ac.as_str(), None).await?,
		Base::Ns => {
			let ns = ctx.get_ns_id(opt).await?;
			txn.all_ns_access_grants(ns, stmt.ac.as_str(), None).await?
		}
		Base::Db => {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			txn.all_db_access_grants(ns, db, stmt.ac.as_str(), None).await?
		}
	};
	for gr in grs.iter() {
		// Determine if the grant should purged based on expiration or revocation.
		let now = Datetime::now();
		// We can convert to unsigned integer as substraction is saturating.
		// Revocation times should never exceed the current time.
		// Grants expired or revoked at a future time will not be purged.
		// Grants expired or revoked at exactly the current second will not be purged.
		let purge_expired = matches!(stmt.kind, PurgeKind::Expired | PurgeKind::Both)
			&& gr.expiration.as_ref().is_some_and(|exp| {
				                 now.timestamp() >= exp.timestamp() // Prevent saturating when not expired yet.
				                     && (now.timestamp().saturating_sub(exp.timestamp()) as u64) > stmt.grace.secs()
				             });
		let purge_revoked = matches!(stmt.kind, PurgeKind::Revoked | PurgeKind::Both)
			&& gr.revocation.as_ref().is_some_and(|rev| {
				                 now.timestamp() >= rev.timestamp() // Prevent saturating when not revoked yet.
				                     && (now.timestamp().saturating_sub(rev.timestamp()) as u64) > stmt.grace.secs()
				             });
		// If it should, delete the grant and append the redacted version to the result.
		if purge_expired || purge_revoked {
			match base {
				Base::Root => {
					let key = RootGrantKey {
						ac: Cow::Borrowed(&stmt.ac),
						gr: Cow::Borrowed(&gr.id),
					};
					txn.del_key(&key).await?
				}
				Base::Ns => {
					let ns = ctx.get_ns_id(opt).await?;
					let key = NsGrantKey {
						ns,
						ac: Cow::Borrowed(&stmt.ac),
						gr: Cow::Borrowed(&gr.id),
					};

					txn.del_key(&key).await?
				}
				Base::Db => {
					let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
					let key = DbGrantKey {
						ns,
						db,
						ac: Cow::Borrowed(&stmt.ac),
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

			purged.push(Value::Object(access_object_from_grant(&gr.clone().redacted())));
		}
	}

	Ok(Value::Array(purged))
}

/// Process this type returning a computed simple Value
pub(crate) async fn access_statement_compute(
	this: &AccessStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	match this {
		AccessStatement::Grant(stmt) => compute_grant(stmt, stk, ctx, opt, doc).await,
		AccessStatement::Show(stmt) => {
			compute_show(stmt, stk, ctx, opt, doc).await.map_err(ControlFlow::Err)
		}
		AccessStatement::Revoke(stmt) => {
			compute_revoke(stmt, stk, ctx, opt, doc).await.map_err(ControlFlow::Err)
		}
		AccessStatement::Purge(stmt) => {
			compute_purge(stmt, ctx, opt, doc).await.map_err(ControlFlow::Err)
		}
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

/// Returns the surrealql object representation of the access grant
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
