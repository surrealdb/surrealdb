use std::borrow::Cow;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::providers::{AuthorisationProvider, NamespaceProvider};
use crate::catalog::{self, Error as CatalogError, ExprText};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::access_type::{
	BearerAccess, BearerAccessSubject, BearerAccessType, JwtAccessVerify,
};
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::access::DefineAccessStatement;
use crate::expr::{AccessType, Algorithm, Base, JwtAccess};
use crate::iam::{Action, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::legacy::expr_to_ident;
use crate::val::{Duration, Value};

pub(crate) async fn define_access_statement_to_definition(
	this: &DefineAccessStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<catalog::AccessDefinition> {
	fn convert_algorithm(access: Algorithm) -> catalog::Algorithm {
		match &access {
			Algorithm::EdDSA => catalog::Algorithm::EdDSA,
			Algorithm::Es256 => catalog::Algorithm::Es256,
			Algorithm::Es384 => catalog::Algorithm::Es384,
			Algorithm::Es512 => catalog::Algorithm::Es512,
			Algorithm::Hs256 => catalog::Algorithm::Hs256,
			Algorithm::Hs384 => catalog::Algorithm::Hs384,
			Algorithm::Hs512 => catalog::Algorithm::Hs512,
			Algorithm::Ps256 => catalog::Algorithm::Ps256,
			Algorithm::Ps384 => catalog::Algorithm::Ps384,
			Algorithm::Ps512 => catalog::Algorithm::Ps512,
			Algorithm::Rs256 => catalog::Algorithm::Rs256,
			Algorithm::Rs384 => catalog::Algorithm::Rs384,
			Algorithm::Rs512 => catalog::Algorithm::Rs512,
		}
	}

	async fn convert_jwt_access(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		access: &JwtAccess,
	) -> Result<catalog::JwtAccess> {
		let verify = match &access.verify {
			JwtAccessVerify::Key(k) => catalog::JwtAccessVerify::Key(catalog::JwtAccessVerifyKey {
				alg: convert_algorithm(k.alg),
				key: stk
					.run(|stk| crate::legacy::expr_compute(&k.key, stk, ctx, opt, doc))
					.await
					.catch_return()?
					.coerce_to::<String>()?,
			}),
			JwtAccessVerify::Jwks(j) => {
				catalog::JwtAccessVerify::Jwks(catalog::JwtAccessVerifyJwks {
					url: stk
						.run(|stk| crate::legacy::expr_compute(&j.url, stk, ctx, opt, doc))
						.await
						.catch_return()?
						.cast_to()?,
				})
			}
		};

		let issue = map_opt!(x as &access.issue => catalog::JwtAccessIssue {
			alg: convert_algorithm(x.alg),
			key: stk.run(|stk| crate::legacy::expr_compute(&x.key, stk, ctx, opt, doc)).await.catch_return()?.cast_to()?,
		});

		// Validate symmetric algorithm key consistency
		if let (catalog::JwtAccessVerify::Key(ver), Some(iss)) = (&verify, &issue)
			&& ver.alg.is_symmetric()
			&& ver.key != iss.key
		{
			bail!(ExecError::Query {
				message: format!(
					"Symmetric algorithm {} requires the same key for signing and verification. \
						Use the same key value for both KEY and WITH ISSUER KEY clauses, or omit WITH ISSUER KEY.",
					ver.alg
				)
			});
		}

		Ok(catalog::JwtAccess {
			verify,
			issue,
		})
	}

	async fn convert_bearer_access(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		access: &BearerAccess,
	) -> Result<catalog::BearerAccess> {
		Ok(catalog::BearerAccess {
			kind: match access.kind {
				BearerAccessType::Bearer => catalog::BearerAccessType::Bearer,
				BearerAccessType::Refresh => catalog::BearerAccessType::Refresh,
			},
			subject: match access.subject {
				BearerAccessSubject::Record => catalog::BearerAccessSubject::Record,
				BearerAccessSubject::User => catalog::BearerAccessSubject::User,
			},
			jwt: convert_jwt_access(stk, ctx, opt, doc, &access.jwt).await?,
		})
	}

	let grant_duration = stk
		.run(|stk| crate::legacy::expr_compute(&this.duration.grant, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to::<Option<Duration>>()?
		.map(|x| x.0);
	let token_duration = stk
		.run(|stk| crate::legacy::expr_compute(&this.duration.token, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to::<Option<Duration>>()?
		.map(|x| x.0);
	// Record-access tokens authenticate end-users (signin/signup) and may be
	// passed to third parties, so they MUST expire. The parser used to
	// reject `DURATION FOR TOKEN NONE` statically, but parameterized
	// durations are only known after `compute`, so the check moved here.
	if matches!(&this.access_type, AccessType::Record(_)) && token_duration.is_none() {
		bail!(ExecError::AccessRecordTokenDurationRequired);
	}
	let session_duration = stk
		.run(|stk| crate::legacy::expr_compute(&this.duration.session, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to::<Option<Duration>>()?
		.map(|x| x.0);
	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	let access_type = match &this.access_type {
		AccessType::Record(record_access) => catalog::AccessType::Record(catalog::RecordAccess {
			signup: record_access.signup.as_ref().map(ExprText::new),
			signin: record_access.signin.as_ref().map(ExprText::new),
			jwt: convert_jwt_access(stk, ctx, opt, doc, &record_access.jwt).await?,
			bearer: map_opt!(x as &record_access.bearer => convert_bearer_access(stk, ctx, opt, doc, x).await?),
		}),
		AccessType::Jwt(jwt_access) => {
			catalog::AccessType::Jwt(convert_jwt_access(stk, ctx, opt, doc, jwt_access).await?)
		}
		AccessType::Bearer(bearer_access) => catalog::AccessType::Bearer(
			convert_bearer_access(stk, ctx, opt, doc, bearer_access).await?,
		),
	};
	// The compiled side expressions come straight from the statement ASTs;
	// they are what the stored `access_type` text (built above) parses back to.
	let (signup, signin) = match &this.access_type {
		AccessType::Record(record_access) => {
			(record_access.signup.clone(), record_access.signin.clone())
		}
		_ => (None, None),
	};
	Ok(catalog::AccessDefinition {
		name: expr_to_ident(stk, ctx, opt, doc, &this.name, "access name").await?.into(),
		base: this.base.into(),
		access_type,
		authenticate: this.authenticate.clone(),
		signup,
		signin,
		grant_duration,
		token_duration,
		session_duration,
		comment,
	})
}

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineAccessStatement::compute", skip_all)]
pub(crate) async fn define_access_statement_compute(
	this: &DefineAccessStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Actor, this.base)?;
	// Compute the definition
	let definition =
		crate::legacy::define_access_statement_to_definition(this, stk, ctx, opt, doc).await?;
	// Check the statement type
	match &this.base {
		Base::Root => {
			// Fetch the transaction
			let txn = ctx.tx();
			// Check if access method already exists
			let mut existing_uses_es512 = false;
			if let Some(access) = txn.get_root_access(definition.name.as_str(), None).await? {
				existing_uses_es512 =
					crate::legacy::define_access_statement_uses_es512(&access.access_type);
				match this.kind {
					DefineKind::Default => {
						if !opt.import {
							bail!(CatalogError::AccessRootAlreadyExists {
								ac: access.name.to_string(),
							});
						}
					}
					DefineKind::Overwrite => {}
					DefineKind::IfNotExists => return Ok(Value::None),
				}
			}
			// Reject ES512 for new definitions (allow during import/restore and
			// overwrite of an existing ES512 definition)
			if !(opt.import || (existing_uses_es512 && this.kind == DefineKind::Overwrite)) {
				crate::legacy::define_access_statement_reject_es512(&definition)?;
			}
			// Process the statement
			let key = crate::key::root::ac::AccessKey {
				ac: Cow::Borrowed(definition.name.as_str()),
			};
			txn.set_key(&key, &definition.to_stored()).await?;
			// Clear the cache
			txn.clear_cache();
			// Ok all good
			Ok(Value::None)
		}
		Base::Ns => {
			// Fetch the transaction
			let txn = ctx.tx();
			// Check if the definition exists
			let ns = ctx.get_ns_id(opt).await?;
			let mut existing_uses_es512 = false;
			if let Some(access) = txn.get_ns_access(ns, definition.name.as_str(), None).await? {
				existing_uses_es512 =
					crate::legacy::define_access_statement_uses_es512(&access.access_type);
				match this.kind {
					DefineKind::Default => {
						if !opt.import {
							bail!(CatalogError::AccessNsAlreadyExists {
								ns: opt.ns()?.to_string(),
								ac: access.name.to_string(),
							});
						}
					}
					DefineKind::Overwrite => {}
					DefineKind::IfNotExists => return Ok(Value::None),
				}
			}
			// Reject ES512 for new definitions (allow during import/restore and
			// overwrite of an existing ES512 definition)
			if !(opt.import || (existing_uses_es512 && this.kind == DefineKind::Overwrite)) {
				crate::legacy::define_access_statement_reject_es512(&definition)?;
			}
			// Process the statement
			let key = crate::key::namespace::ac::AccessKey {
				ns,
				ac: Cow::Borrowed(definition.name.as_str()),
			};
			txn.get_or_add_ns(Some(ctx), opt.ns()?).await?;
			txn.set_key(&key, &definition.to_stored()).await?;
			// Clear the cache
			txn.clear_cache();
			// Ok all good
			Ok(Value::None)
		}
		Base::Db => {
			// Fetch the transaction
			let txn = ctx.tx();
			// Check if the definition exists
			let (ns, db) = ctx.get_ns_db_ids(opt).await?;
			let mut existing_uses_es512 = false;
			if let Some(access) = txn.get_db_access(ns, db, definition.name.as_str(), None).await? {
				existing_uses_es512 =
					crate::legacy::define_access_statement_uses_es512(&access.access_type);
				match this.kind {
					DefineKind::Default => {
						if !opt.import {
							bail!(CatalogError::AccessDbAlreadyExists {
								ns: opt.ns()?.to_string(),
								db: opt.db()?.to_string(),
								ac: access.name.to_string(),
							});
						}
					}
					DefineKind::Overwrite => {}
					DefineKind::IfNotExists => return Ok(Value::None),
				}
			}
			// Reject ES512 for new definitions (allow during import/restore and
			// overwrite of an existing ES512 definition)
			if !(opt.import || (existing_uses_es512 && this.kind == DefineKind::Overwrite)) {
				crate::legacy::define_access_statement_reject_es512(&definition)?;
			}
			// Process the statement
			let key = crate::key::database::ac::AccessKey {
				prefix: DatabaseRoot {
					ns,
					db,
				},
				ac: Cow::Borrowed(definition.name.as_str()),
			};
			txn.set_key(&key, &definition.to_stored()).await?;
			// Clear the cache
			txn.clear_cache();
			// Ok all good
			Ok(Value::None)
		}
	}
}

/// Returns true if the access type uses ES512 in any JWT component.
pub(crate) fn define_access_statement_uses_es512(access_type: &catalog::AccessType) -> bool {
	fn jwt_uses_es512(jwt: &catalog::JwtAccess) -> bool {
		if let catalog::JwtAccessVerify::Key(ref ver) = jwt.verify
			&& matches!(ver.alg, catalog::Algorithm::Es512)
		{
			return true;
		}
		if let Some(ref iss) = jwt.issue
			&& matches!(iss.alg, catalog::Algorithm::Es512)
		{
			return true;
		}
		false
	}

	match access_type {
		catalog::AccessType::Jwt(jwt) => jwt_uses_es512(jwt),
		catalog::AccessType::Record(rec) => {
			jwt_uses_es512(&rec.jwt) || rec.bearer.as_ref().is_some_and(|b| jwt_uses_es512(&b.jwt))
		}
		catalog::AccessType::Bearer(bearer) => jwt_uses_es512(&bearer.jwt),
	}
}

/// Check if the access definition uses ES512, which is not currently supported.
/// This should only be called for new definitions (not during import/restore or
/// overwrite of an existing ES512 definition).
pub(crate) fn define_access_statement_reject_es512(
	definition: &catalog::AccessDefinition,
) -> Result<()> {
	if crate::legacy::define_access_statement_uses_es512(&definition.access_type) {
		bail!(ExecError::AccessUnsupportedAlgorithm);
	}
	Ok(())
}
