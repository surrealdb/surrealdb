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
use crate::key::schema::{DbAccessMethodKey, NsAccessMethodKey, RootAccessMethodKey};
use crate::legacy::expr_to_ident;
use crate::val::{Duration, Value};

/// Validate an access method's `CONTEXT` clause against the access type it is
/// defined on.
///
/// Applied identically by `DEFINE ACCESS` and `ALTER ACCESS`; an ALTER that
/// could install a clause DEFINE would refuse is the same hole with an extra
/// step.
///
/// Two rules:
///
/// * The expression must be read-only. `Datastore::evaluate` opens a *write* transaction for
///   anything that is not (`kvs/ds.rs`), and HTTP re-authenticates on every request, so a
///   side-effecting CONTEXT clause would fire a write per request from an unauthenticated caller's
///   token.
/// * The access method must be able to authenticate a record *at the level it is defined on*.
///   CONTEXT is evaluated under the authenticated record's own permissions precisely because its
///   whole result is exposed as `$session.data`; an access method that only ever produces a system
///   user has no record identity to scope that evaluation to. Records live in a database, and
///   `signin_bearer` only computes the payload for a record subject resolved with both a namespace
///   and a database (`iam/signin.rs`), so a clause on a root- or namespace-level definition could
///   never run. Both parsers reject `TYPE RECORD` and `TYPE BEARER FOR RECORD` outside a database
///   today, which makes the base test unreachable through SQL; it is here so the rule does not
///   depend on a guard in another crate staying correct. A silent no-op is the wrong failure mode
///   for a security clause: `PERMISSIONS FOR select WHERE $session.data.blocked != true` fails
///   *open* against a `NONE` payload.
///
/// `$session.data` then follows `$auth`: it is populated on every path that
/// settles a record identity, and absent on every path that does not.
///
/// Note what that does *not* say about `TYPE BEARER FOR RECORD`. The key
/// exchange settles a record identity, so it gets a payload — but the JWT that
/// exchange issues always carries an `id` claim (`signin_bearer` fills it from
/// the grant's subject), so re-verifying that JWT matches `verify::token`'s
/// record-access arm, which requires the definition's access type to be
/// `RECORD` and rejects a `BEARER` one with `AccessMethodMismatch`. Presenting
/// the issued JWT back is a hard failure, not a graceful downgrade to a
/// session with neither `$auth` nor `$session.data`; the bearer key, not the
/// JWT it mints, is what re-establishes such a session.
pub(crate) fn validate_access_context(
	base: &catalog::base::Base,
	access_type: &catalog::AccessType,
	context: Option<&crate::expr::Expr>,
) -> Result<()> {
	let Some(context) = context else {
		return Ok(());
	};
	if !context.read_only() {
		bail!(ExecError::AccessContextNotReadOnly);
	}
	let authenticates_record = match access_type {
		catalog::AccessType::Record(_) => true,
		catalog::AccessType::Bearer(bearer) => {
			matches!(bearer.subject, catalog::BearerAccessSubject::Record)
		}
		catalog::AccessType::Jwt(_) => false,
	} && matches!(base, catalog::base::Base::Db);
	if !authenticates_record {
		bail!(ExecError::AccessContextRequiresRecord);
	}
	Ok(())
}

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

		let audience = match &access.audience {
			Some(exprs) => {
				let mut list = Vec::with_capacity(exprs.len());
				for expr in exprs {
					list.push(
						stk.run(|stk| crate::legacy::expr_compute(expr, stk, ctx, opt, doc))
							.await
							.catch_return()?
							.coerce_to::<String>()?,
					);
				}
				Some(list)
			}
			None => None,
		};

		Ok(catalog::JwtAccess {
			verify,
			issue,
			audience,
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
	let base: catalog::base::Base = this.base.into();
	validate_access_context(&base, &access_type, this.context.as_ref())?;
	Ok(catalog::AccessDefinition {
		name: expr_to_ident(stk, ctx, opt, doc, &this.name, "access name").await?.into(),
		base,
		access_type,
		authenticate: this.authenticate.clone(),
		context: this.context.clone(),
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
			let key = RootAccessMethodKey {
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
			let key = NsAccessMethodKey {
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
			let key = DbAccessMethodKey {
				ns,
				db,
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

#[cfg(test)]
mod validate_access_context_tests {
	use super::*;

	fn context() -> crate::expr::Expr {
		crate::expr::Expr::Literal(crate::expr::Literal::Integer(1))
	}

	fn jwt() -> catalog::JwtAccess {
		catalog::JwtAccess {
			verify: catalog::JwtAccessVerify::Key(catalog::JwtAccessVerifyKey {
				alg: catalog::Algorithm::Hs512,
				key: "secret".to_owned(),
			}),
			issue: None,
			audience: None,
		}
	}

	fn record() -> catalog::AccessType {
		catalog::AccessType::Record(catalog::RecordAccess {
			signup: None,
			signin: None,
			jwt: jwt(),
			bearer: None,
		})
	}

	fn bearer(subject: catalog::BearerAccessSubject) -> catalog::AccessType {
		catalog::AccessType::Bearer(catalog::BearerAccess {
			kind: catalog::BearerAccessType::Bearer,
			subject,
			jwt: jwt(),
		})
	}

	/// A record-authenticating access method at database level takes the
	/// clause. This is the only shape that does.
	#[test]
	fn accepts_record_authenticating_access_on_a_database() {
		let ctx = context();
		for access_type in [record(), bearer(catalog::BearerAccessSubject::Record)] {
			validate_access_context(&catalog::base::Base::Db, &access_type, Some(&ctx))
				.expect("a record-authenticating database access method takes a CONTEXT clause");
		}
	}

	/// An access method that only ever produces a system user has no record
	/// identity to scope the evaluation to.
	#[test]
	fn refuses_access_that_cannot_authenticate_a_record() {
		let ctx = context();
		for access_type in
			[catalog::AccessType::Jwt(jwt()), bearer(catalog::BearerAccessSubject::User)]
		{
			assert!(
				validate_access_context(&catalog::base::Base::Db, &access_type, Some(&ctx))
					.is_err()
			);
		}
	}

	/// Records live in a database, and `signin_bearer` only computes the
	/// payload for a record subject resolved with both a namespace and a
	/// database. A clause on a root- or namespace-level definition could
	/// never run, and a security clause that silently never runs is worse
	/// than one that is refused: written negatively it fails open.
	///
	/// Both parsers already reject `TYPE RECORD` and `TYPE BEARER FOR RECORD`
	/// outside a database, so this is unreachable through SQL. It is asserted
	/// directly so the rule does not depend on that guard staying correct.
	#[test]
	fn refuses_record_authenticating_access_outside_a_database() {
		let ctx = context();
		for base in [catalog::base::Base::Root, catalog::base::Base::Ns] {
			for access_type in [record(), bearer(catalog::BearerAccessSubject::Record)] {
				assert!(
					validate_access_context(&base, &access_type, Some(&ctx)).is_err(),
					"CONTEXT must be refused on a {base:?}-level definition"
				);
			}
		}
	}

	/// No clause, no rules to apply - at any base, on any access type.
	#[test]
	fn accepts_every_access_method_without_a_clause() {
		for base in [catalog::base::Base::Root, catalog::base::Base::Ns, catalog::base::Base::Db] {
			for access_type in
				[catalog::AccessType::Jwt(jwt()), bearer(catalog::BearerAccessSubject::User)]
			{
				validate_access_context(&base, &access_type, None).unwrap();
			}
		}
	}
}
