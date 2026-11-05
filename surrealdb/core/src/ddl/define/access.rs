use anyhow::{Result, bail};

use crate::catalog::{self};
use crate::exec::Error as ExecError;

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

/// Whether any symmetric key in this access is the redaction placeholder.
///
/// [`catalog::schema::REDACTED`] is what every surface that renders an access
/// substitutes for its keys, export included. Such text is executable
/// SurrealQL, so replaying it defines an access whose signing secret is a
/// string published in the dump, in `INFO FOR DB`, in logs and in this
/// repository — one anyone can mint tokens with.
fn define_access_statement_uses_redacted_key(access_type: &catalog::AccessType) -> bool {
	fn jwt_is_redacted(jwt: &catalog::JwtAccess) -> bool {
		if let catalog::JwtAccessVerify::Key(ref ver) = jwt.verify
			&& ver.key == catalog::schema::REDACTED
		{
			return true;
		}
		if let Some(ref iss) = jwt.issue
			&& iss.key == catalog::schema::REDACTED
		{
			return true;
		}
		false
	}

	match access_type {
		catalog::AccessType::Jwt(jwt) => jwt_is_redacted(jwt),
		catalog::AccessType::Record(rec) => {
			jwt_is_redacted(&rec.jwt)
				|| rec.bearer.as_ref().is_some_and(|b| jwt_is_redacted(&b.jwt))
		}
		catalog::AccessType::Bearer(bearer) => jwt_is_redacted(&bearer.jwt),
	}
}

/// Refuses an access whose key is the redaction placeholder.
///
/// A dump renders its accesses redacted, so restoring one defines an access
/// that verifies tokens anyone can forge. Refused rather than accepted, because
/// an access that fails to restore is a named per-statement failure an operator
/// can act on, where one that restores with a published key is a silent
/// authentication bypass. The refusal applies to a literal typed by hand as
/// well: no caller has a reason to name this string as a secret.
pub(crate) fn define_access_statement_reject_redacted(
	definition: &catalog::AccessDefinition,
) -> Result<()> {
	if define_access_statement_uses_redacted_key(&definition.access_type) {
		bail!(ExecError::AccessRedactedKey {
			ac: definition.name.to_string(),
		});
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
