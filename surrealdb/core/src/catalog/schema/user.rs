use std::time::Duration;

use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::base::Base;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql;
use crate::val::{Array, Value};

/// SCRAM-SHA-256 verifier material for a user.
///
/// This is stored alongside the Argon2 `hash` on [`UserDefinition`] so that
/// transports which negotiate SCRAM (e.g. the Postgres wire protocol) can
/// authenticate a user without SurrealDB ever holding the plaintext password.
///
/// The mechanism is fixed to SCRAM-SHA-256, so it is not stored. See
/// [`crate::iam::scram`] for the derivation and verification logic.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ScramCredential {
	/// PBKDF2 iteration count used to derive the salted password.
	pub iterations: u32,
	/// Random per-user salt.
	pub salt: Vec<u8>,
	/// `H(HMAC(SaltedPassword, "Client Key"))` — used to verify a client proof.
	pub stored_key: Vec<u8>,
	/// `HMAC(SaltedPassword, "Server Key")` — used to sign the server's final message.
	pub server_key: Vec<u8>,
}

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UserDefinition {
	pub name: Strand,
	pub hash: String,
	pub code: String,
	pub roles: Vec<String>,
	/// Duration after which the token obtained after authenticating with user credentials expires
	pub token_duration: Option<Duration>,
	/// Duration after which the session authenticated with user credentials or token expires
	pub session_duration: Option<Duration>,
	pub comment: Option<String>,
	pub base: Base,
	/// SCRAM-SHA-256 verifier material, populated when the user is defined with a
	/// plaintext `PASSWORD` (or an explicit `PASSSCRAM`). `None` for users defined
	/// via `PASSHASH` only, and for users stored before this field existed.
	#[revision(start = 2)]
	pub scram: Option<ScramCredential>,
}

impl UserDefinition {
	fn to_sql_definition(&self) -> sql::statements::define::DefineUserStatement {
		sql::statements::define::DefineUserStatement {
			kind: sql::statements::define::DefineKind::Default,
			name: sql::Expr::Idiom(sql::Idiom::field(self.name.clone())),
			base: sql::Base::from(crate::expr::Base::from(self.base.clone())),
			pass_type: sql::statements::define::user::PassType::Hash(self.hash.clone()),
			// Redact the SCRAM verifier in metadata output. `to_sql_definition` only
			// feeds INFO; export goes through `expr::DefineUserStatement::from_definition`
			// (not redacted), so export/import still round-trips the real verifier.
			// The verifier is a GPU-cheap PBKDF2 representation of the password and
			// must not be disclosed in metadata responses.
			scram: self.scram.as_ref().map(|_| "[REDACTED]".to_string()),
			roles: self.roles.clone(),
			token_duration: self
				.token_duration
				.map(|d| {
					sql::Expr::Literal(sql::Literal::Duration(crate::types::PublicDuration::from(
						d,
					)))
				})
				.unwrap_or_else(|| sql::Expr::Literal(sql::Literal::None)),
			session_duration: self
				.session_duration
				.map(|d| {
					sql::Expr::Literal(sql::Literal::Duration(crate::types::PublicDuration::from(
						d,
					)))
				})
				.unwrap_or_else(|| sql::Expr::Literal(sql::Literal::None)),
			comment: self
				.comment
				.clone()
				.map(|c| sql::Expr::Literal(sql::Literal::String(c.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}
}

impl ToSql for &UserDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for UserDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => Value::String(self.name.clone()),
			"hash" => self.hash.into(),
			// Redacted: see `to_sql_definition`. Only signals presence, not the value.
			"scram", if self.scram.is_some() => Value::from("[REDACTED]"),
			"roles" => Array::from(self.roles.into_iter().map(Value::from).collect::<Vec<_>>()).into(),
			"duration" => Value::from(map! {
				"token" => self.token_duration.map(Value::from).unwrap_or(Value::None),
				"session" => self.session_duration.map(Value::from).unwrap_or(Value::None),
			}),
			"comment", if let Some(v) = self.comment => v.into(),
		})
	}
}

impl_kv_value_revisioned!(UserDefinition);
