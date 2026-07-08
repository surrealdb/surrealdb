use std::time::Duration;

use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::base::Base;
use crate::expr::statements::info::InfoStructure;
use crate::key::impl_kv_value_revisioned;
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
	/// Returns a copy of this definition with password-equivalent secrets
	/// redacted, for serialisation through `INFO FOR …` / `INFO FOR USER …`.
	///
	/// The Argon2 `hash` is a credential — offline cracking of the PHC string
	/// defeats authentication for the named user — so it is replaced with
	/// [`REDACTED`](super::REDACTED). A user defined without a password (empty
	/// hash) is left untouched, so `PASSHASH ''` keeps round-tripping through
	/// the upgrade-test chain, which compares against pre-built binaries from
	/// earlier releases. The SCRAM verifier is likewise password-equivalent;
	/// it is structured rather than a string, so its presence is signalled as
	/// [`REDACTED`](super::REDACTED) by the serialisers below.
	///
	/// Export (`surreal export`) goes through
	/// [`crate::expr::statements::DefineUserStatement::from_definition`] and is
	/// intentionally NOT routed through this redaction, so privileged
	/// hash-preserving exports still round-trip the real secrets.
	fn redacted(mut self) -> Self {
		if !self.hash.is_empty() {
			self.hash = super::REDACTED.to_string();
		}
		self
	}

	fn to_sql_definition(&self) -> sql::statements::define::DefineUserStatement {
		let this = self.clone().redacted();
		sql::statements::define::DefineUserStatement {
			kind: sql::statements::define::DefineKind::Default,
			name: sql::Expr::Idiom(sql::Idiom::field(this.name)),
			base: sql::Base::from(crate::expr::Base::from(this.base)),
			pass_type: sql::statements::define::user::PassType::Hash(this.hash),
			// The verifier is structured (`ScramCredential`) and never rendered as a
			// string here, so only its presence is signalled as redacted.
			scram: this.scram.map(|_| super::REDACTED.to_string()),
			roles: this.roles,
			token_duration: this
				.token_duration
				.map(|d| {
					sql::Expr::Literal(sql::Literal::Duration(crate::types::PublicDuration::from(
						d,
					)))
				})
				.unwrap_or_else(|| sql::Expr::Literal(sql::Literal::None)),
			session_duration: this
				.session_duration
				.map(|d| {
					sql::Expr::Literal(sql::Literal::Duration(crate::types::PublicDuration::from(
						d,
					)))
				})
				.unwrap_or_else(|| sql::Expr::Literal(sql::Literal::None)),
			comment: this
				.comment
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
		let this = self.redacted();
		Value::from(map! {
			"name" => Value::String(this.name.clone()),
			"hash" => this.hash.into(),
			// The SCRAM verifier is structured, so only its presence is signalled.
			"scram", if this.scram.is_some() => Value::from(super::REDACTED),
			"roles" => Array::from(this.roles.into_iter().map(Value::from).collect::<Vec<_>>()).into(),
			"duration" => Value::from(map! {
				"token" => this.token_duration.map(Value::from).unwrap_or(Value::None),
				"session" => this.session_duration.map(Value::from).unwrap_or(Value::None),
			}),
			"comment", if let Some(v) = this.comment => v.into(),
		})
	}
}

impl_kv_value_revisioned!(UserDefinition);
