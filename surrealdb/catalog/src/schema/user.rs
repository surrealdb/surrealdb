use revision::revisioned;
pub use surrealdb_iam::ScramCredential;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::base::Base;
use crate::expr::statements::info::InfoStructure;
use crate::impl_kv_value_catalog;
use crate::stored::Stored;
use crate::val::{Array, BoundedDuration, Value};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct UserDefinition {
	#[stored(n = 2)]
	pub name: Strand,
	#[stored(n = 4)]
	pub hash: String,
	#[stored(n = 6)]
	pub code: String,
	#[stored(n = 8)]
	pub roles: Vec<String>,
	/// Duration after which the token obtained after authenticating with user credentials expires
	#[stored(n = 10)]
	pub token_duration: Option<BoundedDuration>,
	/// Duration after which the session authenticated with user credentials or token expires
	#[stored(n = 12)]
	pub session_duration: Option<BoundedDuration>,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
	#[stored(n = 14)]
	pub base: Base,
	/// SCRAM-SHA-256 verifier material, populated when the user is defined with a
	/// plaintext `PASSWORD` (or an explicit `PASSSCRAM`). `None` for users defined
	/// via `PASSHASH` only, and for users stored before this field existed.
	#[revision(start = 2)]
	#[stored(n = 16)]
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

	fn to_runtime_definition(&self) -> crate::expr::statements::DefineUserStatement {
		let this = self.clone().redacted();
		crate::expr::statements::DefineUserStatement {
			kind: crate::expr::statements::define::DefineKind::Default,
			name: crate::expr::Expr::Idiom(crate::expr::Idiom::field(this.name)),
			base: this.base.into(),
			pending_password: None,
			hash_is_set: true,
			hash: this.hash,
			code: String::new(),
			scram: this.scram,
			roles: this.roles,
			duration: crate::expr::user::UserDuration {
				token: this
					.token_duration
					.map(|d| Value::Duration(d.into()).into_literal())
					.unwrap_or(crate::expr::Expr::Literal(crate::expr::Literal::None)),
				session: this
					.session_duration
					.map(|d| Value::Duration(d.into()).into_literal())
					.unwrap_or(crate::expr::Expr::Literal(crate::expr::Literal::None)),
			},
			comment: this
				.comment
				.map(|c| Value::from(c).into_literal())
				.unwrap_or(crate::expr::Expr::Literal(crate::expr::Literal::None)),
		}
	}
}

impl ToSql for &UserDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_redacted_sql(f, fmt)
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
				"token" => this.token_duration.map(|d| Value::Duration(d.into())).unwrap_or(Value::None),
				"session" => this.session_duration.map(|d| Value::Duration(d.into())).unwrap_or(Value::None),
			}),
			"comment", if let Some(v) = this.comment => v.into(),
		})
	}
}

impl_kv_value_catalog!(UserDefinition);
