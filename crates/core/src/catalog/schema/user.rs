use std::time::Duration;

use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned, revisioned};
use serde::{Deserialize, Serialize};
use surrealdb_types::{ToSql, write_sql};

use crate::catalog::base::Base;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Array, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[repr(transparent)]
pub struct UserId(pub u64);

impl_kv_value_revisioned!(UserId);

impl Revisioned for UserId {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for UserId {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.0, writer)
	}
}

impl DeserializeRevisioned for UserId {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		DeserializeRevisioned::deserialize_revisioned(reader).map(UserId)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UserDefinition {
	pub name: String,
	pub hash: String,
	pub code: String,
	pub roles: Vec<String>,
	/// Duration after which the token obtained after authenticating with user credentials expires
	pub token_duration: Option<Duration>,
	/// Duration after which the session authenticated with user credentials or token expires
	pub session_duration: Option<Duration>,
	pub comment: Option<String>,
	pub base: Base,
}

impl UserDefinition {
	fn to_sql_definition(&self) -> crate::sql::statements::define::DefineUserStatement {
		crate::sql::statements::define::DefineUserStatement {
			kind: crate::sql::statements::define::DefineKind::Default,
			name: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.name.clone())),
			base: crate::sql::Base::from(crate::expr::Base::from(self.base.clone())),
			pass_type: crate::sql::statements::define::user::PassType::Hash(self.hash.clone()),
			roles: self.roles.clone(),
			token_duration: self.token_duration.map(|d| {
				crate::sql::Expr::Literal(crate::sql::Literal::Duration(
					crate::types::PublicDuration::from(d),
				))
			}),
			session_duration: self.session_duration.map(|d| {
				crate::sql::Expr::Literal(crate::sql::Literal::Duration(
					crate::types::PublicDuration::from(d),
				))
			}),
			comment: self
				.comment
				.clone()
				.map(|c| crate::sql::Expr::Literal(crate::sql::Literal::String(c))),
		}
	}
}

impl ToSql for &UserDefinition {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}

impl InfoStructure for UserDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => Value::from(self.name),
			"hash".to_string() => self.hash.into(),
			"roles".to_string() => Array::from(self.roles.into_iter().map(Value::from).collect::<Vec<_>>()).into(),
			"duration".to_string() => Value::from(map! {
				"token".to_string() => self.token_duration.map(Value::from).unwrap_or(Value::None),
				"session".to_string() => self.token_duration.map(Value::from).unwrap_or(Value::None),
			}),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}

impl_kv_value_revisioned!(UserDefinition);
