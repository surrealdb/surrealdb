use std::time::Duration;

use revision::revisioned;

use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Array, Value};

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
