use crate::catalog::Base;
use crate::kvs::impl_kv_value_revisioned;
use revision::revisioned;
use std::time::Duration;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UserDefinition {
	pub name: String,
	pub base: Base,
	pub hash: String,
	pub code: String,
	pub roles: Vec<String>,
	/// Duration after which the token obtained after authenticating with user credentials expires
	pub token: Option<Duration>,
	/// Duration after which the session authenticated with user credentials or token expires
	pub session: Option<Duration>,
	pub comment: Option<String>,
}

impl_kv_value_revisioned!(UserDefinition);
