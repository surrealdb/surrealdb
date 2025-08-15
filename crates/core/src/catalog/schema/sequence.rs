use std::time::Duration;

use revision::revisioned;

use crate::kvs::impl_kv_value_revisioned;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct SequenceDefinition {
	pub name: String,
	pub batch: u32,
	pub start: i64,
	pub timeout: Option<Duration>,
}

impl_kv_value_revisioned!(SequenceDefinition);
