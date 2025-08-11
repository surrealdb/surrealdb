use std::time::Duration;

use revision::revisioned;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct SequenceDefinition {
	pub name: String,
	pub batch: u32,
	pub start: i64,
	pub timeout: Option<Duration>,
}
