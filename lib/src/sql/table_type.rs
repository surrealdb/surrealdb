use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::Kind;

#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[revisioned(revision = 1)]
pub enum TableType {
	Relation(Relation),
	Normal,
	#[default]
	Any,
}

#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[revisioned(revision = 1)]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
}
