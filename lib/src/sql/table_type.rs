use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::Kind;

/// The type of records stored by a table
#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[revisioned(revision = 1)]
pub enum TableType {
	Relation(Relation),
	Normal,
	// Should not be changed in version 2.0.0, this is required for revision compatibility
	#[default]
	Any,
}

#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[revisioned(revision = 1)]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
}
