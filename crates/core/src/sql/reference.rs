use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Reference {
	pub delete: Option<ReferenceDeleteStrategy>,
}

impl fmt::Display for Reference {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REFERENCE")?;
        if let Some(delete) = &self.delete {
            write!(f, " ON DELETE {}", delete)?;
        }

        Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ReferenceDeleteStrategy {
    Block,
    Ignore,
    Cascade,
    Custom(Value),
}

impl fmt::Display for ReferenceDeleteStrategy {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
            ReferenceDeleteStrategy::Block => write!(f, "BLOCK"),
            ReferenceDeleteStrategy::Ignore => write!(f, "IGNORE"),
            ReferenceDeleteStrategy::Cascade => write!(f, "CASCADE"),
            ReferenceDeleteStrategy::Custom(v) => write!(f, "THEN {}", v),
        }
	}
}