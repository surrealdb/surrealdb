use revision::revisioned;

use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ViewDefinition {
    // TODO: STU Implement this
	// pub expr: Fields,
	// pub what: Tables,
	// pub cond: Option<Cond>,
	// pub group: Option<Groups>,
}
