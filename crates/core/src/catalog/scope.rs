use revision::revisioned;

use serde::{Deserialize, Serialize};


#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Scope {
	Root,
	Ns,
	Db,
}
