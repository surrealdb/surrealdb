use crate::sql::Block;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Future";

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Future(pub Block);

impl fmt::Display for Future {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "<future> {}", self.0)
	}
}

impl From<Future> for crate::expr::Future {
	fn from(v: Future) -> Self {
		Self(v.0.into())
	}
}

impl From<crate::expr::Future> for Future {
	fn from(v: crate::expr::Future) -> Self {
		Future(v.0.into())
	}
}
