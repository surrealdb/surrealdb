use std::fmt;

use crate::sql::Expr;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Reference {
	pub on_delete: ReferenceDeleteStrategy,
}

impl fmt::Display for Reference {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ON DELETE {}", &self.on_delete)
	}
}

impl From<Reference> for crate::expr::reference::Reference {
	fn from(v: Reference) -> Self {
		Self {
			on_delete: v.on_delete.into(),
		}
	}
}
impl From<crate::expr::reference::Reference> for Reference {
	fn from(v: crate::expr::reference::Reference) -> Self {
		Self {
			on_delete: v.on_delete.into(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ReferenceDeleteStrategy {
	Reject,
	Ignore,
	Cascade,
	Unset,
	Custom(Expr),
}

impl fmt::Display for ReferenceDeleteStrategy {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ReferenceDeleteStrategy::Reject => write!(f, "REJECT"),
			ReferenceDeleteStrategy::Ignore => write!(f, "IGNORE"),
			ReferenceDeleteStrategy::Cascade => write!(f, "CASCADE"),
			ReferenceDeleteStrategy::Unset => write!(f, "UNSET"),
			ReferenceDeleteStrategy::Custom(v) => write!(f, "THEN {}", v),
		}
	}
}

impl From<ReferenceDeleteStrategy> for crate::expr::reference::ReferenceDeleteStrategy {
	fn from(v: ReferenceDeleteStrategy) -> Self {
		match v {
			ReferenceDeleteStrategy::Reject => {
				crate::expr::reference::ReferenceDeleteStrategy::Reject
			}
			ReferenceDeleteStrategy::Ignore => {
				crate::expr::reference::ReferenceDeleteStrategy::Ignore
			}
			ReferenceDeleteStrategy::Cascade => {
				crate::expr::reference::ReferenceDeleteStrategy::Cascade
			}
			ReferenceDeleteStrategy::Unset => {
				crate::expr::reference::ReferenceDeleteStrategy::Unset
			}
			ReferenceDeleteStrategy::Custom(v) => {
				crate::expr::reference::ReferenceDeleteStrategy::Custom(v.into())
			}
		}
	}
}

impl From<crate::expr::reference::ReferenceDeleteStrategy> for ReferenceDeleteStrategy {
	fn from(v: crate::expr::reference::ReferenceDeleteStrategy) -> Self {
		match v {
			crate::expr::reference::ReferenceDeleteStrategy::Reject => {
				ReferenceDeleteStrategy::Reject
			}
			crate::expr::reference::ReferenceDeleteStrategy::Ignore => {
				ReferenceDeleteStrategy::Ignore
			}
			crate::expr::reference::ReferenceDeleteStrategy::Cascade => {
				ReferenceDeleteStrategy::Cascade
			}
			crate::expr::reference::ReferenceDeleteStrategy::Unset => {
				ReferenceDeleteStrategy::Unset
			}
			crate::expr::reference::ReferenceDeleteStrategy::Custom(v) => {
				ReferenceDeleteStrategy::Custom(v.into())
			}
		}
	}
}
