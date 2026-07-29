//! `sql` -> `expr` conversions for [`crate::sql::reference`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::reference::*;

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
