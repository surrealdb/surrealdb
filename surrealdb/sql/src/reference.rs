use crate::{CoverStmts, Expr};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Reference {
	pub on_delete: ReferenceDeleteStrategy,
}

impl surrealdb_types::ToSql for Reference {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		f.push_str("ON DELETE ");
		self.on_delete.fmt_sql(f, fmt);
	}
}

impl surrealdb_types::ToSql for ReferenceDeleteStrategy {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		match self {
			ReferenceDeleteStrategy::Reject => f.push_str("REJECT"),
			ReferenceDeleteStrategy::Ignore => f.push_str("IGNORE"),
			ReferenceDeleteStrategy::Cascade => f.push_str("CASCADE"),
			ReferenceDeleteStrategy::Unset => f.push_str("UNSET"),
			ReferenceDeleteStrategy::Custom(v) => {
				f.push_str("THEN ");
				CoverStmts(v).fmt_sql(f, fmt);
			}
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
