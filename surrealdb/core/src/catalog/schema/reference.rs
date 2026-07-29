use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{ExprText, FromStored};
use crate::expr::reference::Reference as ExprReference;
use crate::expr::statements::info::InfoStructure;
use crate::val::Value;

/// Stored form of a field's `REFERENCE` clause.
///
/// A byte-identical revisioned twin of the statement-side
/// [`crate::expr::reference::Reference`] (same struct/enum shapes, same
/// variant order), with the `ON DELETE THEN` expression held as canonical
/// SurrealQL text instead of an embedded AST.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredReference {
	pub(crate) on_delete: StoredReferenceDeleteStrategy,
}

/// Stored form of a reference's `ON DELETE` strategy.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum StoredReferenceDeleteStrategy {
	/// Reject the deletion while references exist.
	Reject,
	/// Leave the referencing field untouched.
	Ignore,
	/// Delete the referencing record too.
	Cascade,
	/// Unset the referencing field.
	Unset,
	/// Run a custom expression, stored as canonical SurrealQL text.
	Custom(ExprText),
}

impl From<crate::expr::reference::Reference> for StoredReference {
	fn from(v: crate::expr::reference::Reference) -> Self {
		Self {
			on_delete: v.on_delete.into(),
		}
	}
}

impl From<crate::expr::reference::ReferenceDeleteStrategy> for StoredReferenceDeleteStrategy {
	fn from(v: crate::expr::reference::ReferenceDeleteStrategy) -> Self {
		use crate::expr::reference::ReferenceDeleteStrategy as ExprStrategy;
		match v {
			ExprStrategy::Reject => Self::Reject,
			ExprStrategy::Ignore => Self::Ignore,
			ExprStrategy::Cascade => Self::Cascade,
			ExprStrategy::Unset => Self::Unset,
			ExprStrategy::Custom(e) => Self::Custom(ExprText::new(&e)),
		}
	}
}

impl ToSql for StoredReference {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("ON DELETE ");
		self.on_delete.fmt_sql(f, fmt);
	}
}

impl ToSql for StoredReferenceDeleteStrategy {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			Self::Reject => f.push_str("REJECT"),
			Self::Ignore => f.push_str("IGNORE"),
			Self::Cascade => f.push_str("CASCADE"),
			Self::Unset => f.push_str("UNSET"),
			// The stored text already carries the statement-covering parens
			// the sql-side `CoverStmts` render applies, so it splices in raw.
			Self::Custom(text) => {
				f.push_str("THEN ");
				f.push_str(text.as_str());
			}
		}
	}
}

impl InfoStructure for StoredReference {
	fn structure(self) -> Value {
		map! {
			"on_delete" => self.on_delete.structure(),
		}
		.into()
	}
}

impl InfoStructure for StoredReferenceDeleteStrategy {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}

impl FromStored for ExprReference {
	/// The statement-side `expr::Reference` already has exactly the runtime
	/// shape (strategy enum with a parsed `ON DELETE THEN` expression), so it
	/// doubles as the compiled form.
	type Stored = StoredReference;

	fn from_stored(stored: &StoredReference) -> anyhow::Result<ExprReference> {
		use crate::expr::reference::ReferenceDeleteStrategy as ExprStrategy;
		Ok(ExprReference {
			on_delete: match &stored.on_delete {
				StoredReferenceDeleteStrategy::Reject => ExprStrategy::Reject,
				StoredReferenceDeleteStrategy::Ignore => ExprStrategy::Ignore,
				StoredReferenceDeleteStrategy::Cascade => ExprStrategy::Cascade,
				StoredReferenceDeleteStrategy::Unset => ExprStrategy::Unset,
				StoredReferenceDeleteStrategy::Custom(t) => ExprStrategy::Custom(t.compile()?),
			},
		})
	}
}

/// Stored form of a runtime reference (`ExprReference` doubles as the
/// compiled form, so the reverse render lives here rather than on the type).
pub(crate) fn reference_to_stored(r: &ExprReference) -> StoredReference {
	use crate::expr::reference::ReferenceDeleteStrategy as ExprStrategy;
	StoredReference {
		on_delete: match &r.on_delete {
			ExprStrategy::Reject => StoredReferenceDeleteStrategy::Reject,
			ExprStrategy::Ignore => StoredReferenceDeleteStrategy::Ignore,
			ExprStrategy::Cascade => StoredReferenceDeleteStrategy::Cascade,
			ExprStrategy::Unset => StoredReferenceDeleteStrategy::Unset,
			ExprStrategy::Custom(e) => StoredReferenceDeleteStrategy::Custom(ExprText::new(e)),
		},
	}
}
