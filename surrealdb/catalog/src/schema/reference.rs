use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{ExprText, FromStored, IdiomText};
use crate::expr::reference::Reference as ExprReference;
use crate::expr::statements::info::InfoStructure;
use crate::stored::Stored;
use crate::val::{TableName, Value};

/// Stored form of a field's `REFERENCE` clause.
///
/// A byte-identical revisioned twin of the statement-side
/// [`crate::expr::reference::Reference`] (same struct/enum shapes, same
/// variant order), with the `ON DELETE THEN` expression held as canonical
/// SurrealQL text instead of an embedded AST.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredReference {
	#[stored(n = 2)]
	pub on_delete: StoredReferenceDeleteStrategy,
	/// The `MATERIALIZE INTO` clause. A reader that does not know it would stop
	/// maintaining the list it names, so the field is one an older reader refuses.
	#[revision(start = 2)]
	#[stored(n = 4)]
	pub materialize: Option<StoredMaterialize>,
}

/// Stored form of a reference's `MATERIALIZE INTO` clause.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredMaterialize {
	/// The referenced table that holds the list.
	#[stored(n = 2)]
	pub table: TableName,
	/// Canonical SurrealQL text of the top-level field of `table` that holds the
	/// list.
	#[stored(n = 4)]
	pub field: IdiomText,
	/// Canonical SurrealQL text of the `WHERE` condition, judged on the
	/// referencing record.
	#[stored(n = 6)]
	pub filter: Option<ExprText>,
}

/// Stored form of a reference's `ON DELETE` strategy.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum StoredReferenceDeleteStrategy {
	/// Reject the deletion while references exist.
	#[stored(n = 1)]
	Reject,
	/// Leave the referencing field untouched.
	#[stored(n = 2)]
	Ignore,
	/// Delete the referencing record too.
	#[stored(n = 3)]
	Cascade,
	/// Unset the referencing field.
	#[stored(n = 4)]
	Unset,
	/// Run a custom expression, stored as canonical SurrealQL text.
	#[stored(n = 5)]
	Custom(ExprText),
}

impl From<crate::expr::reference::Reference> for StoredReference {
	fn from(v: crate::expr::reference::Reference) -> Self {
		Self {
			on_delete: v.on_delete.into(),
			materialize: v.materialize.as_ref().map(materialize_to_stored),
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
		if let Some(materialize) = &self.materialize {
			f.push_str(" MATERIALIZE INTO ");
			f.push_str(&common::fmt::EscapeKwFreeIdent(materialize.table.as_str()).to_sql());
			f.push('.');
			f.push_str(materialize.field.as_str());
			if let Some(filter) = &materialize.filter {
				f.push_str(" WHERE ");
				f.push_str(filter.as_str());
			}
		}
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
		let materialize = self.materialize.map(|m| {
			Value::from(map! {
				"table" => Value::String(m.table.into()),
				"field" => Value::String(m.field.as_str().into()),
				"filter", if let Some(filter) = m.filter => Value::from(filter.as_str()),
			})
		});
		map! {
			"on_delete" => self.on_delete.structure(),
			"materialize", if let Some(m) = materialize => m,
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
			materialize: stored
				.materialize
				.as_ref()
				.map(|m| {
					Ok::<_, anyhow::Error>(crate::expr::reference::Materialize {
						table: m.table.clone(),
						field: m.field.compile()?,
						filter: m.filter.as_ref().map(|f| f.compile()).transpose()?,
					})
				})
				.transpose()?,
		})
	}
}

/// Stored form of a runtime reference (`ExprReference` doubles as the
/// compiled form, so the reverse render lives here rather than on the type).
pub fn reference_to_stored(r: &ExprReference) -> StoredReference {
	use crate::expr::reference::ReferenceDeleteStrategy as ExprStrategy;
	StoredReference {
		on_delete: match &r.on_delete {
			ExprStrategy::Reject => StoredReferenceDeleteStrategy::Reject,
			ExprStrategy::Ignore => StoredReferenceDeleteStrategy::Ignore,
			ExprStrategy::Cascade => StoredReferenceDeleteStrategy::Cascade,
			ExprStrategy::Unset => StoredReferenceDeleteStrategy::Unset,
			ExprStrategy::Custom(e) => StoredReferenceDeleteStrategy::Custom(ExprText::new(e)),
		},
		materialize: r.materialize.as_ref().map(materialize_to_stored),
	}
}

fn materialize_to_stored(m: &crate::expr::reference::Materialize) -> StoredMaterialize {
	StoredMaterialize {
		table: m.table.clone(),
		field: IdiomText::new(&m.field),
		filter: m.filter.as_ref().map(ExprText::new),
	}
}
