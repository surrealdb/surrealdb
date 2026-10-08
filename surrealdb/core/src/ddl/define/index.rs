use anyhow::{Result, bail};

use crate::catalog::Error as CatalogError;
use crate::catalog::providers::TableProvider;
use crate::exec::Error as ExecError;
use crate::expr::{Idiom, Part};
use crate::kvs::Transaction;
use crate::val::TableName;

/// The check `DEFINE INDEX` makes of one column of index `index` over the
/// table `table`, in the database `ns`/`db`: a field the table defines, or a
/// path beneath one whose type admits sub-fields, may be indexed unless it is
/// computed; a path no such field covers is refused on a schemafull table.
pub(crate) async fn define_index_check_column(
	txn: &Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	table: &TableName,
	schemafull: bool,
	index: &str,
	idiom: &Idiom,
) -> Result<()> {
	let fd = idiom.to_raw_string();
	// Check if the exact field path (e.g. `document.visible`) is defined
	if let Some(f) = txn.get_tb_field(ns, db, table, &fd, None).await? {
		// Computed fields cannot be indexed
		if f.computed.is_some() {
			bail!(ExecError::ComputedFieldCannotBeIndexed {
				field: fd,
				index: index.to_owned()
			});
		}
		return Ok(());
	}
	// For sub-field paths, extract the top-level parent field name
	if let Some(Part::Field(first)) = idiom.0.first() &&
				// Allow the index when the parent field exists and its type
				// permits sub-field access. If no type is set (field_kind is
				// None), the field is unconstrained and sub-fields are allowed.
				let Some(f) = txn.get_tb_field(ns, db, table, first, None).await?
				&& f.field_kind.as_ref().is_none_or(|k| k.allows_sub_fields())
	{
		// Sub-fields of computed fields cannot be indexed
		if f.computed.is_some() {
			bail!(ExecError::ComputedFieldCannotBeIndexed {
				field: first.as_str().to_owned(),
				index: index.to_owned()
			});
		}
		return Ok(());
	}
	if schemafull {
		bail!(CatalogError::FdNotFound {
			name: idiom.to_raw_string(),
		});
	}
	Ok(())
}
