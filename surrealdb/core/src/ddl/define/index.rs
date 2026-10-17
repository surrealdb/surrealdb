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
///
/// A component of the table's record id is a field the table derives from the
/// id, not one it defines. Its name alone is a column an index may hold
/// ([`key_component_column`]); a path beneath it is refused as a computed
/// field's is.
pub(crate) async fn define_index_check_column(
	txn: &Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	table: &TableName,
	schemafull: bool,
	index: &str,
	idiom: &Idiom,
) -> Result<()> {
	match key_component_column(txn, ns, db, table, idiom).await? {
		Some(ComponentColumn::Whole) => return Ok(()),
		Some(ComponentColumn::Beneath(component)) => {
			bail!(ExecError::ComputedFieldCannotBeIndexed {
				field: component,
				index: index.to_owned()
			});
		}
		None => {}
	}
	let fd = crate::catalog::FieldDefinition::storage_name(idiom);
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
	// For sub-field paths, look up the top-level parent field by the name its
	// definition is stored under
	if let Some(Part::Field(first)) = idiom.0.first() {
		let parent = crate::catalog::FieldDefinition::storage_name(&Idiom::field(first.clone()));
		// Allow the index when the parent field exists and its type
		// permits sub-field access. If no type is set (field_kind is
		// None), the field is unconstrained and sub-fields are allowed.
		if let Some(f) = txn.get_tb_field(ns, db, table, &parent, None).await?
			&& f.field_kind.as_ref().is_none_or(|k| k.allows_sub_fields())
		{
			// Sub-fields of computed fields cannot be indexed
			if f.computed.is_some() {
				bail!(ExecError::ComputedFieldCannotBeIndexed {
					field: parent,
					index: index.to_owned()
				});
			}
			return Ok(());
		}
	}
	if schemafull {
		bail!(CatalogError::FdNotFound {
			name: idiom.to_raw_string(),
		});
	}
	Ok(())
}

/// How an index column relates to a component of the table's record id.
enum ComponentColumn {
	/// The column is the component: the name alone, or `id[i]`.
	Whole,
	/// The column is a path beneath the component, which is the component's
	/// name.
	Beneath(String),
}

/// How `idiom` relates to the named components of `table`'s record id, or
/// `None` when it names none: its root is not a component's name and it is not
/// `id[i]` for a component of the key.
///
/// The structure is the one the `id` field declares now
/// ([`FieldDefinition::live_key`](crate::catalog::FieldDefinition::live_key)).
async fn key_component_column(
	txn: &Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	table: &TableName,
	idiom: &Idiom,
) -> Result<Option<ComponentColumn>> {
	let Some(Part::Field(root)) = idiom.0.first() else {
		return Ok(None);
	};
	let Some(id) = txn.get_tb_field(ns, db, table, "id", None).await? else {
		return Ok(None);
	};
	let Some(key) = id.live_key() else {
		return Ok(None);
	};
	if key.component_of(idiom).is_some() {
		return Ok(Some(ComponentColumn::Whole));
	}
	let names_component =
		key.names.as_ref().is_some_and(|names| names.position(root.as_str()).is_some());
	Ok(names_component.then(|| ComponentColumn::Beneath(root.as_str().to_owned())))
}
