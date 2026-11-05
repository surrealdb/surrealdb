//! The definition-time rules of `MATERIALIZE INTO`.
//!
//! A `REFERENCE` field with the clause keeps, on each record it references, the list of the
//! records that reference it ([`crate::exec::materialize`]). The list is held by a field of the
//! referenced table that the clause names, its destination. The destination is declared on that
//! table, as any field is, and the clause only fills it: it is `READONLY`, so that no statement
//! but the engine's own changes it, and it holds a list of the records of the referencing table.
//! At most one clause fills a destination, and a destination is neither altered nor removed while
//! a clause fills it.

use anyhow::Result;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, DatabaseId, NamespaceId};
use crate::exec::Error as ExecError;
use crate::expr::{Idiom, Kind};
use crate::kvs::Transaction;
use crate::val::TableName;

fn refuse(message: String) -> anyhow::Error {
	anyhow::Error::new(ExecError::Thrown(message))
}

/// The tables the kind `kind` of a `REFERENCE` field can name a record of, or `None` when it can
/// hold anything else but a record or nothing.
fn referenced_tables(kind: &Kind, out: &mut Vec<TableName>) -> bool {
	if let Kind::Record(tables) = kind {
		if tables.is_empty() {
			return false;
		}
		out.extend(tables.iter().cloned());
		true
	} else if let Kind::Array(inner, _) | Kind::Set(inner, _) = kind {
		referenced_tables(inner, out)
	} else if let Kind::Either(kinds) = kind {
		kinds.iter().all(|kind| matches!(kind, Kind::None) || referenced_tables(kind, out))
	} else {
		matches!(kind, Kind::None)
	}
}

/// Whether a record of `child` may be held by a field of kind `kind`.
fn admits_record(kind: &Kind, child: &TableName) -> bool {
	if let Kind::Record(tables) = kind {
		tables.is_empty() || tables.contains(child)
	} else if let Kind::Either(kinds) = kind {
		kinds.iter().any(|kind| admits_record(kind, child))
	} else {
		matches!(kind, Kind::Any)
	}
}

/// Whether the field of kind `kind` may hold a list of the records of `child`, of any length.
fn admits_list(kind: &Kind, child: &TableName) -> bool {
	if let Kind::Array(inner, max) = kind {
		max.is_none() && admits_record(inner, child)
	} else if let Kind::Either(kinds) = kind {
		kinds.iter().any(|kind| admits_list(kind, child))
	} else {
		matches!(kind, Kind::Any)
	}
}

/// The field of `definition`'s `MATERIALIZE INTO` clause, as the catalog holds it.
fn storage_name(field: &Idiom) -> String {
	catalog::FieldDefinition::storage_name(field)
}

/// Refuse `definition`'s `MATERIALIZE INTO` clause unless the field and the destination it names
/// satisfy the rules of the clause: the field is a `REFERENCE` to the destination's table alone,
/// the table exists and declares the destination as a `READONLY` list of records of the field's
/// table that is not computed, no other clause fills it, and it is no field of the clause's own
/// table that the clause would fill with itself. A definition with no clause passes.
pub(crate) async fn validate(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	let Some(materialize) =
		definition.reference.as_ref().and_then(|reference| reference.materialize.as_ref())
	else {
		return Ok(());
	};
	let shown = definition.name.to_raw_string();
	let into = format!("{}.{}", materialize.table, materialize.field.to_raw_string());
	let mut targets = Vec::new();
	let single =
		definition.field_kind.as_ref().is_some_and(|kind| referenced_tables(kind, &mut targets))
			&& !targets.is_empty()
			&& targets.iter().all(|table| *table == materialize.table);
	if !single {
		return Err(refuse(format!(
			"MATERIALIZE INTO {into} requires field `{shown}` to reference records of table `{}` only",
			materialize.table
		)));
	}
	if materialize.field.len() != 1 || materialize.field.is_id() {
		return Err(refuse(format!(
			"MATERIALIZE INTO {into} requires the destination to be a top-level field other than `id`"
		)));
	}
	if txn.get_tb(ns, db, &materialize.table, None).await?.is_none() {
		return Err(refuse(format!(
			"MATERIALIZE INTO {into} requires table `{}` to exist",
			materialize.table
		)));
	}
	let Some(destination) = txn
		.get_tb_field(ns, db, &materialize.table, &storage_name(&materialize.field), None)
		.await?
	else {
		return Err(refuse(format!(
			"MATERIALIZE INTO {into} requires field `{}` to be defined on table `{}` first",
			materialize.field.to_raw_string(),
			materialize.table
		)));
	};
	if destination.computed.is_some() {
		return Err(refuse(format!("MATERIALIZE INTO {into} cannot fill a COMPUTED field")));
	}
	if !destination.readonly {
		return Err(refuse(format!(
			"MATERIALIZE INTO {into} requires field `{}` to be READONLY: the engine keeps its list",
			materialize.field.to_raw_string()
		)));
	}
	let admitted =
		destination.field_kind.as_ref().is_some_and(|kind| admits_list(kind, &definition.table));
	if !admitted {
		return Err(refuse(format!(
			"MATERIALIZE INTO {into} requires field `{}` to hold an array of record<{}> with no length limit",
			materialize.field.to_raw_string(),
			definition.table
		)));
	}
	if definition.table == materialize.table && definition.name == materialize.field {
		return Err(refuse(format!(
			"MATERIALIZE INTO {into} cannot fill the field it is defined on"
		)));
	}
	if let Some((table, field)) =
		filler(txn, ns, db, &materialize.table, &materialize.field).await?
		&& !(table == definition.table && field == definition.name)
	{
		return Err(refuse(format!(
			"MATERIALIZE INTO {into} is filled by field `{}` of table `{table}` already",
			field.to_raw_string()
		)));
	}
	Ok(())
}

/// The field whose `MATERIALIZE INTO` clause fills the field `dest` of table `table`, with its
/// table, if one does.
async fn filler(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	dest: &Idiom,
) -> Result<Option<(TableName, Idiom)>> {
	for definition in txn.all_tb(ns, db, None).await?.iter() {
		for field in txn.all_tb_fields(ns, db, &definition.name, None).await?.iter() {
			let fills = field
				.reference
				.as_ref()
				.and_then(|reference| reference.materialize.as_ref())
				.is_some_and(|m| m.table == *table && m.field == *dest);
			if fills {
				return Ok(Some((definition.name.clone(), field.name.clone())));
			}
		}
	}
	Ok(None)
}

/// Refuse a change to the field `name` of table `table` while a `MATERIALIZE INTO` clause fills
/// it.
pub(crate) async fn refuse_changing_destination(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	name: &Idiom,
) -> Result<()> {
	if let Some((from, field)) = filler(txn, ns, db, table, name).await? {
		return Err(refuse(format!(
			"Field `{}` of table `{table}` cannot be altered or removed while the MATERIALIZE INTO clause of field `{}` of table `{from}` fills it",
			name.to_raw_string(),
			field.to_raw_string()
		)));
	}
	Ok(())
}

/// Refuse the removal of table `table` while a `MATERIALIZE INTO` clause fills one of its fields.
pub(crate) async fn refuse_removing_table(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> Result<()> {
	for definition in txn.all_tb(ns, db, None).await?.iter() {
		if definition.name == *table {
			continue;
		}
		for field in txn.all_tb_fields(ns, db, &definition.name, None).await?.iter() {
			let fills = field
				.reference
				.as_ref()
				.and_then(|reference| reference.materialize.as_ref())
				.is_some_and(|m| m.table == *table);
			if fills {
				return Err(refuse(format!(
					"Table `{table}` cannot be removed while the MATERIALIZE INTO clause of field `{}` of table `{}` fills one of its fields",
					field.name.to_raw_string(),
					definition.name
				)));
			}
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn kind(text: &str) -> Kind {
		crate::syn::kind(text).expect("a kind")
	}

	fn tables(text: &str) -> Option<Vec<String>> {
		let mut out = Vec::new();
		referenced_tables(&kind(text), &mut out)
			.then(|| out.into_iter().map(|table| table.as_str().to_owned()).collect())
	}

	#[test]
	fn a_reference_field_names_the_tables_of_its_records() {
		assert_eq!(tables("record<author>"), Some(vec!["author".to_owned()]));
		assert_eq!(tables("option<record<author>>"), Some(vec!["author".to_owned()]));
		assert_eq!(tables("array<record<author>>"), Some(vec!["author".to_owned()]));
		assert_eq!(tables("set<record<author>>"), Some(vec!["author".to_owned()]));
		assert_eq!(
			tables("record<author | editor>"),
			Some(vec!["author".to_owned(), "editor".to_owned()])
		);
	}

	#[test]
	fn a_field_that_may_hold_anything_else_names_no_tables() {
		assert_eq!(tables("record"), None);
		assert_eq!(tables("string"), None);
		assert_eq!(tables("any"), None);
		assert_eq!(tables("array<record>"), None);
		assert_eq!(tables("record<author> | string"), None);
	}

	#[test]
	fn a_list_holds_records_of_the_referencing_table_with_no_length_limit() {
		let post = TableName::from("post");
		assert!(admits_list(&kind("array<record<post>>"), &post));
		assert!(admits_list(&kind("array<record<post | page>>"), &post));
		assert!(admits_list(&kind("array<record>"), &post));
		assert!(admits_list(&kind("array"), &post));
		assert!(admits_list(&kind("array<any>"), &post));
		assert!(admits_list(&kind("any"), &post));
		assert!(admits_list(&kind("option<array<record<post>>>"), &post));
		assert!(!admits_list(&kind("array<record<post>, 3>"), &post));
		assert!(!admits_list(&kind("array<record<page>>"), &post));
		assert!(!admits_list(&kind("array<string>"), &post));
		assert!(!admits_list(&kind("set<record<post>>"), &post));
		assert!(!admits_list(&kind("record<post>"), &post));
	}
}
