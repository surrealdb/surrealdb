//! The rules a field definition keeps whichever statement forms it.
//!
//! `DEFINE FIELD` and `ALTER FIELD` check the definition they are about to
//! write against these, and so does the VM's run-time check of the
//! definition `ALTER FIELD` merges from the stored field and its clauses, so
//! every route refuses the same definitions with the same errors. Each rule
//! reads only the definitions it is given: the caller reads the catalog.

use anyhow::{Result, ensure};
use surrealdb_types::ToSql;

use crate::catalog::{self, Error as CatalogError};
use crate::exec::Error as ExecError;
use crate::expr::RecordIdKeyLit;
use crate::expr::statements::define::kind_contains_object;

/// Refuse the clauses the `id` field does not permit: `VALUE`, `REFERENCE`,
/// `COMPUTED`, `DEFAULT ALWAYS`, `READONLY`, `FLEXIBLE`, and a `TYPE` that is
/// not a valid record-id key kind. A definition of any other field passes.
///
/// A `TYPE` that names the components of its record id is held to three more
/// rules. Each component name must be a plain identifier
/// (`KeyStructure::is_plain_name`). Each component must have a type a key range
/// can reason about (string, int, datetime, uuid, duration, bool or record). The
/// field takes no `SELECT`
/// permission but the full one, because the components are readable as fields
/// of the table, which the table's permissions govern.
///
/// A plain `DEFAULT` and an `ASSERT` are permitted: the write path applies both
/// to the record-id key when it generates the id (the `GenerateRecordId`
/// stage), and the `ASSERT` binds the key to `$key`.
pub(crate) fn id_field_restrictions(def: &catalog::FieldDefinition) -> Result<()> {
	if !def.name.is_id() {
		return Ok(());
	}
	// `VALUE`, `REFERENCE`, and `COMPUTED` are meaningless or unsafe on an
	// immutable primary key.
	ensure!(def.value.is_none(), ExecError::IdFieldKeywordConflict("VALUE".into()));
	ensure!(def.reference.is_none(), ExecError::IdFieldKeywordConflict("REFERENCE".into()));
	ensure!(def.computed.is_none(), ExecError::IdFieldKeywordConflict("COMPUTED".into()));
	// A plain `DEFAULT` supplies the id when none is given; `DEFAULT ALWAYS`
	// would recompute it on every update, which is nonsensical for an
	// immutable id.
	ensure!(
		!matches!(def.default, catalog::DefineDefault::Always(_)),
		ExecError::IdFieldKeywordConflict("DEFAULT ALWAYS".into())
	);
	// The id is implicitly immutable (`READONLY` is redundant) and a record-id
	// key is not an object (`FLEXIBLE` is meaningless).
	ensure!(!def.readonly, ExecError::IdFieldKeywordConflict("READONLY".into()));
	ensure!(!def.flexible, ExecError::IdFieldKeywordConflict("FLEXIBLE".into()));
	// The declared `TYPE` must be representable as a record-id key.
	if let Some(kind) = &def.field_kind {
		ensure!(
			RecordIdKeyLit::kind_supported(kind),
			ExecError::IdFieldUnsupportedKind(kind.to_sql())
		);
	}
	// The components a record id names keep their order in storage, and a
	// permission on the id alone would not cover the fields that read them.
	if let Some(key) = def.live_key() {
		if let (Some(names), Some(kind)) = (&key.names, &def.field_kind)
			&& let Err(error) = catalog::KeyStructure::from_id_field(kind, names)
		{
			return Err(match error {
				catalog::KeyStructureError::UnsupportedKind {
					name,
					kind,
				} => ExecError::IdComponentUnsupportedKind {
					table: def.table.as_str().to_owned(),
					component: name,
					kind,
				},
				catalog::KeyStructureError::NotPlainName(name) => {
					ExecError::IdComponentNameUnsupported {
						table: def.table.as_str().to_owned(),
						component: name,
					}
				}
				_ => ExecError::IdFieldUnsupportedKind(kind.to_sql()),
			}
			.into());
		}
		ensure!(
			matches!(def.select_permission, catalog::Permission::Full),
			ExecError::IdFieldSelectPermission(def.table.as_str().to_owned())
		);
	}
	Ok(())
}

/// Refuse an `INLINE` definition that cannot be embedded into its edges'
/// adjacency payloads, in the order `ALTER FIELD`'s body refuses it: on a
/// table — `table`, the definition of the table it names — that is not a
/// `TYPE RELATION` table, for the `id`, `in` or `out` field, for a nested
/// field, for a `COMPUTED` field, and as the 256th `INLINE` field of the
/// table, where `fields` are the table's field definitions. A definition that
/// is not `INLINE` passes.
pub(crate) fn inline_restrictions(
	def: &catalog::FieldDefinition,
	table: &catalog::TableDefinition,
	fields: &[catalog::FieldDefinition],
) -> Result<()> {
	if !def.inline {
		return Ok(());
	}
	ensure!(
		matches!(table.table_type, catalog::TableType::Relation(_)),
		ExecError::Thrown(
			"INLINE requires an existing TYPE RELATION table: the value is embedded into the \
			 edges' adjacency payloads"
				.to_owned(),
		)
	);
	ensure!(
		!matches!(def.name.to_raw_string().as_str(), "id" | "in" | "out"),
		ExecError::Thrown(
			"the `id`, `in` and `out` fields already live in the adjacency keys and cannot be \
			 INLINE"
				.to_owned(),
		)
	);
	ensure!(
		def.name.len() == 1,
		ExecError::Thrown("only a top-level field can be INLINE".to_owned())
	);
	ensure!(
		def.computed.is_none(),
		ExecError::Thrown(
			"a COMPUTED field cannot be INLINE: its value is derived at read time, so there is \
			 nothing stored to embed"
				.to_owned(),
		)
	);
	// The payload's value count is a single byte.
	let others = fields.iter().filter(|f| f.inline && f.name != def.name).count();
	ensure!(
		others < u8::MAX as usize,
		ExecError::Thrown(
			"a table cannot carry more than 255 INLINE fields: the adjacency payload's value \
			 count is a single byte"
				.to_owned(),
		)
	);
	Ok(())
}

/// Refuse a `FLEXIBLE` definition whose kind contains no object, then one
/// whose table — `table`, the definition of the table it names — is missing
/// or not `SCHEMAFULL`. A definition that is not `FLEXIBLE` passes.
pub(crate) fn flexible_restrictions(
	def: &catalog::FieldDefinition,
	table: Option<&catalog::TableDefinition>,
) -> Result<()> {
	if !def.flexible {
		return Ok(());
	}
	ensure!(
		def.field_kind.as_ref().is_some_and(kind_contains_object),
		ExecError::Thrown("FLEXIBLE can only be used with types containing object".into())
	);
	let Some(table) = table else {
		return Err(CatalogError::TbNotFound {
			name: def.table.clone(),
		}
		.into());
	};
	ensure!(
		table.schemafull,
		ExecError::Thrown("FLEXIBLE can only be used in SCHEMAFULL tables".into())
	);
	Ok(())
}
