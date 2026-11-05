//! `DEFINE FIELD`, `REMOVE FIELD` and `ALTER FIELD`.
//!
//! A statement whose field path and table are written, and whose checks the planner made
//! ([`DdlChecks`]), is lowered against the catalog it was planned over: the refusals the checks
//! decided are raised where the statement raises them, and the facts they carry (the table and
//! field as they stood) decide the definitions the statement writes beside the field. Any other
//! statement, one with an evaluated name, table or comment, or whose checks were not made, resolves
//! its names when it runs and hands the definition to the host, which checks it against the
//! catalog it finds and writes it.
//!
//! The definition a statement writes is its template, a stored definition with the field path and
//! the table filled in where the statement writes them.

use std::sync::Arc;

use super::{
	AlterAttrs, AlterKind, AlterTemplate, Ddl, DdlGuard, DefineAttrs, DefineKind, Instr, IrError,
	NameClause, Operand, OperandSlot, Reg, RouteId, StatementError, StoreInstr, StoreRefusal,
	TypeId, Value,
};
use crate::catalog::{self, TableType};
use crate::exec::ir::iface::statements::{AlterField, DdlChecks, DefineField};
use crate::exec::ir::logical::ddl::{
	self as statement, field_definition_of, written_field_template,
};
use crate::exec::vm::program::{ClauseObject, FieldCheck, FieldRefusal, MigrateSpec, PathSource};
use crate::expr::statements::define::MigrateClause;
use crate::expr::{Idiom, Kind};
use crate::val::TableName;

/// The refusal of an `INLINE` field the table it is defined on cannot embed, as the body raises
/// it: `table` is the table as the checks found it, `(name, path_len, computed)` the field, and
/// `others` the number of other `INLINE` fields the table carries.
fn inline_field_refusal(
	table: Option<&catalog::TableDefinition>,
	(name, path_len, computed): (&str, usize, bool),
	others: usize,
) -> Option<FieldRefusal> {
	let thrown = |message: &str| Some(FieldRefusal::Thrown(message.to_owned()));
	if !table.is_some_and(|tb| matches!(tb.table_type, TableType::Relation(_))) {
		return thrown(
			"INLINE requires an existing TYPE RELATION table: the value is embedded into the \
			 edges' adjacency payloads",
		);
	}
	if matches!(name, "id" | "in" | "out") {
		return thrown(
			"the `id`, `in` and `out` fields already live in the adjacency keys and cannot be \
			 INLINE",
		);
	}
	if path_len != 1 {
		return thrown("only a top-level field can be INLINE");
	}
	if computed {
		return thrown(
			"a COMPUTED field cannot be INLINE: its value is derived at read time, so there is \
			 nothing stored to embed",
		);
	}
	if others >= u8::MAX as usize {
		return thrown(
			"a table cannot carry more than 255 INLINE fields: the adjacency payload's value \
			 count is a single byte",
		);
	}
	None
}

impl Ddl<'_, '_> {
	/// The path the field name operand `slot` writes, and the slots of the operands that follow it
	/// for the subscripts it leaves to be evaluated, in the order the path holds them. `None` for
	/// an operand that is no written text.
	fn written_path(
		&self,
		slot: OperandSlot,
	) -> Result<Option<(Idiom, Vec<OperandSlot>)>, IrError> {
		statement::written_path(self.ir, &self.operands, slot)
	}

	/// The field path and the table a statement writes, when both are written and the path holds
	/// nothing the statement evaluates and no step beyond a field, `*`, `.first()`, `.last()`,
	/// `...` or an integer subscript; the host resolves any other path.
	fn written_field(
		&self,
		name: OperandSlot,
		table: OperandSlot,
	) -> Result<Option<(Idiom, TableName)>, IrError> {
		statement::written_field(self.ir, &self.operands, name, table)
	}

	/// Resolves the field path operand `slot` into a register when the program runs: a written
	/// path keeps its structure, with the operands of its evaluated subscripts read in order, and
	/// any other value is read as the text of a path.
	fn field_path(&mut self, slot: OperandSlot) -> Result<Reg, IrError> {
		let source = match self.written_path(slot)? {
			Some((idiom, slots)) => {
				let written = self
					.literal(slot)?
					.cloned()
					.ok_or(IrError::Stale("ir_lower::ddl: a written field path that is gone"))?;
				let operand = self.constant(written)?;
				self.read_as(slot, operand)?;
				let mut holes = Vec::with_capacity(slots.len());
				for hole in slots {
					holes.push(self.evaluated(hole)?);
				}
				PathSource::Written {
					idiom,
					holes,
				}
			}
			None => PathSource::Evaluated(self.evaluated(slot)?),
		};
		let dst = self.reg()?;
		self.store(StoreInstr::ResolvePath {
			source,
			validated: true,
			dst,
		});
		Ok(dst)
	}

	/// Consumes the authority guard of a statement whose host makes the check itself.
	fn leave_authority_to_host(&mut self) -> Result<(), IrError> {
		match self.guards.pop_front() {
			Some(DdlGuard::Allow {
				..
			}) => Ok(()),
			Some(DdlGuard::RefuseNoWrite) | None => {
				Err(IrError::Stale("ir_lower::ddl: the authority guard is missing"))
			}
		}
	}

	/// `DEFINE FIELD`.
	pub(super) fn define_field(
		&mut self,
		attrs: &DefineAttrs,
		field: &DefineField,
	) -> Result<(), IrError> {
		let comment_written =
			matches!(self.literal(field.comment)?, Some(Value::None | Value::String(_)));
		let written = self.written_field(field.name, field.what)?;
		let (
			Some((name, table)),
			true,
			DdlChecks::DefineField {
				existing_table,
				existing_field,
				fields,
				indexed,
				refused,
				flexible,
			},
		) = (written, comment_written, &attrs.checks)
		else {
			return self.define_field_resolved(attrs, field);
		};
		let fd = catalog::FieldDefinition::storage_name(&name);
		let template_value = written_field_template(&attrs.template.value, &fd, table.as_str());
		let definition = field_definition_of(&template_value)?;
		// A definition the nesting rules refuse, or one that leaves element definitions an
		// earlier kind implied behind, is defined by the host, which judges the nesting and
		// writes and deletes the element definitions as `DEFINE FIELD` does.
		let Some(table_fields) = fields.as_ref() else {
			return self.define_field_resolved(attrs, field);
		};
		if crate::exec::field_rules::define_field_nesting_restrictions(table_fields, &definition)
			.is_err() || !crate::exec::field_rules::implied_definitions_dropped(
			table_fields,
			existing_field.as_deref(),
			&definition,
		)
		.is_empty()
		{
			return self.define_field_resolved(attrs, field);
		}
		let written_name = self.constant(Value::String(fd.as_str().into()))?;
		self.read_as(field.name, written_name)?;
		let written_table = self.constant(Value::String(table.as_str().into()))?;
		self.read_as(field.what, written_table)?;
		self.comment(field.comment)?;

		// An `INLINE` field's refusal is raised after the refusal of a field on a lightweight
		// relation, which the program makes first, and is skipped on an import.
		let lightweight_table = existing_table.as_ref().is_some_and(|tb| {
			crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_some()
		});
		let mut inline_refusal = None;
		if field.inline && !lightweight_table {
			let fields = fields.as_ref().ok_or(IrError::Stale(
				"ir_lower::ddl: a field definition whose catalog did not read",
			))?;
			let others = fields.iter().filter(|f| f.inline && f.name != name).count();
			inline_refusal = inline_field_refusal(
				existing_table.as_deref(),
				(fd.as_str(), name.len(), field.computed.is_some()),
				others,
			);
		}
		// A record kind for a relation's `in` or `out` that is not the one the table names
		// retargets the relation: the table is written with it, and nothing else the field's kind
		// implies is derived.
		let mut retarget = None;
		// A kind that is no record for a relation's `in` or `out` is raised only when the
		// definition does not exist yet.
		let mut relation_refusal = None;
		if matches!(fd.as_str(), "in" | "out")
			&& let Some(existing) = existing_table.as_ref()
			&& let TableType::Relation(relation) = &existing.table_type
			&& let Some(kind) = &field.field_kind
		{
			if let Kind::Record(field_kind) = kind {
				let current = if fd == "in" {
					&relation.from
				} else {
					&relation.to
				};
				if field_kind != current {
					let mut relation = relation.clone();
					if fd == "in" {
						relation.from.clone_from(field_kind);
					} else {
						relation.to.clone_from(field_kind);
					}
					// The table type in the form the stored table definition holds.
					retarget = Some(
						crate::catalog::stored::StoredField::into_vm_value(TableType::Relation(
							relation,
						))
						.map_err(|_| {
							IrError::Stale("ir_lower::ddl: a table type with no stored form")
						})?,
					);
				}
			} else {
				relation_refusal = Some(FieldRefusal::Thrown(format!(
					"{fd} field on a relation must be a record"
				)));
			}
		}
		// The sub-field definitions an array or set kind implies, derived from the fields already
		// defined, each keyed by its storage name as `put_tb_field` keys an explicit one.
		let mut nested = Vec::new();
		if retarget.is_none() {
			let implied =
				crate::exec::field_rules::implied_definitions(&name, field.field_kind.as_ref());
			for (path, kind) in implied {
				let existing = table_fields.iter().find(|f| f.name == path);
				let sub =
					crate::exec::field_rules::implied_definition(&definition, existing, path, kind);
				nested.push((
					catalog::FieldDefinition::storage_name(&sub.name),
					super::stored(&sub.to_stored(), "StoredFieldDefinition")?,
				));
			}
		}

		self.refuse_no_write()?;
		// A computed field an index of the table covers is refused as its definition is formed,
		// before the permission check.
		if let Some(refusal) = indexed {
			self.store(StoreInstr::RefuseField {
				refusal: refusal.clone(),
			});
		}
		self.allow()?;
		let (ns, db, database) = self.selected_ids(StatementError::DbNotFound)?;
		if let Some(refusal) = refused {
			self.store(StoreInstr::RefuseField {
				refusal: refusal.clone(),
			});
		}
		let template = self.constant(template_value)?;
		// The record id components an `id` names, judged against the catalog and the rows of the
		// table's views as the statements before this one leave them.
		if name.is_id() {
			let id_field = self.load(template)?;
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::KeyComponents {
					field: id_field,
					ns,
					db,
					kind: attrs.mode.clone(),
				},
			});
		}
		if let Some(refusal) = flexible {
			self.store(StoreInstr::RefuseField {
				refusal: refusal.clone(),
			});
		}
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let table_name = self.constant(Value::String(table.as_str().into()))?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", table_name)])?;
		let tb = self.table_creating((ns, db, database), table_name, tb_key)?;
		self.store(StoreInstr::RefuseLightweight {
			table: tb,
			refusal: StoreRefusal::Field,
		});
		if let Some(refusal) = inline_refusal {
			let import = self.label()?;
			self.store(StoreInstr::JumpIfImport {
				target: import,
			});
			self.store(StoreInstr::RefuseField {
				refusal,
			});
			self.bind(import)?;
		}

		let fd_operand = self.constant(Value::String(fd.as_str().into()))?;
		let fd_key =
			self.object(&[("ns", ns_op), ("db", db_op), ("tb", table_name), ("fd", fd_operand)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FieldKey"),
			key: fd_key,
			ty: TypeId::named("StoredFieldDefinition"),
			dst: existing,
		});
		let shown = self.constant(Value::String(surrealdb_types::ToSql::to_sql(&name).into()))?;
		let shown = self.load(shown)?;
		let missing = self.exists(&attrs.mode, existing, StatementError::FdAlreadyExists, shown)?;
		self.bind(missing)?;
		// The types the field names are judged only where it is defined: an `IF NOT EXISTS` that
		// meets the field stores nothing.
		let names = crate::exec::vm::schema_host::field_named_types(&definition);
		self.check_types_defined((ns, db), names, || {
			format!("The field '{}' on the table '{}'", name.to_raw_string(), table.as_str())
		});
		if let Some(refusal) = relation_refusal {
			self.store(StoreInstr::RefuseField {
				refusal,
			});
		}
		// A record id that names its components must admit every record the table holds, once the
		// statement is known to define the field; the scan is skipped when the stored `id` already
		// has the same structure.
		if let Some(key) = definition.live_key() {
			let table_reg = self.load(table_name)?;
			self.store(StoreInstr::ValidateRecordKeys {
				ns,
				db,
				table: table_reg,
				key: Arc::clone(key),
			});
		} else if name.is_id() {
			// A definition that leaves the table with no structure is held to the order the
			// records it already holds are stored in.
			let id_field = self.load(template)?;
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::DroppedKeyStructure {
					field: id_field,
					ns,
					db,
				},
			});
		}
		let stored_field = self.load(template)?;
		self.session_auth_limit(stored_field)?;
		self.store(StoreInstr::RefuseClauseEffects {
			ns,
			db,
			definition: stored_field,
			previous: None,
			object: ClauseObject::Field,
		});
		// A change to the value the field derives is judged against the definition the statement
		// finds, the views and the records of its table as they stand when it runs, and not as
		// they were planned: a block can change any of them before it. A restore defines the
		// table before it holds a record.
		let import = self.label()?;
		self.store(StoreInstr::JumpIfImport {
			target: import,
		});
		self.store(StoreInstr::FieldCheck {
			check: FieldCheck::ComputedViews {
				old: existing,
				field: stored_field,
				ns,
				db,
			},
		});
		self.bind(import)?;
		// A `MATERIALIZE INTO` clause fills a destination its table declares, and a definition
		// that replaces a destination is refused; an import restores definitions as they were.
		let import = self.label()?;
		self.store(StoreInstr::JumpIfImport {
			target: import,
		});
		self.store(StoreInstr::FieldCheck {
			check: FieldCheck::Destination {
				old: existing,
				ns,
				db,
			},
		});
		self.store(StoreInstr::FieldCheck {
			check: FieldCheck::Materialize {
				field: stored_field,
				ns,
				db,
			},
		});
		self.bind(import)?;
		// The rules of a table that enforces migrations, and the migration, are judged once every
		// other check of the definition has passed: either can mint a step, and a refusal leaves
		// no step behind for a transaction its caller goes on to commit.
		self.check_field_change(
			(ns, db),
			table_name,
			(stored_field, existing),
			field.migrate.is_some(),
			crate::exec::migrate::KeptClauses::default(),
		)?;
		if let Some(migrate) = &field.migrate {
			self.migrate((ns, db), table_name, (stored_field, existing), migrate)?;
		}
		self.store(StoreInstr::Put {
			route: RouteId::named("FieldKey"),
			key: fd_key,
			ty: TypeId::named("StoredFieldDefinition"),
			value: stored_field,
		});
		if field.migrate.is_none() {
			self.queue_verification((ns, db), table_name, (stored_field, existing))?;
		}
		// Overwriting a reference field can strand the reference keys it wrote under target
		// tables it no longer references, and a field that starts or widens its `REFERENCE` over
		// stored records has keys to write for the links they hold; an import restores the keys
		// verbatim.
		let import = self.label()?;
		self.store(StoreInstr::JumpIfImport {
			target: import,
		});
		let table_reg = self.load(table_name)?;
		self.store(StoreInstr::PurgeDroppedReferences {
			ns,
			db,
			table: table_reg,
			old: existing,
			new: Some(stored_field),
			rebuild: true,
		});
		self.bind(import)?;
		self.stamp(ns, db, table_name)?;
		// `replace_tb` with the fields' cache stamp refreshed.
		let fields_ts = self.reg()?;
		self.store(StoreInstr::NewUuid {
			dst: fields_ts,
			draw: 0,
			reserved: None,
		});
		self.set_field(tb, "cache_fields_ts", Operand::Reg(fields_ts))?;
		if let Some(table_type) = retarget.as_ref() {
			let table_type = self.constant(table_type.clone())?;
			self.set_field(tb, "table_type", table_type)?;
		}
		let was_inline = existing_field.as_ref().is_some_and(|f| f.inline);
		let inline_toggled = field.inline != was_inline;
		let inline_kind_changed = field.inline
			&& was_inline
			&& existing_field.as_ref().is_some_and(|f| f.field_kind != field.field_kind);
		if retarget.is_none()
			&& (inline_toggled || inline_kind_changed)
			&& let Some(existing_table) = existing_table
		{
			let next_gen = existing_table.graph_inline_gen.wrapping_add(1);
			let generation = self.constant(Value::from(next_gen))?;
			self.set_field(tb, "graph_inline_gen", generation)?;
		}
		self.store(StoreInstr::Lock {
			route: RouteId::named("TableKey"),
			key: tb_key,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key: tb_key,
			ty: TypeId::named("StoredTableDefinition"),
			value: tb,
		});
		self.stamp(ns, db, table_name)?;
		for (path, value) in nested {
			let path = self.constant(Value::String(path.into()))?;
			let key =
				self.object(&[("ns", ns_op), ("db", db_op), ("tb", table_name), ("fd", path)])?;
			let value = self.constant(value)?;
			let value = self.load(value)?;
			self.store(StoreInstr::Put {
				route: RouteId::named("FieldKey"),
				key,
				ty: TypeId::named("StoredFieldDefinition"),
				value,
			});
		}
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `DEFINE FIELD` for a field, table or comment that is evaluated, or whose checks were not
	/// made. The names are resolved as the body resolves them (the comment, the field's path, the
	/// table) and the definition they form is handed to the host, which checks it against the
	/// catalog the statement runs against and writes it. Only the statement's real run defines the
	/// field: the host allocates the identifiers a table the field defines on demand takes, which
	/// a speculative run would allocate again.
	fn define_field_resolved(
		&mut self,
		attrs: &DefineAttrs,
		field: &DefineField,
	) -> Result<(), IrError> {
		self.refuse_no_write()?;
		if field.migrate.is_some() && self.written_field(field.name, field.what)?.is_none() {
			self.refuse_late_bound_migration();
		}
		let comment = self.comment(field.comment)?;
		let name = self.field_path(field.name)?;
		let table = self.name(field.what, NameClause::Table)?;
		self.leave_authority_to_host()?;
		self.store(StoreInstr::RealRunOnly);
		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		self.set_field(definition, "name", Operand::Reg(name))?;
		self.set_name(definition, "table", &table)?;
		self.set_comment(definition, &comment)?;
		self.store(StoreInstr::DefineField {
			definition,
			kind: attrs.mode.clone(),
			migrate: field.migrate.clone(),
		});
		self.answer_none()
	}

	/// `REMOVE FIELD`. A statement whose field path and table are written, and whose checks were
	/// made, is lowered against the table and field as they were planned; any other resolves its
	/// names when it runs and is removed by the host.
	pub(super) fn remove_field(
		&mut self,
		(name, table): (OperandSlot, OperandSlot),
		checks: &DdlChecks,
		if_exists: bool,
	) -> Result<(), IrError> {
		let written = self.written_field(name, table)?;
		let (
			Some((path, table_name)),
			DdlChecks::Field {
				existing_table,
				existing_field,
				fields,
				key_refusal,
			},
		) = (written, checks)
		else {
			return self.remove_field_resolved((name, table), if_exists);
		};
		// A field with fields nested in it is removed by the host, which removes them with it,
		// each under its storage name, and the reference keys each wrote.
		if fields.iter().any(|fd| fd.name.len() > path.len() && fd.name.starts_with(&path)) {
			return self.remove_field_resolved((name, table), if_exists);
		}
		let fd = catalog::FieldDefinition::storage_name(&path);
		let written_name = self.constant(Value::String(fd.as_str().into()))?;
		self.read_as(name, written_name)?;
		let written_table = self.constant(Value::String(table_name.as_str().into()))?;
		self.read_as(table, written_table)?;
		// A lightweight relation's field is refused before the field is looked up.
		let lightweight_table = existing_table.as_ref().is_some_and(|existing| {
			crate::kvs::lightweight::lightweight_relation(&existing.table_type).is_some()
		});
		let was_inline = existing_field.as_ref().is_some_and(|f| f.inline);
		self.guard()?;
		if lightweight_table {
			self.store(StoreInstr::RefuseField {
				refusal: FieldRefusal::Thrown(
					"a LIGHTWEIGHT relation's fields cannot be removed: its in/out contract is \
					 managed by the table definition"
						.to_owned(),
				),
			});
		}
		// A component of a named record id has no definition to remove, and the `id` stays while
		// a definition reads a component, as the catalog and the rows of the table's views stand
		// when the statement runs.
		if let Some(refusal) = key_refusal {
			self.store(StoreInstr::RefuseField {
				refusal: refusal.clone(),
			});
		}
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb = self.constant(Value::String(table_name.as_str().into()))?;
		if path.is_id() {
			let table_reg = self.load(tb)?;
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::IdUnread {
					ns,
					db,
					table: table_reg,
				},
			});
		}
		let fd = self.constant(Value::String(fd.into()))?;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("fd", fd)])?;
		let field = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FieldKey"),
			key,
			ty: TypeId::named("StoredFieldDefinition"),
			dst: field,
		});
		let name_reg = self.load(fd)?;
		self.must_exist(if_exists, field, StatementError::FdNotFound, name_reg)?;
		// Removing a `COMPUTED` field changes the value it derives to none, which the field the
		// statement finds, the views and the records of its table decide when it runs, and not as
		// they were planned: a block can change any of them before it.
		let removed = catalog::FieldDefinition {
			name: path,
			table: table_name,
			..Default::default()
		};
		let removed =
			self.constant(super::stored(&removed.to_stored(), "StoredFieldDefinition")?)?;
		let removed = self.load(removed)?;
		let import = self.label()?;
		self.store(StoreInstr::JumpIfImport {
			target: import,
		});
		self.store(StoreInstr::FieldCheck {
			check: FieldCheck::ComputedViews {
				old: field,
				field: removed,
				ns,
				db,
			},
		});
		self.store(StoreInstr::FieldCheck {
			check: FieldCheck::Destination {
				old: field,
				ns,
				db,
			},
		});
		self.bind(import)?;
		// On a table that enforces migrations the field's stored values go with it; an import
		// restores definitions as they were.
		let import = self.label()?;
		self.store(StoreInstr::JumpIfImport {
			target: import,
		});
		let step_table = self.load(tb)?;
		self.store(StoreInstr::RemoveFieldStep {
			ns,
			db,
			table: step_table,
			field,
		});
		self.bind(import)?;
		// `del_tb_field`.
		self.store(StoreInstr::Delete {
			route: RouteId::named("FieldKey"),
			key,
		});
		// The keys a reference field wrote under its target tables are not the definition's to
		// remove; a removed field leaves none behind, whether or not the statement is an import.
		let table_reg = self.load(tb)?;
		self.store(StoreInstr::PurgeDroppedReferences {
			ns,
			db,
			table: table_reg,
			old: field,
			new: None,
			rebuild: false,
		});
		self.stamp(ns, db, tb)?;
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table_def = self.expect_table(tb_key, tb_name)?;
		if was_inline && let Some(existing) = existing_table {
			let next_gen = existing.graph_inline_gen.wrapping_add(1);
			let generation = self.constant(Value::from(next_gen))?;
			self.set_field(table_def, "graph_inline_gen", generation)?;
		}
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_fields_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `REMOVE FIELD` for a field or table that is evaluated, or whose checks were not made:
	/// resolved as the body resolves them, the table and then the field's path, and removed by the
	/// host against the catalog the statement runs against.
	fn remove_field_resolved(
		&mut self,
		(name, table): (OperandSlot, OperandSlot),
		if_exists: bool,
	) -> Result<(), IrError> {
		self.guard()?;
		let table = self.name(table, NameClause::Table)?;
		let table = self.load(table.operand)?;
		let name = self.field_path(name)?;
		self.store(StoreInstr::RealRunOnly);
		self.store(StoreInstr::RemoveFieldDefinition {
			name,
			table,
			if_exists,
		});
		self.answer_none()
	}

	/// `ALTER FIELD`. A statement whose field path and table are written, and whose checks were
	/// made, is lowered against the table and field as they were planned: the stored field, read
	/// when the statement runs, has each altered clause replaced, and the definition that forms
	/// is checked as the body checks it (the `id` field's restrictions, the record id components it
	/// names, the `REFERENCE` it sets or keeps over a changed type, `INLINE`'s, then `FLEXIBLE`'s,
	/// and the stored records against the record id the type names) before it is written. Any
	/// other statement resolves its names when it runs and is altered by the host.
	pub(super) fn alter_field(
		&mut self,
		attrs: &AlterAttrs,
		field: &AlterField,
	) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		let written = self.written_field(field.name, field.what)?;
		let (
			Some((path, table_name)),
			DdlChecks::Field {
				existing_table,
				existing_field,
				key_refusal,
				..
			},
		) = (written, &attrs.checks)
		else {
			return self.alter_field_resolved(attrs, field, template);
		};
		// An altered kind is altered by the host, which judges it against the fields the field is
		// nested in and the fields nested in the field against it, and writes and deletes the
		// element definitions it implies, as `DEFINE FIELD` does.
		if !matches!(field.kind, AlterKind::None) {
			return self.alter_field_resolved(attrs, field, template);
		}
		let fd = catalog::FieldDefinition::storage_name(&path);
		let written_name = self.constant(Value::String(fd.as_str().into()))?;
		self.read_as(field.name, written_name)?;
		let written_table = self.constant(Value::String(table_name.as_str().into()))?;
		self.read_as(field.what, written_table)?;
		// Altering a lightweight relation's field is refused once the field is found.
		let lightweight_table = existing_table.as_ref().is_some_and(|tb| {
			crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_some()
		});
		let was_inline = existing_field.as_ref().is_some_and(|f| f.inline);
		let will_be_inline = match field.inline {
			AlterKind::Set(()) => true,
			AlterKind::Drop => false,
			AlterKind::None => was_inline,
		};
		// The record id structure the statement gives the table, when it names components: the
		// records are checked against it before it is written.
		let altered = field_definition_of(&template.value)?;
		let scanned = altered.live_key().cloned();
		// The types the clauses the statement sets name; the kind is unchanged here.
		let named = crate::exec::vm::schema_host::field_named_types(&altered);

		self.guard()?;
		let (ns, db) = self.session_ids()?;
		// A component of a named record id has no definition to alter.
		if let Some(refusal) = key_refusal {
			self.store(StoreInstr::RefuseField {
				refusal: refusal.clone(),
			});
		}
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb = self.constant(Value::String(table_name.as_str().into()))?;
		let fd = self.constant(Value::String(fd.into()))?;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("fd", fd)])?;
		let stored = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("FieldKey"),
			key,
			ty: TypeId::named("StoredFieldDefinition"),
			dst: stored,
		});
		let name_reg = self.load(fd)?;
		self.must_exist(attrs.if_exists, stored, StatementError::FdNotFound, name_reg)?;
		if lightweight_table {
			self.store(StoreInstr::RefuseField {
				refusal: FieldRefusal::Thrown(
					"a LIGHTWEIGHT relation's fields cannot be altered: its in/out contract is \
					 managed by the table definition"
						.to_owned(),
				),
			});
		}
		if !named.is_empty() {
			self.store(StoreInstr::CheckTypesDefined {
				ns,
				db,
				names: named.into(),
				subject: format!(
					"The field '{}' on the table '{}'",
					path.to_raw_string(),
					table_name.as_str()
				),
			});
		}
		// The definition as the statement reads it, which the keys a dropped reference left
		// behind are judged against.
		let before = self.load(Operand::Reg(stored))?;
		self.replace_fields(stored, template)?;
		self.session_auth_limit(stored)?;
		// The checks of the definition the clauses merge into, which only the statement's read of
		// the stored field decides.
		if path.is_id() {
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::IdField {
					field: stored,
				},
			});
			// Only a statement that sets or drops the type can change the record id components
			// the definition names; any other keeps the structure that was checked when it was
			// defined.
			if !matches!(field.kind, AlterKind::None) {
				self.store(StoreInstr::FieldCheck {
					check: FieldCheck::KeyComponents {
						field: stored,
						ns,
						db,
						kind: DefineKind::Overwrite,
					},
				});
				// A merged definition that leaves the table with no structure is held to the
				// order the records it already holds are stored in.
				self.store(StoreInstr::FieldCheck {
					check: FieldCheck::DroppedKeyStructure {
						field: stored,
						ns,
						db,
					},
				});
			}
		}
		// A statement that sets `REFERENCE`, or changes the type of a field whose `REFERENCE` it
		// keeps, has its definition judged as `DEFINE FIELD` judges a reference: a top-level
		// field whose kind holds only records. An import loads definitions as they were written.
		if matches!(field.reference, AlterKind::Set(_))
			|| (!matches!(field.kind, AlterKind::None)
				&& !matches!(field.reference, AlterKind::Drop))
		{
			let import = self.label()?;
			self.store(StoreInstr::JumpIfImport {
				target: import,
			});
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::Reference {
					field: stored,
				},
			});
			self.bind(import)?;
		}
		// A field a `MATERIALIZE INTO` clause fills is not altered, and the clause an altered
		// field holds is judged against its destination.
		{
			let import = self.label()?;
			self.store(StoreInstr::JumpIfImport {
				target: import,
			});
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::Destination {
					old: before,
					ns,
					db,
				},
			});
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::Materialize {
					field: stored,
					ns,
					db,
				},
			});
			self.bind(import)?;
		}
		let tb_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let tb_name = self.load(tb)?;
		let table_def = self.expect_table(tb_key, tb_name)?;
		if !matches!(field.inline, AlterKind::Drop) {
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::Inline {
					field: stored,
					table: table_def,
				},
			});
		}
		if !matches!(field.flexible, AlterKind::Drop) {
			self.store(StoreInstr::FieldCheck {
				check: FieldCheck::Flexible {
					field: stored,
					table: table_def,
				},
			});
		}
		if let Some(key) = scanned {
			let table_reg = self.load(tb)?;
			self.store(StoreInstr::ValidateRecordKeys {
				ns,
				db,
				table: table_reg,
				key,
			});
		}
		self.store(StoreInstr::RefuseClauseEffects {
			ns,
			db,
			definition: stored,
			previous: Some(before),
			object: ClauseObject::Field,
		});
		// The rules of a table that enforces migrations, and the migration, are judged once every
		// other check of the definition has passed, as `DEFINE FIELD` judges them. The type is
		// unchanged here, so no clause is kept over a new one.
		self.check_field_change(
			(ns, db),
			tb,
			(stored, before),
			field.migrate.is_some(),
			crate::exec::migrate::KeptClauses::default(),
		)?;
		if let Some(migrate) = &field.migrate {
			self.migrate((ns, db), tb, (stored, before), migrate)?;
		}
		// `put_tb_field`.
		self.store(StoreInstr::Put {
			route: RouteId::named("FieldKey"),
			key,
			ty: TypeId::named("StoredFieldDefinition"),
			value: stored,
		});
		if field.migrate.is_none() {
			self.queue_verification((ns, db), tb, (stored, before))?;
		}
		// Dropping the reference or changing the record kind can strand the reference keys the
		// field wrote; an import restores the keys verbatim.
		let import = self.label()?;
		self.store(StoreInstr::JumpIfImport {
			target: import,
		});
		let table_reg = self.load(tb)?;
		self.store(StoreInstr::PurgeDroppedReferences {
			ns,
			db,
			table: table_reg,
			old: before,
			new: Some(stored),
			rebuild: false,
		});
		self.bind(import)?;
		self.stamp(ns, db, tb)?;
		let inline_toggled = will_be_inline != was_inline;
		let inline_kind_changed = will_be_inline
			&& was_inline
			&& match &field.kind {
				AlterKind::Set(kind) => {
					existing_field.as_ref().is_some_and(|f| f.field_kind.as_ref() != Some(kind))
				}
				AlterKind::Drop => existing_field.as_ref().is_some_and(|f| f.field_kind.is_some()),
				AlterKind::None => false,
			};
		if (inline_toggled || inline_kind_changed)
			&& let Some(existing_table) = existing_table
		{
			let next_gen = existing_table.graph_inline_gen.wrapping_add(1);
			let generation = self.constant(Value::from(next_gen))?;
			self.set_field(table_def, "graph_inline_gen", generation)?;
		}
		self.replace_table((ns, db, tb), tb_key, table_def, "cache_fields_ts")?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// `ALTER FIELD` for a field or table that is evaluated, or whose checks were not made:
	/// resolved as the body resolves them, the field's path and then the table, once the session's
	/// namespace and database are known to exist, and altered by the host against the catalog the
	/// statement runs against.
	fn alter_field_resolved(
		&mut self,
		attrs: &AlterAttrs,
		field: &AlterField,
		template: &AlterTemplate,
	) -> Result<(), IrError> {
		self.guard()?;
		if field.migrate.is_some() && self.written_field(field.name, field.what)?.is_none() {
			self.refuse_late_bound_migration();
		}
		self.session_ids()?;
		let name = self.field_path(field.name)?;
		let table = self.name(field.what, NameClause::Table)?;
		let table = self.load(table.operand)?;
		self.store(StoreInstr::RealRunOnly);
		let altered = self.constant(template.value.clone())?;
		let altered = self.load(altered)?;
		// A changed type leaves the clauses the statement does not restate as they were.
		let kind_changed = !matches!(field.kind, AlterKind::None);
		let kept = crate::exec::migrate::KeptClauses {
			value: kind_changed && matches!(field.value, AlterKind::None),
			assert: kind_changed && matches!(field.assert, AlterKind::None),
		};
		self.store(StoreInstr::AlterFieldClauses {
			name,
			table,
			altered,
			fields: template.fields.iter().map(|field| field.as_str().to_owned()).collect(),
			kind_changed,
			if_exists: attrs.if_exists,
			migrate: field.migrate.clone(),
			kept,
		});
		self.answer_none()
	}

	/// Queues a verification pass of the field definition in `field`, stored with no migration
	/// over the one in `previous` on the table `table` names; an import, which restores
	/// definitions as they were, queues none.
	fn queue_verification(
		&mut self,
		(ns, db): (Reg, Reg),
		table: Operand,
		(field, previous): (Reg, Reg),
	) -> Result<(), IrError> {
		let import = self.label()?;
		self.store(StoreInstr::JumpIfImport {
			target: import,
		});
		let table = self.load(table)?;
		self.store(StoreInstr::QueueVerification {
			ns,
			db,
			table,
			field,
			previous,
		});
		self.bind(import)
	}

	/// Judges the field definition in `field`, about to replace the one in `previous`, against
	/// the rules of a table that enforces migrations (`crate::exec::migrate::enforces_migrations`),
	/// and the clause checks a changed type makes when `migrating`. The instruction reads the
	/// run-time import flag, so an import checks the same clauses and mints no step.
	fn check_field_change(
		&mut self,
		(ns, db): (Reg, Reg),
		table: Operand,
		(field, previous): (Reg, Reg),
		migrating: bool,
		kept: crate::exec::migrate::KeptClauses,
	) -> Result<(), IrError> {
		let table = self.load(table)?;
		self.store(StoreInstr::CheckFieldChange {
			ns,
			db,
			table,
			field,
			previous,
			migrating,
			kept,
		});
		Ok(())
	}

	/// The `MIGRATE EXISTING ROWS` clause `clause` of a field definition, over the stored
	/// definition in `field` that replaces the one in `previous` on the table `table` names:
	/// commits the migration, and answers `NONE` at once when the table has already run it under
	/// its name, so the statement does nothing. Every check that can refuse the statement
	/// precedes it.
	fn migrate(
		&mut self,
		(ns, db): (Reg, Reg),
		table: Operand,
		(field, previous): (Reg, Reg),
		clause: &MigrateClause,
	) -> Result<(), IrError> {
		let fallback = match &clause.fallback {
			None => None,
			Some(expr) => match crate::exec::migrate::constant_fallback(expr) {
				Some(value) => {
					let value = self.constant(value)?;
					Some(self.load(value)?)
				}
				None => {
					self.store(StoreInstr::RefuseField {
						refusal: FieldRefusal::Thrown(
							"A migration's FALLBACK must be a constant".to_owned(),
						),
					});
					None
				}
			},
		};
		let table = self.load(table)?;
		let ran = self.reg()?;
		self.store(StoreInstr::Migrate {
			ns,
			db,
			table,
			field,
			previous,
			fallback,
			migrate: Box::new(MigrateSpec {
				name: clause.name.clone(),
				using: clause.using.clone(),
			}),
			ran,
		});
		let fresh = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(ran),
			truthy: false,
			target: fresh,
		});
		self.answer_none()?;
		self.bind(fresh)
	}

	/// Refuses a migration whose field or table is evaluated: the checks a migration makes are
	/// made against the definition it names as written.
	fn refuse_late_bound_migration(&mut self) {
		self.store(StoreInstr::RefuseField {
			refusal: FieldRefusal::Thrown(
				"MIGRATE EXISTING ROWS needs the field and the table written as names".to_owned(),
			),
		});
	}
}
