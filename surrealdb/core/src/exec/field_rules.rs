//! The rules a field definition keeps whichever statement forms it.
//!
//! `DEFINE FIELD` and `ALTER FIELD` check the definition they are about to
//! write against these, and so does the VM's run-time check of the
//! definition `ALTER FIELD` merges from the stored field and its clauses, so
//! every route refuses the same definitions with the same errors. Each rule
//! reads only the definitions it is given: the caller reads the catalog.

use anyhow::{Result, bail, ensure};
use surrealdb_types::ToSql;

use crate::catalog::{self, Error as CatalogError};
use crate::exec::Error as ExecError;
use crate::expr::statements::define::kind_contains_object;
use crate::expr::{Expr, Idiom, Kind, KindLiteral, Literal, Part, RecordIdKeyLit};

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

/// How a field nested in another is reached from it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Step {
	/// `a.b`: a field of an object.
	Field,
	/// `a.*`, `a[*]`: every element of an object, array or set.
	Element,
	/// `a[0]`: one element of an array or set.
	Index,
}

impl Step {
	fn of(part: &Part) -> Option<Self> {
		match part {
			Part::Field(_) => Some(Self::Field),
			Part::All => Some(Self::Element),
			Part::Value(Expr::Literal(Literal::Integer(_))) => Some(Self::Index),
			_ => None,
		}
	}

	/// The rule a kind that cannot hold this step breaks.
	fn rule(self) -> &'static str {
		match self {
			Self::Field => "only an object holds a field",
			Self::Element => "only an object, array or set holds `*`",
			Self::Index => "only an array or set holds an index",
		}
	}

	/// Whether every value of `kind` but `NONE` can hold this step, or the step
	/// is `*` and `kind` implies a definition for its elements
	/// ([`implied_definitions`]): what a kind implies is held by it, whatever
	/// else the kind admits. `TYPE any` and a field with no `TYPE` hold
	/// nothing: their value need not be an object, an array or a set.
	fn held_by(self, kind: Option<&Kind>) -> bool {
		let holds = |kind: &Kind| match kind {
			Kind::Object | Kind::Literal(KindLiteral::Object(_)) => {
				matches!(self, Self::Field | Self::Element)
			}
			Kind::Array(..) | Kind::Set(..) | Kind::Literal(KindLiteral::Array(_)) => {
				matches!(self, Self::Element | Self::Index)
			}
			_ => false,
		};
		let held = match kind {
			Some(Kind::Either(kinds)) => {
				let mut held = kinds.iter().filter(|kind| !matches!(kind, Kind::None)).peekable();
				held.peek().is_some() && held.all(holds)
			}
			Some(kind) => holds(kind),
			None => false,
		};
		held || (self == Self::Element && kind.and_then(implied_element_kind).is_some())
	}
}

/// The kind `kind` implies for the elements of the arrays and sets it
/// admits, if it implies one: none for `any` elements.
fn implied_element_kind(kind: &Kind) -> Option<Kind> {
	kind.inner_kind().filter(|kind| !matches!(kind, Kind::Any))
}

/// The definitions `kind`, the kind of field `name`, implies for the elements
/// of the arrays and sets it admits, outermost first: `name.*` with the kind
/// of its elements, `name.*.*` with the kind of theirs, and so on until an
/// element kind is `any` or admits no array or set. A union implies the union
/// of what its members imply, so `array<float | array<bool>>` implies
/// `name.*` of `float | array<bool>` and `name.*.*` of `bool`.
pub(crate) fn implied_definitions(name: &Idiom, kind: Option<&Kind>) -> Vec<(Idiom, Kind)> {
	let mut out = Vec::new();
	let mut path = name.clone();
	let mut next = kind.and_then(implied_element_kind);
	while let Some(element) = next {
		next = implied_element_kind(&element);
		path.0.push(Part::All);
		out.push((path.clone(), element));
	}
	out
}

fn kind_text(def: &catalog::FieldDefinition) -> String {
	def.field_kind.as_ref().map_or_else(|| Kind::Any.to_sql(), ToSql::to_sql)
}

/// Refuse a definition that would leave a field nested in something that
/// cannot hold it.
///
/// A field nested in another — `a.b`, `a.*`, `a[0]` in `a` — is held by a
/// field defined in its own right, whose kind holds it: only an object holds a
/// field, only an object, array or set holds `*`, and only an array or set
/// holds an index, so a union holds the step only if each of its members but
/// `NONE` does. `TYPE any` and a field with no `TYPE` hold nothing. A kind
/// also holds the definitions it implies for its elements
/// ([`implied_definitions`]), which describe the elements of the members that
/// have them, and those are not judged against it.
///
/// `fields` are the table's definitions as the statement leaves them, `def`
/// among them, and `previous` the definition `def` replaces, if any. A new
/// field is judged against every field it is nested in, and a new or changed
/// kind against every field nested in it, so no order of statements leaves a
/// field in something that cannot hold it: defining the nested field first and
/// the field that holds it second is refused as surely as the reverse, and so
/// is changing the kind of a field that already holds others. A field nested
/// in `def` that declares a `TYPE` is also judged against what `def`'s kind
/// says of that place, as `DEFINE FIELD` judges a new nested field against
/// the fields holding it. A statement that changes neither leaves the field's
/// place as it found it, and is not judged.
pub(crate) fn nesting_restrictions(
	def: &catalog::FieldDefinition,
	previous: Option<&catalog::FieldDefinition>,
	fields: &[catalog::FieldDefinition],
) -> Result<()> {
	let parts = def.name.0.as_slice();
	if previous.is_none() {
		for (depth, part) in parts.iter().enumerate().skip(1) {
			let Some(step) = Step::of(part) else {
				bail!(ExecError::InvalidStatement(format!(
					"Cannot define field `{}`: {NAMED_ALONE}",
					def.name.to_sql()
				)));
			};
			let parent = &parts[..depth];
			let Some(holder) = fields.iter().find(|fd| fd.name.0.as_slice() == parent) else {
				bail!(ExecError::FieldParentUndefined {
					name: def.name.to_sql(),
					parent: Idiom(parent.to_vec()).to_sql(),
				});
			};
			ensure!(
				step.held_by(holder.field_kind.as_ref()),
				ExecError::FieldParentCannotHold {
					name: def.name.to_sql(),
					parent: holder.name.to_sql(),
					kind: kind_text(holder),
					rule: step.rule().to_owned(),
				}
			);
		}
	}
	nested_field_restrictions(def, previous, fields)
}

/// The rule a field named by more than field names, `*` and indexes breaks.
const NAMED_ALONE: &str = "a field is named by field names, `*` and indexes alone";

/// Refuse a kind for `def` that cannot hold the fields `fields` define in it:
/// the half of [`nesting_restrictions`] that judges what is nested in a
/// definition, and all that is judged of one a kind implies
/// ([`implied_definitions`]), which the kind implying it holds. `previous` is
/// the definition `def` replaces, if any; a kind it leaves as it was is not
/// judged.
pub(crate) fn nested_field_restrictions(
	def: &catalog::FieldDefinition,
	previous: Option<&catalog::FieldDefinition>,
	fields: &[catalog::FieldDefinition],
) -> Result<()> {
	if previous.is_some_and(|previous| previous.field_kind == def.field_kind) {
		return Ok(());
	}
	let parts = def.name.0.as_slice();
	let implied = implied_definitions(&def.name, def.field_kind.as_ref());
	for nested in fields.iter() {
		let Some(rest) = nested.name.0.strip_prefix(parts).filter(|rest| !rest.is_empty()) else {
			continue;
		};
		// A definition the kind implies is part of it.
		if implied.iter().any(|(name, _)| *name == nested.name) {
			continue;
		}
		let step = Step::of(&rest[0]);
		ensure!(
			step.is_some_and(|step| step.held_by(def.field_kind.as_ref())),
			ExecError::FieldKindCannotHoldChild {
				name: def.name.to_sql(),
				kind: kind_text(def),
				child: nested.name.to_sql(),
				rule: step.map_or(NAMED_ALONE, Step::rule).to_owned(),
			}
		);
		if let (Some(kind), Some(nested_kind)) = (&def.field_kind, &nested.field_kind) {
			// The kind an array or set implies for its elements can be a union
			// of one, which judges as the kind it holds.
			let nested_kind = Kind::either(nested_kind.clone().flatten());
			ensure!(
				kind.allows_nested_kind(rest, &nested_kind),
				ExecError::MismatchedFieldTypes {
					name: nested.name.to_sql(),
					kind: nested_kind.to_sql(),
					existing_name: def.name.to_sql(),
					existing_kind: kind.to_sql(),
				}
			);
		}
	}
	Ok(())
}

/// Refuse `definition` when defining it would leave a field nested in
/// something that cannot hold it, judged against the table's fields,
/// `fields`, as the statement leaves them ([`fields_once_defined`]): the
/// definition itself ([`nesting_restrictions`]), then each definition its kind
/// implies for the elements of an array or set, for what is nested in it
/// ([`nested_field_restrictions`]) — the kind that implies it holds it.
pub(crate) fn define_field_nesting_restrictions(
	fields: &[catalog::FieldDefinition],
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	let after = fields_once_defined(fields, definition);
	let implied = implied_definitions(&definition.name, definition.field_kind.as_ref());
	for (name, _) in implied.iter() {
		let Some(written) = after.iter().find(|fd| fd.name == *name) else {
			continue;
		};
		let previous = fields.iter().find(|fd| fd.name == *name);
		nested_field_restrictions(written, previous, &after)?;
	}
	let previous = fields.iter().find(|fd| fd.name == definition.name);
	nesting_restrictions(definition, previous, &after)
}

/// The table's field definitions, `fields`, as defining `definition` leaves
/// them: `definition` in place of the one it overwrites, the definitions its
/// kind implies for the elements of an array or set in place of theirs, and
/// none of those [`implied_definitions_dropped`] names, as
/// [`crate::ddl::define_field_statement_process_recursive_definitions`] writes
/// them.
pub(crate) fn fields_once_defined(
	fields: &[catalog::FieldDefinition],
	definition: &catalog::FieldDefinition,
) -> Vec<catalog::FieldDefinition> {
	let previous = fields.iter().find(|fd| fd.name == definition.name);
	let dropped = implied_definitions_dropped(fields, previous, definition);
	let mut out: Vec<catalog::FieldDefinition> = fields
		.iter()
		.filter(|fd| fd.name != definition.name && !dropped.contains(&fd.name))
		.cloned()
		.collect();
	for (name, kind) in implied_definitions(&definition.name, definition.field_kind.as_ref()) {
		let existing = out.iter().position(|fd| fd.name == name).map(|at| out.remove(at));
		out.push(implied_definition(definition, existing.as_ref(), name, kind));
	}
	out.push(definition.clone());
	out
}

/// The definition `definition` implies at `name` with `kind`
/// ([`implied_definitions`]), over `existing`, the one defined there if any,
/// whose other clauses it keeps: the kind is the implied one, and the elements
/// of a `FLEXIBLE` field are `FLEXIBLE`.
pub(crate) fn implied_definition(
	definition: &catalog::FieldDefinition,
	existing: Option<&catalog::FieldDefinition>,
	name: Idiom,
	kind: Kind,
) -> catalog::FieldDefinition {
	match existing {
		Some(existing) => catalog::FieldDefinition {
			field_kind: Some(kind),
			flexible: existing.flexible || definition.flexible,
			..existing.clone()
		},
		None => catalog::FieldDefinition {
			name,
			table: definition.table.clone(),
			field_kind: Some(kind),
			flexible: definition.flexible,
			..Default::default()
		},
	}
}

/// The definitions the kind of `previous`, the definition `definition`
/// replaces, implied for the elements of an array or set and `definition`'s
/// kind does not, deepest first, that `fields`, the table's definitions, hold
/// and no other field of theirs is defined in. These go with the kind that
/// implied them. One a field is defined in stays, and is judged against the
/// new kind as any field nested in it is.
pub(crate) fn implied_definitions_dropped(
	fields: &[catalog::FieldDefinition],
	previous: Option<&catalog::FieldDefinition>,
	definition: &catalog::FieldDefinition,
) -> Vec<Idiom> {
	let Some(previous) = previous else {
		return Vec::new();
	};
	let kept = implied_definitions(&definition.name, definition.field_kind.as_ref()).len();
	let mut dropped: Vec<Idiom> = Vec::new();
	let stale = implied_definitions(&previous.name, previous.field_kind.as_ref());
	for (name, _) in stale.into_iter().skip(kept).rev() {
		// Every shallower definition holds this one, so none of them is
		// dropped either once this one stays.
		let holds_another = fields.iter().any(|fd| {
			fd.name.len() > name.len() && fd.name.starts_with(&name) && !dropped.contains(&fd.name)
		});
		if holds_another {
			break;
		}
		if fields.iter().any(|fd| fd.name == name) {
			dropped.push(name);
		}
	}
	dropped
}
