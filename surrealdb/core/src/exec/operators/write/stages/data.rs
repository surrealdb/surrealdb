//! The statement's own clauses, applied to one record.
//!
//! Every stage whose input is something the *statement* wrote rather than
//! something the *table* declares: the data clause, and the `WHERE` gate.
//!
//! These are the stages that read
//! [`WriteCtx::clauses`](super::super::ctx::WriteCtx), and the only ones that
//! do.
//!
//! Record-id derivation is the one stage here that also reads the table's
//! schema. It settles what the statement claimed about the record's identity
//! against the `id` field's declared kind and `DEFAULT` in one place, because
//! the key it produces *is* the record's storage key: the field kernel that
//! applies every other field's declaration leaves `id` alone rather than
//! coercing a value that is already committed to.
//!
//! # Named record id components
//!
//! A table whose `id` names the elements of its array keys
//! ([`KeyStructure`]) takes its key from them. The key comes from the first
//! source that applies: an id the statement or the record named; every
//! component written as a field; the `id` field's `DEFAULT`. Without one of
//! those no key is drawn, because a keyed table has no random key to give. From
//! whichever source, every element is converted to the type of its component
//! and held to what the component can store ([`coerce_elements`]).
//!
//! A component is a field of the record that the key derives and the record
//! never stores. A write that names one is judged by [`settle_key_fields`]: the
//! value written must be the key's own element, and is then cut from the
//! record, so a field that restates the key is accepted and a field that
//! disagrees with it is refused.
//!
//! # The pre-mutation snapshot
//!
//! The `WHERE` condition and the data clause read one image of the record:
//! the field-level-`select`-reduced pre-mutation record, carrying the
//! `COMPUTED` fields the caller may see: a field whose `select` guard denies
//! the caller is absent from it and its body never ran. Sharing it is what makes a predicate and a
//! data-clause expression in the same statement agree about what a computed field holds,
//! and what keeps a caller's data clause from observing a field the caller may
//! not select. Neither reader ever observes the statement's own writes: every
//! expression of the data clause is evaluated against this image before any of
//! them is applied, which is what makes `SET a = a + 1, b = a + 1` assign `b`
//! from the value `a` held before the statement.

use std::collections::HashSet;

use surrealdb_types::ToSql;

use crate::catalog::{
	ComponentKind, DefineDefault, FieldDefinition, KeyComponent, KeyStructure, LATEST_EDGE_VARIANT,
	RecordType, TableDefinition, TableType,
};
use crate::doc::Error as DocError;
use crate::exec::Error as ExecError;
use crate::exec::operators::fetch::process_fetched_record_for;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::metadata::{edge_variant, is_edge, set_record_type};
use crate::exec::operators::write::row::{ComputedInput, WriteExtras, WriteRow};
use crate::exec::operators::write::stages::store::{endpoint_moved, endpoint_override};
use crate::exec::trampoline::{Nested, nest};
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{
	AssignOperator, ControlFlow, Error as ExprError, Idiom, Kind, KindLiteral, Operation, Part,
};
use crate::iam::Action;
use crate::val::{
	Array, CoerceError, Number, RecordId, RecordIdKey, TableName, Value, try_encode_component,
};

// ===========================================================================
// The stages
// ===========================================================================

/// The body of [`CheckDataFields`]: refuse a data clause, or a `RELATE`'s
/// value, whose `id`, `in` or `out` disagrees with the record the statement
/// named.
pub(crate) fn check_data_fields(row: &WriteRow) -> Result<(), ControlFlow> {
	// A MERGE, CONTENT or REPLACE value must be an object before its `id` is
	// read: a non-object has no id, so the id check below would refuse with an
	// internal error instead of the clause's own refusal.
	match row.input.as_ref() {
		Some(ComputedInput::Merge(value)) if !value.is_object() => {
			return Err(fail(ExprError::InvalidMerge {
				value: value.as_ref().clone(),
			}));
		}
		Some(ComputedInput::Content(value) | ComputedInput::Replace(value))
			if !value.is_object() =>
		{
			return Err(fail(ExprError::InvalidContent {
				value: value.as_ref().clone(),
			}));
		}
		_ => {}
	}
	if row.generate_in.is_some() {
		return Ok(());
	}
	let rid = record_id(row)?;
	if rid.key.is_range() {
		return Err(fail(ExprError::IdInvalid {
			value: rid.to_sql(),
		}));
	}
	let data = row.input.as_ref();
	if data.is_some_and(|data| matches!(data, ComputedInput::Patch(_))) {
		return Ok(());
	}
	match &row.extras {
		WriteExtras::Normal => {
			if let Some(data) = data {
				check_special_field(&picked(data, &ID), rid)?;
			}
		}
		WriteExtras::Relate(from, to, value) => {
			if let Some(data) = data {
				check_special_field(&picked(data, &ID), rid)?;
				check_special_field(&picked(data, &IN), from)?;
				check_special_field(&picked(data, &OUT), to)?;
			} else if let Some(value) = value {
				check_special_field(&value.pick(&ID), rid)?;
				check_special_field(&value.pick(&IN), from)?;
				check_special_field(&value.pick(&OUT), to)?;
			}
		}
		// An `INSERT`'s row value is the statement's subject rather than a
		// claim about one, and is merged by `process_merge_data` after the id
		// is settled.
		WriteExtras::Insert(_) => {}
	}
	Ok(())
}

/// The read of the stored edge a `RELATE` is about to update, as a caller
/// whose reads are permission-checked may read it: the record cut by its
/// fields' `select` permissions, or `NONE` where the table's `select`
/// permission denies it.
///
/// `None` where the caller's reads are not checked, or where the statement
/// relates no stored edge. [`stamp_record_fields`] judges the endpoints the
/// statement names against the image the read yields, so whether a
/// re-`RELATE` is refused depends only on what the caller may read. The read
/// owns what it reads, and deriving a computed field a field permission reads
/// may dereference a record, so it runs as a nested evaluation, as a fetch by
/// id does.
pub(crate) fn stored_edge_as_read(
	row: &WriteRow,
	ctx: &WriteCtx,
) -> Result<Option<Nested<Result<Value, ControlFlow>>>, ControlFlow> {
	if !matches!(row.extras, WriteExtras::Relate(..)) || row.initial.is_none() {
		return Ok(None);
	}
	let exec = ctx.exec();
	if !exec
		.should_check_perms(Action::View)
		.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?
	{
		return Ok(None);
	}
	let rid = record_id(row)?.clone();
	let mut image = row.initial.clone();
	let exec = exec.clone();
	Ok(Some(nest(async move {
		let needed = HashSet::from(["in".to_owned(), "out".to_owned()]);
		let shown = process_fetched_record_for(&exec, &rid, &mut image, Some(&needed)).await?;
		Ok(if shown {
			image
		} else {
			Value::None
		})
	})))
}

/// The body of [`DefaultRecordData`]: stamp `id`, and an edge's `in`, `out`
/// and record-type marker, onto the post-image.
///
/// `as_read` is the image [`stored_edge_as_read`] yields for the row: the
/// stored edge as a caller whose reads are checked may read it, or `None` for
/// any other caller.
pub(crate) fn stamp_record_fields(
	row: &mut WriteRow,
	as_read: Option<&Value>,
) -> Result<(), ControlFlow> {
	let rid = record_id(row)?.clone();
	row.current.def(rid);
	if let WriteExtras::Relate(from, to, _) = &row.extras {
		let from = from.clone();
		let to = to.clone();
		// Three cases fold into one condition: a brand-new edge is
		// stamped, an edge written against a stale layout generation is
		// advanced, and an edge already on the current generation is
		// left alone.
		if edge_variant(&row.metadata) != Some(LATEST_EDGE_VARIANT) {
			set_record_type(
				&mut row.metadata,
				RecordType::Edge {
					variant: LATEST_EDGE_VARIANT,
				},
			);
		}
		// A record this statement is creating has no endpoint to disagree
		// with; one it is updating must name the same pair. For a caller whose
		// reads are checked, an endpoint it may not read counts as differing
		// whatever the statement names, so the outcome - refused or written -
		// never depends on a hidden endpoint, and the refusal does not name
		// which one differs ([`endpoint_override`]).
		if row.initial.is_none() {
			row.current.put(&IN, Value::RecordId(from));
			row.current.put(&OUT, Value::RecordId(to));
		} else {
			let moved = |path: &[Part], expected: &RecordId| {
				endpoint_moved(&row.initial, path, expected)
					|| as_read.is_some_and(|image| endpoint_moved(image, path, expected))
			};
			let in_moved = moved(&IN, &from);
			let out_moved = moved(&OUT, &to);
			if let Some(error) = endpoint_override(as_read.is_some(), in_moved, out_moved) {
				return Err(fail(error));
			}
			row.current.put(&IN, Value::RecordId(from));
			row.current.put(&OUT, Value::RecordId(to));
		}
	}
	// An update of an edge by any other form restores the endpoints the
	// prior record held. The marker itself needs no re-stamping: it is
	// carried on the row from the before-image and the data clause
	// cannot address it.
	if is_edge(&row.initial_metadata) {
		row.current.put(&IN, row.initial.pick(&IN));
		row.current.put(&OUT, row.initial.pick(&OUT));
	}
	// `id`, `in` and `out` are field values like any other, so whatever
	// was derived from the record before this ran is stale.
	row.fields_computed = false;
	Ok(())
}

/// What settling a record's id still needs, once what the record, the
/// statement and its clause claim about it has been read.
pub(crate) enum IdSettling {
	/// The id is settled.
	Settled,
	/// Nothing claimed a key in `table`, whose `id` field declares a
	/// `DEFAULT`: its value, generated in `table`, is the id, which
	/// [`finish_record_id`] then settles.
	Default {
		table: TableName,
	},
}

/// The `id` field's declared kind.
fn id_kind(ctx: &WriteCtx) -> Option<&Kind> {
	ctx.id_field.and_then(|index| ctx.fields.get(index)).and_then(|fd| fd.field_kind.as_ref())
}

/// Settle the record's id as [`GenerateRecordId`] does, up to the `id`
/// field's `DEFAULT`: a lightweight edge's canonical id, the named record's
/// key held to the declared kind, a key the record or the data clause
/// supplied, or — where the field declares no `DEFAULT` — a synthesised key.
///
/// The `DEFAULT` is the caller's to evaluate, against [`id_default_doc`] in
/// the field's `DEFAULT` frame; the id it yields is settled by
/// [`finish_record_id`].
///
/// Where the `id` names the components of its keys, a record that named no id
/// takes its key from the components it was written with when every one of
/// them is present, from the `DEFAULT` when one is declared, and otherwise
/// fails naming the first component it lacks. Nothing is synthesised.
pub(crate) fn settle_record_id(
	row: &mut WriteRow,
	ctx: &WriteCtx,
) -> Result<IdSettling, ControlFlow> {
	if lightweight_edge_id(row, ctx)? {
		return Ok(IdSettling::Settled);
	}
	let id_field = ctx.id_field.and_then(|index| ctx.fields.get(index));
	let kind = id_field.and_then(|fd| fd.field_kind.as_ref());
	let Some(table) = row.generate_in.clone() else {
		// The statement named the record itself. Range targets never reach a
		// write operator: `WriteRow::for_target` refuses one.
		if kind.is_some()
			&& let Some(existing) = row.id.take()
		{
			let id = coerce_id_key(existing, kind, ctx.key.as_deref())?;
			row.id = Some(check_id_sql_type(id, ctx)?);
		}
		if let Some(id) = &row.id {
			check_id_subfields(id, &ctx.fields, &ctx.id_subfields)?;
		}
		return Ok(IdSettling::Settled);
	};
	// The record's own `id` answers first and the evaluated data clause
	// second, so a clause that named the record is honoured whether or not it
	// has been applied to the record yet. `DEFAULT` answers only where
	// neither did, and a key is synthesised only where none of the three
	// named one.
	let existing = row.current.pick(&ID);
	let supplied = if existing.is_some() {
		existing
	} else {
		row.input.as_ref().map(|data| picked(data, &ID)).unwrap_or(Value::None)
	};
	let has_default = id_field.is_some_and(|fd| matches!(fd.default, DefineDefault::Set(_)));
	let id = if supplied.is_some() {
		supplied.generate(table, false).map_err(ControlFlow::Err)?
	} else if let Some(key) = ctx.key.as_deref() {
		match components_written(key, components_doc(row)) {
			Ok(elements) => RecordId {
				table,
				key: RecordIdKey::Array(Array(elements)),
			},
			// A `DEFAULT` is evaluated only where the components alone do not
			// make the key, and may complete the ones it is given.
			Err(_) if has_default => {
				return Ok(IdSettling::Default {
					table,
				});
			}
			Err(absent) => return Err(fail(missing_component(key, &table, absent))),
		}
	} else if has_default {
		return Ok(IdSettling::Default {
			table,
		});
	} else {
		generate_typed_id(&table, kind)?
	};
	finish_record_id(row, ctx, id)?;
	Ok(IdSettling::Settled)
}

/// The record a `DEFAULT` on the `id` field reads: the record as it stands,
/// so a `DEFAULT` naming another field resolves against the fields the
/// statement supplied — except under `INSERT`, whose id has to be known before
/// the row's own value is merged, so the payload is read instead. Nothing is
/// bound as `$value`, `$before`, `$after` or `$input`.
pub(crate) fn id_default_doc(row: &WriteRow) -> &Value {
	match &row.extras {
		WriteExtras::Insert(value) => value.as_ref(),
		WriteExtras::Relate(_, _, _) | WriteExtras::Normal => &row.current,
	}
}

/// The record the components of a created record's key are read from: the
/// value an `INSERT` or `INSERT RELATION` supplies, which is merged onto the
/// record only once its id is settled, and the record as it stands otherwise.
fn components_doc(row: &WriteRow) -> &Value {
	match &row.extras {
		WriteExtras::Insert(value) | WriteExtras::Relate(_, _, Some(value)) => value.as_ref(),
		WriteExtras::Relate(_, _, None) | WriteExtras::Normal => &row.current,
	}
}

/// Settle `id`, derived for the record, as its id: refused as a range, and
/// its key held to the `id` field's declared kind.
pub(crate) fn finish_record_id(
	row: &mut WriteRow,
	ctx: &WriteCtx,
	id: RecordId,
) -> Result<(), ControlFlow> {
	if id.key.is_range() {
		return Err(fail(ExprError::IdInvalid {
			value: id.to_sql(),
		}));
	}
	let id = coerce_id_key(id, id_kind(ctx), ctx.key.as_deref())?;
	let id = check_id_sql_type(id, ctx)?;
	check_id_subfields(&id, &ctx.fields, &ctx.id_subfields)?;
	row.id = Some(id);
	Ok(())
}

/// Refuse the value an `id` `DEFAULT` of a table with named components
/// yielded, if it is `NONE`: such a table has no key to draw at random, so an
/// absent id is a missing component, the first one the record `row` was not
/// written with. Any other value is the `DEFAULT`'s id, which
/// [`finish_record_id`] holds to the components.
pub(crate) fn check_default_id(
	row: &WriteRow,
	ctx: &WriteCtx,
	table: &TableName,
	value: &Value,
) -> Result<(), ControlFlow> {
	if let Some(key) = ctx.key.as_deref()
		&& value.is_none()
	{
		// The `DEFAULT` runs only where a component is absent, so the
		// written components always name one; the first is the fallback.
		let absent = components_written(key, components_doc(row)).err().unwrap_or(0);
		return Err(fail(missing_component(key, table, absent)));
	}
	Ok(())
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// The pointer an operation reads and then carries somewhere other than
/// where it read it. Matched exhaustively, so an operation added without
/// being classified here does not default to carrying nothing out.
pub(crate) fn exposed_read_pointer(op: &Operation) -> Option<Vec<Part>> {
	let path = match op {
		// `copy` and `move` read at `from` and write the result at `path`.
		Operation::Copy {
			from,
			..
		}
		| Operation::Move {
			from,
			..
		} => from,
		// `test` reads at `path` and reports what it found in the
		// mismatch error.
		Operation::Test {
			path,
			..
		} => path,
		// The rest carry the value they write, or write back to the path
		// they read, so none of them carries a value out of the field it
		// came from.
		Operation::Add {
			..
		}
		| Operation::Remove {
			..
		}
		| Operation::Replace {
			..
		}
		| Operation::Change {
			..
		} => return None,
	};
	Some(Operation::path_to_parts(path))
}

/// What an evaluated data clause holds at `path`.
///
/// A compound assignment is deliberately invisible here: it needs the record's
/// existing value to resolve, so it makes no claim about the record's special
/// fields that could be checked before the record is built.
pub(crate) fn picked(data: &ComputedInput, path: &[Part]) -> Value {
	match data {
		ComputedInput::Patch(value)
		| ComputedInput::Merge(value)
		| ComputedInput::Replace(value)
		| ComputedInput::Content(value) => value.pick(path),
		ComputedInput::Unset(_) => Value::None,
		ComputedInput::Set(assignments) => assignments
			.iter()
			.find(|a| a.operator == AssignOperator::Assign && a.place.0.as_slice() == path)
			.map(|a| a.value.clone())
			.unwrap_or(Value::None),
	}
}

/// Judge one special field of a data clause against the record the statement
/// named.
///
/// Absence is allowed: it is what a statement that named the record itself
/// leaves behind. A bare key is accepted where it matches the expected key,
/// which is the shorthand `CREATE city CONTENT { id: 'london' }` uses.
fn check_special_field(found: &Value, expected: &RecordId) -> Result<(), ControlFlow> {
	if let Value::RecordId(value) = found {
		if value.key.is_range() {
			return Err(fail(ExprError::IdInvalid {
				value: value.to_sql(),
			}));
		}
		if value == expected {
			return Ok(());
		}
	}
	// A bare key is compared against the expected one, which is what admits a
	// record id named in full and one named by its key alone under the same
	// rule.
	if found.is_none() || expected.key == *found {
		return Ok(());
	}
	Err(fail(DocError::IdMismatch {
		value: found.to_sql(),
	}))
}

/// The record this row writes at. It is absent only before the id is derived,
/// so callers read it after the id is settled.
fn record_id(row: &WriteRow) -> Result<&RecordId, ControlFlow> {
	row.id
		.as_ref()
		.ok_or_else(|| ControlFlow::Err(anyhow::anyhow!("Expected a record id to be present")))
}

/// A lightweight relation's edge id, which is canonical rather than derived:
/// the pair `[in, out]` its endpoints name.
///
/// The id is the edge's entire identity and its entire storage — the record is
/// synthesized on read from the adjacency keys — so an arbitrary id would name
/// a different edge than the one related. A `RELATE` that supplied one is
/// refused unless it supplied exactly this pair.
///
/// Answers whether the id is settled here, which is the whole of the stage for
/// such an edge: nothing is derived, and no declared kind applies to a key the
/// endpoints fix.
fn lightweight_edge_id(row: &mut WriteRow, ctx: &WriteCtx) -> Result<bool, ControlFlow> {
	let WriteExtras::Relate(from, to, _) = &row.extras else {
		return Ok(false);
	};
	if !table_is_lightweight(&ctx.table) {
		return Ok(false);
	}
	let canonical =
		RecordIdKey::Array(Array(vec![Value::RecordId(from.clone()), Value::RecordId(to.clone())]));
	if let Some(existing) = row.id.as_ref()
		&& existing.key != canonical
	{
		return Err(fail(ExecError::Thrown(format!(
			"a LIGHTWEIGHT relation's edge id is canonical: expected {}:{}, found {}",
			ctx.table.name,
			canonical.to_sql(),
			existing.to_sql(),
		))));
	}
	row.id = Some(RecordId {
		table: ctx.table.name.clone(),
		key: canonical,
	});
	Ok(true)
}

/// Whether the table's edges are vertex-side adjacency keys alone, synthesized
/// on read from their canonical `[in, out]` ids.
fn table_is_lightweight(table: &TableDefinition) -> bool {
	matches!(&table.table_type, TableType::Relation(rel) if rel.lightweight)
}

/// Synthesise a key for a record nothing supplied one for, conforming to the
/// declared `id` kind.
///
/// A kind that admits more than one value and cannot be sampled — an `int`, an
/// `object`, a union — has no key to synthesise, and the statement is told so
/// rather than given a key the declaration forbids.
pub(crate) fn generate_typed_id(
	table: &TableName,
	kind: Option<&Kind>,
) -> Result<RecordId, ControlFlow> {
	let key = match kind {
		// No declared kind, or an unconstrained / string id: a random string.
		None | Some(Kind::Any | Kind::String) => RecordIdKey::rand(),
		// A uuid id: a fresh, time-ordered UUIDv7.
		Some(Kind::Uuid) => RecordIdKey::uuid(),
		// A singleton scalar literal has exactly one valid value, so the
		// declaration itself is the key.
		Some(Kind::Literal(KindLiteral::Integer(i))) => RecordIdKey::Number(*i),
		Some(Kind::Literal(KindLiteral::String(s))) => RecordIdKey::String(s.clone()),
		Some(other) => {
			return Err(fail(DocError::IdFieldGenerateUnsupported {
				table: table.to_string(),
				kind: other.to_sql(),
			}));
		}
	};
	Ok(RecordId {
		table: table.clone(),
		key,
	})
}

/// Hold a record id's key to the declared `id` kind.
///
/// Returns the id unchanged where no kind is declared, and where the kind
/// constrains the outer record rather than the key. Otherwise the key is
/// converted to a value, coerced, and converted back through the same path a
/// user-supplied id takes, so a synthesised key and a supplied one end up
/// under one rule.
///
/// An array key of a table with named components is held to each component in
/// turn instead ([`coerce_elements`]). Any other key of such a table is
/// coerced to the declared tuple, which refuses it. A key admitted by a
/// literal kind stays as written, so the id a statement names is the id it
/// writes.
fn coerce_id_key(
	id: RecordId,
	kind: Option<&Kind>,
	key: Option<&KeyStructure>,
) -> Result<RecordId, ControlFlow> {
	let Some(kind) = kind else {
		return Ok(id);
	};
	if kind.is_record() {
		return Ok(id);
	}
	let id = match (key, id.key) {
		(Some(key), RecordIdKey::Array(Array(elements))) => {
			return coerce_elements(id.table, elements, key);
		}
		(_, id_key) => RecordId {
			table: id.table,
			key: id_key,
		},
	};
	coerce_whole_key(id, kind)
}

/// Hold the key of `id` to the `TYPE` of every field defined inside the `id`
/// field (`DEFINE FIELD id.name`, `DEFINE FIELD id[0]`), each at the places of
/// the key its path names. `subfields` are the indexes in `fields` of those
/// definitions ([`WriteCtx::id_subfields`]).
///
/// The key is the record's storage key, and the field schema stage never
/// produces or coerces a place inside it, so this is where those definitions'
/// types apply. The key is checked and left as written, as a literal kind on
/// `id` itself leaves it: a part that only coerces to the definition's kind,
/// such as an `int` held to `float`, stays the value the key holds. A path that
/// names no place in the key, such as an index past the end of an array key,
/// checks nothing, as it does for an ordinary field; a field path the key does
/// not hold is a `NONE` there, which a kind without `NONE` refuses.
fn check_id_subfields(
	id: &RecordId,
	fields: &[FieldDefinition],
	subfields: &[usize],
) -> Result<(), ControlFlow> {
	if subfields.is_empty() {
		return Ok(());
	}
	let key = id.key.clone().into_value();
	for fd in subfields.iter().filter_map(|&index| fields.get(index)) {
		let Some(kind) = fd.field_kind.as_ref() else {
			continue;
		};
		for (_, part) in key.walk(&fd.name.0[1..]) {
			if let Err(error) = part.coerce_to_kind(kind) {
				return Err(fail(DocError::FieldCoerce {
					record: id.to_sql(),
					field_name: fd.name.to_sql(),
					error: Box::new(error),
				}));
			}
		}
	}
	Ok(())
}

/// Whether `kind` admits `key` through a literal kind: `kind` itself, or the
/// first arm of a union that accepts it, which is the arm coercion selects.
fn admitted_by_literal(kind: &Kind, key: &Value) -> bool {
	if let Kind::Literal(_) = kind {
		key.can_coerce_to_kind(kind)
	} else if let Kind::Either(arms) = kind {
		arms.iter()
			.find(|arm| key.can_coerce_to_kind(arm))
			.is_some_and(|arm| admitted_by_literal(arm, key))
	} else {
		false
	}
}

/// Coerce the whole key of `id` to the declared `kind`.
fn coerce_whole_key(id: RecordId, kind: &Kind) -> Result<RecordId, ControlFlow> {
	// The key is cloned so the original id stays whole for the error, which is
	// rendered only on failure.
	let key = id.key.clone().into_value();
	let literal_arm = admitted_by_literal(kind, &key);
	match key.coerce_to_kind(kind) {
		// A literal kind (a tuple, an object shape) admits a key as written: its
		// members are not converted, so the key stays the id the statement
		// named and a float or decimal element stays one.
		Ok(_) if literal_arm => Ok(id),
		Ok(coerced) => coerced.generate(id.table, false).map_err(ControlFlow::Err),
		Err(error) => Err(fail(DocError::FieldCoerce {
			record: id.to_sql(),
			field_name: "id".to_string(),
			error: Box::new(error),
		})),
	}
}

/// Hold the key of `id`, already coerced to the `id` field's kind, to the field's declared SQL
/// type, if it declares one: a key the type refuses fails the write, and a key the type
/// canonicalises (a rounded `numeric`, trimmed spaces) is the key the record is written under.
/// The field schema stage never visits `id`, so this is where its declared type applies.
fn check_id_sql_type(id: RecordId, ctx: &WriteCtx) -> Result<RecordId, ControlFlow> {
	let Some(index) = ctx.id_field else {
		return Ok(id);
	};
	let Some(declared) = ctx.declared_types().get(index).and_then(Option::as_ref) else {
		return Ok(id);
	};
	let declared = match declared {
		Ok(declared) => declared,
		Err(error) => {
			return Err(ControlFlow::Err(anyhow::anyhow!("invalid declared SQL type: {error}")));
		}
	};
	let key = id.key.clone().into_value();
	match crate::expr::statements::define::sql_types::validate(declared, key.clone(), "id") {
		Ok(stored) if stored == key => Ok(id),
		Ok(stored) => stored.generate(id.table, false).map_err(ControlFlow::Err),
		Err(violation) => Err(fail(DocError::FieldSqlType {
			record: id.to_sql(),
			field_name: "id".to_string(),
			sql_type: declared.canonical_text(),
			message: violation.message,
		})),
	}
}

// ===========================================================================
// Named record id components
// ===========================================================================

/// The name of the component at `position`, as the messages give it. A
/// structure built from an `id` field names every component.
fn component_name(key: &KeyStructure, position: u32) -> String {
	key.name_of(position).map_or_else(|| position.to_string(), str::to_owned)
}

/// The names of the components, as a list: `[city, time]`.
fn component_list(key: &KeyStructure) -> String {
	let names: Vec<String> =
		key.components.iter().map(|component| component_name(key, component.position)).collect();
	format!("[{}]", names.join(", "))
}

/// The failure of a record made without the component at `position`.
fn missing_component(key: &KeyStructure, table: &TableName, position: u32) -> DocError {
	DocError::IdComponentMissing {
		table: table.to_string(),
		component: component_name(key, position),
	}
}

/// The elements of a key, from the fields of `doc` that name its components: the
/// value of each in position order, or the position of the first one `doc`
/// lacks. A field holding `NONE` is no value.
fn components_written(key: &KeyStructure, doc: &Value) -> Result<Vec<Value>, u32> {
	let Value::Object(object) = doc else {
		return Err(0);
	};
	let mut elements = Vec::with_capacity(key.arity());
	for component in key.components.iter() {
		if let Some(value) = key
			.name_of(component.position)
			.and_then(|name| object.get(name))
			.filter(|value| !value.is_none())
		{
			elements.push(value.clone());
		} else {
			return Err(component.position);
		}
	}
	Ok(elements)
}

/// A key built from `elements`, held to the components of `key`: there is one
/// element for each, none is `NONE`, and each is converted to the type of its
/// component and is a value that component can store ([`component_value`]).
fn coerce_elements(
	table: TableName,
	elements: Vec<Value>,
	key: &KeyStructure,
) -> Result<RecordId, ControlFlow> {
	if elements.len() != key.arity() {
		return Err(fail(DocError::IdComponentArity {
			table: table.to_string(),
			expected: component_list(key),
			found: elements.len(),
		}));
	}
	let mut coerced = Vec::with_capacity(elements.len());
	for (component, element) in key.components.iter().zip(elements) {
		if element.is_none() {
			return Err(fail(missing_component(key, &table, component.position)));
		}
		match component_value(component, element) {
			Ok(element) => coerced.push(element),
			Err(error) => {
				return Err(fail(DocError::IdComponentCoerce {
					table: table.to_string(),
					component: component_name(key, component.position),
					error,
				}));
			}
		}
	}
	Ok(RecordId {
		table,
		key: RecordIdKey::Array(Array(coerced)),
	})
}

/// `value` as the element its component stores, or the reason it cannot be one.
///
/// An int component holds only a `Number::Int`, so a number that is a whole
/// value within the range of an int is converted to one and any other number
/// is refused: coercing a float to `int` saturates out of range, which would
/// store a key the caller never wrote. Every other component's value is coerced
/// to the component's declared type, and must then be one the component can
/// store (a datetime within the years 0000 to 9999, a record of an admitted
/// table).
fn component_value(component: &KeyComponent, value: Value) -> Result<Value, String> {
	let converted = match (&component.kind, value) {
		(ComponentKind::Int, Value::Number(number)) => match number.as_int_lossless() {
			Some(int) => Value::Number(Number::Int(int)),
			None => {
				return Err(CoerceError::InvalidKind {
					from: Value::Number(number),
					into: "int".to_owned(),
				}
				.to_string());
			}
		},
		(_, value) => match &component.declared {
			Some(kind) => value.coerce_to_kind(kind).map_err(|error| error.to_string())?,
			None => value,
		},
	};
	match component.kind.refusal(&converted) {
		Some(reason) => Err(format!("{} {reason}", converted.to_sql())),
		None => Ok(converted),
	}
}

/// Whether two elements are stored under the same bytes, which is what
/// addresses the record: numbers of different kinds that are equal by value are
/// not the same element.
fn same_element(a: &Value, b: &Value) -> bool {
	if let (Some(a), Some(b)) = (try_encode_component(a), try_encode_component(b)) {
		a == b
	} else {
		false
	}
}

/// Whether the value written for a component is the key's own element, after it
/// is converted as a key element would be. A record whose key has no such
/// element (it is not an array, or is too short) restates nothing.
fn restates(component: &KeyComponent, element: Option<&Value>, written: &Value) -> bool {
	let Some(element) = element else {
		return false;
	};
	match component.kind {
		// The conversion of these is the identity, so equality is the answer.
		ComponentKind::String
		| ComponentKind::Datetime
		| ComponentKind::Uuid
		| ComponentKind::Duration
		| ComponentKind::Bool => written == element,
		ComponentKind::Int | ComponentKind::Record(_) | ComponentKind::Unknown | _ => {
			match component_value(component, written.clone()) {
				Ok(converted) => same_element(&converted, element),
				Err(_) => false,
			}
		}
	}
}

/// How a data clause writes the field that names a component.
enum Addressed {
	/// It does not name the field.
	No,
	/// It writes the field itself, whose value the record then holds.
	Whole,
	/// It writes beneath the field (`city.x = 1`, `UNSET city[0]`), which is
	/// never the component's own value. Carries what it wrote there.
	Below(Value),
}

/// How the evaluated data clause `input` writes the field `name`.
///
/// A key field the record holds after the clause is judged by its value.
/// A field the clause removes, by `UNSET` or by a `NONE` assigned or merged,
/// has no value left to judge, so the clause itself decides it. A write
/// beneath a field is a mismatch whatever the field holds. A `CONTENT` or
/// `REPLACE` names the whole body, and a component it leaves out is derived
/// from the key.
fn addressed(input: Option<&ComputedInput>, name: &str) -> Addressed {
	/// The number of parts of `path` when it starts at the field `name`.
	fn depth(path: &Idiom, name: &str) -> Option<usize> {
		if let Some(Part::Field(field)) = path.0.first()
			&& field.as_str() == name
		{
			return Some(path.0.len());
		}
		None
	}
	let mut whole = false;
	match input {
		None | Some(ComputedInput::Replace(_) | ComputedInput::Content(_)) => {}
		// A merge writes the fields its object names, and one it gives `NONE`
		// is removed as an `UNSET` removes it.
		Some(ComputedInput::Merge(merged)) => {
			if let Value::Object(object) = merged.as_ref()
				&& object.get(name).is_some()
			{
				whole = true;
			}
		}
		Some(ComputedInput::Set(assignments)) => {
			for assignment in assignments {
				match depth(&assignment.place, name) {
					Some(1) => whole = true,
					Some(_) => return Addressed::Below(assignment.value.clone()),
					None => {}
				}
			}
		}
		Some(ComputedInput::Unset(paths)) => {
			for path in paths {
				match depth(path, name) {
					Some(1) => whole = true,
					Some(_) => return Addressed::Below(Value::None),
					None => {}
				}
			}
		}
		Some(ComputedInput::Patch(patch)) => {
			// This runs after the patch was applied, which fails a patch whose
			// operations do not parse; one that does not parse names nothing.
			let operations =
				Operation::value_to_operations(Value::clone(patch)).unwrap_or_default();
			let rooted = |path: &[surrealdb_strand::Strand]| {
				path.first().is_some_and(|root| root.as_str() == name).then_some(path.len())
			};
			for operation in &operations {
				let (written, moved, value) = match operation {
					Operation::Add {
						path,
						value,
					}
					| Operation::Replace {
						path,
						value,
					}
					| Operation::Change {
						path,
						value,
					} => (rooted(path), None, value.clone()),
					Operation::Remove {
						path,
					}
					| Operation::Copy {
						path,
						..
					} => (rooted(path), None, Value::None),
					Operation::Move {
						path,
						from,
					} => (rooted(path), rooted(from), Value::None),
					// A test only reads.
					Operation::Test {
						..
					} => (None, None, Value::None),
				};
				if let Some(1) = written {
					whole = true;
				} else if written.is_some() {
					return Addressed::Below(value);
				}
				if let Some(1) = moved {
					whole = true;
				} else if moved.is_some() {
					return Addressed::Below(Value::None);
				}
			}
		}
	}
	if whole {
		Addressed::Whole
	} else {
		Addressed::No
	}
}

/// The body of [`SettleKeyFields`]: judge the fields of the record that name
/// components of its record id, then cut them.
///
/// The components are derived from the id and are never stored. A field that
/// names one and is present after the data clause must be the key's own
/// element once it is converted as a key element would be, and is then cut
/// from the post-image, so that a statement that restates a record's key
/// (`UPDATE reading SET city = 'London'`, or `CONTENT` of a row read with `*`)
/// succeeds and one that disagrees fails with the value the caller wrote and
/// nothing of the record's own. A clause that removes a component, or writes
/// beneath one, is a disagreement as well. A copy a record's body holds that
/// equals the key is cut the same way.
///
/// What was derived onto the post-image is stale once a field is cut, so the
/// derivation and the reduced image are dropped.
pub(crate) fn settle_key_fields(row: &mut WriteRow, ctx: &WriteCtx) -> Result<(), ControlFlow> {
	let Some(key) = ctx.key.as_deref() else {
		return Ok(());
	};
	let cut =
		settle_components(key, record_id(row)?, &row.current, row.input.as_ref(), &row.initial)
			.map_err(fail)?;
	if cut.is_empty() {
		return Ok(());
	}
	if let Value::Object(object) = &mut row.current {
		for name in cut {
			object.remove(name);
		}
	}
	row.fields_computed = false;
	row.current_reduced = None;
	Ok(())
}

/// The names of the component fields `current` holds, all of which restate the
/// key `rid`; or the failure of the first one that does not, in position order.
///
/// `input` is the evaluated data clause and `initial` the record as it was read.
fn settle_components<'k>(
	key: &'k KeyStructure,
	rid: &RecordId,
	current: &Value,
	input: Option<&ComputedInput>,
	initial: &Value,
) -> Result<Vec<&'k str>, DocError> {
	let elements: &[Value] = if let RecordIdKey::Array(array) = &rid.key {
		&array.0
	} else {
		&[]
	};
	let held = |image: &'_ Value, name: &str| {
		if let Value::Object(object) = image {
			object.get(name).filter(|value| !value.is_none()).cloned()
		} else {
			None
		}
	};
	let mut cut = Vec::new();
	for component in key.components.iter() {
		let Some(name) = key.name_of(component.position) else {
			continue;
		};
		let element = elements.get(component.position as usize);
		let mismatch = |value: &Value| DocError::IdComponentMismatch {
			table: rid.table.to_string(),
			component: name.to_owned(),
			value: value.to_sql(),
		};
		let addressed = addressed(input, name);
		if let Addressed::Below(value) = &addressed {
			return Err(mismatch(value));
		}
		match held(current, name) {
			Some(written) => {
				if !restates(component, element, &written) {
					return Err(mismatch(&written));
				}
				cut.push(name);
			}
			// Removed by the clause. A record whose key has no such element, or
			// whose body held a copy that equals it, loses nothing by it.
			None if matches!(addressed, Addressed::Whole) => {
				let copy = held(initial, name);
				let harmless = element.is_none()
					|| copy.is_some_and(|copy| restates(component, element, &copy));
				if !harmless {
					return Err(mismatch(&Value::None));
				}
			}
			None => {}
		}
	}
	Ok(cut)
}

/// Abandon the statement with a named failure.
fn fail(error: impl std::error::Error + Send + Sync + 'static) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(error))
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use chrono::{TimeZone, Utc};
	use surrealdb_strand::Strand;

	use super::*;
	use crate::doc::alter::ComputedAssignment;
	use crate::expr::KeyNames;
	use crate::val::{Datetime, Object};

	/// The structure of `TYPE [<name>: <kind>, ...]` for the given components.
	fn key_of(components: &[(&str, Kind)]) -> KeyStructure {
		let names: Vec<Strand> = components.iter().map(|(name, _)| Strand::new(name)).collect();
		let kinds: Vec<Kind> = components.iter().map(|(_, kind)| kind.clone()).collect();
		let names = KeyNames::new(names, components.len()).expect("valid names");
		KeyStructure::from_id_field(&Kind::Literal(KindLiteral::Array(kinds)), &names)
			.expect("a valid structure")
	}

	/// `[city: string, time: datetime]`.
	fn reading() -> KeyStructure {
		key_of(&[("city", Kind::String), ("time", Kind::Datetime)])
	}

	/// `[site: string, n: int]`.
	fn counter() -> KeyStructure {
		key_of(&[("site", Kind::String), ("n", Kind::Int)])
	}

	fn at(year: i32) -> Value {
		Value::Datetime(Datetime(
			Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single().expect("a valid date"),
		))
	}

	fn text(value: &str) -> Value {
		Value::from(value)
	}

	fn int(value: i64) -> Value {
		Value::Number(Number::Int(value))
	}

	fn object(fields: Vec<(&str, Value)>) -> Value {
		Value::Object(Object::from_iter(fields))
	}

	fn record(key: Vec<Value>) -> RecordId {
		RecordId {
			table: TableName::from("reading"),
			key: RecordIdKey::Array(Array(key)),
		}
	}

	/// The document error a refusal carries.
	fn refusal<T: std::fmt::Debug>(result: Result<T, ControlFlow>) -> DocError {
		let Err(ControlFlow::Err(error)) = result else {
			panic!("expected a refusal, found {result:?}");
		};
		error.downcast::<DocError>().expect("a document error")
	}

	fn is_int(value: &Value, expected: i64) -> bool {
		matches!(value, Value::Number(Number::Int(found)) if *found == expected)
	}

	#[test]
	fn the_components_are_read_in_position_order_from_the_fields_written() {
		let doc = object(vec![("extra", int(1)), ("time", at(2024)), ("city", text("London"))]);
		let elements = components_written(&reading(), &doc).expect("every component is written");
		assert_eq!(elements, vec![text("London"), at(2024)]);
	}

	#[test]
	fn the_first_component_a_record_lacks_is_the_one_reported() {
		let key = reading();
		assert_eq!(components_written(&key, &object(vec![("city", text("London"))])), Err(1));
		assert_eq!(components_written(&key, &object(vec![("time", at(2024))])), Err(0));
		assert_eq!(components_written(&key, &object(vec![])), Err(0));
		// A field holding `NONE` is no value.
		let none = object(vec![("city", Value::None), ("time", at(2024))]);
		assert_eq!(components_written(&key, &none), Err(0));
		assert_eq!(components_written(&key, &Value::None), Err(0));
	}

	#[test]
	fn an_int_component_holds_the_whole_number_however_it_was_written() {
		let key = counter();
		let component = &key.components[1];
		for written in
			[int(5), Value::Number(Number::Float(5.0)), Value::Number(Number::Decimal(5.into()))]
		{
			let element = component_value(component, written.clone())
				.unwrap_or_else(|e| panic!("{} was refused: {e}", written.to_sql()));
			assert!(is_int(&element, 5), "{} became {}", written.to_sql(), element.to_sql());
		}
		for written in [
			Value::Number(Number::Float(5.5)),
			Value::Number(Number::Float(1e30)),
			Value::Number(Number::Float(-1e30)),
			Value::Number(Number::Float(f64::NAN)),
			Value::Number(Number::Float(f64::INFINITY)),
			// 2^63 is one past the largest int, and is what `i64::MAX as f64` rounds to.
			Value::Number(Number::Float(2f64.powi(63))),
		] {
			assert!(
				component_value(component, written.clone()).is_err(),
				"{} was accepted for an int component",
				written.to_sql()
			);
		}
	}

	#[test]
	fn an_element_is_refused_when_it_is_not_the_declared_type() {
		let key = reading();
		// Neither a string nor a number is a datetime: nothing is parsed.
		for written in [text("2024-01-01T00:00:00Z"), int(5), Value::None] {
			assert!(
				component_value(&key.components[1], written.clone()).is_err(),
				"{} was accepted for a datetime component",
				written.to_sql()
			);
		}
		assert!(component_value(&key.components[0], int(5)).is_err());
		let error = component_value(&key.components[1], at(10_000)).expect_err("out of range");
		assert!(error.contains("years 0000 to 9999"), "{error}");
		assert!(component_value(&key.components[1], at(2024)).is_ok());
		assert!(component_value(&key.components[0], text("London")).is_ok());
	}

	#[test]
	fn a_key_has_one_element_for_each_component() {
		let key = reading();
		for elements in [vec![text("London")], vec![text("London"), at(2024), int(1)], vec![]] {
			let found = elements.len();
			let refused = refusal(coerce_elements(TableName::from("reading"), elements, &key));
			let DocError::IdComponentArity {
				table,
				expected,
				found: given,
			} = refused
			else {
				panic!("expected an arity refusal, found {refused:?}");
			};
			assert_eq!(table, "reading");
			assert_eq!(expected, "[city, time]");
			assert_eq!(given, found);
		}
	}

	#[test]
	fn a_none_element_is_a_missing_component() {
		let key = reading();
		let table = TableName::from("reading");
		let first = refusal(coerce_elements(table.clone(), vec![Value::None, at(2024)], &key));
		assert!(
			matches!(&first, DocError::IdComponentMissing { component, .. } if component == "city"),
			"{first:?}"
		);
		let second = refusal(coerce_elements(table, vec![text("London"), Value::None], &key));
		assert!(
			matches!(&second, DocError::IdComponentMissing { component, .. } if component == "time"),
			"{second:?}"
		);
	}

	#[test]
	fn an_element_that_is_not_its_component_names_the_component() {
		let key = reading();
		let refused = refusal(coerce_elements(
			TableName::from("reading"),
			vec![text("London"), int(5)],
			&key,
		));
		let DocError::IdComponentCoerce {
			table,
			component,
			..
		} = refused
		else {
			panic!("expected a coercion refusal, found {refused:?}");
		};
		assert_eq!(table, "reading");
		assert_eq!(component, "time");
	}

	#[test]
	fn a_conforming_key_is_an_array_of_elements_with_ints_normalised() {
		let key = counter();
		let rid = coerce_elements(
			TableName::from("counter"),
			vec![text("a"), Value::Number(Number::Float(5.0))],
			&key,
		)
		.expect("a conforming key");
		let RecordIdKey::Array(Array(elements)) = rid.key else {
			panic!("the key is not an array");
		};
		assert_eq!(elements.len(), 2);
		assert_eq!(elements[0], text("a"));
		assert!(is_int(&elements[1], 5), "the int element is {}", elements[1].to_sql());
	}

	#[test]
	fn a_field_restating_the_key_is_accepted_and_cut() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		let current = object(vec![("city", text("London")), ("time", at(2024)), ("v", int(1))]);
		let cut = settle_components(&key, &rid, &current, None, &Value::None).expect("restated");
		assert_eq!(cut, vec!["city", "time"]);
	}

	#[test]
	fn a_record_that_names_no_component_is_untouched() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		let current = object(vec![("v", int(1))]);
		let cut = settle_components(&key, &rid, &current, None, &Value::None).expect("untouched");
		assert!(cut.is_empty());
	}

	#[test]
	fn a_field_that_disagrees_is_refused_with_the_value_written_and_nothing_else() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		let current = object(vec![("city", text("Paris")), ("time", at(2024))]);
		let refused = settle_components(&key, &rid, &current, None, &Value::None)
			.expect_err("a different city");
		let message = refused.to_string();
		let DocError::IdComponentMismatch {
			table,
			component,
			value,
		} = &refused
		else {
			panic!("expected a mismatch, found {refused:?}");
		};
		assert_eq!(table, "reading");
		assert_eq!(component, "city");
		assert_eq!(value, "'Paris'");
		assert!(!message.contains("London"), "the key's own value is shown: {message}");
	}

	#[test]
	fn components_are_judged_in_position_order() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		let current = object(vec![("time", at(2025)), ("city", text("Paris"))]);
		let refused =
			settle_components(&key, &rid, &current, None, &Value::None).expect_err("both differ");
		assert!(
			matches!(&refused, DocError::IdComponentMismatch { component, .. } if component == "city"),
			"{refused:?}"
		);
	}

	#[test]
	fn an_int_field_restates_its_component_in_any_number_kind() {
		let key = counter();
		let rid = RecordId {
			table: TableName::from("counter"),
			key: RecordIdKey::Array(Array(vec![text("a"), int(5)])),
		};
		for restated in
			[int(5), Value::Number(Number::Float(5.0)), Value::Number(Number::Decimal(5.into()))]
		{
			let current = object(vec![("n", restated.clone())]);
			let cut = settle_components(&key, &rid, &current, None, &Value::None)
				.unwrap_or_else(|e| panic!("{} was refused: {e}", restated.to_sql()));
			assert_eq!(cut, vec!["n"]);
		}
		for different in [int(6), Value::Number(Number::Float(5.5)), text("5"), Value::Null] {
			let current = object(vec![("n", different.clone())]);
			let refused = settle_components(&key, &rid, &current, None, &Value::None)
				.expect_err("not the component");
			assert!(
				matches!(&refused, DocError::IdComponentMismatch { value, .. } if *value == different.to_sql()),
				"{refused:?}"
			);
		}
	}

	#[test]
	fn a_key_that_is_no_array_restates_nothing() {
		let key = reading();
		let rid = RecordId {
			table: TableName::from("reading"),
			key: RecordIdKey::Number(1),
		};
		let current = object(vec![("city", text("London"))]);
		let refused = settle_components(&key, &rid, &current, None, &Value::None)
			.expect_err("there is no element to restate");
		assert!(matches!(refused, DocError::IdComponentMismatch { .. }), "{refused:?}");
	}

	fn assignment(path: &[&str], value: Value) -> ComputedAssignment {
		ComputedAssignment {
			place: Idiom(path.iter().map(|name| Part::Field(Strand::new(name))).collect()),
			operator: AssignOperator::Assign,
			value,
		}
	}

	#[test]
	fn a_write_beneath_a_component_is_a_mismatch_whatever_the_field_holds() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		// The field still holds the key's element, but the clause wrote beneath
		// it, which is a mismatch.
		let current = object(vec![("city", text("London"))]);
		let input = ComputedInput::Set(vec![assignment(&["city", "x"], int(1))]);
		let refused = settle_components(&key, &rid, &current, Some(&input), &Value::None)
			.expect_err("a nested write");
		assert!(
			matches!(&refused, DocError::IdComponentMismatch { component, value, .. }
				if component == "city" && value == "1"),
			"{refused:?}"
		);
	}

	#[test]
	fn a_set_of_the_whole_component_is_judged_by_what_the_record_holds() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		let input = ComputedInput::Set(vec![assignment(&["city"], text("London"))]);
		let current = object(vec![("city", text("London"))]);
		let cut = settle_components(&key, &rid, &current, Some(&input), &Value::None)
			.expect("a restatement");
		assert_eq!(cut, vec!["city"]);
	}

	#[test]
	fn unsetting_a_component_is_a_mismatch_unless_the_body_held_an_equal_copy() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		let input = ComputedInput::Unset(vec![Idiom::field(Strand::new("city"))]);
		let removed = object(vec![("v", int(1))]);

		// A derived component has no stored copy to remove.
		let refused = settle_components(&key, &rid, &removed, Some(&input), &removed)
			.expect_err("nothing to unset");
		assert!(
			matches!(&refused, DocError::IdComponentMismatch { component, value, .. }
				if component == "city" && value == "NONE"),
			"{refused:?}"
		);

		// A stored copy that equals the key is the record's own to remove.
		let copy = object(vec![("city", text("London")), ("v", int(1))]);
		let cut = settle_components(&key, &rid, &removed, Some(&input), &copy)
			.expect("an equal copy is removed");
		assert!(cut.is_empty());

		// A stored copy that differs is not.
		let stale = object(vec![("city", text("Paris")), ("v", int(1))]);
		let refused = settle_components(&key, &rid, &removed, Some(&input), &stale)
			.expect_err("a differing copy");
		assert!(matches!(refused, DocError::IdComponentMismatch { .. }), "{refused:?}");
	}

	#[test]
	fn a_merge_that_removes_a_component_is_a_mismatch() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		let removed = object(vec![("v", int(1))]);
		let merge = |fields: Vec<(&str, Value)>| ComputedInput::Merge(Arc::new(object(fields)));

		// `NONE` merged onto a component removes it, as `UNSET` does.
		let refused = settle_components(
			&key,
			&rid,
			&removed,
			Some(&merge(vec![("city", Value::None)])),
			&removed,
		)
		.expect_err("a removal");
		assert!(
			matches!(&refused, DocError::IdComponentMismatch { component, value, .. }
				if component == "city" && value == "NONE"),
			"{refused:?}"
		);

		// A merge that restates a component is judged by what the record holds.
		let current = object(vec![("city", text("London")), ("v", int(1))]);
		let cut = settle_components(
			&key,
			&rid,
			&current,
			Some(&merge(vec![("city", text("London"))])),
			&removed,
		)
		.expect("a restatement");
		assert_eq!(cut, vec!["city"]);

		// A merge of other fields says nothing about the components.
		let cut =
			settle_components(&key, &rid, &removed, Some(&merge(vec![("v", int(2))])), &removed)
				.expect("another field");
		assert!(cut.is_empty());
	}

	fn patch(operations: Vec<Vec<(&str, Value)>>) -> ComputedInput {
		let operations = operations.into_iter().map(object).collect();
		ComputedInput::Patch(Arc::new(Value::Array(Array(operations))))
	}

	#[test]
	fn a_patch_is_judged_by_the_paths_it_names() {
		let key = reading();
		let rid = record(vec![text("London"), at(2024)]);
		let removed = object(vec![("v", int(1))]);

		let remove = patch(vec![vec![("op", text("remove")), ("path", text("/city"))]]);
		let refused = settle_components(&key, &rid, &removed, Some(&remove), &removed)
			.expect_err("a removal");
		assert!(
			matches!(&refused, DocError::IdComponentMismatch { component, value, .. }
				if component == "city" && value == "NONE"),
			"{refused:?}"
		);

		let beneath =
			patch(vec![vec![("op", text("add")), ("path", text("/city/x")), ("value", int(1))]]);
		let current = object(vec![("city", text("London"))]);
		let refused = settle_components(&key, &rid, &current, Some(&beneath), &current)
			.expect_err("a write beneath");
		assert!(
			matches!(&refused, DocError::IdComponentMismatch { value, .. } if value == "1"),
			"{refused:?}"
		);

		let replace = patch(vec![vec![
			("op", text("replace")),
			("path", text("/city")),
			("value", text("London")),
		]]);
		let cut = settle_components(&key, &rid, &current, Some(&replace), &current)
			.expect("a restatement");
		assert_eq!(cut, vec!["city"]);

		// A patch that names another field says nothing about the components.
		let other =
			patch(vec![vec![("op", text("replace")), ("path", text("/v")), ("value", int(2))]]);
		let cut =
			settle_components(&key, &rid, &removed, Some(&other), &removed).expect("another field");
		assert!(cut.is_empty());
	}

	/// A definition of `name` on `reading` with `kind`.
	fn defined(name: &str, kind: Kind) -> FieldDefinition {
		FieldDefinition {
			name: name.parse().expect("a valid path"),
			table: TableName::from("reading"),
			field_kind: Some(kind),
			..Default::default()
		}
	}

	#[test]
	fn a_key_is_held_to_the_types_defined_inside_id() {
		let fields = vec![
			defined("id", Kind::Object),
			defined("id.city", Kind::String),
			defined("id.n", Kind::Int),
		];
		let subfields = [1, 2];
		let rid = |key: Value| RecordId {
			table: TableName::from("reading"),
			key: RecordIdKey::Object(match key {
				Value::Object(object) => object,
				other @ Value::None
				| other @ Value::Null
				| other @ Value::Bool(_)
				| other @ Value::Number(_)
				| other @ Value::String(_)
				| other @ Value::Duration(_)
				| other @ Value::Datetime(_)
				| other @ Value::Date(_)
				| other @ Value::Time(_)
				| other @ Value::Timestamp(_)
				| other @ Value::Interval(_)
				| other @ Value::Uuid(_)
				| other @ Value::Array(_)
				| other @ Value::Set(_)
				| other @ Value::Geometry(_)
				| other @ Value::Bytes(_)
				| other @ Value::Table(_)
				| other @ Value::RecordId(_)
				| other @ Value::File(_)
				| other @ Value::Regex(_)
				| other @ Value::Range(_)
				| other @ Value::Closure(_) => panic!("not an object: {other:?}"),
			}),
		};
		let fits = rid(object(vec![("city", text("London")), ("n", int(1))]));
		check_id_subfields(&fits, &fields, &subfields).expect("a key that fits");

		let wrong = rid(object(vec![("city", int(1)), ("n", int(1))]));
		let refused = refusal(check_id_subfields(&wrong, &fields, &subfields));
		assert!(
			matches!(&refused, DocError::FieldCoerce { field_name, .. } if field_name == "id.city"),
			"{refused:?}"
		);
		// A path the key does not hold is `NONE`, which `int` refuses.
		let short = rid(object(vec![("city", text("London"))]));
		let refused = refusal(check_id_subfields(&short, &fields, &subfields));
		assert!(
			matches!(&refused, DocError::FieldCoerce { field_name, .. } if field_name == "id.n"),
			"{refused:?}"
		);
	}

	#[test]
	fn an_index_past_the_end_of_a_key_checks_nothing() {
		let fields = vec![
			defined("id", Kind::Array(Box::new(Kind::Any), None)),
			defined("id[0]", Kind::Int),
			defined("id[1000000000]", Kind::String),
		];
		let fits = record(vec![int(1), text("a")]);
		check_id_subfields(&fits, &fields, &[1, 2]).expect("no place past the end");
		let wrong = record(vec![text("b"), text("a")]);
		let refused = refusal(check_id_subfields(&wrong, &fields, &[1, 2]));
		assert!(
			matches!(&refused, DocError::FieldCoerce { field_name, .. } if field_name == "id[0]"),
			"{refused:?}"
		);
	}
}
