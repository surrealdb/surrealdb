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
//! # The pre-mutation snapshot
//!
//! The `WHERE` condition and the data clause read one image of the record:
//! the field-level-`select`-reduced pre-mutation record with its `COMPUTED`
//! fields derived. Sharing it is what makes a predicate and a data-clause
//! expression in the same statement agree about what a computed field holds,
//! and what keeps a caller's data clause from observing a field the caller may
//! not select. Neither reader ever observes the statement's own writes: every
//! expression of the data clause is evaluated against this image before any of
//! them is applied, which is what makes `SET a = a + 1, b = a + 1` assign `b`
//! from the old `a`.

use surrealdb_types::ToSql;

use crate::catalog::{DefineDefault, LATEST_EDGE_VARIANT, RecordType, TableDefinition, TableType};
use crate::doc::Error as DocError;
use crate::exec::Error as ExecError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::metadata::{edge_variant, is_edge, set_record_type};
use crate::exec::operators::write::row::{ComputedInput, WriteExtras, WriteRow};
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{
	AssignOperator, ControlFlow, Error as ExprError, Kind, KindLiteral, Operation, Part,
};
use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

// ===========================================================================
// The stages
// ===========================================================================

/// The body of [`CheckDataFields`]: refuse a data clause, or a `RELATE`'s
/// value, whose `id`, `in` or `out` disagrees with the record the statement
/// named.
pub(crate) fn check_data_fields(row: &WriteRow) -> Result<(), ControlFlow> {
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

/// The body of [`DefaultRecordData`]: stamp `id`, and an edge's `in`, `out`
/// and record-type marker, onto the post-image.
pub(crate) fn stamp_record_fields(row: &mut WriteRow) -> Result<(), ControlFlow> {
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
		// A record this statement is creating has no endpoint to
		// disagree with; one it is updating must name the same pair.
		let new = row.initial.is_none();
		match (row.initial.pick(&IN), new) {
			(Value::RecordId(id), false) if id == from => {
				row.current.put(&IN, Value::RecordId(from))
			}
			(_, true) => row.current.put(&IN, Value::RecordId(from)),
			(found, _) => {
				return Err(fail(DocError::InOverride {
					value: found.to_sql(),
				}));
			}
		}
		match (row.initial.pick(&OUT), new) {
			(Value::RecordId(id), false) if id == to => row.current.put(&OUT, Value::RecordId(to)),
			(_, true) => row.current.put(&OUT, Value::RecordId(to)),
			(found, _) => {
				return Err(fail(DocError::OutOverride {
					value: found.to_sql(),
				}));
			}
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
			row.id = Some(coerce_id_key(existing, kind)?);
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
	let id = if supplied.is_some() {
		supplied.generate(table, false).map_err(ControlFlow::Err)?
	} else if id_field.is_some_and(|fd| matches!(fd.default, DefineDefault::Set(_))) {
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
	row.id = Some(coerce_id_key(id, id_kind(ctx))?);
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

/// The record this row writes at. Absent only for a creating order whose id
/// has still to be derived, which every caller here runs after.
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
fn coerce_id_key(id: RecordId, kind: Option<&Kind>) -> Result<RecordId, ControlFlow> {
	let Some(kind) = kind else {
		return Ok(id);
	};
	if kind.is_record() {
		return Ok(id);
	}
	// The key is cloned so the original id stays whole for the failure, which
	// is rendered only when there is one rather than on every typed-id write.
	match id.key.clone().into_value().coerce_to_kind(kind) {
		Ok(coerced) => coerced.generate(id.table, false).map_err(ControlFlow::Err),
		Err(error) => Err(fail(DocError::FieldCoerce {
			record: id.to_sql(),
			field_name: "id".to_string(),
			error: Box::new(error),
		})),
	}
}

/// Abandon the statement with a named failure.
fn fail(error: impl std::error::Error + Send + Sync + 'static) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(error))
}
