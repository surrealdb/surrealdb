//! The host's side of the data stages run as instructions: the `WHERE`
//! condition, the data clause's evaluation and application, the merge of an
//! `INSERT` row's own value, and the settling of the record's id and special
//! fields.
//!
//! The instructions act on the record in flight through the write state; an
//! expression they evaluate is a host pipeline over one of the record's
//! images (see `doc/executor/VM_WRITE_STAGES.md`).
//!
//! # The pre-mutation snapshot
//!
//! The condition and the data clause read one image of the record, which
//! [`DataInstr::Snapshot`] derives. For a caller whose reads are checked it is
//! the post-image reduced to what the caller may select, with its `COMPUTED`
//! fields derived when there is a pre-image to derive them from, held as the
//! row's reduced post-image. For any other caller it is the post-image itself,
//! with its `COMPUTED` fields derived onto it. Both are memoised on the row —
//! the reduction on `current_reduced`, the derivation on `fields_computed` —
//! and every stage that changes the record's field values clears them.
//!
//! An unchecked caller's snapshot of a table whose only `COMPUTED` fields are
//! the named components of its record id is derived by
//! [`DataInstr::SnapshotKeyParts`]: each component is the element of the
//! image's `id` at its position, written onto the image in place, which is
//! what the field view's host pipeline derives, without running it.

use std::sync::Arc;

use surrealdb_types::ToSql;

use super::{State, WriteState, internal, run_image, run_place, spec_of};
use crate::doc::Error as DocError;
use crate::doc::alter::ComputedAssignment;
use crate::exe::FlowResultExt as _;
use crate::exec::operators::write::apply;
use crate::exec::operators::write::place::PlaceEval;
use crate::exec::operators::write::row::{ComputedInput, WriteExtras, WriteRow};
use crate::exec::operators::write::stages::data;
use crate::exec::permission::field_default_frame;
use crate::exec::vm::program::{DataShape, Label, Program};
use crate::exec::{BoxFut, ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, Error as ExprError, Idiom, Operation};
use crate::val::{Object, Value};

/// An instruction of the data stages.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum DataInstr {
	/// The statement has no condition: the record is matched.
	Matched,
	/// Derive the pre-mutation snapshot onto the record, unless it is derived
	/// already. A checked caller's is the post-image run through host pipeline
	/// `view` — the table's field view — when there is a pre-image, and
	/// through `bare` — its guards on stored fields alone — when there is
	/// none. An unchecked caller's is the post-image run through `view`, the
	/// table's `COMPUTED` fields, when there is a pre-image. An absent
	/// pipeline changes nothing, and neither does any pipeline over a record
	/// with no id or a post-image that is no object.
	Snapshot {
		view: Option<u16>,
		bare: Option<u16>,
		checked: bool,
	},
	/// [`DataInstr::Snapshot`] for an unchecked caller of a table whose field
	/// view, host pipeline `view`, is one layer deriving `parts` and nothing
	/// else. Where the post-image is an object whose `id` is a record id, each
	/// part is set on it, in place, to `id[position]` held to the part's kind;
	/// any other post-image is run through `view`. Either is taken only when
	/// there is a pre-image, and a record already derived, or with no id, is
	/// left as it is.
	SnapshotKeyParts {
		parts: Box<[KeyPart]>,
		view: Option<u16>,
	},
	/// Judge the statement's condition, host pipeline `pipeline` over the
	/// snapshot: a record it accepts is matched, and one it rejects jumps to
	/// `skip`.
	Where {
		pipeline: u16,
		skip: Label,
	},
	/// Jump to `target` when the record already holds an evaluated data clause.
	JumpIfInput {
		target: Label,
	},
	/// Evaluate the statement's data clause into the record's input, from
	/// host pipeline `pipeline` over the snapshot: the clause's value, or the
	/// values of a `SET`'s assignments in written order. `UNSET` evaluates
	/// nothing and has none. With `binds_input` — `ON DUPLICATE KEY UPDATE` —
	/// `$input` is the value the statement supplied for the record while the
	/// clause is evaluated.
	ComputeInput {
		pipeline: Option<u16>,
		binds_input: bool,
	},
	/// Apply the record's evaluated data clause, which is no `PATCH`, to its
	/// post-image, and drop what was derived from the post-image before.
	ApplyInput,
	/// Apply the record's evaluated `PATCH` to its post-image, and drop what
	/// was derived from the post-image before. First a `copy`, `move` or
	/// `test` pointer is refused where it reads a field the caller may not
	/// select: each is judged against the post-image with the operations
	/// before it applied, and against that image reduced as
	/// [`DataInstr::Snapshot`] reduces one, through `view` or `bare`.
	ApplyPatch {
		view: Option<u16>,
		bare: Option<u16>,
		checked: bool,
	},
	/// Refuse a data clause, or a `RELATE`'s value, whose `id`, `in` or `out`
	/// disagrees with the record the statement named.
	CheckDataFields,
	/// Stamp `id`, and an edge's `in`, `out` and record-type marker, onto the
	/// post-image.
	StampRecordFields,
	/// Judge the fields of the record that name components of its record id, and
	/// cut them from the post-image: each must be the key's own element, which
	/// is held by the key and is never stored. Emitted only for a table whose
	/// `id` field names its components, and a no-op when the table resolved none.
	SettleKeyFields,
	/// Settle the record's id: a key the record or its data clause supplied,
	/// the `id` field's `DEFAULT` — host pipeline `default`, run over the
	/// record the `DEFAULT` reads in the field's `DEFAULT` frame — or a
	/// synthesised key, held to the field's declared kind.
	GenerateId {
		default: Option<u16>,
	},
	/// Stamp the record's id on its post-image and merge the value the
	/// statement supplied for it, then drop what was derived from the
	/// post-image before.
	MergeSupplied,
}

assert_plain!(DataInstr);

/// A named component of the record id as a field view derives it: the field
/// `name`, whose value is `id[position]`, coerced to `kind` where it declares
/// one.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct KeyPart {
	pub(in crate::exec::vm) name: surrealdb_strand::Strand,
	pub(in crate::exec::vm) position: u32,
	pub(in crate::exec::vm) kind: Option<crate::expr::Kind>,
}

/// The most parts [`DataInstr::SnapshotKeyParts`] derives in place; a view
/// deriving more is run as its host pipeline.
const KEY_PARTS_INLINE: usize = 8;

assert_plain!(KeyPart);

impl DataInstr {
	/// The jump target the instruction holds, for the lowering to patch.
	pub(in crate::exec::vm) fn target_mut(&mut self) -> Option<&mut Label> {
		match self {
			DataInstr::Where {
				skip,
				..
			} => Some(skip),
			DataInstr::JumpIfInput {
				target,
			} => Some(target),
			DataInstr::Matched
			| DataInstr::Snapshot {
				..
			}
			| DataInstr::SnapshotKeyParts {
				..
			}
			| DataInstr::ComputeInput {
				..
			}
			| DataInstr::ApplyInput
			| DataInstr::ApplyPatch {
				..
			}
			| DataInstr::CheckDataFields
			| DataInstr::StampRecordFields
			| DataInstr::SettleKeyFields
			| DataInstr::GenerateId {
				..
			}
			| DataInstr::MergeSupplied => None,
		}
	}
}

/// The write state of the statement, for an instruction of its body.
fn write_state(state: &mut State) -> Result<&mut WriteState, ControlFlow> {
	state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))
}

/// The clause this statement evaluates per record: the data clause, or
/// `ON DUPLICATE KEY UPDATE` when there is none.
fn statement_data<'p>(
	program: &'p Program,
	state: &State,
) -> Result<Option<&'p DataShape>, ControlFlow> {
	Ok(spec_of(program, state)?.shape.statement_data())
}

/// Evaluates what the places of a data clause embed by running the host
/// pipelines the program holds for them.
///
/// A subscript is evaluated with no record in scope,
/// and a `WHERE` step judges one element as the row.
pub(super) struct PlaceRun<'a> {
	pub(super) program: &'a Program,
	pub(super) state: &'a mut State,
	pub(super) exec: &'a ExecutionContext,
}

impl PlaceEval for PlaceRun<'_> {
	type Expr = u16;

	fn subscript<'a>(&'a mut self, expr: &'a u16) -> BoxFut<'a, Result<Value, ControlFlow>> {
		Box::pin(async move {
			let ran = run_place(self.program, *expr, Value::None, true, self.state, self.exec);
			clause(ran.await)
		})
	}

	fn accepts<'a>(
		&'a mut self,
		expr: &'a u16,
		element: &'a Value,
	) -> BoxFut<'a, Result<bool, ControlFlow>> {
		Box::pin(async move {
			let ran = run_place(self.program, *expr, element.clone(), false, self.state, self.exec);
			Ok(clause(ran.await)?.is_truthy())
		})
	}
}

/// Apply the evaluated `SET` clause `computed` to `target`, its places the
/// statement's own.
async fn apply_places(
	program: &Program,
	target: &mut Value,
	computed: &[ComputedAssignment],
	state: &mut State,
	exec: &ExecutionContext,
) -> Result<(), ControlFlow> {
	let Some(DataShape::Set(assignments)) = statement_data(program, state)? else {
		return Err(internal("an evaluated SET clause paired with a clause that is not a SET"));
	};
	let places = assignments.iter().map(|assignment| assignment.steps.as_slice());
	let mut run = PlaceRun {
		program,
		state,
		exec,
	};
	apply::apply_set_with(target, places, computed, &mut run).await
}

/// The value `$input` names for the record in flight: what the statement
/// supplied, derived from the evaluated data clause. `NONE` where no clause
/// has been evaluated.
pub(super) async fn statement_input(
	program: &Program,
	exec: &ExecutionContext,
	state: &mut State,
) -> Result<Arc<Value>, ControlFlow> {
	let Some(input) = write_state(state)?.row()?.input.take() else {
		return Ok(Arc::new(Value::None));
	};
	let materialised = match statement_data(program, state) {
		Err(error) => Err(error),
		Ok(None) => Err(ControlFlow::Err(anyhow::anyhow!(
			"an evaluated data clause with no clause behind it"
		))),
		Ok(Some(_)) => match &input {
			ComputedInput::Patch(value)
			| ComputedInput::Merge(value)
			| ComputedInput::Replace(value)
			| ComputedInput::Content(value) => Ok(Arc::clone(value)),
			// An `UNSET` says what to take away, which is no value at all.
			ComputedInput::Unset(_) => Ok(Arc::new(Value::None)),
			// What the assignments make of an empty object, which a compound
			// operator there combines with `NONE`.
			ComputedInput::Set(computed) => {
				let mut value = Value::Object(Object::default());
				apply_places(program, &mut value, computed, state, exec)
					.await
					.map(|()| Arc::new(value))
			}
		},
	};
	write_state(state)?.row()?.input = Some(input);
	materialised
}

/// A clause's value from a host pipeline's run, for a clause written in the
/// statement: a `RETURN` inside it is its value, and a `BREAK` or `CONTINUE`
/// passes outwards to the loop the statement is written in.
fn clause(ran: FlowResult<Value>) -> Result<Value, ControlFlow> {
	match ran {
		Err(ControlFlow::Return(value)) => Ok(value),
		ran => ran,
	}
}

/// A clause's value from a host pipeline's run, for a clause the schema
/// stores: a `RETURN` inside it is its value, and a `BREAK` or `CONTINUE` is
/// an error, since the loop it names is not written in it.
fn field_clause(ran: FlowResult<Value>) -> Result<Value, ControlFlow> {
	ran.catch_return().map_err(ControlFlow::Err)
}

/// The image the condition and the data clause read: the reduced post-image
/// where the caller's reads are checked, and the post-image otherwise.
fn snapshot(row: &WriteRow) -> &Value {
	row.current_reduced.as_ref().unwrap_or(&row.current)
}

/// The image the data clause reads: the [`snapshot`], with the record's id at
/// `id` when the record is being created under an id already known.
///
/// A created record's id is stamped onto its post-image only after the data
/// clause is applied, but an id the statement named, or one a range target
/// expanded to, is known before the clause runs, so `SET name = id.id()` reads
/// it, and `$this` is an object holding that `id` alone. An id still to be
/// generated is not known: `id` reads `NONE`, and so does `$this`. Only the
/// record's own `id` is given: a path reaching an `id` nested in a field, such
/// as `child.id`, reads the image as it is. No other image is touched, so the
/// field clauses, permissions and `$before` of the record are unaffected.
fn data_clause_image(row: &WriteRow) -> Value {
	let image = snapshot(row).clone();
	let Some(rid) = row.id.as_ref().filter(|_| row.initial.is_none()) else {
		return image;
	};
	let mut object = match image {
		Value::Object(object) => object,
		Value::None => Object::default(),
		image @ Value::Null
		| image @ Value::Bool(_)
		| image @ Value::Number(_)
		| image @ Value::String(_)
		| image @ Value::Duration(_)
		| image @ Value::Datetime(_)
		| image @ Value::Date(_)
		| image @ Value::Time(_)
		| image @ Value::Timestamp(_)
		| image @ Value::Interval(_)
		| image @ Value::Uuid(_)
		| image @ Value::Array(_)
		| image @ Value::Set(_)
		| image @ Value::Geometry(_)
		| image @ Value::Bytes(_)
		| image @ Value::Table(_)
		| image @ Value::RecordId(_)
		| image @ Value::File(_)
		| image @ Value::Regex(_)
		| image @ Value::Range(_)
		| image @ Value::Closure(_) => return image,
	};
	if object.get("id").is_none_or(Value::is_none) {
		object.insert("id", Value::RecordId(rid.clone()));
	}
	Value::Object(object)
}

/// The value the statement supplied for the record, which `$input` names.
fn supplied_value(extras: &WriteExtras) -> Option<&Value> {
	match extras {
		WriteExtras::Insert(value) => Some(value),
		WriteExtras::Relate(_, _, Some(value)) => Some(value),
		WriteExtras::Relate(_, _, None) | WriteExtras::Normal => None,
	}
}

/// Derive the record's pre-mutation snapshot, as [`DataInstr::Snapshot`]
/// states.
async fn materialise(
	program: &Program,
	view: Option<u16>,
	bare: Option<u16>,
	checked: bool,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<()> {
	let row = write_state(state)?.row()?;
	if row.id.is_none() {
		return Ok(());
	}
	let derive = !row.initial.is_none();
	if checked {
		if row.current_reduced.is_some() {
			return Ok(());
		}
		let image = row.current.clone();
		let reduced = readable(program, view, bare, derive, image, state, exec).await?;
		write_state(state)?.row()?.current_reduced = Some(reduced);
		return Ok(());
	}
	if derive && !row.fields_computed {
		if let Some(pipeline) = view
			&& matches!(row.current, Value::Object(_))
		{
			let image = row.current.clone();
			let derived = run_image(program, pipeline, image, None, state, exec).await?;
			write_state(state)?.row()?.current = derived;
		}
		write_state(state)?.row()?.fields_computed = true;
	}
	Ok(())
}

/// Derive the record's pre-mutation snapshot, as
/// [`DataInstr::SnapshotKeyParts`] states.
///
/// The field view's host pipeline reads `id` from the image, indexes it
/// ([`evaluate_index`](crate::exec::vm::access::index::evaluate_index): the
/// element of an array key, `NONE` past its end or for any other key),
/// coerces the element to the column's kind, failing with `FieldCoerce`
/// naming the record id, and writes it under the column's name. Over an
/// image whose `id` is a record id this does the same where the image is,
/// with nothing written unless every part coerces, and leaves any other
/// image to the pipeline.
async fn materialise_key_parts(
	program: &Program,
	parts: &[KeyPart],
	view: Option<u16>,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<()> {
	let row = write_state(state)?.row()?;
	if row.id.is_none() || row.initial.is_none() || row.fields_computed {
		return Ok(());
	}
	let in_place = parts.len() <= KEY_PARTS_INLINE
		&& matches!(&row.current, Value::Object(image) if matches!(image.get("id"), Some(Value::RecordId(_))));
	if !in_place {
		return materialise(program, view, None, false, state, exec).await;
	}
	let Value::Object(image) = &mut row.current else {
		return Err(internal("a key-part snapshot of an image that is no object"));
	};
	let Some(Value::RecordId(rid)) = image.get("id") else {
		return Err(internal("a key-part snapshot of an image with no record id"));
	};
	let mut elements = [const { Value::None }; KEY_PARTS_INLINE];
	let derived = &mut elements[..parts.len()];
	for (part, slot) in parts.iter().zip(derived.iter_mut()) {
		let element = crate::exec::operators::scan::key_range::key_element(&rid.key, part.position)
			.cloned()
			.unwrap_or(Value::None);
		*slot = match &part.kind {
			Some(kind) => element.coerce_to_kind(kind).map_err(|error| {
				ControlFlow::Err(anyhow::Error::new(DocError::FieldCoerce {
					record: rid.to_sql(),
					field_name: part.name.as_str().to_owned(),
					error: Box::new(error),
				}))
			})?,
			None => element,
		};
	}
	for (part, element) in parts.iter().zip(derived.iter_mut()) {
		image.insert(part.name.clone(), std::mem::replace(element, Value::None));
	}
	row.fields_computed = true;
	Ok(())
}

/// `image` as a checked caller may read it: run through `view` where there is
/// a pre-image to derive `COMPUTED` fields from, and through `bare` where
/// there is none. An image that is no object, or an absent pipeline, is read
/// as it is.
async fn readable(
	program: &Program,
	view: Option<u16>,
	bare: Option<u16>,
	derive: bool,
	image: Value,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<Value> {
	let through = if derive {
		view
	} else {
		bare
	};
	match through {
		Some(pipeline) if matches!(image, Value::Object(_)) => {
			run_image(program, pipeline, image, None, state, exec).await
		}
		_ => Ok(image),
	}
}

/// Refuse a `PATCH` whose `copy`/`move` `from` or `test` `path` pointer reads
/// a field the caller may not select, as [`DataInstr::ApplyPatch`] states.
///
/// A pointer is judged by what it reads on the record, the operations before
/// it applied, against what it reads on that record's reduced image: any
/// difference is a pointer reaching what the reduction removed. Where the
/// caller's reads are not checked the image is the record and nothing is
/// refused. An operation that fails stops the walk and leaves the failure to
/// the patch applied for real.
async fn check_patch_pointers(
	program: &Program,
	view: Option<u16>,
	bare: Option<u16>,
	checked: bool,
	ops: &Value,
	state: &mut State,
	exec: &ExecutionContext,
) -> FlowResult<()> {
	let operations = Operation::value_to_operations(ops.clone())
		.map_err(ExprError::InvalidPatch)
		.map_err(|error| ControlFlow::Err(anyhow::Error::new(error)))?;
	if !operations.iter().any(|op| data::exposed_read_pointer(op).is_some()) {
		return Ok(());
	}
	materialise(program, view, bare, checked, state, exec).await?;
	let row = write_state(state)?.row()?;
	let Some(reduced) = row.current_reduced.clone() else {
		return Ok(());
	};
	let derive = !row.initial.is_none();
	let mut working = row.current.clone();
	let mut image = Some(reduced);
	for op in operations {
		if let Some(pointer) = data::exposed_read_pointer(&op) {
			let readable = match image.take() {
				Some(readable) => readable,
				None => readable(program, view, bare, derive, working.clone(), state, exec).await?,
			};
			if working.pick(&pointer) != readable.pick(&pointer) {
				return Err(ControlFlow::Err(anyhow::Error::new(
					DocError::PatchPointerNotAllowed(Idiom(pointer).to_sql()),
				)));
			}
		}
		if working.patch(Operation::operations_to_value(vec![op])).is_err() {
			return Ok(());
		}
		image = None;
	}
	Ok(())
}

/// Take `instr` for the record in flight, and answer the instruction the body
/// continues at: `next`, or a target the instruction holds.
/// The settled id of `row` judged against the named types its kind holds where `exec` reaches
/// the catalog, and written under the key those types coerce it to: the `id` field's kind
/// judges the whole key, and on a table whose ids name their components, each component's
/// declared kind judges its element. The settling before it coerced the key to those kinds,
/// which take a named type as written.
async fn check_id_named_types(
	row: &mut WriteRow,
	ctx: &crate::exec::operators::write::ctx::WriteCtx,
	exec: &ExecutionContext,
) -> Result<(), ControlFlow> {
	let Some(id) = row.id.as_ref() else {
		return Ok(());
	};
	if let Some(key) = ctx.key.as_deref() {
		let crate::val::RecordIdKey::Array(elements) = &id.key else {
			return Ok(());
		};
		if !key.components.iter().any(|c| c.declared.as_ref().is_some_and(|k| k.has_named_type())) {
			return Ok(());
		}
		let mut checked = Vec::with_capacity(elements.len());
		for (position, element) in elements.iter().enumerate() {
			let declared = key
				.components
				.iter()
				.find(|c| c.position as usize == position)
				.and_then(|c| c.declared.as_ref().map(|kind| (c, kind)));
			let element = match declared {
				Some((component, kind)) if kind.has_named_type() => {
					crate::exec::vm::schema_host::check_types_held(exec, kind, element.clone())
						.await
						.map_err(|error| {
							ControlFlow::Err(anyhow::Error::new(DocError::IdComponentCoerce {
								table: id.table.to_string(),
								component: match &component.name {
									Some(name) => name.to_string(),
									None => position.to_string(),
								},
								error: error.to_string(),
							}))
						})?
				}
				Some(_) | None => element.clone(),
			};
			checked.push(element);
		}
		let key = Value::Array(checked.into());
		if key != id.key.clone().into_value() {
			let table = id.table.clone();
			row.id = Some(key.generate(table, false).map_err(ControlFlow::Err)?);
		}
		return Ok(());
	}
	let Some(kind) = ctx
		.id_field
		.and_then(|index| ctx.fields.get(index))
		.and_then(|fd| fd.field_kind.as_ref())
		.filter(|kind| kind.has_named_type() && !kind.is_record())
	else {
		return Ok(());
	};
	let key = id.key.clone().into_value();
	let checked = crate::exec::vm::schema_host::check_types_held(exec, kind, key.clone())
		.await
		.map_err(|error| {
			ControlFlow::Err(anyhow::Error::new(DocError::FieldCoerce {
				record: id.to_sql(),
				field_name: "id".to_string(),
				error: Box::new(error),
			}))
		})?;
	if checked != key {
		let table = id.table.clone();
		row.id = Some(checked.generate(table, false).map_err(ControlFlow::Err)?);
	}
	Ok(())
}

pub(super) async fn step(
	program: &Program,
	instr: &DataInstr,
	next: usize,
	state: &mut State,
) -> FlowResult<usize> {
	let (row, ctx) = write_state(state)?.in_flight()?;
	let exec = ctx.exec();
	match instr {
		DataInstr::Matched => {
			row.matched = true;
			Ok(next)
		}
		DataInstr::Snapshot {
			view,
			bare,
			checked,
		} => {
			materialise(program, *view, *bare, *checked, state, exec).await?;
			Ok(next)
		}
		DataInstr::SnapshotKeyParts {
			parts,
			view,
		} => {
			materialise_key_parts(program, parts, *view, state, exec).await?;
			Ok(next)
		}
		DataInstr::Where {
			pipeline,
			skip,
		} => {
			let image = snapshot(row).clone();
			let verdict = clause(run_image(program, *pipeline, image, None, state, exec).await)?;
			if verdict.is_truthy() {
				// Recorded before carrying on: a later stage may still decline
				// to show the record, and it is this flag that tells a matched
				// record apart from one the condition rejected.
				write_state(state)?.row()?.matched = true;
				Ok(next)
			} else {
				Ok(*skip as usize)
			}
		}
		DataInstr::JumpIfInput {
			target,
		} => Ok(if row.input.is_some() {
			*target as usize
		} else {
			next
		}),
		DataInstr::ComputeInput {
			pipeline,
			binds_input,
		} => {
			// The clause is evaluated against the record's content as an object,
			// so a stored scalar is edited as the object of its id.
			row.content_from_id();
			// A clause evaluated before the record has an id may derive it. What
			// it draws from sequences the transaction defined is noted for a
			// retry of the record under that id (see `WriteRow::id_draws`).
			let txn = (row.id.is_none() && row.generate_in.is_some()).then(|| ctx.txn());
			let recording = txn.as_ref().map(|txn| txn.record_provisional_draws());
			let value = match pipeline {
				Some(pipeline) => {
					let image = data_clause_image(row);
					let input = if *binds_input {
						supplied_value(&row.extras).cloned()
					} else {
						None
					};
					let outer = std::mem::replace(&mut state.write_input, input);
					let ran = run_image(program, *pipeline, image, None, state, exec).await;
					state.write_input = outer;
					Some(clause(ran)?)
				}
				None => None,
			};
			let draws = recording.map(|recording| recording.finish());
			let data = statement_data(program, state)?
				.ok_or_else(|| internal("a data clause with none"))?;
			let computed = match (data, value) {
				(DataShape::Unset(paths), None) => ComputedInput::Unset(paths.clone()),
				(DataShape::Patch, Some(value)) => ComputedInput::Patch(Arc::new(value)),
				(DataShape::Merge, Some(value)) => ComputedInput::Merge(Arc::new(value)),
				(DataShape::Replace, Some(value)) => ComputedInput::Replace(Arc::new(value)),
				(DataShape::Content, Some(value)) => ComputedInput::Content(Arc::new(value)),
				(DataShape::Set(assignments), Some(Value::Array(values)))
					if values.len() == assignments.len() =>
				{
					ComputedInput::Set(
						assignments
							.iter()
							.zip(values.0)
							.map(|(assignment, value)| ComputedAssignment {
								place: assignment.path.clone(),
								operator: assignment.operator.clone(),
								value,
							})
							.collect(),
					)
				}
				_ => return Err(internal("a data clause evaluated to a shape it does not take")),
			};
			let row = write_state(state)?.row()?;
			row.input = Some(computed);
			if let Some(draws) = draws {
				row.id_draws = draws;
			}
			Ok(next)
		}
		DataInstr::ApplyInput => {
			// Taken rather than cloned so the record can be mutated while the
			// clause is read, and put back before this returns either way.
			let Some(input) = row.input.take() else {
				return Ok(next);
			};
			let applied = match &input {
				ComputedInput::Merge(data) => {
					row.current.merge(Value::clone(data)).map_err(ControlFlow::Err)
				}
				ComputedInput::Replace(data) | ComputedInput::Content(data) => {
					row.current.replace(Value::clone(data)).map_err(ControlFlow::Err)
				}
				// Cutting a constant path out of a value reads nothing and
				// evaluates nothing.
				ComputedInput::Unset(paths) => {
					for path in paths.iter() {
						row.current.cut(path);
					}
					Ok(())
				}
				// A place may embed expressions, which run on the state, so
				// the record is set aside while they do.
				ComputedInput::Set(computed) => {
					let mut target = std::mem::replace(&mut row.current, Value::None);
					let applied = apply_places(program, &mut target, computed, state, exec).await;
					write_state(state)?.row()?.current = target;
					applied
				}
				ComputedInput::Patch(_) => {
					Err(internal("a PATCH applied by the data instructions"))
				}
			};
			let row = write_state(state)?.row()?;
			row.input = Some(input);
			applied?;
			row.current_reduced = None;
			row.fields_computed = false;
			Ok(next)
		}
		DataInstr::ApplyPatch {
			view,
			bare,
			checked,
		} => {
			let Some(ComputedInput::Patch(data)) = &row.input else {
				return Err(internal("a PATCH applied with no evaluated PATCH"));
			};
			let data = Arc::clone(data);
			check_patch_pointers(program, *view, *bare, *checked, &data, state, exec).await?;
			let row = write_state(state)?.row()?;
			row.current.patch(Value::clone(&data)).map_err(ControlFlow::Err)?;
			// A root replaced by NONE or NULL leaves the record with its id alone.
			// Any other non-object root is refused below.
			if matches!(row.current, Value::None | Value::Null) {
				row.object_of_id();
			}
			// A record's content is an object. The check is on the result, so a
			// PATCH that replaces the root with a scalar is refused as a CONTENT
			// clause is, whichever operation put it there.
			if !row.current.is_object() {
				return Err(ControlFlow::Err(anyhow::Error::new(ExprError::InvalidContent {
					value: row.current.clone(),
				})));
			}
			row.current_reduced = None;
			row.fields_computed = false;
			Ok(next)
		}
		DataInstr::CheckDataFields => {
			data::check_data_fields(row)?;
			Ok(next)
		}
		DataInstr::StampRecordFields => {
			let as_read = match data::stored_edge_as_read(row, &ctx)? {
				Some(read) => Some(read.await?),
				None => None,
			};
			data::stamp_record_fields(row, as_read.as_ref())?;
			Ok(next)
		}
		DataInstr::SettleKeyFields => {
			data::settle_key_fields(row, &ctx)?;
			Ok(next)
		}
		DataInstr::GenerateId {
			default,
		} => {
			let data::IdSettling::Default {
				table,
			} = data::settle_record_id(row, &ctx)?
			else {
				check_id_named_types(row, &ctx, exec).await?;
				return Ok(next);
			};
			let (Some(pipeline), Some(field)) =
				(default, ctx.id_field.and_then(|index| ctx.fields.get(index)))
			else {
				return Err(internal("an id DEFAULT the statement was not planned with"));
			};
			let frame = field_default_frame(exec, field)
				.map_err(ControlFlow::Err)?
				.refusing_writes(crate::dbs::NoWriteFrame::FieldDefault);
			let doc = data::id_default_doc(row).clone();
			// What the DEFAULT draws from sequences the transaction defined is
			// noted for a retry of the record under this id (see
			// `WriteRow::id_draws`).
			let txn = ctx.txn();
			let recording = txn.record_provisional_draws();
			let value = run_image(program, *pipeline, doc, Some(frame), state, exec).await;
			let draws = recording.finish();
			let value = field_clause(value)?;
			let row = write_state(state)?.row()?;
			data::check_default_id(row, &ctx, &table, &value)?;
			row.id_draws.extend(draws);
			let id = value.generate(table, false).map_err(ControlFlow::Err)?;
			data::finish_record_id(row, &ctx, id)?;
			check_id_named_types(row, &ctx, exec).await?;
			Ok(next)
		}
		DataInstr::MergeSupplied => {
			let rid = row.id.clone().ok_or_else(|| {
				ControlFlow::Err(anyhow::anyhow!("Expected a record id to be present"))
			})?;
			row.current.def(rid);
			let merged = match &row.extras {
				WriteExtras::Insert(value) => Some(Value::clone(value)),
				WriteExtras::Relate(_, _, Some(value)) => Some(Value::clone(value)),
				WriteExtras::Relate(_, _, None) | WriteExtras::Normal => None,
			};
			if let Some(merged) = merged {
				row.current.merge(merged).map_err(ControlFlow::Err)?;
			}
			// The record's field values have changed, so anything derived from
			// the previous ones is stale.
			row.fields_computed = false;
			Ok(next)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{RecordId, RecordIdKey, TableName};

	fn earth() -> RecordId {
		RecordId::new(TableName::from("planet"), RecordIdKey::String("earth".into()))
	}

	fn object(fields: Vec<(&str, Value)>) -> Value {
		Value::Object(Object::from_iter(fields))
	}

	#[test]
	fn a_record_created_under_a_named_id_reads_it() {
		let row = WriteRow::for_target(Value::RecordId(earth())).expect("a record target");
		assert_eq!(data_clause_image(&row), object(vec![("id", Value::RecordId(earth()))]));
	}

	#[test]
	fn a_record_whose_id_is_still_to_be_generated_reads_none() {
		let row =
			WriteRow::for_target(Value::Table(TableName::from("planet"))).expect("a table target");
		assert_eq!(data_clause_image(&row), Value::None);
	}

	#[test]
	fn an_existing_record_reads_its_image_as_it_is() {
		let mut row = WriteRow::for_target(Value::RecordId(earth())).expect("a record target");
		let stored = object(vec![("name", Value::from("earth"))]);
		row.initial = stored.clone();
		row.current = stored.clone();
		assert_eq!(data_clause_image(&row), stored);
	}

	#[test]
	fn an_id_the_image_holds_is_kept() {
		let mut row = WriteRow::for_target(Value::RecordId(earth())).expect("a record target");
		let mars = Value::RecordId(RecordId::new(
			TableName::from("planet"),
			RecordIdKey::String("mars".into()),
		));
		row.current = object(vec![("id", mars.clone())]);
		assert_eq!(data_clause_image(&row), object(vec![("id", mars)]));
	}
}
