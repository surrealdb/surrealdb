//! The host's side of the schema stages run as instructions: ProcessTableFields, CleanupTableFields
//! and MaterialiseObservedFields.
//!
//! The instructions act on the record in flight through the write state; an
//! expression they evaluate is a host pipeline over one of the record's
//! images (see `doc/executor/VM_WRITE_STAGES.md`).
//!
//! The field schema is applied in the order the field kernel of
//! [`crate::exec::operators::write::stages::schema`] uses: one field at a
//! time in the table's field order and, within a field, one place at a
//! time. [`SchemaInstr::Field`] opens a field, [`SchemaInstr::NextPlace`]
//! steps to its next place, the clause instructions produce the place's
//! value and [`SchemaInstr::Land`] writes it back. The field definitions
//! are read from the write's context and must be the fields the plan names.

use std::sync::Arc;

use anyhow::Context as _;
use surrealdb_types::ToSql;

use super::data::statement_input;
use super::{State, WriteState, internal, run_image, spec_of};
use crate::catalog::{DefineDefault, FieldDefinition, TableDefinition};
use crate::dbs::NoWriteFrame;
use crate::doc::Error as DocError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::stages::schema::{
	clean_none, has_record_observers, normalise_set_elements, optional_and_absent,
	refuse_undefined_fields,
};
use crate::exec::permission::{field_default_frame, should_check_perms};
use crate::exec::vm::program::{DataShape, Label, Program};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::expr::statements::define::declared_type::TypeViolation;
use crate::expr::statements::define::sql_types;
use crate::iam::Action;
use crate::val::{Array, RecordId, Set, TableName, Value};

/// What the schema instructions know of the table, read from the catalog when
/// the statement was planned: which fields it declared, in the order the
/// catalog held them. A field is named by its place in that order; the
/// definitions themselves are the ones the write's context reads from the
/// catalog when the record is written, and the instructions fail where those
/// are not the fields this plan names.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct SchemaPlan {
	pub(in crate::exec::vm) table: TableName,
	/// Whether the table is `SCHEMAFULL`.
	pub(in crate::exec::vm) schemafull: bool,
	/// Index of the `id` field in `fields`.
	pub(in crate::exec::vm) id_field: Option<usize>,
	/// The table's fields, in the order the catalog holds them.
	pub(in crate::exec::vm) fields: Vec<FieldPlan>,
}

/// A field of a [`SchemaPlan`].
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct FieldPlan {
	/// The field's path as the catalog names it.
	pub(in crate::exec::vm) name: String,
}

assert_plain!(SchemaPlan);

impl SchemaPlan {
	/// The definitions `ctx` reads of the table's fields, when they are the
	/// fields this plan names.
	fn definitions(&self, ctx: &WriteCtx) -> Result<Arc<[FieldDefinition]>, ControlFlow> {
		let fields = Arc::clone(&ctx.fields);
		let same = fields.len() == self.fields.len()
			&& ctx.id_field == self.id_field
			&& ctx
				.field_names
				.get_or_init(|| fields.iter().map(|fd| fd.name.to_raw_string()).collect())
				.iter()
				.zip(&self.fields)
				.all(|(name, plan)| *name == plan.name);
		if !same {
			return Err(internal(&format!(
				"the fields of {} are not those its write was planned against",
				self.table
			)));
		}
		Ok(fields)
	}
}

/// A field's `PERMISSIONS FOR create` or `update`, as the instructions judge
/// a change against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::vm) enum FieldPermission {
	/// `FULL`: every change stands.
	Allow,
	/// `NONE`: no change stands.
	Deny,
	/// `WHERE p`: a change stands when host pipeline
	/// [`Program::host_code`]`[pipeline]` answers truthy over the post-image,
	/// with the place's parameters bound and its reads not permission-checked.
	Predicate(u16),
}

/// What a field's `PERMISSIONS FOR create` and `update` answer for a change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::vm) struct FieldWrite {
	/// `PERMISSIONS FOR create`, judged for a record being created.
	pub(in crate::exec::vm) create: FieldPermission,
	/// `PERMISSIONS FOR update`, judged for a record that exists.
	pub(in crate::exec::vm) update: FieldPermission,
}

impl FieldWrite {
	/// Whether every change stands whether the record is created or updated.
	pub(in crate::exec::vm) fn admits_all(self) -> bool {
		self.create == FieldPermission::Allow && self.update == FieldPermission::Allow
	}
}

/// An instruction of the schema stages.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum SchemaInstr {
	/// Begin applying `table`'s field schema to the record in flight: jump to
	/// `end` under `OPTION IMPORT`, where no clause applies, and otherwise take
	/// the record's id, what the statement supplied (`$input`) and whether the
	/// record is being created.
	Begin {
		table: Arc<SchemaPlan>,
		end: Label,
	},
	/// Open field `field` of the table: its frame — the caller's authority
	/// narrowed to the field's `AUTH LIMIT` — whether its `PERMISSIONS`, `write`,
	/// are enforced under that frame, and every place its path names in the
	/// post-image. A field inside `id` names no place: it is a part of the
	/// record-id key, and only its `ASSERT` is deferred, for a record being
	/// created, against each place of the key its path names.
	Field {
		field: u16,
		write: FieldWrite,
	},
	/// Step to the open field's next place, or jump to `done` when none is
	/// left. The `id` field of an existing record, and a `READONLY` field of
	/// one, are checked here and never produced. A place inside an absent
	/// optional field's empty value is not produced either, but its enforced
	/// permission predicate is still judged and the answer is discarded.
	NextPlace {
		done: Label,
	},
	/// The field's `DEFAULT` — or its static `VALUE`, for a field declaring no
	/// `DEFAULT` — evaluated by host pipeline
	/// [`Program::host_code`]`[pipeline]` for a place that holds nothing, on a
	/// record being created or for a `DEFAULT ALWAYS`.
	Default {
		pipeline: u16,
	},
	/// Jump to `target` when the place holds nothing.
	JumpIfPlaceNone {
		target: Label,
	},
	/// Coerce the place's value to the field's `TYPE`, and, when `sql_type` is set, hold the
	/// coerced value to the field's declared SQL type. A field's last coercion sets it, so the
	/// declared type judges the value the field stores exactly once, after any `VALUE`.
	Coerce {
		sql_type: bool,
	},
	/// The field's `VALUE`, evaluated by host pipeline
	/// [`Program::host_code`]`[pipeline]`: its answer is the place's value.
	Value {
		pipeline: u16,
	},
	/// Judge the place's `ASSERT` once every field is produced.
	DeferAssert,
	/// Revert the place to the value the record held before the statement
	/// when the open field's `PERMISSIONS` deny the change.
	Permit,
	/// Write the place's value back into the post-image, and note a place an
	/// absent optional field left empty, whose places within are skipped.
	Land,
	/// Close the open field.
	EndField,
	/// Judge every deferred `ASSERT` against the record as it will be stored:
	/// field `f`'s by host pipeline [`Program::host_code`]`[pipelines[f]]`.
	Asserts {
		pipelines: Arc<[Option<u16>]>,
	},
	/// Strip the table's `COMPUTED` fields, `computed`, from the post-image.
	Finish {
		computed: Arc<[Idiom]>,
	},
	/// On a `SCHEMAFULL` table refuse a field it does not define, then drop
	/// every `NONE` the post-image holds in an object.
	Cleanup {
		table: Arc<SchemaPlan>,
	},
	/// Derive the table's `COMPUTED` fields onto both images when anything
	/// observes the record after it is written: each image is handed through
	/// host pipelines [`Program::host_code`]`[pipelines[..]]` in order, each
	/// deriving the fields after those the earlier pipelines derived.
	Observe {
		pipelines: Arc<[u16]>,
	},
	/// Derive one `COMPUTED` field a later field's clause reads onto the
	/// post-image, by host pipeline [`Program::host_code`]`[pipeline]`, for a
	/// record with an id. The post-image is not marked as carrying its
	/// computed values: only this one is derived. While a set awaits its
	/// rebuild, the derivation reads the image with it rebuilt, and only the
	/// table's `COMPUTED` fields, `computed`, are taken back from it.
	Derive {
		pipeline: u16,
		computed: Arc<[Idiom]>,
	},
	/// Refuse the record: the table's producing clauses read each other in a
	/// cycle, `fields`, so no record is correctly produced.
	Cycle {
		table: TableName,
		fields: Arc<str>,
	},
}

assert_plain!(SchemaInstr);

impl SchemaInstr {
	/// The jump target the instruction holds, for the lowering to patch.
	pub(in crate::exec::vm) fn target_mut(&mut self) -> Option<&mut Label> {
		match self {
			SchemaInstr::Begin {
				end,
				..
			} => Some(end),
			SchemaInstr::NextPlace {
				done,
			} => Some(done),
			SchemaInstr::JumpIfPlaceNone {
				target,
			} => Some(target),
			SchemaInstr::Field {
				..
			}
			| SchemaInstr::Default {
				..
			}
			| SchemaInstr::Coerce {
				..
			}
			| SchemaInstr::Value {
				..
			}
			| SchemaInstr::DeferAssert
			| SchemaInstr::Permit
			| SchemaInstr::Land
			| SchemaInstr::EndField
			| SchemaInstr::Asserts {
				..
			}
			| SchemaInstr::Finish {
				..
			}
			| SchemaInstr::Cleanup {
				..
			}
			| SchemaInstr::Observe {
				..
			}
			| SchemaInstr::Derive {
				..
			}
			| SchemaInstr::Cycle {
				..
			} => None,
		}
	}
}

/// The field schema's state for the record in flight, between
/// [`SchemaInstr::Begin`] and [`SchemaInstr::Finish`].
pub(super) struct SchemaRecord {
	table: Arc<SchemaPlan>,
	/// The definitions of the table's fields, as the plan names them.
	fields: Arc<[FieldDefinition]>,
	rid: RecordId,
	/// What the statement supplied, which a clause reads at its own place.
	input: Arc<Value>,
	/// Whether the record is being created: it has no before-image.
	creating: bool,
	/// The places left empty under a kind admitting `NONE`, as paths within
	/// the record. A place within one of them is not produced: there is no
	/// object for its clauses to be about. Each is a place, not a field, so
	/// one element of `a.*` left empty skips the places within that element
	/// and no other.
	absent: Vec<Idiom>,
	/// The places whose `ASSERT` is judged once every field is produced.
	deferred: Vec<Deferred>,
	/// The fields that landed a value within a set, whose sets are rebuilt
	/// once every field is produced. Rebuilding a set reorders its elements,
	/// and every field names an element of one by its position, in the
	/// post-image and in the before-image alike, so no set is rebuilt while a
	/// field may still read one.
	rebuild: Vec<usize>,
	/// The field open.
	field: Option<FieldCursor>,
}

/// A place whose `ASSERT` is judged once every field is produced.
struct Deferred {
	/// The index of the field.
	index: usize,
	/// The place's path within the record.
	path: Idiom,
}

/// The field of a [`SchemaRecord`] that is open.
struct FieldCursor {
	index: usize,
	/// The frame every clause of the field runs in.
	frame: ExecutionContext,
	/// Whether the field's `PERMISSIONS` are enforced.
	guarded: bool,
	/// The field's `PERMISSIONS FOR create` and `update`.
	write: FieldWrite,
	/// The places the field's path names, as the post-image held them when
	/// the field was opened, followed by any places present in the pre-image
	/// whose parent container vanished from the post-image (`true`).
	places: std::vec::IntoIter<(Idiom, Value, bool)>,
	/// The place stepped to.
	place: Option<Place>,
	/// Whether the field landed a value within a set, whose sets are then
	/// rebuilt once every field is produced.
	within_set: bool,
}

/// One place of a field: its path, its value, what the record held there
/// before the statement, and what the statement supplied there.
struct Place {
	path: Idiom,
	val: Value,
	old: Value,
	supplied: Value,
}

impl SchemaRecord {
	fn cursor(&mut self) -> Result<&mut FieldCursor, ControlFlow> {
		self.field.as_mut().ok_or_else(|| internal("a field step with no field open"))
	}

	fn place(&mut self) -> Result<&mut Place, ControlFlow> {
		self.cursor()?.place.as_mut().ok_or_else(|| internal("a place step with no place"))
	}

	fn definition(&self) -> Result<&FieldDefinition, ControlFlow> {
		let index = self.field.as_ref().ok_or_else(|| internal("no field open"))?.index;
		Ok(&self.fields[index])
	}
}

/// The parameters a field clause reads, bound on `frame` for one place:
/// `$before` and `$input` the place's, and `$value` and `$after` the value the
/// clause is about.
fn clause_frame(frame: &ExecutionContext, place: &Place, value: &Value) -> ExecutionContext {
	frame.with_params([
		("before", place.old.clone()),
		("input", place.supplied.clone()),
		("after", value.clone()),
		("value", value.clone()),
	])
}

fn fail(error: impl std::error::Error + Send + Sync + 'static) -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(error))
}

/// Holds `val`, a value of field `index` already coerced to the field's kind, to the field's
/// declared SQL type, if it declares one. The outer error is a declared type that does not parse,
/// which the stored definition should not hold. The inner error is the violation with the text
/// of the declared type.
fn check_sql_type(
	ctx: &WriteCtx,
	index: usize,
	val: Value,
) -> Result<Result<Value, (TypeViolation, String)>, ControlFlow> {
	let Some(declared) = ctx.declared_types().get(index).and_then(Option::as_ref) else {
		return Ok(Ok(val));
	};
	let declared = match declared {
		Ok(declared) => declared,
		Err(error) => return Err(internal(&format!("invalid declared SQL type: {error}"))),
	};
	let column = ctx.fields[index].name.to_raw_string();
	Ok(sql_types::validate(declared, val, &column)
		.map_err(|violation| (violation, declared.canonical_text())))
}

/// Reads `val` as the text of field `index`'s declared SQL type when it is a string and the
/// type has a text form of its own (`money`, `pg_lsn`, the geometric types, the ranges), as
/// PostgreSQL reads an untyped literal assigned to a column. Any other value, and a string for a
/// field whose declared type is stored as text, is returned unchanged. The errors are those of
/// [`check_sql_type`].
fn read_sql_text(
	ctx: &WriteCtx,
	index: usize,
	val: Value,
) -> Result<Result<Value, (TypeViolation, String)>, ControlFlow> {
	let Value::String(text) = &val else {
		return Ok(Ok(val));
	};
	let Some(declared) = ctx.declared_types().get(index).and_then(Option::as_ref) else {
		return Ok(Ok(val));
	};
	let declared = match declared {
		Ok(declared) => declared,
		Err(error) => return Err(internal(&format!("invalid declared SQL type: {error}"))),
	};
	match sql_types::text_form::read(declared, text.as_str()) {
		None => Ok(Ok(val)),
		Some(read) => Ok(read.map_err(|violation| (violation, declared.canonical_text()))),
	}
}

/// Fills the open place with the `DEFAULT` of the type the open field's `TYPE` names
/// ([`crate::exec::vm::schema_host::type_default`]) where the place holds nothing, the record is
/// being created and the field declares no `DEFAULT` of its own. As a field's plain `DEFAULT`
/// does, the type's `DEFAULT` spares a record that already exists, so a write that leaves the
/// field `NONE` there leaves it `NONE`.
async fn fill_type_default(state: &mut State) -> FlowResult<()> {
	let write = write_state(state)?;
	let ctx = write.in_flight()?.1;
	let (exec, table) = (ctx.exec().clone(), Arc::clone(&ctx.table));
	let schema = record(write)?;
	if !schema.creating {
		return Ok(());
	}
	let fields = Arc::clone(&schema.fields);
	let fd = &fields[schema.cursor()?.index];
	let Some(kind @ crate::expr::Kind::Named(_)) = fd.field_kind.as_ref() else {
		return Ok(());
	};
	if fd.name.is_id()
		|| !matches!(fd.default, DefineDefault::None)
		|| !schema.place()?.val.is_none()
	{
		return Ok(());
	}
	let default = crate::exec::vm::schema_host::type_default(
		&exec,
		table.namespace_id,
		table.database_id,
		kind,
	)
	.await
	.map_err(ControlFlow::Err)?;
	if let Some(default) = default {
		record(write_state(state)?)?.place()?.val = default;
	}
	Ok(())
}

fn write_state(state: &mut State) -> Result<&mut WriteState, ControlFlow> {
	state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))
}

fn record(write: &mut WriteState) -> Result<&mut SchemaRecord, ControlFlow> {
	write.schema.as_deref_mut().ok_or_else(|| internal("a schema step before its record began"))
}

/// The fields whose sets await their rebuild — those recorded, and the open
/// field when it has landed a value within a set — and the table's fields.
///
/// The open field is included because a clause reads a copy of the record,
/// never the record itself: the record keeps its sets' positions while the
/// field's later places still name elements by position, and the copy a
/// clause reads holds every set rebuilt. A set the field has landed a value in
/// is out of order until it is rebuilt, and a lookup in it (`CONTAINS`, `IN`)
/// searches its elements by order, so read unrebuilt it would miss elements it
/// holds.
fn pending_sets(schema: &SchemaRecord) -> (Vec<usize>, Arc<[FieldDefinition]>) {
	let open = schema.field.as_ref().filter(|cursor| cursor.within_set).map(|cursor| cursor.index);
	(schema.rebuild.iter().copied().chain(open).collect(), Arc::clone(&schema.fields))
}

/// Rebuild every set holding a value a field landed, once every field is
/// produced: the record's sets are then distinct and ascending, as they are
/// stored and as its assertions judge them.
fn rebuild_sets(write: &mut WriteState) -> Result<(), ControlFlow> {
	let schema = record(write)?;
	if schema.rebuild.is_empty() {
		return Ok(());
	}
	let rebuild = std::mem::take(&mut schema.rebuild);
	let fields = Arc::clone(&schema.fields);
	normalise_set_elements(&mut write.row()?.current, rebuild.iter().map(|&index| &fields[index]));
	Ok(())
}

/// Run clause pipeline `pipeline` for the open field's place: over the
/// post-image as it stands, in the field's frame with the place's parameters
/// bound to `value`, or to the place's own value, refusing what the clause
/// `refusing` names must not do.
///
/// A field clause is a barrier to loops. A `BREAK` or `CONTINUE` in it names
/// a loop outside the clause, so it fails with
/// [`InvalidControlFlow`](crate::exec::Error::InvalidControlFlow) and never
/// leaves the write. A `RETURN` gives the clause's value.
///
/// The post-image's sets are rebuilt only once every field is produced, so
/// that each field names the elements of one by position; a clause reads the
/// image with every set a field has landed a value within rebuilt, as it will
/// be stored.
async fn place_clause(
	program: &Program,
	pipeline: u16,
	state: &mut State,
	value: Option<Value>,
	refusing: NoWriteFrame,
) -> FlowResult<Value> {
	let write = write_state(state)?;
	let (pending, fields) = pending_sets(record(write)?);
	let (row, ctx) = write.in_flight()?;
	let exec = ctx.exec();
	let mut doc = row.current.clone();
	if !pending.is_empty() {
		normalise_set_elements(&mut doc, pending.iter().map(|&index| &fields[index]));
	}
	let cursor = record(write)?.cursor()?;
	let place = cursor.place.as_ref().ok_or_else(|| internal("a clause with no place"))?;
	let value = value.unwrap_or_else(|| place.val.clone());
	let bound = clause_frame(&cursor.frame, place, &value).refusing_writes(refusing);
	match run_image(program, pipeline, doc, Some(bound), state, exec).await {
		Err(ControlFlow::Return(value)) => Ok(value),
		Err(ControlFlow::Break | ControlFlow::Continue) => {
			Err(fail(crate::exec::Error::InvalidControlFlow))
		}
		ran => ran,
	}
}

/// Judge write permission predicate `pipeline` for the open field's place and
/// `value` — over the post-image as it stands, in the field's frame with the
/// place's parameters bound, reading without permission checks — and answer
/// whether the change stands.
async fn judged(
	program: &Program,
	pipeline: u16,
	state: &mut State,
	value: Value,
) -> FlowResult<bool> {
	let outer = std::mem::replace(&mut state.unchecked_reads, true);
	let answer =
		place_clause(program, pipeline, state, Some(value), NoWriteFrame::WritePermission).await;
	state.unchecked_reads = outer;
	Ok(answer?.is_truthy())
}

/// The permission of the open field that governs the record's change: its
/// `PERMISSIONS FOR create` for a record being created, and its `update`
/// otherwise; `None` when the field's permissions are not enforced.
fn governing(schema: &mut SchemaRecord) -> Result<Option<FieldPermission>, ControlFlow> {
	let creating = schema.creating;
	let cursor = schema.cursor()?;
	Ok(cursor.guarded.then_some(if creating {
		cursor.write.create
	} else {
		cursor.write.update
	}))
}

/// Restores `val` at `path` within `current`, recreating any intermediate
/// object, array, or set containers from `initial` when an ancestor was deleted
/// or replaced.
fn restore_place(current: &mut Value, initial: &Value, path: &[Part], val: Value) {
	let Some((first, rest)) = path.split_first() else {
		*current = val;
		return;
	};
	match first {
		Part::Field(f) => {
			if !matches!(current, Value::Object(_)) {
				*current = Value::empty_object();
			}
			let Value::Object(obj) = current else {
				return;
			};
			if rest.is_empty() {
				if val.is_none() {
					obj.remove(f.as_str());
				} else {
					obj.insert(f.clone(), val);
				}
			} else {
				let init_child = initial.pick(std::slice::from_ref(first));
				let entry = obj.entry(f.clone()).or_insert_with(|| Value::None);
				restore_place(entry, &init_child, rest, val);
			}
		}
		Part::All
		| Part::Flatten
		| Part::First
		| Part::Last
		| Part::Where(_)
		| Part::Lookup(_)
		| Part::Value(_)
		| Part::Start(_)
		| Part::Method(_, _)
		| Part::Destructure(_)
		| Part::Optional
		| Part::Recurse(_, _, _)
		| Part::Doc
		| Part::RepeatRecurse => {
			let Some(idx) = first.as_old_index() else {
				return;
			};
			let init_child = initial.pick(std::slice::from_ref(first));
			if !matches!(current, Value::Array(_) | Value::Set(_)) {
				*current = if matches!(initial, Value::Set(_)) {
					Value::Set(Set::new())
				} else {
					Value::Array(Array::new())
				};
			}
			match current {
				Value::Array(arr) => {
					while arr.len() <= idx {
						let filler =
							if arr.len() < idx && matches!(rest.first(), Some(Part::Field(_))) {
								Value::empty_object()
							} else {
								Value::None
							};
						arr.push(filler);
					}
					restore_place(&mut arr[idx], &init_child, rest, val);
				}
				Value::Set(set) => {
					if let Some(slot) = set.nth_mut(idx) {
						restore_place(slot, &init_child, rest, val);
					} else {
						let mut elem = Value::None;
						restore_place(&mut elem, &init_child, rest, val);
						set.insert(elem);
					}
				}
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::Bytes(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Table(_)
				| Value::Closure(_) => {}
			}
		}
	}
}

/// Whether the caller's `SELECT` permissions restrict reading `fields[index]`:
/// the table's own `SELECT` permission, or that of the field or any of its
/// ancestor fields, is anything but `FULL`. Error messages for `TYPE` coercion
/// and `ASSERT` failures on that field then redact the rejected value, since
/// the caller may be able to write a record it cannot read.
fn select_value_hidden(
	state: &State,
	exec: &ExecutionContext,
	table: &TableDefinition,
	fields: &[FieldDefinition],
	index: usize,
) -> Result<bool, ControlFlow> {
	if state.permissions_bypassed || state.unchecked_reads {
		return Ok(false);
	}
	let Ok(db_ctx) = exec.database() else {
		return Ok(false);
	};
	if !should_check_perms(db_ctx, Action::View).map_err(|e| ControlFlow::Err(e.into()))? {
		return Ok(false);
	}
	if !matches!(table.permissions.select, crate::catalog::Permission::Full) {
		return Ok(true);
	}
	let target = &fields[index].name.0;
	Ok(fields.iter().any(|anc| {
		anc.name.0.len() <= target.len()
			&& target.starts_with(&anc.name.0)
			&& !matches!(anc.select_permission, crate::catalog::Permission::Full)
	}))
}

/// Take `instr` for the record in flight, and answer the instruction the body
/// continues at: `next`, or a target the instruction holds.
pub(super) async fn step(
	program: &Program,
	instr: &SchemaInstr,
	next: usize,
	state: &mut State,
) -> FlowResult<usize> {
	match instr {
		SchemaInstr::Begin {
			table,
			end,
		} => {
			let write = write_state(state)?;
			let (row, ctx) = write.in_flight()?;
			// An import replays records as they were exported, and the state a
			// write derives around them was derived when they were first
			// written.
			if ctx.exec().options().is_some_and(|options| options.import) {
				return Ok(*end as usize);
			}
			let rid =
				row.id.clone().ok_or_else(|| internal("Expected a record id to be present"))?;
			let creating = row.initial.is_none();
			// What the statement supplied, derived once per record.
			let input = statement_input(program, ctx.exec(), state).await?;
			let fields = table.definitions(&ctx)?;
			let write = write_state(state)?;
			write.schema = Some(Box::new(SchemaRecord {
				table: Arc::clone(table),
				fields,
				rid,
				input,
				creating,
				absent: Vec::new(),
				deferred: Vec::new(),
				rebuild: Vec::new(),
				field: None,
			}));
			Ok(next)
		}
		SchemaInstr::Field {
			field,
			write: permissions,
		} => {
			let write = write_state(state)?;
			let ctx = write.in_flight()?.1;
			let fields = Arc::clone(&record(write)?.fields);
			let fd = &fields[*field as usize];
			let places = if fd.name.is_id_subpath() {
				// A place inside `id` is a part of the record-id key, which the
				// post-image holds as a record id the walk does not enter. Its
				// `TYPE` is held to the key where the id is settled, and a
				// `VALUE`, `DEFAULT` or `PERMISSIONS` has nothing to act on, since
				// the key is fixed once settled: the field produces no place.
				// Only its `ASSERT` applies, judged once against the key of a
				// record being created.
				let schema = record(write)?;
				if schema.creating && fd.assert.is_some() {
					let key = schema.rid.key.clone().into_value();
					for (path, _) in key.walk(&fd.name.0[1..]) {
						let path = Idiom([&fd.name.0[..1], path.0.as_slice()].concat());
						schema.deferred.push(Deferred {
							index: *field as usize,
							path,
						});
					}
				}
				Vec::new()
			} else {
				let row = write.row()?;
				let mut places: Vec<(Idiom, Value, bool)> = row
					.current
					.walk(&fd.name)
					.into_iter()
					.map(|(path, val)| (path, val, false))
					.collect();
				if !row.initial.is_none() {
					for (path, old) in row.initial.walk(&fd.name) {
						if !old.is_none() && !places.iter().any(|(p, _, _)| *p == path) {
							places.push((path, Value::None, true));
						}
					}
				}
				places
			};
			// Every clause of the field, and whether its `PERMISSIONS` are
			// enforced, is answered under the field's own authority, in a
			// privileged frame that holds none of the caller's own bindings.
			let frame = field_default_frame(ctx.exec(), fd).map_err(ControlFlow::Err)?;
			let db_ctx =
				frame.database().context("the field kernel requires a database context")?;
			let guarded =
				should_check_perms(db_ctx, Action::Edit).map_err(|e| ControlFlow::Err(e.into()))?;
			let schema = record(write)?;
			schema.field = Some(FieldCursor {
				index: *field as usize,
				frame,
				guarded,
				write: *permissions,
				places: places.into_iter(),
				place: None,
				within_set: false,
			});
			Ok(next)
		}
		SchemaInstr::NextPlace {
			done,
		} => {
			let data = &spec_of(program, state)?.shape.data;
			let content = matches!(data, Some(DataShape::Content));
			let replace = matches!(data, Some(DataShape::Replace));
			loop {
				let write = write_state(state)?;
				let ctx = write.in_flight()?.1;
				// A write that maintains a `MATERIALIZE INTO` list may change its `READONLY`
				// destination, and the rewrite of a renamed enum's labels any `READONLY` field.
				let maintaining = crate::exec::materialize::is_marked(ctx.exec());
				let schema = record(write)?;
				let creating = schema.creating;
				let table = Arc::clone(&schema.table);
				let fields = Arc::clone(&schema.fields);
				let rid = schema.rid.clone();
				let cursor = schema.cursor()?;
				let index = cursor.index;
				let Some((path, val, vanished)) = cursor.places.next() else {
					cursor.place = None;
					return Ok(*done as usize);
				};
				let skipped = schema
					.absent
					.iter()
					.any(|empty| path.len() > empty.len() && path.starts_with(empty));
				let supplied = schema.input.pick(&path);
				let fd = &fields[index];
				let row = write.row()?;
				let old = row.initial.pick(&path);
				// The key the record is stored under, which this stage only
				// checks: the id's kind and default were enforced where it was
				// derived.
				if Some(index) == table.id_field && !creating {
					if val != old {
						return Err(fail(DocError::FieldReadonly {
							field: fd.name.clone(),
							record: rid.to_sql(),
						}));
					}
					continue;
				}
				// A `READONLY` field may not change once the record exists; a
				// place a `CONTENT` clause left empty is the clause restating the
				// record, and is restored. So is the place of a `MATERIALIZE INTO`
				// destination a `REPLACE` left empty: the engine keeps that list,
				// and no statement restates it.
				if fd.readonly && !creating && !maintaining {
					if val == old {
						continue;
					}
					if val.is_none() && (content || replace && ctx.materialized.contains(&index)) {
						restore_place(&mut row.current, &row.initial, &path, old);
						continue;
					}
					return Err(fail(DocError::FieldReadonly {
						field: fd.name.clone(),
						record: rid.to_sql(),
					}));
				}
				let place = Place {
					path,
					val,
					old,
					supplied,
				};
				// A skipped place (beneath an absent optional parent) or a place
				// whose parent container vanished from the post-image does not
				// run `DEFAULT`, `VALUE`, or `TYPE`, which would fabricate a
				// value beneath an absent parent. Its write permission is still
				// enforced: when the caller is denied changing `old` to `NONE`,
				// `old` is restored into the post-image along with its ancestor
				// container.
				if skipped || vanished {
					let schema = record(write)?;
					let mut place = place;
					let allowed = match governing(schema)? {
						None | Some(FieldPermission::Allow) => true,
						Some(FieldPermission::Deny) => false,
						Some(FieldPermission::Predicate(pipeline)) => {
							if skipped || place.val != place.old {
								let value = place.val.clone();
								schema.cursor()?.place = Some(place);
								let allowed = judged(program, pipeline, state, value).await?;
								place = record(write_state(state)?)?
									.cursor()?
									.place
									.take()
									.ok_or_else(|| internal("no place"))?;
								allowed
							} else {
								true
							}
						}
					};
					if !allowed && place.val != place.old {
						let write = write_state(state)?;
						let fields = Arc::clone(&record(write)?.fields);
						let fd = &fields[index];
						let row = write.row()?;
						restore_place(&mut row.current, &row.initial, &place.path, place.old);
						let within_set = row.current.path_crosses_set(&place.path);
						let schema = record(write)?;
						if within_set {
							schema.cursor()?.within_set = true;
						}
						if fd.assert.is_some() {
							schema.deferred.push(Deferred {
								index,
								path: place.path,
							});
						}
					}
					continue;
				}
				record(write)?.cursor()?.place = Some(place);
				return Ok(next);
			}
		}
		SchemaInstr::Default {
			pipeline,
		} => {
			let write = write_state(state)?;
			// The rewrite of a renamed enum's labels derives nothing a field clause derives.
			if crate::exec::vm::schema_host::is_relabel(write.in_flight()?.1.exec()) {
				return Ok(next);
			}
			let schema = record(write)?;
			let creating = schema.creating;
			// A field that declares no `DEFAULT` is defaulted by its static
			// `VALUE`, and an effect that body reaches is the `VALUE` clause's.
			let (always, clause) = match schema.definition()?.default {
				DefineDefault::None => (false, NoWriteFrame::FieldValue),
				DefineDefault::Set(_) => (false, NoWriteFrame::FieldDefault),
				DefineDefault::Always(_) => (true, NoWriteFrame::FieldDefault),
			};
			// A place holding a value keeps it, and a plain `DEFAULT` spares a
			// record that already exists.
			if !schema.place()?.val.is_none() || (!creating && !always) {
				return Ok(next);
			}
			let value = place_clause(program, *pipeline, state, None, clause).await?;
			record(write_state(state)?)?.place()?.val = value;
			Ok(next)
		}
		SchemaInstr::JumpIfPlaceNone {
			target,
		} => {
			// A place the type's `DEFAULT` fills holds a value, which the field's `VALUE` reads
			// as `$value` once it is coerced.
			fill_type_default(state).await?;
			let schema = record(write_state(state)?)?;
			Ok(if schema.place()?.val.is_none() {
				*target as usize
			} else {
				next
			})
		}
		SchemaInstr::Coerce {
			sql_type,
		} => {
			fill_type_default(state).await?;
			let write = write_state(state)?;
			let ctx = write.in_flight()?.1;
			let (exec, table) = (ctx.exec().clone(), Arc::clone(&ctx.table));
			let schema = record(write)?;
			let fields = Arc::clone(&schema.fields);
			let index = schema.cursor()?.index;
			let fd = &fields[index];
			// The `id` field's kind is enforced where the record id is derived.
			if fd.name.is_id() {
				return Ok(next);
			}
			let place = schema.place()?;
			let val = std::mem::take(&mut place.val);
			// A string written to a field whose declared SQL type has a text form of its own is
			// that type's text, read before the kind is enforced.
			let val = match read_sql_text(&ctx, index, val)? {
				Ok(val) => val,
				Err((violation, sql_type)) => {
					return Err(fail(DocError::FieldSqlType {
						record: schema.rid.to_sql(),
						field_name: fd.name.to_sql(),
						sql_type,
						message: violation.message,
					}));
				}
			};
			let coerced = match fd.field_kind.as_ref() {
				None => Ok(val),
				Some(kind) => {
					let named = kind.has_named_type();
					match val.coerce_to_kind(kind) {
						Ok(val) if named => {
							crate::exec::vm::schema_host::check_named_types(
								&exec,
								table.namespace_id,
								table.database_id,
								kind,
								val,
							)
							.await
						}
						other => other,
					}
				}
			};
			match coerced {
				Ok(val) if !*sql_type => place.val = val,
				Ok(val) => match check_sql_type(&ctx, index, val)? {
					Ok(val) => place.val = val,
					// The declared type's reason and text name the refusal; the value is not
					// quoted.
					Err((violation, sql_type)) => {
						return Err(fail(DocError::FieldSqlType {
							record: schema.rid.to_sql(),
							field_name: fd.name.to_sql(),
							sql_type,
							message: violation.message,
						}));
					}
				},
				// The record and field are rendered only for the error.
				Err(e) => {
					let record = schema.rid.to_sql();
					let field_name = fd.name.to_sql();
					let error = if select_value_hidden(state, &exec, &table, &fields, index)? {
						e.redact()
					} else {
						e
					};
					return Err(fail(DocError::FieldCoerce {
						record,
						field_name,
						error: Box::new(error),
					}));
				}
			}
			Ok(next)
		}
		SchemaInstr::Value {
			pipeline,
		} => {
			let write = write_state(state)?;
			if crate::exec::vm::schema_host::is_relabel(write.in_flight()?.1.exec()) {
				return Ok(next);
			}
			let value =
				place_clause(program, *pipeline, state, None, NoWriteFrame::FieldValue).await?;
			record(write_state(state)?)?.place()?.val = value;
			Ok(next)
		}
		SchemaInstr::DeferAssert => {
			let schema = record(write_state(state)?)?;
			let index = schema.cursor()?.index;
			let path = schema.place()?.path.clone();
			schema.deferred.push(Deferred {
				index,
				path,
			});
			Ok(next)
		}
		SchemaInstr::Permit => {
			let schema = record(write_state(state)?)?;
			let allowed = match governing(schema)? {
				None | Some(FieldPermission::Allow) => true,
				Some(FieldPermission::Deny) => false,
				Some(FieldPermission::Predicate(pipeline)) => {
					let value = schema.place()?.val.clone();
					judged(program, pipeline, state, value).await?
				}
			};
			let place = record(write_state(state)?)?.place()?;
			if !allowed && place.val != place.old {
				place.val = place.old.clone();
			}
			Ok(next)
		}
		SchemaInstr::Land => {
			let write = write_state(state)?;
			let schema = record(write)?;
			let index = schema.cursor()?.index;
			let place = schema.cursor()?.place.take().ok_or_else(|| internal("no place"))?;
			if optional_and_absent(&schema.fields[index], &place.val) {
				schema.absent.push(place.path.clone());
			}
			let row = write.row()?;
			let within_set = row.current.path_crosses_set(&place.path);
			// `put` on a `NONE` keeps an array's element positions, which `cut`
			// would shift.
			row.current.put(&place.path, place.val);
			if within_set {
				record(write)?.cursor()?.within_set = true;
			}
			Ok(next)
		}
		SchemaInstr::EndField => {
			let write = write_state(state)?;
			let schema = record(write)?;
			let cursor = schema.field.take().ok_or_else(|| internal("no field open"))?;
			// Only a set holding a value the field landed can have lost its
			// order or the distinctness of its elements.
			if cursor.within_set && !schema.rebuild.contains(&cursor.index) {
				schema.rebuild.push(cursor.index);
			}
			Ok(next)
		}
		SchemaInstr::Asserts {
			pipelines,
		} => {
			let write = write_state(state)?;
			let schema = record(write)?;
			let pending = !schema.rebuild.is_empty();
			let deferred = std::mem::take(&mut schema.deferred);
			// `$value` is each place's finished value, after its `PERMISSIONS`
			// may have reverted what the field produced and after any nested
			// field rewrote within it. It is read before the sets are rebuilt,
			// while every path still names the element its field wrote, and
			// with every set within it in the order it is stored.
			let key = if deferred.iter().any(|place| place.path.is_id_subpath()) {
				schema.rid.key.clone().into_value()
			} else {
				Value::None
			};
			let current = &write.row()?.current;
			let values: Vec<Value> = deferred
				.iter()
				.map(|place| {
					// A place inside `id` is a part of the record-id key.
					if place.path.is_id_subpath() {
						return key.pick(&place.path.0[1..]);
					}
					let mut val = current.pick(&place.path);
					if pending {
						val.normalize_all_sets();
					}
					val
				})
				.collect();
			rebuild_sets(write)?;
			if deferred.is_empty() {
				return Ok(next);
			}
			let schema = record(write_state(state)?)?;
			let fields = Arc::clone(&schema.fields);
			let rid = schema.rid.clone();
			let input = Arc::clone(&schema.input);
			for (
				Deferred {
					index,
					path,
				},
				val,
			) in deferred.into_iter().zip(values)
			{
				let Some(pipeline) = pipelines[index] else {
					continue;
				};
				let fd = &fields[index];
				let write = write_state(state)?;
				let (row, ctx) = write.in_flight()?;
				let exec = ctx.exec();
				let table = Arc::clone(&ctx.table);
				let doc = row.current.clone();
				// An absent optional value is not judged: the field declared
				// that absence acceptable.
				if optional_and_absent(fd, &val) {
					continue;
				}
				let place = Place {
					old: row.initial.pick(&path),
					supplied: input.pick(&path),
					path,
					val: Value::None,
				};
				let frame = field_default_frame(exec, fd).map_err(ControlFlow::Err)?;
				let bound =
					clause_frame(&frame, &place, &val).refusing_writes(NoWriteFrame::FieldAssert);
				let answer = run_image(program, pipeline, doc, Some(bound), state, exec).await?;
				if !answer.is_truthy() {
					let value = if !val.is_none()
						&& select_value_hidden(state, exec, &table, &fields, index)?
					{
						"[REDACTED]".to_owned()
					} else {
						val.to_sql()
					};
					return Err(fail(DocError::FieldValue {
						record: rid.to_sql(),
						field: fd.name.clone(),
						check: fd.assert.as_ref().map(ToSql::to_sql).unwrap_or_default(),
						value,
					}));
				}
			}
			Ok(next)
		}
		SchemaInstr::Finish {
			computed,
		} => {
			let write = write_state(state)?;
			rebuild_sets(write)?;
			write.schema = None;
			let row = write.row()?;
			// A computed value is never stored.
			for name in computed.iter() {
				row.current.cut(name);
			}
			// SECURITY: the write permission gate derives onto its scratch copy
			// only the computed values this flag says the post-image lacks. Every
			// computed value was stripped above, so the flag is cleared.
			row.fields_computed = false;
			// The field values changed, so the reduced image no longer
			// describes the record.
			row.current_reduced = None;
			Ok(next)
		}
		SchemaInstr::Cleanup {
			table,
		} => {
			let write = write_state(state)?;
			let ctx = write.in_flight()?.1;
			let fields = table.definitions(&ctx)?;
			let row = write.row()?;
			if table.schemafull {
				refuse_undefined_fields(&row.current, &fields, &table.table)?;
			}
			// A `NONE` is the absence of a value, and storing one would make the
			// key present on read.
			clean_none(&mut row.current);
			Ok(next)
		}
		SchemaInstr::Derive {
			pipeline,
			computed,
		} => {
			let write = write_state(state)?;
			let (pending, fields) = pending_sets(record(write)?);
			let (row, ctx) = write.in_flight()?;
			// A record with no id is not addressable, so there is no subject to
			// derive a body for.
			if row.id.is_none() {
				return Ok(next);
			}
			let exec = ctx.exec();
			let mut image = row.current.clone();
			if pending.is_empty() {
				let derived = run_image(program, *pipeline, image, None, state, exec).await?;
				write_state(state)?.row()?.current = derived;
				return Ok(next);
			}
			// The record keeps its sets' positions until their rebuild, so the
			// derivation reads a rebuilt copy, and only the computed values
			// it derived are taken back.
			normalise_set_elements(&mut image, pending.iter().map(|&index| &fields[index]));
			let derived = run_image(program, *pipeline, image, None, state, exec).await?;
			let row = write_state(state)?.row()?;
			for name in computed.iter() {
				row.current.put(name, derived.pick(name));
			}
			Ok(next)
		}
		SchemaInstr::Cycle {
			table,
			fields,
		} => Err(fail(DocError::FieldDependencyCycle {
			table: table.as_str().to_string(),
			fields: fields.to_string(),
		})),
		SchemaInstr::Observe {
			pipelines,
		} => {
			let write = write_state(state)?;
			let (row, ctx) = write.in_flight()?;
			// A record with no id is not addressable, so there is no subject to
			// derive a body for.
			if !has_record_observers(&ctx) || row.id.is_none() {
				return Ok(next);
			}
			let exec = ctx.exec();
			let current =
				(!row.current.is_none() && !row.fields_computed).then(|| row.current.clone());
			let initial = (!row.initial.is_none()).then(|| row.initial.clone());
			if let Some(mut image) = current {
				for pipeline in pipelines.iter() {
					image = run_image(program, *pipeline, image, None, state, exec).await?;
				}
				let row = write_state(state)?.row()?;
				row.current = image;
				row.fields_computed = true;
			}
			if let Some(mut image) = initial {
				for pipeline in pipelines.iter() {
					image = run_image(program, *pipeline, image, None, state, exec).await?;
				}
				let row = write_state(state)?.row()?;
				row.initial = image;
				row.initial_fields_computed = true;
			}
			Ok(next)
		}
	}
}
