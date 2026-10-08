//! The host's side of the schema stages run as instructions: ProcessTableFields, CleanupTableFields
//! and MaterialiseObservedFields.
//!
//! The instructions act on the record in flight through the write state; an
//! expression they evaluate is a host pipeline over one of the record's
//! images (see `doc/executor/VM_WRITE_STAGES.md`).
//!
//! The field schema is applied as the field kernel of
//! [`crate::exec::operators::write::stages::schema`] applies it: one field at a
//! time in the table's field order and, within a field, one place at a time.
//! [`SchemaInstr::Field`] opens a field, [`SchemaInstr::NextPlace`] steps to its
//! next place, the clause instructions produce the place's value and
//! [`SchemaInstr::Land`] writes it back. The field definitions are those the
//! catalog held when the statement was planned.

use std::sync::Arc;

use anyhow::Context as _;
use surrealdb_types::ToSql;

use super::data::statement_input;
use super::{State, WriteState, internal, run_image, spec_of};
use crate::catalog::{DefineDefault, FieldDefinition};
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
use crate::iam::Action;
use crate::val::{RecordId, TableName, Value};

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
			&& fields
				.iter()
				.zip(&self.fields)
				.all(|(fd, plan)| fd.name.to_raw_string() == plan.name);
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
	/// are enforced under that frame, whether an absent optional parent skips
	/// it, and every place its path names in the post-image.
	Field {
		field: u16,
		write: FieldWrite,
	},
	/// Step to the open field's next place, or jump to `done` when none is
	/// left. The `id` field of an existing record, and a `READONLY` field of
	/// one, are checked here and never produced. A field a parent skips is not
	/// produced either, but its enforced permission predicate is still judged
	/// and its answer discarded.
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
	/// Coerce the place's value to the field's `TYPE`.
	Coerce,
	/// The field's `VALUE`, evaluated by host pipeline
	/// [`Program::host_code`]`[pipeline]`: its answer is the place's value.
	Value {
		pipeline: u16,
	},
	/// Judge the place's `ASSERT` once every field is produced.
	DeferAssert,
	/// Keep the value the record held when the open field's `PERMISSIONS` are
	/// enforced and refuse the change.
	Permit,
	/// Write the place's value back into the post-image, and note an absent
	/// optional parent, whose children are skipped.
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
	/// deriving the fields after those the ones before it derived.
	Observe {
		pipelines: Arc<[u16]>,
	},
	/// Derive one `COMPUTED` field a later field's clause reads onto the
	/// post-image, by host pipeline [`Program::host_code`]`[pipeline]`, for a
	/// record with an id. The post-image is not marked as carrying its
	/// computed values: only this one is derived.
	Derive {
		pipeline: u16,
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
			| SchemaInstr::Coerce
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
	/// The path of a field left empty under a kind admitting `NONE`, whose
	/// children are not produced.
	skip: Option<Idiom>,
	/// The places whose `ASSERT` is judged once every field is produced, as
	/// the field's index and the path within the record.
	deferred: Vec<(usize, Idiom)>,
	/// The field open.
	field: Option<FieldCursor>,
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
	/// Whether an absent optional parent skips the field.
	skipped: bool,
	/// The places the field's path names, as the post-image held them when
	/// the field was opened.
	places: std::vec::IntoIter<(Idiom, Value)>,
	/// The place stepped to.
	place: Option<Place>,
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

fn write_state(state: &mut State) -> Result<&mut WriteState, ControlFlow> {
	state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))
}

fn record(write: &mut WriteState) -> Result<&mut SchemaRecord, ControlFlow> {
	write.schema.as_deref_mut().ok_or_else(|| internal("a schema step before its record began"))
}

/// Run clause pipeline `pipeline` for the open field's place: over the
/// post-image as it stands, in the field's frame with the place's parameters
/// bound to `value`, or to the place's own value.
async fn place_clause(
	program: &Program,
	pipeline: u16,
	state: &mut State,
	value: Option<Value>,
) -> FlowResult<Value> {
	let write = write_state(state)?;
	let (row, ctx) = write.in_flight()?;
	let exec = ctx.exec().clone();
	let doc = row.current.clone();
	let cursor = record(write)?.cursor()?;
	let place = cursor.place.as_ref().ok_or_else(|| internal("a clause with no place"))?;
	let value = value.unwrap_or_else(|| place.val.clone());
	let bound = clause_frame(&cursor.frame, place, &value);
	run_image(program, pipeline, doc, Some(bound), state, &exec).await
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
	let answer = place_clause(program, pipeline, state, Some(value)).await;
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
				skip: None,
				deferred: Vec::new(),
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
			let places = write.row()?.current.walk(&fd.name);
			// Every clause of the field, and whether its `PERMISSIONS` are
			// enforced, is answered under the field's own authority, in a
			// privileged frame that holds none of the caller's own bindings.
			let frame = field_default_frame(ctx.exec(), fd).map_err(ControlFlow::Err)?;
			let db_ctx =
				frame.database().context("the field kernel requires a database context")?;
			let guarded =
				should_check_perms(db_ctx, Action::Edit).map_err(|e| ControlFlow::Err(e.into()))?;
			let schema = record(write)?;
			let skipped = schema.skip.as_ref().is_some_and(|parent| fd.name.starts_with(parent));
			if !skipped {
				schema.skip = None;
			}
			schema.field = Some(FieldCursor {
				index: *field as usize,
				frame,
				guarded,
				write: *permissions,
				skipped,
				places: places.into_iter(),
				place: None,
			});
			Ok(next)
		}
		SchemaInstr::NextPlace {
			done,
		} => {
			let content = matches!(spec_of(program, state)?.shape.data, Some(DataShape::Content));
			loop {
				let write = write_state(state)?;
				let schema = record(write)?;
				let creating = schema.creating;
				let table = Arc::clone(&schema.table);
				let fields = Arc::clone(&schema.fields);
				let rid = schema.rid.clone();
				let cursor = schema.cursor()?;
				let index = cursor.index;
				let skipped = cursor.skipped;
				let Some((path, val)) = cursor.places.next() else {
					cursor.place = None;
					return Ok(*done as usize);
				};
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
				// record, and is restored.
				if fd.readonly && !creating {
					if val == old {
						continue;
					}
					if val.is_none() && content {
						row.current.put(&path, old);
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
				// A skipped field is neither produced nor written back. Its
				// permission predicate is judged all the same, as the field
				// kernel judges it, and the answer is discarded with the value.
				if skipped {
					let schema = record(write)?;
					if let Some(FieldPermission::Predicate(pipeline)) = governing(schema)? {
						let value = place.val.clone();
						schema.cursor()?.place = Some(place);
						judged(program, pipeline, state, value).await?;
						record(write_state(state)?)?.cursor()?.place = None;
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
			let schema = record(write_state(state)?)?;
			let creating = schema.creating;
			let always = matches!(schema.definition()?.default, DefineDefault::Always(_));
			// A place holding a value keeps it, and a plain `DEFAULT` spares a
			// record that already exists.
			if !schema.place()?.val.is_none() || (!creating && !always) {
				return Ok(next);
			}
			let value = place_clause(program, *pipeline, state, None).await?;
			record(write_state(state)?)?.place()?.val = value;
			Ok(next)
		}
		SchemaInstr::JumpIfPlaceNone {
			target,
		} => {
			let schema = record(write_state(state)?)?;
			Ok(if schema.place()?.val.is_none() {
				*target as usize
			} else {
				next
			})
		}
		SchemaInstr::Coerce => {
			let schema = record(write_state(state)?)?;
			let rid = schema.rid.to_sql();
			let fd = schema.definition()?;
			// The `id` field's kind is enforced where the record id is derived.
			let (Some(kind), false) = (fd.field_kind.clone(), fd.name.is_id()) else {
				return Ok(next);
			};
			let field_name = fd.name.to_sql();
			let place = schema.place()?;
			let val = std::mem::take(&mut place.val);
			place.val = val.coerce_to_kind(&kind).map_err(|e| {
				fail(DocError::FieldCoerce {
					record: rid,
					field_name,
					error: Box::new(e),
				})
			})?;
			Ok(next)
		}
		SchemaInstr::Value {
			pipeline,
		} => {
			let value = place_clause(program, *pipeline, state, None).await?;
			record(write_state(state)?)?.place()?.val = value;
			Ok(next)
		}
		SchemaInstr::DeferAssert => {
			let schema = record(write_state(state)?)?;
			let index = schema.cursor()?.index;
			let path = schema.place()?.path.clone();
			schema.deferred.push((index, path));
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
				schema.skip = Some(schema.fields[index].name.clone());
			}
			// `put` on a `NONE` keeps an array's element positions, which `cut`
			// would shift.
			write.row()?.current.put(&place.path, place.val);
			Ok(next)
		}
		SchemaInstr::EndField => {
			let write = write_state(state)?;
			let schema = record(write)?;
			let cursor = schema.field.take().ok_or_else(|| internal("no field open"))?;
			let fields = Arc::clone(&schema.fields);
			normalise_set_elements(write.row()?, &fields[cursor.index]);
			Ok(next)
		}
		SchemaInstr::Asserts {
			pipelines,
		} => {
			let schema = record(write_state(state)?)?;
			let deferred = std::mem::take(&mut schema.deferred);
			let fields = Arc::clone(&schema.fields);
			let rid = schema.rid.to_sql();
			let input = Arc::clone(&schema.input);
			for (index, path) in deferred {
				let Some(pipeline) = pipelines[index] else {
					continue;
				};
				let fd = &fields[index];
				let write = write_state(state)?;
				let (row, ctx) = write.in_flight()?;
				let exec = ctx.exec().clone();
				let doc = row.current.clone();
				// `$value` is read back from the record: the field's
				// `PERMISSIONS` may have reverted what it produced.
				let val = doc.pick(&path);
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
				let frame = field_default_frame(&exec, fd).map_err(ControlFlow::Err)?;
				let bound = clause_frame(&frame, &place, &val);
				let answer = run_image(program, pipeline, doc, Some(bound), state, &exec).await?;
				if !answer.is_truthy() {
					return Err(fail(DocError::FieldValue {
						record: rid,
						field: fd.name.clone(),
						check: fd.assert.as_ref().map(ToSql::to_sql).unwrap_or_default(),
						value: val.to_sql(),
					}));
				}
			}
			Ok(next)
		}
		SchemaInstr::Finish {
			computed,
		} => {
			let write = write_state(state)?;
			write.schema = None;
			let row = write.row()?;
			// A computed value is never stored.
			for name in computed.iter() {
				row.current.cut(name);
			}
			// SECURITY: the write permission gate derives onto its scratch copy
			// only the computed values this flag says the post-image lacks, and
			// every one was just stripped (GHSA-f6h4-5xf2-86q9).
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
		} => {
			let write = write_state(state)?;
			let (row, ctx) = write.in_flight()?;
			// A record with no id is not addressable, so there is no subject to
			// derive a body for.
			if row.id.is_none() {
				return Ok(next);
			}
			let exec = ctx.exec().clone();
			let image = row.current.clone();
			let derived = run_image(program, *pipeline, image, None, state, &exec).await?;
			write_state(state)?.row()?.current = derived;
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
			let exec = ctx.exec().clone();
			let current =
				(!row.current.is_none() && !row.fields_computed).then(|| row.current.clone());
			let initial = (!row.initial.is_none()).then(|| row.initial.clone());
			if let Some(mut image) = current {
				for pipeline in pipelines.iter() {
					image = run_image(program, *pipeline, image, None, state, &exec).await?;
				}
				let row = write_state(state)?.row()?;
				row.current = image;
				row.fields_computed = true;
			}
			if let Some(mut image) = initial {
				for pipeline in pipelines.iter() {
					image = run_image(program, *pipeline, image, None, state, &exec).await?;
				}
				let row = write_state(state)?.row()?;
				row.initial = image;
				row.initial_fields_computed = true;
			}
			Ok(next)
		}
	}
}
