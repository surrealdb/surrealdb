//! The data stages lowered to instructions: the `WHERE` condition, the data
//! clause's evaluation and application, and the merge of an `INSERT` row's own
//! value.
//!
//! Every data stage lowers: a clause that cannot be instructions declines the
//! statement, which fails with an error naming the clause ([`NotCovered`](super::NotCovered)). The
//! condition, the data clause and a `PATCH`'s pointer check read the
//! pre-mutation snapshot (see [`DataInstr::Snapshot`]) of the one table the
//! statement writes to, whose field view must have a layer form. The id's
//! derivation has the `id` field's `DEFAULT` as a host pipeline where the
//! field declares one, and binds nothing at plan time for an authority other
//! than the caller's. The checks and stamps of the record's special fields
//! evaluate no expression.

use surrealdb_types::ToSql as _;

use super::{Lowered, Lowering, NotCovered};
use crate::catalog::KeyOrigin;
use crate::exec::operators::write::stage::StageId;
use crate::exec::vm::program::{
	AssignShape, DataShape, Enforce, InsertFieldSpec, InsertInto, InsertRowsSpec, Instr, Label,
	PlaceStepSpec, Source, StoreInstr, WriteInstr, WriteShape,
};
use crate::exec::vm::write::DataInstr;
use crate::expr::logical::{
	EvalMode, LogicalData, LogicalExpr, LogicalField, LogicalFields, LogicalInsertRows,
	LogicalInserts, LogicalWriteStatement, Schema, lower_expr,
};
use crate::expr::{Expr, Idiom, Part};

impl Lowering<'_, '_> {
	/// Emit stage `id` as instructions, recording in `skips` the jumps
	/// taken when it abandons the record.
	pub(super) async fn data_stage(&mut self, id: StageId, skips: &mut Vec<Label>) -> Lowered<()> {
		let Some(write) = self.write_logical.clone() else {
			return Err(NotCovered::internal("a data stage of a write with no statement"));
		};
		// The clause the statement evaluates per record, and whether it is
		// `ON DUPLICATE KEY UPDATE`, which binds `$input`.
		let (data, binds_input) = match (&write.data, &write.duplicate_update) {
			(Some(data), _) => (Some(data), false),
			(None, Some(update)) => (Some(update), true),
			(None, None) => (None, false),
		};
		#[allow(
			clippy::wildcard_enum_match_arm,
			reason = "only the data stages are offered here, and the rest keep their bodies"
		)]
		match id {
			StageId::CheckWhereCondition => {
				let Some(cond) = &write.cond else {
					self.emit(data_instr(DataInstr::Matched));
					return Ok(());
				};
				let snapshot = crate::exec::vm::host::out_of_line(|| self.snapshot()).await?;
				let pipeline = crate::exec::vm::host::out_of_line(|| {
					self.data_clause_pipeline(std::slice::from_ref(cond), false)
				})
				.await?;
				self.emit(data_instr(snapshot));
				skips.push(self.emit(data_instr(DataInstr::Where {
					pipeline,
					skip: 0,
				})));
				Ok(())
			}
			StageId::ComputeInputData => {
				let Some(data) = data else {
					return Ok(());
				};
				if crate::exec::vm::host::out_of_line(|| self.write_shape()).await.is_none() {
					return Err(self.place_refusal());
				}
				let (values, set): (Vec<LogicalExpr>, bool) = match data {
					LogicalData::Set(assignments) => {
						(assignments.iter().map(|a| a.value.clone()).collect(), true)
					}
					LogicalData::Unset(_) => (Vec::new(), false),
					LogicalData::Patch(expr)
					| LogicalData::Merge(expr)
					| LogicalData::Replace(expr)
					| LogicalData::Content(expr) => (vec![expr.clone()], false),
					LogicalData::Single(_) | LogicalData::Values(_) => {
						return Err(NotCovered::internal("an INSERT's rows are not a data clause"));
					}
				};
				let snapshot = crate::exec::vm::host::out_of_line(|| self.snapshot()).await?;
				let pipeline = if matches!(data, LogicalData::Unset(_)) {
					None
				} else {
					Some(
						crate::exec::vm::host::out_of_line(|| {
							self.data_clause_pipeline(&values, set)
						})
						.await?,
					)
				};
				let evaluated = self.emit(data_instr(DataInstr::JumpIfInput {
					target: 0,
				}));
				self.emit(data_instr(snapshot));
				self.emit(data_instr(DataInstr::ComputeInput {
					pipeline,
					binds_input,
				}));
				let here = self.here();
				self.patch(evaluated, here);
				Ok(())
			}
			StageId::ProcessRecordData => {
				if matches!(data, Some(LogicalData::Patch(_))) {
					let DataInstr::Snapshot {
						view,
						bare,
						checked,
					} = crate::exec::vm::host::out_of_line(|| self.snapshot()).await?
					else {
						return Err(NotCovered::internal(
							"the snapshot of a PATCH is not a snapshot",
						));
					};
					self.emit(data_instr(DataInstr::ApplyPatch {
						view,
						bare,
						checked,
					}));
					return Ok(());
				}
				if crate::exec::vm::host::out_of_line(|| self.write_shape()).await.is_none() {
					return Err(self.place_refusal());
				}
				self.emit(data_instr(DataInstr::ApplyInput));
				Ok(())
			}
			StageId::CheckDataFields => {
				self.emit(data_instr(DataInstr::CheckDataFields));
				Ok(())
			}
			StageId::DefaultRecordData => {
				self.emit(data_instr(DataInstr::StampRecordFields));
				if self.writes_keyed_table()? {
					self.emit(data_instr(DataInstr::SettleKeyFields));
				}
				Ok(())
			}
			StageId::GenerateRecordId => {
				let default = crate::exec::vm::host::out_of_line(|| self.id_default()).await?;
				self.emit(data_instr(DataInstr::GenerateId {
					default,
				}));
				Ok(())
			}
			StageId::ProcessMergeData => {
				self.emit(data_instr(DataInstr::MergeSupplied));
				Ok(())
			}
			other => Err(NotCovered::internal(format!("{} is not a data stage", other.name()))),
		}
	}

	/// The statement's clauses as the instructions of its body read them, or
	/// `None` when a place embeds an expression no host pipeline evaluates.
	/// A statement with no logical form of its own, such as a view's row,
	/// has none.
	pub(super) async fn write_shape(&mut self) -> Option<WriteShape> {
		if let Some(shape) = &self.shape_of {
			return shape.clone();
		}
		let shape = crate::exec::vm::host::out_of_line(|| self.lower_shape()).await;
		self.shape_of = Some(shape.clone());
		shape
	}

	/// [`Self::write_shape`], lowered afresh.
	async fn lower_shape(&mut self) -> Option<WriteShape> {
		let Some(write) = self.write_logical.clone() else {
			return Some(WriteShape::default());
		};
		let data = match &write.data {
			Some(data) => Some(crate::exec::vm::host::out_of_line(|| self.data_shape(data)).await?),
			None => None,
		};
		let duplicate_update = match &write.duplicate_update {
			Some(data) => Some(crate::exec::vm::host::out_of_line(|| self.data_shape(data)).await?),
			None => None,
		};
		Some(WriteShape {
			cond: write.cond.is_some(),
			data,
			duplicate_update,
		})
	}

	/// One data clause's shape, `None` for the forms that are an `INSERT`'s
	/// rows rather than a clause or when a place cannot be addressed.
	async fn data_shape(&mut self, data: &LogicalData) -> Option<DataShape> {
		Some(match data {
			LogicalData::Set(assignments) => {
				let mut shaped = Vec::with_capacity(assignments.len());
				for assignment in assignments {
					shaped.push(AssignShape {
						path: assignment.place.clone(),
						operator: assignment.operator.clone(),
						steps: crate::exec::vm::host::out_of_line(|| {
							self.place_steps(&assignment.place)
						})
						.await?,
					});
				}
				DataShape::Set(shaped)
			}
			LogicalData::Unset(paths) => DataShape::Unset(paths.clone()),
			LogicalData::Content(_) => DataShape::Content,
			LogicalData::Replace(_) => DataShape::Replace,
			LogicalData::Merge(_) => DataShape::Merge,
			LogicalData::Patch(_) => DataShape::Patch,
			LogicalData::Single(_) | LogicalData::Values(_) => return None,
		})
	}

	/// The steps a place is walked by, the expressions its path embeds each a
	/// host pipeline; `None` when an embedded expression has no pipeline. A
	/// part no place addresses is a step that stops the walk.
	pub(super) async fn place_steps(&mut self, path: &Idiom) -> Option<Vec<PlaceStepSpec>> {
		let mut steps = Vec::with_capacity(path.0.len());
		for part in &path.0 {
			steps.push(match part {
				Part::Field(name) => PlaceStepSpec::Field(name.as_str().to_owned()),
				Part::Lookup(lookup) => PlaceStepSpec::Lookup(lookup.to_sql()),
				Part::Value(expr) => PlaceStepSpec::Subscript {
					expr: crate::exec::vm::host::out_of_line(|| self.place_pipeline(expr)).await?,
					literal_index: part.as_old_index().is_some(),
				},
				Part::Where(pred) => PlaceStepSpec::Where(
					crate::exec::vm::host::out_of_line(|| self.place_pipeline(pred)).await?,
				),
				Part::First => PlaceStepSpec::First,
				Part::Last => PlaceStepSpec::Last,
				Part::All => PlaceStepSpec::All,
				Part::Flatten
				| Part::Start(_)
				| Part::Method(_, _)
				| Part::Destructure(_)
				| Part::Optional
				| Part::Recurse(_, _, _)
				| Part::Doc
				| Part::RepeatRecurse => PlaceStepSpec::Unaddressed,
			});
		}
		Some(steps)
	}

	/// The source of an `INSERT`'s rows: every row expression a host scalar,
	/// and every place of a `VALUES` tuple its steps.
	pub(super) async fn insert_source(
		&mut self,
		statement: &LogicalWriteStatement,
		inserts: &LogicalInserts,
	) -> Lowered<Source> {
		let LogicalWriteStatement::Insert(stmt) = statement else {
			return Err(NotCovered::internal(format!(
				"{} does not name its rows as its subjects",
				statement.kind()
			)));
		};
		let into = match &inserts.into {
			Some(into) => Some(InsertInto {
				scalar: crate::exec::vm::host::out_of_line(|| self.host_scalar(into)).await?,
				sql: into.to_sql(),
			}),
			None => None,
		};
		let rows = match &inserts.rows {
			LogicalInsertRows::Tuples(tuples) => {
				let mut compiled = Vec::with_capacity(tuples.len());
				for tuple in tuples {
					let mut fields = Vec::with_capacity(tuple.len());
					for (path, value) in tuple {
						let place = crate::exec::vm::host::out_of_line(|| self.place_steps(path))
							.await
							.ok_or_else(|| self.place_refusal())?;
						fields.push(InsertFieldSpec {
							place,
							value: crate::exec::vm::host::out_of_line(|| self.host_scalar(value))
								.await?,
						});
					}
					compiled.push(fields);
				}
				InsertRowsSpec::Tuples(compiled)
			}
			LogicalInsertRows::Value(expr) => InsertRowsSpec::Value(
				crate::exec::vm::host::out_of_line(|| self.host_scalar(expr)).await?,
			),
		};
		Ok(Source::WriteInserts {
			into,
			rows,
			relation: stmt.relation,
		})
	}

	/// The host pipeline of one expression a place embeds.
	async fn place_pipeline(&mut self, expr: &Expr) -> Option<u16> {
		let lowered = match lower_expr(expr.clone()) {
			Ok(logical) => {
				crate::exec::vm::host::out_of_line(|| {
					self.data_clause_pipeline(std::slice::from_ref(&logical), false)
				})
				.await
			}
			Err(e) => {
				Err(NotCovered::internal(format!("a place's expression with no logical form: {e}")))
			}
		};
		match lowered {
			Ok(pipeline) => Some(pipeline),
			Err(refused) => {
				// A `LET` written as an expression is refused only when the
				// place is evaluated for a record, so its pipeline raises the
				// refusal then, and a place no record reaches raises nothing.
				if let Some(crate::exec::vm::plan::Refusal::InvalidStatement {
					message,
				}) = &refused.refusal
					&& message == crate::exec::vm::map::LET_IN_AN_EXPRESSION
				{
					let refusal = crate::exec::vm::plan::Refusal::InvalidStatement {
						message: message.clone(),
					};
					return match crate::exec::vm::host::out_of_line(|| {
						self.refusing_pipeline(refusal)
					})
					.await
					{
						Ok(pipeline) => Some(pipeline),
						Err(e) => {
							self.place_refused.get_or_insert(e);
							None
						}
					};
				}
				self.place_refused.get_or_insert(refused);
				None
			}
		}
	}

	/// The pipeline of a place whose embedded expression raises `refusal`
	/// when it runs: a single row holding `NONE` after the refusal, which
	/// the refusal never lets it reach.
	async fn refusing_pipeline(&mut self, refusal: crate::exec::vm::plan::Refusal) -> Lowered<u16> {
		let aside = self.set_aside();
		self.rows_are_objects = false;
		self.document = super::DocumentRoot::RowElseReceiver;
		let built = self.refusing_body(refusal);
		self.restore(aside, built)
	}

	/// The body of [`Self::refusing_pipeline`].
	fn refusing_body(&mut self, refusal: crate::exec::vm::plan::Refusal) -> Lowered<()> {
		self.emit(Instr::Store(StoreInstr::Refuse {
			refusal: Box::new(refusal),
		}));
		let out = self.register()?;
		let none = self.constant(crate::val::Value::None);
		self.emit(Instr::Load {
			dst: out,
			src: none,
		});
		self.emit(Instr::Emit {
			src: out,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	/// What a statement a place of which has no steps is answered with: the
	/// refusal or the reason an embedded expression has no pipeline.
	pub(super) fn place_refusal(&self) -> NotCovered {
		self.place_refused
			.clone()
			.unwrap_or_else(|| NotCovered::new("a place of the statement has no steps"))
	}

	/// The instruction deriving the pre-mutation snapshot of a record of the
	/// statement's table. A table whose field view has no layer form has none.
	async fn snapshot(&mut self) -> Lowered<DataInstr> {
		if let Some(snapshot) = &self.snapshot_of {
			return Ok(snapshot.clone());
		}
		let snapshot = crate::exec::vm::host::out_of_line(|| self.lower_snapshot()).await?;
		self.snapshot_of = Some(snapshot.clone());
		Ok(snapshot)
	}

	/// [`Self::snapshot`], lowered afresh.
	async fn lower_snapshot(&mut self) -> Lowered<DataInstr> {
		let Some(table) = self.write_table.clone() else {
			return Err(NotCovered::internal("a snapshot of a write with no table"));
		};
		let Some((layers, checked)) =
			crate::exec::vm::host::out_of_line(|| self.view_layers(&table)).await
		else {
			// A view with no layer form is derived and cut at run time, from
			// the table's field declarations.
			let (view, checked) =
				crate::exec::vm::host::out_of_line(|| self.shown_view(&table)).await?;
			let bare = if checked {
				Some(
					crate::exec::vm::host::out_of_line(|| {
						self.enforced_image(Enforce::Guards(table))
					})
					.await?,
				)
			} else {
				None
			};
			return Ok(DataInstr::Snapshot {
				view,
				bare,
				checked,
			});
		};
		// The table's image view is the snapshot's view.
		let view = match crate::exec::vm::host::out_of_line(|| self.image_view(&table)).await? {
			Some((view, _)) => view,
			None => {
				return Err(NotCovered::internal(
					"the field view of the written table has no layer form",
				));
			}
		};
		// With no pre-image a checked caller's snapshot is cut by the guards
		// on stored fields alone: nothing is derived.
		let bare = if checked {
			let guards: Vec<(LogicalFields, Schema)> = layers
				.iter()
				.filter_map(|(fields, schema)| {
					stored_guards(fields, schema).map(|fields| (fields, schema.clone()))
				})
				.collect();
			crate::exec::vm::host::out_of_line(|| self.view_pipeline(&guards)).await?
		} else {
			None
		};
		Ok(DataInstr::Snapshot {
			view,
			bare,
			checked,
		})
	}

	/// Whether the `id` field of the statement's one table names the components of
	/// its record ids, as the plan read the catalog. Such a table's records are
	/// judged against their keys ([`DataInstr::SettleKeyFields`]); any other pays
	/// no instruction for it. A write with no table, which names no `id` field,
	/// has none.
	fn writes_keyed_table(&self) -> Lowered<bool> {
		let Some(table) = self.write_table.as_ref() else {
			return Ok(false);
		};
		let Some(txn) = self.planner.txn() else {
			return Err(NotCovered::internal("the id field was read with no transaction"));
		};
		Ok(self.catalog.table(txn, table)?.is_some_and(|read| {
			read.key.as_ref().is_some_and(|key| key.origin == KeyOrigin::IdField)
		}))
	}

	/// The host pipeline of the `id` field's `DEFAULT` of the statement's one
	/// table, `None` where the field declares none.
	///
	/// The stage evaluates the `DEFAULT` under the field's `AUTH LIMIT`, and a
	/// statement, a closure or a user-defined function inside it is bound when
	/// this statement is planned, for its caller. So such a `DEFAULT` lowers
	/// only when its limit narrows no caller.
	async fn id_default(&mut self) -> Lowered<Option<u16>> {
		use crate::catalog::DefineDefault;
		let Some(table) = self.write_table.clone() else {
			return Err(NotCovered::internal("an id derived for a write with no table"));
		};
		let Some(txn) = self.planner.txn().cloned() else {
			return Err(NotCovered::internal("the id field was read with no transaction"));
		};
		// A database the catalog does not hold has no table, so no `id` field,
		// and a table the statement defines on demand has no fields yet.
		let Some(read) = self.catalog.table(&txn, &table)? else {
			return Ok(None);
		};
		if read.definition.is_none() {
			return Ok(None);
		}
		let fields = std::sync::Arc::clone(&read.fields);
		let Some(field) = crate::doc::id_field_index(&fields).and_then(|index| fields.get(index))
		else {
			return Ok(None);
		};
		if !matches!(field.default, DefineDefault::Set(_)) {
			return Ok(None);
		}
		let default = match field.default_logical() {
			Some(Ok(default)) => default,
			Some(Err(e)) => {
				return Err(NotCovered::internal(format!(
					"the id's DEFAULT has no logical form: {e}"
				)));
			}
			None => return Ok(None),
		};
		let narrowing =
			crate::exec::operators::scan::pipeline::narrowing_auth_limit(&field.auth_limit)
				.map_err(|e| {
					NotCovered::internal(format!("the id's AUTH LIMIT did not convert: {e}"))
				})?;
		let binds_early = super::holds_statement(default)
			|| default.any(|node| {
				matches!(node, LogicalExpr::Call(call)
					if matches!(call.receiver, crate::expr::Function::Custom(_)))
			});
		if narrowing.is_some() && binds_early {
			return Err(NotCovered::new(
				"the id's DEFAULT under an AUTH LIMIT holds a statement or a call of a function",
			));
		}
		let default = default.clone();
		// The `DEFAULT` is definer code, run in the field's privileged frame,
		// which holds none of the caller's bindings.
		let outer_privileged = std::mem::replace(&mut self.privileged, true);
		let built = crate::exec::vm::host::out_of_line(|| {
			self.data_clause_pipeline(std::slice::from_ref(&default), false)
		})
		.await;
		self.privileged = outer_privileged;
		Ok(Some(built?))
	}

	/// A host pipeline applying `layers` to an image, or `None` for none.
	async fn view_pipeline(&mut self, layers: &[(LogicalFields, Schema)]) -> Lowered<Option<u16>> {
		if layers.is_empty() {
			return Ok(None);
		}
		let aside = self.set_aside();
		let built = crate::exec::vm::host::out_of_line(|| self.view_body(layers)).await;
		self.restore(aside, built).map(Some)
	}

	/// A host pipeline over the snapshot evaluating `exprs` in written order,
	/// answering the one value, or the array of them when `many`.
	async fn data_clause_pipeline(&mut self, exprs: &[LogicalExpr], many: bool) -> Lowered<u16> {
		let aside = self.set_aside();
		// A creating record's snapshot is no record at all.
		self.rows_are_objects = false;
		// The snapshot is the document root an index or a method argument of
		// a path step is evaluated against, as `arr[n % 3]` reads `n` from it.
		self.document = super::DocumentRoot::RowElseReceiver;
		let built = crate::exec::vm::host::out_of_line(|| self.data_clause_body(exprs, many)).await;
		self.restore(aside, built)
	}

	/// The body of [`Self::data_clause_pipeline`].
	async fn data_clause_body(&mut self, exprs: &[LogicalExpr], many: bool) -> Lowered<()> {
		let out = self.register()?;
		if many {
			self.emit(Instr::NewArray {
				dst: out,
			});
		}
		// The values are evaluated in written order against one snapshot: one
		// evaluation region, of which the pipeline's `WHERE` is not a part.
		let roots = exprs
			.iter()
			.map(|expr| crate::expr::logical::dag::Root {
				expr,
				frame: crate::expr::logical::dag::Frame::new(0),
			})
			.collect::<Vec<_>>();
		let scope = self.shared_scope(&roots);
		self.shared.push(scope);
		let evaluated: Lowered<()> = async {
			for expr in exprs {
				let value = crate::exec::vm::host::out_of_line(|| self.projected(expr)).await?;
				if many {
					self.emit(Instr::ArrayPush {
						array: out,
						value,
					});
				} else {
					self.emit(Instr::Load {
						dst: out,
						src: value,
					});
				}
			}
			Ok(())
		}
		.await;
		self.shared.pop();
		evaluated?;
		self.emit(Instr::Emit {
			src: out,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}
}

/// A data instruction, as a write instruction.
fn data_instr(instr: DataInstr) -> Instr {
	Instr::Write(WriteInstr::Data(instr))
}

/// Layer `fields` with only its columns that guard a stored field — a field
/// `input_schema` does not declare `COMPUTED` — or `None` when it has none.
fn stored_guards(fields: &LogicalFields, input_schema: &Schema) -> Option<LogicalFields> {
	let LogicalFields::Select(list) = fields else {
		return None;
	};
	let columns = crate::exec::planner::fold::layer_columns(fields.clone()).ok()?;
	let mut columns = columns.into_iter();
	let computed: Vec<_> = input_schema
		.fields
		.iter()
		.filter(|field| field.computed.is_some())
		.filter_map(|field| field.path.as_field().cloned())
		.collect();
	let mut kept = Vec::with_capacity(list.len());
	let mut guards = false;
	for field in list {
		match field {
			LogicalField::All => kept.push(LogicalField::All),
			LogicalField::Single {
				frame,
				..
			} => {
				let column = columns.next()?;
				if matches!(frame.mode, EvalMode::Permission | EvalMode::MutablePermission)
					&& !computed.contains(&column.name)
				{
					kept.push(field.clone());
					guards = true;
				}
			}
		}
	}
	guards.then_some(LogicalFields::Select(kept))
}
