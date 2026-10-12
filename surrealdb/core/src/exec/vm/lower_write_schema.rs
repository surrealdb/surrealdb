//! The schema stages lowered to instructions: ProcessTableFields, CleanupTableFields and
//! MaterialiseObservedFields.
//!
//! Every schema stage lowers: a clause that cannot be instructions fails the
//! statement, which has no other engine to run on. The table's
//! field definitions are read from the catalog when the statement is planned,
//! for the one table the statement writes to, and each clause a field declares
//! is a host pipeline over the post-image (see
//! [`crate::exec::vm::write::SchemaInstr`]). A clause lowers from its logical
//! form. The host evaluates it under the field's `AUTH LIMIT`
//! ([`crate::exec::permission::field_default_frame`]), so a statement or a call
//! of a user-defined function it holds — each lowered for a principal — is
//! lowered for the caller narrowed by that limit; a closure it holds is made
//! as any closure literal is ([`super::lower_closure`]). A clause with no
//! logical form, or one holding a write no operator compiles, is not lowered.

use std::sync::Arc;

use super::{Lowered, Lowering, NotCovered, lend, needs_caller};
use crate::catalog::{FieldDefinition, LogicalPermissionBody, Permission};
use crate::exec::operators::write::stage::StageId;
use crate::exec::planner::Planner;
use crate::exec::vm::program::{FrameMode, Instr, Label, Operand, ROW, WriteInstr};
use crate::exec::vm::write::{FieldPermission, FieldPlan, FieldWrite, SchemaInstr, SchemaPlan};
use crate::expr::logical::{LogicalExpr, LogicalPermission};

/// Whether `body` holds a statement that holds a `LET`, at any depth.
fn holds_a_let(body: &LogicalExpr) -> bool {
	body.any(|node| match node {
		LogicalExpr::Subquery(op) => op
			.visit_ops_deep(&mut |op| match op {
				crate::expr::logical::LogicalOp::Let(_) => std::ops::ControlFlow::Break(()),
				_ => std::ops::ControlFlow::Continue(()),
			})
			.is_break(),
		_ => false,
	})
}

/// A clause of field `fd` the instructions can evaluate: its logical form,
/// when it has one that holds no write the planner declines.
fn clause(body: Option<anyhow::Result<&LogicalExpr>>) -> Lowered<Option<&LogicalExpr>> {
	let Some(body) = body else {
		return Ok(None);
	};
	let body =
		body.map_err(|e| NotCovered::internal(format!("a field clause has no logical form: {e}")))?;
	lowerable(body)?;
	Ok(Some(body))
}

/// Whether a field's clause `body` lowers, or why it does not.
fn lowerable(body: &LogicalExpr) -> Lowered<()> {
	if let Some(why) = crate::exec::logical::unplannable_write_in_expr(body) {
		return Err(NotCovered::internal(format!(
			"a field clause holds a write no operator runs: {why}"
		)));
	}
	Ok(())
}

/// What a host pipeline of one field answers.
#[derive(Clone, Copy)]
enum FieldCode<'e> {
	/// A `DEFAULT`, `VALUE` or `ASSERT`: the clause's value.
	Clause(&'e LogicalExpr),
	/// A `PERMISSIONS FOR create` or `update` predicate: its value, read with
	/// the reads it makes unchecked, as the host judges it, over the image
	/// with the `COMPUTED` fields it reads derived first.
	Predicate(&'e LogicalExpr, &'e [super::write_gate::Derivation]),
	/// A `COMPUTED` field's body, derived onto the image among the table's
	/// `computed` fields.
	Derive {
		computed: &'e [&'e FieldDefinition],
		fd: &'e FieldDefinition,
		body: &'e LogicalExpr,
	},
}

impl FieldCode<'_> {
	fn expr(&self) -> &LogicalExpr {
		match self {
			Self::Clause(expr)
			| Self::Predicate(expr, _)
			| Self::Derive {
				body: expr,
				..
			} => expr,
		}
	}
}

/// A field's `PERMISSIONS FOR create` or `update` the instructions can judge:
/// `FULL`, `NONE`, or a predicate in logical form.
enum Admits {
	Allow,
	Deny,
	Predicate(LogicalExpr),
}

fn admits(permission: &Permission) -> Lowered<Admits> {
	match permission {
		Permission::Full => Ok(Admits::Allow),
		Permission::None => Ok(Admits::Deny),
		Permission::Specific(_) => {
			let guard = match LogicalPermissionBody::of(permission).logical() {
				Ok(LogicalPermission::Specific(guard)) => guard,
				Ok(_) => {
					return Err(NotCovered::internal("a specific permission has no predicate"));
				}
				Err(e) => {
					return Err(NotCovered::internal(format!(
						"a field permission has no logical form: {e}"
					)));
				}
			};
			lowerable(&guard)?;
			Ok(Admits::Predicate(guard))
		}
	}
}

impl<'ctx> Lowering<'_, 'ctx> {
	/// Emit stage `id` as instructions, recording in `skips` the jumps
	/// taken when it abandons the record.
	pub(super) async fn schema_stage(
		&mut self,
		id: StageId,
		skips: &mut Vec<Label>,
	) -> Lowered<()> {
		// No schema stage abandons a record.
		let _ = skips;
		let (plan, fields) = crate::exec::vm::host::out_of_line(|| self.schema_table()).await?;
		match id {
			StageId::ProcessTableFields => {
				if crate::exec::vm::host::out_of_line(|| self.write_shape()).await.is_none() {
					return Err(self.place_refusal());
				}
				crate::exec::vm::host::out_of_line(|| self.process_table_fields(plan, &fields))
					.await
			}
			StageId::CleanupTableFields => {
				self.emit(Instr::Write(WriteInstr::Schema(SchemaInstr::Cleanup {
					table: plan,
				})));
				Ok(())
			}
			StageId::MaterialiseObservedFields => {
				crate::exec::vm::host::out_of_line(|| self.materialise_observed_fields(&fields))
					.await
			}
			other => Err(NotCovered::internal(format!("{} is not a schema stage", other.name()))),
		}
	}

	/// The field schema of the one table the statement writes to, as its
	/// plan read the catalog: the plan the instructions carry and the
	/// definitions it was made from. A table the catalog does not hold is one
	/// the statement defines on demand, schemaless and with no fields.
	async fn schema_table(&mut self) -> Lowered<(Arc<SchemaPlan>, Arc<[FieldDefinition]>)> {
		let Some(table) = self.write_table.clone() else {
			return Err(NotCovered::internal("a schema stage with no table to read the fields of"));
		};
		let Some(txn) = self.planner.txn().cloned() else {
			return Err(NotCovered::internal(
				"the fields of a table were read with no transaction",
			));
		};
		let (fields, schemafull): (Arc<[FieldDefinition]>, bool) =
			match self.catalog.table(&txn, &table)? {
				Some(read) => match &read.definition {
					Some(definition) => (Arc::clone(&read.fields), definition.schemafull),
					None => (Arc::from([]), false),
				},
				None => (Arc::from([]), false),
			};
		let plan = SchemaPlan {
			table,
			schemafull,
			id_field: crate::doc::id_field_index(&fields),
			fields: fields
				.iter()
				.map(|fd| FieldPlan {
					name: fd.name.to_raw_string(),
				})
				.collect(),
		};
		Ok((Arc::new(plan), fields))
	}

	/// A host pipeline over the post-image answering `code`, a clause of field
	/// `fd`, evaluated in the frame the host binds for the field's place.
	///
	/// The host binds that frame under the field's `AUTH LIMIT`. A clause
	/// holding a statement or a call of a user-defined function, each lowered
	/// for a principal, is therefore lowered with a planner whose principal is
	/// the caller's narrowed by that limit, in a lowering of its own that
	/// shares this one's tables; any other clause is lowered here.
	async fn field_pipeline(&mut self, fd: &FieldDefinition, code: FieldCode<'_>) -> Lowered<u16> {
		match self.narrowed_planner(fd, code.expr())? {
			Some(planner) => {
				crate::exec::vm::host::out_of_line(|| self.field_code_in(&planner, code)).await
			}
			None => crate::exec::vm::host::out_of_line(|| self.field_code(code)).await,
		}
	}

	/// The planner a clause of field `fd` holding `body` is lowered with where
	/// it is not this lowering's: this one's, planning a stored clause, when
	/// `body` holds a statement that holds a `LET`
	/// ([`Planner::is_stored_clause`]), and under the caller's principal
	/// narrowed by the field's `AUTH LIMIT` when that limit narrows it and
	/// `body` holds a statement or a call of a user-defined function.
	fn narrowed_planner(
		&self,
		fd: &FieldDefinition,
		body: &LogicalExpr,
	) -> Lowered<Option<Planner<'ctx>>> {
		if !needs_caller(body) {
			return Ok(None);
		}
		let limit = crate::exec::operators::scan::pipeline::narrowing_auth_limit(&fd.auth_limit)
			.map_err(|e| {
				NotCovered::internal(format!("a field's AUTH LIMIT did not convert: {e}"))
			})?;
		let Some(limit) = limit else {
			return Ok(holds_a_let(body)
				.then(|| self.planner.shadowing(std::iter::empty()).in_stored_clause()));
		};
		let Some(caller) = self.planner.auth.as_ref() else {
			return Err(NotCovered::internal(
				"a field clause under an AUTH LIMIT lowered for no principal",
			));
		};
		let narrowed = Arc::new(caller.new_limited(&limit));
		Ok(Some(self.planner.shadowing(std::iter::empty()).with_auth(narrowed).in_stored_clause()))
	}

	/// [`Self::field_code`], lowered with `planner` in a lowering of its own
	/// that shares this one's tables.
	async fn field_code_in(
		&mut self,
		planner: &Planner<'ctx>,
		code: FieldCode<'_>,
	) -> Lowered<u16> {
		let mut child = Lowering::new(planner);
		child.catalog = Arc::clone(&self.catalog);
		lend(self, &mut child);
		child.ahead = self.ahead;
		child.bundle = self.bundle;
		child.versioned = self.versioned;
		child.write_table = self.write_table.clone();
		let built = Box::pin(child.field_code(code)).await;
		let context = child.delegate_context;
		let frame_reads = child.frame_reads;
		lend(self, &mut child);
		self.delegate_context = self.delegate_context.max(context);
		self.frame_reads += frame_reads;
		built
	}

	/// A host pipeline answering `code`, lowered here. A derivation's planner
	/// carries the principal its field's `AUTH LIMIT` narrows the caller to
	/// when `code` reached here through [`Self::field_code_in`], which is the
	/// one route a derivation that needs it takes.
	async fn field_code(&mut self, code: FieldCode<'_>) -> Lowered<u16> {
		let aside = self.set_aside();
		// A field's clause is definer code, run in the field's privileged
		// frame, which holds none of the caller's bindings.
		let outer_privileged = std::mem::replace(&mut self.privileged, true);
		// The image the pipeline runs over is the clause's document root.
		self.document = super::DocumentRoot::RowElseReceiver;
		let built = match code {
			FieldCode::Clause(body) => {
				crate::exec::vm::host::out_of_line(|| self.field_clause_body(body)).await
			}
			FieldCode::Derive {
				computed,
				fd,
				body,
			} => self.derivation_body(computed, &[(fd, body)], true).await,
			FieldCode::Predicate(guard, derivations) => {
				// The host judges the predicate with its reads unchecked.
				let outer = std::mem::replace(&mut self.unchecked_reads, true);
				let built =
					crate::exec::vm::host::out_of_line(|| self.predicate_body(guard, derivations))
						.await;
				self.unchecked_reads = outer;
				built
			}
		};
		self.privileged = outer_privileged;
		self.restore(aside, built)
	}

	async fn field_clause_body(&mut self, body: &LogicalExpr) -> Lowered<()> {
		// A `RETURN` inside the clause is the clause's value.
		let _held = crate::exec::vm::host::out_of_line(|| self.hold_write_table(body)).await?;
		let value = crate::exec::vm::host::out_of_line(|| self.caught_value(body)).await?;
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	fn schema(&mut self, instr: SchemaInstr) -> Label {
		self.emit(Instr::Write(WriteInstr::Schema(instr)))
	}

	/// Emit `ProcessTableFields`: every field in the table's field order, each
	/// place's value produced by the field's clauses — a computed field another
	/// field's clause reads derived at its place in the order — then the
	/// deferred `ASSERT`s, then the `COMPUTED` values stripped. A table whose
	/// producing clauses read each other in a cycle refuses every record.
	async fn process_table_fields(
		&mut self,
		table: Arc<SchemaPlan>,
		fields: &[FieldDefinition],
	) -> Lowered<()> {
		let order = crate::doc::field_eval_order(fields);
		if let Some(cycle) = &order.cycle {
			// An import applies no clause, so it has no order to lack.
			let import = self.emit(Instr::Write(WriteInstr::JumpIfImport {
				target: 0,
			}));
			self.schema(SchemaInstr::Cycle {
				table: table.table.clone(),
				fields: Arc::from(cycle.join(", ")),
			});
			let here = self.here();
			self.patch(import, here);
			return Ok(());
		}
		// Every clause is compiled before anything is emitted, so a table the
		// instructions cannot apply declines the statement with the body as it
		// was.
		struct Lowerable<'f> {
			index: usize,
			default: Option<&'f LogicalExpr>,
			value: Option<&'f LogicalExpr>,
			assert: Option<&'f LogicalExpr>,
			create: Admits,
			update: Admits,
		}
		enum Step<'f> {
			Field(Lowerable<'f>),
			Derive(&'f FieldDefinition, &'f LogicalExpr),
		}
		let mut steps = Vec::new();
		for &index in &order.order {
			let fd = &fields[index];
			if fd.computed.is_some() {
				// One that another field's clause reads is derived here; the
				// rest are left to the read side.
				if order.required_computed.contains(&index) {
					let Some(body) = clause(fd.computed_logical())? else {
						return Err(NotCovered::internal("a computed field has no body"));
					};
					steps.push(Step::Derive(fd, body));
				}
				continue;
			}
			let create = admits(&fd.create_permission)?;
			let update = admits(&fd.update_permission)?;
			// A field declaring no `DEFAULT` takes a static `VALUE` as one.
			let default = if fd.default.expression().is_some() {
				fd.default_logical()
			} else {
				match (fd.value.as_ref(), fd.value_body.as_ref()) {
					(Some(expr), Some(body)) if expr.is_static() => Some(body.logical()),
					_ => None,
				}
			};
			let default = clause(default)?;
			let value = clause(fd.value_body.as_ref().map(|body| body.logical()))?;
			let assert = clause(fd.assert_body.as_ref().map(|body| body.logical()))?;
			steps.push(Step::Field(Lowerable {
				index,
				default,
				value,
				assert,
				create,
				update,
			}));
		}
		let computed: Vec<&FieldDefinition> =
			fields.iter().filter(|fd| fd.computed.is_some()).collect();
		enum Compiled {
			Field {
				index: usize,
				default: Option<u16>,
				value: Option<u16>,
				asserted: bool,
				write: FieldWrite,
			},
			Derive(u16),
		}
		let mut compiled = Vec::with_capacity(steps.len());
		let mut asserts: Vec<Option<u16>> = vec![None; fields.len()];
		for step in &steps {
			match step {
				Step::Derive(fd, body) => {
					let code = FieldCode::Derive {
						computed: &computed,
						fd,
						body,
					};
					compiled.push(Compiled::Derive(
						crate::exec::vm::host::out_of_line(|| self.field_pipeline(fd, code))
							.await?,
					));
				}
				Step::Field(field) => {
					let fd = &fields[field.index];
					let default = match field.default {
						Some(body) => Some(
							crate::exec::vm::host::out_of_line(|| {
								self.field_pipeline(fd, FieldCode::Clause(body))
							})
							.await?,
						),
						None => None,
					};
					let value = match field.value {
						Some(body) => Some(
							crate::exec::vm::host::out_of_line(|| {
								self.field_pipeline(fd, FieldCode::Clause(body))
							})
							.await?,
						),
						None => None,
					};
					if let Some(body) = field.assert {
						asserts[field.index] = Some(
							crate::exec::vm::host::out_of_line(|| {
								self.field_pipeline(fd, FieldCode::Clause(body))
							})
							.await?,
						);
					}
					let write = FieldWrite {
						create: crate::exec::vm::host::out_of_line(|| {
							self.field_permission(fd, &field.create)
						})
						.await?,
						update: crate::exec::vm::host::out_of_line(|| {
							self.field_permission(fd, &field.update)
						})
						.await?,
					};
					compiled.push(Compiled::Field {
						index: field.index,
						default,
						value,
						asserted: field.assert.is_some(),
						write,
					});
				}
			}
		}
		let index = |index: usize| {
			u16::try_from(index).map_err(|_| NotCovered::internal("more fields than a body names"))
		};
		let begin = self.schema(SchemaInstr::Begin {
			table: Arc::clone(&table),
			end: 0,
		});
		for step in compiled {
			let Compiled::Field {
				index: field,
				default,
				value,
				asserted,
				write,
			} = step
			else {
				let Compiled::Derive(pipeline) = step else {
					unreachable!("a step is a field or a derivation")
				};
				self.schema(SchemaInstr::Derive {
					pipeline,
				});
				continue;
			};
			self.schema(SchemaInstr::Field {
				field: index(field)?,
				write,
			});
			let head = self.here();
			let done = self.schema(SchemaInstr::NextPlace {
				done: 0,
			});
			if let Some(pipeline) = default {
				self.schema(SchemaInstr::Default {
					pipeline,
				});
			}
			match value {
				// A place holding nothing is produced by the `VALUE` and coerced
				// after it; one holding a value is coerced, rewritten, and
				// coerced again.
				Some(pipeline) => {
					let empty = self.schema(SchemaInstr::JumpIfPlaceNone {
						target: 0,
					});
					self.schema(SchemaInstr::Coerce);
					let here = self.here();
					self.patch(empty, here);
					self.schema(SchemaInstr::Value {
						pipeline,
					});
					self.schema(SchemaInstr::Coerce);
				}
				None => {
					self.schema(SchemaInstr::Coerce);
				}
			}
			if asserted {
				self.schema(SchemaInstr::DeferAssert);
			}
			if !write.admits_all() {
				self.schema(SchemaInstr::Permit);
			}
			self.schema(SchemaInstr::Land);
			self.emit(Instr::Jump {
				target: head,
			});
			let here = self.here();
			self.patch(done, here);
			self.schema(SchemaInstr::EndField);
		}
		if asserts.iter().any(Option::is_some) {
			self.schema(SchemaInstr::Asserts {
				pipelines: Arc::from(asserts),
			});
		}
		let computed = computed.iter().map(|fd| fd.name.clone()).collect();
		self.schema(SchemaInstr::Finish {
			computed,
		});
		let here = self.here();
		self.patch(begin, here);
		Ok(())
	}

	/// Field `fd`'s `PERMISSIONS FOR create` or `update` as the instructions
	/// judge it: a predicate as a host pipeline over the post-image answering
	/// its truth, evaluated in the frame the host binds for the field's place,
	/// with the `COMPUTED` fields it reads derived onto the image first.
	async fn field_permission(
		&mut self,
		fd: &FieldDefinition,
		admits: &Admits,
	) -> Lowered<FieldPermission> {
		Ok(match admits {
			Admits::Allow => FieldPermission::Allow,
			Admits::Deny => FieldPermission::Deny,
			Admits::Predicate(guard) => {
				let derivations = match self.write_table.clone() {
					Some(table) => {
						crate::exec::vm::host::out_of_line(|| {
							self.logical_guard_derivations(&table, guard)
						})
						.await?
					}
					None => Vec::new(),
				};
				FieldPermission::Predicate(
					crate::exec::vm::host::out_of_line(|| {
						self.field_pipeline(fd, FieldCode::Predicate(guard, &derivations))
					})
					.await?,
				)
			}
		})
	}

	async fn predicate_body(
		&mut self,
		guard: &LogicalExpr,
		derivations: &[super::write_gate::Derivation],
	) -> Lowered<()> {
		let _held = crate::exec::vm::host::out_of_line(|| self.hold_write_table(guard)).await?;
		// The image carries no `COMPUTED` value of its own, or one the
		// statement assigned to the field's name: the predicate reads each
		// as it computes from the record being written.
		if !derivations.is_empty() {
			crate::exec::vm::host::out_of_line(|| self.derive(derivations)).await?;
		}
		let value = crate::exec::vm::host::out_of_line(|| self.value(guard)).await?;
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: value,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	/// Emit `MaterialiseObservedFields`: every `COMPUTED` field derived onto
	/// both images, in the table's computed dependency order, when the record
	/// has an observer.
	///
	/// The fields that name the components of a record id are `COMPUTED`
	/// definitions of `fields` ([`catalog::KeyStructure::derived_fields`]),
	/// so a table whose `id` names its components derives them here though it
	/// declares no other computed field; only a table with neither lowers
	/// nothing.
	///
	/// [`catalog::KeyStructure::derived_fields`]: crate::catalog::KeyStructure::derived_fields
	async fn materialise_observed_fields(&mut self, fields: &[FieldDefinition]) -> Lowered<()> {
		let computed: Vec<&FieldDefinition> =
			fields.iter().filter(|fd| fd.computed.is_some()).collect();
		if computed.is_empty() {
			return Ok(());
		}
		let mut bodies = Vec::with_capacity(computed.len());
		for fd in &computed {
			let Some(body) = clause(fd.computed_logical())? else {
				return Err(NotCovered::internal("a computed field has no body"));
			};
			bodies.push(body);
		}
		let topo: Vec<(String, Vec<String>)> = computed
			.iter()
			.map(|fd| {
				let expr = fd.computed.as_ref().expect("a computed field");
				(
					fd.name.to_raw_string(),
					crate::expr::computed_deps::extract_computed_deps(expr).fields,
				)
			})
			.collect();
		let derived: Vec<(&FieldDefinition, &LogicalExpr)> =
			crate::expr::computed_deps::topological_sort_computed_fields(&topo)
				.into_iter()
				.map(|index| (computed[index], bodies[index]))
				.collect();
		// The fields are derived in runs, each a pipeline handing the image to
		// the next. A field whose body is lowered for its `AUTH LIMIT`'s
		// principal ([`Self::field_pipeline`]) is a run of its own; the fields
		// between such ones share one.
		let mut pipelines = Vec::new();
		let mut run: Vec<(&FieldDefinition, &LogicalExpr)> = Vec::new();
		for (fd, body) in derived {
			if self.narrowed_planner(fd, body)?.is_none() {
				run.push((fd, body));
				continue;
			}
			if !run.is_empty() {
				pipelines
					.push(self.derivation_pipeline(&computed, &std::mem::take(&mut run)).await?);
			}
			let code = FieldCode::Derive {
				computed: &computed,
				fd,
				body,
			};
			pipelines
				.push(crate::exec::vm::host::out_of_line(|| self.field_pipeline(fd, code)).await?);
		}
		if !run.is_empty() {
			pipelines.push(
				crate::exec::vm::host::out_of_line(|| self.derivation_pipeline(&computed, &run))
					.await?,
			);
		}
		self.schema(SchemaInstr::Observe {
			pipelines: Arc::from(pipelines),
		});
		Ok(())
	}

	/// A host pipeline deriving `derived`, none of them lowered for a
	/// narrowed principal, onto an image.
	async fn derivation_pipeline(
		&mut self,
		computed: &[&FieldDefinition],
		derived: &[(&FieldDefinition, &LogicalExpr)],
	) -> Lowered<u16> {
		let aside = self.set_aside();
		let built =
			crate::exec::vm::host::out_of_line(|| self.derivation_body(computed, derived, false))
				.await;
		self.restore(aside, built)
	}

	/// The body of the pipeline deriving `derived` onto an image, in order:
	/// each body evaluated against the image with the values derived before it
	/// set, in a computed frame under the field's `AUTH LIMIT`, coerced to the
	/// field's kind and landed before the next is evaluated. A self-dereference
	/// reads the image's stored fields when it carries a key under a name of
	/// `computed`, every `COMPUTED` field of the table, and storage otherwise.
	///
	/// A body holding a statement or a call of a user-defined function is
	/// lowered for this lowering's principal, so one under an `AUTH LIMIT`
	/// that narrows the caller lowers only when `narrowed`: the planner already
	/// carries the narrowed principal ([`Self::field_pipeline`]).
	async fn derivation_body(
		&mut self,
		computed: &[&FieldDefinition],
		derived: &[(&FieldDefinition, &LogicalExpr)],
		narrowed: bool,
	) -> Lowered<()> {
		let names = computed
			.iter()
			.map(|fd| surrealdb_strand::Strand::new(fd.name.to_raw_string()))
			.collect();
		self.computed_names.push(names);
		let names = (self.computed_names.len() - 1) as u32;
		let id = self.field("id");
		self.emit(Instr::BeginComputing {
			id,
			doc: None,
			names,
		});
		for &(fd, body) in derived {
			let name = fd.name.to_raw_string();
			let auth = crate::exec::operators::scan::pipeline::narrowing_auth_limit(&fd.auth_limit)
				.map_err(|e| {
					NotCovered::internal(format!("a field's AUTH LIMIT did not convert: {e}"))
				})?;
			if auth.is_some() && needs_caller(body) && !narrowed {
				return Err(NotCovered::internal(
					"a computed field under an AUTH LIMIT holds a statement or a call of a function",
				));
			}
			let frame = self.layer_frame(FrameMode::Computed, &auth);
			let next_reg = self.next_reg;
			let released = self.released.clone();
			let value = self.register()?;
			let outer = self.eval_frame.replace(frame);
			let outer_privileged = std::mem::replace(&mut self.privileged, true);
			// A `RETURN` in the body is the field's value.
			let held = crate::exec::vm::host::out_of_line(|| self.hold_write_table(body)).await;
			let binds = std::mem::replace(&mut self.binds_in_frames, true);
			let src = match held {
				Ok(_held) => crate::exec::vm::host::out_of_line(|| self.caught_value(body)).await,
				Err(e) => Err(e),
			};
			self.binds_in_frames = binds;
			self.eval_frame = outer;
			self.privileged = outer_privileged;
			self.emit(Instr::Load {
				dst: value,
				src: src?,
			});
			let field = self.field(&name);
			if let Some(kind) = &fd.field_kind {
				self.kinds.push(kind.clone());
				self.emit(Instr::CoerceField {
					reg: value,
					kind: (self.kinds.len() - 1) as u32,
					field,
				});
			}
			let sets = self.register()?;
			self.emit(Instr::NewObject {
				dst: sets,
			});
			let cuts = self.register()?;
			self.emit(Instr::NewObject {
				dst: cuts,
			});
			self.paths.push(crate::exec::field_path::FieldPath::field(&name));
			self.emit(Instr::SetField {
				object: sets,
				path: (self.paths.len() - 1) as u32,
				value: Operand::Reg(value),
			});
			self.emit(Instr::LandRow {
				sets,
				cuts,
			});
			self.released = released;
			self.released.extend(next_reg..self.next_reg);
		}
		self.uses_eval = true;
		self.may_fail = true;
		self.emit(Instr::Emit {
			src: ROW,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}
}
