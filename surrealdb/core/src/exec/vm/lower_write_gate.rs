//! The gate stages lowered to instructions: CheckRecordUserAccess, CheckPermissionsQuickCreate, the
//! create, update and delete permission checks, RecheckUpdatePermissions and
//! CheckSelectPermissions.
//!
//! Every gate lowers: a clause that cannot be instructions declines the
//! statement, which fails with an error naming the clause ([`NotCovered`](super::NotCovered)).
//!
//! Each gate reads its clause from the definition of the one table the
//! statement writes to, as the catalog holds it when the statement is planned,
//! and judges it as the gate's body does. A table the catalog does not hold is
//! one the write defines on demand, whose every permission is `NONE`:
//!
//! - a gate runs only for a record with an id, and only for a caller whose writes (a select gate:
//!   reads) are permission-checked, which the host decides per record;
//! - `FULL` admits, `NONE` skips the record, and a predicate is judged against a copy of the image
//!   the gate names, as both the value and the document root, in a gate frame: a read guard refuses
//!   writes, and what the predicate reads is not permission-checked;
//! - a guard that may read a `COMPUTED` field — a write guard, and a select guard on the image the
//!   statement shows — is judged against a copy of the image with the `COMPUTED` fields it reads,
//!   and those they read in turn, derived in definition dependency order whatever the image carries
//!   under their names: each body in the guard's own frame, its reads unchecked and its writes
//!   refused, under its `AUTH LIMIT`, coerced to its kind.
//!
//! A statement a clause holds is a unit lowered for the caller, with the
//! written table held on the planner's cycle guard, so a clause reading its own
//! table cannot plan its own permission again; what the instructions have no
//! form for is compiled by the planner that holds no transaction. A clause with
//! no logical form, and a clause holding a write no operator compiles, are not
//! lowered.

use std::sync::Arc;

use surrealdb_strand::Strand;

use super::{Lowered, Lowering, NotCovered, needs_caller};
use crate::catalog::{Permission, PermissionKind, TableDefinition};
use crate::exec::operators::write::plan::WriteForm;
use crate::exec::operators::write::stage::{RecordImage, StageId};
use crate::exec::vm::program::{FrameMode, Instr, Label, Operand, WriteInstr};
use crate::exec::vm::write::GateInstr;
use crate::expr::logical::{GuardKind, LogicalExpr, LogicalPermission};
use crate::iam::Action;

/// The permissions every table the catalog does not hold starts with.
static NO_ACCESS: Permission = Permission::None;

/// The table a gate judges: its definition, or none where the catalog holds
/// no such table and the write defines it on demand, with every permission
/// `NONE`.
enum GateTable {
	Defined(Arc<TableDefinition>),
	Absent,
}

impl GateTable {
	/// The table's `PERMISSIONS FOR` clause `kind`.
	fn permission(&self, kind: PermissionKind) -> &Permission {
		let Self::Defined(def) = self else {
			return &NO_ACCESS;
		};
		match kind {
			PermissionKind::Select => &def.permissions.select,
			PermissionKind::Create => &def.permissions.create,
			PermissionKind::Update => &def.permissions.update,
			PermissionKind::Delete => &def.permissions.delete,
		}
	}

	/// The logical form of the `PERMISSIONS FOR` clause `kind`, which only a
	/// defined table can hold as a predicate.
	fn logical(&self, kind: PermissionKind) -> anyhow::Result<LogicalPermission> {
		match self {
			Self::Defined(def) => def.permission_logical(kind),
			Self::Absent => Ok(LogicalPermission::None),
		}
	}
}

/// A `COMPUTED` field as a permission predicate derives it onto a copy of the
/// record it judges.
pub(super) struct Derivation {
	name: String,
	body: LogicalExpr,
	kind: Option<crate::expr::Kind>,
	auth: Option<surrealdb_iam::AuthLimit>,
}

impl Lowering<'_, '_> {
	/// Emit stage `id` as instructions, recording in `skips` the jumps
	/// taken when it abandons the record.
	pub(super) async fn gate_stage(&mut self, id: StageId, skips: &mut Vec<Label>) -> Lowered<()> {
		if id == StageId::CheckRecordUserAccess {
			self.emit(gate(GateInstr::Tenant));
			self.target_refusal(skips)?;
			return Ok(());
		}
		let Some(table) = self.write_table.clone() else {
			return Err(NotCovered::internal("a gate with no table to judge"));
		};
		let def = self.gate_table(&table)?;
		#[allow(
			clippy::wildcard_enum_match_arm,
			reason = "`write_stages` offers this module only the gate stages"
		)]
		match id {
			StageId::CheckPermissionsQuickCreate => {
				// Only a `NONE` create permission refuses ahead of the data
				// clause; a predicate is judged after it, against the record
				// it built.
				if matches!(def.permission(PermissionKind::Create), Permission::None) {
					let passed = self.gate_preamble(Action::Edit);
					skips.push(self.emit(Instr::Jump {
						target: 0,
					}));
					self.gate_end(passed);
				}
				Ok(())
			}
			StageId::CheckCreatePermissions => {
				crate::exec::vm::host::out_of_line(|| {
					self.write_gate(&table, &def, PermissionKind::Create, false, skips)
				})
				.await
			}
			StageId::CheckUpdatePermissions => {
				crate::exec::vm::host::out_of_line(|| {
					self.write_gate(&table, &def, PermissionKind::Update, false, skips)
				})
				.await
			}
			StageId::CheckDeletePermissions => {
				crate::exec::vm::host::out_of_line(|| {
					self.write_gate(&table, &def, PermissionKind::Delete, false, skips)
				})
				.await
			}
			StageId::RecheckUpdatePermissions => {
				crate::exec::vm::host::out_of_line(|| {
					self.write_gate(&table, &def, PermissionKind::Update, true, skips)
				})
				.await
			}
			StageId::CheckSelectPermissions(image) => {
				crate::exec::vm::host::out_of_line(|| self.select_gate(&table, &def, image, skips))
					.await
			}
			other => Err(NotCovered::internal(format!("{} is not a gate", other.name()))),
		}
	}

	/// The identifiers of the database the statement runs in, as the plan
	/// read it; none where the catalog holds no such database, so that it
	/// holds none of the tables, indexes, events, views or references the
	/// statement could be gated by, judged or repaired against.
	pub(super) fn database_ids(
		&self,
	) -> Lowered<Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>> {
		if self.planner.txn().is_none() {
			return Ok(None);
		}
		self.catalog.database_ids()
	}

	/// The table `table` as the plan read it. A gate lowered without a
	/// transaction could judge a permission its plan never read.
	fn gate_table(&self, table: &surrealdb_strand::TableName) -> Lowered<GateTable> {
		let Some(txn) = self.planner.txn() else {
			return Err(NotCovered::internal(
				"a gate is lowered with no transaction to read the table by",
			));
		};
		match self.catalog.table(txn, table)?.and_then(|read| read.definition.clone()) {
			Some(def) => Ok(GateTable::Defined(def)),
			None => Ok(GateTable::Absent),
		}
	}

	/// Skip the record, after tenant confinement, where the statement's
	/// target is a defined table whose clause for the statement — `delete`
	/// for `DELETE`, `create` for `CREATE`, `update` for `UPDATE` and
	/// `UPSERT` — is `NONE` and the caller's writes are permission-checked:
	/// such a target yields no record to write, so the statement answers no
	/// rows rather than a refusal. An `UPSERT` is gated by its `update` clause
	/// on its create attempt too, as the document pipeline admits a target
	/// only by the clause its statement kind names. The tenant guard runs
	/// first, so a caller outside its tenant is refused whatever the clause.
	/// A table the catalog does not hold, a clause that is `FULL` or a
	/// predicate, and `INSERT` and `RELATE`, whose subjects are not read from
	/// a target, take no skip.
	fn target_refusal(&mut self, skips: &mut Vec<Label>) -> Lowered<()> {
		let clause = match self.write_form {
			Some(WriteForm::Create) => PermissionKind::Create,
			Some(WriteForm::Update | WriteForm::Upsert) => PermissionKind::Update,
			Some(WriteForm::Delete) => PermissionKind::Delete,
			Some(WriteForm::Relate | WriteForm::Insert | WriteForm::View) | None => {
				return Ok(());
			}
		};
		let Some(table) = self.write_table.clone() else {
			return Ok(());
		};
		let def = self.gate_table(&table)?;
		if matches!(def, GateTable::Absent) || !matches!(def.permission(clause), Permission::None) {
			return Ok(());
		}
		let unchecked = self.emit(gate(GateInstr::JumpIfUnchecked {
			action: Action::Edit,
			target: 0,
		}));
		skips.push(self.emit(Instr::Jump {
			target: 0,
		}));
		let here = self.here();
		self.patch_all(vec![unchecked], here);
		Ok(())
	}

	/// The jumps past a gate: for a record with no id, and for a caller whose
	/// `action`s are not permission-checked.
	fn gate_preamble(&mut self, action: Action) -> [Label; 2] {
		[
			self.emit(gate(GateInstr::JumpIfNoId {
				target: 0,
			})),
			self.emit(gate(GateInstr::JumpIfUnchecked {
				action,
				target: 0,
			})),
		]
	}

	/// Point the jumps past a gate at the instruction after it.
	fn gate_end(&mut self, passed: [Label; 2]) {
		let here = self.here();
		self.patch_all(passed.to_vec(), here);
	}

	/// A table `PERMISSIONS FOR create`, `update` or `delete` clause, judged
	/// against the post-image; with `only_if_predicate`, only when the clause
	/// is a predicate.
	async fn write_gate(
		&mut self,
		table: &surrealdb_strand::TableName,
		def: &GateTable,
		clause: PermissionKind,
		only_if_predicate: bool,
		skips: &mut Vec<Label>,
	) -> Lowered<()> {
		let permission = def.permission(clause);
		// `FULL` and `NONE` were settled by the pre-write gate.
		if only_if_predicate && !matches!(permission, Permission::Specific(_)) {
			return Ok(());
		}
		match permission {
			Permission::Full => Ok(()),
			Permission::None => {
				let passed = self.gate_preamble(Action::Edit);
				skips.push(self.emit(Instr::Jump {
					target: 0,
				}));
				self.gate_end(passed);
				Ok(())
			}
			Permission::Specific(stored) => {
				let guard = gate_guard(def.logical(clause))?;
				let derived = crate::exec::vm::host::out_of_line(|| {
					self.stored_guard_derivations(table, stored)
				})
				.await?;
				let plain = crate::exec::vm::host::out_of_line(|| {
					self.gate_pipeline(&guard, GuardKind::Write, &[])
				})
				.await?;
				let derived = match derived.is_empty() {
					true => None,
					false => Some(
						crate::exec::vm::host::out_of_line(|| {
							self.gate_pipeline(&guard, GuardKind::Write, &derived)
						})
						.await?,
					),
				};
				let passed = self.gate_preamble(Action::Edit);
				skips.push(self.emit(gate(GateInstr::Judge {
					image: RecordImage::Current,
					plain,
					derived,
					skip: 0,
				})));
				self.gate_end(passed);
				Ok(())
			}
		}
	}

	/// The table's `PERMISSIONS FOR select` clause, judged against `image`
	/// as it stands, for a caller whose reads are checked.
	async fn select_gate(
		&mut self,
		table: &surrealdb_strand::TableName,
		def: &GateTable,
		image: RecordImage,
		skips: &mut Vec<Label>,
	) -> Lowered<()> {
		match def.permission(PermissionKind::Select) {
			Permission::Full => Ok(()),
			Permission::None => {
				let passed = self.gate_preamble(Action::View);
				skips.push(self.emit(Instr::Jump {
					target: 0,
				}));
				self.gate_end(passed);
				Ok(())
			}
			Permission::Specific(stored) => {
				let guard = gate_guard(def.logical(PermissionKind::Select))?;
				let derived = crate::exec::vm::host::out_of_line(|| {
					self.stored_guard_derivations(table, stored)
				})
				.await?;
				let plain = crate::exec::vm::host::out_of_line(|| {
					self.gate_pipeline(&guard, GuardKind::Read, &[])
				})
				.await?;
				let derived = match derived.is_empty() {
					true => None,
					false => Some(
						crate::exec::vm::host::out_of_line(|| {
							self.gate_pipeline(&guard, GuardKind::Read, &derived)
						})
						.await?,
					),
				};
				let passed = self.gate_preamble(Action::View);
				skips.push(self.emit(gate(GateInstr::Judge {
					image,
					plain,
					derived,
					skip: 0,
				})));
				self.gate_end(passed);
				Ok(())
			}
		}
	}

	/// The `COMPUTED` fields of `table` stored permission clause `clause`
	/// reads, as [`Self::guard_derivations`] answers them: every one when the
	/// clause reads the record other than by name, and none when it reads no
	/// field of it.
	pub(super) async fn stored_guard_derivations(
		&self,
		table: &surrealdb_strand::TableName,
		clause: &crate::expr::Expr,
	) -> Lowered<Vec<Derivation>> {
		let deps = crate::expr::computed_deps::extract_computed_deps(clause);
		if deps.is_complete && deps.fields.is_empty() {
			return Ok(Vec::new());
		}
		let reads = deps.is_complete.then_some(deps.fields);
		crate::exec::vm::host::out_of_line(|| self.guard_derivations(table, reads.as_deref())).await
	}

	/// The `COMPUTED` fields of `table` logical permission predicate
	/// `predicate` reads, as [`Self::guard_derivations`] answers them: every
	/// one when it reads the record other than by name
	/// ([`reads_row_opaquely`](crate::exec::logical::reads::reads_row_opaquely)),
	/// and otherwise those it reads by name.
	pub(super) async fn logical_guard_derivations(
		&self,
		table: &surrealdb_strand::TableName,
		predicate: &LogicalExpr,
	) -> Lowered<Vec<Derivation>> {
		use crate::exec::logical::reads::{reads_beneath, reads_row_opaquely};
		let Some(txn) = self.planner.txn() else {
			return Err(NotCovered::internal(
				"the fields of a guarded table were read with no transaction",
			));
		};
		let Some(read) = self.catalog.table(txn, table)? else {
			return Ok(Vec::new());
		};
		let computed: Vec<String> = read
			.fields
			.iter()
			.filter(|fd| fd.computed.is_some())
			.map(|fd| fd.name.to_raw_string())
			.collect();
		if computed.is_empty() {
			return Ok(Vec::new());
		}
		if reads_row_opaquely(predicate, self.planner.function_registry()) {
			return crate::exec::vm::host::out_of_line(|| self.guard_derivations(table, None))
				.await;
		}
		let reads: Vec<String> = computed
			.into_iter()
			.filter(|name| {
				reads_beneath(predicate, &[crate::expr::logical::FieldPath::field(name.clone())])
			})
			.collect();
		if reads.is_empty() {
			return Ok(Vec::new());
		}
		crate::exec::vm::host::out_of_line(|| self.guard_derivations(table, Some(&reads))).await
	}

	/// The `COMPUTED` fields of `table` a permission predicate reading the
	/// fields `reads` — every field of the record when `None` — derives, with
	/// the computed fields those read in turn, in the order the table's view
	/// derives them. A field whose body has no logical form is not lowered.
	///
	/// The fields that name the components of a record id are computed
	/// fields of the table, derived from `id`, so a predicate that reads one
	/// derives it here like any other.
	pub(super) async fn guard_derivations(
		&self,
		table: &surrealdb_strand::TableName,
		reads: Option<&[String]>,
	) -> Lowered<Vec<Derivation>> {
		let Some(txn) = self.planner.txn() else {
			return Err(NotCovered::internal(
				"the fields of a gated table were read with no transaction",
			));
		};
		let Some(read) = self.catalog.table(txn, table)? else {
			return Err(NotCovered::internal(
				"the fields of a gated table were read with no database",
			));
		};
		let fields = Arc::clone(&read.fields);
		let mut computed = Vec::new();
		let mut order = Vec::new();
		for fd in fields.iter() {
			let (Some(stored), Some(body)) = (fd.computed.as_ref(), fd.computed_logical()) else {
				continue;
			};
			let body = body
				.map_err(|e| {
					NotCovered::internal(format!("a computed body has no logical form: {e}"))
				})?
				.clone();
			let name = fd.name.to_raw_string();
			let deps = crate::expr::computed_deps::extract_computed_deps(stored);
			order.push((name.clone(), deps.fields));
			computed.push(Derivation {
				name,
				body,
				kind: fd.field_kind.clone(),
				auth: crate::exec::operators::scan::pipeline::narrowing_auth_limit(&fd.auth_limit)
					.map_err(|e| {
						NotCovered::internal(format!("an AUTH LIMIT did not convert: {e}"))
					})?,
			});
		}
		let needed = reads.map(|reads| needed_fields(&order, reads));
		let sorted = crate::expr::computed_deps::topological_sort_computed_fields(&order);
		let mut slots: Vec<Option<Derivation>> = computed.into_iter().map(Some).collect();
		Ok(sorted
			.into_iter()
			.filter_map(|index| slots[index].take())
			.filter(|field| needed.as_ref().is_none_or(|needed| needed.contains(&field.name)))
			.collect())
	}

	/// A host pipeline over a copy of an image that derives `derivations` on
	/// it and answers `guard`, a read guard, over it, in a gate frame that
	/// refuses writes.
	pub(super) async fn read_guard_pipeline(
		&mut self,
		guard: &LogicalExpr,
		derivations: &[Derivation],
	) -> Lowered<u16> {
		crate::exec::vm::host::out_of_line(|| {
			self.gate_pipeline(guard, GuardKind::Read, derivations)
		})
		.await
	}

	/// A host pipeline over a copy of an image that derives `derivations` on
	/// it and answers `guard`'s value over it, in a gate frame for `kind`.
	async fn gate_pipeline(
		&mut self,
		guard: &LogicalExpr,
		kind: GuardKind,
		derivations: &[Derivation],
	) -> Lowered<u16> {
		let frame = self.layer_frame(FrameMode::Gate(kind), &None);
		crate::exec::vm::host::out_of_line(|| self.guard_pipeline(guard, frame, derivations)).await
	}

	/// A host pipeline over a copy of a record that derives `derivations` on
	/// it and answers `guard`'s value over it, in permission frame `frame`.
	pub(super) async fn guard_pipeline(
		&mut self,
		guard: &LogicalExpr,
		frame: u16,
		derivations: &[Derivation],
	) -> Lowered<u16> {
		let _held = crate::exec::vm::host::out_of_line(|| self.hold_write_table(guard)).await?;
		let outer = std::mem::replace(&mut self.binds_in_frames, true);
		let aside = self.set_aside();
		// A gate judges the image it is handed, which need not be a record,
		// and which is the guard's document root.
		self.rows_are_objects = false;
		self.document = super::DocumentRoot::RowElseReceiver;
		let built =
			crate::exec::vm::host::out_of_line(|| self.guard_body(guard, frame, derivations)).await;
		self.binds_in_frames = outer;
		self.restore(aside, built)
	}

	/// The body of [`Self::guard_pipeline`].
	async fn guard_body(
		&mut self,
		guard: &LogicalExpr,
		frame: u16,
		derivations: &[Derivation],
	) -> Lowered<()> {
		if !derivations.is_empty() {
			crate::exec::vm::host::out_of_line(|| self.derive(derivations)).await?;
		}
		// A write's gate takes a `RETURN` leaving the guard as its verdict,
		// as the gate stage judges it; a read's guard has nowhere to send one.
		let returns_value = self
			.permissions
			.get(frame as usize)
			.is_some_and(|spec| matches!(spec.mode, FrameMode::Gate(_)));
		let outer = self.eval_frame.replace(frame);
		let outer_privileged = std::mem::replace(&mut self.privileged, true);
		let verdict =
			crate::exec::vm::host::out_of_line(|| self.bare_value(guard, returns_value)).await;
		self.eval_frame = outer;
		self.privileged = outer_privileged;
		let verdict = verdict?;
		let dst = self.register()?;
		self.emit(Instr::Load {
			dst,
			src: verdict,
		});
		self.emit(Instr::Emit {
			src: dst,
		});
		self.emit(Instr::EndRow);
		Ok(())
	}

	/// Derive `derivations` onto the row in order, each landing before the
	/// next is evaluated, with a self-dereference answered from the row as it
	/// was before any landed. Each body runs in a frame for a permission
	/// predicate ([`FrameMode::Derived`]).
	pub(super) async fn derive(&mut self, derivations: &[Derivation]) -> Lowered<()> {
		let names: Vec<Strand> =
			derivations.iter().map(|field| Strand::new(field.name.as_str())).collect();
		self.computed_names.push(names);
		let names = (self.computed_names.len() - 1) as u32;
		let id = self.field("id");
		self.emit(Instr::BeginComputing {
			id,
			doc: None,
			names,
		});
		for field in derivations {
			let next_reg = self.next_reg;
			let released = self.released.clone();
			let frame = self.layer_frame(FrameMode::Derived, &field.auth);
			let value = self.register()?;
			if field.auth.is_some() && needs_caller(&field.body) {
				// What the body holds is lowered for the principal its
				// `AUTH LIMIT` narrows the caller to, by a pipeline of its own
				// over the row as it stands.
				let pipeline =
					crate::exec::vm::host::out_of_line(|| self.narrowed_derivation(field, frame))
						.await?;
				let row = self.register()?;
				self.emit(Instr::LoadRow {
					dst: row,
				});
				self.emit(Instr::JudgeCopy {
					pipeline,
					src: row,
					dst: value,
				});
				self.uses_eval = true;
			} else {
				let outer = self.eval_frame.replace(frame);
				let outer_privileged = std::mem::replace(&mut self.privileged, true);
				// A `RETURN` in the body is the field's value.
				let src =
					crate::exec::vm::host::out_of_line(|| self.caught(&field.body, true)).await;
				self.eval_frame = outer;
				self.privileged = outer_privileged;
				self.emit(Instr::Load {
					dst: value,
					src: src?,
				});
			}
			let name = self.field(&field.name);
			if let Some(kind) = &field.kind {
				self.kinds.push(kind.clone());
				self.emit(Instr::CoerceField {
					reg: value,
					kind: (self.kinds.len() - 1) as u32,
					field: name,
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
			self.paths.push(crate::exec::field_path::FieldPath::field(field.name.as_str()));
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
		self.may_fail = true;
		Ok(())
	}
}

impl Lowering<'_, '_> {
	/// A host pipeline over a record answering `field`'s value in frame
	/// `frame`, lowered with a planner whose principal is the caller's
	/// narrowed by the field's `AUTH LIMIT`, in a lowering of its own that
	/// shares this one's tables.
	async fn narrowed_derivation(&mut self, field: &Derivation, frame: u16) -> Lowered<u16> {
		let Some(limit) = field.auth.as_ref() else {
			return Err(NotCovered::internal("a narrowed derivation with no AUTH LIMIT"));
		};
		let Some(caller) = self.planner.auth.as_ref() else {
			return Err(NotCovered::internal(
				"a computed field under an AUTH LIMIT derived for no principal",
			));
		};
		let narrowed = Arc::new(caller.new_limited(limit));
		let planner = self.planner.shadowing(std::iter::empty()).with_auth(narrowed);
		let mut child = Lowering::new(&planner);
		child.catalog = Arc::clone(&self.catalog);
		super::lend(self, &mut child);
		child.ahead = self.ahead;
		child.bundle = self.bundle;
		child.versioned = self.versioned;
		child.write_table = self.write_table.clone();
		let built = Box::pin(child.derivation_value(&field.body, frame)).await;
		let context = child.delegate_context;
		let frame_reads = child.frame_reads;
		super::lend(self, &mut child);
		self.delegate_context = self.delegate_context.max(context);
		self.frame_reads += frame_reads;
		built
	}

	/// A host pipeline over a record answering `body`'s value in frame
	/// `frame`, a `RETURN` in it being its value.
	async fn derivation_value(&mut self, body: &LogicalExpr, frame: u16) -> Lowered<u16> {
		let outer = std::mem::replace(&mut self.binds_in_frames, true);
		let outer_privileged = std::mem::replace(&mut self.privileged, true);
		let aside = self.set_aside();
		self.rows_are_objects = false;
		self.document = super::DocumentRoot::RowElseReceiver;
		let built = Box::pin(async {
			self.eval_frame = Some(frame);
			let src = crate::exec::vm::host::out_of_line(|| self.caught(body, true)).await?;
			let dst = self.register()?;
			self.emit(Instr::Load {
				dst,
				src,
			});
			self.emit(Instr::Emit {
				src: dst,
			});
			self.emit(Instr::EndRow);
			self.may_fail = true;
			Ok(())
		})
		.await;
		self.binds_in_frames = outer;
		self.privileged = outer_privileged;
		self.restore(aside, built)
	}
}

/// The names of the `COMPUTED` fields `reads` reaches through `order`, each
/// field's name with the fields its body reads: those of `reads` that are
/// computed, and every computed field one of them reads, transitively.
fn needed_fields(order: &[(String, Vec<String>)], reads: &[String]) -> Vec<String> {
	let mut needed: Vec<String> = Vec::new();
	let mut pending: Vec<&str> = reads.iter().map(String::as_str).collect();
	while let Some(name) = pending.pop() {
		if needed.iter().any(|seen| seen == name) {
			continue;
		}
		if let Some((name, deps)) = order.iter().find(|(field, _)| field == name) {
			needed.push(name.clone());
			pending.extend(deps.iter().map(String::as_str));
		}
	}
	needed
}

/// A gate instruction as the write body holds it.
fn gate(instr: GateInstr) -> Instr {
	Instr::Write(WriteInstr::Permissions(instr))
}

/// The predicate of a clause in logical form. A clause with no logical form,
/// and one that holds a write no operator compiles, are not lowered.
pub(super) fn gate_guard(logical: anyhow::Result<LogicalPermission>) -> Lowered<LogicalExpr> {
	match logical {
		Ok(LogicalPermission::Specific(guard)) => {
			if let Some(why) = crate::exec::logical::unplannable_write_in_expr(&guard) {
				return Err(NotCovered::internal(format!(
					"a permission holds a write no operator runs: {why}"
				)));
			}
			Ok(guard)
		}
		Ok(_) => Err(NotCovered::internal("a specific permission has no predicate")),
		Err(e) => Err(NotCovered::internal(format!("a permission has no logical form: {e}"))),
	}
}
