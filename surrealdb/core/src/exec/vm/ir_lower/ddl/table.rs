//! `DEFINE TABLE`, `REMOVE TABLE` and `ALTER TABLE`.
//!
//! The name of the table is resolved before the authority check, as both statements' bodies
//! resolve it. A table is rewritten under a locked read, so a concurrent rewrite of the same
//! definition is serialised against it.

use super::super::expr::Lowerer;
use super::{
	AlterAttrs, Ddl, DefineAttrs, IdSequence, Instr, IrError, NameClause, Operand, OperandSlot,
	Reg, RouteId, Service, StatementError, StoreInstr, TypeId, Value, stored,
};
use crate::catalog::TableType;
use crate::ddl::define::table::TableConversion;
use crate::exec::ir::iface::statements::{AlterTable, DdlChecks, DefineTable, ViewShape};
use crate::exec::vm::program::{Backfill, ClauseObject, CompareOp, FieldRefusal};
use crate::expr::statements::alter::AlterKind;

/// The view the definition template `template` holds, as the catalog compiles it.
fn view_of(template: &Value) -> Result<crate::catalog::ViewDefinition, IrError> {
	use crate::catalog::FromStored as _;
	use crate::key::KVValue as _;
	let bytes = crate::exec::vm::codec::encode(
		&crate::exec::vm::codec::FULL_CODEC,
		"StoredTableDefinition",
		template,
	)
	.map_err(|_| IrError::Stale("ir_lower::ddl: a table template that does not encode"))?;
	let stored = crate::catalog::StoredTableDefinition::kv_decode_value(&bytes, ())
		.map_err(|_| IrError::Stale("ir_lower::ddl: a table template that does not decode"))?;
	let view = stored
		.view
		.as_ref()
		.ok_or(IrError::Stale("ir_lower::ddl: a view's table template that holds no view"))?;
	crate::catalog::ViewDefinition::from_stored(view)
		.map_err(|_| IrError::Stale("ir_lower::ddl: a view's template that does not compile"))
}

/// The refusal of an `INLINE` cache cap above the hard bound.
fn inline_cap_refusal() -> FieldRefusal {
	FieldRefusal::Thrown(format!(
		"an INLINE cache cap cannot exceed {}",
		crate::idx::inline_cache::MAX_INLINE_CACHE_CAP
	))
}

impl Ddl<'_, '_> {
	/// `DEFINE TABLE`: the name is resolved before the authority check, and an `INLINE` cache cap
	/// above the bound is refused after it. A table that exists keeps its id and graph markers;
	/// the definition is formed from the template with the ids, stamps and markers the program
	/// fills, and the refusals its checks decided are raised where the body raises them. A
	/// relation's `in` and `out` fields are defined with it, a changefeed is recorded, and a cache
	/// built under one cap regime is cleared when the cap changes. A definition that replaces a
	/// view removes the view's registration from every table the view read, whatever the new
	/// definition is; a view is then registered on the tables it reads and filled from them
	/// ([`Ddl::define_view`]).
	pub(super) fn define_table(
		&mut self,
		attrs: &DefineAttrs,
		table: &DefineTable,
	) -> Result<(), IrError> {
		let DdlChecks::DefineTable {
			view_refusal,
			definition_refusal,
			redefined,
			..
		} = &attrs.checks
		else {
			return Err(IrError::Stale("ir_lower::ddl: a DEFINE TABLE planned with no checks"));
		};
		let table_name = self.name(table.name, NameClause::Table)?;
		let name_op = table_name.operand;
		self.refuse_no_write()?;
		self.allow()?;
		// What the body refuses from the statement alone is raised where the body raises it, once
		// the permission check has passed: an `INLINE` cap above the bound before a view the
		// catalog refuses. The binder raises a GraphQL alias that is no name before either.
		if [table.inline_edges_cap, table.inline_refs_cap]
			.into_iter()
			.any(|cap| cap.is_some_and(|cap| cap > crate::idx::inline_cache::MAX_INLINE_CACHE_CAP))
		{
			self.store(StoreInstr::RefuseField {
				refusal: inline_cap_refusal(),
			});
		} else if let Some(refusal) = view_refusal {
			self.store(StoreInstr::RefuseField {
				refusal: refusal.clone(),
			});
		}
		// A table is defined in a database the session reaches: a namespace that does not exist
		// is raised as itself.
		let (ns, db, _database) = self.selected_ids(StatementError::NsNotFound)?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let name_reg = self.load(name_op)?;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", name_op)])?;
		let existing = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: existing,
		});
		// The tables the definition being replaced reads as a view, read before it is replaced.
		let old_sources = self.reg()?;
		self.store(StoreInstr::ViewSources {
			ns,
			db,
			tb: name_reg,
			dst: old_sources,
		});
		let table_id = self.reg()?;
		let [folded, doc_ids, inline_gen, old_edges, old_refs] =
			[self.reg()?, self.reg()?, self.reg()?, self.reg()?, self.reg()?];
		let missing =
			self.exists(&attrs.mode, existing, StatementError::TbAlreadyExists, name_reg)?;
		// A redefinition keeps the table's id and its graph markers.
		self.get_field(existing, "table_id", table_id)?;
		self.get_field(existing, "graph_folded", folded)?;
		self.get_field(existing, "graph_doc_ids", doc_ids)?;
		self.get_field(existing, "graph_inline_gen", inline_gen)?;
		self.get_field(existing, "inline_edges_cap", old_edges)?;
		self.get_field(existing, "inline_refs_cap", old_refs)?;
		let built = self.label()?;
		self.emit(Instr::Jump {
			target: built,
		});
		self.bind(missing)?;
		self.store(StoreInstr::NextId {
			sequence: IdSequence::Table,
			ns: Some(ns),
			db: Some(db),
			dst: table_id,
			draw: 0,
			reserved: None,
		});
		let (no, zero, none) = (
			self.constant(Value::Bool(false))?,
			self.constant(Value::from(0))?,
			self.constant(Value::None)?,
		);
		for (dst, value) in
			[(folded, no), (doc_ids, no), (inline_gen, zero), (old_edges, none), (old_refs, none)]
		{
			self.emit(Instr::Load {
				dst,
				src: value,
			});
		}
		self.bind(built)?;
		let written: crate::catalog::TableDefinition =
			super::definition_of(&attrs.template.value, "StoredTableDefinition")?;
		let names = crate::exec::vm::schema_host::table_named_types(&written);
		if !names.is_empty() {
			let subject = self.shown_subject("The table", table.name)?;
			self.check_types_defined((ns, db), names, || subject);
		}

		let template = self.constant(attrs.template.value.clone())?;
		let definition = self.load(template)?;
		let comment = self.comment(table.comment)?.operand;
		if let Some(refusal) = definition_refusal {
			self.store(StoreInstr::RefuseField {
				refusal: refusal.clone(),
			});
		}
		if let Some(redefined) = redefined {
			self.store(StoreInstr::RefuseRedefiningTable {
				ns,
				db,
				definition: std::sync::Arc::clone(redefined),
			});
		}
		// A relation or a view has no record id components to name: a table whose `id` names them
		// is refused, whether its name is written or evaluated.
		let conversion = match (&table.table_type, &table.view) {
			(TableType::Relation(_), _) => Some(TableConversion::Relation),
			(_, Some(_)) => Some(TableConversion::View),
			_ => None,
		};
		if let Some(to) = conversion {
			let table = self.load(name_op)?;
			self.store(StoreInstr::RefuseNamedIdConversion {
				ns,
				db,
				table,
				to,
			});
		}
		let stamp = self.reg()?;
		self.store(StoreInstr::NewUuid {
			dst: stamp,
			draw: 0,
			reserved: None,
		});
		for (field, value) in [
			("namespace_id", Operand::Reg(ns)),
			("database_id", Operand::Reg(db)),
			("table_id", Operand::Reg(table_id)),
			("name", name_op),
			("comment", comment),
			("cache_fields_ts", Operand::Reg(stamp)),
			("cache_events_ts", Operand::Reg(stamp)),
			("cache_tables_ts", Operand::Reg(stamp)),
			("cache_indexes_ts", Operand::Reg(stamp)),
			("cache_lives_ts", Operand::Reg(stamp)),
			("graph_folded", Operand::Reg(folded)),
			("graph_doc_ids", Operand::Reg(doc_ids)),
			("graph_inline_gen", Operand::Reg(inline_gen)),
		] {
			self.set_field(definition, field, value)?;
		}
		// The definition's remaining refusals are raised here, before the relation's fields, the
		// changefeed and the definition are written: a statement that fails after it wrote refuses
		// the commit of a transaction its caller owns. A migration step judges the change last, as
		// `ALTER TABLE` judges it: the step's refusal lifts once the rewrite job has finished and
		// the others do not, so a caller told to wait for the job is refused for nothing else once
		// it has.
		self.store(StoreInstr::RefuseClauseEffects {
			ns,
			db,
			definition,
			previous: None,
			object: ClauseObject::Table,
		});
		if let Some(view) = &table.view {
			self.store(StoreInstr::RefuseViewCycle {
				ns,
				db,
				tb: name_reg,
				sources: view.what.clone(),
				definition,
			});
		}
		if let Some(redefined) = redefined {
			self.store(StoreInstr::CheckTableChange {
				ns,
				db,
				definition: std::sync::Arc::clone(redefined),
			});
		}

		// A relation's `in` and `out` are field definitions of their own.
		if let TableType::Relation(relation) = &table.table_type {
			for (field, tables) in [("in", &relation.from), ("out", &relation.to)] {
				let field_definition = crate::catalog::FieldDefinition {
					name: crate::expr::Idiom::field(field),
					table: crate::val::TableName::new(
						table_name.written.clone().unwrap_or_default(),
					),
					field_kind: Some(crate::expr::Kind::Record(tables.clone())),
					..Default::default()
				};
				let value = stored(&field_definition.to_stored(), "StoredFieldDefinition")?;
				let value = self.constant(value)?;
				let value = self.load(value)?;
				self.set_name(value, "table", &table_name)?;
				let fd = self.constant(Value::String(field.into()))?;
				let field_key =
					self.object(&[("ns", ns_op), ("db", db_op), ("tb", name_op), ("fd", fd)])?;
				self.store(StoreInstr::Put {
					route: RouteId::named("FieldKey"),
					key: field_key,
					ty: TypeId::named("StoredFieldDefinition"),
					value,
				});
				self.stamp(ns, db, name_op)?;
			}
			let fields_ts = self.reg()?;
			self.store(StoreInstr::NewUuid {
				dst: fields_ts,
				draw: 0,
				reserved: None,
			});
			self.set_field(definition, "cache_fields_ts", Operand::Reg(fields_ts))?;
		}

		// The definition change is recorded, and the database's changefeed fence moves, in the
		// same transaction.
		if table.changefeed.is_some() {
			self.store(StoreInstr::BufferTableChange {
				ns,
				db,
				tb: name_reg,
				definition,
			});
			self.store(StoreInstr::BumpChangefeedFence {
				ns,
				db,
			});
		}

		// The table key is locked before the write, so the write is serialised against a concurrent
		// graph fold that flips the table's marker. The write is followed by its stamp.
		self.store(StoreInstr::Lock {
			route: RouteId::named("TableKey"),
			key,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			value: definition,
		});
		self.stamp(ns, db, name_op)?;
		// The tables the replaced definition read as a view feed it no longer. A view this
		// statement defines registers on the tables it reads afterwards ([`Ddl::define_view`]).
		self.unregister_view((ns, db), name_op, old_sources, false)?;

		// A cache built under one cap regime is not trusted under another.
		for (old, new, route) in [
			(old_edges, table.inline_edges_cap, "EdgeCachePrefix"),
			(old_refs, table.inline_refs_cap, "RefCachePrefix"),
		] {
			let new = self.constant(match new {
				Some(cap) => Value::from(i64::from(cap)),
				None => Value::None,
			})?;
			let same = self.reg()?;
			self.emit(Instr::Compare {
				op: CompareOp::Equal,
				left: Operand::Reg(old),
				right: new,
				dst: same,
			});
			let skip = self.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(same),
				truthy: true,
				target: skip,
			});
			let prefix = self.object(&[("ns", ns_op), ("db", db_op), ("tb", name_op)])?;
			self.store(StoreInstr::DeleteRange {
				route: RouteId::named(route),
				key: prefix,
			});
			self.bind(skip)?;
		}
		self.store(StoreInstr::ClearCache);
		if let Some(view) = &table.view {
			self.define_view(
				view,
				&attrs.template.value,
				(ns, db),
				(name_op, name_reg),
				definition,
				old_sources,
			)?;
			self.store(StoreInstr::ClearCache);
		}
		self.answer_none()
	}

	/// The view part of `DEFINE TABLE`, after the view table's definition `definition` is
	/// written and the registrations of the definition it replaced are removed: every
	/// subscription on the view table is killed, the adjacency its edges left on their endpoints
	/// goes, and the table is emptied of what it held but the registrations of the views that read
	/// it. Its rows go after its other definitions, each through the table's order for a deleted
	/// row, so the views reading it take their removal. Each table the view reads records it as a
	/// foreign table and refreshes its views' stamp; then the view is filled from them
	/// ([`Ddl::backfill_view`]), each row through the order for a created row. `name` is the view
	/// table's name as an operand and in a register. `old_sources` holds the tables the replaced
	/// definition read as a view, none when it was not one: a view redefined as a view keeps the
	/// fields declared on it, and a table that becomes a view keeps none of its old fields.
	fn define_view(
		&mut self,
		view: &ViewShape,
		template: &Value,
		(ns, db): (Reg, Reg),
		(name_op, name_reg): (Operand, Reg),
		definition: Reg,
		old_sources: Reg,
	) -> Result<(), IrError> {
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		self.with_service(Service::Broker, |l| l.kill_table_subscriptions(ns, db, name_reg))?;
		// An edge the table held is also a pointer on each of its endpoints, outside the table's
		// keyspace, so those go while the edges are still there to name them.
		self.store(StoreInstr::PurgeEdgePointers {
			ns,
			db,
			tb: name_reg,
			expunge: false,
		});
		// The records stay, with the layout state they are decoded through, until they are retired
		// through the order compiled from the definitions left.
		self.store(StoreInstr::ClearViewTable {
			ns,
			db,
			tb: name_reg,
			keep_fields: old_sources,
			keep_records: true,
		});
		self.store(StoreInstr::ClearCache);
		self.store(StoreInstr::RetireViewRows {
			table: name_reg,
		});
		self.store(StoreInstr::ClearViewTable {
			ns,
			db,
			tb: name_reg,
			keep_fields: old_sources,
			keep_records: false,
		});
		// The stamp follows the clear of the table's layout state, so no read of that state is
		// served under the catalog version it leaves, and gives the table the generation the view's
		// rows are written in.
		self.stamp(ns, db, name_op)?;
		self.store(StoreInstr::ClearCache);
		for source in view.what.iter() {
			let ft = self.constant(Value::String(source.as_str().into()))?;
			let foreign =
				self.object(&[("ns", ns_op), ("db", db_op), ("tb", ft), ("ft", name_op)])?;
			self.store(StoreInstr::Put {
				route: RouteId::named("ForeignTableKey"),
				key: foreign,
				ty: TypeId::named("StoredTableDefinition"),
				value: definition,
			});
			let ft_key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", ft)])?;
			let ft_name = self.load(ft)?;
			let source_table = self.expect_table(ft_key, ft_name)?;
			self.replace_table((ns, db, ft), ft_key, source_table, "cache_tables_ts")?;
			self.store(StoreInstr::ClearCache);
		}
		self.backfill_view(view, template, name_reg)
	}

	/// Fills the view table whose name is in `name` from the tables the view reads: the backfill
	/// region, run once as a unit of its own, computes the rows (or, for a grouped view, the
	/// groups) the view holds, and each is stored under its key and run through the view table's
	/// own order. A grouped view's fields are evaluated over the document each group's keys and
	/// aggregates make, by a host pipeline per field. A speculative run leaves the backfill to the
	/// statement's real run. A view that holds no rows of its own has no backfill.
	///
	/// The statement changes the catalog the view table's order is read from, so the order is no
	/// unit of the program: the run takes the order of the view table's catalog version when the
	/// backfill meets the table.
	fn backfill_view(
		&mut self,
		view: &ViewShape,
		template: &Value,
		name: Reg,
	) -> Result<(), IrError> {
		use crate::catalog::ViewDefinition;
		use crate::catalog::aggregation::AggregateFields;
		let Some(backfill) = view.backfill else {
			return Ok(());
		};
		let grouped = match view_of(template)? {
			ViewDefinition::Aggregated {
				analysis,
				..
			} => {
				let AggregateFields::Fields(items) = &analysis.fields else {
					return Err(IrError::Invariant {
						what: "ir_lower::ddl: a grouped view that selects a value",
					});
				};
				let values = analysis.backfill_values().map_err(|_| IrError::Invariant {
					what: "ir_lower::ddl: a grouped view with no backfill values",
				})?;
				let paths: Vec<_> = items.iter().map(|(path, _)| path.clone()).collect();
				if paths.len() != view.group_fields.len() {
					return Err(IrError::Stale(
						"ir_lower::ddl: a grouped view whose fields its regions do not match",
					));
				}
				Some((
					Backfill {
						aggregations: analysis.aggregations.clone(),
						values,
						groups: analysis.group_expressions.len(),
					},
					paths,
				))
			}
			ViewDefinition::Materialized {
				..
			} => None,
			ViewDefinition::Select {
				..
			} => {
				return Err(IrError::Stale(
					"ir_lower::ddl: a backfill of a view that holds no rows",
				));
			}
		};
		let speculating = self.label()?;
		self.store(StoreInstr::JumpIfSpeculating {
			target: speculating,
		});
		let region = self.region_slot(backfill)?;
		let mut lowerer = Lowerer::in_context(&mut *self.em, self.ir, &mut *self.cx);
		let rows = lowerer.subquery_region(region)?;
		let needs = lowerer.needs();
		self.needs = self.needs.merged(&needs);
		let rows = match rows {
			Operand::Reg(reg) => reg,
			other @ (Operand::Const(_) | Operand::Field(_)) => self.load(other)?,
		};
		let mut pipelines = Vec::with_capacity(view.group_fields.len());
		for slot in view.group_fields.iter() {
			let region = self.region_slot(*slot)?;
			let mut lowerer = Lowerer::in_context(&mut *self.em, self.ir, &mut *self.cx);
			pipelines.push(lowerer.host_code_over_row(region)?);
			let needs = lowerer.needs();
			self.needs = self.needs.merged(&needs);
		}
		self.each(rows, |l, row| {
			let Some((backfill, paths)) = &grouped else {
				l.store(StoreInstr::BackfillRow {
					table: name,
					row,
				});
				return Ok(());
			};
			let document = l.reg()?;
			let empty = l.label()?;
			l.store(StoreInstr::BackfillGroup {
				backfill: backfill.clone(),
				row,
				dst: document,
				empty,
			});
			let mut fields = Vec::with_capacity(paths.len());
			for (path, pipeline) in paths.iter().zip(&pipelines) {
				let dst = l.reg()?;
				l.store(StoreInstr::EvaluateOver {
					pipeline: *pipeline,
					row: document,
					dst,
				});
				fields.push((path.clone(), dst));
			}
			l.store(StoreInstr::BackfillGroupRow {
				backfill: backfill.clone(),
				table: name,
				row,
				fields,
			});
			l.bind(empty)
		})?;
		self.bind(speculating)
	}

	/// `REMOVE TABLE`: a table a view reads from refuses the removal, and so does a non-empty
	/// relation that uses the table as an endpoint, as the edges stand when the statement runs.
	/// The adjacency each of the table's edges left on its endpoints is purged, the table's indexes
	/// are retired and its subscriptions killed before the definition and its data go; each table a
	/// view of it read is rewritten with its tables stamp refreshed.
	pub(super) fn remove_table(
		&mut self,
		name: OperandSlot,
		if_exists: bool,
		expunge: bool,
	) -> Result<(), IrError> {
		let name = self.name(name, NameClause::Table)?.operand;
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let tb = self.load(name)?;
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", name)])?;
		let table = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: table,
		});
		self.must_exist(if_exists, table, StatementError::TbNotFound, tb)?;
		// The tables a view reads from, as its definition holds them when the statement runs. A
		// view defined by an earlier statement in the same block is included.
		let sources = self.reg()?;
		self.store(StoreInstr::ViewSources {
			ns,
			db,
			tb,
			dst: sources,
		});
		// A view that still reads the table refuses the removal; a registration whose view no
		// longer reads it refuses nothing.
		self.store(StoreInstr::RefuseRemovingViewed {
			ns,
			db,
			tb,
		});
		// The check runs before the table's data is removed. A non-empty relation that uses the
		// table as an endpoint refuses the removal, judged on the edges as they stand when this
		// statement runs.
		self.store(StoreInstr::RefuseRemovingLightweight {
			ns,
			db,
			tb,
		});
		// A table whose field a `MATERIALIZE INTO` clause fills is not removed.
		self.store(StoreInstr::FieldCheck {
			check: crate::exec::vm::program::FieldCheck::TableFilled {
				ns,
				db,
				table: tb,
			},
		});
		// The rows are deleted key by key in this transaction, so the table migration job is held
		// off them before the first write: a page it committed after this transaction's snapshot
		// would fail this transaction's commit. The checks above only read, and a refused removal
		// holds nothing.
		self.store(StoreInstr::HoldTableForRemoval {
			ns,
			db,
			tb,
		});
		// Each edge the table holds is also a pointer on each of its endpoints, outside the
		// table's keyspace, so those go while the edges are still there to name them.
		self.store(StoreInstr::PurgeEdgePointers {
			ns,
			db,
			tb,
			expunge,
		});
		self.retire_table_indexes(ns, db, table, tb, expunge)?;
		self.with_service(Service::Broker, |l| l.kill_table_subscriptions(ns, db, tb))?;
		self.delete("TableKey", key, expunge);
		// The table migration job's progress and requests sit at the root, outside the table's
		// own range. They are discarded through a request of their own rather than deleted, since
		// the job writes the progress and deletes the requests: the job drops them, and a table
		// defined again under the name inherits nothing.
		self.store(StoreInstr::DiscardTableMigration {
			ns,
			db,
			tb,
		});
		self.stamp(ns, db, name)?;
		self.delete_range("TblRoot", key, expunge);
		self.unregister_view((ns, db), name, sources, expunge)?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}

	/// Removes the registration of the view named `name` from each table in the array in
	/// `sources`, the tables its definition read, and refreshes each one's views stamp, so a
	/// write to it no longer maintains the view. Under `EXPUNGE` every earlier version of a
	/// registration goes too. A source that no longer exists holds no registration and is
	/// passed over.
	fn unregister_view(
		&mut self,
		(ns, db): (Reg, Reg),
		name: Operand,
		sources: Reg,
		expunge: bool,
	) -> Result<(), IrError> {
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		self.each(sources, |l, source| {
			let ft = Operand::Reg(source);
			let ft_key = l.object(&[("ns", ns_op), ("db", db_op), ("tb", ft)])?;
			let source_table = l.reg()?;
			l.store(StoreInstr::Get {
				route: RouteId::named("TableKey"),
				key: ft_key,
				ty: TypeId::named("StoredTableDefinition"),
				dst: source_table,
			});
			let gone = l.label()?;
			l.emit(Instr::JumpIf {
				cond: Operand::Reg(source_table),
				truthy: false,
				target: gone,
			});
			let foreign = l.object(&[("ns", ns_op), ("db", db_op), ("tb", ft), ("ft", name)])?;
			l.delete("ForeignTableKey", foreign, expunge);
			l.replace_table((ns, db, ft), ft_key, source_table, "cache_tables_ts")?;
			l.store(StoreInstr::ClearCache);
			l.bind(gone)
		})
	}

	/// `ALTER TABLE`: the clauses the template names replace the table's, which is then validated
	/// against the table as it was. A relation's `in` and `out` fields are formed from the type
	/// the statement sets. A changefeed that replaces another is recorded as the table rewritten,
	/// and a cache built under one cap regime is cleared when the cap changes.
	pub(super) fn alter_table(
		&mut self,
		attrs: &AlterAttrs,
		alter: &AlterTable,
	) -> Result<(), IrError> {
		let template = Self::alter_template(attrs)?;
		let table_name = self.name(alter.name, NameClause::Table)?;
		let tb = table_name.operand;
		let cap_refused = [&alter.inline_edges_cap, &alter.inline_refs_cap].into_iter().any(|cap| {
			matches!(cap, AlterKind::Set(cap) if *cap > crate::idx::inline_cache::MAX_INLINE_CACHE_CAP)
		});
		self.guard()?;
		let (ns, db) = self.session_ids()?;
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb)])?;
		let table = self.reg()?;
		self.store(StoreInstr::Get {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			dst: table,
		});
		let tb_reg = self.load(tb)?;
		self.must_exist(attrs.if_exists, table, StatementError::TbNotFound, tb_reg)?;
		// A migration the table has already run under its name makes the whole statement a
		// no-op.
		if let Some(migrate) = &alter.migrate {
			let ran = self.reg()?;
			self.store(StoreInstr::MigrateTable {
				ns,
				db,
				table: tb_reg,
				migrate: migrate.clone(),
				ran,
			});
			let fresh = self.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(ran),
				truthy: false,
				target: fresh,
			});
			self.answer_none()?;
			self.bind(fresh)?;
		}
		let subject = self.shown_subject("The table", alter.name)?;
		self.check_altered_types(
			template,
			(ns, db),
			crate::exec::vm::schema_host::table_named_types,
			|| subject,
		)?;
		// What the rewrite is compared against, read before it.
		let previous = self.load(Operand::Reg(table))?;
		let (old_changefeed, old_edges, old_refs) = (self.reg()?, self.reg()?, self.reg()?);
		self.get_field(table, "changefeed", old_changefeed)?;
		self.get_field(table, "inline_edges_cap", old_edges)?;
		self.get_field(table, "inline_refs_cap", old_refs)?;
		if cap_refused {
			self.store(StoreInstr::RefuseField {
				refusal: inline_cap_refusal(),
			});
		}
		self.replace_fields(table, template)?;
		let changefeed_set = matches!(alter.changefeed, AlterKind::Set(_));
		// The body validates the table as altered against the table as it was, then forms the
		// relation's `in` and `out` fields. As `DEFINE TABLE` does, it judges a migration step
		// after every other refusal of the change.
		self.store(StoreInstr::CheckTableAlter {
			previous,
			updated: table,
		});
		if let Some(crate::catalog::TableType::Relation(relation)) = &alter.kind {
			let relation_table =
				crate::val::TableName::new(table_name.written.clone().unwrap_or_default());
			for (field, tables) in [("in", &relation.from), ("out", &relation.to)] {
				let definition = crate::catalog::FieldDefinition {
					name: crate::expr::Idiom::field(field),
					table: relation_table.clone(),
					field_kind: Some(crate::expr::Kind::Record(tables.clone())),
					..Default::default()
				};
				let value = stored(&definition.to_stored(), "StoredFieldDefinition")?;
				let value = self.constant(value)?;
				let value = self.load(value)?;
				self.set_name(value, "table", &table_name)?;
				let fd = self.constant(Value::String(field.into()))?;
				let key = self.object(&[("ns", ns_op), ("db", db_op), ("tb", tb), ("fd", fd)])?;
				self.store(StoreInstr::Put {
					route: RouteId::named("FieldKey"),
					key,
					ty: TypeId::named("StoredFieldDefinition"),
					value,
				});
				self.stamp(ns, db, tb)?;
			}
			let fields_ts = self.reg()?;
			self.store(StoreInstr::NewUuid {
				dst: fields_ts,
				draw: 0,
				reserved: None,
			});
			self.set_field(table, "cache_fields_ts", Operand::Reg(fields_ts))?;
		}
		// A changefeed replacing another is recorded, as the table rewritten.
		if changefeed_set {
			let first = self.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(old_changefeed),
				truthy: false,
				target: first,
			});
			self.store(StoreInstr::BufferTableChange {
				ns,
				db,
				tb: tb_reg,
				definition: table,
			});
			self.bind(first)?;
		}
		if alter.compact {
			self.store(StoreInstr::Compact {
				route: RouteId::named("TblRoot"),
				key,
			});
		}
		// The table key is locked before the write, so it is serialised against a concurrent graph
		// fold.
		self.store(StoreInstr::Lock {
			route: RouteId::named("TableKey"),
			key,
		});
		self.store(StoreInstr::Put {
			route: RouteId::named("TableKey"),
			key,
			ty: TypeId::named("StoredTableDefinition"),
			value: table,
		});
		self.stamp(ns, db, tb)?;
		if changefeed_set {
			self.store(StoreInstr::BumpChangefeedFence {
				ns,
				db,
			});
		}
		// A cache built under one cap regime is not trusted under another.
		for (old, field, route) in [
			(old_edges, "inline_edges_cap", "EdgeCachePrefix"),
			(old_refs, "inline_refs_cap", "RefCachePrefix"),
		] {
			let new = self.reg()?;
			self.get_field(table, field, new)?;
			let same = self.reg()?;
			self.emit(Instr::Compare {
				op: CompareOp::Equal,
				left: Operand::Reg(old),
				right: Operand::Reg(new),
				dst: same,
			});
			let skip = self.label()?;
			self.emit(Instr::JumpIf {
				cond: Operand::Reg(same),
				truthy: true,
				target: skip,
			});
			self.store(StoreInstr::DeleteRange {
				route: RouteId::named(route),
				key,
			});
			self.bind(skip)?;
		}
		self.store(StoreInstr::ClearCache);
		self.answer_none()
	}
}
