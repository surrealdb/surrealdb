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
use crate::exec::vm::program::{Backfill, CompareOp, FieldRefusal};
use crate::expr::statements::alter::AlterKind;

/// The view the definition template `template` holds, as the catalog compiles it.
fn view_of(template: &Value) -> Result<crate::catalog::ViewDefinition, IrError> {
	use crate::catalog::FromStored as _;
	use crate::key::KVValue as _;
	let bytes = crate::exec::vm::codec::encode("StoredTableDefinition", template)
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
	/// built under one cap regime is cleared when the cap changes. A view is then filled from the
	/// tables it reads ([`Ddl::define_view`]).
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
			let fence = self.object(&[("ns", ns_op), ("db", db_op)])?;
			self.store(StoreInstr::Increment {
				route: RouteId::named("ChangefeedFenceKey"),
				key: fence,
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
			)?;
			self.store(StoreInstr::ClearCache);
		}
		self.answer_none()
	}

	/// The view part of `DEFINE TABLE`, after the view table's definition `definition` is
	/// written: the view table is emptied of what it held and every subscription on it killed;
	/// each table it reads records the view as a foreign table and refreshes its views' stamp;
	/// then the view is filled from them ([`Ddl::backfill_view`]). `name` is the view table's name
	/// as an operand and in a register.
	fn define_view(
		&mut self,
		view: &ViewShape,
		template: &Value,
		(ns, db): (Reg, Reg),
		(name_op, name_reg): (Operand, Reg),
		definition: Reg,
	) -> Result<(), IrError> {
		let (ns_op, db_op) = (Operand::Reg(ns), Operand::Reg(db));
		self.with_service(Service::Broker, |l| l.kill_table_subscriptions(ns, db, name_reg))?;
		let root = self.object(&[("ns", ns_op), ("db", db_op), ("tb", name_op)])?;
		self.store(StoreInstr::DeleteRange {
			route: RouteId::named("TblRoot"),
			key: root,
		});
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
	/// The table's indexes are retired and its subscriptions killed before the definition and its
	/// data go; each table a view of it read is rewritten with its tables stamp refreshed.
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
		let views = self.scan(
			"ForeignTablePrefix",
			&[("ns", ns_op), ("db", db_op), ("tb", name)],
			"StoredTableDefinition",
		)?;
		let no_views = self.label()?;
		self.emit(Instr::JumpIf {
			cond: Operand::Reg(views),
			truthy: false,
			target: no_views,
		});
		let detail = self.object(&[("table", name), ("views", Operand::Reg(views))])?;
		self.store(StoreInstr::Raise {
			error: StatementError::TbHasViews,
			arg: detail,
		});
		self.bind(no_views)?;
		// The check runs before the table's data is removed. A non-empty relation that uses the
		// table as an endpoint refuses the removal, judged on the edges as they stand when this
		// statement runs.
		self.store(StoreInstr::RefuseRemovingLightweight {
			ns,
			db,
			tb,
		});
		self.retire_table_indexes(ns, db, table, tb, expunge)?;
		self.with_service(Service::Broker, |l| l.kill_table_subscriptions(ns, db, tb))?;
		self.delete("TableKey", key, expunge);
		self.stamp(ns, db, name)?;
		self.delete_range("TblRoot", key, expunge);
		self.each(sources, |l, source| {
			let ft = Operand::Reg(source);
			let foreign = l.object(&[("ns", ns_op), ("db", db_op), ("tb", ft), ("ft", name)])?;
			l.delete("ForeignTableKey", foreign, expunge);
			let ft_key = l.object(&[("ns", ns_op), ("db", db_op), ("tb", ft)])?;
			let ft_name = l.load(ft)?;
			let source_table = l.expect_table(ft_key, ft_name)?;
			l.replace_table((ns, db, ft), ft_key, source_table, "cache_tables_ts")?;
			l.store(StoreInstr::ClearCache);
			Ok(())
		})?;
		self.store(StoreInstr::ClearCache);
		self.answer_none()
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
		// relation's `in` and `out` fields.
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
			let fence = self.object(&[("ns", ns_op), ("db", db_op)])?;
			self.store(StoreInstr::Increment {
				route: RouteId::named("ChangefeedFenceKey"),
				key: fence,
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
