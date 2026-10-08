//! The catalog a physical plan's lowering takes its definitions from.
//!
//! Mapping reads every catalog object the plan's lowering consults, and the
//! plan carries what it read ([`super::plan::PhysicalPlan::catalog`]): each
//! definition, and for a table the catalog version it was read at. Lowering
//! takes each definition from there and reads nothing of the catalog itself;
//! a build with debug assertions panics on a read it makes
//! ([`crate::kvs::catalog_reads::sealed`]). What lowering takes is noted in the
//! transaction's open catalog-read recordings as the read it stands for, so a
//! unit lowered from it depends on the definitions it was lowered from, as it
//! would had it read them.
//!
//! What a plan reads follows from its statement and the definitions those
//! reach, never from what the database holds besides:
//!
//! - every table its sources, writes, schema statements and graph lookups name, with its fields;
//! - for a table one of its writes names, the table's indexes, events and views, and the same of
//!   each view table those views fill, in turn;
//! - every parameter, user-defined function, model and module its expressions name, and those the
//!   clauses of the definitions above name.
//!
//! Reading a table a plan writes to compiles the units of its live
//! subscriptions too, which the write's stage looks up when it runs
//! ([`super::lower_live::prepare`]).
//!
//! A plan mapped inside another's mapping — a statement nested in an
//! expression, the input of a join — carries no catalog of its own and is
//! lowered with its enclosing plan's. A table only data names is read where
//! its unit is lowered, never through a plan's catalog.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use surrealdb_strand::TableName;
use uuid::Uuid;

use super::NotCovered;
use super::lower::Lowered;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseId, EventDefinition, FieldDefinition, IndexDefinition, NamespaceId, ParamDefinition,
	TableDefinition,
};
use crate::exec::planner::{FunctionLookup, Planner};
use crate::expr::Function;
use crate::expr::logical::{LogicalDdlStatement, LogicalExpr, LogicalOp, LogicalWrite, SourceRef};
use crate::kvs::Transaction;

type Mapped<T> = Result<T, NotCovered>;

/// One table's catalog as a plan read it.
#[derive(Debug, Clone)]
pub(crate) struct TableCatalog {
	pub(crate) table: TableName,
	/// The table's catalog version when it was read; `None` for a table
	/// whose catalog has never been stamped.
	pub(crate) version: Option<Uuid>,
	/// `None` where the catalog holds no such table.
	pub(crate) definition: Option<Arc<TableDefinition>>,
	pub(crate) fields: Arc<[FieldDefinition]>,
	/// What a write to the table runs, for a table the plan writes to.
	pub(crate) written: Option<WrittenTable>,
}

/// What a write to a table runs besides its fields.
#[derive(Debug, Clone)]
pub(crate) struct WrittenTable {
	pub(crate) indexes: Arc<[IndexDefinition]>,
	pub(crate) events: Arc<[EventDefinition]>,
	/// The definitions of the view tables the table's views fill.
	pub(crate) views: Arc<[TableDefinition]>,
	/// The `REFERENCE` fields that can name a record of the table, whose
	/// repairs a delete of one runs; read for a table the plan deletes from.
	pub(crate) referencing: Option<crate::exec::catalog_deps::Referencing>,
}

/// The database a plan runs in, as the catalog held it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum PlanDatabase {
	/// The plan was mapped with no transaction, or named no database to read.
	#[default]
	Unread,
	/// The catalog holds no such database, so none of its objects.
	Absent,
	Ids(NamespaceId, DatabaseId),
}

/// What a plan read of the catalog for its lowering; see the module.
#[derive(Debug, Clone, Default)]
pub(crate) struct PlanCatalog {
	pub(crate) database: PlanDatabase,
	tables: BTreeMap<TableName, Arc<TableCatalog>>,
	params: BTreeMap<String, Option<Arc<ParamDefinition>>>,
	/// Each function as the planner found it, with whether a call of it may
	/// write and whether calls of it keep row order
	/// ([`Planner::resolve_custom_function`]).
	functions: BTreeMap<String, (FunctionLookup, crate::exec::planner::ResolvedCustomFunction)>,
	#[cfg_attr(not(feature = "ml"), allow(dead_code))]
	models: BTreeMap<(String, String), Option<Arc<crate::catalog::MlModelDefinition>>>,
	#[cfg_attr(not(feature = "surrealism"), allow(dead_code))]
	modules: BTreeMap<String, Option<Arc<crate::catalog::ModuleDefinition>>>,
	/// The `REFERENCE` fields that can name a record of any table, for the
	/// stage unit that stands for every table the catalog does not hold.
	any_referencing: Option<crate::exec::catalog_deps::Referencing>,
}

/// Two catalogs are one where they read the same objects, each table at the
/// same catalog version.
impl PartialEq for PlanCatalog {
	fn eq(&self, other: &Self) -> bool {
		self.database == other.database
			&& self.tables.len() == other.tables.len()
			&& self.tables.iter().zip(other.tables.iter()).all(|((a, ta), (b, tb))| {
				a == b && ta.version == tb.version && ta.written.is_some() == tb.written.is_some()
			}) && self.params.keys().eq(other.params.keys())
			&& self.functions.keys().eq(other.functions.keys())
			&& self.models.keys().eq(other.models.keys())
			&& self.modules.keys().eq(other.modules.keys())
	}
}

impl PlanCatalog {
	/// The tables the plan read, each with the catalog version it was read
	/// at.
	pub(crate) fn tables(&self) -> impl Iterator<Item = &Arc<TableCatalog>> {
		self.tables.values()
	}

	/// The ids of the database the plan runs in; `None` where the catalog
	/// holds no such database. A plan that read no database is not lowered
	/// against one.
	pub(crate) fn database_ids(&self) -> Lowered<Option<(NamespaceId, DatabaseId)>> {
		match self.database {
			PlanDatabase::Ids(ns, db) => Ok(Some((ns, db))),
			PlanDatabase::Absent => Ok(None),
			PlanDatabase::Unread => Err(NotCovered::internal(
				"a lowering consulted the catalog of a plan that read none",
			)),
		}
	}

	/// The catalog of `table`; `None` where the database is absent. The
	/// read it stands for is noted in `txn`'s open recordings.
	pub(crate) fn table(
		&self,
		txn: &Transaction,
		table: &TableName,
	) -> Lowered<Option<&Arc<TableCatalog>>> {
		let Some((ns, db)) = self.database_ids()? else {
			return Ok(None);
		};
		let read = self.tables.get(table).ok_or_else(|| {
			NotCovered::internal(format!(
				"a lowering reached the table {table}, unread by its plan"
			))
		})?;
		txn.note_table_read(ns, db, table);
		Ok(Some(read))
	}

	/// [`Self::table`] for a table the plan writes to, with what a write to
	/// it runs.
	pub(crate) fn written(
		&self,
		txn: &Transaction,
		table: &TableName,
	) -> Lowered<Option<(&Arc<TableCatalog>, &WrittenTable)>> {
		let Some(read) = self.table(txn, table)? else {
			return Ok(None);
		};
		let written = read.written.as_ref().ok_or_else(|| {
			NotCovered::internal(format!(
				"a lowering wrote to the table {table}, unread as written"
			))
		})?;
		Ok(Some((read, written)))
	}

	/// The `REFERENCE` fields that can name a record of `target`, or of any
	/// table where it is `None`. The caller notes what it lowers from them
	/// depends on ([`super::units::note_referencing`]).
	pub(crate) fn referencing(
		&self,
		txn: &Transaction,
		target: Option<&TableName>,
	) -> Lowered<crate::exec::catalog_deps::Referencing> {
		match target {
			Some(table) => match self.written(txn, table)? {
				Some((_, written)) => written.referencing.clone().ok_or_else(|| {
					NotCovered::internal(format!(
						"a lowering deleted from the table {table}, unread as deleted from"
					))
				}),
				None => Ok(Arc::from([])),
			},
			None => self.any_referencing.clone().ok_or_else(|| {
				NotCovered::internal("a lowering reached the references of any table, unread")
			}),
		}
	}

	/// The `DEFINE PARAM $name`; `None` where the catalog holds none.
	pub(crate) fn param(
		&self,
		txn: &Transaction,
		name: &str,
	) -> Lowered<Option<&Arc<ParamDefinition>>> {
		let Some((ns, db)) = self.database_ids()? else {
			return Ok(None);
		};
		let read = self.params.get(name).ok_or_else(|| {
			NotCovered::internal(format!("a lowering reached ${name}, unread by its plan"))
		})?;
		txn.note_param_read(ns, db, name);
		Ok(read.as_ref())
	}

	/// What the planner the plan was mapped with found of `fn::name`
	/// ([`Planner::function_snapshot`]).
	pub(crate) fn function(&self, txn: &Transaction, name: &str) -> Lowered<FunctionLookup> {
		let (read, _) = self.functions.get(name).ok_or_else(|| {
			NotCovered::internal(format!("a lowering reached fn::{name}, unread by its plan"))
		})?;
		if let PlanDatabase::Ids(ns, db) = self.database {
			txn.note_function_read(ns, db, name);
		}
		Ok(read.clone())
	}

	/// Whether a call of `fn::name` may write, as the planner the plan was
	/// mapped with resolved it over the function's call graph.
	pub(crate) fn function_access(&self, name: &str) -> Lowered<crate::exec::AccessMode> {
		let (_, resolved) = self.functions.get(name).ok_or_else(|| {
			NotCovered::internal(format!("a lowering reached fn::{name}, unread by its plan"))
		})?;
		Ok(resolved.access_mode)
	}

	/// Whether calls of `fn::name` for several rows must run one after
	/// another in row order, as the planner the plan was mapped with resolved
	/// it over the function's call graph.
	pub(crate) fn function_keeps_row_order(&self, name: &str) -> Lowered<bool> {
		let (_, resolved) = self.functions.get(name).ok_or_else(|| {
			NotCovered::internal(format!("a lowering reached fn::{name}, unread by its plan"))
		})?;
		Ok(resolved.keeps_row_order)
	}

	/// The model `name` at `version`; `None` where the catalog holds none.
	#[cfg(feature = "ml")]
	pub(crate) fn model(
		&self,
		txn: &Transaction,
		name: &str,
		version: &str,
	) -> Lowered<Option<&Arc<crate::catalog::MlModelDefinition>>> {
		if self.database_ids()?.is_none() {
			return Ok(None);
		}
		let read = self.models.get(&(name.to_owned(), version.to_owned())).ok_or_else(|| {
			NotCovered::internal(format!("a lowering reached ml::{name}<{version}>, unread"))
		})?;
		txn.note_opaque_read();
		Ok(read.as_ref())
	}

	/// The module stored under `storage`; `None` where the catalog holds
	/// none, or it could not be read.
	#[cfg(feature = "surrealism")]
	pub(crate) fn module(
		&self,
		txn: &Transaction,
		storage: &str,
	) -> Lowered<Option<&Arc<crate::catalog::ModuleDefinition>>> {
		if self.database_ids()?.is_none() {
			return Ok(None);
		}
		let read = self.modules.get(storage).ok_or_else(|| {
			NotCovered::internal(format!("a lowering reached the module {storage}, unread"))
		})?;
		txn.note_opaque_read();
		Ok(read.as_ref())
	}
}

/// The key the catalog stores the module a call of `function` names under;
/// `None` for a call of anything else.
pub(super) fn module_storage(function: &Function) -> Option<String> {
	match function {
		Function::Module(module, _) => Some(format!("mod::{module}")),
		Function::Silo {
			org,
			pkg,
			major,
			minor,
			patch,
			..
		} => Some(format!("silo::{org}::{pkg}::<{major}.{minor}.{patch}>")),
		Function::Normal(_) | Function::Custom(_) | Function::Script(_) | Function::Model(_) => {
			None
		}
	}
}

/// How a plan's lowering consults a table, each reading more than the last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Access {
	/// Its definition and fields.
	Read,
	/// What a write to it runs too.
	Written,
	/// The `REFERENCE` fields that can name its records too, whose repairs a
	/// delete runs.
	Deleted,
}

/// What a plan's lowering consults, by name.
#[derive(Default)]
struct Needs {
	/// Each table, and how the plan consults it.
	tables: BTreeMap<TableName, Access>,
	params: BTreeSet<String>,
	functions: BTreeSet<String>,
	models: BTreeSet<(String, String)>,
	modules: BTreeSet<String>,
}

impl Needs {
	fn table(&mut self, table: &TableName) {
		self.consult(table, Access::Read);
	}

	fn written(&mut self, table: &TableName) {
		self.consult(table, Access::Written);
	}

	fn consult(&mut self, table: &TableName, access: Access) {
		let held = self.tables.entry(table.clone()).or_insert(access);
		*held = (*held).max(access);
	}

	/// What `op`, and every statement it holds, consults.
	fn op(&mut self, op: &LogicalOp) {
		let _ = op.visit_ops_deep(&mut |node| {
			match node {
				LogicalOp::Source(source) => self.source(&source.source),
				LogicalOp::Write(write) => self.write(write),
				LogicalOp::Ddl(ddl) => self.ddl(&ddl.statement),
				_ => {}
			}
			let _ = node.visit_scalars(&mut |scalar| {
				self.expr(scalar);
				Ok::<(), std::convert::Infallible>(())
			});
			std::ops::ControlFlow::<()>::Continue(())
		});
	}

	/// What reading `source` consults: the table it names, or the parameter
	/// it reads.
	fn source(&mut self, source: &SourceRef) {
		match source {
			SourceRef::Table(table) => self.table(table),
			SourceRef::RecordId(rid) => self.table(&rid.table),
			SourceRef::Param(param) => {
				self.params.insert(param.as_str().to_owned());
			}
			SourceRef::Mock(_) | SourceRef::Dynamic(_) => {}
		}
	}

	/// The tables `write` names as those it writes to, and what its
	/// `RETURN` clause consults. A write that reaches
	/// a table only data names reaches it through the stage units lowered
	/// for it, which read their own tables.
	fn write(&mut self, write: &LogicalWrite) {
		let access = match write.statement {
			crate::expr::logical::LogicalWriteStatement::Delete(_) => Access::Deleted,
			_ => Access::Written,
		};
		if let super::dispatch::Reach::Tables(tables) = super::lower::write_reach(write) {
			for table in &tables {
				self.consult(table, access);
			}
		}
		if let Some(table) = super::lower::write_table(write) {
			self.consult(&table, access);
		}
		self.output(write);
	}

	/// What `write`'s clauses kept as written consult: its `RETURN` clause,
	/// and the places its data clauses assign to.
	fn output(&mut self, write: &LogicalWrite) {
		use crate::expr::logical::LogicalData;
		use crate::expr::visit::Visit as _;
		if let Some(output) = write.statement.output() {
			let _ = output.visit(&mut StoredNames(self));
		}
		for data in [&write.data, &write.duplicate_update].into_iter().flatten() {
			let places: Vec<&crate::expr::Idiom> = match data {
				LogicalData::Set(assignments) => assignments.iter().map(|a| &a.place).collect(),
				LogicalData::Unset(places) => places.iter().collect(),
				LogicalData::Values(rows) => {
					rows.iter().flat_map(|row| row.iter().map(|(place, _)| place)).collect()
				}
				LogicalData::Patch(_)
				| LogicalData::Merge(_)
				| LogicalData::Replace(_)
				| LogicalData::Content(_)
				| LogicalData::Single(_) => Vec::new(),
			};
			for place in places {
				let _ = place.visit(&mut StoredNames(self));
			}
		}
	}

	/// What `write`'s clauses consult, the tables it writes to aside.
	fn write_clauses(&mut self, write: &LogicalWrite) {
		self.output(write);
		let op = LogicalOp::Write(write.clone());
		let top: *const LogicalOp = &op;
		let _ = op.visit_ops_deep(&mut |node| {
			match node {
				LogicalOp::Source(source) => self.source(&source.source),
				LogicalOp::Write(nested) if !std::ptr::eq(node, top) => self.write(nested),
				_ => {}
			}
			let _ = node.visit_scalars(&mut |scalar| {
				self.expr(scalar);
				Ok::<(), std::convert::Infallible>(())
			});
			std::ops::ControlFlow::<()>::Continue(())
		});
	}

	/// The tables a schema statement's lowering checks it against, and what
	/// its clauses, kept as written, name.
	fn ddl(&mut self, statement: &LogicalDdlStatement) {
		{
			use crate::expr::visit::Visit as _;
			let _ = match statement {
				LogicalDdlStatement::Define(define) => define.visit(&mut StoredNames(self)),
				LogicalDdlStatement::Remove(remove) => remove.visit(&mut StoredNames(self)),
				LogicalDdlStatement::Alter(alter) => alter.visit(&mut StoredNames(self)),
				LogicalDdlStatement::Rebuild(rebuild) => rebuild.visit(&mut StoredNames(self)),
			};
		}
		if let Some(table) = crate::exec::logical::footprint::defines_on_demand(statement) {
			self.table(&table);
		}
		use crate::exec::logical::footprint::written;
		use crate::expr::statements::{AlterStatement, DefineStatement, RemoveStatement};
		let named = match statement {
			LogicalDdlStatement::Define(define) => match define.as_ref() {
				// A view's table is filled from its sources as its statement
				// runs, as a write to it.
				DefineStatement::Table(stmt) if stmt.view.is_some() => {
					if let Some(table) = written(&stmt.name) {
						self.written(&TableName::new(&table));
					}
					None
				}
				DefineStatement::Table(stmt) => written(&stmt.name),
				_ => None,
			},
			LogicalDdlStatement::Remove(remove) => match remove.as_ref() {
				RemoveStatement::Table(stmt) => written(&stmt.name),
				RemoveStatement::Field(stmt) => written(&stmt.table_name),
				_ => None,
			},
			LogicalDdlStatement::Alter(alter) => match alter.as_ref() {
				AlterStatement::Field(stmt) => written(&stmt.what),
				_ => None,
			},
			_ => None,
		};
		if let Some(table) = named {
			self.table(&TableName::new(&table));
		}
	}

	/// What evaluating `expr` consults, the statements it holds aside.
	fn expr(&mut self, expr: &LogicalExpr) {
		let _ = expr.visit(&mut |node| {
			match node {
				LogicalExpr::Param(param) => {
					self.params.insert(param.as_str().to_owned());
				}
				LogicalExpr::Call(call) => self.function(&call.receiver),
				LogicalExpr::Lookup {
					lookup,
					..
				} => {
					for subject in &lookup.what {
						match subject {
							crate::expr::logical::LogicalLookupSubject::Table {
								table,
								..
							}
							| crate::expr::logical::LogicalLookupSubject::Range {
								table,
								..
							} => self.table(table),
						}
					}
				}
				_ => {}
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
	}

	/// What evaluating `expr` consults, the statements it holds included.
	fn expr_deep(&mut self, expr: &LogicalExpr) {
		self.expr(expr);
		let _ = expr.visit(&mut |node| {
			if let LogicalExpr::Subquery(op) = node {
				self.op(op);
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
	}

	fn function(&mut self, function: &Function) {
		match function {
			Function::Custom(name) => {
				self.functions.insert(name.clone());
			}
			Function::Model(model) => {
				self.models.insert((model.name.to_string(), model.version.to_string()));
			}
			Function::Module(..)
			| Function::Silo {
				..
			} => {
				if let Some(storage) = module_storage(function) {
					self.modules.insert(storage);
				}
			}
			Function::Normal(_) | Function::Script(_) => {}
		}
	}

	/// What a stored clause consults, as written in the catalog.
	fn stored(&mut self, expr: &crate::expr::Expr) {
		use crate::expr::visit::Visit as _;
		let _ = expr.visit(&mut StoredNames(self));
	}

	/// What the clauses of `read`'s definitions consult: its permissions, the
	/// clauses of the view it is, and its fields' clauses and permissions.
	fn table_clauses(&mut self, read: &TableCatalog) {
		use crate::expr::visit::Visit as _;
		if let Some(definition) = &read.definition {
			let _ = definition.permissions.visit(&mut StoredNames(self));
			if let Some(view) = &definition.view {
				let _ = view.visit_clauses(&mut StoredNames(self));
			}
		}
		for field in read.fields.iter() {
			for expr in [&field.value, &field.assert, &field.computed].into_iter().flatten() {
				self.stored(expr);
			}
			if let crate::catalog::DefineDefault::Set(expr)
			| crate::catalog::DefineDefault::Always(expr) = &field.default
			{
				self.stored(expr);
			}
			for permission in
				[&field.select_permission, &field.create_permission, &field.update_permission]
			{
				let _ = permission.visit(&mut StoredNames(self));
			}
		}
		if let Some(written) = &read.written {
			for index in written.indexes.iter() {
				for column in &index.cols {
					let _ = column.visit(&mut StoredNames(self));
				}
				if let Some(cond) = &index.count_cond {
					self.stored(&cond.0);
				}
			}
			for event in written.events.iter() {
				self.stored(&event.when);
				for then in &event.then {
					self.stored(then);
				}
			}
		}
	}
}

/// The parameters, functions, models, modules and lookup tables a stored
/// clause names.
struct StoredNames<'n>(&'n mut Needs);

impl crate::expr::visit::Visitor for StoredNames<'_> {
	type Error = std::convert::Infallible;

	fn visit_param(&mut self, param: &crate::expr::Param) -> Result<(), Self::Error> {
		self.0.params.insert(param.as_str().to_owned());
		Ok(())
	}

	fn visit_function(&mut self, function: &Function) -> Result<(), Self::Error> {
		self.0.function(function);
		Ok(())
	}

	fn visit_lookup_subject(
		&mut self,
		subject: &crate::expr::lookup::LookupSubject,
	) -> Result<(), Self::Error> {
		match subject {
			crate::expr::lookup::LookupSubject::Table {
				table,
				..
			}
			| crate::expr::lookup::LookupSubject::Range {
				table,
				..
			} => self.0.table(table),
		}
		Ok(())
	}
}

/// Read what the lowering of `op`, mapped by `planner`, consults; see the
/// module.
/// `scanned` are the tables the mapping chose to scan, which a source the
/// statement evaluates may name.
pub(super) async fn read(
	planner: &Planner<'_>,
	op: &LogicalOp,
	scanned: impl Iterator<Item = &TableName>,
) -> Mapped<PlanCatalog> {
	let mut needs = Needs::default();
	needs.op(op);
	for table in scanned {
		needs.table(table);
	}
	// A view named by an evaluated clause is filled under the name the plan
	// resolves it to ([`super::lower::plan_time_name`]).
	if let LogicalOp::Ddl(ddl) = op
		&& let crate::expr::logical::LogicalDdlStatement::Define(define) = &ddl.statement
		&& let crate::expr::statements::DefineStatement::Table(stmt) = define.as_ref()
		&& stmt.view.is_some()
		&& crate::exec::logical::footprint::written(&stmt.name).is_none()
		&& let Some(Ok(name)) =
			super::lower::plan_time_name(planner, &stmt.name, super::program::NameClause::Table)
	{
		needs.written(&TableName::new(&name));
	}
	fill(planner, needs, false).await
}

/// Read what the given `needs` name, and what the definitions read name in
/// turn; with `any`, the `REFERENCE` fields that can name a record of any
/// table too.
async fn fill(planner: &Planner<'_>, mut needs: Needs, any: bool) -> Mapped<PlanCatalog> {
	let mut catalog = PlanCatalog::default();
	let Some(txn) = planner.txn().cloned() else {
		return Ok(catalog);
	};
	catalog.database = plan_database(planner, &txn).await?;
	if any && let PlanDatabase::Ids(ns, db) = catalog.database {
		catalog.any_referencing =
			Some(super::units::referencing_unnoted(planner, &txn, (ns, db), None).await?);
	}
	let mut tables_seen: BTreeMap<TableName, Access> = BTreeMap::new();
	let mut params_seen = BTreeSet::new();
	let mut functions_seen = BTreeSet::new();
	loop {
		let tables: Vec<(TableName, Access)> = needs
			.tables
			.iter()
			.filter(|(table, access)| tables_seen.get(*table).is_none_or(|seen| seen < access))
			.map(|(table, access)| (table.clone(), *access))
			.collect();
		let params: Vec<String> = needs.params.difference(&params_seen).cloned().collect();
		let functions: Vec<String> = needs.functions.difference(&functions_seen).cloned().collect();
		if tables.is_empty() && params.is_empty() && functions.is_empty() {
			break;
		}
		for (table, access) in tables {
			tables_seen.insert(table.clone(), access);
			let read = read_table(planner, &txn, catalog.database, &table, access).await?;
			if read.written.is_some() {
				// The units of the table's live subscriptions, which a write's
				// stage looks up when it runs.
				Box::pin(super::lower_live::prepare(planner, &table)).await;
			}
			if let Some(written) = &read.written {
				for view in written.views.iter() {
					needs.written(&view.name);
				}
				// A delete of a record runs the repairs the `REFERENCE` fields
				// that can name it direct, lowered with the write.
				for (referencing, _) in written.referencing.iter().flat_map(|found| found.iter()) {
					needs.table(referencing);
				}
			}
			needs.table_clauses(&read);
			catalog.tables.insert(table, Arc::new(read));
		}
		for name in params {
			params_seen.insert(name.clone());
			let read = read_param(&txn, catalog.database, &name).await?;
			if let Some(definition) = &read {
				use crate::expr::visit::Visit as _;
				let _ = definition.permissions.visit(&mut StoredNames(&mut needs));
			}
			catalog.params.insert(name, read);
		}
		for name in functions {
			functions_seen.insert(name.clone());
			let read = planner.function_snapshot(&name).await;
			if let FunctionLookup::Found(snapshot) = &read {
				use crate::expr::visit::Visit as _;
				let _ = snapshot.definition().permissions.visit(&mut StoredNames(&mut needs));
			}
			let resolved = planner.resolve_custom_function(&name).await;
			catalog.functions.insert(name, (read, resolved));
		}
	}
	for (name, version) in std::mem::take(&mut needs.models) {
		let read = read_model(&txn, catalog.database, &name, &version).await?;
		catalog.models.insert((name, version), read);
	}
	for storage in std::mem::take(&mut needs.modules) {
		let read = read_module(&txn, catalog.database, &storage).await;
		catalog.modules.insert(storage, read);
	}
	Ok(catalog)
}

/// The database `planner` plans in, as `txn` reads it. A planner that names
/// no database plans in none; one whose database it could not name, though
/// the catalog holds it, is not covered.
async fn plan_database(planner: &Planner<'_>, txn: &Transaction) -> Mapped<PlanDatabase> {
	if let Some((ns, db)) = planner.ns_db_ids().await {
		return Ok(PlanDatabase::Ids(ns, db));
	}
	let (Some(ns), Some(db)) = (planner.ns(), planner.db()) else {
		return Ok(PlanDatabase::Absent);
	};
	match txn.get_db_by_name(ns, db, None).await {
		Ok(None) => Ok(PlanDatabase::Absent),
		Ok(Some(_)) => Err(NotCovered::internal("a database the planner could not name")),
		Err(e) => Err(NotCovered::internal(format!("the database did not read: {e}"))),
	}
}

/// The catalog of `table` in `database` as `txn` reads it, as far as
/// `access` consults it.
async fn read_table(
	planner: &Planner<'_>,
	txn: &Transaction,
	database: PlanDatabase,
	table: &TableName,
	access: Access,
) -> Mapped<TableCatalog> {
	let written = access >= Access::Written;
	let unread = |what: &str, e: anyhow::Error| {
		NotCovered::internal(format!("the {what} of {table} did not read: {e}"))
	};
	let PlanDatabase::Ids(ns, db) = database else {
		return Ok(TableCatalog {
			table: table.clone(),
			version: None,
			definition: None,
			fields: Arc::from([]),
			written: written.then(|| WrittenTable {
				indexes: Arc::from([]),
				events: Arc::from([]),
				views: Arc::from([]),
				referencing: (access == Access::Deleted).then(|| Arc::from([])),
			}),
		});
	};
	let version = txn.catalog_version(ns, db, table).await.map_err(|e| unread("version", e))?;
	let definition = txn.get_tb(ns, db, table, None).await.map_err(|e| unread("definition", e))?;
	let fields = match &definition {
		Some(_) => txn.all_tb_fields(ns, db, table, None).await.map_err(|e| unread("fields", e))?,
		None => Arc::from([]),
	};
	let written = match written {
		true => Some(WrittenTable {
			indexes: txn
				.all_tb_indexes(ns, db, table, None)
				.await
				.map_err(|e| unread("indexes", e))?,
			events: txn
				.all_tb_events(ns, db, table, None)
				.await
				.map_err(|e| unread("events", e))?,
			views: txn.all_tb_views(ns, db, table, None).await.map_err(|e| unread("views", e))?,
			referencing: match access {
				Access::Deleted => Some(
					super::units::referencing_unnoted(planner, txn, (ns, db), Some(table)).await?,
				),
				Access::Read | Access::Written => None,
			},
		}),
		false => None,
	};
	Ok(TableCatalog {
		table: table.clone(),
		version,
		definition,
		fields,
		written,
	})
}

/// A catalog of `tables`, each read as written, for a lowering of `write`'s
/// stage units outside any plan: the tables a write reaches only through
/// data, which the run serves through those units, with what the write's own
/// clauses consult. For a delete, `any` reads as well the `REFERENCE` fields
/// that can name a record of any table, for the unit that stands for every
/// table the catalog does not hold.
pub(super) async fn read_written(
	planner: &Planner<'_>,
	write: &LogicalWrite,
	tables: &[TableName],
	any: bool,
) -> Mapped<PlanCatalog> {
	let mut needs = Needs::default();
	needs.write_clauses(write);
	let access = match write.statement {
		crate::expr::logical::LogicalWriteStatement::Delete(_) => Access::Deleted,
		_ => Access::Written,
	};
	for table in tables {
		needs.consult(table, access);
	}
	fill(planner, needs, any && access == Access::Deleted).await
}

/// A catalog for the lowering of `closure`'s body outside any plan
/// ([`super::lower::lower_closure_body`]): what the body consults.
pub(super) async fn read_closure(
	planner: &Planner<'_>,
	closure: &crate::expr::logical::LogicalClosure,
) -> Mapped<PlanCatalog> {
	let mut needs = Needs::default();
	needs.expr_deep(&closure.body);
	fill(planner, needs, false).await
}

/// A catalog for the lowering of a parameter's `PERMISSIONS` predicate
/// `guard` outside any plan ([`super::lower::param_guard_unit`]): what the
/// predicate consults.
pub(super) async fn read_guard(planner: &Planner<'_>, guard: &LogicalExpr) -> Mapped<PlanCatalog> {
	let mut needs = Needs::default();
	needs.expr_deep(guard);
	fill(planner, needs, false).await
}

/// A catalog for the lowering of view table `table`'s order outside any plan
/// ([`super::fault::view_rows`]): what a write of the table's rows consults.
pub(super) async fn read_view_table(
	planner: &Planner<'_>,
	table: &TableName,
) -> Mapped<PlanCatalog> {
	let mut needs = Needs::default();
	needs.consult(table, Access::Written);
	fill(planner, needs, false).await
}

/// A catalog for the lowering of a live subscription's work on records of
/// `table` ([`super::lower::lower_subscription`]): the table, and what the
/// subscription's clauses name.
pub(super) async fn read_subscription(
	planner: &Planner<'_>,
	table: &TableName,
	compiled: &crate::catalog::CompiledSubscription,
) -> Mapped<PlanCatalog> {
	use crate::expr::visit::Visit as _;
	let mut needs = Needs::default();
	needs.table(table);
	if let Some(cond) = &compiled.cond {
		needs.stored(cond);
	}
	if let crate::catalog::SubscriptionFields::Select(fields) = &compiled.fields {
		let _ = fields.visit(&mut StoredNames(&mut needs));
	}
	for path in compiled.fetch.iter().flatten() {
		needs.stored(path);
	}
	fill(planner, needs, false).await
}

async fn read_param(
	txn: &Transaction,
	database: PlanDatabase,
	name: &str,
) -> Mapped<Option<Arc<ParamDefinition>>> {
	let PlanDatabase::Ids(ns, db) = database else {
		return Ok(None);
	};
	match txn.get_db_param(ns, db, name, None).await {
		Ok(definition) => Ok(Some(definition)),
		Err(e) if matches!(e.downcast_ref(), Some(crate::catalog::Error::PaNotFound { .. })) => {
			Ok(None)
		}
		Err(e) => Err(NotCovered::internal(format!("a parameter that could not be read: {e}"))),
	}
}

async fn read_model(
	txn: &Transaction,
	database: PlanDatabase,
	name: &str,
	version: &str,
) -> Mapped<Option<Arc<crate::catalog::MlModelDefinition>>> {
	let PlanDatabase::Ids(ns, db) = database else {
		return Ok(None);
	};
	txn.get_db_model(ns, db, name, version, None)
		.await
		.map_err(|e| NotCovered::internal(format!("a model that could not be read: {e}")))
}

async fn read_module(
	txn: &Transaction,
	database: PlanDatabase,
	storage: &str,
) -> Option<Arc<crate::catalog::ModuleDefinition>> {
	let PlanDatabase::Ids(ns, db) = database else {
		return None;
	};
	txn.get_db_module(ns, db, storage, None).await.ok()
}

/// The footprint of statement `op` ([`crate::exec::logical::footprint`]),
/// expanded by what `catalog` read of the tables it touches rather than by the
/// catalog itself: a table the catalog did not read is answered as the
/// expansion answers one it cannot tell about. Only the writes are kept where
/// `reads` does not hold.
pub(super) async fn footprint(
	planner: &Planner<'_>,
	catalog: &Arc<PlanCatalog>,
	op: &LogicalOp,
	reads: bool,
) -> crate::exec::logical::footprint::Footprint {
	use crate::exec::logical::footprint;
	let mut fp = footprint::of(op, planner.function_registry());
	let Some(txn) = planner.txn().cloned() else {
		return footprint::Footprint::unknown();
	};
	if !reads {
		fp.reads = vec![footprint::Resource::Unknown];
	}
	let view = FootprintView {
		catalog: Arc::clone(catalog),
		txn,
		schema: crate::exec::planner::fold::changes_the_catalog(op),
	};
	footprint::expand(&mut fp, &view).await;
	fp
}

/// A plan's catalog as a footprint's expansion reads it.
struct FootprintView {
	catalog: Arc<PlanCatalog>,
	txn: Arc<Transaction>,
	/// Whether the statement changes the catalog, whose own checks read the
	/// tables rather than their permissions.
	schema: bool,
}

type ViewFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl crate::exec::logical::footprint::CatalogView for FootprintView {
	fn triggers<'a>(&'a self, table: &'a TableName) -> ViewFuture<'a, (bool, bool)> {
		let answer = match self.catalog.written(&self.txn, table) {
			Ok(Some((_, written))) => (!written.events.is_empty(), !written.views.is_empty()),
			Ok(None) => (false, false),
			Err(_) => (true, true),
		};
		Box::pin(async move { answer })
	}

	fn any_triggers(&self) -> ViewFuture<'_, bool> {
		Box::pin(async { true })
	}

	fn reads_through<'a>(&'a self, table: &'a TableName) -> ViewFuture<'a, bool> {
		use crate::catalog::Permission;
		let answer = match self.catalog.table(&self.txn, table) {
			Ok(Some(read)) => match &read.definition {
				None => false,
				Some(def)
					if !self.schema
						&& matches!(def.permissions.select, Permission::Specific(_)) =>
				{
					true
				}
				Some(_) => read.fields.iter().any(|field| {
					field.computed.is_some()
						|| (!self.schema
							&& matches!(field.select_permission, Permission::Specific(_)))
				}),
			},
			Ok(None) => true,
			Err(_) => true,
		};
		Box::pin(async move { answer })
	}

	fn defines<'a>(&'a self, table: &'a TableName) -> ViewFuture<'a, bool> {
		let answer = matches!(
			self.catalog.table(&self.txn, table),
			Ok(Some(read)) if read.definition.is_some()
		);
		Box::pin(async move { answer })
	}
}
