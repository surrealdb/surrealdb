//! The names a statement and its definitions reach: tables, `fn::` functions and free `$` names.
//!
//! The memo loads these before planning. A collection need not be complete: a name it misses is
//! answered `NotLoaded` and loaded through the pending set. It names only what a lookup of the
//! memo can ask for:
//! - a table, function or stored parameter the text of the statement or definition names;
//! - a free `$` name of the statement's own text, which is the only frame a stored parameter is
//!   answered in. A name bound inside the statement (`LET`, `FOR`, a closure or function argument)
//!   is not free, and a definition's own `$` names are never collected;
//! - no `record<t>` or relation target: a table is loaded when a lookup asks for it.
//!
//! `ml::` models, `api` definitions, sequences and analyzers are not collected, since no lookup
//! of the memo answers them.

use std::collections::BTreeSet;
use std::convert::Infallible;

use surrealdb_expr::parser::TopLevelExpr;

use super::slots::{OccurrenceClass, SlotTable};
use crate::catalog::{DefineDefault, EventDefinition, FunctionDefinition};
use crate::expr::logical::ITERATION_PARAMS;
use crate::expr::visit::{Visit, Visitor};
use crate::expr::{Expr, Function};
use crate::kvs::cache::ds::TableSnapshot;
use crate::val::TableName;

/// The tables, functions and parameters a statement or definition reaches.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NameSet {
	pub(crate) tables: BTreeSet<TableName>,
	/// Custom function names, without the `fn::` prefix.
	pub(crate) functions: BTreeSet<String>,
	/// Free `$` names of a statement's own text, with the iteration names and the principal names
	/// excluded.
	pub(crate) params: BTreeSet<String>,
}

impl NameSet {
	/// Whether no name is held.
	pub(crate) fn is_empty(&self) -> bool {
		self.tables.is_empty() && self.functions.is_empty() && self.params.is_empty()
	}

	/// Adds every name of `other`.
	pub(crate) fn extend(&mut self, other: NameSet) {
		self.tables.extend(other.tables);
		self.functions.extend(other.functions);
		self.params.extend(other.params);
	}

	/// The names of `self` that `held` does not hold.
	pub(crate) fn missing_from(&self, held: &NameSet) -> NameSet {
		NameSet {
			tables: self.tables.difference(&held.tables).cloned().collect(),
			functions: self.functions.difference(&held.functions).cloned().collect(),
			params: self.params.difference(&held.params).cloned().collect(),
		}
	}

	/// The tables and functions a table's definitions reach: field `VALUE`, `DEFAULT`, `ASSERT` and
	/// `COMPUTED` clauses, table and field permissions, view clauses and event clauses. No
	/// parameter: a definition's `$` names are not answered at plan time.
	pub(crate) fn from_table(snapshot: &TableSnapshot, events: &[EventDefinition]) -> NameSet {
		let mut collector = Collector::default();
		if let Some(def) = &snapshot.definition {
			let _ = def.permissions.visit(&mut collector);
			if let Some(view) = &def.view {
				let _ = view.visit_clauses(&mut collector);
			}
		}
		for field in snapshot.fields.iter() {
			for expr in [&field.value, &field.assert, &field.computed].into_iter().flatten() {
				let _ = expr.visit(&mut collector);
			}
			if let DefineDefault::Set(expr) | DefineDefault::Always(expr) = &field.default {
				let _ = expr.visit(&mut collector);
			}
			for permission in
				[&field.select_permission, &field.create_permission, &field.update_permission]
			{
				let _ = permission.visit(&mut collector);
			}
		}
		for event in events {
			let _ = event.when.visit(&mut collector);
			for expr in &event.then {
				let _ = expr.visit(&mut collector);
			}
		}
		collector.names
	}

	/// The tables and functions a function body and its permission reach. No parameter.
	pub(crate) fn from_function(def: &FunctionDefinition) -> NameSet {
		let mut collector = Collector::default();
		let _ = def.block.visit(&mut collector);
		let _ = def.permissions.visit(&mut collector);
		collector.names
	}
}

/// The names `stmt` reaches directly: tables in sources and targets (`Expr::Table`, record-id
/// tables, graph lookup subjects), `fn::` calls, and the free `$` names of its text.
pub(crate) fn collect(stmt: &TopLevelExpr) -> NameSet {
	collect_with(stmt, &SlotTable::of(stmt)).0
}

/// [`collect`], reading the free names from `slots`, the slot table of `stmt`, together with the
/// tables the `DELETE` statements of `stmt` name directly: the tables whose records a delete
/// purges, and so the tables whose references the delete repairs.
///
/// Like the names, the deleted tables need not be complete: a table missed is asked for, and
/// loaded, through the memo's pending set. Deletes are recorded only for a `TopLevelExpr::Expr`.
pub(crate) fn collect_with(
	stmt: &TopLevelExpr,
	slots: &SlotTable,
) -> (NameSet, BTreeSet<TableName>) {
	let mut collector = Collector::default();
	let _ = match stmt {
		TopLevelExpr::Begin(_) | TopLevelExpr::Cancel | TopLevelExpr::Commit => Ok(()),
		TopLevelExpr::Access(access) => collector.visit_access(access),
		TopLevelExpr::Kill(kill) => collector.visit_kill(kill),
		TopLevelExpr::Live(live) => collector.visit_live(live),
		TopLevelExpr::Option(option) => collector.visit_option(option),
		TopLevelExpr::Use(use_) => collector.visit_use(use_),
		TopLevelExpr::Show(show) => collector.visit_show(show),
		TopLevelExpr::Expr(expr) => {
			collector.deleted = Some(BTreeSet::new());
			collector.visit_expr(expr)
		}
	};
	let deleted = collector.deleted.unwrap_or_default();
	let mut names = collector.names;
	for occurrence in slots.occurrences() {
		let name = occurrence.name.as_str();
		if matches!(occurrence.class, OccurrenceClass::Free(_) | OccurrenceClass::FreeUnslotted)
			&& !ITERATION_PARAMS.contains(&name)
		{
			names.params.insert(name.to_owned());
		}
	}
	(names, deleted)
}

/// The visitor behind [`collect_with`], the definition walks and the parameter names of a
/// definition.
#[derive(Default)]
pub(crate) struct Collector {
	names: NameSet,
	/// The tables the `DELETE` statements visited name directly. `None` records nothing.
	deleted: Option<BTreeSet<TableName>>,
	/// Every `$` name visited, at any depth. `None` records nothing.
	params: Option<BTreeSet<String>>,
}

impl Collector {
	/// A collector of the `$` names that occur, added to `also`.
	pub(crate) fn params(also: BTreeSet<String>) -> Self {
		Self {
			params: Some(also),
			..Self::default()
		}
	}

	/// The `$` names gathered by a collector made with [`Collector::params`].
	pub(crate) fn into_params(self) -> BTreeSet<String> {
		self.params.unwrap_or_default()
	}
}

impl Visitor for Collector {
	type Error = Infallible;

	fn visit_delete(
		&mut self,
		delete: &crate::expr::statements::DeleteStatement,
	) -> Result<(), Self::Error> {
		if self.deleted.is_some() {
			let mut named = Collector::default();
			for what in &delete.what {
				let _ = named.visit_expr(what);
			}
			if let Some(deleted) = &mut self.deleted {
				deleted.extend(named.names.tables);
			}
		}
		delete.visit(self)
	}

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if let Expr::Table(table) = expr {
			self.names.tables.insert(table.clone());
		}
		expr.visit(self)
	}

	fn visit_record_id(&mut self, id: &crate::expr::RecordIdLit) -> Result<(), Self::Error> {
		self.names.tables.insert(id.table.clone());
		id.visit(self)
	}

	fn visit_lookup_subject(
		&mut self,
		subject: &crate::expr::lookup::LookupSubject,
	) -> Result<(), Self::Error> {
		let (crate::expr::lookup::LookupSubject::Table {
			table,
			..
		}
		| crate::expr::lookup::LookupSubject::Range {
			table,
			..
		}) = subject;
		self.names.tables.insert(table.clone());
		subject.visit(self)
	}

	fn visit_function(&mut self, function: &Function) -> Result<(), Self::Error> {
		if let Function::Custom(name) = function {
			self.names.functions.insert(name.strip_prefix("fn::").unwrap_or(name).to_owned());
		}
		function.visit(self)
	}

	fn visit_param(&mut self, param: &crate::expr::Param) -> Result<(), Self::Error> {
		if let Some(params) = &mut self.params {
			params.insert(param.as_str().to_owned());
		}
		Ok(())
	}
}
