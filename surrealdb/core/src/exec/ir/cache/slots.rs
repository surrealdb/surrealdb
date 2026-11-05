//! The slot table of a statement: its free `$` names, numbered by first free occurrence.
//!
//! A free name is read through a VM run-time parameter slot. The table is a pure function of the
//! statement's AST, so two statements of one shape have equal tables. It also counts the literals
//! of the statement text: the count is the ordinal space of
//! [`LiteralOrdinal`](super::deps::LiteralOrdinal), numbered in the order this walk visits them.
//!
//! Scoping follows the logical binder. A block opens a scope. A `LET` binds its name after its
//! value, in the enclosing scope. A `FOR` variable binds in the scope of the body, after the range
//! is visited. A closure binds its parameters, and a `DEFINE FUNCTION` binds its arguments, for
//! the body only. An occurrence is classified as bound before it is classified as a row or
//! principal name, so `FOR $session IN … { $session }` reads a bound value.
//!
//! A row name (`$this`, `$value`, `$before`, ...) is a [`OccurrenceClass::Row`] only where the
//! binder declares it: inside the region of a clause that binds that name, per the region tables
//! of `build/logical.rs` (`ROW_ONLY`, `STATEMENT_ROW`, `WITH_INPUT`, `WITH_IMAGES`, `FIELD_VALUE`,
//! `EVENT_IMAGES` and the per-row loop of a `SELECT`). Anywhere else the binder reads it as a
//! free name, so it has a slot. A region isolated from above (a closure, a function body, a
//! permission, field or event clause) sees only the names it declares itself. `$parent` is
//! declared only where a row frame encloses the region. A clause that the binder may place in a
//! region declaring fewer names than the clause's position suggests is classified as outside any
//! row region. That gives a name a slot the binder may not read. An unread slot is harmless. A
//! free read with no slot is not.

use std::collections::HashMap;
use std::convert::Infallible;

use surrealdb_expr::parser::TopLevelExpr;
use surrealdb_strand::Strand;

use super::answer::PRINCIPAL_NAMES;
use crate::exec::ir::iface::premise::SlotId;
use crate::expr::permission::Permission;
use crate::expr::statements::define::field::DefineDefault;
use crate::expr::statements::{
	CreateStatement, DefineEventStatement, DefineFieldStatement, DefineFunctionStatement,
	DeleteStatement, ForeachStatement, InsertStatement, RelateStatement, SelectStatement,
	SetStatement, UpdateStatement, UpsertStatement,
};
use crate::expr::visit::{Visit, Visitor};
use crate::expr::{Block, ClosureExpr, Literal, Output, Param};

/// The row names bound by a permission clause and a `COMPUTED` field.
const ROW_ONLY: &[&str] = &["this", "self", "value"];
/// The row names a clause of a statement gives the row it is evaluated against: the projection,
/// condition and split of a `SELECT`, and a write's clause values and condition. `$value` is no
/// name of the row there: it reads the parameter of the frame around the statement.
const STATEMENT_ROW: &[&str] = &["this", "self"];
/// The row names bound around the `ON DUPLICATE KEY UPDATE` clause of an `INSERT`.
const WITH_INPUT: &[&str] = &["this", "self", "input"];
/// The row names bound around the `RETURN` projection of a write.
const WITH_IMAGES: &[&str] = &["this", "self", "after", "before"];
/// The row names bound around the `VALUE`, `DEFAULT` and `ASSERT` clauses of a field.
const FIELD_VALUE: &[&str] = &["this", "self", "value", "after", "before", "input"];
/// The row names bound around the `WHEN` and `THEN` of an event.
const EVENT_IMAGES: &[&str] = &["this", "self", "value", "before", "after", "event", "input"];

/// One `$name` occurrence of a statement, in AST pre-order.
///
/// The binding position of a `LET`, `FOR`, closure parameter or function argument is not an
/// occurrence: only reads are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Occurrence {
	pub(crate) name: Strand,
	pub(crate) class: OccurrenceClass,
}

/// How an occurrence of a `$name` is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OccurrenceClass {
	/// Free in the statement: read through slot `SlotId` at run time.
	Free(SlotId),
	/// Free, but past the slot space (more than `u16::MAX + 1` distinct names). `id_of` answers
	/// `None` for the name.
	FreeUnslotted,
	/// Bound inside the statement (`LET`, `FOR`, closure or function argument): an SSA value.
	Bound,
	/// A row name inside a region that binds it, unless bound inside the statement.
	Row,
	/// A principal name (`PRINCIPAL_NAMES`), unless bound inside the statement.
	Principal,
}

/// The free names, `$name` occurrences and literal count of one statement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SlotTable {
	/// Free names, indexed by slot: ordered by first free occurrence.
	names: Vec<Strand>,
	/// Every `$name` read, in AST pre-order.
	occurrences: Vec<Occurrence>,
	/// The literals visited.
	literals: u32,
}

impl SlotTable {
	/// The table of `stmt`.
	pub(crate) fn of(stmt: &TopLevelExpr) -> Self {
		let mut walker = Walker::default();
		let _ = match stmt {
			TopLevelExpr::Begin(_) | TopLevelExpr::Cancel | TopLevelExpr::Commit => Ok(()),
			TopLevelExpr::Access(access) => walker.visit_access(access),
			TopLevelExpr::Kill(kill) => walker.visit_kill(kill),
			TopLevelExpr::Live(live) => walker.visit_live(live),
			TopLevelExpr::Option(option) => walker.visit_option(option),
			TopLevelExpr::Use(use_) => walker.visit_use(use_),
			TopLevelExpr::Show(show) => walker.visit_show(show),
			TopLevelExpr::Expr(expr) => walker.visit_expr(expr),
		};
		walker.table
	}

	/// The slot of the free name `name`, or `None` for a name that is bound, a row or principal
	/// name, absent from the statement, or past the slot space.
	pub(crate) fn id_of(&self, name: &str) -> Option<SlotId> {
		let index = self.names.iter().position(|n| n.as_str() == name)?;
		Some(SlotId(u16::try_from(index).ok()?))
	}

	/// The free name held by `slot`.
	pub(crate) fn name(&self, slot: SlotId) -> Option<&str> {
		self.names.get(usize::from(slot.0)).map(Strand::as_str)
	}

	/// The number of slotted free names.
	pub(crate) fn len(&self) -> usize {
		self.names.len()
	}

	/// Every `$name` occurrence of the statement, in AST pre-order.
	pub(crate) fn occurrences(&self) -> &[Occurrence] {
		&self.occurrences
	}

	/// The number of literals of the statement text, in the walk that numbers `LiteralOrdinal`s.
	pub(crate) fn literal_count(&self) -> u32 {
		self.literals
	}
}

/// The visitor behind [`SlotTable::of`].
#[derive(Default)]
struct Walker {
	table: SlotTable,
	/// The slot of each slotted free name, by name.
	slots: HashMap<String, SlotId>,
	/// The names bound by each open scope, innermost last.
	scopes: Vec<Vec<String>>,
	/// The row regions open around the walk, innermost last.
	rows: Vec<RowFrame>,
}

/// The row names a region binds, as the binder declares them.
struct RowFrame {
	names: &'static [&'static str],
	/// Whether `$parent` is declared: a row frame encloses the region.
	parent: bool,
	/// Whether the region sees no name of the regions around it.
	isolated: bool,
}

impl Walker {
	fn is_bound(&self, name: &str) -> bool {
		self.scopes.iter().any(|scope| scope.iter().any(|bound| bound == name))
	}

	/// Whether `name` is a row name an open region binds.
	fn is_row(&self, name: &str) -> bool {
		for frame in self.rows.iter().rev() {
			let bound = if name == "parent" {
				frame.parent
			} else {
				frame.names.contains(&name)
			};
			if bound {
				return true;
			}
			if frame.isolated {
				return false;
			}
		}
		false
	}

	/// Whether a region that binds a row encloses the walk, visibly.
	fn has_row(&self) -> bool {
		for frame in self.rows.iter().rev() {
			if !frame.names.is_empty() {
				return true;
			}
			if frame.isolated {
				return false;
			}
		}
		false
	}

	/// Runs `body` inside a region that binds `names`. An `isolated` region sees none of the
	/// names of the regions around it, and binds no `$parent`.
	fn within_rows<R>(
		&mut self,
		names: &'static [&'static str],
		isolated: bool,
		body: impl FnOnce(&mut Self) -> R,
	) -> R {
		let parent = !isolated && !names.is_empty() && self.has_row();
		self.rows.push(RowFrame {
			names,
			parent,
			isolated,
		});
		let result = body(self);
		self.rows.pop();
		result
	}

	/// Binds `name` in the innermost scope, or in a new root scope when none is open.
	fn bind(&mut self, name: &str) {
		if self.scopes.is_empty() {
			self.scopes.push(Vec::new());
		}
		if let Some(scope) = self.scopes.last_mut() {
			scope.push(name.to_owned());
		}
	}

	/// The condition of a write, which is judged against the row being written.
	fn write_cond(&mut self, cond: Option<&crate::expr::Expr>) -> Result<(), Infallible> {
		match cond {
			Some(cond) => self.within_rows(STATEMENT_ROW, false, |walker| walker.visit_expr(cond)),
			None => Ok(()),
		}
	}

	/// A `VERSION` or `TIMEOUT` clause. The parser holds an absent clause as `NONE`, which is not a
	/// literal of the statement text, so it is not visited.
	fn optional_clause(&mut self, clause: &crate::expr::Expr) -> Result<(), Infallible> {
		if matches!(clause, crate::expr::Expr::Literal(Literal::None)) {
			Ok(())
		} else {
			self.visit_expr(clause)
		}
	}

	/// The `RETURN` clause of a write, which sees the record before and after the write.
	fn write_output(&mut self, output: Option<&Output>) -> Result<(), Infallible> {
		match output {
			Some(output) => {
				self.within_rows(WITH_IMAGES, false, |walker| walker.visit_output(output))
			}
			None => Ok(()),
		}
	}

	/// The class of a read of the free name `name`, numbering it on its first free occurrence.
	fn free(&mut self, name: &str) -> OccurrenceClass {
		if let Some(slot) = self.slots.get(name) {
			return OccurrenceClass::Free(*slot);
		}
		match u16::try_from(self.table.names.len()) {
			Ok(id) => {
				let slot = SlotId(id);
				self.table.names.push(Strand::new(name));
				self.slots.insert(name.to_owned(), slot);
				OccurrenceClass::Free(slot)
			}
			Err(_) => OccurrenceClass::FreeUnslotted,
		}
	}
}

impl Visitor for Walker {
	type Error = Infallible;

	fn visit_param(&mut self, param: &Param) -> Result<(), Self::Error> {
		let name = param.as_str();
		let class = if self.is_bound(name) {
			OccurrenceClass::Bound
		} else if self.is_row(name) {
			OccurrenceClass::Row
		} else if PRINCIPAL_NAMES.contains(&name) {
			OccurrenceClass::Principal
		} else {
			self.free(name)
		};
		self.table.occurrences.push(Occurrence {
			name: Strand::new(name),
			class,
		});
		Ok(())
	}

	fn visit_literal(&mut self, literal: &Literal) -> Result<(), Self::Error> {
		self.table.literals = self.table.literals.saturating_add(1);
		literal.visit(self)
	}

	fn visit_block(&mut self, block: &Block) -> Result<(), Self::Error> {
		self.scopes.push(Vec::new());
		let result = block.visit(self);
		self.scopes.pop();
		result
	}

	fn visit_set(&mut self, set: &SetStatement) -> Result<(), Self::Error> {
		set.visit(self)?;
		self.bind(set.name.as_str());
		Ok(())
	}

	fn visit_foreach(&mut self, foreach: &ForeachStatement) -> Result<(), Self::Error> {
		self.visit_expr(&foreach.range)?;
		self.scopes.push(vec![foreach.param.as_str().to_owned()]);
		let result = self.visit_block(&foreach.block);
		self.scopes.pop();
		result
	}

	fn visit_closure(&mut self, closure: &ClosureExpr) -> Result<(), Self::Error> {
		self.scopes.push(closure.args.iter().map(|(param, _)| param.as_str().to_owned()).collect());
		let result = self.within_rows(&[], true, |walker| closure.visit(walker));
		self.scopes.pop();
		result
	}

	fn visit_define_function(
		&mut self,
		define: &DefineFunctionStatement,
	) -> Result<(), Self::Error> {
		for (_, kind) in &define.args {
			self.visit_kind(kind)?;
		}
		self.scopes.push(define.args.iter().map(|(name, _)| name.clone()).collect());
		let result = self.within_rows(&[], true, |walker| walker.visit_block(&define.block));
		self.scopes.pop();
		result?;
		self.visit_permission(&define.permissions)?;
		self.visit_expr(&define.comment)?;
		if let Some(kind) = &define.returns {
			self.visit_kind(kind)?;
		}
		Ok(())
	}

	fn visit_permission(&mut self, permission: &Permission) -> Result<(), Self::Error> {
		self.within_rows(ROW_ONLY, true, |walker| permission.visit(walker))
	}

	fn visit_select(&mut self, select: &SelectStatement) -> Result<(), Self::Error> {
		// The projection of an aggregating `SELECT` is bound outside the per-row loop.
		if select.group.is_some() {
			self.visit_fields(&select.fields)?;
		} else {
			self.within_rows(STATEMENT_ROW, false, |walker| walker.visit_fields(&select.fields))?;
		}
		for omit in &select.omit {
			self.visit_expr(omit)?;
		}
		for what in &select.what {
			self.visit_expr(what)?;
		}
		self.within_rows(STATEMENT_ROW, false, |walker| {
			if let Some(cond) = &select.cond {
				walker.visit_expr(&cond.0)?;
			}
			if let Some(split) = &select.split {
				for split in &split.0 {
					walker.visit_idiom(&split.0)?;
				}
			}
			Ok::<(), Infallible>(())
		})?;
		if let Some(group) = &select.group {
			for group in &group.0 {
				self.visit_idiom(&group.0)?;
			}
		}
		if let Some(order) = &select.order {
			self.visit_ordering(order)?;
		}
		if let Some(limit) = &select.limit {
			self.visit_expr(&limit.0)?;
		}
		if let Some(start) = &select.start {
			self.visit_expr(&start.0)?;
		}
		if let Some(fetch) = &select.fetch {
			for fetch in fetch.iter() {
				self.visit_expr(&fetch.0)?;
			}
		}
		self.optional_clause(&select.version)?;
		self.optional_clause(&select.timeout)
	}

	fn visit_create(&mut self, create: &CreateStatement) -> Result<(), Self::Error> {
		for what in &create.what {
			self.visit_expr(what)?;
		}
		if let Some(data) = &create.data {
			self.within_rows(STATEMENT_ROW, false, |walker| walker.visit_data(data))?;
		}
		self.write_output(create.output.as_ref())?;
		self.optional_clause(&create.timeout)
	}

	fn visit_update(&mut self, update: &UpdateStatement) -> Result<(), Self::Error> {
		for what in &update.what {
			self.visit_expr(what)?;
		}
		if let Some(data) = &update.data {
			self.within_rows(STATEMENT_ROW, false, |walker| walker.visit_data(data))?;
		}
		self.write_output(update.output.as_ref())?;
		self.write_cond(update.cond.as_ref().map(|cond| &cond.0))?;
		self.optional_clause(&update.timeout)
	}

	fn visit_upsert(&mut self, upsert: &UpsertStatement) -> Result<(), Self::Error> {
		for what in &upsert.what {
			self.visit_expr(what)?;
		}
		if let Some(data) = &upsert.data {
			self.within_rows(STATEMENT_ROW, false, |walker| walker.visit_data(data))?;
		}
		self.write_output(upsert.output.as_ref())?;
		self.write_cond(upsert.cond.as_ref().map(|cond| &cond.0))?;
		self.optional_clause(&upsert.timeout)
	}

	fn visit_delete(&mut self, delete: &DeleteStatement) -> Result<(), Self::Error> {
		for what in &delete.what {
			self.visit_expr(what)?;
		}
		self.write_cond(delete.cond.as_ref().map(|cond| &cond.0))?;
		self.write_output(delete.output.as_ref())?;
		self.optional_clause(&delete.timeout)
	}

	fn visit_relate(&mut self, relate: &RelateStatement) -> Result<(), Self::Error> {
		self.visit_expr(&relate.through)?;
		self.visit_expr(&relate.from)?;
		self.visit_expr(&relate.to)?;
		if let Some(data) = &relate.data {
			self.within_rows(STATEMENT_ROW, false, |walker| walker.visit_data(data))?;
		}
		self.write_output(relate.output.as_ref())?;
		self.optional_clause(&relate.timeout)
	}

	fn visit_insert(&mut self, insert: &InsertStatement) -> Result<(), Self::Error> {
		if let Some(into) = &insert.into {
			self.visit_expr(into)?;
		}
		self.visit_data(&insert.data)?;
		if let Some(update) = &insert.update {
			self.within_rows(WITH_INPUT, false, |walker| walker.visit_data(update))?;
		}
		self.write_output(insert.output.as_ref())?;
		self.optional_clause(&insert.timeout)
	}

	fn visit_define_field(&mut self, define: &DefineFieldStatement) -> Result<(), Self::Error> {
		self.visit_expr(&define.name)?;
		self.visit_expr(&define.what)?;
		if let Some(kind) = &define.field_kind {
			self.visit_kind(kind)?;
		}
		self.within_rows(FIELD_VALUE, true, |walker| {
			if let Some(value) = &define.value {
				walker.visit_expr(value)?;
			}
			if let Some(assert) = &define.assert {
				walker.visit_expr(assert)?;
			}
			Ok::<(), Infallible>(())
		})?;
		if let Some(computed) = &define.computed {
			self.within_rows(ROW_ONLY, true, |walker| walker.visit_expr(computed))?;
		}
		if let DefineDefault::Always(expr) | DefineDefault::Set(expr) = &define.default {
			self.within_rows(FIELD_VALUE, true, |walker| walker.visit_expr(expr))?;
		}
		self.visit_permissions(&define.permissions)?;
		if let Some(reference) = &define.reference {
			self.visit_reference(reference)?;
		}
		self.visit_expr(&define.comment)
	}

	fn visit_define_event(&mut self, define: &DefineEventStatement) -> Result<(), Self::Error> {
		self.visit_expr(&define.name)?;
		self.visit_expr(&define.target_table)?;
		self.within_rows(EVENT_IMAGES, true, |walker| {
			walker.visit_expr(&define.when)?;
			for then in &define.then {
				walker.visit_expr(then)?;
			}
			Ok::<(), Infallible>(())
		})?;
		self.visit_expr(&define.comment)
	}
}
