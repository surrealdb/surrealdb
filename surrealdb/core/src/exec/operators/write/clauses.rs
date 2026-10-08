//! The statement's own clauses, compiled once and shared by every record.
//!
//! A stage is addressed by a [`StageId`](super::stage::StageId), which is
//! `Copy` and carries no expressions, so a stage that needs the statement's
//! text reads it from here through [`WriteCtx`](super::ctx::WriteCtx). These
//! are statement-constant: compiled when the operator is built, never per
//! record, because compiling per record would re-plan a user expression once
//! per row.

use std::sync::Arc;

use super::place::WritePlace;
use crate::exec::operators::FieldSelection;
use crate::exec::physical_expr::PhysicalExpr;
use crate::expr::AssignOperator;

/// A data clause, compiled.
///
/// The shapes are the ones that survive to evaluation — the same six
/// `ComputedData` carries — rather than the ten
/// the parser distinguishes. `INSERT`'s `VALUES` and single-object forms are
/// absent because they produce the statement's *subjects* rather than a clause
/// applied to one, and reach the operator as its source.
#[derive(Debug)]
pub(crate) enum WriteData {
	/// `SET a = 1, b += 2`, and `ON DUPLICATE KEY UPDATE`'s assignments.
	Set(Vec<WriteAssignment>),
	/// `UNSET a, b`. Each path is a place: a constant one carries nothing to
	/// evaluate, and one that filters or subscripts carries the expressions it
	/// embeds, compiled like any other.
	Unset(Vec<WritePlace>),
	/// `CONTENT { … }` — replaces the record.
	Content(Arc<dyn PhysicalExpr>),
	/// `REPLACE { … }` — replaces the record.
	Replace(Arc<dyn PhysicalExpr>),
	/// `MERGE { … }` — merges into the record.
	Merge(Arc<dyn PhysicalExpr>),
	/// `PATCH [ … ]` — a JSON Patch document.
	Patch(Arc<dyn PhysicalExpr>),
}

/// One assignment of a `SET` clause, compiled.
///
/// The right-hand side is evaluated against the pre-mutation snapshot before
/// any assignment is applied, which is what makes `SET a = a + 1, b = a + 1`
/// assign `b` from the old `a`. The operator is applied when the value is
/// written, not when it is evaluated.
#[derive(Debug)]
pub(crate) struct WriteAssignment {
	/// The place assigned to: where in the record the value goes, with the
	/// expressions its path embeds compiled.
	pub(crate) place: WritePlace,
	/// Plain assignment, or one of the compound forms.
	pub(crate) operator: AssignOperator,
	/// The right-hand side.
	pub(crate) value: Arc<dyn PhysicalExpr>,
}

/// `RETURN <fields>`, compiled.
///
/// The two shapes are the projection's, not the clause's: `RETURN VALUE e`
/// makes the row the bare value, and every other field list builds an object.
/// The other `RETURN` forms — `NONE`, `NULL`, `BEFORE`, `AFTER`, `DIFF` — name
/// an image rather than a projection and carry nothing to compile.
///
/// Each field is the read side's [`FieldSelection`]: the output path the value
/// lands at, and the expression that produces it. A write's `RETURN` and a
/// `SELECT`'s projection name their fields by the same rules, so they are
/// compiled into the same shape and evaluated by the same code.
#[derive(Debug)]
pub(crate) enum WriteProjection {
	/// `RETURN VALUE e`: the row is `e`, with no object around it.
	Value(Arc<dyn PhysicalExpr>),
	/// `RETURN a, b AS c`, and `RETURN *`: an object per record.
	Fields {
		/// The named fields, in the order they were written.
		fields: Vec<FieldSelection>,
		/// Whether the list held `*`, which seeds the object with the record
		/// so a named field lands on top of it rather than beside it.
		include_all: bool,
	},
}

/// Everything a stage needs from the statement, as opposed to from the record.
///
/// Held behind an `Arc` on every [`WriteCtx`](super::ctx::WriteCtx) a statement
/// resolves, so a statement writing to several tables compiles its clauses
/// once.
#[derive(Debug, Default)]
pub(crate) struct WriteClauses {
	/// The `WHERE` condition, on the forms that take one: `UPDATE`, `UPSERT`
	/// and `DELETE`. It is judged per record *before* the data clause is
	/// evaluated, so a data clause with side effects runs only for records the
	/// condition accepts — which is what keeps a full-scan plan and an
	/// index-backed plan observably equivalent.
	pub(crate) cond: Option<Arc<dyn PhysicalExpr>>,
	/// The data clause, on every form that takes one. `None` for a bare
	/// `CREATE t` or `DELETE`.
	pub(crate) data: Option<WriteData>,
	/// `INSERT … ON DUPLICATE KEY UPDATE`: the clause applied when the insert
	/// conflicts and is retried as an update. Distinct from `data`, which for
	/// an `INSERT` describes the rows themselves.
	pub(crate) duplicate_update: Option<WriteData>,
	/// `RETURN <fields>`, compiled. `None` for every other `RETURN` form and
	/// for a statement with no `RETURN` at all; which form the statement wrote
	/// is [`WriteCtx::output`](super::ctx::WriteCtx), and this is what that
	/// form needs when it is a projection.
	pub(crate) output_fields: Option<WriteProjection>,
}
