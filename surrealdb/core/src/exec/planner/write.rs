//! Compiling a mutation into its write operator.
//!
//! A mutation is one operator holding an ordered list of per-record stages, so
//! there is nothing here that resembles the pipeline a read compiles to. What
//! this does is settle three things before any record exists: which orders the
//! statement runs, what its clauses compile to, and where its subjects come
//! from. See `doc/adr/0001-streaming-write-path.md`.
//!
//! The statement's clauses are compiled **once** and shared by every record and
//! every table the statement writes to. Nothing a user wrote is planned per
//! row.

use crate::exec::operators::write::plan::{WriteFacts, WriteForm};
use crate::expr::logical::{
	LogicalData, LogicalExpr, LogicalOp, LogicalSource, LogicalSubquery, LogicalUnionAll,
	LogicalValues, LogicalWrite, LogicalWriteStatement, SourceRef,
};

/// Whether every leaf of a mutation's row source is a target the statement
/// classifies itself rather than a source the pipeline reads.
///
/// A value literal and a dynamic expression the planner compiles to a bare
/// scalar are both of them: neither resolves to a table at bind time, so
/// neither carries a schema, a layer stack or a permission node, and there is
/// nothing in the pipeline's reading of them that the write needs.
///
/// A source that becomes a value only when the statement runs — a call, a
/// closure applied to its arguments — is one too. What it answers with is
/// classified by the form that wrote it rather than read as a row: an object
/// carrying an `id` is the stored record that `id` names, and a table name is
/// scanned under the statement's own clause rather than the reader's `SELECT`.
/// That is what [`crate::exec::operators::write`] does with every target, so
/// naming one through a call needs nothing further.
///
/// A subquery is one as well, and so is a parameter the binding could not
/// resolve: one a block `LET` binds, or one bound per record or per iteration,
/// as a `FOR` variable or the referencing record a delete cascades to is. What
/// they answer with are values, not records as stored: a subquery's projection
/// chose the fields of its rows, and the caller's field permissions removed the
/// ones the caller may not see. Taking such a row as the record would store it
/// in place of the record, deleting every field it lacks. So each is evaluated
/// once, before the statement writes anything, a subquery as the read it is,
/// and each value is classified like any other target: a record id, or an
/// object carrying one, names the record that the write reads from storage for
/// itself.
///
/// All or nothing, so a list mixing the two keeps the one reading it has.
pub(crate) fn classified_targets(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Values(LogicalValues {
			for_update,
			..
		}) => !for_update,
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Dynamic(_) | SourceRef::Param(_),
			for_update,
			..
		}) => !for_update,
		LogicalOp::Subquery(_) => true,
		LogicalOp::UnionAll(LogicalUnionAll {
			inputs,
		}) => !inputs.is_empty() && inputs.iter().all(classified_targets),
		_ => false,
	}
}

/// The target expressions of a source [`classified_targets`] admitted, in the
/// order the statement wrote them.
pub(crate) fn target_exprs(op: LogicalOp) -> Vec<LogicalExpr> {
	match op {
		LogicalOp::Values(LogicalValues {
			expr,
			..
		})
		| LogicalOp::Source(LogicalSource {
			source: SourceRef::Dynamic(expr),
			..
		}) => vec![expr],
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Param(param),
			..
		}) => vec![LogicalExpr::Param(param)],
		LogicalOp::Subquery(LogicalSubquery {
			input,
		}) => vec![LogicalExpr::Subquery(input)],
		LogicalOp::UnionAll(LogicalUnionAll {
			inputs,
		}) => inputs.into_iter().flat_map(target_exprs).collect(),
		other => unreachable!("a source {other} the classification did not admit"),
	}
}

/// What the statement contributes to the shape of its orders.
///
/// Read once, when the orders are assembled: a difference a record decides is
/// branching, and a difference the statement decides is membership in a list.
pub(crate) fn write_facts(write: &LogicalWrite) -> WriteFacts {
	let statement = &write.statement;
	let form = match statement {
		LogicalWriteStatement::Create(_) => WriteForm::Create,
		LogicalWriteStatement::Update(_) => WriteForm::Update,
		LogicalWriteStatement::Upsert(_) => WriteForm::Upsert,
		LogicalWriteStatement::Delete(_) => WriteForm::Delete,
		LogicalWriteStatement::Insert(_) => WriteForm::Insert,
		LogicalWriteStatement::Relate(_) => WriteForm::Relate,
	};
	// Read from the lowered clauses, which are what the planner compiles. The
	// statement beside them is the surface form, and a rule that rewrote a
	// clause changed only the lowered one — so asking the statement would
	// build a stage list for a clause the plan does not run.
	WriteFacts {
		form,
		has_data: write.data.is_some(),
		on_duplicate_key_update: write.duplicate_update.is_some(),
		ignore: match statement {
			LogicalWriteStatement::Insert(stmt) => stmt.ignore,
			_ => false,
		},
		repeatable: matches!(statement, LogicalWriteStatement::Upsert(_))
			&& write.cond.is_none()
			&& matches!(
				write.data,
				Some(
					LogicalData::Set(_)
						| LogicalData::Unset(_)
						| LogicalData::Merge(_)
						| LogicalData::Patch(_)
				)
			),
	}
}
