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
use crate::expr::logical::{LogicalData, LogicalWrite, LogicalWriteStatement};

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
	// statement holds the surface form. A rewrite changes only the lowered
	// clause, so reading the statement would build a stage list for a clause
	// the plan does not run.
	WriteFacts {
		form,
		has_data: write.data.is_some(),
		on_duplicate_key_update: write.duplicate_update.is_some(),
		ignore: if let LogicalWriteStatement::Insert(stmt) = statement {
			stmt.ignore
		} else {
			false
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
