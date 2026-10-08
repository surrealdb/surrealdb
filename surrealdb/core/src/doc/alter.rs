//! The shape of an evaluated data clause.

use std::sync::Arc;

use crate::expr::{AssignOperator, Idiom};
use crate::val::Value;

/// The result of evaluating a statement's data clause once, so that the steps
/// that follow (permission checks, applying the clause to the record,
/// `$input` materialisation, the upsert retry) can reuse it without re-running
/// any user-supplied expression.
///
/// Each variant mirrors a SurrealQL data-clause form:
/// - [`ComputedData::Patch`] — `PATCH [{op: "...", path: "...", value: ...}, …]`
/// - [`ComputedData::Merge`] — `MERGE {…}`
/// - [`ComputedData::Replace`] — `REPLACE {…}`
/// - [`ComputedData::Content`] — `CONTENT {…}`
/// - [`ComputedData::Unset`] — `UNSET field, …`
/// - [`ComputedData::Set`] — `SET …` / `ON DUPLICATE KEY UPDATE …`
///
/// The materialised value variants store an `Arc<Value>` so the payload can
/// be cheaply shared between the cached entry and the consumers that need it
/// by value.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum ComputedData {
	Patch(Arc<Value>),
	Merge(Arc<Value>),
	Replace(Arc<Value>),
	Content(Arc<Value>),
	Unset(Vec<Idiom>),
	Set(Vec<ComputedAssignment>),
}

/// A single pre-evaluated assignment from a `SET …` / `ON DUPLICATE KEY
/// UPDATE …` data clause, carried by [`ComputedData::Set`].
///
/// The right-hand side has already been evaluated against the record, so
/// re-applying the assignment (e.g. on the upsert retry path) does not re-run
/// any user-supplied expression.
///
/// Fields:
/// - `place` — target field path (the left-hand `Idiom`).
/// - `operator` — how the value combines with the existing field (plain `=`, compound
///   `+=`/`-=`/`+?`).
/// - `value` — the already-evaluated right-hand side.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ComputedAssignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Value,
}
