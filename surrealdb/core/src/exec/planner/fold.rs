//! What the VM's mapping reads of a relational pipeline: its clauses, and what a chain of
//! permission and layer nodes over a table source offers the access path.
//!
//! # The relational pipeline
//!
//! A relational tree is read by walking *down* from its top, collecting the
//! clauses the access-path chooser needs — the predicate, ordering, paging,
//! projection, fetch, version, timeout, `ONLY`, split and grouping — into a
//! [`PipelineClauses`], until the walk reaches either a leaf (`Source`,
//! `Values`, `Subquery`, `UnionAll`, `Match`) or a node it does not collect
//! ([`collect_pipeline`]). The mapping then maps the leaf with the whole
//! clause set in hand, through the access-path machinery in `select`, and puts
//! the clauses back on in pipeline order.
//!
//! Clauses are collected in the order a `SELECT` pipeline applies them, from
//! `ONLY` at the top down to the `WHERE` just above the leaf, and each at most
//! once. A clause met out of that order, a node the walk does not collect
//! (`PermissionFilter`, a projection layer, `Distinct`), or a statement in
//! relational position stops the collection: the node becomes the source the
//! clauses collected above it apply to — every one of them, in full. Nothing
//! collected above such a node is pushed beneath it as a per-row filter, a
//! limit or a threshold, which is what upholds the barrier rule for
//! `PermissionFilter` and the layers.
//!
//! A projection layer ([`LogicalOp::is_layer`]) is a `Project` of the form
//! `*, e₁ AS n₁, …` whose every column evaluates in a frame that is not the
//! caller's: the analyzer's encoding of a table's `COMPUTED` fields and field
//! `SELECT` permissions, stacked directly above the table's `PermissionFilter`.
//! The stack is the pipeline's source chain, not a projection stage, so the
//! walk stops at a layer as it stops at a `PermissionFilter`; a `Project` that
//! is not a layer is the user's and is collected as the projection clause.
//!
//! The barrier protects evaluation, not access-path choice. A chain of
//! `PermissionFilter` and layer nodes directly over a table `Source` — with
//! the filter the binding rules placed inside it, holding the conjuncts that
//! read no layer column — is offered the pipeline's predicate and ordering by
//! access-path selection exactly as a bare leaf is: an index lookup reads
//! keys, and the nodes above still judge every row the access path yields.
//! What the offer withholds, and why, is stated where the crossing happens, in
//! [`chain_offer`].
//!
//! # Joins
//!
//! A [`LogicalOp::Join`] is a node the walk does not collect: the clauses
//! above it apply to the joined rows in full. The conjuncts of the pipeline's
//! predicate that read the left row alone are moved beneath the join by the
//! binding rules, as a filter over the left input, so the left input is
//! offered no predicate of the pipeline's; its ordering is offered, less every
//! key that reads the join's alias or the row opaquely, since a join adds to
//! or removes from the left rows without changing them. A semi or anti join
//! lifted out of a table's `PERMISSIONS FOR select` predicate stands in the
//! source chain between the table's layers and its `PermissionFilter`, and is
//! one of the chain's nodes: it withholds nothing from the offer and
//! contributes nothing to it.

// Adding a `LogicalOp` variant must break the walk until it states whether the
// variant is collected, which a wildcard arm would silently absorb.
#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;
use std::sync::Arc;

use super::Planner;
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::reads::{reads_beneath, reads_row_opaquely};
use crate::expr::logical::{
	EvalMode, FieldPath, JoinKind, LogicalExpr, LogicalFilter, LogicalJoin, LogicalOp,
	LogicalPermissionFilter, LogicalProject, LogicalSource, Schema, SourceRef,
};
use crate::val::{Strand, TableName};

/// Whether `join` is a semi or anti join lifted out of a permission
/// predicate, which stands in a table's source chain.
fn is_permission_join(join: &LogicalJoin) -> bool {
	join.frame.mode != EvalMode::Caller && matches!(join.kind, JoinKind::Semi | JoinKind::Anti)
}

/// The table whose rows reach a `PermissionFilter` over `input`, and the
/// schema the analyzer resolved for it: the resolved `Source` beneath the
/// permission, layer, filter and permission-join nodes the analyzer and the
/// rules stack over it. `None` for any other shape, which carries no resolved
/// schema to name the table's computed fields.
pub(crate) fn permission_subject(input: &LogicalOp) -> Option<(TableName, Arc<Schema>)> {
	let mut cur = input;
	loop {
		cur = match cur {
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				..
			})
			| LogicalOp::Filter(LogicalFilter {
				input,
				..
			}) => input,
			LogicalOp::Project(LogicalProject {
				input,
				..
			}) if cur.is_layer() => input,
			LogicalOp::Join(join) if is_permission_join(join) => join.left(),
			LogicalOp::Source(LogicalSource {
				source,
				schema: Some(schema),
				..
			}) => {
				let table = match source {
					SourceRef::Table(table) => table.clone(),
					SourceRef::RecordId(rid) => rid.table.clone(),
					SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => {
						return None;
					}
				};
				return Some((table, Arc::clone(schema)));
			}
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_) => return None,
		};
	}
}

/// The computed fields of `schema` a table permission predicate reads, as
/// [`ComputedFieldGuard::for_reads`](crate::exec::permission::ComputedFieldGuard::for_reads) takes
/// them: `None` when it reads none of them, `Some(None)` when it reads the row other than by name
/// ([`reads_row_opaquely`]), so any of them may be read, and otherwise the
/// names it reads.
pub(crate) fn computed_reads(
	predicate: &LogicalExpr,
	schema: &Schema,
	registry: &FunctionRegistry,
) -> Option<Option<HashSet<String>>> {
	let computed: Vec<&Strand> = schema
		.fields
		.iter()
		.filter(|field| field.computed.is_some())
		.filter_map(|field| field.path.as_field())
		.collect();
	if computed.is_empty() {
		return None;
	}
	if reads_row_opaquely(predicate, registry) {
		return Some(None);
	}
	let reads: HashSet<String> = computed
		.into_iter()
		.filter(|name| reads_beneath(predicate, &[FieldPath::field((*name).clone())]))
		.map(|name| name.as_str().to_owned())
		.collect();
	(!reads.is_empty()).then_some(Some(reads))
}

impl Planner<'_> {}

/// Whether a statement changes the catalog the statements after it would be
/// compiled against.
///
/// A schema statement does, wherever it sits inside the statement: one reached
/// through a block, a `FOR` body or a subquery has still run by the time the
/// next statement of the enclosing block does.
pub(crate) fn changes_the_catalog(op: &LogicalOp) -> bool {
	if matches!(op, LogicalOp::Ddl(_)) {
		return true;
	}
	let mut found = false;
	let _ = op.visit_ops_deep(&mut |node| {
		if matches!(node, LogicalOp::Ddl(_)) {
			found = true;
			return std::ops::ControlFlow::Break(());
		}
		std::ops::ControlFlow::Continue(())
	});
	found
}
