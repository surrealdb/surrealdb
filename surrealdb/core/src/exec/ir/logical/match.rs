//! The `MATCH` statement family.
//!
//! A `MATCH` query is one `logical.match` op with one `Subquery` region per stage, in order, and
//! every binding in scope is a block argument of each stage region, which holds the stage's op. A
//! read stage holds one `Where` clause region per predicate; update, delete and insert stages hold
//! the write clauses of their targets; the output stage holds the projection.
//!
//! A binding is in scope for the stage that follows the clause or stage that introduces it. The
//! scope of a read stage includes the bindings its own patterns introduce, so its predicates
//! read them; the output stage sees every binding.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::{MatchInsertAttrs, MatchUpdateAttrs};
use crate::exec::ir::iface::statements::{
	MatchColumnShape, MatchData, MatchInsertEdge, MatchInsertNode, MatchOutputShape, MatchPattern,
};
use crate::expr::match_plan::{
	BindingId, InsertStage, MatchClausePlan, MatchOutput, MutationStage, PatternPlan, UpdateData,
};

/// The bindings the patterns of `clause` introduce, in the order they are written.
pub(crate) fn clause_bindings(clause: &MatchClausePlan) -> Vec<BindingId> {
	let mut out = Vec::new();
	for pattern in &clause.patterns {
		out.extend(pattern.path_var);
		out.push(pattern.start.binding);
		for (edge, node) in &pattern.steps {
			out.push(edge.binding);
			out.push(node.binding);
		}
	}
	out
}

/// The bindings an insert stage creates: its nodes, then its edges.
pub(crate) fn insert_bindings(stage: &InsertStage) -> Vec<BindingId> {
	stage
		.nodes
		.iter()
		.map(|node| node.binding)
		.chain(stage.edges.iter().map(|edge| edge.binding))
		.collect()
}

/// `scope` with `added` appended, each binding once, and sorted.
pub(crate) fn extend_scope(scope: &mut Vec<BindingId>, added: impl IntoIterator<Item = BindingId>) {
	for binding in added {
		if !scope.contains(&binding) {
			scope.push(binding);
		}
	}
	scope.sort_unstable();
}

/// A pattern as owned data.
pub(crate) fn pattern_of(pattern: &PatternPlan) -> MatchPattern {
	MatchPattern {
		path_var: pattern.path_var,
		search: pattern.search,
		start: pattern.start.clone(),
		steps: pattern.steps.clone().into_boxed_slice(),
	}
}

/// The patterns of a read stage as owned data.
pub(crate) fn patterns_of(clause: &MatchClausePlan) -> Box<[MatchPattern]> {
	clause.patterns.iter().map(pattern_of).collect()
}

/// The attributes of an update or delete stage's data: the names it sets or unsets, or the
/// content that replaces the properties.
pub(crate) fn update_attrs(target: BindingId, data: &UpdateData) -> MatchUpdateAttrs {
	MatchUpdateAttrs {
		target,
		data: match data {
			UpdateData::Set(assignments) => {
				MatchData::Set(assignments.iter().map(|(name, _)| name.clone()).collect())
			}
			UpdateData::Unset(names) => MatchData::Unset(names.clone().into_boxed_slice()),
			UpdateData::Content(_) => MatchData::Content,
		},
	}
}

/// The attributes of an insert stage.
pub(crate) fn insert_attrs(stage: &InsertStage) -> MatchInsertAttrs {
	MatchInsertAttrs {
		nodes: stage
			.nodes
			.iter()
			.map(|node| MatchInsertNode {
				binding: node.binding,
				label: node.label.clone(),
			})
			.collect(),
		edges: stage
			.edges
			.iter()
			.map(|edge| MatchInsertEdge {
				binding: edge.binding,
				label: edge.label.clone(),
				from: edge.from,
				to: edge.to,
			})
			.collect(),
	}
}

/// The attributes of the output stage.
pub(crate) fn output_shape(output: &MatchOutput) -> MatchOutputShape {
	MatchOutputShape {
		columns: output
			.columns
			.iter()
			.map(|column| MatchColumnShape {
				name: column.name.clone(),
				hidden: column.hidden,
			})
			.collect(),
		distinct: output.distinct,
		order: output.order.iter().map(|order| order.ascending).collect(),
	}
}

/// Whether `stage` is a write stage.
pub(crate) fn is_write(stage: &MutationStage) -> bool {
	match stage {
		MutationStage::Update {
			..
		}
		| MutationStage::Delete {
			..
		}
		| MutationStage::Insert(_) => true,
	}
}
