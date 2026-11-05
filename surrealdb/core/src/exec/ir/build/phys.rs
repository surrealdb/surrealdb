//! Phys attribute shapes for the planner's access and payload types.
//!
//! Each function maps a planner decision (an index access, a key range, a bitmap or a count) or a
//! runtime payload type (the structure an expression sits in, and a flag or count for each
//! expression that becomes a region) to its shape. No shape holds an expression. An index is named
//! by [`IndexRef`], its table and its name, and the physical catalog resolves it again.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use crate::exec::config::DEFAULT_SCAN_BATCH_BYTES;
use crate::exec::index::access_path::{self, BitmapPlan};
use crate::exec::index::id_range::{IdBound, IdRangeAccess};
use crate::exec::index::key_range::{KeyBound, KeyEq, KeyRangeAccess, KeyRangeBound};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::IndexRef;
use crate::exec::ir::iface::statements::OperandSlot;
use crate::exec::ir::phys::ops::{
	Batch, BitmapShape, BranchReadShape, ComputedReadsShape, DerivedReads, IdBoundsShape,
	IndexBranchShape, KeyBoundShape, KeyEqShape, KeyRangeBoundShape, KeyRangeOnShape,
	KeyRangeShape, KnnFilterShape, KnnPermissionShape, KnnPrefilterShape, ProjectedFieldShape,
	ReadClauses,
};
use crate::exec::vm::payload::{BranchRead, ComputedReads, IndexBranch, KnnFilter, ProjectedField};
use crate::expr::logical::LogicalExpr;

/// The bounds of a loop that takes one iteration at a time, as the planner's `FOR` runs.
///
/// The conversion of every `FOR` carries them
/// ([`Placement::batch_of`](crate::exec::ir::phys::placement::Placement::batch_of)).
/// The placement policy states the bounds the loops of the other kinds are fanned out under.
/// The byte bound is the default scan batch, so a fill's claim on the statement's read buffer is
/// bounded; a fill always keeps its first hit, so one iteration's record never needs more.
pub(crate) const UNBATCHED: Batch = Batch {
	max_iterations: 1,
	max_bytes: DEFAULT_SCAN_BATCH_BYTES as u32,
};

// ---------------------------------------------------------------------------------------------
// Expression-free mirrors
// ---------------------------------------------------------------------------------------------

/// The IR's reference to the index `index` names: its table and name, its position among the
/// table's online indexes and the columns known to hold no array.
pub(crate) fn index_ref(index: &access_path::IndexRef) -> IndexRef {
	IndexRef::of(index.definition(), index.idx, index.scalar_columns())
}

/// What the read of a plan node keeps of its source: the schema the node holds. The `WITH` hint
/// is not kept, because the access path is chosen before the read is built.
fn plan_read(schema: &Option<std::sync::Arc<crate::expr::logical::Schema>>) -> ReadClauses {
	ReadClauses {
		schema: schema.clone(),
		with: None,
	}
}

/// The shape of one bound of a key range; a deferred bound takes the next slot of `operands`.
fn key_bound_shape<'a>(
	bound: &'a KeyBound,
	operands: &mut Vec<&'a LogicalExpr>,
) -> Result<KeyBoundShape, IrError> {
	match bound {
		KeyBound::Literal(value) => Ok(KeyBoundShape::Literal(value.clone())),
		KeyBound::Deferred(expr) => {
			let slot = OperandSlot::try_from(operands.len())
				.map_err(|_| IrError::Capacity("key-range operand bounds"))?;
			operands.push(expr);
			Ok(KeyBoundShape::Operand(slot))
		}
	}
}

/// The shape of a key-range access, and the expressions of its deferred bounds in operand-slot
/// order: the equality run by position, each `IN` element in written order, then the low and the
/// high bound of the range. The n-th deferred bound holds slot n, so the expressions are the
/// operands of the `phys.key_range_scan` the shape belongs to.
///
/// `Err(IrError::Capacity)`: the access holds more deferred bounds than a `u16` counts.
pub(crate) fn key_range_shape(
	access: &KeyRangeAccess,
) -> Result<(KeyRangeShape, Vec<&LogicalExpr>), IrError> {
	/// The shape of one side of a range.
	fn side<'a>(
		side: &'a Option<KeyRangeBound>,
		operands: &mut Vec<&'a LogicalExpr>,
	) -> Result<Option<KeyRangeBoundShape>, IrError> {
		side.as_ref()
			.map(|side| {
				Ok(KeyRangeBoundShape {
					bound: key_bound_shape(&side.bound, operands)?,
					inclusive: side.inclusive,
				})
			})
			.transpose()
	}
	let mut operands: Vec<&LogicalExpr> = Vec::new();
	let mut eq = Vec::with_capacity(access.eq.len());
	for position in &access.eq {
		eq.push(match position {
			KeyEq::One(one) => KeyEqShape::One(key_bound_shape(one, &mut operands)?),
			KeyEq::In(many) => {
				let mut held = Vec::with_capacity(many.len());
				for one in many {
					held.push(key_bound_shape(one, &mut operands)?);
				}
				KeyEqShape::In(held)
			}
		});
	}
	let range = match &access.range {
		None => None,
		Some(range) => {
			let low = side(&range.low, &mut operands)?;
			let high = side(&range.high, &mut operands)?;
			Some(KeyRangeOnShape {
				position: range.position,
				low,
				high,
			})
		}
	};
	let shape = KeyRangeShape {
		key: Arc::clone(&access.key),
		eq,
		range,
		direction: access.direction,
		delivers: access.delivers,
		ordered_by: access.ordered_by.clone(),
	};
	Ok((shape, operands))
}

/// The shape of the record-id bounds of a table scan, and the expressions of its deferred bounds
/// in operand-slot order: the low bound, then the high. The n-th deferred bound holds slot n, so
/// the expressions are the operands of the `phys.table_scan` the shape belongs to.
///
/// `Err(IrError::Capacity)`: the bounds hold more deferred bounds than a `u16` counts.
pub(crate) fn id_bounds_shape(
	access: &IdRangeAccess,
) -> Result<(IdBoundsShape, Vec<&LogicalExpr>), IrError> {
	/// The shape of one side of the bounds.
	fn side<'a>(
		side: &'a Option<IdBound>,
		operands: &mut Vec<&'a LogicalExpr>,
	) -> Result<Option<KeyRangeBoundShape>, IrError> {
		side.as_ref()
			.map(|side| {
				Ok(KeyRangeBoundShape {
					bound: key_bound_shape(&side.bound, operands)?,
					inclusive: side.inclusive,
				})
			})
			.transpose()
	}
	let mut operands = Vec::new();
	let low = side(&access.low, &mut operands)?;
	let high = side(&access.high, &mut operands)?;
	Ok((
		IdBoundsShape {
			low,
			high,
		},
		operands,
	))
}

/// The shape of a fused bitmap plan, the tree of its leaves and set operations.
pub(crate) fn bitmap_shape(plan: &BitmapPlan) -> BitmapShape {
	match plan {
		BitmapPlan::BTree {
			index_ref: index,
			access,
		} => BitmapShape::BTree {
			index: index_ref(index),
			access: access.clone(),
		},
		BitmapPlan::FullText {
			index_ref: index,
			query,
			operator,
		} => BitmapShape::FullText {
			index: index_ref(index),
			query: query.clone(),
			operator: operator.clone(),
		},
		BitmapPlan::Graph {
			source,
			direction,
			edge_tables,
		} => BitmapShape::Graph {
			source: source.clone(),
			direction: *direction,
			edge_tables: edge_tables.clone(),
		},
		BitmapPlan::And(children) => BitmapShape::And(children.iter().map(bitmap_shape).collect()),
		BitmapPlan::Or(children) => BitmapShape::Or(children.iter().map(bitmap_shape).collect()),
		BitmapPlan::AndNot {
			base,
			subtract,
		} => BitmapShape::AndNot {
			base: Box::new(bitmap_shape(base)),
			subtract: Box::new(bitmap_shape(subtract)),
		},
	}
}

/// The shape of one scan of an index union.
pub(crate) fn index_branch_shape(branch: &IndexBranch) -> IndexBranchShape {
	IndexBranchShape {
		index: index_ref(&branch.index),
		read: match &branch.read {
			BranchRead::BTree(access) => BranchReadShape::BTree(access.clone()),
			BranchRead::FullText {
				query,
				operator,
			} => BranchReadShape::FullText {
				query: query.clone(),
				operator: operator.clone(),
				column: branch.index.cols.first().cloned(),
				shared_doc_ids: branch.index.uses_shared_doc_ids(),
			},
		},
		direction: branch.direction,
	}
}

/// The shape of what a KNN search gates each candidate on. The predicates are regions: one per
/// permission, then the residual, the `MATCHES` condition and the prefilter's residual, each
/// where the shape says it is present.
pub(crate) fn knn_filter_shape(filter: &KnnFilter) -> KnnFilterShape {
	KnnFilterShape {
		permissions: filter
			.permissions
			.iter()
			.map(|(_, auth, guard, computed)| KnnPermissionShape {
				auth: auth.clone(),
				guard: *guard,
				computed: computed.as_ref().map(computed_reads_shape),
			})
			.collect(),
		residual: filter.residual.is_some(),
		matches_condition: filter.matches_condition.is_some(),
		prefilter: filter.prefilter.as_ref().map(|prefilter| KnnPrefilterShape {
			root: bitmap_shape(&prefilter.root),
			residual: prefilter.residual.is_some(),
			uncovered_matches: prefilter.uncovered_matches,
		}),
	}
}

/// The shape of a plan's computed reads. A plan names the fields read but not their definitions,
/// so `derived` is `LateBound` and the lowering plans the statement again when it reaches the read.
fn computed_reads_shape(computed: &ComputedReads) -> ComputedReadsShape {
	ComputedReadsShape {
		table: computed.table.clone(),
		fields: computed.fields.clone(),
		derived: DerivedReads::LateBound,
	}
}

fn projected_field_shape(field: &ProjectedField) -> ProjectedFieldShape {
	ProjectedFieldShape {
		output: field.output.clone(),
		aliased: field.aliased,
		sort_key: field.sort_key,
	}
}
