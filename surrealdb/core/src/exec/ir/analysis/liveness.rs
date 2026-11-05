//! Field liveness: which fields of a row an op reads, so the physical plan reads no more.
//!
//! The demand of a loop is read from every region of its body, frames and clauses included. A row
//! is the loop's element and every value that carries it on: the result of an op that passes its
//! input row through, and the block arguments of the regions of an op that reads its input row
//! through them. A field read over a row adds the field to the stored demand; a use of a row that
//! is not a field read needs the whole record. A demand can only be too large: an op the analysis
//! does not recognise as a field read takes the whole row.
//!
//! Field guards and the fields a `COMPUTED` definition reads come from the catalog, which holds no
//! such facts for the analysis: reading a `COMPUTED` field demands every stored field, and no
//! guard is reported. No `COMPUTED` derivation is dropped, so derivation stays demand-driven.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;
use surrealdb_strand::Strand;

use super::multiplicity;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::effects::FieldSet;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, LogOp, LoopKind};
use crate::exec::ir::op_kind::OpKind;

/// What a loop reads of the rows it visits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FieldDemand {
	/// The fields of the stored row some region reads; `FieldSet::All` for a whole-row use.
	pub(crate) stored: FieldSet,
	/// The `COMPUTED` fields some op reads, which are derived on demand.
	pub(crate) computed: SmallVec<[Strand; 4]>,
	/// The fields whose select guard must run. Empty: guards are catalog facts.
	pub(crate) guarded: SmallVec<[Strand; 4]>,
}

impl FieldDemand {
	/// The demand of a loop nothing is known about: the whole row.
	pub(crate) fn top() -> Self {
		Self {
			stored: FieldSet::All,
			computed: SmallVec::new(),
			guarded: SmallVec::new(),
		}
	}
}

/// The demand over the rows the loop `loop_op` visits.
///
/// A loop that writes demands the whole record, since the stages of a write read and rewrite it.
/// A dangling id, an id that is not a loop and a loop without an element answer
/// [`FieldDemand::top`]. Never memoised.
pub(crate) fn of(ir: &Ir, _facts: &mut FactCache, loop_op: OpId) -> FieldDemand {
	let Some(data) = ir.op(loop_op) else {
		return FieldDemand::top();
	};
	let Some(kind) = multiplicity::loop_kind(ir, loop_op) else {
		return FieldDemand::top();
	};
	if kind == LoopKind::Write {
		return FieldDemand::top();
	}
	let Some(element) = multiplicity::loop_element(ir, loop_op) else {
		return FieldDemand::top();
	};
	let regions: SmallVec<[RegionId; 2]> = data.regions.iter().copied().collect();
	demand_over(ir, &[element], &regions)
}

/// The demand over the rows `seeds` name, read from every op of `regions` and of the regions
/// beneath them.
pub(super) fn demand_over(ir: &Ir, seeds: &[ValueId], regions: &[RegionId]) -> FieldDemand {
	let mut state = State {
		rows: seeds.iter().copied().collect(),
		names: SmallVec::new(),
		computed: SmallVec::new(),
		all: false,
	};
	for region in regions.iter().copied() {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			state.visit(ir, op);
			Walk::Continue
		});
	}
	state.finish()
}

/// The demand over the rows `rows` that the runs of physical streaming ops `runs` read, each run
/// the rows it starts from and its ops in data-flow order, the first starting from `rows`.
///
/// A run continues the one before it: where the rows that run ends with are the record, so are
/// the rows the next one starts from (the element of a loop over them, or the rows the loop
/// yields). A step that keeps the row (`phys.filter`, `phys.permission_filter`, `phys.layer`,
/// `phys.limit`, `phys.sort`) reads it through the block arguments of its regions and delivers it
/// on, a sort reading a path key off the row as a read of the path's first field; a
/// `phys.project` reads it through its regions and delivers a value of its own, unless its shape
/// keeps the row whole (`*`, a sort key, or an omission); a `phys.aggregate` does the same through
/// its key and field regions and delivers its groups, so a `count()` reads no field of the row.
/// The record id of a row (`expr.doc`) is
/// a read of `id`. Any other step over the row, a step that takes the row as more than its input,
/// and a record the last run ends with, which the pipeline hands on, need the whole record.
pub(crate) fn pipeline_demand(ir: &Ir, rows: ValueId, runs: &[(ValueId, &[OpId])]) -> FieldDemand {
	use crate::exec::field_path::FieldPathPart;
	use crate::exec::ir::phys::ops::{PhysOp, ProjectionShape, SortKeyReadShape};
	let mut state = State {
		rows: FxHashSet::from_iter([rows]),
		names: SmallVec::new(),
		computed: SmallVec::new(),
		all: false,
	};
	let mut last: Option<ValueId> = None;
	'runs: for (from, steps) in runs.iter().copied() {
		if last.is_some_and(|last| state.rows.contains(&last)) {
			state.rows.insert(from);
		}
		last = Some(from);
		for step in steps.iter().copied() {
			let Some(data) = ir.op(step) else {
				state.all = true;
				break 'runs;
			};
			last = data.results.first().copied();
			let first_is_row =
				data.operands.first().is_some_and(|operand| state.rows.contains(operand));
			let later_is_row =
				data.operands.iter().skip(1).any(|operand| state.rows.contains(operand));
			if !first_is_row && !later_is_row {
				continue;
			}
			let (keeps, whole) = match &data.kind {
				OpKind::Phys(
					PhysOp::Filter
					| PhysOp::PermissionFilter(_)
					| PhysOp::Layer(_)
					| PhysOp::Limit {
						..
					},
				) => (true, false),
				// A window reads every key and argument through its regions and delivers the row
				// on, with its calls' fields appended.
				OpKind::Phys(PhysOp::Window(_)) => (true, false),
				// A sort reads its computed keys through its regions and its path keys off the
				// row, and delivers the row on.
				OpKind::Phys(PhysOp::Sort(attrs)) => {
					let mut whole = false;
					for key in &attrs.keys {
						match &key.read {
							SortKeyReadShape::Computed => {}
							SortKeyReadShape::Path(path) => match path.0.first() {
								Some(FieldPathPart::Field(name)) => {
									let name = Strand::new(name);
									if !state.names.contains(&name) {
										state.names.push(name);
									}
								}
								_ => whole = true,
							},
						}
					}
					(true, whole)
				}
				OpKind::Phys(PhysOp::Project(shape)) => (
					false,
					matches!(
						shape.as_ref(),
						ProjectionShape::All
							| ProjectionShape::Star { .. }
							| ProjectionShape::SortedValue(_)
					) || !shape.omissions().is_empty(),
				),
				// An aggregate reads the row through its key and field regions and delivers its
				// groups, not the row, so the rows end here and a field no region reads is not
				// demanded.
				OpKind::Phys(PhysOp::Aggregate(attrs)) => {
					(false, shape_reads_whole_row(&attrs.fields))
				}
				OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => (false, true),
			};
			if whole || later_is_row {
				state.all = true;
				break 'runs;
			}
			if keeps {
				state.rows.extend(data.results.iter().copied());
			}
			for region in data.regions.iter().copied() {
				state.add_region_arguments(ir, region);
			}
			for region in data.regions.iter().copied() {
				ir.walk(region, WalkOrder::PreOrder, &mut |op| {
					state.visit_step_expr(ir, op);
					Walk::Continue
				});
			}
		}
	}
	if last.is_none_or(|last| state.rows.contains(&last)) {
		state.all = true;
	}
	state.finish()
}

/// The field an `expr.field` or `expr.access` op reads off its first operand.
pub(super) fn field_read_name(kind: &OpKind) -> Option<&Strand> {
	if let OpKind::Expr(ExprOp::Field {
		name,
	}) = kind
	{
		Some(name)
	} else if let OpKind::Expr(ExprOp::Access {
		field,
	}) = kind
	{
		Some(field)
	} else {
		None
	}
}

/// How an op treats an operand that is a row.
enum Role {
	/// Reads the named field off its first operand.
	Field,
	/// Reads the row's `COMPUTED` memo; counted for every op of this kind, whatever its operand.
	Computed,
	/// Takes the statement timestamp, which is not a row.
	Clock,
	/// Reads its first operand only through the regions it owns and, when `keeps`, delivers the
	/// row again. A `whole` consumer needs every field of the row.
	Consumer {
		keeps: bool,
		whole: bool,
	},
	/// Any other op: a row operand is a whole-row use.
	Other,
}

fn shape_reads_whole_row(shape: &FieldsShape) -> bool {
	match shape {
		FieldsShape::Value {
			..
		} => false,
		FieldsShape::Select(fields) => fields.contains(&FieldShape::All),
	}
}

fn role_of(kind: &OpKind) -> Role {
	if field_read_name(kind).is_some() {
		return Role::Field;
	}
	match kind {
		OpKind::Expr(ExprOp::ComputedRead(_)) => Role::Computed,
		OpKind::Expr(ExprOp::Now {
			..
		}) => Role::Clock,
		OpKind::Logical(LogOp::Filter)
		| OpKind::Logical(LogOp::PermissionFilter {
			..
		})
		| OpKind::Logical(LogOp::Sort(_))
		| OpKind::Logical(LogOp::Window(_))
		| OpKind::Logical(LogOp::Split {
			..
		})
		| OpKind::Logical(LogOp::Limit {
			..
		})
		| OpKind::Logical(LogOp::Only) => Role::Consumer {
			keeps: true,
			whole: false,
		},
		OpKind::Logical(LogOp::Project(attrs)) => Role::Consumer {
			keeps: attrs.layer,
			whole: attrs.omit > 0 || shape_reads_whole_row(&attrs.fields),
		},
		OpKind::Logical(LogOp::Aggregate(shape)) => Role::Consumer {
			keeps: false,
			whole: shape_reads_whole_row(&shape.fields),
		},
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => Role::Other,
	}
}

/// The rows found so far and what has been read of them.
struct State {
	rows: FxHashSet<ValueId>,
	names: SmallVec<[Strand; 4]>,
	computed: SmallVec<[Strand; 4]>,
	all: bool,
}

impl State {
	fn visit(&mut self, ir: &Ir, op: OpId) {
		let Some(data) = ir.op(op) else {
			self.all = true;
			return;
		};
		let role = role_of(&data.kind);
		if let OpKind::Expr(ExprOp::ComputedRead(attrs)) = &data.kind {
			if !self.computed.contains(&attrs.field) {
				self.computed.push(attrs.field.clone());
			}
			self.all = true;
		}
		let first_is_row = data.operands.first().is_some_and(|operand| self.rows.contains(operand));
		let later_is_row = data.operands.iter().skip(1).any(|operand| self.rows.contains(operand));
		if !first_is_row && !later_is_row {
			return;
		}
		match role {
			Role::Field => {
				if later_is_row {
					self.all = true;
				} else if let Some(name) = field_read_name(&data.kind)
					&& !self.names.contains(name)
				{
					self.names.push(name.clone());
				}
			}
			Role::Computed | Role::Clock => {}
			Role::Consumer {
				keeps,
				whole,
			} => {
				if whole || later_is_row {
					self.all = true;
				}
				if first_is_row {
					if keeps {
						self.rows.extend(data.results.iter().copied());
					}
					for region in data.regions.iter().copied() {
						self.add_region_arguments(ir, region);
					}
				}
			}
			Role::Other => self.all = true,
		}
	}

	/// [`Self::visit`] for an op inside a region of a pipeline step, where the record id of a row
	/// is a read of `id`.
	fn visit_step_expr(&mut self, ir: &Ir, op: OpId) {
		if let Some(data) = ir.op(op)
			&& let OpKind::Expr(ExprOp::Doc) = &data.kind
			&& let [operand] = data.operands.as_slice()
			&& self.rows.contains(operand)
		{
			if !self.names.iter().any(|name| name.as_str() == "id") {
				self.names.push(Strand::new("id"));
			}
			return;
		}
		self.visit(ir, op);
	}

	/// Counts every block argument of `region` as a row: an argument that is not one only adds
	/// demand.
	fn add_region_arguments(&mut self, ir: &Ir, region: RegionId) {
		let Some(info) = ir.region(region) else {
			return;
		};
		for block in info.blocks.iter().copied() {
			if let Some(block) = ir.block(block) {
				self.rows.extend(block.args.iter().copied());
			}
		}
	}

	fn finish(self) -> FieldDemand {
		FieldDemand {
			stored: if self.all {
				FieldSet::All
			} else {
				FieldSet::Some(self.names)
			},
			computed: self.computed,
			guarded: SmallVec::new(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_top_demand_is_the_whole_row() {
		let top = FieldDemand::top();
		assert_eq!(top.stored, FieldSet::All);
		assert!(top.computed.is_empty());
		assert!(top.guarded.is_empty());
	}

	#[test]
	fn a_projection_of_every_field_reads_the_whole_row() {
		assert!(shape_reads_whole_row(&FieldsShape::Select(Box::new([FieldShape::All]))));
		assert!(!shape_reads_whole_row(&FieldsShape::Select(Box::new([FieldShape::Single {
			alias: None,
			kind: None,
		}]))));
		assert!(!shape_reads_whole_row(&FieldsShape::Value {
			alias: None,
		}));
	}
}
