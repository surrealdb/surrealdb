//! `phys.fetch`: the records the paths of a `FETCH` reach, fetched into the row in place.
//!
//! A fetch is one [`Instr::Fetch`] over the row the body holds, with a path for each region of the
//! op. A path is the chain of steps its region reads from the value the region is evaluated
//! against: a field, every element, the first or last element, and a computed index. The host walks
//! the steps at run time and reads the records they reach, so the body that holds a fetch runs
//! outside the storage cursor, and the tables the walk reaches are not known to the lowering: the
//! program reads records of any table.

use super::{Body, Needs, first_arg, yielded_value};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{RegionId, ValueId};
use crate::exec::ir::logical::ops::{ClosureLoopAttrs, LogOp, LoopKind, StepAttrs};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::vm::dispatch::Reach;
use crate::exec::vm::ir_lower::expr::Lowerer;
use crate::exec::vm::program::{FetchStepSpec, Instr, RowsAheadSpec};

impl Body<'_, '_, '_> {
	/// Lowers the fetch op `step_data` over the row the body holds.
	///
	/// The register of the fetched value becomes the row the next step reads. The program reads
	/// records of every table, as the walk reaches whichever its values name.
	pub(super) fn fetch(&mut self, step_data: &OpData) -> Result<(), IrError> {
		let ir = self.ir;
		if step_data.regions.is_empty() {
			return Err(invariant("a fetch with no path"));
		}
		let mut paths = Vec::with_capacity(step_data.regions.len());
		let mut lowerer = Lowerer::in_context(&mut *self.em, ir, self.cx);
		for region in &step_data.regions {
			paths.push(path_steps(&mut lowerer, ir, *region)?);
		}
		self.claims.extend(lowerer.take_claims());
		self.needs = self.needs.merged(&lowerer.needs());
		let fetch = self.em.fetch(paths, self.io.fetch_windows)?;
		if let Some(ahead) = self.io.rows_ahead.as_ref().filter(|ahead| !ahead.derefs) {
			// The clause the plan names has been lowered by the time the rows reach the fetch, and
			// holds the slot the run counts its paging in.
			self.rows_ahead = Some(RowsAheadSpec {
				fetch: Some(fetch),
				fields: ahead.fields.iter().cloned().collect(),
				paging: paging_slot(ahead.paging, &self.pipeline_limits)?,
			});
		}
		let mut reach = Needs::none();
		reach.reach = Some(Reach::any());
		self.needs = self.needs.merged(&reach);
		let dst = self.em.reg()?;
		self.em.emit(Instr::Fetch {
			fetch,
			src: self.current,
			dst,
		});
		self.current = dst;
		Ok(())
	}
}

/// The steps the region `region` reads from the value it is evaluated against, in the order the
/// walk takes them, root first. The list ends at the first step the fetch cannot walk, and that
/// step is kept as the last entry.
///
/// A path starts from the value the region is evaluated against and continues through field
/// reads, every element, the first or last element and computed indices; a computed index is the
/// host pipeline that evaluates it. A broadcast is its base's steps followed by its body's, which
/// continue the same path over each element. Any other form is [`FetchStepSpec::Unsupported`],
/// after the steps of its base where it is a step of a path (a flatten, an element filter, a
/// method, a destructure, an optional chain, a lookup, a recursion), and the walk ends there: a
/// path rooted anywhere but the evaluated value fetches nothing.
pub(in crate::exec::vm::ir_lower) fn path_steps(
	lowerer: &mut Lowerer<'_>,
	ir: &Ir,
	region: RegionId,
) -> Result<Vec<FetchStepSpec>, IrError> {
	let mut steps = Vec::new();
	collect(lowerer, ir, yielded_value(ir, region)?, first_arg(ir, region), &mut steps)?;
	if let Some(end) = steps.iter().position(|step| matches!(step, FetchStepSpec::Unsupported)) {
		steps.truncate(end + 1);
	}
	Ok(steps)
}

/// Appends to `out` the steps that lead from `root` to `at`, root first.
fn collect(
	lowerer: &mut Lowerer<'_>,
	ir: &Ir,
	at: ValueId,
	root: Option<ValueId>,
	out: &mut Vec<FetchStepSpec>,
) -> Result<(), IrError> {
	if Some(at) == root {
		return Ok(());
	}
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(at).ok_or(IrError::Stale("ir_lower::pipe: value"))?.def
	else {
		out.push(FetchStepSpec::Unsupported);
		return Ok(());
	};
	let producer = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let operand = |at: usize| -> Result<ValueId, IrError> {
		producer.operands.get(at).copied().ok_or_else(|| invariant("a path step with no base"))
	};
	match &producer.kind {
		OpKind::Expr(ExprOp::Field {
			name,
		}) => out.push(FetchStepSpec::Field(name.clone())),
		OpKind::Expr(ExprOp::ComputedRead(attrs)) => {
			out.push(FetchStepSpec::Field(attrs.field.clone()))
		}
		OpKind::Expr(ExprOp::Access {
			field,
		}) => {
			collect(lowerer, ir, operand(0)?, root, out)?;
			out.push(FetchStepSpec::Field(field.clone()));
		}
		OpKind::Expr(ExprOp::Each) => {
			collect(lowerer, ir, operand(0)?, root, out)?;
			out.push(FetchStepSpec::All);
		}
		OpKind::Expr(ExprOp::First) => {
			collect(lowerer, ir, operand(0)?, root, out)?;
			out.push(FetchStepSpec::First);
		}
		OpKind::Expr(ExprOp::Last) => {
			collect(lowerer, ir, operand(0)?, root, out)?;
			out.push(FetchStepSpec::Last);
		}
		OpKind::Expr(ExprOp::Index) => {
			let [from, index] = <[ValueId; 2]>::try_from(producer.operands.as_slice())
				.map_err(|_| invariant("an index step with other than two operands"))?;
			collect(lowerer, ir, from, root, out)?;
			let pipeline = lowerer.host_code(index)?;
			out.push(FetchStepSpec::Index {
				pipeline,
			});
		}
		OpKind::Expr(
			ExprOp::Flatten {
				..
			}
			| ExprOp::RepeatRecurse
			| ExprOp::Destructure {
				..
			}
			| ExprOp::Optional,
		) => {
			collect(lowerer, ir, operand(0)?, root, out)?;
			out.push(FetchStepSpec::Unsupported);
		}
		// A method's operands are its dispatch, then its receiver.
		OpKind::Expr(ExprOp::Method {
			..
		}) => {
			collect(lowerer, ir, operand(1)?, root, out)?;
			out.push(FetchStepSpec::Unsupported);
		}
		OpKind::Logical(LogOp::Loop {
			kind,
			step,
			..
		})
		| OpKind::Phys(PhysOp::Loop {
			kind,
			step,
			..
		}) => match kind {
			LoopKind::Broadcast => {
				collect(lowerer, ir, operand(0)?, root, out)?;
				let body = *producer
					.regions
					.first()
					.ok_or_else(|| invariant("a broadcast with no body"))?;
				match body_yield(ir, body) {
					Some(value) => collect(lowerer, ir, value, first_arg(ir, body), out)?,
					None => out.push(FetchStepSpec::Unsupported),
				}
			}
			LoopKind::ElementFilter | LoopKind::GraphStep | LoopKind::Recurse => {
				collect(lowerer, ir, operand(0)?, root, out)?;
				out.push(FetchStepSpec::Unsupported);
			}
			// A closure builtin written as a method of its collection is a step of the path over
			// the collection; written as a function it is no step.
			LoopKind::Closure
				if step.as_deref().is_some_and(|step| {
					matches!(
						step,
						StepAttrs::Closure(ClosureLoopAttrs {
							method: true,
							..
						})
					)
				}) =>
			{
				collect(lowerer, ir, operand(0)?, root, out)?;
				out.push(FetchStepSpec::Unsupported);
			}
			LoopKind::Select | LoopKind::Write | LoopKind::For | LoopKind::Closure => {
				out.push(FetchStepSpec::Unsupported);
			}
		},
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
			out.push(FetchStepSpec::Unsupported)
		}
	}
	Ok(())
}

/// The one value the body region `region` yields, or `None` where it yields none.
fn body_yield(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let data = ir.op(ops_in(ir, region).last()?)?;
	match (&data.kind, data.operands.as_slice()) {
		(
			OpKind::Expr(ExprOp::Yield)
			| OpKind::Logical(LogOp::Yield)
			| OpKind::Phys(PhysOp::Yield),
			[value],
		) => Some(*value),
		_ => None,
	}
}

/// The program's slot of the paging clause the plan names by its place among the pipeline's
/// paging clauses, or `None` where no clause pages the rows first.
pub(super) fn paging_slot(place: Option<usize>, limits: &[u16]) -> Result<Option<u16>, IrError> {
	place
		.map(|place| {
			limits.get(place).copied().ok_or_else(|| {
				invariant("the paging clause a rows window reads ahead of is not lowered")
			})
		})
		.transpose()
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::super::tests::{lowered, pipeline, placed, row_region, scan, step};
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::ids::{OpId, ValueId};
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::phys::ops::PhysOp;
	use crate::exec::vm::dispatch::Reach;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::program::{FetchStepSpec, Instr, ROW};
	use crate::val::Value;

	/// The paths of each fetch of `program`.
	fn paths_of(program: &crate::exec::vm::program::Program) -> Vec<Vec<Vec<FetchStepSpec>>> {
		program.fetches.iter().map(|fetch| fetch.paths.clone()).collect()
	}

	/// A path built over the row it is evaluated against.
	type PathBuilder = Box<dyn FnOnce(&mut Builder, ValueId) -> ValueId>;

	/// A `FETCH` of one path per entry of `paths`, each built over the row by its closure, over a
	/// scan.
	fn fetching(paths: Vec<PathBuilder>) -> Builder {
		let mut b = Builder::new();
		pipeline(&mut b, 0, |b| {
			let rows = scan(b);
			let fetch: OpId = step(b, PhysOp::Fetch, rows, &[]);
			for path in paths {
				row_region(b, fetch, RegionKind::Clause(ClauseRole::Projection), path);
			}
			b.result_of(fetch)
		});
		placed(b)
	}

	fn field(b: &mut Builder, row: ValueId, name: &str) -> ValueId {
		b.add(
			ExprOp::Field {
				name: Strand::from(name),
			},
			&[row],
		)
	}

	#[test]
	fn a_fetch_walks_the_steps_of_each_path_over_the_row() {
		let b = fetching(vec![
			Box::new(|b, row| {
				let a = field(b, row, "a");
				b.add(
					ExprOp::Access {
						field: Strand::from("b"),
					},
					&[a],
				)
			}),
			Box::new(|b, row| {
				let c = field(b, row, "c");
				b.add(ExprOp::Each, &[c])
			}),
			Box::new(|b, row| {
				let d = field(b, row, "d");
				let first = b.add(ExprOp::First, &[d]);
				b.add(ExprOp::Last, &[first])
			}),
		]);
		let lowered = lowered(&b);
		let program = &lowered.program;
		assert_eq!(
			paths_of(program),
			vec![vec![
				vec![
					FetchStepSpec::Field(Strand::from("a")),
					FetchStepSpec::Field(Strand::from("b")),
				],
				vec![FetchStepSpec::Field(Strand::from("c")), FetchStepSpec::All],
				vec![
					FetchStepSpec::Field(Strand::from("d")),
					FetchStepSpec::First,
					FetchStepSpec::Last,
				],
			]]
		);
		let code = &program.pipelines[0].code;
		assert!(
			code.iter().any(|instr| matches!(
				instr,
				Instr::Fetch {
					fetch: 0,
					src: ROW,
					..
				}
			)),
			"{code:?}"
		);
	}

	#[test]
	fn a_fetch_reads_records_of_any_table_and_runs_beside_the_storage_cursor() {
		let b = fetching(vec![Box::new(|b, row| field(b, row, "a"))]);
		let lowered = lowered(&b);
		assert_eq!(lowered.program.reach, Some(Reach::any()));
		let pipeline = &lowered.program.pipelines[0];
		assert!(!pipeline.in_place);
	}

	#[test]
	fn a_computed_index_is_a_host_pipeline() {
		let b = fetching(vec![Box::new(|b, row| {
			let a = field(b, row, "a");
			let index = b.literal(Value::from(2_i64));
			b.add(ExprOp::Index, &[a, index])
		})]);
		let lowered = lowered(&b);
		let program = &lowered.program;
		assert_eq!(
			paths_of(program),
			vec![vec![vec![
				FetchStepSpec::Field(Strand::from("a")),
				FetchStepSpec::Index {
					pipeline: 0
				},
			]]]
		);
		assert_eq!(program.host_code.len(), 1);
	}

	/// A path not rooted at the evaluated value walks nothing; a step the fetch cannot walk ends
	/// the path after the steps of its base.
	#[test]
	fn a_step_the_fetch_cannot_walk_ends_the_path_after_its_base() {
		let b = fetching(vec![
			Box::new(|b, _| b.literal(Value::from(1_i64))),
			Box::new(|b, row| {
				let a = field(b, row, "a");
				let flattened = b.add(
					ExprOp::Flatten {
						explicit: true,
					},
					&[a],
				);
				b.add(
					ExprOp::Access {
						field: Strand::from("b"),
					},
					&[flattened],
				)
			}),
		]);
		let lowered = lowered(&b);
		assert_eq!(
			paths_of(&lowered.program),
			vec![vec![
				vec![FetchStepSpec::Unsupported],
				vec![FetchStepSpec::Field(Strand::from("a")), FetchStepSpec::Unsupported],
			]]
		);
	}
}
