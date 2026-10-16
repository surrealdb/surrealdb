//! The statement of a GQL `MATCH`: `phys.match`.
//!
//! The binder folds a `MATCH` plan into one `phys.match` op whose one `Subquery` region holds the
//! plan as the ordinary ops of a read: the sources the anchors scan, the select loops that filter
//! and project rows, the steps of the binding table (`phys.match_step`), and the sorts, pagings,
//! distincts and aggregates of the output. A binding row is an object keyed by binding name.
//!
//! [`lower_match`] lowers the region as the pipelines of a program of one statement:
//!
//! - a step that binds a row under a name runs in the body of the pipeline that holds it, and any
//!   other step is a split the host runs between two pipelines (`pipe::step`);
//! - a sort and an aggregate feed the pipeline that reads them, and a paging or a distinct that
//!   follows a pipeline or a breaker runs in the pipeline that reads those rows, as in any read;
//! - the pipeline that ends the region's value is the statement's output.
//!
//! The program reads through the database, so it takes the database context, and a plan that holds
//! a write step takes a transaction that may write.
//!
//! The binder builds nothing else: the container's top region holds the `phys.match` and the
//! `phys.yield` of its value, every source and step it builds is read, and its region always ends
//! in a projection a pipeline runs. No pass of the production configuration moves an op out of the
//! match's region or leaves a source unread, so a container of another shape is an invariant
//! break, answered as [`IrError::Invariant`]. An expression of the region that a pipeline's body
//! reads besides a paging or a source answers `NotYet`, as it does in any read.

use super::dispatch::{apply_needs, check_claims, lowers_where_read};
use super::emit::Emitter;
use super::pipe::{self, Reads};
use super::place::Lowered;
use super::read::is_source;
use super::units::LowerCx;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{PhysOp, PhysTag};
use crate::exec::{AccessMode, ContextLevel};

/// The `phys.match` that makes up the statement of the container `ir`, when its top region holds
/// one.
pub(super) fn match_in(ir: &Ir) -> Option<OpId> {
	ops_in(ir, ir.top()).find(|op| {
		matches!(ir.op(*op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Match { .. })))
	})
}

/// Lowers the container `ir`, whose statement is the `phys.match` `matched`, to a program.
///
/// The top region holds the `phys.match` and the `phys.yield` that ends the statement with its
/// value, which delivers nothing the program does not already deliver as the output of its last
/// pipeline. A container of any other shape answers [`IrError::Invariant`] (the module's
/// contract).
pub(super) fn lower_match(
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	matched: OpId,
) -> Result<Lowered, IrError> {
	check_top(ir, matched)?;
	let data = ir.op(matched).ok_or(IrError::Stale("ir_lower::matching: op"))?;
	let [region] = <[_; 1]>::try_from(data.regions.as_slice()).map_err(|_| IrError::Invariant {
		what: "a MATCH with other than one region",
	})?;
	check_region(ir, cx.registry(), region)?;
	let mut em = Emitter::new();
	let mut facts = FactCache::new();
	let reads = pipe::lower_pipelines(&mut em, cx, ir, &mut facts, region)?;
	if reads.pipelines.is_empty() {
		return Err(IrError::Invariant {
			what: "a MATCH whose region holds no pipeline",
		});
	}
	finish(cx, ir, em, reads)
}

/// Checks that the top region of `ir` holds the `phys.match` `matched`, the `phys.yield` of its
/// value and literals, and nothing else.
fn check_top(ir: &Ir, matched: OpId) -> Result<(), IrError> {
	for op in ops_in(ir, ir.top()) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::matching: op"))?;
		match &data.kind {
			OpKind::Phys(PhysOp::Match {
				..
			}) if op == matched => {}
			OpKind::Phys(PhysOp::Yield) if data.operands.len() <= 1 => {}
			OpKind::Expr(ExprOp::Literal(_)) => {}
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => {
				return Err(IrError::Invariant {
					what: "an op beside the MATCH in the top region of its container",
				});
			}
		}
	}
	Ok(())
}

/// Checks the ops of `region`, the region of a `phys.match`, as the read lowering checks the
/// region of a read statement: pipelines, the streaming ops that follow a pipeline or a breaker,
/// breakers, the sources and steps a pipeline reads, the values a source or a paging evaluates when
/// the statement starts, literals and the region's `phys.yield`.
fn check_region(ir: &Ir, registry: &FunctionRegistry, region: RegionId) -> Result<(), IrError> {
	let mut trailing = pipe::bodies_outside_regions(ir, region)?;
	for op in ops_in(ir, region).filter(|op| pipe::is_breaker(ir, *op)) {
		trailing.extend(pipe::trail_of(ir, region, op)?);
	}
	for op in ops_in(ir, region) {
		let held = ir.op(op).ok_or(IrError::Stale("ir_lower::matching: op"))?;
		match &held.kind {
			OpKind::Phys(PhysOp::Pipeline(_) | PhysOp::Yield)
			| OpKind::Expr(ExprOp::Literal(_)) => {}
			OpKind::Phys(_) if trailing.contains(&op) || pipe::is_breaker(ir, op) => {}
			OpKind::Phys(kind)
				if is_source(kind.tag())
					|| matches!(kind.tag(), PhysTag::MatchStep | PhysTag::Subquery) =>
			{
				if held.results.iter().all(|result| ir.uses(*result).is_empty()) {
					return Err(IrError::Invariant {
						what: "a source or a step of a MATCH that no pipeline reads",
					});
				}
			}
			OpKind::Phys(_) => {
				return Err(IrError::Invariant {
					what: "an op of a MATCH outside its pipelines that no read is made of",
				});
			}
			// A value read both where the statement starts and inside a pipeline's body (a
			// parameter the paging and a per-row clause share) is lowered by each reader only when
			// a second evaluation answers what the first did, as in any read.
			OpKind::Expr(_) => {
				if !pipe::feeds_host_scalar(ir, held) && !lowers_where_read(ir, registry, op, held)
				{
					return Err(IrError::NotYet {
						what: "ir_lower::matching: an expression outside every pipeline",
					});
				}
			}
			OpKind::Logical(_) => {
				return Err(IrError::Invariant {
					what: "a logical operation in a physical container",
				});
			}
		}
	}
	Ok(())
}

/// The program of the pipelines `reads` holds, which the emitter `em` filled the pools of.
fn finish(cx: &LowerCx<'_>, ir: &Ir, em: Emitter, reads: Reads) -> Result<Lowered, IrError> {
	let Reads {
		base: _,
		pipelines,
		limits,
		distincts,
		sorts,
		aggregates,
		splits,
		joins,
		probes,
		groups,
		unowned: _,
		table,
		provenance,
		needs,
		claims,
		..
	} = reads;
	let mut program = em.finish_program(pipelines, AccessMode::ReadOnly)?;
	apply_needs(&mut program, &needs);
	program.required_context = program.required_context.max(ContextLevel::Database);
	program.table = table;
	program.limits = limits;
	program.distincts = distincts;
	program.sorts = sorts;
	program.aggregates = aggregates;
	program.splits = splits;
	program.joins = joins;
	program.probes = probes;
	program.groups = groups;
	for (pipeline, claimed) in program.pipelines.iter().zip(&claims) {
		check_claims(ir, claimed, &pipeline.code, &program, cx.registry())?;
	}
	Ok(Lowered {
		program,
		provenance,
		needs: Vec::new(),
	})
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::*;
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::ir::iface::statements::MatchBindings;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::val::Value;

	/// A container whose top region holds a `phys.match` whose region `body` builds, ended by the
	/// yield of a literal, and the yield of the match's value; `beside` builds ops in the top
	/// region before the match.
	fn container(beside: impl FnOnce(&mut Builder), body: impl FnOnce(&mut Builder)) -> Builder {
		let mut b = Builder::new();
		beside(&mut b);
		let matched = b.add_phys_value(
			PhysOp::Match {
				bindings: MatchBindings(Box::new([])),
				stages: 1,
			},
			&[],
		);
		b.open(matched, RegionKind::Subquery, &[]);
		body(&mut b);
		let rows = b.literal(Value::None);
		b.yield_phys(rows);
		b.close();
		let value = b.result_of(matched);
		b.yield_phys(value);
		b.physical();
		b
	}

	/// What lowering the container `b` answers, as the text of its invariant break.
	fn refused(b: &Builder) -> &'static str {
		let matched = match_in(&b.ir).expect("the container holds a MATCH");
		let mut cx = LowerCx::detached();
		match lower_match(&mut cx, &b.ir, matched) {
			Err(IrError::Invariant {
				what,
			}) => what,
			other => panic!("expected an invariant break, got {other:?}"),
		}
	}

	#[test]
	fn an_op_beside_the_match_is_an_invariant_break() {
		let b = container(
			|b| {
				b.call("time::now", &[]);
			},
			|_| {},
		);
		assert_eq!(refused(&b), "an op beside the MATCH in the top region of its container");
	}

	#[test]
	fn a_source_no_pipeline_reads_is_an_invariant_break() {
		let b = container(
			|_| {},
			|b| {
				b.add_phys_value(
					PhysOp::Vacant {
						table: TableName::from("person"),
					},
					&[],
				);
			},
		);
		assert_eq!(refused(&b), "a source or a step of a MATCH that no pipeline reads");
	}

	#[test]
	fn an_op_no_read_is_made_of_is_an_invariant_break() {
		let b = container(
			|_| {},
			|b| {
				b.add_phys(PhysOp::Only, &[]);
			},
		);
		assert_eq!(refused(&b), "an op of a MATCH outside its pipelines that no read is made of");
	}

	#[test]
	fn a_match_with_no_pipeline_is_an_invariant_break() {
		let b = container(|_| {}, |_| {});
		assert_eq!(refused(&b), "a MATCH whose region holds no pipeline");
	}
}
