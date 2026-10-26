//! A join: `phys.join` as the source of the pipeline that reads its pairings.
//!
//! The op reads two inputs, the left rows first, and its `Clause(Where)` regions are a left and a
//! right key region per key pair, then the residual when the op states one, then the lookup arm
//! when the op reads an index. The join is an entry of the program's joins ([`JoinSpec`]), and the
//! pipeline that reads it opens [`Source::Join`]:
//!
//! - The left input is a feeder that ends in the hand-off of each row to the join, keyed by the
//!   values its left key regions take over the row ([`Sink::JoinProbe`]).
//! - The right input is read by the build side. Where the op builds on the right
//!   ([`JoinBuild::Right`]) it is a group of the program, read whole into the build table when the
//!   first left row reaches the join. Where the op holds its left input's one row
//!   ([`JoinBuild::Left`]) it is a second feeder, run after the left one and only where that one
//!   handed the join a row, which pairs each right row with the held row ([`Sink::JoinPair`]); the
//!   pipeline that reads the join is fed by it, and it by the left one, so a limit spent above the
//!   join spares the right input, then the left.
//! - The right key regions and the residual are host scalars of the program: a right key is
//!   evaluated over the build row under the join's alias, and the residual over the pairing.
//!
//! The join empties its build table when the group that owns it starts: the group the code being
//! lowered belongs to, or none for the program's own pipelines. A pairing is the left row with the
//! right row under the alias, which no schema of the program describes and the body reads as a
//! value. The op states the join's build side and its index read; this lowers what it states and
//! decides neither. A join that reads its right input through an index
//! ([`JoinAttrs::lookup`](crate::exec::ir::phys::ops::JoinAttrs::lookup)) holds the read in its
//! last region, the lookup arm, which yields the right input's chain over a lookup keyed by the
//! left row's value of the key pair the op names. The arm is a group of the program beside the
//! build group, and the join's spec names both: the run reads the arm for the first left rows and
//! the build group for the rest, and neither is compiled while the join runs.

use super::nested::lower_group;
use super::step::row_scalar;
use super::{Needs, Opened, Reads, Sink, lower_feeder, pipeline_behind, pool_index};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::expr::invariant;
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::payload::JoinBuild;
use crate::exec::vm::program::{HostScalar, JoinSpec, Source};

/// Opens the join `op`, a `phys.join`, as the source of the pipeline being lowered: its inputs are
/// lowered first, and the source reads its pairings.
pub(super) fn open_join(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	op: OpId,
) -> Result<Opened, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let OpKind::Phys(PhysOp::Join(attrs)) = &data.kind else {
		return Err(invariant("a join source that is not a phys.join"));
	};
	let [left, right] = <[ValueId; 2]>::try_from(data.operands.as_slice())
		.map_err(|_| invariant("a join with other than two inputs"))?;
	let pairs = usize::from(attrs.keys) * 2;
	if data.regions.len()
		!= pairs + usize::from(attrs.residual) + usize::from(attrs.lookup.is_some())
	{
		return Err(invariant("a join whose regions disagree with its key pairs"));
	}
	let (keys, rest) = data.regions.split_at(pairs);
	let (residual, arm) = rest.split_at(usize::from(attrs.residual));
	if attrs.lookup.is_some() && attrs.build != JoinBuild::Right {
		return Err(invariant("a join that holds its left row and reads an index lookup"));
	}
	let probe_keys: Vec<RegionId> = keys.iter().step_by(2).copied().collect();
	let build_keys: Vec<RegionId> = keys.iter().skip(1).step_by(2).copied().collect();
	let id = pool_index(reads.base.joins, reads.joins.len(), "vm joins")?;
	let at = reads.joins.len();
	reads.joins.push(JoinSpec {
		kind: attrs.matching,
		alias: attrs.alias.clone(),
		build_keys: Vec::new(),
		residual: None,
		build: 0,
		owner: em.group(),
		pipeline: 0,
		lookup: None,
		streams: None,
	});
	let probe = Sink::JoinProbe {
		id,
		keys: probe_keys,
	};
	let (feeder, build, streams) = match attrs.build {
		JoinBuild::Right => {
			// The build side is a group of the program, read when the first left row reaches the
			// join.
			let build = lower_group(em, cx, facts, reads, |em, cx, facts, reads| {
				let (input, trail) = pipeline_behind(ir, right)?;
				lower_feeder(em, cx, ir, facts, input, &trail, &Sink::Output, reads)?;
				Ok(None)
			})?;
			// The lookup arm is a group of the program too, read in place of the build group for
			// the first left rows. The run hands it the left row's key.
			if let (Some(key), [region]) = (attrs.lookup, arm) {
				let rows = arm_rows(ir, *region)?;
				let looked = lower_group(em, cx, facts, reads, |em, cx, facts, reads| {
					let (input, trail) = pipeline_behind(ir, rows)?;
					lower_feeder(em, cx, ir, facts, input, &trail, &Sink::Output, reads)?;
					Ok(None)
				})?;
				let spec =
					reads.joins.get_mut(at).ok_or_else(|| invariant("a join reserved and lost"))?;
				spec.lookup = Some((looked, usize::from(key)));
			}
			let (build_keys, residual) = scalars(em, cx, ir, reads, &build_keys, residual)?;
			set_scalars(reads, at, build_keys, residual)?;
			let (input, trail) = pipeline_behind(ir, left)?;
			lower_feeder(em, cx, ir, facts, input, &trail, &probe, reads)?;
			(reads.pipelines.len() - 1, build, None)
		}
		JoinBuild::Left => {
			let (build_keys, residual) = scalars(em, cx, ir, reads, &build_keys, residual)?;
			set_scalars(reads, at, build_keys, residual)?;
			// The left input's pipeline hands the join its one row, then the right input's
			// pipeline streams past it.
			let (input, trail) = pipeline_behind(ir, left)?;
			lower_feeder(em, cx, ir, facts, input, &trail, &probe, reads)?;
			let held = reads.pipelines.len() - 1;
			let (input, trail) = pipeline_behind(ir, right)?;
			let pair = Sink::JoinPair {
				id,
			};
			lower_feeder(em, cx, ir, facts, input, &trail, &pair, reads)?;
			let streamed = reads.pipelines.len() - 1;
			let fed = reads
				.feeders
				.get_mut(streamed)
				.ok_or_else(|| invariant("a join's streamed pipeline outside the pool"))?;
			*fed = Some(held);
			(streamed, 0, Some(reads.base.pipelines + streamed))
		}
	};
	let spec = reads.joins.get_mut(at).ok_or_else(|| invariant("a join reserved and lost"))?;
	spec.build = build;
	spec.streams = streams;
	Ok(Opened {
		source: Source::Join {
			join: id,
		},
		source_op: None,
		table: None,
		schema: None,
		feeder: Some(feeder),
		objects: false,
		ids: false,
	})
}

/// The rows the lookup arm `region` of a join yields.
fn arm_rows(ir: &Ir, region: RegionId) -> Result<ValueId, IrError> {
	let last = ops_in(ir, region).last().ok_or_else(|| invariant("a lookup arm with no op"))?;
	match ir.op(last).map(|data| (&data.kind, data.operands.as_slice())) {
		Some((OpKind::Phys(PhysOp::Yield), [rows])) => Ok(*rows),
		_ => Err(invariant("a lookup arm that does not yield one value")),
	}
}

/// The host scalars of the right key regions `keys` and of the residual region `residual`, if
/// any. What their host pipelines need of the run is merged into `reads`.
fn scalars(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	reads: &mut Reads,
	keys: &[RegionId],
	residual: &[RegionId],
) -> Result<(Vec<HostScalar>, Option<HostScalar>), IrError> {
	let mut needs = Needs::none();
	let mut build_keys = Vec::with_capacity(keys.len());
	for region in keys {
		build_keys.push(row_scalar(em, cx, ir, *region, &mut needs)?);
	}
	let residual = match residual {
		[] => None,
		[region] => Some(row_scalar(em, cx, ir, *region, &mut needs)?),
		_ => return Err(invariant("a join with more than one residual")),
	};
	reads.needs = reads.needs.merged(&needs);
	Ok((build_keys, residual))
}

/// Sets the build keys and the residual of the join at `at` in the pool of `reads`.
fn set_scalars(
	reads: &mut Reads,
	at: usize,
	build_keys: Vec<HostScalar>,
	residual: Option<HostScalar>,
) -> Result<(), IrError> {
	let spec = reads.joins.get_mut(at).ok_or_else(|| invariant("a join reserved and lost"))?;
	spec.build_keys = build_keys;
	spec.residual = residual;
	Ok(())
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::super::tests::{lowered, pipeline, placed, row_region, scan};
	use crate::exec::ContextLevel;
	use crate::exec::ir::error::IrError;
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::ids::{OpId, ValueId};
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::op_kind::OpKind;
	use crate::exec::ir::phys::ops::{JoinAttrs, PhysOp};
	use crate::exec::vm::ir_lower::pipe::lower_reads;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::ir_lower::units::LowerCx;
	use crate::exec::vm::payload::{JoinBuild, JoinMatch, Spill};
	use crate::exec::vm::program::{HostScalar, Instr, Source};

	/// The field `name` of `base`.
	fn access(b: &mut Builder, base: ValueId, name: &str) -> ValueId {
		b.add(
			ExprOp::Access {
				field: Strand::from(name),
			},
			&[base],
		)
	}

	/// An inner join of `left` and `right` under the alias `r`, keyed by `a` on the left and
	/// `r.a` on the right, built on `build`, read through an index where `lookup` says so.
	fn join(
		b: &mut Builder,
		left: ValueId,
		right: ValueId,
		build: JoinBuild,
		lookup: Option<u16>,
	) -> OpId {
		let op = b.create_kind(
			OpKind::Phys(PhysOp::Join(Box::new(JoinAttrs {
				matching: JoinMatch::Inner,
				build,
				spill: Spill::Spillable,
				alias: "r".to_owned(),
				keys: 1,
				residual: false,
				lookup,
			}))),
			&[left, right],
			1,
		);
		row_region(b, op, RegionKind::Clause(ClauseRole::Where), |b, row| access(b, row, "a"));
		row_region(b, op, RegionKind::Clause(ClauseRole::Where), |b, row| {
			let right = access(b, row, "r");
			access(b, right, "a")
		});
		op
	}

	/// Two scans, each a pipeline of its own, joined by `join` and read by a third pipeline.
	fn joined(build: JoinBuild, lookup: Option<u16>) -> Builder {
		let mut b = Builder::new();
		let left = pipeline(&mut b, 0, scan);
		let left = b.result_of(left);
		let right = pipeline(&mut b, 1, scan);
		let right = b.result_of(right);
		pipeline(&mut b, 2, |b| {
			let op = join(b, left, right, build, lookup);
			b.result_of(op)
		});
		placed(b)
	}

	/// A join that builds on the right reads its right input as a group of the program, lowered
	/// first; its left input hands each row to the join keyed by the left key; and the pipeline
	/// that reads the pairings opens the join, fed by the left input.
	#[test]
	fn a_join_built_on_the_right_reads_its_right_input_as_a_group() {
		let b = joined(JoinBuild::Right, None);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 3);
		assert_eq!(program.groups.len(), 1);
		assert_eq!(program.pipelines[0].group, Some(0));
		assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(program.pipelines[1].group, None);
		assert!(program.pipelines[1].code.iter().any(|instr| matches!(
			instr,
			Instr::JoinProbe {
				join: 0,
				count: 1,
				..
			}
		)));
		assert_eq!(
			program.pipelines[2].source,
			Source::Join {
				join: 0
			}
		);
		let [spec] = program.joins.as_slice() else {
			panic!("one join: {:?}", program.joins);
		};
		assert_eq!(spec.kind, JoinMatch::Inner);
		assert_eq!(spec.alias, "r");
		assert_eq!(spec.build, 0);
		assert_eq!(spec.owner, None);
		assert_eq!(spec.pipeline, 2);
		assert_eq!(spec.streams, None);
		assert_eq!(spec.lookup, None);
		assert!(matches!(spec.build_keys.as_slice(), [HostScalar::Code(_)]));
		assert!(spec.residual.is_none());
		assert_eq!(program.required_context, ContextLevel::Database);
	}

	/// A join that holds its left row streams its right input past it: the left input's pipeline
	/// hands the row over, the right input's pairs each row with it and feeds the pipeline that
	/// reads the join, and no group is opened.
	#[test]
	fn a_join_that_holds_its_left_row_streams_its_right_input_past_it() {
		let b = joined(JoinBuild::Left, None);
		let program = lowered(&b).program;
		assert_eq!(program.pipelines.len(), 3);
		assert!(program.groups.is_empty());
		assert!(program.pipelines[0].code.iter().any(|instr| matches!(
			instr,
			Instr::JoinProbe {
				join: 0,
				..
			}
		)));
		assert!(program.pipelines[1].code.iter().any(|instr| matches!(
			instr,
			Instr::JoinPair {
				join: 0
			}
		)));
		assert_eq!(
			program.pipelines[2].source,
			Source::Join {
				join: 0
			}
		);
		let [spec] = program.joins.as_slice() else {
			panic!("one join: {:?}", program.joins);
		};
		assert_eq!(spec.streams, Some(1));
		assert_eq!(spec.pipeline, 2);
		assert!(matches!(spec.build_keys.as_slice(), [HostScalar::Code(_)]));
	}

	/// A join that states an index read of its right input names a read the op does not carry.
	#[test]
	fn a_join_read_through_an_index_is_an_internal_error() {
		let b = joined(JoinBuild::Right, Some(0));
		assert!(matches!(
			lower_reads(&mut LowerCx::detached(), &b.ir, false),
			Err(IrError::Invariant { .. })
		));
	}
}
