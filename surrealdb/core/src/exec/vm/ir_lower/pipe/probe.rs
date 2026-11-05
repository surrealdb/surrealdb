//! A probe: `phys.probe` as the source of the pipeline that reads the rows it keeps.
//!
//! The op reads two operands: the build input, the rows of the statement beneath its predicate,
//! and the outer key, the value of the row the statement is written in that the predicate compares
//! the field `field` with. The physical phase puts a probe only in a statement written in value
//! position whose strategy keeps the rows it reads (`Hash`, or the hashed plan of `Lookup`), in
//! front of the predicate or the select loop that reads those rows.
//!
//! The build input is a group of the program, read whole at the first opening of the probe, and
//! the pipeline that reads the probe opens [`Source::Probe`]: the kept rows whose `field` may
//! equal the key. The probe is an entry of the program's probes ([`Program::probes`]), whose value
//! is the probe's owner: the group that evaluates the statement, whose start empties the kept rows,
//! or none for the program's own code. The lowering of the statement states the owner
//! ([`Emitter::own_probes`]).
//!
//! A probe no evaluation owns stands in a `Hash` statement that the code it is written in does not
//! run in place, so it runs as a unit of its own for every evaluation. There the build input is a
//! group read whole each time the pipeline opens ([`Source::Nested`]), and the predicate above the
//! probe judges the key on every row, as it does for a statement run as a program of its own.
//!
//! The key is read as a path of field names into `$parent` when the probe opens, so the outer key
//! must be `$parent`, or the row of the frame the statement is written in, followed by field reads.
//! Any other key is an internal error. The rows are the build's records, which no schema of the
//! program describes.
//!
//! [`Program::probes`]: crate::exec::vm::program::Program

use surrealdb_strand::Strand;

use super::nested::lower_group;
use super::{Opened, Reads, Sink, lower_feeder, lower_reader};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::ir::phys::pipeline::streams;
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::expr::invariant;
use crate::exec::vm::ir_lower::read::{is_source, producer_of};
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::program::Source;

/// What main raises for a probe whose key is not a path into `$parent`.
const NOT_A_PARENT_PATH: &str = "a probe key that is not a path into $parent";

/// Opens the probe `op`, a `phys.probe`, as the source of the pipeline being lowered: its build
/// input is lowered first, as a group of the program, and the source reads the rows it keeps, or,
/// where no evaluation owns the probe, every row of the group.
pub(super) fn open_probe(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	op: OpId,
) -> Result<Opened, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let OpKind::Phys(PhysOp::Probe {
		field,
	}) = &data.kind
	else {
		return Err(invariant("a probe source that is not a phys.probe"));
	};
	let [rows, key] = <[ValueId; 2]>::try_from(data.operands.as_slice())
		.map_err(|_| invariant("a probe with other than two operands"))?;
	let owner = em.probe_owner();
	let build = lower_group(em, cx, facts, reads, |em, cx, facts, reads| {
		lower_build(em, cx, ir, facts, reads, rows)?;
		Ok(None)
	})?;
	let Some(owner) = owner else {
		return Ok(Opened {
			source: Source::Nested {
				group: build,
			},
			source_op: None,
			table: None,
			schema: None,
			feeder: None,
			objects: true,
			ids: false,
		});
	};
	let outer = parent_path(ir, key)?;
	let probe = super::pool_index(reads.base.probes, reads.probes.len(), "vm probes")?;
	reads.probes.push(owner);
	Ok(Opened {
		source: Source::Probe {
			build,
			probe,
			field: field.clone(),
			outer,
		},
		source_op: None,
		table: None,
		schema: None,
		feeder: None,
		objects: true,
		ids: false,
	})
}

/// Lowers the pipeline that yields the build rows `rows` into the group open in `em`: the source,
/// pipeline or breaker that defines them, then the streaming ops between it and the probe.
fn lower_build(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	rows: ValueId,
) -> Result<(), IrError> {
	let mut trail = Vec::new();
	let mut at = producer_of(ir, rows)?;
	loop {
		let data = ir.op(at).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		let OpKind::Phys(kind) = &data.kind else {
			return Err(invariant("a probe over rows no physical op yields"));
		};
		if super::is_pipeline(ir, at) {
			trail.reverse();
			lower_feeder(em, cx, ir, facts, at, &trail, &Sink::Output, reads)?;
			return Ok(());
		}
		if super::is_breaker(ir, at) || is_source(kind.tag()) {
			trail.reverse();
			lower_reader(em, cx, ir, facts, at, &trail, &Sink::Output, reads)?;
			return Ok(());
		}
		match data.operands.first() {
			Some(input) if streams(kind) => {
				trail.push(at);
				at = producer_of(ir, *input)?;
			}
			_ => return Err(invariant("a probe over rows no pipeline holds")),
		}
	}
}

/// The field names the key `key` reads into `$parent`, outermost first.
///
/// The key is `$parent` (the free name, or the row of the loop or clause region the statement is
/// written in) followed by field reads.
fn parent_path(ir: &Ir, key: ValueId) -> Result<Vec<Strand>, IrError> {
	let mut path = Vec::new();
	let mut at = key;
	loop {
		match ir.value(at).ok_or(IrError::Stale("ir_lower::pipe: value"))?.def {
			ValueDef::BlockArg {
				block,
				index,
			} => {
				let region = ir.block(block).ok_or(IrError::Stale("ir_lower::pipe: block"))?.parent;
				let kind = ir.region(region).ok_or(IrError::Stale("ir_lower::pipe: region"))?.kind;
				if index != 0 || !matches!(kind, RegionKind::Loop | RegionKind::Clause(_)) {
					return Err(invariant(NOT_A_PARENT_PATH));
				}
				break;
			}
			ValueDef::OpResult {
				op,
				..
			} => {
				let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
				match &data.kind {
					OpKind::Expr(ExprOp::FreeParam {
						name,
					}) if name.as_str() == "parent" => break,
					OpKind::Expr(ExprOp::Access {
						field,
					}) => {
						path.push(field.clone());
						at = *data.operands.first().ok_or_else(|| invariant(NOT_A_PARENT_PATH))?;
					}
					OpKind::Expr(_) | OpKind::Phys(_) | OpKind::Logical(_) => {
						return Err(invariant(NOT_A_PARENT_PATH));
					}
				}
			}
		}
	}
	path.reverse();
	Ok(path)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::super::tests::{pipeline, placed, scan, select_loop};
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::error::IrError;
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::ids::ValueId;
	use crate::exec::ir::op_kind::OpKind;
	use crate::exec::ir::phys::ops::PhysOp;
	use crate::exec::vm::ir_lower::emit::Emitter;
	use crate::exec::vm::ir_lower::pipe::{lower_pipelines, lower_reads};
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::ir_lower::units::LowerCx;
	use crate::exec::vm::program::Source;

	/// `$parent` followed by the field reads `path`.
	fn parent_path(b: &mut Builder, path: &[&str]) -> ValueId {
		let mut at = b.add(
			ExprOp::FreeParam {
				name: Strand::from("parent"),
			},
			&[],
		);
		for field in path {
			at = b.add(
				ExprOp::Access {
					field: Strand::from(*field),
				},
				&[at],
			);
		}
		at
	}

	/// A scan of `t` kept by a probe keyed by its field `a` against `key`, read by one pipeline.
	fn probed(key: impl FnOnce(&mut Builder) -> ValueId) -> Builder {
		let mut b = Builder::new();
		let rows = scan(&mut b);
		pipeline(&mut b, 0, |b| {
			let key = key(b);
			let op = b.create_kind(
				OpKind::Phys(PhysOp::Probe {
					field: Strand::from("a"),
				}),
				&[rows, key],
				1,
			);
			b.result_of(op)
		});
		placed(b)
	}

	/// A probe the statement's evaluation owns reads its build input as a group of the program,
	/// lowered first, and the pipeline that reads it opens the probe with the key's path into
	/// `$parent`; the probe's entry names its owner.
	#[test]
	fn a_probe_reads_its_build_input_as_a_group_and_its_key_as_a_path_into_parent() {
		let b = probed(|b| parent_path(b, &["x", "y"]));
		let mut em = Emitter::new();
		let mut cx = LowerCx::detached();
		let mut facts = FactCache::new();
		let mark = em.open_group(4);
		let outer = em.own_probes();
		let reads = lower_pipelines(&mut em, &mut cx, &b.ir, &mut facts, b.ir.top())
			.expect("the probe lowers");
		em.release_probes(outer);
		em.close_group(mark).expect("the permission range");
		assert_eq!(em.probe_owner(), None);
		assert_eq!(reads.probes, vec![Some(4)]);
		assert_eq!(reads.groups.len(), 1);
		assert_eq!(reads.pipelines.len(), 2);
		assert!(matches!(reads.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(
			reads.pipelines[1].source,
			Source::Probe {
				build: 0,
				probe: 0,
				field: Strand::from("a"),
				outer: vec![Strand::from("x"), Strand::from("y")],
			}
		);
	}

	/// The layout the physical phase builds: the probe stands in the statement's region in front
	/// of the pipeline, after the copy of the key, and the select loop of the pipeline reads it.
	/// The pipeline continues nothing and reads the probe as its source.
	#[test]
	fn a_probe_in_front_of_a_select_loop_is_the_loops_source() {
		let mut b = Builder::new();
		let rows = scan(&mut b);
		let key = parent_path(&mut b, &["x"]);
		let probe = b.create_kind(
			OpKind::Phys(PhysOp::Probe {
				field: Strand::from("a"),
			}),
			&[rows, key],
			1,
		);
		let probed = b.result_of(probe);
		select_loop(&mut b, 0, probed, |_, element| element);
		let b = placed(b);
		let mut em = Emitter::new();
		let outer = em.own_probes();
		let reads = lower_pipelines(
			&mut em,
			&mut LowerCx::detached(),
			&b.ir,
			&mut FactCache::new(),
			b.ir.top(),
		)
		.expect("the probe lowers");
		em.release_probes(outer);
		assert_eq!(reads.probes, vec![None]);
		assert_eq!(reads.groups.len(), 1);
		assert_eq!(reads.pipelines.len(), 2);
		assert_eq!(reads.pipelines[0].group, Some(0));
		assert_eq!(reads.pipelines[1].group, None);
		assert_eq!(
			reads.pipelines[1].source,
			Source::Probe {
				build: 0,
				probe: 0,
				field: Strand::from("a"),
				outer: vec![Strand::from("x")],
			}
		);
	}

	/// A probe no evaluation owns, in a statement run as a unit of its own, reads every row of its
	/// build input each time its pipeline opens, and keeps none.
	#[test]
	fn a_probe_no_evaluation_owns_reads_its_build_input_whole() {
		let b = probed(|b| parent_path(b, &["x"]));
		let lowered =
			lower_reads(&mut LowerCx::detached(), &b.ir, false).expect("the probe lowers");
		let program = &lowered.program;
		assert!(program.probes.is_empty());
		assert_eq!(program.groups.len(), 1);
		assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(
			program.pipelines[1].source,
			Source::Nested {
				group: 0
			}
		);
	}

	/// A key that is not a path into `$parent` is an internal error.
	#[test]
	fn a_probe_keyed_by_another_value_is_an_internal_error() {
		let b = probed(|b| {
			b.add(
				ExprOp::FreeParam {
					name: Strand::from("other"),
				},
				&[],
			)
		});
		let mut em = Emitter::new();
		let outer = em.own_probes();
		let lowered = lower_pipelines(
			&mut em,
			&mut LowerCx::detached(),
			&b.ir,
			&mut FactCache::new(),
			b.ir.top(),
		);
		em.release_probes(outer);
		assert!(matches!(lowered, Err(IrError::Invariant { .. })));
	}
}
