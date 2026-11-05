//! A statement in `FROM`: `phys.subquery` as the source of a pipeline, alone or as an input of a
//! union.
//!
//! The statement is a group of the program ([`Source::Nested`]): its pipelines are lowered into the
//! program, stamped with the group, and the host runs the group once when the pipeline that reads
//! it opens, gathering its rows as a subquery gathers them. The group takes the next index of the
//! program's groups before its region is lowered, so a group nested in it takes the one after it.
//! Its spec is the sorts, aggregates and permission frames its pipelines filled and its tail:
//! `ONLY` makes it scalar, and its `VERSION` and `TIMEOUT` are its own.
//!
//! The rows are values of any kind, which no schema of the program describes, and the pipeline
//! that reads them reads them as values. The statement's reads are the program's: a table it reads
//! raises the program to the database context, and the table the program reports is the one its
//! own sources read.
//!
//! A statement in `FROM` is a read or a write. A write is a group whose spec holds the write's
//! [`WriteSpec`](crate::exec::vm::program::WriteSpec), as a write in a block is
//! ([`write_group`]): the program then writes, at least in the database, and a read-only
//! transaction refuses it as it refuses any write. A plain `EXPLAIN` is a scalar group that answers
//! the rendering of the plan it explains ([`super::explained_group`]). A block, a schema statement,
//! `INFO` or `EXPLAIN ANALYZE` is refused with the error `NOT_A_SOURCE`.

use super::{Opened, Reads, Wrappers, lower_statement_into, pool_index, tail_of};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::planner::placement::Parallel;
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::explain_hook::explain_of;
use crate::exec::vm::ir_lower::expr::Needs;
use crate::exec::vm::ir_lower::read::{lower_source_under, producer_of};
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::ir_lower::write_stmt::{WritePipelines, lower_write_in};
use crate::exec::vm::program::{GroupSpec, Source, TailSpec, WriteSpec};
use crate::exec::{AccessMode, ContextLevel};

/// The error for a statement in `FROM` that is neither a read nor a write.
const NOT_A_SOURCE: &str = "a block or schema statement in FROM";

/// Opens the statement `op`, a `phys.subquery`, as the source of the pipeline being lowered.
pub(super) fn open_nested(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	op: OpId,
) -> Result<Opened, IrError> {
	let group = nested_group(em, cx, ir, facts, reads, op)?;
	Ok(Opened {
		source: Source::Nested {
			group,
		},
		source_op: None,
		table: None,
		schema: None,
		feeder: None,
		objects: false,
		ids: false,
	})
}

/// Lowers the statement `op`, a `phys.subquery`, as a group of the program, and answers the
/// group's index.
pub(super) fn nested_group(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	op: OpId,
) -> Result<u16, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	if !matches!(&data.kind, OpKind::Phys(PhysOp::Subquery)) {
		return Err(invariant("a nested statement that is not a phys.subquery"));
	}
	let [region] = <[RegionId; 1]>::try_from(data.regions.as_slice())
		.map_err(|_| invariant("a phys.subquery with other than one region"))?;
	let (held, unowned) = (reads.pipelines.len(), reads.unowned);
	// An `EXPLAIN` is matched before a write: it wraps a statement that does not run.
	// `EXPLAIN ANALYZE` runs a block's statement, which is refused in `FROM`.
	let group = match explain_of(ir, region) {
		Some((_, true)) => {
			return Err(IrError::Invariant {
				what: NOT_A_SOURCE,
			});
		}
		Some((explain, false)) => {
			super::explained_group(em, cx, facts, reads, ir, region, explain)?
		}
		// A write is matched first: the pipeline that wraps the write's subject loop, and the
		// pipelines of a read whose rows the write changes, would otherwise look like a read's.
		None => match write_of(ir, region)? {
			Some(write) => write_group(em, cx, facts, reads, ir, write)?,
			None => {
				refuse_non_read(ir, region)?;
				lower_group(em, cx, facts, reads, |em, cx, facts, reads| {
					lower_statement_into(em, cx, ir, facts, region, reads)
				})?
			}
		},
	};
	// The group's pipelines stand for no pipeline op of the region that reads the statement, so
	// each counts once there, including a reader that counted itself in the statement's region.
	reads.unowned = unowned + (reads.pipelines.len() - held);
	Ok(group)
}

/// Lowers the write the `Subquery` region `region` of a binding step holds as a group of the
/// program, and answers the group's index. The group's pipelines stand for no pipeline op of the
/// region being lowered.
pub(super) fn write_region_group(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	region: RegionId,
) -> Result<u16, IrError> {
	let (held, unowned) = (reads.pipelines.len(), reads.unowned);
	let write = write_of(ir, region)?
		.ok_or_else(|| invariant("a binding step's write region holds no write"))?;
	let group = write_group(em, cx, facts, reads, ir, write)?;
	reads.unowned = unowned + (reads.pipelines.len() - held);
	Ok(group)
}

/// Reserves the next group of the program, lowers into `reads` what `body` lowers while the group
/// is open, and answers the group's index.
///
/// The index is taken before `body` runs, so a group `body` opens takes the one after it. The
/// group's spec is the sorts, aggregates and permission frames its pipelines filled and the tail
/// `body` answers: `ONLY` makes it scalar, and its `VERSION` and `TIMEOUT` are its own. A table the
/// group reads raises the program to the database context, and the table the program reports
/// stays the one its own sources read.
pub(super) fn lower_group<'c>(
	em: &mut Emitter,
	cx: &mut LowerCx<'c>,
	facts: &mut FactCache,
	reads: &mut Reads,
	body: impl FnOnce(
		&mut Emitter,
		&mut LowerCx<'c>,
		&mut FactCache,
		&mut Reads,
	) -> Result<Option<TailSpec>, IrError>,
) -> Result<u16, IrError> {
	let group = pool_index(reads.base.groups, reads.groups.len(), "vm groups")?;
	let at = reads.groups.len();
	reads.groups.push(GroupSpec {
		write: None,
		scalar: false,
		rowless: false,
		sorts: 0..0,
		aggregates: 0..0,
		permissions: 0..0,
		version: None,
		timeout: None,
		only: false,
	});
	let (sorts, aggregates) = (reads.sorts.len(), reads.aggregates.len());
	let outer_table = reads.table.take();
	let mark = em.open_group(group);
	let lowered = body(em, cx, facts, reads);
	let permissions = em.close_group(mark);
	let inner_table = std::mem::replace(&mut reads.table, outer_table);
	let tail = lowered?;
	let permissions = permissions?;
	if inner_table.is_some() {
		reads.needs = reads.needs.merged(&Needs {
			context: ContextLevel::Database,
			keeps_row_order: false,
			reach: None,
			access_mode: AccessMode::ReadOnly,
		});
	}
	let range = |base: u16, from: usize, to: usize, pool: &'static str| {
		Ok::<_, IrError>(pool_index(base, from, pool)?..pool_index(base, to, pool)?)
	};
	let (version, timeout, only) = match tail {
		Some(tail) => (tail.version, tail.timeout, tail.only),
		None => (None, None, false),
	};
	let spec = GroupSpec {
		write: None,
		// `ONLY` unwraps the statement's one row; a read's value is otherwise its rows.
		scalar: only,
		rowless: false,
		sorts: range(reads.base.sorts, sorts, reads.sorts.len(), "vm sorts")?,
		aggregates: range(
			reads.base.aggregates,
			aggregates,
			reads.aggregates.len(),
			"vm aggregates",
		)?,
		permissions,
		version,
		timeout,
		only,
	};
	let slot = reads.groups.get_mut(at).ok_or_else(|| invariant("a group reserved and lost"))?;
	*slot = spec;
	Ok(group)
}

/// Whether an input of the union `op` is a statement in `FROM`.
pub(super) fn holds_a_statement(ir: &Ir, op: OpId) -> Result<bool, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	for input in &data.operands {
		let member = producer_of(ir, *input)?;
		if matches!(ir.op(member).map(|held| &held.kind), Some(OpKind::Phys(PhysOp::Subquery))) {
			return Ok(true);
		}
	}
	Ok(false)
}

/// Opens the union `op` as the source of the pipeline being lowered. An input that is a statement
/// in `FROM`, or rows a pipeline delivers, is a group of the program, read when the union reaches
/// it. Every other input is the source it stands for. The rows come from several inputs, so no one
/// schema describes them, and the table the program reports is the first input's.
pub(super) fn open_union(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	facts: &mut FactCache,
	reads: &mut Reads,
	op: OpId,
	topk_sort: Option<u16>,
) -> Result<Opened, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
	let mut sources = Vec::with_capacity(data.operands.len());
	let mut table = None;
	for input in &data.operands {
		let member = producer_of(ir, *input)?;
		if matches!(ir.op(member).map(|held| &held.kind), Some(OpKind::Phys(PhysOp::Subquery))) {
			sources.push(Source::Nested {
				group: nested_group(em, cx, ir, facts, reads, member)?,
			});
			continue;
		}
		// Rows a pipeline delivers are a group as well ([`super::member_group`]).
		if let Some(group) = super::member_group(em, cx, ir, facts, reads, *input)? {
			sources.push(Source::Nested {
				group,
			});
			continue;
		}
		let mut needs = Needs::none();
		let leaf = lower_source_under(em, cx, ir, member, topk_sort, &mut needs)?;
		reads.needs = reads.needs.merged(&needs);
		table = table.or(leaf.table);
		sources.push(leaf.source);
	}
	let source = Source::Union(sources);
	Ok(Opened {
		objects: super::yields_records(&source),
		ids: false,
		source,
		source_op: None,
		table,
		schema: None,
		feeder: None,
	})
}

/// The write a statement in `FROM` is: its `phys.write`, the region that holds it, and the duration
/// of its `TIMEOUT`.
#[derive(Clone, Copy)]
struct NestedWrite {
	write: OpId,
	holder: RegionId,
	timeout: Option<ValueId>,
}

/// The write the statement `region` holds, or `None` for a statement that is no write.
///
/// The binder wraps a write in one clause at most, its `TIMEOUT` (`with_timeout`), bound as a
/// `phys.timeout` whose `Deadline` region holds the write; the write's `ONLY` and `VERSION` are
/// attributes of the write itself. So the write stands in `region` or in that one timed body.
fn write_of(ir: &Ir, region: RegionId) -> Result<Option<NestedWrite>, IrError> {
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::pipe: op"))?;
		match &data.kind {
			OpKind::Phys(PhysOp::Write(_)) => {
				return Ok(Some(NestedWrite {
					write: op,
					holder: region,
					timeout: None,
				}));
			}
			OpKind::Phys(PhysOp::Timeout) => {
				let [duration] = <[ValueId; 1]>::try_from(data.operands.as_slice())
					.map_err(|_| invariant("a phys.timeout with other than one operand"))?;
				let [deadline] = <[RegionId; 1]>::try_from(data.regions.as_slice())
					.map_err(|_| invariant("a phys.timeout with other than one region"))?;
				let timed = ops_in(ir, deadline).find(|inner| {
					matches!(
						ir.op(*inner).map(|held| &held.kind),
						Some(OpKind::Phys(PhysOp::Write(_)))
					)
				});
				if let Some(write) = timed {
					return Ok(Some(NestedWrite {
						write,
						holder: deadline,
						timeout: Some(duration),
					}));
				}
			}
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => {}
		}
	}
	Ok(None)
}

/// Lowers the write `nested` as a group of the program and answers the group's index.
///
/// The write lowers inside the group [`lower_group`] opens, on the program's emitter, so its
/// pipelines, paging clauses and permission frames belong to the group. A write over the rows a
/// read yields numbers the read's pools after everything `reads` holds, its nested groups after
/// the write's own, and they are appended to `reads` as they stand. The group's spec holds the
/// write's [`WriteSpec`], and its shape follows the rule for a group, applied in this order:
///
/// - the tail's `ONLY` makes it scalar; a write's tail holds none, its `ONLY` being the write's;
/// - a statement that hands on rows whatever its value is not scalar; only a block's `RETURN` is
///   such a statement, never a write;
/// - the write's own `ONLY` makes it scalar;
/// - otherwise its value is its rows, under a `TIMEOUT` or not: a write's value is never its one
///   row, and it reads the database, so it is never rowless.
///
/// The `TIMEOUT` is the group's own, a host scalar the run evaluates when the group starts. The
/// program's needs merge the write's: a transaction that writes, at least in the database.
///
/// Answers [`IrError::Invariant`] for a group that writes and holds a pipeline planned as
/// morsels: the placement puts every pipeline of a writing statement beside its writes.
fn write_group(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	facts: &mut FactCache,
	reads: &mut Reads,
	ir: &Ir,
	nested: NestedWrite,
) -> Result<u16, IrError> {
	let first = reads.pipelines.len();
	let mut written: Option<WriteSpec> = None;
	let group = lower_group(em, cx, facts, reads, |em, cx, _facts, reads| {
		// `lower_group` has reserved the group, so the next free index follows it.
		let (next, mut base) = reads.next_group()?;
		base.groups = next;
		let lowered = lower_write_in(cx, em, ir, nested.write, nested.holder, base)?;
		reads.needs = reads.needs.merged(&lowered.needs);
		match lowered.pipelines {
			WritePipelines::Own(pipeline) => reads.append(pipeline),
			WritePipelines::Over(over) => absorb(reads, over),
		}
		written = Some(lowered.spec);
		let wrappers = Wrappers {
			only: false,
			version: None,
			timeout: nested.timeout,
		};
		tail_of(em, cx, ir, wrappers, reads)
	})?;
	if reads.pipelines[first..]
		.iter()
		.any(|pipeline| matches!(pipeline.parallel, Parallel::Morsels(_)))
	{
		return Err(invariant("a writing program planned morsels"));
	}
	let write = written.ok_or_else(|| invariant("a write group lowered no write"))?;
	let at = usize::from(
		group.checked_sub(reads.base.groups).ok_or_else(|| invariant("a group before its base"))?,
	);
	let slot = reads.groups.get_mut(at).ok_or_else(|| invariant("a group reserved and lost"))?;
	slot.scalar = slot.only || write.only;
	slot.rowless = false;
	slot.write = Some(write);
	Ok(group)
}

/// Appends to `reads` the pipelines and pools of `over`, the read a write in `FROM` writes the rows
/// of, lowered inside the group `reads` last reserved and numbered from where `reads` ends.
///
/// The group's spec is already reserved, so `over`'s nested groups follow it with no spec of their
/// own pushed; [`Reads::append_group`] differs in pushing one. The pipelines read no sort `reads`
/// feeds. What their bodies need reaches `reads` through the write's needs, and the caller counts
/// them among the pipelines no op of the outer region stands for.
fn absorb(reads: &mut Reads, over: Reads) {
	let table = over.table.clone();
	let super::Pools {
		pipelines,
		claims,
		limits,
		distincts,
		sorts,
		aggregates,
		splits,
		joins,
		probes,
		groups,
		provenance,
	} = over.into_pools();
	reads.feeders.extend(pipelines.iter().map(|_| None));
	reads.pipelines.extend(pipelines);
	reads.claims.extend(claims);
	reads.limits.extend(limits);
	reads.distincts.extend(distincts);
	reads.sorts.extend(sorts);
	reads.aggregates.extend(aggregates);
	reads.splits.extend(splits);
	reads.joins.extend(joins);
	reads.probes.extend(probes);
	reads.provenance.pipelines.extend(provenance.pipelines);
	reads.groups.extend(groups);
	if reads.table.is_none() {
		reads.table = table;
	}
}

/// Refuses the statement `region` holds where it is not a read: a block, a schema statement or
/// `INFO` gets the `NOT_A_SOURCE` error. A write is matched before this check ([`write_of`]).
fn refuse_non_read(ir: &Ir, region: RegionId) -> Result<(), IrError> {
	if ops_in(ir, region).any(|op| super::is_read_op(ir, op)) {
		return Ok(());
	}
	Err(IrError::Invariant {
		what: NOT_A_SOURCE,
	})
}

#[cfg(test)]
mod tests {
	use super::super::tests::{lowered, pipeline, placed, scan, select_loop};
	use crate::exec::ir::error::IrError;
	use crate::exec::ir::iface::region_kind::RegionKind;
	use crate::exec::ir::op_kind::OpKind;
	use crate::exec::ir::phys::ops::PhysOp;
	use crate::exec::vm::ir_lower::pipe::lower_reads;
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::exec::vm::ir_lower::units::LowerCx;
	use crate::exec::vm::program::Source;
	use crate::val::Value;

	/// A `phys.subquery` whose region holds the ops `body` builds and yields the value it answers,
	/// read by a select loop of a pipeline of its own.
	fn read_from(
		b: &mut Builder,
		body: impl FnOnce(&mut Builder) -> crate::exec::ir::iface::ids::ValueId,
	) {
		let nested = b.create_kind(OpKind::Phys(PhysOp::Subquery), &[], 1);
		b.open(nested, RegionKind::Subquery, &[]);
		let value = body(b);
		b.create_kind(OpKind::Phys(PhysOp::Yield), &[value], 0);
		b.close();
		let rows = b.result_of(nested);
		select_loop(b, 2, rows, |_, row| row);
	}

	/// `SELECT * FROM (SELECT * FROM t)`: the inner statement is a group of the program, run when
	/// the outer pipeline opens, and the outer pipeline reads its rows as values.
	#[test]
	fn a_statement_in_from_is_a_group_the_pipeline_reads() {
		let mut b = Builder::new();
		read_from(&mut b, |b| {
			let inner = pipeline(b, 1, scan);
			b.result_of(inner)
		});
		let program = lowered(&placed(b)).program;
		assert_eq!(program.groups.len(), 1);
		assert_eq!(program.pipelines.len(), 2);
		assert_eq!(program.pipelines[0].group, Some(0));
		assert!(matches!(program.pipelines[0].source, Source::Scan { .. }));
		assert_eq!(program.pipelines[1].group, None);
		assert!(matches!(
			program.pipelines[1].source,
			Source::Nested {
				group: 0
			}
		));
		assert!(!program.groups[0].scalar);
		assert_eq!(program.table, None);
	}

	/// A statement in `FROM` that is no read is refused with the error `NOT_A_SOURCE`.
	#[test]
	fn a_statement_in_from_that_is_no_read_is_refused_as_main_refuses_it() {
		let mut b = Builder::new();
		read_from(&mut b, |b| b.literal(Value::None));
		let b = placed(b);
		match lower_reads(&mut LowerCx::detached(), &b.ir, false) {
			Err(IrError::Invariant {
				what,
			}) => assert_eq!(what, "a block or schema statement in FROM"),
			Err(error) => panic!("an unexpected error: {error}"),
			Ok(_) => panic!("the container lowers"),
		}
	}
}
