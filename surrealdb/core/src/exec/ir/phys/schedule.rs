//! The request-level schedule: which reads are issued ahead of the statement that needs them.
//!
//! The schedule is derived from the dependence edges between statements. Reads may be batched
//! across statements and inside a serial loop, when the read's footprint is disjoint from every
//! write of the statement; a batched read raises nothing, each consumer raises at its own
//! position. Statements never run concurrently, and writes are never merged across statements,
//! the consecutive `INSERT`s of an import included.
//!
//! The schedule only decides. A `phys.prefetch` or a `phys.materialize` is inserted only while the
//! constants below allow it.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};

use super::ops::PhysOp;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::footprint;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::iface::effects::{IrFootprint, IrResource};
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::footprint::CatalogKind;

/// Whether a `phys.prefetch` may be inserted into a plan.
///
/// Set to `true` only when the batched-read instruction of the lowering, its execution and its
/// host service are all present. The verifier rejects the op while this is `false`.
pub(crate) const PREFETCH_EXECUTABLE: bool = false;

/// Whether a `phys.materialize` may be inserted into a plan.
///
/// Set to `true` only when the batched-read instruction of the lowering, its execution and the
/// bounded spilling holder of rows are present. The verifier rejects the op while this is `false`.
pub(crate) const MATERIALIZE_EXECUTABLE: bool = false;

/// Whether two optional names may denote one value. A `None` names every value, so it overlaps
/// any other side; two values overlap only when equal.
fn same<T: PartialEq>(a: &Option<T>, b: &Option<T>) -> bool {
	match (a, b) {
		(Some(a), Some(b)) => a == b,
		_ => true,
	}
}

/// Whether two catalog resources name a definition in common. A table's definition covers every
/// definition on it.
fn catalogs_overlap(
	(ka, ta, na): (CatalogKind, &Option<TableName>, &Option<Strand>),
	(kb, tb, nb): (CatalogKind, &Option<TableName>, &Option<Strand>),
) -> bool {
	if !same(ta, tb) {
		return false;
	}
	let tabled = |kind: CatalogKind| {
		matches!(
			kind,
			CatalogKind::Field | CatalogKind::Index | CatalogKind::Event | CatalogKind::View
		)
	};
	match (ka, kb) {
		(CatalogKind::Table, other) | (other, CatalogKind::Table)
			if other == CatalogKind::Table || tabled(other) =>
		{
			ka != kb || same(na, nb)
		}
		_ => ka == kb && same(na, nb),
	}
}

/// Whether an access to `a` and one to `b` may touch a resource in common.
///
/// Conservative: `Unknown` overlaps everything, two data resources overlap when they name one
/// table or either names every table (the fields and the key are not weighed, because a write of
/// one field still changes the record a read takes whole), and a cascade may write the data of any
/// table. Resources of different kinds are disjoint otherwise.
pub(crate) fn overlaps(a: &IrResource, b: &IrResource) -> bool {
	match (a, b) {
		(IrResource::Unknown, _) | (_, IrResource::Unknown) => true,
		(
			IrResource::Data {
				table: ta,
				..
			},
			IrResource::Data {
				table: tb,
				..
			},
		) => same(ta, tb),
		(
			IrResource::Catalog {
				kind: ka,
				table: ta,
				name: na,
			},
			IrResource::Catalog {
				kind: kb,
				table: tb,
				name: nb,
			},
		) => catalogs_overlap((*ka, ta, na), (*kb, tb, nb)),
		(IrResource::Snapshot(x), IrResource::Snapshot(y)) => x == y,
		(
			IrResource::Cascade {
				table: x,
			},
			IrResource::Cascade {
				table: y,
			},
		)
		| (
			IrResource::Reject {
				table: x,
			},
			IrResource::Reject {
				table: y,
			},
		) => x == y,
		(
			IrResource::Queue {
				event: x,
			},
			IrResource::Queue {
				event: y,
			},
		) => x == y,
		(
			IrResource::Session {
				name: x,
			},
			IrResource::Session {
				name: y,
			},
		) => x == y,
		(
			IrResource::Data {
				..
			},
			IrResource::Cascade {
				..
			},
		)
		| (
			IrResource::Cascade {
				..
			},
			IrResource::Data {
				..
			},
		) => true,
		_ => false,
	}
}

/// What batching reads of a statement: the statement op, what it reads and writes, and whether it
/// binds a name later statements read.
#[derive(Clone, Debug)]
pub(crate) struct StatementSummary {
	pub(crate) op: OpId,
	pub(crate) footprint: IrFootprint,
	pub(crate) binds: bool,
}

/// The statements whose reads are issued together, ahead of the first of them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadBatch {
	/// The transaction the statements stand in.
	pub(crate) transaction: OpId,
	/// The statements, in request order. The batch is issued where the first one stands.
	pub(crate) statements: SmallVec<[OpId; 8]>,
}

/// The reads the request issues ahead of the statements that need them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RequestSchedule {
	pub(crate) batches: Vec<ReadBatch>,
}

/// Groups the statements of one transaction into batches of reads.
///
/// Only a statement that writes nothing and reads something joins a batch, so a write is never in
/// one and writes are never merged. A statement joins the batch begun at an earlier statement only
/// if no statement between them that writes has a write that overlaps what it reads; otherwise it
/// begins a batch of its own, issued at its own position. A statement that binds a name ends its
/// batch after itself, because the keys of a later statement may be read from the binding. A batch
/// of one statement is not a batch.
pub(crate) fn batch_statements(statements: &[StatementSummary]) -> Vec<SmallVec<[OpId; 8]>> {
	fn close(
		members: &mut SmallVec<[OpId; 8]>,
		hazards: &mut Vec<IrResource>,
		out: &mut Vec<SmallVec<[OpId; 8]>>,
	) {
		if members.len() >= 2 {
			out.push(std::mem::take(members));
		} else {
			members.clear();
		}
		hazards.clear();
	}

	let mut out = Vec::new();
	let mut members: SmallVec<[OpId; 8]> = SmallVec::new();
	// What the statements since the batch began write.
	let mut hazards: Vec<IrResource> = Vec::new();
	for statement in statements {
		let IrFootprint {
			reads,
			writes,
		} = &statement.footprint;
		if !writes.is_empty() {
			hazards.extend(writes.iter().cloned());
		} else if !reads.is_empty() {
			let reached = reads.iter().any(|read| hazards.iter().any(|w| overlaps(w, read)));
			if reached {
				close(&mut members, &mut hazards, &mut out);
			}
			members.push(statement.op);
		}
		if statement.binds {
			close(&mut members, &mut hazards, &mut out);
		}
	}
	close(&mut members, &mut hazards, &mut out);
	out
}

/// The reads of the statements of each transaction of the request that are issued together.
///
/// Statements are the `phys.stmt` ops of an explicit or session transaction, whose footprint is
/// the one the analysis states for the op and the ops of its regions; reads batch only inside one
/// transaction, since statements of different transactions are ordered and an autocommit statement
/// is a transaction of its own. A read joins a batch only if no write of an earlier statement of
/// the transaction reaches it, and writes are never merged across statements. The schedule decides
/// and inserts nothing while [`PREFETCH_EXECUTABLE`] is `false`.
pub(crate) fn request_schedule(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
) -> RequestSchedule {
	let mut schedule = RequestSchedule::default();
	for op in transactions(ir) {
		let Some(region) = ir.op(op).and_then(|data| data.regions.first().copied()) else {
			continue;
		};
		let summaries: Vec<StatementSummary> = ops_in(ir, region)
			.filter(|stmt| {
				matches!(
					ir.op(*stmt).map(|data| &data.kind),
					Some(OpKind::Phys(PhysOp::Stmt { .. }))
				)
			})
			.map(|stmt| StatementSummary {
				op: stmt,
				footprint: footprint::of_with(ir, facts, &structural_cx(registry, ir), stmt),
				binds: statement_binds(ir, stmt),
			})
			.collect();
		for statements in batch_statements(&summaries) {
			schedule.batches.push(ReadBatch {
				transaction: op,
				statements,
			});
		}
	}
	schedule
}

/// The transaction ops of the request, in order: those in the top region and in the region of a
/// request op that stands there.
fn transactions(ir: &Ir) -> Vec<OpId> {
	let mut found = Vec::new();
	let mut scopes = vec![ir.top()];
	while let Some(scope) = scopes.pop() {
		for op in ops_in(ir, scope) {
			let Some(data) = ir.op(op) else {
				continue;
			};
			match &data.kind {
				OpKind::Phys(PhysOp::Txn {
					..
				}) => found.push(op),
				OpKind::Phys(PhysOp::Request {
					nested: false,
					..
				}) => scopes.extend(data.regions.iter().copied().filter(|region| {
					ir.region(*region).is_some_and(|data| data.kind == RegionKind::Request)
				})),
				OpKind::Phys(_) | OpKind::Logical(_) | OpKind::Expr(_) => {}
			}
		}
	}
	found
}

/// Whether the statement `stmt` binds a name: a `LET` stands directly in its region.
fn statement_binds(ir: &Ir, stmt: OpId) -> bool {
	ir.op(stmt).is_some_and(|data| {
		data.regions.iter().any(|region| {
			ops_in(ir, *region).any(|op| {
				matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Let(..))))
			})
		})
	})
}

#[cfg(test)]
mod tests {
	use slotmap::SlotMap;

	use super::*;
	use crate::exec::ir::iface::effects::{FieldSet, KeyOf};

	fn data(table: &str) -> IrResource {
		IrResource::Data {
			table: Some(TableName::new(Strand::new(table))),
			fields: FieldSet::All,
			key: KeyOf::Any,
		}
	}

	fn statements(
		footprints: Vec<(Vec<IrResource>, Vec<IrResource>, bool)>,
	) -> Vec<StatementSummary> {
		let mut arena: SlotMap<OpId, ()> = SlotMap::with_key();
		footprints
			.into_iter()
			.map(|(reads, writes, binds)| StatementSummary {
				op: arena.insert(()),
				footprint: IrFootprint {
					reads: reads.into(),
					writes: writes.into(),
				},
				binds,
			})
			.collect()
	}

	fn positions(
		summaries: &[StatementSummary],
		batches: &[SmallVec<[OpId; 8]>],
	) -> Vec<Vec<usize>> {
		batches
			.iter()
			.map(|batch| {
				batch
					.iter()
					.filter_map(|op| summaries.iter().position(|summary| summary.op == *op))
					.collect()
			})
			.collect()
	}

	#[test]
	#[expect(
		clippy::assertions_on_constants,
		reason = "the test pins the state of the gate constants"
	)]
	fn the_gates_are_closed() {
		assert!(!PREFETCH_EXECUTABLE);
		assert!(!MATERIALIZE_EXECUTABLE);
	}

	#[test]
	fn independent_reads_share_a_batch() {
		let summaries = statements(vec![
			(vec![data("a")], vec![], false),
			(vec![data("b")], vec![], false),
			(vec![data("a")], vec![], false),
		]);
		let batches = batch_statements(&summaries);
		assert_eq!(positions(&summaries, &batches), vec![vec![0, 1, 2]]);
	}

	#[test]
	fn writes_are_never_batched() {
		// Consecutive writes, as the `INSERT`s of an import, join no batch.
		let summaries = statements(vec![
			(vec![], vec![data("t")], false),
			(vec![], vec![data("t")], false),
			(vec![data("t")], vec![data("t")], false),
			(vec![data("u")], vec![], false),
			(vec![data("v")], vec![], false),
		]);
		let batches = batch_statements(&summaries);
		for batch in &batches {
			for op in batch {
				let summary =
					summaries.iter().find(|summary| summary.op == *op).expect("a statement");
				assert!(summary.footprint.writes.is_empty());
			}
		}
		assert_eq!(positions(&summaries, &batches), vec![vec![3, 4]]);
	}

	#[test]
	fn a_read_after_a_write_to_its_table_is_not_batched() {
		let summaries = statements(vec![
			(vec![data("a")], vec![], false),
			(vec![], vec![data("t")], false),
			(vec![data("t")], vec![], false),
			(vec![data("a")], vec![], false),
		]);
		let batches = batch_statements(&summaries);
		// The read of `t` begins a batch of its own, which the later read of `a` joins.
		assert_eq!(positions(&summaries, &batches), vec![vec![2, 3]]);
	}

	#[test]
	fn a_read_of_another_table_batches_across_a_write() {
		let summaries = statements(vec![
			(vec![data("a")], vec![], false),
			(vec![], vec![data("t")], false),
			(vec![data("b")], vec![], false),
		]);
		let batches = batch_statements(&summaries);
		assert_eq!(positions(&summaries, &batches), vec![vec![0, 2]]);
	}

	#[test]
	fn an_unknown_write_stops_every_batch() {
		let summaries = statements(vec![
			(vec![data("a")], vec![], false),
			(vec![], vec![IrResource::Unknown], false),
			(vec![data("b")], vec![], false),
		]);
		assert!(batch_statements(&summaries).is_empty());
	}

	#[test]
	fn a_binding_ends_its_batch() {
		let summaries = statements(vec![
			(vec![data("a")], vec![], false),
			(vec![data("b")], vec![], true),
			(vec![data("c")], vec![], false),
			(vec![data("d")], vec![], false),
		]);
		let batches = batch_statements(&summaries);
		assert_eq!(positions(&summaries, &batches), vec![vec![0, 1], vec![2, 3]]);
	}

	#[test]
	fn one_statement_is_not_a_batch() {
		let summaries = statements(vec![(vec![data("a")], vec![], false)]);
		assert!(batch_statements(&summaries).is_empty());
	}

	#[test]
	fn overlap_is_conservative_and_symmetric() {
		let table = |name: &str| TableName::new(Strand::new(name));
		let any_data = IrResource::Data {
			table: None,
			fields: FieldSet::All,
			key: KeyOf::Any,
		};
		let cascade = IrResource::Cascade {
			table: table("a"),
		};
		let pairs = [
			(data("a"), data("a"), true),
			(data("a"), data("b"), false),
			(data("a"), any_data, true),
			(IrResource::Unknown, data("a"), true),
			(cascade.clone(), data("b"), true),
			(
				cascade,
				IrResource::Cascade {
					table: table("b"),
				},
				false,
			),
			(
				IrResource::Session {
					name: Strand::new("x"),
				},
				data("a"),
				false,
			),
		];
		for (a, b, expected) in pairs {
			assert_eq!(overlaps(&a, &b), expected, "{a:?} against {b:?}");
			assert_eq!(overlaps(&b, &a), expected, "{b:?} against {a:?}");
		}
		let catalog =
			|table_of: Option<&str>, name: Option<&str>, kind: CatalogKind| IrResource::Catalog {
				kind,
				table: table_of.map(table),
				name: name.map(Strand::new),
			};
		assert!(overlaps(
			&catalog(Some("a"), None, CatalogKind::Table),
			&catalog(Some("a"), Some("x"), CatalogKind::Field)
		));
		assert!(!overlaps(
			&catalog(Some("a"), None, CatalogKind::Table),
			&catalog(Some("b"), Some("x"), CatalogKind::Field)
		));
		assert!(!overlaps(
			&catalog(Some("a"), Some("x"), CatalogKind::Field),
			&catalog(Some("a"), Some("y"), CatalogKind::Field)
		));
	}
}
