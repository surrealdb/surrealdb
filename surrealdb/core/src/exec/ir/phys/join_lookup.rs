//! The index lookup of a join's right input.
//!
//! A [`PhysOp::Join`] that builds on its right input reads that input whole into a build table when
//! its first left row arrives. Where the right input is a table read through an index over a join
//! key, the first left rows read only the records their key reaches. [`run`] states that read: it
//! sets [`JoinAttrs::lookup`] to the key pair the index serves and appends the lookup arm to the
//! join, a [`RegionKind::JoinLookupArm`] region that is a copy of the right input's chain with the
//! table scan replaced by a `phys.index_lookup` keyed by the arm's block argument, which is the
//! left row's value of that key pair. The region follows the join's key and residual regions.
//!
//! The run reads the arm for the first
//! [`LOOKUP_EVALUATIONS`](crate::exec::vm::payload::LOOKUP_EVALUATIONS) left rows of a join that
//! has no build table, and builds the table, from the right input as it stands, for every row
//! after. Both are lowered before the run starts and neither compiles while the other runs. The
//! pairing of a row the arm read is judged by the same right key regions and residual as the build
//! table's, so a pair is paired by either arm or by neither.
//!
//! A join takes a lookup when all of the following hold:
//!
//! - It builds on its right input and is not a semi join, which a membership stands in for.
//! - The right input reaches a forward table scan with no top-k key and no `WITH` hint through
//!   filters, permissions, layers and table enforcement alone, each read by the next only. A
//!   statement in `FROM` (a `phys.subquery`) stands between the join and its scan for any other
//!   input, which is read whole.
//! - Its statement has no `VERSION` scope.
//! - A key pair has a right key that is a field of the row held under the join's alias, and an
//!   index over that one field alone that the access-path decision reads an equality on that field
//!   through. The first such pair, in key order, is the lookup's. The decision answers from the
//!   table answers of the principal the container is planned for, which name the columns whose
//!   `SELECT` permission it may not read, so no index over such a column is chosen.
//!
//! No other condition gates the lookup. A join that cannot take it builds the whole right input,
//! which evaluates a right-side predicate or derived field on rows no key reaches, so a gate on
//! writes or determinism would raise where the lookup answers. A join whose left input yields one
//! row takes the lookup too, and so never holds that row: it reads the records that row's key
//! reaches where holding it would stream the whole right input. Each arm evaluates the right
//! input's chain on the rows it reads: a row no left key reaches is judged only once the build
//! table is read.
//!
//! Two further shapes are declined, which the join front end does not produce: a scan with a top-k
//! key, which a lookup would read past its limit, and a join inside a stored clause, which is
//! judged outside the caller's frame.
//!
//! The VM counts the left rows read through the lookup per run state, so "the first
//! [`LOOKUP_EVALUATIONS`](crate::exec::vm::payload::LOOKUP_EVALUATIONS) left rows the join sees"
//! holds because the pipeline that hands the join its left rows is never placed as morsels:
//! placement refuses a join probe as a morsel pipeline.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::smallvec;
use surrealdb_strand::{Strand, TableName};

use super::access::ReadVersion;
use super::conjunct::{def_op, is_row, is_total_form};
use super::duplicate;
use super::hold_left::enforcement_is_total;
use super::nested::{
	enclosing_statement, holds_version, impure, in_stored_clause, index_for, index_operator,
	runs_once, statement_writes, yielded,
};
use super::ops::{IndexLookupAttrs, JoinAttrs, LookupKeyShape, PhysOp, TableScanAttrs};
use super::rewrite::{beneath, keeps_order};
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx};
use crate::exec::ir::iface::choose::IndexRef;
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::payload::{JoinBuild, JoinMatch};
use crate::kvs::Direction;

/// Where a join reads its right input through an index: what the edit installs.
struct Site {
	join: OpId,
	/// The key pair the index serves.
	pair: u16,
	/// The right input of the join.
	right: ValueId,
	/// The table scan the right input reads, and the ops between it and the join, scan first, as
	/// the ops of the join's block that stand for them.
	scan: OpId,
	chain: Vec<OpId>,
	index: IndexRef,
	/// The field the index covers: the field of the alias the pair's right key reads.
	field: Strand,
}

/// Reads the right input of every join of `ir` that satisfies the conditions of the module through
/// an index lookup, and returns the number of joins changed.
///
/// Runs in the physical phase, after the rewrite rules reach their fixed point and before a join
/// holds its one left row, which a join with a lookup never does: its one left row reads only the
/// records its key reaches. `cx` answers the table and index questions of the principal the
/// container is planned for.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<u32, IrError> {
	let mut joins = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Join(attrs))) if reads_right_whole(attrs)
		) {
			joins.push(op);
		}
		Walk::Continue
	});
	let sites: Vec<Site> =
		joins.into_iter().filter_map(|join| site_of(ir, facts, cx, join)).collect();
	let changed = u32::try_from(sites.len()).unwrap_or(u32::MAX);
	let mut rw = Rewriter::new(ir, facts);
	for site in &sites {
		install(&mut rw, site)?;
	}
	Ok(changed)
}

/// Whether the join `attrs` states reads its right input whole into a build table, and pairs rows
/// rather than handing on the left row.
fn reads_right_whole(attrs: &JoinAttrs) -> bool {
	attrs.build == JoinBuild::Right && attrs.matching != JoinMatch::Semi && attrs.lookup.is_none()
}

/// The field of the row held under `alias` that the key region `region` reads and yields: an
/// `expr.access` of one field on an `expr.field` named `alias` of the region's row, and nothing
/// else.
///
/// A key of any other shape has no index to be read through and yields `None`.
pub(super) fn build_key_field(ir: &Ir, region: RegionId, alias: &str) -> Option<Strand> {
	let access = ir.op(def_op(ir, yielded(ir, region)?)?)?;
	let OpKind::Expr(ExprOp::Access {
		field,
	}) = &access.kind
	else {
		return None;
	};
	let held = ir.op(def_op(ir, *access.operands.first()?)?)?;
	let OpKind::Expr(ExprOp::Field {
		name,
	}) = &held.kind
	else {
		return None;
	};
	(name.as_str() == alias && is_row(ir, held.operands.first())).then(|| field.clone())
}

/// The table scan the right input `right` of a join reads, with the ops it reads it through, or
/// `None` when the input is not one a lookup may replace.
fn scan_beneath(ir: &Ir, right: ValueId) -> Option<(OpId, &TableScanAttrs, Vec<OpId>)> {
	let below = beneath(ir, right, keeps_order)?;
	let OpKind::Phys(PhysOp::TableScan(scan)) = &ir.op(below.source)?.kind else {
		return None;
	};
	if scan.direction != Direction::Forward || scan.topk.is_some() || scan.read.with.is_some() {
		return None;
	}
	let mut chain: Vec<OpId> = below.crossed.to_vec();
	chain.push(below.source);
	Some((below.source, scan, chain))
}

/// The table of the forward table scan the input `right` reads through filters, permissions,
/// layers and table enforcement alone, which a lookup arm over `right` reads.
pub(super) fn right_table(ir: &Ir, right: ValueId) -> Option<TableName> {
	scan_beneath(ir, right).map(|(_, scan, _)| scan.table.clone())
}

/// Where the join `join` reads its right input through an index, when it does.
fn site_of(ir: &Ir, facts: &mut FactCache, cx: &PhysCx<'_>, join: OpId) -> Option<Site> {
	if in_stored_clause(ir, join) || holds_version(ir, join) {
		return None;
	}
	let (_, owner) = enclosing_statement(ir, join)?;
	if statement_writes(ir, facts, &cx.effects, owner) {
		return None;
	}
	let data = ir.op(join)?;
	let OpKind::Phys(PhysOp::Join(attrs)) = &data.kind else {
		return None;
	};
	let [_, right] = data.operands.as_slice() else {
		return None;
	};
	let (scan, attributes, below) = scan_beneath(ir, *right)?;
	if !chain_is_repeatable(ir, facts, cx, &below) {
		return None;
	}
	for pair in 0..attrs.keys {
		let probe_region = data.regions.get(usize::from(pair) * 2)?;
		let region = data.regions.get(usize::from(pair) * 2 + 1)?;
		let Some(field) = build_key_field(ir, *region, &attrs.alias) else {
			continue;
		};
		let key_ty = yielded(ir, *probe_region).and_then(|value| ir.value(value)).map(|v| &v.ty);
		let Some(index) = index_for(cx, &attributes.table, &field, key_ty) else {
			continue;
		};
		return Some(Site {
			join,
			pair,
			right: *right,
			scan,
			chain: block_level(ir, data.parent, &below)?,
			index,
			field,
		});
	}
	None
}

/// Whether every op in `chain` is repeatable and total, so evaluating the chain per lookup and
/// reading only the rows matched by the index cannot observe side effects, non-determinism or
/// deferred errors.
fn chain_is_repeatable(ir: &Ir, facts: &mut FactCache, cx: &PhysCx<'_>, chain: &[OpId]) -> bool {
	chain.iter().all(|op| {
		let Some(data) = ir.op(*op) else {
			return false;
		};
		let OpKind::Phys(kind) = &data.kind else {
			return false;
		};
		#[allow(clippy::wildcard_enum_match_arm)]
		match kind {
			PhysOp::TableScan(_) => true,
			// Enforcement that evaluates nothing (literal permissions, no computed field) judges
			// every row alike however often the chain runs.
			PhysOp::Enforced {
				table,
			} => {
				matches!(
					cx.physical.table_answers(&(table.clone(), None, ReadVersion::Current)),
					CatalogAnswer::Found(answers) if !answers.enforces
				) || enforcement_is_total(cx, table)
			}
			PhysOp::PermissionFilter(attrs) => {
				attrs.computed.is_none() && regions_are_repeatable(ir, facts, cx, &data.regions)
			}
			PhysOp::Layer(attrs) => {
				attrs.computed.is_empty()
					&& attrs.judged.is_none()
					&& regions_are_repeatable(ir, facts, cx, &data.regions)
			}
			PhysOp::Filter => regions_are_repeatable(ir, facts, cx, &data.regions),
			_ => false,
		}
	})
}

/// Whether every op in `regions` is deterministic, side-effect-free and total.
fn regions_are_repeatable(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	regions: &[RegionId],
) -> bool {
	let mut ok = true;
	for region in regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
			let Some(data) = ir.op(op) else {
				ok = false;
				return Walk::Stop;
			};
			let OpKind::Expr(kind) = &data.kind else {
				ok = false;
				return Walk::Stop;
			};
			let repeatable = (!impure(kind) || runs_once(kind))
				&& !index_operator(kind)
				&& effects::local(ir, facts, &cx.effects, op).set.is_deterministic()
				&& is_total_region_op(ir, data, kind);
			if !repeatable {
				ok = false;
				return Walk::Stop;
			}
			Walk::Continue
		});
		if !ok {
			return false;
		}
	}
	true
}

/// Whether `data` is a total expression op within a right-chain region.
fn is_total_region_op(ir: &Ir, data: &OpData, kind: &ExprOp) -> bool {
	is_total_form(ir, data) || matches!(kind, ExprOp::If | ExprOp::Doc | ExprOp::SetField { .. })
}

/// The ops of `block` that stand for the ops `chain` names, each once, in the order the block
/// holds them: an op of `chain` that stands in a region of an op of the block is stood for by
/// that op.
fn block_level(ir: &Ir, block: BlockId, chain: &[OpId]) -> Option<Vec<OpId>> {
	let mut lifted: Vec<OpId> = Vec::with_capacity(chain.len());
	for op in chain {
		let mut at = *op;
		while ir.op(at)?.parent != block {
			let region = ir.block(ir.op(at)?.parent)?.parent;
			at = ir.region(region)?.parent?;
		}
		if !lifted.contains(&at) {
			lifted.push(at);
		}
	}
	let held = &ir.block(block)?.ops;
	lifted.sort_by_key(|op| held.iter().position(|member| member == op));
	Some(lifted)
}

/// Appends the lookup arm to the join of `site` and states the lookup.
fn install(rw: &mut Rewriter<'_>, site: &Site) -> Result<(), IrError> {
	let join = rw.ir().op(site.join).ok_or(IrError::Stale("op"))?.clone();
	let scan = rw.ir().op(site.scan).ok_or(IrError::Stale("op"))?.clone();
	let OpKind::Phys(PhysOp::TableScan(attrs)) = &scan.kind else {
		return Err(IrError::Stale("a table scan"));
	};
	let scan_result = *scan.results.first().ok_or(IrError::Stale("result"))?;
	let rows_type =
		rw.ir().value(scan_result).map_or(TypeKnowledge::Unknown, |value| value.ty.clone());
	let (_, block) =
		rw.create_region(site.join, RegionKind::JoinLookupArm, &[TypeKnowledge::Unknown])?;
	let key = *rw
		.ir()
		.block(block)
		.and_then(|held| held.args.first())
		.ok_or(IrError::Stale("block argument"))?;
	let copied = duplicate::ops_into(rw, &site.chain, block)?;
	let copied_scan = *copied.ops.get(&site.scan).ok_or(IrError::Stale("op"))?;
	let lookup = rw.derived(&[site.scan], |rw| {
		rw.create(
			InsertPoint::Before(copied_scan),
			OpSpec {
				kind: OpKind::Phys(PhysOp::IndexLookup(Box::new(IndexLookupAttrs {
					table: attrs.table.clone(),
					read: attrs.read.clone(),
					index: site.index.clone(),
					field: site.field.clone(),
					key: LookupKeyShape::Probe,
				}))),
				operands: smallvec![key],
				results: smallvec![rows_type],
				frame: scan.frame,
				snapshot: scan.snapshot,
				loc: scan.loc.clone(),
			},
		)
	})?;
	let rows = *rw
		.ir()
		.op(lookup)
		.and_then(|created| created.results.first())
		.ok_or(IrError::Stale("result"))?;
	rw.replace_op(copied_scan, &[rows])?;
	// The value the chain delivers: the lookup's rows where no op stands between the scan and the
	// join, and the copy of the chain's last op otherwise.
	let delivered = match site.right == scan_result {
		true => rows,
		false => *copied.values.get(&site.right).ok_or(IrError::Stale("value"))?,
	};
	rw.create(
		InsertPoint::End(block),
		OpSpec {
			kind: OpKind::Phys(PhysOp::Yield),
			operands: smallvec![delivered],
			results: smallvec::SmallVec::new(),
			frame: join.frame,
			snapshot: join.snapshot,
			loc: join.loc,
		},
	)?;
	rw.update_kind(site.join, |kind| {
		if let OpKind::Phys(PhysOp::Join(attrs)) = kind {
			attrs.lookup = Some(site.pair);
		}
	})
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "postgres")]
	use super::{OpKind, PhysOp, Walk, WalkOrder};
	use crate::exec::logical::pipelines::{
		ChainLegality, ErrorOrder, Legality, Paging, SinkCombine, SourceSplit, StatementLegality,
	};
	use crate::exec::planner::placement::{
		Decision, NestedUse, PhysicalFacts, Policy, SinkKind, SourceKind,
	};

	/// The legality and facts of a pipeline over a table scan that a merge can concatenate, ending
	/// in `sink`.
	fn splittable(sink: SinkKind) -> (Legality, PhysicalFacts) {
		let legality = Legality {
			source: SourceSplit::Range,
			chain: ChainLegality::RowLocal,
			sink: SinkCombine::Concat,
			paging: Paging::None,
			error_order: ErrorOrder::FirstByInputPosition,
			may_raise: false,
			arrival: crate::exec::logical::pipelines::Arrival::Serial,
			ordered_fold: false,
		};
		let facts = PhysicalFacts {
			source: SourceKind::Scan,
			enforce: false,
			writes: false,
			limits: 0,
			output: true,
			sink,
			binds_row: false,
			fetches_row: false,
			distincts: 0,
			nested: NestedUse::None,
			host_blocks: false,
			paging_replayable: false,
			estimate: None,
		};
		(legality, facts)
	}

	/// A database of two left tables and right tables of every kind a lookup reads or declines:
	/// `b` is indexed on `k` and `j`, with `k` hidden from a record user, `c` has no index, and
	/// `d` is indexed on `j` and reads no permission.
	#[cfg(feature = "postgres")]
	const SETUP: &str = "
		DEFINE TABLE a SCHEMALESS;
		DEFINE TABLE b SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD k ON b PERMISSIONS FOR select NONE;
		DEFINE INDEX b_k ON b FIELDS k;
		DEFINE INDEX b_j ON b FIELDS j;
		DEFINE TABLE c SCHEMALESS;
		DEFINE TABLE d SCHEMALESS;
		DEFINE INDEX d_j ON d FIELDS j;
	";

	/// The container the pgsql statement `sql` plans to for `principal`, verified at the physical
	/// boundary, which checks the lookup arm of every join that has one.
	#[cfg(feature = "postgres")]
	async fn planned(
		principal: &crate::dbs::Session,
		sql: &str,
	) -> crate::exec::ir::PlannedRequest {
		use crate::dbs::Capabilities;
		use crate::exec::ir::core::ir::IrScope;
		use crate::exec::ir::tests_plan_request::plan_as;
		use crate::exec::ir::{RequestInput, RequestStatement};

		let plans = crate::pgsql::parse_to_plan(
			sql,
			&Capabilities::all(),
			&crate::syn::ParserConfig::default(),
		)
		.expect("the statement lowers");
		let statements: Vec<RequestStatement<'_>> =
			plans.iter().map(RequestStatement::Logical).collect();
		let input = RequestInput::request(
			&statements,
			IrScope::Request {
				fragment: None,
			},
		);
		plan_as(SETUP, principal, &input).await.expect("the statement plans")
	}

	/// The key pair each join of the pgsql statement `sql` reads through an index, as planned for
	/// `principal`, in the order the container holds the joins.
	#[cfg(feature = "postgres")]
	async fn lookups(principal: &crate::dbs::Session, sql: &str) -> Vec<Option<u16>> {
		let planned = planned(principal, sql).await;
		let mut found = Vec::new();
		planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Phys(PhysOp::Join(attrs))) =
				planned.ir.op(op).map(|data| &data.kind)
			{
				found.push(attrs.lookup);
			}
			Walk::Continue
		});
		found
	}

	#[cfg(feature = "postgres")]
	fn record_user() -> crate::dbs::Session {
		crate::dbs::Session::for_record(
			"test",
			"test",
			"test",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("users", "alice")),
		)
	}

	/// A join whose right input is an indexed table is read through the index by the first key
	/// pair whose right key is a field of the alias that an index covers alone. A join whose left
	/// input yields one row keeps its lookup and does not hold the row.
	#[cfg(feature = "postgres")]
	#[tokio::test]
	async fn the_first_key_pair_an_index_serves_is_the_lookup() {
		let owner = crate::exec::operators::test_util::TestDb::owner();
		for (sql, expected) in [
			("SELECT a.n, b.s FROM a JOIN b ON a.j = b.j", Some(0)),
			("SELECT a.n, b.s FROM a LEFT JOIN b ON a.j = b.j", Some(0)),
			("SELECT a.n, b.s FROM a JOIN b ON a.k = b.k AND a.j = b.j", Some(0)),
			("SELECT a.n, b.s FROM a JOIN b ON a.n = b.n AND a.j = b.j", Some(1)),
			("SELECT a.n, b.s FROM a JOIN b ON a.j = b.j AND a.n = b.n", Some(0)),
			(
				"SELECT l.n FROM (SELECT n, j FROM a ORDER BY n LIMIT 1) l JOIN d ON l.j = d.j",
				Some(0),
			),
		] {
			assert_eq!(lookups(&owner, sql).await, vec![expected], "{sql}");
		}
	}

	/// A right key that is not a field of the alias, a table with no index over the field and a
	/// statement in `FROM` are read whole.
	#[cfg(feature = "postgres")]
	#[tokio::test]
	async fn a_join_the_lookup_does_not_serve_is_read_whole() {
		let owner = crate::exec::operators::test_util::TestDb::owner();
		for sql in [
			"SELECT a.n FROM a JOIN b ON a.j = b.j + 0",
			"SELECT a.n FROM a JOIN c ON a.j = c.j",
			"SELECT a.n FROM a JOIN (SELECT * FROM b WHERE j > 1) r ON a.j = r.j",
		] {
			assert_eq!(lookups(&owner, sql).await, vec![None], "{sql}");
		}
	}

	/// The index over a column a record user may not read is never read for a lookup: the
	/// principal's answers name the column, so the pair that reads it is skipped, and a pair over
	/// a column it may read serves.
	#[cfg(feature = "postgres")]
	#[tokio::test]
	async fn a_column_the_caller_may_not_read_has_no_lookup() {
		let owner = crate::exec::operators::test_util::TestDb::owner();
		let record = record_user();
		for (sql, owned, recorded) in [
			("SELECT a.n FROM a JOIN b ON a.k = b.k", Some(0), None),
			("SELECT a.n FROM a JOIN b ON a.k = b.k AND a.j = b.j", Some(0), Some(1)),
			("SELECT a.n FROM a JOIN b ON a.j = b.j AND a.k = b.k", Some(0), Some(0)),
		] {
			assert_eq!(lookups(&owner, sql).await, vec![owned], "owner: {sql}");
			assert_eq!(lookups(&record, sql).await, vec![recorded], "record user: {sql}");
		}
	}

	/// The VM counts the left rows a join reads through its lookup in the state of the run, which
	/// is the join's count only where one state sees every left row: the pipeline that hands a
	/// join its left rows is never cut into morsels, though the same pipeline ending in the
	/// statement's output is.
	#[test]
	fn the_pipeline_that_feeds_a_join_is_never_placed_as_morsels() {
		let statement = StatementLegality {
			refusal: None,
		};
		let (legality, facts) = splittable(SinkKind::Output);
		assert!(
			matches!(Policy::judge(&legality, statement, &facts), Ok(Decision::Morsels { .. })),
			"the control pipeline is cut into morsels"
		);
		let (legality, facts) = splittable(SinkKind::JoinProbe);
		assert_eq!(
			Policy::judge(&legality, statement, &facts),
			Err("the sink holds state a part cannot own")
		);
	}

	/// A pipeline that holds a join with a lookup is placed serially in the planned container, so
	/// one run state counts every left row the join sees.
	#[cfg(feature = "postgres")]
	#[tokio::test]
	async fn a_pipeline_holding_a_lookup_join_is_planned_serial() {
		let owner = crate::exec::operators::test_util::TestDb::owner();
		for sql in [
			"SELECT a.n, b.s FROM a JOIN b ON a.j = b.j",
			"SELECT a.n, b.s FROM a LEFT JOIN b ON a.j = b.j WHERE a.n > 3",
		] {
			let planned = planned(&owner, sql).await;
			let mut joins = 0;
			let mut pipelines = 0;
			planned.ir.walk(planned.ir.top(), WalkOrder::PreOrder, &mut |op| {
				match planned.ir.op(op).map(|data| &data.kind) {
					Some(OpKind::Phys(PhysOp::Join(attrs))) if attrs.lookup.is_some() => joins += 1,
					Some(OpKind::Phys(PhysOp::Pipeline(_))) => {
						pipelines += 1;
						let placed = planned.ir.placement(op).expect("the pipeline is placed");
						assert_eq!(placed.decision(), Some(Decision::Serial), "{sql}");
					}
					_ => {}
				}
				Walk::Continue
			});
			assert_eq!(joins, 1, "the join reads through its lookup: {sql}");
			assert!(pipelines > 0, "the statement has a pipeline: {sql}");
		}
	}
}
