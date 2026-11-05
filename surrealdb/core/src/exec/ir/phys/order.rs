//! Delivery order of physical ops and the licence to change it.
//!
//! [`order_sig`] states the order a physical op delivers its rows in, given the order of its row
//! input. [`total_order_for`] and [`order_change_admissible`] state, over the logical statement,
//! whether the producer of a row source may deliver another order: the nearest order-sensitive
//! consumer above it must sort by a key that never ties, and every per-row op between the two must
//! be infallible and deterministic, so that neither the rows, their order nor the first error
//! depends on the order the producer delivers.
//!
//! The only key that is total is the record id of the rows of a table read, compared by plain value
//! order: the records of one table have distinct ids. The rows of any other source (a record id
//! list, a mock, a parameter, a dynamic value, a `logical.values`) carry ids that may repeat or be
//! absent, so no key over them is total. A key of a unique index counts only once a catalog states
//! the field types that make it unique, so the answers may refuse a change that is safe and never
//! admit one that is not.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use super::ops::PhysOp;
use crate::exec::function::FunctionRegistry;
use crate::exec::index::key_range::DeliveredKeyOrder;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::egraph::decl::{OrderSig, tag_index};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind, RegionProperties};
use crate::exec::ir::logical::ops::{
	FieldShape, FieldsShape, LogOp, LoopKind, LoopSink, ProjectAttrs, SortShape, SourceRefShape,
};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::logical::OutputPathPart;
use crate::kvs::Direction;

/// The order `op` delivers its rows in, given the order `input` of its row input.
///
/// - A table scan whose key order is id order delivers record ids in its direction, an index scan
///   the order of its index in its direction, and a value read the positions of the value.
/// - A key-range scan over a key whose every component stores in value order delivers record ids in
///   its direction. Over any other key it delivers the components its access orders by, when the
///   access delivers the offered ordering exactly, and no stated order otherwise: a datetime
///   component orders only to the whole second, and the sort above stays.
/// - An op that drops, annotates, reshapes or bounds rows one at a time delivers its input's order.
/// - Every other row producer or reorderer delivers [`OrderSig::Opaque`] of its own tag: an order
///   that equals no other op's and is the same for every op of the tag. An order that is not stated
///   is therefore never taken to be another order.
/// - An op that delivers no rows delivers [`OrderSig::Unordered`].
pub(crate) fn order_sig(op: &PhysOp, input: OrderSig) -> OrderSig {
	match op {
		PhysOp::TableScan(attrs) if attrs.id_order => match attrs.direction {
			Direction::Forward => OrderSig::IdAsc,
			Direction::Backward => OrderSig::IdDesc,
		},
		PhysOp::TableScan(..) => opaque(op),
		PhysOp::KeyRangeScan(attrs) => {
			let access = &attrs.access;
			let reverse = matches!(access.direction, Direction::Backward);
			if access.key.all_exactly_ordered() {
				if reverse {
					OrderSig::IdDesc
				} else {
					OrderSig::IdAsc
				}
			} else if access.delivers == DeliveredKeyOrder::Exact && !access.ordered_by.is_empty() {
				OrderSig::KeyComponents {
					positions: access.ordered_by.clone(),
					reverse,
				}
			} else {
				opaque(op)
			}
		}
		PhysOp::IndexScan(attrs) => OrderSig::Index {
			index: attrs.index.clone(),
			reverse: matches!(attrs.direction, Direction::Backward),
		},
		PhysOp::Values {
			..
		} => OrderSig::Positional,
		PhysOp::Enforced {
			..
		}
		| PhysOp::PermissionFilter(_)
		| PhysOp::Layer(_)
		| PhysOp::Filter
		| PhysOp::Fetch
		| PhysOp::Limit {
			..
		}
		| PhysOp::Project(_)
		| PhysOp::VersionScope
		| PhysOp::Window(_)
		| PhysOp::Materialize {
			..
		} => input,
		PhysOp::Loop {
			kind,
			..
		} => {
			if matches!(kind, LoopKind::Select) {
				input
			} else {
				opaque(op)
			}
		}
		PhysOp::IndexUnion(_)
		| PhysOp::Bitmap(_)
		| PhysOp::IndexLookup(_)
		| PhysOp::FullTextScan(_)
		| PhysOp::KnnScan(_)
		| PhysOp::KnnTopK(_)
		| PhysOp::Count(_)
		| PhysOp::RecordScan(..)
		| PhysOp::Vacant {
			..
		}
		| PhysOp::Subquery
		| PhysOp::Probe {
			..
		}
		| PhysOp::Union
		| PhysOp::Join(_)
		| PhysOp::Distinct {
			..
		}
		| PhysOp::Split {
			..
		}
		| PhysOp::Sort(_)
		| PhysOp::Aggregate(_)
		| PhysOp::Match {
			..
		}
		| PhysOp::MatchStep(_)
		| PhysOp::Show(_)
		| PhysOp::Write(_)
		| PhysOp::Timeout
		| PhysOp::Only
		| PhysOp::Explain {
			..
		}
		| PhysOp::Pipeline(_)
		| PhysOp::Choose(_) => opaque(op),
		PhysOp::Eval
		| PhysOp::Request {
			..
		}
		| PhysOp::Txn {
			..
		}
		| PhysOp::Stmt {
			..
		}
		| PhysOp::Statement {
			..
		}
		| PhysOp::DdlGuard {
			..
		}
		| PhysOp::Define(_)
		| PhysOp::Remove(_)
		| PhysOp::Alter(_)
		| PhysOp::Rebuild(_)
		| PhysOp::Info {
			..
		}
		| PhysOp::Use {
			..
		}
		| PhysOp::Option {
			..
		}
		| PhysOp::Access(_)
		| PhysOp::Kill
		| PhysOp::Live(_)
		| PhysOp::Subscriber(_)
		| PhysOp::WriteStage {
			..
		}
		| PhysOp::Events(..)
		| PhysOp::Assign(_)
		| PhysOp::Unset {
			..
		}
		| PhysOp::Content {
			..
		}
		| PhysOp::Block
		| PhysOp::Let(_)
		| PhysOp::Sleep
		| PhysOp::If {
			..
		}
		| PhysOp::Prefetch(_)
		| PhysOp::LoadUnit(_)
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next => OrderSig::Unordered,
	}
}

/// The order that is distinct for each op tag and is the same for every op of the tag.
fn opaque(op: &PhysOp) -> OrderSig {
	OrderSig::Opaque(tag_index(OpTag::Phys(op.tag())) as u32)
}

/// Whether one sort key never ties between the rows of one source: it reads the record id, and it
/// is compared by plain value order, because a collation or a numeric comparison may call two
/// different ids equal.
pub(crate) fn key_is_total(collate: bool, numeric: bool, reads_id: bool) -> bool {
	reads_id && !collate && !numeric
}

/// Whether the rows of the source `source`, a `logical.source` that reads a whole table, reach a
/// sort whose keys never tie, so that no producer of those rows can change the sorted result by
/// delivering another order. Every other source, whatever its consumers, answers false.
///
/// The rows are followed through the only user of each value. A filter, a permission filter, a
/// `Select` row loop whose body holds only those and projections, and a projection that lands no
/// aliased field under the name `id` keep the rows' identity and are crossed. The first other
/// consumer decides: a sort whose last key reads the record `id` of its row without a collation
/// or a numeric comparison answers true; every other consumer, a value with no user or several,
/// and a random sort answer false.
pub(crate) fn total_order_for(ir: &Ir, source: OpId) -> bool {
	licence(ir, source).is_some()
}

/// Whether the producer of `source` may deliver another order without changing the result of its
/// statement: [`total_order_for`] holds, and every op the rows cross on the way to the sort, and
/// every op of the sort's key regions before the total last key, with the ops of their regions,
/// is infallible and deterministic.
///
/// A failing or non-deterministic per-row op makes the first error, or the value a row gets,
/// depend on the order rows are visited in. A key before the last is evaluated per row in delivery
/// order like a crossed op; the last key reads the record id and fails on nothing. Deterministic
/// is judged with reads of written data counted, so the answer holds whatever the statement
/// writes.
pub(crate) fn order_change_admissible(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	source: OpId,
) -> bool {
	let Some(crossed) = licence(ir, source) else {
		return false;
	};
	let under = structural_cx(registry, ir);
	crossed.into_iter().all(|op| {
		guards::infallible(fallibility::of_with(ir, facts, &under, op))
			&& guards::deterministic(effects::of_with(ir, facts, &under, op).determinism, true)
	})
}

/// The ops that run per row in delivery order before a sort with a total key, or `None` when
/// `source` is not a table read or no such sort is the first order-sensitive consumer: the ops
/// the rows of `source` cross, then the top-level ops of the sort's key regions before the last.
fn licence(ir: &Ir, source: OpId) -> Option<SmallVec<[OpId; 8]>> {
	let data = ir.op(source)?;
	let OpKind::Logical(LogOp::Source(attrs)) = &data.kind else {
		return None;
	};
	if !matches!(attrs.source_ref, SourceRefShape::Table(_)) {
		return None;
	}
	let mut crossed: SmallVec<[OpId; 8]> = SmallVec::new();
	let mut rows = *data.results.first()?;
	loop {
		let [only] = ir.uses(rows) else {
			return None;
		};
		if only.operand != 0 {
			return None;
		}
		let user = ir.op(only.user)?;
		let OpKind::Logical(kind) = &user.kind else {
			return None;
		};
		if let LogOp::Sort(attrs) = kind {
			let SortShape::Keys(keys) = &attrs.ordering else {
				return None;
			};
			let last = keys.last()?;
			let region = *user.regions.last()?;
			if user.regions.len() != keys.len() {
				return None;
			}
			if !key_is_total(last.collate, last.numeric, key_reads_id(ir, region)) {
				return None;
			}
			if let Some((_, earlier)) = user.regions.split_last() {
				crossed.extend(earlier.iter().flat_map(|region| ops_in(ir, *region)));
			}
			return Some(crossed);
		}
		let transparent = if let LogOp::Loop {
			kind: LoopKind::Select,
			sink: LoopSink::Output,
			..
		} = kind
		{
			loop_body_is_transparent(ir, user.regions.as_slice())
		} else if let LogOp::Project(attrs) = kind {
			keeps_id(attrs)
		} else {
			matches!(kind, LogOp::Filter | LogOp::PermissionFilter { .. })
		};
		if !transparent {
			return None;
		}
		crossed.push(only.user);
		rows = *user.results.first()?;
	}
}

/// Whether the body of a `Select` row loop holds only ops the rows cross: filters, permission
/// filters, projections that keep the record id, and the terminator.
fn loop_body_is_transparent(ir: &Ir, regions: &[RegionId]) -> bool {
	let [body] = regions else {
		return false;
	};
	ops_in(ir, *body).all(|op| {
		let Some(OpKind::Logical(kind)) = ir.op(op).map(|data| &data.kind) else {
			return false;
		};
		if let LogOp::Project(attrs) = kind {
			keeps_id(attrs)
		} else {
			matches!(kind, LogOp::Filter | LogOp::PermissionFilter { .. } | LogOp::Yield)
		}
	})
}

/// Whether a projection leaves the record `id` of its rows readable as it was: it lands no aliased
/// field under the name `id` (an output alias `id` over another value is not the record id) and
/// omits nothing.
fn keeps_id(attrs: &ProjectAttrs) -> bool {
	if attrs.omit > 0 {
		return false;
	}
	match &attrs.fields {
		FieldsShape::Value {
			..
		} => true,
		FieldsShape::Select(fields) => !fields.iter().any(|field| match field {
			FieldShape::All => false,
			FieldShape::Single {
				alias,
				..
			} => alias.as_ref().is_some_and(
				|alias| matches!(alias.parts().first(), Some(OutputPathPart::Field(name)) if name == "id"),
			),
		}),
	}
}

/// Whether the `ORDER BY` key region `region` yields the value of the field `id` of its row
/// argument, and nothing else.
fn key_reads_id(ir: &Ir, region: RegionId) -> bool {
	let Some(data) = ir.region(region) else {
		return false;
	};
	if data.kind != RegionKind::Clause(ClauseRole::OrderKey) {
		return false;
	}
	let [block] = data.blocks.as_slice() else {
		return false;
	};
	let Some(block) = ir.block(*block) else {
		return false;
	};
	let Some(row) = block.args.get(usize::from(data.kind.isolated_from_above())) else {
		return false;
	};
	let Some(last) = block.ops.last().and_then(|op| ir.op(*op)) else {
		return false;
	};
	let (OpKind::Expr(ExprOp::Yield), [yielded]) = (&last.kind, last.operands.as_slice()) else {
		return false;
	};
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(*yielded).map(|value| value.def)
	else {
		return false;
	};
	let Some(read) = ir.op(op) else {
		return false;
	};
	matches!(&read.kind, OpKind::Expr(ExprOp::Field { name }) if *name == "id")
		&& read.operands.as_slice() == [*row]
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::catalog::KeyStructure;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::location::{LocOrigin, Location};
	use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
	use crate::exec::ir::iface::choose::IndexRef;
	use crate::exec::ir::iface::ids::{FrameId, SnapshotId, ValueId};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{SortAttrs, SortKeyShape, SourceAttrs};
	use crate::exec::ir::phys::ops::{
		Batch, IndexScanAttrs, KeyRangeScanAttrs, KeyRangeShape, ProjectionShape, ReadClauses,
		RecordScanAttrs, SortAttrs as PhysSortAttrs, TableScanAttrs,
	};
	use crate::exec::vm::payload::{Spill, ValuesRead};
	use crate::expr::logical::OutputPath;
	use crate::val::Value;

	fn table() -> TableName {
		TableName::new(Strand::new("t"))
	}

	fn table_scan(direction: Direction) -> PhysOp {
		PhysOp::TableScan(Box::new(TableScanAttrs {
			probes: None,
			bounds: None,
			prefilter: None,
			table: table(),
			direction,
			topk: None,
			lightweight: false,
			id_order: true,
			read: ReadClauses::default(),
		}))
	}

	fn index() -> IndexRef {
		IndexRef {
			table: table(),
			index: Strand::new("ix"),
			position: 0,
			scalar: Box::default(),
		}
	}

	fn index_scan(direction: Direction) -> PhysOp {
		PhysOp::IndexScan(Box::new(IndexScanAttrs {
			table: table(),
			index: index(),
			access: crate::exec::index::access_path::BTreeAccess::literal_equality(Value::None),
			direction,
			ordered_by: None,
			pinned: None,
			stated: None,
			rechecked: false,
			decode: None,
			read: ReadClauses::default(),
		}))
	}

	fn select_loop(kind: LoopKind) -> PhysOp {
		PhysOp::Loop {
			kind,
			first: false,
			batch: Batch {
				max_iterations: 1,
				max_bytes: 1,
			},
			unroll: 1,
			value: false,
			step: None,
		}
	}

	fn sort() -> PhysOp {
		PhysOp::Sort(Box::new(PhysSortAttrs {
			keys: Vec::new(),
			keep: None,
			pushdown: false,
			random: false,
			spill: Spill::of_sort(None),
			tempfiles: false,
		}))
	}

	fn opaque_of(tag: crate::exec::ir::phys::ops::PhysTag) -> OrderSig {
		OrderSig::Opaque(tag_index(OpTag::Phys(tag)) as u32)
	}

	#[test]
	fn order_sig_follows_the_access_path() {
		use crate::exec::ir::phys::ops::PhysTag;

		let input = OrderSig::Index {
			index: index(),
			reverse: true,
		};
		let rows: [(PhysOp, OrderSig, OrderSig); 14] = [
			(table_scan(Direction::Forward), OrderSig::Unordered, OrderSig::IdAsc),
			(table_scan(Direction::Backward), OrderSig::Unordered, OrderSig::IdDesc),
			(
				index_scan(Direction::Forward),
				OrderSig::Unordered,
				OrderSig::Index {
					index: index(),
					reverse: false,
				},
			),
			(
				index_scan(Direction::Backward),
				OrderSig::Unordered,
				OrderSig::Index {
					index: index(),
					reverse: true,
				},
			),
			(
				PhysOp::Values {
					read: ValuesRead::Dynamic,
				},
				OrderSig::Unordered,
				OrderSig::Positional,
			),
			(PhysOp::Filter, input.clone(), input),
			(
				PhysOp::Limit {
					limit: None,
					start: None,
				},
				OrderSig::IdAsc,
				OrderSig::IdAsc,
			),
			(PhysOp::Project(Box::new(ProjectionShape::All)), OrderSig::IdDesc, OrderSig::IdDesc),
			(select_loop(LoopKind::Select), OrderSig::IdAsc, OrderSig::IdAsc),
			(select_loop(LoopKind::For), OrderSig::IdAsc, opaque_of(PhysTag::Loop)),
			(sort(), OrderSig::IdAsc, opaque_of(PhysTag::Sort)),
			(PhysOp::Union, OrderSig::IdAsc, opaque_of(PhysTag::Union)),
			(
				PhysOp::RecordScan(Box::new(RecordScanAttrs {
					table: TableName::new(Strand::new("t")),
					for_update: false,
					lightweight: false,
					read: ReadClauses::default(),
				})),
				OrderSig::Unordered,
				opaque_of(PhysTag::RecordScan),
			),
			(PhysOp::Yield, OrderSig::IdAsc, OrderSig::Unordered),
		];
		for (op, given, want) in rows {
			assert_eq!(order_sig(&op, given), want, "{:?}", op.tag());
		}
		assert_ne!(opaque_of(PhysTag::Sort), opaque_of(PhysTag::Union));
	}

	/// A key-range scan over `key` with the access `delivers` and `ordered_by`, read along
	/// `direction`.
	fn key_range_scan(
		key: Arc<KeyStructure>,
		direction: Direction,
		delivers: DeliveredKeyOrder,
		ordered_by: Vec<u32>,
	) -> PhysOp {
		PhysOp::KeyRangeScan(Box::new(KeyRangeScanAttrs {
			prefilter: None,
			table: table(),
			read: Default::default(),
			access: KeyRangeShape {
				key,
				eq: Vec::new(),
				range: None,
				direction,
				delivers,
				ordered_by,
			},
			topk: None,
		}))
	}

	#[test]
	fn a_table_scan_that_is_not_in_id_order_states_no_order() {
		let PhysOp::TableScan(mut attrs) = table_scan(Direction::Forward) else {
			panic!("a table scan");
		};
		attrs.id_order = false;
		assert_eq!(
			order_sig(&PhysOp::TableScan(attrs), OrderSig::Unordered),
			opaque_of(crate::exec::ir::phys::ops::PhysTag::TableScan)
		);
	}

	/// A key whose components all store in value order is read in id order; any other key
	/// delivers the components its access orders by, when the access delivers them exactly, and
	/// states no order when it delivers them to the whole second only.
	#[test]
	fn a_key_range_scan_delivers_what_its_access_delivers() {
		use crate::exec::ir::phys::ops::{PhysTag, key_range_fixtures};

		let exact = key_range_fixtures::pair();
		let coarse = key_range_fixtures::reading();
		let unordered = DeliveredKeyOrder::None;
		let rows = [
			(
				key_range_scan(Arc::clone(&exact), Direction::Forward, unordered, Vec::new()),
				OrderSig::IdAsc,
			),
			(
				key_range_scan(exact, Direction::Backward, DeliveredKeyOrder::Exact, vec![0, 1]),
				OrderSig::IdDesc,
			),
			(
				key_range_scan(
					Arc::clone(&coarse),
					Direction::Backward,
					DeliveredKeyOrder::Exact,
					vec![0],
				),
				OrderSig::KeyComponents {
					positions: vec![0],
					reverse: true,
				},
			),
			(
				key_range_scan(
					Arc::clone(&coarse),
					Direction::Forward,
					DeliveredKeyOrder::Coarse {
						position: 1,
					},
					vec![0, 1],
				),
				opaque_of(PhysTag::KeyRangeScan),
			),
			(
				key_range_scan(coarse, Direction::Forward, unordered, Vec::new()),
				opaque_of(PhysTag::KeyRangeScan),
			),
		];
		for (op, want) in rows {
			assert_eq!(order_sig(&op, OrderSig::Unordered), want, "{op:?}");
		}
	}

	#[test]
	fn a_key_is_total_only_as_a_plain_id() {
		assert!(key_is_total(false, false, true));
		assert!(!key_is_total(false, false, false));
		assert!(!key_is_total(true, false, true));
		assert!(!key_is_total(false, true, true));
	}

	/// The ops of one key region: a read of a field of the row, or a literal.
	enum KeyBody {
		Field(&'static str),
		Literal,
	}

	fn plain() -> SortKeyShape {
		SortKeyShape {
			collate: false,
			numeric: false,
			ascending: true,
			path: false,
		}
	}

	fn spec(kind: OpKind, operands: &[ValueId], results: usize) -> OpSpec {
		OpSpec {
			kind,
			operands: operands.iter().copied().collect(),
			results: vec![TypeKnowledge::Unknown; results].into_iter().collect(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Synthesized,
			},
		}
	}

	fn table_source() -> LogOp {
		source_of(SourceRefShape::Table(table()))
	}

	fn source_of(source_ref: SourceRefShape) -> LogOp {
		LogOp::Source(Box::new(SourceAttrs {
			source_ref,
			with: None,
			for_update: false,
			may_be_defined: false,
			may_meet_change: false,
			resolved: false,
		}))
	}

	/// A container whose one `source` is read directly by a `logical.sort` with one
	/// `Clause(OrderKey)` region per entry of `keys`, and the source op.
	fn sorted(source: LogOp, keys: &[(KeyBody, SortKeyShape)]) -> (Ir, OpId) {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let source = rw
			.create(InsertPoint::End(top), spec(OpKind::Logical(source), &[], 1))
			.expect("the source is created");
		let rows = rw.ir().op(source).expect("the source exists").results[0];
		let shapes: Box<[SortKeyShape]> = keys.iter().map(|(_, shape)| *shape).collect();
		let sort = rw
			.create(
				InsertPoint::End(top),
				spec(
					OpKind::Logical(LogOp::Sort(Box::new(SortAttrs {
						ordering: SortShape::Keys(shapes),
						tempfiles: false,
					}))),
					&[rows],
					1,
				),
			)
			.expect("the sort is created");
		for (body, _) in keys {
			let (_, block) = rw
				.create_region(
					sort,
					RegionKind::Clause(ClauseRole::OrderKey),
					&[TypeKnowledge::Unknown],
				)
				.expect("the key region is created");
			let row = rw.ir().block(block).expect("the key block exists").args[0];
			let (kind, operands) = match body {
				KeyBody::Field(name) => (
					ExprOp::Field {
						name: Strand::new(*name),
					},
					vec![row],
				),
				KeyBody::Literal => (ExprOp::Literal(Box::new(Value::None)), Vec::new()),
			};
			let value = rw
				.create(InsertPoint::End(block), spec(OpKind::Expr(kind), &operands, 1))
				.expect("the key value is created");
			let yielded = rw.ir().op(value).expect("the key value exists").results[0];
			rw.create(InsertPoint::End(block), spec(OpKind::Expr(ExprOp::Yield), &[yielded], 0))
				.expect("the key is closed");
		}
		(ir, source)
	}

	fn total_of(source: LogOp, keys: &[(KeyBody, SortKeyShape)]) -> bool {
		let (ir, source) = sorted(source, keys);
		total_order_for(&ir, source)
	}

	#[test]
	fn a_table_ordered_by_its_id_is_total() {
		let (ir, source) = sorted(table_source(), &[(KeyBody::Field("id"), plain())]);
		assert!(total_order_for(&ir, source));
		assert!(order_change_admissible(
			&ir,
			&mut FactCache::new(),
			crate::exec::ir::analysis::cx::builtin_registry(),
			source
		));
	}

	#[test]
	fn a_source_that_is_not_a_table_is_not_total() {
		let id = [(KeyBody::Field("id"), plain())];
		for (name, source) in [
			("record id", source_of(SourceRefShape::RecordId)),
			("mock", source_of(SourceRefShape::Mock)),
			("param", source_of(SourceRefShape::Param)),
			("dynamic", source_of(SourceRefShape::Dynamic)),
			(
				"values",
				LogOp::Values {
					for_update: false,
					written: false,
				},
			),
		] {
			assert!(!total_of(source, &id), "{name}");
		}
	}

	#[test]
	fn a_source_that_is_not_a_table_is_not_admissible() {
		let (ir, source) = sorted(
			LogOp::Values {
				for_update: false,
				written: false,
			},
			&[(KeyBody::Field("id"), plain())],
		);
		assert!(!order_change_admissible(
			&ir,
			&mut FactCache::new(),
			crate::exec::ir::analysis::cx::builtin_registry(),
			source
		));
	}

	#[test]
	fn a_key_that_can_tie_is_not_total() {
		let collated = SortKeyShape {
			collate: true,
			..plain()
		};
		let numeric = SortKeyShape {
			numeric: true,
			..plain()
		};
		assert!(!total_of(table_source(), &[(KeyBody::Field("name"), plain())]));
		assert!(!total_of(table_source(), &[(KeyBody::Field("id"), collated)]));
		assert!(!total_of(table_source(), &[(KeyBody::Field("id"), numeric)]));
		assert!(!total_of(table_source(), &[(KeyBody::Literal, plain())]));
	}

	#[test]
	fn only_the_last_key_has_to_be_total() {
		let keys = [(KeyBody::Field("name"), plain()), (KeyBody::Field("id"), plain())];
		assert!(total_of(table_source(), &keys));
		let keys = [(KeyBody::Field("id"), plain()), (KeyBody::Field("name"), plain())];
		assert!(!total_of(table_source(), &keys));
	}

	#[test]
	fn a_fallible_earlier_key_is_not_admissible() {
		let (ir, source) = sorted(
			table_source(),
			&[(KeyBody::Field("name"), plain()), (KeyBody::Field("id"), plain())],
		);
		assert!(total_order_for(&ir, source));
		assert!(!order_change_admissible(
			&ir,
			&mut FactCache::new(),
			crate::exec::ir::analysis::cx::builtin_registry(),
			source
		));
	}

	#[test]
	fn an_infallible_earlier_key_is_admissible() {
		let (ir, source) =
			sorted(table_source(), &[(KeyBody::Literal, plain()), (KeyBody::Field("id"), plain())]);
		assert!(total_order_for(&ir, source));
		assert!(order_change_admissible(
			&ir,
			&mut FactCache::new(),
			crate::exec::ir::analysis::cx::builtin_registry(),
			source
		));
	}

	#[test]
	fn a_projection_keeps_the_id_unless_it_renames_or_omits() {
		let alias = |name: &str| FieldShape::Single {
			alias: Some(OutputPath::field(name)),
			kind: None,
		};
		let select = |fields: Vec<FieldShape>, omit: u16| ProjectAttrs {
			fields: FieldsShape::Select(fields.into_boxed_slice()),
			layer: false,
			omit,
			names: Vec::new(),
			computed: Vec::new(),
			sort_keys: Vec::new(),
			judged: None,
		};
		assert!(keeps_id(&select(vec![FieldShape::All, alias("name")], 0)));
		assert!(!keeps_id(&select(vec![alias("id")], 0)));
		assert!(!keeps_id(&select(vec![FieldShape::All], 1)));
		assert!(keeps_id(&ProjectAttrs {
			fields: FieldsShape::Value {
				alias: None,
			},
			layer: false,
			omit: 0,
			names: Vec::new(),
			computed: Vec::new(),
			sort_keys: Vec::new(),
			judged: None,
		}));
	}

	#[test]
	fn choose_shares_the_key_predicate() {
		let code =
			include_str!("choose.rs").split("#[cfg(test)]").next().expect("the module has code");
		assert!(
			code.contains("order::key_is_total("),
			"choose.rs does not call the shared predicate"
		);
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod bound_tests {
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::build::logical::bind_request;
	use crate::exec::ir::core::ir::{IrScope, Walk, WalkOrder};
	use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::logical::ops::LogTag;
	use crate::exec::ir::{RequestInput, RequestStatement};

	/// A semantic catalog that holds no definition.
	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// Binds `sql` without converting it.
	async fn bound(sql: &str) -> Ir {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let effects = EffectCx {
			registry: &registry,
			catalog: &NoCatalog,
			frames: &frames,
		};
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let request_statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(
			&request_statements,
			IrScope::Request {
				fragment: None,
			},
		);
		bind_request(&input, &effects).await.expect("the request binds")
	}

	/// The `logical.source` or `logical.values` of `ir`; the statement reads one source.
	fn source(ir: &Ir) -> OpId {
		let mut found = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Logical(kind)) = ir.op(op).map(|data| &data.kind)
				&& matches!(kind.tag(), LogTag::Source | LogTag::Values)
			{
				found.push(op);
			}
			Walk::Continue
		});
		assert_eq!(found.len(), 1, "one source");
		found[0]
	}

	async fn total(sql: &str) -> bool {
		let ir = bound(sql).await;
		total_order_for(&ir, source(&ir))
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn no_order_by_is_not_total() {
		assert!(!total("SELECT VALUE id FROM t WHERE x > 0").await);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn order_by_id_is_total() {
		assert!(total("SELECT VALUE id FROM t WHERE x > 0 ORDER BY id").await);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn a_tied_key_is_not_total() {
		assert!(!total("SELECT VALUE id FROM t WHERE x > 0 ORDER BY y").await);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn a_collated_id_is_not_total() {
		assert!(!total("SELECT VALUE id FROM t WHERE x > 0 ORDER BY id COLLATE ASC").await);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn an_array_source_is_not_total() {
		assert!(!total("SELECT VALUE id FROM [{ id: 1 }, { id: 1 }] ORDER BY id").await);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn a_parameter_source_is_not_total() {
		assert!(!total("SELECT VALUE id FROM $p ORDER BY id").await);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn an_inner_limit_is_not_total() {
		assert!(
			!total("SELECT VALUE id FROM (SELECT * FROM t WHERE x > 0 LIMIT 2) ORDER BY id").await
		);
	}

	/// The binder lowers a single-level `SELECT` as source, sort, projection, and resolves the key
	/// `id` against the projection: it reads `g`, the field the alias names, so the keys do not end
	/// in the record id.
	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn an_alias_named_id_is_not_total() {
		assert!(!total("SELECT g AS id FROM t ORDER BY id").await);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn an_alias_named_id_below_a_sort_is_not_total() {
		assert!(
			!total("SELECT VALUE id FROM (SELECT g AS id FROM t WHERE x > 0) ORDER BY id").await
		);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn a_group_is_not_total() {
		assert!(!total("SELECT g, array::group(id) AS ids FROM t GROUP BY g ORDER BY g").await);
	}

	#[tokio::test]
	#[ignore = "needs the binder to bind expressions (build::expr::bind_expr)"]
	async fn a_fallible_residual_is_not_admissible() {
		let sql = "SELECT VALUE id FROM t WHERE x > 0 AND (IF x = 1 { THROW \"from x1\" } ELSE IF x = 3 { THROW \"from x3\" } ELSE { true }) ORDER BY id";
		let ir = bound(sql).await;
		let source = source(&ir);
		assert!(total_order_for(&ir, source));
		let mut facts = FactCache::new();
		assert!(!order_change_admissible(
			&ir,
			&mut facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			source
		));
	}
}
