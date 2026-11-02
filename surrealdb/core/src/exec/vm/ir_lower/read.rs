//! Lowering of the access ops: the source a pipeline reads its rows from.
//!
//! A source is the [`Source`] opened at run time for the access path its op stands for, with
//! the payload that access path states. A source op carries everything its source holds as
//! attributes (the table, the direction, the whole access of an index scan, the decoded fields),
//! so lowering reads no catalog. The one thing an attribute does not hold is the resolved
//! reference to an index, which the scan opens the index by. An op names an index by table and
//! name. The [`IndexResolver`] given to lowering answers with the reference that planning
//! already resolved for that name.
//!
//! # What a source op lowers to
//!
//! - `phys.table_scan`: [`Source::Scan`] over the whole table, or [`Source::LightweightEdges`] for
//!   a lightweight relation, which stores no record range. The scan holds the pre-decode prefilter
//!   its op states, and the top-k key its op states where a sort reads its threshold.
//! - `phys.index_scan`, `phys.index_union`, `phys.bitmap`, `phys.index_lookup` and
//!   `phys.fulltext_scan`: [`Source::Index`], [`Source::IndexUnion`], [`Source::Bitmap`],
//!   [`Source::Lookup`] and [`Source::FullText`]. A bitmap holds the source of the region that
//!   delivers its fallback.
//! - `phys.key_range_scan`: [`Source::Scan`] over the key ranges the op's access names, in key
//!   order along its direction. Each bound of the access is a [`HostScalar`] the scan evaluates
//!   when it opens: the literal the access holds, or the value of the operand its slot names. An
//!   operand the pipeline's host code cannot evaluate reads as `NONE`, which widens the side or
//!   ends the equality run it bounds, and the predicate above the scan judges every row it yields.
//! - `phys.vacant`: [`Source::Vacant`], which raises the error of an undefined table when it opens.
//! - `phys.record_scan`: [`Source::Record`], or [`Source::LightweightEdges`] holding the record for
//!   a lightweight relation. The record is the [`HostScalar`] its operand denotes, and the source
//!   reads the table the record id expression names.
//! - `phys.values`: [`Source::Values`] over the [`HostScalar`] its operand denotes.
//! - `phys.count`: [`Source::Count`] of a table, of the record range its operand denotes (a host
//!   pipeline, [`CountedValue::Code`]), or of the index or bitmap the op names. The plan beside a
//!   count under a conditional permission is the unit [`UnitRequest::Subquery`] of its region,
//!   which the run reads where the caller's reads are judged.
//! - `phys.knn_scan`: [`Source::Knn`] over the index the op names. Its regions are what the search
//!   gates each candidate on ([`KnnGate`]), each a host pipeline of the program built by
//!   [`gate_code`](super::pipe::gate_code): the permissions alone, and the permissions followed by
//!   each condition the op's filter states. The search publishes each row's distance in the slot of
//!   the statement being lowered ([`Emitter::knn_slot`]), which `vector::distance::knn()` of the
//!   same statement reads.
//! - `phys.union` of such sources: [`Source::Union`] of the sources of its operands, in order.
//!
//! A value an operand denotes without evaluation ([`static_value`]) is a literal, a collection of
//! such values, or a record id with a plain key, and is a [`HostScalar::Const`]. An operand that
//! is anything else is a host pipeline of the program that evaluates it when the pipeline opens
//! ([`host_scalar_of`]).
//!
//! A scan whose op carries a top-k key reads the threshold of the sort named by `topk_sort`, the
//! sort that publishes it; with no such sort the key is read by nothing and the scan holds none.
//!
//! Every other op answers `NotYet` naming its mnemonic: a source that is a nested program or a
//! breaker is lowered with the construct that owns it.

use std::collections::HashMap;

use surrealdb_strand::{Strand, TableName};

use super::super::payload::{BranchRead, IndexBranch};
use super::super::plain::ValueData;
use super::super::program::{
	CountSource, CountVia, CountWalk, CountedValue, DeferredVector, HostScalar, KnnGate,
	KnnPrefilterSpec, ScanRange, ScanSpec, Source, TopKScan, UnionTopK, UnitId,
};
use super::dispatch::phys_refusal;
use super::emit::Emitter;
use super::expr::{Lowerer, Needs};
use super::pipe::gate_code;
use super::units::{LowerCx, UnitRequest};
use crate::exec::index::access_path::{self, BitmapPlan};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, KeyShape};
use crate::exec::ir::iface::IndexRef;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::logical::ops::SourceRefShape;
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{
	BitmapShape, BranchReadShape, CountAttrs, CountShape, IdBoundsShape, IndexBranchShape,
	KeyBoundShape, KeyEqShape, KeyRangeBoundShape, KeyRangeShape, KnnFilterShape,
	KnnPermissionShape, LookupKeyShape, PhysOp, PhysTag,
};
use crate::exec::operators::scan::id_range::ScanIdRange;
use crate::exec::operators::scan::key_range::{ScanEq, ScanKeyRange, ScanRangeOn, ScanSide};
use crate::exec::vm::dispatch::Reach;
use crate::expr::logical::Schema;
use crate::val::{Array, Object, RecordId, RecordIdKey, Set, Value};

/// Answers the resolved reference an index scan opens the index by.
///
/// The reference carries the definition the planning session read and the per-column facts the
/// analyzer derived, which an op does not hold. An implementor answers from what planning already
/// resolved and reads no catalog. It is `Send + Sync` because the compile driver holds it across
/// the planning callback's await.
pub(crate) trait IndexResolver: Send + Sync {
	/// The reference for the index `index` names, or `None` when planning resolved none.
	fn resolve(&self, index: &IndexRef) -> Option<access_path::IndexRef>;
}

/// The resolver that knows no index: a container that reads one is refused.
pub(crate) struct NoIndexes;

impl IndexResolver for NoIndexes {
	fn resolve(&self, _index: &IndexRef) -> Option<access_path::IndexRef> {
		None
	}
}

impl IndexResolver for HashMap<(TableName, Strand), access_path::IndexRef> {
	fn resolve(&self, index: &IndexRef) -> Option<access_path::IndexRef> {
		self.get(&(index.table.clone(), index.index.clone())).cloned()
	}
}

/// A source with what the program that holds it records of it.
#[derive(Debug)]
pub(super) struct Leaf {
	pub(super) source: Source,
	/// The table the source reads, for the program's `table`: the first member's for a union.
	pub(super) table: Option<TableName>,
}

/// The source the op `op` reads rows from, whose scans read no top-k threshold, in a program of
/// its own.
///
/// See [`lower_source_under`]. The host code a source needs lands in an emitter this holds, so
/// the source is one a caller can inspect and not one a program can run.
#[cfg(test)]
pub(super) fn lower_source(cx: &mut LowerCx<'_>, ir: &Ir, op: OpId) -> Result<Leaf, IrError> {
	lower_source_under(&mut Emitter::new(), cx, ir, op, None, &mut Needs::none())
}

/// The source the op `op` reads rows from, for a pipeline that feeds the sort `topk_sort`.
///
/// `op` is a source op of the physical dialect. Answers an internal error naming its mnemonic for
/// an op that is not one, for a union input that is not a scan, and for an index no
/// resolver knows, or for an op
/// whose operands or regions do not have the shape its tag states. A scan whose op carries a
/// top-k key reads the threshold of `topk_sort` when there is one. A value the source evaluates
/// when it opens is a host pipeline of `em`'s program, and what that pipeline needs of the run
/// is merged into `needs`.
pub(super) fn lower_source_under(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	op: OpId,
	topk_sort: Option<u16>,
	needs: &mut Needs,
) -> Result<Leaf, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::read: op"))?;
	let OpKind::Phys(kind) = &data.kind else {
		return Err(invariant("a source that is not a physical op"));
	};
	let indexes = cx.indexes();
	let resolve = |index: &IndexRef| -> Result<access_path::IndexRef, IrError> {
		indexes
			.resolve(index)
			.ok_or_else(|| invariant("an index the planning session never resolved"))
	};
	match kind {
		PhysOp::TableScan(attrs) => {
			let range = match &attrs.bounds {
				Some(bounds) if !attrs.lightweight => {
					Some(std::sync::Arc::new(id_range(em, cx, ir, data, bounds, needs)?))
				}
				Some(_) => {
					return Err(invariant("record-id bounds on a scan of a lightweight relation"));
				}
				None => None,
			};
			let source = if attrs.lightweight {
				Source::LightweightEdges {
					table: attrs.table.clone(),
					record: None,
					direction: attrs.direction,
				}
			} else {
				Source::Scan {
					table: attrs.table.clone(),
					direction: attrs.direction,
					topk: match (&attrs.topk, topk_sort) {
						(Some(key), Some(sort))
							if key.rejects_entries || key.coarse_cutoff.is_some() =>
						{
							Some(TopKScan::new(key, sort))
						}
						_ => None,
					},
					prefilter: attrs
						.prefilter
						.as_ref()
						.map(|shape| plain(shape.clone(), "a scan's prefilter"))
						.transpose()?,
					range,
					probes: attrs
						.probes
						.as_deref()
						.map(|set| scan_probes(set, &resolve))
						.transpose()?
						.map(std::sync::Arc::new),
				}
			};
			Ok(Leaf {
				source,
				table: Some(attrs.table.clone()),
			})
		}
		PhysOp::IndexScan(attrs) => Ok(Leaf {
			source: Source::Index {
				index: resolve(&attrs.index)?,
				access: plain(attrs.access.clone(), "an index access")?,
				direction: attrs.direction,
				decode: attrs.decode.clone(),
			},
			table: Some(attrs.table.clone()),
		}),
		PhysOp::IndexUnion(attrs) => {
			let mut branches = Vec::with_capacity(attrs.branches.len());
			for shape in &attrs.branches {
				branches.push(branch(shape, &resolve)?);
			}
			Ok(Leaf {
				source: Source::IndexUnion {
					branches,
					merge: attrs.merge.clone(),
					decode: attrs.decode.clone(),
					topk: match (&attrs.topk, topk_sort) {
						(Some((key, sum)), Some(sort)) if key.rejects_entries => Some(UnionTopK {
							scan: TopKScan::new(key, sort),
							sum: sum.clone(),
						}),
						_ => None,
					},
				},
				table: Some(attrs.table.clone()),
			})
		}
		PhysOp::Bitmap(attrs) => {
			let fallback = match data.regions.as_slice() {
				[] => None,
				[region] => Some(Box::new(
					lower_region_source(em, cx, ir, *region, topk_sort, needs)?.source,
				)),
				_ => return Err(invariant("a bitmap with more than one fallback region")),
			};
			Ok(Leaf {
				source: Source::Bitmap {
					table: attrs.table.clone(),
					root: plain(bitmap_plan(&attrs.root, &resolve)?, "a bitmap plan")?,
					fallback,
					decode: attrs.decode.clone(),
				},
				table: Some(attrs.table.clone()),
			})
		}
		PhysOp::IndexLookup(attrs) => {
			let outer = match attrs.key {
				LookupKeyShape::Parent => {
					let [key] = <[ValueId; 1]>::try_from(data.operands.as_slice())
						.map_err(|_| invariant("an index lookup with other than one operand"))?;
					Some(parent_path(ir, key)?)
				}
				LookupKeyShape::Probe => None,
			};
			Ok(Leaf {
				source: Source::Lookup {
					table: attrs.table.clone(),
					index: resolve(&attrs.index)?,
					outer,
				},
				table: Some(attrs.table.clone()),
			})
		}
		PhysOp::FullTextScan(attrs) => Ok(Leaf {
			source: Source::FullText {
				table: attrs.table.clone(),
				index: resolve(&attrs.index)?,
				query: attrs.query.clone(),
				operator: attrs.operator.clone(),
				decode: attrs.decode.clone(),
				topk: match (&attrs.topk, topk_sort) {
					(Some(key), Some(sort)) if key.rejects_entries => {
						Some(TopKScan::new(key, sort))
					}
					_ => None,
				},
			},
			table: Some(attrs.table.clone()),
		}),
		PhysOp::KeyRangeScan(attrs) => {
			let range = key_range(em, cx, ir, data, &attrs.access, needs)?;
			Ok(Leaf {
				source: Source::Scan {
					table: attrs.table.clone(),
					direction: attrs.access.direction,
					topk: match (&attrs.topk, topk_sort) {
						(Some(key), Some(sort))
							if key.rejects_entries || key.coarse_cutoff.is_some() =>
						{
							Some(TopKScan::new(key, sort))
						}
						_ => None,
					},
					prefilter: attrs
						.prefilter
						.as_ref()
						.map(|shape| plain(shape.clone(), "a scan's prefilter"))
						.transpose()?,
					range: Some(std::sync::Arc::new(range)),
					probes: None,
				},
				table: Some(attrs.table.clone()),
			})
		}
		PhysOp::Vacant {
			table,
		} => Ok(Leaf {
			source: Source::Vacant {
				table: table.clone(),
			},
			table: Some(table.clone()),
		}),
		PhysOp::RecordScan(attrs) => {
			let [value] = <[ValueId; 1]>::try_from(data.operands.as_slice())
				.map_err(|_| invariant("a record read with other than one operand"))?;
			let table = attrs.table.clone();
			let record = host_scalar_of(em, cx, ir, value, needs)?;
			let source = if attrs.lightweight {
				Source::LightweightEdges {
					table: table.clone(),
					record: Some(record),
					direction: crate::kvs::Direction::Forward,
				}
			} else {
				Source::Record {
					table: table.clone(),
					record,
					for_update: attrs.for_update,
				}
			};
			Ok(Leaf {
				source,
				table: Some(table),
			})
		}
		PhysOp::Values {
			read,
		} => {
			let [value] = <[ValueId; 1]>::try_from(data.operands.as_slice())
				.map_err(|_| invariant("a values read with other than one operand"))?;
			*needs = needs.merged(&Needs {
				reach: Some(values_reach(ir, value)),
				..Needs::none()
			});
			Ok(Leaf {
				source: Source::Values {
					value: host_scalar_of(em, cx, ir, value, needs)?,
					read: *read,
				},
				table: None,
			})
		}
		PhysOp::Count(attrs) => lower_count(em, cx, ir, data, attrs, &resolve, needs),
		PhysOp::KnnScan(attrs) => {
			let gate = match &attrs.filter {
				Some(filter) => Some(knn_gate(em, cx, ir, data, filter, &resolve, needs)?),
				None if data.regions.is_empty() => None,
				None => return Err(invariant("a KNN scan with regions and no filter")),
			};
			let vector_query = match data.operands.as_slice() {
				[] => None,
				[vector] => {
					let text = match &attrs.vector_text {
						Some(text) => std::sync::Arc::from(text.as_str()),
						None => {
							return Err(invariant(
								"a KNN scan reading its query vector with no text for it",
							));
						}
					};
					Some(em.knn_search(*vector, |em| {
						Ok(DeferredVector {
							scalar: host_scalar_of(em, cx, ir, *vector, needs)?,
							text,
						})
					})?)
				}
				_ => return Err(invariant("a KNN scan with more than its query vector")),
			};
			Ok(Leaf {
				source: Source::Knn {
					table: attrs.table.clone(),
					index: resolve(&attrs.index)?,
					vector: std::sync::Arc::clone(&attrs.vector),
					vector_query,
					k: attrs.k,
					ef: attrs.ef,
					slot: Some(em.knn_slot()),
					gate,
					ids_only: attrs.ids_only,
				},
				table: Some(attrs.table.clone()),
			})
		}
		PhysOp::Union => {
			let mut sources = Vec::with_capacity(data.operands.len());
			let mut table = None;
			for value in &data.operands {
				let member =
					lower_source_under(em, cx, ir, producer_of(ir, *value)?, topk_sort, needs)?;
				table = table.or(member.table);
				sources.push(member.source);
			}
			Ok(Leaf {
				source: Source::Union(sources),
				table,
			})
		}
		// A pipeline opens this only for a source tag. Under a union or a bitmap's fallback, an
		// input that is not a scan is an internal error as well.
		other @ (PhysOp::KnnTopK(_)
		| PhysOp::Enforced {
			..
		}
		| PhysOp::Subquery
		| PhysOp::Probe {
			..
		}
		| PhysOp::Join(_)
		| PhysOp::Distinct {
			..
		}
		| PhysOp::PermissionFilter(_)
		| PhysOp::Layer(_)
		| PhysOp::Filter
		| PhysOp::Fetch
		| PhysOp::Split {
			..
		}
		| PhysOp::Sort(_)
		| PhysOp::Window(_)
		| PhysOp::Aggregate(_)
		| PhysOp::Limit {
			..
		}
		| PhysOp::Project(_)
		| PhysOp::Timeout
		| PhysOp::VersionScope
		| PhysOp::Only
		| PhysOp::Explain {
			..
		}
		| PhysOp::Eval
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
		| PhysOp::Match {
			..
		}
		| PhysOp::MatchStep(_)
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
		| PhysOp::Show(_)
		| PhysOp::Write(_)
		| PhysOp::WriteStage {
			..
		}
		| PhysOp::Events(_)
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
		| PhysOp::Loop {
			..
		}
		| PhysOp::Pipeline(_)
		| PhysOp::Choose(_)
		| PhysOp::Prefetch(_)
		| PhysOp::Materialize {
			..
		}
		| PhysOp::LoadUnit(_)
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next) => Err(IrError::Invariant {
			what: phys_refusal(other.tag()),
		}),
	}
}

/// The source the region `region` delivers: the op that defines the value it yields.
fn lower_region_source(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	region: RegionId,
	topk_sort: Option<u16>,
	needs: &mut Needs,
) -> Result<Leaf, IrError> {
	let last = ops_in(ir, region).last().ok_or_else(|| invariant("a source region with no op"))?;
	let terminator = ir.op(last).ok_or(IrError::Stale("ir_lower::read: op"))?;
	match (&terminator.kind, terminator.operands.as_slice()) {
		(OpKind::Phys(PhysOp::Yield), [yielded]) => {
			lower_source_under(em, cx, ir, producer_of(ir, *yielded)?, topk_sort, needs)
		}
		_ => Err(invariant("a source region that does not yield one value")),
	}
}

/// The op that defines the rows `value` holds.
///
/// Rows are always the result of an op. No region kind takes rows as a block argument: a
/// region's arguments are its row, its element or its named images, which are values. A unit
/// copied from a region ([`Ir::copy_region`]) takes as arguments only the values its code reads
/// from outside, and a statement's rows are bound inside the statement's own region. A pipeline
/// region takes no argument. No pass shares a relational op across regions: CSE merges only
/// expression ops with no region, and the e-graph's write-back builds one subtree per anchor and
/// never shares what the program did not. A block argument read as rows is therefore an invariant
/// break. A caller that asks of a value that may hold any kind of value, not only rows, reads
/// the error as "no producer".
pub(super) fn producer_of(ir: &Ir, value: ValueId) -> Result<OpId, IrError> {
	match ir.value(value).ok_or(IrError::Stale("ir_lower::read: value"))?.def {
		ValueDef::OpResult {
			op,
			..
		} => Ok(op),
		ValueDef::BlockArg {
			..
		} => Err(IrError::Invariant {
			what: "ir_lower::read: rows no physical op produces",
		}),
	}
}

/// Whether `tag` is an op [`lower_source`] reads rows from, which a pipeline starts at.
pub(super) fn is_source(tag: PhysTag) -> bool {
	matches!(
		tag,
		PhysTag::TableScan
			| PhysTag::KeyRangeScan
			| PhysTag::IndexScan
			| PhysTag::IndexUnion
			| PhysTag::Bitmap
			| PhysTag::IndexLookup
			| PhysTag::FullTextScan
			| PhysTag::KnnScan
			| PhysTag::RecordScan
			| PhysTag::Values
			| PhysTag::Count
			| PhysTag::Vacant
			| PhysTag::Union
	)
}

/// The tables a value source reads records of by id: those of the record ids, ranges and tables
/// its value holds, as its elements and the elements of an array among them, whether the value is
/// known when the program is lowered or built by record-id and table ops whose keys only the run
/// computes; and any table besides where some part of the value only the run knows. The program
/// reads those records through the units of their tables ([`crate::exec::vm::dispatch`]), as a
/// `FETCH` reads the records its links name.
fn values_reach(ir: &Ir, value: ValueId) -> Reach {
	let mut tables = std::collections::BTreeSet::new();
	if named_tables(ir, value, 0, &mut tables) {
		Reach::Tables(tables)
	} else {
		Reach::Any(tables)
	}
}

/// Adds to `tables` the tables of the records `value` names at `depth`: 0 for the value itself, 1
/// for an element of it, 2 for an element of an array among its elements. Whether every part of
/// it is known.
fn named_tables(
	ir: &Ir,
	value: ValueId,
	depth: u8,
	tables: &mut std::collections::BTreeSet<TableName>,
) -> bool {
	fn of_value(value: &Value, depth: u8, tables: &mut std::collections::BTreeSet<TableName>) {
		match value {
			Value::RecordId(rid) => {
				tables.insert(rid.table.clone());
			}
			Value::Table(table) => {
				tables.insert(table.clone());
			}
			Value::Array(items) if depth < 2 => {
				for item in items.iter() {
					of_value(item, depth + 1, tables);
				}
			}
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => {}
		}
	}
	if let Some(known) = static_value(ir, value) {
		of_value(&known, depth, tables);
		return true;
	}
	let Some(ValueDef::OpResult {
		op,
		index: 0,
	}) = ir.value(value).map(|data| data.def)
	else {
		return false;
	};
	let Some(data) = ir.op(op) else {
		return false;
	};
	match &data.kind {
		OpKind::Expr(ExprOp::RecordId(attrs)) => {
			tables.insert(attrs.table.clone());
			true
		}
		OpKind::Expr(ExprOp::Table {
			name,
		}) => {
			tables.insert(name.clone());
			true
		}
		OpKind::Expr(ExprOp::Array) if depth < 2 => {
			let mut known = true;
			for item in &data.operands {
				known &= named_tables(ir, *item, depth + 1, tables);
			}
			known
		}
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// The value `value` denotes without evaluation, when it denotes one.
///
/// A literal is one, and so is a collection of such values. A record id is one when its key is
/// a plain number, string or uuid: a generated key is allocated when the statement runs and the
/// compound keys hold expressions. An object is built through a sorted map, as evaluation builds
/// it, so key order and the resolution of duplicate keys agree with it.
pub(super) fn static_value(ir: &Ir, value: ValueId) -> Option<Value> {
	let ValueDef::OpResult {
		op,
		index: 0,
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	let OpKind::Expr(expr) = &data.kind else {
		return None;
	};
	match expr {
		ExprOp::Literal(literal) => Some((**literal).clone()),
		ExprOp::Array => data
			.operands
			.iter()
			.map(|item| static_value(ir, *item))
			.collect::<Option<Vec<_>>>()
			.map(|items| Value::Array(Array(items))),
		ExprOp::Set => {
			let mut set = Set::new();
			for item in &data.operands {
				set.insert(static_value(ir, *item)?);
			}
			Some(Value::Set(set))
		}
		ExprOp::Object {
			keys,
		} => {
			if keys.len() != data.operands.len() {
				return None;
			}
			let mut map = std::collections::BTreeMap::new();
			for (key, item) in keys.iter().zip(&data.operands) {
				map.insert(key.clone(), static_value(ir, *item)?);
			}
			Some(Value::Object(Object::from(map)))
		}
		ExprOp::RecordId(attrs) => {
			let key = match &attrs.key_shape {
				KeyShape::Number(n) => RecordIdKey::Number(*n),
				KeyShape::String(s) => RecordIdKey::String(s.clone()),
				KeyShape::Uuid(u) => RecordIdKey::Uuid(*u),
				KeyShape::Generate(_)
				| KeyShape::Array {
					..
				}
				| KeyShape::Object {
					..
				}
				| KeyShape::Range(_) => return None,
			};
			Some(Value::RecordId(RecordId::new(attrs.table.clone(), key)))
		}
		ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::Table {
			..
		}
		| ExprOp::Mock(_)
		| ExprOp::Constant {
			..
		}
		| ExprOp::Field {
			..
		}
		| ExprOp::ComputedRead(_)
		| ExprOp::Doc
		| ExprOp::Access {
			..
		}
		| ExprOp::Deref {
			..
		}
		| ExprOp::Index
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Dispatch(_)
		| ExprOp::Method {
			..
		}
		| ExprOp::Destructure {
			..
		}
		| ExprOp::Optional
		| ExprOp::RepeatRecurse
		| ExprOp::SetField {
			..
		}
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Prefix(_)
		| ExprOp::Convert(_)
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Invoke
		| ExprOp::Binary {
			..
		}
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::Call(_)
		| ExprOp::ClosureCreate(_)
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Subquery(_)
		| ExprOp::Yield
		| ExprOp::Truthy
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::DepthCharge {
			..
		} => None,
	}
}

/// The declared kinds of the rows the source op `op` reads, when it reads one table's rows.
///
/// A union reads rows of more than one declaration, so it holds none.
pub(super) fn schema_of(ir: &Ir, op: OpId) -> Option<std::sync::Arc<Schema>> {
	let OpKind::Phys(kind) = &ir.op(op)?.kind else {
		return None;
	};
	match kind {
		PhysOp::TableScan(attrs) => attrs.read.schema.clone(),
		PhysOp::IndexScan(attrs) => attrs.read.schema.clone(),
		PhysOp::IndexUnion(attrs) => attrs.read.schema.clone(),
		PhysOp::Bitmap(attrs) => attrs.read.schema.clone(),
		PhysOp::IndexLookup(attrs) => attrs.read.schema.clone(),
		PhysOp::FullTextScan(attrs) => attrs.read.schema.clone(),
		PhysOp::RecordScan(attrs) => attrs.read.schema.clone(),
		PhysOp::KeyRangeScan(attrs) => attrs.read.schema.clone(),
		PhysOp::KnnScan(attrs) => attrs.read.schema.clone(),
		PhysOp::KnnTopK(_)
		| PhysOp::Count(_)
		| PhysOp::Vacant {
			..
		}
		| PhysOp::Enforced {
			..
		}
		| PhysOp::Values {
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
		| PhysOp::PermissionFilter(_)
		| PhysOp::Layer(_)
		| PhysOp::Filter
		| PhysOp::Fetch
		| PhysOp::Split {
			..
		}
		| PhysOp::Sort(_)
		| PhysOp::Window(_)
		| PhysOp::Aggregate(_)
		| PhysOp::Limit {
			..
		}
		| PhysOp::Project(_)
		| PhysOp::Timeout
		| PhysOp::VersionScope
		| PhysOp::Only
		| PhysOp::Explain {
			..
		}
		| PhysOp::Eval
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
		| PhysOp::Match {
			..
		}
		| PhysOp::MatchStep(_)
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
		| PhysOp::Show(_)
		| PhysOp::Write(_)
		| PhysOp::WriteStage {
			..
		}
		| PhysOp::Events(_)
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
		| PhysOp::Loop {
			..
		}
		| PhysOp::Pipeline(_)
		| PhysOp::Choose(_)
		| PhysOp::Prefetch(_)
		| PhysOp::Materialize {
			..
		}
		| PhysOp::LoadUnit(_)
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next => None,
	}
}

/// The host scalar that evaluates `value` when the pipeline opens.
///
/// A value that denotes a constant is the constant. Any other is a host pipeline of the program
/// `em` builds, over no row; what it needs of the run is merged into `needs`. A constant holding a
/// closure is an internal error, since no program holds a closure value.
pub(super) fn host_scalar_of(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	value: ValueId,
	needs: &mut Needs,
) -> Result<HostScalar, IrError> {
	if let Some(constant) = static_value(ir, value) {
		return ValueData::new(constant, "a host scalar").map(HostScalar::Const).map_err(|_| {
			IrError::Invariant {
				what: "ir_lower::read: a host scalar holding a closure",
			}
		});
	}
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	let scalar = lowerer.host_scalar(value)?;
	*needs = needs.merged(&lowerer.needs());
	Ok(scalar)
}

/// Appends the host scalar of `bound` to `bounds`, the values of a scan's bounds in slot order,
/// and answers its slot.
///
/// A literal is a constant. An operand is the host scalar of the value the op holds at that
/// position, or `NONE` when the host code cannot evaluate it: two answers are not swallowed, a
/// bound whose plan depends on a catalog change, which late-binds the statement
/// ([`IrError::LateBind`]), and a refusal of the language ([`IrError::Rejected`]).
fn bound_slot(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	data: &crate::exec::ir::core::op::OpData,
	needs: &mut Needs,
	bounds: &mut Vec<HostScalar>,
	bound: &KeyBoundShape,
) -> Result<u16, IrError> {
	let at = u16::try_from(bounds.len()).map_err(|_| IrError::Capacity("key bounds"))?;
	let scalar = match bound {
		KeyBoundShape::Literal(value) => {
			HostScalar::Const(ValueData::new(value.clone(), "a key bound").map_err(|_| {
				IrError::Invariant {
					what: "ir_lower::read: a key bound holding a closure",
				}
			})?)
		}
		KeyBoundShape::Operand(operand) => {
			let value = *data
				.operands
				.get(usize::from(*operand))
				.ok_or_else(|| invariant("a key bound past the operands of its scan"))?;
			match host_scalar_of(em, cx, ir, value, needs) {
				Ok(scalar) => scalar,
				Err(
					error @ (IrError::LateBind {
						..
					}
					| IrError::Rejected(_)),
				) => return Err(error),
				Err(_) => HostScalar::Const(
					ValueData::new(Value::None, "a key bound")
						.map_err(|_| IrError::Stale("ir_lower::read: a key bound"))?,
				),
			}
		}
	};
	bounds.push(scalar);
	Ok(at)
}

/// The record-id bounds of the table scan `data`, as the scan builds its key range when it
/// opens: the shape, and a host scalar per bound.
///
/// Bounds are numbered in the order [`IdBoundsShape::bounds`] lists them, which is the order of
/// the scan's operand slots, and are read as [`key_range`] reads its bounds.
fn id_range(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	data: &crate::exec::ir::core::op::OpData,
	access: &IdBoundsShape,
	needs: &mut Needs,
) -> Result<ScanRange, IrError> {
	if data.operands.len() != access.holes() {
		return Err(invariant("a table scan whose operands are not the holes of its bounds"));
	}
	let mut bounds = Vec::new();
	let mut side = |side: &Option<KeyRangeBoundShape>| -> Result<Option<ScanSide>, IrError> {
		side.as_ref()
			.map(|side| {
				Ok(ScanSide {
					bound: bound_slot(em, cx, ir, data, needs, &mut bounds, &side.bound)?,
					inclusive: side.inclusive,
				})
			})
			.transpose()
	};
	let spec = ScanIdRange {
		low: side(&access.low)?,
		high: side(&access.high)?,
	};
	Ok(ScanRange {
		spec: ScanSpec::RecordKey(spec),
		bounds: bounds.into_boxed_slice(),
	})
}

/// The key ranges of the access `access` of the key-range scan `data`, as the scan builds them when
/// it opens: the shape, and a host scalar per bound.
///
/// Bounds are numbered in the order [`KeyRangeShape::bounds`] lists them, which is the order of
/// the access's operand slots. A bound that is an operand the host code cannot evaluate is `NONE`,
/// as a deferred bound is. Two answers are not swallowed: a bound whose plan depends on a
/// catalog change late-binds the statement ([`IrError::LateBind`]), which plans it when it is
/// reached; and a refusal of the language ([`IrError::Rejected`]) fails the
/// statement, before any bound is lowered. A literal bound holding a
/// closure is an internal error, since no program holds a closure value.
fn key_range(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	data: &crate::exec::ir::core::op::OpData,
	access: &KeyRangeShape,
	needs: &mut Needs,
) -> Result<ScanRange, IrError> {
	if data.operands.len() != access.holes() {
		return Err(invariant("a key-range scan whose operands are not the holes of its access"));
	}
	let mut bounds = Vec::new();
	let mut slot = |bound: &KeyBoundShape| -> Result<u16, IrError> {
		bound_slot(em, cx, ir, data, needs, &mut bounds, bound)
	};
	let mut eq = Vec::with_capacity(access.eq.len());
	for pinned in &access.eq {
		eq.push(match pinned {
			KeyEqShape::One(bound) => ScanEq::One(slot(bound)?),
			KeyEqShape::In(list) => ScanEq::In(
				list.iter().map(&mut slot).collect::<Result<Vec<_>, _>>()?.into_boxed_slice(),
			),
		});
	}
	let range = match &access.range {
		Some(range) => {
			let mut side = |side: &Option<crate::exec::ir::phys::ops::KeyRangeBoundShape>| -> Result<Option<ScanSide>, IrError> {
				side.as_ref()
					.map(|side| {
						Ok(ScanSide {
							bound: slot(&side.bound)?,
							inclusive: side.inclusive,
						})
					})
					.transpose()
			};
			Some(ScanRangeOn {
				low: side(&range.low)?,
				high: side(&range.high)?,
			})
		}
		None => None,
	};
	Ok(ScanRange {
		spec: ScanSpec::Components(ScanKeyRange {
			kinds: access.key.components.iter().map(|component| component.kind.clone()).collect(),
			eq: eq.into_boxed_slice(),
			range,
		}),
		bounds: bounds.into_boxed_slice(),
	})
}

/// The count source of the op `data`, which holds `attrs`.
///
/// A region of kind `Clause(Where)` states the condition of an indexed count, which the access the
/// op names already carries. A region of kind `Subquery` is the statement as the ordinary plan
/// answers it, which a count under a conditional permission runs when the caller's reads are
/// judged: it is the unit [`UnitRequest::Subquery`] of that region, run with no inputs, so a region
/// that reads a value from outside it other than the statement clock is an internal error (the
/// count producer declines that statement). A record range is counted as the value of a host
/// pipeline of `em`'s program, whose needs are merged into `needs`. A conditional permission with
/// no plan beside it, any other region, and a count of a source the statement resolves when it
/// runs are internal errors: the planner builds none of them.
fn lower_count(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	data: &crate::exec::ir::core::op::OpData,
	attrs: &CountAttrs,
	resolve: &dyn Fn(&IndexRef) -> Result<access_path::IndexRef, IrError>,
	needs: &mut Needs,
) -> Result<Leaf, IrError> {
	let mut beside = None;
	for region in &data.regions {
		let kind = ir.region(*region).ok_or(IrError::Stale("ir_lower::read: region"))?.kind;
		match kind {
			RegionKind::Clause(ClauseRole::Where) => {}
			RegionKind::Subquery if beside.is_none() => beside = Some(*region),
			RegionKind::Request
			| RegionKind::Transaction(_)
			| RegionKind::Statement(_)
			| RegionKind::Block
			| RegionKind::ValueBlock
			| RegionKind::StatementIf
			| RegionKind::ValueIf
			| RegionKind::StatementFor
			| RegionKind::ValueFor
			| RegionKind::Loop
			| RegionKind::Subquery
			| RegionKind::FunctionBody
			| RegionKind::ClosureBody
			| RegionKind::PermissionFrame
			| RegionKind::FieldFrame(_)
			| RegionKind::EventFrame
			| RegionKind::SubscriberFrame
			| RegionKind::Clause(_)
			| RegionKind::LazyOperand
			| RegionKind::ErrorScope(_)
			| RegionKind::ChooseArm
			| RegionKind::Pipeline
			| RegionKind::JoinLookupArm
			| RegionKind::WriteStage
			| RegionKind::CatalogUnit
			| RegionKind::Deadline
			| RegionKind::StoredClause(_) => {
				return Err(invariant("a count region that is neither its condition nor its plan"));
			}
		}
	}
	let ordinary = match (attrs.select, beside) {
		(super::super::payload::CountSelect::Conditional, Some(region)) => {
			Some(ordinary_unit(cx, ir, region)?)
		}
		(super::super::payload::CountSelect::Conditional, None) => {
			return Err(invariant("a count under a conditional permission with no plan beside it"));
		}
		(_, Some(_)) => return Err(invariant("a plan beside a count no permission judges")),
		(_, None) => None,
	};
	let (table, count, output) = match &attrs.shape {
		CountShape::All {
			source,
			output,
		} => {
			let (table, counted) = match source {
				SourceRefShape::Table(table) => (table.clone(), CountedValue::Table(table.clone())),
				SourceRefShape::RecordId => counted_record(em, cx, ir, data, needs)?,
				SourceRefShape::Mock | SourceRefShape::Param | SourceRefShape::Dynamic => {
					return Err(invariant("a count over a source resolved when it runs"));
				}
			};
			(
				table.clone(),
				CountSource::All {
					table,
					source: counted,
					select: attrs.select,
					deltas: attrs.deltas,
					ordinary,
				},
				output,
			)
		}
		CountShape::Indexed {
			table,
			output,
			btree,
			bitmap: fused,
		} => {
			let via = match (attrs.deltas, btree, fused) {
				(Some(index), _, _) => CountVia::Deltas {
					index,
				},
				(None, Some(btree), _) => CountVia::BTreeKeys(
					btree
						.iter()
						.map(|(index, access)| {
							Ok(CountWalk {
								index: resolve(index)?,
								access: plain(access.clone(), "a counted index access")?,
							})
						})
						.collect::<Result<_, IrError>>()?,
				),
				(None, None, Some(shape)) => {
					CountVia::Bitmap(plain(bitmap_plan(shape, resolve)?, "a counted bitmap plan")?)
				}
				(None, None, None) => return Err(invariant("an indexed count no index answers")),
			};
			(
				table.clone(),
				CountSource::Indexed {
					table: table.clone(),
					select: attrs.select,
					via,
					ordinary,
				},
				output,
			)
		}
	};
	Ok(Leaf {
		source: Source::Count {
			count,
			output: output.clone(),
			cap: attrs.cap,
		},
		table: Some(table),
	})
}

/// The table and the counted value of a count of the record range the one operand of `data`
/// denotes: the record id's table, and a host pipeline of `em`'s program that evaluates the range
/// when the count opens, whose needs are merged into `needs`.
///
/// The producer counts a record id only where its key is a range ([`ExprOp::RecordId`] with a
/// [`KeyShape::Range`] key), which names its table.
fn counted_record(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	data: &crate::exec::ir::core::op::OpData,
	needs: &mut Needs,
) -> Result<(TableName, CountedValue), IrError> {
	let [value] = <[ValueId; 1]>::try_from(data.operands.as_slice())
		.map_err(|_| invariant("a count of a record range with other than one operand"))?;
	let table = match producer_kind(ir, value) {
		Some(ExprOp::RecordId(attrs)) if matches!(attrs.key_shape, KeyShape::Range(_)) => {
			attrs.table.clone()
		}
		_ => return Err(invariant("a count of a record id that is not a record range")),
	};
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	let pipeline = lowerer.host_code(value)?;
	*needs = needs.merged(&lowerer.needs());
	Ok((table, CountedValue::Code(pipeline)))
}

/// The expression op whose first result is `value`, if one is.
fn producer_kind(ir: &Ir, value: ValueId) -> Option<&ExprOp> {
	let ValueDef::OpResult {
		op,
		index: 0,
	} = ir.value(value)?.def
	else {
		return None;
	};
	match &ir.op(op)?.kind {
		OpKind::Expr(kind) => Some(kind),
		OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// The unit that answers a count's statement as its ordinary plan, `region`.
///
/// The count source runs the unit with no inputs, so a value the region reads from outside it is
/// one the unit reads as an unbound name, which is `NONE`. That is the value the statement clock
/// is handed as where it is read only as the timestamp of `expr.now`
/// (`nested::read_only_as_clock`); any other value from outside is an internal error.
fn ordinary_unit(cx: &mut LowerCx<'_>, ir: &Ir, region: RegionId) -> Result<UnitId, IrError> {
	for value in ir.region_inputs(region)? {
		if !super::nested::read_only_as_clock(ir, region, value) {
			return Err(invariant("a count whose ordinary plan reads a value from outside it"));
		}
	}
	cx.unit_for(UnitRequest::Subquery {
		region,
	})
}

/// What the KNN scan `data`, whose filter is `filter`, gates each candidate on.
///
/// The op's regions are one `PermissionFrame` region per permission of the filter, each followed
/// by a `FieldFrame(Computed)` region per field it derives, then the residual, the `MATCHES`
/// condition and the prefilter's residual, each where the filter states it. Each gate is a host
/// pipeline of the program `em` builds that judges every permission, then its own condition; what
/// they need of the run is merged into `needs`.
fn knn_gate(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	data: &crate::exec::ir::core::op::OpData,
	filter: &KnnFilterShape,
	resolve: &dyn Fn(&IndexRef) -> Result<access_path::IndexRef, IrError>,
	needs: &mut Needs,
) -> Result<KnnGate, IrError> {
	let (mut permitted, conditions) = data
		.regions
		.split_at_checked(filter.permission_regions())
		.ok_or_else(|| invariant("a KNN scan with fewer regions than permissions"))?;
	let mut permissions: Vec<(&[RegionId], &KnnPermissionShape)> =
		Vec::with_capacity(filter.permissions.len());
	for shape in &filter.permissions {
		let (own, rest) = permitted.split_at(shape.regions());
		permissions.push((own, shape));
		permitted = rest;
	}
	let mut conditions = conditions.iter().copied();
	let mut condition = |present: bool| -> Result<Option<RegionId>, IrError> {
		if !present {
			return Ok(None);
		}
		conditions.next().map(Some).ok_or_else(|| invariant("a KNN condition with no region"))
	};
	let residual = condition(filter.residual)?;
	let matches_condition = condition(filter.matches_condition)?;
	let prefilter_residual =
		condition(filter.prefilter.as_ref().is_some_and(|prefilter| prefilter.residual))?;
	if conditions.next().is_some() {
		return Err(invariant("a KNN scan with a region its filter does not state"));
	}
	let mut gate = |cond: Option<RegionId>| gate_code(em, cx, ir, &permissions, cond, needs);
	let permission = match permissions.is_empty() {
		true => None,
		false => Some(gate(None)?),
	};
	let residual = residual.map(|cond| gate(Some(cond))).transpose()?;
	let matches_condition = matches_condition.map(|cond| gate(Some(cond))).transpose()?;
	let prefilter = match &filter.prefilter {
		Some(prefilter) => Some(KnnPrefilterSpec {
			root: plain(bitmap_plan(&prefilter.root, resolve)?, "a KNN prefilter")?,
			residual: prefilter_residual.map(|cond| gate(Some(cond))).transpose()?,
			uncovered_matches: prefilter.uncovered_matches,
		}),
		None => None,
	};
	Ok(KnnGate {
		permission,
		residual,
		matches_condition,
		prefilter,
	})
}

/// `data` as plain data, or an internal error when it holds a closure: no program
/// holds a closure value.
fn plain<T: super::super::plain::Values>(
	data: T,
	_what: &'static str,
) -> Result<ValueData<T>, IrError> {
	ValueData::new(data, "an access").map_err(|_| IrError::Invariant {
		what: "ir_lower::read: an access holding a closure",
	})
}

/// The selectivity probes of a table scan, over the indexes `resolve` knows.
fn scan_probes(
	set: &crate::exec::ir::phys::ops::ScanProbes,
	resolve: &dyn Fn(&IndexRef) -> Result<access_path::IndexRef, IrError>,
) -> Result<crate::exec::vm::program::ScanProbes, IrError> {
	let mut probes = Vec::with_capacity(set.probes.len());
	for (index, access) in &set.probes {
		probes.push((resolve(index)?, plain(access.clone(), "a selectivity probe")?));
	}
	Ok(crate::exec::vm::program::ScanProbes {
		probes: probes.into(),
		bound: set.bound.clone(),
	})
}

/// One scan of an index union.
fn branch(
	shape: &IndexBranchShape,
	resolve: &dyn Fn(&IndexRef) -> Result<access_path::IndexRef, IrError>,
) -> Result<IndexBranch, IrError> {
	Ok(IndexBranch {
		index: resolve(&shape.index)?,
		read: match &shape.read {
			BranchReadShape::BTree(access) => BranchRead::BTree(access.clone()),
			BranchReadShape::FullText {
				query,
				operator,
				..
			} => BranchRead::FullText {
				query: query.clone(),
				operator: operator.clone(),
			},
		},
		direction: shape.direction,
	})
}

/// The fused bitmap plan `shape` stands for.
pub(super) fn bitmap_plan(
	shape: &BitmapShape,
	resolve: &dyn Fn(&IndexRef) -> Result<access_path::IndexRef, IrError>,
) -> Result<BitmapPlan, IrError> {
	Ok(match shape {
		BitmapShape::BTree {
			index,
			access,
		} => BitmapPlan::BTree {
			index_ref: resolve(index)?,
			access: access.clone(),
		},
		BitmapShape::FullText {
			index,
			query,
			operator,
		} => BitmapPlan::FullText {
			index_ref: resolve(index)?,
			query: query.clone(),
			operator: operator.clone(),
		},
		BitmapShape::Graph {
			source,
			direction,
			edge_tables,
		} => BitmapPlan::Graph {
			source: source.clone(),
			direction: *direction,
			edge_tables: edge_tables.clone(),
		},
		BitmapShape::And(children) => BitmapPlan::And(
			children
				.iter()
				.map(|child| bitmap_plan(child, resolve))
				.collect::<Result<Vec<_>, _>>()?,
		),
		BitmapShape::Or(children) => BitmapPlan::Or(
			children
				.iter()
				.map(|child| bitmap_plan(child, resolve))
				.collect::<Result<Vec<_>, _>>()?,
		),
		BitmapShape::AndNot {
			base,
			subtract,
		} => BitmapPlan::AndNot {
			base: Box::new(bitmap_plan(base, resolve)?),
			subtract: Box::new(bitmap_plan(subtract, resolve)?),
		},
	})
}

/// The fields of `$parent` a lookup's key reads, outermost first.
///
/// A `Parent` lookup's key is the row of the statement the lookup is evaluated for, read through a
/// chain of field accesses: the scan reads the value through objects alone, without evaluating
/// anything. The chain's base is the row itself, which a region holds as a block argument or an
/// `expr.doc` and a statement outside every row as the free name `parent`. Any other base, or any
/// other op in the chain, is an internal error, because the planner only builds a `Parent` lookup
/// over such a path.
pub(super) fn parent_path(ir: &Ir, key: ValueId) -> Result<Vec<Strand>, IrError> {
	let mut path = Vec::new();
	let mut at = key;
	loop {
		let def = ir.value(at).ok_or(IrError::Stale("ir_lower::read: value"))?.def;
		let ValueDef::OpResult {
			op,
			..
		} = def
		else {
			path.reverse();
			return Ok(path);
		};
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::read: op"))?;
		match &data.kind {
			OpKind::Expr(ExprOp::Access {
				field,
			}) => {
				path.push(field.clone());
				at = *data
					.operands
					.first()
					.ok_or_else(|| invariant("a field access with no base"))?;
			}
			OpKind::Expr(ExprOp::Doc) => {
				path.reverse();
				return Ok(path);
			}
			OpKind::Expr(ExprOp::FreeParam {
				name,
			}) if name.as_str() == "parent" => {
				path.reverse();
				return Ok(path);
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
				return Err(invariant("a lookup key that is not a path into the parent row"));
			}
		}
	}
}

#[cfg(test)]
pub(super) mod tests {
	use super::*;
	use crate::catalog::{Index, IndexDefinition, IndexId};
	use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
	use crate::exec::ir::expr::ops::KeyShape;
	use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::phys::ops::{
		BitmapAttrs, FullTextScanAttrs, IndexLookupAttrs, IndexScanAttrs, IndexUnionAttrs,
		RecordScanAttrs, TableScanAttrs,
	};
	use crate::exec::vm::ir_lower::tests::Builder;
	use crate::expr::operator::{BooleanOperator, MatchesOperator};
	use crate::kvs::Direction;

	fn definition(name: &str) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(1),
			name: Strand::from(name),
			table_name: "t".into(),
			cols: Vec::new(),
			index: Index::Idx,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
			nulls_not_distinct: false,
		}
	}

	pub(in crate::exec::vm::ir_lower) fn named(name: &str) -> IndexRef {
		IndexRef {
			table: TableName::from("t"),
			index: Strand::from(name),
			position: 0,
			scalar: Box::new([]),
		}
	}

	fn matches_operator() -> MatchesOperator {
		MatchesOperator {
			rf: None,
			operator: BooleanOperator::And,
		}
	}

	/// A physical op of one result over no operand.
	fn rows(b: &mut Builder, op: PhysOp) -> OpId {
		rows_over(b, op, &[])
	}

	/// A physical op of one result over `operands`.
	fn rows_over(b: &mut Builder, op: PhysOp, operands: &[ValueId]) -> OpId {
		b.create_kind(OpKind::Phys(op), operands, 1)
	}

	pub(in crate::exec::vm::ir_lower) fn resolver()
	-> HashMap<(TableName, Strand), access_path::IndexRef> {
		let mut map = HashMap::new();
		map.insert(
			(TableName::from("t"), Strand::from("by_a")),
			access_path::IndexRef::new(&[definition("by_a")], 0),
		);
		map
	}

	fn plain_scan() -> PhysOp {
		PhysOp::TableScan(Box::new(TableScanAttrs {
			probes: None,
			bounds: None,
			prefilter: None,
			table: TableName::from("t"),
			direction: Direction::Forward,
			topk: None,
			lightweight: false,
			id_order: false,
			read: Default::default(),
		}))
	}

	fn scan(b: &mut Builder, lightweight: bool) -> OpId {
		let PhysOp::TableScan(mut attrs) = plain_scan() else {
			panic!("a table scan");
		};
		attrs.lightweight = lightweight;
		rows(b, PhysOp::TableScan(attrs))
	}

	#[test]
	fn a_table_scan_is_a_scan_of_the_whole_table_with_no_prefilter_or_key() {
		let mut b = Builder::new();
		let op = scan(&mut b, false);
		let leaf = lower_source(&mut LowerCx::detached(), &b.ir, op).expect("a table scan lowers");
		assert_eq!(leaf.table, Some(TableName::from("t")));
		assert_eq!(
			leaf.source,
			Source::Scan {
				table: TableName::from("t"),
				direction: Direction::Forward,
				topk: None,
				prefilter: None,
				range: None,
				probes: None,
			}
		);
	}

	#[test]
	fn a_scan_judges_the_stored_bytes_by_the_prefilter_its_op_states() {
		use crate::exec::index::key_range::DeliveredKeyOrder;
		use crate::exec::ir::phys::ops::{KeyRangeScanAttrs, key_range_fixtures};
		use crate::exec::pre_decode_filter::PreDecodeShape;
		let shape = PreDecodeShape::Leaf {
			path: std::sync::Arc::from([crate::exec::object_extract::PathSegment::from("age")]),
			op: crate::expr::BinaryOperator::MoreThan,
			literal: Value::from(1_i64),
			reversed: false,
		};
		let mut b = Builder::new();
		let table_scan = rows(
			&mut b,
			PhysOp::TableScan(Box::new(TableScanAttrs {
				probes: None,
				bounds: None,
				prefilter: Some(shape.clone()),
				table: TableName::from("t"),
				direction: Direction::Forward,
				topk: None,
				lightweight: false,
				id_order: false,
				read: Default::default(),
			})),
		);
		let key_range = rows_over(
			&mut b,
			PhysOp::KeyRangeScan(Box::new(KeyRangeScanAttrs {
				prefilter: Some(shape.clone()),
				table: TableName::from("t"),
				read: Default::default(),
				access: KeyRangeShape {
					key: key_range_fixtures::pair(),
					eq: Vec::new(),
					range: None,
					direction: Direction::Forward,
					delivers: DeliveredKeyOrder::None,
					ordered_by: Vec::new(),
				},
				topk: None,
			})),
			&[],
		);
		for op in [table_scan, key_range] {
			let leaf = lower_source(&mut LowerCx::detached(), &b.ir, op).expect("a scan lowers");
			let Source::Scan {
				prefilter: Some(held),
				..
			} = leaf.source
			else {
				panic!("a scan with a prefilter");
			};
			assert_eq!(*held, shape);
		}
	}

	#[test]
	fn a_lightweight_table_is_read_as_its_edges() {
		let mut b = Builder::new();
		let op = scan(&mut b, true);
		let leaf =
			lower_source(&mut LowerCx::detached(), &b.ir, op).expect("a lightweight scan lowers");
		assert_eq!(
			leaf.source,
			Source::LightweightEdges {
				table: TableName::from("t"),
				record: None,
				direction: Direction::Forward,
			}
		);
	}

	#[test]
	fn a_vacant_table_is_a_vacant_source() {
		let mut b = Builder::new();
		let op = rows(
			&mut b,
			PhysOp::Vacant {
				table: TableName::from("t"),
			},
		);
		let leaf = lower_source(&mut LowerCx::detached(), &b.ir, op).expect("a vacant read lowers");
		assert_eq!(
			leaf.source,
			Source::Vacant {
				table: TableName::from("t")
			}
		);
	}

	#[test]
	fn an_index_the_planning_session_never_resolved_is_an_internal_error() {
		let mut b = Builder::new();
		let op = rows(
			&mut b,
			PhysOp::FullTextScan(Box::new(FullTextScanAttrs {
				table: TableName::from("t"),
				index: named("by_a"),
				query: "x".to_owned(),
				operator: matches_operator(),
				column: None,
				decode: None,
				read: Default::default(),
				topk: None,
			})),
		);
		assert!(matches!(
			lower_source(&mut LowerCx::detached(), &b.ir, op),
			Err(IrError::Verify(_))
		));
		let leaf = lower_source(&mut LowerCx::detached().with_indexes(&resolver()), &b.ir, op)
			.expect("a resolved index lowers");
		assert!(matches!(leaf.source, Source::FullText { .. }));
	}

	#[test]
	fn an_index_scan_holds_its_whole_access_and_its_resolved_index() {
		let mut b = Builder::new();
		let access = access_path::BTreeAccess::literal_equality(crate::val::Value::from(1_i64));
		let op = rows(
			&mut b,
			PhysOp::IndexScan(Box::new(IndexScanAttrs {
				table: TableName::from("t"),
				index: named("by_a"),
				access: access.clone(),
				direction: Direction::Backward,
				ordered_by: None,
				pinned: Some(vec![crate::exec::field_path::FieldPath::field("a")]),
				stated: None,
				rechecked: false,
				decode: None,
				read: Default::default(),
			})),
		);
		let leaf = lower_source(&mut LowerCx::detached().with_indexes(&resolver()), &b.ir, op)
			.expect("an index scan lowers");
		let Source::Index {
			index,
			access: held,
			direction,
			..
		} = leaf.source
		else {
			panic!("an index source");
		};
		assert_eq!(index.idx, 0);
		assert_eq!(*held, access);
		assert_eq!(direction, Direction::Backward);
	}

	#[test]
	fn a_union_of_scans_is_the_union_of_their_sources_in_order() {
		let mut b = Builder::new();
		let first = scan(&mut b, false);
		let second = rows(
			&mut b,
			PhysOp::Vacant {
				table: TableName::from("u"),
			},
		);
		let (first, second) = (b.result_of(first), b.result_of(second));
		let union = rows_over(&mut b, PhysOp::Union, &[first, second]);
		let leaf = lower_source(&mut LowerCx::detached(), &b.ir, union).expect("a union lowers");
		let Source::Union(sources) = leaf.source else {
			panic!("a union source");
		};
		assert!(matches!(sources[0], Source::Scan { .. }));
		assert!(matches!(sources[1], Source::Vacant { .. }));
		assert_eq!(leaf.table, Some(TableName::from("t")));
		let empty = rows_over(&mut b, PhysOp::Union, &[]);
		let leaf =
			lower_source(&mut LowerCx::detached(), &b.ir, empty).expect("an empty union lowers");
		assert_eq!(leaf.source, Source::Union(Vec::new()));
		assert_eq!(leaf.table, None);
	}

	#[test]
	fn an_index_union_keeps_the_order_and_read_of_its_branches() {
		let mut b = Builder::new();
		let op = rows(
			&mut b,
			PhysOp::IndexUnion(Box::new(IndexUnionAttrs {
				topk: None,
				table: TableName::from("t"),
				branches: vec![IndexBranchShape {
					index: named("by_a"),
					read: BranchReadShape::FullText {
						query: "q".to_owned(),
						operator: matches_operator(),
						column: None,
						shared_doc_ids: true,
					},
					direction: Direction::Forward,
				}],
				merge: None,
				ordered_by: None,
				decode: None,
				read: Default::default(),
			})),
		);
		let leaf = lower_source(&mut LowerCx::detached().with_indexes(&resolver()), &b.ir, op)
			.expect("an index union lowers");
		let Source::IndexUnion {
			branches,
			..
		} = leaf.source
		else {
			panic!("an index union source");
		};
		assert_eq!(branches.len(), 1);
		assert!(matches!(branches[0].read, BranchRead::FullText { .. }));
	}

	#[test]
	fn a_bitmap_holds_the_source_of_its_fallback_region() {
		let mut b = Builder::new();
		let op = rows(
			&mut b,
			PhysOp::Bitmap(Box::new(BitmapAttrs {
				table: TableName::from("t"),
				root: BitmapShape::And(vec![BitmapShape::FullText {
					index: named("by_a"),
					query: "q".to_owned(),
					operator: matches_operator(),
				}]),
				decode: None,
				fallback: false,
				read: Default::default(),
			})),
		);
		let leaf = lower_source(&mut LowerCx::detached().with_indexes(&resolver()), &b.ir, op)
			.expect("a bitmap lowers");
		let Source::Bitmap {
			fallback,
			..
		} = leaf.source
		else {
			panic!("a bitmap source");
		};
		assert!(fallback.is_none());

		let mut b = Builder::new();
		let bitmap = rows(
			&mut b,
			PhysOp::Bitmap(Box::new(BitmapAttrs {
				table: TableName::from("t"),
				root: BitmapShape::Or(Vec::new()),
				decode: None,
				fallback: true,
				read: Default::default(),
			})),
		);
		b.open(bitmap, RegionKind::Pipeline, &[]);
		let fallback = scan(&mut b, false);
		let rows = b.result_of(fallback);
		b.add_phys(PhysOp::Yield, &[rows]);
		b.close();
		let leaf = lower_source(&mut LowerCx::detached(), &b.ir, bitmap)
			.expect("a bitmap with a fallback lowers");
		let Source::Bitmap {
			fallback,
			..
		} = leaf.source
		else {
			panic!("a bitmap source");
		};
		assert!(matches!(fallback.as_deref(), Some(Source::Scan { .. })));
	}

	#[test]
	fn a_parent_lookup_reads_a_path_into_parent_and_a_probe_lookup_none() {
		let mut b = Builder::new();
		let parent = b.add(
			ExprOp::FreeParam {
				name: Strand::from("parent"),
			},
			&[],
		);
		let owner = b.add(
			ExprOp::Access {
				field: Strand::from("owner"),
			},
			&[parent],
		);
		let key = b.add(
			ExprOp::Access {
				field: Strand::from("id"),
			},
			&[owner],
		);
		let attrs = |key| {
			Box::new(IndexLookupAttrs {
				table: TableName::from("t"),
				index: named("by_a"),
				field: Strand::from("a"),
				key,
				read: Default::default(),
			})
		};
		let lookup = rows_over(&mut b, PhysOp::IndexLookup(attrs(LookupKeyShape::Parent)), &[key]);
		let leaf = lower_source(&mut LowerCx::detached().with_indexes(&resolver()), &b.ir, lookup)
			.expect("a parent lookup lowers");
		let Source::Lookup {
			outer,
			..
		} = leaf.source
		else {
			panic!("a lookup source");
		};
		assert_eq!(outer, Some(vec![Strand::from("owner"), Strand::from("id")]));

		let probe = rows_over(&mut b, PhysOp::IndexLookup(attrs(LookupKeyShape::Probe)), &[key]);
		let leaf = lower_source(&mut LowerCx::detached().with_indexes(&resolver()), &b.ir, probe)
			.expect("a probe lookup lowers");
		assert!(matches!(
			leaf.source,
			Source::Lookup {
				outer: None,
				..
			}
		));

		// The row a region holds is the base of the path as well as the free name is.
		let row = b.add(ExprOp::Doc, &[]);
		let city = b.add(
			ExprOp::Access {
				field: Strand::from("city"),
			},
			&[row],
		);
		let from_row =
			rows_over(&mut b, PhysOp::IndexLookup(attrs(LookupKeyShape::Parent)), &[city]);
		let leaf =
			lower_source(&mut LowerCx::detached().with_indexes(&resolver()), &b.ir, from_row)
				.expect("a lookup of the row lowers");
		assert!(
			matches!(leaf.source, Source::Lookup { outer: Some(path), .. } if path == [Strand::from("city")])
		);

		// A key that is not a path into the parent row cannot be read without evaluating it.
		let other = b.literal(crate::val::Value::from(1_i64));
		let bad = rows_over(&mut b, PhysOp::IndexLookup(attrs(LookupKeyShape::Parent)), &[other]);
		assert!(matches!(
			lower_source(&mut LowerCx::detached().with_indexes(&resolver()), &b.ir, bad),
			Err(IrError::Verify(_))
		));
	}

	fn param(b: &mut Builder, name: &str) -> ValueId {
		b.add(
			ExprOp::FreeParam {
				name: Strand::from(name),
			},
			&[],
		)
	}

	#[test]
	fn a_parent_lookup_key_is_based_on_the_name_parent_alone() {
		let mut b = Builder::new();
		let parent = param(&mut b, "parent");
		let other = param(&mut b, "x");
		let on_parent = b.add(
			ExprOp::Access {
				field: Strand::from("a"),
			},
			&[parent],
		);
		let on_other = b.add(
			ExprOp::Access {
				field: Strand::from("a"),
			},
			&[other],
		);
		assert_eq!(
			parent_path(&b.ir, on_parent).expect("a path into $parent"),
			vec![Strand::from("a")]
		);
		assert!(matches!(parent_path(&b.ir, on_other), Err(IrError::Verify(_))));
	}

	/// A key-range scan of `t` over `[a: int, b: int]` that pins `a` to the operand at slot 0,
	/// bounds `b` below by the operand at slot 1 and above by the literal 9.
	fn key_range_scan(b: &mut Builder, operands: &[ValueId]) -> OpId {
		use crate::exec::index::key_range::DeliveredKeyOrder;
		use crate::exec::ir::phys::ops::{
			KeyRangeBoundShape, KeyRangeOnShape, KeyRangeScanAttrs, key_range_fixtures,
		};
		rows_over(
			b,
			PhysOp::KeyRangeScan(Box::new(KeyRangeScanAttrs {
				prefilter: None,
				table: TableName::from("t"),
				read: Default::default(),
				access: KeyRangeShape {
					key: key_range_fixtures::pair(),
					eq: vec![KeyEqShape::One(KeyBoundShape::Operand(0))],
					range: Some(KeyRangeOnShape {
						position: 1,
						low: Some(KeyRangeBoundShape {
							bound: KeyBoundShape::Operand(1),
							inclusive: true,
						}),
						high: Some(KeyRangeBoundShape {
							bound: KeyBoundShape::Literal(Value::from(9_i64)),
							inclusive: false,
						}),
					}),
					direction: Direction::Backward,
					delivers: DeliveredKeyOrder::Exact,
					ordered_by: vec![0, 1],
				},
				topk: None,
			})),
			operands,
		)
	}

	#[test]
	fn a_key_range_scan_numbers_its_bounds_and_holds_a_scalar_for_each() {
		let mut b = Builder::new();
		let a = b.literal(Value::from(3_i64));
		let low = param(&mut b, "low");
		let op = key_range_scan(&mut b, &[a, low]);
		let mut em = Emitter::new();
		let leaf = lower_source_under(
			&mut em,
			&mut LowerCx::detached(),
			&b.ir,
			op,
			None,
			&mut Needs::none(),
		)
		.expect("a key-range scan lowers");
		assert_eq!(leaf.table, Some(TableName::from("t")));
		let Source::Scan {
			direction,
			range: Some(range),
			topk: None,
			prefilter: None,
			..
		} = leaf.source
		else {
			panic!("a scan over key ranges");
		};
		assert_eq!(direction, Direction::Backward);
		let ScanSpec::Components(spec) = &range.spec else {
			panic!("a scan over the components of a record id");
		};
		assert_eq!(spec.kinds.len(), 2);
		assert_eq!(spec.eq.len(), 1);
		assert!(matches!(spec.eq[0], ScanEq::One(0)));
		let on = spec.range.expect("a range after the pinned run");
		assert_eq!(on.low.map(|side| (side.bound, side.inclusive)), Some((1, true)));
		assert_eq!(on.high.map(|side| (side.bound, side.inclusive)), Some((2, false)));
		assert_eq!(range.bounds.len(), 3);
		assert!(
			matches!(&range.bounds[0], HostScalar::Const(value) if **value == Value::from(3_i64))
		);
		assert!(matches!(range.bounds[1], HostScalar::Code(0)));
		assert!(
			matches!(&range.bounds[2], HostScalar::Const(value) if **value == Value::from(9_i64))
		);
		assert_eq!(em.code().len(), 0, "the bound's code is a host pipeline of its own");
	}

	/// A table scan of `t` bounded below by the operand at slot 0, exclusively, and above by the
	/// literal record id `t:9`, inclusively.
	fn bounded_table_scan(b: &mut Builder, operands: &[ValueId]) -> OpId {
		use crate::exec::ir::phys::ops::{IdBoundsShape, KeyRangeBoundShape};
		let PhysOp::TableScan(mut attrs) = plain_scan() else {
			panic!("a table scan");
		};
		attrs.bounds = Some(IdBoundsShape {
			low: Some(KeyRangeBoundShape {
				bound: KeyBoundShape::Operand(0),
				inclusive: false,
			}),
			high: Some(KeyRangeBoundShape {
				bound: KeyBoundShape::Literal(Value::RecordId(RecordId {
					table: TableName::from("t"),
					key: RecordIdKey::Number(9),
				})),
				inclusive: true,
			}),
		});
		rows_over(b, PhysOp::TableScan(attrs), operands)
	}

	#[test]
	fn a_bounded_table_scan_numbers_its_bounds_and_holds_a_scalar_for_each() {
		let mut b = Builder::new();
		let low = param(&mut b, "low");
		let op = bounded_table_scan(&mut b, &[low]);
		let mut em = Emitter::new();
		let leaf = lower_source_under(
			&mut em,
			&mut LowerCx::detached(),
			&b.ir,
			op,
			None,
			&mut Needs::none(),
		)
		.expect("a bounded table scan lowers");
		let Source::Scan {
			range: Some(range),
			..
		} = leaf.source
		else {
			panic!("a scan over record ids");
		};
		let ScanSpec::RecordKey(spec) = &range.spec else {
			panic!("a scan over record-id bounds");
		};
		assert_eq!(spec.low.map(|side| (side.bound, side.inclusive)), Some((0, false)));
		assert_eq!(spec.high.map(|side| (side.bound, side.inclusive)), Some((1, true)));
		assert_eq!(range.bounds.len(), 2);
		assert!(matches!(range.bounds[0], HostScalar::Code(0)));
		assert!(matches!(&range.bounds[1], HostScalar::Const(_)));
	}

	#[test]
	fn an_unbounded_table_scan_reads_no_range() {
		let mut b = Builder::new();
		let op = rows_over(&mut b, plain_scan(), &[]);
		let leaf = lower_source(&mut LowerCx::detached(), &b.ir, op).expect("a table scan lowers");
		assert!(matches!(
			leaf.source,
			Source::Scan {
				range: None,
				..
			}
		));
	}

	#[test]
	fn a_bounded_table_scan_takes_one_operand_for_each_hole() {
		let mut b = Builder::new();
		let op = bounded_table_scan(&mut b, &[]);
		assert!(matches!(
			lower_source(&mut LowerCx::detached(), &b.ir, op),
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				..
			}))
		));
	}

	#[test]
	fn a_key_bound_the_host_cannot_evaluate_reads_as_none() {
		let mut b = Builder::new();
		let a = b.literal(Value::from(3_i64));
		// A block argument no pipeline binds is a value host code cannot evaluate.
		let owner = rows(&mut b, PhysOp::Filter);
		b.open(owner, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown]);
		b.close();
		let region = *b.ir.op(owner).expect("the op exists").regions.last().expect("a region");
		let block = b.ir.region(region).expect("the region exists").blocks[0];
		let unbound = b.ir.block(block).expect("the block exists").args[0];
		let op = key_range_scan(&mut b, &[a, unbound]);
		let leaf =
			lower_source(&mut LowerCx::detached(), &b.ir, op).expect("a key-range scan lowers");
		let Source::Scan {
			range: Some(range),
			..
		} = leaf.source
		else {
			panic!("a scan over key ranges");
		};
		assert!(matches!(&range.bounds[1], HostScalar::Const(value) if **value == Value::None));
	}

	/// A bound whose expression the language refuses fails the statement with the refusal
	/// before any bound is lowered; it never reads as `NONE`.
	#[test]
	fn a_key_bound_the_language_refuses_fails_with_the_refusal() {
		use crate::exec::ir::expr::ops::NestedStrategy;
		use crate::exec::ir::logical::ops::{LetAttrs, LetBinding};
		let mut b = Builder::new();
		let a = b.literal(Value::from(3_i64));
		let owner = b.add_with_regions(ExprOp::Subquery(NestedStrategy::Apply), &[]);
		b.open(owner, RegionKind::Subquery, &[]);
		let bound = b.literal(Value::from(1_i64));
		b.add_phys(
			PhysOp::Let(Box::new(LetAttrs {
				name: Strand::new("a"),
				binding: LetBinding::Local,
				ty: None,
			})),
			&[bound],
		);
		b.close();
		let refused = b.result_of(owner);
		let op = key_range_scan(&mut b, &[a, refused]);
		assert!(matches!(
			lower_source(&mut LowerCx::detached(), &b.ir, op),
			Err(IrError::Rejected(_))
		));
	}

	#[test]
	fn a_key_range_scan_takes_one_operand_for_each_hole() {
		let mut b = Builder::new();
		let a = b.literal(Value::from(3_i64));
		let op = key_range_scan(&mut b, &[a]);
		assert!(matches!(
			lower_source(&mut LowerCx::detached(), &b.ir, op),
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				..
			}))
		));
	}

	/// A count op of `t` over `operands`, counting `source` as `select` judges it.
	fn count_op(
		b: &mut Builder,
		source: SourceRefShape,
		select: super::super::super::payload::CountSelect,
		operands: &[ValueId],
	) -> OpId {
		rows_over(
			b,
			PhysOp::Count(Box::new(CountAttrs {
				shape: CountShape::All {
					source,
					output: crate::exec::operators::scan::CountOutput::Fields(vec![
						"count".to_owned(),
					]),
				},
				select,
				deltas: None,
				cap: None,
			})),
			operands,
		)
	}

	#[test]
	fn a_count_of_a_record_range_counts_the_value_of_a_host_pipeline() {
		use crate::exec::ir::expr::ops::{KeyBound, KeyRange, RecordIdAttrs};
		let mut b = Builder::new();
		let record = b.add(
			ExprOp::RecordId(Box::new(RecordIdAttrs {
				table: TableName::from("t"),
				key_shape: KeyShape::Range(Box::new(KeyRange {
					start: std::ops::Bound::Included(KeyBound::Number(1)),
					end: std::ops::Bound::Included(KeyBound::Number(3)),
				})),
			})),
			&[],
		);
		let op = count_op(
			&mut b,
			SourceRefShape::RecordId,
			super::super::super::payload::CountSelect::Allow,
			&[record],
		);
		let mut em = Emitter::new();
		let mut cx = LowerCx::detached();
		let leaf = lower_source_under(&mut em, &mut cx, &b.ir, op, None, &mut Needs::none())
			.expect("a count of a record range lowers");
		assert_eq!(leaf.table, Some(TableName::from("t")));
		let Source::Count {
			count:
				CountSource::All {
					table,
					source: CountedValue::Code(0),
					ordinary: None,
					..
				},
			..
		} = leaf.source
		else {
			panic!("a count of the range host pipeline 0 emits");
		};
		assert_eq!(table, TableName::from("t"));
		assert!(cx.take_needed().is_empty(), "a count no permission judges plans no unit");
	}

	#[test]
	fn a_count_of_a_record_id_that_is_no_range_is_an_invariant() {
		let mut b = Builder::new();
		let key = param(&mut b, "n");
		let record = b.add(
			ExprOp::RecordId(Box::new(crate::exec::ir::expr::ops::RecordIdAttrs {
				table: TableName::from("t"),
				key_shape: KeyShape::Array {
					len: 1,
				},
			})),
			&[key],
		);
		let op = count_op(
			&mut b,
			SourceRefShape::RecordId,
			super::super::super::payload::CountSelect::Allow,
			&[record],
		);
		assert!(matches!(
			lower_source(&mut LowerCx::detached(), &b.ir, op),
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				..
			}))
		));
	}

	#[test]
	fn a_count_under_a_conditional_permission_runs_the_plan_beside_it_as_a_unit() {
		let mut b = Builder::new();
		let op = count_op(
			&mut b,
			SourceRefShape::Table(TableName::from("t")),
			super::super::super::payload::CountSelect::Conditional,
			&[],
		);
		b.open(op, RegionKind::Subquery, &[]);
		let counted = b.literal(Value::from(2_i64));
		b.terminate(ExprOp::Yield, &[counted]);
		b.close();
		let region = *b.ir.op(op).expect("the op exists").regions.last().expect("a region");
		let mut cx = LowerCx::detached();
		let leaf =
			lower_source_under(&mut Emitter::new(), &mut cx, &b.ir, op, None, &mut Needs::none())
				.expect("a conditional count lowers");
		let Source::Count {
			count:
				CountSource::All {
					source: CountedValue::Table(_),
					ordinary: Some(unit),
					..
				},
			..
		} = leaf.source
		else {
			panic!("a count with its ordinary unit");
		};
		let needed = cx.take_needed();
		assert_eq!(needed.len(), 1);
		assert_eq!(needed[0].id, Some(unit));
		assert!(matches!(
			needed[0].request,
			UnitRequest::Subquery { region: asked } if asked == region
		));
	}

	#[test]
	fn a_conditional_count_whose_plan_reads_an_outside_value_is_an_invariant() {
		let mut b = Builder::new();
		let outside = param(&mut b, "x");
		let op = count_op(
			&mut b,
			SourceRefShape::Table(TableName::from("t")),
			super::super::super::payload::CountSelect::Conditional,
			&[],
		);
		b.open(op, RegionKind::Subquery, &[]);
		b.terminate(ExprOp::Yield, &[outside]);
		b.close();
		assert!(matches!(
			lower_source(&mut LowerCx::detached(), &b.ir, op),
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				..
			}))
		));
	}

	#[test]
	fn a_conditional_count_with_no_plan_beside_it_is_an_invariant() {
		let mut b = Builder::new();
		let op = count_op(
			&mut b,
			SourceRefShape::Table(TableName::from("t")),
			super::super::super::payload::CountSelect::Conditional,
			&[],
		);
		assert!(matches!(
			lower_source(&mut LowerCx::detached(), &b.ir, op),
			Err(IrError::Verify(VerifyError {
				rule: VerifyRule::PerOp,
				..
			}))
		));
	}

	#[test]
	fn a_record_read_of_a_computed_id_is_a_host_scalar_and_reads_the_table_the_id_names() {
		let mut b = Builder::new();
		let key = param(&mut b, "n");
		let record = b.add(
			ExprOp::RecordId(Box::new(crate::exec::ir::expr::ops::RecordIdAttrs {
				table: TableName::from("t"),
				key_shape: KeyShape::Array {
					len: 1,
				},
			})),
			&[key],
		);
		let op = rows_over(
			&mut b,
			PhysOp::RecordScan(Box::new(RecordScanAttrs {
				table: TableName::from("t"),
				for_update: false,
				lightweight: false,
				read: Default::default(),
			})),
			&[record],
		);
		let mut em = Emitter::new();
		let leaf = lower_source_under(
			&mut em,
			&mut LowerCx::detached(),
			&b.ir,
			op,
			None,
			&mut Needs::none(),
		)
		.expect("a record read of a computed id lowers");
		assert_eq!(leaf.table, Some(TableName::from("t")));
		let Source::Record {
			record,
			..
		} = leaf.source
		else {
			panic!("a record read");
		};
		assert!(matches!(record, HostScalar::Code(0)));

		// A value that is no record id expression reads the table the op names.
		let mut b = Builder::new();
		let other = param(&mut b, "r");
		let op = rows_over(
			&mut b,
			PhysOp::RecordScan(Box::new(RecordScanAttrs {
				table: TableName::from("u"),
				for_update: false,
				lightweight: false,
				read: Default::default(),
			})),
			&[other],
		);
		let leaf = lower_source(&mut LowerCx::detached(), &b.ir, op)
			.expect("a record read of a parameter lowers");
		assert_eq!(leaf.table, Some(TableName::from("u")));
	}

	#[test]
	fn a_values_read_of_a_parameter_is_a_host_scalar() {
		let mut b = Builder::new();
		let value = param(&mut b, "rows");
		let op = rows_over(
			&mut b,
			PhysOp::Values {
				read: crate::exec::vm::payload::ValuesRead::Expression {
					written: false,
				},
			},
			&[value],
		);
		let leaf = lower_source(&mut LowerCx::detached(), &b.ir, op).expect("a values read lowers");
		assert!(matches!(
			leaf.source,
			Source::Values {
				value: HostScalar::Code(0),
				..
			}
		));
		assert_eq!(leaf.table, None);
	}

	#[test]
	fn the_declared_schema_is_that_of_the_one_table_a_source_reads() {
		let mut b = Builder::new();
		let op = scan(&mut b, false);
		assert!(schema_of(&b.ir, op).is_none());
		let rows_of_scan = b.result_of(op);
		let union = rows_over(&mut b, PhysOp::Union, &[rows_of_scan]);
		assert!(schema_of(&b.ir, union).is_none());
	}

	#[test]
	fn a_key_range_scan_reads_the_schema_its_read_clauses_declare() {
		use crate::exec::ir::phys::ops::{KeyRangeScanAttrs, ReadClauses};
		use crate::expr::logical::{FieldPath as DeclaredPath, FieldSchema, LogicalPermission};
		let schema = std::sync::Arc::new(Schema::closed(vec![FieldSchema {
			path: DeclaredPath::field("a"),
			kind: crate::expr::Kind::Int,
			checked: false,
			flexible: false,
			computed: None,
			select_permission: LogicalPermission::Full,
		}]));
		let mut b = Builder::new();
		let op = key_range_scan(&mut b, &[]);
		let Some(OpKind::Phys(PhysOp::KeyRangeScan(attrs))) =
			b.ir.op(op).map(|data| data.kind.clone())
		else {
			panic!("a key-range scan");
		};
		assert!(schema_of(&b.ir, op).is_none());
		let declaring = rows_over(
			&mut b,
			PhysOp::KeyRangeScan(Box::new(KeyRangeScanAttrs {
				prefilter: None,
				read: ReadClauses {
					schema: Some(std::sync::Arc::clone(&schema)),
					with: None,
				},
				..*attrs
			})),
			&[],
		);
		assert_eq!(schema_of(&b.ir, declaring), Some(schema));
	}

	/// A constant holding a closure is no value a program holds, and the lowering answers the
	/// internal error; a closure held deeper in the constant is the same.
	#[test]
	fn a_host_scalar_holding_a_closure_is_an_internal_error() {
		let closure = || {
			Value::Closure(Box::new(crate::val::Closure::Expr {
				args: Vec::new(),
				returns: None,
				body: crate::expr::Expr::Literal(crate::expr::Literal::None),
				captures: Default::default(),
			}))
		};
		for constant in [closure(), Value::Array(crate::val::Array(vec![Value::None, closure()]))] {
			let mut b = Builder::new();
			let value = b.literal(constant);
			let held = host_scalar_of(
				&mut Emitter::new(),
				&mut LowerCx::detached(),
				&b.ir,
				value,
				&mut Needs::none(),
			);
			assert!(
				matches!(
					held,
					Err(IrError::Invariant {
						what: "ir_lower::read: a host scalar holding a closure"
					})
				),
				"{held:?}"
			);
		}
		assert!(matches!(
			plain(closure(), "a constant"),
			Err(IrError::Invariant {
				what: "ir_lower::read: an access holding a closure"
			})
		));
	}

	#[test]
	fn rows_that_are_a_block_argument_are_an_invariant_break() {
		let mut b = Builder::new();
		let owner = b.create_kind(OpKind::Phys(PhysOp::Subquery), &[], 1);
		let mut arg = None;
		b.open_with(owner, RegionKind::Subquery, &[TypeKnowledge::Unknown], |_, args| {
			arg = args.first().copied();
		});
		let arg = arg.expect("the region takes one argument");
		assert!(matches!(
			producer_of(&b.ir, arg),
			Err(IrError::Invariant {
				what: "ir_lower::read: rows no physical op produces"
			})
		));
		assert_eq!(producer_of(&b.ir, b.result_of(owner)).ok(), Some(owner));
	}

	#[test]
	fn an_op_that_is_no_source_is_an_internal_error_naming_its_mnemonic() {
		let mut b = Builder::new();
		let op = rows(&mut b, PhysOp::Filter);
		let Err(IrError::Invariant {
			what,
		}) = lower_source(&mut LowerCx::detached(), &b.ir, op)
		else {
			panic!("a filter is no source");
		};
		assert!(what.ends_with("phys.filter"), "{what}");
	}
}
