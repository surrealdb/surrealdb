//! The `WHERE` of a graph hop judged from each adjacency entry, before the edge is read.
//!
//! An adjacency entry carries the edge's record id, the vertex at its far end and, for a table
//! that declares `INLINE` fields, those fields' values as the last write stamped them. A hop's
//! `WHERE` that reads nothing else is judged from the entry, and only an edge it admits is read.
//! [`run`] states that decision as the hop's [`HopKey`], which lowering turns into the hop's key
//! condition. A hop without one judges its `WHERE` over each record it reads.
//!
//! A `phys.loop` of kind `GraphStep` that states a graph lookup has a key condition exactly when
//! all of the following hold:
//!
//! - It names at least one edge table, has a `WHERE`, and reads the current version.
//! - Its `WHERE` region reads nothing from outside itself but values built of literals alone: no
//!   row of the pipeline around the hop (`$parent`) and no parameter. The row the region judges is
//!   read only by a field read of a name, the field's root.
//! - Every op of the region is an expression op of a form that reads the row only through those
//!   field reads: literals and their constructors, paths below a field, the comparison, boolean and
//!   arithmetic operators, `IF`, an index by a literal, and a call of a builtin that is neither a
//!   projection nor an index function. Its effects write nothing, read nothing outside the
//!   transaction and do not wait. A path below a field may read the record a link names, which it
//!   reads alike whether the field's value comes from the entry or from the edge's record.
//! - Every root it reads is `id`, `in` or `out`, which the entry's key carries; or, for every edge
//!   table the hop names, the table is a relation that is not lightweight, its record ids name no
//!   components, every other root is one of its `INLINE` fields, and the caller's reads of the
//!   table and of every field under those roots are not judged by a `SELECT` permission.
//! - For every edge table the hop names, no field at or beneath the `id`, `in` or `out` it reads is
//!   governed for the caller by a `SELECT` permission that is not `FULL`. The entry's key holds the
//!   raw value, while the record path judges the `WHERE` over the caller's view of the edge, where
//!   such a field the caller fails reads `NONE`; a key condition would prune edges by a value the
//!   caller may not read.
//!
//! The table answers the physical catalog holds for each edge table's current read decide the
//! conditions on the table; a table it holds none for gives the hop no key condition.
//!
//! The condition covers the whole `WHERE` or the hop has none. An entry that cannot answer the
//! fields the `WHERE` reads is judged over its record when the hop runs, so the condition only
//! decides how many edges are read, never which edges the hop admits. The decision is derived
//! from the container as it stands and is held in the container's hop key table, which is not
//! identity.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeSet;
use std::sync::Arc;

use super::access::ReadVersion;
use super::conjunct::{def_op, strip_truthy};
use super::ops::PhysOp;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::side_map::SideMap;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx, SourceKey};
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::logical::ops::{LoopKind, StepAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::operators::scan::PayloadTableSpec;
use crate::expr::logical::{EffectSet, FieldSegment};
use crate::expr::lookup::LookupKind;
use crate::expr::{BinaryOperator, Function};
use crate::val::{TableName, Value};

/// A graph hop's `WHERE` as an adjacency entry answers it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HopKey {
	/// Whether the `WHERE` reads the edge's record id.
	pub(crate) needs_id: bool,
	/// Whether the `WHERE` reads the edge's `in`.
	pub(crate) needs_in: bool,
	/// Whether the `WHERE` reads the edge's `out`.
	pub(crate) needs_out: bool,
	/// For a `WHERE` reading `INLINE` fields, each edge table's inline generation and inline field
	/// names when the hop was planned, sorted by table; `None` for one reading only the key.
	pub(crate) payload: Option<Vec<(TableName, PayloadTableSpec)>>,
	/// The `WHERE` as comparisons of a root it reads with a literal, joined by `AND` in written
	/// order; `None` where it has any other shape.
	pub(crate) checks: Option<Vec<KeyComparison>>,
}

/// One comparison of a [`HopKey`]: root `field` compared with `value` by `op`, the field the left
/// operand unless `swapped`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyComparison {
	pub(crate) field: String,
	pub(crate) op: BinaryOperator,
	pub(crate) value: Value,
	pub(crate) swapped: bool,
}

/// The key condition of every graph hop of one container that has one, keyed by the id of its
/// `phys.loop`. Never part of identity.
#[derive(Clone, Debug, Default)]
pub(crate) struct HopKeyTable(SideMap<OpId, Arc<HopKey>>);

impl HopKeyTable {
	/// A table holding no key condition.
	pub(crate) fn new() -> Self {
		Self(SideMap::new())
	}

	/// The key condition of the hop `op`, or `None` when it has none.
	pub(crate) fn get(&self, op: OpId) -> Option<&HopKey> {
		self.0.get(op).map(Arc::as_ref)
	}

	/// Gives each `to` of `pairs` the key condition `source` holds for the `from` it is paired
	/// with. Use this when the container is a copy of code in the container `source` belongs to.
	pub(in crate::exec::ir) fn carry(
		&mut self,
		source: &HopKeyTable,
		pairs: impl IntoIterator<Item = (OpId, OpId)>,
	) {
		for (from, to) in pairs {
			if let Some(key) = source.0.get(from) {
				self.0.insert(to, Arc::clone(key));
			}
		}
	}
}

/// The effects an op of a key condition may not have. An entry is judged whether or not the caller
/// is shown its edge, so the condition may be judged for more edges than the records the hop
/// shows: a write, a read outside the transaction and a wait would each make that observable.
const REFUSED: EffectSet = EffectSet::WRITES_DATA
	.union(EffectSet::WRITES_EXTERNAL)
	.union(EffectSet::READS_EXTERNAL)
	.union(EffectSet::ADVANCES_STATE)
	.union(EffectSet::BLOCKING)
	.union(EffectSet::WRITES_SESSION)
	.union(EffectSet::NOTIFY)
	.union(EffectSet::DEFERS)
	.union(EffectSet::CHANGES_CATALOG);

/// States the key condition of every graph hop of `ir`, by the rules of the module, replacing
/// every condition the table held.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) {
	let mut hops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Loop {
				kind: LoopKind::GraphStep,
				..
			}))
		) {
			hops.push(op);
		}
		Walk::Continue
	});
	let mut table = HopKeyTable::new();
	for hop in hops {
		if let Some(key) = key_of(ir, facts, cx, hop) {
			table.0.insert(hop, Arc::new(key));
		}
	}
	*ir.hop_key_table_mut() = table;
}

/// The key condition of the hop `hop`, or `None` where the conditions of the module do not hold.
fn key_of(ir: &Ir, facts: &mut FactCache, cx: &PhysCx<'_>, hop: OpId) -> Option<HopKey> {
	let data = ir.op(hop)?;
	let OpKind::Phys(PhysOp::Loop {
		kind: LoopKind::GraphStep,
		step,
		..
	}) = &data.kind
	else {
		return None;
	};
	let Some(StepAttrs::Lookup(attrs)) = step.as_deref() else {
		return None;
	};
	if !matches!(attrs.kind, LookupKind::Graph(_)) || !attrs.cond || attrs.subjects.is_empty() {
		return None;
	}
	if !matches!(ir.snapshot(data.snapshot), Some(Snapshot::Current) | None) {
		return None;
	}
	let region = *data.regions.get(1)?;
	if ir.region(region)?.kind != RegionKind::Clause(ClauseRole::Where) {
		return None;
	}
	let row = *ir.block(*ir.region(region)?.blocks.first()?)?.args.first()?;
	let roots = roots_of(ir, facts, cx, region, row)?;
	let needs_id = roots.contains("id");
	let needs_in = roots.contains("in");
	let needs_out = roots.contains("out");
	if attrs
		.subjects
		.iter()
		.any(|subject| !key_fields_readable(cx, &subject.table, needs_id, needs_in, needs_out))
	{
		return None;
	}
	let needed: Vec<&String> =
		roots.iter().filter(|root| !matches!(root.as_str(), "id" | "in" | "out")).collect();
	let payload = if needed.is_empty() {
		None
	} else {
		let mut tables = Vec::with_capacity(attrs.subjects.len());
		for subject in attrs.subjects.iter() {
			let spec = payload_of(cx, &subject.table, &needed)?;
			if !tables.iter().any(|(table, _)| *table == subject.table) {
				tables.push((subject.table.clone(), spec));
			}
		}
		tables.sort_by(|(a, _), (b, _)| a.cmp(b));
		Some(tables)
	};
	Some(HopKey {
		needs_id,
		needs_in,
		needs_out,
		payload,
		checks: checks_of(ir, region, row),
	})
}

/// The roots the `WHERE` region `region` reads from its row `row`, or `None` where an op of the
/// region is not one a key condition admits, reads a value from outside the region that is not
/// built of literals alone, or reads the row other than by a field read.
fn roots_of(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	region: RegionId,
	row: ValueId,
) -> Option<BTreeSet<String>> {
	let mut ops = Vec::new();
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		ops.push(op);
		Walk::Continue
	});
	let inside: FxHashSet<OpId> = ops.iter().copied().collect();
	let mut roots = BTreeSet::new();
	for op in &ops {
		let data = ir.op(*op)?;
		let OpKind::Expr(kind) = &data.kind else {
			return None;
		};
		if let ExprOp::Field {
			name,
		} = kind && data.operands.as_slice() == [row]
		{
			roots.insert(name.as_str().to_owned());
			continue;
		}
		if !admitted_form(ir, cx, kind, &data.operands) {
			return None;
		}
		for operand in &data.operands {
			let held = match ir.value(*operand)?.def {
				ValueDef::OpResult {
					op,
					..
				} => inside.contains(&op) || super::index_answer::literal(ir, *operand).is_some(),
				ValueDef::BlockArg {
					block,
					..
				} => {
					*operand != row
						&& ir
							.block(block)
							.and_then(|held| ir.parent_op(held.parent))
							.is_some_and(|owner| inside.contains(&owner))
				}
			};
			if !held {
				return None;
			}
		}
	}
	for op in &ops {
		if effects::summary_with(ir, facts, &cx.effects, *op).set.intersects(REFUSED) {
			return None;
		}
	}
	Some(roots)
}

/// Whether an op of kind `kind` over `operands`, other than a field read of the row, may stand in
/// a key condition.
fn admitted_form(ir: &Ir, cx: &PhysCx<'_>, kind: &ExprOp, operands: &[ValueId]) -> bool {
	match kind {
		ExprOp::Call(call) => match &call.function {
			Function::Normal(name) => {
				!cx.effects.registry.is_projection(name)
					&& !cx.effects.registry.is_index_function(name)
			}
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => false,
		},
		// An index other than a literal may read anything the row holds.
		ExprOp::Index => operands.get(1).and_then(|index| def_op(ir, *index)).is_some_and(|op| {
			matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Expr(ExprOp::Literal(_))))
		}),
		ExprOp::Literal(_)
		| ExprOp::Array
		| ExprOp::Set
		| ExprOp::Object {
			..
		}
		| ExprOp::RecordId(_)
		| ExprOp::Table {
			..
		}
		| ExprOp::Constant {
			..
		}
		| ExprOp::Access {
			..
		}
		| ExprOp::Prefix(_)
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Binary {
			..
		}
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::If
		| ExprOp::Yield
		| ExprOp::Truthy => true,
		ExprOp::Field {
			..
		}
		| ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Method {
			..
		}
		| ExprOp::Destructure {
			..
		}
		| ExprOp::Optional
		| ExprOp::SetField {
			..
		}
		| ExprOp::Convert(_)
		| ExprOp::Mock(_)
		| ExprOp::ComputedRead(_)
		| ExprOp::Doc
		| ExprOp::Deref {
			..
		}
		| ExprOp::Dispatch(_)
		| ExprOp::RepeatRecurse
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Invoke
		| ExprOp::ClosureCreate(_)
		| ExprOp::Throw
		| ExprOp::Subquery(_)
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::DepthCharge {
			..
		} => false,
	}
}

/// Whether the caller reads every key field among `id`, `in` and `out` that `needs_id`,
/// `needs_in` and `needs_out` select, of the edge table `table`, as the entry's key holds it: the
/// caller's reads of the table are not governed by a `SELECT` permission, and no field at or
/// beneath one of them is governed by a `SELECT` permission that is not `FULL` for the caller.
/// `false` where the physical catalog holds no answers for the table's current read, or holds an
/// unresolved permission context.
fn key_fields_readable(
	cx: &PhysCx<'_>,
	table: &TableName,
	needs_id: bool,
	needs_in: bool,
	needs_out: bool,
) -> bool {
	let key: SourceKey = (table.clone(), None, ReadVersion::Current);
	let answers = match cx.physical.table_answers(&key) {
		CatalogAnswer::Found(answers) => answers,
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown | CatalogAnswer::Absent => return false,
	};
	if answers.permissioned {
		return false;
	}
	match &answers.restricted {
		super::access::RestrictedSelect::None => true,
		super::access::RestrictedSelect::Unresolved => false,
		super::access::RestrictedSelect::Fields(paths) => {
			!paths.iter().any(|path| match path.segments().first() {
				Some(FieldSegment::Field(root)) => match root.as_str() {
					"id" => needs_id,
					"in" => needs_in,
					"out" => needs_out,
					_ => false,
				},
				Some(FieldSegment::All) | None => true,
			})
		}
	}
}

/// The inline-field snapshot of the edge table `table` a `WHERE` reading the roots `needed` is
/// judged from, or `None` where the table cannot answer them from its entries.
fn payload_of(cx: &PhysCx<'_>, table: &TableName, needed: &[&String]) -> Option<PayloadTableSpec> {
	let key: SourceKey = (table.clone(), None, ReadVersion::Current);
	let answers = match cx.physical.table_answers(&key) {
		CatalogAnswer::Found(answers) => answers,
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown | CatalogAnswer::Absent => return None,
	};
	let inline = answers.inline.as_ref()?;
	if answers.permissioned
		|| answers.key.as_ref().is_some_and(|key| key.origin == crate::catalog::KeyOrigin::IdField)
	{
		return None;
	}
	// The payload holds a field's raw stored value, everything nested inside it included, so no
	// field at or beneath a root the `WHERE` reads may be cut from the caller's view.
	match &answers.restricted {
		super::access::RestrictedSelect::None => {}
		super::access::RestrictedSelect::Unresolved => return None,
		super::access::RestrictedSelect::Fields(paths) => {
			if paths.iter().any(|path| match path.segments().first() {
				Some(FieldSegment::Field(root)) => needed.iter().any(|name| *name == root.as_str()),
				Some(FieldSegment::All) | None => true,
			}) {
				return None;
			}
		}
	}
	if needed.iter().any(|name| !inline.names.iter().any(|held| held == *name)) {
		return None;
	}
	Some(PayloadTableSpec {
		generation: inline.generation,
		fields: inline.names.to_vec(),
		referenced: inline.names.iter().map(|held| needed.contains(&held)).collect(),
	})
}

/// The comparisons the `WHERE` region `region` is joined from, in written order, or `None` where
/// its predicate is not an `AND` of comparisons of a root of `row` with a literal.
fn checks_of(ir: &Ir, region: RegionId, row: ValueId) -> Option<Vec<KeyComparison>> {
	let last = ops_in(ir, region).last()?;
	let closing = ir.op(last)?;
	let (OpKind::Expr(ExprOp::Yield), [predicate]) = (&closing.kind, closing.operands.as_slice())
	else {
		return None;
	};
	let mut out = Vec::new();
	collect(ir, row, *predicate, &mut out)?;
	Some(out)
}

/// Pushes onto `out` the comparisons `value` is joined from, left to right.
fn collect(ir: &Ir, row: ValueId, value: ValueId, out: &mut Vec<KeyComparison>) -> Option<()> {
	let data = ir.op(def_op(ir, strip_truthy(ir, value))?)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	if let (
		ExprOp::ShortCircuit {
			op: ShortCircuitOp::And,
		},
		[left],
	) = (kind, data.operands.as_slice())
	{
		collect(ir, row, *left, out)?;
		let [region] = data.regions.as_slice() else {
			return None;
		};
		let last = ops_in(ir, *region).last()?;
		let closing = ir.op(last)?;
		let (OpKind::Expr(ExprOp::Yield), [right]) = (&closing.kind, closing.operands.as_slice())
		else {
			return None;
		};
		collect(ir, row, *right, out)
	} else if let (
		ExprOp::Binary {
			op,
		},
		[left, right],
	) = (kind, data.operands.as_slice())
	{
		let (field, literal, swapped) = match (root(ir, row, *left), root(ir, row, *right)) {
			(Some(field), None) => (field, *right, false),
			(None, Some(field)) => (field, *left, true),
			(Some(_), Some(_)) | (None, None) => return None,
		};
		out.push(KeyComparison {
			field,
			op: op.clone(),
			value: super::index_answer::literal(ir, literal)?,
			swapped,
		});
		Some(())
	} else {
		None
	}
}

/// The root `value` reads when it is a field read of the row `row`.
fn root(ir: &Ir, row: ValueId, value: ValueId) -> Option<String> {
	let data = ir.op(def_op(ir, value)?)?;
	match (&data.kind, data.operands.as_slice()) {
		(
			OpKind::Expr(ExprOp::Field {
				name,
			}),
			[base],
		) if *base == row => Some(name.as_str().to_owned()),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::analysis::cx::builtin_registry;
	use crate::exec::ir::iface::catalog::{
		EffectCx, PhysicalCatalog, SemanticCatalog, default_config,
	};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::phys::access::{RestrictedSelect, TableAnswers};

	struct OneTable(Arc<TableAnswers>);

	impl SemanticCatalog for OneTable {}
	impl PhysicalCatalog for OneTable {
		fn table_answers(&self, key: &SourceKey) -> CatalogAnswer<Arc<TableAnswers>> {
			if key.0 == self.0.table {
				CatalogAnswer::Found(Arc::clone(&self.0))
			} else {
				CatalogAnswer::Unknown
			}
		}
	}

	#[test]
	fn key_fields_readable_rejects_permissioned_table() {
		let table = TableName::from("edge");
		let frames = FrameTable::default();
		let registry = builtin_registry();

		let mut answers = TableAnswers::plain(table.clone(), true);
		answers.restricted = RestrictedSelect::None;
		answers.permissioned = false;
		let catalog = OneTable(Arc::new(answers));
		let cx = PhysCx {
			effects: EffectCx {
				registry,
				catalog: &catalog,
				frames: &frames,
			},
			physical: &catalog,
			config: default_config(),
		};
		assert!(key_fields_readable(&cx, &table, true, true, true));

		let mut perm_answers = TableAnswers::plain(table.clone(), true);
		perm_answers.restricted = RestrictedSelect::None;
		perm_answers.permissioned = true;
		let perm_catalog = OneTable(Arc::new(perm_answers));
		let perm_cx = PhysCx {
			effects: EffectCx {
				registry,
				catalog: &perm_catalog,
				frames: &frames,
			},
			physical: &perm_catalog,
			config: default_config(),
		};
		assert!(!key_fields_readable(&perm_cx, &table, true, false, false));
		assert!(!key_fields_readable(&perm_cx, &table, false, true, false));
		assert!(!key_fields_readable(&perm_cx, &table, false, false, true));
		assert!(!key_fields_readable(&perm_cx, &table, false, false, false));
	}
}
