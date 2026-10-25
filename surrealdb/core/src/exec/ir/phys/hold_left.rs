//! The one left row of a join.
//!
//! A [`PhysOp::Join`] reads its right input whole into a build table when its first left row
//! arrives, then streams the left input past the table. When the left input yields at most one row,
//! the table is the costlier side. [`run`] then sets `build: Left` and `spill: Bounded` on the
//! join, so it holds that one left row and streams the right input past it, row by row.
//!
//! A join is changed only when all of the following hold. The rows, their order and the errors
//! raised then match those of the build table:
//!
//! - It builds on its right input, reads that input through no index lookup, and is not a semi
//!   join. A semi join hands on the left row itself rather than a pairing.
//! - Its statement has no `VERSION` scope, and the join is not inside a stored clause.
//! - Its left input yields at most one row whatever the data ([`at_most_one_row`]).
//! - Reading its right input raises no error and has no effect ([`total_read`]). A scan under total
//!   predicates and permissions qualifies, and so does the enforcement of tables that
//!   [`enforcement_is_total`] accepts.
//! - The right key of every pair and the residual raise no error, have no effect, and read only the
//!   fields of the paired rows, parameters and literals ([`paired_total`]).
//! - The statement the join stands in writes nothing, so no row the right input yields was written
//!   by a pairing it was streamed past.
//!
//! A fact the container or the catalog cannot state keeps the build table. A permission predicate
//! that reads a field of the row is not judged total, so a right input read through such a
//! permission keeps the build table.

#![deny(clippy::wildcard_enum_match_arm)]

use std::slice;

use surrealdb_strand::TableName;

use super::access::ReadVersion;
use super::chain;
use super::conjunct::{def_op, is_total_form, judge, literal_record};
use super::nested::{
	enclosing_statement, holds_version, in_stored_clause, statement_writes, yielded,
};
use super::ops::{IndexScanAttrs, JoinAttrs, PhysOp, ProjectionShape};
use super::rewrite::literal_count;
use crate::catalog::{Index, Permission};
use crate::exec::index::access_path::{BTreeAccess, BoundOperand};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet};
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx, SourceKey};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::payload::{CountSelect, JoinBuild, JoinMatch, Spill};

/// How many ops of a chain a walk follows before it gives up.
const MAX_DEPTH: usize = 256;

/// The most ops one key or residual is judged from before it is taken as not total.
const NODE_BUDGET: usize = 512;

/// Holds the one left row of every join of `ir` that satisfies the conditions of the module, and
/// returns the number of joins changed.
///
/// Runs in the physical phase, after the rewrite rules reach their fixed point, so the left and
/// right inputs are judged as they will run. Whether a statement writes is asked once per
/// statement, and only for a join whose other conditions all hold.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<u32, IrError> {
	let mut joins = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::Join(attrs))) if builds_on_right(attrs)
		) {
			joins.push(op);
		}
		Walk::Continue
	});
	let mut writes = FxHashMap::default();
	let mut held = Vec::new();
	for join in joins {
		if holds_one_row(ir, facts, cx, join, &mut writes) {
			held.push(join);
		}
	}
	let changed = u32::try_from(held.len()).unwrap_or(u32::MAX);
	let mut rw = Rewriter::new(ir, facts);
	for join in held {
		rw.update_kind(join, |kind| {
			if let OpKind::Phys(PhysOp::Join(attrs)) = kind {
				attrs.build = JoinBuild::Left;
				attrs.spill = Spill::Bounded;
			}
		})?;
	}
	Ok(changed)
}

/// Whether the join `attrs` states is one the rule may change: it builds on its right input, reads
/// it through no index lookup, and pairs rows rather than handing on the left row.
fn builds_on_right(attrs: &JoinAttrs) -> bool {
	attrs.build == JoinBuild::Right && attrs.matching != JoinMatch::Semi && attrs.lookup.is_none()
}

/// Whether the join `join` may hold its one left row. `writes` caches, per statement, whether the
/// statement writes; it is asked only once every other condition holds.
fn holds_one_row(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	join: OpId,
	writes: &mut FxHashMap<OpId, bool>,
) -> bool {
	let Some(data) = ir.op(join) else {
		return false;
	};
	let OpKind::Phys(PhysOp::Join(attrs)) = &data.kind else {
		return false;
	};
	let [left, right] = data.operands.as_slice() else {
		return false;
	};
	if in_stored_clause(ir, join) || holds_version(ir, join) {
		return false;
	}
	if !at_most_one_row(ir, cx, *left) {
		return false;
	}
	let inert = inert_tables(ir, cx, *right);
	if !total_read(ir, *right, &inert) {
		return false;
	}
	let mut aliases = vec![attrs.alias.as_str()];
	left_aliases(ir, *left, &mut aliases);
	// The key pairs are a left and a right region each, then the residual.
	let keys = usize::from(attrs.keys);
	let right_keys = (0..keys).map(|pair| pair * 2 + 1);
	let residual = attrs.residual.then_some(keys * 2);
	let paired = right_keys.chain(residual).all(|at| {
		data.regions
			.get(at)
			.copied()
			.and_then(|region| yielded(ir, region))
			.is_some_and(|value| paired_total(ir, value, &aliases))
	});
	if !paired {
		return false;
	}
	let Some((_, owner)) = enclosing_statement(ir, join) else {
		return false;
	};
	!*writes.entry(owner).or_insert_with(|| statement_writes(ir, facts, &cx.effects, owner))
}

/// Steps `value` through the containers a chain is read through, and answers the op that defines
/// what the chain delivers, or `None` when no op does.
fn chain_op(ir: &Ir, mut value: ValueId) -> Option<OpId> {
	for _ in 0..MAX_DEPTH {
		match chain::enter(ir, value) {
			Some(through) => value = through,
			None => return def_op(ir, value),
		}
	}
	None
}

/// The physical op that defines what the chain at `value` delivers, with its data.
fn step(ir: &Ir, value: ValueId) -> Option<(&OpData, &PhysOp)> {
	let data = ir.op(chain_op(ir, value)?)?;
	match &data.kind {
		OpKind::Phys(kind) => Some((data, kind)),
		OpKind::Expr(_) | OpKind::Logical(_) => None,
	}
}

/// Whether the rows of `from` are at most one whatever the data: a read of one record by a literal
/// record id, an equality on every column of a `UNIQUE` index with no key exempt from uniqueness
/// ([`crate::catalog::IndexDefinition::exempts_from_uniqueness`] — the index lists exempt keys of
/// several records), a `GROUP ALL`, a `LIMIT` of at most one, or a statement that yields at most
/// one, beneath ops that yield at most one row per row they are handed.
fn at_most_one_row(ir: &Ir, cx: &PhysCx<'_>, from: ValueId) -> bool {
	let mut value = from;
	for _ in 0..MAX_DEPTH {
		let Some((data, kind)) = step(ir, value) else {
			return false;
		};
		if let PhysOp::Limit {
			limit: Some(limit),
			..
		} = kind && literal_at_most_one(ir, data, usize::from(*limit))
		{
			return true;
		}
		if let PhysOp::Aggregate(attrs) = kind {
			return attrs.groups == 0;
		}
		if matches!(kind, PhysOp::RecordScan(_)) {
			return has_literal_record(ir, data);
		}
		if let PhysOp::IndexScan(attrs) = kind {
			return unique_equality(cx, attrs);
		}
		let next = if matches!(kind, PhysOp::Subquery) {
			data.regions.first().and_then(|region| yielded(ir, *region))
		} else if passes_one_per_row(kind) {
			data.operands.first().copied()
		} else {
			None
		};
		let Some(next) = next else {
			return false;
		};
		value = next;
	}
	false
}

/// Whether the operand of `data` at `at` is a literal count of at most one.
fn literal_at_most_one(ir: &Ir, data: &OpData, at: usize) -> bool {
	data.operands
		.get(at)
		.and_then(|bound| literal_count(ir, *bound))
		.is_some_and(|count| count <= 1)
}

/// Whether the first operand of `data` is the id of one record, written as a literal.
fn has_literal_record(ir: &Ir, data: &OpData) -> bool {
	data.operands.first().is_some_and(|record| literal_record(ir, *record).is_some())
}

/// The ops that yield at most one row per row they are handed.
fn passes_one_per_row(kind: &PhysOp) -> bool {
	matches!(
		kind,
		PhysOp::Filter
			| PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Enforced { .. }
			| PhysOp::Project(_)
			| PhysOp::Fetch
			| PhysOp::Sort(_)
			| PhysOp::Distinct { .. }
			| PhysOp::Limit { .. }
	)
}

/// Whether the index scan `attrs` reads one record at most: an equality on every column of a
/// `UNIQUE` index whose keys are literal, or the statement clock, which resolves to a datetime.
fn unique_equality(cx: &PhysCx<'_>, attrs: &IndexScanAttrs) -> bool {
	let pinned: &[BoundOperand] = if let BTreeAccess::Equality(key) = &attrs.access {
		slice::from_ref(key)
	} else if let BTreeAccess::Compound {
		prefix,
		range: None,
		..
	} = &attrs.access
	{
		prefix.as_slice()
	} else {
		return false;
	};
	let answers: SourceKey = (attrs.table.clone(), attrs.read.with.clone(), ReadVersion::Current);
	let CatalogAnswer::Found(answers) = cx.physical.table_answers(&answers) else {
		return false;
	};
	let Some(definition) = answers.indexes.iter().find(|held| held.name == attrs.index.index)
	else {
		return false;
	};
	matches!(definition.index, Index::Uniq)
		&& pinned.len() == definition.cols.len()
		&& pinned.iter().all(|key| !key.is_exempt_from_uniqueness(definition))
}

/// Whether reading the rows of `from` raises no error and has no effect: a scan of a table, an
/// index or a literal record, beneath predicates and permissions that are total, the identity
/// projection, and the enforcement of a table among `inert`.
fn total_read(ir: &Ir, from: ValueId, inert: &[TableName]) -> bool {
	let mut value = from;
	for _ in 0..MAX_DEPTH {
		let Some((data, kind)) = step(ir, value) else {
			return false;
		};
		if matches!(kind, PhysOp::Filter) {
			if !data.regions.iter().all(|region| region_total(ir, *region)) {
				return false;
			}
		} else if let PhysOp::PermissionFilter(attrs) = kind {
			// Deriving computed fields runs definitions that may raise.
			if attrs.computed.is_some()
				|| !data.regions.iter().all(|region| region_total(ir, *region))
			{
				return false;
			}
		} else if let PhysOp::Project(shape) = kind {
			if **shape != ProjectionShape::All {
				return false;
			}
		} else if let PhysOp::Enforced {
			table,
		} = kind
		{
			if !inert.contains(table) {
				return false;
			}
		} else if matches!(
			kind,
			PhysOp::TableScan(_) | PhysOp::KeyRangeScan(_) | PhysOp::IndexScan(_)
		) {
			// A key-range scan evaluates its deferred bounds when it opens. A bound that would
			// raise is widened instead, so the scan itself raises nothing.
			return true;
		} else if let PhysOp::RecordScan(attrs) = kind {
			return !attrs.for_update && has_literal_record(ir, data);
		} else {
			return false;
		}
		let Some(input) = data.operands.first().copied() else {
			return false;
		};
		value = input;
	}
	false
}

/// Whether the value `region` yields is total ([`judge`]).
fn region_total(ir: &Ir, region: RegionId) -> bool {
	yielded(ir, region).is_some_and(|value| judge(ir, value).total)
}

/// The aliases a row of the join input `from` holds the rows of the joins beneath it under.
fn left_aliases<'a>(ir: &'a Ir, from: ValueId, aliases: &mut Vec<&'a str>) {
	let mut value = from;
	for _ in 0..MAX_DEPTH {
		let Some((data, kind)) = step(ir, value) else {
			return;
		};
		if let PhysOp::Join(attrs) = kind {
			aliases.push(attrs.alias.as_str());
		} else if let PhysOp::Project(shape) = kind {
			if **shape != ProjectionShape::All {
				return;
			}
		} else if !matches!(kind, PhysOp::Filter) {
			return;
		}
		let Some(input) = data.operands.first().copied() else {
			return;
		};
		value = input;
	}
}

/// The tables of `from`'s chain whose enforcement evaluates nothing ([`enforcement_is_total`]).
fn inert_tables(ir: &Ir, cx: &PhysCx<'_>, from: ValueId) -> Vec<TableName> {
	let mut enforced = Vec::new();
	let mut value = from;
	for _ in 0..MAX_DEPTH {
		let Some((data, kind)) = step(ir, value) else {
			break;
		};
		if let PhysOp::Enforced {
			table,
		} = kind
		{
			if !enforced.contains(table) {
				enforced.push(table.clone());
			}
		} else if !matches!(kind, PhysOp::Filter | PhysOp::PermissionFilter(_) | PhysOp::Project(_))
		{
			break;
		}
		let Some(input) = data.operands.first().copied() else {
			break;
		};
		value = input;
	}
	enforced.retain(|table| enforcement_is_total(cx, table));
	enforced
}

/// Whether the enforcement of `table` evaluates nothing: no `SELECT` permission predicate, and no
/// field that is computed (other than a component of the record id, which is read from the id) or
/// carries a field permission predicate. A table the catalog does not state, or does not define, is
/// not total.
pub(super) fn enforcement_is_total(cx: &PhysCx<'_>, table: &TableName) -> bool {
	let key: SourceKey = (table.clone(), None, ReadVersion::Current);
	let CatalogAnswer::Found(answers) = cx.physical.table_answers(&key) else {
		return false;
	};
	answers.defined
		&& !matches!(answers.select, CountSelect::Conditional)
		&& answers.fields.iter().all(|field| {
			(field.computed.is_none() || field.is_key_part())
				&& !matches!(field.select_permission, Permission::Specific(_))
		})
}

/// Whether evaluating `value` over a pairing raises no error, has no effect and reads nothing but
/// the pairing's fields, the fields of the rows it holds under `aliases`, parameters and literals:
/// what [`judge`] admits, a field read of a row held under an alias, and an `IF` whose conditions
/// and values are such.
fn paired_total(ir: &Ir, value: ValueId, aliases: &[&str]) -> bool {
	let mut seen = FxHashSet::default();
	let mut budget = NODE_BUDGET;
	paired_value(ir, value, aliases, &mut seen, &mut budget)
}

fn paired_value(
	ir: &Ir,
	value: ValueId,
	aliases: &[&str],
	seen: &mut FxHashSet<OpId>,
	budget: &mut usize,
) -> bool {
	def_op(ir, value).is_none_or(|op| paired_op(ir, op, aliases, seen, budget))
}

fn paired_op(
	ir: &Ir,
	op: OpId,
	aliases: &[&str],
	seen: &mut FxHashSet<OpId>,
	budget: &mut usize,
) -> bool {
	if !seen.insert(op) {
		return true;
	}
	let Some(data) = ir.op(op) else {
		return false;
	};
	if *budget == 0 {
		return false;
	}
	*budget -= 1;
	let own = match &data.kind {
		OpKind::Expr(ExprOp::Access {
			..
		}) => reads_alias(ir, data.operands.first().copied(), aliases),
		OpKind::Expr(ExprOp::If) => true,
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => is_total_form(ir, data),
	};
	own && data.operands.iter().all(|operand| paired_value(ir, *operand, aliases, seen, budget))
		&& data.regions.iter().all(|region| {
			ops_in(ir, *region).all(|inner| paired_op(ir, inner, aliases, seen, budget))
		})
}

/// Whether `base` is the read of a field named as one of `aliases`: the row held under an alias.
fn reads_alias(ir: &Ir, base: Option<ValueId>, aliases: &[&str]) -> bool {
	base.and_then(|base| def_op(ir, base)).and_then(|op| ir.op(op)).is_some_and(|data| {
		matches!(&data.kind, OpKind::Expr(ExprOp::Field { name }) if aliases.contains(&name.as_str()))
	})
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;
	use std::sync::Arc;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{IndexDefinition, IndexId};
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::iface::IndexRef;
	use crate::exec::ir::iface::catalog::{EffectCx, PhysicalCatalog, SemanticCatalog};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::phys::access::TableAnswers;
	use crate::exec::ir::phys::ops::ReadClauses;
	use crate::expr::Idiom;
	use crate::kvs::Direction;
	use crate::val::Value;

	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	struct Held(Vec<(SourceKey, CatalogAnswer<Arc<TableAnswers>>)>);

	impl PhysicalCatalog for Held {
		fn table_answers(&self, key: &SourceKey) -> CatalogAnswer<Arc<TableAnswers>> {
			self.0
				.iter()
				.find(|(held, _)| held == key)
				.map_or(CatalogAnswer::NotLoaded, |(_, answer)| answer.clone())
		}
	}

	fn uniq_def(name: &str, cols: &[&str], nulls_not_distinct: bool) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(1),
			name: Strand::from(name),
			table_name: "t".into(),
			cols: cols.iter().map(|c| Idiom::from_str(c).expect("valid idiom")).collect(),
			index: Index::Uniq,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 2,
			nulls_not_distinct,
		}
	}

	fn scan_with_access(index: &str, access: BTreeAccess) -> IndexScanAttrs {
		IndexScanAttrs {
			table: "t".into(),
			index: IndexRef {
				table: "t".into(),
				index: Strand::from(index),
				position: 0,
				scalar: Box::default(),
			},
			access,
			direction: Direction::Forward,
			ordered_by: None,
			pinned: None,
			stated: None,
			rechecked: false,
			decode: None,
			read: ReadClauses::default(),
		}
	}

	fn scan(index: &str, key: Value) -> IndexScanAttrs {
		scan_with_access(index, BTreeAccess::literal_equality(key))
	}

	fn compound_scan(index: &str, keys: Vec<Value>) -> IndexScanAttrs {
		scan_with_access(
			index,
			BTreeAccess::Compound {
				prefix: keys.into_iter().map(Into::into).collect(),
				range: None,
				upper: None,
			},
		)
	}

	#[test]
	fn unique_equality_respects_nulls_not_distinct() {
		let answers = TableAnswers::plain("t".into(), true).with_indexes(vec![
			uniq_def("u", &["a"], false),
			uniq_def("u_nnd", &["a"], true),
			uniq_def("cu", &["a", "b"], false),
			uniq_def("cu_nnd", &["a", "b"], true),
		]);
		let held = Held(vec![(
			("t".into(), None, ReadVersion::Current),
			CatalogAnswer::Found(Arc::new(answers)),
		)]);
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let cx = PhysCx {
			effects: EffectCx {
				registry: &registry,
				catalog: &NoCatalog,
				frames: &frames,
			},
			physical: &held,
			config: crate::exec::ir::iface::catalog::default_config(),
		};

		// Default `UNIQUE`: non-nullish key (including an object) is ≤1 row;
		// `NULL` and `NONE` are exempt.
		assert!(unique_equality(&cx, &scan("u", Value::from(1))));
		assert!(unique_equality(&cx, &scan("u", Value::Object(Default::default()))));
		assert!(!unique_equality(&cx, &scan("u", Value::Null)));
		assert!(!unique_equality(&cx, &scan("u", Value::None)));

		// `UNIQUE NULLS NOT DISTINCT`: `NULL` is ≤1 row; `NONE` remains exempt.
		assert!(unique_equality(&cx, &scan("u_nnd", Value::from(1))));
		assert!(unique_equality(&cx, &scan("u_nnd", Value::Null)));
		assert!(!unique_equality(&cx, &scan("u_nnd", Value::None)));

		// Compound unique prefix: `(1, NULL)` is ≤1 row only under
		// `NULLS NOT DISTINCT`, and `(1, NONE)` is never ≤1 row.
		assert!(unique_equality(&cx, &compound_scan("cu", vec![Value::from(1), Value::from(2)])));
		assert!(!unique_equality(&cx, &compound_scan("cu", vec![Value::from(1), Value::Null])));
		assert!(unique_equality(&cx, &compound_scan("cu_nnd", vec![Value::from(1), Value::Null])));
		assert!(!unique_equality(&cx, &compound_scan("cu_nnd", vec![Value::from(1), Value::None])));
	}
}
