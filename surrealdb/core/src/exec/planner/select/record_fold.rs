//! Plan-time folding of row-independent record-idiom traversals.
//!
//! After bind parameters are substituted into a `WHERE` condition
//! ([`resolve_condition_params`](super::super::util::resolve_condition_params)),
//! a comparison operand can be an idiom rooted at a literal record id —
//! `$scan.task.finishedAt` becomes `(scan:one).task.finishedAt`. The value of
//! such an idiom does not depend on the row being scanned, but index analysis
//! ([`IndexAnalyzer`](crate::exec::index::analysis::IndexAnalyzer)) only
//! matches `idiom op literal` comparisons, so the condition would otherwise
//! plan as a full table scan.
//!
//! [`Planner::fold_constant_record_idioms`] resolves those idioms to their
//! values at plan time — through the same transaction the plan executes
//! against, so the values match what any per-row evaluation would read — and
//! replaces them with literals.
//!
//! # Parity contract
//!
//! Folding replaces the per-row runtime walk
//! ([`evaluate_field`](crate::exec::parts::field)) with one plan-time walk,
//! so it must produce exactly the value the runtime walk would produce. The
//! fold therefore only runs when the two are provably equivalent; otherwise
//! it leaves the idiom unfolded, and the condition keeps its per-row
//! evaluation over a table scan:
//!
//! - Permission checks must be disabled at execute time ([`Planner::should_check_perms_for_view`]
//!   returns `false`): the runtime dereference applies table- and field-level SELECT permissions,
//!   which the plan-time walk does not replicate.
//! - Every table the walk fetches from must define no computed fields: the runtime dereference
//!   evaluates computed fields even when permission checks are disabled (see
//!   [`process_fetched_record`](crate::exec::operators::fetch)).
//! - The idiom must be a literal record id start followed by plain field parts, and every
//!   intermediate value must be an object, a record id (fetched, honouring the
//!   [`RecordIdKey::Object`] key-component shortcut), or a scalar (which yields `NONE`, as at
//!   runtime). Arrays and geometries have their own traversal semantics and are left unfolded.
//! - The folded value must survive the literal round trip (see [`literal_if_round_trips`]):
//!   `Value::into_literal` is lossy for some variants, and substituting a lossy form changes
//!   results.
//! - The walk never descends into subqueries, blocks, closures, loops, or nested statements (see
//!   [`is_fold_boundary`]): those evaluate in their own scope at execution time.
//! - The call site additionally requires the whole statement to be read-only — a write anywhere in
//!   the statement could change a record between the plan-time read and the per-row evaluation the
//!   fold replaces — and excludes versioned SELECTs, including an enclosing version context: the
//!   plan-time walk reads current state, not a versioned snapshot.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use super::super::Planner;
use super::super::util::try_literal_to_value;
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::visit::{MutVisitor, Visit, VisitMut, Visitor};
use crate::expr::{Cond, Expr, Idiom, Part};
use crate::kvs::Transaction;
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Upper bound on plan-time record hops per folded condition. The fold
/// reads storage during planning, before any operator can bound the work;
/// the budget keeps a pathological condition (hundreds of distinct record
/// idioms, or long traversal chains) from turning planning into unbounded
/// storage I/O. One unit is charged per record hop, covering both the
/// hop's field-metadata lookup (a catalog read on a per-table cache miss)
/// and its record read. Generous relative to real predicates.
const FOLD_FETCH_BUDGET: u32 = 32;

impl<'ctx> Planner<'ctx> {
	/// Replace row-independent record-idiom traversals in `cond` with their
	/// literal values, when the plan-time walk is provably equivalent to the
	/// per-row runtime walk (see the module docs for the parity contract).
	///
	/// Best-effort: any guard failure, unsupported shape, or storage error
	/// leaves the affected idiom unfolded rather than failing the plan.
	pub(crate) async fn fold_constant_record_idioms(&self, cond: &mut Cond) {
		let Some(txn) = self.txn.as_ref() else {
			return;
		};
		let (Some(ns_name), Some(db_name)) = (self.ns.as_deref(), self.db.as_deref()) else {
			return;
		};
		// SECURITY: the plan-time walk reads records without evaluating
		// table- or field-level SELECT permissions. It may only run when the
		// runtime dereference it replaces would bypass permission evaluation
		// entirely — the session holds a viewer-or-above role covering this
		// whole database — so no value the actor could not SELECT can reach
		// the plan or its EXPLAIN output.
		if self.should_check_perms_for_view(ns_name, db_name) {
			return;
		}

		let mut collector = CandidateCollector {
			idioms: Vec::new(),
		};
		let _ = collector.visit_expr(&cond.0);
		if collector.idioms.is_empty() {
			return;
		}

		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return;
		};

		// A `None` entry records a failed fold, so duplicate occurrences of
		// the same idiom don't repeat the walk and its storage reads.
		let mut folded: HashMap<Idiom, Option<Expr>> = HashMap::new();
		let mut computed_cache: HashMap<TableName, bool> = HashMap::new();
		let mut fetch_budget = FOLD_FETCH_BUDGET;
		for (idiom, rid) in collector.idioms {
			if folded.contains_key(&idiom) {
				continue;
			}
			let value = fold_record_walk(
				txn,
				ns_id,
				db_id,
				rid,
				&idiom,
				&mut computed_cache,
				&mut fetch_budget,
			)
			.await;
			folded.insert(idiom, value.and_then(|v| literal_if_round_trips(&v)));
		}
		if !folded.values().any(Option::is_some) {
			return;
		}

		let _ = FoldApplier {
			folded: &folded,
		}
		.visit_mut_expr(&mut cond.0);
	}
}

/// The literal form of `value`, when substituting it is value-preserving.
///
/// `Value::into_literal` is lossy for some variants — a closure literal drops
/// its captures, a set becomes an array literal (which compares unequal to a
/// set at runtime), a range lowers to an operator tree — so a folded value
/// may only be substituted when converting its literal form back yields the
/// same value.
fn literal_if_round_trips(value: &Value) -> Option<Expr> {
	let expr = value.clone().into_literal();
	match &expr {
		Expr::Literal(lit) if try_literal_to_value(lit).as_ref() == Some(value) => Some(expr),
		_ => None,
	}
}

/// Walk the field parts of `idiom` from the literal record id `start`,
/// mirroring the runtime traversal in
/// [`evaluate_field`](crate::exec::parts::field). Returns `None` when the
/// walk cannot be proven equivalent to the runtime walk (computed fields on a
/// fetched table, array/geometry pivots, storage errors, exhausted fetch
/// budget) — the caller leaves the idiom unfolded.
async fn fold_record_walk(
	txn: &Arc<Transaction>,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	start: RecordId,
	idiom: &Idiom,
	computed_cache: &mut HashMap<TableName, bool>,
	fetch_budget: &mut u32,
) -> Option<Value> {
	let mut pivot = Value::RecordId(start);
	for part in idiom.0.iter().skip(1) {
		// The collector only admits Start + Field idioms.
		let Part::Field(name) = part else {
			return None;
		};
		let name = name.as_str();
		pivot = match pivot {
			Value::RecordId(rid) => {
				// An object record-id key resolves its own components without
				// a fetch — the runtime walk does the same so that key
				// components (which are immutable) cannot be shadowed by
				// document fields.
				if let RecordIdKey::Object(obj) = &rid.key
					&& let Some(component) = obj.get(name)
				{
					component.clone()
				} else {
					// Charge the budget before any storage access for this
					// hop: the field-metadata lookup below reads the catalog
					// on a (table) cache miss, so checking the budget only
					// before the record read would leave catalog I/O
					// unbounded for conditions with many distinct tables.
					if *fetch_budget == 0 {
						tracing::debug!(
							table = %rid.table,
							"plan-time record fetch budget exhausted in \
							 fold_constant_record_idioms; leaving record idiom unfolded",
						);
						return None;
					}
					*fetch_budget -= 1;
					// The runtime dereference evaluates computed fields on the
					// fetched record; the plan-time walk cannot, so any
					// computed field on the table disqualifies the fold.
					if table_has_computed_fields(txn, ns_id, db_id, &rid.table, computed_cache)
						.await?
					{
						return None;
					}
					let record =
						match txn.get_record(ns_id, db_id, &rid.table, &rid.key, None).await {
							Ok(record) => record,
							Err(e) => {
								tracing::debug!(
									table = %rid.table,
									error = %e,
									"plan-time record read failed in \
									 fold_constant_record_idioms; leaving record idiom unfolded",
								);
								return None;
							}
						};
					match &record.data {
						Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
						// Missing record (or non-object data): field access
						// yields NONE, as at runtime.
						_ => Value::None,
					}
				}
			}
			Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
			// Arrays map field access over their elements and geometries
			// expose GeoJSON fields; neither walk is replicated here.
			Value::Array(_) | Value::Geometry(_) => return None,
			// Field access on any other value yields NONE at runtime.
			_ => Value::None,
		};
	}
	Some(pivot)
}

/// `true` when the table defines at least one computed field, matching the
/// `has_computed` criterion of
/// [`build_field_state_raw`](crate::exec::operators::scan::pipeline::build_field_state_raw).
/// Returns `None` when the field definitions cannot be read — callers must
/// treat that as "do not fold".
async fn table_has_computed_fields(
	txn: &Arc<Transaction>,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	cache: &mut HashMap<TableName, bool>,
) -> Option<bool> {
	if let Some(has) = cache.get(table) {
		return Some(*has);
	}
	let fields = match txn.all_tb_fields(ns_id, db_id, table, None).await {
		Ok(fields) => fields,
		Err(e) => {
			tracing::debug!(
				table = %table,
				error = %e,
				"plan-time field list failed in fold_constant_record_idioms; \
				 leaving record idiom unfolded",
			);
			return None;
		}
	};
	let has = fields.iter().any(|fd| fd.computed.is_some());
	cache.insert(table.clone(), has);
	Some(has)
}

/// Extract the record id root of a foldable idiom: a
/// `Part::Start(Expr::Literal(record id))` followed by one or more plain
/// `Part::Field` parts. Record id literals with complex keys (objects,
/// arrays, ranges, generators) are rejected by [`try_literal_to_value`].
fn candidate_record_root(idiom: &Idiom) -> Option<RecordId> {
	let mut parts = idiom.0.iter();
	let Some(Part::Start(Expr::Literal(lit))) = parts.next() else {
		return None;
	};
	if idiom.0.len() < 2 || !parts.all(|p| matches!(p, Part::Field(_))) {
		return None;
	}
	match try_literal_to_value(lit) {
		Some(Value::RecordId(rid)) => Some(rid),
		_ => None,
	}
}

/// Subexpressions the fold never descends into: each evaluates in its own
/// scope at execution time — a nested SELECT resolves its own condition when
/// it is planned; blocks, closures, loop bodies and nested statements run
/// per invocation, possibly after writes — so a value folded inside one at
/// plan time would not match what execution observes.
fn is_fold_boundary(expr: &Expr) -> bool {
	matches!(
		expr,
		Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Block(_)
			| Expr::Closure(_)
			| Expr::Sleep(_)
	)
}

/// Collects foldable record-idiom candidates, stopping at fold boundaries
/// (see [`is_fold_boundary`]).
struct CandidateCollector {
	idioms: Vec<(Idiom, RecordId)>,
}

impl Visitor for CandidateCollector {
	type Error = Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if is_fold_boundary(expr) {
			return Ok(());
		}
		if let Expr::Idiom(idiom) = expr
			&& let Some(rid) = candidate_record_root(idiom)
		{
			self.idioms.push((idiom.clone(), rid));
			// A Start(literal) + Field idiom contains no nested expressions.
			return Ok(());
		}
		expr.visit(self)
	}
}

/// Replaces collected idioms with their folded literal values, stopping at
/// the same boundaries as the collector. `None` entries (failed folds) are
/// left as idioms.
struct FoldApplier<'a> {
	folded: &'a HashMap<Idiom, Option<Expr>>,
}

impl MutVisitor for FoldApplier<'_> {
	type Error = Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		if is_fold_boundary(expr) {
			return Ok(());
		}
		if let Expr::Idiom(idiom) = expr
			&& let Some(Some(folded)) = self.folded.get(idiom)
		{
			*expr = folded.clone();
			return Ok(());
		}
		expr.visit_mut(self)
	}
}
