//! Plan-time folding of row-independent record-path traversals.
//!
//! After bind parameters are substituted into a `WHERE` predicate, a
//! comparison operand can be a path rooted at a literal record id —
//! `$scan.task.finishedAt` becomes `(scan:one).task.finishedAt`. The value of
//! such a path does not depend on the row being scanned, but index analysis
//! ([`IndexAnalyzer`](crate::exec::index::analysis::IndexAnalyzer)) only
//! matches `path op literal` comparisons, so the predicate would otherwise
//! plan as a full table scan.
//!
//! [`Planner::fold_constant_record_paths`] resolves those paths to their
//! values at plan time — through the same transaction the plan executes
//! against, so the values match what any per-row evaluation would read — and
//! replaces them with literals.
//!
//! # Parity contract
//!
//! Folding replaces the per-row runtime walk
//! ([`evaluate_field`](crate::exec::vm::access::field)) with one plan-time walk,
//! so it must produce exactly the value the runtime walk would produce. The
//! fold therefore only runs when the two are provably equivalent; otherwise
//! it leaves the path unfolded, and the predicate keeps its per-row
//! evaluation over a table scan:
//!
//! - Permission checks must be disabled at execute time ([`Planner::should_check_perms_for_view`]
//!   returns `false`): the runtime dereference applies table- and field-level SELECT permissions,
//!   which the plan-time walk does not replicate.
//! - Every table the walk fetches from must define no computed fields: the runtime dereference
//!   evaluates computed fields even when permission checks are disabled (see
//!   [`process_fetched_record`](crate::exec::operators::fetch)).
//! - Every table the walk fetches from must carry no migration step: the runtime dereference
//!   replays a record stamped before the table's newest step through the pending steps, and the
//!   plan-time walk reads the stored bytes as they are.
//! - The path must be a literal record id followed by plain field accesses, and every intermediate
//!   value must be an object, a record id (fetched, honouring the [`RecordIdKey::Object`]
//!   key-component shortcut), or a scalar (which yields `NONE`, as at runtime). Arrays and
//!   geometries have their own traversal semantics and are left unfolded.
//! - The folded value must have a literal form that evaluates back to the same value (see
//!   [`literal_if_round_trips`]): a closure loses its captures and a set becomes an array, and
//!   substituting a lossy form changes results.
//! - The walk never descends into a scalar subquery or a closure body (see [`is_fold_boundary`]):
//!   each evaluates in its own scope at execution time, possibly after writes.
//! - The call site additionally requires the whole statement to be read-only — a write anywhere in
//!   the statement could change a record between the plan-time read and the per-row evaluation the
//!   fold replaces — and excludes versioned SELECTs, including an enclosing version context: the
//!   plan-time walk reads current state, not a versioned snapshot.

use std::collections::HashMap;
use std::sync::Arc;

use super::super::Planner;
use super::super::util::{fold_expressions, try_literal_to_value, try_value_to_logical_expr};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::logical::{LogicalExpr, LogicalRecordIdKey};
use crate::kvs::Transaction;
use crate::val::{RecordId, RecordIdKey, Strand, TableName, Value};

/// Upper bound on plan-time record hops per folded predicate. The fold
/// reads storage during planning, before any operator can bound the work;
/// the budget keeps a pathological predicate (hundreds of distinct record
/// paths, or long traversal chains) from turning planning into unbounded
/// storage I/O. One unit is charged per record hop, covering both the
/// hop's table lookups (catalog reads on a per-table cache miss) and its
/// record read. Generous relative to real predicates.
const FOLD_FETCH_BUDGET: u32 = 32;

impl<'ctx> Planner<'ctx> {
	/// Replace row-independent record-path traversals in `cond` with their
	/// literal values, when the plan-time walk is provably equivalent to the
	/// per-row runtime walk (see the module docs for the parity contract), and
	/// simplify the result again under the expression fold. Answers whether
	/// `cond` changed.
	///
	/// Best-effort: any guard failure, unsupported shape, or storage error
	/// leaves the affected path unfolded rather than failing the plan. A walk
	/// that reads a record notes an opaque catalog read on the transaction, so
	/// no compilation holding the folded value is kept for another request:
	/// the value is the record's at this transaction's snapshot.
	pub(crate) async fn fold_constant_record_paths(&self, cond: &mut LogicalExpr) -> bool {
		let Some(txn) = self.txn() else {
			return false;
		};
		let (Some(ns_name), Some(db_name)) = (self.ns(), self.db()) else {
			return false;
		};
		// SECURITY: the plan-time walk reads records without evaluating
		// table- or field-level SELECT permissions. It may only run when the
		// runtime dereference it replaces would bypass permission evaluation
		// entirely — the session holds a viewer-or-above role covering this
		// whole database — so no value the actor could not SELECT can reach
		// the plan or its EXPLAIN output.
		if self.should_check_perms_for_view(ns_name, db_name) {
			return false;
		}

		let mut candidates = Vec::new();
		collect_candidates(cond, &mut candidates);
		if candidates.is_empty() {
			return false;
		}

		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return false;
		};

		txn.note_opaque_read();
		// A `None` entry records a failed fold, so duplicate occurrences of
		// the same path don't repeat the walk and its storage reads.
		let mut folded: HashMap<LogicalExpr, Option<LogicalExpr>> = HashMap::new();
		let mut blocked_cache: HashMap<TableName, bool> = HashMap::new();
		let mut fetch_budget = FOLD_FETCH_BUDGET;
		for (path, rid, fields) in candidates {
			if folded.contains_key(&path) {
				continue;
			}
			let value = fold_record_walk(
				txn,
				ns_id,
				db_id,
				rid,
				&fields,
				&mut blocked_cache,
				&mut fetch_budget,
			)
			.await;
			folded.insert(path, value.and_then(|v| literal_if_round_trips(&v)));
		}
		if !folded.values().any(Option::is_some) {
			return false;
		}

		apply_folds(cond, &folded);
		// Substitution can leave an operand constant that was not constant
		// during the first pass, so simplify again under the same rules. A
		// literal operand is inert, which is what lets the enclosing
		// comparison collapse.
		let capabilities = self.frozen().get_capabilities();
		fold_expressions(cond, self.function_registry(), &capabilities, None, false);
		true
	}
}

/// The literal form of `value`, when substituting it is value-preserving.
///
/// A value's literal form can be lossy — a closure literal drops its
/// captures, a set becomes an array literal (which compares unequal to a set
/// at runtime) — so a folded value may only be substituted when evaluating
/// its literal form yields the same value.
fn literal_if_round_trips(value: &Value) -> Option<LogicalExpr> {
	let expr = try_value_to_logical_expr(value.clone())?;
	(try_literal_to_value(&expr).as_ref() == Some(value)).then_some(expr)
}

/// Walk `fields` in order from the literal record id `start`,
/// mirroring the runtime traversal in
/// [`evaluate_field`](crate::exec::vm::access::field). Returns `None` when the
/// walk cannot be proven equivalent to the runtime walk (computed fields on a
/// fetched table, array/geometry pivots, storage errors, exhausted fetch
/// budget) — the caller leaves the path unfolded.
async fn fold_record_walk(
	txn: &Arc<Transaction>,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	start: RecordId,
	fields: &[Strand],
	blocked_cache: &mut HashMap<TableName, bool>,
	fetch_budget: &mut u32,
) -> Option<Value> {
	let mut pivot = Value::RecordId(start);
	for name in fields {
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
					// hop: the table lookups below read the catalog on a
					// (table) cache miss, so checking the budget only before
					// the record read would leave catalog I/O unbounded for
					// predicates with many distinct tables.
					if *fetch_budget == 0 {
						tracing::debug!(
							table = %rid.table,
							"plan-time record fetch budget exhausted in \
							 fold_constant_record_paths; leaving record path unfolded",
						);
						return None;
					}
					*fetch_budget -= 1;
					// The runtime dereference evaluates computed fields on the
					// fetched record and replays its migration steps; the
					// plan-time walk does neither, so a table with either
					// disqualifies the fold.
					if table_blocks_fold(txn, ns_id, db_id, &rid.table, blocked_cache).await? {
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
									 fold_constant_record_paths; leaving record path unfolded",
								);
								return None;
							}
						};
					if let Value::Object(obj) = &record.data {
						obj.get(name).cloned().unwrap_or(Value::None)
					} else {
						// Missing record (or non-object data): field access
						// yields NONE, as at runtime.
						Value::None
					}
				}
			}
			Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
			// Arrays map field access over their elements and geometries
			// expose GeoJSON fields; neither walk is replicated here.
			Value::Array(_) | Value::Geometry(_) => return None,
			// Field access on any other value yields NONE at runtime.
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
			| Value::Set(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => Value::None,
		};
	}
	Some(pivot)
}

/// `true` when the runtime dereference of a record of `table` answers
/// something the plan-time walk does not: the table defines at least one
/// computed field, matching the `has_computed` criterion of
/// [`build_field_state_raw`](crate::exec::operators::scan::pipeline::build_field_state_raw),
/// or its layouts name a migration step
/// ([`TableLayouts::newest_migration`](crate::catalog::TableLayouts::newest_migration)),
/// which the dereference replays a record stamped before it through. Returns
/// `None` when the field definitions or the layouts cannot be read — callers
/// must treat that as "do not fold".
async fn table_blocks_fold(
	txn: &Arc<Transaction>,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	cache: &mut HashMap<TableName, bool>,
) -> Option<bool> {
	if let Some(blocked) = cache.get(table) {
		return Some(*blocked);
	}
	let fields = match txn.all_tb_fields(ns_id, db_id, table, None).await {
		Ok(fields) => fields,
		Err(e) => {
			tracing::debug!(
				table = %table,
				error = %e,
				"plan-time field list failed in fold_constant_record_paths; \
				 leaving record path unfolded",
			);
			return None;
		}
	};
	let blocked = if fields.iter().any(|fd| fd.computed.is_some()) {
		true
	} else {
		match txn.table_layouts(ns_id, db_id, table, None).await {
			Ok(layouts) => layouts.newest_migration().is_some(),
			Err(e) => {
				tracing::debug!(
					table = %table,
					error = %e,
					"plan-time table layouts failed in fold_constant_record_paths; \
					 leaving record path unfolded",
				);
				return None;
			}
		}
	};
	cache.insert(table.clone(), blocked);
	Some(blocked)
}

/// Decompose a foldable path into its record id and the fields read from it:
/// one or more [`LogicalExpr::Access`] steps over a [`LogicalExpr::RecordId`]
/// literal with a scalar key. Consecutive field accesses nest plainly, so no
/// other form appears in such a path. A key holding expressions, a generator
/// or a range names no single record and is rejected, as is any other step.
fn candidate_record_path(expr: &LogicalExpr) -> Option<(RecordId, Vec<Strand>)> {
	let mut fields = Vec::new();
	let mut current = expr;
	let rid = loop {
		if let LogicalExpr::Access {
			base,
			field,
		} = current
		{
			fields.push(field.clone());
			current = base;
		} else if let LogicalExpr::RecordId(rid) = current {
			if !fields.is_empty() {
				break rid;
			}
			return None;
		} else {
			return None;
		}
	};
	fields.reverse();
	let key = match &rid.key {
		LogicalRecordIdKey::Number(n) => RecordIdKey::Number(*n),
		LogicalRecordIdKey::String(s) => RecordIdKey::String(s.clone()),
		LogicalRecordIdKey::Uuid(u) => RecordIdKey::Uuid(*u),
		LogicalRecordIdKey::Array(_)
		| LogicalRecordIdKey::Object(_)
		| LogicalRecordIdKey::Generate(_)
		| LogicalRecordIdKey::Range(_) => return None,
	};
	Some((
		RecordId {
			table: rid.table.clone(),
			key,
		},
		fields,
	))
}

/// Subexpressions the fold never descends into: each evaluates in its own
/// scope at execution time — a nested statement resolves its own predicate
/// when it is planned, and a closure body runs per invocation, possibly after
/// writes — so a value folded inside one at plan time would not match what
/// execution observes.
fn is_fold_boundary(expr: &LogicalExpr) -> bool {
	matches!(expr, LogicalExpr::Subquery(_) | LogicalExpr::Closure(_))
}

/// Collects foldable record-path candidates, stopping at fold boundaries
/// (see [`is_fold_boundary`]).
fn collect_candidates(expr: &LogicalExpr, out: &mut Vec<(LogicalExpr, RecordId, Vec<Strand>)>) {
	if is_fold_boundary(expr) {
		return;
	}
	if let Some((rid, fields)) = candidate_record_path(expr) {
		// A record id + field-access path contains no nested expressions.
		out.push((expr.clone(), rid, fields));
		return;
	}
	for child in expr.children() {
		collect_candidates(child, out);
	}
}

/// Replaces collected paths with their folded literal values, stopping at
/// the same boundaries as the collector. `None` entries (failed folds) are
/// left as paths.
fn apply_folds(expr: &mut LogicalExpr, folded: &HashMap<LogicalExpr, Option<LogicalExpr>>) {
	if is_fold_boundary(expr) {
		return;
	}
	if let Some(Some(literal)) = folded.get(expr) {
		*expr = literal.clone();
		return;
	}
	for child in expr.children_mut() {
		apply_folds(child, folded);
	}
}
