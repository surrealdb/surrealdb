//! Expression registry for tracking and deduplicating computed expressions.
//!
//! This module provides infrastructure for collecting expressions that need computation
//! during query execution, assigning them internal names, and tracking where in the
//! execution pipeline they must be computed.
//!
//! The key insight is "compute once, reference by name" — complex expressions are
//! evaluated once in a `Compute` operator and then referenced by field name in
//! downstream operators (`Sort`, `Project`, etc.).
//!
//! Registration is keyed by expression *and* alias, and a registered expression
//! is evaluated exactly once per row. That is a semantic guarantee, not only an
//! optimisation: for a non-deterministic expression such as `rand()`, ORDER BY
//! on the alias sorts by the value the projection reports, so
//! `SELECT rand() AS r FROM t ORDER BY r` is ascending in the `r` values it
//! returns. Two aliases over the same expression are two entries, hence two
//! independent values per row.
//!
//! # Current Usage
//!
//! The registry is created once per SELECT pipeline in `plan_select_pipeline` and
//! shared between `plan_sort_consolidated` (which registers ORDER BY expressions at
//! `ComputePoint::Sort`) and `plan_projections_fast` (which registers complex
//! SELECT expressions at `ComputePoint::Project`). This deduplication ensures that
//! an expression appearing in both ORDER BY and SELECT is evaluated only once.
//!
//! # Integration with Aggregates
//!
//! This system coexists with the aggregate handling pattern:
//!
//! - **Aggregates**: Use synthetic names `_a0`, `_a1`, etc. and are handled by the `Aggregate`
//!   operator. The `AggregateExtractor` visitor extracts aggregate function calls and replaces them
//!   with field references.
//!
//! - **Computed Expressions**: Use synthetic names `_e0`, `_e1`, etc. (or output aliases when
//!   available) and are handled by the `Compute` operator.
//!
//! When GROUP BY is present, the Aggregate operator handles all expression evaluation
//! internally, so we don't use the expression registry for those queries. The
//! consolidated approach is used for queries without GROUP BY where ORDER BY
//! references SELECT aliases.
//!
//! # Reserved Names
//!
//! The registry can reserve field names that synthetic `_eN` generation must
//! avoid, and can protect source-field names from all Compute internal names.
//! Reserved names do not block alias-based names by themselves; protected names
//! do, because overwriting them before projection would change later field reads.
//!
//! # Relation to `pre_decode_filter`
//!
//! Pre-decode WHERE rejection using raw KV bytes lives in [`crate::exec::pre_decode_filter`] and
//! runs *before* any `PhysicalExpr` evaluation. This registry is **post-decode only**: it names
//! expressions computed on decoded `Value`s for `Sort` / `SelectProject` via `Compute`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use surrealdb_types::ToSql;

use super::planner::Planner;
use crate::err::Error;
use crate::exec::PhysicalExpr;
use crate::exec::logical::{BatchRetry, ColumnEffects, columns_may_run_column_major};
use crate::expr::logical::{LogicalExpr, LogicalField, LogicalFields, OutputPath, OutputPathPart};

/// Identifies when an expression must be computed in the execution pipeline.
///
/// Currently only `Sort` and `Project` are used by the planner. `Filter` and
/// `Aggregate` are reserved for future optimisations (e.g. pre-computing
/// complex WHERE sub-expressions, or consolidating GROUP BY key evaluation).
/// The `Aggregate` variant is intentionally unused today because the
/// aggregate operator evaluates its own expressions (see module-level docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(dead_code)] // Filter and Aggregate are reserved extension points
pub enum ComputePoint {
	/// Compute before Filter (expressions used in WHERE).
	/// Reserved for future pre-filter expression computation.
	Filter = 0,
	/// Compute before Aggregate (GROUP BY keys, aggregate inputs).
	/// Reserved — GROUP BY currently evaluates its expressions in the `Aggregate` operator.
	Aggregate = 1,
	/// Compute before Sort (ORDER BY keys, SELECT expressions).
	Sort = 2,
	/// Compute before Project (complex SELECT expressions).
	Project = 3,
}

/// Information about a registered expression.
#[derive(Debug, Clone)]
pub struct ExpressionInfo {
	/// The internal field name (e.g., "city_population" or "_e0")
	pub internal_name: String,
	/// The physical expression to evaluate
	pub expr: Arc<dyn PhysicalExpr>,
	/// Where this expression must be computed
	pub compute_point: ComputePoint,
	/// Position in the registration sequence, which orders the fields of the
	/// `Compute` operator built from a compute point, and so the order their
	/// effects happen in. The planner registers a point's expressions as the
	/// statement writes them — ORDER BY keys as listed, SELECT fields left to
	/// right — so within one point this is written order.
	///
	/// Assigned once: promoting an expression to an earlier compute point
	/// keeps the position its first registration gave it.
	pub order: usize,
	/// What evaluating the expression does beyond producing its value, which
	/// is what decides the shape of the `Compute` operator built from the
	/// point's expressions; see
	/// [`Compute::column_major`](crate::exec::operators::Compute::column_major).
	pub effects: ColumnEffects,
}

/// Registry that tracks all expressions needing computation in a query.
///
/// This follows the pattern established by aggregate handling, where expressions
/// are replaced with synthetic field references and evaluated once by a dedicated
/// operator.
#[derive(Debug, Default, Clone)]
pub struct ExpressionRegistry {
	/// Map from expression SQL representation to its info.
	/// Using SQL string as key for deduplication (same expression = same SQL).
	expressions: HashMap<String, ExpressionInfo>,
	/// Counter for generating synthetic field names.
	counter: usize,
	/// Counter handing out [`ExpressionInfo::order`] positions.
	next_order: usize,
	/// Field names that synthetic `_eN` generation must avoid. This does NOT
	/// affect alias-based names — those are user-chosen and accepted unless the
	/// name is also present in `used_internal_names` as a protected source field.
	/// Populated from SELECT field names at plan time.
	reserved_names: HashSet<String>,
	/// Internal names already assigned to expressions — enables O(1) conflict checks
	/// in `choose_internal_name` instead of scanning all `expressions.values()`.
	used_internal_names: HashSet<String>,
}

impl ExpressionRegistry {
	/// Create a new empty registry (no reserved names, no protected names).
	///
	/// Test-only convenience. Production code uses
	/// [`Self::with_reserved_and_protected_names`] populated from the
	/// SELECT's field set so that synthetic `_eN` names cannot collide
	/// with user fields and Compute outputs cannot shadow source fields.
	#[cfg(test)]
	pub(crate) fn new() -> Self {
		Self {
			expressions: HashMap::new(),
			counter: 0,
			next_order: 0,
			reserved_names: HashSet::new(),
			used_internal_names: HashSet::new(),
		}
	}

	/// Create a registry with reserved field names but no protected names.
	///
	/// Test-only convenience for asserting synthetic-name avoidance in
	/// isolation. Production code always uses
	/// [`Self::with_reserved_and_protected_names`] so source fields read
	/// by simple Include/Rename projections are also protected from
	/// being clobbered by Compute internal names.
	#[cfg(test)]
	pub(crate) fn with_reserved_names(names: Vec<String>) -> Self {
		Self::with_reserved_and_protected_names(names, Vec::new())
	}

	/// Create a registry with names reserved from synthetic generation and
	/// source-field names protected from all Compute internal names.
	///
	/// Protected names are inserted into `used_internal_names` up front. This
	/// makes `choose_internal_name` skip them for both aliases and synthetic
	/// fallbacks, preserving source fields that later projections still need
	/// to read after Sort/Project Compute operators have run.
	pub fn with_reserved_and_protected_names(
		reserved_names: Vec<String>,
		protected_names: Vec<String>,
	) -> Self {
		Self {
			expressions: HashMap::new(),
			counter: 0,
			next_order: 0,
			reserved_names: reserved_names.into_iter().collect(),
			used_internal_names: protected_names.into_iter().collect(),
		}
	}

	/// Hand out the next [`ExpressionInfo::order`] position.
	fn next_order(&mut self) -> usize {
		let order = self.next_order;
		self.next_order += 1;
		order
	}

	/// Register an expression and return its internal field name.
	///
	/// If the expression is already registered, returns the existing name.
	/// If the expression has an alias that doesn't conflict, uses the alias.
	/// Otherwise, generates a synthetic name (_e0, _e1, etc.).
	///
	/// When an expression is re-registered at an earlier compute point, the
	/// existing entry is promoted so the expression is evaluated sooner.
	///
	/// Compilation goes through the supplied [`Planner`] so that the
	/// transaction (and ns/db context) attached to it propagate into nested
	/// expressions — most importantly into scalar subqueries used in
	/// ORDER BY / projection. Without this, those subqueries would compile
	/// via a fresh `Planner::new` (no transaction), losing plan-time index
	/// resolution and sort elimination.
	pub async fn register(
		&mut self,
		expr: &LogicalExpr,
		compute_point: ComputePoint,
		alias: Option<String>,
		planner: &Planner<'_>,
	) -> Result<String, Error> {
		// Generate SQL representation for deduplication.
		// When an alias is present, include it in the key so that
		// `false AS isLiked` and `false AS isLocked` are distinct entries.
		let expr_sql = expr.to_sql();
		let dedup_key = match &alias {
			Some(a) => format!("{expr_sql}\0{a}"),
			None => expr_sql,
		};

		// Check if already registered
		if let Some(info) = self.expressions.get(&dedup_key) {
			// If registered at a later point, update to earlier (we need it sooner)
			if compute_point < info.compute_point {
				let mut updated = info.clone();
				updated.compute_point = compute_point;
				self.expressions.insert(dedup_key.clone(), updated);
			}
			return Ok(self.expressions[&dedup_key].internal_name.clone());
		}

		// Judged from the expression as written, before it is compiled away.
		let effects = ColumnEffects::of(expr, planner.function_registry());

		// Convert to physical expression via the live planner so the
		// transaction and ns/db propagate into nested subqueries.
		let physical_expr = planner.physical_expr(expr.clone()).await?;

		// Determine internal name
		let internal_name = self.choose_internal_name(&alias);
		let order = self.next_order();

		let info = ExpressionInfo {
			internal_name: internal_name.clone(),
			expr: physical_expr,
			compute_point,
			order,
			effects,
		};

		self.expressions.insert(dedup_key, info);

		Ok(internal_name)
	}

	/// Register an expression that's already been converted to physical form.
	///
	/// If the same expression key is already registered at a later compute point,
	/// promotes it to the earlier point (matching `register` behaviour).
	#[allow(clippy::needless_pass_by_value)] // `expr_key` is moved into `dedup_key` in the no-alias path; borrowing forces an extra allocation in the planner hot path.
	pub fn register_physical(
		&mut self,
		expr_key: String,
		physical_expr: Arc<dyn PhysicalExpr>,
		compute_point: ComputePoint,
		alias: Option<String>,
		effects: ColumnEffects,
	) -> String {
		// Include alias in the dedup key (same logic as `register`)
		let dedup_key = match &alias {
			Some(a) => format!("{expr_key}\0{a}"),
			None => expr_key,
		};

		// Check if already registered
		if let Some(info) = self.expressions.get(&dedup_key) {
			// Promote to earlier compute point if needed (same logic as `register`)
			if compute_point < info.compute_point {
				let mut updated = info.clone();
				updated.compute_point = compute_point;
				self.expressions.insert(dedup_key.clone(), updated);
			}
			return self.expressions[&dedup_key].internal_name.clone();
		}

		let internal_name = self.choose_internal_name(&alias);
		let order = self.next_order();

		let info = ExpressionInfo {
			internal_name: internal_name.clone(),
			expr: physical_expr,
			compute_point,
			order,
			effects,
		};

		self.expressions.insert(dedup_key, info);

		internal_name
	}

	/// Choose an internal name, preferring the alias if available and not conflicting.
	///
	/// Aliases are user-chosen names (e.g. `AS doubled`) and are only rejected
	/// if `used_internal_names` already contains the name, either because
	/// another expression claimed it or because the planner protected a source
	/// field that later projections still need to read. `reserved_names` is
	/// intentionally NOT checked for aliases because those only guard
	/// synthetic fallback names.
	///
	/// Synthetic `_eN` names, on the other hand, are system-generated and must
	/// avoid both `reserved_names` (to protect document fields) and
	/// `used_internal_names` (to prevent inter-expression collisions).
	fn choose_internal_name(&mut self, alias: &Option<String>) -> String {
		if let Some(name) = alias
			&& !self.used_internal_names.contains(name)
		{
			self.used_internal_names.insert(name.clone());
			return name.clone();
		}

		// Generate synthetic name, skipping any that collide with reserved
		// or already-used names.
		loop {
			let name = format!("_e{}", self.counter);
			self.counter += 1;
			if !self.reserved_names.contains(&name) && !self.used_internal_names.contains(&name) {
				self.used_internal_names.insert(name.clone());
				return name;
			}
		}
	}

	/// Get all expressions that need to be computed at a specific point, in
	/// the order the statement wrote them.
	///
	/// [`ExpressionInfo::order`] is what makes that order deterministic:
	/// `HashMap` iteration is not, and the order a compute point's expressions
	/// come back in is the order the `Compute` operator built from them
	/// evaluates their effects in.
	pub fn get_expressions_for_point(
		&self,
		point: ComputePoint,
	) -> Vec<(String, Arc<dyn PhysicalExpr>)> {
		let mut exprs: Vec<_> = self
			.expressions
			.values()
			.filter(|info| info.compute_point == point)
			.map(|info| (info.order, info.internal_name.clone(), Arc::clone(&info.expr)))
			.collect();
		exprs.sort_by_key(|(order, _, _)| *order);
		exprs.into_iter().map(|(_, name, expr)| (name, expr)).collect()
	}

	/// Whether the `Compute` operator built from `point`'s expressions may
	/// evaluate a batch a field at a time; see
	/// [`Compute::column_major`](crate::exec::operators::Compute::column_major).
	/// `Compute` evaluates a field again when its batch call ended in a
	/// signal, which is the repetition the rule weighs.
	pub fn point_may_run_column_major(&self, point: ComputePoint) -> bool {
		let mut columns: Vec<(usize, ColumnEffects)> = self
			.expressions
			.values()
			.filter(|info| info.compute_point == point)
			.map(|info| (info.order, info.effects))
			.collect();
		columns.sort_by_key(|(order, _)| *order);
		let columns: Vec<ColumnEffects> = columns.into_iter().map(|(_, e)| e).collect();
		columns_may_run_column_major(&columns, BatchRetry::OnSignal)
	}

	/// Check if there are any expressions registered for a specific compute point.
	pub fn has_expressions_for_point(&self, point: ComputePoint) -> bool {
		self.expressions.values().any(|info| info.compute_point == point)
	}

	/// Check if a name has been registered as an expression output field.
	pub fn contains_name(&self, name: &str) -> bool {
		self.expressions.values().any(|info| info.internal_name == name)
	}
}

// ============================================================================
// Alias Resolution
// ============================================================================

/// The output path an ORDER BY key spells when it is a plain chain of field
/// reads (`x`, `b.c`) or lookups, `None` for any other shape. Unlike
/// [`LogicalExpr::output_path`], a step that would contribute nothing to the
/// name (`[0]`, `[WHERE …]`, `.*`) disqualifies the key, so `ORDER BY b[0].c`
/// does not resolve to `AS b.c`.
fn exact_output_path(expr: &LogicalExpr) -> Option<OutputPath> {
	let mut parts = Vec::new();
	let mut current = expr;
	loop {
		match current {
			LogicalExpr::Field(name) => {
				parts.push(OutputPathPart::Field(name.as_str().to_owned()));
				break;
			}
			LogicalExpr::Access {
				base,
				field,
			} => {
				parts.push(OutputPathPart::Field(field.as_str().to_owned()));
				current = base;
			}
			LogicalExpr::Lookup {
				base,
				lookup,
			} => {
				parts.push(OutputPathPart::Lookup(lookup.to_sql()));
				current = base;
			}
			LogicalExpr::Current => break,
			_ => return None,
		}
	}
	if parts.is_empty() {
		return None;
	}
	parts.reverse();
	Some(OutputPath::keys(parts))
}

/// The name of a single-field read, `None` for any other shape.
fn single_field_name(expr: &LogicalExpr) -> Option<&str> {
	match expr {
		LogicalExpr::Field(name) => Some(name.as_str()),
		_ => None,
	}
}

/// Resolve a path reference in ORDER BY to the underlying SELECT expression.
///
/// Supports both single-part aliases (`ORDER BY x` matching `AS x`) and
/// nested aliases (`ORDER BY b.c` matching `AS b.c`).
///
/// Returns the resolved expression and a flat string name for registry use.
pub fn resolve_order_by_alias(
	order_value: &LogicalExpr,
	fields: &LogicalFields,
) -> Option<(LogicalExpr, String)> {
	let order_name = single_field_name(order_value);
	match fields {
		LogicalFields::Value {
			expr,
			alias,
		} => {
			// SELECT VALUE aliases are guaranteed single-part by the parser.
			if let Some(alias) = alias
				&& let Some(alias_name) = alias.as_field()
				&& order_name == Some(alias_name)
			{
				return Some((expr.clone(), alias_name.to_owned()));
			}
			// Unaliased single-part field match
			if let Some(name) = order_name
				&& single_field_name(expr) == Some(name)
			{
				return Some((expr.clone(), name.to_owned()));
			}
			None
		}
		LogicalFields::Select(field_list) => {
			let order_path = exact_output_path(order_value);
			for field in field_list {
				if let LogicalField::Single {
					expr,
					alias,
					..
				} = field
				{
					// Alias match (any depth)
					if let Some(alias) = alias
						&& order_path.as_ref() == Some(alias)
					{
						return Some((expr.clone(), alias.name()));
					}
					// Unaliased single-part field match
					if let Some(name) = order_name
						&& alias.is_none() && single_field_name(expr) == Some(name)
					{
						return Some((expr.clone(), name.to_owned()));
					}
				}
			}
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::physical_expr::Literal;
	use crate::val::{Number, Value};

	/// A column whose evaluation has an effect to reorder — what a record
	/// dereference or a call the plan cannot see through answers.
	const EFFECTFUL: ColumnEffects = ColumnEffects {
		effect_free: false,
		may_signal: false,
		may_raise: true,
	};

	fn literal_expr(n: i64) -> Arc<dyn PhysicalExpr> {
		Arc::new(Literal(Value::Number(Number::Int(n))))
	}

	#[test]
	fn test_registry_deduplication() {
		let mut registry = ExpressionRegistry::new();

		// Check synthetic name generation
		let name1 = registry.choose_internal_name(&None);
		assert_eq!(name1, "_e0");

		let name2 = registry.choose_internal_name(&None);
		assert_eq!(name2, "_e1");

		// Check alias preference
		let name3 = registry.choose_internal_name(&Some("city_population".into()));
		assert_eq!(name3, "city_population");
	}

	#[test]
	fn test_reserved_names_do_not_block_aliases() {
		let mut registry =
			ExpressionRegistry::with_reserved_names(vec!["id".into(), "name".into()]);

		// Aliases are user-chosen and always accepted, even if the name
		// appears in reserved_names.
		let name1 = registry.choose_internal_name(&Some("id".into()));
		assert_eq!(name1, "id");

		let name2 = registry.choose_internal_name(&Some("name".into()));
		assert_eq!(name2, "name");

		let name3 = registry.choose_internal_name(&Some("city".into()));
		assert_eq!(name3, "city");
	}

	#[test]
	fn test_reserved_names_block_synthetic_only() {
		let mut registry =
			ExpressionRegistry::with_reserved_names(vec!["id".into(), "name".into()]);

		// Aliases are fine
		let name1 = registry.choose_internal_name(&Some("id".into()));
		assert_eq!(name1, "id");

		// Synthetic names must avoid reserved names
		let mut registry2 =
			ExpressionRegistry::with_reserved_names(vec!["_e0".into(), "_e1".into()]);
		let syn1 = registry2.choose_internal_name(&None);
		assert_eq!(syn1, "_e2"); // Skipped _e0 and _e1
	}

	#[test]
	fn test_register_physical_dedup_returns_existing_name() {
		let mut registry = ExpressionRegistry::new();

		let name1 = registry.register_physical(
			"val * 2".into(),
			literal_expr(42),
			ComputePoint::Sort,
			Some("doubled".into()),
			ColumnEffects::INERT,
		);
		assert_eq!(name1, "doubled");

		// Re-registering the same key returns the existing internal name
		let name2 = registry.register_physical(
			"val * 2".into(),
			literal_expr(99),
			ComputePoint::Project,
			Some("doubled".into()),
			ColumnEffects::INERT,
		);
		assert_eq!(name2, "doubled");

		// Only one expression should be registered (deduplication)
		let sort_exprs = registry.get_expressions_for_point(ComputePoint::Sort);
		assert_eq!(sort_exprs.len(), 1);
		assert_eq!(sort_exprs[0].0, "doubled");
	}

	#[test]
	fn a_compute_point_reports_its_expressions_in_registration_order() {
		// The planner registers in the order the statement writes, and the
		// order a point's expressions come back in is the order the `Compute`
		// operator built from them evaluates their effects in — so it follows
		// registration and not how the internal names happen to sort.
		let mut registry = ExpressionRegistry::new();
		for (key, alias) in [("expr_b", "b"), ("expr_a", "a"), ("expr_c", "c")] {
			registry.register_physical(
				key.into(),
				literal_expr(1),
				ComputePoint::Project,
				Some(alias.into()),
				ColumnEffects::INERT,
			);
		}

		let names: Vec<String> = registry
			.get_expressions_for_point(ComputePoint::Project)
			.into_iter()
			.map(|(name, _)| name)
			.collect();
		assert_eq!(names, vec!["b", "a", "c"]);
	}

	#[test]
	fn a_point_runs_column_major_while_at_most_one_expression_has_effects() {
		let mut registry = ExpressionRegistry::new();
		registry.register_physical(
			"expr_a".into(),
			literal_expr(1),
			ComputePoint::Project,
			Some("a".into()),
			ColumnEffects::INERT,
		);
		assert!(registry.point_may_run_column_major(ComputePoint::Project));

		// One expression with effects still runs column-major: there is no
		// second effect for it to be reordered against, and `Compute` only
		// evaluates a column twice when the first call ended in a signal.
		registry.register_physical(
			"expr_b".into(),
			literal_expr(2),
			ComputePoint::Project,
			Some("b".into()),
			EFFECTFUL,
		);
		assert!(registry.point_may_run_column_major(ComputePoint::Project));

		// A second one takes the whole point off the column-at-a-time shape:
		// the two effects would happen in the order the batch ran the columns
		// rather than the order the statement writes them.
		registry.register_physical(
			"expr_c".into(),
			literal_expr(3),
			ComputePoint::Project,
			Some("c".into()),
			EFFECTFUL,
		);
		assert!(!registry.point_may_run_column_major(ComputePoint::Project));

		// A point with nothing registered has nothing that could have an
		// effect, and an empty `Compute` is never built anyway.
		assert!(registry.point_may_run_column_major(ComputePoint::Sort));
	}

	#[test]
	fn test_register_physical_promotes_compute_point() {
		let mut registry = ExpressionRegistry::new();

		// Register at a later compute point first
		registry.register_physical(
			"complex_expr".into(),
			literal_expr(1),
			ComputePoint::Project,
			Some("total".into()),
			ColumnEffects::INERT,
		);
		assert!(registry.has_expressions_for_point(ComputePoint::Project));
		assert!(!registry.has_expressions_for_point(ComputePoint::Sort));

		// Re-register the same key at an earlier compute point
		registry.register_physical(
			"complex_expr".into(),
			literal_expr(1),
			ComputePoint::Sort,
			Some("total".into()),
			ColumnEffects::INERT,
		);

		// Expression should now be at the earlier (Sort) point
		assert!(registry.has_expressions_for_point(ComputePoint::Sort));
		assert!(!registry.has_expressions_for_point(ComputePoint::Project));
	}

	#[test]
	fn test_duplicate_alias_gets_synthetic_name() {
		let mut registry = ExpressionRegistry::new();

		let name1 = registry.register_physical(
			"expr_a".into(),
			literal_expr(1),
			ComputePoint::Project,
			Some("total".into()),
			ColumnEffects::INERT,
		);
		assert_eq!(name1, "total");

		// Second expression with same alias should get a synthetic name
		let name2 = registry.register_physical(
			"expr_b".into(),
			literal_expr(2),
			ComputePoint::Project,
			Some("total".into()),
			ColumnEffects::INERT,
		);
		assert_eq!(name2, "_e0"); // Falls back to synthetic
	}

	#[test]
	fn test_synthetic_name_skips_reserved() {
		let mut registry = ExpressionRegistry::with_reserved_names(vec!["_e0".into()]);

		// _e0 is reserved, so synthetic name generation should skip to _e1
		let name = registry.choose_internal_name(&None);
		assert_eq!(name, "_e1");
	}

	#[test]
	fn test_synthetic_name_skips_multiple_reserved() {
		let mut registry =
			ExpressionRegistry::with_reserved_names(vec!["_e0".into(), "_e1".into(), "_e3".into()]);

		// Should skip _e0 and _e1, land on _e2
		let name1 = registry.choose_internal_name(&None);
		assert_eq!(name1, "_e2");

		// Next should skip _e3, land on _e4
		let name2 = registry.choose_internal_name(&None);
		assert_eq!(name2, "_e4");
	}
}
