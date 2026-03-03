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
//! The `with_reserved_names` constructor accepts field names that synthetic `_eN`
//! generation must avoid. This prevents auto-generated names from colliding with
//! fields the user explicitly selected. Importantly, `reserved_names` does NOT
//! block alias-based names — those are user-chosen and always accepted. Only
//! synthetic fallback names are guarded.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use surrealdb_types::ToSql;

use super::planner::expr_to_physical_expr;
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::PhysicalExpr;
use crate::expr::part::Part;
use crate::expr::{Expr, Idiom};

/// Identifies when an expression must be computed in the execution pipeline.
///
/// Currently only `Sort` and `Project` are used by the planner. `Filter` and
/// `Aggregate` are reserved for future optimisations (e.g. pre-computing
/// complex WHERE sub-expressions, or consolidating GROUP BY key evaluation).
/// The `Aggregate` variant is intentionally unused today because GROUP BY
/// queries route through the legacy aggregate path which handles expression
/// evaluation internally (see module-level docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(dead_code)] // Filter and Aggregate are reserved extension points
pub enum ComputePoint {
	/// Compute before Filter (expressions used in WHERE).
	/// Reserved for future pre-filter expression computation.
	Filter = 0,
	/// Compute before Aggregate (GROUP BY keys, aggregate inputs).
	/// Reserved — GROUP BY currently uses the legacy `Aggregate` operator.
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
}

/// Registry that tracks all expressions needing computation in a query.
///
/// This follows the pattern established by aggregate handling, where expressions
/// are replaced with synthetic field references and evaluated once by a dedicated
/// operator.
#[derive(Debug, Default)]
pub struct ExpressionRegistry {
	/// Map from expression SQL representation to its info.
	/// Using SQL string as key for deduplication (same expression = same SQL).
	expressions: HashMap<String, ExpressionInfo>,
	/// Counter for generating synthetic field names.
	counter: usize,
	/// Field names that synthetic `_eN` generation must avoid. This does NOT
	/// affect alias-based names — those are user-chosen and always accepted.
	/// Populated from SELECT field names at plan time.
	reserved_names: HashSet<String>,
	/// Internal names already assigned to expressions — enables O(1) conflict checks
	/// in `choose_internal_name` instead of scanning all `expressions.values()`.
	used_internal_names: HashSet<String>,
}

impl ExpressionRegistry {
	/// Create a new empty registry (no reserved names).
	#[allow(dead_code)] // used in tests; production code uses `with_reserved_names`
	pub fn new() -> Self {
		Self {
			expressions: HashMap::new(),
			counter: 0,
			reserved_names: HashSet::new(),
			used_internal_names: HashSet::new(),
		}
	}

	/// Create a registry with reserved field names that cannot be used as internal names.
	///
	/// The planner populates this with SELECT field names so that synthetic
	/// `_eN` names never shadow fields the user explicitly selected.
	pub fn with_reserved_names(names: Vec<String>) -> Self {
		Self {
			expressions: HashMap::new(),
			counter: 0,
			reserved_names: names.into_iter().collect(),
			used_internal_names: HashSet::new(),
		}
	}

	/// Register an expression and return its internal field name.
	///
	/// If the expression is already registered, returns the existing name.
	/// If the expression has an alias that doesn't conflict, uses the alias.
	/// Otherwise, generates a synthetic name (_e0, _e1, etc.).
	///
	/// When an expression is re-registered at an earlier compute point, the
	/// existing entry is promoted so the expression is evaluated sooner.
	pub async fn register(
		&mut self,
		expr: &Expr,
		compute_point: ComputePoint,
		alias: Option<String>,
		ctx: &FrozenContext,
	) -> Result<String, Error> {
		// Generate SQL representation for deduplication
		let expr_sql = expr.to_sql();

		// Check if already registered
		if let Some(info) = self.expressions.get(&expr_sql) {
			// If registered at a later point, update to earlier (we need it sooner)
			if compute_point < info.compute_point {
				let mut updated = info.clone();
				updated.compute_point = compute_point;
				self.expressions.insert(expr_sql.clone(), updated);
			}
			return Ok(self.expressions[&expr_sql].internal_name.clone());
		}

		// Convert to physical expression
		let physical_expr = expr_to_physical_expr(expr.clone(), ctx).await?;

		// Determine internal name
		let internal_name = self.choose_internal_name(&alias);

		let info = ExpressionInfo {
			internal_name: internal_name.clone(),
			expr: physical_expr,
			compute_point,
		};

		self.expressions.insert(expr_sql, info);

		Ok(internal_name)
	}

	/// Register an expression that's already been converted to physical form.
	///
	/// If the same expression key is already registered at a later compute point,
	/// promotes it to the earlier point (matching `register` behaviour).
	pub fn register_physical(
		&mut self,
		expr_key: String,
		physical_expr: Arc<dyn PhysicalExpr>,
		compute_point: ComputePoint,
		alias: Option<String>,
	) -> String {
		// Check if already registered
		if let Some(info) = self.expressions.get(&expr_key) {
			// Promote to earlier compute point if needed (same logic as `register`)
			if compute_point < info.compute_point {
				let mut updated = info.clone();
				updated.compute_point = compute_point;
				self.expressions.insert(expr_key.clone(), updated);
			}
			return self.expressions[&expr_key].internal_name.clone();
		}

		let internal_name = self.choose_internal_name(&alias);

		let info = ExpressionInfo {
			internal_name: internal_name.clone(),
			expr: physical_expr,
			compute_point,
		};

		self.expressions.insert(expr_key, info);

		internal_name
	}

	/// Choose an internal name, preferring the alias if available and not conflicting.
	///
	/// Aliases are user-chosen names (e.g. `AS doubled`) and are only rejected
	/// if another expression already claimed the same name. `reserved_names`
	/// is intentionally NOT checked for aliases because the user explicitly
	/// chose them.
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

	/// Get all expressions that need to be computed at a specific point.
	///
	/// Returns expressions sorted by internal name for deterministic
	/// iteration order (HashMap iteration is non-deterministic).
	pub fn get_expressions_for_point(
		&self,
		point: ComputePoint,
	) -> Vec<(String, Arc<dyn PhysicalExpr>)> {
		let mut exprs: Vec<_> = self
			.expressions
			.values()
			.filter(|info| info.compute_point == point)
			.map(|info| (info.internal_name.clone(), Arc::clone(&info.expr)))
			.collect();
		exprs.sort_by(|(a, _), (b, _)| a.cmp(b));
		exprs
	}

	/// Check if there are any expressions registered for a specific compute point.
	pub fn has_expressions_for_point(&self, point: ComputePoint) -> bool {
		self.expressions.values().any(|info| info.compute_point == point)
	}
}

// ============================================================================
// Alias Resolution
// ============================================================================

use crate::expr::field::{Field, Fields};

/// Resolve an idiom reference in ORDER BY to the underlying SELECT expression.
///
/// When ORDER BY references an alias like `city_population`, we need to find
/// the corresponding SELECT expression and use that for computation.
///
/// Returns the resolved expression and the alias name.
pub fn resolve_order_by_alias(order_idiom: &Idiom, fields: &Fields) -> Option<(Expr, String)> {
	// Only resolve single-part field references (aliases)
	if order_idiom.len() != 1 {
		return None;
	}

	let alias_name = match order_idiom.first() {
		Some(Part::Field(name)) => name.as_str(),
		_ => return None,
	};

	// Search through SELECT fields for a matching alias
	match fields {
		Fields::Value(_) => None, // SELECT VALUE doesn't have aliases
		Fields::Select(field_list) => {
			for field in field_list {
				if let Field::Single(selector) = field {
					// Check if this field has the matching alias
					let field_alias = selector.alias.as_ref().map(idiom_to_string);

					if let Some(ref alias) = field_alias
						&& alias == alias_name
					{
						return Some((selector.expr.clone(), alias.clone()));
					}

					// Also check if the expression itself is a simple field with this name
					if field_alias.is_none()
						&& let Expr::Idiom(ref expr_idiom) = selector.expr
						&& expr_idiom.len() == 1
						&& let Some(Part::Field(name)) = expr_idiom.first()
						&& name.as_str() == alias_name
					{
						return Some((selector.expr.clone(), alias_name.to_string()));
					}
				}
			}
			None
		}
	}
}

/// Convert an idiom to a simple string (for single-part field idioms).
fn idiom_to_string(idiom: &Idiom) -> String {
	if idiom.len() == 1
		&& let Some(Part::Field(name)) = idiom.first()
	{
		return name.clone();
	}
	// Fallback to SQL representation
	idiom.to_sql()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::physical_expr::Literal;
	use crate::val::{Number, Value};

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
		);
		assert_eq!(name1, "doubled");

		// Re-registering the same key returns the existing internal name
		let name2 = registry.register_physical(
			"val * 2".into(),
			literal_expr(99),
			ComputePoint::Project,
			Some("doubled".into()),
		);
		assert_eq!(name2, "doubled");

		// Only one expression should be registered (deduplication)
		let sort_exprs = registry.get_expressions_for_point(ComputePoint::Sort);
		assert_eq!(sort_exprs.len(), 1);
		assert_eq!(sort_exprs[0].0, "doubled");
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
		);
		assert!(registry.has_expressions_for_point(ComputePoint::Project));
		assert!(!registry.has_expressions_for_point(ComputePoint::Sort));

		// Re-register the same key at an earlier compute point
		registry.register_physical(
			"complex_expr".into(),
			literal_expr(1),
			ComputePoint::Sort,
			Some("total".into()),
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
		);
		assert_eq!(name1, "total");

		// Second expression with same alias should get a synthetic name
		let name2 = registry.register_physical(
			"expr_b".into(),
			literal_expr(2),
			ComputePoint::Project,
			Some("total".into()),
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
