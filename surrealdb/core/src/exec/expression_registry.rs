//! Expression registry for tracking and deduplicating computed expressions.
//!
//! This module provides infrastructure for collecting expressions that need computation
//! during query execution, assigning them internal names, and tracking where in the
//! execution pipeline they must be computed.
//!
//! The key insight is "compute once, reference by name" - complex expressions are
//! evaluated once in a Compute operator and then referenced by field name in
//! downstream operators (Sort, Project, etc.).
//!
//! # Integration with Aggregates
//!
//! This system is designed to coexist with the existing aggregate handling pattern:
//!
//! - **Aggregates**: Use synthetic names `_a0`, `_a1`, etc. and are handled by the `Aggregate`
//!   operator. The `AggregateExtractor` visitor extracts aggregate function calls and replaces them
//!   with field references.
//!
//! - **Computed Expressions**: Use synthetic names `_e0`, `_e1`, etc. (or output aliases when
//!   available) and are handled by the `Compute` operator.
//!
//! When GROUP BY is present, the Aggregate operator handles all expression evaluation
//! internally, so we don't use the expression registry for those queries. The consolidated
//! approach is used for queries without GROUP BY where ORDER BY references SELECT aliases.

use std::collections::HashMap;
use std::sync::Arc;

use surrealdb_types::ToSql;

use super::planner::expr_to_physical_expr;
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::exec::PhysicalExpr;
use crate::expr::part::Part;
use crate::expr::{Expr, Idiom};

/// Identifies when an expression must be computed in the execution pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComputePoint {
	/// Compute before Filter (expressions used in WHERE)
	Filter = 0,
	/// Compute before Aggregate (GROUP BY keys, aggregate inputs)
	Aggregate = 1,
	/// Compute before Sort (ORDER BY keys, SELECT expressions)
	Sort = 2,
	/// Compute before Project (complex SELECT expressions)
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
	/// Original output alias (if any)
	pub output_alias: Option<String>,
}

/// Registry that tracks all expressions needing computation in a query.
///
/// This follows the pattern established by aggregate handling, where expressions
/// are replaced with synthetic field references and evaluated once by a dedicated
/// operator.
#[derive(Debug, Default)]
pub struct ExpressionRegistry {
	/// Map from expression SQL representation to its info
	/// Using SQL string as key for deduplication (same expression = same SQL)
	expressions: HashMap<String, ExpressionInfo>,
	/// Counter for generating synthetic field names
	counter: usize,
	/// Track field names that are already in use (from scan/input)
	reserved_names: Vec<String>,
}

impl ExpressionRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			expressions: HashMap::new(),
			counter: 0,
			reserved_names: Vec::new(),
		}
	}

	/// Create a registry with reserved field names that cannot be used as internal names.
	pub fn with_reserved_names(names: Vec<String>) -> Self {
		Self {
			expressions: HashMap::new(),
			counter: 0,
			reserved_names: names,
		}
	}

	/// Register an expression and return its internal field name.
	///
	/// If the expression is already registered, returns the existing name.
	/// If the expression has an alias that doesn't conflict, uses the alias.
	/// Otherwise, generates a synthetic name (_e0, _e1, etc.).
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
			// If already registered at an earlier compute point, keep that
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
			output_alias: alias,
		};

		self.expressions.insert(expr_sql, info);

		Ok(internal_name)
	}

	/// Register an expression that's already been converted to physical form.
	pub fn register_physical(
		&mut self,
		expr_key: String,
		physical_expr: Arc<dyn PhysicalExpr>,
		compute_point: ComputePoint,
		alias: Option<String>,
	) -> String {
		// Check if already registered
		if let Some(info) = self.expressions.get(&expr_key) {
			return info.internal_name.clone();
		}

		let internal_name = self.choose_internal_name(&alias);

		let info = ExpressionInfo {
			internal_name: internal_name.clone(),
			expr: physical_expr,
			compute_point,
			output_alias: alias,
		};

		self.expressions.insert(expr_key, info);

		internal_name
	}

	/// Choose an internal name, preferring the alias if available and not conflicting.
	fn choose_internal_name(&mut self, alias: &Option<String>) -> String {
		if let Some(name) = alias {
			// Check if alias conflicts with reserved names or existing internal names
			let conflicts = self.reserved_names.contains(name)
				|| self.expressions.values().any(|info| &info.internal_name == name);

			if !conflicts {
				return name.clone();
			}
		}

		// Generate synthetic name
		let name = format!("_e{}", self.counter);
		self.counter += 1;
		name
	}

	/// Get all expressions that need to be computed at a specific point.
	pub fn get_expressions_for_point(
		&self,
		point: ComputePoint,
	) -> Vec<(String, Arc<dyn PhysicalExpr>)> {
		self.expressions
			.values()
			.filter(|info| info.compute_point == point)
			.map(|info| (info.internal_name.clone(), Arc::clone(&info.expr)))
			.collect()
	}

	/// Check if an expression is already registered and return its internal name.
	pub fn get_internal_name(&self, expr: &Expr) -> Option<String> {
		let expr_sql = expr.to_sql();
		self.expressions.get(&expr_sql).map(|info| info.internal_name.clone())
	}

	/// Check if there are any expressions registered for a specific compute point.
	pub fn has_expressions_for_point(&self, point: ComputePoint) -> bool {
		self.expressions.values().any(|info| info.compute_point == point)
	}

	/// Get all expressions that need to be computed at or before a specific point.
	pub fn get_expressions_up_to_point(
		&self,
		point: ComputePoint,
	) -> Vec<(String, Arc<dyn PhysicalExpr>)> {
		self.expressions
			.values()
			.filter(|info| info.compute_point <= point)
			.map(|info| (info.internal_name.clone(), Arc::clone(&info.expr)))
			.collect()
	}

	/// Check if there are any expressions registered at or before a specific point.
	pub fn has_expressions_up_to_point(&self, point: ComputePoint) -> bool {
		self.expressions.values().any(|info| info.compute_point <= point)
	}

	/// Get the total number of registered expressions.
	pub fn len(&self) -> usize {
		self.expressions.len()
	}

	/// Check if the registry is empty.
	pub fn is_empty(&self) -> bool {
		self.expressions.is_empty()
	}

	/// Mark a field name as reserved (e.g., fields from the scanned record).
	pub fn reserve_name(&mut self, name: String) {
		if !self.reserved_names.contains(&name) {
			self.reserved_names.push(name);
		}
	}

	/// Get all registered expression infos.
	pub fn iter(&self) -> impl Iterator<Item = &ExpressionInfo> {
		self.expressions.values()
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

	#[test]
	fn test_registry_deduplication() {
		let mut registry = ExpressionRegistry::new();

		// Create a mock context (we'll test without actually converting)
		// For this test, we just check the naming logic

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
	fn test_registry_reserved_names() {
		let mut registry =
			ExpressionRegistry::with_reserved_names(vec!["id".into(), "name".into()]);

		// Reserved names should be skipped
		let name1 = registry.choose_internal_name(&Some("id".into()));
		assert_eq!(name1, "_e0"); // Falls back to synthetic

		let name2 = registry.choose_internal_name(&Some("name".into()));
		assert_eq!(name2, "_e1"); // Falls back to synthetic

		let name3 = registry.choose_internal_name(&Some("city".into()));
		assert_eq!(name3, "city"); // Not reserved, uses alias
	}
}
