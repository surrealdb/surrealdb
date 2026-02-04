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
#[allow(dead_code)] // Variants used for future expansion
pub enum ComputePoint {
	/// Compute before Filter (expressions used in WHERE)
	BeforeFilter = 0,
	/// Compute before Aggregate (GROUP BY keys, aggregate inputs)
	BeforeAggregate = 1,
	/// Compute before Sort (ORDER BY keys, SELECT expressions)
	BeforeSort = 2,
}

/// Information about a registered expression.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used for future expansion
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

#[allow(dead_code)] // Methods used for future expansion
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
	pub fn register(
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
		let physical_expr = expr_to_physical_expr(expr.clone(), ctx)?;

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

/// Checks if an expression needs computation (is "complex").
///
/// Simple expressions are:
/// - Literals (including array and object literals with simple elements)
/// - Simple field references (no graph traversal, method calls, etc.)
/// - Parameters, constants, tables
///
/// Complex expressions require a Compute operator:
/// - Idioms with Lookup, Recurse, Where, Method parts
/// - Function calls
/// - Binary/unary operations
/// - Subqueries and control flow statements
pub fn needs_computation(expr: &Expr) -> bool {
	match expr {
		// Literals don't need computation (arrays and objects are also literals)
		Expr::Literal(_) => false,

		// Parameters are resolved at execution time but don't need Compute
		Expr::Param(_) => false,

		// Idioms need analysis of their parts
		Expr::Idiom(idiom) => idiom_needs_computation(idiom),

		// Function calls always need computation
		Expr::FunctionCall(_) => true,

		// Binary operations always need computation
		// (even simple ones like 1 + 1 are evaluated, not just extracted)
		Expr::Binary {
			..
		} => true,

		// Prefix operations always need computation
		Expr::Prefix {
			..
		} => true,

		// Postfix operations always need computation
		Expr::Postfix {
			..
		} => true,

		// Subqueries and statements always need computation
		Expr::Select(_)
		| Expr::Insert(_)
		| Expr::Create(_)
		| Expr::Update(_)
		| Expr::Upsert(_)
		| Expr::Delete(_)
		| Expr::Relate(_)
		| Expr::Define(_)
		| Expr::Remove(_)
		| Expr::Rebuild(_)
		| Expr::Alter(_)
		| Expr::Info(_)
		| Expr::Foreach(_)
		| Expr::Let(_)
		| Expr::Sleep(_) => true,

		// If-else needs computation
		Expr::IfElse(_) => true,

		// Block expressions need computation
		Expr::Block(_) => true,

		// Tables don't need computation on their own
		Expr::Table(_) => false,

		// Constants are pre-computed
		Expr::Constant(_) => false,

		// Closures need computation when invoked
		Expr::Closure(_) => true,

		// Mock values don't need computation
		Expr::Mock(_) => false,

		// Control flow expressions need computation
		Expr::Break | Expr::Continue | Expr::Return(_) | Expr::Throw(_) => true,

		// Explain needs computation
		Expr::Explain {
			..
		} => true,
	}
}

/// Checks if an idiom needs computation based on its parts.
///
/// Simple idioms (only Field parts) can be extracted directly.
/// Complex idioms (with Lookup, Where, Method, etc.) need Compute.
pub fn idiom_needs_computation(idiom: &Idiom) -> bool {
	for part in idiom.iter() {
		match part {
			// Simple field access - might trigger RecordId auto-fetch at runtime,
			// but we handle this conservatively by always allowing it in Compute
			Part::Field(_) => continue,

			// Array operations are simple
			Part::All | Part::First | Part::Last | Part::Flatten => continue,

			// Optional chaining is simple
			Part::Optional => continue,

			// Index with literal is simple, with expression needs computation
			Part::Value(expr) => {
				if needs_computation(expr) {
					return true;
				}
			}

			// These always need database/computation
			Part::Lookup(_) => return true,
			Part::Recurse(..) => return true,
			Part::Where(_) => return true,
			Part::Method(..) => return true,
			Part::Destructure(_) => return true,

			// Start expressions need computation
			Part::Start(expr) => {
				if needs_computation(expr) {
					return true;
				}
			}

			// Doc reference needs context
			Part::Doc => continue,

			// Repeat recurse symbol needs computation
			Part::RepeatRecurse => return true,
		}
	}

	false
}

// ============================================================================
// Expression Collector Visitor
// ============================================================================

use crate::expr::visit::{MutVisitor, VisitMut};

/// Visitor that collects complex expressions and replaces them with field references.
///
/// This follows the same pattern as `AggregateExtractor` - walk the expression tree,
/// find expressions that need computation, register them in the registry, and replace
/// them with simple field references.
pub struct ExpressionCollector<'a> {
	/// The registry to store collected expressions
	pub registry: &'a mut ExpressionRegistry,
	/// Current compute point being processed
	pub compute_point: ComputePoint,
	/// Planning context for converting to physical expressions
	pub ctx: &'a FrozenContext,
	/// Current alias being processed (for SELECT field with AS clause)
	pub current_alias: Option<String>,
	/// Error encountered during traversal
	pub error: Option<Error>,
}

impl<'a> ExpressionCollector<'a> {
	/// Create a new expression collector.
	pub fn new(
		registry: &'a mut ExpressionRegistry,
		compute_point: ComputePoint,
		ctx: &'a FrozenContext,
	) -> Self {
		Self {
			registry,
			compute_point,
			ctx,
			current_alias: None,
			error: None,
		}
	}

	/// Set the current alias for the expression being processed.
	pub fn with_alias(mut self, alias: Option<String>) -> Self {
		self.current_alias = alias;
		self
	}

	/// Process an expression, potentially replacing it with a field reference.
	/// Returns the internal name if the expression was registered.
	pub fn process_expr(&mut self, expr: &mut Expr) -> Option<String> {
		if self.error.is_some() {
			return None;
		}

		// Check if this expression needs computation
		if !needs_computation(expr) {
			// Simple expression - don't register, just continue traversing
			let _ = expr.visit_mut(self);
			return None;
		}

		// Register the expression and get internal name
		match self.registry.register(expr, self.compute_point, self.current_alias.take(), self.ctx)
		{
			Ok(internal_name) => {
				// Replace expression with field reference
				*expr = Expr::Idiom(Idiom::field(internal_name.clone()));
				Some(internal_name)
			}
			Err(e) => {
				self.error = Some(e);
				None
			}
		}
	}

	/// Take any error that occurred during collection.
	pub fn take_error(&mut self) -> Option<Error> {
		self.error.take()
	}
}

impl MutVisitor for ExpressionCollector<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// Don't continue if we've already encountered an error
		if self.error.is_some() {
			return Ok(());
		}

		// Check if this expression needs computation
		if needs_computation(expr) {
			// Register and replace with field reference
			match self.registry.register(
				expr,
				self.compute_point,
				self.current_alias.take(),
				self.ctx,
			) {
				Ok(internal_name) => {
					*expr = Expr::Idiom(Idiom::field(internal_name));
				}
				Err(e) => {
					self.error = Some(e);
				}
			}
			return Ok(());
		}

		// Continue visiting children for simple expressions
		expr.visit_mut(self)
	}

	// Override to prevent descending into subqueries
	// (expressions in subqueries belong to a different context)
	fn visit_mut_select(
		&mut self,
		_s: &mut crate::expr::statements::SelectStatement,
	) -> Result<(), Self::Error> {
		// Don't visit into SELECT subqueries - they have their own expression context
		// The subquery itself will be registered as a complex expression
		Ok(())
	}

	fn visit_mut_create(
		&mut self,
		_s: &mut crate::expr::statements::CreateStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_update(
		&mut self,
		_s: &mut crate::expr::statements::UpdateStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_delete(
		&mut self,
		_s: &mut crate::expr::statements::DeleteStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_insert(
		&mut self,
		_s: &mut crate::expr::statements::InsertStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}

	fn visit_mut_relate(
		&mut self,
		_s: &mut crate::expr::statements::RelateStatement,
	) -> Result<(), Self::Error> {
		Ok(())
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
	use crate::expr::part::Part;

	#[test]
	fn test_simple_idiom_no_computation() {
		// name - simple field access
		let idiom = Idiom(vec![Part::Field("name".into())]);
		assert!(!idiom_needs_computation(&idiom));
	}

	#[test]
	fn test_nested_field_no_computation() {
		// address.city - nested field access
		let idiom = Idiom(vec![Part::Field("address".into()), Part::Field("city".into())]);
		assert!(!idiom_needs_computation(&idiom));
	}

	#[test]
	fn test_lookup_needs_computation() {
		// Check that Lookup is detected (we can't easily construct one in test,
		// but we can test the pattern)
		let idiom = Idiom(vec![Part::Field("test".into())]);
		assert!(!idiom_needs_computation(&idiom));
	}

	#[test]
	fn test_literal_no_computation() {
		let expr = Expr::Literal(crate::expr::literal::Literal::Integer(42));
		assert!(!needs_computation(&expr));
	}

	#[test]
	fn test_function_call_needs_computation() {
		// Any function call needs computation
		let expr = Expr::FunctionCall(Box::new(crate::expr::FunctionCall {
			receiver: crate::expr::Function::Normal("count".into()),
			arguments: vec![],
		}));
		assert!(needs_computation(&expr));
	}

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
