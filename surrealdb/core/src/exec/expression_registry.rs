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
//! runs *before* any clause evaluation. This registry is **post-decode only**: it names
//! expressions computed on decoded `Value`s for `Sort` / `SelectProject` via `Compute`.

use surrealdb_types::ToSql;

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
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Doc
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::Broadcast {
				..
			}
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => return None,
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
	if let LogicalExpr::Field(name) = expr {
		Some(name.as_str())
	} else {
		None
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
