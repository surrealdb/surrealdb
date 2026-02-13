//! Pure utility functions for the planner.
//!
//! These functions have no dependency on `Planner` or `FrozenContext` and perform
//! static conversions, validation, or predicate checks.

use crate::catalog::Distance;
use crate::err::Error;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::expr::field::{Field, Fields};
use crate::expr::operator::NearestNeighbor;
use crate::expr::{BinaryOperator, Cond, Expr, Idiom, Literal};
use crate::val::Number;

// ============================================================================
// Literal / Value Conversion
// ============================================================================

/// Convert a `Literal` to a `Value` for static (non-computed) cases.
///
/// Note: `Literal::RecordId` is handled directly in `Planner::physical_expr()`
/// via `RecordIdExpr`, so it should never reach this function. Array, Object,
/// and Set literals are similarly handled upstream by the planner.
pub(super) fn literal_to_value(
	lit: crate::expr::literal::Literal,
) -> Result<crate::val::Value, Error> {
	use crate::expr::literal::Literal;
	use crate::val::{Number, Range, Value};

	match lit {
		Literal::None => Ok(Value::None),
		Literal::Null => Ok(Value::Null),
		Literal::UnboundedRange => Ok(Value::Range(Box::new(Range::unbounded()))),
		Literal::Bool(x) => Ok(Value::Bool(x)),
		Literal::Float(x) => Ok(Value::Number(Number::Float(x))),
		Literal::Integer(i) => Ok(Value::Number(Number::Int(i))),
		Literal::Decimal(d) => Ok(Value::Number(Number::Decimal(d))),
		Literal::String(s) => Ok(Value::String(s)),
		Literal::Bytes(b) => Ok(Value::Bytes(b)),
		Literal::Regex(r) => Ok(Value::Regex(r)),
		Literal::Duration(d) => Ok(Value::Duration(d)),
		Literal::Datetime(dt) => Ok(Value::Datetime(dt)),
		Literal::Uuid(u) => Ok(Value::Uuid(u)),
		Literal::Geometry(g) => Ok(Value::Geometry(g)),
		Literal::File(f) => Ok(Value::File(f)),
		// RecordId is handled by RecordIdExpr in physical_expr() before reaching here.
		Literal::RecordId(_) => Err(Error::PlannerUnimplemented(
			"Literal::RecordId should be handled by RecordIdExpr in physical_expr()".to_string(),
		)),
		Literal::Array(_) => Err(Error::PlannerUnimplemented(
			"Array literals in USE statements not yet supported".to_string(),
		)),
		Literal::Set(_) => Err(Error::PlannerUnimplemented(
			"Set literals in USE statements not yet supported".to_string(),
		)),
		Literal::Object(_) => Err(Error::PlannerUnimplemented(
			"Object literals in USE statements not yet supported".to_string(),
		)),
	}
}

/// Convert a `RecordIdKeyLit` to an `Expr`.
pub(super) fn key_lit_to_expr(lit: &crate::expr::RecordIdKeyLit) -> Result<Expr, Error> {
	use crate::expr::RecordIdKeyLit;
	match lit {
		RecordIdKeyLit::Number(n) => Ok(Expr::Literal(crate::expr::literal::Literal::Integer(*n))),
		RecordIdKeyLit::String(s) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::String(s.clone())))
		}
		RecordIdKeyLit::Uuid(u) => Ok(Expr::Literal(crate::expr::literal::Literal::Uuid(*u))),
		RecordIdKeyLit::Array(exprs) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::Array(exprs.clone())))
		}
		RecordIdKeyLit::Object(entries) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::Object(entries.clone())))
		}
		RecordIdKeyLit::Generate(_) | RecordIdKeyLit::Range(_) => Err(Error::PlannerUnimplemented(
			"Generated/range keys in graph range bounds".to_string(),
		)),
	}
}

// ============================================================================
// Predicate / Validation Helpers
// ============================================================================

/// Parameters extracted from a brute-force KNN expression.
pub(super) struct BruteForceKnnParams {
	/// The idiom path to the vector field.
	pub field: Idiom,
	/// The query vector.
	pub vector: Vec<Number>,
	/// Number of nearest neighbors.
	pub k: u32,
	/// Distance metric.
	pub distance: Distance,
}

/// Extract brute-force KNN parameters from a WHERE clause.
///
/// Returns the parameters if a `NearestNeighbor::K(k, dist)` expression is
/// found at the top level of AND-connected conditions.
pub(super) fn extract_bruteforce_knn(cond: &Cond) -> Option<BruteForceKnnParams> {
	extract_bruteforce_knn_from_expr(&cond.0).map(|(params, _residual)| params)
}

/// Strip any KNN operator from a WHERE clause, returning the residual condition.
///
/// Both `NearestNeighbor::Approximate` (HNSW) and `NearestNeighbor::K` (brute-force)
/// KNN operators cannot be evaluated as boolean predicates. They are handled by
/// dedicated scan operators (`KnnScan`, `KnnTopK`) and must be removed from the
/// filter predicate to avoid evaluation errors.
pub(super) fn strip_knn_from_condition(cond: &Cond) -> Option<Cond> {
	strip_knn_from_expr(&cond.0).map(Cond)
}

/// Recursively strip KNN operator expressions from an expression tree.
///
/// Returns the expression with KNN operators removed, or `None` if the entire
/// expression was a KNN operator (nothing left).
fn strip_knn_from_expr(expr: &Expr) -> Option<Expr> {
	match expr {
		// Any NearestNeighbor operator -- strip it entirely
		Expr::Binary {
			op: BinaryOperator::NearestNeighbor(_),
			..
		} => None,
		// AND: strip from either side, keeping the other
		Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => {
			let stripped_left = strip_knn_from_expr(left);
			let stripped_right = strip_knn_from_expr(right);
			match (stripped_left, stripped_right) {
				(Some(l), Some(r)) => Some(Expr::Binary {
					left: Box::new(l),
					op: BinaryOperator::And,
					right: Box::new(r),
				}),
				(Some(l), None) => Some(l),
				(None, Some(r)) => Some(r),
				(None, None) => None,
			}
		}
		// No KNN found at this level -- keep unchanged
		_ => Some(expr.clone()),
	}
}

/// Recursively extract brute-force KNN from an expression tree.
///
/// Returns `(params, residual_expr)` where `residual_expr` is the remaining
/// condition after removing the KNN predicate.
fn extract_bruteforce_knn_from_expr(expr: &Expr) -> Option<(BruteForceKnnParams, Option<Expr>)> {
	match expr {
		Expr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(nn),
			right,
		} => {
			let NearestNeighbor::K(k, dist) = &**nn else {
				return None;
			};

			// Extract idiom from left side
			let idiom = match left.as_ref() {
				Expr::Idiom(idiom) => idiom.clone(),
				_ => return None,
			};

			// Extract numeric vector from right side
			let vector = extract_literal_vector(right)?;

			Some((
				BruteForceKnnParams {
					field: idiom,
					vector,
					k: *k,
					distance: dist.clone(),
				},
				None, // No residual -- the entire expression was the KNN predicate
			))
		}
		Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => {
			// Try the left side first
			if let Some((params, residual_left)) = extract_bruteforce_knn_from_expr(left) {
				let residual = match residual_left {
					Some(rl) => Some(Expr::Binary {
						left: Box::new(rl),
						op: BinaryOperator::And,
						right: right.clone(),
					}),
					None => Some(right.as_ref().clone()),
				};
				return Some((params, residual));
			}
			// Try the right side
			if let Some((params, residual_right)) = extract_bruteforce_knn_from_expr(right) {
				let residual = match residual_right {
					Some(rr) => Some(Expr::Binary {
						left: left.clone(),
						op: BinaryOperator::And,
						right: Box::new(rr),
					}),
					None => Some(left.as_ref().clone()),
				};
				return Some((params, residual));
			}
			None
		}
		_ => None,
	}
}

/// Extract a `Vec<Number>` from a literal array expression.
fn extract_literal_vector(expr: &Expr) -> Option<Vec<Number>> {
	match expr {
		Expr::Literal(lit) => {
			if let Literal::Array(arr) = lit {
				let mut nums = Vec::with_capacity(arr.len());
				for elem in arr.iter() {
					match elem {
						Expr::Literal(Literal::Integer(i)) => {
							nums.push(Number::Int(*i));
						}
						Expr::Literal(Literal::Float(f)) => {
							nums.push(Number::Float(*f));
						}
						Expr::Literal(Literal::Decimal(d)) => {
							nums.push(Number::Decimal(*d));
						}
						_ => return None,
					}
				}
				Some(nums)
			} else {
				None
			}
		}
		_ => None,
	}
}

/// Check if a source expression represents a "value source" (array, primitive).
pub(super) fn is_value_source_expr(expr: &Expr) -> bool {
	match expr {
		Expr::Literal(Literal::Array(_)) => true,
		Expr::Literal(Literal::String(_))
		| Expr::Literal(Literal::Integer(_))
		| Expr::Literal(Literal::Float(_))
		| Expr::Literal(Literal::Decimal(_))
		| Expr::Literal(Literal::Bool(_))
		| Expr::Literal(Literal::None)
		| Expr::Literal(Literal::Null) => true,
		Expr::Table(_) => false,
		Expr::Literal(Literal::RecordId(_)) => false,
		Expr::Param(_) => false,
		Expr::Select(_) => false,
		_ => false,
	}
}

/// Check if ALL source expressions are value sources.
pub(super) fn all_value_sources(sources: &[Expr]) -> bool {
	!sources.is_empty() && sources.iter().all(is_value_source_expr)
}

// ============================================================================
// MATCHES Context Extraction
// ============================================================================

/// Extract MATCHES clause information from a WHERE condition for index functions.
pub(super) fn extract_matches_context(cond: &Cond) -> crate::exec::function::MatchesContext {
	let mut ctx = crate::exec::function::MatchesContext::new();
	collect_matches(&cond.0, &mut ctx);
	ctx
}

fn collect_matches(expr: &Expr, ctx: &mut crate::exec::function::MatchesContext) {
	match expr {
		Expr::Binary {
			left,
			op: BinaryOperator::Matches(matches_op),
			right,
		} => {
			if let Expr::Idiom(idiom) = left.as_ref() {
				let query = match right.as_ref() {
					Expr::Literal(Literal::String(s)) => s.clone(),
					_ => return,
				};
				let match_ref = matches_op.rf.unwrap_or(0);
				ctx.insert(
					match_ref,
					crate::exec::function::MatchInfo {
						idiom: idiom.clone(),
						query,
					},
				);
			}
		}
		Expr::Binary {
			left,
			right,
			..
		} => {
			collect_matches(left, ctx);
			collect_matches(right, ctx);
		}
		Expr::Prefix {
			expr: inner,
			..
		} => collect_matches(inner, ctx),
		_ => {}
	}
}

/// Try to extract the primary table name from the frozen context.
pub(super) fn extract_table_from_context(ctx: &crate::ctx::FrozenContext) -> crate::val::TableName {
	if let Some(mc) = ctx.get_matches_context()
		&& let Some(table) = mc.table()
	{
		return table.clone();
	}
	crate::val::TableName::from("unknown".to_string())
}

// ============================================================================
// VERSION Extraction
// ============================================================================

/// Extract version expression from VERSION clause.
///
/// Returns a physical expression that, when evaluated at execution time,
/// produces the version timestamp (u64).
pub(super) fn extract_version(
	version_expr: Expr,
	planner: &super::Planner,
) -> Result<Option<std::sync::Arc<dyn crate::exec::PhysicalExpr>>, Error> {
	match version_expr {
		Expr::Literal(Literal::None) => Ok(None),
		_ => {
			let expr = planner.physical_expr(version_expr)?;
			Ok(Some(expr))
		}
	}
}

// ============================================================================
// GROUP BY Validation
// ============================================================================

/// Check if fields contain `$this` or `$parent` parameters (invalid in GROUP BY).
pub(super) fn check_forbidden_group_by_params(fields: &Fields) -> Result<(), Error> {
	match fields {
		Fields::Value(selector) => check_expr_for_forbidden_params(&selector.expr),
		Fields::Select(field_list) => {
			for field in field_list {
				match field {
					Field::All => {}
					Field::Single(selector) => {
						check_expr_for_forbidden_params(&selector.expr)?;
					}
				}
			}
			Ok(())
		}
	}
}

fn check_expr_for_forbidden_params(expr: &Expr) -> Result<(), Error> {
	match expr {
		Expr::Param(param) => {
			let name = param.as_str();
			if name == "this" || name == "self" {
				return Err(Error::Query {
					message: "Found a `$this` parameter refering to the document of a group by select statement\nSelect statements with a group by currently have no defined document to refer to".to_string(),
				});
			}
			if name == "parent" {
				return Err(Error::Query {
					message: "Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to".to_string(),
				});
			}
			Ok(())
		}
		Expr::Binary {
			left,
			right,
			..
		} => {
			check_expr_for_forbidden_params(left)?;
			check_expr_for_forbidden_params(right)
		}
		Expr::Prefix {
			expr,
			..
		} => check_expr_for_forbidden_params(expr),
		Expr::Postfix {
			expr,
			..
		} => check_expr_for_forbidden_params(expr),
		Expr::FunctionCall(fc) => {
			for arg in &fc.arguments {
				check_expr_for_forbidden_params(arg)?;
			}
			Ok(())
		}
		Expr::Literal(Literal::Array(elements)) => {
			for elem in elements {
				check_expr_for_forbidden_params(elem)?;
			}
			Ok(())
		}
		Expr::Literal(Literal::Object(entries)) => {
			for entry in entries {
				check_expr_for_forbidden_params(&entry.value)?;
			}
			Ok(())
		}
		Expr::Select(select) => match &select.fields {
			Fields::Value(selector) => check_expr_for_forbidden_params(&selector.expr),
			Fields::Select(field_list) => {
				for field in field_list {
					if let Field::Single(selector) = field {
						check_expr_for_forbidden_params(&selector.expr)?;
					}
				}
				Ok(())
			}
		},
		Expr::Block(block) => {
			for stmt in &block.0 {
				check_expr_for_forbidden_params(stmt)?;
			}
			Ok(())
		}
		Expr::IfElse(ifelse) => {
			for (cond, body) in &ifelse.exprs {
				check_expr_for_forbidden_params(cond)?;
				check_expr_for_forbidden_params(body)?;
			}
			if let Some(close) = &ifelse.close {
				check_expr_for_forbidden_params(close)?;
			}
			Ok(())
		}
		Expr::Closure(closure) => check_expr_for_forbidden_params(&closure.body),
		Expr::Idiom(idiom) => {
			for part in &idiom.0 {
				match part {
					crate::expr::Part::Start(expr)
					| crate::expr::Part::Where(expr)
					| crate::expr::Part::Value(expr) => {
						check_expr_for_forbidden_params(expr)?;
					}
					crate::expr::Part::Method(_, args) => {
						for arg in args {
							check_expr_for_forbidden_params(arg)?;
						}
					}
					_ => {}
				}
			}
			Ok(())
		}
		Expr::Literal(_) | Expr::Constant(_) | Expr::Table(_) | Expr::Break | Expr::Continue => {
			Ok(())
		}
		_ => Ok(()),
	}
}

// ============================================================================
// Pushdown Eligibility
// ============================================================================

/// Check if ORDER BY is compatible with the natural KV scan direction.
///
/// Returns `true` when ORDER BY is absent, or is exactly `id ASC` or `id DESC`
/// with no COLLATE/NUMERIC modifiers. In these cases the scan already produces
/// rows in the requested order and no separate Sort operator is needed.
pub(super) fn order_is_scan_compatible(order: &Option<crate::expr::order::Ordering>) -> bool {
	use crate::expr::order::Ordering;
	match order {
		None => true,
		Some(Ordering::Random) => false,
		Some(Ordering::Order(list)) => {
			list.0.len() == 1 && list.0[0].value.is_id() && !list.0[0].collate && !list.0[0].numeric
		}
	}
}

/// Check if LIMIT/START can be pushed down into the Scan operator.
///
/// This is safe when no pipeline operator between Scan and Limit changes
/// row cardinality. Note that WHERE does NOT block pushdown because the
/// filter predicate is also pushed into Scan.
pub(super) fn can_push_limit_to_scan(
	split: &Option<crate::expr::split::Splits>,
	group: &Option<crate::expr::group::Groups>,
	order: &Option<crate::expr::order::Ordering>,
) -> bool {
	if split.is_some() || group.is_some() {
		return false;
	}
	order_is_scan_compatible(order)
}

// ============================================================================
// LIMIT Helpers
// ============================================================================

/// Try to get the effective limit (start + limit) if both are literals.
pub(super) fn get_effective_limit_literal(
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
) -> Option<usize> {
	let limit_val = limit.as_ref().and_then(|l| match &l.0 {
		Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
		Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
		_ => None,
	})?;

	let start_val = start
		.as_ref()
		.map(|s| match &s.0 {
			Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
			Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
			_ => None,
		})
		.unwrap_or(Some(0))?;

	Some(start_val + limit_val)
}

// ============================================================================
// Field Name Derivation
// ============================================================================

/// Derive a field name from an expression for projection output.
pub(super) fn derive_field_name(expr: &Expr) -> String {
	match expr {
		Expr::Idiom(idiom) => idiom_to_field_name(idiom),
		Expr::Param(param) => param.as_str().to_string(),
		Expr::FunctionCall(call) => {
			let idiom: crate::expr::idiom::Idiom = call.receiver.to_idiom();
			idiom_to_field_name(&idiom)
		}
		_ => {
			use surrealdb_types::ToSql;
			expr.to_sql()
		}
	}
}

/// Extract a field name from an idiom.
pub(super) fn idiom_to_field_name(idiom: &crate::expr::idiom::Idiom) -> String {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& let Some(alias) = &lookup.alias
		{
			return idiom_to_field_name(alias);
		}
	}

	let simplified = idiom.simplify();

	if simplified.len() == 1
		&& let Some(Part::Field(name)) = simplified.first()
	{
		return name.clone();
	}
	simplified.to_sql()
}

/// Extract a field path from an idiom for nested output construction.
pub(super) fn idiom_to_field_path(idiom: &crate::expr::idiom::Idiom) -> FieldPath {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& lookup.alias.is_some()
		{
			return FieldPath::field(idiom_to_field_name(idiom));
		}
	}

	let has_lookups = idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)));

	if !has_lookups {
		let name = idiom_to_field_name(idiom);
		if name.contains('.') && !name.contains(['[', '(', ' ']) {
			return FieldPath(
				name.split('.').map(|s| FieldPathPart::Field(s.to_string())).collect(),
			);
		}
		return FieldPath::field(name);
	}

	let mut parts = Vec::new();
	for part in idiom.0.iter() {
		match part {
			Part::Lookup(lookup) => {
				let lookup_key = lookup.to_sql();
				parts.push(FieldPathPart::Lookup(lookup_key));
			}
			Part::Field(name) => {
				parts.push(FieldPathPart::Field(name.clone()));
			}
			_ => {}
		}
	}

	if parts.is_empty() {
		return FieldPath::field(idiom.to_sql());
	}

	FieldPath(parts)
}

// ============================================================================
// COUNT() Fast-Path Detection
// ============================================================================

/// Check if a SELECT statement is eligible for the CountScan optimisation.
///
/// Returns `true` when the query matches:
///   `SELECT count() FROM <single-table> WHERE <cond> GROUP ALL`
/// with a WHERE clause and no SPLIT, ORDER BY, FETCH, or OMIT clauses.
///
/// When this returns `true`, the `IndexCountScan` operator can be used.
/// At execution time it will look up the table's indexes and, if a COUNT
/// index with a matching condition exists, sum delta counts instead of
/// scanning all records.
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)] // Ready for use when plan-time index detection is added.
pub(super) fn is_indexed_count_eligible(
	fields: &Fields,
	group: &Option<crate::expr::group::Groups>,
	cond: &Option<crate::expr::cond::Cond>,
	split: &Option<crate::expr::split::Splits>,
	order: &Option<crate::expr::order::Ordering>,
	fetch: &Option<crate::expr::fetch::Fetchs>,
	omit: &[Expr],
	what: &[Expr],
) -> bool {
	// Must be count()-only fields.
	if !fields.is_count_all_only() {
		return false;
	}
	// Must have GROUP ALL.
	let Some(groups) = group else {
		return false;
	};
	if !groups.is_group_all_only() {
		return false;
	}
	// Must have a WHERE clause (the no-WHERE case is handled by `is_count_all_eligible`).
	if cond.is_none() {
		return false;
	}
	// No SPLIT, ORDER BY, FETCH, or OMIT.
	if split.is_some() || order.is_some() || fetch.is_some() || !omit.is_empty() {
		return false;
	}
	// Source must be a single table (no record-id ranges for indexed counts).
	if what.len() != 1 {
		return false;
	}
	matches!(&what[0], Expr::Table(_) | Expr::Param(_))
}

/// Returns `true` when the query matches:
///   `SELECT count() FROM <single-table-or-range> GROUP ALL`
/// with no WHERE, SPLIT, ORDER BY, FETCH, or OMIT clauses.
///
/// The CountScan operator replaces the entire Scan -> Aggregate -> Project
/// pipeline with a single `txn.count()` call on the KV key range.
#[allow(clippy::too_many_arguments)]
pub(super) fn is_count_all_eligible(
	fields: &Fields,
	group: &Option<crate::expr::group::Groups>,
	cond: &Option<crate::expr::cond::Cond>,
	split: &Option<crate::expr::split::Splits>,
	order: &Option<crate::expr::order::Ordering>,
	fetch: &Option<crate::expr::fetch::Fetchs>,
	omit: &[Expr],
	what: &[Expr],
) -> bool {
	// Must be count()-only fields (no arguments, no other fields).
	if !fields.is_count_all_only() {
		return false;
	}
	// Must have GROUP ALL (explicit `GROUP ALL` in the AST = Some(Groups(vec![]))).
	let Some(groups) = group else {
		return false;
	};
	if !groups.is_group_all_only() {
		return false;
	}
	// No WHERE clause (index-accelerated WHERE is a follow-up).
	if cond.is_some() {
		return false;
	}
	// No SPLIT, ORDER BY, FETCH, or OMIT.
	if split.is_some() || order.is_some() || fetch.is_some() || !omit.is_empty() {
		return false;
	}
	// Source must be a single table, record-id, or param (resolving to table).
	if what.len() != 1 {
		return false;
	}
	matches!(
		&what[0],
		Expr::Table(_)
			| Expr::Literal(crate::expr::literal::Literal::RecordId(_))
			| Expr::Param(_)
			| Expr::Postfix { .. }
	)
}

/// Extract the output field names for a CountScan fast-path query.
///
/// For each `count()` field in the SELECT list, this returns the alias name
/// (if `AS alias` is present) or the default derived name (`"count"`).
///
/// Pre-condition: `is_count_all_eligible` returned `true`, so every field is
/// a `count()` function call.
pub(super) fn extract_count_field_names(fields: &Fields) -> Vec<String> {
	match fields {
		Fields::Value(selector) => {
			if let Some(alias) = &selector.alias {
				vec![idiom_to_field_name(alias)]
			} else {
				vec![derive_field_name(&selector.expr)]
			}
		}
		Fields::Select(field_list) => field_list
			.iter()
			.filter_map(|f| match f {
				Field::Single(selector) => {
					if let Some(alias) = &selector.alias {
						Some(idiom_to_field_name(alias))
					} else {
						Some(derive_field_name(&selector.expr))
					}
				}
				_ => None,
			})
			.collect(),
	}
}
