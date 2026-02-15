//! Pure utility functions for the planner.
//!
//! These functions have no dependency on `Planner` or `FrozenContext` and perform
//! static conversions, validation, or predicate checks.

use crate::catalog::Distance;
use crate::err::Error;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::expr::field::{Field, Fields};
use crate::expr::operator::NearestNeighbor;
use crate::expr::visit::{MutVisitor, Visit, VisitMut, Visitor};
use crate::expr::{BinaryOperator, Cond, Expr, Idiom, Literal, Param};
use crate::val::Number;

// ============================================================================
// Literal / Value Conversion
// ============================================================================

/// Best-effort conversion of a `Literal` to a `Value`.
///
/// Handles all scalar types, simple record IDs (Number/String/Uuid keys), and
/// arrays of convertible expressions. Returns `None` for types that require
/// async computation or are otherwise unsupported (Object, Set, Generate keys,
/// Range keys, etc.).
///
/// Used by both the planner (for physical expression compilation) and the index
/// analyzer (for index matching).
pub(crate) fn try_literal_to_value(
	lit: &crate::expr::literal::Literal,
) -> Option<crate::val::Value> {
	use crate::expr::literal::Literal;
	use crate::val::Value;

	match lit {
		Literal::None => Some(Value::None),
		Literal::Null => Some(Value::Null),
		Literal::Bool(x) => Some(Value::Bool(*x)),
		Literal::Float(x) => Some(Value::Number(Number::Float(*x))),
		Literal::Integer(i) => Some(Value::Number(Number::Int(*i))),
		Literal::Decimal(d) => Some(Value::Number(Number::Decimal(*d))),
		Literal::String(s) => Some(Value::String(s.clone())),
		Literal::Uuid(u) => Some(Value::Uuid(*u)),
		Literal::Datetime(dt) => Some(Value::Datetime(dt.clone())),
		Literal::Duration(d) => Some(Value::Duration(*d)),
		Literal::RecordId(rid) => {
			// Convert simple record ID literals (Number, String, Uuid keys).
			// Complex keys (Array, Object, Generate, Range) may contain
			// expressions requiring async computation and are skipped.
			use crate::expr::RecordIdKeyLit;
			let key = match &rid.key {
				RecordIdKeyLit::Number(n) => crate::val::RecordIdKey::Number(*n),
				RecordIdKeyLit::String(s) => crate::val::RecordIdKey::String(s.clone()),
				RecordIdKeyLit::Uuid(u) => crate::val::RecordIdKey::Uuid(*u),
				_ => return None,
			};
			Some(Value::RecordId(crate::val::RecordId::new(rid.table.clone(), key)))
		}
		Literal::Array(arr) => {
			let values: Option<Vec<Value>> = arr.iter().map(try_expr_to_value).collect();
			values.map(|v| Value::Array(v.into()))
		}
		// Types that cannot be converted without async or are unsupported
		Literal::Bytes(_)
		| Literal::Regex(_)
		| Literal::Geometry(_)
		| Literal::File(_)
		| Literal::Object(_)
		| Literal::Set(_)
		| Literal::UnboundedRange => None,
	}
}

/// Try to convert an expression to a constant value.
pub(crate) fn try_expr_to_value(expr: &Expr) -> Option<crate::val::Value> {
	match expr {
		Expr::Literal(lit) => try_literal_to_value(lit),
		_ => None,
	}
}

/// Convert a `Literal` to a `Value` for static (non-computed) cases.
///
/// Delegates to [`try_literal_to_value`] for common types, then handles
/// planner-specific types (UnboundedRange, Bytes, Regex, Geometry, File).
/// Returns `Error::Internal` for types that should have been handled upstream
/// by `physical_expr()` (RecordId, Array, Object, Set).
pub(super) fn literal_to_value(
	lit: crate::expr::literal::Literal,
) -> Result<crate::val::Value, Error> {
	use crate::expr::literal::Literal;
	use crate::val::{Range, Value};

	// Try the shared conversion first (handles scalars, simple RecordIds, arrays)
	if let Some(value) = try_literal_to_value(&lit) {
		return Ok(value);
	}

	// Handle types that try_literal_to_value doesn't cover but are valid here
	match lit {
		Literal::UnboundedRange => Ok(Value::Range(Box::new(Range::unbounded()))),
		Literal::Bytes(b) => Ok(Value::Bytes(b)),
		Literal::Regex(r) => Ok(Value::Regex(r)),
		Literal::Geometry(g) => Ok(Value::Geometry(g)),
		Literal::File(f) => Ok(Value::File(f)),
		// Everything else should be handled upstream in physical_expr()
		other => Err(Error::Internal(format!(
			"Literal should be handled upstream in physical_expr(): {:?}",
			std::mem::discriminant(&other)
		))),
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
		RecordIdKeyLit::Generate(_) => Err(Error::Query {
			message: "Generated keys (rand, ulid, uuid) cannot be used in graph range bounds"
				.to_string(),
		}),
		RecordIdKeyLit::Range(_) => Err(Error::Query {
			message: "Nested range keys cannot be used in graph range bounds".to_string(),
		}),
	}
}

// ============================================================================
// Predicate / Validation Helpers
// ============================================================================

/// Check if a condition has a top-level OR operator.
///
/// Used to prevent LIMIT/START pushdown into Scan when the condition may
/// trigger a multi-index union at runtime. Union streams don't maintain
/// a global ordering, so pushing LIMIT would truncate results arbitrarily.
pub(super) fn has_top_level_or(cond: Option<&Cond>) -> bool {
	match cond {
		Some(c) => matches!(
			c.0,
			Expr::Binary {
				op: BinaryOperator::Or,
				..
			}
		),
		None => false,
	}
}

/// Check if an expression contains any KNN (nearest neighbor) operators.
pub(super) fn has_knn_operator(expr: &Expr) -> bool {
	let mut checker = KnnOperatorChecker {
		found_any: false,
		found_k: false,
	};
	let _ = checker.visit_expr(expr);
	checker.found_any
}

/// Check if an expression contains a brute-force KNN operator (`NearestNeighbor::K`).
///
/// Used to distinguish between brute-force KNN with parameter-based vectors
/// (where `extract_bruteforce_knn` fails) and HNSW KNN (`Approximate`).
pub(super) fn has_knn_k_operator(expr: &Expr) -> bool {
	let mut checker = KnnOperatorChecker {
		found_any: false,
		found_k: false,
	};
	let _ = checker.visit_expr(expr);
	checker.found_k
}

/// Visitor that detects the presence of KNN operators in an expression tree.
struct KnnOperatorChecker {
	found_any: bool,
	found_k: bool,
}

impl Visitor for KnnOperatorChecker {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if let Expr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} = expr
		{
			self.found_any = true;
			if matches!(nn.as_ref(), NearestNeighbor::K(..)) {
				self.found_k = true;
			}
		}
		expr.visit(self)
	}

	// Don't descend into subqueries -- only check outer WHERE.
	fn visit_select(&mut self, _: &crate::expr::SelectStatement) -> Result<(), Self::Error> {
		Ok(())
	}
}

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
	let mut expr = cond.0.clone();
	let mut extractor = BruteForceKnnExtractor {
		params: None,
	};
	let _ = extractor.visit_mut_expr(&mut expr);
	extractor.params
}

/// Strip handled KNN operators from a WHERE clause, returning the residual condition.
///
/// Both `NearestNeighbor::K` (consumed by `KnnTopK`) and `NearestNeighbor::Approximate`
/// (consumed by `KnnScan` via HNSW index) are stripped. `KTree` is left in place --
/// the caller should verify the residual contains no remaining KNN operators and
/// return an error if it does.
pub(crate) fn strip_knn_from_condition(cond: &Cond) -> Option<Cond> {
	let mut expr = cond.0.clone();
	let _ = KnnStripper.visit_mut_expr(&mut expr);
	let _ = BoolSimplifier.visit_mut_expr(&mut expr);
	if matches!(expr, Expr::Literal(Literal::Bool(true))) {
		None
	} else {
		Some(Cond(expr))
	}
}

// ---------------------------------------------------------------------------
// MutVisitors for KNN condition rewriting
// ---------------------------------------------------------------------------

/// Replaces handled KNN expressions (`NearestNeighbor::K` and
/// `NearestNeighbor::Approximate`) with `Literal::Bool(true)`.
/// Run `BoolSimplifier` afterwards to collapse the resulting
/// `true AND x` chains.
struct KnnStripper;

impl MutVisitor for KnnStripper {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// Replace handled KNN expressions with `true`.
		if let Expr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} = expr && matches!(
			nn.as_ref(),
			NearestNeighbor::K(..) | NearestNeighbor::Approximate(..)
		) {
			*expr = Expr::Literal(Literal::Bool(true));
			return Ok(());
		}
		// Only recurse into AND chains. KNN operators nested under OR/NOT
		// must be preserved so that `has_knn_operator` can detect and reject
		// those unsupported shapes.
		if let Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			self.visit_mut_expr(left)?;
			self.visit_mut_expr(right)?;
		}
		Ok(())
	}

	// Don't strip KNN inside subqueries -- only top-level WHERE.
	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Extracts a single brute-force KNN (`NearestNeighbor::K`) expression,
/// replacing it with `Literal::Bool(true)`. The extracted parameters are
/// stashed in `params`.
struct BruteForceKnnExtractor {
	params: Option<BruteForceKnnParams>,
}

impl MutVisitor for BruteForceKnnExtractor {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// Already found one -- stop looking.
		if self.params.is_some() {
			return Ok(());
		}
		if let Expr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(nn),
			right,
		} = expr && let NearestNeighbor::K(k, dist) = nn.as_ref()
			&& let Expr::Idiom(idiom) = left.as_ref()
			&& let Some(vector) = extract_literal_vector(right)
		{
			self.params = Some(BruteForceKnnParams {
				field: idiom.clone(),
				vector,
				k: *k,
				distance: dist.clone(),
			});
			*expr = Expr::Literal(Literal::Bool(true));
			return Ok(());
		}
		expr.visit_mut(self)
	}

	// Don't descend into subqueries.
	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Reusable postorder pass that collapses boolean-literal sentinels in AND
/// chains: `true AND x → x`, `x AND true → x`, `true AND true → true`.
///
/// Used after `KnnStripper` / `BruteForceKnnExtractor` to clean up the tree.
struct BoolSimplifier;

impl MutVisitor for BoolSimplifier {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		// Postorder: recurse first, then simplify this node.
		expr.visit_mut(self)?;

		if let Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			let l_true = matches!(left.as_ref(), Expr::Literal(Literal::Bool(true)));
			let r_true = matches!(right.as_ref(), Expr::Literal(Literal::Bool(true)));
			match (l_true, r_true) {
				(true, true) => *expr = Expr::Literal(Literal::Bool(true)),
				(true, false) => {
					let r = std::mem::replace(right.as_mut(), Expr::Literal(Literal::None));
					*expr = r;
				}
				(false, true) => {
					let l = std::mem::replace(left.as_mut(), Expr::Literal(Literal::None));
					*expr = l;
				}
				_ => {}
			}
		}
		Ok(())
	}

	// Don't descend into subqueries.
	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
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

// ============================================================================
// Record ID Point-Lookup Extraction
// ============================================================================

/// Check whether a WHERE condition contains `id = <RecordId literal>` in its
/// top-level AND chain, where the RecordId's table matches the FROM table.
///
/// Returns the `RecordId` literal expression when found, `None` otherwise.
/// Does NOT extract from OR branches (those require a full table scan).
///
/// This enables the planner to convert `SELECT * FROM table WHERE id = table:x`
/// into a direct point lookup (`RecordIdScan`) instead of a full table scan.
///
/// Only matches point-key RecordIds (not range keys like `table:1..5`).
pub(super) fn extract_record_id_point_lookup(
	cond: &Cond,
	table_name: &crate::val::TableName,
) -> Option<Expr> {
	find_id_equality_in_and_chain(&cond.0, table_name)
}

/// Walk the top-level AND chain looking for `id = <RecordId literal>`.
fn find_id_equality_in_and_chain(expr: &Expr, table_name: &crate::val::TableName) -> Option<Expr> {
	match expr {
		// AND: check both branches
		Expr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => find_id_equality_in_and_chain(left, table_name)
			.or_else(|| find_id_equality_in_and_chain(right, table_name)),

		// Equality: check for `id = <RecordId>` or `<RecordId> = id`
		Expr::Binary {
			left,
			op: BinaryOperator::Equal | BinaryOperator::ExactEqual,
			right,
		} => check_id_recordid_pair(left, right, table_name)
			.or_else(|| check_id_recordid_pair(right, left, table_name)),

		// Any other node (OR, comparisons, etc.): no match
		_ => None,
	}
}

/// Check if `idiom_side` is the `id` idiom and `lit_side` is a matching
/// RecordId literal with a non-range key.
fn check_id_recordid_pair(
	idiom_side: &Expr,
	lit_side: &Expr,
	table_name: &crate::val::TableName,
) -> Option<Expr> {
	if let Expr::Idiom(idiom) = idiom_side
		&& idiom.is_id()
		&& let Expr::Literal(Literal::RecordId(rid)) = lit_side
		&& &rid.table == table_name
		&& !matches!(rid.key, crate::expr::RecordIdKeyLit::Range(_))
	{
		Some(lit_side.clone())
	} else {
		None
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
	let mut collector = MatchesCollector(crate::exec::function::MatchesContext::new());
	let _ = collector.visit_expr(&cond.0);
	collector.0
}

/// Visitor that collects MATCHES clause entries from expression trees.
struct MatchesCollector(crate::exec::function::MatchesContext);

impl Visitor for MatchesCollector {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if let Expr::Binary {
			left,
			op: BinaryOperator::Matches(matches_op),
			right,
		} = expr && let Expr::Idiom(idiom) = left.as_ref()
			&& let Expr::Literal(Literal::String(s)) = right.as_ref()
		{
			let match_ref = matches_op.rf.unwrap_or(0);
			self.0.insert(
				match_ref,
				crate::exec::function::MatchInfo {
					idiom: idiom.clone(),
					query: s.clone(),
				},
			);
		}
		expr.visit(self)
	}

	// Don't descend into subqueries -- only collect outer MATCHES.
	fn visit_select(&mut self, _: &crate::expr::SelectStatement) -> Result<(), Self::Error> {
		Ok(())
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
pub(super) async fn extract_version(
	version_expr: Expr,
	planner: &super::Planner<'_>,
) -> Result<Option<std::sync::Arc<dyn crate::exec::PhysicalExpr>>, Error> {
	match version_expr {
		Expr::Literal(Literal::None) => Ok(None),
		_ => {
			let expr = planner.physical_expr(version_expr).await?;
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
	expr.visit(&mut ForbiddenParamChecker)
}

/// Visitor that detects `$this`, `$self`, or `$parent` parameters which are
/// invalid inside GROUP BY projections. The default `visit_expr`
/// implementation handles all the recursion; we only need to inspect params.
struct ForbiddenParamChecker;

impl Visitor for ForbiddenParamChecker {
	type Error = Error;

	fn visit_param(&mut self, param: &Param) -> Result<(), Self::Error> {
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

/// Check if an index + scan direction satisfies the given ORDER BY.
///
/// Builds the same `SortProperty` vector that `IndexScan::output_ordering()`
/// would produce and checks whether it satisfies the ORDER BY requirements.
/// This allows the planner to decide on limit pushdown before the IndexScan
/// operator is created.
pub(super) fn index_covers_ordering(
	index_ref: &crate::exec::index::access_path::IndexRef,
	direction: crate::idx::planner::ScanDirection,
	order: &crate::expr::order::Ordering,
) -> bool {
	use crate::exec::operators::SortDirection;
	use crate::exec::ordering::{OutputOrdering, SortProperty};
	use crate::expr::order::Ordering;

	let Ordering::Order(order_list) = order else {
		return false; // Random ordering can't be satisfied by an index
	};

	// Convert ORDER BY to required SortProperty
	let required: Vec<SortProperty> = order_list
		.iter()
		.filter_map(|field| {
			crate::exec::field_path::FieldPath::try_from(&field.value).ok().map(|path| {
				let direction = if field.direction {
					SortDirection::Asc
				} else {
					SortDirection::Desc
				};
				SortProperty {
					path,
					direction,
					collate: field.collate,
					numeric: field.numeric,
				}
			})
		})
		.collect();

	if required.len() != order_list.len() {
		return false;
	}

	// Build the index ordering (same as IndexScan::output_ordering())
	let dir = match direction {
		crate::idx::planner::ScanDirection::Forward => SortDirection::Asc,
		crate::idx::planner::ScanDirection::Backward => SortDirection::Desc,
	};
	let ix_def = index_ref.definition();
	let cols: Vec<SortProperty> = ix_def
		.cols
		.iter()
		.filter_map(|idiom| {
			crate::exec::field_path::FieldPath::try_from(idiom).ok().map(|path| SortProperty {
				path,
				direction: dir,
				collate: false,
				numeric: false,
			})
		})
		.collect();

	if cols.is_empty() {
		return false;
	}

	OutputOrdering::Sorted(cols).satisfies(&required)
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
