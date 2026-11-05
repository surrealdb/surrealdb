//! WHERE-clause analysis and rewriting over the logical scalar type.
//!
//! Predicate inspection (top-level OR, KNN/FTS detection), the
//! classification of a KNN query vector and brute-force KNN parameter
//! extraction, condition stripping after a scan operator has consumed parts
//! of the predicate, MATCHES context collection for index functions, and the
//! record-id point-lookup detector.
//!
//! Every predicate walk here stops at a [`LogicalExpr::Subquery`]: a nested
//! plan is bound and planned on its own, and its predicates do not contribute
//! to the outer scan's analysis. Whether a KNN query vector reads the row being
//! scanned, which enters nested plans, is the row-scope analysis's to say
//! ([`reads_scanned_row`]). The field reference is any path expression
//! ([`LogicalExpr::is_path`]); the literal side of a comparison is whatever
//! [`try_literal_to_value`] accepts.
//!
//! Index columns come from the catalog as expression-layer idioms. They are
//! compared against logical paths by [`idiom_matches_column`], which is the
//! one place that comparison lives.

use std::convert::Infallible;
use std::ops::{Bound, ControlFlow};

use surrealdb_strand::TableName;
use surrealdb_types::ToSql;

use super::fields::{idiom_is_id, path_steps};
use super::literals::try_bound_operand;
use crate::catalog::{Distance, KeyStructure};
use crate::exec::function::FunctionRegistry;
use crate::exec::index::access_path::BoundOperand;
pub(crate) use crate::exec::index::access_path::KnnQueryVector;
use crate::exec::index::key_range::component_position;
use crate::exec::planner::row_scope::reads_scanned_row;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalRecordIdKey};
use crate::expr::operator::NearestNeighbor;
use crate::expr::{BinaryOperator, Idiom, Part};
use crate::val::{Number, Value};

// ============================================================================
// Predicate / Validation Helpers
// ============================================================================

/// Check if an expression contains any KNN (nearest neighbor) operators,
/// outside closure literals (see [`visit_outside_closures`]).
pub(crate) fn has_knn_operator(expr: &LogicalExpr) -> bool {
	scan_knn_operators(expr).found_any
}

/// How many KNN operators a KNN source would serve (`<|k, DIST|>` and
/// `<|k, EF|>`) an expression contains, outside closure literals.
///
/// A KNN source answers one operator: the planner refuses a condition with
/// more than one rather than serve one and drop the others.
pub(crate) fn count_knn_source_operators(expr: &LogicalExpr) -> usize {
	scan_knn_operators(expr).source_count
}

/// Check if an expression contains a `KTree` KNN operator (`<|k|>` form).
///
/// `KTree` has no index behind it. The planner uses this to tell an
/// unsupported variant apart from a KNN nested under OR or NOT when it emits
/// the user-facing error.
pub(crate) fn has_knn_ktree_operator(expr: &LogicalExpr) -> bool {
	scan_knn_operators(expr).found_ktree
}

/// Which KNN operator variants an expression tree contains.
#[derive(Default)]
struct KnnOperators {
	found_any: bool,
	found_k: bool,
	found_ktree: bool,
	/// The `K` and `Approximate` operators found.
	source_count: usize,
}

fn scan_knn_operators(expr: &LogicalExpr) -> KnnOperators {
	let mut found = KnnOperators::default();
	let _ = visit_outside_closures(expr, &mut |e| {
		if let LogicalExpr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} = e
		{
			found.found_any = true;
			match nn.as_ref() {
				NearestNeighbor::K(..) => {
					found.found_k = true;
					found.source_count += 1;
				}
				NearestNeighbor::KTree(..) => found.found_ktree = true,
				NearestNeighbor::Approximate(..) => found.source_count += 1,
			}
		}
		ControlFlow::<()>::Continue(())
	});
	found
}

/// Walk `expr` in pre-order, as [`LogicalExpr::visit`] does, without
/// entering a closure literal's body. A KNN operator there runs only when the
/// closure is invoked, as an ordinary expression, and is no KNN operator of
/// the condition: no KNN source is planned for it, it is neither stripped nor
/// collected, and it is not refused at plan time.
fn visit_outside_closures<B>(
	expr: &LogicalExpr,
	f: &mut dyn FnMut(&LogicalExpr) -> ControlFlow<B>,
) -> ControlFlow<B> {
	if matches!(expr, LogicalExpr::Closure(_)) {
		return ControlFlow::Continue(());
	}
	f(expr)?;
	let mut flow = ControlFlow::Continue(());
	expr.for_each_child(&mut |child| {
		if flow.is_continue() {
			flow = visit_outside_closures(child, &mut *f);
		}
	});
	flow
}

/// Parameters extracted from a brute-force KNN expression.
pub(crate) struct BruteForceKnnParams {
	/// The path to the vector field, a path expression.
	pub field: LogicalExpr,
	/// The query vector.
	pub vector: KnnQueryVector,
	/// Number of nearest neighbors.
	pub k: u32,
	/// Distance metric.
	pub distance: Distance,
}

/// What a KNN operator's right-hand side is to a KNN source.
pub(crate) enum KnnVectorClass {
	/// The query vector, literal or evaluated once when the source runs.
	Vector(KnnQueryVector),
	/// An expression that reads the row being scanned, which has no value
	/// before a row is read: the planner refuses it.
	ReadsRow,
}

/// Whether this expression is computable with no document context: a
/// literal, an array, a record id, a parameter, a call, or a binary operation
/// over them. Fields, subqueries, closures, prefix expressions and every other
/// node kind are not. A bound of a key range that is one may be evaluated once
/// when the scan opens (`index::key_range`).
pub(crate) fn is_plan_time_computable(expr: &LogicalExpr) -> bool {
	match expr {
		// `Tree::eval_array` fully computes array elements, whatever they are.
		LogicalExpr::Array(_) => true,
		LogicalExpr::Literal(
			LogicalLiteral::Integer(_)
			| LogicalLiteral::I8(_)
			| LogicalLiteral::U8(_)
			| LogicalLiteral::I16(_)
			| LogicalLiteral::U16(_)
			| LogicalLiteral::I32(_)
			| LogicalLiteral::U32(_)
			| LogicalLiteral::U64(_)
			| LogicalLiteral::Bool(_)
			| LogicalLiteral::String(_)
			| LogicalLiteral::Duration(_)
			| LogicalLiteral::Uuid(_)
			| LogicalLiteral::Datetime(_)
			| LogicalLiteral::Date(_)
			| LogicalLiteral::Time(_)
			| LogicalLiteral::Timestamp(_)
			| LogicalLiteral::Interval(_)
			| LogicalLiteral::None
			| LogicalLiteral::Null
			| LogicalLiteral::Decimal(_)
			| LogicalLiteral::Float(_)
			| LogicalLiteral::F16(_)
			| LogicalLiteral::F32(_),
		) => true,
		LogicalExpr::RecordId(_) => true,
		LogicalExpr::Param(_) | LogicalExpr::Call(_) => true,
		LogicalExpr::Binary {
			left,
			right,
			..
		} => is_plan_time_computable(left) && is_plan_time_computable(right),
		LogicalExpr::Literal(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
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
		| LogicalExpr::Lookup {
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
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => false,
	}
}

/// The query vector of a KNN operator's right-hand side, read the same way
/// for brute force and for an ANN index: a literal array of numbers, or any
/// expression that does not [read the scanned row](reads_scanned_row), which
/// the source evaluates once, with no row in scope, before it reads any
/// input, and coerces to `array<number>`; a value that does not coerce, or
/// an evaluation that fails, fails the query.
pub(crate) fn classify_knn_vector(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
) -> KnnVectorClass {
	if let Some(vector) = extract_literal_vector(expr) {
		KnnVectorClass::Vector(KnnQueryVector::Literal(vector.into()))
	} else if reads_scanned_row(expr, registry) {
		KnnVectorClass::ReadsRow
	} else {
		KnnVectorClass::Vector(KnnQueryVector::Deferred(expr.clone()))
	}
}

/// The right-hand side of the first KNN operator in `cond`, in pre-order,
/// that a KNN source would read its query vector from (`<|k,DIST|>` or
/// `<|k,ef|>`) and that [reads the scanned row](reads_scanned_row). Such a
/// KNN has no query vector before a row is read, and the planner refuses it.
pub(crate) fn first_row_dependent_knn_vector(
	cond: &LogicalExpr,
	registry: &FunctionRegistry,
) -> Option<LogicalExpr> {
	let mut found = None;
	let _ = visit_outside_closures(cond, &mut |e| {
		if let LogicalExpr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			right,
			..
		} = e && matches!(nn.as_ref(), NearestNeighbor::K(..) | NearestNeighbor::Approximate(..))
			&& reads_scanned_row(right, registry)
		{
			found = Some(right.as_ref().clone());
			return ControlFlow::Break(());
		}
		ControlFlow::Continue(())
	});
	found
}

/// The right-hand side of every KNN operator in `cond` that a KNN source
/// would read its query vector from (`<|k,DIST|>` or `<|k,ef|>`), in
/// pre-order and outside closure literals.
pub(crate) fn knn_query_vectors(cond: &LogicalExpr) -> Vec<LogicalExpr> {
	let mut found = Vec::new();
	let _ = visit_outside_closures(cond, &mut |e| {
		if let LogicalExpr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			right,
			..
		} = e && matches!(nn.as_ref(), NearestNeighbor::K(..) | NearestNeighbor::Approximate(..))
		{
			found.push(right.as_ref().clone());
		}
		ControlFlow::<()>::Continue(())
	});
	found
}

/// Whether `vector` is one a distance ranks a record against: it holds at
/// least one number, and every number is finite. An empty vector has no
/// dimension a record's vector could share, and a NaN or an infinity makes
/// every distance NaN or infinite, which orders no record before another.
pub(crate) fn is_rankable_query_vector(vector: &[Number]) -> bool {
	!vector.is_empty()
		&& vector.iter().all(|n| match n {
			Number::Float(f) => f.is_finite(),
			Number::F16(f) => f.is_finite(),
			Number::F32(f) => f.is_finite(),
			Number::Int(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_) => true,
		})
}

/// Rewrite the left operand of each KNN operator in `cond` that is a
/// projection builtin naming a literal field path (`type::field('point')`)
/// into that path, which a KNN source reads its vectors from: the operand
/// denotes the same field either way, and only a path names one an index or a
/// brute-force ranking can serve.
pub(crate) fn resolve_knn_field_operands(cond: &mut LogicalExpr, registry: &FunctionRegistry) {
	let Ok(()) = cond.visit_mut(&mut |e| {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(_),
			..
		} = e && !left.is_path()
		{
			super::params::resolve_projection_field_idioms(left, registry);
		}
		Ok::<_, Infallible>(())
	});
}

/// Extract brute-force KNN parameters from a WHERE clause.
///
/// Returns the parameters of the first `NearestNeighbor::K(k, dist)`
/// expression, in pre-order and outside closure literals, whose left side is
/// a path and whose right side [is a query vector](classify_knn_vector). A
/// right side that reads the scanned row is refused by the planner (see
/// [`first_row_dependent_knn_vector`]) and is skipped here.
pub(crate) fn extract_bruteforce_knn(
	cond: &LogicalExpr,
	registry: &FunctionRegistry,
) -> Option<BruteForceKnnParams> {
	let mut params = None;
	let _ = visit_outside_closures(cond, &mut |e| {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(nn),
			right,
		} = e && let NearestNeighbor::K(k, dist) = nn.as_ref()
			&& left.is_path()
			&& let KnnVectorClass::Vector(vector) = classify_knn_vector(right, registry)
		{
			params = Some(BruteForceKnnParams {
				field: left.as_ref().clone(),
				vector,
				k: *k,
				distance: dist.clone().into(),
			});
			return ControlFlow::Break(());
		}
		ControlFlow::Continue(())
	});
	params
}

/// Whether `e` is a KNN operator a scan operator consumes: `NearestNeighbor::K`
/// (by `KnnTopK`) or `NearestNeighbor::Approximate` (by `KnnScan` via HNSW).
fn is_handled_knn(e: &LogicalExpr) -> bool {
	matches!(
		e,
		LogicalExpr::Binary {
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} if matches!(nn.as_ref(), NearestNeighbor::K(..) | NearestNeighbor::Approximate(..))
	)
}

/// Strip handled KNN operators from a WHERE clause, returning the residual condition.
///
/// Both `NearestNeighbor::K` (consumed by `KnnTopK`) and `NearestNeighbor::Approximate`
/// (consumed by `KnnScan` via HNSW index) are stripped. `KTree` is left in place --
/// the caller should verify the residual contains no remaining KNN operators and
/// return an error if it does.
pub(crate) fn strip_knn_from_condition(cond: &LogicalExpr) -> Option<LogicalExpr> {
	strip_and_simplify(cond.clone(), is_handled_knn)
}

/// Strip handled KNN operators AND every conjunct containing a MATCHES
/// operator from a WHERE clause, returning the in-traversal residual for a
/// `KnnScan`.
///
/// The residual is evaluated per candidate inside the ANN search, where
/// MATCHES has no full-text index to answer from and computes to `false` —
/// leaving a MATCHES conjunct in would reject every candidate and return zero
/// rows. Dropping a conjunct from the in-traversal residual only
/// admits more candidates (the outer `Filter` re-applies the KNN-stripped
/// WHERE — with `MATCHES` evaluated through its physical operator — so final
/// rows stay correct); it can never produce wrong rows.
///
/// The user-visible trade-off when a MATCHES conjunct is NOT exactly
/// bitmap-coverable (no FULLTEXT index, stale index format, negated MATCHES,
/// ...): non-matching candidates occupy top-K slots inside the search, so
/// the statement may return **fewer than k rows even when k matching rows
/// exist** — the k nearest overall are found first and then filtered, rather
/// than the k nearest *matching* rows. Covered conjuncts (an online,
/// current-format FULLTEXT index) avoid it entirely by joining the allow-list.
pub(crate) fn strip_knn_and_matches_from_condition(cond: &LogicalExpr) -> Option<LogicalExpr> {
	strip_and_simplify(cond.clone(), |e| is_handled_knn(e) || expr_contains_matches(e))
}

/// Whether a MATCHES operator appears in `expr`, looking through operator
/// applications and call arguments. Other shapes (a field's own parts, an
/// `IF`, a subquery) are not searched: a MATCHES inside them has no index
/// context of the enclosing scan to resolve against. `mentions_index_operator`
/// (`ir/build/inline.rs`) answers a different question — whether an expression
/// names an index of its statement anywhere inside it, so that it never leaves
/// that statement — and so searches every shape.
pub(crate) fn expr_contains_matches(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => {
			matches!(op, BinaryOperator::Matches(_))
				|| expr_contains_matches(left)
				|| expr_contains_matches(right)
		}
		LogicalExpr::Prefix {
			expr: inner,
			..
		} => expr_contains_matches(inner),
		LogicalExpr::Call(call) => call.args.iter().any(expr_contains_matches),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
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
		| LogicalExpr::Lookup {
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
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => false,
	}
}

/// Walk the top-level AND chain of `expr` and replace each leaf for which
/// `should_strip` returns `true` with `true`. After the walk, collapse the
/// boolean sentinels via [`simplify_and_sentinels`]. Returns the rewritten
/// expression, or `None` if the whole condition reduced to `true`.
///
/// Shared body for the per-clause strippers ([`strip_knn_from_condition`],
/// [`strip_index_conditions_in`]). Does not
/// descend below AND boundaries; KNN/FTS operators nested under OR/NOT
/// remain so the planner can detect and reject those shapes.
fn strip_and_simplify<F>(mut expr: LogicalExpr, mut should_strip: F) -> Option<LogicalExpr>
where
	F: FnMut(&LogicalExpr) -> bool,
{
	fn walk<F: FnMut(&LogicalExpr) -> bool>(expr: &mut LogicalExpr, f: &mut F) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			walk(left, f);
			walk(right, f);
		} else if f(expr) {
			*expr = LogicalExpr::Literal(LogicalLiteral::Bool(true));
		}
	}
	walk(&mut expr, &mut should_strip);
	let Ok(()) = expr.visit_mut(&mut |e| {
		simplify_and_sentinels(e);
		Ok::<_, Infallible>(())
	});
	if matches!(expr, LogicalExpr::Literal(LogicalLiteral::Bool(true))) {
		None
	} else {
		Some(expr)
	}
}

/// Collapses boolean-literal sentinels in one AND node: `true AND x → x`,
/// `x AND true → x`, `true AND true → true`. Applied bottom-up over a
/// condition after stripping, so a chain of consumed leaves collapses to a
/// single `true`.
fn simplify_and_sentinels(expr: &mut LogicalExpr) {
	let is_true = |e: &LogicalExpr| matches!(e, LogicalExpr::Literal(LogicalLiteral::Bool(true)));
	if let LogicalExpr::Binary {
		left,
		op: BinaryOperator::And,
		right,
	} = expr
	{
		match (is_true(left), is_true(right)) {
			(true, true) => *expr = LogicalExpr::Literal(LogicalLiteral::Bool(true)),
			(true, false) => {
				let r = std::mem::replace(right.as_mut(), LogicalExpr::Break);
				*expr = r;
			}
			(false, true) => {
				let l = std::mem::replace(left.as_mut(), LogicalExpr::Break);
				*expr = l;
			}
			(false, false) => {}
		}
	}
}

// ---------------------------------------------------------------------------
// Index column comparison
// ---------------------------------------------------------------------------

/// Whether a logical path names the same path as a catalog index column.
///
/// The path's steps ([`path_steps`]) are compared one to one with the
/// column's parts. Steps that hold no expression compare structurally; a step
/// that does (a filter, an index expression, a method call, a lookup) compares
/// by its rendering, which is identical across the two representations. An
/// expression that is not a path never matches a column.
pub(crate) fn idiom_matches_column(expr: &LogicalExpr, col: &Idiom) -> bool {
	if !expr.is_path() {
		return false;
	}
	let steps = path_steps(expr);
	steps.len() == col.0.len()
		&& steps.iter().zip(col.0.iter()).all(|(step, p)| step_matches(step, p))
}

/// [`idiom_matches_column`], with the components of a table's record id
/// matched by position: where `key` is the structure of the table's keys and
/// its `id` names them, a path and a column that each address one component
/// ([`KeyStructure::component_of`], by its name or as `id[i]`) match when they
/// address the same one, so `id[0]` matches the column `city` of a table whose
/// id names `city` first, and `city` matches the column `id[0]`. Every other
/// pair is compared as [`idiom_matches_column`] compares it. With no `key`, or
/// the key of an aggregated view (whose group fields are stored in its rows
/// and read by name), the two are the same.
pub(crate) fn idiom_matches_column_in(
	expr: &LogicalExpr,
	col: &Idiom,
	key: Option<&KeyStructure>,
) -> bool {
	if idiom_matches_column(expr, col) {
		return true;
	}
	let Some(key) = key.filter(|key| key.origin == crate::catalog::KeyOrigin::IdField) else {
		return false;
	};
	match (component_position(key, expr), key.component_of(col)) {
		(Some(read), Some(column)) => read == column,
		_ => false,
	}
}

fn step_matches(step: &LogicalExpr, p: &Part) -> bool {
	match (step, p) {
		(LogicalExpr::Each(_), Part::All)
		| (
			LogicalExpr::Flatten {
				..
			},
			Part::Flatten,
		)
		| (LogicalExpr::Last(_), Part::Last)
		| (LogicalExpr::First(_), Part::First)
		| (
			LogicalExpr::Optional {
				..
			},
			Part::Optional,
		)
		| (LogicalExpr::Doc, Part::Doc)
		| (LogicalExpr::RepeatRecurse(_), Part::RepeatRecurse) => true,
		(LogicalExpr::Field(a), Part::Field(b))
		| (
			LogicalExpr::Access {
				field: a,
				..
			},
			Part::Field(b),
		) => a == b,
		(
			LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Recurse(_),
			_,
		) => step_rendering(step) == p.to_sql(),
		// The expression a path starts from renders as a whole.
		(start, _) if !start.is_path() => start.to_sql() == p.to_sql(),
		_ => false,
	}
}

/// How one step renders on its own: the step's rendering with its base's
/// rendering removed from the front, `[WHERE x]` for a filter, `->likes` for
/// a lookup.
fn step_rendering(step: &LogicalExpr) -> String {
	let sql = step.to_sql();
	let Some(base) = step.step_base() else {
		return sql;
	};
	let base_sql = base.to_sql();
	match sql.strip_prefix(base_sql.as_str()) {
		Some(suffix) => suffix.to_owned(),
		None => sql,
	}
}

/// Whether a field reference in a containment leaf (`field CONTAINS v`,
/// `v INSIDE field`) is served by an array-element index column: the column
/// must flatten (`.*` / `[*]`), and the two paths must agree once every
/// flatten is removed from both.
pub(crate) fn idiom_matches_containment(expr: &LogicalExpr, index_col: &Idiom) -> bool {
	if !expr.is_path() || !index_col.0.iter().any(|p| matches!(p, Part::All)) {
		return false;
	}
	let col_without_all: Vec<&Part> =
		index_col.0.iter().filter(|p| !matches!(p, Part::All)).collect();
	let expr_without_all: Vec<&LogicalExpr> =
		path_steps(expr).into_iter().filter(|s| !matches!(s, LogicalExpr::Each(_))).collect();
	col_without_all.len() == expr_without_all.len()
		&& expr_without_all
			.iter()
			.zip(col_without_all.iter())
			.all(|(step, p)| step_matches(step, p))
}

// ---------------------------------------------------------------------------
// Index condition stripping
// ---------------------------------------------------------------------------

/// [`strip_index_conditions_in`], matching a conjunct to an index column the way
/// [`idiom_matches_column_in`] does: with the structure `key` of the table's
/// record ids, a path and a column that address the same component are the
/// same column, whichever way each spells it. A conjunct may read a component
/// as `id[i]`, while the index column names it.
pub(crate) fn strip_index_conditions_in(
	cond: &LogicalExpr,
	access: &crate::exec::index::access_path::BTreeAccess,
	cols: &[Idiom],
	key: Option<&KeyStructure>,
) -> Option<LogicalExpr> {
	let matcher = IndexConditionMatcher {
		cols,
		access,
		key,
	};
	strip_and_simplify(cond.clone(), |e| {
		if let LogicalExpr::Binary {
			left,
			op,
			right,
		} = e
		{
			matcher.matches_access(left, op, right)
		} else {
			false
		}
	})
}

/// Decides whether a single binary comparison leaf is covered by a chosen
/// BTree access pattern. Used as the leaf predicate by
/// [`strip_index_conditions_in`].
struct IndexConditionMatcher<'a> {
	/// Index columns in definition order (`IndexDefinition.cols`).
	cols: &'a [Idiom],
	/// The chosen access pattern describing which conditions are covered.
	access: &'a crate::exec::index::access_path::BTreeAccess,
	/// The structure of the table's record ids, where a column and a path may
	/// address one component by different spellings.
	key: Option<&'a KeyStructure>,
}

impl IndexConditionMatcher<'_> {
	/// Check whether a binary comparison leaf is covered by the access pattern.
	fn matches_access(&self, left: &LogicalExpr, op: &BinaryOperator, right: &LogicalExpr) -> bool {
		use crate::exec::index::access_path::BTreeAccess;

		// Extract field, value, and the effective operator (normalized so the
		// field is always on the left side of the comparison). `idiom_on_left`
		// tracks the original position because some operator rewrites are
		// only valid when the field was the left operand (see the Inside
		// normalisation below).
		//
		// The value side is a literal or the statement clock, as the access
		// holds either; it is covered only where it equals the access's own
		// operand.
		let (idiom, value, effective_op, idiom_on_left) = match (left, right) {
			(i, other) if i.is_path() => match try_bound_operand(other, true) {
				Some(v) => (i, v, op.clone(), true),
				None => return false,
			},
			(other, i) if i.is_path() => match try_bound_operand(other, true) {
				Some(v) => {
					let flipped = match op {
						BinaryOperator::LessThan => BinaryOperator::MoreThan,
						BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
						BinaryOperator::MoreThan => BinaryOperator::LessThan,
						BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
						other @ BinaryOperator::Subtract
						| other @ BinaryOperator::Add
						| other @ BinaryOperator::Multiply
						| other @ BinaryOperator::Divide
						| other @ BinaryOperator::Remainder
						| other @ BinaryOperator::Power
						| other @ BinaryOperator::Equal
						| other @ BinaryOperator::ExactEqual
						| other @ BinaryOperator::NotEqual
						| other @ BinaryOperator::AllEqual
						| other @ BinaryOperator::AnyEqual
						| other @ BinaryOperator::Or
						| other @ BinaryOperator::And
						| other @ BinaryOperator::NullCoalescing
						| other @ BinaryOperator::TenaryCondition
						| other @ BinaryOperator::Contain
						| other @ BinaryOperator::NotContain
						| other @ BinaryOperator::ContainAll
						| other @ BinaryOperator::ContainAny
						| other @ BinaryOperator::ContainNone
						| other @ BinaryOperator::Inside
						| other @ BinaryOperator::NotInside
						| other @ BinaryOperator::AllInside
						| other @ BinaryOperator::AnyInside
						| other @ BinaryOperator::NoneInside
						| other @ BinaryOperator::Outside
						| other @ BinaryOperator::Intersects
						| other @ BinaryOperator::Range
						| other @ BinaryOperator::RangeInclusive
						| other @ BinaryOperator::RangeSkip
						| other @ BinaryOperator::RangeSkipInclusive
						| other @ BinaryOperator::Matches(_)
						| other @ BinaryOperator::NearestNeighbor(_) => other.clone(),
					};
					(i, v, flipped, false)
				}
				None => return false,
			},
			_ => return false,
		};

		// Normalise the leaf so the strip path sees the same canonical
		// form the analyzer produced: a single-element `field IN [v]` is
		// rewritten by `extract_simple_condition` (analyzer side) to
		// `field = v`, but ONLY when the field is on the left.
		// `[v] IN field` (Right-position) has different runtime
		// semantics — `[v].contains(field)` — and the analyzer correctly
		// produces no candidate for it, so we must not strip it from the
		// residual filter either. Gate the rewrite on `idiom_on_left`.
		let (effective_op, value) = if idiom_on_left
			&& matches!(effective_op, BinaryOperator::Inside)
			&& let Some(Value::Array(arr)) = value.as_literal()
			&& arr.len() == 1
		{
			(BinaryOperator::Equal, BoundOperand::Literal(arr[0].clone()))
		} else {
			(effective_op, value)
		};

		let is_equality =
			matches!(effective_op, BinaryOperator::Equal | BinaryOperator::ExactEqual);

		// `field CONTAINS scalar` (field on left) and `scalar INSIDE field`
		// (field on right) are covered when the matching index column is an
		// array-element flatten (`.*`/`[*]`) and the scalar matches the
		// access's prefix/equality value. Mirrors the analyser's
		// `try_match_containment` so a containment leaf is stripped from the
		// residual, which in turn enables `strip_index_conditions_in` to report
		// `FullyConsumed` and the SELECT planner to push LIMIT into the scan.
		//
		// A NONE or NULL value is never covered: an empty array has no
		// element to index and is keyed as NULL at its column, and an absent
		// field is keyed as NONE, so an entry carrying either does not prove
		// the column holds such an element.
		let is_containment = ((matches!(effective_op, BinaryOperator::Contain) && idiom_on_left)
			|| (matches!(effective_op, BinaryOperator::Inside) && !idiom_on_left))
			&& value.as_literal().is_some_and(|v| !v.is_nullish());

		match self.access {
			BTreeAccess::Compound {
				prefix,
				range,
				upper,
			} => {
				// Check equality conditions against prefix values.
				if is_equality {
					for (col, val) in self.cols.iter().zip(prefix.iter()) {
						if idiom_matches_column_in(idiom, col, self.key) && value == *val {
							return true;
						}
					}
				}
				// CONTAINS / INSIDE leaf on an array-element column the
				// prefix reaches: the analyser pinned that column to this
				// value, so the scan range already filters to matching
				// rows. Positional like the equality arm above — the
				// analyser extends a prefix across any column a containment
				// leaf pins, not only the leading one.
				if is_containment {
					for (col, val) in self.cols.iter().zip(prefix.iter()) {
						if idiom_matches_containment(idiom, col) && value == *val {
							return true;
						}
					}
				}
				// Check range condition on the column after the prefix.
				if let Some(col) = self.cols.get(prefix.len())
					&& idiom_matches_column_in(idiom, col, self.key)
					&& upper
						.as_ref()
						.is_some_and(|(op, bound)| effective_op == *op && value == *bound)
				{
					return true;
				}
				if let Some((range_op, range_val)) = range
					&& let Some(col) = self.cols.get(prefix.len())
					&& idiom_matches_column_in(idiom, col, self.key)
				{
					if effective_op == *range_op && value == *range_val {
						return true;
					}
					// `field != NONE` after an equality prefix is encoded
					// as `Some((MoreThan, NONE))` by
					// `analyze_compound_conditions`. Mirror the Range arm
					// so the residual Filter doesn't keep this leaf.
					if matches!(range_op, BinaryOperator::MoreThan)
						&& matches!(range_val.as_literal(), Some(Value::None))
						&& effective_op == BinaryOperator::NotEqual
						&& matches!(value.as_literal(), Some(Value::None))
					{
						return true;
					}
				}
				false
			}
			BTreeAccess::Equality(val) => {
				let Some(col) = self.cols.first() else {
					return false;
				};
				if is_equality && idiom_matches_column_in(idiom, col, self.key) && value == *val {
					return true;
				}
				// CONTAINS / INSIDE on the single-column array-element index:
				// strip the leaf for the same reason as the Compound case.
				if is_containment && idiom_matches_containment(idiom, col) && value == *val {
					return true;
				}
				false
			}
			BTreeAccess::Range {
				range,
			} => {
				let Some(col) = self.cols.first() else {
					return false;
				};
				if !idiom_matches_column_in(idiom, col, self.key) {
					return false;
				}
				// A range literal became this access whole, so the leaf is
				// covered exactly when the literal's bounds are the access's.
				if matches!(effective_op, BinaryOperator::Inside)
					&& idiom_on_left
					&& let Some(Value::Range(r)) = value.as_literal()
					&& range.as_literal().is_some_and(|literal| **r == literal)
				{
					return true;
				}
				match &range.start {
					Bound::Included(x) => {
						if effective_op == BinaryOperator::MoreThanEqual && value == *x {
							return true;
						}
					}
					Bound::Excluded(x) => {
						if effective_op == BinaryOperator::MoreThan && value == *x {
							return true;
						}

						if matches!(x.as_literal(), Some(Value::None))
							&& matches!(value.as_literal(), Some(Value::None))
							&& effective_op == BinaryOperator::NotEqual
						{
							return true;
						}
					}
					Bound::Unbounded => {}
				}

				match &range.end {
					Bound::Included(x) => {
						if effective_op == BinaryOperator::LessThanEqual && value == *x {
							return true;
						}
					}
					Bound::Excluded(x) => {
						if effective_op == BinaryOperator::LessThan && value == *x {
							return true;
						}
					}
					Bound::Unbounded => {}
				}

				false
			}
			// FullText and KNN access types have their own stripping logic.
			BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => false,
		}
	}
}

/// Extract a `Vec<Number>` from an array literal whose every element is a
/// numeric literal.
fn extract_literal_vector(expr: &LogicalExpr) -> Option<Vec<Number>> {
	let LogicalExpr::Array(items) = expr else {
		return None;
	};
	let mut nums = Vec::with_capacity(items.len());
	for elem in items {
		match elem {
			LogicalExpr::Literal(LogicalLiteral::Integer(i)) => nums.push(Number::Int(*i)),
			LogicalExpr::Literal(LogicalLiteral::I8(i)) => nums.push(Number::I8(*i)),
			LogicalExpr::Literal(LogicalLiteral::U8(u)) => nums.push(Number::U8(*u)),
			LogicalExpr::Literal(LogicalLiteral::I16(i)) => nums.push(Number::I16(*i)),
			LogicalExpr::Literal(LogicalLiteral::U16(u)) => nums.push(Number::U16(*u)),
			LogicalExpr::Literal(LogicalLiteral::I32(i)) => nums.push(Number::I32(*i)),
			LogicalExpr::Literal(LogicalLiteral::U32(u)) => nums.push(Number::U32(*u)),
			LogicalExpr::Literal(LogicalLiteral::U64(u)) => nums.push(Number::U64(*u)),
			LogicalExpr::Literal(LogicalLiteral::Float(f)) => nums.push(Number::Float(*f)),
			LogicalExpr::Literal(LogicalLiteral::F16(f)) => nums.push(Number::F16(*f)),
			LogicalExpr::Literal(LogicalLiteral::F32(f)) => nums.push(Number::F32(*f)),
			LogicalExpr::Literal(LogicalLiteral::Decimal(d)) => nums.push(Number::Decimal(*d)),
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Access {
				..
			}
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
			| LogicalExpr::Lookup {
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
	Some(nums)
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
pub(crate) fn extract_record_id_point_lookup(
	cond: &LogicalExpr,
	table_name: &TableName,
) -> Option<LogicalExpr> {
	find_id_equality_in_and_chain(cond, table_name)
}

/// Walk the top-level AND chain looking for `id = <RecordId literal>`.
fn find_id_equality_in_and_chain(
	expr: &LogicalExpr,
	table_name: &TableName,
) -> Option<LogicalExpr> {
	match expr {
		// AND: check both branches
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => find_id_equality_in_and_chain(left, table_name)
			.or_else(|| find_id_equality_in_and_chain(right, table_name)),

		// Equality: check for `id = <RecordId>` or `<RecordId> = id`
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::Equal | BinaryOperator::ExactEqual,
			right,
		} => check_id_recordid_pair(left, right, table_name)
			.or_else(|| check_id_recordid_pair(right, left, table_name)),

		// Any other node (OR, comparisons, etc.): no match
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
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
		| LogicalExpr::Lookup {
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
		| LogicalExpr::Subquery(_) => None,
	}
}

/// Check if `idiom_side` is the `id` field and `lit_side` is a matching
/// RecordId literal with a non-range key.
fn check_id_recordid_pair(
	idiom_side: &LogicalExpr,
	lit_side: &LogicalExpr,
	table_name: &TableName,
) -> Option<LogicalExpr> {
	if idiom_is_id(idiom_side)
		&& let LogicalExpr::RecordId(rid) = lit_side
		&& &rid.table == table_name
		&& !matches!(rid.key, LogicalRecordIdKey::Range(_))
	{
		Some(lit_side.clone())
	} else {
		None
	}
}

/// The list of the conjunct `cond` when it is `id IN <list>` or `<list> CONTAINS id`, `<list>` an
/// array or set literal.
fn record_id_list(cond: &LogicalExpr) -> Option<&LogicalExpr> {
	let LogicalExpr::Binary {
		left,
		op,
		right,
	} = cond
	else {
		return None;
	};
	if let BinaryOperator::Inside = op
		&& idiom_is_id(left)
	{
		Some(right)
	} else if let BinaryOperator::Contain = op
		&& idiom_is_id(right)
	{
		Some(left)
	} else {
		None
	}
}

/// Whether [`extract_record_id_list_lookup`] answers for `cond`, judged without building the list:
/// every element is a literal, and every record id of `table` among them has a number, string or
/// uuid key.
pub(crate) fn is_record_id_list_lookup(cond: &LogicalExpr, table: &TableName) -> bool {
	let Some(LogicalExpr::Array(items) | LogicalExpr::Set(items)) = record_id_list(cond) else {
		return false;
	};
	items.iter().all(|item| match item {
		LogicalExpr::RecordId(record) if &record.table == table => matches!(
			record.key,
			LogicalRecordIdKey::Number(_)
				| LogicalRecordIdKey::String(_)
				| LogicalRecordIdKey::Uuid(_)
		),
		other @ LogicalExpr::Literal(_)
		| other @ LogicalExpr::Array(_)
		| other @ LogicalExpr::Set(_)
		| other @ LogicalExpr::Object(_)
		| other @ LogicalExpr::RecordId(_)
		| other @ LogicalExpr::Param(_)
		| other @ LogicalExpr::Table(_)
		| other @ LogicalExpr::Mock(_)
		| other @ LogicalExpr::Constant(_)
		| other @ LogicalExpr::Field(_)
		| other @ LogicalExpr::Doc
		| other @ LogicalExpr::Current
		| other @ LogicalExpr::Access {
			..
		}
		| other @ LogicalExpr::Index {
			..
		}
		| other @ LogicalExpr::First(_)
		| other @ LogicalExpr::Last(_)
		| other @ LogicalExpr::Each(_)
		| other @ LogicalExpr::Flatten {
			..
		}
		| other @ LogicalExpr::Filter {
			..
		}
		| other @ LogicalExpr::Method {
			..
		}
		| other @ LogicalExpr::Destructure {
			..
		}
		| other @ LogicalExpr::Optional {
			..
		}
		| other @ LogicalExpr::Lookup {
			..
		}
		| other @ LogicalExpr::Recurse(_)
		| other @ LogicalExpr::RepeatRecurse(_)
		| other @ LogicalExpr::Broadcast {
			..
		}
		| other @ LogicalExpr::SetField {
			..
		}
		| other @ LogicalExpr::Prefix {
			..
		}
		| other @ LogicalExpr::Postfix {
			..
		}
		| other @ LogicalExpr::Invoke {
			..
		}
		| other @ LogicalExpr::Binary {
			..
		}
		| other @ LogicalExpr::Call(_)
		| other @ LogicalExpr::Closure(_)
		| other @ LogicalExpr::If(_)
		| other @ LogicalExpr::Throw(_)
		| other @ LogicalExpr::Break
		| other @ LogicalExpr::Continue
		| other @ LogicalExpr::Subquery(_) => super::literals::try_literal_to_value(other).is_some(),
	})
}

/// The records of `table` the conjunct `cond` admits when it is `id IN <list>` or
/// `<list> CONTAINS id`, `<list>` an array or set literal: its record ids of `table`, in key order
/// and without repeats. An element that is not a record id of `table` names no record of it and is
/// left out. `None` when `cond` is not of that form, or when a record id of `table` in the list
/// has a key other than a number, a string or a uuid: only for those keys does a record id that
/// equals the list's element name the one stored key the element spells, and do record ids
/// compare in the order their stored keys are read in.
pub(crate) fn extract_record_id_list_lookup(
	cond: &LogicalExpr,
	table: &TableName,
) -> Option<Vec<crate::val::RecordId>> {
	if !is_record_id_list_lookup(cond, table) {
		return None;
	}
	let val = super::literals::try_literal_to_value(record_id_list(cond)?)?;
	let elements: Vec<Value> = if let Value::Array(arr) = val {
		arr.0
	} else if let Value::Set(set) = val {
		set.into_iter().collect()
	} else {
		return None;
	};
	let mut records: Vec<crate::val::RecordId> = elements
		.into_iter()
		.filter_map(|element| {
			if let Value::RecordId(record) = element
				&& &record.table == table
			{
				Some(record)
			} else {
				None
			}
		})
		.collect();
	records.sort();
	records.dedup();
	Some(records)
}

// ============================================================================
// MATCHES Context Extraction
// ============================================================================

/// The primary table the in-scope MATCHES entries were collected against.
///
/// A scope only carries a table when the SELECT that built it had a literal
/// table source, so this falls back to the placeholder `"unknown"` rather than
/// naming a real table it cannot confirm. That placeholder matches no index, so
/// the index function resolves against no full-text entry and yields `NONE` per
/// row — the same outcome as a MATCHES whose index does not cover the idiom.
pub(crate) fn extract_table_from_matches(
	matches_context: &crate::exec::function::MatchesContext,
) -> TableName {
	if let Some(table) = matches_context.table() {
		return table.clone();
	}
	TableName::from("unknown".to_string())
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;
	use surrealdb_types::ToSql;

	use super::extract_record_id_list_lookup;
	use crate::expr::logical::{LogicalExpr, lower_expr};

	fn expr(sql: &str) -> LogicalExpr {
		let ast = crate::syn::parse(&format!("RETURN {sql}")).expect("expression should parse");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Return(out)) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a RETURN");
		};
		lower_expr(out.what).expect("expression should lower")
	}

	fn listed(sql: &str) -> Option<Vec<String>> {
		extract_record_id_list_lookup(&expr(sql), &TableName::from("t"))
			.map(|records| records.iter().map(|r| r.to_sql()).collect())
	}

	#[test]
	fn an_id_list_names_its_records_of_the_table_in_key_order() {
		assert_eq!(
			listed("id IN [t:3, t:1, t:3, u:2, 'x', t:b, t:a]"),
			Some(vec!["t:1".into(), "t:3".into(), "t:a".into(), "t:b".into()])
		);
		assert_eq!(listed("[t:2, t:1] CONTAINS id"), Some(vec!["t:1".into(), "t:2".into()]));
		assert_eq!(listed("id IN [u:1, NONE]"), Some(vec![]));
	}

	#[test]
	fn an_id_list_with_a_composite_or_range_key_names_no_records() {
		assert_eq!(listed("id IN [t:1, t:[1, 2]]"), None);
		assert_eq!(listed("id IN [t:1, t:{ a: 1 }]"), None);
		assert_eq!(listed("id IN [t:1..3]"), None);
	}

	#[test]
	fn only_a_list_literal_against_id_names_records() {
		assert_eq!(listed("id IN $list"), None);
		assert_eq!(listed("other IN [t:1]"), None);
		assert_eq!(listed("id CONTAINS t:1"), None);
		assert_eq!(listed("id = t:1"), None);
	}
}
