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
use super::literals::try_literal_to_value;
use crate::catalog::{Distance, KeyStructure};
use crate::exec::function::FunctionRegistry;
pub(crate) use crate::exec::index::access_path::KnnQueryVector;
use crate::exec::index::key_range::component_position;
use crate::exec::planner::row_scope::reads_scanned_row;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalRecordIdKey};
use crate::expr::operator::{MatchesOperator, NearestNeighbor};
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
/// `KTree` was backed by the M-Tree index, which has been removed. The
/// planner uses this to distinguish "unsupported variant" from
/// "KNN nested under OR/NOT" when emitting the user-facing error.
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
			| LogicalLiteral::Bool(_)
			| LogicalLiteral::String(_)
			| LogicalLiteral::Duration(_)
			| LogicalLiteral::Uuid(_)
			| LogicalLiteral::Datetime(_)
			| LogicalLiteral::None
			| LogicalLiteral::Null
			| LogicalLiteral::Decimal(_)
			| LogicalLiteral::Float(_),
		) => true,
		LogicalExpr::RecordId(_) => true,
		LogicalExpr::Param(_) | LogicalExpr::Call(_) => true,
		LogicalExpr::Binary {
			left,
			right,
			..
		} => is_plan_time_computable(left) && is_plan_time_computable(right),
		_ => false,
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
			Number::Int(_) | Number::Decimal(_) => true,
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

/// Replace each approximate KNN operator (`<|k, ef|>`) in the top-level `AND`
/// chain of `cond` with the brute-force operator of the same `k` over the
/// distance `distance_of` names for its field, leaving one `distance_of`
/// declines as it is.
///
/// The brute-force operator ranks the rows themselves, so it answers from
/// whatever state those rows are read at, where an ANN index answers only for
/// the state its last write left.
pub(crate) fn exact_knn_by(
	cond: &mut LogicalExpr,
	distance_of: &impl Fn(&LogicalExpr) -> Option<Distance>,
) {
	match cond {
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => {
			exact_knn_by(left, distance_of);
			exact_knn_by(right, distance_of);
		}
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::NearestNeighbor(nn),
			..
		} => {
			if let NearestNeighbor::Approximate(k, _) = nn.as_ref()
				&& let Some(distance) = distance_of(left)
			{
				**nn = NearestNeighbor::K(*k, distance.into());
			}
		}
		_ => {}
	}
}

/// Strip from a WHERE clause the MATCHES (`@@`) conjuncts that every one of
/// several full-text scans answers, returning the residual.
///
/// Each probe is one scan's `(column, operator, query)`, as for
/// [`strip_fts_condition`]. A conjunct is stripped only when it is the probe of
/// every scan, because a union of scans lists a row for any one of them and so
/// says nothing about a conjunct another scan did not read; with no probe
/// nothing is stripped.
pub(crate) fn strip_fts_probes(
	cond: &LogicalExpr,
	probes: &[(&Idiom, &MatchesOperator, &str)],
) -> Option<LogicalExpr> {
	strip_and_simplify(cond.clone(), |e| {
		!probes.is_empty()
			&& probes
				.iter()
				.all(|(column, operator, query)| is_fts_probe(e, column, operator, query))
	})
}

/// Whether `e` is the MATCHES the full-text scan over `column` probes: its
/// operator is `operator`, its query the literal string `query`, and its left
/// side the column.
fn is_fts_probe(e: &LogicalExpr, column: &Idiom, operator: &MatchesOperator, query: &str) -> bool {
	let LogicalExpr::Binary {
		left,
		op: BinaryOperator::Matches(op),
		right,
	} = e
	else {
		return false;
	};
	op == operator
		&& matches!(try_literal_to_value(right), Some(Value::String(s)) if s.as_str() == query)
		&& idiom_matches_column(left, column)
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
/// context of the enclosing scan to resolve against. The logical rules'
/// `mentions_index_operator` answers a different question — whether a
/// conjunct names an index of its statement anywhere inside it, so that no
/// rule moves it out of that statement — and so searches every shape.
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
		_ => false,
	}
}

/// Walk the top-level AND chain of `expr` and replace each leaf for which
/// `should_strip` returns `true` with `true`. After the walk, collapse the
/// boolean sentinels via [`simplify_and_sentinels`]. Returns the rewritten
/// expression, or `None` if the whole condition reduced to `true`.
///
/// Shared body for the per-clause strippers ([`strip_fts_condition`],
/// [`strip_knn_from_condition`], [`strip_index_conditions`]). Does not
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

/// Strip conditions covered by a BTree index access path from a WHERE clause.
///
/// Returns `None` when all conditions are consumed (no Filter needed),
/// or `Some(residual)` when conditions remain that the index does not cover.
pub(crate) fn strip_index_conditions(
	cond: &LogicalExpr,
	access: &crate::exec::index::access_path::BTreeAccess,
	cols: &[Idiom],
) -> Option<LogicalExpr> {
	strip_index_conditions_in(cond, access, cols, None)
}

/// [`strip_index_conditions`], matching a conjunct to an index column the way
/// [`idiom_matches_column_in`] does: with the structure `key` of the table's
/// record ids, a path and a column that address the same component are the
/// same column, whichever way each spells it. A conjunct a layer moved beneath
/// itself reads a component as `id[i]`, and the column an index holds for it is
/// spelled by the component's name.
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
	strip_and_simplify(cond.clone(), |e| match e {
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => matcher.matches_access(left, op, right),
		_ => false,
	})
}

/// Decides whether a single binary comparison leaf is covered by a chosen
/// BTree access pattern. Used as the leaf predicate by
/// [`strip_index_conditions`].
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
		let (idiom, value, effective_op, idiom_on_left) = match (left, right) {
			(i, other) if i.is_path() => match try_literal_to_value(other) {
				Some(v) => (i, v, op.clone(), true),
				None => return false,
			},
			(other, i) if i.is_path() => match try_literal_to_value(other) {
				Some(v) => {
					let flipped = match op {
						BinaryOperator::LessThan => BinaryOperator::MoreThan,
						BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
						BinaryOperator::MoreThan => BinaryOperator::LessThan,
						BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
						other => other.clone(),
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
			&& let Value::Array(arr) = &value
			&& arr.len() == 1
		{
			(BinaryOperator::Equal, arr[0].clone())
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
		// residual, which in turn enables `strip_index_conditions` to report
		// `FullyConsumed` and the SELECT planner to push LIMIT into the scan.
		//
		// A NONE or NULL value is never covered: an empty array has no
		// element to index and is keyed as NULL at its column, and an absent
		// field is keyed as NONE, so an entry carrying either does not prove
		// the column holds such an element.
		let is_containment = ((matches!(effective_op, BinaryOperator::Contain) && idiom_on_left)
			|| (matches!(effective_op, BinaryOperator::Inside) && !idiom_on_left))
			&& !value.is_nullish();

		match self.access {
			BTreeAccess::Compound {
				prefix,
				range,
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
						&& matches!(range_val, Value::None)
						&& effective_op == BinaryOperator::NotEqual
						&& matches!(value, Value::None)
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
					&& let Value::Range(r) = &value
					&& **r == *range
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

						if matches!(x, Value::None)
							&& matches!(value, Value::None)
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
			_ => false,
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
			LogicalExpr::Literal(LogicalLiteral::Float(f)) => nums.push(Number::Float(*f)),
			LogicalExpr::Literal(LogicalLiteral::Decimal(d)) => nums.push(Number::Decimal(*d)),
			_ => return None,
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
		_ => None,
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

// ============================================================================
// MATCHES Context Extraction
// ============================================================================

/// Extract MATCHES clause information from a WHERE condition for index functions.
///
/// Accepts an optional `FrozenContext` to resolve bind parameters (`$query`)
/// that appear on the right-hand side of `@N@` operators. A parameter for
/// which `shadowed` answers `true` is bound by the plan itself — a function's
/// argument, a closure's or `FOR` loop's variable — and holds its value only
/// when the plan runs, so the context's value of that name is never read and
/// the clause is not extracted.
pub(crate) fn extract_matches_context(
	cond: &LogicalExpr,
	ctx: Option<&crate::ctx::FrozenContext>,
	shadowed: impl Fn(&str) -> bool,
) -> crate::exec::function::MatchesContext {
	let mut matches = crate::exec::function::MatchesContext::new();
	let _ = cond.visit(&mut |e| {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Matches(matches_op),
			right,
		} = e && left.is_path()
		{
			// Extract the query string from the right-hand side.
			// Supports both literal strings and bind parameters.
			let query_str = match right.as_ref() {
				LogicalExpr::Literal(LogicalLiteral::String(s)) => Some(s.as_str().to_owned()),
				LogicalExpr::Param(param) if shadowed(param.as_str()) => None,
				LogicalExpr::Param(param) => {
					// Resolve the bind parameter from the frozen context
					ctx.and_then(|ctx| {
						ctx.value(param.as_str()).and_then(|v| {
							if let crate::val::Value::String(s) = v {
								Some(s.as_str().to_owned())
							} else {
								None
							}
						})
					})
				}
				_ => None,
			};

			if let Some(query) = query_str {
				let match_ref = matches_op.rf.unwrap_or(0);
				matches.insert(
					match_ref,
					crate::exec::function::MatchInfo {
						field: left.as_ref().clone(),
						query,
					},
				);
			}
		}
		ControlFlow::<()>::Continue(())
	});
	matches
}

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
/// Whether an expression contains a brute-force KNN operator
/// (`NearestNeighbor::K`, the `<|k, DIST|>` form), whatever its right-hand
/// side, outside closure literals.
pub(crate) fn has_knn_k_operator(expr: &LogicalExpr) -> bool {
	scan_knn_operators(expr).found_k
}

#[cfg(test)]
/// Strip the MATCHES (`@@`) conjuncts a full-text scan answers from a WHERE
/// clause, returning the residual.
///
/// A conjunct is stripped only when it is the scan's own probe: its operator is
/// `operator`, its query is the literal string `query`, and its left side is
/// the index's first column `column`. Every other conjunct — a MATCHES on
/// another field, with another reference or another query — stays in the
/// residual, where it is evaluated per row or refused; the scan's rows say
/// nothing about it.
///
/// Returns `None` when the entire condition is consumed (just a single `@@`),
/// or `Some(residual)` when additional predicates remain (e.g., `content @@ 'x' AND status = 'a'`).
pub(crate) fn strip_fts_condition(
	cond: &LogicalExpr,
	column: &Idiom,
	operator: &MatchesOperator,
	query: &str,
) -> Option<LogicalExpr> {
	strip_fts_probes(cond, &[(column, operator, query)])
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use surrealdb_types::ToSql;

	use super::*;
	use crate::expr::logical::lower_expr;

	/// The builtin function registry, which says which calls read the row.
	fn registry() -> &'static FunctionRegistry {
		static REGISTRY: std::sync::LazyLock<FunctionRegistry> =
			std::sync::LazyLock::new(FunctionRegistry::with_builtins);
		&REGISTRY
	}

	fn parse_cond(snippet: &str) -> LogicalExpr {
		let src = format!("SELECT * FROM t WHERE {snippet}");
		let mut exprs = crate::syn::parse(&src).expect("parse").expressions;
		assert_eq!(exprs.len(), 1, "expected one statement from {src:?}");
		match exprs.remove(0) {
			surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Select(s)) => {
				lower_expr(s.cond.expect("WHERE").0).expect("lowers")
			}
			other => panic!("expected SELECT, got {other:?}"),
		}
	}

	#[test]
	fn index_columns_compare_against_logical_paths() {
		let col = |s: &str| Idiom::from_str(s).expect("valid idiom");
		let path = |s: &str| {
			lower_expr(crate::expr::Expr::Idiom(crate::syn::idiom(s).expect("idiom")))
				.expect("lowers")
		};
		let a_b = path("a.b");
		assert!(idiom_matches_column(&a_b, &col("a.b")));
		assert!(!idiom_matches_column(&a_b, &col("a")));
		assert!(!idiom_matches_column(&a_b, &col("a.c")));
		assert!(idiom_matches_containment(&a_b, &col("a.b.*")));
		assert!(idiom_matches_containment(&a_b, &col("a.*.b")));
		assert!(!idiom_matches_containment(&a_b, &col("a.b")));
		// Steps that hold an expression compare by their rendering; a
		// broadcast contributes no step of its own.
		assert!(idiom_matches_column(&path("a.b[0]"), &col("a.b[0]")));
		assert!(idiom_matches_column(&path("a[WHERE x = 1].b"), &col("a[WHERE x = 1].b")));
		assert!(!idiom_matches_column(&path("a[WHERE x = 1].b"), &col("a[WHERE x = 2].b")));
		assert!(idiom_matches_column(&path("a.*"), &col("a.*")));
		assert!(idiom_matches_column(&path("a[*].b"), &col("a[*].b")));
		assert!(!idiom_matches_column(&parse_cond("a + 1"), &col("a")));
	}

	#[test]
	fn a_path_is_plan_time_computable_when_rooted_at_a_parameter() {
		for src in [
			"$o.v",
			"$m[0]",
			"$m[$i]",
			"$m[1].v",
			"$o.v[WHERE x > 0]",
			"$o.v.w[$]",
			"$parent.v",
			"(SELECT VALUE v FROM t)[0]",
		] {
			assert!(!reads_scanned_row(&parse_cond(src), registry()), "{src}");
			assert!(
				matches!(
					classify_knn_vector(&parse_cond(src), registry()),
					KnnVectorClass::Vector(KnnQueryVector::Deferred(_))
				),
				"{src}"
			);
		}
		for src in ["a.b", "a[0]", "@.v", "$this.v"] {
			assert!(reads_scanned_row(&parse_cond(src), registry()), "{src}");
		}
	}

	/// A prefix expression over a row-independent operand (a cast, a
	/// negation) and a constant are plan-time computable, and become a
	/// deferred query vector; forms that read the row stay excluded, under a
	/// prefix as without one.
	#[test]
	fn a_prefix_expression_and_a_constant_are_plan_time_computable() {
		let constant = parse_cond("math::pi");
		assert!(matches!(constant, LogicalExpr::Constant(_)), "{}", constant.to_sql());
		for src in ["<array<float>> $q", "-$q", "<float> $x", "math::pi"] {
			assert!(!reads_scanned_row(&parse_cond(src), registry()), "{src}");
			assert!(
				matches!(
					classify_knn_vector(&parse_cond(src), registry()),
					KnnVectorClass::Vector(KnnQueryVector::Deferred(_))
				),
				"{src}"
			);
		}
		for src in ["a.b", "a[0]", "@.v", "<array<float>> a", "-a.b"] {
			assert!(reads_scanned_row(&parse_cond(src), registry()), "{src}");
			assert!(
				matches!(
					classify_knn_vector(&parse_cond(src), registry()),
					KnnVectorClass::ReadsRow
				),
				"{src}"
			);
		}
	}

	/// The two classes of a KNN right-hand side: plan-time computable (the
	/// deferred query vector) or a reference to the scanned row (refused). A
	/// field inside a body that is evaluated against an element, not the row,
	/// is no row reference.
	#[test]
	fn a_knn_right_hand_side_is_computable_or_row_dependent() {
		let computable = [
			"[$x, 0f]",
			"fn::q($x)",
			"$m[$i]",
			"$arr[WHERE x > 1]",
			"<array<float>> [$x, 0f]",
			"$m.join($sep)",
			"array::map([2, 3], |$x| <float> $x)",
		];
		for src in computable {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
		}
		let row_dependent = [
			"point",
			"@.v",
			"a[0]",
			"[point, 0f]",
			"fn::q(point)",
			"array::flatten([point])",
			"$this",
			"$self",
			"$this.point",
			"$m[point]",
			"$m.join(point)",
			"-point",
			"$q + point",
			"<array<float>> point",
		];
		for src in row_dependent {
			let expr = parse_cond(src);
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(
				matches!(classify_knn_vector(&expr, registry()), KnnVectorClass::ReadsRow),
				"{src}"
			);
		}
	}

	/// Whatever its kind, a right-hand side that does not read the scanned row
	/// is the deferred query vector: an `IF`, an object, a set, a block, a
	/// bare closure, a path rooted at a call, a table. The same kinds reading
	/// the row — in a condition, a value, a statement, an index expression or
	/// method argument inside an element body — are row-dependent. A
	/// destructure's body is evaluated against the destructured value, so an
	/// index expression there reads that value.
	#[test]
	fn any_kind_that_does_not_read_the_row_is_computable() {
		for src in [
			"[IF $high THEN 5f ELSE 1f END, 0f]",
			"IF $high THEN [5f, 0f] ELSE [1f, 0f] END",
			"fn::q().v",
			"|$x| $x",
			"{ RETURN [1f, 0f] }",
			"{ LET $v = [1f, 0f]; RETURN $v }",
			"{ a: 1 }",
			"{1, 2}",
			"object::from_entries([['v', [5f, 0f]]]).v",
			"$o.v.{ a, b }",
			"$o.v.{ a: b[point] }",
			"$o.v.?.w",
			"THROW 'x'",
		] {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(
				matches!(
					classify_knn_vector(&expr, registry()),
					KnnVectorClass::Vector(KnnQueryVector::Deferred(_))
				),
				"{src}"
			);
		}
		for src in [
			"IF point THEN [5f, 0f] ELSE [1f, 0f] END",
			"IF $high THEN point ELSE [1f, 0f] END",
			"[IF enabled THEN 5f ELSE 1f END, 0f]",
			"{ a: point }",
			"{point, 1}",
			"{ RETURN point }",
			"{ LET $v = point; RETURN $v }",
			"{ RETURN $this.point }",
			"THROW point",
			"$o.v.map(|$e| $e)[point]",
			"$o.v[WHERE x = 1][point]",
			"$o.v[WHERE x[point]]",
			"$o.v.{ a: b }[point]",
			"->edge->pts",
		] {
			let expr = parse_cond(src);
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(
				matches!(classify_knn_vector(&expr, registry()), KnnVectorClass::ReadsRow),
				"{src}"
			);
		}
	}

	/// `$parent` at the scanned statement's own level is the enclosing
	/// statement's row, fixed while the scan runs, so it is computable, bare
	/// or as a path root, inside an array or a call.
	#[test]
	fn parent_is_the_enclosing_row_and_plan_time_computable() {
		for src in ["$parent", "$parent.v", "$parent.v[0]", "[$parent.x, 0f]", "fn::q($parent.v)"] {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
		}
	}

	/// A `[WHERE …]` predicate and a lookup bind `$parent` to the document
	/// the path was evaluated from, the row being scanned: a `$parent` in one
	/// reads that row, while a field in one reads the element and does not.
	/// A correlated SELECT nested in the predicate reads that document too:
	/// it binds the document the path was evaluated from as its `$parent`.
	#[test]
	fn parent_in_a_path_filter_or_lookup_reads_the_scanned_row() {
		for src in [
			"$vectors[WHERE $parent.enabled]",
			"$vectors[WHERE $parent.enabled].v",
			"[$vectors[WHERE $parent.enabled][0], 0f]",
			"$o.v[WHERE x = $parent.y]",
			"$r->edge[WHERE out = $parent.id]",
			"$vectors[WHERE id IN (SELECT VALUE id FROM t WHERE x = $parent.y)]",
		] {
			let expr = parse_cond(src);
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(reads_scanned_row(&expr, registry()), "{src}");
		}
		for src in [
			"$vectors[WHERE enabled]",
			"$vectors[WHERE enabled].v",
			"$r->edge[WHERE out = $x]",
			"$vectors[WHERE id IN (SELECT VALUE id FROM t)]",
		] {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
		}
	}

	/// An aliased destructure evaluates its bodies with the destructured value
	/// as the document: a `$parent` that a `[WHERE …]`, a lookup or a
	/// correlated SELECT in a body binds is that value, and so is the
	/// document an index expression or a method argument there reads. Only
	/// the expression destructured can read the row being scanned, and a
	/// `[WHERE …]` applied to the destructure's result, outside its bodies,
	/// still binds the row.
	#[test]
	fn a_destructure_body_reads_the_destructured_value() {
		for src in [
			"$o.{ v: [5f, 0f][WHERE $parent.enabled] }.v",
			"$o.{ v: person:a->likes[WHERE w = $parent.w][0].v }.v",
			"$o.{ v: (SELECT VALUE v FROM qv WHERE n = $parent.n)[0] }.v",
			"$o.{ v: $vs[i] }.v",
			"$o.{ v: $vs.at(i) }.v",
			"$o.{ v: $this.v }.v",
			"$o.{ a: { v: $vs[WHERE $parent.enabled] } }.a.v",
			"$o.{ v: { RETURN $vs[WHERE $parent.enabled] } }.v",
		] {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
		}
		for src in [
			"point.{ v: [5f, 0f][WHERE $parent.enabled] }.v",
			"{ p: point }.{ v: [5f, 0f][WHERE $parent.p] }.v",
			"$o[WHERE $parent.enabled][0].{ v }.v",
			"$o.{ v }.v[WHERE $parent.enabled][0]",
			"$vs[i].{ v: $parent }.v",
			"[$o.{ v: $parent }.v, point]",
		] {
			let expr = parse_cond(src);
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(reads_scanned_row(&expr, registry()), "{src}");
		}
		let found = |sql: &str| {
			first_row_dependent_knn_vector(&parse_cond(sql), registry()).map(|e| e.to_sql())
		};
		assert_eq!(found("v <|1,40|> $o.{ v: [5f, 0f][WHERE $parent.enabled] }.v"), None);
		assert!(found("v <|1,40|> point.{ v: [5f, 0f][WHERE $parent.enabled] }.v").is_some());
	}

	/// A SELECT subquery rescopes the row parameters: one that reads its
	/// enclosing row — `$parent` at its own level, or a row-scoped parameter
	/// as a source — reads the row being scanned; any other is evaluated
	/// once, wherever it appears. A SELECT nested in it rescopes again.
	#[test]
	fn a_subquery_is_computable_unless_it_reads_its_enclosing_row() {
		for src in [
			"(SELECT VALUE v FROM ONLY q:one)",
			"[(SELECT VALUE n FROM ONLY q:one), 0f]",
			"(SELECT VALUE v FROM q:one)[0]",
			"fn::q((SELECT VALUE v FROM ONLY q:one))",
			"(SELECT VALUE v FROM q WHERE $this.ok)[0]",
			"(SELECT VALUE (SELECT VALUE x FROM t WHERE y = $parent.z) FROM q)[0]",
		] {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
		}
		for src in [
			"(SELECT VALUE v FROM ONLY q WHERE id = $parent.ref)",
			"[(SELECT VALUE n FROM q WHERE x = $parent.x)[0], 0f]",
			"(SELECT VALUE v FROM $this)[0]",
			"(SELECT VALUE v FROM $parent.items)[0]",
		] {
			let expr = parse_cond(src);
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(reads_scanned_row(&expr, registry()), "{src}");
		}
	}

	/// Inside a SELECT of the vector, the WHERE condition and the projection
	/// are evaluated per row of that SELECT with the row as their document: a
	/// `[WHERE …]`, a lookup's clauses, a destructure body or a block there
	/// binds `$parent` to that row, and the vector is computable. A closure
	/// literal anywhere in the SELECT is a value, as it is at the vector's top
	/// level: one invoked and reading `$parent` fails when the vector is
	/// evaluated. A `$parent` at the SELECT's own level, in its source, or in
	/// a filter applied to it, is the row being scanned.
	#[test]
	fn parent_bound_inside_a_subquery_is_its_own_row() {
		for src in [
			"(SELECT VALUE [[5f, 0f]][WHERE $parent.enabled][0] FROM ONLY { enabled: true })",
			"(SELECT VALUE person:a->likes[WHERE w = $parent.w][0].v FROM ONLY { w: 4 })",
			"(SELECT VALUE [[5f, 0f]][WHERE $parent.enabled][0] FROM qv LIMIT 1)[0]",
			"(SELECT VALUE v FROM qv WHERE [1][WHERE $parent.enabled])[0]",
			"(SELECT VALUE { o: 1 }.{ v: [[5f, 0f]][WHERE $parent.o = 1][0] }.v FROM ONLY { e: 1 })",
			"(SELECT VALUE { RETURN $parent.v } FROM qv)[0]",
			"(SELECT VALUE fn::constant(|$x| $parent.point) FROM ONLY { enabled: true })",
			"(SELECT VALUE [1].map(|$y| $parent.v)[0] FROM ONLY { v: [5f, 0f] })",
			"(SELECT VALUE v FROM [1].map(|$y| { v: $parent.v }))[0]",
			"(SELECT VALUE v FROM ONLY $o.{ v: [[5f, 0f]][WHERE $parent.enabled][0] })",
			"(SELECT VALUE v FROM ONLY $o.{ v: person:a->likes[WHERE w = $parent.w][0].v })",
		] {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
		}
		for src in [
			"(SELECT VALUE v FROM qv WHERE n = $parent.point[0])[0]",
			"(SELECT VALUE $parent.v FROM qv)[0]",
			"(SELECT VALUE v FROM $parent.items)[0]",
			"(SELECT VALUE v FROM [{ v: [5f, 0f] }][WHERE $parent.enabled])[0]",
			"(SELECT VALUE $parent.vs[WHERE enabled][0] FROM ONLY { k: 1 })",
			"(SELECT VALUE v FROM ONLY $o.{ v: $parent.v })",
			"(SELECT VALUE $o.{ v: $parent.v }.v FROM ONLY { k: 1 })",
			"(SELECT VALUE v FROM ONLY $parent.{ v })",
			"(SELECT VALUE v FROM { RETURN [{ v: $parent.v }] })[0]",
		] {
			let expr = parse_cond(src);
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(reads_scanned_row(&expr, registry()), "{src}");
		}
	}

	/// A closure literal is a value at plan time, whatever its body reads: the
	/// body runs only when the closure is invoked, and a read of the row then
	/// is refused when the vector is evaluated. A closure that is only passed
	/// or stored leaves the vector computable; row reads around it still count.
	#[test]
	fn a_closure_literal_is_a_value_at_plan_time() {
		for src in [
			"array::map([1, 2], |$x| IF enabled THEN $x ELSE 0 END)",
			"array::map([1, 2], |$x| $this.enabled)",
			"$arr.map(|$x| enabled)",
			"(|$x| $x + point)(1)",
			"fn::constant(|$x| point)",
			"[|$x| point, 5f][1]",
			"{ f: |$x| $vs[WHERE $parent.enabled] }.f",
			"array::map([1, 2], |$x| $x * 2)",
			"array::map([1, 2], |$this| $this * 2)",
		] {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
		}
		for src in ["array::map([point], |$x| $x)", "fn::constant(|$x| $x, point)"] {
			let expr = parse_cond(src);
			assert!(reads_scanned_row(&expr, registry()), "{src}");
		}
		let found = |sql: &str| {
			first_row_dependent_knn_vector(&parse_cond(sql), registry()).map(|e| e.to_sql())
		};
		assert_eq!(
			found("v <|1,40|> array::map([1, 2], |$x| IF enabled THEN $x ELSE 0 END)"),
			None
		);
		assert_eq!(found("v <|1,40|> fn::constant(|$x| point)"), None);
	}

	/// A projection builtin reads the fields of the value in scope by name:
	/// of the row being scanned in a position evaluated against it, of the
	/// element inside a body evaluated against one. A user-defined function
	/// call is classified by its arguments.
	#[test]
	fn a_projection_builtin_reads_the_scanned_row() {
		for src in [
			"type::field('point')",
			"type::field($name)",
			"type::fields(['point'])",
			"[type::field($name), 0f]",
			"<array<float>> type::field($name)",
			"$m[type::field('i')]",
		] {
			let expr = parse_cond(src);
			assert!(reads_scanned_row(&expr, registry()), "{src}");
			assert!(reads_scanned_row(&expr, registry()), "{src}");
		}
		for src in ["$arr[WHERE type::field('x') = 1]", "fn::q($x)", "type::string($x)"] {
			let expr = parse_cond(src);
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
			assert!(!reads_scanned_row(&expr, registry()), "{src}");
		}
		assert_eq!(
			first_row_dependent_knn_vector(
				&parse_cond("v <|1,40|> type::field($name)"),
				registry()
			)
			.map(|e| e.to_sql())
			.as_deref(),
			Some("type::field($name)")
		);
	}

	/// The first KNN operator, of either form, whose right-hand side
	/// references the scanned row; a computable or residual one is not
	/// reported.
	#[test]
	fn first_row_dependent_knn_vector_reports_both_knn_forms() {
		let found = |sql: &str| {
			first_row_dependent_knn_vector(&parse_cond(sql), registry()).map(|e| e.to_sql())
		};
		assert_eq!(found("v <|1,EUCLIDEAN|> point").as_deref(), Some("point"));
		assert_eq!(found("v <|1,40|> [point, 0f]").as_deref(), Some("[point, 0f]"));
		assert_eq!(found("a = 1 AND v <|1,40|> $this.point").as_deref(), Some("$this.point"));
		assert!(found("v <|1,40|> $vectors[WHERE $parent.enabled]").is_some());
		assert!(found("v <|1,40|> (SELECT VALUE v FROM ONLY q WHERE id = $parent.r)").is_some());
		assert_eq!(found("v <|1,40|> $q"), None);
		assert_eq!(found("v <|1,40|> $parent.v"), None);
		assert_eq!(found("v <|1,EUCLIDEAN|> [1, 2]"), None);
		assert_eq!(found("v <|1,40|> (SELECT VALUE v FROM t)[0]"), None);
		assert_eq!(found("point = [1, 2]"), None);
	}

	/// A KNN operator inside a closure literal is no KNN operator of the
	/// condition: it is not reported, extracted, collected, refused or
	/// stripped, whatever its operands; one beside the closure still is.
	#[test]
	fn a_knn_operator_in_a_closure_is_not_one_of_the_condition() {
		for src in [
			"fn::constant(|$x| point <|1,EUCLIDEAN|> point)",
			"fn::constant(|$x| point <|1,40|> [1f, 0f])",
			"array::any([1], |$x| point <|1|> $q)",
		] {
			let cond = parse_cond(src);
			assert!(!has_knn_operator(&cond), "{src}");
			assert!(!has_knn_k_operator(&cond), "{src}");
			assert!(!has_knn_ktree_operator(&cond), "{src}");
			assert!(first_row_dependent_knn_vector(&cond, registry()).is_none(), "{src}");
			assert!(extract_bruteforce_knn(&cond, registry()).is_none(), "{src}");
			assert_eq!(strip_knn_from_condition(&cond).map(|c| c.to_sql()), Some(cond.to_sql()));
		}
		let cond = parse_cond("fn::constant(|$x| v <|1,40|> point) AND v <|2,EUCLIDEAN|> $q");
		assert!(has_knn_k_operator(&cond));
		assert!(first_row_dependent_knn_vector(&cond, registry()).is_none());
		assert_eq!(extract_bruteforce_knn(&cond, registry()).map(|p| p.k), Some(2));
	}

	/// The KNN operators a KNN source would serve are counted outside closure
	/// literals, wherever else they sit; a KTree operator is not one.
	#[test]
	fn count_knn_source_operators_counts_the_conditions_own() {
		for (src, count) in [
			("v <|1,EUCLIDEAN|> [1f, 2f]", 1),
			("v <|1,EUCLIDEAN|> [1f, 2f] AND v <|1,EUCLIDEAN|> [8f, 9f]", 2),
			("v <|1,40|> [1f, 2f] AND w <|1,40|> [8f, 9f]", 2),
			("v <|1,40|> [1f, 2f] AND v <|1,EUCLIDEAN|> [8f, 9f] AND a = 1", 2),
			("v <|1,40|> $q OR w <|1,40|> $q", 2),
			("v <|1,40|> $q AND fn::accept(|$x| v <|1,40|> $x)", 1),
			("v <|1|> $q AND w <|1,40|> $q", 1),
			("a = 1", 0),
		] {
			assert_eq!(count_knn_source_operators(&parse_cond(src)), count, "{src}");
		}
	}

	#[test]
	fn a_component_of_the_record_id_matches_its_column_however_it_is_spelled() {
		use crate::expr::{KeyNames, Kind, KindLiteral};
		use crate::val::Strand;
		let col = |s: &str| Idiom::from_str(s).expect("valid idiom");
		let path = |s: &str| {
			lower_expr(crate::expr::Expr::Idiom(crate::syn::idiom(s).expect("idiom")))
				.expect("lowers")
		};
		let names = KeyNames::new(vec![Strand::new("city"), Strand::new("time")], 2).unwrap();
		let kind = Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Datetime]));
		let key = KeyStructure::from_id_field(&kind, &names).unwrap();
		let key = Some(&key);
		// A name and a position address the same component, either way round.
		assert!(idiom_matches_column_in(&path("id[0]"), &col("city"), key));
		assert!(idiom_matches_column_in(&path("city"), &col("id[0]"), key));
		assert!(idiom_matches_column_in(&path("time"), &col("id[1]"), key));
		// Without the structure they are only the paths they spell.
		assert!(!idiom_matches_column_in(&path("id[0]"), &col("city"), None));
		assert!(idiom_matches_column_in(&path("city"), &col("city"), None));
		// Different components, a path beneath one, and a position past the
		// arity match nothing new.
		assert!(!idiom_matches_column_in(&path("time"), &col("city"), key));
		assert!(!idiom_matches_column_in(&path("city.x"), &col("id[0]"), key));
		assert!(!idiom_matches_column_in(&path("id[0].x"), &col("city"), key));
		assert!(!idiom_matches_column_in(&path("id[2]"), &col("city"), key));
		assert!(!idiom_matches_column_in(&path("other"), &col("city"), key));
	}

	#[test]
	fn knn_and_matches_strippers_leave_the_rest() {
		let cond = parse_cond("v <|2, EUCLIDEAN|> [1, 2] AND a = 1");
		assert!(has_knn_operator(&cond));
		assert!(has_knn_k_operator(&cond));
		assert_eq!(strip_knn_from_condition(&cond).map(|c| c.to_sql()).as_deref(), Some("a = 1"));
		let params = extract_bruteforce_knn(&cond, registry()).expect("brute-force KNN");
		assert_eq!(params.k, 2);
		assert_eq!(
			params.vector,
			KnnQueryVector::Literal(vec![Number::Int(1), Number::Int(2)].into())
		);
		let title = Idiom::from_str("title").expect("valid idiom");
		let op = |sql: &str| match parse_cond(sql) {
			LogicalExpr::Binary {
				op: BinaryOperator::Matches(op),
				..
			} => op,
			other => panic!("expected a MATCHES, got {}", other.to_sql()),
		};
		let (any, first) = (op("title @@ 'x'"), op("title @1@ 'x'"));
		let strip = |sql: &str, op: &MatchesOperator| {
			strip_fts_condition(&parse_cond(sql), &title, op, "x").map(|c| c.to_sql())
		};
		assert_eq!(strip("title @@ 'x' AND a = 1", &any).as_deref(), Some("a = 1"));
		assert_eq!(strip("title @@ 'x'", &any), None);
		// Only the scan's own probe is consumed: a MATCHES on another field,
		// with another reference or another query, stays in the residual.
		assert_eq!(
			strip("title @1@ 'x' AND body @2@ 'y'", &first).as_deref(),
			Some("body @2@ 'y'")
		);
		assert_eq!(
			strip("title @1@ 'x' AND title @2@ 'x'", &first).as_deref(),
			Some("title @2@ 'x'")
		);
		assert_eq!(
			strip("title @1@ 'x' AND title @1@ $q", &first).as_deref(),
			Some("title @1@ $q")
		);
	}

	#[test]
	fn point_lookup_is_found_in_the_and_chain_only() {
		let t = TableName::from("t");
		assert_eq!(
			extract_record_id_point_lookup(&parse_cond("a = 1 AND id = t:1"), &t)
				.map(|e| e.to_sql())
				.as_deref(),
			Some("t:1")
		);
		assert!(extract_record_id_point_lookup(&parse_cond("a = 1 OR id = t:1"), &t).is_none());
		assert!(extract_record_id_point_lookup(&parse_cond("id = u:1"), &t).is_none());
		assert!(extract_record_id_point_lookup(&parse_cond("id = t:1..5"), &t).is_none());
	}
}
