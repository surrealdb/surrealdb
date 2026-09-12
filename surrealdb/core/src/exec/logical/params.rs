//! Parameter resolution over the logical plan.
//!
//! Replaces `$param` references whose value is fixed for the whole statement
//! with their literal form, so later passes and the physical planner see a
//! literal where index analysis, constant folding and source classification
//! need one.
//!
//! Two clause positions are rewritten: a predicate, and a `FROM` source. Every
//! other clause keeps its parameters and resolves them per row. What is
//! substituted in those two positions, and what is deliberately left for
//! runtime:
//!
//! - **Substituted:** `LET` bindings, client bind parameters and session values from the context,
//!   and `DEFINE PARAM`s readable with `Permission::Full`. `bind_param` is the single rule, shared
//!   with the physical planner.
//! - **Unbound names:** a name with no value in the context and no `DEFINE PARAM` in the database
//!   evaluates to `NONE`, and is substituted as `NONE`: a `FROM $p` no binding reaches is the
//!   no-row source a literal `NONE` is, and a predicate on the name folds. The catalog has to have
//!   been asked: without a transaction and a database the name stays a runtime lookup.
//! - **Row-scoped names** (`BindContext::row_scoped`): bound per row at execution, so their
//!   plan-time value, if any, is the wrong one.
//! - **Names the plan binds itself:** a `LET` in a block or a `FOR` loop variable. Each is a
//!   runtime lookup wherever it appears, and a context value under the same name never wins.
//! - **Closure bodies:** not entered. A closure is invoked by later statements, so every parameter
//!   its body reads — its own arguments included — is resolved where it is invoked, from the values
//!   captured when the closure was created and the arguments of the call.
//! - **Layer columns:** a `Project`'s fields are neither a predicate nor a source, and a layer
//!   column's body comes from a schema definition rather than from the statement, so its parameters
//!   are bound where the definition is evaluated. A layer binds `$value` for its columns, so a plan
//!   with a layer treats `$value` as a name it binds itself, in the plans nested in a column's body
//!   included.
//! - **Lossy values:** a value whose literal form evaluates to something else
//!   (`substitution_is_lossy`) stays a parameter.
//!
//! A scalar subquery is part of the statement — it runs in the same
//! transaction with the same context — so its plan is resolved with the
//! enclosing one; a block body is an expression rather than a predicate or a
//! source and is not touched. A `Source` whose expression becomes a value
//! literal is reclassified as `Values`, so the leaf kinds a bound plan exposes
//! match what lowering would have produced had the literal been written in
//! the statement.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::ops::ControlFlow;

use super::{
	BindContext, classify_source, rewrite_deep, visit_binding, visit_binding_mut, visit_deep,
};
use crate::exec::planner::util::{
	ParamBinding, bind_param, substitution_is_lossy, value_to_logical_expr,
};
use crate::expr::logical::{
	LogicalExpr, LogicalFilter, LogicalForeach, LogicalJoin, LogicalLet, LogicalOp,
	LogicalPermissionFilter, LogicalSource, SourceRef,
};
use crate::val::Value;

/// Resolve every statement-scoped parameter in `op`. Names that cannot or
/// must not be resolved are left in place.
pub(super) async fn resolve_params(op: LogicalOp, bx: &BindContext<'_>) -> LogicalOp {
	// Phase 1: the names the plan references in a resolvable position, and
	// the names it binds itself.
	let (names, bound) = referenced_and_bound(&op);

	// Phase 2: look each name up once.
	let mut literals: HashMap<String, LogicalExpr> = HashMap::new();
	for name in names {
		if bound.contains(&name) {
			continue;
		}
		let value = match bind_param(&name, bx.ctx, bx.txn.as_ref(), bx.ns_db, bx.row_scoped).await
		{
			ParamBinding::Value(value) => value,
			ParamBinding::Unbound => Value::None,
			ParamBinding::Runtime => continue,
		};
		if substitution_is_lossy(&value) {
			continue;
		}
		literals.insert(name, value_to_logical_expr(value));
	}
	if literals.is_empty() {
		return op;
	}

	// Phase 3: substitute, then re-derive each leaf's kind from its new
	// expression.
	substitute_all(op, &literals)
}

/// Whether some name of `names` is read where [`resolve_params`] would
/// substitute a statement-scoped value for it — a predicate, a join key or
/// residual, a source expression — in `op` or in a plan nested in it, and is
/// not a name the plan binds itself.
pub(super) fn reads_in_resolvable_position(op: &LogicalOp, names: &[&str]) -> bool {
	let (referenced, bound) = referenced_and_bound(op);
	names.iter().any(|name| referenced.contains(*name) && !bound.contains(*name))
}

/// The names `op` references in a position [`resolve_params`] substitutes,
/// and the names it binds itself, in `op` and every plan nested in it.
fn referenced_and_bound(op: &LogicalOp) -> (HashSet<String>, HashSet<String>) {
	let mut names: HashSet<String> = HashSet::new();
	let mut bound: HashSet<String> = HashSet::new();
	visit_deep(op, &mut |node| {
		match node {
			LogicalOp::Let(LogicalLet {
				name,
				..
			}) => {
				bound.insert(name.clone());
			}
			LogicalOp::Foreach(LogicalForeach {
				param,
				..
			}) => {
				bound.insert(param.as_str().to_owned());
			}
			// A layer binds `$value` for each of its columns.
			LogicalOp::Project(_) => {
				if node.is_layer() {
					bound.insert("value".to_owned());
				}
			}
			LogicalOp::Filter(LogicalFilter {
				predicate,
				..
			})
			| LogicalOp::PermissionFilter(LogicalPermissionFilter {
				predicate,
				..
			}) => collect_names(predicate, &mut names),
			LogicalOp::Join(LogicalJoin {
				on,
				filter,
				..
			}) => {
				for (left, right) in on {
					collect_names(left, &mut names);
					collect_names(right, &mut names);
				}
				if let Some(filter) = filter {
					collect_names(filter, &mut names);
				}
			}
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => match source {
				SourceRef::Param(param) => {
					names.insert(param.as_str().to_owned());
				}
				SourceRef::RecordId(_) | SourceRef::Dynamic(_) => {
					let mut exprs = Vec::new();
					source.exprs(&mut exprs);
					for expr in exprs {
						collect_names(expr, &mut names);
					}
				}
				SourceRef::Table(_) | SourceRef::Mock(_) => {}
			},
			// Operators holding no predicate and no source expression.
			// Listed rather than matched with a wildcard: an operator added
			// later that carries either must be considered here, not silently
			// skipped.
			LogicalOp::Values(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Match(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Return(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => {}
		}
	});
	(names, bound)
}

/// `op` with `literals` substituted for the parameters in every resolvable
/// position, each leaf's kind re-derived from its new expression.
fn substitute_all(op: LogicalOp, literals: &HashMap<String, LogicalExpr>) -> LogicalOp {
	rewrite_deep(op, &mut |node| match node {
		LogicalOp::Filter(LogicalFilter {
			input,
			mut predicate,
		}) => {
			substitute(&mut predicate, literals);
			LogicalOp::Filter(LogicalFilter {
				input,
				predicate,
			})
		}
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			mut predicate,
			auth,
		}) => {
			substitute(&mut predicate, literals);
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				predicate,
				auth,
			})
		}
		LogicalOp::Join(mut join) => {
			for (left, right) in &mut join.on {
				substitute(left, literals);
				substitute(right, literals);
			}
			if let Some(filter) = &mut join.filter {
				substitute(filter, literals);
			}
			LogicalOp::Join(join)
		}
		LogicalOp::Source(LogicalSource {
			source,
			schema,
			with,
			for_update,
		}) => match source {
			SourceRef::Param(param) => match literals.get(param.as_str()) {
				Some(literal) => classify_source(literal.clone(), None, with, for_update),
				None => LogicalOp::Source(LogicalSource {
					source: SourceRef::Param(param),
					schema,
					with,
					for_update,
				}),
			},
			mut source @ SourceRef::RecordId(_) => {
				let mut exprs = Vec::new();
				source.exprs_mut(&mut exprs);
				for expr in exprs {
					substitute(expr, literals);
				}
				LogicalOp::Source(LogicalSource {
					source,
					schema,
					with,
					for_update,
				})
			}
			SourceRef::Dynamic(mut expr) => {
				substitute(&mut expr, literals);
				classify_source(expr, None, with, for_update)
			}
			source @ (SourceRef::Table(_) | SourceRef::Mock(_)) => {
				LogicalOp::Source(LogicalSource {
					source,
					schema,
					with,
					for_update,
				})
			}
		},
		// Operators holding no predicate and no source expression. Listed
		// rather than matched with a wildcard: an operator added later that
		// carries a statement expression must be considered here.
		other @ (LogicalOp::Values(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Match(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_)) => other,
	})
}

/// The parameter names whose value the statement fetches from the catalog
/// where it reads them, rather than holding it before the plan runs.
///
/// Such a read is not inert: the fetch judges the `DEFINE PARAM`'s
/// `PERMISSIONS` clause against the caller and is denied where the clause
/// denies, so a rule that drops the read unevaluated drops that error with it
/// (`super::rules::may_raise`). A name bound by the context, by a `LET`, by
/// the plan or per row is not one of these — its value is in hand before any
/// catalog read — and neither is a `DEFINE PARAM` readable with
/// `Permission::Full`, which [`bind_param`] resolves to its value here.
///
/// [`bind_param`] is the single rule, so this answers what the runtime does
/// for the same name. Row-scoped names are classified by the catalog rather
/// than assumed bound: the answer is about the name, not about the frame
/// reading it. Without a transaction and a database the catalog cannot be
/// asked, and every name the plan reads is one of these.
pub(super) async fn permission_checked_names(
	op: &LogicalOp,
	bx: &BindContext<'_>,
) -> HashSet<String> {
	let mut names: HashSet<String> = HashSet::new();
	visit_deep(op, &mut |node| {
		let Ok(()) = node.visit_scalars(&mut |scalar| {
			collect_names(scalar, &mut names);
			Ok::<_, Infallible>(())
		});
	});
	let mut checked = HashSet::new();
	for name in names {
		// `$this` and `$self` are answered from the row, or as `NONE`, before
		// any catalog read (`physical_expr::Param`), so neither is a fetch.
		if matches!(name.as_str(), "this" | "self") {
			continue;
		}
		if let ParamBinding::Runtime =
			bind_param(&name, bx.ctx, bx.txn.as_ref(), bx.ns_db, &[]).await
		{
			checked.insert(name);
		}
	}
	checked
}

/// The parameter names `expr` references, not entering a nested plan, whose
/// own predicates and sources are visited as nodes of their own, and not
/// entering a closure body, whose parameters are read where the closure is
/// invoked.
fn collect_names(expr: &LogicalExpr, names: &mut HashSet<String>) {
	let _ = visit_binding(expr, &mut |e| {
		if let LogicalExpr::Param(param) = e {
			names.insert(param.as_str().to_owned());
		}
		ControlFlow::<()>::Continue(())
	});
}

/// Replaces parameter references with the literals in `literals`, not
/// entering a nested plan or a closure body.
fn substitute(expr: &mut LogicalExpr, literals: &HashMap<String, LogicalExpr>) {
	let Ok(()) = visit_binding_mut(expr, &mut |e| {
		if let LogicalExpr::Param(param) = e
			&& let Some(literal) = literals.get(param.as_str())
		{
			*e = literal.clone();
		}
		Ok::<_, Infallible>(())
	});
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use std::sync::Arc;

	use surrealdb_strand::TableName;

	use super::super::lower_test as lower;
	use super::*;
	use crate::ctx::FrozenContext;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::planner::util::SELECT_ITERATION_PARAMS;
	use crate::expr::logical::{EvalFrame, LogicalField, LogicalFields, LogicalProject};
	use crate::expr::variables::Variables;
	use crate::expr::{BinaryOperator, Param};
	use crate::kvs::Datastore;
	use crate::val::{Closure, Set};

	async fn frozen_with(values: Vec<(&str, Value)>) -> FrozenContext {
		let ds = Datastore::new("memory").await.expect("datastore");
		let mut ctx = ds.setup_ctx().expect("context");
		for (name, value) in values {
			ctx.add_value(name.to_owned(), Arc::new(value));
		}
		ctx.freeze()
	}

	async fn bound(op: LogicalOp, values: Vec<(&str, Value)>) -> LogicalOp {
		let ctx = frozen_with(values).await;
		let registry = FunctionRegistry::with_builtins();
		let bx = BindContext {
			ctx: &ctx,
			txn: None,
			ns_db: None,
			auth: None,
			row_scoped: SELECT_ITERATION_PARAMS,
			registry: &registry,
		};
		resolve_params(op, &bx).await
	}

	fn int(n: i64) -> Value {
		Value::from(n)
	}

	fn source(table: &str) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(TableName::from(table)),
			schema: None,
			with: None,
			for_update: false,
		})
	}

	#[tokio::test]
	async fn substitutes_statement_scoped_params_in_a_predicate() {
		let plan =
			bound(lower("SELECT * FROM t WHERE a > $p"), vec![("p", int(5))]).await.to_string();
		assert!(plan.contains("Filter[a > 5]"), "{plan}");
	}

	#[tokio::test]
	async fn other_clauses_keep_their_parameters_for_runtime() {
		// A name outside a predicate or a source is resolved per row, where it
		// may mean something different each time.
		let plan = bound(
			lower("SELECT $p AS q FROM t ORDER BY a LIMIT $p START $p FETCH $p"),
			vec![("p", int(5))],
		)
		.await
		.to_string();
		for expected in ["$p AS q", "LIMIT $p", "START $p"] {
			assert!(plan.contains(expected), "missing {expected:?} in:\n{plan}");
		}
	}

	#[tokio::test]
	async fn leaves_row_scoped_and_unknown_params_alone() {
		// `$this` and `$parent` have a plan-time value here, and must still
		// not be substituted: their real value is bound per row. `$missing`
		// is bound without a database to consult, so it stays a runtime
		// lookup.
		let plan = bound(
			lower("SELECT * FROM t WHERE a = $this.b AND c = $parent.d AND e = $missing"),
			vec![("this", int(1)), ("parent", int(2))],
		)
		.await
		.to_string();
		for expected in ["$this.b", "$parent.d", "$missing"] {
			assert!(plan.contains(expected), "missing {expected:?} in:\n{plan}");
		}
	}

	#[tokio::test]
	async fn a_from_param_becomes_a_source_or_a_values_leaf() {
		let plan = bound(
			lower("SELECT * FROM $tb, $arr"),
			vec![
				("tb", Value::Table(TableName::from("person"))),
				("arr", Value::from(vec![int(1), int(2)])),
			],
		)
		.await;
		let leaves: Vec<_> = plan.children()[0].children().iter().map(LogicalOp::name).collect();
		assert_eq!(leaves, ["Source", "Values"]);
		let rendered = plan.to_string();
		assert!(rendered.contains("Source[person]"), "{rendered}");
		assert!(rendered.contains("Values[[1, 2]]"), "{rendered}");
	}

	#[tokio::test]
	async fn descends_into_scalar_subqueries() {
		// A scalar subquery runs in the statement's transaction with the
		// statement's context, so its predicate resolves with the enclosing one.
		let plan = bound(
			lower("SELECT * FROM t WHERE a IN (SELECT b FROM u WHERE c = $p) AND d = $p"),
			vec![("p", int(5))],
		)
		.await
		.to_string();
		assert!(plan.contains("c = 5"), "subquery predicate should resolve:\n{plan}");
		assert!(plan.contains("d = 5"), "outer reference should resolve:\n{plan}");
	}

	#[tokio::test]
	async fn does_not_enter_blocks_and_respects_closure_scope() {
		// A block body is an expression, not a predicate. A closure argument
		// is bound within the closure body alone: the body is left as it is,
		// and the same name outside the closure is the context's.
		let plan = bound(
			lower("SELECT { RETURN $p } AS b, |$q: int| $q AS c FROM t WHERE d = $p AND e = $q"),
			vec![("p", int(5)), ("q", int(6))],
		)
		.await
		.to_string();
		assert!(plan.contains("Eval[$p]"), "block body should be untouched:\n{plan}");
		assert!(plan.contains("|$q: int| $q"), "closure body should be untouched:\n{plan}");
		assert!(plan.contains("d = 5"), "outer reference should resolve:\n{plan}");
		assert!(plan.contains("e = 6"), "the name outside the closure is the context's:\n{plan}");
	}

	#[tokio::test]
	async fn descends_into_relational_subqueries() {
		let plan =
			bound(lower("SELECT * FROM (SELECT * FROM t WHERE a = $p)"), vec![("p", int(5))])
				.await
				.to_string();
		assert!(plan.contains("a = 5"), "{plan}");
	}

	#[tokio::test]
	async fn never_substitutes_a_name_a_let_or_a_loop_binds() {
		let plan = bound(
			lower("{ LET $x = 1; FOR $y IN [1] { SELECT * FROM t WHERE a = $x AND b = $y }; }"),
			vec![("x", int(7)), ("y", int(8))],
		)
		.await
		.to_string();
		assert!(plan.contains("a = $x AND b = $y"), "{plan}");
	}

	#[tokio::test]
	async fn a_layer_column_keeps_its_parameters() {
		// A column's body is a schema definition's, bound where the definition
		// is evaluated; a predicate above the layer still resolves.
		let op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(LogicalOp::Project(LogicalProject {
				input: Box::new(source("t")),
				fields: LogicalFields::Select(vec![
					LogicalField::All,
					LogicalField::column(
						"c",
						LogicalExpr::Param(Param::from("p".to_owned())),
						None,
						EvalFrame::computed(None),
					),
				]),
				omit: vec![],
			})),
			predicate: LogicalExpr::Binary {
				left: Box::new(LogicalExpr::Field("c".into())),
				op: BinaryOperator::MoreThan,
				right: Box::new(LogicalExpr::Param(Param::from("p".to_owned()))),
			},
		});
		let plan = bound(op, vec![("p", int(7))]).await.to_string();
		assert!(plan.contains("Project[*, $p AS c @computed]"), "{plan}");
		assert!(plan.contains("Filter[c > 7]"), "{plan}");
	}

	#[tokio::test]
	async fn a_layer_binds_value_for_the_plans_nested_in_its_columns() {
		use super::super::test_fixture::{Fixture, owner};
		// `$value` is bound per column when the layer evaluates it, so it is
		// never the unbound name the catalog would report it as — in a plan
		// nested in a column's body included, where it is a predicate.
		let fixture = Fixture::new("").await;
		let bound = fixture.bound_as(&owner(), vec![]).await;
		let bx = fixture.bind_context(&bound);
		let op = LogicalOp::Project(LogicalProject {
			input: Box::new(source("t")),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(
					"c",
					LogicalExpr::Subquery(Box::new(lower("SELECT * FROM u WHERE f = $value"))),
					None,
					EvalFrame::computed(None),
				),
			]),
			omit: vec![],
		});
		let plan = resolve_params(op, &bx).await.to_string();
		assert!(plan.contains("Filter[f = $value]"), "{plan}");
	}

	#[tokio::test]
	async fn a_set_is_substituted_as_a_set_literal() {
		// A set has a literal form of its own, so it is substituted like any
		// other value and the predicate no longer reads the parameter.
		let plan = bound(
			lower("SELECT * FROM t WHERE a IN $s"),
			vec![("s", Value::Set(Set::from(vec![int(1)])))],
		)
		.await
		.to_string();
		assert!(!plan.contains("$s"), "{plan}");
		assert!(plan.contains("a INSIDE {"), "{plan}");
	}

	#[tokio::test]
	async fn an_unbound_name_is_none_once_the_catalog_has_been_asked() {
		use super::super::test_fixture::{Fixture, owner};
		let fixture = Fixture::new(
			"DEFINE PARAM $open VALUE 3 PERMISSIONS FULL; \
			 DEFINE PARAM $gated VALUE 4 PERMISSIONS WHERE $auth.id = user:x;",
		)
		.await;
		let bound = fixture.bound_as(&owner(), vec![("ctx", int(1))]).await;
		let bx = fixture.bind_context(&bound);
		let plan = resolve_params(
			lower(
				"SELECT * FROM t, $absent WHERE a = $ctx AND b = $open AND c = $gated AND d = $absent",
			),
			&bx,
		)
		.await
		.to_string();
		// The context value and the readable `DEFINE PARAM` are literals; the
		// gated one is judged per read; the absent one is `NONE` everywhere,
		// the source included.
		assert!(plan.contains("a = 1 AND b = 3 AND c = $gated AND d = NONE"), "{plan}");
		assert!(plan.contains("Values[NONE]"), "{plan}");
	}

	#[tokio::test]
	async fn lossy_values_are_left_for_runtime() {
		// A closure has no literal form, so it must stay a parameter.
		let closure = Closure::Expr {
			args: Vec::new(),
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::None),
			captures: Variables::default(),
		};
		let plan = bound(
			lower("SELECT * FROM t WHERE a = $f"),
			vec![("f", Value::Closure(Box::new(closure)))],
		)
		.await
		.to_string();
		assert!(plan.contains("$f"), "{plan}");
	}
}
