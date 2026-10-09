//! Constant folding over the logical plan.
//!
//! Reduces row-independent subexpressions in a predicate to literals, so that
//! index analysis sees `field < <datetime literal>` rather than
//! `field < time::now() - 1d` and can turn it into a range.
//!
//! What folds is decided by `try_fold_to_literal`, the rule shared with the
//! physical planner: arithmetic on literals, calls to a pure, deterministic,
//! synchronous built-in whose arguments are all literals, `time::now()` once
//! per statement in a `WHERE` and nowhere else ([`is_condition`]), `x INSIDE []` to `false`,
//! AND/OR short-circuits against a boolean literal, and a path over a constant
//! that reads no storage (`{ name: 'x' }.name`, `['a', 'b'][1]`, the shapes a
//! substituted `$obj.field` or `$arr[1]` takes), so that a comparison or a
//! MATCHES against one is an index candidate. The last two eliminate an
//! operand, so they apply only where evaluating it provably reaches nothing,
//! and a field read reaches something whenever the rows are not plain records
//! or the field is derived or guarded. [`restricted_fields`] answers that from
//! the schema the resolve pass attached, with the same rule the scan operators
//! use for the predicate they evaluate: `None` — nothing may be discarded —
//! for a source without a schema, for a nested plan, for a checked caller
//! reading a table with a conditional table or field permission; otherwise
//! the computed fields, whose read runs their body.
//!
//! A call folds only when the caller's function capabilities permit it:
//! folding runs the function here, at plan time, so the name check that
//! guards evaluation has to guard the fold too, or a denied function would
//! execute for anyone who could get it into a predicate. A denied call is left
//! unfolded rather than rejected, so it fails at evaluation with the same error
//! as on the unfolded path.
//!
//! The rule owns no traversal. It is an [`super::rules::ExprRule`]: the
//! optimiser's driver applies it to every scalar of every node, at every node
//! of the plan and of every plan nested in one, and re-applies it to whatever
//! a node rule produces — so a constant an inlined function body brings with
//! it folds like one that was written. Within a scalar the fold is bottom-up,
//! so arguments fold before the call that takes them. Blocks and closures are
//! entered, because folding only ever replaces a subtree made entirely of
//! literals, and such a subtree means the same thing in every scope.
//!
//! Two things that look foldable are not, and each is declined where the
//! reason for it lives rather than by exempting a clause:
//!
//! - **A call the aggregate extractor matches by name.** It reads the projection after binding and
//!   matches on the name, and `count()` is a scalar function returning `1` as well as an aggregate,
//!   so a folded one would be a constant before the extractor saw it. `fold_condition_expressions`
//!   leaves any `registry.is_aggregate` call whole, wherever it sits, and still folds its
//!   arguments.
//! - **A `FROM` expression.** Resolving a source is the physical planner's job: it decides whether
//!   a call names a table to scan or produces the rows themselves. Folding one here would settle
//!   that question with a literal before the planner ever saw the call. The driver hands a source
//!   leaf's scalars to no expression rule.

use super::rewrite_deep;
use super::rules::ExprContext;
use crate::dbs::capabilities::Capabilities;
use crate::exec::function::FunctionRegistry;
use crate::exec::planner::util::{
	RestrictedFields, fold_expressions, source_field_reads_are_inert,
};
use crate::expr::logical::{
	LogicalDistinct, LogicalExpr, LogicalField, LogicalFields, LogicalFilter, LogicalLimit,
	LogicalLiteral, LogicalOnly, LogicalOp, LogicalPermission, LogicalPermissionFilter,
	LogicalProject, LogicalSort, LogicalSource, LogicalSplit, LogicalTimeout, LogicalUnionAll,
	LogicalVersionScope, SourceRef,
};

/// The fold rule: reduce every row-independent subexpression of `expr` to a
/// literal, and answer whether anything folded.
///
/// The driver supplies the context — which fields of the row beneath this
/// node are inert to read, and what the caller may execute — so the rule
/// itself is the fold and nothing else.
pub(super) fn apply(expr: &mut LogicalExpr, cx: &ExprContext<'_>) -> bool {
	fold_expressions(
		expr,
		cx.registry,
		cx.capabilities,
		cx.restricted,
		cx.aggregates_survive,
		cx.condition,
	)
}

/// The [`ExprContext::restricted`] for the scalars of `node`: what a read off
/// the row they are evaluated against reaches.
///
/// A node's row is its first input's. A node with no input has no row, and
/// answers as restrictively as a node whose input nothing is known about.
pub(super) fn restricted_for(node: &LogicalOp, checks: bool) -> Option<RestrictedFields> {
	restricted_fields(node.children().first()?, checks)
}

/// The folds that discard an operand, over the plan as the resolve pass left
/// it: `x AND false` where evaluating `x` reaches nothing, and `x INSIDE []`.
///
/// They run here rather than as an [`super::rules::ExprRule`] because whether
/// an operand may be discarded is read off the node beneath the predicate,
/// and only before the rules run is that node the one resolve put there — the
/// `identity` rule drops a `PermissionFilter[true]`, and its presence is the
/// only record that a table's permission was conditional. Every other fold
/// holds wherever the expression sits and runs in the driver instead.
///
/// `checks` is whether the caller is permission-checked, which decides
/// whether a conditional permission makes a table's field reads non-inert.
#[cfg(test)]
pub(super) fn fold_constants(
	op: LogicalOp,
	registry: &FunctionRegistry,
	capabilities: &Capabilities,
	checks: bool,
) -> LogicalOp {
	fold_constants_with(op, registry, capabilities, checks, true)
}

/// [`fold_constants`], folding a `time::now()` in a condition only where
/// `folds_clock`.
pub(super) fn fold_constants_with(
	op: LogicalOp,
	registry: &FunctionRegistry,
	capabilities: &Capabilities,
	checks: bool,
	folds_clock: bool,
) -> LogicalOp {
	rewrite_deep(op, &mut |mut node| {
		if !matches!(node, LogicalOp::Filter(_) | LogicalOp::PermissionFilter(_)) {
			return node;
		}
		let restricted = restricted_for(&node, checks);
		let cx = ExprContext {
			registry,
			capabilities,
			restricted: restricted.as_ref(),
			aggregates_survive: false,
			condition: folds_clock && is_condition(&node),
		};
		let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
			apply(scalar, &cx);
			Ok::<_, std::convert::Infallible>(())
		});
		node
	})
}

/// Whether `node` is a `FROM` leaf, whose scalar is the clause as written.
///
/// Resolving a source is the physical planner's job — it decides whether a
/// call names a table to scan or produces the rows itself — and the driver
/// re-classifies a rewritten one from its expression. Both leaves are left as
/// they were written so that decision reads what the statement said.
pub(super) fn is_from_leaf(node: &LogicalOp) -> bool {
	matches!(node, LogicalOp::Source(_) | LogicalOp::Values(_))
}

/// Whether the scalars of `node` are a condition: a `WHERE`, where
/// `time::now()` is one value for the statement.
///
/// A condition is where one value is needed: `at > time::now() - 1d` becomes
/// an index range only once the right side is a literal. In a projection or a
/// data clause each call is evaluated on its own, and `CREATE |t:50| SET at =
/// time::now()` writes the value each row's call read. A permission predicate
/// and a mutation's `WHERE` are evaluated per row as well: neither chooses an
/// access path.
pub(super) fn is_condition(node: &LogicalOp) -> bool {
	matches!(node, LogicalOp::Filter(_))
}

/// Whether a call the aggregate extractor knows must survive folding whole at
/// `node`.
///
/// True everywhere but a predicate. The extractor reads a projection after
/// binding and matches its calls by name, and a projection reaches it from
/// more node kinds than are worth enumerating — the `SELECT` inside a
/// reference clause among them — so an aggregate surviving is the default and
/// a node kind nobody thought about keeps its aggregates. A predicate is the
/// one place no extractor looks, and there a call that merely shares an
/// aggregate's name is just a call on its arguments.
pub(super) fn aggregates_survive(node: &LogicalOp) -> bool {
	!matches!(node, LogicalOp::Filter(_) | LogicalOp::PermissionFilter(_))
}

/// The fields whose read off a row of `input` is not inert, or `None` when no
/// field read is.
///
/// Walks the row-preserving operators down to the leaves. A table source
/// answers from its schema: `None` while the caller is checked and the table
/// or one of its fields carries a conditional permission (any `WHERE` clause,
/// whatever it folds to) — enforced for every row whatever the predicate
/// reads, so no read there is free — and otherwise
/// the leading segment of every `COMPUTED` field, whose read runs its body. A
/// layer passes the row through with its columns set, so it restricts what
/// its input restricts and every column, whose read runs the column's body
/// or guard. A source without a schema, a nested plan, a grouped or projected
/// input and every statement answer `None`; a literal source answers as
/// `source_field_reads_are_inert` does. A union restricts what any input
/// restricts.
fn restricted_fields(input: &LogicalOp, checks: bool) -> Option<RestrictedFields> {
	if input.is_layer() {
		let LogicalOp::Project(LogicalProject {
			input: beneath,
			fields,
			..
		}) = input
		else {
			unreachable!("a layer is a projection")
		};
		let mut out = restricted_fields(beneath, checks)?;
		if let LogicalFields::Select(columns) = fields {
			out.extend(
				columns
					.iter()
					.filter_map(LogicalField::output_path)
					.filter_map(|path| path.as_field().map(str::to_owned)),
			);
		}
		return Some(out);
	}
	match input {
		LogicalOp::Source(LogicalSource {
			source,
			schema,
			..
		}) => {
			if !source_field_reads_are_inert(std::slice::from_ref(input)) {
				return None;
			}
			match (source, schema) {
				(SourceRef::Table(_), Some(schema)) => {
					if checks
						&& schema.fields.iter().any(|field| {
							matches!(field.select_permission, LogicalPermission::Specific(_))
						}) {
						return None;
					}
					Some(
						schema
							.fields
							.iter()
							.filter(|field| field.computed.is_some())
							.filter_map(|field| field.path.root().map(|name| name.to_string()))
							.collect(),
					)
				}
				(SourceRef::Table(_), None) => None,
				(SourceRef::Dynamic(_), _) => Some(RestrictedFields::new()),
				(SourceRef::RecordId(_) | SourceRef::Mock(_) | SourceRef::Param(_), _) => None,
			}
		}
		LogicalOp::Values(_) => source_field_reads_are_inert(std::slice::from_ref(input)).then(RestrictedFields::new),
		LogicalOp::UnionAll(LogicalUnionAll {
			inputs,
		}) => {
			let mut out = RestrictedFields::new();
			for input in inputs {
				out.extend(restricted_fields(input, checks)?);
			}
			Some(out)
		}
		// A table permission in place because the caller is checked. A
		// conditional permission is evaluated for every row whatever the
		// predicate reads, so no read above it is free — including one whose
		// clause is the literal `true`, which the scan operators enforce like
		// any other conditional clause. Only the `false` a `NONE` permission
		// lowers to leaves the reads inert: it withholds every row before the
		// predicate is reached.
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			predicate,
			..
		}) => {
			if checks && !matches!(predicate, LogicalExpr::Literal(LogicalLiteral::Bool(false))) {
				return None;
			}
			restricted_fields(input, checks)
		}
		LogicalOp::Filter(LogicalFilter {
			input,
			..
		})
		| LogicalOp::Split(LogicalSplit {
			input,
			..
		})
		| LogicalOp::Distinct(LogicalDistinct {
			input,
		})
		| LogicalOp::Sort(LogicalSort {
			input,
			..
		})
		| LogicalOp::Limit(LogicalLimit {
			input,
			..
		})
		| LogicalOp::Timeout(LogicalTimeout {
			input,
			..
		})
		| LogicalOp::VersionScope(LogicalVersionScope {
			input,
			..
		})
		| LogicalOp::Only(LogicalOnly {
			input,
		}) => restricted_fields(input, checks),
		LogicalOp::Match(_)
		| LogicalOp::Write(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Aggregate(_)
		// A join's rows carry the matched right row under the alias, whose
		// reads the left input's declarations say nothing about.
		| LogicalOp::Join(_)
		// The user's projection reshapes the row, so nothing beneath it is a
		// field of the rows above.
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Info(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::Sleep(_) => None,
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_strand::TableName;

	use super::super::lower_test as lower;
	use super::*;
	use crate::dbs::capabilities::{Capabilities, Targets};
	use crate::expr::Kind;
	use crate::expr::logical::{
		ComputedBody, FieldPath, FieldSchema, GuardKind, LogicalOp, Schema, lower_expr,
	};

	fn folded(sql: &str) -> LogicalOp {
		folded_with(sql, &Capabilities::all())
	}

	fn folded_with(sql: &str, capabilities: &Capabilities) -> LogicalOp {
		fold_constants(lower(sql), &FunctionRegistry::with_builtins(), capabilities, true)
	}

	fn expr(sql: &str) -> LogicalExpr {
		let ast = crate::syn::parse(&format!("RETURN {sql}")).expect("expression should parse");
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Return(out)) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a RETURN");
		};
		lower_expr(out.what).expect("expression should lower")
	}

	/// `SELECT * FROM t WHERE <predicate>` with `t` resolved to `schema`.
	fn over(schema: Option<Schema>, predicate: &str) -> LogicalOp {
		LogicalOp::Project(LogicalProject {
			input: Box::new(LogicalOp::Filter(LogicalFilter {
				input: Box::new(LogicalOp::Source(LogicalSource {
					source: SourceRef::Table(TableName::from("t")),
					schema: schema.map(Arc::new),
					with: None,
					for_update: false,
				})),
				predicate: expr(predicate),
			})),
			fields: crate::expr::logical::LogicalFields::all(),
			omit: vec![],
		})
	}

	fn plain_closed() -> Schema {
		Schema::closed(vec![
			FieldSchema::new(FieldPath::field("a"), Kind::Int),
			FieldSchema::new(FieldPath::field("b"), Kind::Int),
		])
	}

	#[test]
	fn folds_arithmetic_and_pure_calls_in_a_predicate() {
		let plan = folded("SELECT * FROM t WHERE a > 1 + 1 AND b = math::max([1, 2])").to_string();
		assert!(plan.contains("Filter[a > 2 AND b = 2]"), "{plan}");
	}

	/// A path over a constant object folds to the value it reads, so a
	/// comparison against `$obj.field` is a comparison against a literal.
	#[test]
	fn a_field_access_on_a_constant_object_folds() {
		let plan =
			folded("SELECT * FROM t WHERE a = { data: { org: 'acme' } }.data.org").to_string();
		assert!(plan.contains("Filter[a = 'acme']"), "{plan}");
	}

	#[test]
	fn an_aggregate_name_in_a_predicate_is_just_a_call() {
		// No extractor reads a predicate, so a call that happens to share an
		// aggregate's name folds there like any other call on literals.
		let plan = folded("SELECT * FROM t WHERE b = math::max([1, 2])").to_string();
		assert!(plan.contains("Filter[b = 2]"), "{plan}");
	}

	/// The driver applies [`apply`] at every node, so these read the whole
	/// optimiser rather than the pre-pass above.
	mod as_an_expression_rule {
		use super::super::super::lower_test;
		use super::super::super::rules::test_support::{rewritten, rewritten_sql};
		use crate::expr::logical::{LogicalExpr, LogicalOp};

		/// `time::now()` is one value for the statement in a `WHERE`, where an
		/// index range needs a literal, and a call evaluated per row in the
		/// projection of the same statement.
		#[test]
		fn the_clock_folds_in_a_where_and_not_in_a_projection() {
			let plan = rewritten_sql("SELECT time::now() AS at FROM t WHERE ts < time::now()");
			assert!(plan.contains("time::now() AS at"), "the projection keeps the call:\n{plan}");
			assert!(plan.contains("ts < d'"), "the condition folds to a literal:\n{plan}");
		}

		/// Entropy is drawn per row even in a condition: `WHERE rand::float() <
		/// 0.1` samples each row, and one draw would keep all of them or none.
		#[test]
		fn entropy_is_not_folded_even_in_a_where() {
			let plan = rewritten_sql("SELECT * FROM t WHERE rand::float() < 0.1");
			assert!(plan.contains("::float() < 0.1"), "the condition keeps the call:\n{plan}");
		}

		/// A data clause evaluates `time::now()` once per subject, so each
		/// created record holds the value its own call read.
		#[test]
		fn the_clock_is_not_folded_in_a_data_clause() {
			let plan = rewritten(lower_test("CREATE |t:3| SET at = time::now()"));
			let LogicalOp::Write(write) = &plan else {
				panic!("a CREATE lowers to a write:\n{plan}");
			};
			let data = write.data.as_ref().expect("the SET clause is in the plan");
			assert!(
				data.values().iter().any(|value| matches!(value, LogicalExpr::Call(_))),
				"the assignment keeps the call: {data:?}"
			);
		}

		#[test]
		fn an_aggregate_survives_a_projection_that_folds_around_it() {
			// `count()` is a scalar function returning 1 as well as an
			// aggregate, and the extractor matches it by name after binding,
			// so it survives whole. Everything beside it in the same
			// projection still folds.
			let plan = rewritten_sql("SELECT count(), 1 + 1 AS s FROM t GROUP ALL");
			assert!(plan.contains("count()"), "{plan}");
			assert!(plan.contains("2 AS s"), "{plan}");
		}

		#[test]
		fn every_clause_folds_not_only_the_predicate() {
			let plan = rewritten_sql("SELECT * FROM t WHERE a LIMIT 1 + 1 START 2 - 1");
			assert!(plan.contains("LIMIT 2"), "{plan}");
			assert!(plan.contains("START 1"), "{plan}");
			let plan = rewritten_sql("SELECT string::uppercase('a') AS c FROM t");
			assert!(plan.contains("'A' AS c"), "{plan}");
			let plan = rewritten_sql("RETURN 1 + 1");
			assert!(plan.contains("Eval[2]"), "{plan}");
		}

		#[test]
		fn a_from_leaf_is_left_as_it_was_written() {
			// Which rows a `FROM` expression names is the physical planner's
			// to decide from the expression as written, so neither leaf folds.
			let plan = rewritten_sql("SELECT * FROM type::table('person'), [1 + 1]");
			assert!(plan.contains("type::table('person')"), "{plan}");
			assert!(plan.contains("[1 + 1]"), "{plan}");
		}
	}

	#[test]
	fn leaves_row_dependent_and_nondeterministic_expressions() {
		let plan = folded("SELECT * FROM t WHERE a > rand::int(1, 10) AND b = c + 1").to_string();
		// `rand` is a keyword, so the renderer backtick-escapes it.
		assert!(plan.contains("`rand`::int(1, 10)"), "{plan}");
		assert!(plan.contains("c + 1"), "{plan}");
	}

	#[test]
	fn time_now_is_folded_once_per_statement() {
		let plan = folded("SELECT * FROM t WHERE ts < time::now() - 1d").to_string();
		assert!(!plan.contains("time::now"), "{plan}");
		assert!(plan.contains("ts < d'"), "should be a datetime literal:\n{plan}");
	}

	#[test]
	fn boolean_short_circuits_collapse_against_a_literal() {
		let plan = folded("SELECT * FROM t WHERE true AND b = 1").to_string();
		assert!(plan.contains("Filter[b = 1]"), "{plan}");
		let plan = folded("SELECT * FROM t WHERE 1 IN [] AND b = 1").to_string();
		assert!(plan.contains("Filter[false]"), "{plan}");
	}

	#[test]
	fn a_short_circuit_that_would_discard_a_field_read_needs_the_schema() {
		// Over an unresolved source nothing is known about `a`, so the read
		// survives for runtime.
		let plan = folded("SELECT * FROM t WHERE a IN [] AND b = 1").to_string();
		assert!(!plan.contains("Filter[false]"), "must not discard the read of `a`:\n{plan}");
		assert!(plan.contains("a INSIDE []"), "{plan}");

		// Over a closed schema of plain stored fields the read is a key lookup
		// and the predicate folds to `false`.
		let plan = fold_constants(
			over(Some(plain_closed()), "a IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			true,
		)
		.to_string();
		assert!(plan.contains("Filter[false]"), "{plan}");
	}

	#[test]
	fn a_computed_field_read_is_never_discarded() {
		let mut c = FieldSchema::new(FieldPath::field("c"), Kind::Int);
		c.computed = Some(ComputedBody {
			expr: expr("a + 1"),
			kind: Some(Kind::Int),
			auth: None,
		});
		let mut schema = plain_closed();
		schema.fields.push(c);
		let plan = fold_constants(
			over(Some(schema), "c IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			true,
		)
		.to_string();
		assert!(plan.contains("c INSIDE []"), "reading `c` runs its body:\n{plan}");
	}

	#[test]
	fn a_layer_restricts_its_columns_and_passes_its_input_through() {
		use crate::expr::logical::{EvalFrame, LogicalField, LogicalFields};

		// Above a layer deriving `c`, a read of `c` runs the body and is kept;
		// a read of the stored `a` beneath it is a key lookup and folds.
		let layer = LogicalOp::Project(LogicalProject {
			input: Box::new(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from("t")),
				schema: Some(Arc::new(plain_closed())),
				with: None,
				for_update: false,
			})),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(
					"c",
					expr("a + 1"),
					Some(Kind::Int),
					EvalFrame::computed(None),
				),
			]),
			omit: vec![],
		});
		let over_layer = |predicate: &str| {
			LogicalOp::Filter(LogicalFilter {
				input: Box::new(layer.clone()),
				predicate: expr(predicate),
			})
		};
		let plan = fold_constants(
			over_layer("c IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			true,
		)
		.to_string();
		assert!(plan.contains("Filter[c INSIDE []"), "reading `c` runs its body:\n{plan}");
		let plan = fold_constants(
			over_layer("a IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			true,
		)
		.to_string();
		assert!(plan.contains("Filter[false]"), "{plan}");
	}

	#[test]
	fn a_conditional_field_permission_makes_every_read_non_inert_for_a_checked_caller() {
		let mut secret = FieldSchema::new(FieldPath::field("secret"), Kind::String);
		secret.select_permission = LogicalPermission::Specific(expr("$auth.id = owner"));
		let mut schema = plain_closed();
		schema.fields.push(secret);
		let checked = fold_constants(
			over(Some(schema.clone()), "a IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			true,
		)
		.to_string();
		assert!(checked.contains("a INSIDE []"), "{checked}");
		// A caller the runtime does not check reads plain stored fields freely.
		let unchecked = fold_constants(
			over(Some(schema), "a IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			false,
		)
		.to_string();
		assert!(unchecked.contains("Filter[false]"), "{unchecked}");
	}

	#[test]
	fn descends_into_scalar_subqueries() {
		let plan =
			folded("SELECT * FROM t WHERE a IN (SELECT b FROM u WHERE c = 1 + 1) AND d = 1 + 1")
				.to_string();
		assert!(plan.contains("c = 2"), "the nested plan is bound with the statement:\n{plan}");
		assert!(plan.contains("d = 2"), "{plan}");
	}

	#[test]
	fn source_expressions_are_left_for_the_physical_planner() {
		// Source resolution belongs to the physical planner, which decides
		// whether the call names a table to scan or yields the rows itself.
		let plan = folded("SELECT * FROM type::table('person'), [1 + 1]").to_string();
		assert!(plan.contains("type::table('person')"), "{plan}");
		assert!(plan.contains("Values[[1 + 1]]"), "{plan}");
	}

	#[test]
	fn a_denied_function_is_left_unfolded() {
		// Folding runs the function at plan time, so a caller who may not
		// execute `math::max` must not get it folded either. The call is left
		// in place rather than rejected, so it fails at evaluation with the
		// same error it would raise had it never been a fold candidate.
		let denied = Capabilities::all().with_functions(Targets::None);
		let plan = folded_with("SELECT * FROM t WHERE b = math::max([1, 2])", &denied).to_string();
		assert!(plan.contains("math::max"), "denied call should survive the fold:\n{plan}");

		// The arithmetic beside it is not a call and still folds, so the gate
		// is on the call and not on folding as a whole.
		let plan = folded_with("SELECT * FROM t WHERE a > 1 + 1", &denied).to_string();
		assert!(plan.contains("Filter[a > 2]"), "{plan}");
	}

	#[test]
	fn a_permission_predicate_is_folded_like_a_user_one() {
		let plan = fold_constants(
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input: Box::new(LogicalOp::Source(LogicalSource {
					source: SourceRef::Table(TableName::from("t")),
					schema: None,
					with: None,
					for_update: false,
				})),
				predicate: expr("a > 1 + 1"),
				auth: None,
				guard: GuardKind::Read,
			}),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			true,
		)
		.to_string();
		assert!(plan.contains("PermissionFilter[a > 2]"), "{plan}");
	}

	/// `SELECT * FROM t WHERE <predicate>` over `t` with the table permission
	/// `guard` in place and a closed schema of plain stored fields.
	fn over_permission(guard: &str, predicate: &str) -> LogicalOp {
		LogicalOp::Project(LogicalProject {
			input: Box::new(LogicalOp::Filter(LogicalFilter {
				input: Box::new(LogicalOp::PermissionFilter(LogicalPermissionFilter {
					input: Box::new(LogicalOp::Source(LogicalSource {
						source: SourceRef::Table(TableName::from("t")),
						schema: Some(Arc::new(plain_closed())),
						with: None,
						for_update: false,
					})),
					predicate: expr(guard),
					auth: None,
					guard: GuardKind::Read,
				})),
				predicate: expr(predicate),
			})),
			fields: crate::expr::logical::LogicalFields::all(),
			omit: vec![],
		})
	}

	#[test]
	fn a_conditional_table_permission_makes_every_read_non_inert_however_it_folds() {
		// `WHERE true` is a conditional clause the scan enforces per row, so
		// the field read under it is kept for the checked caller...
		let checked = fold_constants(
			over_permission("true", "a IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			true,
		)
		.to_string();
		assert!(checked.contains("a INSIDE []"), "{checked}");
		// ...and discarded for a caller the runtime does not check.
		let unchecked = fold_constants(
			over_permission("true", "a IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			false,
		)
		.to_string();
		assert!(unchecked.contains("Filter[false]"), "{unchecked}");
		// A permission that withholds every row reaches no read at all.
		let denied = fold_constants(
			over_permission("false", "a IN [] AND b = 1"),
			&FunctionRegistry::with_builtins(),
			&Capabilities::all(),
			true,
		)
		.to_string();
		assert!(denied.contains("Filter[false]"), "{denied}");
	}
}
