//! Lowering of the GQL AST onto the declarative [`MatchPlan`] IR.
//!
//! Implements the normative design in `doc/gql/V2_DESIGN.md` §8: a parsed
//! [`GqlQuery`] becomes an [`Expr::Match`] holding a [`MatchPlan`] (the
//! language-neutral binding-table plan node), wrapped in a [`LogicalPlan`]. No
//! SurrealQL surface AST is produced; the streaming execution planner compiles
//! the [`MatchPlan`] into operators.
//!
//! Layout: [`binding`] runs the variable-resolution semantic pass over the
//! whole query (the binding registry, hidden bindings, anchorability, and the
//! node-variable-reuse-as-join-key / repeated-edge / kind-mismatch rules),
//! [`pattern`] emits each clause's [`PatternPlan`]s and NNF-split predicates
//! with their dependency sets, [`expr`] lowers expressions with uniform binding
//! addressing and the three-valued-logic guards, [`naming`] owns the
//! column-name and reserved-name rules, and this module dispatches and
//! assembles the output spec (R7/R8) and the final [`LogicalPlan`].

mod binding;
mod expr;
mod mutation;
mod naming;
mod pattern;

#[cfg(test)]
mod test;

use reblessive::{Stack, Stk};
use surrealdb_types::ToSql;

use self::binding::Registry;
use self::expr::Scope;
use crate::expr::match_plan::{MatchColumn, MatchOrder, MatchOutput, MatchPlan, MatchStage};
use crate::expr::plan::{LogicalPlan, TopLevelExpr};
use crate::expr::{Expr, Idiom, Literal, Param};
use crate::gql::ast::{
	GqlExpr, GqlGroupItem, GqlLiteral, GqlQuery, GqlStep, LinearQuery, MatchItem, OrderItem,
	ReturnClause, ReturnItems, SetQuantifier,
};
use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::token::Span;

/// Lowers a parsed GQL query into a [`LogicalPlan`] carrying a single
/// [`Expr::Match`].
///
/// Runs on a [`reblessive`] stack: the GQL AST contains arbitrarily deep
/// expression chains which the machine stack must not recurse over.
pub(super) fn lower(query: GqlQuery) -> Result<LogicalPlan, SyntaxError> {
	let LinearQuery {
		steps,
		ret,
		span,
	} = query.program;
	let mut stack = Stack::new();
	let plan = stack.enter(|stk| lower_program(stk, &steps, ret.as_ref(), span)).finish()?;
	Ok(LogicalPlan {
		expressions: vec![TopLevelExpr::Expr(Expr::Match(Box::new(plan)))],
	})
}

/// Lowers a whole linear query — `MATCH`/`OPTIONAL` reads and
/// `INSERT`/`SET`/`REMOVE`/`DELETE` mutations interleaved in textual order, plus
/// an optional `RETURN` — into a [`MatchPlan`].
///
/// A single ordered pass threads the binding registry through the steps: each
/// read clause declares and lowers its bindings against the registry built so
/// far (so a later `MATCH` can anchor on a variable an earlier `INSERT`
/// created), and each mutation lowers and extends the registry. The output spec
/// lowers last, against the complete registry. For a read-only query (no
/// mutation) the per-step walk produces exactly the previous read-body lowering.
async fn lower_program(
	stk: &mut Stk,
	steps: &[GqlStep],
	ret: Option<&ReturnClause>,
	span: Span,
) -> Result<MatchPlan, SyntaxError> {
	reject_leading_optional(steps)?;

	let mut analyzer = binding::Analyzer::new();
	let mut stages: Vec<MatchStage> = Vec::new();
	for step in steps {
		match step {
			GqlStep::Read(item) => {
				// `read` declares the item's bindings (anchorability checked
				// against the registry built so far, which includes earlier
				// INSERT-created variables) and returns one [`ClauseBindings`] per
				// flattened clause; each lowers against the registry so far.
				for clause_bindings in analyzer.read(item)? {
					let clause =
						pattern::lower_clause(stk, &clause_bindings, analyzer.registry()).await?;
					stages.push(MatchStage::Read(clause));
				}
			}
			GqlStep::Mutate(stmt) => {
				for mutation in
					mutation::lower_statement(stk, analyzer.registry_mut(), stmt).await?
				{
					stages.push(MatchStage::Mutate(mutation));
				}
			}
		}
	}

	let registry = analyzer.into_registry();

	// A query needs at least one read clause or mutation (a bare `RETURN` has no
	// `MATCH`).
	if stages.is_empty() {
		bail!(
			"A query without a MATCH clause is not supported yet",
			@span => "start the query with a MATCH clause"
		);
	}

	let output = match ret {
		Some(ret) => Some(lower_output(stk, ret, &registry).await?),
		None => {
			// A read-only query must project a RETURN; a mutation-only query
			// need not (it runs for its side effects and returns nothing).
			if !stages.iter().any(|s| matches!(s, MatchStage::Mutate(_))) {
				bail!(
					"A GQL query must end with a RETURN clause",
					@span => "add a `RETURN …` clause"
				);
			}
			None
		}
	};

	Ok(MatchPlan {
		bindings: registry.into_defs(),
		stages,
		output,
	})
}

/// Rejects a query that LEADS with `OPTIONAL`: an `OPTIONAL` is a left-outer
/// join and needs a preceding step (a `MATCH`, or a mutation that seeds the
/// binding table) to join against — there is nothing to left-outer against
/// otherwise, and the planner relies on the first read unit being mandatory.
fn reject_leading_optional(steps: &[GqlStep]) -> Result<(), SyntaxError> {
	if let Some(GqlStep::Read(MatchItem::Optional(block))) = steps.first() {
		bail!(
			"A query cannot start with OPTIONAL MATCH: OPTIONAL is a left-outer join and needs a \
			 preceding MATCH to join against",
			@block.span => "begin with a plain `MATCH …` clause before any `OPTIONAL`"
		);
	}
	Ok(())
}

/// A projected column: its final name (the row object key) and the lowered
/// binding-row value expression.
struct Column {
	name: String,
	expr: Expr,
	/// A sort-only column materialised so an aggregating query can ORDER BY a
	/// value it does not project (see [`MatchColumn::hidden`]); dropped before
	/// the rows are returned. `false` for user-projected columns.
	hidden: bool,
}

/// Lowers the RETURN clause into the [`MatchOutput`] spec: the projected
/// columns (R8), the DISTINCT flag, the resolved ORDER BY keys (R7) and the
/// SKIP/LIMIT counts.
async fn lower_output(
	stk: &mut Stk,
	ret: &ReturnClause,
	registry: &Registry,
) -> Result<MatchOutput, SyntaxError> {
	// GROUP BY keys lower first (no aggregates permitted in a key) so the
	// RETURN-item lowering can validate every non-aggregate column against them.
	let group_keys = lower_group_keys(stk, &ret.group_by, registry).await?;

	let (mut columns, aggregating) = lower_return_items(stk, ret, registry, &group_keys).await?;
	let distinct = matches!(ret.quantifier, Some(SetQuantifier::Distinct));

	// ORDER BY resolution may append hidden sort-only columns (an aggregating
	// query ordering by a value it does not project), so `columns` is mutable.
	let mut order = Vec::with_capacity(ret.order_by.len());
	for item in &ret.order_by {
		order.push(
			lower_order_item(stk, item, &mut columns, &group_keys, distinct, aggregating, registry)
				.await?,
		);
	}

	let skip = match &ret.skip {
		Some(skip) => Some(lower_count(skip)?),
		None => None,
	};
	let limit = match &ret.limit {
		Some(limit) => Some(lower_count(limit)?),
		None => None,
	};

	Ok(MatchOutput {
		columns: columns
			.into_iter()
			.map(|c| MatchColumn {
				name: c.name,
				expr: c.expr,
				hidden: c.hidden,
			})
			.collect(),
		distinct,
		// `aggregating` is `any aggregate column || GROUP BY present`. When the
		// only trigger is an aggregate (no GROUP BY) the key list is empty, which
		// the planner reads as GROUP ALL.
		group_by: aggregating.then_some(group_keys),
		order,
		skip,
		limit,
	})
}

/// Lowers the GROUP BY grouping elements to binding-row key expressions. A key
/// may not itself contain an aggregate (rejected by [`expr::lower_value`] under
/// a non-aggregate scope).
async fn lower_group_keys(
	stk: &mut Stk,
	items: &[GqlGroupItem],
	registry: &Registry,
) -> Result<Vec<Expr>, SyntaxError> {
	let scope = Scope {
		registry,
		allow_aggregates: false,
	};
	let mut keys = Vec::with_capacity(items.len());
	for item in items {
		keys.push(expr::lower_value(stk, &item.expr, &scope).await?.into());
	}
	Ok(keys)
}

/// Lowers the RETURN items into named columns (R8): explicit aliases win,
/// unaliased items are named by their verbatim source text, `RETURN *` expands
/// to the user-named bindings (incl. group and path variables) in alphabetical
/// order, and duplicate column names are rejected.
async fn lower_return_items(
	stk: &mut Stk,
	ret: &ReturnClause,
	registry: &Registry,
	group_keys: &[Expr],
) -> Result<(Vec<Column>, bool), SyntaxError> {
	// Aggregates are permitted in RETURN value position.
	let scope = Scope {
		registry,
		allow_aggregates: true,
	};
	let mut columns: Vec<Column> = Vec::new();
	match &ret.items {
		ReturnItems::Star => {
			// `RETURN *` carries no aggregates, so the only way it could be an
			// aggregating query is an attached GROUP BY — which has no meaning
			// over `*`.
			if !group_keys.is_empty() {
				bail!(
					"RETURN * cannot be combined with GROUP BY",
					@ret.span => "list the grouping keys and aggregates explicitly"
				);
			}
			let mut names: Vec<&str> = registry
				.bindings()
				.iter()
				.filter(|b| b.user_named)
				.map(|b| b.name.as_str())
				.collect();
			names.sort_unstable();
			if names.is_empty() {
				bail!(
					"RETURN * requires at least one named pattern variable",
					@ret.span => "name a pattern element or list the return items explicitly"
				);
			}
			for name in names {
				// `RETURN *` returns the whole binding value (a group or path
				// variable surfaces its composite value, never a field).
				columns.push(Column {
					name: name.to_owned(),
					expr: Expr::Idiom(Idiom::field(name)),
					hidden: false,
				});
			}
			Ok((columns, false))
		}
		ReturnItems::Items(items) => {
			// The query aggregates when any RETURN item carries an aggregate, or
			// a GROUP BY is present.
			let aggregating = !group_keys.is_empty()
				|| items.iter().any(|i| expr::gql_contains_aggregate(&i.expr));

			for item in items {
				let (name, name_span) = naming::column_name(item)?;
				if columns.iter().any(|c| c.name == name) {
					bail!(
						"Duplicate column name `{name}`",
						@name_span => "use `AS` to give the items distinct column names"
					);
				}
				let is_aggregate = expr::gql_contains_aggregate(&item.expr);
				// `lower_value` builds `sql::Expr`; the IR is `expr::Expr`.
				let lowered: Expr = expr::lower_value(stk, &item.expr, &scope).await?.into();

				// Strict GQL/SQL: when aggregating, a non-aggregate column must be
				// determined by the GROUP BY keys — either a key itself or a value
				// built only from grouped keys (e.g. `GROUP BY a; RETURN a.name`,
				// which is constant within each group). A genuinely ungrouped
				// column is rejected (no silent first-value).
				if aggregating && !is_aggregate && !column_is_grouped(&lowered, group_keys) {
					bail!(
						"RETURN item `{name}` must be a GROUP BY key, an aggregate, or determined by \
						 the GROUP BY keys",
						@item.expr.span() => "add it to GROUP BY or wrap it in an aggregate"
					);
				}

				columns.push(Column {
					name,
					expr: lowered,
					hidden: false,
				});
			}
			Ok((columns, aggregating))
		}
	}
}

/// Lowers an ORDER BY item (R7).
///
/// Three regimes, by where the Sort runs:
/// - DISTINCT: Sort runs over the projected output, so the key must name a returned column
///   (standard SQL for `SELECT DISTINCT`); any other key is rejected.
/// - aggregating (non-DISTINCT): Sort runs after the `Aggregate`. The key may be a returned column,
///   a grouping key, a value determined by the grouping keys, or an aggregate — a non-projected one
///   is materialised as a hidden sort-only column (dropped before output).
/// - plain: Sort runs pre-projection over the binding rows, so any binding-row expression is valid;
///   a key naming a RETURN column sorts on that column's underlying expression.
async fn lower_order_item(
	stk: &mut Stk,
	item: &OrderItem,
	columns: &mut Vec<Column>,
	group_keys: &[Expr],
	distinct: bool,
	aggregating: bool,
	registry: &Registry,
) -> Result<MatchOrder, SyntaxError> {
	if item.nulls_first.is_some() {
		bail!("`NULLS FIRST`/`NULLS LAST` ordering is not supported yet", @item.span);
	}
	let ascending = item.ascending.unwrap_or(true);
	// An aggregate may appear in ORDER BY only when the query already aggregates.
	let scope = Scope {
		registry,
		allow_aggregates: aggregating,
	};

	if distinct {
		let column = order_output_column(stk, item, columns, &scope).await?;
		return Ok(MatchOrder {
			expr: Expr::Idiom(Idiom::field(column)),
			ascending,
		});
	}

	if aggregating {
		let column = lower_aggregating_order(stk, item, columns, group_keys, &scope).await?;
		return Ok(MatchOrder {
			expr: Expr::Idiom(Idiom::field(column)),
			ascending,
		});
	}

	// Plain: a key naming a RETURN column (its alias, or the verbatim text of an
	// unaliased item) sorts on that column's underlying binding-row expression.
	// Any other key lowers directly.
	if let Some(name) = order_key_name(&item.expr)
		&& let Some(column) = columns.iter().find(|c| c.name == name)
	{
		return Ok(MatchOrder {
			expr: column.expr.clone(),
			ascending,
		});
	}
	let lowered: Expr = expr::lower_value(stk, &item.expr, &scope).await?.into();
	Ok(MatchOrder {
		expr: lowered,
		ascending,
	})
}

/// Resolves an aggregating-query ORDER BY key to the output column name the Sort
/// references. Reuses a projected column (matched by name or lowered expression);
/// otherwise materialises a hidden sort-only column for a grouping key, a
/// value determined by the grouping keys, or an aggregate — rejecting any other
/// key (a genuinely ungrouped, non-aggregate sort key). Returns the column name.
async fn lower_aggregating_order(
	stk: &mut Stk,
	item: &OrderItem,
	columns: &mut Vec<Column>,
	group_keys: &[Expr],
	scope: &Scope<'_>,
) -> Result<String, SyntaxError> {
	// A key naming a projected column by alias / verbatim text.
	if let Some(name) = order_key_name(&item.expr)
		&& let Some(column) = columns.iter().find(|c| !c.hidden && c.name == name)
	{
		return Ok(column.name.clone());
	}
	let is_aggregate = expr::gql_contains_aggregate(&item.expr);
	let lowered: Expr = expr::lower_value(stk, &item.expr, scope).await?.into();
	// Reuse a column (projected or already-materialised) with the same expression.
	if let Some(column) = columns.iter().find(|c| c.expr == lowered) {
		return Ok(column.name.clone());
	}
	// Otherwise it must be a valid aggregating key; materialise a hidden column.
	if !is_aggregate && !column_is_grouped(&lowered, group_keys) {
		bail!(
			"ORDER BY key must be a returned column, a GROUP BY key, an aggregate, or determined by \
			 the GROUP BY keys",
			@item.span => "order by a returned column, a grouping key, or an aggregate"
		);
	}
	let name = format!("__order{}", columns.iter().filter(|c| c.hidden).count());
	columns.push(Column {
		name: name.clone(),
		expr: lowered,
		hidden: true,
	});
	Ok(name)
}

/// Resolves a DISTINCT ORDER BY key to the name of the RETURN column it
/// references — by dotted name, or by lowering to the same expression as a
/// column — rejecting any key that is not a returned column.
async fn order_output_column(
	stk: &mut Stk,
	item: &OrderItem,
	columns: &[Column],
	scope: &Scope<'_>,
) -> Result<String, SyntaxError> {
	// A key matching a column by name: its alias, or the verbatim text of an
	// unaliased item.
	if let Some(name) = order_key_name(&item.expr)
		&& let Some(column) = columns.iter().find(|c| c.name == name)
	{
		return Ok(column.name.clone());
	}
	let lowered: Expr = expr::lower_value(stk, &item.expr, scope).await?.into();
	if let Some(column) = columns.iter().find(|c| c.expr == lowered) {
		return Ok(column.name.clone());
	}
	bail!(
		"With RETURN DISTINCT, ORDER BY may only reference returned columns",
		@item.span => "return the sort expression under an alias and order by the alias"
	);
}

/// Whether an aggregating-query value expression is determined by the GROUP BY
/// keys: it equals a key, or every leaf is a constant or an idiom prefixed by a
/// grouping-key idiom (so the value is constant within each group and the
/// `Aggregate`'s first-value fold is exact). Walks on an explicit stack so a deep
/// operator spine cannot overflow.
fn column_is_grouped(expr: &Expr, keys: &[Expr]) -> bool {
	let mut stack = vec![expr];
	while let Some(e) = stack.pop() {
		// A sub-expression equal to a whole grouping key is covered outright.
		if keys.contains(e) {
			continue;
		}
		match e {
			Expr::Literal(Literal::Array(items)) => stack.extend(items.iter()),
			Expr::Literal(Literal::Object(entries)) => {
				stack.extend(entries.iter().map(|entry| &entry.value));
			}
			Expr::Literal(_) | Expr::Param(_) | Expr::Constant(_) => {}
			Expr::Idiom(idiom) => {
				let covered = keys.iter().any(
					|key| matches!(key, Expr::Idiom(key) if !key.0.is_empty() && idiom.0.starts_with(&key.0)),
				);
				if !covered {
					return false;
				}
			}
			Expr::Binary {
				left,
				right,
				..
			} => {
				stack.push(left);
				stack.push(right);
			}
			Expr::Prefix {
				expr,
				..
			}
			| Expr::Postfix {
				expr,
				..
			} => stack.push(expr),
			// Anything else (function calls, subqueries, tables, …) is not a value
			// determined by the grouping keys.
			_ => return false,
		}
	}
	true
}

/// The dotted name of a sort key that is a plain variable or property chain,
/// used to match RETURN columns by name.
fn order_key_name(expr: &GqlExpr) -> Option<String> {
	let mut names: Vec<&str> = Vec::new();
	let mut base = expr;
	while let GqlExpr::Property(inner, name, _) = base {
		names.push(&name.name);
		base = inner;
	}
	let GqlExpr::Variable(var) = base else {
		return None;
	};
	names.push(&var.name);
	names.reverse();
	Some(names.join("."))
}

/// Lowers a SKIP/LIMIT count: an unsigned integer literal or a parameter. The
/// parser only produces these two forms.
fn lower_count(expr: &GqlExpr) -> Result<Expr, SyntaxError> {
	match expr {
		GqlExpr::Literal(GqlLiteral::Integer(i), _) => Ok(Expr::Literal(Literal::Integer(*i))),
		GqlExpr::Param {
			name,
			span,
		} => {
			naming::validate_param_name(name, *span)?;
			Ok(Expr::Param(Param::from(name.clone())))
		}
		other => Err(syntax_error!(
			"Expected an unsigned integer or a parameter",
			@other.span()
		)),
	}
}

/// A lowered, prepared GQL query: a [`LogicalPlan`] containing the single
/// top-level [`Expr::Match`].
///
/// Renders (`Debug`/`ToSql`) via the [`MatchPlan`]'s deterministic GQL-ish
/// rendering rather than the SurrealQL surface, so EXPLAIN and logs show the
/// plan as a MATCH query.
///
/// `Clone` lets a caller lower a query once and execute the same prepared plan
/// repeatedly (e.g. the language-test bench harness, which keeps parse+lowering
/// out of the timed loop).
#[derive(Clone)]
pub struct PreparedGqlQuery(pub LogicalPlan);

impl std::fmt::Debug for PreparedGqlQuery {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_tuple("PreparedGqlQuery").field(&self.to_sql()).finish()
	}
}

impl ToSql for PreparedGqlQuery {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		// Render the embedded `MatchPlan` directly via its own `ToSql`. The
		// `LogicalPlan`/`Ast` rendering round-trips through
		// `From<expr::Expr> for sql::Expr`, which has no `sql` surface for
		// `Expr::Match` (it logs + `debug_assert!`s + emits a `None`
		// placeholder); going straight to the `MatchPlan` keeps the GQL-ish
		// rendering and avoids that placeholder path.
		match self.0.expressions.as_slice() {
			[TopLevelExpr::Expr(Expr::Match(plan))] => plan.fmt_sql(f, fmt),
			// A `PreparedGqlQuery` is only ever a single top-level `Expr::Match`
			// by construction; fall back to the plan rendering otherwise so the
			// method never panics.
			_ => self.0.fmt_sql(f, fmt),
		}
	}
}
