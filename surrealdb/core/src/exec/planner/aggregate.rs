//! Aggregation planning for the planner.
//!
//! Handles GROUP BY, aggregate function extraction, and the `AggregateExtractor` visitor.

use std::sync::Arc;

use super::Planner;
use super::util::{derive_field_name, idiom_to_field_path};
use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::field_path::FieldPathPart;
use crate::exec::operators::{
	AggregateExprInfo, AggregateField, ExtractedAggregate, aggregate_field_name,
};
use crate::expr::field::{Field, Fields};
use crate::expr::visit::{MutVisitor, VisitMut};
use crate::expr::{Expr, Function, FunctionCall, Idiom, Literal, Part};

/// Build a nested output path from an idiom for use as an [`AggregateField::output_path`].
///
/// Single source of truth: delegates to [`idiom_to_field_path`] so the
/// projection pipeline and the aggregation pipeline always agree on output
/// field names. The returned `Vec<String>` is the flattened form of the
/// canonical [`FieldPath`]:
///
/// - Graph-traversal aliases (`` ->friends->person AS buddy ``) collapse to a single flat key (the
///   alias identifier).
/// - Multi-part idioms (`AS foo.bar`) nest as `["foo", "bar"]`.
/// - Single-part idioms whose identifier contains a dot (`` AS `foo.bar` ``) stay a single flat
///   `["foo.bar"]` key.
/// - Execution-only parts (array filters, indices, method calls, etc.) are dropped via
///   `Idiom::simplify`, matching projection behaviour.
fn alias_output_path(idiom: &Idiom) -> Vec<String> {
	idiom_to_field_path(idiom)
		.0
		.into_iter()
		.map(|part| match part {
			FieldPathPart::Field(s) | FieldPathPart::Lookup(s) => s,
			// `idiom_to_field_path` walks `Idiom::simplify`, which strips
			// indices and First/Last markers, so these variants should not
			// appear here. If a future change to `idiom_to_field_path`
			// emits them anyway, render them in their textual form rather
			// than panicking — preserving the "produce a stable string
			// key" invariant.
			FieldPathPart::Index(i) => format!("[{i}]"),
			FieldPathPart::First => "[0]".to_string(),
			FieldPathPart::Last => "[$]".to_string(),
		})
		.collect()
}

// ============================================================================
// impl Planner — Aggregation
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Plan aggregation fields from SELECT expression and GROUP BY.
	///
	/// Returns the aggregate fields and the physical expressions for group keys.
	#[allow(clippy::type_complexity)]
	pub(crate) async fn plan_aggregation(
		&self,
		fields: &Fields,
		group_by: &[crate::expr::idiom::Idiom],
	) -> Result<(Vec<AggregateField>, Vec<Arc<dyn crate::exec::PhysicalExpr>>), Error> {
		use surrealdb_types::ToSql;

		// Build alias -> expression map from SELECT fields
		let mut alias_to_expr: std::collections::HashMap<String, Expr> =
			std::collections::HashMap::new();
		match fields {
			Fields::Value(selector) => {
				if let Some(alias) = &selector.alias {
					alias_to_expr.insert(alias.to_sql(), selector.expr.clone());
				}
			}
			Fields::Select(field_list) => {
				for field in field_list {
					if let Field::Single(selector) = field
						&& let Some(alias) = &selector.alias
					{
						alias_to_expr.insert(alias.to_sql(), selector.expr.clone());
					}
				}
			}
		}

		// Build group-by expressions, expanding aliases
		let mut group_by_exprs = Vec::with_capacity(group_by.len());
		for idiom in group_by {
			let idiom_str = idiom.to_sql();
			let expr = if let Some(select_expr) = alias_to_expr.get(&idiom_str) {
				select_expr.clone()
			} else {
				Expr::Idiom(idiom.clone())
			};
			let physical_expr = self.physical_expr(expr).await?;
			group_by_exprs.push(physical_expr);
		}

		match fields {
			Fields::Value(selector) => {
				let group_key_index =
					find_group_key_index(&selector.expr, selector.alias.as_ref(), group_by);
				let is_group_key = group_key_index.is_some();

				let (aggregate_expr_info, fallback_expr) = if is_group_key {
					(None, None)
				} else {
					self.extract_aggregate_info(selector.expr.clone()).await?
				};

				// A fallback reaching here came from the row-independent branch
				// of `extract_aggregate_info`, which is the only place one is
				// produced, so it has a single value for the whole group.
				let mut field = AggregateField::new(
					String::new(),
					is_group_key,
					group_key_index,
					aggregate_expr_info,
					fallback_expr,
				);
				if field.fallback_expr.is_some() {
					field = field.with_constant_fallback();
				}

				Ok((vec![field], group_by_exprs))
			}

			Fields::Select(field_list) => {
				let mut aggregates = Vec::with_capacity(field_list.len());

				for field in field_list {
					match field {
						Field::All => {
							return Err(ExecError::Query {
								message: "Incorrect selector for aggregate selection, expression `*` within in selector cannot be aggregated in a group."
									.to_string(),
							}.into());
						}
						Field::Single(selector) => {
							// For an explicit alias, walk the idiom parts
							// so `AS foo.bar` nests as `[foo, bar]` while
							// `` AS `foo.bar` `` stays a flat single key.
							// For an unaliased idiom expression (e.g.
							// `SELECT address.city ...`), walk the source
							// idiom's parts to preserve the same nesting
							// structure. Other unaliased expressions
							// derive a flat name.
							let output_path = if let Some(alias) = &selector.alias {
								alias_output_path(alias)
							} else if let Expr::Idiom(idiom) = &selector.expr {
								alias_output_path(idiom)
							} else {
								vec![derive_field_name(&selector.expr)]
							};

							let group_key_index = find_group_key_index(
								&selector.expr,
								selector.alias.as_ref(),
								group_by,
							);
							let is_group_key = group_key_index.is_some();

							let (aggregate_expr_info, fallback_expr) = if is_group_key {
								(None, None)
							} else {
								self.extract_aggregate_info(selector.expr.clone()).await?
							};

							let mut field = AggregateField::with_output_path(
								output_path,
								is_group_key,
								group_key_index,
								aggregate_expr_info,
								fallback_expr,
							);
							if field.fallback_expr.is_some() {
								field = field.with_constant_fallback();
							}
							aggregates.push(field);
						}
					}
				}

				Ok((aggregates, group_by_exprs))
			}
		}
	}

	/// Extract aggregate function information from an expression.
	///
	/// Uses `AggregateExtractor` to walk the expression tree. An expression
	/// carrying no aggregate of its own still has to reduce to one value per
	/// group, and which way it reduces depends on whether the row can influence
	/// it: an expression that cannot is returned as the non-aggregate
	/// `fallback_expr` and evaluated once per group, while one that reads the
	/// row is wrapped in the implicit collect aggregate, keeping each row's
	/// value.
	///
	/// Takes `expr` by value. When no aggregate functions are found, the visitor
	/// leaves the expression unchanged, so either path can use it directly
	/// without an extra clone.
	#[allow(clippy::type_complexity)]
	#[allow(clippy::clone_on_ref_ptr)] // Several paths clone `Arc<dyn AggregateFunction>` from concrete registry entries
	pub(crate) async fn extract_aggregate_info(
		&self,
		mut expr: Expr,
	) -> Result<(Option<AggregateExprInfo>, Option<Arc<dyn crate::exec::PhysicalExpr>>), Error> {
		let registry = self.function_registry();

		let mut extractor = AggregateExtractor::new(registry);
		let _ = extractor.visit_mut_expr(&mut expr);

		if let Some(err) = extractor.error {
			return Err(err);
		}

		if extractor.aggregates.is_empty() {
			// No aggregates found — the visitor left expr unchanged.
			//
			// An expression the row cannot influence has one value for the
			// whole group, so it is evaluated once as a non-aggregate field
			// rather than collected. Collecting it would return an array whose
			// length is the group's row count, which no reading of GROUP BY
			// produces.
			if !expr_reads_row(&expr, registry) {
				return Ok((None, Some(self.physical_expr(expr).await?)));
			}

			// The expression reads the row, so the group is reduced by
			// collecting each row's value, duplicates included. That is the
			// established answer for this shape, which is why it folds with
			// `array::collect` and not `array::group` — the latter owes its
			// caller a deduplicated result.
			let argument_expr = self.physical_expr(expr).await?;
			let collect = registry.get_implicit_collect_aggregate();
			return Ok((
				Some(AggregateExprInfo {
					aggregates: vec![ExtractedAggregate {
						function: collect,
						argument_expr,
						extra_args: vec![],
					}],
					post_expr: None,
				}),
				None,
			));
		}

		let mut extracted_aggregates = Vec::new();
		for (name, call) in extractor.aggregates {
			let func = if name.as_str() == "count" {
				registry.get_count_aggregate(!call.arguments.is_empty())
			} else {
				Arc::clone(registry.get_aggregate(&name).expect("aggregate function should exist"))
			};

			let mut args = call.arguments.into_iter();
			let argument_expr = if let Some(first_arg) = args.next() {
				self.physical_expr(first_arg).await
			} else {
				self.physical_expr(Expr::Literal(Literal::None)).await
			}?;

			let mut extra_args = Vec::new();
			for arg in args {
				extra_args.push(self.physical_expr(arg).await?);
			}

			extracted_aggregates.push(ExtractedAggregate {
				function: func,
				argument_expr,
				extra_args,
			});
		}

		// expr has been modified by the visitor (aggregate calls replaced with
		// placeholder idioms like `_a0`)
		let is_direct_single_aggregate = extracted_aggregates.len() == 1 && {
			use surrealdb_types::ToSql;
			matches!(&expr, Expr::Idiom(i) if i.to_sql() == "_a0")
		};

		let post_expr = if is_direct_single_aggregate {
			None
		} else {
			Some(self.physical_expr(expr).await?)
		};

		Ok((
			Some(AggregateExprInfo {
				aggregates: extracted_aggregates,
				post_expr,
			}),
			None,
		))
	}
}

// ============================================================================
// Free Functions
// ============================================================================

/// Whether evaluating `expr` can depend on which row of the group it sees.
///
/// After `GROUP BY` a group is a set of rows, so a projected expression has one
/// value for the group only if the row cannot influence it. That is the
/// question this answers, and it decides how an expression carrying no
/// aggregate of its own is reduced: one that reads no row is evaluated once,
/// one that does has its per-row values collected.
///
/// The analysis walks to the leaves rather than matching whole shapes, so
/// `math::abs(-7)` reduces the same way `-7` and `4 + 3` do. Getting that
/// uniformity right is the point: a rule that reduces some constant expressions
/// and collects others cannot be predicted from the query text.
///
/// Deliberately not [`Expr::is_static`], which asks a related but different
/// question — whether an expression is pure enough to fold — and answers it
/// unsoundly for this purpose by its own admission: it treats any call whose
/// arguments are static as static, so a zero-argument user-defined function
/// passes, and it counts every parameter as non-static.
///
/// A row is read through an idiom, through `$this`/`$self`/`$parent`, or by a
/// body plan time cannot see. So these are *not* row-independent, and each for
/// a reason rather than for want of a case arm:
///
/// - Any call target other than a builtin — a user-defined function, script, model, module or silo
///   package — whose body can reach the row via `$parent`.
/// - A projection function such as `type::field`, and an index function behind `@@`/`<|..|>`, both
///   bound to the row being projected or matched.
/// - A writer builtin, whose number of effects depends on how many times it runs, so it is not a
///   value to be reduced.
/// - Blocks, closures, subqueries and statements, which carry bodies.
/// - An idiom rooted at the row (`name`, `address.city`). One rooted in an explicit expression
///   (`$config.label`) is judged by that expression — see [`idiom_is_row_independent`].
///
/// Non-determinism is a separate axis and does not make an expression
/// row-*dependent*: `rand()`, `time::now()` and a generated record id are
/// evaluated once for the group, which is what PostgreSQL does with `random()`
/// under `GROUP BY`.
fn expr_reads_row(expr: &Expr, registry: &crate::exec::function::FunctionRegistry) -> bool {
	!expr_is_row_independent(expr, registry)
}

fn expr_is_row_independent(
	expr: &Expr,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	use crate::expr::operator::{BinaryOperator, PostfixOperator};

	match expr {
		// A literal is fixed, but a composite one evaluates its elements.
		Expr::Literal(lit) => literal_is_row_independent(lit, registry),
		// `math::PI` and friends resolve to a fixed value.
		Expr::Constant(_) => true,
		// A parameter resolves from the session, the statement or a block,
		// none of which vary across a group — except the three that are bound
		// to the row being projected and would otherwise read one row on the
		// whole group's behalf.
		Expr::Param(param) => !matches!(param.as_str(), "this" | "self" | "parent"),
		Expr::Prefix {
			expr,
			..
		} => expr_is_row_independent(expr, registry),
		// Range postfixes only bound the operand; the other two are calls.
		Expr::Postfix {
			expr,
			op,
		} => {
			matches!(op, PostfixOperator::Range | PostfixOperator::RangeSkip)
				&& expr_is_row_independent(expr, registry)
		}
		Expr::Binary {
			left,
			op,
			right,
		} => {
			// `@@` and `<|..|>` dispatch index functions bound to the row
			// being matched, whatever their operands look like.
			!matches!(op, BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_))
				&& expr_is_row_independent(left, registry)
				&& expr_is_row_independent(right, registry)
		}
		Expr::FunctionCall(call) => function_call_is_row_independent(call, registry),
		Expr::Idiom(idiom) => idiom_is_row_independent(idiom, registry),
		_ => false,
	}
}

/// Whether an idiom navigates from something other than the current row.
///
/// An idiom is a path from a root. `name` and `address.city` are rooted at the
/// row and read it. `$config.label`, `({ label: 'x' }).label` and `[1, 2][0]`
/// carry an explicit [`Part::Start`], and are no more row-dependent than the
/// expression they start from — so they must reduce the way that expression
/// does, or `$config` and `$config.label` would answer differently for the same
/// group.
fn idiom_is_row_independent(
	idiom: &Idiom,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	let Some((first, rest)) = idiom.0.split_first() else {
		return false;
	};
	let Part::Start(start) = first else {
		// Any other leading part begins at the row.
		return false;
	};
	expr_is_row_independent(start, registry)
		&& rest.iter().all(|part| part_is_row_independent(part, registry))
}

/// Whether a navigation step keeps an idiom independent of the row.
///
/// Reached only for parts after a [`Part::Start`], so the value being walked is
/// already known not to come from the row; what matters here is whether the
/// step itself reaches back out to the row.
fn part_is_row_independent(
	part: &Part,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	match part {
		// Navigation within the value the idiom is rooted in.
		Part::All | Part::Flatten | Part::Last | Part::First | Part::Optional | Part::Field(_) => {
			true
		}
		// An index or a filter carries an expression of its own. A field name
		// inside a filter denotes the element rather than the row, so it is
		// read as row-reading here and the idiom conservatively collects.
		Part::Value(expr) | Part::Where(expr) => expr_is_row_independent(expr, registry),
		// A second start is invalid state rather than a navigation step.
		Part::Start(_) => false,
		// Reaches the record or runs a body of its own: graph traversal, a
		// method, destructuring, recursion, and the document itself.
		Part::Lookup(_)
		| Part::Method(_, _)
		| Part::Destructure(_)
		| Part::Recurse(_, _, _)
		| Part::Doc
		| Part::RepeatRecurse => false,
	}
}

/// Whether a call can be shown to reach the row only through its arguments.
fn function_call_is_row_independent(
	call: &FunctionCall,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	// Only a builtin has no body of its own to reach the row with.
	let Function::Normal(name) = &call.receiver else {
		return false;
	};
	let name = name.as_str();

	// A projection or index function reads the row it is bound to, and a
	// writer's effects multiply with the number of evaluations.
	if registry.is_projection(name)
		|| registry.is_index_function(name)
		|| crate::expr::method::is_writer_builtin(name)
	{
		return false;
	}

	// An aggregate reduces the group rather than describing one row. The
	// extractor lifts those out before this runs, so this is a guard against a
	// future caller rather than a case reached today.
	if registry.get_aggregate(name).is_some() {
		return false;
	}

	// An unregistered name is not known to be free of a row reference. It
	// would fail at evaluation anyway, so refusing to reduce it costs nothing.
	let Some(func) = registry.get(name) else {
		return false;
	};

	// A function whose effects outlive the statement — `sequence::nextval`,
	// a bucket write, a non-safe HTTP request — must not have the number of
	// times it runs decided by how the rows grouped, and must not run at all
	// for a `GROUP ALL` that saw no rows. Reading the row is not the only way
	// an expression can owe an answer per row.
	if func.mutates_state() {
		return false;
	}

	call.arguments.iter().all(|arg| expr_is_row_independent(arg, registry))
}

fn literal_is_row_independent(
	lit: &Literal,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	match lit {
		Literal::None
		| Literal::Null
		| Literal::UnboundedRange
		| Literal::Bool(_)
		| Literal::Float(_)
		| Literal::Integer(_)
		| Literal::Decimal(_)
		| Literal::String(_)
		| Literal::Bytes(_)
		| Literal::Regex(_)
		| Literal::Duration(_)
		| Literal::Datetime(_)
		| Literal::Uuid(_)
		| Literal::Geometry(_)
		| Literal::File(_) => true,
		Literal::Array(exprs) | Literal::Set(exprs) => {
			exprs.iter().all(|expr| expr_is_row_independent(expr, registry))
		}
		Literal::Object(entries) => {
			entries.iter().all(|entry| expr_is_row_independent(&entry.value, registry))
		}
		// A record id literal can carry expressions in its key.
		Literal::RecordId(rid) => record_id_key_is_row_independent(&rid.key, registry),
	}
}

fn record_id_key_is_row_independent(
	key: &crate::expr::RecordIdKeyLit,
	registry: &crate::exec::function::FunctionRegistry,
) -> bool {
	use std::ops::Bound;

	use crate::expr::RecordIdKeyLit;

	let bound_is_row_independent = |bound: &Bound<RecordIdKeyLit>| match bound {
		Bound::Included(key) | Bound::Excluded(key) => {
			record_id_key_is_row_independent(key, registry)
		}
		Bound::Unbounded => true,
	};

	match key {
		// A generated key is a fresh value per evaluation, not a row
		// reference; reducing it to one value for the group is correct.
		RecordIdKeyLit::Number(_)
		| RecordIdKeyLit::String(_)
		| RecordIdKeyLit::Uuid(_)
		| RecordIdKeyLit::Generate(_) => true,
		RecordIdKeyLit::Array(exprs) => {
			exprs.iter().all(|expr| expr_is_row_independent(expr, registry))
		}
		RecordIdKeyLit::Object(entries) => {
			entries.iter().all(|entry| expr_is_row_independent(&entry.value, registry))
		}
		RecordIdKeyLit::Range(range) => {
			bound_is_row_independent(&range.start) && bound_is_row_independent(&range.end)
		}
	}
}

/// Find the index of the group-by key for an expression.
pub(super) fn find_group_key_index(
	expr: &Expr,
	alias: Option<&Idiom>,
	group_by: &[crate::expr::idiom::Idiom],
) -> Option<usize> {
	use surrealdb_types::ToSql;

	if let Expr::Idiom(idiom) = expr
		&& let Some(idx) = group_by.iter().position(|g| g.to_sql() == idiom.to_sql())
	{
		return Some(idx);
	}

	if let Some(alias) = alias
		&& let Some(idx) = group_by.iter().position(|g| g.to_sql() == alias.to_sql())
	{
		return Some(idx);
	}

	None
}

// ============================================================================
// AggregateExtractor Visitor
// ============================================================================

/// Visitor that extracts aggregate functions from an expression.
struct AggregateExtractor<'a> {
	registry: &'a crate::exec::function::FunctionRegistry,
	aggregates: Vec<(String, FunctionCall)>,
	aggregate_count: usize,
	inside_aggregate: bool,
	error: Option<Error>,
}

impl<'a> AggregateExtractor<'a> {
	fn new(registry: &'a crate::exec::function::FunctionRegistry) -> Self {
		Self {
			registry,
			aggregates: Vec::new(),
			aggregate_count: 0,
			inside_aggregate: false,
			error: None,
		}
	}

	fn contains_aggregate_call(&self, expr: &Expr) -> bool {
		if let Expr::FunctionCall(func_call) = expr
			&& let Function::Normal(name) = &func_call.receiver
		{
			return self.registry.get_aggregate(name.as_str()).is_some();
		}
		false
	}
}

impl MutVisitor for AggregateExtractor<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		if self.error.is_some() {
			return Ok(());
		}

		if let Expr::FunctionCall(func_call) = expr
			&& let Function::Normal(name) = &func_call.receiver
		{
			if name.as_str() == "array::distinct"
				&& !func_call.arguments.is_empty()
				&& self.contains_aggregate_call(&func_call.arguments[0])
			{
				return expr.visit_mut(self);
			}

			if self.registry.get_aggregate(name.as_str()).is_some() {
				if self.inside_aggregate {
					self.error = Some(
						ExecError::Query {
							message: "Nested aggregate functions are not supported".to_string(),
						}
						.into(),
					);
					return Ok(());
				}

				self.inside_aggregate = true;
				for arg in &mut func_call.arguments {
					arg.visit_mut(self)?;
				}
				self.inside_aggregate = false;

				if self.error.is_some() {
					return Ok(());
				}

				let field_name = aggregate_field_name(self.aggregate_count);
				self.aggregates.push((name.clone(), func_call.as_ref().clone()));
				self.aggregate_count += 1;

				*expr = Expr::Idiom(Idiom::field(field_name));
				return Ok(());
			}
		}

		expr.visit_mut(self)
	}

	fn visit_mut_function_call(&mut self, f: &mut FunctionCall) -> Result<(), Self::Error> {
		if self.error.is_some() {
			return Ok(());
		}
		for arg in &mut f.arguments {
			self.visit_mut_expr(arg)?;
		}
		Ok(())
	}

	fn visit_mut_select(
		&mut self,
		_s: &mut crate::expr::statements::SelectStatement,
	) -> Result<(), Self::Error> {
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
}
