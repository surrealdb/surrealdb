//! Aggregation planning for the planner.
//!
//! Handles GROUP BY, aggregate function extraction, and the `AggregateExtractor` rewrite.

use std::ops::Bound;

use surrealdb_types::ToSql;

use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::exec::function::FunctionRegistry;
use crate::exec::operators::{AggregateId, aggregate_field_name};
use crate::exec::planner::util::SELECT_ITERATION_PARAMS;
use crate::expr::Function;
use crate::expr::logical::{
	LogicalCall, LogicalExpr, LogicalField, LogicalFields, LogicalRecordIdKey, OutputPath,
};
use crate::expr::operator::BinaryOperator;

// ============================================================================
// Logical analysis
// ============================================================================

/// One aggregate call of a projected expression, before any of its
/// expressions is compiled.
#[derive(Debug, Clone)]
pub(crate) struct AggregateCallAnalysis {
	/// The function the call names.
	pub(crate) id: AggregateId,
}

/// The aggregates a projected expression holds, as the planner extracts them.
#[derive(Debug, Clone)]
pub(crate) struct AggregateInfoAnalysis {
	/// The calls, in the order of their `_aN` placeholders.
	pub(crate) calls: Vec<AggregateCallAnalysis>,
}

/// A projected field of an aggregation, before any of its expressions is
/// compiled.
#[derive(Debug, Clone)]
pub(crate) struct AggregateFieldAnalysis {
	/// The aggregates of the field, when it holds any or reads the row.
	pub(crate) info: Option<AggregateInfoAnalysis>,
}

/// An aggregation's grouping keys and projected fields, before any of their
/// expressions is compiled.
#[derive(Debug, Clone)]
pub(crate) struct AggregationAnalysis {
	pub(crate) fields: Vec<AggregateFieldAnalysis>,
}

/// What an expression analyses to: its aggregates, or else the expression
/// itself as a field evaluated once for the group.
type AggregateInfoOrFallback = (Option<AggregateInfoAnalysis>, Option<LogicalExpr>);

/// Read the keys and fields of an aggregation over `fields` grouped by
/// `group_by` as [`Planner::plan_aggregation`] reads them, without compiling
/// any expression.
pub(crate) fn analyse_aggregation(
	registry: &FunctionRegistry,
	fields: &LogicalFields,
	group_by: &[LogicalExpr],
) -> Result<AggregationAnalysis, Error> {
	let analyse =
		|expr: &LogicalExpr, alias: Option<&OutputPath>| -> Result<AggregateFieldAnalysis, Error> {
			let info = match find_group_key_index(expr, alias, group_by) {
				Some(_) => None,
				None => analyse_aggregate_info(registry, expr.clone())?.0,
			};
			Ok(AggregateFieldAnalysis {
				info,
			})
		};

	let fields = match fields {
		LogicalFields::Value {
			expr,
			alias,
		} => vec![analyse(expr, alias.as_ref())?],
		LogicalFields::Select(field_list) => {
			let mut analysed = Vec::with_capacity(field_list.len());
			for field in field_list {
				match field {
					LogicalField::All => {
						return Err(ExecError::Query {
							message: "Incorrect selector for aggregate selection, expression `*` within in selector cannot be aggregated in a group."
								.to_string(),
						}
						.into());
					}
					LogicalField::Single {
						expr,
						alias,
						..
					} => {
						analysed.push(analyse(expr, alias.as_ref())?);
					}
				}
			}
			analysed
		}
	};
	Ok(AggregationAnalysis {
		fields,
	})
}

/// Extract the aggregate calls of `expr` with [`AggregateExtractor`].
///
/// An expression carrying no aggregate of its own still has to reduce to one
/// value per group, and which way it reduces depends on whether the row can
/// influence it: an expression that cannot is returned as the non-aggregate
/// fallback and evaluated once per group, while one that reads the row is
/// wrapped in the implicit collect aggregate, keeping each row's value.
pub(crate) fn analyse_aggregate_info(
	registry: &FunctionRegistry,
	mut expr: LogicalExpr,
) -> Result<AggregateInfoOrFallback, Error> {
	let mut extractor = AggregateExtractor::new(registry);
	extractor.extract(&mut expr)?;

	if extractor.aggregates.is_empty() {
		// No aggregate was found, so the extractor left `expr` unchanged.
		//
		// An expression the row cannot influence has one value for the
		// whole group, so it is evaluated once as a non-aggregate field
		// rather than collected. Collecting it would return an array whose
		// length is the group's row count, which no reading of GROUP BY
		// produces.
		if !expr_reads_row(&expr, registry) {
			return Ok((None, Some(expr)));
		}

		// The expression reads the row, so the group is reduced by
		// collecting each row's value, duplicates included.
		// The fold is `array::collect`, not `array::group`, because
		// `array::group` returns deduplicated values.
		return Ok((
			Some(AggregateInfoAnalysis {
				calls: vec![AggregateCallAnalysis {
					id: AggregateId::ImplicitCollect,
				}],
			}),
			None,
		));
	}

	let mut calls = Vec::with_capacity(extractor.aggregates.len());
	for (name, call) in extractor.aggregates {
		let id = AggregateId::of_call(name.as_str(), !call.args.is_empty());
		calls.push(AggregateCallAnalysis {
			id,
		});
	}

	Ok((
		Some(AggregateInfoAnalysis {
			calls,
		}),
		None,
	))
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
/// `math::abs(-7)` reduces the same way `-7` and `4 + 3` do. Every
/// expression with no row reference gets this treatment, so the outcome can
/// be predicted from the query text.
///
/// A row is read through a field of its own, through `$this`/`$self`/`$parent`,
/// or by a body that plan time cannot see. These expressions are therefore
/// row-dependent. The cases are:
///
/// - A read of the row's own field (`name`, and `address.city` as an [`LogicalExpr::Access`] over
///   one), and the record id the document is addressed by (`@`).
/// - Any call target other than a builtin — a user-defined function, script, model, module or silo
///   package — whose body can reach the row via `$parent`.
/// - A projection function such as `type::field`, and an index function behind `@@`/`<|..|>`, both
///   bound to the row being projected or matched.
/// - A writer builtin, whose number of effects depends on how many times it runs, so it is not a
///   value to be reduced.
/// - Blocks, closures, subqueries and statements, which carry bodies, and graph traversal, methods,
///   destructuring and recursion, which reach the record or run a body of their own.
///
/// Non-determinism is a separate axis and does not make an expression
/// row-*dependent*: `rand()`, `time::now()` and a generated record id are
/// evaluated once for the group, which is what PostgreSQL does with `random()`
/// under `GROUP BY`.
fn expr_reads_row(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	!is_row_independent(expr, registry, false)
}

/// `current_is_independent` says whether [`LogicalExpr::Current`] — the value
/// the enclosing body is applied to — is itself free of the row. A projected
/// expression is entered with nothing applied, so the row is what a bare
/// `Current` would stand for and the flag starts `false`; a body whose base is
/// row-independent raises it, which is what lets `$config.label` reduce the way
/// `$config` does.
fn is_row_independent(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
	current_is_independent: bool,
) -> bool {
	let sub = |e: &LogicalExpr| is_row_independent(e, registry, current_is_independent);

	match expr {
		// A scalar literal is fixed; the composite forms carry expressions.
		LogicalExpr::Literal(_) => true,
		LogicalExpr::Array(exprs) | LogicalExpr::Set(exprs) => exprs.iter().all(sub),
		LogicalExpr::Object(entries) => entries.iter().all(|(_, value)| sub(value)),
		LogicalExpr::RecordId(record_id) => {
			record_id_key_is_row_independent(&record_id.key, registry, current_is_independent)
		}
		// `math::PI` and friends resolve to a fixed value.
		LogicalExpr::Constant(_) => true,
		// A mock generates its ids afresh on each evaluation, so the value it
		// denotes is not shared across the rows of a group even though it
		// reads none of them.
		LogicalExpr::Mock(_) => false,
		// A parameter resolves from the session, the statement or a block,
		// none of which vary across a group — except the three that are bound
		// to the row being projected and would otherwise read one row on the
		// whole group's behalf.
		LogicalExpr::Param(param) => !SELECT_ITERATION_PARAMS.contains(&param.as_str()),
		// A field of the row, and the record id the row is addressed by.
		LogicalExpr::Field(_) | LogicalExpr::Doc => false,
		// The value the enclosing body is applied to.
		LogicalExpr::Current => current_is_independent,
		// Navigation within the value the step is applied to.
		LogicalExpr::Access {
			base,
			..
		}
		| LogicalExpr::First(base)
		| LogicalExpr::Last(base)
		| LogicalExpr::Each(base)
		| LogicalExpr::Flatten {
			base,
			..
		} => sub(base),
		// The index is evaluated against the row rather than against `base`,
		// so it is judged in the position the whole path is judged in.
		LogicalExpr::Index {
			base,
			index,
		} => sub(base) && sub(index),
		LogicalExpr::SetField {
			base,
			value,
			..
		} => sub(base) && sub(value),
		// A body is evaluated with an element of `base` as the current value,
		// so what `Current` stands for inside it is as row-free as `base` is.
		// A bare field name inside such a body is still a read of the row,
		// which is what makes `a[WHERE b = 1]` collect.
		LogicalExpr::Filter {
			base,
			predicate: body,
		}
		| LogicalExpr::Optional {
			base,
			then: body,
		} => {
			let base_is_independent = sub(base);
			base_is_independent && is_row_independent(body, registry, base_is_independent)
		}
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => {
			let base_is_independent = sub(base);
			base_is_independent && is_row_independent(body, registry, base_is_independent)
		}
		LogicalExpr::Prefix {
			expr,
			..
		} => sub(expr),
		// A range postfix only bounds its operand; an applied value runs a body
		// of its own.
		LogicalExpr::Postfix {
			expr,
			..
		} => sub(expr),
		LogicalExpr::Invoke {
			..
		} => false,
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => {
			// `@@` and `<|..|>` dispatch index functions bound to the row
			// being matched, whatever their operands look like.
			!matches!(op, BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_))
				&& sub(left) && sub(right)
		}
		LogicalExpr::Call(call) => call_is_row_independent(call, registry, current_is_independent),
		// Reaches the record, names a source, or runs a body of its own.
		LogicalExpr::Table(_)
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => false,
	}
}

/// Whether a call can be shown to reach the row only through its arguments.
fn call_is_row_independent(
	call: &LogicalCall,
	registry: &FunctionRegistry,
	current_is_independent: bool,
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
	// extractor removes aggregate calls before this runs, so this guards
	// against future callers.
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
	// times it runs decided by how the rows grouped. Reading the row is not
	// the only way an expression can owe an answer per row.
	if func.mutates_state() {
		return false;
	}

	call.args.iter().all(|arg| is_row_independent(arg, registry, current_is_independent))
}

fn record_id_key_is_row_independent(
	key: &LogicalRecordIdKey,
	registry: &FunctionRegistry,
	current_is_independent: bool,
) -> bool {
	let bound_is_row_independent = |bound: &Bound<LogicalRecordIdKey>| match bound {
		Bound::Included(key) | Bound::Excluded(key) => {
			record_id_key_is_row_independent(key, registry, current_is_independent)
		}
		Bound::Unbounded => true,
	};

	match key {
		// A generated key is a fresh value per evaluation, not a row
		// reference; reducing it to one value for the group is correct.
		LogicalRecordIdKey::Number(_)
		| LogicalRecordIdKey::String(_)
		| LogicalRecordIdKey::Uuid(_)
		| LogicalRecordIdKey::Generate(_) => true,
		LogicalRecordIdKey::Array(exprs) => {
			exprs.iter().all(|expr| is_row_independent(expr, registry, current_is_independent))
		}
		LogicalRecordIdKey::Object(entries) => entries
			.iter()
			.all(|(_, value)| is_row_independent(value, registry, current_is_independent)),
		LogicalRecordIdKey::Range(range) => {
			bound_is_row_independent(&range.start) && bound_is_row_independent(&range.end)
		}
	}
}

/// Find the index of the group-by key for an expression: the expression is
/// one written in `GROUP BY` (a path, or any other expression), or its alias is. A key and an alias
/// are the same name when they render the same, which is how a key spelled as an alias
/// (`GROUP BY total` for `… AS total`) is matched.
pub(super) fn find_group_key_index(
	expr: &LogicalExpr,
	alias: Option<&OutputPath>,
	group_by: &[LogicalExpr],
) -> Option<usize> {
	if let Some(idx) = group_by.iter().position(|g| g == expr) {
		return Some(idx);
	}

	if let Some(alias) = alias {
		let alias = alias.to_sql();
		if let Some(idx) = group_by.iter().position(|g| g.to_sql() == alias) {
			return Some(idx);
		}
	}

	None
}

// ============================================================================
// AggregateExtractor
// ============================================================================

/// Rewrites a projected expression so each outermost aggregate call becomes a
/// read of a placeholder field (`_a0`, `_a1`, …), recording the calls it
/// replaced in order.
struct AggregateExtractor<'a> {
	registry: &'a FunctionRegistry,
	aggregates: Vec<(String, LogicalCall)>,
	aggregate_count: usize,
	inside_aggregate: bool,
}

impl<'a> AggregateExtractor<'a> {
	fn new(registry: &'a FunctionRegistry) -> Self {
		Self {
			registry,
			aggregates: Vec::new(),
			aggregate_count: 0,
			inside_aggregate: false,
		}
	}

	fn contains_aggregate_call(&self, expr: &LogicalExpr) -> bool {
		if let LogicalExpr::Call(call) = expr
			&& let Function::Normal(name) = &call.receiver
		{
			return self.registry.get_aggregate(name.as_str()).is_some();
		}
		false
	}

	/// Rewrites `expr` in place. Descends into every nested scalar except a
	/// `Subquery`, which is planned on its own. `array::distinct` wrapping an
	/// aggregate is not itself extracted: the aggregate inside it is, and the
	/// wrapper stays in the post-expression. A nested aggregate is an error.
	fn extract(&mut self, expr: &mut LogicalExpr) -> Result<(), Error> {
		if let LogicalExpr::Call(call) = expr
			&& let Function::Normal(name) = &call.receiver
		{
			if name.as_str() == "array::distinct"
				&& call.args.first().is_some_and(|arg| self.contains_aggregate_call(arg))
			{
				for arg in &mut call.args {
					self.extract(arg)?;
				}
				return Ok(());
			}

			if self.registry.get_aggregate(name.as_str()).is_some() {
				if self.inside_aggregate {
					return Err(ExecError::Query {
						message: "Nested aggregate functions are not supported".to_string(),
					}
					.into());
				}
				let name = name.clone();

				self.inside_aggregate = true;
				for arg in &mut call.args {
					self.extract(arg)?;
				}
				self.inside_aggregate = false;

				let field_name = aggregate_field_name(self.aggregate_count);
				self.aggregates.push((name, (**call).clone()));
				self.aggregate_count += 1;

				*expr = LogicalExpr::Field(field_name.into());
				return Ok(());
			}
		}

		for child in expr.children_mut() {
			self.extract(child)?;
		}
		Ok(())
	}
}
