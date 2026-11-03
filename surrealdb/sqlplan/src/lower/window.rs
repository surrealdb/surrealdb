//! The window function calls of a select, as the [`LogicalOp::Window`] operators that compute them.
//!
//! Each distinct call of the select list and of the `ORDER BY` of a select is computed once, into
//! a hidden field `__sql_window_<i>` of every row, by a `Window` operator below the select's
//! `DISTINCT`, `ORDER BY`, `LIMIT` and projection; those clauses read the field in place of the
//! call. Calls that compare equal but for the name of the window they were written over share one
//! field. Calls over the same `PARTITION BY` and `ORDER BY` are computed by one operator, and the
//! operators are stacked in the order their first call appears.
//!
//! A select without aggregation computes its windows over the rows its `WHERE` clause keeps. An
//! aggregate select computes them over the groups its `HAVING` clause keeps, so the arguments and
//! keys of its calls read the aggregation's output columns.
//!
//! A call is refused (`0A000`) when the engine has no window function of its name, when it is
//! `DISTINCT`, and when its frame is a `GROUPS` frame, excludes rows, is a `RANGE` frame with an
//! offset, or has an offset that is not a non-negative integer literal, and when the offset of a
//! `lag` or `lead`, the position of an `nth_value` or the bucket count of an `ntile` reads the row.

use surrealdb_expr::expr::BinaryOperator;
use surrealdb_expr::expr::logical::{
	LogicalExpr, LogicalIf, LogicalLiteral, LogicalOp, LogicalOrder, LogicalWindow,
	LogicalWindowCall, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction,
};
use surrealdb_strand::Strand;

use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::{
	BoundColumnRef, BoundExpr, BoundLiteral, BoundOrder, BoundSelect, BoundTarget,
	BoundWindowFrame, BoundWindowFrameBound, BoundWindowFrameExclusion, BoundWindowFrameUnits,
	BoundWindowSpec,
};
use crate::lower::expr::{
	bound_expr_contains_aggregate, counts_every_row, lower_bound_expr, lower_bound_order,
};

/// The prefix of the hidden field each window call is computed into.
const OUTPUT_PREFIX: &str = "__sql_window_";

/// The distinct window calls of a select.
pub(super) struct Windows {
	calls: Vec<Call>,
}

/// A window call, and the hidden field it is computed into.
struct Call {
	name: String,
	args: Vec<BoundExpr>,
	distinct: bool,
	spec: BoundWindowSpec,
	output: String,
}

impl Call {
	/// Whether `expr` is a call that computes the same value for every row: the same function
	/// over the same arguments, partition, order and frame, whatever named window either was
	/// written over.
	fn computes(&self, expr: &BoundExpr) -> bool {
		matches!(expr, BoundExpr::WindowFunction { name, args, distinct, spec }
			if *name == self.name
				&& *args == self.args
				&& *distinct == self.distinct
				&& spec.partition_by == self.spec.partition_by
				&& spec.order_by == self.spec.order_by
				&& spec.frame == self.spec.frame)
	}

	/// The arguments, partition keys and order keys of the call.
	fn operands(&self) -> impl Iterator<Item = &BoundExpr> {
		self.args
			.iter()
			.chain(&self.spec.partition_by)
			.chain(self.spec.order_by.iter().map(|key| &key.expr))
	}
}

impl Windows {
	/// The window calls of the select list of `select`, then of its `ORDER BY`. The calls of the
	/// `ORDER BY` of a set operation are refused: that clause orders the combined rows by their
	/// columns. Equal calls share one field, so a select list that holds two equal calls over a
	/// volatile expression, which would each answer their own values, is refused; an `ORDER BY`
	/// call equal to a selected one orders by the selected values.
	pub(super) fn collect(select: &BoundSelect) -> Result<Self, PlanError> {
		let mut out = Self {
			calls: Vec::new(),
		};
		let mut volatile_twice = false;
		for target in &select.targets {
			visit_calls(&target.expr, &mut |call| {
				if out.output_of(call).is_some() && call_may_be_volatile(call) {
					volatile_twice = true;
				}
				out.add(call);
			});
		}
		if volatile_twice {
			return Err(refuse("a window call over a volatile expression selected twice"));
		}
		for order in &select.order_by {
			if select.set_op.is_some() && order.expr.contains_window() {
				return Err(refuse("a window function in the ORDER BY of a set operation"));
			}
			visit_calls(&order.expr, &mut |call| out.add(call));
		}
		Ok(out)
	}

	fn add(&mut self, expr: &BoundExpr) {
		if self.output_of(expr).is_some() {
			return;
		}
		if let BoundExpr::WindowFunction {
			name,
			args,
			distinct,
			spec,
		} = expr
		{
			self.calls.push(Call {
				name: name.clone(),
				args: args.clone(),
				distinct: *distinct,
				spec: spec.clone(),
				output: format!("{OUTPUT_PREFIX}{}", self.calls.len()),
			});
		}
	}

	/// The field the window call `expr` is computed into.
	fn output_of(&self, expr: &BoundExpr) -> Option<&str> {
		self.calls.iter().find(|call| call.computes(expr)).map(|call| call.output.as_str())
	}

	pub(super) fn is_empty(&self) -> bool {
		self.calls.is_empty()
	}

	/// The hidden fields the calls are computed into, as reads of those fields.
	pub(super) fn outputs(&self) -> Vec<LogicalExpr> {
		self.calls
			.iter()
			.map(|call| LogicalExpr::Field(Strand::from(call.output.as_str())))
			.collect()
	}

	/// Whether `column` is a hidden field a call is computed into, which no wildcard reads.
	pub(super) fn computes(&self, column: &str) -> bool {
		self.calls.iter().any(|call| call.output == column)
	}

	/// Whether an argument or a key of a call holds an aggregate, which makes the select an
	/// aggregate select.
	pub(super) fn read_aggregates(&self) -> bool {
		self.calls.iter().any(|call| call.operands().any(bound_expr_contains_aggregate))
	}

	/// `expr` with each window call replaced by a read of the field it is computed into.
	pub(super) fn replace(&self, expr: &BoundExpr) -> BoundExpr {
		replace_calls(expr, self)
	}

	/// `targets` with each target that is a window call and has no alias named by its function,
	/// as the descriptor names its column, so that reading the call from its field keeps the name.
	pub(super) fn name_targets(&self, targets: &[BoundTarget]) -> Vec<BoundTarget> {
		targets
			.iter()
			.map(|target| {
				let mut target = target.clone();
				if target.alias.is_none()
					&& let BoundExpr::WindowFunction {
						name,
						..
					} = &target.expr
				{
					target.alias = Some(name.clone());
				}
				target
			})
			.collect()
	}

	/// The first column an argument or key of a call reads from the row it is computed over once
	/// `rewrite` has read each group key and aggregate from the aggregation's output: a column
	/// that is neither, which a grouped row does not hold.
	pub(super) fn ungrouped_column(
		&self,
		rewrite: &dyn Fn(&BoundExpr) -> BoundExpr,
	) -> Option<String> {
		self.calls.iter().flat_map(Call::operands).find_map(|operand| row_column(&rewrite(operand)))
	}

	/// `input` under one `Window` operator for each distinct `PARTITION BY` and `ORDER BY` of the
	/// calls, in the order of their first call. `rewrite` maps each argument and key to the
	/// expression evaluated over the rows of `input`.
	pub(super) fn lower(
		&self,
		mut input: LogicalOp,
		rewrite: &dyn Fn(&BoundExpr) -> BoundExpr,
		policy: &DialectPolicy,
	) -> Result<LogicalOp, PlanError> {
		let mut groups: Vec<(&[BoundExpr], &[BoundOrder], Vec<usize>)> = Vec::new();
		for (index, call) in self.calls.iter().enumerate() {
			let spec = &call.spec;
			match groups.iter_mut().find(|(partition, order, _)| {
				*partition == spec.partition_by.as_slice() && *order == spec.order_by.as_slice()
			}) {
				Some((_, _, members)) => members.push(index),
				None => groups.push((&spec.partition_by, &spec.order_by, vec![index])),
			}
		}
		for (partition, order, members) in groups {
			let partition = partition
				.iter()
				.map(|key| lower_bound_expr(&rewrite(key), policy))
				.collect::<Result<Vec<_>, _>>()?;
			let mut keys: Vec<LogicalOrder> = Vec::new();
			for key in order {
				keys.extend(lower_bound_order(
					&BoundOrder {
						expr: rewrite(&key.expr),
						direction: key.direction,
						nulls_first: key.nulls_first,
					},
					policy,
				)?);
			}
			let ordered = !order.is_empty();
			let calls = members
				.into_iter()
				.map(|index| lower_call(&self.calls[index], ordered, rewrite, policy))
				.collect::<Result<Vec<_>, _>>()?;
			input = LogicalOp::Window(LogicalWindow {
				input: Box::new(input),
				partition,
				order: keys,
				calls,
			});
		}
		Ok(input)
	}
}

/// The `0A000` refusal of a window construct the engine cannot compute.
fn refuse(what: &str) -> PlanError {
	crate::observability::global_metrics().record_unsupported_feature("WINDOW");
	PlanError::UnsupportedExpression(format!("{what} is not supported"))
}

/// The engine call for the window call `call`. `ordered` says whether its window has an
/// `ORDER BY`, which decides the default frame.
fn lower_call(
	call: &Call,
	ordered: bool,
	rewrite: &dyn Fn(&BoundExpr) -> BoundExpr,
	policy: &DialectPolicy,
) -> Result<LogicalWindowCall, PlanError> {
	let name = call.name.to_ascii_lowercase();
	if call.distinct {
		return Err(refuse(&format!("DISTINCT in the window function {name}")));
	}
	let function = match name.as_str() {
		"row_number" => WindowFunction::RowNumber,
		"rank" => WindowFunction::Rank,
		"dense_rank" => WindowFunction::DenseRank,
		"percent_rank" => WindowFunction::PercentRank,
		"cume_dist" => WindowFunction::CumeDist,
		"ntile" => WindowFunction::Ntile,
		"lag" => WindowFunction::Lag,
		"lead" => WindowFunction::Lead,
		"first_value" => WindowFunction::FirstValue,
		"last_value" => WindowFunction::LastValue,
		"nth_value" => WindowFunction::NthValue,
		"sum" => WindowFunction::Sum,
		"avg" => WindowFunction::Avg,
		"count" => WindowFunction::Count,
		"min" => WindowFunction::Min,
		"max" => WindowFunction::Max,
		_ => return Err(refuse(&format!("{name} as a window function"))),
	};
	let parameter = if matches!(
		function,
		WindowFunction::Lag | WindowFunction::Lead | WindowFunction::NthValue
	) {
		call.args.get(1)
	} else if function == WindowFunction::Ntile {
		call.args.first()
	} else {
		None
	};
	if parameter.is_some_and(|arg| !is_constant(arg)) {
		return Err(refuse(&format!(
			"a {name} argument that is not a constant: one that reads the row"
		)));
	}
	let counts_rows = function == WindowFunction::Count
		&& match call.args.as_slice() {
			[] => true,
			[BoundExpr::Column(col)] => counts_every_row(col),
			_ => false,
		};
	let mut args = if counts_rows {
		Vec::new()
	} else {
		call.args
			.iter()
			.map(|arg| lower_bound_expr(&rewrite(arg), policy))
			.collect::<Result<Vec<_>, _>>()?
	};
	// PostgreSQL reads a negative `lag` or `lead` offset the other way, as the engine does; MySQL
	// refuses one.
	if policy.dialect == SqlDialect::MySql
		&& matches!(function, WindowFunction::Lag | WindowFunction::Lead)
		&& let Some(offset) = args.get_mut(1)
	{
		let value = std::mem::replace(offset, LogicalExpr::Literal(LogicalLiteral::Null));
		*offset = LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				LogicalExpr::Binary {
					left: Box::new(value.clone()),
					op: BinaryOperator::LessThan,
					right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(0))),
				},
				LogicalExpr::Throw(Box::new(LogicalExpr::Literal(LogicalLiteral::String(
					Strand::from(format!("Incorrect arguments to {name}: a negative offset")),
				)))),
			)],
			otherwise: Some(value),
		}));
	}
	let frame = match &call.spec.frame {
		Some(frame) => lower_frame(frame)?,
		None if ordered => WindowFrame {
			units: WindowFrameUnits::Range,
			start: WindowFrameBound::UnboundedPreceding,
			end: WindowFrameBound::CurrentRow,
		},
		None => WindowFrame {
			units: WindowFrameUnits::Rows,
			start: WindowFrameBound::UnboundedPreceding,
			end: WindowFrameBound::UnboundedFollowing,
		},
	};
	Ok(LogicalWindowCall {
		output: Strand::from(call.output.as_str()),
		function,
		args,
		frame,
	})
}

/// Whether `expr` answers the same for every row of a statement: literals and bind parameters,
/// under operators and casts. The engine reads the offset of `lag` and `lead`, the position of
/// `nth_value` and the bucket count of `ntile` once per partition, so a parameter of those calls
/// must be constant.
fn is_constant(expr: &BoundExpr) -> bool {
	match expr {
		BoundExpr::Literal(_)
		| BoundExpr::Param {
			..
		} => true,
		BoundExpr::Unary {
			expr,
			..
		}
		| BoundExpr::Cast {
			expr,
			..
		} => is_constant(expr),
		BoundExpr::Binary {
			left,
			right,
			..
		} => is_constant(left) && is_constant(right),
		BoundExpr::Column(_)
		| BoundExpr::Access {
			..
		}
		| BoundExpr::FunctionCall {
			..
		}
		| BoundExpr::Case {
			..
		}
		| BoundExpr::Between {
			..
		}
		| BoundExpr::InList {
			..
		}
		| BoundExpr::IsNull {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::InSubquery {
			..
		}
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::WindowFunction {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => false,
	}
}

/// The functions whose every evaluation may answer differently.
const VOLATILE: &[&str] = &[
	"clock_timestamp",
	"gen_random_uuid",
	"nextval",
	"pg_sleep",
	"rand",
	"random",
	"random_bytes",
	"setseed",
	"sleep",
	"timeofday",
	"uuid",
	"uuid_short",
];

/// Whether an argument or key of the window call `call` may answer differently each time it is
/// evaluated.
fn call_may_be_volatile(call: &BoundExpr) -> bool {
	let BoundExpr::WindowFunction {
		args,
		spec,
		..
	} = call
	else {
		return false;
	};
	args.iter()
		.chain(&spec.partition_by)
		.chain(spec.order_by.iter().map(|key| &key.expr))
		.any(may_be_volatile)
}

/// Whether `expr` may answer differently each time it is evaluated: it calls a [`VOLATILE`]
/// function or holds a subquery, which may.
pub(super) fn may_be_volatile(expr: &BoundExpr) -> bool {
	match expr {
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} => {
			VOLATILE.contains(&name.to_ascii_lowercase().as_str())
				|| args.iter().any(may_be_volatile)
		}
		BoundExpr::WindowFunction {
			..
		} => call_may_be_volatile(expr),
		BoundExpr::Binary {
			left,
			right,
			..
		} => may_be_volatile(left) || may_be_volatile(right),
		BoundExpr::Unary {
			expr,
			..
		}
		| BoundExpr::Cast {
			expr,
			..
		}
		| BoundExpr::IsNull {
			expr,
			..
		} => may_be_volatile(expr),
		BoundExpr::Access {
			base,
			..
		} => may_be_volatile(base),
		BoundExpr::Between {
			expr,
			low,
			high,
			..
		} => may_be_volatile(expr) || may_be_volatile(low) || may_be_volatile(high),
		BoundExpr::InList {
			expr,
			list,
			..
		} => may_be_volatile(expr) || list.iter().any(may_be_volatile),
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			operand.as_deref().is_some_and(may_be_volatile)
				|| branches
					.iter()
					.any(|(when, then)| may_be_volatile(when) || may_be_volatile(then))
				|| else_expr.as_deref().is_some_and(may_be_volatile)
		}
		BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::InSubquery {
			..
		}
		| BoundExpr::ArraySubquery(_) => true,
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => false,
	}
}

/// The first column of the current query that `expr` reads and an aggregation does not answer,
/// outside its subqueries. The `*` of `count(*)` reads no column.
fn row_column(expr: &BoundExpr) -> Option<String> {
	match expr {
		BoundExpr::Column(col) => {
			(col.correlation_depth == 0 && !col.aggregate_output && col.column != "*")
				.then(|| col.column.clone())
		}
		BoundExpr::FunctionCall {
			args,
			..
		} => args.iter().find_map(row_column),
		BoundExpr::WindowFunction {
			args,
			spec,
			..
		} => args
			.iter()
			.chain(&spec.partition_by)
			.chain(spec.order_by.iter().map(|key| &key.expr))
			.find_map(row_column),
		BoundExpr::Binary {
			left,
			right,
			..
		} => row_column(left).or_else(|| row_column(right)),
		BoundExpr::Unary {
			expr,
			..
		}
		| BoundExpr::Cast {
			expr,
			..
		}
		| BoundExpr::IsNull {
			expr,
			..
		} => row_column(expr),
		BoundExpr::Access {
			base,
			..
		} => row_column(base),
		BoundExpr::Between {
			expr,
			low,
			high,
			..
		} => row_column(expr).or_else(|| row_column(low)).or_else(|| row_column(high)),
		BoundExpr::InList {
			expr,
			list,
			..
		} => row_column(expr).or_else(|| list.iter().find_map(row_column)),
		BoundExpr::InSubquery {
			expr,
			..
		} => row_column(expr),
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => operand
			.as_deref()
			.and_then(row_column)
			.or_else(|| {
				branches
					.iter()
					.find_map(|(when, then)| row_column(when).or_else(|| row_column(then)))
			})
			.or_else(|| else_expr.as_deref().and_then(row_column)),
		BoundExpr::Literal(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => None,
	}
}

/// The engine frame of an explicit SQL frame, whose end is `CURRENT ROW` when only its start is
/// given.
fn lower_frame(frame: &BoundWindowFrame) -> Result<WindowFrame, PlanError> {
	let units = match frame.units {
		BoundWindowFrameUnits::Rows => WindowFrameUnits::Rows,
		BoundWindowFrameUnits::Range => WindowFrameUnits::Range,
		BoundWindowFrameUnits::Groups => return Err(refuse("a GROUPS window frame")),
	};
	match frame.exclusion {
		None | Some(BoundWindowFrameExclusion::NoOthers) => {}
		Some(
			BoundWindowFrameExclusion::CurrentRow
			| BoundWindowFrameExclusion::Group
			| BoundWindowFrameExclusion::Ties,
		) => return Err(refuse("a window frame EXCLUDE clause")),
	}
	let start = lower_frame_bound(&frame.start, units)?;
	let end = match &frame.end {
		Some(end) => lower_frame_bound(end, units)?,
		None => WindowFrameBound::CurrentRow,
	};
	if bound_rank(start) > bound_rank(end)
		|| start == WindowFrameBound::UnboundedFollowing
		|| end == WindowFrameBound::UnboundedPreceding
	{
		return Err(refuse("a window frame that starts after it ends"));
	}
	Ok(WindowFrame {
		units,
		start,
		end,
	})
}

/// The position of the kind of `bound` along a partition, which the start of a frame may not pass
/// its end by.
fn bound_rank(bound: WindowFrameBound) -> u8 {
	match bound {
		WindowFrameBound::UnboundedPreceding => 0,
		WindowFrameBound::Preceding(_) => 1,
		WindowFrameBound::CurrentRow => 2,
		WindowFrameBound::Following(_) => 3,
		WindowFrameBound::UnboundedFollowing => 4,
	}
}

fn lower_frame_bound(
	bound: &BoundWindowFrameBound,
	units: WindowFrameUnits,
) -> Result<WindowFrameBound, PlanError> {
	let offset = |expr: &BoundExpr| -> Result<u64, PlanError> {
		if units == WindowFrameUnits::Range {
			return Err(refuse("a RANGE window frame with an offset"));
		}
		if let BoundExpr::Literal(BoundLiteral::Integer(n)) = expr
			&& let Ok(n) = u64::try_from(*n)
		{
			return Ok(n);
		}
		Err(refuse("a window frame offset that is not a non-negative integer literal"))
	};
	Ok(match bound {
		BoundWindowFrameBound::UnboundedPreceding => WindowFrameBound::UnboundedPreceding,
		BoundWindowFrameBound::Preceding(n) => WindowFrameBound::Preceding(offset(n)?),
		BoundWindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
		BoundWindowFrameBound::Following(n) => WindowFrameBound::Following(offset(n)?),
		BoundWindowFrameBound::UnboundedFollowing => WindowFrameBound::UnboundedFollowing,
	})
}

/// The aggregate calls in `expr`, in the arguments and keys of its window calls too, outside its
/// subqueries: the values an aggregate select computes for a clause that reads windows, which are
/// computed above its aggregation.
pub(super) fn aggregate_calls(expr: &BoundExpr, acc: &mut Vec<BoundExpr>) {
	match expr {
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} => {
			if crate::functions::FunctionRegistry::is_bound_aggregate(name) {
				if !acc.iter().any(|seen| seen.is_compatible_with(expr)) {
					acc.push(expr.clone());
				}
			} else {
				args.iter().for_each(|arg| aggregate_calls(arg, acc));
			}
		}
		BoundExpr::WindowFunction {
			args,
			spec,
			..
		} => {
			args.iter().for_each(|arg| aggregate_calls(arg, acc));
			spec.partition_by.iter().for_each(|key| aggregate_calls(key, acc));
			spec.order_by.iter().for_each(|key| aggregate_calls(&key.expr, acc));
		}
		BoundExpr::Access {
			base,
			..
		} => aggregate_calls(base, acc),
		BoundExpr::Binary {
			left,
			right,
			..
		} => {
			aggregate_calls(left, acc);
			aggregate_calls(right, acc);
		}
		BoundExpr::Unary {
			expr,
			..
		}
		| BoundExpr::IsNull {
			expr,
			..
		}
		| BoundExpr::Cast {
			expr,
			..
		}
		| BoundExpr::InSubquery {
			expr,
			..
		} => aggregate_calls(expr, acc),
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			if let Some(operand) = operand {
				aggregate_calls(operand, acc);
			}
			for (when, then) in branches {
				aggregate_calls(when, acc);
				aggregate_calls(then, acc);
			}
			if let Some(otherwise) = else_expr {
				aggregate_calls(otherwise, acc);
			}
		}
		BoundExpr::Between {
			expr,
			low,
			high,
			..
		} => {
			aggregate_calls(expr, acc);
			aggregate_calls(low, acc);
			aggregate_calls(high, acc);
		}
		BoundExpr::InList {
			expr,
			list,
			..
		} => {
			aggregate_calls(expr, acc);
			list.iter().for_each(|item| aggregate_calls(item, acc));
		}
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => {}
	}
}

/// Calls `out` with each window call in `expr`, outside its subqueries, which are selects of their
/// own.
fn visit_calls(expr: &BoundExpr, out: &mut dyn FnMut(&BoundExpr)) {
	match expr {
		BoundExpr::WindowFunction {
			..
		} => out(expr),
		BoundExpr::Access {
			base,
			..
		} => visit_calls(base, out),
		BoundExpr::Binary {
			left,
			right,
			..
		} => {
			visit_calls(left, out);
			visit_calls(right, out);
		}
		BoundExpr::Unary {
			expr,
			..
		}
		| BoundExpr::IsNull {
			expr,
			..
		}
		| BoundExpr::Cast {
			expr,
			..
		}
		| BoundExpr::InSubquery {
			expr,
			..
		} => visit_calls(expr, out),
		BoundExpr::FunctionCall {
			args,
			..
		} => args.iter().for_each(|arg| visit_calls(arg, out)),
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			if let Some(operand) = operand {
				visit_calls(operand, out);
			}
			for (when, then) in branches {
				visit_calls(when, out);
				visit_calls(then, out);
			}
			if let Some(otherwise) = else_expr {
				visit_calls(otherwise, out);
			}
		}
		BoundExpr::Between {
			expr,
			low,
			high,
			..
		} => {
			visit_calls(expr, out);
			visit_calls(low, out);
			visit_calls(high, out);
		}
		BoundExpr::InList {
			expr,
			list,
			..
		} => {
			visit_calls(expr, out);
			list.iter().for_each(|item| visit_calls(item, out));
		}
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => {}
	}
}

/// `expr` with each window call of `windows` replaced by a read of the field it is computed into.
fn replace_calls(expr: &BoundExpr, windows: &Windows) -> BoundExpr {
	let replace = |expr: &BoundExpr| Box::new(replace_calls(expr, windows));
	match expr {
		BoundExpr::WindowFunction {
			..
		} => match windows.output_of(expr) {
			Some(output) => BoundExpr::Column(BoundColumnRef {
				relation_index: 0,
				relation_name: String::new(),
				column: output.to_string(),
				declared_type: crate::SqlType::Unknown,
				correlation_depth: 0,
				id_table: None,
				link_table: None,
				enum_column: None,
				composite: None,
				declared_sql: None,
				aggregate_output: false,
				key_read: crate::ir::KeyRead::None,
				also_in: Vec::new(),
				carried_as: None,
			}),
			None => expr.clone(),
		},
		BoundExpr::Access {
			base,
			field,
		} => BoundExpr::Access {
			base: replace(base),
			field: field.clone(),
		},
		BoundExpr::Binary {
			left,
			op,
			right,
		} => BoundExpr::Binary {
			left: replace(left),
			op: *op,
			right: replace(right),
		},
		BoundExpr::Unary {
			op,
			expr,
		} => BoundExpr::Unary {
			op: *op,
			expr: replace(expr),
		},
		BoundExpr::FunctionCall {
			name,
			args,
			distinct,
		} => BoundExpr::FunctionCall {
			name: name.clone(),
			args: args.iter().map(|arg| replace_calls(arg, windows)).collect(),
			distinct: *distinct,
		},
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => BoundExpr::Case {
			operand: operand.as_deref().map(replace),
			branches: branches
				.iter()
				.map(|(when, then)| (replace_calls(when, windows), replace_calls(then, windows)))
				.collect(),
			else_expr: else_expr.as_deref().map(replace),
		},
		BoundExpr::Between {
			expr,
			low,
			high,
			negated,
		} => BoundExpr::Between {
			expr: replace(expr),
			low: replace(low),
			high: replace(high),
			negated: *negated,
		},
		BoundExpr::InList {
			expr,
			list,
			negated,
		} => BoundExpr::InList {
			expr: replace(expr),
			list: list.iter().map(|item| replace_calls(item, windows)).collect(),
			negated: *negated,
		},
		BoundExpr::IsNull {
			expr,
			negated,
		} => BoundExpr::IsNull {
			expr: replace(expr),
			negated: *negated,
		},
		BoundExpr::Cast {
			expr,
			target_type,
		} => BoundExpr::Cast {
			expr: replace(expr),
			target_type: target_type.clone(),
		},
		BoundExpr::InSubquery {
			expr,
			subquery,
			negated,
		} => BoundExpr::InSubquery {
			expr: replace(expr),
			subquery: subquery.clone(),
			negated: *negated,
		},
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => expr.clone(),
	}
}
