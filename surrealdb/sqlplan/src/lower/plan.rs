//! Lowering from BoundSelect to SurrealDB `LogicalOp`.

use std::borrow::Cow;

use surrealdb_expr::expr::logical::{
	EvalFrame, JoinKind, LogicalAggregate, LogicalCall, LogicalDistinct, LogicalEval, LogicalExpr,
	LogicalField, LogicalFields, LogicalFilter, LogicalIf, LogicalJoin, LogicalLet, LogicalLimit,
	LogicalLiteral, LogicalOp, LogicalOrdering, LogicalProject, LogicalSequence, LogicalSort,
	LogicalSource, LogicalSplit, LogicalSubquery, LogicalUnionAll, LogicalValues, OutputPath,
	RelationAlias, SourceRef,
};
use surrealdb_expr::expr::{BinaryOperator, Function};
use surrealdb_strand::TableName;

use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundColumnRef, BoundDistinct, BoundExpr, BoundJoinCondition, BoundJoinType,
	BoundLiteral, BoundOrder, BoundSelect, BoundSetOpKind, BoundSetOperation, BoundTableSource,
	BoundTarget,
};
use crate::lower::expr::{
	bound_expr_contains_aggregate, lower_bound_expr, lower_bound_order, lower_bound_target,
	lower_column_raw, lower_lookup_predicate, lower_parent_column, pg_bool_spelling,
};
use crate::lower::id_key::{id_column, is_key_or_link, key_of, key_read_as, record_of};
use crate::lower::link::{LinkOperand, link_column, link_operand, lower_stored, same_table_pair};
use crate::lower::window::{Windows, aggregate_calls, may_be_volatile};
use crate::output_keys::output_keys;

mod intersect_except;

/// Lower a fully bound SQL `BoundSelect` into a SurrealDB `LogicalOp` execution plan.
///
/// The plan answers the select to a client, which reads each column by position. When the select's
/// columns are all listed, every row holds each column under its own field, the one the
/// [`ResultColumn::key`](crate::ResultColumn::key) of the select's descriptor names.
pub fn lower_bound_select(
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	lower_bound_select_internal(select, false, true, policy)
}

/// Lower a subquery `BoundSelect` where single-column rows should be emitted as bare values.
pub fn lower_bound_subquery(
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	lower_bound_select_internal(select, true, false, policy)
}

/// The targets as a select's rows hold them: with `keys`, each under its field, which the target
/// then names as its alias. Without, the rows are read by the names the engine gives their fields:
/// a derived table is read by the name its enclosing query uses for a column, and a select with a
/// wildcard has columns that are known only from its rows.
fn keyed_targets(targets: &[BoundTarget], keys: Option<Vec<String>>) -> Cow<'_, [BoundTarget]> {
	let Some(keys) = keys else {
		return Cow::Borrowed(targets);
	};
	Cow::Owned(
		targets
			.iter()
			.zip(keys)
			.map(|(target, key)| BoundTarget {
				alias: Some(key),
				..target.clone()
			})
			.collect(),
	)
}

/// The hidden column a SELECT with set-returning `UNNEST` targets splits on.
const UNNEST_COLUMN: &str = "__sql_unnest";

/// What a set-returning call yields from its first argument.
#[derive(Clone, Copy)]
enum SetValues<'a> {
	/// `UNNEST(array)`: the elements of the array.
	Elements,
	/// `GENERATE_SUBSCRIPTS(array, 1)`: the positions of the elements, from one.
	Positions,
	/// `GENERATE_SERIES(start, stop)`: the integers from the argument up to `stop`, inclusive.
	Series(&'a BoundExpr),
}

/// A select-list target that is a bare set-returning call: its position, its first argument and
/// the values it yields from it.
type SetReturningTarget<'a> = (usize, &'a BoundExpr, SetValues<'a>);

/// The select-list targets that are a bare `UNNEST(array)`, `GENERATE_SUBSCRIPTS(array, 1)` or
/// `GENERATE_SERIES(start, stop)`, in order.
fn unnest_targets(select: &BoundSelect) -> Vec<SetReturningTarget<'_>> {
	select
		.targets
		.iter()
		.enumerate()
		.filter_map(|(index, target)| {
			let BoundExpr::FunctionCall {
				name,
				args,
				..
			} = &target.expr
			else {
				return None;
			};
			if name == "unnest" && args.len() == 1 {
				Some((index, &args[0], SetValues::Elements))
			} else if name == "generate_subscripts"
				&& args.len() == 2
				&& matches!(args[1], BoundExpr::Literal(BoundLiteral::Integer(1)))
			{
				Some((index, &args[0], SetValues::Positions))
			} else if name == "generate_series" && args.len() == 2 {
				Some((index, &args[0], SetValues::Series(&args[1])))
			} else {
				None
			}
		})
		.collect()
}

/// A `LIMIT` or `OFFSET`, which is a row count: a parameter that holds text is read as the
/// integer it spells, as a client that sends every parameter as text needs.
fn lower_row_count(expr: &BoundExpr, policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	Ok(key_read_as(crate::ir::KeyRead::Int, lower_bound_expr(expr, policy)?))
}

/// The values a set-returning call over `argument` advances through: the elements of the array,
/// their positions from one, or the integers of a series. An argument that is not an array has no
/// elements, and a series that ends before it starts has no integers.
fn set_returning_values(
	argument: &BoundExpr,
	values: SetValues<'_>,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	if let SetValues::Series(stop) = values {
		if [argument, stop].iter().any(|bound| {
			matches!(bound, BoundExpr::Literal(BoundLiteral::Float(_) | BoundLiteral::Numeric(_)))
		}) {
			return Err(PlanError::UnsupportedExpression(
				"GENERATE_SERIES over bounds that are not integers".into(),
			));
		}
		let start = lower_bound_expr(argument, policy)?;
		let stop = lower_bound_expr(stop, policy)?;
		// An inclusive range reads each end once and holds the largest integer, and is empty when
		// `stop` is before `start`.
		return Ok(LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal("array::range".to_string()),
			args: vec![LogicalExpr::Binary {
				left: Box::new(start),
				op: BinaryOperator::RangeInclusive,
				right: Box::new(stop),
			}],
		})));
	}
	let array = lower_bound_expr(argument, policy)?;
	let elements = if matches!(values, SetValues::Positions) {
		// The first position of an array is one; a vector column of the catalog is numbered
		// from zero.
		let first = i64::from(!matches!(
			argument,
			BoundExpr::Column(BoundColumnRef {
				declared_type: crate::SqlType::Int2Vector | crate::SqlType::OidVector,
				..
			})
		));
		LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal("array::range".to_string()),
			args: vec![
				LogicalExpr::Literal(LogicalLiteral::Integer(first)),
				LogicalExpr::Binary {
					left: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("array::len".to_string()),
						args: vec![array.clone()],
					}))),
					op: BinaryOperator::Add,
					right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(first))),
				},
			],
		}))
	} else {
		array.clone()
	};
	Ok(LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(
			LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("type::is_array".to_string()),
				args: vec![array],
			})),
			elements,
		)],
		otherwise: Some(LogicalExpr::Array(Vec::new())),
	})))
}

/// Expand each row of `op` into one row per element of the arrays `unnested` names.
///
/// Several set-returning targets of one select list advance together, as PostgreSQL evaluates
/// them: row `i` carries element `i` of every array, and a shorter array is padded with NULL up
/// to the longest. The arrays are zipped into the hidden [`UNNEST_COLUMN`] and the rows are split
/// on it. A split reads the column of the projected row it names, so the projection that carries
/// the column and the split form a pipeline of their own, and each target then reads its position
/// of the column from the rows it delivers ([`unnested_field`]). An argument that is not an array
/// has no elements.
fn split_unnested(
	op: LogicalOp,
	unnested: &[SetReturningTarget<'_>],
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let mut arrays = Vec::with_capacity(unnested.len());
	for (_, argument, values) in unnested {
		arrays.push(set_returning_values(argument, *values, policy)?);
	}
	// A lone target splits its array as it is, so that the array is not copied; several are
	// zipped into rows of one element each.
	let lone = arrays.len() == 1;
	let zipped = match <[LogicalExpr; 1]>::try_from(arrays) {
		Ok([array]) => array,
		Err(arrays) => LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal("array::transpose".to_string()),
			args: vec![LogicalExpr::Array(arrays)],
		})),
	};
	let with_column = LogicalOp::Project(LogicalProject {
		input: Box::new(op),
		fields: LogicalFields::Select(vec![
			LogicalField::All,
			LogicalField::user(zipped, Some(OutputPath::field(UNNEST_COLUMN.to_owned()))),
		]),
		omit: vec![],
	});
	// A row whose arrays are all empty splits into a row that holds an empty array instead of an
	// element, and is dropped.
	let split = LogicalOp::Subquery(LogicalSubquery {
		input: Box::new(LogicalOp::Split(LogicalSplit {
			input: Box::new(with_column),
			exprs: vec![LogicalExpr::Field(UNNEST_COLUMN.into())],
		})),
	});
	let column = || LogicalExpr::Field(UNNEST_COLUMN.into());
	let non_empty = LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal("array::len".to_string()),
			args: vec![column()],
		}))),
		op: BinaryOperator::MoreThan,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(0))),
	};
	// An element of a lone target may be any value, and only an array can be empty.
	let predicate = if lone {
		LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal("type::is_array".to_string()),
					args: vec![column()],
				})),
				non_empty,
			)],
			otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Bool(true))),
		}))
	} else {
		non_empty
	};
	Ok(LogicalOp::Filter(LogicalFilter {
		input: Box::new(split),
		predicate,
	}))
}

/// The projected field of the `slot`-th of `count` `UNNEST` targets after [`split_unnested`].
fn unnested_field(target: &BoundTarget, slot: usize, count: usize) -> LogicalField {
	let column = LogicalExpr::Field(UNNEST_COLUMN.into());
	let element = if count == 1 {
		column
	} else {
		LogicalExpr::Index {
			base: Box::new(column),
			index: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(slot as i64))),
		}
	};
	let natural = if let BoundExpr::FunctionCall {
		name,
		..
	} = &target.expr
	{
		name.clone()
	} else {
		"unnest".to_owned()
	};
	let alias = target.alias.clone().unwrap_or(natural);
	LogicalField::user(element, Some(OutputPath::field(alias)))
}

/// A set-returning call in the target of a `LATERAL` subquery.
enum SetReturning<'a> {
	/// `UNNEST(array)`: the elements of the array.
	Unnest(&'a BoundExpr),
	/// `GENERATE_SUBSCRIPTS(array, 1)`: the positions of the elements, from one.
	Subscripts(&'a BoundExpr),
}

/// The hidden column of the `slot`-th set-returning call of the lateral relation `alias_idx`.
fn lateral_slot(alias_idx: usize, slot: usize) -> String {
	format!("__lateral{alias_idx}_{slot}")
}

/// `expr` with each set-returning call it holds replaced by the hidden column that carries the
/// element of the call, which `calls` collects in order. Such a call may stand in an expression
/// over other values, as in `GENERATE_SUBSCRIPTS(a, 1) + 1`; it may not stand under a call that is
/// not an operator or a cast.
fn replace_set_returning<'a>(
	expr: &'a BoundExpr,
	alias_idx: usize,
	calls: &mut Vec<SetReturning<'a>>,
) -> Result<BoundExpr, PlanError> {
	let slot_column = |slot: usize| {
		BoundExpr::Column(BoundColumnRef {
			relation_index: 0,
			relation_name: String::new(),
			column: lateral_slot(alias_idx, slot),
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
		})
	};
	match expr {
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} if name == "unnest" && args.len() == 1 => {
			calls.push(SetReturning::Unnest(&args[0]));
			Ok(slot_column(calls.len() - 1))
		}
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} if name == "generate_subscripts"
			&& matches!(args.get(1), Some(BoundExpr::Literal(BoundLiteral::Integer(1))))
			&& args.len() == 2 =>
		{
			calls.push(SetReturning::Subscripts(&args[0]));
			Ok(slot_column(calls.len() - 1))
		}
		BoundExpr::Binary {
			left,
			op,
			right,
		} => Ok(BoundExpr::Binary {
			left: Box::new(replace_set_returning(left, alias_idx, calls)?),
			op: *op,
			right: Box::new(replace_set_returning(right, alias_idx, calls)?),
		}),
		BoundExpr::Unary {
			op,
			expr,
		} => Ok(BoundExpr::Unary {
			op: *op,
			expr: Box::new(replace_set_returning(expr, alias_idx, calls)?),
		}),
		BoundExpr::Cast {
			expr,
			target_type,
		} => Ok(BoundExpr::Cast {
			expr: Box::new(replace_set_returning(expr, alias_idx, calls)?),
			target_type: target_type.clone(),
		}),
		other @ (BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Access {
			..
		}
		| BoundExpr::Param {
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
		}) => {
			if contains_set_returning(other) {
				return Err(PlanError::UnsupportedExpression(
					"a set-returning call under a function in a LATERAL target".into(),
				));
			}
			Ok(other.clone())
		}
	}
}

/// Whether `expr` holds an `UNNEST`, `GENERATE_SUBSCRIPTS` or `GENERATE_SERIES` call.
fn contains_set_returning(expr: &BoundExpr) -> bool {
	match expr {
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} => {
			name == "unnest"
				|| name == "generate_subscripts"
				|| name == "generate_series"
				|| args.iter().any(contains_set_returning)
		}
		BoundExpr::Binary {
			left,
			right,
			..
		} => contains_set_returning(left) || contains_set_returning(right),
		BoundExpr::Unary {
			expr,
			..
		}
		| BoundExpr::Cast {
			expr,
			..
		} => contains_set_returning(expr),
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			operand.as_deref().is_some_and(contains_set_returning)
				|| branches.iter().any(|(when, then)| {
					contains_set_returning(when) || contains_set_returning(then)
				}) || else_expr.as_deref().is_some_and(contains_set_returning)
		}
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Access {
			..
		}
		| BoundExpr::Param {
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

/// Extend each row of `left` with the lateral relation `alias_idx` whose columns are `targets`.
///
/// The relation's columns are the fields of one object under the alias of the join, as a joined
/// relation's are. A set-returning call in a target yields one row of the relation for each of its
/// elements: the calls advance together, as in a select list, so row `i` carries element `i` of
/// every call and a shorter call is padded with NULL. The arrays the calls name are zipped into a
/// hidden column the rows are split on, and each call then reads its position of that column. An
/// argument that is not an array has no elements, and a row of `left` with no element is dropped.
fn lower_lateral(
	left: LogicalOp,
	targets: &[BoundTarget],
	alias_idx: usize,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let alias = RelationAlias::new(alias_idx);
	let mut calls = Vec::new();
	let replaced = targets
		.iter()
		.map(|target| replace_set_returning(&target.expr, alias_idx, &mut calls))
		.collect::<Result<Vec<_>, _>>()?;
	let mut op = left;
	if !calls.is_empty() {
		let mut arrays = Vec::with_capacity(calls.len());
		for call in &calls {
			let (argument, values) = match call {
				SetReturning::Unnest(argument) => (argument, SetValues::Elements),
				SetReturning::Subscripts(argument) => (argument, SetValues::Positions),
			};
			arrays.push(set_returning_values(argument, values, policy)?);
		}
		let count = arrays.len();
		let zipped = match <[LogicalExpr; 1]>::try_from(arrays) {
			Ok([array]) => array,
			Err(arrays) => LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("array::transpose".to_string()),
				args: vec![LogicalExpr::Array(arrays)],
			})),
		};
		let hidden = format!("__lateral{alias_idx}");
		let with_column = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::user(zipped, Some(OutputPath::field(hidden.clone()))),
			]),
			omit: vec![],
		});
		let split = LogicalOp::Subquery(LogicalSubquery {
			input: Box::new(LogicalOp::Split(LogicalSplit {
				input: Box::new(with_column),
				exprs: vec![LogicalExpr::Field(hidden.clone().into())],
			})),
		});
		let mut fields = vec![LogicalField::All];
		for slot in 0..count {
			let column = LogicalExpr::Field(hidden.as_str().into());
			let element = if count == 1 {
				column
			} else {
				LogicalExpr::Index {
					base: Box::new(column),
					index: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(slot as i64))),
				}
			};
			fields.push(LogicalField::user(
				element,
				Some(OutputPath::field(lateral_slot(alias_idx, slot))),
			));
		}
		// A projection does not read a projection, so each one is a subquery of its own.
		op = LogicalOp::Subquery(LogicalSubquery {
			input: Box::new(LogicalOp::Project(LogicalProject {
				input: Box::new(split),
				fields: LogicalFields::Select(fields),
				omit: vec![],
			})),
		});
	}
	let mut members = Vec::with_capacity(targets.len());
	for (target, expr) in targets.iter().zip(&replaced) {
		let name = target.alias.clone().unwrap_or_default();
		members.push((name, lower_bound_expr(expr, policy)?));
	}
	Ok(LogicalOp::Subquery(LogicalSubquery {
		input: Box::new(LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::user(
					LogicalExpr::Object(members),
					Some(OutputPath::field(alias.name().as_str().to_owned())),
				),
			]),
			omit: vec![],
		})),
	}))
}

/// Lower `select`, binding first the columns of the enclosing query that queries nested in it read
/// ([`BoundSelect::carried`]).
fn lower_bound_select_internal(
	select: &BoundSelect,
	as_value: bool,
	keyed: bool,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let op = lower_bound_select_pipeline(select, as_value, keyed, policy)?;
	Ok(bind_carried_columns(select, op))
}

/// `op`, the lowering of `select`, as a block that first binds each column `select` carries to its
/// parameter. The block is evaluated where `select` is, so a column is read once from the row of
/// the query above, as `$parent` reads it, and `op` and every query nested in it read the
/// parameter.
fn bind_carried_columns(select: &BoundSelect, op: LogicalOp) -> LogicalOp {
	if select.carried.is_empty() {
		return op;
	}
	let mut stmts: Vec<LogicalOp> = select
		.carried
		.iter()
		.map(|carried| {
			LogicalOp::Let(LogicalLet {
				name: carried.name.clone(),
				kind: None,
				value: Box::new(LogicalOp::Eval(LogicalEval {
					expr: lower_parent_column(&carried.column),
				})),
				synthetic: true,
			})
		})
		.collect();
	stmts.push(op);
	LogicalOp::Sequence(LogicalSequence {
		stmts,
	})
}

fn lower_bound_select_pipeline(
	select: &BoundSelect,
	as_value: bool,
	keyed: bool,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let windows = Windows::collect(select)?;

	// 1. FROM clause
	let mut op = match &select.from {
		None | Some(BoundTableSource::Values) => LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(vec![LogicalExpr::Object(vec![])]),
			for_update: select.for_update,
			written: false,
		}),
		Some(source) => {
			let mut join_counter = 0;
			lower_table_source(
				source,
				select.for_update,
				&mut join_counter,
				policy,
				&select.user_tables,
				&select.user_types,
				select.database.as_deref().unwrap_or("test"),
			)?
		}
	};

	// 2. WHERE clause, and the nearest-neighbour search an `ORDER BY` distance asks for
	let predicate =
		select.where_clause.as_ref().map(|expr| lower_where(expr, policy)).transpose()?;
	let predicate = match (predicate, crate::lower::vector_knn::knn_predicate(select, policy)?) {
		(Some(predicate), Some(search)) => Some(LogicalExpr::Binary {
			left: Box::new(predicate),
			op: BinaryOperator::And,
			right: Box::new(search),
		}),
		(predicate, search) => predicate.or(search),
	};
	if let Some(predicate) = predicate {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate,
		});
	}

	// 3. GROUP BY & Aggregation
	if select.group_by.iter().any(bound_expr_contains_aggregate) {
		return Err(PlanError::AggregateNotAllowed("GROUP BY".into()));
	}
	let grouped = !select.group_by.is_empty();
	let has_aggregates = select.targets.iter().any(|t| bound_expr_contains_aggregate(&t.expr));
	let has_having_agg = select.having.as_ref().is_some_and(bound_expr_contains_aggregate);
	let has_order_agg = select.order_by.iter().any(|o| bound_expr_contains_aggregate(&o.expr));
	let is_aggregate = grouped
		|| has_aggregates
		|| has_having_agg
		|| has_order_agg
		|| select.having.is_some()
		|| windows.read_aggregates();
	// The windows of an aggregate select are computed over its groups, above its aggregation,
	// which computes every value they read; the windows of any other select over the rows its
	// WHERE clause keeps.
	let windowed_aggregate = is_aggregate && !windows.is_empty();

	let unnested = unnest_targets(select);
	if !unnested.is_empty() {
		if is_aggregate {
			return Err(PlanError::UnsupportedExpression(
				"UNNEST in the select list of an aggregate query".into(),
			));
		}
		op = split_unnested(op, &unnested, policy)?;
	}
	if !is_aggregate && !windows.is_empty() {
		op = windows.lower(op, &BoundExpr::clone, policy)?;
	}

	// The select is read by position only if it is asked to be and its columns are all listed;
	// the queries of its set operations are read the same way.
	let keys = keyed.then(|| output_keys(&select.targets)).flatten();
	let keyed = keys.is_some();
	let targets = keyed_targets(&select.targets, keys);
	let targets = if windows.is_empty() {
		targets
	} else {
		Cow::Owned(windows.name_targets(&targets))
	};
	let mut aggregate_targets = targets.to_vec();

	// The engine recognises a projected field as a grouping key only when it is the stored path the
	// aggregation groups by. Grouping by the stored record ids of a table groups by its keys, so
	// an `id` grouped by is grouped and projected as stored, and read as its key afterwards.
	let stored_keys: Vec<&BoundExpr> = if is_aggregate {
		select
			.group_by
			.iter()
			.filter(|g| id_column(g).is_some_and(|(col, _)| col.correlation_depth == 0))
			.collect()
	} else {
		Vec::new()
	};
	let mut key_outputs: Vec<String> = Vec::new();
	for (target, original) in aggregate_targets.iter_mut().zip(targets.iter()) {
		if stored_keys.contains(&&original.expr) {
			let name = target_output_name(original);
			target.expr = as_stored(&original.expr);
			target.alias = Some(name.clone());
			key_outputs.push(name);
		}
	}

	let mut agg_mapping: Vec<(BoundExpr, String)> = Vec::new();
	// A target that reads a window is computed above the aggregation, so no clause reads it from
	// the aggregation's output.
	for t in targets.iter().filter(|t| !t.expr.contains_window()) {
		let col_name = target_output_name(t);
		agg_mapping.push((t.expr.clone(), col_name));
	}

	for (index, group) in select.group_by.iter().enumerate() {
		if !agg_mapping.iter().any(|(expr, _)| expr.is_compatible_with(group)) {
			let name = unused_name(&format!("__sql_group_{index}"), &agg_mapping, &targets);
			aggregate_targets.push(BoundTarget {
				expr: if stored_keys.contains(&group) {
					as_stored(group)
				} else {
					group.clone()
				},
				alias: Some(name.clone()),
				is_wildcard: false,
				table_wildcard: None,
			});
			agg_mapping.push((group.clone(), name));
		}
	}

	let mut synthetic_idx = 0;
	if let Some(having_expr) = &select.having {
		let mut having_aggs = Vec::new();
		collect_aggregate_exprs(having_expr, &mut having_aggs);
		for agg in having_aggs {
			if !agg_mapping.iter().any(|(e, _)| e.is_compatible_with(&agg)) {
				let syn_name =
					next_unused_name("__sql_having_", &mut synthetic_idx, &agg_mapping, &targets);
				aggregate_targets.push(BoundTarget {
					expr: agg.clone(),
					alias: Some(syn_name.clone()),
					is_wildcard: false,
					table_wildcard: None,
				});
				agg_mapping.push((agg, syn_name));
			}
		}
	}

	let mut order_syn_idx = 0;
	for o in &select.order_by {
		if windowed_aggregate && o.expr.contains_window() {
			continue;
		}
		let mut order_aggs = Vec::new();
		collect_aggregate_exprs(&o.expr, &mut order_aggs);
		for agg in order_aggs {
			if !agg_mapping.iter().any(|(e, _)| e.is_compatible_with(&agg)) {
				let syn_name =
					next_unused_name("__sql_order_", &mut order_syn_idx, &agg_mapping, &targets);
				aggregate_targets.push(BoundTarget {
					expr: agg.clone(),
					alias: Some(syn_name.clone()),
					is_wildcard: false,
					table_wildcard: None,
				});
				agg_mapping.push((agg, syn_name));
			}
		}
	}

	// The aggregation computes each aggregate a clause that reads a window reads, and the windows,
	// the targets that read them and the ORDER BY are computed above it.
	if windowed_aggregate {
		let mut window_aggs = Vec::new();
		for expr in targets
			.iter()
			.map(|t| &t.expr)
			.chain(select.order_by.iter().map(|o| &o.expr))
			.filter(|expr| expr.contains_window())
		{
			aggregate_calls(expr, &mut window_aggs);
		}
		let mut window_agg_idx = 0;
		for agg in window_aggs {
			if agg.contains_window() {
				return Err(PlanError::WindowNotAllowed("aggregate function calls".into()));
			}
			if agg_mapping.iter().any(|(e, _)| e.is_compatible_with(&agg)) {
				continue;
			}
			let name =
				next_unused_name("__sql_window_agg_", &mut window_agg_idx, &agg_mapping, &targets);
			aggregate_targets.push(BoundTarget {
				expr: agg.clone(),
				alias: Some(name.clone()),
				is_wildcard: false,
				table_wildcard: None,
			});
			agg_mapping.push((agg, name));
		}
		aggregate_targets.retain(|t| !t.expr.contains_window());
	} else if !windows.is_empty() {
		for t in &mut aggregate_targets {
			t.expr = windows.replace(&t.expr);
		}
	}

	// The projection after an aggregate that keeps its rows as objects reads each selected column
	// back by `target_output_name`. A target that lowering leaves unnamed, a computed expression,
	// is given that name so the column is found.
	let reads_columns_by_name = is_aggregate
		&& (windowed_aggregate
			|| aggregate_targets.len() > select.targets.len()
			|| (as_value
				&& select.targets.len() == 1
				&& (select.having.is_some() || !select.order_by.is_empty())));
	if reads_columns_by_name {
		for t in &mut aggregate_targets {
			if !t.is_wildcard && t.alias.is_none() && !target_names_itself(t) {
				t.alias = Some(target_output_name(t));
			}
		}
	}

	// A subquery that selects one column answers with bare values. An aggregate with a HAVING or
	// an ORDER BY keeps its rows as objects, so that those clauses read their columns, and takes
	// the values from the one column it selects after them.
	let value_column = as_value && select.targets.len() == 1 && !select.targets[0].is_wildcard;
	let as_value = as_value
		&& !windowed_aggregate
		&& (!is_aggregate || (select.having.is_none() && select.order_by.is_empty()));
	let fields = if as_value && select.targets.len() == 1 && !select.targets[0].is_wildcard {
		let target = &select.targets[0].expr;
		let expr = lower_bound_expr(&windows.replace(target), policy)?;
		let expr = if matches!(target, BoundExpr::Column(col) if col.id_table.is_none() && col.link_table.is_none())
		{
			key_of(expr, true)
		} else {
			expr
		};
		let alias = select.targets[0].alias.as_ref().map(|a| OutputPath::field(a.clone()));
		LogicalFields::Value {
			expr,
			alias,
		}
	} else {
		let mut lowered = aggregate_targets
			.iter()
			.enumerate()
			.map(|(index, t)| match unnested.iter().position(|(at, ..)| *at == index) {
				Some(slot) => Ok(unnested_field(t, slot, unnested.len())),
				None => lower_bound_target(t, policy),
			})
			.collect::<Result<Vec<_>, _>>()?;
		if !is_aggregate && sole_base_table(select).is_some() {
			let last_wildcard =
				aggregate_targets.iter().rposition(BoundTarget::is_unlisted_wildcard);
			read_wildcard_id_as_key(&mut lowered, last_wildcard);
		}
		LogicalFields::Select(lowered)
	};

	// A clause over the rows of an aggregation reads its stored `id` columns; one over the rows of
	// a set operation reads the keys the operands projected; any other reads the rows the query
	// reads.
	let rewritten_having =
		select.having.as_ref().map(|h| rewrite_aggregates_to_columns(h, &agg_mapping, true));

	let mut rewritten_order_by = Vec::new();
	for o in &select.order_by {
		let expr = windows.replace(&o.expr);
		let expr = if select.set_op.is_some() {
			rewrite_aggregates_to_columns(&expr, &agg_mapping, false)
		} else if is_aggregate {
			rewrite_aggregates_to_columns(&expr, &agg_mapping, true)
		} else {
			expr
		};
		rewritten_order_by.push(BoundOrder {
			expr,
			direction: o.direction,
			nulls_first: o.nulls_first,
		});
	}

	if is_aggregate {
		let group = select
			.group_by
			.iter()
			.map(|g| {
				if stored_keys.contains(&g) {
					lower_bound_expr(&as_stored(g), policy)
				} else {
					// A key that is a selected expression is lowered as that expression, which
					// is how the engine knows the item is the key. The first selected expression
					// the key is compatible with is that expression.
					let selected = select
						.targets
						.iter()
						.map(|t| &t.expr)
						.find(|expr| expr.is_compatible_with(g));
					lower_stored(selected.unwrap_or(g), policy)
				}
			})
			.collect::<Result<Vec<_>, _>>()?;
		op = LogicalOp::Aggregate(LogicalAggregate {
			input: Box::new(op),
			group,
			fields: fields.clone(),
		});
	}

	// 4. HAVING clause
	if let Some(having_expr) = &rewritten_having {
		op = LogicalOp::Filter(LogicalFilter {
			input: Box::new(op),
			predicate: lower_bound_expr(having_expr, policy)?,
		});
	}
	if windowed_aggregate {
		let rewrite = |expr: &BoundExpr| rewrite_aggregates_to_columns(expr, &agg_mapping, true);
		if let Some(column) = windows.ungrouped_column(&rewrite) {
			return Err(PlanError::ColumnNotInGroupBy(column));
		}
		op = windows.lower(op, &rewrite, policy)?;
	}

	// A `DISTINCT` compares the columns a select selects, so it is taken over the projected rows,
	// with the `ORDER BY` and `LIMIT` above it. The rows below the projection hold more than those
	// columns: an `id`, the fields the windows are computed into, and an aggregate select's group
	// keys and the aggregates only its other clauses read. Each `ORDER BY` key must be a selected
	// column, as PostgreSQL and MySQL require, and is the expression of that column.
	let projected_distinct =
		if select.set_op.is_none() && matches!(select.distinct, Some(BoundDistinct::All)) {
			let selected = |t: &BoundTarget| {
				let expr = windows.replace(&t.expr);
				if is_aggregate {
					rewrite_aggregates_to_columns(&expr, &agg_mapping, true)
				} else {
					expr
				}
			};
			Some(distinct_order_keys(
				&targets,
				&selected,
				&|column| windows.computes(column),
				&rewritten_order_by,
			)?)
		} else {
			None
		};

	// 5. DISTINCT clause
	if select.distinct.is_some() && projected_distinct.is_none() {
		op = LogicalOp::Distinct(LogicalDistinct {
			input: Box::new(op),
		});
	}

	// 6. ORDER BY clause (if not deferred to set_op)
	if select.set_op.is_none() && projected_distinct.is_none() {
		op = sort_by(op, &rewritten_order_by, policy)?;
	}

	// 7. LIMIT & OFFSET clause (if not deferred to set_op)
	if select.set_op.is_none() && projected_distinct.is_none() {
		op = limit_by(op, select, policy)?;
	}

	// 8. Projection. A wildcard does not read the fields the windows are computed into.
	if !is_aggregate {
		let omit = if has_wildcard(&targets) {
			windows.outputs()
		} else {
			Vec::new()
		};
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields,
			omit,
		});
	} else if windowed_aggregate {
		// Each target is read from the aggregation's output, and one that reads a window is
		// computed from the window fields and the aggregation's output columns.
		let read = |t: &BoundTarget| -> Result<LogicalExpr, PlanError> {
			if t.expr.contains_window() {
				let expr =
					rewrite_aggregates_to_columns(&windows.replace(&t.expr), &agg_mapping, true);
				lower_bound_expr(&expr, policy)
			} else {
				Ok(LogicalExpr::Field(target_output_name(t).into()))
			}
		};
		let fields = if value_column {
			LogicalFields::Value {
				expr: read(&targets[0])?,
				alias: None,
			}
		} else {
			LogicalFields::Select(
				targets
					.iter()
					.map(|t| {
						let col_name = target_output_name(t);
						if !t.expr.contains_window() && key_outputs.contains(&col_name) {
							return Ok(key_output_field(col_name));
						}
						let alias = t.alias.as_ref().map(|a| OutputPath::field(a.clone()));
						Ok(LogicalField::user(read(t)?, alias))
					})
					.collect::<Result<Vec<_>, PlanError>>()?,
			)
		};
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields,
			omit: vec![],
		});
	} else if !matches!(fields, LogicalFields::Value { .. }) && value_column {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Value {
				expr: LogicalExpr::Field(target_output_name(&targets[0]).into()),
				alias: None,
			},
			omit: vec![],
		});
	} else if matches!(fields, LogicalFields::Value { .. }) {
		// The aggregate already answers with the value of the one column.
	} else if aggregate_targets.len() > select.targets.len() {
		let project_fields = targets
			.iter()
			.map(|t| {
				let col_name = target_output_name(t);
				if key_outputs.contains(&col_name) {
					return key_output_field(col_name);
				}
				let alias = t.alias.as_ref().map(|a| OutputPath::field(a.clone()));
				LogicalField::user(LogicalExpr::Field(col_name.into()), alias)
			})
			.collect::<Vec<_>>();
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Select(project_fields),
			omit: vec![],
		});
	} else if key_outputs.is_empty() {
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::all(),
			omit: vec![],
		});
	} else {
		let mut fields = vec![LogicalField::All];
		fields.extend(key_outputs.into_iter().map(key_output_field));
		op = LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Select(fields),
			omit: vec![],
		});
	}

	if let Some(keys) = &projected_distinct {
		op = distinct_over_projection(op, keys, &targets, select, policy)?;
	}

	// 9. Set Operations (UNION, INTERSECT, EXCEPT)
	if let Some(first) = &select.set_op {
		op = lower_set_operations(&targets, first, op, keyed, policy)?;

		// Apply outer ORDER BY after set operation
		if !rewritten_order_by.is_empty() {
			let mut orders = Vec::new();
			for o in &rewritten_order_by {
				orders.extend(lower_bound_order(o, policy)?);
			}
			op = LogicalOp::Sort(LogicalSort {
				input: Box::new(op),
				ordering: LogicalOrdering::Order(orders),
				tempfiles: false,
			});
		}

		// Apply outer LIMIT & OFFSET after set operation
		if select.limit.is_some() || select.offset.is_some() {
			let limit = select.limit.as_ref().map(|l| lower_row_count(l, policy)).transpose()?;
			let start = select.offset.as_ref().map(|o| lower_row_count(o, policy)).transpose()?;
			op = LogicalOp::Limit(LogicalLimit {
				input: Box::new(op),
				start,
				limit,
			});
		}
	}

	Ok(op)
}

/// `input` sorted by `order`, or `input` where `order` is empty.
fn sort_by(
	input: LogicalOp,
	order: &[BoundOrder],
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	if order.is_empty() {
		return Ok(input);
	}
	let mut orders = Vec::new();
	for o in order {
		orders.extend(lower_bound_order(o, policy)?);
	}
	Ok(LogicalOp::Sort(LogicalSort {
		input: Box::new(input),
		ordering: LogicalOrdering::Order(orders),
		tempfiles: false,
	}))
}

/// `input` under the `LIMIT` and `OFFSET` of `select`, or `input` where it has neither.
fn limit_by(
	input: LogicalOp,
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	if select.limit.is_none() && select.offset.is_none() {
		return Ok(input);
	}
	let limit = select.limit.as_ref().map(|l| lower_row_count(l, policy)).transpose()?;
	let start = select.offset.as_ref().map(|o| lower_row_count(o, policy)).transpose()?;
	Ok(LogicalOp::Limit(LogicalLimit {
		input: Box::new(input),
		start,
		limit,
	}))
}

/// What a `SELECT DISTINCT` orders its rows by: an `ORDER BY` key, and where its value is read.
struct DistinctKey {
	order: BoundOrder,
	/// The output column the key reads, a selected column that is named; `None` for a key the
	/// projection computes into a hidden field, a selected column the projection names itself.
	column: Option<String>,
}

/// The `ORDER BY` of a `SELECT DISTINCT`. A key that is or names a selected column whose output
/// name is known reads that column, so the rows are ordered by the very values they answer; a key
/// that is a selected column the projection names itself is computed again into a hidden field
/// (`distinct_over_projection`). A column of a relation a wildcard covers is selected: the first
/// relation's under its own name, a joined relation's computed into a hidden field. A column of a
/// relation no wildcard covers is not, nor is a window's hidden field, which no wildcard reads.
/// `selected` computes a target over the rows the projection reads. A key that is no selected
/// column is refused: the `DISTINCT` cannot order its rows by it.
fn distinct_order_keys(
	targets: &[BoundTarget],
	selected: &dyn Fn(&BoundTarget) -> BoundExpr,
	hidden: &dyn Fn(&str) -> bool,
	order: &[BoundOrder],
) -> Result<Vec<DistinctKey>, PlanError> {
	order
		.iter()
		.map(|key| {
			let column = targets.iter().filter(|t| !t.is_wildcard).find_map(|t| {
				let named = t.alias.is_some() || target_names_itself(t);
				let expr = selected(t);
				let names = named && names_output(&key.expr, &target_output_name(t));
				(names || expr.is_compatible_with(&key.expr))
					.then(|| (expr, named.then(|| target_output_name(t))))
			});
			let (expr, column) = match column {
				Some(found) => found,
				None => {
					let BoundExpr::Column(col) = &key.expr else {
						return Err(PlanError::DistinctOrderByNotSelected);
					};
					let covered = col.correlation_depth == 0
						&& !hidden(&col.column)
						&& targets.iter().any(|t| t.is_wildcard && wildcard_covers(t, col));
					if !covered {
						return Err(PlanError::DistinctOrderByNotSelected);
					}
					// The first relation's columns are fields of the row the wildcard projects; a
					// joined relation's are not, so its column is computed into a hidden field.
					(key.expr.clone(), (col.relation_index == 0).then(|| col.column.clone()))
				}
			};
			Ok(DistinctKey {
				order: BoundOrder {
					expr,
					direction: key.direction,
					nulls_first: key.nulls_first,
				},
				column,
			})
		})
		.collect()
}

/// Whether `wildcard`, a wildcard target whose columns are not known, selects `col`: a `*` selects
/// the columns of every relation, a `t.*` those of `t` alone.
fn wildcard_covers(wildcard: &BoundTarget, col: &BoundColumnRef) -> bool {
	match (&wildcard.table_wildcard, &wildcard.expr) {
		(None, _) => true,
		(Some(_), BoundExpr::Column(relation)) => relation.relation_index == col.relation_index,
		(Some(_), _) => false,
	}
}

/// The prefix of the hidden field a computed `ORDER BY` key of a `SELECT DISTINCT` is projected
/// into. A field of that name the select does not compute is answered as the row holds it, unless a
/// wildcard reads one that shares a hidden field's name.
const DISTINCT_ORDER_PREFIX: &str = "__sql_distinct_order_";

/// The field the value of a `SELECT DISTINCT` that answers bare values is projected into while
/// its rows are ordered.
const DISTINCT_VALUE: &str = "__sql_distinct_value";

/// `projected`, the projection of a `SELECT DISTINCT`, under its `DISTINCT`, `ORDER BY` and
/// `LIMIT`. With no `ORDER BY` the `DISTINCT` compares the projected rows as they are. Otherwise
/// the rows are ordered by the selected columns the keys read, and a key the projection names
/// itself is computed into a hidden field beside the selected columns, which adds nothing the
/// `DISTINCT` can tell rows apart by, as each key is a selected column; a projection above drops
/// those fields. A hidden field takes a name no selected column has, and a key that may answer
/// differently each time it is evaluated is refused there, since it would order the rows by
/// values other than those they answer. A projection that answers
/// bare values answers an object holding the value until then, which each key reads.
fn distinct_over_projection(
	projected: LogicalOp,
	keys: &[DistinctKey],
	targets: &[BoundTarget],
	select: &BoundSelect,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let distinct = |input: LogicalOp| {
		LogicalOp::Distinct(LogicalDistinct {
			input: Box::new(input),
		})
	};
	if keys.is_empty() {
		return limit_by(distinct(projected), select, policy);
	}
	let LogicalOp::Project(mut project) = projected else {
		return Err(PlanError::unsupported("an ORDER BY of a SELECT DISTINCT over bare values"));
	};
	let bare = matches!(project.fields, LogicalFields::Value { .. });
	let mut hidden = Vec::new();
	let mut omit = Vec::new();
	let mut order = Vec::with_capacity(keys.len());
	let mut next = 0;
	for key in keys {
		let name = match (&key.column, bare) {
			(_, true) => DISTINCT_VALUE.to_owned(),
			(Some(column), false) => column.clone(),
			(None, false) => {
				if may_be_volatile(&key.order.expr) {
					return Err(PlanError::unsupported(
						"an ORDER BY of a SELECT DISTINCT by a volatile column with no name; name \
						 the column with AS and order by that name",
					));
				}
				let name = next_unused_name(DISTINCT_ORDER_PREFIX, &mut next, &[], targets);
				hidden.push(LogicalField::user(
					lower_bound_expr(&key.order.expr, policy)?,
					Some(OutputPath::field(name.clone())),
				));
				omit.push(LogicalExpr::Field(name.clone().into()));
				name
			}
		};
		order.push(BoundOrder {
			expr: output_column(&name),
			direction: key.order.direction,
			nulls_first: key.order.nulls_first,
		});
	}
	let value = match std::mem::replace(&mut project.fields, LogicalFields::Select(Vec::new())) {
		LogicalFields::Value {
			expr,
			alias,
		} => {
			project.fields = LogicalFields::Select(vec![LogicalField::user(
				expr,
				Some(OutputPath::field(DISTINCT_VALUE.to_string())),
			)]);
			Some(alias)
		}
		LogicalFields::Select(mut fields) => {
			fields.extend(hidden);
			project.fields = LogicalFields::Select(fields);
			None
		}
	};
	let op = sort_by(distinct(LogicalOp::Project(project)), &order, policy)?;
	let op = limit_by(op, select, policy)?;
	Ok(match value {
		Some(alias) => LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::Value {
				expr: LogicalExpr::Field(DISTINCT_VALUE.into()),
				alias,
			},
			omit: Vec::new(),
		}),
		None if omit.is_empty() => op,
		None => LogicalOp::Project(LogicalProject {
			input: Box::new(op),
			fields: LogicalFields::all(),
			omit,
		}),
	})
}

/// A read of the output column `name` of the select.
fn output_column(name: &str) -> BoundExpr {
	BoundExpr::Column(BoundColumnRef {
		relation_index: 0,
		relation_name: String::new(),
		column: name.to_string(),
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
		declared_type: crate::SqlType::Unknown,
	})
}

/// Whether `key` is a reference the binder resolved to the output column `column` of the select,
/// as an `ORDER BY` names a column by its alias.
fn names_output(key: &BoundExpr, column: &str) -> bool {
	matches!(key, BoundExpr::Column(col)
		if col.relation_name.is_empty() && col.correlation_depth == 0 && col.column == column)
}

/// Combine `left`, the lowered first query of a set operation, with the queries chained after it.
///
/// A chain `a op b op c` is held as nested right operands with no record of which operands were
/// parenthesised, so `a UNION ALL b UNION c` and `a UNION ALL (b UNION c)` are the same chain.
/// Only a chain made of `UNION` links alone, or of `UNION ALL` links alone, reads alike under every
/// grouping, so a chain of more than two queries is lowered only when it is one of those, and is
/// refused otherwise: a chain with an `INTERSECT` or `EXCEPT` link as a chain, a mix of `UNION`
/// and `UNION ALL` with advice to group it. An operand followed by further links cannot carry its
/// own ORDER BY, LIMIT or OFFSET for the same reason: only a parenthesised group can hold them.
///
/// A set operation answers rows under the columns of its first query, and each later query is
/// lowered under the same fields. When `keyed`, those are the keys of the first query.
fn lower_set_operations(
	first_targets: &[BoundTarget],
	first: &BoundSetOperation,
	left: LogicalOp,
	keyed: bool,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	if first.right.set_op.is_some() {
		let links = || std::iter::successors(Some(first), |l| l.right.set_op.as_ref());
		if links().any(|l| l.op != BoundSetOpKind::Union) {
			return Err(PlanError::unsupported(
				"a chain of more than two queries is supported only with UNION or UNION ALL",
			));
		}
		if links().any(|l| l.all != first.all) {
			return Err(PlanError::unsupported(
				"a chain of more than two queries must join them all with UNION or all with \
				 UNION ALL; group a mix with subqueries",
			));
		}
	}

	// The chain is cloned once and taken apart link by link, so each query is lowered without the
	// rest of the chain and the work stays linear in its length.
	let mut branches = Vec::new();
	let mut next = Some(first.clone());
	while let Some(link) = next.take() {
		let mut branch = *link.right;
		next = branch.set_op.take();
		if next.is_some()
			&& (!branch.order_by.is_empty() || branch.limit.is_some() || branch.offset.is_some())
		{
			return Err(PlanError::unsupported(
				"ORDER BY, LIMIT or OFFSET on a query that is followed by another set operation",
			));
		}
		if !has_wildcard(first_targets)
			&& !has_wildcard(&branch.targets)
			&& first_targets.len() != branch.targets.len()
		{
			return Err(PlanError::Custom(
				"each query of a set operation must have the same number of columns".into(),
			));
		}
		name_as_first_query(first_targets, &mut branch);
		branches
			.push((lower_bound_select_internal(&branch, false, keyed, policy)?, branch.targets));
	}

	let all = first.all;
	match first.op {
		BoundSetOpKind::Union => {
			let mut inputs = vec![left];
			inputs.extend(branches.into_iter().map(|(branch_op, _)| branch_op));
			let union_all = LogicalOp::UnionAll(LogicalUnionAll {
				inputs,
			});
			Ok(if all {
				union_all
			} else {
				LogicalOp::Distinct(LogicalDistinct {
					input: Box::new(union_all),
				})
			})
		}
		BoundSetOpKind::Intersect | BoundSetOpKind::Except => {
			// A chain of these is refused above, so there is exactly one right query.
			let Some((right_op, right_targets)) = branches.pop() else {
				return Ok(left);
			};
			let kind = if first.op == BoundSetOpKind::Intersect {
				JoinKind::Semi
			} else {
				JoinKind::Anti
			};
			intersect_except::lower(kind, all, first_targets, &right_targets, left, right_op)
		}
	}
}

/// Whether any of `targets` is a wildcard, whose columns are not known at plan time.
fn has_wildcard(targets: &[BoundTarget]) -> bool {
	targets.iter().any(|t| t.is_wildcard || t.table_wildcard.is_some())
}

/// The name a target gives its column in the rows it produces: its alias, the column it reads or
/// the function it calls. `None` where the name is not one of those or the target is a wildcard.
fn column_name(target: &BoundTarget) -> Option<String> {
	if target.is_wildcard || target.table_wildcard.is_some() {
		return None;
	}
	match (&target.alias, &target.expr) {
		(Some(alias), _) => Some(alias.clone()),
		(
			None,
			BoundExpr::Column(BoundColumnRef {
				column,
				..
			}),
		) if column != "*" => Some(column.clone()),
		(
			None,
			BoundExpr::FunctionCall {
				name,
				..
			},
		) => Some(name.clone()),
		(None, _) => None,
	}
}

/// Name the columns of `branch`, a later query of a set operation, as the columns of the first
/// query are.
///
/// A set operation answers rows under the names its first query gives its columns, whatever the
/// later queries call theirs, and a query over a UNION reads them under those names. A column is
/// left as it is where the first query's name for it cannot be told, where the later query's
/// column is a wildcard, or where the two queries do not have the same number of columns.
fn name_as_first_query(first: &[BoundTarget], branch: &mut BoundSelect) {
	if first.len() != branch.targets.len() {
		return;
	}
	for (first, later) in first.iter().zip(&mut branch.targets) {
		let Some(name) = column_name(first) else {
			continue;
		};
		if !later.is_wildcard
			&& later.table_wildcard.is_none()
			&& column_name(later).as_deref() != Some(name.as_str())
		{
			later.alias = Some(name);
		}
	}
}

/// `name`, or `name` suffixed with `_` until no selected column or computed output of the
/// aggregation answers to it, so a hidden output never overwrites a column the query selects.
fn unused_name(name: &str, outputs: &[(BoundExpr, String)], targets: &[BoundTarget]) -> String {
	let mut name = name.to_owned();
	while outputs.iter().any(|(_, taken)| *taken == name)
		|| targets.iter().any(|t| !t.is_wildcard && target_output_name(t) == name)
	{
		name.push('_');
	}
	name
}

/// The next hidden output name `{prefix}{n}`, counting `n` up from `next`, that `unused_name`
/// leaves as it is or extends.
fn next_unused_name(
	prefix: &str,
	next: &mut usize,
	outputs: &[(BoundExpr, String)],
	targets: &[BoundTarget],
) -> String {
	let name = unused_name(&format!("{prefix}{next}"), outputs, targets);
	*next += 1;
	name
}

fn target_output_name(target: &BoundTarget) -> String {
	match &target.alias {
		Some(a) => a.clone(),
		None => match &target.expr {
			BoundExpr::Column(col) if col.column != "*" => col.column.clone(),
			BoundExpr::FunctionCall {
				name,
				..
			} => name.clone(),
			BoundExpr::Literal(_)
			| BoundExpr::Access {
				..
			}
			| BoundExpr::Param {
				..
			}
			| BoundExpr::Binary {
				..
			}
			| BoundExpr::Unary {
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
			| BoundExpr::Cast {
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
			}
			| BoundExpr::Column(_) => "col".to_string(),
		},
	}
}

/// Whether `lower_bound_target` names the output of `target` when it has no alias.
fn target_names_itself(target: &BoundTarget) -> bool {
	matches!(&target.expr, BoundExpr::Column(col) if col.column != "*")
		|| matches!(
			&target.expr,
			BoundExpr::FunctionCall { .. } | BoundExpr::UserVar(_) | BoundExpr::SystemVar { .. }
		)
}

fn collect_aggregate_exprs(expr: &BoundExpr, acc: &mut Vec<BoundExpr>) {
	match expr {
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} => {
			// A call that holds an aggregate is one expression of the aggregation, so that its
			// result is named once and read by a HAVING or an ORDER BY as that column.
			let aggregates = crate::functions::FunctionRegistry::is_bound_aggregate(name)
				|| args.iter().any(bound_expr_contains_aggregate);
			if aggregates && !acc.iter().any(|seen| seen.is_compatible_with(expr)) {
				acc.push(expr.clone());
			}
		}
		BoundExpr::Binary {
			left,
			right,
			..
		} => {
			collect_aggregate_exprs(left, acc);
			collect_aggregate_exprs(right, acc);
		}
		BoundExpr::Unary {
			expr,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
		}
		BoundExpr::Access {
			base,
			..
		} => {
			collect_aggregate_exprs(base, acc);
		}
		BoundExpr::Between {
			expr,
			low,
			high,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
			collect_aggregate_exprs(low, acc);
			collect_aggregate_exprs(high, acc);
		}
		BoundExpr::InList {
			expr,
			list,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
			for item in list {
				collect_aggregate_exprs(item, acc);
			}
		}
		BoundExpr::IsNull {
			expr,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
		}
		BoundExpr::Cast {
			expr,
			..
		} => {
			collect_aggregate_exprs(expr, acc);
		}
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			if let Some(op) = operand {
				collect_aggregate_exprs(op, acc);
			}
			for (c, v) in branches {
				collect_aggregate_exprs(c, acc);
				collect_aggregate_exprs(v, acc);
			}
			if let Some(el) = else_expr {
				collect_aggregate_exprs(el, acc);
			}
		}
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
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
		} => {}
	}
}

/// Rewrite the sub-expressions of `expr` that are projected targets to references to the output
/// column of the target, for a clause evaluated over the rows a projection or aggregation
/// produced. A reference to the key of an `id` keeps reading the key when `keep_id` says the
/// output column holds the stored record id.
fn rewrite_aggregates_to_columns(
	expr: &BoundExpr,
	mapping: &[(BoundExpr, String)],
	keep_id: bool,
) -> BoundExpr {
	for (agg, col_name) in mapping {
		if agg.is_compatible_with(expr) {
			return BoundExpr::Column(BoundColumnRef {
				relation_index: 0,
				relation_name: String::new(),
				column: col_name.clone(),
				correlation_depth: 0,
				id_table: id_column(agg).filter(|_| keep_id).map(|(_, table)| table.to_string()),
				link_table: link_column(agg).map(|(_, table)| table.to_string()),
				enum_column: if let BoundExpr::Column(col) = agg {
					col.enum_column.clone()
				} else {
					None
				},
				composite: if let BoundExpr::Column(col) = agg {
					col.composite.clone()
				} else {
					None
				},
				declared_sql: if let BoundExpr::Column(col) = agg {
					col.declared_sql.clone()
				} else {
					None
				},
				aggregate_output: keep_id,
				key_read: crate::ir::KeyRead::None,
				also_in: Vec::new(),
				carried_as: None,
				declared_type: match agg {
					BoundExpr::Column(col) => col.declared_type,
					BoundExpr::Literal(_)
					| BoundExpr::Access {
						..
					}
					| BoundExpr::Param {
						..
					}
					| BoundExpr::Binary {
						..
					}
					| BoundExpr::Unary {
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
					| BoundExpr::Cast {
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
					} => crate::SqlType::Unknown,
				},
			});
		}
	}
	match expr {
		BoundExpr::Binary {
			left,
			op,
			right,
		} => BoundExpr::Binary {
			left: Box::new(rewrite_aggregates_to_columns(left, mapping, keep_id)),
			op: *op,
			right: Box::new(rewrite_aggregates_to_columns(right, mapping, keep_id)),
		},
		BoundExpr::FunctionCall {
			name,
			args,
			distinct,
		} => BoundExpr::FunctionCall {
			name: name.clone(),
			args: args
				.iter()
				.map(|arg| rewrite_aggregates_to_columns(arg, mapping, keep_id))
				.collect(),
			distinct: *distinct,
		},
		BoundExpr::InSubquery {
			expr,
			subquery,
			negated,
		} => BoundExpr::InSubquery {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			subquery: subquery.clone(),
			negated: *negated,
		},
		BoundExpr::Unary {
			op,
			expr,
		} => BoundExpr::Unary {
			op: *op,
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
		},
		BoundExpr::Access {
			base,
			field,
		} => BoundExpr::Access {
			base: Box::new(rewrite_aggregates_to_columns(base, mapping, keep_id)),
			field: field.clone(),
		},
		BoundExpr::Between {
			expr,
			low,
			high,
			negated,
		} => BoundExpr::Between {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			low: Box::new(rewrite_aggregates_to_columns(low, mapping, keep_id)),
			high: Box::new(rewrite_aggregates_to_columns(high, mapping, keep_id)),
			negated: *negated,
		},
		BoundExpr::InList {
			expr,
			list,
			negated,
		} => BoundExpr::InList {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			list: list
				.iter()
				.map(|item| rewrite_aggregates_to_columns(item, mapping, keep_id))
				.collect(),
			negated: *negated,
		},
		BoundExpr::IsNull {
			expr,
			negated,
		} => BoundExpr::IsNull {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			negated: *negated,
		},
		BoundExpr::Cast {
			expr,
			target_type,
		} => BoundExpr::Cast {
			expr: Box::new(rewrite_aggregates_to_columns(expr, mapping, keep_id)),
			target_type: target_type.clone(),
		},
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => BoundExpr::Case {
			operand: operand
				.as_ref()
				.map(|op| Box::new(rewrite_aggregates_to_columns(op, mapping, keep_id))),
			branches: branches
				.iter()
				.map(|(c, v)| {
					(
						rewrite_aggregates_to_columns(c, mapping, keep_id),
						rewrite_aggregates_to_columns(v, mapping, keep_id),
					)
				})
				.collect(),
			else_expr: else_expr
				.as_ref()
				.map(|el| Box::new(rewrite_aggregates_to_columns(el, mapping, keep_id))),
		},
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::ArraySubquery(_)
		| BoundExpr::WindowFunction {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => expr.clone(),
	}
}

fn lower_table_source(
	source: &BoundTableSource,
	for_update: bool,
	join_counter: &mut usize,
	policy: &DialectPolicy,
	user_tables: &[crate::catalog::TableSchema],
	user_types: &crate::catalog::UserTypes,
	database: &str,
) -> Result<LogicalOp, PlanError> {
	match source {
		BoundTableSource::Values => Ok(LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(vec![LogicalExpr::Object(vec![])]),
			for_update,
			written: false,
		})),
		BoundTableSource::Table {
			name,
			schema,
			..
		} => {
			if let Some(rows) = crate::catalog::system::get_system_table_rows_in(
				schema.as_deref(),
				name,
				user_tables,
				user_types,
				database,
				policy.dialect,
			) {
				return Ok(LogicalOp::Values(LogicalValues {
					expr: LogicalExpr::Array(rows),
					for_update,
					written: false,
				}));
			}
			Ok(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from(name.as_str())),
				schema: None,
				with: None,
				for_update,
			}))
		}
		BoundTableSource::Lateral {
			..
		} => Err(PlanError::UnsupportedStatement(
			"a LATERAL subquery that is not joined to a relation on its left".into(),
		)),
		BoundTableSource::Subquery {
			query,
			..
		} => {
			let inner = lower_bound_select_internal(query, false, false, policy)?;
			Ok(LogicalOp::Subquery(LogicalSubquery {
				input: Box::new(inner),
			}))
		}
		BoundTableSource::Join {
			left,
			join_type,
			right,
			condition,
		} => {
			if let BoundTableSource::Lateral {
				targets,
				..
			} = right.as_ref()
			{
				if !matches!(join_type, BoundJoinType::Inner | BoundJoinType::Cross) {
					return Err(PlanError::UnsupportedStatement(format!(
						"{join_type:?} join of a LATERAL subquery"
					)));
				}
				let left_op = lower_table_source(
					left,
					for_update,
					join_counter,
					policy,
					user_tables,
					user_types,
					database,
				)?;
				let alias_idx = *join_counter;
				*join_counter += 1;
				let op = lower_lateral(left_op, targets, alias_idx, policy)?;
				return match condition {
					None => Ok(op),
					Some(BoundJoinCondition::On(expr)) => Ok(LogicalOp::Filter(LogicalFilter {
						input: Box::new(op),
						predicate: lower_where(expr, policy)?,
					})),
					Some(BoundJoinCondition::Using(_)) => Err(PlanError::UnsupportedStatement(
						"USING on a join of a LATERAL subquery".into(),
					)),
				};
			}
			let left_op = lower_table_source(
				left,
				for_update,
				join_counter,
				policy,
				user_tables,
				user_types,
				database,
			)?;
			let right_op = lower_table_source(
				right,
				for_update,
				join_counter,
				policy,
				user_tables,
				user_types,
				database,
			)?;

			let kind = match join_type {
				BoundJoinType::Inner | BoundJoinType::Cross => JoinKind::Inner,
				BoundJoinType::Left => JoinKind::Left,
				BoundJoinType::Right | BoundJoinType::Full => {
					return Err(PlanError::UnsupportedStatement(format!(
						"Join type {join_type:?} not supported directly in streaming engine"
					)));
				}
			};

			let alias_idx = *join_counter;
			*join_counter += 1;
			let alias = RelationAlias::new(alias_idx);

			let mut on = Vec::new();
			let mut filter = None;
			let mut key_guards = Vec::new();

			if let Some(cond) = condition {
				match cond {
					BoundJoinCondition::On(expr) => {
						let mut equi_pairs = Vec::new();
						let mut residual = Vec::new();
						extract_equi_conditions(expr, &mut equi_pairs, &mut residual);

						// An equality that compares columns of the left input with each other is
						// not a key of the join but a condition on it: a row of the left
						// input that fails it still appears in a left join, with the right
						// input's columns null.
						for (l, r) in equi_pairs {
							let is_constant = |expr: &BoundExpr| {
								matches!(expr, BoundExpr::Literal(_) | BoundExpr::Param { .. })
							};
							let within_left = !is_constant(&l)
								&& !is_constant(&r) && matches!(
								(expr_max_relation_index(&l), expr_max_relation_index(&r)),
								(Some(l_max), Some(r_max)) if l_max <= alias_idx && r_max <= alias_idx
							);
							if within_left {
								residual.push(BoundExpr::Binary {
									left: Box::new(l),
									op: BoundBinaryOp::Equal,
									right: Box::new(r),
								});
								continue;
							}
							match probe_is_right(&l, &r, alias_idx) {
								Some(swap) => {
									let (probe, build) = if swap {
										(r, l)
									} else {
										(l, r)
									};
									let (probe_key, build_key) =
										lower_join_key(&probe, &build, policy)?;
									key_guards.push(null_key_guard(&probe_key, &build_key, &alias));
									on.push((probe_key, build_key));
								}
								None => residual.push(BoundExpr::Binary {
									left: Box::new(l),
									op: BoundBinaryOp::Equal,
									right: Box::new(r),
								}),
							}
						}

						if !residual.is_empty() {
							let mut combined: Option<LogicalExpr> = None;
							for res in residual {
								let lowered = lower_bound_expr(&res, policy)?;
								combined = match combined {
									Some(c) => Some(LogicalExpr::Binary {
										left: Box::new(c),
										op: BinaryOperator::And,
										right: Box::new(lowered),
									}),
									None => Some(lowered),
								};
							}
							filter = combined;
						}
					}
					BoundJoinCondition::Using(cols) => {
						for col in cols {
							let left_key = LogicalExpr::Field(col.clone().into());
							let right_key = RelationAlias::new(alias_idx).access(col.clone());
							let (probe_key, build_key) = if col == "id" {
								lower_using_id_key(left, right, left_key, right_key)
							} else {
								(left_key, right_key)
							};
							key_guards.push(null_key_guard(&probe_key, &build_key, &alias));
							on.push((probe_key, build_key));
						}
					}
				}
			}

			// The engine pairs rows whose keys are equal, a NULL key with a NULL one and a NONE key
			// with a NONE one, where in SQL a NULL equals nothing. The guards come first so that a
			// pair that cannot match is not judged by the residual.
			if let Some(guard) = key_guards.into_iter().reduce(|all, next| LogicalExpr::Binary {
				left: Box::new(all),
				op: BinaryOperator::And,
				right: Box::new(next),
			}) {
				filter = Some(match filter {
					Some(residual) => LogicalExpr::Binary {
						left: Box::new(guard),
						op: BinaryOperator::And,
						right: Box::new(residual),
					},
					None => guard,
				});
			}

			Ok(LogicalOp::Join(LogicalJoin::new(
				left_op,
				right_op,
				kind,
				on,
				filter,
				alias,
				EvalFrame::caller(),
			)))
		}
	}
}

/// The condition that the pair of keys `probe` and `build` of a join over `alias` is not a NULL
/// key, which the engine pairs with a NULL key and a NONE key with a NONE one.
///
/// Rows are paired only by equal keys, so both keys of a pair are NULL, both NONE or neither, and
/// the test of one of them serves. It tests the build key when that reads a field of the right row
/// as it is, which is the cheapest read and is a form the engine knows to raise no error, as
/// some plans of a join (holding a single left row) require of its condition. Otherwise it tests
/// the probe key.
fn null_key_guard(probe: &LogicalExpr, build: &LogicalExpr, alias: &RelationAlias) -> LogicalExpr {
	let reads_right_field =
		matches!(build, LogicalExpr::Access { base, .. } if **base == alias.field());
	crate::lower::expr::is_not_null_expr(if reads_right_field {
		build.clone()
	} else {
		probe.clone()
	})
}

/// `expr` with the key read of an `id` column undone: the stored field itself.
fn as_stored(expr: &BoundExpr) -> BoundExpr {
	match expr {
		BoundExpr::Column(col) => BoundExpr::Column(BoundColumnRef {
			id_table: None,
			..col.clone()
		}),
		other @ BoundExpr::Literal(_)
		| other @ BoundExpr::Access {
			..
		}
		| other @ BoundExpr::Param {
			..
		}
		| other @ BoundExpr::Binary {
			..
		}
		| other @ BoundExpr::Unary {
			..
		}
		| other @ BoundExpr::FunctionCall {
			..
		}
		| other @ BoundExpr::Case {
			..
		}
		| other @ BoundExpr::Between {
			..
		}
		| other @ BoundExpr::InList {
			..
		}
		| other @ BoundExpr::IsNull {
			..
		}
		| other @ BoundExpr::Cast {
			..
		}
		| other @ BoundExpr::ScalarSubquery(_)
		| other @ BoundExpr::Exists(_)
		| other @ BoundExpr::InSubquery {
			..
		}
		| other @ BoundExpr::ArraySubquery(_)
		| other @ BoundExpr::WindowFunction {
			..
		}
		| other @ BoundExpr::UserVar(_)
		| other @ BoundExpr::SystemVar {
			..
		} => other.clone(),
	}
}

/// The field that reads the stored record id an aggregation produced as the output `name` as its
/// key, under the same name. The group of the absent id an outer join leaves reads as absent.
fn key_output_field(name: String) -> LogicalField {
	LogicalField::user(
		key_of(LogicalExpr::Field(name.clone().into()), true),
		Some(OutputPath::field(name)),
	)
}

/// Lower a `WHERE` predicate.
///
/// A predicate is judged for truth alone. A top-level conjunct that compares the `id` of a base
/// table with keys is lowered as the lookup of the records the keys name, and its conjuncts are
/// joined by the engine's own `AND` so the engine finds the lookup among them. A predicate without
/// such a conjunct keeps the three-valued `AND`.
fn lower_where(expr: &BoundExpr, policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let mut conjuncts = Vec::new();
	collect_conjuncts(expr, &mut conjuncts);
	let lookups = conjuncts
		.iter()
		.map(|conjunct| lower_lookup_predicate(conjunct, policy))
		.collect::<Result<Vec<_>, _>>()?;
	if lookups.iter().all(Option::is_none) {
		return lower_bound_expr(expr, policy);
	}
	let mut lowered = conjuncts.into_iter().zip(lookups).map(|(conjunct, lookup)| match lookup {
		Some(lookup) => Ok(lookup),
		None => lower_bound_expr(conjunct, policy),
	});
	let Some(first) = lowered.next() else {
		return lower_bound_expr(expr, policy);
	};
	lowered.try_fold(first?, |acc, next| {
		Ok(LogicalExpr::Binary {
			left: Box::new(acc),
			op: BinaryOperator::And,
			right: Box::new(next?),
		})
	})
}

/// The operands of the `AND` chain `expr`, left to right.
fn collect_conjuncts<'a>(expr: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
	match expr {
		BoundExpr::Binary {
			left,
			op: BoundBinaryOp::And,
			right,
		} => {
			collect_conjuncts(left, out);
			collect_conjuncts(right, out);
		}
		other @ BoundExpr::Literal(_)
		| other @ BoundExpr::Column(_)
		| other @ BoundExpr::Access {
			..
		}
		| other @ BoundExpr::Param {
			..
		}
		| other @ BoundExpr::Binary {
			..
		}
		| other @ BoundExpr::Unary {
			..
		}
		| other @ BoundExpr::FunctionCall {
			..
		}
		| other @ BoundExpr::Case {
			..
		}
		| other @ BoundExpr::Between {
			..
		}
		| other @ BoundExpr::InList {
			..
		}
		| other @ BoundExpr::IsNull {
			..
		}
		| other @ BoundExpr::Cast {
			..
		}
		| other @ BoundExpr::ScalarSubquery(_)
		| other @ BoundExpr::Exists(_)
		| other @ BoundExpr::InSubquery {
			..
		}
		| other @ BoundExpr::ArraySubquery(_)
		| other @ BoundExpr::WindowFunction {
			..
		}
		| other @ BoundExpr::UserVar(_)
		| other @ BoundExpr::SystemVar {
			..
		} => out.push(other),
	}
}

/// The base table `source` reads when it is a single base table.
fn base_table_of(source: &BoundTableSource) -> Option<&str> {
	match source {
		BoundTableSource::Table {
			name,
			schema,
			..
		} if !crate::catalog::system::is_system_catalog_table(schema.as_deref(), name) => {
			Some(name.as_str())
		}
		BoundTableSource::Table {
			..
		}
		| BoundTableSource::Subquery {
			..
		}
		| BoundTableSource::Lateral {
			..
		}
		| BoundTableSource::Join {
			..
		}
		| BoundTableSource::Values => None,
	}
}

/// The base table `select` reads, when its only source is a single base table.
fn sole_base_table(select: &BoundSelect) -> Option<&str> {
	select.from.as_ref().and_then(base_table_of)
}

/// The base table whose row is the left row of a join over `source`: its leftmost relation.
fn leftmost_base_table(source: &BoundTableSource) -> Option<&str> {
	match source {
		BoundTableSource::Join {
			left,
			..
		} => leftmost_base_table(left),
		other @ BoundTableSource::Table {
			..
		}
		| other @ BoundTableSource::Subquery {
			..
		}
		| other @ BoundTableSource::Lateral {
			..
		}
		| other @ BoundTableSource::Values => base_table_of(other),
	}
}

/// Make a wildcard over a base table yield the key of the record id as its `id`, as an explicit
/// `id` column does: the wildcard, `*` or `t.*`, is followed by the key as `id`, which replaces the
/// stored record id the wildcard yields. `last_wildcard` is the position of the last wildcard of
/// the projection, if there is one.
fn read_wildcard_id_as_key(fields: &mut Vec<LogicalField>, last_wildcard: Option<usize>) {
	if let Some(last_all) = last_wildcard {
		fields.insert(
			last_all + 1,
			LogicalField::user(
				key_of(LogicalExpr::Field("id".into()), false),
				Some(OutputPath::field("id".to_string())),
			),
		);
	}
}

/// Lower an equality join key `probe = build`.
///
/// When the build side is the `id` of a base table, it keeps the shape of the stored column and
/// the probe side is turned into the record of that table its value names (a key, or a record link
/// to the row), unless the probe is the `id` of the same table, whose stored record id is compared
/// as it is. When the probe side is the `id` of a base table and the build side a column or an
/// expression, the stored id is compared with the record of its table the build value names, which
/// leaves a build key that is no longer a bare column for the engine to read the build table by an
/// index.
///
/// A link column is joined as stored with the `id` of its target table and with a link column to
/// the same table. A link column on either side of anything else is joined with the record of its
/// target table the other side names. Otherwise the keys are lowered as they are.
fn lower_join_key(
	probe: &BoundExpr,
	build: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<(LogicalExpr, LogicalExpr), PlanError> {
	if let Some((probe_col, build_col)) = same_table_pair(probe, build) {
		return Ok((lower_column_raw(probe_col), lower_column_raw(build_col)));
	}
	if let Some((build_col, build_table)) = id_column(build) {
		if let Some((probe_col, probe_table)) = id_column(probe)
			&& probe_table == build_table
		{
			return Ok((lower_column_raw(probe_col), lower_column_raw(build_col)));
		}
		return Ok((
			record_of(build_table, lower_stored(probe, policy)?),
			lower_column_raw(build_col),
		));
	}
	if let Some((probe_col, probe_table)) = id_column(probe)
		&& is_key_or_link(build)
	{
		return Ok((
			lower_column_raw(probe_col),
			record_of(probe_table, lower_stored(build, policy)?),
		));
	}
	if let Some((build_col, build_table)) = link_column(build)
		&& let LinkOperand::Record(record) =
			link_operand(build_table, build_col.key_read, probe, policy)?
	{
		return Ok((record, lower_column_raw(build_col)));
	}
	if let Some((probe_col, probe_table)) = link_column(probe)
		&& let LinkOperand::Record(record) =
			link_operand(probe_table, probe_col.key_read, build, policy)?
	{
		return Ok((lower_column_raw(probe_col), record));
	}
	Ok((lower_stored(probe, policy)?, lower_stored(build, policy)?))
}

/// Lower the keys of `USING (id)` between `left` and `right`, whose row is read through
/// `right_key`. The key shapes follow the rules of [`lower_join_key`].
fn lower_using_id_key(
	left: &BoundTableSource,
	right: &BoundTableSource,
	left_key: LogicalExpr,
	right_key: LogicalExpr,
) -> (LogicalExpr, LogicalExpr) {
	match (leftmost_base_table(left), base_table_of(right)) {
		(Some(left_table), Some(right_table)) if left_table == right_table => (left_key, right_key),
		(Some(_), Some(right_table)) => {
			(record_of(right_table, key_of(left_key, false)), right_key)
		}
		(None, Some(right_table)) => (record_of(right_table, left_key), right_key),
		(Some(_), None) => (key_of(left_key, false), right_key),
		(None, None) => (left_key, right_key),
	}
}

fn extract_equi_conditions(
	expr: &BoundExpr,
	on: &mut Vec<(BoundExpr, BoundExpr)>,
	residual: &mut Vec<BoundExpr>,
) {
	match expr {
		BoundExpr::Binary {
			left,
			op: BoundBinaryOp::And,
			right,
		} => {
			extract_equi_conditions(left, on, residual);
			extract_equi_conditions(right, on, residual);
		}
		// A literal that spells a boolean is read as the type of the other side, which a key
		// comparison of the two values would not do.
		BoundExpr::Binary {
			left,
			op: BoundBinaryOp::Equal,
			right,
		} if pg_bool_spelling(left).is_none() && pg_bool_spelling(right).is_none() => {
			on.push((*left.clone(), *right.clone()));
		}
		other @ (BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Access {
			..
		}
		| BoundExpr::Param {
			..
		}
		| BoundExpr::Binary {
			..
		}
		| BoundExpr::Unary {
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
		| BoundExpr::Cast {
			..
		}
		| BoundExpr::FunctionCall {
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
		}) => {
			residual.push(other.clone());
		}
	}
}

/// Whether the key of the rows on the left is the right side of the equality `l = r` in the
/// condition of the join whose right relation is `alias_idx + 1`, or `None` when the equality is
/// no key of the join.
///
/// A side orients the pair by the highest relation it reads. A side that reads a column whose
/// relation the schema left open ([`BoundColumnRef::also_in`]) is the opposite of a side that
/// names a relation, as the two sides of a key are. When no side names a relation (both are open,
/// or one is open and the other a literal or a parameter) the equality is judged on the joined
/// row.
fn probe_is_right(l: &BoundExpr, r: &BoundExpr, alias_idx: usize) -> Option<bool> {
	let is_constant =
		|expr: &BoundExpr| matches!(expr, BoundExpr::Literal(_) | BoundExpr::Param { .. });
	match (expr_max_relation_index(l), expr_max_relation_index(r)) {
		(Some(l_max), Some(r_max)) => Some(l_max > r_max),
		(None, Some(r_max)) if !is_constant(r) => Some(r_max <= alias_idx),
		(Some(l_max), None) if !is_constant(l) => Some(l_max > alias_idx),
		(None, _) | (_, None) => None,
	}
}

/// The highest relation index of the columns `expr` reads, or `None` when it reads a column whose
/// relation the schema left open ([`BoundColumnRef::also_in`]).
fn expr_max_relation_index(expr: &BoundExpr) -> Option<usize> {
	match expr {
		BoundExpr::Column(col) if col.also_in.is_empty() => Some(col.relation_index),
		BoundExpr::Column(_) => None,
		BoundExpr::Access {
			base,
			..
		} => expr_max_relation_index(base),
		BoundExpr::Binary {
			left,
			right,
			..
		} => Some(expr_max_relation_index(left)?.max(expr_max_relation_index(right)?)),
		BoundExpr::Unary {
			expr,
			..
		} => expr_max_relation_index(expr),
		BoundExpr::Literal(_)
		| BoundExpr::Param {
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
		| BoundExpr::Cast {
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
		} => Some(0),
	}
}
