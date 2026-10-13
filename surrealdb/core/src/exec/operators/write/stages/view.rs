//! The foreign views a write maintains.
//!
//! One stage with three arms, because a
//! `DEFINE TABLE … AS SELECT` produces three different obligations:
//!
//! - a view nothing maintains, which a write does nothing for;
//! - a **materialized** view, which holds one row per source record, under that record's own key;
//! - an **aggregated** view, which holds one row per group, whose stored stats a write adjusts by
//!   the delta the record made rather than by recomputing the group.
//!
//! # A view row is a record
//!
//! Both maintained arms end the same way: the row they wrote or deleted is
//! driven through [the view-maintenance
//! order](super::super::plan::WritePlan::view_maintenance), which is the view
//! table's own indexes, views, events, subscriptions and change feed. A view
//! table is a real table — a second-order view can read it, a subscription can
//! watch it, an index can cover it — and `DEFINE TABLE … AS SELECT`
//! runs that lifecycle over every row of its initial build.
//!
//! # The frame
//!
//! Every clause here runs with permission enforcement off and writes allowed,
//! under the *writer's* authority: a view definition records no `AUTH LIMIT`,
//! so there is nothing to narrow to. See
//! [`view_frame`](crate::exec::permission::view_frame).

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::catalog::aggregation::{AggregationAnalysis, AggregationStat, Recalculation};
use crate::catalog::{Metadata, Record, RecordType};
use crate::exec::Error as ExecError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::expr::field::Selector;
use crate::expr::logical::LogicalOp;
use crate::expr::statements::SelectStatement;
use crate::expr::{
	BinaryOperator, Cond, ControlFlow, Expr, Fields, Function, FunctionCall, Groups, Literal, Param,
};
use crate::key::schema::RecordKey;
use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

#[cfg(test)]
thread_local! {
	/// How many times this thread ran the views stage's body over a record, for
	/// tests that must know a write maintained its views as instructions.
	pub(crate) static VIEW_BODIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// What an aggregated view's stored definition says it is over: the pieces a
/// group's row and its recalculation read beside the view's clauses.
pub(crate) struct ViewSources<'a> {
	/// The stored analysis, which says how many stats a new group starts with
	/// and how the recalculation names them.
	pub(crate) analysis: &'a AggregationAnalysis,
	/// The tables the view selects from, which a recalculation re-reads.
	pub(crate) tables: &'a [TableName],
	/// The view's condition in its stored form, which a recalculation ANDs
	/// into its own.
	pub(crate) condition: Option<&'a Expr>,
}

// ===========================================================================
// One row per group
// ===========================================================================

/// The extremes `analysis`'s stats keep, each as the aggregate function that recomputes it and
/// the aggregate argument it reads, once each, in the order of the aggregations that first keep
/// it: the columns of a recalculation before its count.
pub(crate) fn extremes_of(analysis: &AggregationAnalysis) -> Vec<(String, usize)> {
	let mut extremes: Vec<(String, usize)> = Vec::new();
	for aggregation in &analysis.aggregations {
		let extreme = match aggregation.to_stat() {
			AggregationStat::NumberMax {
				arg,
				..
			} => ("math::max", arg),
			AggregationStat::NumberMin {
				arg,
				..
			} => ("math::min", arg),
			AggregationStat::TimeMax {
				arg,
				..
			} => ("time::max", arg),
			AggregationStat::TimeMin {
				arg,
				..
			} => ("time::min", arg),
			AggregationStat::Count {
				..
			}
			| AggregationStat::CountValue {
				..
			}
			| AggregationStat::Sum {
				..
			}
			| AggregationStat::Mean {
				..
			}
			| AggregationStat::StdDev {
				..
			}
			| AggregationStat::Variance {
				..
			}
			| AggregationStat::Accumulate {
				..
			} => continue,
		};
		let extreme = (extreme.0.to_owned(), extreme.1);
		if !extremes.contains(&extreme) {
			extremes.push(extreme);
		}
	}
	extremes
}

/// The names a recalculation reads the key values of its group from, one per group expression
/// of `analysis`, in order.
pub(crate) fn group_names(analysis: &AggregationAnalysis) -> Vec<String> {
	(0..analysis.group_expressions.len()).map(|index| format!("__view_group_{index}")).collect()
}

/// The statement [`recalculate_aggregates`] runs: `SELECT VALUE [extremes…,
/// count()]` over the view's sources, narrowed to the group whose key values
/// are the parameters `names` and by the view's own condition, `GROUP ALL`.
pub(crate) fn recalculation_of(
	target: &ViewSources<'_>,
	recalculations: &[Recalculation],
	names: &[String],
) -> Result<LogicalOp, ControlFlow> {
	let analysis = target.analysis;
	let mut exprs: Vec<Expr> = recalculations
		.iter()
		.map(|recalculation| {
			Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal(recalculation.function.clone()),
				arguments: vec![analysis.aggregate_arguments[recalculation.arg].clone()],
			}))
		})
		.collect();
	exprs.push(Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("count".to_string()),
		arguments: Vec::new(),
	})));

	// The group-key equalities lead and the view's condition is ANDed last:
	// `&&` short-circuits left to right, so a row of another group is rejected
	// on a key comparison without the view's condition being evaluated for it.
	let mut cond: Option<Expr> = None;
	for (index, expr) in analysis.group_expressions.iter().enumerate() {
		let equality = Expr::Binary {
			left: Box::new(expr.clone()),
			op: BinaryOperator::Equal,
			right: Box::new(Expr::Param(Param::from(names[index].clone()))),
		};
		cond = Some(conjoin(cond, equality));
	}
	if let Some(view_condition) = target.condition.cloned() {
		cond = Some(conjoin(cond, view_condition));
	}

	let statement = SelectStatement {
		// SELECT VALUE [recalc1, .., count()]
		fields: Fields::Value(Box::new(Selector {
			expr: Expr::Literal(Literal::Array(exprs)),
			alias: None,
		})),
		// FROM source1, source2, .. — `ONLY` is rejected for several targets
		// without a limit, so the single `GROUP ALL` row is taken below.
		only: false,
		what: target.tables.iter().cloned().map(Expr::Table).collect(),
		cond: cond.map(Cond),
		group: Some(Groups(Vec::new())),
		omit: vec![],
		with: None,
		split: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	};
	crate::expr::logical::lower_select(statement)
		.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
}

/// `left && right`, or `right` alone when there is no left.
fn conjoin(left: Option<Expr>, right: Expr) -> Expr {
	match left {
		Some(left) => Expr::Binary {
			left: Box::new(left),
			op: BinaryOperator::And,
			right: Box::new(right),
		},
		None => right,
	}
}

// ===========================================================================
// Shared
// ===========================================================================

/// A view row as it is stored.
///
/// Decoded as written rather than through the record decoder, because a view
/// row's `data` carries no `id`: the row's identity is its key, and splicing it
/// in would change what the view table's own events and subscribers are shown.
pub(crate) async fn read_view_row(
	ctx: &WriteCtx,
	table: &TableName,
	key: &RecordIdKey,
) -> Result<Option<Arc<Record>>, ControlFlow> {
	let record_key = RecordKey {
		ns: ctx.ns.namespace_id,
		db: ctx.db.database_id,
		tb: Cow::Borrowed(table),
		id: Cow::Borrowed(key),
	};
	match ctx.txn().get_key_raw(&record_key, None).await.map_err(ControlFlow::Err)? {
		Some(bytes) => Ok(Some(Arc::new(
			ctx.txn()
				.decode_record_as_written(
					ctx.ns.namespace_id,
					ctx.db.database_id,
					table,
					&bytes,
					RecordId {
						table: table.clone(),
						key: key.clone(),
					},
					None,
				)
				.await
				.map_err(ControlFlow::Err)?,
		))),
		None => Ok(None),
	}
}

/// The row a group no record was in yet starts from: no data, and the
/// analysis's own stats.
pub(crate) fn new_group(analysis: &AggregationAnalysis) -> Record {
	Record {
		data: Value::None,
		metadata: Some(Metadata {
			record_type: RecordType::Table,
			aggregation_stats: analysis
				.aggregations
				.iter()
				.map(|aggregation| aggregation.to_stat())
				.collect(),
		}),
	}
}

/// The failure of a record leaving `group` of `table`, which holds no row for
/// the group.
pub(crate) fn left_no_row(group: &[Value], table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"a record left the group {} of the view {}, which holds no row for it",
		Value::Array(Array(group.to_vec())).to_sql(),
		table
	))))
}

/// The failure of a record changing within `group` of `table`, which holds no
/// row for the group.
pub(crate) fn changed_no_row(group: &[Value], table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"a record changed within the group {} of the view {}, which holds no row for it",
		Value::Array(Array(group.to_vec())).to_sql(),
		table
	))))
}

/// The failure of a row of `table` whose stats hold no record count.
pub(crate) fn no_record_count(table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"the view {table} holds a row with no record count"
	))))
}

/// The stored stats of a view row, which every aggregated row carries.
pub(crate) fn stats_of(record: &mut Record) -> Result<&mut [AggregationStat], ControlFlow> {
	match record.metadata.as_mut() {
		Some(metadata) => Ok(&mut metadata.aggregation_stats),
		None => Err(ControlFlow::Err(anyhow::anyhow!(crate::err::EngineError::unreachable(
			"a row of an aggregated view carries no stats"
		)))),
	}
}

/// The group values a view row's key is made of.
///
/// An aggregated view keys each group row by the array of its group values, so
/// a key of any other shape belongs to no group and names none.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "only an array key carries group values; every other key shape carries none"
)]
pub(crate) fn group_values(key: &RecordIdKey) -> Vec<Value> {
	match key {
		RecordIdKey::Array(Array(values)) => values.clone(),
		_ => Vec::new(),
	}
}
