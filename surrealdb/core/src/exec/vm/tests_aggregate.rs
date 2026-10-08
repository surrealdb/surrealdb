//! An aggregate's keys, arguments, extra arguments, answers over its groups
//! and constant columns are lowered into the program as instructions and
//! pipelines, its functions are names resolved when a run starts, and the VM
//! answers every aggregate.

use super::golden::Golden;
use super::program::{AggSpec, Instr, Program};
use super::tests::{SCHEMA, bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::AggregateId;
use crate::exec::operators::test_util::TestDb;

/// Aggregates the VM covers: computed keys, computed arguments, arguments after the first, answers
/// computed over several aggregates and keys, constant columns, and values a row cannot give
/// an aggregate.
const AGGREGATES: &[&str] = &[
	// computed arguments
	"SELECT city, math::sum(age * 2) AS s FROM person GROUP BY city",
	"SELECT math::mean(age + 1) AS m, math::max(age - 1) AS hi FROM person GROUP ALL",
	"SELECT math::min(string::len(name)) AS lo, math::max(string::len(name)) AS hi FROM person GROUP ALL",
	"SELECT count(age > 20) AS adults FROM person GROUP ALL",
	"SELECT array::group(nested.x) AS xs FROM person GROUP ALL",
	"SELECT city, array::group(string::uppercase(name)) AS names FROM person GROUP BY city",
	"SELECT math::stddev(age) AS sd, math::variance(age) AS v FROM person GROUP ALL",
	"SELECT math::mean(i % 7) AS m, count() AS c FROM big GROUP ALL",
	// computed keys
	"SELECT age % 2 AS parity, count() AS c FROM person GROUP BY parity",
	"SELECT string::len(name) AS l, count() AS c FROM person GROUP BY l",
	"SELECT age > 20 AS adult, count() AS c, math::max(age) AS hi FROM person GROUP BY adult",
	"SELECT n % 3 AS k, count() AS c, math::sum(i) AS s FROM big GROUP BY k",
	// answers computed over aggregates and keys
	"SELECT city, math::sum(age) / count() AS avg FROM person GROUP BY city",
	"SELECT math::sum(age) + math::max(age) AS t FROM person GROUP ALL",
	"SELECT age, math::sum(age) + age AS t FROM person GROUP BY age",
	"SELECT math::round(math::mean(age)) AS m FROM person GROUP ALL",
	"SELECT array::distinct(array::group(city)) AS cities FROM person GROUP ALL",
	"SELECT count() * 2 AS twice, math::max(age) - math::min(age) AS span FROM person GROUP ALL",
	// arguments after the first
	"SELECT array::join(name, '-') AS j FROM person GROUP ALL",
	"SELECT city, array::join(name, ', ') AS j FROM person GROUP BY city",
	"SELECT array::join(name, string::concat('<', '>')) AS j FROM person GROUP ALL",
	"SELECT age, array::join(name, string::concat('<', age)) AS j FROM person GROUP BY age",
	// constant columns
	"SELECT 'k' AS label, count() AS c FROM person GROUP ALL",
	"SELECT $limit AS l, count() AS c FROM person GROUP ALL",
	"SELECT math::abs(-7) AS k, count() AS c FROM person WHERE age > 1000 GROUP ALL",
	"SELECT $missing AS z, count() AS c FROM person GROUP ALL",
	"SELECT city, 7 AS seven, count() AS c FROM person GROUP BY city",
	"SELECT city, $missing AS z, count() AS c FROM person GROUP BY city",
	"SELECT city, $limit + 1 AS l FROM person GROUP BY city",
	"SELECT VALUE 5 FROM person GROUP ALL",
	// values a row cannot give an aggregate
	"SELECT math::sum(<int> name) AS s FROM person GROUP ALL",
	"SELECT city, count(<int> name) AS c FROM person GROUP BY city",
	"SELECT count(age + 'x') AS c FROM person GROUP ALL",
	"SELECT <int> name AS k, count() AS c FROM person GROUP BY k",
	"SELECT math::sum(age / 0) AS s FROM person GROUP ALL",
	"SELECT math::sum((SELECT VALUE age FROM person LIMIT 1)[0] + name) AS s FROM person GROUP ALL",
	"SELECT math::sum((SELECT VALUE count() FROM person GROUP ALL)[0] + age) AS s FROM person GROUP ALL",
	"SELECT math::sum(n) AS s FROM mixed GROUP ALL",
	"SELECT math::sum(n + 1) AS s, math::mean(n * 2) AS m FROM mixed GROUP ALL",
	// the functions the VM names
	"SELECT count() AS c, math::mean(age) AS m, math::stddev(age) AS sd, math::min(age) AS lo, math::max(age) AS hi FROM person GROUP ALL",
	"SELECT city, count() AS c, math::mean(age) AS m, math::min(age) AS lo, math::max(age) AS hi FROM person GROUP BY city",
	"SELECT VALUE math::median(age) FROM person GROUP ALL",
	// SPLIT and GROUP
	"SELECT tags, count() AS c FROM (SELECT tags FROM person SPLIT tags) GROUP BY tags",
	"SELECT tags, math::sum(age * 2) AS s FROM (SELECT tags, age FROM person SPLIT tags) GROUP BY tags",
	"SELECT count() AS c FROM (SELECT tags FROM person SPLIT tags) GROUP ALL",
	// grouping rows that are not records
	"SELECT count() AS c, math::sum(a * 2) AS s FROM [{ a: 1 }, { a: 2 }, { a: 'x' }, 5] GROUP ALL",
	"SELECT k, math::sum(a + 1) AS s FROM [{ k: 1, a: 1 }, { k: 1, a: 2 }, { k: 2, a: 3 }] GROUP BY k",
	"SELECT math::mean(a.b) AS m FROM [{ a: { b: 1 } }, { a: { b: 3 } }, { a: 7 }] GROUP ALL",
	// across sources and clauses
	"SELECT age % 3 AS k, count() AS c FROM indexed WHERE age > 2 GROUP BY k",
	"SELECT kind * 2 AS k, math::sum(age + 1) AS s FROM indexed GROUP BY k ORDER BY k",
	"SELECT age % 2 AS parity, count() AS c FROM person GROUP BY parity ORDER BY c DESC LIMIT 1",
];

async fn lowered(ctx: &ExecutionContext, sql: &str) -> Program {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	super::lower::lower(&planner(ctx), &physical).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

#[tokio::test]
async fn aggregates_answer_their_groups() {
	let mut golden = Golden::new("aggregates_answer_their_groups");
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await.with_param("limit", crate::val::Value::from(30));
	let mut declined = Vec::new();
	for sql in AGGREGATES {
		let bound = bind(&ctx, sql).await;
		let Some(vm) = super::plan(&planner(&ctx), &bound).await else {
			declined.push(*sql);
			continue;
		};
		golden.rows(sql, &outcome(&vm, &ctx).await);
	}
	assert!(declined.is_empty(), "the VM declined {declined:#?}");
	golden.check();
}

/// A key, an argument, an argument after the first, an answer over the group
/// and a constant column the body answers are instructions and pipelines of
/// the program: the statement lowers.
#[tokio::test]
async fn an_aggregate_the_body_answers_lowers() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await.with_param("limit", crate::val::Value::from(30));
	for sql in [
		"SELECT city, math::sum(age * 2) AS s FROM person GROUP BY city",
		"SELECT age % 2 AS parity, count() AS c FROM person GROUP BY parity",
		"SELECT city, math::sum(age) / count() AS avg FROM person GROUP BY city",
		"SELECT array::join(name, string::concat('<', '>')) AS j FROM person GROUP ALL",
		"SELECT $limit AS l, count() AS c FROM person GROUP ALL",
		"SELECT city, 7 AS seven, count() AS c FROM person GROUP BY city",
		"SELECT math::sum(<int> name) AS s FROM person GROUP ALL",
	] {
		lowered(&ctx, sql).await;
	}
}

/// An argument that can fail on the shape of the data is read inside a region
/// that answers `NONE`; a plain field and a literal need none.
#[tokio::test]
async fn an_argument_that_can_fail_is_read_in_a_region() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let regions = |program: &Program| {
		program
			.pipelines
			.iter()
			.flat_map(|p| &p.code)
			.filter(|i| matches!(i, Instr::TryNone { .. }))
			.count()
	};
	let computed = lowered(&ctx, "SELECT math::sum(age * 2) AS s FROM person GROUP ALL").await;
	assert_eq!(regions(&computed), 1);
	let plain = lowered(&ctx, "SELECT city, math::sum(age) AS s FROM person GROUP BY city").await;
	assert_eq!(regions(&plain), 0);
	let ends = |program: &Program| {
		program
			.pipelines
			.iter()
			.flat_map(|p| &p.code)
			.filter(|i| matches!(i, Instr::TryEnd))
			.count()
	};
	assert_eq!(ends(&computed), regions(&computed));
}

fn specs(program: &Program) -> &[AggSpec] {
	&program.aggregates[0].fields
}

/// A program names its aggregate functions and holds the pipelines of what
/// its groups finish with, whatever the registry holds.
#[tokio::test]
async fn an_aggregate_names_its_functions_and_finishing_pipelines() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	let program = lowered(
		&ctx,
		"SELECT city, math::sum(age) / count(age) AS avg, array::join(name, ', ') AS j, 'k' AS label FROM person GROUP BY city",
	)
	.await;
	let fields = specs(&program);
	assert_eq!(fields.len(), 4);
	assert!(fields[0].is_group_key);
	let average = fields[1].aggregate.as_ref().expect("an aggregate");
	let names: Vec<_> = average.calls.iter().map(|call| call.name.clone()).collect();
	assert_eq!(names, [AggregateId::Named("math::sum".into()), AggregateId::CountField]);
	assert!(average.post.is_some());
	let join = fields[2].aggregate.as_ref().expect("an aggregate");
	assert_eq!(join.calls[0].extra_args.len(), 1);
	assert!(join.post.is_none());
	// Under `GROUP BY` a constant column is evaluated by the body as its group
	// takes rows; only `GROUP ALL` evaluates it as the group finishes.
	assert!(fields[3].aggregate.is_none() && fields[3].finish.is_none());
	let all = lowered(&ctx, "SELECT 'k' AS label, count() AS c FROM person GROUP ALL").await;
	assert!(specs(&all)[0].finish.is_some());
	let collected = lowered(&ctx, "SELECT city, name FROM person GROUP BY city").await;
	let collect = specs(&collected)[1].aggregate.as_ref().expect("an aggregate");
	assert_eq!(collect.calls[0].name, AggregateId::ImplicitCollect);
}

/// A call that advances a sequence runs once for each row, in the order of the
/// rows.
#[tokio::test]
async fn a_non_deterministic_argument_is_evaluated_once_per_row() {
	let mut golden = Golden::new("a_non_deterministic_argument_is_evaluated_once_per_row");
	for sql in [
		"SELECT array::group(sequence::nextval('s')) AS n FROM person GROUP ALL",
		"SELECT city, array::group(sequence::nextval('s')) AS n FROM person GROUP BY city",
		"SELECT math::sum(sequence::nextval('s')) AS n FROM person GROUP ALL",
		"SELECT sequence::nextval('s') AS n, count() AS c FROM person GROUP ALL",
	] {
		let db =
			TestDb::new_with_auth(&format!("{SCHEMA} DEFINE SEQUENCE s BATCH 1000 START 1;")).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		golden.rows(sql, &outcome(&op, &ctx).await);
	}
	golden.check();
}

/// A column that is constant for the group is evaluated once for it, however
/// many rows it holds.
#[tokio::test]
async fn a_constant_column_is_evaluated_once_per_group() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT city, rand::uuid() AS u, count() AS c FROM person GROUP BY city",
		"SELECT rand::uuid() AS u, count() AS c FROM person GROUP ALL",
	] {
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("covered");
		let rows = outcome(&vm, &ctx).await.expect("runs");
		let mut seen = Vec::new();
		for row in &rows {
			let crate::val::Value::Object(object) = row else {
				panic!("`{sql}` answered {row:?}");
			};
			let uuid = object.get("u").expect("a column");
			assert!(matches!(uuid, crate::val::Value::Uuid(_)), "`{sql}`: {uuid:?}");
			assert!(!seen.contains(uuid), "`{sql}` shared a draw between groups");
			seen.push(uuid.clone());
		}
	}
}
