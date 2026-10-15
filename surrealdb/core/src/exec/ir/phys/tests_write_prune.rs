//! Tests of the write-prune pass, over an in-memory datastore.
//!
//! The structural tests plan a statement and read the stage ops of its physical write. The
//! differential tests run the same statements on two datastores, one planned with the pass and one
//! without it, and compare what each answers and leaves behind.

use surrealdb_strand::TableName;

use super::write_prune::{Disabled, Judged, dormancy, judged, take_tally};
use crate::dbs::Session;
use crate::doc::Action;
use crate::exec::ir::core::ir::{IrScope, Walk, WalkOrder};
use crate::exec::ir::logical::ops::{GateClause, WriteStageKind};
use crate::exec::ir::logical::stages::{body_fault, write_stage_ops};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::ir::tests_plan_request::plan_on;
use crate::exec::ir::unit::UnitKind;
use crate::exec::ir::{RequestInput, RequestStatement};
use crate::exec::operators::test_util::TestDb;

/// What the first physical write of `sql` holds, planned over a database `setup` builds.
struct Planned {
	/// The stage ops of the write's loop body, in order.
	run: Vec<WriteStageKind>,
	/// Whether the write's body names every stage op and no other.
	body_fits: bool,
	/// The stages of each order the body names, in the order they run: the creating order, then
	/// the updating order of a form with two.
	orders: Vec<Vec<WriteStageKind>>,
	/// Whether the write states that its `WHERE` condition is never satisfied.
	where_never: bool,
}

impl Planned {
	fn has_select_gate(&self) -> bool {
		self.run.iter().any(|stage| matches!(stage, WriteStageKind::CheckSelectPermissions(_)))
	}
}

async fn planned(setup: &str, sql: &str) -> Planned {
	planned_on(&TestDb::new(setup).await, sql).await
}

async fn planned_on(db: &TestDb, sql: &str) -> Planned {
	let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
	let statements = RequestStatement::of(&parsed);
	let input = RequestInput::request(
		&statements,
		IrScope::Request {
			fragment: None,
		},
	);
	let planned = plan_on(db, &input).await.expect("the request plans");
	let ir = &planned.ir;
	let mut found = None;
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(data) = ir.op(op)
			&& let OpKind::Phys(PhysOp::Write(attrs)) = &data.kind
			&& found.is_none()
		{
			let ops = write_stage_ops(ir, data).unwrap_or_default();
			let run: Vec<WriteStageKind> = ops.into_iter().map(|(_, stage)| stage).collect();
			let body = &attrs.write.body;
			let orders = [&body.first, &body.second]
				.into_iter()
				.filter(|order| !order.is_empty())
				.map(|order| order.iter().map(|at| run[usize::from(*at)]).collect())
				.collect();
			found = Some(Planned {
				body_fits: body_fault(body, run.len()).is_none(),
				run,
				orders,
				where_never: attrs.write.flags.where_never,
			});
		}
		Walk::Continue
	});
	found.expect("the request holds a physical write")
}

/// Tables `tb` and `r`, each with `select` as its select clause and every other action `FULL`.
fn tables(select: &str) -> String {
	format!(
		"DEFINE TABLE tb SCHEMALESS PERMISSIONS FOR select {select} FOR create, update, delete FULL;
		 DEFINE TABLE r SCHEMALESS PERMISSIONS FOR select {select} FOR create, update, delete FULL;"
	)
}

/// One statement of each write form, whose output is `output`. Every record the statements write
/// has an id of its own, so two runs of them over equal data answer alike.
fn forms(output: &str) -> Vec<String> {
	vec![
		format!("CREATE tb:10 SET age = 1 RETURN {output}"),
		format!("UPDATE tb SET age = 2 RETURN {output}"),
		format!("UPSERT tb:11 SET age = 3 RETURN {output}"),
		format!("UPSERT tb:1 SET age = 3 RETURN {output}"),
		format!("INSERT INTO tb {{ id: tb:12, age: 4 }} RETURN {output}"),
		format!(
			"INSERT INTO tb {{ id: tb:12, age: 4 }} ON DUPLICATE KEY UPDATE age = 5 RETURN {output}"
		),
		format!("RELATE tb:1->r:e1->tb:2 SET age = 6 RETURN {output}"),
		format!("DELETE tb RETURN {output}"),
	]
}

/// Every write form drops its select gate under `RETURN NONE` when the clause is `FULL`, `NONE` or
/// an inert predicate, and keeps it under every other output. The clauses and statements are those
/// of the `return_none_pruned_gate` language test, so that test runs the pruned path.
#[tokio::test]
async fn return_none_drops_an_inert_select_gate_of_every_form() {
	for select in ["FULL", "NONE", "WHERE 1 < 2", "WHERE $auth != NONE", "WHERE true AND !(1 > 2)"]
	{
		let setup = tables(select);
		for sql in forms("NONE") {
			let held = planned(&setup, &sql).await;
			assert!(!held.has_select_gate(), "{select}: {sql}: {:?}", held.run);
			assert!(held.body_fits, "{select}: {sql}");
		}
		for output in ["AFTER", "BEFORE", "DIFF", "NULL", "id", "$after.age, $before.age"] {
			for sql in forms(output) {
				let held = planned(&setup, &sql).await;
				assert!(held.has_select_gate(), "{select}: {sql}");
				assert!(held.body_fits, "{select}: {sql}");
			}
		}
		for sql in [
			"CREATE tb:10 SET age = 1",
			"UPDATE tb SET age = 2",
			"DELETE tb",
			"UPSERT tb:11 SET age = 3",
			"INSERT INTO tb { id: tb:12, age: 4 }",
			"RELATE tb:1->r:e1->tb:2 SET age = 6",
		] {
			let held = planned(&setup, sql).await;
			assert!(held.has_select_gate(), "{select}: {sql}");
		}
	}
}

/// The select gate is followed by the output stage alone in every order of every form, so a write
/// that does not judge it still runs every stage that stores, indexes, maintains a view, fires an
/// event, notifies a live query and records a change feed.
#[tokio::test]
async fn nothing_but_the_output_follows_the_select_gate() {
	let setup = tables("FULL");
	for sql in forms("AFTER") {
		let held = planned(&setup, &sql).await;
		for order in &held.orders {
			let gate = order
				.iter()
				.position(|stage| matches!(stage, WriteStageKind::CheckSelectPermissions(_)))
				.unwrap_or_else(|| panic!("{sql}: {order:?}"));
			assert_eq!(order[gate + 1..], [WriteStageKind::OutputWrite], "{sql}: {order:?}");
			assert!(order.contains(&WriteStageKind::ProcessChangefeeds), "{sql}: {order:?}");
		}
	}
}

/// A write whose targets name no one table keeps its select gate: the clause is not stated at
/// bind, and the stages come from per-table bundles.
#[tokio::test]
async fn a_write_that_names_no_one_table_keeps_its_gate() {
	let setup = tables("FULL");
	let held = planned(&setup, "UPDATE (SELECT VALUE id FROM tb) SET age = 1 RETURN NONE").await;
	assert!(held.has_select_gate(), "{:?}", held.run);
}

/// A predicate that reads a field of the record keeps the gate: the analyses declare a read of a
/// row field as one that can fail, so the select predicates of the permission benchmarks
/// (`age < 200`, and a predicate that reads a `COMPUTED` field) are not inert.
#[tokio::test]
async fn a_predicate_that_reads_a_field_keeps_the_gate() {
	let where_perms = "DEFINE TABLE record SCHEMALESS PERMISSIONS FOR select, update, delete WHERE age < 200 FOR create FULL;";
	let held = planned(where_perms, "UPDATE record SET active = true RETURN NONE").await;
	assert!(held.has_select_gate(), "where: {:?}", held.run);
	let held = planned(where_perms, "DELETE record WHERE age <= 50 RETURN NONE").await;
	assert!(held.has_select_gate(), "where, delete: {:?}", held.run);
	let derived = "DEFINE TABLE record SCHEMALESS PERMISSIONS FOR select WHERE age_band != 'x' FOR update, delete WHERE age < 200 FOR create FULL;
		DEFINE FIELD age_band ON record COMPUTED string::concat('b', <string> age);";
	let held = planned(derived, "UPDATE record SET active = true RETURN NONE").await;
	assert!(held.has_select_gate(), "where_derived: {:?}", held.run);
}

/// A predicate that can raise, or has an effect, keeps the gate under `RETURN NONE`.
#[tokio::test]
async fn a_select_predicate_that_can_fail_or_act_keeps_the_gate() {
	for select in [
		"WHERE <int> code > 0",
		"WHERE rand() < 2",
		"WHERE http::get('http://127.0.0.1:1/') != NONE",
		"WHERE fn::allowed(code)",
		"WHERE (SELECT VALUE id FROM tb) != NONE",
		"WHERE array::len(tags) > 0",
	] {
		let setup = format!(
			"{} DEFINE FUNCTION fn::allowed($c: any) {{ RETURN true }} PERMISSIONS FULL;",
			tables(select)
		);
		let held = planned(&setup, "UPDATE tb SET code = 1 RETURN NONE").await;
		assert!(held.has_select_gate(), "{select}: {:?}", held.run);
	}
}

/// A gate that derives a computed field keeps its op, whatever the derivation is: a predicate reads
/// a computed field through an op the analyses answer with the top element. That holds when the
/// derivation can fail, has an effect, cannot fail at all, or declares a kind.
#[tokio::test]
async fn a_select_derivation_keeps_the_gate() {
	for field in [
		"DEFINE FIELD d ON tb COMPUTED <int> code",
		"DEFINE FIELD d ON tb COMPUTED rand()",
		"DEFINE FIELD d ON tb COMPUTED code TYPE int",
		"DEFINE FIELD d ON tb COMPUTED code TYPE string",
		"DEFINE FIELD d ON tb COMPUTED 1 + 1",
		"DEFINE FIELD d ON tb COMPUTED 'x' TYPE any",
	] {
		let setup = format!("{} {field};", tables("WHERE d != NONE"));
		let held = planned(&setup, "UPDATE tb SET code = 1 RETURN NONE").await;
		assert!(held.has_select_gate(), "{field}: {:?}", held.run);
		assert!(held.body_fits, "{field}");
	}
}

/// A gate keeps its op when it derives a field that runs under an `AUTH LIMIT` narrowing the
/// caller, which a database editor's definition stamps on the field.
#[tokio::test]
async fn a_select_derivation_under_a_narrowing_auth_limit_keeps_the_gate() {
	let db = TestDb::new(&tables("WHERE d != NONE")).await;
	let editor = Session::editor().with_ns("test").with_db("test");
	db.run_as(&editor, "DEFINE FIELD d ON tb COMPUTED 'x'").await;
	let held = planned_on(&db, "UPDATE tb SET code = 1 RETURN NONE").await;
	assert!(held.has_select_gate(), "{:?}", held.run);
	assert!(held.body_fits);
}

/// A select clause the write cannot state keeps its gate: each lowers to a refusal the compile must
/// still reach. `FULL`, `NONE` and absent host no region, and a predicate
/// hosts the one it states.
#[test]
fn only_a_stated_clause_is_judged() {
	let cases = [
		(GateClause::Full, Judged::Unconditional),
		(GateClause::None, Judged::Unconditional),
		(GateClause::Absent, Judged::Unconditional),
		(
			GateClause::Predicate {
				derives: false,
			},
			Judged::Predicate {
				derives: false,
			},
		),
		(
			GateClause::Predicate {
				derives: true,
			},
			Judged::Predicate {
				derives: true,
			},
		),
		(GateClause::Dynamic, Judged::Kept),
		(GateClause::LateBound, Judged::Kept),
		(GateClause::Unlowerable, Judged::Kept),
	];
	for (clause, expected) in cases {
		assert_eq!(judged(clause), expected, "{clause:?}");
	}
}

/// How a script answers, statement by statement, and the rows it leaves in `tables`.
async fn outcome(db: &TestDb, session: &Session, sql: &str, tables: &[&str]) -> Vec<String> {
	let mut answers = Vec::new();
	let responses = db.datastore().execute(sql, session, None).await.expect("the script executes");
	for response in responses {
		answers.push(match response.result {
			Ok(value) => format!("ok {value:?}"),
			Err(error) => format!("err {error}"),
		});
	}
	for table in tables {
		let state = db
			.datastore()
			.execute(&format!("SELECT * FROM {table} ORDER BY id"), &TestDb::owner(), None)
			.await
			.expect("the state reads");
		for response in state {
			answers
				.push(format!("{table} {:?}", response.result.map_err(|error| error.to_string())));
		}
	}
	answers
}

fn record_session() -> Session {
	Session::for_record(
		"test",
		"test",
		"user",
		surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "bench")),
	)
}

const ACCESS: &str = "DEFINE TABLE user SCHEMALESS; CREATE user:bench SET name = 'bench';
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM type::record('user', $id))
	DURATION FOR TOKEN 1h, FOR SESSION 1h;";

/// What a script answered and left with the pass and without it, and how many stage ops the pass
/// removed in the run that had it.
struct Ways {
	pruned: Vec<String>,
	general: Vec<String>,
	removed: usize,
}

/// Runs `script` as the record user on a database `setup` builds, with the pass and without it.
///
/// Each run plans on the thread that awaits it. The pass is entered in both runs, so a run that
/// bypassed planning (a cached program, another thread) fails here instead of comparing a run with
/// itself, and the run without the pass removes nothing.
async fn both_ways(setup: &str, script: &str, tables: &[&str]) -> Ways {
	take_tally();
	let pruned = {
		let db = TestDb::new(&format!("{ACCESS} {setup}")).await;
		outcome(&db, &record_session(), script, tables).await
	};
	let (entered, removed) = take_tally();
	assert!(entered > 0, "the pass was never entered");
	let general = {
		let _off = Disabled::new();
		let db = TestDb::new(&format!("{ACCESS} {setup}")).await;
		outcome(&db, &record_session(), script, tables).await
	};
	let (entered, none) = take_tally();
	assert!(entered > 0, "the run without the pass never reached it");
	assert_eq!(none, 0, "the pass removed a stage while it was disabled");
	Ways {
		pruned,
		general,
		removed,
	}
}

/// Every form under `RETURN NONE`, as a record user, answers and leaves what the unpruned plan
/// does, whether the select predicate admits the record, refuses it or raises on it. Where the
/// gate is inert it is removed, and under any other output it is not.
#[tokio::test]
async fn pruned_writes_answer_and_leave_what_the_general_plan_does() {
	let data = "CREATE tb:1 SET age = 10, code = 1; CREATE tb:2 SET age = 300, code = 2;
		CREATE tb:3 SET age = 20, code = 3;";
	for (select, inert) in [
		("FULL", true),
		("NONE", true),
		("WHERE 1 < 2", true),
		("WHERE $auth != NONE", true),
		("WHERE age < 200", false),
		("WHERE age < 50", false),
		("WHERE <int> code > 0", false),
		("WHERE <int> code > 2", false),
	] {
		let setup = format!("{} {data}", tables(select));
		for output in ["NONE", "NULL", "AFTER", "BEFORE", "DIFF", "id", "$before.age, $after.age"] {
			let script = forms(output).join(";\n");
			let ways = both_ways(&setup, &script, &["tb", "r"]).await;
			assert_eq!(ways.pruned, ways.general, "{select} RETURN {output}");
			assert_eq!(ways.removed > 0, inert && output == "NONE", "{select} RETURN {output}");
		}
	}
}

/// A raising predicate keeps its error, at the record main raised it, with the writes undone.
#[tokio::test]
async fn a_raising_predicate_fails_the_statement_as_it_did() {
	let setup = format!(
		"{} CREATE tb:1 SET code = 1; CREATE tb:2 SET code = 'x'; CREATE tb:3 SET code = 3;",
		tables("WHERE <int> code > 0")
	);
	let script = "UPDATE tb SET touched = true RETURN NONE; SELECT * FROM tb ORDER BY id;
		BEGIN; UPDATE tb SET touched = true RETURN NONE; COMMIT; DELETE tb RETURN NONE;";
	let ways = both_ways(&setup, script, &["tb"]).await;
	assert_eq!(ways.pruned, ways.general);
	assert!(ways.pruned.iter().any(|answer| answer.starts_with("err")), "{:?}", ways.pruned);
	assert_eq!(ways.removed, 0);
}

/// A write that names its table only by data runs its stages from per-table bundles, which keep
/// every stage, whether the predicate is inert or raises.
#[tokio::test]
async fn a_write_through_bundles_answers_as_it_did() {
	let data = "CREATE tb:1 SET age = 10, code = 1; CREATE tb:2 SET age = 300, code = 'x';";
	for select in ["WHERE age < 200", "WHERE <int> code > 0", "NONE"] {
		let setup = format!("{} {data}", tables(select));
		let script = "UPDATE (SELECT VALUE id FROM tb) SET age = 9 RETURN NONE;
			DELETE (SELECT VALUE id FROM tb) RETURN NONE;";
		let ways = both_ways(&setup, script, &["tb"]).await;
		assert_eq!(ways.pruned, ways.general, "{select}");
		assert_eq!(ways.removed, 0, "{select}");
	}
}

/// A body fits the stage ops it names after the pass, so the verifier accepts it.
#[tokio::test]
async fn the_pruned_body_names_the_stages_that_are_left() {
	let setup = tables("WHERE age < 200");
	for sql in forms("NONE") {
		let held = planned(&setup, &sql).await;
		assert!(held.body_fits, "{sql}");
	}
}

/// The ops the expression lowering emits from their operands alone are accepted, and every other
/// kind keeps the gate.
#[test]
fn only_ops_that_lower_without_refusal_are_accepted() {
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::expr::ops::{
		CallAttrs, ConvertAttrs, ConvertMode, ConvertSite, ExprOp, OnFail, ShortCircuitOp,
	};
	use crate::expr::{BinaryOperator, Function, Kind, PrefixOperator};
	use crate::val::Value;

	let registry = FunctionRegistry::with_builtins();
	let call = |function| {
		ExprOp::Call(Box::new(CallAttrs {
			function,
			row_names: 0,
			callee: None,
		}))
	};
	let convert = |mode, on_fail| {
		ExprOp::Convert(Box::new(ConvertAttrs {
			mode,
			kind: Kind::Int,
			site: ConvertSite(0),
			on_fail,
		}))
	};
	let cases = [
		(ExprOp::Literal(Box::new(Value::None)), true),
		(
			ExprOp::FreeParam {
				name: "p".into(),
			},
			true,
		),
		(
			ExprOp::SessionParam {
				name: "auth".into(),
			},
			true,
		),
		(
			ExprOp::CatalogParam {
				name: "p".into(),
			},
			true,
		),
		(
			ExprOp::Field {
				name: "f".into(),
			},
			true,
		),
		(ExprOp::Doc, true),
		(
			ExprOp::Access {
				field: "f".into(),
			},
			true,
		),
		(
			ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			},
			true,
		),
		(ExprOp::Truthy, true),
		(ExprOp::Yield, true),
		(
			ExprOp::Binary {
				op: BinaryOperator::LessThan,
			},
			true,
		),
		(
			ExprOp::Binary {
				op: BinaryOperator::Add,
			},
			false,
		),
		(ExprOp::Prefix(Box::new(PrefixOperator::Not)), true),
		(ExprOp::Prefix(Box::new(PrefixOperator::Range)), false),
		(convert(ConvertMode::Cast, OnFail::Error), true),
		(convert(ConvertMode::Cast, OnFail::None), false),
		(convert(ConvertMode::Coerce, OnFail::Error), false),
		(call(Function::Normal("array::len".into())), true),
		(call(Function::Normal("search::score".into())), false),
		(call(Function::Custom("f".into())), false),
		(ExprOp::Array, false),
		(ExprOp::Index, false),
		(ExprOp::If, false),
		(ExprOp::Invoke, false),
		(ExprOp::Throw, false),
	];
	for (op, accepted) in cases {
		assert_eq!(super::write_prune::expr_lowers(&registry, &op), accepted, "{op:?}");
	}
}

/// The last stage of the first order `sql` binds.
async fn last_stage(setup: &str, sql: &str) -> WriteStageKind {
	let held = planned(setup, sql).await;
	assert!(held.body_fits, "{sql}");
	*held.orders[0].last().expect("an order holds a stage")
}

/// A statement `UPDATE` or `DELETE` whose condition is the literal `false`, `NONE` or `NULL` binds
/// the stages up to and including its `WHERE` stage, and no other condition does: a computed
/// condition is not decided at bind time, whatever it folds to.
#[tokio::test]
async fn a_falsy_literal_where_cuts_the_stages_after_the_where_stage() {
	let setup = "DEFINE TABLE tb SCHEMALESS;";
	for statement in
		["UPDATE tb:1, tb:2 SET x = 1", "UPDATE tb SET x = 1", "DELETE tb:1, tb:2", "DELETE tb"]
	{
		for cond in ["false", "NONE", "NULL"] {
			let held = planned(setup, &format!("{statement} WHERE {cond}")).await;
			assert!(held.body_fits, "{statement} WHERE {cond}");
			assert_eq!(held.orders.len(), 1);
			assert_eq!(held.orders[0].last(), Some(&WriteStageKind::CheckWhereCondition));
			assert_eq!(held.run, held.orders[0], "{statement} WHERE {cond}");
			assert!(held.where_never, "{statement} WHERE {cond}");
		}
		for cond in [
			"true",
			"0",
			"''",
			"1 > 2",
			"!true",
			"true AND false",
			"false AND true",
			"rand() < 0",
			"x",
		] {
			let held = planned(setup, &format!("{statement} WHERE {cond}")).await;
			assert_eq!(
				held.run.last(),
				Some(&WriteStageKind::OutputWrite),
				"{statement} WHERE {cond}"
			);
			assert!(held.body_fits, "{statement} WHERE {cond}");
			assert!(!held.where_never, "{statement} WHERE {cond}");
		}
	}
}

/// No other form is cut: a record a condition does not admit is still created by an `UPSERT`.
#[tokio::test]
async fn a_falsy_literal_where_cuts_no_other_form() {
	let setup = "DEFINE TABLE tb SCHEMALESS;";
	for sql in [
		"UPSERT tb:1 SET x = 1 WHERE false",
		"UPSERT tb SET x = 1 WHERE false",
		"CREATE tb:1 SET x = 1",
		"INSERT INTO tb { id: tb:1 }",
		"RELATE tb:1->r:1->tb:2 SET x = 1",
	] {
		let held = planned(setup, sql).await;
		assert_eq!(held.run.last(), Some(&WriteStageKind::OutputWrite), "{sql}");
		assert!(held.body_fits, "{sql}");
		assert!(!held.where_never, "{sql}");
	}
}

/// A write under a `WHERE` no record satisfies states it whatever its table stores, and is cut
/// only when no stage after the `WHERE` stage requests a unit or can refuse to lower: a table that
/// stores an event, a view, a `REFERENCE` field or a field a statement or a function call computes
/// keeps every stage, all of them bound.
#[tokio::test]
async fn a_cut_write_names_one_table_whose_later_stages_request_no_unit() {
	let sql = "UPDATE tb:1 SET x = 1 WHERE false";
	let cut = |held: &Planned| held.run.last() == Some(&WriteStageKind::CheckWhereCondition);
	for setup in [
		"DEFINE TABLE tb SCHEMALESS;",
		"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD x ON tb TYPE int; DEFINE FIELD y ON tb TYPE int VALUE x + 1;",
		"DEFINE TABLE tb SCHEMALESS; DEFINE INDEX ix ON tb FIELDS x; DEFINE INDEX iu ON tb FIELDS x UNIQUE;",
		"DEFINE TABLE tb SCHEMALESS PERMISSIONS FOR select WHERE x > 1 FOR update FULL;",
		"DEFINE TABLE tb SCHEMALESS; DEFINE FIELD c ON tb COMPUTED x + 1;",
	] {
		let held = planned(setup, sql).await;
		assert!(cut(&held), "{setup}");
		assert!(held.where_never, "{setup}");
	}
	for setup in [
		"DEFINE TABLE tb SCHEMALESS; DEFINE EVENT e ON tb WHEN true THEN (CREATE audit);",
		"DEFINE TABLE tb SCHEMALESS; DEFINE TABLE v AS SELECT count() FROM tb GROUP ALL;",
		"DEFINE TABLE tb SCHEMALESS; DEFINE FIELD r ON tb TYPE option<record<tb>> REFERENCE;",
		"DEFINE FUNCTION fn::f() { RETURN 1 }; DEFINE TABLE tb SCHEMALESS; DEFINE FIELD y ON tb VALUE fn::f();",
		"DEFINE TABLE tb SCHEMALESS; DEFINE FIELD y ON tb VALUE (SELECT VALUE a FROM ONLY t:1);",
		"DEFINE TABLE tb SCHEMALESS PERMISSIONS FOR update WHERE (SELECT VALUE a FROM ONLY t:1) FOR create FULL;",
	] {
		let held = planned(setup, sql).await;
		assert!(!cut(&held), "{setup}: {:?}", held.run);
		assert!(held.body_fits, "{setup}");
		assert!(held.where_never, "{setup}");
	}
	// A delete states the `REFERENCE` fields of any table that name its table.
	let setup = "DEFINE TABLE tb SCHEMALESS; DEFINE TABLE o SCHEMALESS; DEFINE FIELD r ON o TYPE option<record<tb>> REFERENCE;";
	let held = planned(setup, "DELETE tb:1 WHERE false").await;
	assert_eq!(held.run.last(), Some(&WriteStageKind::OutputWrite));
	assert!(held.where_never);
	// A write whose targets a read yields names no one table.
	let held = planned(
		"DEFINE TABLE tb SCHEMALESS;",
		"UPDATE (SELECT VALUE id FROM tb) SET x = 1 WHERE false",
	)
	.await;
	assert_eq!(held.run.last(), Some(&WriteStageKind::OutputWrite));
	assert!(held.where_never);
}

/// Which events of the events unit of `tb` for `action` the pass marks dormant, planned over a
/// database `setup` builds.
async fn dormant_events(setup: &str, action: Action) -> Vec<bool> {
	let db = TestDb::new(setup).await;
	let input = RequestInput {
		statements: &[],
		scope: IrScope::Request {
			fragment: None,
		},
		unit: UnitKind::Events {
			table: TableName::new("tb"),
			action,
		},
		serves_late: false,
		late_bound: false,
	};
	let planned = plan_on(&db, &input).await.expect("the events plan");
	let ir = &planned.ir;
	let mut found = None;
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(data) = ir.op(op)
			&& let OpKind::Phys(PhysOp::Events(attrs)) = &data.kind
		{
			found = Some(attrs.events.iter().map(|event| event.dormant).collect());
		}
		Walk::Continue
	});
	found.expect("the unit holds an events op")
}

fn events_setup(whens: &[&str]) -> String {
	let mut setup = "DEFINE TABLE tb SCHEMALESS; DEFINE TABLE audit SCHEMALESS;".to_owned();
	for (at, when) in whens.iter().enumerate() {
		setup.push_str(&format!("DEFINE EVENT e{at} ON tb WHEN ({when}) THEN (CREATE audit);"));
	}
	setup
}

/// An event whose `WHEN` is never truthy for the unit's action is marked dormant, and no other is.
#[tokio::test]
async fn an_event_that_never_fires_for_the_action_is_dormant() {
	let setup = events_setup(&[
		"$event = 'CREATE'",
		"$event = 'DELETE'",
		"$event IN ['CREATE', 'DELETE']",
		"false",
		"true",
		"$after.flag",
		"0",
		"$event != $event",
		"fn::f()",
	]);
	assert_eq!(
		dormant_events(&setup, Action::Update).await,
		[true, true, true, true, false, false, false, true, false]
	);
	assert_eq!(
		dormant_events(&setup, Action::Create).await,
		[false, true, false, true, false, false, false, true, false]
	);
	assert_eq!(
		dormant_events(&setup, Action::Delete).await,
		[true, false, false, true, false, false, false, true, false]
	);
}

/// An op that states no clause for its events marks none dormant, however its definitions read.
#[test]
fn an_op_that_states_no_clause_marks_no_event() {
	use std::sync::Arc;

	use crate::catalog::auth::AuthLimit;
	use crate::catalog::{EventDefinition, EventKind};
	use crate::exec::ir::logical::stages::events_over;

	let definition = |when: &str| {
		EventDefinition::new(
			"e".into(),
			"tb".into(),
			crate::syn::expr(when).expect("a clause"),
			vec![crate::syn::expr("1").expect("a clause")],
			EventKind::Sync,
			AuthLimit::new_no_limit(),
			None,
		)
	};
	let definitions: Arc<[EventDefinition]> =
		Arc::from(vec![definition("false"), definition("true")]);
	let mut attrs = events_over(&definitions, Action::Update).attrs;
	assert_eq!(dormancy(&attrs), Some(vec![true, false]));
	attrs.unlowerable = true;
	assert_eq!(dormancy(&attrs), None);
	attrs.unlowerable = false;
	attrs.events = Box::default();
	assert_eq!(dormancy(&attrs), None);
}
