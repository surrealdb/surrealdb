//! What a run pays per unit it invokes: a run resolves its aggregates against
//! the registry at most once, however many batches and invocations share them;
//! a closure body reads what its lowering read of the catalog once while the
//! transaction is unchanged; a record read through its table's unit runs in
//! place, its layers landed there, when the unit answers without another host
//! step, and is otherwise continued by the host where the run in place stopped;
//! and a dynamic read of a table the run defined hands its rows on as storage
//! yields them, reading no row past a limit it spends.

use super::closure::{BODIES_RUN, READS_READ};
use super::dispatch::{CONTINUED_READS, HOSTED_READS};
use super::golden::Golden;
use super::interp::{RESOLUTIONS, State};
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// `sql` run on the VM, with how many times `counter` counted on the run.
async fn counted(
	ctx: &ExecutionContext,
	sql: &str,
	counter: &'static std::thread::LocalKey<std::cell::Cell<usize>>,
) -> (Result<Vec<Value>, String>, usize) {
	let bound = bind(ctx, sql).await;
	let op = super::plan(&planner(ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let before = counter.with(std::cell::Cell::get);
	let vm = outcome(&op, ctx).await;
	(vm, counter.with(std::cell::Cell::get) - before)
}

#[tokio::test]
async fn a_run_resolves_its_aggregates_once() {
	let mut golden = Golden::new("a_run_resolves_its_aggregates_once");
	let db = TestDb::new(
		"
		DEFINE TABLE t SCHEMALESS;
		DEFINE FUNCTION fn::c() { RETURN (SELECT VALUE count() FROM t GROUP ALL) };
		FOR $i IN 0..300 { CREATE type::record('t', $i) SET n = $i, g = $i % 7 };
		",
	)
	.await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"SELECT count() AS c, math::sum(n) AS s FROM t GROUP ALL",
		"SELECT g, count() AS c, math::max(n) AS m FROM t GROUP BY g ORDER BY g",
		// A function body holding an aggregate, run once per row.
		"SELECT VALUE fn::c() FROM t LIMIT 20",
	] {
		let (vm, resolved) = counted(&ctx, sql, &RESOLUTIONS).await;
		golden.rows(sql, &vm);
		// A spare state holding the same functions is not resolved again.
		assert!(resolved <= 1, "`{sql}` resolved its aggregates {resolved} times");
	}
	golden.check();
}

/// The program `sql` lowers to.
async fn lowered(ctx: &ExecutionContext, sql: &str) -> super::program::Program {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	super::lower::lower(&planner(ctx), &physical).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

#[tokio::test]
async fn a_spare_state_resolves_again_against_a_registry_where_a_dropped_one_was() {
	use std::sync::Arc;

	use crate::exec::function::FunctionRegistry;

	let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
	let ctx = db.exec_ctx().await;
	let program = lowered(&ctx, "SELECT g, count() AS c FROM t GROUP BY g").await;
	let method = super::program::MethodSpec {
		name: "len".to_string(),
		kinds: 0,
		has_fallback: false,
		plan_depth: 0,
		closure_args: Vec::new(),
		writing: false,
	};
	let count = |state: &State| {
		let mut calls = state.aggregates.functions.iter().flatten().flatten();
		let call = Arc::clone(calls.next().expect("the program calls an aggregate"));
		assert!(calls.next().is_none(), "the program calls one aggregate");
		call
	};
	let mut registry = Box::new(FunctionRegistry::with_builtins());
	let address = std::ptr::from_ref::<FunctionRegistry>(&registry);
	let (first, first_method) = {
		let mut state = State::new(&program, Vec::new(), &registry).expect("the state starts");
		let held = state.method_descriptor(0, &method, &registry).expect("`len` is a method");
		(count(&state), held)
	};
	// The registry is dropped and another takes its place at the same address,
	// as a datastore reopened in the same process may: the spare state given
	// back holds functions and descriptors of the dropped one.
	*registry = FunctionRegistry::with_builtins();
	assert_eq!(std::ptr::from_ref::<FunctionRegistry>(&registry), address);
	let before = RESOLUTIONS.with(std::cell::Cell::get);
	{
		let mut state = State::new(&program, Vec::new(), &registry).expect("the state starts");
		assert_eq!(RESOLUTIONS.with(std::cell::Cell::get) - before, 1);
		let answered = count(&state);
		assert!(!Arc::ptr_eq(&answered, &first), "a function of the dropped registry was kept");
		assert!(Arc::ptr_eq(&answered, registry.get_aggregate("count").expect("`count` exists")));
		let held = state.method_descriptor(0, &method, &registry).expect("`len` is a method");
		assert!(!Arc::ptr_eq(&held, &first_method), "a method of the dropped registry was kept");
		assert!(Arc::ptr_eq(&held, registry.get_method("len").expect("`len` is a method")));
	}
	// Against the registry it resolved them from, a spare state keeps them.
	let before = RESOLUTIONS.with(std::cell::Cell::get);
	let state = State::new(&program, Vec::new(), &registry).expect("the state starts");
	assert_eq!(RESOLUTIONS.with(std::cell::Cell::get) - before, 0);
	assert!(Arc::ptr_eq(&count(&state), registry.get_aggregate("count").expect("`count` exists")));
}

#[tokio::test]
async fn a_closure_reads_its_catalog_reads_once_while_the_transaction_is_unchanged() {
	let mut golden =
		Golden::new("a_closure_reads_its_catalog_reads_once_while_the_transaction_is_unchanged");
	let db = TestDb::new(
		"
		DEFINE FUNCTION fn::v() { RETURN 1; } PERMISSIONS FULL;
		DEFINE TABLE t SCHEMALESS;
		FOR $i IN 0..5 { CREATE type::record('t', $i) SET tags = [1, 2, 3] };
		",
	)
	.await;
	let ctx = db.exec_ctx().await;
	for sql in [
		"array::fold([1, 2, 3, 4], 0, |$acc, $v| $acc + fn::v() + $v)",
		"SELECT VALUE array::fold(tags, 0, |$acc, $v| $acc + fn::v() + $v) FROM t",
	] {
		let ran = BODIES_RUN.with(std::cell::Cell::get);
		let (vm, read) = counted(&ctx, sql, &READS_READ).await;
		let ran = BODIES_RUN.with(std::cell::Cell::get) - ran;
		golden.rows(sql, &vm);
		assert!(ran > 1, "`{sql}` ran {ran} bodies as programs");
		assert_eq!(read, 1, "`{sql}` read its catalog reads {read} times over {ran} bodies");
	}
	golden.check();
}

#[tokio::test]
async fn a_record_its_unit_answers_without_the_host_is_read_in_place() {
	let mut golden = Golden::new("a_record_its_unit_answers_without_the_host_is_read_in_place");
	let db = TestDb::new(
		"
		DEFINE TABLE person SCHEMALESS;
		DEFINE TABLE pet SCHEMALESS;
		DEFINE FIELD label ON pet COMPUTED 'pet ' + name;
		DEFINE TABLE post SCHEMALESS;
		FOR $i IN 0..4 { CREATE type::record('person', $i) SET name = <string> $i };
		FOR $i IN 0..4 { CREATE type::record('pet', $i) SET name = <string> $i };
		FOR $i IN 0..20 {
			CREATE type::record('post', $i) SET n = $i,
				author = type::record('person', $i % 4), pet = type::record('pet', $i % 4)
		};
		",
	)
	.await;
	let ctx = db.exec_ctx().await;
	// A table with nothing to compute or judge answers its record as it is
	// stored; one with a computed field lands its layer in place.
	for sql in [
		"SELECT n, author FROM post ORDER BY n FETCH author",
		"SELECT n, author.name AS name FROM post ORDER BY n",
		"SELECT n, pet FROM post ORDER BY n FETCH pet",
		"SELECT n, pet.label AS label FROM post ORDER BY n",
	] {
		let (vm, ran) = counted(&ctx, sql, &HOSTED_READS).await;
		golden.rows(sql, &vm);
		assert_eq!(ran, 0, "`{sql}` read {ran} records through the host");
	}
	golden.check();
}

#[tokio::test]
async fn a_record_its_unit_needs_the_host_for_is_continued_where_the_run_in_place_stopped() {
	let db = TestDb::new(
		"
		DEFINE FUNCTION fn::tag($s: string) { RETURN 'tag ' + $s };
		DEFINE TABLE toy SCHEMALESS;
		DEFINE FIELD label ON toy COMPUTED fn::tag(name);
		DEFINE TABLE post SCHEMALESS;
		FOR $i IN 0..4 { CREATE type::record('toy', $i) SET name = <string> $i };
		FOR $i IN 0..20 { CREATE type::record('post', $i) SET n = $i, toy = type::record('toy', $i % 4) };
		",
	)
	.await;
	let ctx = db.exec_ctx().await;
	// The computed field calls a function, a host step: the first record's
	// run in place stops there and the host continues it on that run's state;
	// every later record of the run goes to the host from the start.
	let sql = "SELECT n, toy FROM post ORDER BY n FETCH toy";
	let continued = CONTINUED_READS.with(std::cell::Cell::get);
	let (vm, hosted) = counted(&ctx, sql, &HOSTED_READS).await;
	let continued = CONTINUED_READS.with(std::cell::Cell::get) - continued;
	let mut golden = Golden::new(
		"a_record_its_unit_needs_the_host_for_is_continued_where_the_run_in_place_stopped",
	);
	golden.rows(sql, &vm);
	golden.check();
	assert_eq!(hosted, 20, "`{sql}` read {hosted} records through the host");
	assert_eq!(continued, 1, "`{sql}` continued {continued} records on their in-place state");
}

#[tokio::test]
async fn a_body_run_in_place_that_stops_past_a_layer_is_continued_from_that_layer() {
	use std::borrow::Cow;

	use super::host::{
		host_frames, run_cow_row_from, run_unit_inline, statement_state, unit_state,
	};

	let db = TestDb::new(
		"
		DEFINE FUNCTION fn::inc($x: int) { RETURN $x + 1 };
		DEFINE TABLE pet SCHEMALESS;
		DEFINE FIELD label ON pet COMPUTED 'pet ' + name;
		DEFINE FIELD hidden ON pet PERMISSIONS FOR select NONE;
		CREATE pet:1 SET name = '1', n = 1, hidden = 'h';
		",
	)
	.await;
	let ctx = db.exec_ctx().await;
	let rid = crate::val::RecordId::new(crate::val::TableName::from("pet"), 1);
	let raw = crate::exec::operators::fetch::fetch_raw_record(&ctx, &rid, None)
		.await
		.expect("reads")
		.expect("the record exists");
	// The layer derives `label` and cuts `hidden`; the call after it is a
	// host step.
	let program =
		super::tests_sources::program_of(&ctx, "SELECT *, fn::inc(n) AS m FROM pet").await;
	let pipeline = &program.pipelines[0];
	let whole = {
		let mut state = statement_state(&program, &ctx).expect("a state");
		run_cow_row_from(&program, pipeline, Cow::Borrowed(&raw), &mut state, &ctx, 0)
			.await
			.expect("runs");
		std::mem::take(&mut state.output)
	};
	assert_eq!(whole.len(), 1, "{whole:?}");
	let mut state = unit_state(&program, &ctx).expect("a state");
	let Err(unfinished) = run_unit_inline(&program, pipeline, Cow::Borrowed(&raw), &mut state)
	else {
		panic!("a body that calls a function after its layer finished in place");
	};
	let start = unfinished.resume.expect("a body that pages nothing is continued");
	assert!(start > 0, "the body stopped before its layer");
	assert!(state.output.is_empty() && state.tries.is_empty() && state.pending.is_none());
	host_frames(&program, &mut state, &ctx);
	run_cow_row_from(&program, pipeline, unfinished.row, &mut state, &ctx, start)
		.await
		.expect("runs");
	assert_eq!(state.output, whole);
}

#[tokio::test]
async fn a_dynamic_read_of_a_table_the_run_defined_streams_and_stops_at_its_limit() {
	let mut golden =
		Golden::new("a_dynamic_read_of_a_table_the_run_defined_streams_and_stops_at_its_limit");
	let db = TestDb::new("DEFINE TABLE other SCHEMALESS;").await;
	let ctx = db.exec_ctx().await;
	// The last fails on a row past its limit, which the read stops before.
	let reads = [
		"SELECT * FROM type::table('fresh') LIMIT 3",
		"SELECT n FROM type::table('fresh') START 2 LIMIT 3",
		"SELECT * FROM type::table('fresh') WHERE <int> s > 0 LIMIT 3",
	];
	// Lowered before the table exists, so the program holds no unit for it.
	let mut lowered = Vec::new();
	for sql in reads {
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		lowered.push((sql, op));
	}
	db.datastore()
		.execute(
			"CREATE |fresh:1..=40| SET n = 1; UPDATE fresh SET n = <int> record::id(id), s = '1'; \
			 UPDATE fresh:30 SET s = 'x' RETURN NONE;",
			&TestDb::owner(),
			None,
		)
		.await
		.expect("creates");
	let ctx = db.exec_ctx().await;
	for (sql, op) in lowered {
		golden.rows(sql, &outcome(&op, &ctx).await);
	}
	golden.check();
}
