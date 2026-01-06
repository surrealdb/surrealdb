#![allow(clippy::unwrap_used)]

mod common;

use common::{block_on, setup_datastore, setup_datastore_with_query};
use criterion::{Criterion, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};

// ============================================================================
// Benchmark: CREATE Statement
// ============================================================================

fn bench_create(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("create");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(
		group,
		create_simple,
		&dbs,
		&ses,
		"CREATE person SET name = 'Test', age = 30, email = 'test@example.com', scores = [90, 80, 70];"
	);

	bench!(
		group,
		create_merge,
		&dbs,
		&ses,
		"CREATE person MERGE {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(
		group,
		create_content,
		&dbs,
		&ses,
		"CREATE person CONTENT {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(
		group,
		create_replace,
		&dbs,
		&ses,
		"CREATE person REPLACE {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(
		group,
		create_complex,
		&dbs,
		&ses,
		"CREATE person SET name = 'Test', age = 30, email = 'test@example.com', scores = [90, 80, 70], address = {{ street: '123 Main St', city: 'NYC', zip: '10001' }}, hobbies = ['reading', 'coding', 'gaming'];"
	);

	bench!(
		group,
		create_large_string,
		&dbs,
		&ses,
		"CREATE person SET name = 'Test', age = 30, email = 'test@example.com', scores = [90, 80, 70], description = '{}';",
		"Lorem ipsum dolor sit amet, consectetur adipiscing elit.".repeat(10_000)
	);

	group.finish();
}

// ============================================================================
// Benchmark: UPSERT Statement
// ============================================================================

fn bench_upsert(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("upsert");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(
		group,
		upsert_simple,
		&dbs,
		&ses,
		"UPSERT person:test SET name = 'Test', age = 30, email = 'test@example.com', scores = [90, 80, 70];"
	);

	bench!(
		group,
		upsert_merge,
		&dbs,
		&ses,
		"UPSERT person:test MERGE {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(
		group,
		upsert_content,
		&dbs,
		&ses,
		"UPSERT person:test CONTENT {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(
		group,
		upsert_replace,
		&dbs,
		&ses,
		"UPSERT person:test REPLACE {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(group, upsert_computation, &dbs, &ses, "UPSERT person:test SET age = age + 1");

	group.finish();
}

// ============================================================================
// Benchmark: UPDATE Statement
// ============================================================================

fn bench_update(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("update");
	// Setup the datastore with a query
	let (dbs, ses) = block_on(setup_datastore_with_query(
		"CREATE person:test SET name = 'Tobie', age = 30, email = 'test@example.com';",
	));

	bench!(
		group,
		update_simple,
		&dbs,
		&ses,
		"UPDATE person:test SET name = 'Test', age = 30, email = 'test@example.com', scores = [90, 80, 70];"
	);

	bench!(
		group,
		update_merge,
		&dbs,
		&ses,
		"UPDATE person:test MERGE {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(
		group,
		update_content,
		&dbs,
		&ses,
		"UPDATE person:test CONTENT {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(
		group,
		update_replace,
		&dbs,
		&ses,
		"UPDATE person:test REPLACE {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(group, update_computation, &dbs, &ses, "UPDATE person:test SET age = age + 1");

	group.finish();
}

// ============================================================================
// Benchmark: INSERT Statement
// ============================================================================

fn bench_insert(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("insert");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(
		group,
		insert_values,
		&dbs,
		&ses,
		"INSERT INTO person (name, age, email, scores) VALUES ('Test',30, 'test@example.com', [90, 80, 70]);"
	);

	bench!(
		group,
		insert_object,
		&dbs,
		&ses,
		"INSERT INTO person {{ name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	bench!(
		group,
		insert_with_id,
		&dbs,
		&ses,
		"INSERT INTO person {{ id: person:test, name: 'Test', age: 30, email: 'test@example.com', scores: [90, 80, 70] }};"
	);

	group.finish();
}

// ============================================================================
// Benchmark: RELATE Statement
// ============================================================================

fn bench_relate(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("relate");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(group, relate_empty, &dbs, &ses, "RELATE person:test->knows->person:other;");

	bench!(
		group,
		relate_simple,
		&dbs,
		&ses,
		"RELATE person:test->knows->person:other SET date = time::now(), weight = 0.8;"
	);

	bench!(
		group,
		relate_merge,
		&dbs,
		&ses,
		"RELATE person:test->knows->person:other MERGE {{ date: time::now(), weight: 0.8 }};"
	);

	bench!(
		group,
		relate_content,
		&dbs,
		&ses,
		"RELATE person:test->knows->person:other CONTENT {{ date: time::now(), weight: 0.8 }};"
	);

	bench!(
		group,
		relate_replace,
		&dbs,
		&ses,
		"RELATE person:test->knows->person:other REPLACE {{ date: time::now(), weight: 0.8 }};"
	);

	group.finish();
}

criterion_group!(
	name = benches;
	config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
	targets = bench_create,
		bench_upsert,
		bench_update,
		bench_insert,
		bench_relate,
);
criterion_main!(benches);
