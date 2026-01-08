#![allow(clippy::unwrap_used)]

mod common;

use common::{block_on, setup_datastore, setup_datastore_with_query, setup_datastore_with_records};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};
use surrealdb_core::syn::value;

// ============================================================================
// Benchmark: SELECT from objects and arrays
// ============================================================================

fn bench_value_select(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_value");
	// Setup the datastore with no data
	let (dbs, ses) = block_on(setup_datastore());

	bench!(
		group,
		select_from_object,
		&dbs,
		&ses,
		throughput: 1,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT * FROM {{ id: 1, name: 'test', value: 42 }};"
	);

	bench!(
		group,
		select_from_array_small,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT * FROM [{}];",
		(1..=1_000).map(|n| n.to_string()).collect::<Vec<_>>().join(", ")
	);

	bench!(
		group,
		select_from_array_large,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM [{}];",
		(1..=10_000).map(|n| n.to_string()).collect::<Vec<_>>().join(", ")
	);

	bench!(
		group,
		select_from_array_objects_small,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT * FROM [{}];",
		(1..=1_000)
			.map(|n| format!("{{ id: {n}, name: 'item_{n}' }}"))
			.collect::<Vec<_>>()
			.join(", ")
	);

	bench!(
		group,
		select_from_array_objects_large,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM [{}];",
		(1..=10_000)
			.map(|n| format!("{{ id: {n}, name: 'item_{n}' }}"))
			.collect::<Vec<_>>()
			.join(", ")
	);

	group.finish();
}

// ============================================================================
// Benchmark: SELECT by ID
// ============================================================================

fn bench_record_select(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_record");
	// Setup the datastore with a query
	let (dbs, ses) = block_on(setup_datastore_with_query(
		"CREATE item:test SET name = 'Tobie', age = 30, email = 'test@example.com';",
	));

	bench!(
		group,
		select_by_id,
		&dbs,
		&ses,
		throughput: 1,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT * FROM item:test;"
	);

	bench!(
		group,
		select_by_id_where,
		&dbs,
		&ses,
		throughput: 1,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT * FROM item:test WHERE age > 25;"
	);

	bench!(
		group,
		select_by_id_projection,
		&dbs,
		&ses,
		throughput: 1,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT name, age FROM item:test;"
	);

	group.finish();
}

// ============================================================================
// Benchmark: SELECT from various size tables
// ============================================================================

fn bench_table_select(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_all");

	for count in [1, 10, 100, 1_000, 10_000] {
		// Configure throughput for the benchmark
		group.throughput(Throughput::Elements(count));
		// Benchmark the query with the given parameter
		group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &cnt| {
			// Setup the datastore with the given number of records
			let (dbs, ses) = block_on(setup_datastore_with_records(cnt));
			// Create a multithreaded runtime for async benchmarking
			let runtime = common::create_runtime();
			// Run a query to ensure the correct result is returned
			let mut res = execute!(&dbs, &ses, "SELECT * FROM item;");
			// Get the length of the first result
			let len = res.remove(0).result.unwrap().as_array().unwrap().len();
			// Ensure the correct number of records were returned
			assert_eq!(len, cnt as usize, "Expected {cnt} records, got {len}");
			// Benchmark the query with the given parameter
			b.to_async(&runtime).iter(|| async { query!(&dbs, &ses, "SELECT * FROM item;") });
		});
	}

	group.finish();
}

// ============================================================================
// Benchmark: SELECT table with LIMIT
// ============================================================================

fn bench_table_select_limit(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_limit");
	// Setup the datastore with 100,000 records
	let (dbs, ses) = block_on(setup_datastore_with_records(100_000));
	// Create a multithreaded runtime for async benchmarking
	let runtime = common::create_runtime();

	for limit in [10, 100, 1000] {
		// Configure throughput for the benchmark
		group.throughput(Throughput::Elements(limit));
		// Benchmark the query with the given parameter
		group.bench_with_input(BenchmarkId::new("limit", limit), &limit, |b, _| {
			b.to_async(&runtime)
				.iter(|| async { query!(&dbs, &ses, "SELECT * FROM item LIMIT {limit};") });
		});
	}

	group.finish();
}

// ============================================================================
// Benchmark: SELECT table with START
// ============================================================================

fn bench_table_select_start(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_start");
	// Setup the datastore with 100,000 records
	let (dbs, ses) = block_on(setup_datastore_with_records(100_000));
	// Create a multithreaded runtime for async benchmarking
	let runtime = common::create_runtime();

	for start in [100, 5_000, 10_000] {
		// Configure throughput for the benchmark
		group.throughput(Throughput::Elements(100));
		// Benchmark the query with the given parameter
		group.bench_with_input(BenchmarkId::new("start", start), &start, |b, _| {
			b.to_async(&runtime).iter(|| async {
				query!(&dbs, &ses, "SELECT * FROM item START {start} LIMIT 100;")
			})
		});
	}

	group.finish();
}

// ============================================================================
// Benchmark: SELECT table with START and LIMIT
// ============================================================================

fn bench_table_select_start_limit(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_start_limit");
	// Setup the datastore with 100,000 records
	let (dbs, ses) = block_on(setup_datastore_with_records(100_000));
	// Create a multithreaded runtime for async benchmarking
	let runtime = common::create_runtime();

	for start in [100, 5_000, 10_000] {
		for limit in [10, 100, 1000] {
			// Configure throughput for the benchmark
			group.throughput(Throughput::Elements(limit));
			// Benchmark the query with the given parameter
			group.bench_with_input(
				BenchmarkId::new("start+limit", format!("{start}+{limit}")),
				&(start, limit),
				|b, _| {
					b.to_async(&runtime).iter(|| async {
						query!(&dbs, &ses, "SELECT * FROM item START {start} LIMIT {limit};")
					});
				},
			);
		}
	}

	group.finish();
}

// ============================================================================
// Benchmark: SELECT table with WHERE condition
// ============================================================================

fn bench_table_select_where_condition(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_where_condition");
	// Setup the datastore with 10,000 records
	let (dbs, ses) = block_on(setup_datastore_with_records(10_000));
	// Create a multithreaded runtime for async benchmarking
	let runtime = common::create_runtime();

	let conditions = [
		("level = 70", "WHERE level = 70 (returning ~100 items)"),
		("level > 95", "WHERE level > 95 (returning ~500 items)"),
		("level > 90", "WHERE level > 90 (returning ~1000 items)"),
	];

	for (condition, explanation) in conditions {
		// Configure throughput for the benchmark
		group.throughput(Throughput::Elements(10_000));
		// Benchmark the query with the given parameter
		group.bench_with_input(
			BenchmarkId::new("where_condition", explanation),
			&condition,
			|b, _| {
				b.to_async(&runtime)
					.iter(|| async { query!(&dbs, &ses, "SELECT * FROM item WHERE {condition};") });
			},
		);
	}

	group.finish();
}

// ============================================================================
// Benchmark: SELECT from table with VALUE
// ============================================================================

fn bench_table_select_expression(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_expression");
	// Setup the datastore with 10,000 records
	let (dbs, ses) = block_on(setup_datastore_with_records(10_000));

	bench!(
		group,
		select_expression_all,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item;"
	);

	bench!(
		group,
		select_expression_id,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT id FROM item;"
	);

	bench!(
		group,
		select_expression_fields,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT id, name, level FROM item;"
	);

	bench!(
		group,
		select_expression_value_field,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT VALUE level FROM item;"
	);

	bench!(
		group,
		select_expression_value_nested_field,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT VALUE stats.rank FROM item;"
	);

	group.finish();
}

// ============================================================================
// Benchmark: SELECT table with ORDER
// ============================================================================

fn bench_table_select_order(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_order");
	// Setup the datastore with 10,000 records
	let (dbs, ses) = block_on(setup_datastore_with_records(10_000));

	bench!(
		group,
		select_order_none,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item;"
	);

	bench!(
		group,
		select_order_by_id,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item ORDER BY id;"
	);

	bench!(
		group,
		select_order_by_single_ascending,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item ORDER BY level ASC;"
	);

	bench!(
		group,
		select_order_by_multiple,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item ORDER BY level ASC, stats.rank ASC;"
	);

	bench!(
		group,
		select_order_by_single_descending,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item ORDER BY level DESC;"
	);

	bench!(
		group,
		select_order_by_multiple_descending,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item ORDER BY level DESC, stats.rank DESC;"
	);

	bench!(
		group,
		select_order_by_single_ascending_limit_1,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT * FROM item ORDER BY level ASC LIMIT 1;"
	);

	bench!(
		group,
		select_order_by_multiple_limit_1,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT * FROM item ORDER BY level ASC, stats.rank ASC LIMIT 1;"
	);

	bench!(
		group,
		select_order_by_single_descending_limit_1,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT * FROM item ORDER BY level DESC LIMIT 1;"
	);

	bench!(
		group,
		select_order_by_multiple_descending_limit_1,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT * FROM item ORDER BY level DESC, stats.rank DESC LIMIT 1;"
	);

	bench!(
		group,
		select_order_by_rand,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item ORDER BY RAND();"
	);

	bench!(
		group,
		select_order_by_rand_limit_1,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT * FROM item ORDER BY RAND() LIMIT 1;"
	);

	group.finish();
}

// ============================================================================
// Benchmark: SELECT table with GROUP
// ============================================================================

fn bench_table_select_group(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_group");
	// Setup the datastore with 10,000 records
	let (dbs, ses) = block_on(setup_datastore_with_records(10_000));

	bench!(
		group,
		select_group_by_single_field_count,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 100,
		"SELECT count(), level FROM item GROUP BY level;"
	);

	bench!(
		group,
		select_group_by_single_field_sum,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 100,
		"SELECT math::sum(stats.score), level FROM item GROUP BY level;"
	);

	bench!(
		group,
		select_group_by_single_field_avg,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 100,
		"SELECT math::mean(stats.score), level FROM item GROUP BY level;"
	);

	bench!(
		group,
		select_group_by_single_field_min_max,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 100,
		"SELECT math::min(stats.score), math::max(stats.score), level FROM item GROUP BY level;"
	);

	bench!(
		group,
		select_group_by_multiple_fields,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| !result.as_array().unwrap().is_empty(),
		"SELECT count(), level, stats.rank FROM item GROUP BY level, stats.rank;"
	);

	bench!(
		group,
		select_group_all_count,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result == value("[{ count: 10000 }]").unwrap(),
		"SELECT count() FROM item GROUP ALL;"
	);

	bench!(
		group,
		select_group_all_sum,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 1,
		"SELECT math::sum(stats.score) FROM item GROUP ALL;"
	);

	group.finish();
}

// ============================================================================
// Benchmark: Graph traversal operations
// ============================================================================

fn bench_graph_traversal(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("graph_traversal");
	// Setup the datastore with graph data
	let (dbs, ses) = block_on(setup_datastore_with_query(
		r#"
			FOR $i IN 0..1000 {
				CREATE person SET id = type::record('person', $i), name = type::string($i);
			};
			FOR $i IN 0..1000 {
				LET $from = type::record('person', $i);
				LET $to1 = type::record('person', ($i + 1) % 1000);
				LET $to2 = type::record('person', ($i + 2) % 1000);
				LET $to3 = type::record('person', ($i + 3) % 1000);
				RELATE $from->knows->$to1 SET weight = rand::float();
				RELATE $from->knows->$to2 SET weight = rand::float();
				RELATE $from->knows->$to3 SET weight = rand::float();
			};
		"#,
	));

	bench!(
		group,
		traversal_outbound,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT ->knows FROM person;"
	);

	bench!(
		group,
		traversal_outbound_target,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT ->knows->person FROM person;"
	);

	bench!(
		group,
		traversal_inbound,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT <-knows FROM person;"
	);

	bench!(
		group,
		traversal_inbound_source,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT <-knows<-person FROM person;"
	);

	bench!(
		group,
		traversal_bidirectional,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT <->knows FROM person;"
	);

	bench!(
		group,
		traversal_with_where,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT ->(knows WHERE weight > 0.5) FROM person;"
	);

	bench!(
		group,
		traversal_with_limit,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT ->(knows LIMIT 1) FROM person;"
	);

	group.finish();
}

// ============================================================================
// Benchmark: Subqueries
// ============================================================================

fn bench_subqueries(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("subqueries");
	// Setup the datastore with 1,000 records
	let (dbs, ses) = block_on(setup_datastore_with_records(1_000));

	bench!(
		group,
		subquery_in_from,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT * FROM (SELECT * FROM item);"
	);

	bench!(
		group,
		subquery_in_from_with_where,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| !result.as_array().unwrap().is_empty(),
		"SELECT * FROM (SELECT * FROM item WHERE level > 50);"
	);

	bench!(
		group,
		subquery_in_projection,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT *, (SELECT VALUE level FROM item WHERE level > 50 LIMIT 1) AS max_level FROM item;"
	);

	bench!(
		group,
		subquery_value_id,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT * FROM (SELECT VALUE id FROM item);"
	);

	bench!(
		group,
		nested_subquery,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| !result.as_array().unwrap().is_empty(),
		"SELECT * FROM (SELECT * FROM (SELECT * FROM item WHERE level > 50) WHERE active = true);"
	);

	group.finish();
}

// ============================================================================
// Benchmark: Record links and FETCH operations
// ============================================================================

fn bench_record_links(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("record_links");
	// Setup the datastore with linked records
	let (dbs, ses) = block_on(setup_datastore_with_query(
		r#"
			FOR $i IN 0..1000 {
				LET $friend = type::record('person', ($i + 500) % 1000);
				CREATE person SET
					id = type::record('person', $i),
					name = type::string($i),
					friend = $friend,
					tags = [
						type::record('tag', $i % 10),
						type::record('tag', ($i + 1) % 10),
						type::record('tag', ($i + 2) % 10)
					];
			};
			FOR $i IN 0..10 {
				CREATE tag SET
					id = type::record('tag', $i),
					name = 'Tag ' + type::string($i);
			};
		"#,
	));

	bench!(
		group,
		field_access_on_link,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT friend.name FROM person;"
	);

	bench!(
		group,
		fetch_single_level,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT * FROM person FETCH friend;"
	);

	bench!(
		group,
		fetch_nested_path,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT * FROM person FETCH friend.friend;"
	);

	bench!(
		group,
		array_links_wildcard,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT tags.*.name FROM person;"
	);

	bench!(
		group,
		fetch_array_links,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT * FROM person FETCH tags;"
	);

	bench!(
		group,
		multiple_fetches,
		&dbs,
		&ses,
		throughput: 1_000,
		expected: |result| result.as_array().unwrap().len() == 1_000,
		"SELECT * FROM person FETCH friend, tags;"
	);

	group.finish();
}

// ============================================================================
// Benchmark: SELECT table with INDEX and NOINDEX
// ============================================================================

fn bench_table_select_index_noindex(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_index_noindex");
	// Setup the datastore with a query
	let (dbs, ses) = block_on(setup_datastore_with_query(
		r#"
			DEFINE INDEX idx ON TABLE item COLUMNS field;
			FOR $i IN 0..10000 {
				CREATE item SET field = $i;
			};
		"#,
	));

	bench!(
		group,
		select_with_noindex,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 5_000,
		"SELECT * FROM item WITH NOINDEX WHERE field > 4999;"
	);

	bench!(
		group,
		select_with_noindex_count,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result == value("[{ count: 5000 }]").unwrap(),
		"SELECT count() FROM item WITH NOINDEX WHERE field > 4999 GROUP ALL;"
	);

	bench!(
		group,
		select_with_index,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result.as_array().unwrap().len() == 5_000,
		"SELECT * FROM item WHERE field > 4999;"
	);

	bench!(
		group,
		select_with_index_count,
		&dbs,
		&ses,
		throughput: 10_000,
		expected: |result| result == value("[{ count: 5000 }]").unwrap(),
		"SELECT count() FROM item WHERE field > 4999 GROUP ALL;"
	);

	group.finish();
}

// ============================================================================
// Benchmark: SELECT table with FULLTEXT INDEX
// ============================================================================

fn bench_table_select_fulltext_index(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("select_table_fulltext_index");
	// Setup the datastore with a query
	let (dbs, ses) = block_on(setup_datastore_with_query(
		r#"
			DEFINE INDEX number ON item FIELDS number;
			DEFINE ANALYZER simple TOKENIZERS blank,class;
			DEFINE INDEX search ON item FIELDS label FULLTEXT ANALYZER simple BM25;
			FOR $i IN 0..10000 {
				LET $a = $i * 5;
				LET $b = $i * 5 + 1;
				LET $c = $i * 5 + 2;
				LET $d = $i * 5 + 3;
				LET $e = $i * 5 + 4;
				CREATE item SET id = type::number($a), name = type::string($a), number = 0, label='alpha';
				CREATE item SET id = type::number($b), name = type::string($b), number = 1, label='bravo';
				CREATE item SET id = type::number($c), name = type::string($c), number = 2, label='charlie';
				CREATE item SET id = type::number($d), name = type::string($d), number = 3, label='delta';
				CREATE item SET id = type::number($e), name = type::string($e), number = 4, label='echo';
			};
		"#,
	));

	bench!(
		group,
		select_count_with_fulltext_index,
		&dbs,
		&ses,
		throughput: 50_000,
		expected: |result| result == value("[{ count: 50000 }]").unwrap(),
		"SELECT count() FROM item GROUP ALL;"
	);

	bench!(
		group,
		select_with_numeric_index,
		&dbs,
		&ses,
		throughput: 50_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item WHERE number = 4;"
	);

	bench!(
		group,
		select_with_numeric_index_count,
		&dbs,
		&ses,
		throughput: 50_000,
		expected: |result| result == value("[{ count: 10000 }]").unwrap(),
		"SELECT count() FROM item WHERE number = 4 GROUP ALL;"
	);

	bench!(
		group,
		select_with_fulltext_index,
		&dbs,
		&ses,
		throughput: 50_000,
		expected: |result| result.as_array().unwrap().len() == 10_000,
		"SELECT * FROM item WHERE label @@ 'charlie';"
	);

	bench!(
		group,
		select_with_fulltext_index_count,
		&dbs,
		&ses,
		throughput: 50_000,
		expected: |result| result == value("[{ count: 10000 }]").unwrap(),
		"SELECT count() FROM item WHERE label @@ 'charlie' GROUP ALL;"
	);

	group.finish();
}

criterion_group!(
	name = benches;
	config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
	targets = bench_value_select,
		bench_record_select,
		bench_table_select,
		bench_table_select_limit,
		bench_table_select_start,
		bench_table_select_start_limit,
		bench_table_select_expression,
		bench_table_select_order,
		bench_table_select_group,
		bench_graph_traversal,
		bench_subqueries,
		bench_record_links,
		bench_table_select_where_condition,
		bench_table_select_index_noindex,
		bench_table_select_fulltext_index,
);
criterion_main!(benches);
