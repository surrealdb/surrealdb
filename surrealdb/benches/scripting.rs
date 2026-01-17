#![allow(clippy::unwrap_used)]

use std::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;
use tokio::runtime::Builder;

// ============================================================================
// Benchmark: Simple expression overhead
// ============================================================================

fn bench_scripting_overhead_simple(c: &mut Criterion) {
	let mut group = c.benchmark_group("scripting_overhead_simple");
	group.throughput(Throughput::Elements(1));
	group.measurement_time(Duration::from_secs(10));

	let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
	let (dbs, ses) = runtime.block_on(setup_datastore());

	// Native arithmetic
	group.bench_function("native_arithmetic", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs.execute(black_box("RETURN 1 + 2 + 3;"), &ses, None).await.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted arithmetic
	group.bench_function("scripted_arithmetic", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(black_box("RETURN function() { return 1 + 2 + 3; };"), &ses, None)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Native string concatenation
	group.bench_function("native_string_concat", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(black_box("RETURN 'hello' + ' ' + 'world';"), &ses, None)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted string concatenation
	group.bench_function("scripted_string_concat", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box("RETURN function() { return 'hello' + ' ' + 'world'; };"),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Native array creation
	group.bench_function("native_array_creation", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r =
				dbs.execute(black_box("RETURN [1, 2, 3, 4, 5];"), &ses, None).await.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted array creation
	group.bench_function("scripted_array_creation", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box("RETURN function() { return [1, 2, 3, 4, 5]; };"),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Native object creation
	group.bench_function("native_object_creation", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(black_box("RETURN { name: 'test', value: 42 };"), &ses, None)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted object creation
	group.bench_function("scripted_object_creation", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box("RETURN function() { return { name: 'test', value: 42 }; };"),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	group.finish();
}

// ============================================================================
// Benchmark: Query execution overhead (surrealdb.query vs native)
// ============================================================================

fn bench_scripting_overhead_query(c: &mut Criterion) {
	let mut group = c.benchmark_group("scripting_overhead_query");
	group.measurement_time(Duration::from_secs(10));

	let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

	// Test with different dataset sizes
	for count in [100000] {
		let (dbs, ses) = runtime.block_on(setup_datastore_with_records(count));
		group.throughput(Throughput::Elements(count));

		// Native SELECT
		group.bench_function(format!("native_select_{count}"), |b| {
			b.to_async(&runtime).iter(|| async {
				let mut r =
					dbs.execute(black_box("SELECT * FROM item;"), &ses, None).await.unwrap();
				black_box(r.remove(0).result.unwrap());
			})
		});

		// Scripted SELECT via surrealdb.query
		group.bench_function(format!("scripted_query_{count}"), |b| {
			b.to_async(&runtime).iter(|| async {
				let mut r = dbs
					.execute(
						black_box(
							"RETURN function() { return surrealdb.query('SELECT * FROM item'); };",
						),
						&ses,
						None,
					)
					.await
					.unwrap();
				black_box(r.remove(0).result.unwrap());
			})
		});

		// Native SELECT with WHERE clause
		group.bench_function(format!("native_select_where_{count}"), |b| {
			b.to_async(&runtime).iter(|| async {
				let mut r = dbs
					.execute(black_box("SELECT * FROM item WHERE level > 50;"), &ses, None)
					.await
					.unwrap();
				black_box(r.remove(0).result.unwrap());
			})
		});

		// Scripted SELECT with WHERE via surrealdb.query
		group.bench_function(format!("scripted_query_where_{count}"), |b| {
			b.to_async(&runtime).iter(|| async {
				let mut r = dbs
					.execute(
						black_box("RETURN function() { return surrealdb.query('SELECT * FROM item WHERE level > 50'); };"),
						&ses,
						None,
					)
					.await
					.unwrap();
				black_box(r.remove(0).result.unwrap());
			})
		});
	}

	group.finish();
}

// ============================================================================
// Benchmark: Function call overhead
// ============================================================================

fn bench_scripting_overhead_functions(c: &mut Criterion) {
	let mut group = c.benchmark_group("scripting_overhead_functions");
	group.throughput(Throughput::Elements(1));
	group.measurement_time(Duration::from_secs(10));

	let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
	let (dbs, ses) = runtime.block_on(setup_datastore());

	// Native math::sum
	group.bench_function("native_math_sum", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(black_box("RETURN math::sum([1, 2, 3, 4, 5]);"), &ses, None)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted math.sum
	group.bench_function("scripted_math_sum", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box(
						"RETURN function() { return surrealdb.functions.math.sum([1, 2, 3, 4, 5]); };",
					),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Native string::lowercase
	group.bench_function("native_string_lowercase", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(black_box("RETURN string::lowercase('HELLO WORLD');"), &ses, None)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted string.lowercase
	group.bench_function("scripted_string_lowercase", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box(
						"RETURN function() { return surrealdb.functions.string.lowercase('HELLO WORLD'); };",
					),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Native array::len
	group.bench_function("native_array_len", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(black_box("RETURN array::len([1, 2, 3, 4, 5]);"), &ses, None)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted array.len
	group.bench_function("scripted_array_len", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box(
						"RETURN function() { return surrealdb.functions.array.len([1, 2, 3, 4, 5]); };",
					),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	group.finish();
}

// ============================================================================
// Benchmark: Complex operations overhead
// ============================================================================

fn bench_scripting_overhead_complex(c: &mut Criterion) {
	let mut group = c.benchmark_group("scripting_overhead_complex");
	group.throughput(Throughput::Elements(1));
	group.measurement_time(Duration::from_secs(10));

	let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
	let (dbs, ses) = runtime.block_on(setup_datastore_with_records(100));

	// Native array operations
	group.bench_function("native_array_ops", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box("RETURN array::sort([5, 2, 8, 1, 9]) + array::reverse([1, 2, 3]);"),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted array operations
	group.bench_function("scripted_array_ops", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box(
						"RETURN function() { 
							let sorted = surrealdb.functions.array.sort([5, 2, 8, 1, 9]);
							let reversed = surrealdb.functions.array.reverse([1, 2, 3]);
							return sorted.concat(reversed);
						};"
					),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Native nested object access
	group.bench_function("native_nested_object", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box(
						"RETURN { 
							user: { 
								name: 'test', 
								stats: { score: 100, rank: 5 } 
							} 
						}.user.stats.score;"
					),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted nested object access
	group.bench_function("scripted_nested_object", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box(
						"RETURN function() { 
							let obj = { 
								user: { 
									name: 'test', 
									stats: { score: 100, rank: 5 } 
								} 
							};
							return obj.user.stats.score;
						};"
					),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Native conditional logic
	group.bench_function("native_conditional", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box("LET $x = 42; RETURN IF $x > 40 THEN 'high' ELSE 'low' END;"),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted conditional logic
	group.bench_function("scripted_conditional", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box(
						"RETURN function() { 
							let x = 42; 
							return x > 40 ? 'high' : 'low';
						};"
					),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Native loop with array building
	group.bench_function("native_loop", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box("RETURN array::map([1, 2, 3, 4, 5], |$v| { $v * 2 });"),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	// Scripted loop with array building
	group.bench_function("scripted_loop", |b| {
		b.to_async(&runtime).iter(|| async {
			let mut r = dbs
				.execute(
					black_box(
						"RETURN function() { 
							return [1, 2, 3, 4, 5].map(v => v * 2);
						};"
					),
					&ses,
					None,
				)
				.await
				.unwrap();
			black_box(r.remove(0).result.unwrap());
		})
	});

	group.finish();
}

// ============================================================================
// Helper functions
// ============================================================================

async fn setup_datastore() -> (Datastore, Session) {
	let dbs = Datastore::new("memory").await.unwrap();
	let dbs = dbs.with_capabilities(Capabilities::all());
	let ses = Session::owner().with_ns("bench").with_db("bench");
	dbs.execute("USE NAMESPACE bench DATABASE bench", &ses, None).await.unwrap();
	(dbs, ses)
}

async fn setup_datastore_with_records(count: u64) -> (Datastore, Session) {
	let dbs = Datastore::new("memory").await.unwrap();
	let dbs = dbs.with_capabilities(Capabilities::all());
	let ses = Session::owner().with_ns("bench").with_db("bench");
	dbs.execute("USE NAMESPACE bench DATABASE bench", &ses, None).await.unwrap();

	if count > 0 {
		let mut setup = String::new();
		for i in 0..count {
			setup.push_str(&format!(
				r#"CREATE item:{i} SET
					name = 'Item {i}',
					level = {},
					active = {},
					stats = {{
						score: {},
						rank: {}
					}};
				"#,
				1 + (i % 100),
				i % 2 == 0,
				i % 100,
				i % 10,
			));
		}
		dbs.execute(&setup, &ses, None).await.unwrap();
	}

	(dbs, ses)
}

criterion_group!(
	name = benches;
	config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
	targets = bench_scripting_overhead_simple,
		bench_scripting_overhead_query,
		bench_scripting_overhead_functions,
		bench_scripting_overhead_complex
);
criterion_main!(benches);
