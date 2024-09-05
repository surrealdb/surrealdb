use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};

macro_rules! parser {
	($c: expr, $name: ident, $parser: path, $text: expr) => {
		$c.bench_function(stringify!($name), |b| {
			let text = $text;

			b.iter(|| black_box($parser(black_box(text))).unwrap())
		});
	};
}

fn bench_parser(c: &mut Criterion) {
	let mut c = c.benchmark_group("parser");
	c.throughput(Throughput::Elements(1));
	parser!(c, select_simple, surrealdb::sql::parse, "SELECT * FROM person;");
	parser!(
		c,
		select_complex,
		surrealdb::sql::parse,
		"SELECT name, age, country FROM person WHERE hair = 'brown' AND is_vegetarian;"
	);
	parser!(c, transaction, surrealdb::sql::parse, "BEGIN TRANSACTION; UPDATE person:finn SET squirrels = 'yes'; SELECT * FROM person; COMMIT TRANSACTION;");
	parser!(c, datetime, surrealdb::sql::parse, "RETURN '2022-07-03T07:18:52.841147+02:00';");
	parser!(c, duration, surrealdb::sql::parse, "RETURN [100w, 5d, 20m, 2s];");
	parser!(
		c,
		casting_deep,
		surrealdb::sql::parse,
		"RETURN <float><float><float><float><float>1.0;"
	);
	parser!(
		c,
		json_geo,
		surrealdb::sql::parse,
		"RETURN { type: 'Point', coordinates: [-0.118092, 51.509865] };"
	);
	parser!(c, json_number, surrealdb::sql::json, "1.2345");
	parser!(
		c,
		json_small_object,
		surrealdb::sql::json,
		"{'key': true, 'number': 42.0, 'value': null}"
	);
	parser!(c, json_small_array, surrealdb::sql::json, "[1, false, null, 'foo']");
	parser!(
		c,
		json_large_array,
		surrealdb::sql::json,
		&format!("[{}]", (1..=100).map(|n| n.to_string()).collect::<Vec<_>>().join(", "))
	);
	parser!(
		c,
		json_large_object,
		surrealdb::sql::json,
		&format!(
			"{{{}}}",
			&(1..=100).map(|n| format!("'{n}': {n}")).collect::<Vec<_>>().join(", ")
		)
	);
	parser!(c, full_test, surrealdb::sql::parse, include_str!("../../core/test.surql"));
	c.finish();
}

criterion_group!(
	name = benches;
	config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
	targets = bench_parser
);
criterion_main!(benches);
