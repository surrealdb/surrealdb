use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};
use surrealdb_core::syn;

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
	parser!(c, select_simple, syn::parse, "SELECT * FROM person;");
	parser!(
		c,
		select_complex,
		syn::parse,
		"SELECT name, age, country FROM person WHERE hair = 'brown' AND is_vegetarian;"
	);
	parser!(
		c,
		transaction,
		syn::parse,
		"BEGIN TRANSACTION; UPDATE person:finn SET squirrels = 'yes'; SELECT * FROM person; COMMIT TRANSACTION;"
	);
	parser!(c, datetime, syn::parse, "RETURN '2022-07-03T07:18:52.841147+02:00';");
	parser!(c, duration, syn::parse, "RETURN [100w, 5d, 20m, 2s];");
	parser!(c, casting_deep, syn::parse, "RETURN <float><float><float><float><float>1.0;");
	parser!(
		c,
		json_geo,
		syn::parse,
		"RETURN { type: 'Point', coordinates: [-0.118092, 51.509865] };"
	);
	parser!(c, json_number, syn::json, "1.2345");
	parser!(c, json_small_object, syn::json, "{'key': true, 'number': 42.0, 'value': null}");
	parser!(c, json_small_array, syn::json, "[1, false, null, 'foo']");
	parser!(
		c,
		json_large_array,
		syn::json,
		&format!("[{}]", (1..=100).map(|n| n.to_string()).collect::<Vec<_>>().join(", "))
	);
	parser!(
		c,
		json_large_object,
		syn::json,
		&format!(
			"{{{}}}",
			&(1..=100).map(|n| format!("'{n}': {n}")).collect::<Vec<_>>().join(", ")
		)
	);
	parser!(c, full_test, syn::parse, include_str!("../../core/test.surql"));
	c.finish();
}

criterion_group!(
	name = benches;
	config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
	targets = bench_parser
);
criterion_main!(benches);
