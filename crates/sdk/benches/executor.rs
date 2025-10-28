use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::Future;
use pprof::criterion::{Output, PProfProfiler};
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;

macro_rules! query {
	($c: expr, $name: ident, $query: expr) => {
		query!($c, $name, "", $query);
	};
	($c: expr, $name: ident, $setup: expr, $query: expr) => {
		$c.bench_function(stringify!($name), |b| {
			let (dbs, ses) = block_on(async {
				let dbs =
					Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());
				let ses = Session::owner().with_ns("test").with_db("test");
				let setup = $setup;
				if !setup.is_empty() {
					dbs.execute(setup, &ses, None).await.unwrap();
				}
				(dbs, ses)
			});

			b.iter(|| {
				block_on(async {
					black_box(dbs.execute(black_box($query), &ses, None).await).unwrap();
				});
			})
		});
	};
}

#[tokio::main]
async fn block_on<T>(future: impl Future<Output = T>) -> T {
	future.await
}

fn bench_executor(c: &mut Criterion) {
	let mut c = c.benchmark_group("executor");
	c.throughput(Throughput::Elements(1));
	query!(c, create_delete_simple, "CREATE person:one; DELETE person:one;");
	query!(c, select_simple_one, "CREATE person:tobie;", "SELECT * FROM person;");
	query!(
		c,
		select_simple_five,
		"CREATE person:one; CREATE person:two; CREATE person:three; CREATE person:four; CREATE person:five;",
		"SELECT * FROM person;"
	);
	query!(
		c,
		update_simple,
		"CREATE thing:one SET value = 0;",
		"UPDATE thing SET value = value + 1;"
	);
	query!(
		c,
		select_record_link,
		"CREATE person:one SET friend = person:two; CREATE person:two SET age = 30;",
		"SELECT * FROM person:one.friend.age;"
	);
	#[cfg(feature = "scripting")]
	query!(c, javascript_simple, "RETURN function() { return 1 + 1; };");
	#[cfg(feature = "scripting")]
	query!(
		c,
		javascript_function,
		"RETURN function() { return surrealdb::functions::count([1, 2, 3]); };"
	);
	c.finish();
}

criterion_group!(
	name = benches;
	config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
	targets = bench_executor
);
criterion_main!(benches);
