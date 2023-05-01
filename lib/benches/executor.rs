use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use surrealdb::{dbs::Session, kvs::Datastore};

macro_rules! query {
	($c: expr, $name: ident, $query: expr) => {
		query!($c, $name, "", $query);
	};
	($c: expr, $name: ident, $setup: expr, $query: expr) => {
		$c.bench_function(stringify!($name), |b| {
			let (dbs, ses) = futures::executor::block_on(async {
				let dbs = Datastore::new("memory").await.unwrap();
				let ses = Session::for_kv().with_ns("test").with_db("test");
				let setup = $setup;
				if !setup.is_empty() {
					dbs.execute(setup, &ses, None, false).await.unwrap();
				}
				(dbs, ses)
			});

			b.iter(|| {
				futures::executor::block_on(async {
					black_box(dbs.execute(black_box($query), &ses, None, false).await).unwrap();
				});
			})
		});
	};
}

fn bench_executor(c: &mut Criterion) {
	let mut c = c.benchmark_group("executor");
	c.throughput(Throughput::Elements(1));
	query!(c, create_delete_simple, "CREATE person:one; DELETE person:one;");
	query!(c, select_simple_one, "CREATE person:tobie;", "SELECT * FROM person;");
	query!(c, select_simple_five, "CREATE person:one; CREATE person:two; CREATE person:three; CREATE person:four; CREATE person:five;", "SELECT * FROM person;");
	query!(c, select_future, "SELECT * FROM <future>{5};");
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
	c.finish();
}

criterion_group!(benches, bench_executor);
criterion_main!(benches);
