#![feature(test)]

extern crate test;
use surrealdb::{dbs::Session, kvs::Datastore};
use test::{black_box, Bencher};

macro_rules! query {
	($name: ident, $query: expr) => {
		query!($name, "", $query);
	};
	($name: ident, $setup: expr, $query: expr) => {
		#[bench]
		fn $name(b: &mut Bencher) {
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
			});
		}
	};
}

query!(create_delete_simple, "CREATE person:one; DELETE person:one;");
query!(select_simple_one, "CREATE person:tobie;", "SELECT * FROM person;");
query!(select_simple_five, "CREATE person:one; CREATE person:two; CREATE person:three; CREATE person:four; CREATE person:five;", "SELECT * FROM person;");
query!(select_future, "SELECT * FROM <future>{5};");
query!(update_simple, "CREATE thing:one SET value = 0;", "UPDATE thing SET value = value + 1;");
query!(
	select_record_link,
	"CREATE person:one SET friend = person:two; CREATE person:two SET age = 30;",
	"SELECT * FROM person:one.friend.age;"
);
