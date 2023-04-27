#![feature(test)]

extern crate test;
use surrealdb::{dbs::Session, kvs::Datastore};
use test::{black_box, Bencher};

#[bench]
fn select_simple(b: &mut Bencher) {
    let (dbs, ses) = futures::executor::block_on(async {
        let dbs = Datastore::new("memory").await.unwrap();
        let ses = Session::for_kv().with_ns("test").with_db("test");
        dbs.execute(&"CREATE person:tobie;", &ses, None, false).await.unwrap();
        (dbs, ses)
    });

	b.iter(|| {
        futures::executor::block_on(async {
            black_box(dbs.execute(black_box(&"SELECT * FROM person;"), &ses, None, false).await).unwrap();
        });
	});
}


