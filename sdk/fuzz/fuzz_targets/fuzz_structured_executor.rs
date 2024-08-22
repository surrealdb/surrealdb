#![no_main]

use libfuzzer_sys::fuzz_target;
use std::hint::black_box;
use surrealdb::{dbs::Session, kvs::Datastore, sql::Query};

fuzz_target!(|query: Query| {
	tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
		let dbs = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().with_ns("test").with_db("test");
		_ = black_box(dbs.process(query, &ses, None).await);
	})
});
