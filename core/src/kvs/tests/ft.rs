// RUST_LOG=trace RUSTFLAGS="--cfg surrealdb_unstable" cargo test --features sql2,kv-mem -p surrealdb kvs::tests::mem:: -- --nocapture --ignored

#[cfg(test)]
mod tests {

	#[tokio::test]
	#[cfg(feature="kv-mem")]
	async fn table_define_multiple_views() {
		// Setup

		use crate::{dbs::Session, kvs::Datastore};
		let ds = Datastore::new_full("memory", None)
			.await
			.unwrap();

		let ses = Session::owner().with_ns("test").with_db("test");
		let res = ds.execute(r#"
			CREATE happy:1 SET year=2024, month=1, day=1;
			DEFINE TABLE monthly AS
				SELECT count() as activeRounds, year, month
				FROM happy GROUP BY year, month;

			DEFINE TABLE daily AS
				SELECT count() as activeRounds, year, month, day
				FROM happy GROUP BY year, month, day;
		"#, &ses, None).await.unwrap();
		//
		match res.get(0).result {
			Ok(a) => {
				assert_eq!(a.len(), 3);
				//
			},
			e => panic!("{}", e)
		}
	}
}
