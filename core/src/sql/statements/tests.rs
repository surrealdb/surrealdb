#[cfg(test)]
mod tests {
	use crate::{
		dbs::Session,
		kvs::{Datastore, LockType, TransactionType},
		sql::Value,
	};

	#[tokio::test]
	async fn cross_transaction_caching_uuids_updated() {
		let ds = Datastore::new("memory").await.unwrap();
		let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

		// Define the table, set the initial uuids
		let sql = r"DEFINE TABLE test;".to_owned();
		let res = &mut ds.execute(&sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 1);
		assert!(res.remove(0).result.is_ok());
		// Obtain the initial uuids
		let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await.unwrap();
		let initial = txn.get_tb(&"test", &"test", &"test").await.unwrap();
		drop(txn);

		// Define some resources to refresh the UUIDs
		let sql = r"
            DEFINE FIELD test ON test;
            DEFINE EVENT test ON test WHEN {} THEN {};
            DEFINE TABLE view AS SELECT * FROM test;
            DEFINE INDEX test ON test FIELDS test;
            LIVE SELECT * FROM test;
        "
		.to_owned();
		let res = &mut ds.execute(&sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 5);
		assert!(res.remove(0).result.is_ok());
		assert!(res.remove(0).result.is_ok());
		assert!(res.remove(0).result.is_ok());
		assert!(res.remove(0).result.is_ok());
		let lqid = res.remove(0).result.unwrap();
		assert!(matches!(lqid, Value::Uuid(_)));
		// Obtain the uuids after definitions
		let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await.unwrap();
		let after_define = txn.get_tb(&"test", &"test", &"test").await.unwrap();
		drop(txn);
		// Compare uuids after definitions
		assert_ne!(initial.cache_fields_ts, after_define.cache_fields_ts);
		assert_ne!(initial.cache_events_ts, after_define.cache_events_ts);
		assert_ne!(initial.cache_tables_ts, after_define.cache_tables_ts);
		assert_ne!(initial.cache_indexes_ts, after_define.cache_indexes_ts);
		assert_ne!(initial.cache_lives_ts, after_define.cache_lives_ts);

		// Remove the defined resources to refresh the UUIDs
		let sql = r"
            REMOVE FIELD test ON test;
            REMOVE EVENT test ON test;
            REMOVE TABLE view;
            REMOVE INDEX test ON test;
            KILL $lqid;
        "
		.to_owned();
		let vars = map! { "lqid".to_string() => lqid };
		let res = &mut ds.execute(&sql, &ses, Some(vars)).await.unwrap();
		assert_eq!(res.len(), 5);
		assert!(res.remove(0).result.is_ok());
		assert!(res.remove(0).result.is_ok());
		assert!(res.remove(0).result.is_ok());
		assert!(res.remove(0).result.is_ok());
		assert!(res.remove(0).result.is_ok());
		// Obtain the uuids after definitions
		let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await.unwrap();
		let after_remove = txn.get_tb(&"test", &"test", &"test").await.unwrap();
		drop(txn);
		// Compare uuids after definitions
		assert_ne!(after_define.cache_fields_ts, after_remove.cache_fields_ts);
		assert_ne!(after_define.cache_events_ts, after_remove.cache_events_ts);
		assert_ne!(after_define.cache_tables_ts, after_remove.cache_tables_ts);
		assert_ne!(after_define.cache_indexes_ts, after_remove.cache_indexes_ts);
		assert_ne!(after_define.cache_lives_ts, after_remove.cache_lives_ts);
	}
}
