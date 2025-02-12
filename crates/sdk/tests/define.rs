mod parse;

use parse::Parse;

mod helpers;
use helpers::*;

use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::kvs::{LockType, TransactionType};
use surrealdb::sql::Idiom;
use surrealdb::sql::{Part, Value};
use surrealdb_core::cnf::{INDEXING_BATCH_SIZE, NORMAL_FETCH_SIZE};
use test_log::test;
use tracing::info;

#[test(tokio::test)]
async fn define_statement_index_concurrently_building_status() -> Result<(), Error> {
	let session = Session::owner().with_ns("test").with_db("test");
	let ds = new_ds().await?;
	// Populate initial records
	let initial_count = *INDEXING_BATCH_SIZE * 3 / 2;
	info!("Populate: {}", initial_count);
	for i in 0..initial_count {
		let mut responses = ds
			.execute(
				&format!("CREATE user:{i} SET email = 'test{i}@surrealdb.com';"),
				&session,
				None,
			)
			.await?;
		skip_ok(&mut responses, 1)?;
	}
	// Create the index concurrently
	info!("Indexing starts");
	let mut r =
		ds.execute("DEFINE INDEX test ON user FIELDS email CONCURRENTLY", &session, None).await?;
	skip_ok(&mut r, 1)?;
	//
	let mut appending_count = *NORMAL_FETCH_SIZE * 3 / 2;
	info!("Appending: {}", appending_count);
	// Loop until the index is built
	let now = SystemTime::now();
	let mut initial_count = None;
	let mut updates_count = None;
	// While the concurrent indexing is running, we update and delete records
	let time_out = Duration::from_secs(120);
	loop {
		if now.elapsed().map_err(|e| Error::Internal(e.to_string()))?.gt(&time_out) {
			panic!("Time-out {time_out:?}");
		}
		if appending_count > 0 {
			let sql = if appending_count % 2 != 0 {
				format!("UPDATE user:{appending_count} SET email = 'new{appending_count}@surrealdb.com';")
			} else {
				format!("DELETE user:{appending_count}")
			};
			let mut responses = ds.execute(&sql, &session, None).await?;
			skip_ok(&mut responses, 1)?;
			appending_count -= 1;
		}
		// We monitor the status
		let mut r = ds.execute("INFO FOR INDEX test ON user", &session, None).await?;
		let tmp = r.remove(0).result?;
		if let Value::Object(o) = &tmp {
			if let Some(Value::Object(b)) = o.get("building") {
				if let Some(v) = b.get("status") {
					if Value::from("started").eq(v) {
						continue;
					}
					let new_count = b.get("count").cloned();
					if Value::from("initial").eq(v) {
						if new_count != initial_count {
							assert!(new_count > initial_count, "{new_count:?}");
							info!("New initial count: {:?}", new_count);
							initial_count = new_count;
						}
						continue;
					}
					if Value::from("updates").eq(v) {
						if new_count != updates_count {
							assert!(new_count > updates_count, "{new_count:?}");
							info!("New updates count: {:?}", new_count);
							updates_count = new_count;
						}
						continue;
					}
					let val = Value::parse(
						"{
										building: {
											status: 'built'
										}
									}",
					);
					assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
					break;
				}
			}
		}
		panic!("Unexpected value {tmp:#}");
	}
	Ok(())
}

//
// Permissions
//

#[tokio::test]
async fn permissions_checks_define_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE NAMESPACE NS"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let access1 = "{ accesses: {  }, namespaces: { NS: 'DEFINE NAMESPACE NS' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }";
	let access2 = "{ accesses: {  }, namespaces: {  }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }";
	let check_results = [vec![access1], vec![access2]];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), false),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_db() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "DEFINE DATABASE DB"), ("check", "INFO FOR NS")]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
		vec!["{ accesses: {  }, databases: { DB: 'DEFINE DATABASE DB' }, users: {  } }"],
		vec!["{ accesses: {  }, databases: {  }, users: {  } }"],
	];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_function() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE FUNCTION fn::greet() {RETURN \"Hello\";}"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: { greet: \"DEFINE FUNCTION fn::greet() { RETURN 'Hello'; } PERMISSIONS FULL\" }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_analyzer() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ANALYZER analyzer TOKENIZERS BLANK"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: { analyzer: 'DEFINE ANALYZER analyzer TOKENIZERS BLANK' }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_root() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let access1 = r#"{ accesses: { access: "DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, namespaces: {  }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }"#;
	let access2 = "{ accesses: {  }, namespaces: {  }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }";
	let check_results = [vec![access1], vec![access2]];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), false),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON NS TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { access: \"DEFINE ACCESS access ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, databases: {  }, users: {  } }"],
		vec!["{ accesses: {  }, databases: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_db() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS access ON DB TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { access: \"DEFINE ACCESS access ON DATABASE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_root() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check1 = r#"{ accesses: {  }, namespaces: {  }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: { user: "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h" } }"#;
	let check2 = "{ accesses: {  }, namespaces: {  }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }";
	let check_results = [vec![check1], vec![check2]];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), false),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_ns() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE USER user ON NS PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, databases: {  }, users: { user: \"DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h\" } }"],
		vec!["{ accesses: {  }, databases: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_user_db() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE USER user ON DB PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: { user: \"DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h\" } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_access_record() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'secret' DURATION FOR TOKEN 15m, FOR SESSION 12h"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: { account: \"DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 15m, FOR SESSION 12h\" }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), false),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_param() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE PARAM $param VALUE 'foo'"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: { param: \"DEFINE PARAM $param VALUE 'foo' PERMISSIONS FULL\" }, tables: {  }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_table() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "DEFINE TABLE TB"), ("check", "INFO FOR DB")]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: { TB: 'DEFINE TABLE TB TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"],
		vec!["{ accesses: {  }, analyzers: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, tables: {  }, users: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_event() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE EVENT event ON TABLE TB WHEN true THEN RETURN 'foo'"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ events: { event: \"DEFINE EVENT event ON TB WHEN true THEN (RETURN 'foo')\" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"],
		vec!["{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_field() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE FIELD field ON TABLE TB"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ events: {  }, fields: { field: 'DEFINE FIELD field ON TB PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"],
		vec!["{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_define_index() {
	let scenario = HashMap::from([
		("prepare", ""),
		("test", "DEFINE INDEX index ON TABLE TB FIELDS field"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ events: {  }, fields: {  }, indexes: { index: 'DEFINE INDEX index ON TB FIELDS field' }, lives: {  }, tables: {  } }"],
		vec!["{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"]
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), false),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn cross_transaction_caching_uuids_updated() -> Result<(), Error> {
	let ds = new_ds().await?;
	let cache = ds.get_cache();
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

	// Define the table, set the initial uuids
	let sql = r"DEFINE TABLE test;".to_owned();
	let res = &mut ds.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	res.remove(0).result.unwrap();
	// Obtain the initial uuids
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let initial = txn.get_tb("test", "test", "test").await?;
	let initial_live_query_version = cache.get_live_queries_version("test", "test", "test")?;
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
	let res = &mut ds.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	let lqid = res.remove(0).result?;
	assert!(matches!(lqid, Value::Uuid(_)));
	// Obtain the uuids after definitions
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let after_define = txn.get_tb("test", "test", "test").await?;
	let after_define_live_query_version = cache.get_live_queries_version("test", "test", "test")?;
	drop(txn);
	// Compare uuids after definitions
	assert_ne!(initial.cache_fields_ts, after_define.cache_fields_ts);
	assert_ne!(initial.cache_events_ts, after_define.cache_events_ts);
	assert_ne!(initial.cache_tables_ts, after_define.cache_tables_ts);
	assert_ne!(initial.cache_indexes_ts, after_define.cache_indexes_ts);
	assert_ne!(initial_live_query_version, after_define_live_query_version);

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
	let res = &mut ds.execute(&sql, &ses, Some(vars)).await?;
	assert_eq!(res.len(), 5);
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	// Obtain the uuids after definitions
	let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
	let after_remove = txn.get_tb("test", "test", "test").await?;
	let after_remove_live_query_version = cache.get_live_queries_version("test", "test", "test")?;
	drop(txn);
	// Compare uuids after definitions
	assert_ne!(after_define.cache_fields_ts, after_remove.cache_fields_ts);
	assert_ne!(after_define.cache_events_ts, after_remove.cache_events_ts);
	assert_ne!(after_define.cache_tables_ts, after_remove.cache_tables_ts);
	assert_ne!(after_define.cache_indexes_ts, after_remove.cache_indexes_ts);
	assert_ne!(after_define_live_query_version, after_remove_live_query_version);
	//
	Ok(())
}
