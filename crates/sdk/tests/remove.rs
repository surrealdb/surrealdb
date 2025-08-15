mod helpers;
use helpers::*;
use surrealdb_core::iam::Level;
use surrealdb_core::syn;
use surrealdb_core::val::Value;

#[macro_use]
mod util;

use std::collections::HashMap;

use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::Role;

#[tokio::test]
async fn remove_statement_table() -> Result<()> {
	let sql = "
		DEFINE TABLE test SCHEMALESS;
		REMOVE TABLE test;
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: {},
			analyzers: {},
			apis: {},
			buckets: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			sequences: {},
			tables: {},
			users: {}
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn remove_statement_namespace() -> Result<()> {
	// Namespace not selected
	{
		let sql = "
			REMOVE NAMESPACE test;
			DEFINE NAMESPACE test;
			REMOVE NAMESPACE test;
		";
		let dbs = new_ds().await?;
		let ses = Session::owner();
		let res = &mut dbs.execute(sql, &ses, None).await?;
		assert_eq!(res.len(), 3);
		//
		let tmp = res.remove(0).result;
		assert!(tmp.is_err());
		//
		let tmp = res.remove(0).result;
		tmp.unwrap();
		//
		let tmp = res.remove(0).result;
		tmp.unwrap();
	}
	// Namespace selected
	{
		let sql = "
			REMOVE NAMESPACE test;
			DEFINE NAMESPACE test;
			REMOVE NAMESPACE test;
		";
		let dbs = new_ds().await?;
		// No namespace is selected
		let ses = Session::owner().with_ns("test");
		let res = &mut dbs.execute(sql, &ses, None).await?;
		assert_eq!(res.len(), 3);
		//
		let tmp = res.remove(0).result;
		assert!(tmp.is_err());
		//
		let tmp = res.remove(0).result;
		tmp.unwrap();
		//
		let tmp = res.remove(0).result;
		tmp.unwrap();
	}
	Ok(())
}

#[tokio::test]
async fn remove_statement_database() -> Result<()> {
	// Database not selected
	{
		let sql = "
			REMOVE DATABASE test;
			DEFINE DATABASE test;
			REMOVE DATABASE test;
		";
		let dbs = new_ds().await?;
		let ses = Session::owner().with_ns("test");
		let res = &mut dbs.execute(sql, &ses, None).await?;
		assert_eq!(res.len(), 3);
		//
		let tmp = res.remove(0).result;
		assert!(tmp.is_err());
		//
		let tmp = res.remove(0).result;
		tmp.unwrap();
		//
		let tmp = res.remove(0).result;
		tmp.unwrap();
	}
	// Database selected
	{
		let sql = "
			REMOVE DATABASE test;
			DEFINE DATABASE test;
			REMOVE DATABASE test;
		";
		let dbs = new_ds().await?;
		// No database is selected
		let ses = Session::owner().with_ns("test").with_db("test");
		let res = &mut dbs.execute(sql, &ses, None).await?;
		assert_eq!(res.len(), 3);
		//
		let tmp = res.remove(0).result;
		assert!(tmp.is_err());
		//
		let tmp = res.remove(0).result;
		tmp.unwrap();
		//
		let tmp = res.remove(0).result;
		tmp.unwrap();
	}
	Ok(())
}

#[tokio::test]
async fn remove_statement_analyzer() -> Result<()> {
	let sql = "
		DEFINE ANALYZER english TOKENIZERS blank,class FILTERS lowercase,snowball(english);
		REMOVE ANALYZER english;
		INFO FOR DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	// Analyzer is defined
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// Analyzer is removed
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// Check infos output
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			accesses: {},
			analyzers: {},
			apis: {},
			buckets: {},
			configs: {},
			functions: {},
			models: {},
			params: {},
			sequences: {},
			tables: {},
			users: {}
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn remove_statement_index() -> Result<()> {
	let sql = "
		DEFINE INDEX uniq_isbn ON book FIELDS isbn UNIQUE;
		DEFINE INDEX idx_author ON book FIELDS author;
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX ft_title ON book FIELDS title SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		CREATE book:1 SET title = 'Rust Web Programming', isbn = '978-1803234694', author = 'Maxwell Flitton';
		REMOVE INDEX uniq_isbn ON book;
		REMOVE INDEX idx_author ON book;
		REMOVE INDEX ft_title ON book;
		INFO FOR TABLE book;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	for _ in 0..8 {
		let tmp = res.remove(0).result;
		tmp.unwrap();
	}
	// Check infos output
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"{
			events: {},
			fields: {},
			indexes: {},
			tables: {},
			lives: {},
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_table_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE TABLE IF EXISTS foo;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE TABLE IF EXISTS foo;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_analyzer_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE ANALYZER IF EXISTS foo;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE ANALYZER IF EXISTS foo;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_database_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE DATABASE IF EXISTS foo;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE DATABASE IF EXISTS foo;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_event_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE EVENT IF EXISTS foo ON bar;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE EVENT IF EXISTS foo ON bar;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_field_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE FIELD IF EXISTS foo ON bar;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE FIELD IF EXISTS foo ON bar;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_function_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE FUNCTION IF EXISTS fn::foo;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE FUNCTION IF EXISTS fn::foo;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_index_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE INDEX IF EXISTS foo ON bar;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE INDEX IF EXISTS foo ON bar;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_namespace_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE NAMESPACE IF EXISTS foo;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE NAMESPACE IF EXISTS foo;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_param_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE PARAM IF EXISTS $foo;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE PARAM IF EXISTS $foo;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_access_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE ACCESS IF EXISTS foo ON DB;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE ACCESS IF EXISTS foo ON DB;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

#[tokio::test]
async fn should_not_error_when_remove_user_if_exists() -> Result<()> {
	let sql = "
		USE NS test DB test;
		REMOVE USER IF EXISTS foo ON ROOT;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	// USE NS test DB test;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	// REMOVE USER IF EXISTS foo ON ROOT;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);

	Ok(())
}

//
// Permissions
//

fn level_root() -> Level {
	Level::Root
}
fn level_ns() -> Level {
	Level::Namespace("NS".to_owned())
}
fn level_db() -> Level {
	Level::Database("NS".to_owned(), "DB".to_owned())
}

#[tokio::test]
async fn permissions_checks_remove_ns() {
	let scenario = HashMap::from([
		("prepare", "DEFINE NAMESPACE NS"),
		("test", "REMOVE NAMESPACE NS"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, namespaces: {  }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
	];

	let check_anonymous_error = check_error.replace("{{NS}}", "NS");

	let res = iam_check_cases_impl(
		test_cases.iter(),
		&scenario,
		&check_success,
		&check_anonymous_error,
		false,
		false,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_db() {
	let scenario = HashMap::from([
		("prepare", "DEFINE DATABASE DB"),
		("test", "REMOVE DATABASE DB"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, databases: {  }, users: {  } }".to_string();
	let check_error =
		"{ accesses: {  }, databases: { DB: 'DEFINE DATABASE DB' }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let check_anonymous_error = check_error.clone();
	let res = iam_check_cases_impl(
		test_cases.iter(),
		&scenario,
		&check_success,
		&check_anonymous_error,
		true,
		false,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_function() {
	let scenario = HashMap::from([
		("prepare", "DEFINE FUNCTION fn::greet() {RETURN \"Hello\";}"),
		("test", "REMOVE FUNCTION fn::greet()"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: { greet: \"DEFINE FUNCTION fn::greet() { RETURN 'Hello' } PERMISSIONS FULL\" }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_analyzer() {
	let scenario = HashMap::from([
		("prepare", "DEFINE ANALYZER analyzer TOKENIZERS BLANK"),
		("test", "REMOVE ANALYZER analyzer"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, analyzers: { analyzer: 'DEFINE ANALYZER analyzer TOKENIZERS BLANK' }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_root_access() {
	let scenario = HashMap::from([
		("prepare", "DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("test", "REMOVE ACCESS access ON ROOT"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, namespaces: { NS: 'DEFINE NAMESPACE NS' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }".to_string();
	let check_error = r#"{ accesses: { access: "DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }"#.to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
	];

	let check_anonymous_error = check_error.replace("{{NS}}", "NS");

	let res =
		iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_anonymous_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_ns_access() {
	let scenario = HashMap::from([
		("prepare", "DEFINE ACCESS access ON NS TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("test", "REMOVE ACCESS access ON NS"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success =
		"{ accesses: {  }, databases: { {{DB}}: 'DEFINE DATABASE {{DB}}' }, users: {  } }"
			.to_string();
	let check_error = "{ accesses: { access: \"DEFINE ACCESS access ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, databases: { {{DB}}: 'DEFINE DATABASE {{DB}}' }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Owner),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
	];

	let check_anonymous_success = check_success.replace("{{DB}}", "DB");
	let check_anonymous_error = check_error.replace("{{DB}}", "DB");

	let res = iam_check_cases(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_anonymous_error,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_db_access() {
	let scenario = HashMap::from([
		("prepare", "DEFINE ACCESS access ON DB TYPE JWT ALGORITHM HS512 KEY 'secret'"),
		("test", "REMOVE ACCESS access ON DB"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: { access: \"DEFINE ACCESS access ON DATABASE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE\" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_root_user() {
	let scenario = HashMap::from([
		("prepare", "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER"),
		("test", "REMOVE USER user ON ROOT"),
		("check", "INFO FOR ROOT"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }".to_string();
	let check_error = r#"{ accesses: {  }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: { user: "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" } }"#.to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{NS}}", "NS")),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{NS}}", "NS"),
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{NS}}", "OTHER_NS"),
		),
	];

	let check_anonymous_success = check_success.replace("{{NS}}", "NS");
	let check_anonymous_error = check_error.replace("{{NS}}", "NS");

	let res = iam_check_cases(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_anonymous_error,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_ns_user() {
	let scenario = HashMap::from([
		("prepare", "DEFINE USER user ON NS PASSHASH 'secret' ROLES VIEWER"),
		("test", "REMOVE USER user ON NS"),
		("check", "INFO FOR NS"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success =
		"{ accesses: {  }, databases: { {{DB}}: 'DEFINE DATABASE {{DB}}' }, users: {  } }"
			.to_string();
	let check_error = "{ accesses: {  }, databases: { {{DB}}: 'DEFINE DATABASE {{DB}}' }, users: { user: \"DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE\" } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.replace("{{DB}}", "DB")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Owner),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check_error.replace("{{DB}}", "OTHER_DB"),
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check_error.replace("{{DB}}", "DB"),
		),
	];

	let check_anonymous_success = check_success.replace("{{DB}}", "DB");
	let check_anonymous_error = check_error.replace("{{DB}}", "DB");

	let res = iam_check_cases(
		test_cases.iter(),
		&scenario,
		&check_anonymous_success,
		&check_anonymous_error,
	)
	.await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_db_user() {
	let scenario = HashMap::from([
		("prepare", "DEFINE USER user ON DB PASSHASH 'secret' ROLES VIEWER"),
		("test", "REMOVE USER user ON DB"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: { user: \"DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE\" } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_param() {
	let scenario = HashMap::from([
		("prepare", "DEFINE PARAM $param VALUE 'foo'"),
		("test", "REMOVE PARAM $param"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: { param: \"DEFINE PARAM $param VALUE 'foo' PERMISSIONS FULL\" }, sequences: {  }, tables: {  }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_table() {
	let scenario = HashMap::from([
		("prepare", "DEFINE TABLE TB"),
		("test", "REMOVE TABLE TB"),
		("check", "INFO FOR DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }".to_string();
	let check_error = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { TB: 'DEFINE TABLE TB TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_event() {
	let scenario = HashMap::from([
		("prepare", "DEFINE EVENT event ON TABLE TB WHEN true THEN RETURN 'foo'"),
		("test", "REMOVE EVENT event ON TABLE TB"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success =
		"{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }".to_string();
	let check_error = "{ events: { event: \"DEFINE EVENT event ON TB WHEN true THEN RETURN 'foo'\" }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_field() {
	let scenario = HashMap::from([
		("prepare", "DEFINE FIELD field ON TABLE TB"),
		("test", "REMOVE FIELD field ON TABLE TB"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success =
		"{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }".to_string();
	let check_error = "{ events: {  }, fields: { field: 'DEFINE FIELD field ON TB PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_remove_index() {
	let scenario = HashMap::from([
		("prepare", "DEFINE INDEX index ON TABLE TB FIELDS field"),
		("test", "REMOVE INDEX index ON TABLE TB"),
		("check", "INFO FOR TABLE TB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check_success =
		"{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }".to_string();
	let check_error = "{ events: {  }, fields: {  }, indexes: { index: 'DEFINE INDEX index ON TB FIELDS field' }, lives: {  }, tables: {  } }".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check_success.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check_error.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check_error.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check_success, &check_error).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}
