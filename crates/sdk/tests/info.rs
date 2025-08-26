mod helpers;
use std::collections::HashMap;

use helpers::*;
use regex::Regex;
use surrealdb_core::dbs::capabilities::ExperimentalTarget;
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::iam::{Level, Role};

#[tokio::test]
async fn info_for_root() {
	let sql = r#"
        DEFINE NAMESPACE NS;
        DEFINE USER user ON ROOT PASSWORD 'pass';
        DEFINE ACCESS access ON ROOT TYPE JWT ALGORITHM HS512 KEY 'secret';
       	INFO FOR ROOT;
      	INFO FOR ROOT STRUCTURE;
    "#;
	let mut t = Test::new(sql).await.unwrap();
	t.skip_ok(3).unwrap();
	t.expect_regex(r"\{ accesses: \{ access: .* \}, namespaces: \{ NS: .* \}, nodes: \{ .* \}, system: \{ .* \}, users: \{ user: .* \} \}").unwrap();
	t.expect_regex(r"\{ accesses: \[\{.* \}\], namespaces: \[\{ .* \}\], nodes: \[.*\], system: \{ .* \}, users: \[\{ .* \}\] \}").unwrap();
}

#[tokio::test]
async fn info_for_ns() {
	let sql = r#"
        DEFINE DATABASE DB;
        DEFINE USER user ON NS PASSWORD 'pass';
        DEFINE ACCESS access ON NS TYPE JWT ALGORITHM HS512 KEY 'secret';
        INFO FOR NS
    "#;
	let mut t = Test::new(sql).await.unwrap();
	t.skip_ok(3).unwrap();
	t.expect_regex(
		r"\{ accesses: \{ access: .* \}, databases: \{ DB: .* \}, users: \{ user: .* \} \}",
	)
	.unwrap();
}

#[tokio::test]
async fn info_for_db() {
	let sql = r#"
        DEFINE TABLE TB;
        DEFINE ACCESS jwt ON DB TYPE JWT ALGORITHM HS512 KEY 'secret';
        DEFINE ACCESS record ON DB TYPE RECORD DURATION FOR TOKEN 30m, FOR SESSION 12h;
        DEFINE USER user ON DB PASSWORD 'pass';
        DEFINE FUNCTION fn::greet() {RETURN "Hello";};
        DEFINE PARAM $param VALUE "foo";
        DEFINE ANALYZER analyzer TOKENIZERS BLANK;
        INFO FOR DB
    "#;
	let mut t = Test::new(sql).await.unwrap();
	t.skip_ok(7).unwrap();
	t.expect_regex(
		r"\{ accesses: \{ jwt: .*, record: .* \}, analyzers: \{ analyzer: .* \}, functions: \{ greet: .* \}, params: \{ param: .* \}, tables: \{ TB: .* \}, users: \{ user: .* \} \}",
	)
		.unwrap();
}

#[tokio::test]
async fn info_for_table() {
	let sql = r#"
        DEFINE TABLE TB;
        DEFINE EVENT event ON TABLE TB WHEN true THEN RETURN "foo";
        DEFINE FIELD field ON TABLE TB;
        DEFINE INDEX index ON TABLE TB FIELDS field;
        INFO FOR TABLE TB;
    "#;
	let mut t = Test::new(sql).await.unwrap();
	t.skip_ok(4).unwrap();
	t.expect_regex(
		r"\{ events: \{ event: .* \}, fields: \{ field: .* \}, indexes: \{ index: .* \}, lives: \{  \}, tables: \{  \} \}",
	)
		.unwrap();
}

#[tokio::test]
async fn info_for_user() {
	let sql = r#"
        DEFINE USER user ON ROOT PASSWORD 'pass';
        DEFINE USER user ON NS PASSWORD 'pass';
        DEFINE USER user ON DB PASSWORD 'pass';
    "#;
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("ns").with_db("db");

	let res = dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 3);

	// Info for ROOT user
	let sql = "INFO FOR USER user ON ROOT";
	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);
	let output_regex = Regex::new(r"DEFINE USER user ON ROOT PASSHASH .* ROLES VIEWER").unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);

	// Info for NS user
	let sql = "INFO FOR USER user ON NS";
	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);
	let output_regex =
		Regex::new(r"DEFINE USER user ON NAMESPACE PASSHASH .* ROLES VIEWER").unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);

	// Info for DB user
	let sql = "INFO FOR USER user ON DB";
	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);
	let output_regex =
		Regex::new(r"DEFINE USER user ON DATABASE PASSHASH .* ROLES VIEWER").unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);

	// Info for user on selected level
	let sql = "INFO FOR USER user";
	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);
	let output_regex =
		Regex::new(r"DEFINE USER user ON DATABASE PASSHASH .* ROLES VIEWER").unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);
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
async fn permissions_checks_info_root() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "INFO FOR ROOT"), ("check", "INFO FOR ROOT")]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check = "{ accesses: {  }, namespaces: { {{NS}}: 'DEFINE NAMESPACE {{NS}}' }, nodes: {  }, system: { available_parallelism: 0, cpu_usage: 0.0f, load_average: [0.0f, 0.0f, 0.0f], memory_allocated: 0, memory_usage: 0, physical_cores: 0, threads: 0 }, users: {  } }";

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_root(), Role::Editor), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_root(), Role::Viewer), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), false, check.replace("{{NS}}", "NS")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check.replace("{{NS}}", "OTHER_NS")),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check.replace("{{NS}}", "OTHER_NS")),
		((level_db(), Role::Editor), ("NS", "DB"), false, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
	];

	let anonymous_result = check.replace("{{NS}}", "NS");

	let res =
		iam_check_cases(test_cases.iter(), &scenario, &anonymous_result, &anonymous_result).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_ns() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "INFO FOR NS"), ("check", "INFO FOR NS")]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check = "{ accesses: {  }, databases: { {{DB}}: 'DEFINE DATABASE {{DB}}' }, users: {  } }";

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check.replace("{{DB}}", "DB")),
		((level_root(), Role::Editor), ("NS", "DB"), true, check.replace("{{DB}}", "DB")),
		((level_root(), Role::Viewer), ("NS", "DB"), true, check.replace("{{DB}}", "DB")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check.replace("{{DB}}", "DB")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check.replace("{{DB}}", "DB")),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check.replace("{{DB}}", "DB")),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check.replace("{{DB}}", "DB")),
		((level_ns(), Role::Viewer), ("NS", "DB"), true, check.replace("{{DB}}", "DB")),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check.replace("{{DB}}", "DB")),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check.replace("{{DB}}", "DB")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check.replace("{{DB}}", "OTHER_DB")),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check.replace("{{DB}}", "DB")),
		((level_db(), Role::Editor), ("NS", "DB"), false, check.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			check.replace("{{DB}}", "OTHER_DB"),
		),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check.replace("{{DB}}", "DB")),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check.replace("{{DB}}", "DB")),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			check.replace("{{DB}}", "OTHER_DB"),
		),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check.replace("{{DB}}", "DB")),
	];

	let anonymous_result = check.replace("{{DB}}", "DB");

	let res =
		iam_check_cases(test_cases.iter(), &scenario, &anonymous_result, &anonymous_result).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_db() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "INFO FOR DB"), ("check", "INFO FOR DB")]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }";

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_root(), Role::Editor), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_root(), Role::Viewer), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check.replace("{{NS}}", "OTHER_NS")),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check.replace("{{NS}}", "OTHER_NS")),
		((level_db(), Role::Editor), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
	];

	let anonymous_result = check.replace("{{NS}}", "NS");

	let res =
		iam_check_cases(test_cases.iter(), &scenario, &anonymous_result, &anonymous_result).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_table() {
	let scenario = HashMap::from([
		("prepare", "DEFINE TABLE tb"),
		("test", "INFO FOR TABLE tb"),
		("check", "INFO FOR TABLE tb"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }";

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_root(), Role::Editor), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_root(), Role::Viewer), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check.replace("{{NS}}", "OTHER_NS")),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		((level_ns(), Role::Viewer), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check.replace("{{NS}}", "OTHER_NS")),
		((level_db(), Role::Editor), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
		((level_db(), Role::Viewer), ("NS", "DB"), true, check.replace("{{NS}}", "NS")),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check.replace("{{NS}}", "NS")),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			check.replace("{{NS}}", "OTHER_NS"),
		),
	];

	let anonymous_result = check.replace("{{NS}}", "NS");

	let res =
		iam_check_cases(test_cases.iter(), &scenario, &anonymous_result, &anonymous_result).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_user_root() {
	let scenario = HashMap::from([
		(
			"prepare",
			"DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h",
		),
		("test", "INFO FOR USER user ON ROOT"),
		("check", "INFO FOR USER user ON ROOT"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check = "\"DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h\"".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), true, check.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), false, check.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), false, check.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), false, check.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check, &check).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_user_ns() {
	let scenario = HashMap::from([
		(
			"prepare",
			"DEFINE USER user ON NS PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h",
		),
		("test", "INFO FOR USER user ON NS"),
		("check", "INFO FOR USER user ON NS"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check = "\"DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h\"".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), true, check.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), true, check.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), false, check.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), false, check.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), false, check.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check, &check).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_user_db() {
	let scenario = HashMap::from([
		(
			"prepare",
			"DEFINE USER user ON DB PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h",
		),
		("test", "INFO FOR USER user ON DB"),
		("check", "INFO FOR USER user ON DB"),
	]);

	// Define the expected results for the check statement when the test statement
	// succeeded and when it failed
	let check = "\"DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER DURATION FOR TOKEN 15m, FOR SESSION 6h\"".to_string();

	let test_cases = [
		// Root level
		((level_root(), Role::Owner), ("NS", "DB"), true, check.clone()),
		((level_root(), Role::Editor), ("NS", "DB"), true, check.clone()),
		((level_root(), Role::Viewer), ("NS", "DB"), true, check.clone()),
		// Namespace level
		((level_ns(), Role::Owner), ("NS", "DB"), true, check.clone()),
		((level_ns(), Role::Owner), ("OTHER_NS", "DB"), false, check.clone()),
		((level_ns(), Role::Editor), ("NS", "DB"), true, check.clone()),
		((level_ns(), Role::Editor), ("OTHER_NS", "DB"), false, check.clone()),
		((level_ns(), Role::Viewer), ("NS", "DB"), true, check.clone()),
		((level_ns(), Role::Viewer), ("OTHER_NS", "DB"), false, check.clone()),
		// Database level
		((level_db(), Role::Owner), ("NS", "DB"), true, check.clone()),
		((level_db(), Role::Owner), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Owner), ("OTHER_NS", "DB"), false, check.clone()),
		((level_db(), Role::Editor), ("NS", "DB"), true, check.clone()),
		((level_db(), Role::Editor), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Editor), ("OTHER_NS", "DB"), false, check.clone()),
		((level_db(), Role::Viewer), ("NS", "DB"), true, check.clone()),
		((level_db(), Role::Viewer), ("NS", "OTHER_DB"), false, check.clone()),
		((level_db(), Role::Viewer), ("OTHER_NS", "DB"), false, check.clone()),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, &check, &check).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn access_info_redacted() {
	// Symmetric
	{
		let sql = r#"
			DEFINE ACCESS access ON NS TYPE JWT ALGORITHM HS512 KEY 'secret' WITH ISSUER KEY 'secret';
			INFO FOR NS
		"#;
		let dbs = new_ds().await.unwrap().with_capabilities(
			Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
		);
		let ses = Session::owner().with_ns("ns");

		let mut res = dbs.execute(sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 2);

		let out = res.pop().unwrap().output();
		assert!(out.is_ok(), "Unexpected error: {:?}", out);

		let out_expected =
            r#"{ accesses: { access: "DEFINE ACCESS access ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, databases: {  }, users: {  } }"#.to_string();
		let out_str = out.unwrap().to_string();
		assert_eq!(
			out_str, out_expected,
			"Output '{out_str}' doesn't match expected output '{out_expected}'",
		);
	}
	// Asymmetric
	{
		let sql = r#"
			DEFINE ACCESS access ON NS TYPE JWT ALGORITHM PS512 KEY 'public' WITH ISSUER KEY 'private';
			INFO FOR NS
		"#;
		let dbs = new_ds().await.unwrap().with_capabilities(
			Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
		);
		let ses = Session::owner().with_ns("ns");

		let mut res = dbs.execute(sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 2);

		let out = res.pop().unwrap().output();
		assert!(out.is_ok(), "Unexpected error: {:?}", out);

		let out_expected =
            r#"{ accesses: { access: "DEFINE ACCESS access ON NAMESPACE TYPE JWT ALGORITHM PS512 KEY 'public' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, databases: {  }, users: {  } }"#.to_string();
		let out_str = out.unwrap().to_string();
		assert_eq!(
			out_str, out_expected,
			"Output '{out_str}' doesn't match expected output '{out_expected}'",
		);
	}
	// Record
	{
		let sql = r#"
			DEFINE ACCESS access ON DB TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'secret' WITH ISSUER KEY 'secret';
			INFO FOR DB
		"#;
		let dbs = new_ds().await.unwrap().with_capabilities(
			Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
		);
		let ses = Session::owner().with_ns("ns").with_db("test");

		let mut res = dbs.execute(sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 2);

		let out = res.pop().unwrap().output();
		assert!(out.is_ok(), "Unexpected error: {:?}", out);

		let out_expected =
            r#"{ accesses: { access: "DEFINE ACCESS access ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"#.to_string();
		let out_str = out.unwrap().to_string();
		assert_eq!(
			out_str, out_expected,
			"Output '{out_str}' doesn't match expected output '{out_expected}'",
		);
	}
	// Record with refresh token
	{
		let sql = r#"
			DEFINE ACCESS access ON DB TYPE RECORD WITH REFRESH, WITH JWT ALGORITHM HS512 KEY 'secret' WITH ISSUER KEY 'secret';
			INFO FOR DB
		"#;
		let dbs = new_ds().await.unwrap().with_capabilities(
			Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
		);
		let ses = Session::owner().with_ns("ns").with_db("test");

		let mut res = dbs.execute(sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 2);

		let out = res.pop().unwrap().output();
		assert!(out.is_ok(), "Unexpected error: {:?}", out);

		let out_expected =
			r#"{ accesses: { access: "DEFINE ACCESS access ON DATABASE TYPE RECORD WITH REFRESH WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"#.to_string();
		let out_str = out.unwrap().to_string();
		assert_eq!(
			out_str, out_expected,
			"Output '{out_str}' doesn't match expected output '{out_expected}'",
		);
	}
}

#[tokio::test]
async fn access_info_redacted_structure() {
	// Symmetric
	{
		let sql = r#"
			DEFINE ACCESS access ON NS TYPE JWT ALGORITHM HS512 KEY 'secret' DURATION FOR TOKEN 15m, FOR SESSION 6h;
			INFO FOR NS STRUCTURE
		"#;
		let dbs = new_ds().await.unwrap().with_capabilities(
			Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
		);
		let ses = Session::owner().with_ns("ns");

		let mut res = dbs.execute(sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 2);

		let out = res.pop().unwrap().output();
		assert!(out.is_ok(), "Unexpected error: {:?}", out);

		let out_expected =
            r#"{ accesses: [{ duration: { session: 6h, token: 15m }, kind: { jwt: { issuer: { alg: 'HS512', key: '[REDACTED]' }, verify: { alg: 'HS512', key: '[REDACTED]' } }, kind: 'JWT' }, name: 'access' }], databases: [], users: [] }"#.to_string();
		let out_str = out.unwrap().to_string();
		assert_eq!(
			out_str, out_expected,
			"Output '{out_str}' doesn't match expected output '{out_expected}'",
		);
	}
	// Asymmetric
	{
		let sql = r#"
			DEFINE ACCESS access ON NS TYPE JWT ALGORITHM PS512 KEY 'public' WITH ISSUER KEY 'private' DURATION FOR TOKEN 15m, FOR SESSION 6h;
			INFO FOR NS STRUCTURE
		"#;
		let dbs = new_ds().await.unwrap().with_capabilities(
			Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
		);
		let ses = Session::owner().with_ns("ns");

		let mut res = dbs.execute(sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 2);

		let out = res.pop().unwrap().output();
		assert!(out.is_ok(), "Unexpected error: {:?}", out);

		let out_expected =
            r#"{ accesses: [{ duration: { session: 6h, token: 15m }, kind: { jwt: { issuer: { alg: 'PS512', key: '[REDACTED]' }, verify: { alg: 'PS512', key: 'public' } }, kind: 'JWT' }, name: 'access' }], databases: [], users: [] }"#.to_string();
		let out_str = out.unwrap().to_string();
		assert_eq!(
			out_str, out_expected,
			"Output '{out_str}' doesn't match expected output '{out_expected}'",
		);
	}
	// Record
	{
		let sql = r#"
			DEFINE ACCESS access ON DB TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'secret' DURATION FOR TOKEN 15m, FOR SESSION 6h;
			INFO FOR DB STRUCTURE
		"#;
		let dbs = new_ds().await.unwrap().with_capabilities(
			Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
		);
		let ses = Session::owner().with_ns("ns").with_db("db");

		let mut res = dbs.execute(sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 2);

		let out = res.pop().unwrap().output();
		assert!(out.is_ok(), "Unexpected error: {:?}", out);

		let out_expected =
            r#"{ accesses: [{ duration: { session: 6h, token: 15m }, kind: { jwt: { issuer: { alg: 'HS512', key: '[REDACTED]' }, verify: { alg: 'HS512', key: '[REDACTED]' } }, kind: 'RECORD' }, name: 'access' }], analyzers: [], apis: [], buckets: [], configs: [], functions: [], models: [], params: [], sequences: [], tables: [], users: [] }"#.to_string();
		let out_str = out.unwrap().to_string();
		assert_eq!(
			out_str, out_expected,
			"Output '{out_str}' doesn't match expected output '{out_expected}'",
		);
	}
	// Record with refresh token
	{
		let sql = r#"
			DEFINE ACCESS access ON DB TYPE RECORD WITH REFRESH, WITH JWT ALGORITHM HS512 KEY 'secret' DURATION FOR GRANT 1w, FOR TOKEN 15m, FOR SESSION 6h;
			INFO FOR DB STRUCTURE
		"#;
		let dbs = new_ds().await.unwrap().with_capabilities(
			Capabilities::default().with_experimental(ExperimentalTarget::BearerAccess.into()),
		);
		let ses = Session::owner().with_ns("ns").with_db("db");

		let mut res = dbs.execute(sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 2);

		let out = res.pop().unwrap().output();
		assert!(out.is_ok(), "Unexpected error: {:?}", out);

		let out_expected =
			r#"{ accesses: [{ duration: { grant: 1w, session: 6h, token: 15m }, kind: { jwt: { issuer: { alg: 'HS512', key: '[REDACTED]' }, verify: { alg: 'HS512', key: '[REDACTED]' } }, kind: 'RECORD', refresh: true }, name: 'access' }], analyzers: [], apis: [], buckets: [], configs: [], functions: [], models: [], params: [], sequences: [], tables: [], users: [] }"#.to_string();
		let out_str = out.unwrap().to_string();
		assert_eq!(
			out_str, out_expected,
			"Output '{out_str}' doesn't match expected output '{out_expected}'",
		);
	}
}

#[tokio::test]
async fn function_info_structure() {
	let sql = r#"
        DEFINE FUNCTION fn::example($name: string) -> string { RETURN "Hello, " + $name + "!" };
        INFO FOR DB STRUCTURE;
    "#;
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("ns").with_db("db");

	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 2);

	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);

	let out_expected =
        r#"{ accesses: [], analyzers: [], apis: [], buckets: [], configs: [], functions: [{ args: [['name', 'string']], block: "{ RETURN 'Hello, ' + $name + '!' }", name: 'example', permissions: true, returns: 'string' }], models: [], params: [], sequences: [], tables: [], users: [] }"#.to_string();
	let out_str = out.unwrap().to_string();
	assert_eq!(
		out_str, out_expected,
		"Output '{out_str}' doesn't match expected output '{out_expected}'",
	);
}
