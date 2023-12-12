mod helpers;
use helpers::*;

use std::collections::HashMap;

use regex::Regex;
use surrealdb::dbs::Session;
use surrealdb::iam::Role;

#[tokio::test]
async fn info_for_root() {
	let sql = r#"
        DEFINE NAMESPACE NS;
        DEFINE USER user ON ROOT PASSWORD 'pass';
        INFO FOR ROOT
    "#;
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner();

	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 3);

	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);

	let output_regex =
		Regex::new(r"\{ namespaces: \{ NS: .* \}, users: \{ user: .* \} \}").unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);
}

#[tokio::test]
async fn info_for_ns() {
	let sql = r#"
        DEFINE DATABASE DB;
        DEFINE USER user ON NS PASSWORD 'pass';
        DEFINE TOKEN token ON NS TYPE HS512 VALUE 'secret';
        INFO FOR NS
    "#;
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("ns");

	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 4);

	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);

	let output_regex = Regex::new(
		r"\{ databases: \{ DB: .* \}, tokens: \{ token: .* \}, users: \{ user: .* \} \}",
	)
	.unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);
}

#[tokio::test]
async fn info_for_db() {
	let sql = r#"
        DEFINE TABLE TB;
        DEFINE SCOPE account SESSION 24h;
        DEFINE USER user ON DB PASSWORD 'pass';
        DEFINE TOKEN token ON DB TYPE HS512 VALUE 'secret';
        DEFINE FUNCTION fn::greet() {RETURN "Hello";};
        DEFINE PARAM $param VALUE "foo";
        DEFINE ANALYZER analyzer TOKENIZERS BLANK;
        INFO FOR DB
    "#;
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("ns").with_db("db");

	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 8);

	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);

	let output_regex = Regex::new(r"\{ analyzers: \{ analyzer: .* \}, functions: \{ greet: .* \}, params: \{ param: .* \}, scopes: \{ account: .* \}, tables: \{ TB: .* \}, tokens: \{ token: .* \}, users: \{ user: .* \} \}").unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);
}

#[tokio::test]
async fn info_for_scope() {
	let sql = r#"
        DEFINE SCOPE account SESSION 24h;
        DEFINE TOKEN token ON SCOPE account TYPE HS512 VALUE 'secret';
        INFO FOR SCOPE account;
    "#;
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("ns").with_db("db");

	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 3);

	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);

	let output_regex = Regex::new(r"\{ tokens: \{ token: .* \} \}").unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);
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
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("ns").with_db("db");

	let mut res = dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 5);

	let out = res.pop().unwrap().output();
	assert!(out.is_ok(), "Unexpected error: {:?}", out);

	let output_regex = Regex::new(
		r"\{ events: \{ event: .* \}, fields: \{ field: .* \}, indexes: \{ index: .* \}, lives: \{  \}, tables: \{  \} \}",
	)
	.unwrap();
	let out_str = out.unwrap().to_string();
	assert!(
		output_regex.is_match(&out_str),
		"Output '{}' doesn't match regex '{}'",
		out_str,
		output_regex
	);
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

#[tokio::test]
async fn permissions_checks_info_root() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "INFO FOR ROOT"), ("check", "INFO FOR ROOT")]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results =
		[vec!["{ namespaces: {  }, users: {  } }"], vec!["{ namespaces: {  }, users: {  } }"]];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), true),
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
async fn permissions_checks_info_ns() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "INFO FOR NS"), ("check", "INFO FOR NS")]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
		vec!["{ databases: {  }, tokens: {  }, users: {  } }"],
		vec!["{ databases: {  }, tokens: {  }, users: {  } }"],
	];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), true),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), true),
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
async fn permissions_checks_info_db() {
	let scenario =
		HashMap::from([("prepare", ""), ("test", "INFO FOR DB"), ("check", "INFO FOR DB")]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
        vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"],
        vec!["{ analyzers: {  }, functions: {  }, models: {  }, params: {  }, scopes: {  }, tables: {  }, tokens: {  }, users: {  } }"],
    ];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), true),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), true),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_scope() {
	let scenario = HashMap::from([
		("prepare", "DEFINE SCOPE scope SESSION 1h"),
		("test", "INFO FOR SCOPE scope"),
		("check", "INFO FOR SCOPE scope"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [vec!["{ tokens: {  } }"], vec!["{ tokens: {  } }"]];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), true),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), true),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_table() {
	let scenario = HashMap::from([
		("prepare", "DEFINE TABLE tb"),
		("test", "INFO FOR TABLE tb"),
		("check", "INFO FOR TABLE tb"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
		vec!["{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"],
		vec!["{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"],
	];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), true),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), true),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}

#[tokio::test]
async fn permissions_checks_info_user_root() {
	let scenario = HashMap::from([
		("prepare", "DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER"),
		("test", "INFO FOR USER user ON ROOT"),
		("check", "INFO FOR USER user ON ROOT"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
		vec!["\"DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER\""],
		vec!["\"DEFINE USER user ON ROOT PASSHASH 'secret' ROLES VIEWER\""],
	];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), true),
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
async fn permissions_checks_info_user_ns() {
	let scenario = HashMap::from([
		("prepare", "DEFINE USER user ON NS PASSHASH 'secret' ROLES VIEWER"),
		("test", "INFO FOR USER user ON NS"),
		("check", "INFO FOR USER user ON NS"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
		vec!["\"DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER\""],
		vec!["\"DEFINE USER user ON NAMESPACE PASSHASH 'secret' ROLES VIEWER\""],
	];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), true),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), true),
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
async fn permissions_checks_info_user_db() {
	let scenario = HashMap::from([
		("prepare", "DEFINE USER user ON DB PASSHASH 'secret' ROLES VIEWER"),
		("test", "INFO FOR USER user ON DB"),
		("check", "INFO FOR USER user ON DB"),
	]);

	// Define the expected results for the check statement when the test statement succeeded and when it failed
	let check_results = [
		vec!["\"DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER\""],
		vec!["\"DEFINE USER user ON DATABASE PASSHASH 'secret' ROLES VIEWER\""],
	];

	let test_cases = [
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true),
		((().into(), Role::Editor), ("NS", "DB"), true),
		((().into(), Role::Viewer), ("NS", "DB"), true),
		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), true),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false),
		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), true),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false),
	];

	let res = iam_check_cases(test_cases.iter(), &scenario, check_results).await;
	assert!(res.is_ok(), "{}", res.unwrap_err());
}
