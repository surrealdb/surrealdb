mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use regex::Regex;
use surrealdb::dbs::Session;
use surrealdb::iam::Role;
use surrealdb::sql::Value;

#[tokio::test]
async fn access_bearer_database() -> () {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let sql = "
		-- Initial setup
		DEFINE ACCESS api ON DATABASE TYPE BEARER;
		DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
		-- Should succeed
		ACCESS api ON DATABASE GRANT FOR USER tobie;
		ACCESS api ON DATABASE GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api ON DATABASE LIST;
		ACCESS api LIST;
		-- Should fail
		ACCESS invalid ON DATABASE GRANT FOR USER tobie;
		ACCESS invalid GRANT FOR USER tobie;
		ACCESS api ON DATABASE GRANT FOR USER invalid;
		ACCESS api GRANT FOR USER invalid;
		ACCESS invalid ON DATABASE LIST;
		ACCESS invalid LIST;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 14);
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}\]",
	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}\]",
	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(
		tmp.to_string(),
		"The access method 'invalid' does not exist in the database 'test'"
	);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(
		tmp.to_string(),
		"The access method 'invalid' does not exist in the database 'test'"
	);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The user 'invalid' does not exist in the database 'test'");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The user 'invalid' does not exist in the database 'test'");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(
		tmp.to_string(),
		"The access method 'invalid' does not exist in the database 'test'"
	);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(
		tmp.to_string(),
		"The access method 'invalid' does not exist in the database 'test'"
	);
}

#[tokio::test]
async fn access_bearer_namespace() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let sql = "
		-- Initial setup
		DEFINE ACCESS api ON NAMESPACE TYPE BEARER;
		DEFINE USER tobie ON NAMESPACE PASSWORD 'secret' ROLES EDITOR;
		-- Should succeed
		ACCESS api ON NAMESPACE GRANT FOR USER tobie;
		ACCESS api ON NAMESPACE GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api ON NAMESPACE LIST;
		ACCESS api LIST;
		-- Should fail
		ACCESS invalid ON NAMESPACE GRANT FOR USER tobie;
		ACCESS invalid GRANT FOR USER tobie;
		ACCESS api ON NAMESPACE GRANT FOR USER invalid;
		ACCESS api GRANT FOR USER invalid;
		ACCESS invalid ON NAMESPACE LIST;
		ACCESS invalid LIST;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("test");
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 14);
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}\]",
	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}\]",
	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(
		tmp.to_string(),
		"The access method 'invalid' does not exist in the namespace 'test'"
	);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(
		tmp.to_string(),
		"The access method 'invalid' does not exist in the namespace 'test'"
	);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The user 'invalid' does not exist in the namespace 'test'");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The user 'invalid' does not exist in the namespace 'test'");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(
		tmp.to_string(),
		"The access method 'invalid' does not exist in the namespace 'test'"
	);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(
		tmp.to_string(),
		"The access method 'invalid' does not exist in the namespace 'test'"
	);
}

#[tokio::test]
async fn access_bearer_root() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let sql = "
		-- Initial setup
		DEFINE ACCESS api ON ROOT TYPE BEARER;
		DEFINE USER tobie ON ROOT PASSWORD 'secret' ROLES EDITOR;
		-- Should succeed
		ACCESS api ON ROOT GRANT FOR USER tobie;
		ACCESS api ON ROOT GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api ON ROOT LIST;
		ACCESS api LIST;
		-- Should fail
		ACCESS invalid ON ROOT GRANT FOR USER tobie;
		ACCESS invalid GRANT FOR USER tobie;
		ACCESS api ON ROOT GRANT FOR USER invalid;
		ACCESS api GRANT FOR USER invalid;
		ACCESS invalid ON ROOT LIST;
		ACCESS invalid LIST;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner();
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 14);
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}\]",
	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}, \{ ac: 'api', .* \}\]",
	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The root access method 'invalid' does not exist");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The root access method 'invalid' does not exist");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The root user 'invalid' does not exist");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The root user 'invalid' does not exist");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The root access method 'invalid' does not exist");
	//
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "The root access method 'invalid' does not exist");
}

//
// Permissions
//

#[tokio::test]
async fn permissions_access_grant_db() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let tests = vec![
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to issue a grant"),
		((().into(), Role::Editor), ("NS", "DB"), false, "editor at root level should not be able to issue a grant"),
		((().into(), Role::Viewer), ("NS", "DB"), false, "viewer at root level should not be able to issue a grant"),

		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to issue a grant on another namespace"),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false, "editor at namespace level should not be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to issue a grant on another namespace"),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false, "viewer at namespace level should not be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to issue a grant on another namespace"),

		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true, "owner at database level should be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to issue a grant on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false, "editor at database level should not be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to issue a grant on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false, "viewer at database level should not be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to issue a grant on another namespace even if the database name matches"),
	];
	let statement = "ACCESS api ON DATABASE GRANT FOR USER tobie";

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		let sess_setup =
			Session::for_level(("NS", "DB").into(), Role::Owner).with_ns("NS").with_db("DB");
		let statement_setup =
			"DEFINE ACCESS api ON DATABASE TYPE BEARER; DEFINE USER tobie ON DATABASE ROLES OWNER";

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(true);

			let mut resp = ds.execute(&statement_setup, &sess_setup, None).await.unwrap();
			let res = resp.remove(0).output();
			assert!(res.is_ok(), "Error setting up access method: {:?}", res);
			let res = resp.remove(0).output();
			assert!(res.is_ok(), "Error setting up user: {:?}", res);

			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok(), "{}: {:?}", msg, res);
				assert_ne!(res.unwrap(), Value::parse("[]"), "{}", msg);
			} else {
				let err = res.unwrap_err().to_string();
				assert!(
					err.contains("Not enough permissions to perform this action"),
					"{}: {}",
					msg,
					err
				)
			}
		}
	}
}

#[tokio::test]
async fn permissions_access_grant_ns() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let tests = vec![
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to issue a grant"),
		((().into(), Role::Editor), ("NS", "DB"), false, "editor at root level should not be able to issue a grant"),
		((().into(), Role::Viewer), ("NS", "DB"), false, "viewer at root level should not be able to issue a grant"),

		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to issue a grant on another namespace"),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false, "editor at namespace level should not be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to issue a grant on another namespace"),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false, "viewer at namespace level should not be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to issue a grant on another namespace"),

		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false, "owner at database level should not be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to issue a grant on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false, "editor at database level should not be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to issue a grant on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false, "viewer at database level should not be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to issue a grant on another namespace even if the database name matches"),
	];
	let statement = "ACCESS api ON NAMESPACE GRANT FOR USER tobie";

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		let sess_setup =
			Session::for_level(("NS",).into(), Role::Owner).with_ns("NS").with_db("DB");
		let statement_setup = "DEFINE ACCESS api ON NAMESPACE TYPE BEARER; DEFINE USER tobie ON NAMESPACE ROLES OWNER";

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(true);

			let mut resp = ds.execute(&statement_setup, &sess_setup, None).await.unwrap();
			let res = resp.remove(0).output();
			assert!(res.is_ok(), "Error setting up access method: {:?}", res);
			let res = resp.remove(0).output();
			assert!(res.is_ok(), "Error setting up user: {:?}", res);

			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok(), "{}: {:?}", msg, res);
				assert_ne!(res.unwrap(), Value::parse("[]"), "{}", msg);
			} else {
				let err = res.unwrap_err().to_string();
				assert!(
					err.contains("Not enough permissions to perform this action"),
					"{}: {}",
					msg,
					err
				)
			}
		}
	}
}

#[tokio::test]
async fn permissions_access_grant_root() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let tests = vec![
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to issue a grant"),
		((().into(), Role::Editor), ("NS", "DB"), false, "editor at root level should not be able to issue a grant"),
		((().into(), Role::Viewer), ("NS", "DB"), false, "viewer at root level should not be able to issue a grant"),

		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), false, "owner at namespace level should not be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to issue a grant on another namespace"),
		((("NS",).into(), Role::Editor), ("NS", "DB"), false, "editor at namespace level should not be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to issue a grant on another namespace"),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false, "viewer at namespace level should not be able to issue a grant on its namespace"),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to issue a grant on another namespace"),

		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false, "owner at database level should not be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to issue a grant on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false, "editor at database level should not be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to issue a grant on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false, "viewer at database level should not be able to issue a grant on its database"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to issue a grant on another database"),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to issue a grant on another namespace even if the database name matches"),
	];
	let statement = "ACCESS api ON ROOT GRANT FOR USER tobie";

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		let sess_setup = Session::for_level(().into(), Role::Owner).with_ns("NS").with_db("DB");
		let statement_setup =
			"DEFINE ACCESS api ON ROOT TYPE BEARER; DEFINE USER tobie ON ROOT ROLES OWNER";

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(true);

			let mut resp = ds.execute(&statement_setup, &sess_setup, None).await.unwrap();
			let res = resp.remove(0).output();
			assert!(res.is_ok(), "Error setting up access method: {:?}", res);
			let res = resp.remove(0).output();
			assert!(res.is_ok(), "Error setting up user: {:?}", res);

			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok(), "{}: {:?}", msg, res);
				assert_ne!(res.unwrap(), Value::parse("[]"), "{}", msg);
			} else {
				let err = res.unwrap_err().to_string();
				assert!(
					err.contains("Not enough permissions to perform this action"),
					"{}: {}",
					msg,
					err
				)
			}
		}
	}
}
