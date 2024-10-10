mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use regex::Regex;
use surrealdb::dbs::Session;
use surrealdb::iam::Role;
use surrealdb::sql::Value;

#[tokio::test]
async fn access_bearer_database() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let sql = "
		-- Initial setup
		DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER;
		DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
		INFO FOR DB;
		-- Should succeed
		ACCESS api ON DATABASE GRANT FOR USER tobie;
		ACCESS api ON DATABASE GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api ON DATABASE SHOW;
		ACCESS api SHOW;
		-- Should fail
		ACCESS invalid ON DATABASE GRANT FOR USER tobie;
		ACCESS invalid GRANT FOR USER tobie;
		ACCESS api ON DATABASE GRANT FOR USER invalid;
		ACCESS api GRANT FOR USER invalid;
		ACCESS invalid ON DATABASE SHOW;
		ACCESS invalid SHOW;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 15);
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	// Ensure the access method was created as expected
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ accesses: \{ api: 'DEFINE ACCESS api ON DATABASE TYPE BEARER DURATION FOR GRANT NONE, FOR TOKEN 1h, FOR SESSION NONE' \}, .* \}")
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
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}\]",
	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}\]",
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
		DEFINE ACCESS api ON NAMESPACE TYPE BEARER FOR USER;
		DEFINE USER tobie ON NAMESPACE PASSWORD 'secret' ROLES EDITOR;
		INFO FOR NS;
		-- Should succeed
		ACCESS api ON NAMESPACE GRANT FOR USER tobie;
		ACCESS api ON NAMESPACE GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api ON NAMESPACE SHOW;
		ACCESS api SHOW;
		-- Should fail
		ACCESS invalid ON NAMESPACE GRANT FOR USER tobie;
		ACCESS invalid GRANT FOR USER tobie;
		ACCESS api ON NAMESPACE GRANT FOR USER invalid;
		ACCESS api GRANT FOR USER invalid;
		ACCESS invalid ON NAMESPACE SHOW;
		ACCESS invalid SHOW;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("test");
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 15);
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	// Ensure the access method was created as expected
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ accesses: \{ api: 'DEFINE ACCESS api ON NAMESPACE TYPE BEARER DURATION FOR GRANT NONE, FOR TOKEN 1h, FOR SESSION NONE' \}, .* \}")
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
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}\]",
	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}\]",
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

// TODO(PR): Merge tests for each level into one.
#[tokio::test]
async fn access_bearer_root() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let sql = "
		-- Initial setup
		DEFINE ACCESS api ON ROOT TYPE BEARER FOR USER;
		DEFINE USER tobie ON ROOT PASSWORD 'secret' ROLES EDITOR;
		INFO FOR ROOT;
		-- Should succeed
		ACCESS api ON ROOT GRANT FOR USER tobie;
		ACCESS api ON ROOT GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api GRANT FOR USER tobie;
		ACCESS api ON ROOT SHOW;
		ACCESS api LIST;
		-- Should fail
		ACCESS invalid ON ROOT GRANT FOR USER tobie;
		ACCESS invalid GRANT FOR USER tobie;
		ACCESS api ON ROOT GRANT FOR USER invalid;
		ACCESS api GRANT FOR USER invalid;
		ACCESS invalid ON ROOT SHOW;
		ACCESS invalid SHOW;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner();
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 15);
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	// Ensure the access method was created as expected
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok =
		Regex::new(r"\{ accesses: \{ api: 'DEFINE ACCESS api ON ROOT TYPE BEARER DURATION FOR GRANT NONE, FOR TOKEN 1h, FOR SESSION NONE' \}, .* \}")
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
	let ok =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}\]",

	)
	.unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	//
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(
		r"\[\{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}, \{ ac: 'api', .*, grant: \{ id: .*, key: '\[REDACTED\]' \}, .* \}\]",
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

#[tokio::test]
async fn access_bearer_revoke_db() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let sql = "
		-- Initial setup
		DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER;
		DEFINE USER tobie ON DATABASE PASSWORD 'secret' ROLES EDITOR;
		ACCESS api ON DATABASE GRANT FOR USER tobie;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	// Retrieve the generated bearer key
	let tmp = res.remove(0).result.unwrap().to_string();
	let re =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ id: '(.*)', key: .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	let kid = re.captures(&tmp).unwrap().get(1).unwrap().as_str();
	// Revoke bearer key
	let res =
		&mut dbs.execute(&format!("ACCESS api REVOKE GRANT {kid}"), &ses, None).await.unwrap();
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(r"\{ ac: 'api', .*, revocation: d'.*', .* \}").unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	// Attempt to revoke bearer key again
	let res = &mut dbs.execute(&format!("ACCESS api REVOKE {kid}"), &ses, None).await.unwrap();
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "This access grant has been revoked");
}

#[tokio::test]
async fn access_bearer_revoke_ns() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let sql = "
		-- Initial setup
		DEFINE ACCESS api ON NAMESPACE TYPE BEARER FOR USER;
		DEFINE USER tobie ON NAMESPACE PASSWORD 'secret' ROLES EDITOR;
		ACCESS api ON NAMESPACE GRANT FOR USER tobie;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner().with_ns("test");
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	// Retrieve the generated bearer key
	let tmp = res.remove(0).result.unwrap().to_string();
	let re =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ id: '(.*)', key: .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	let kid = re.captures(&tmp).unwrap().get(1).unwrap().as_str();
	// Revoke bearer key
	let res =
		&mut dbs.execute(&format!("ACCESS api REVOKE GRANT {kid}"), &ses, None).await.unwrap();
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(r"\{ ac: 'api', .*, revocation: d'.*', .* \}").unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	// Attempt to revoke bearer key again
	let res =
		&mut dbs.execute(&format!("ACCESS api REVOKE GRANT {kid}"), &ses, None).await.unwrap();
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "This access grant has been revoked");
}

#[tokio::test]
async fn access_bearer_revoke_root() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let sql = "
		-- Initial setup
		DEFINE ACCESS api ON ROOT TYPE BEARER FOR USER;
		DEFINE USER tobie ON ROOT PASSWORD 'secret' ROLES EDITOR;
		ACCESS api ON ROOT GRANT FOR USER tobie;
	";
	let dbs = new_ds().await.unwrap();
	let ses = Session::owner();
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	// Consume the results of the setup statements
	res.remove(0).result.unwrap();
	res.remove(0).result.unwrap();
	// Retrieve the generated bearer key
	let tmp = res.remove(0).result.unwrap().to_string();
	let re =
		Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ id: '(.*)', key: .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
				.unwrap();
	let kid = re.captures(&tmp).unwrap().get(1).unwrap().as_str();
	// Revoke bearer key
	let res =
		&mut dbs.execute(&format!("ACCESS api REVOKE GRANT {kid}"), &ses, None).await.unwrap();
	let tmp = res.remove(0).result.unwrap().to_string();
	let ok = Regex::new(r"\{ ac: 'api', .*, revocation: d'.*', .* \}").unwrap();
	assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	// Attempt to revoke bearer key again
	let res = &mut dbs.execute(&format!("ACCESS api REVOKE {kid}"), &ses, None).await.unwrap();
	let tmp = res.remove(0).result.unwrap_err();
	assert_eq!(tmp.to_string(), "This access grant has been revoked");
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
			"DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER; DEFINE USER tobie ON DATABASE ROLES OWNER";

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(true);

			let mut resp = ds.execute(statement_setup, &sess_setup, None).await.unwrap();
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
		let statement_setup = "DEFINE ACCESS api ON NAMESPACE TYPE BEARER FOR USER; DEFINE USER tobie ON NAMESPACE ROLES OWNER";

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(true);

			let mut resp = ds.execute(statement_setup, &sess_setup, None).await.unwrap();
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
			"DEFINE ACCESS api ON ROOT TYPE BEARER FOR USER; DEFINE USER tobie ON ROOT ROLES OWNER";

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(true);

			let mut resp = ds.execute(statement_setup, &sess_setup, None).await.unwrap();
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
