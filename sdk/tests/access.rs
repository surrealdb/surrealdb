mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use regex::Regex;
use surrealdb::dbs::Session;
use surrealdb::iam::Role;
use surrealdb::sql::{Base, Value};

struct TestLevel {
	base: Base,
	ns: Option<&'static str>,
	db: Option<&'static str>,
}

#[tokio::test]
async fn access_bearer_operations() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let test_levels = vec![
		TestLevel {
			base: Base::Root,
			ns: None,
			db: None,
		},
		TestLevel {
			base: Base::Ns,
			ns: Some("test"),
			db: None,
		},
		TestLevel {
			base: Base::Db,
			ns: Some("test"),
			db: Some("test"),
		},
	];

	for level in &test_levels {
		let base = level.base.to_string();
		let sql = format!(
			"
			-- Initial setup
			DEFINE ACCESS api ON {base} TYPE BEARER FOR USER;
			DEFINE USER tobie ON {base} PASSWORD 'secret' ROLES EDITOR;
			INFO FOR {base};
			-- Should succeed
			ACCESS api ON {base} GRANT FOR USER tobie;
			ACCESS api ON {base} GRANT FOR USER tobie;
			ACCESS api GRANT FOR USER tobie;
			ACCESS api GRANT FOR USER tobie;
			ACCESS api ON {base} SHOW;
			ACCESS api SHOW;
			-- Should fail
			ACCESS invalid ON {base} GRANT FOR USER tobie;
			ACCESS invalid GRANT FOR USER tobie;
			ACCESS api ON {base} GRANT FOR USER invalid;
			ACCESS api GRANT FOR USER invalid;
			ACCESS invalid ON {base} SHOW;
			ACCESS invalid SHOW;
		"
		);
		let dbs = new_ds().await.unwrap();
		let ses = match level.base {
			Base::Root => Session::owner(),
			Base::Ns => Session::owner().with_ns(level.ns.unwrap()),
			Base::Db => Session::owner().with_ns(level.ns.unwrap()).with_db(level.db.unwrap()),
			_ => panic!("Invalid base"),
		};
		let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
		assert_eq!(res.len(), 15);
		// Consume the results of the setup statements
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		// Ensure the access method was created as expected
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok =
			Regex::new(&format!(
				r"\{{ accesses: \{{ api: 'DEFINE ACCESS api ON {base} TYPE BEARER DURATION FOR GRANT NONE, FOR TOKEN 1h, FOR SESSION NONE' \}}, .* \}}"
			)).unwrap();
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
		let expected = if matches!(level.base, Base::Root) {
			"The root access method 'invalid' does not exist".to_string()
		} else {
			format!(
				"The access method 'invalid' does not exist in the {} 'test'",
				level.base.to_string().to_lowercase()
			)
		};
		assert_eq!(tmp.to_string(), expected);
		//
		let tmp = res.remove(0).result.unwrap_err();
		let expected = if matches!(level.base, Base::Root) {
			"The root access method 'invalid' does not exist".to_string()
		} else {
			format!(
				"The access method 'invalid' does not exist in the {} 'test'",
				level.base.to_string().to_lowercase()
			)
		};
		assert_eq!(tmp.to_string(), expected);
		//
		let tmp = res.remove(0).result.unwrap_err();
		let expected = if matches!(level.base, Base::Root) {
			"The root user 'invalid' does not exist".to_string()
		} else {
			format!(
				"The user 'invalid' does not exist in the {} 'test'",
				level.base.to_string().to_lowercase()
			)
		};
		assert_eq!(tmp.to_string(), expected);
		//
		let tmp = res.remove(0).result.unwrap_err();
		let expected = if matches!(level.base, Base::Root) {
			"The root user 'invalid' does not exist".to_string()
		} else {
			format!(
				"The user 'invalid' does not exist in the {} 'test'",
				level.base.to_string().to_lowercase()
			)
		};
		assert_eq!(tmp.to_string(), expected);
		//
		let tmp = res.remove(0).result.unwrap_err();
		let expected = if matches!(level.base, Base::Root) {
			"The root access method 'invalid' does not exist".to_string()
		} else {
			format!(
				"The access method 'invalid' does not exist in the {} 'test'",
				level.base.to_string().to_lowercase()
			)
		};
		assert_eq!(tmp.to_string(), expected);
		//
		let tmp = res.remove(0).result.unwrap_err();
		let expected = if matches!(level.base, Base::Root) {
			"The root access method 'invalid' does not exist".to_string()
		} else {
			format!(
				"The access method 'invalid' does not exist in the {} 'test'",
				level.base.to_string().to_lowercase()
			)
		};
		assert_eq!(tmp.to_string(), expected);
	}
}

#[tokio::test]
async fn access_bearer_revoke() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let test_levels = vec![
		TestLevel {
			base: Base::Root,
			ns: None,
			db: None,
		},
		TestLevel {
			base: Base::Ns,
			ns: Some("test"),
			db: None,
		},
		TestLevel {
			base: Base::Db,
			ns: Some("test"),
			db: Some("test"),
		},
	];

	for level in &test_levels {
		let base = level.base.to_string();
		let sql = format!(
			r"
			-- Initial setup
			DEFINE ACCESS api ON {base} TYPE BEARER FOR USER;
			DEFINE USER tobie ON {base} PASSWORD 'secret' ROLES EDITOR;
			ACCESS api ON {base} GRANT FOR USER tobie;
			ACCESS api ON {base} GRANT FOR USER tobie;
			ACCESS api ON {base} GRANT FOR USER tobie;
		"
		);
		let dbs = new_ds().await.unwrap();
		let ses = match level.base {
			Base::Root => Session::owner(),
			Base::Ns => Session::owner().with_ns(level.ns.unwrap()),
			Base::Db => Session::owner().with_ns(level.ns.unwrap()).with_db(level.db.unwrap()),
			_ => panic!("Invalid base"),
		};
		let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
		// Consume the results of the setup statements
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		// Retrieve the first generated bearer grant
		let tmp = res.remove(0).result.unwrap().to_string();
		let re =
			Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ id: '(.*)', key: .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \} \}")
					.unwrap();
		let kid = re.captures(&tmp).unwrap().get(1).unwrap().as_str();
		// Consume the results of the other two
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		// Revoke the first bearer grant
		let res =
			&mut dbs.execute(&format!("ACCESS api REVOKE GRANT {kid}"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(r"\{ ac: 'api', .*, revocation: d'.*', .* \}").unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Attempt to revoke the first bearer grant again
		let res =
			&mut dbs.execute(&format!("ACCESS api REVOKE GRANT {kid}"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap_err();
		assert_eq!(tmp.to_string(), "This access grant has been revoked");
		// Ensure that only that bearer grant is revoked
		// TODO(PR): This should not have passed
		let res = &mut dbs.execute(&format!("ACCESS api SHOW REVOKED"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(
			r"\[\{{ ac: 'api', .*, id: '{kid}', .*, revocation: d'.*', .* \}}\]"
		))
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Revoke the rest of the bearer grants
		let res = &mut dbs.execute(&format!("ACCESS api REVOKE ALL"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(
			r"\[\{ ac: 'api', .*, revocation: d'.*', .* \}, \{ ac: 'api', .*, revocation: d'.*', .* \}\]",
		)
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Ensure that all bearer grants are now revoked
		let res = &mut dbs.execute(&format!("ACCESS api SHOW REVOKED"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(r"\[\{ ac: 'api', .*, revocation: d'.*', .* \}, \{ ac: 'api', .*, revocation: d'.*', .* \}, \{ ac: 'api', .*, revocation: d'.*', .* \}\]").unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
	}
}

//
// Permissions
//

#[tokio::test]
async fn permissions_access_grant() {
	// TODO(gguillemas): Remove this once bearer access is no longer experimental.
	std::env::set_var("SURREAL_EXPERIMENTAL_BEARER_ACCESS", "true");

	let test_levels = vec![Base::Root, Base::Ns, Base::Db];

	for level in &test_levels {
		let base = level.to_string();

		let tests = vec![
			// Root level
			((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to issue a grant"),
			((().into(), Role::Editor), ("NS", "DB"), false, "editor at root level should not be able to issue a grant"),
			((().into(), Role::Viewer), ("NS", "DB"), false, "viewer at root level should not be able to issue a grant"),

			// Namespace level
			match level {
				Base::Db => ((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to issue a grant on its namespace"),
				Base::Ns => ((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to issue a grant on its namespace"),
				Base::Root => ((("NS",).into(), Role::Owner), ("NS", "DB"), false, "owner at namespace level should not be able to issue a grant on its namespace"),
				_ => panic!("Invalid base")
			},
			((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to issue a grant on another namespace"),
			((("NS",).into(), Role::Editor), ("NS", "DB"), false, "editor at namespace level should not be able to issue a grant on its namespace"),
			((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to issue a grant on another namespace"),
			((("NS",).into(), Role::Viewer), ("NS", "DB"), false, "viewer at namespace level should not be able to issue a grant on its namespace"),
			((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to issue a grant on another namespace"),

			// Database level
			match level {
				Base::Db => ((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true, "owner at database level should be able to issue a grant on its database"),
				Base::Ns => ((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false, "owner at database level should not be able to issue a grant on its database"),
				Base::Root => ((("NS", "DB").into(), Role::Owner), ("NS", "DB"), false, "owner at database level should not be able to issue a grant on its database"),
				_ => panic!("Invalid base")
			},
			((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to issue a grant on another database"),
			((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to issue a grant on another namespace even if the database name matches"),
			((("NS", "DB").into(), Role::Editor), ("NS", "DB"), false, "editor at database level should not be able to issue a grant on its database"),
			((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to issue a grant on another database"),
			((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to issue a grant on another namespace even if the database name matches"),
			((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false, "viewer at database level should not be able to issue a grant on its database"),
			((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to issue a grant on another database"),
			((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to issue a grant on another namespace even if the database name matches"),
		];
		let statement = format!("ACCESS api ON {base} GRANT FOR USER tobie");

		let test_level = level;
		for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
			let sess = Session::for_level(level, role).with_ns(ns).with_db(db);
			let sess_setup = match test_level {
				Base::Root => {
					Session::for_level(().into(), Role::Owner).with_ns("NS").with_db("DB")
				}
				Base::Ns => {
					Session::for_level(("NS",).into(), Role::Owner).with_ns("NS").with_db("DB")
				}
				Base::Db => {
					Session::for_level(("NS", "DB").into(), Role::Owner).with_ns("NS").with_db("DB")
				}
				_ => panic!("Invalid base"),
			};
			let statement_setup =
				format!("DEFINE ACCESS api ON {base} TYPE BEARER FOR USER; DEFINE USER tobie ON {base} ROLES OWNER");

			{
				let ds = new_ds().await.unwrap().with_auth_enabled(true);

				let mut resp = ds.execute(&statement_setup, &sess_setup, None).await.unwrap();
				let res = resp.remove(0).output();
				assert!(res.is_ok(), "Error setting up access method: {:?}", res);
				let res = resp.remove(0).output();
				assert!(res.is_ok(), "Error setting up user: {:?}", res);

				let mut resp = ds.execute(&statement, &sess, None).await.unwrap();
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
}
