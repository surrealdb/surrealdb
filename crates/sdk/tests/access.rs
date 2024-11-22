mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use regex::Regex;
use surrealdb::dbs::Session;
use surrealdb::iam::Role;
use surrealdb::sql::{Base, Value};
use tokio::time::Duration;

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
		println!("Test level: {}", base);
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
			ACCESS api ON {base} SHOW ALL;
			ACCESS api SHOW ALL;
			-- Should fail
			ACCESS invalid ON {base} GRANT FOR USER tobie;
			ACCESS invalid GRANT FOR USER tobie;
			ACCESS api ON {base} GRANT FOR USER invalid;
			ACCESS api GRANT FOR USER invalid;
			ACCESS invalid ON {base} SHOW ALL;
			ACCESS invalid SHOW ALL;
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
			Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
					.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		//
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok =
			Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
					.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		//
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok =
			Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
					.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		//
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok =
			Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
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
async fn access_bearer_grant() {
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
		println!("Test level: {}", base);
		let sql = format!(
			"
			-- Initial setup
			DEFINE ACCESS srv ON {base} TYPE BEARER FOR USER;
			DEFINE USER tobie ON {base} PASSWORD 'secret' ROLES EDITOR;
			-- Should succeed
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv GRANT FOR USER tobie;
			-- Should fail
			ACCESS srv ON {base} GRANT FOR USER jaime;
			ACCESS srv GRANT FOR USER jaime;
			ACCESS srv ON {base} GRANT FOR RECORD user:tobie;
			ACCESS srv GRANT FOR RECORD user:tobie;
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
		assert_eq!(res.len(), 8);
		// Consume the results of the setup statements
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		//
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok =
			Regex::new(r"\{ ac: 'srv', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
					.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		//
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok =
			Regex::new(r"\{ ac: 'srv', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
					.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		//
		let tmp = res.remove(0).result.unwrap_err();
		let expected = if matches!(level.base, Base::Root) {
			"The root user 'jaime' does not exist".to_string()
		} else {
			format!(
				"The user 'jaime' does not exist in the {} 'test'",
				level.base.to_string().to_lowercase()
			)
		};
		assert_eq!(tmp.to_string(), expected);
		//
		let tmp = res.remove(0).result.unwrap_err();
		let expected = if matches!(level.base, Base::Root) {
			"The root user 'jaime' does not exist".to_string()
		} else {
			format!(
				"The user 'jaime' does not exist in the {} 'test'",
				level.base.to_string().to_lowercase()
			)
		};
		assert_eq!(tmp.to_string(), expected);
		//
		let tmp = res.remove(0).result.unwrap_err();
		if let Base::Db = level.base {
			assert_eq!(tmp.to_string(), "This access grant has an invalid subject");
		} else {
			assert_eq!(tmp.to_string(), "Specify a database to use");
		}
		//
		let tmp = res.remove(0).result.unwrap_err();
		if let Base::Db = level.base {
			assert_eq!(tmp.to_string(), "This access grant has an invalid subject");
		} else {
			assert_eq!(tmp.to_string(), "Specify a database to use");
		}
		//
		if let Base::Db = level.base {
			let sql = format!(
				"
				-- Initial setup on database
				DEFINE ACCESS api ON {base} TYPE BEARER FOR RECORD;
				CREATE user:tobie;
				-- Should succeed on database
				ACCESS api ON {base} GRANT FOR RECORD user:tobie;
				ACCESS api GRANT FOR RECORD user:tobie;
				ACCESS api ON {base} GRANT FOR RECORD user:jaime;
				ACCESS api GRANT FOR RECORD user:jaime;
				-- Should fail on database
				ACCESS api ON {base} GRANT FOR USER tobie;
				ACCESS api GRANT FOR USER tobie;
			"
			);
			let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
			assert_eq!(res.len(), 8);
			// Consume the results of the setup statements
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			//
			let tmp = res.remove(0).result.unwrap().to_string();
			let ok =
				Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ record: user:tobie \}, type: 'bearer' \}")
						.unwrap();
			assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
			//
			let tmp = res.remove(0).result.unwrap().to_string();
			let ok =
				Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ record: user:tobie \}, type: 'bearer' \}")
						.unwrap();
			assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
			//
			let tmp = res.remove(0).result.unwrap().to_string();
			let ok =
				Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ record: user:jaime \}, type: 'bearer' \}")
						.unwrap();
			assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
			//
			let tmp = res.remove(0).result.unwrap().to_string();
			let ok =
				Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ record: user:jaime \}, type: 'bearer' \}")
						.unwrap();
			assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
			//
			let tmp = res.remove(0).result.unwrap_err();
			assert_eq!(tmp.to_string(), "This access grant has an invalid subject");
			//
			let tmp = res.remove(0).result.unwrap_err();
			assert_eq!(tmp.to_string(), "This access grant has an invalid subject");
		}
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
		println!("Test level: {}", base);
		let sql = format!(
			r"
			-- Initial setup
			DEFINE ACCESS srv ON {base} TYPE BEARER FOR USER;
			DEFINE USER tobie ON {base} PASSWORD 'secret' ROLES EDITOR;
			DEFINE USER jaime ON {base} PASSWORD 'secret' ROLES EDITOR;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER jaime;
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
		res.remove(0).result.unwrap();
		// Retrieve the first generated bearer grant
		let tmp = res.remove(0).result.unwrap().to_string();
		let re =
			Regex::new(r"\{ ac: 'srv', creation: .*?, expiration: NONE, grant: \{ id: '(.*?)', key: .*? \}, id: .*?, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
					.unwrap();
		let kid = re.captures(&tmp).unwrap().get(1).unwrap().as_str();
		// Consume the results of the other three
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		// Revoke the first bearer grant
		let res =
			&mut dbs.execute(&format!("ACCESS srv REVOKE GRANT {kid}"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(r"\{ ac: 'srv', .*?, revocation: d'.*?', .*? \}").unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Attempt to revoke the first bearer grant again
		let res =
			&mut dbs.execute(&format!("ACCESS srv REVOKE GRANT {kid}"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap_err();
		assert_eq!(tmp.to_string(), "This access grant has been revoked");
		// Ensure that only that bearer grant is revoked
		let res = &mut dbs
			.execute(&format!("ACCESS srv SHOW WHERE revocation IS NOT NONE"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(
			r"\[\{{ ac: 'srv', .*?, id: '{kid}', revocation: d'.*?', .*? \}}\]"
		))
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Revoke all bearer grants for a specific user
		let res = &mut dbs
			.execute(&format!("ACCESS srv REVOKE WHERE subject.user = 'jaime'"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(
			r"\[\{ ac: 'srv', .*?, revocation: d'.*?', subject: \{ user: 'jaime' \}, type: 'bearer' \}\]",
		)
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Revoke the rest of the bearer grants
		let res = &mut dbs.execute(&format!("ACCESS srv REVOKE ALL"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(
			r"\[\{ ac: 'srv', .*?, revocation: d'.*?', .*? \}, \{ ac: 'srv', .*?, revocation: d'.*?', .*? \}\]",
		)
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Ensure that all bearer grants are now revoked
		let res = &mut dbs
			.execute(&format!("ACCESS srv SHOW WHERE revocation IS NONE"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(r"\[\]").unwrap();

		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		if let Base::Db = level.base {
			let sql = format!(
				"
				-- Initial setup on database
				DEFINE ACCESS api ON {base} TYPE BEARER FOR RECORD;
				CREATE user:tobie;
				ACCESS api GRANT FOR RECORD user:tobie;
				ACCESS api GRANT FOR RECORD user:jaime;
				-- Tests
				ACCESS api REVOKE WHERE subject.record = user:tobie;
				ACCESS api REVOKE WHERE subject.record = user:jaime;
			"
			);
			let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
			// Consume the results of the setup statements
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			//
			let tmp = res.remove(0).result.unwrap().to_string();
			let ok =
				Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: d'.*?', subject: \{ record: user:tobie \}, type: 'bearer' \}")
						.unwrap();
			assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
			//
			let tmp = res.remove(0).result.unwrap().to_string();
			let ok =
				Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: d'.*?', subject: \{ record: user:jaime \}, type: 'bearer' \}")
						.unwrap();
			assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		}
	}
}

#[tokio::test]
async fn access_bearer_show() {
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
		println!("Test level: {}", base);
		let sql = format!(
			r"
			-- Initial setup
			DEFINE ACCESS srv ON {base} TYPE BEARER FOR USER;
			DEFINE USER tobie ON {base} PASSWORD 'secret' ROLES EDITOR;
			DEFINE USER jaime ON {base} PASSWORD 'secret' ROLES EDITOR;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER jaime;
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
		res.remove(0).result.unwrap();
		// Retrieve the first generated bearer grant
		let tmp = res.remove(0).result.unwrap().to_string();
		let re =
			Regex::new(r"\{ ac: 'srv', creation: .*?, expiration: NONE, grant: \{ id: '(.*?)', key: .*? \}, id: .*?, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
					.unwrap();
		let kid = re.captures(&tmp).unwrap().get(1).unwrap().as_str();
		// Consume the results of the other three
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		// Revoke the first bearer grant
		let res =
			&mut dbs.execute(&format!("ACCESS srv REVOKE GRANT {kid}"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(r"\{ ac: 'srv', .*?, revocation: d'.*?', .*? \}").unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Show a specific bearer grant
		let res = &mut dbs
			.execute(&format!("ACCESS srv SHOW WHERE grant.id = '{kid}'"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(
			r"\[\{{ ac: 'srv', .*?, grant: \{{ id: '{kid}', key: '\[REDACTED\]' \}}, id: '{kid}', revocation: d'.*?', subject: \{{ user: 'tobie' \}}, type: 'bearer' \}}\]",
		))
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Show all bearer grants for a specific user
		let res = &mut dbs
			.execute(&format!("ACCESS srv SHOW WHERE subject.user = 'jaime'"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(
			r"\[\{ ac: 'srv', .*?, revocation: NONE, subject: \{ user: 'jaime' \}, type: 'bearer' \}\]",
		)
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Show all non-revoked bearer grants for a specific user
		let res = &mut dbs
			.execute(
				&format!("ACCESS srv SHOW WHERE subject.user = 'tobie' AND revocation IS NONE"),
				&ses,
				None,
			)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(
			r"\[\{ ac: 'srv', .*?, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}, \{ ac: 'srv', .*?, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}\]",
		)
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Show all revoked bearer grants
		let res = &mut dbs
			.execute(&format!("ACCESS srv SHOW WHERE revocation IS NOT NONE"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(
			r"\[\{{ ac: 'srv', .*?, grant: \{{ id: '{kid}', key: '\[REDACTED\]' \}}, id: '{kid}', revocation: d'.*?', subject: \{{ user: 'tobie' \}}, type: 'bearer' \}}\]",
		))
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Show all active bearer grants
		let res = &mut dbs
			.execute(&format!("ACCESS srv SHOW WHERE revocation IS NONE AND (expiration IS NONE OR expiration < time::now())"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(
			r"\[\{ ac: 'srv', .*?, revocation: NONE, subject: \{ user: '(tobie|jaime)' \}, type: 'bearer' \}, \{ ac: 'srv', .*?, revocation: NONE, subject: \{ user: '(tobie|jaime)' \}, type: 'bearer' \}, \{ ac: 'srv', .*?, revocation: NONE, subject: \{ user: '(tobie|jaime)' \}, type: 'bearer' \}\]",
		)
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		if let Base::Db = level.base {
			let sql = format!(
				"
				-- Initial setup on database
				DEFINE ACCESS api ON {base} TYPE BEARER FOR RECORD;
				CREATE user:tobie;
				ACCESS api GRANT FOR RECORD user:tobie;
				ACCESS api GRANT FOR RECORD user:jaime;
				-- Tests
				ACCESS api SHOW WHERE subject.record = user:tobie;
				ACCESS api SHOW WHERE subject.record = user:jaime;
			"
			);
			let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
			// Consume the results of the setup statements
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			//
			let tmp = res.remove(0).result.unwrap().to_string();
			let ok =
				Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ record: user:tobie \}, type: 'bearer' \}")
						.unwrap();
			assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
			//
			let tmp = res.remove(0).result.unwrap().to_string();
			let ok =
				Regex::new(r"\{ ac: 'api', creation: .*, expiration: NONE, grant: \{ .* \}, id: .*, revocation: NONE, subject: \{ record: user:jaime \}, type: 'bearer' \}")
						.unwrap();
			assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		}
	}
}

#[tokio::test]
async fn access_bearer_purge() {
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
		println!("Test level: {}", base);
		let sql = format!(
			r"
			-- Initial setup
			DEFINE ACCESS srv ON {base} TYPE BEARER FOR USER DURATION FOR GRANT 2s;
			DEFINE USER tobie ON {base} PASSWORD 'secret' ROLES EDITOR;
			DEFINE USER jaime ON {base} PASSWORD 'secret' ROLES EDITOR;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER tobie;
			ACCESS srv ON {base} GRANT FOR USER jaime;
			ACCESS srv ON {base} GRANT FOR USER jaime;
			ACCESS srv ON {base} GRANT FOR USER jaime;
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
		res.remove(0).result.unwrap();
		// Retrieve the first generated bearer grant
		let tmp = res.remove(0).result.unwrap().to_string();
		let re =
			Regex::new(r"\{ ac: 'srv', creation: .*?, expiration: d'.*?', grant: \{ id: '(.*?)', key: .*? \}, id: .*?, revocation: NONE, subject: \{ user: 'tobie' \}, type: 'bearer' \}")
					.unwrap();
		let kid = re.captures(&tmp).unwrap().get(1).unwrap().as_str();
		// Consume the results of the other three
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		res.remove(0).result.unwrap();
		// Revoke the first bearer grant
		let res =
			&mut dbs.execute(&format!("ACCESS srv REVOKE GRANT {kid}"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok =
			Regex::new(&format!(r"\{{ ac: 'srv', .*?, id: '{kid}', revocation: d'.*?', .*? \}}"))
				.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Show revoked bearer grant
		let res =
			&mut dbs.execute(&format!("ACCESS srv SHOW GRANT {kid}"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok =
			Regex::new(&format!(r"\{{ ac: 'srv', .*?, id: '{kid}', revocation: d'.*?', .*? \}}"))
				.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Wait for a second
		std::thread::sleep(Duration::from_secs(1));
		// Purge revoked bearer grants
		let res = &mut dbs.execute(&format!("ACCESS srv PURGE REVOKED"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(
			r"\[\{{ ac: 'srv', .*?, id: '{kid}', revocation: d'.*?', .*? \}}\]"
		))
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Ensure that only that bearer grant is purged
		let res = &mut dbs.execute(&format!("ACCESS srv SHOW ALL"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(
			r"\[\{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}\]"
		))
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Wait for all grants to expire
		std::thread::sleep(Duration::from_secs(2));
		// Purge grants expired for 2 seconds
		let res = &mut dbs
			.execute(&format!("ACCESS srv PURGE EXPIRED FOR 2s"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(r"\[\]")).unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Ensure that no grants have been purged
		let res = &mut dbs.execute(&format!("ACCESS srv SHOW ALL"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(
			r"\[\{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}\]"
		))
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Wait for grants to be expired for 2 seconds
		std::thread::sleep(Duration::from_secs(2));
		// Purge grants expired for 2 seconds
		let res = &mut dbs
			.execute(&format!("ACCESS srv PURGE EXPIRED FOR 2s"), &ses, None)
			.await
			.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(
			r"\[\{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}, \{{ ac: 'srv', .*? \}}\]"
		))
		.unwrap();
		assert!(ok.is_match(&tmp), "Output '{}' doesn't match regex '{}'", tmp, ok);
		// Ensure that all grants have been purged
		let res = &mut dbs.execute(&format!("ACCESS srv SHOW ALL"), &ses, None).await.unwrap();
		let tmp = res.remove(0).result.unwrap().to_string();
		let ok = Regex::new(&format!(r"\[\]")).unwrap();
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
		println!("Test level: {}", base);

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
