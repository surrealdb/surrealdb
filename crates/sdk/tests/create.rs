mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::sql::Part;
use surrealdb::sql::Thing;
use surrealdb::sql::Value;

#[tokio::test]
async fn create_or_insert_with_permissions() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;
		CREATE user:test;
		DEFINE TABLE demo SCHEMAFULL PERMISSIONS FOR select, create, update WHERE user = $auth.id;
		DEFINE FIELD user ON TABLE demo VALUE $auth.id;
	";
	let dbs = new_ds().await?.with_auth_enabled(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let sql = "
		CREATE demo SET id = demo:one;
		INSERT INTO demo (id) VALUES (demo:two);
	";
	let ses = Session::for_record("test", "test", "test", Thing::from(("user", "test")).into());
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: demo:one,
				user: user:test,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: demo:two,
				user: user:test,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

//
// Permissions
//

async fn common_permissions_checks(auth_enabled: bool) {
	let tests = vec![
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to create a new record"),
		((().into(), Role::Editor), ("NS", "DB"), true, "editor at root level should be able to create a new record"),
		((().into(), Role::Viewer), ("NS", "DB"), false, "viewer at root level should not be able to create a new record"),

		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to create a new record on its namespace"),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to create a new record on another namespace"),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true, "editor at namespace level should be able to create a new record on its namespace"),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to create a new record on another namespace"),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false, "viewer at namespace level should not be able to create a new record on its namespace"),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to create a new record on another namespace"),

		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true, "owner at database level should be able to create a new record on its database"),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to create a new record on another database"),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to create a new record on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true, "editor at database level should be able to create a new record on its database"),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to create a new record on another database"),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to create a new record on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false, "viewer at database level should not be able to create a new record on its database"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to create a new record on another database"),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to create a new record on another namespace even if the database name matches"),
	];
	let statement = "CREATE person";

	// Test the CREATE statement when the table has to be created
	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok(), "{}: {:?}", msg, res);
				assert_ne!(res.unwrap(), Value::parse("[]"), "{}", msg);
			} else if res.is_ok() {
				// Permissions clause doesn't allow to query the table
				assert_eq!(res.unwrap(), Value::parse("[]"), "{}", msg);
			} else {
				// Not allowed to create a table
				let err = res.unwrap_err().to_string();
				assert!(
					err.contains("Not enough permissions to perform this action"),
					"{}: {}",
					msg,
					err
				)
			}
		}

		// Test the CREATE statement when the table already exists
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::parse("[]"),
				"unexpected error creating person record"
			);

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("OTHER_NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::parse("[]"),
				"unexpected error creating person record"
			);

			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("NS").with_db("OTHER_DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::parse("[]"),
				"unexpected error creating person record"
			);

			// Run the test
			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok(), "{}: {:?}", msg, res);
				assert_ne!(res.unwrap(), Value::parse("[]"), "{}", msg);
			} else if res.is_ok() {
				// Permissions clause doesn't allow to query the table
				assert_eq!(res.unwrap(), Value::parse("[]"), "{}", msg);
			} else {
				// Not allowed to create a table
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
async fn check_permissions_auth_enabled() {
	let auth_enabled = true;
	//
	// Test common scenarios
	//
	common_permissions_checks(auth_enabled).await;

	//
	// Test Anonymous user
	//

	// When the table doesn't exist
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute("CREATE person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		let err = res.unwrap_err().to_string();
		assert!(
			err.contains("Not enough permissions to perform this action"),
			"anonymous user should not be able to create the table: {}",
			err
		);
	}

	// When the table exists but grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute("CREATE person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.unwrap() == Value::parse("[]"), "{}", "anonymous user should not be able to create a new record if the table exists but has no permissions");
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute("CREATE person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.unwrap() != Value::parse("[]"), "{}", "anonymous user should be able to create a new record if the table exists and grants full permissions");
	}
}

#[tokio::test]
async fn check_permissions_auth_disabled() {
	let auth_enabled = false;
	//
	// Test common scenarios
	//
	common_permissions_checks(auth_enabled).await;

	//
	// Test Anonymous user
	//

	// When the table doesn't exist
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute("CREATE person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::parse("[]"),
			"{}",
			"anonymous user should be able to create the table"
		);
	}

	// When the table exists but grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute("CREATE person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.unwrap() != Value::parse("[]"), "{}", "anonymous user should not be able to create a new record if the table exists but has no permissions");
	}

	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		// When the table exists and grants full permissions
		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute("CREATE person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.unwrap() != Value::parse("[]"), "{}", "anonymous user should be able to create a new record if the table exists and grants full permissions");
	}
}
