mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn create_with_id() -> Result<(), Error> {
	let sql = "
		CREATE person:test SET name = 'Tester';
		CREATE person SET id = person:tobie, name = 'Tobie';
		CREATE person CONTENT { id: person:jaime, name: 'Jaime' };
		CREATE user CONTENT { id: 1, name: 'Robert' };
		CREATE city CONTENT { id: 'london', name: 'London' };
		CREATE city CONTENT { id: '8e60244d-95f6-4f95-9e30-09a98977efb0', name: 'London' };
		CREATE temperature CONTENT { id: ['London', '2022-09-30T20:25:01.406828Z'], name: 'London' };
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Tester'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:tobie,
				name: 'Tobie'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:jaime,
				name: 'Jaime'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:1,
				name: 'Robert'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: city:london,
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: city:⟨8e60244d-95f6-4f95-9e30-09a98977efb0⟩,
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: temperature:['London', '2022-09-30T20:25:01.406828Z'],
				name: 'London'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn create_on_non_values_with_unique_index() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX national_id_idx ON foo FIELDS national_id UNIQUE;
		CREATE foo SET name = 'John Doe';
		CREATE foo SET name = 'Jane Doe';
	";

	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..3 {
		let _ = res.remove(0).result?;
	}
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
			let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(auth_enabled);

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
			let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(auth_enabled);

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
		let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(auth_enabled);

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
		let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(auth_enabled);

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
		let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(auth_enabled);

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
		let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(auth_enabled);

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
		let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(auth_enabled);

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
		let ds = Datastore::new("memory").await.unwrap().with_auth_enabled(auth_enabled);

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
