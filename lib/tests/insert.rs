mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::sql::Part;
use surrealdb::sql::Value;

#[tokio::test]
async fn insert_statement_object_single() -> Result<(), Error> {
	let sql = "
		INSERT INTO test {
			id: 'tester',
			test: true,
			something: 'other',
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:tester, test: true, something: 'other' }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_object_multiple() -> Result<(), Error> {
	let sql = "
		INSERT INTO test [
			{
				id: 1,
				test: true,
				something: 'other',
			},
			{
				id: 2,
				test: false,
				something: 'else',
			},
		];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{ id: test:1, test: true, something: 'other' },
			{ id: test:2, test: false, something: 'else' }
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_values_single() -> Result<(), Error> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other');
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:tester, test: true, something: 'other' }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_values_multiple() -> Result<(), Error> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES (1, true, 'other'), (2, false, 'else');
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{ id: test:1, test: true, something: 'other' },
			{ id: test:2, test: false, something: 'else' }
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_values_retable_id() -> Result<(), Error> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES (person:1, true, 'other'), (person:2, false, 'else');
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{ id: test:1, test: true, something: 'other' },
			{ id: test:2, test: false, something: 'else' }
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_on_duplicate_key() -> Result<(), Error> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other');
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other') ON DUPLICATE KEY UPDATE something = 'else';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:tester, test: true, something: 'other' }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:tester, test: true, something: 'else' }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_output() -> Result<(), Error> {
	let sql = "
		INSERT INTO test (id, test, something) VALUES ('tester', true, 'other') RETURN something;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ something: 'other' }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_statement_duplicate_key_update() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX name ON TABLE company COLUMNS name UNIQUE;
		INSERT INTO company (name, founded) VALUES ('SurrealDB', '2021-09-10') ON DUPLICATE KEY UPDATE founded = $input.founded;
		INSERT INTO company (name, founded) VALUES ('SurrealDB', '2021-09-11') ON DUPLICATE KEY UPDATE founded = $input.founded;
		INSERT INTO company (name, founded) VALUES ('SurrealDB', '2021-09-12') ON DUPLICATE KEY UPDATE founded = $input.founded PARALLEL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp.first().pick(&[Part::from("name")]), Value::from("SurrealDB"));
	assert_eq!(tmp.first().pick(&[Part::from("founded")]), Value::from("2021-09-10"));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp.first().pick(&[Part::from("name")]), Value::from("SurrealDB"));
	assert_eq!(tmp.first().pick(&[Part::from("founded")]), Value::from("2021-09-11"));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp.first().pick(&[Part::from("name")]), Value::from("SurrealDB"));
	assert_eq!(tmp.first().pick(&[Part::from("founded")]), Value::from("2021-09-12"));
	//
	Ok(())
}

//
// Permissions
//

async fn common_permissions_checks(auth_enabled: bool) {
	let tests = vec![
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to insert a new record"),
		((().into(), Role::Editor), ("NS", "DB"), true, "editor at root level should be able to insert a new record"),
		((().into(), Role::Viewer), ("NS", "DB"), false, "viewer at root level should not be able to insert a new record"),

		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to insert a new record on its namespace"),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to insert a new record on another namespace"),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true, "editor at namespace level should be able to insert a new record on its namespace"),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to insert a new record on another namespace"),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false, "viewer at namespace level should not be able to insert a new record on its namespace"),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to insert a new record on another namespace"),

		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true, "owner at database level should be able to insert a new record on its database"),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to insert a new record on another database"),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to insert a new record on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true, "editor at database level should be able to insert a new record on its database"),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to insert a new record on another database"),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to insert a new record on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false, "viewer at database level should not be able to insert a new record on its database"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to insert a new record on another database"),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to insert a new record on another namespace even if the database name matches"),
	];
	let statement = "INSERT INTO person (id) VALUES ('id')";

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		// Test the INSERT statement when the table has to be created
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			if should_succeed {
				assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", msg);
			} else if res.is_ok() {
				assert!(res.unwrap() == Value::parse("[]"), "{}", msg);
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

		// Test the INSERT statement when the table already exists
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
				assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", msg);
			} else if res.is_ok() {
				assert!(res.unwrap() == Value::parse("[]"), "{}", msg);
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
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
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
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.unwrap() == Value::parse("[]"), "{}", "anonymous user should not be able to insert a new record if the table exists but has no permissions");
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
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.unwrap() != Value::parse("[]"), "{}", "anonymous user should be able to insert a new record if the table exists and grants full permissions");
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
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
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
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.unwrap() != Value::parse("[]"), "{}", "anonymous user should not be able to insert a new record if the table exists but has no permissions");
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
			.execute(
				"INSERT INTO person (id) VALUES ('id')",
				&Session::default().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(res.unwrap() != Value::parse("[]"), "{}", "anonymous user should be able to insert a new record if the table exists and grants full permissions");
	}
}

#[tokio::test]
async fn insert_statement_unique_index() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX name ON TABLE company COLUMNS name UNIQUE;
		INSERT INTO company { name: 'SurrealDB' };
		INSERT INTO company { name: 'SurrealDB' };
		SELECT count() FROM company GROUP ALL;
	";
	let dbs = new_ds().await?;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse("[ { count: 1 } ]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_relation() -> Result<(), Error> {
	let sql = "
		INSERT INTO person [
			{ id: person:1 },
			{ id: person:2 },
			{ id: person:3 },
		];
		INSERT RELATION INTO likes {
			in: person:1,
			id: 'object',
			out: person:2,
		};
		INSERT RELATION INTO likes [
			{
				in: person:1,
				id: 'array',
				out: person:2,
			},
			{
				in: person:2,
				id: 'array_twoo',
				out: person:3,
			}
		];
		INSERT RELATION INTO likes (in, id, out)
			VALUES (person:1, 'values', person:2);
		SELECT VALUE ->likes FROM person;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: person:1 }, { id: person:2 }, { id: person:3 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"
		[
			{
					id: likes:object,
					in: person:1,
					out: person:2
			}
		]
	",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"
		[
			{
                id: likes:array,
                in: person:1,
                out: person:2
			},
			{
				id: likes:array_twoo,
				in: person:2,
				out: person:3
			}
		]
	",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"
		[
			{
                id: likes:values,
                in: person:1,
                out: person:2
       		}
		]
	",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"
		[
			[
                likes:array,
                likes:object,
                likes:values
			],
			[
				likes:array_twoo
			],
			[]
		]
	",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn insert_invalid_relation() -> Result<(), Error> {
	let sql = "
		INSERT RELATION INTO likes {
			id: 'object',
		};

		INSERT RELATION {
			in: person:1,
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	match res.remove(0).result {
		Err(Error::InsertStatementIn {
			value,
		}) if value == "NONE" => (),
		found => panic!("Expected Err(Error::InsertStatementIn), found '{:?}'", found),
	}
	//
	match res.remove(0).result {
		Err(Error::InsertStatementId {
			value,
		}) if value == "NONE" => (),
		found => panic!("Expected Err(Error::InsertStatementId), found '{:?}'", found),
	}
	//
	Ok(())
}

#[tokio::test]
async fn insert_without_into() -> Result<(), Error> {
	let sql = "
		INSERT [
			{ id: test:1 }
		];

		INSERT { id: test:2 };
		INSERT (id) VALUES (test:3);

		INSERT {};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:1 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:2 }]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: test:3 }]");
	assert_eq!(tmp, val);
	//
	match res.remove(0).result {
		Err(Error::InsertStatementId {
			value,
		}) if value == "NONE" => (),
		found => panic!("Expected Err(Error::RelateStatementId), found '{:?}'", found),
	}
	//
	Ok(())
}
