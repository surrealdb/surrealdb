mod parse;
use parse::Parse;
mod helpers;
use crate::helpers::Test;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::sql::Value;

#[tokio::test]
async fn upsert_merge_and_content() -> Result<(), Error> {
	let sql = "
		CREATE person:test CONTENT { name: 'Tobie' };
		UPSERT person:test CONTENT { name: 'Jaime' };
		UPSERT person:test CONTENT 'some content';
		UPSERT person:test REPLACE 'some content';
		UPSERT person:test MERGE { age: 50 };
		UPSERT person:test MERGE 'some content';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Jaime',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Can not use 'some content' in a CONTENT clause"#
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Can not use 'some content' in a CONTENT clause"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Jaime',
				age: 50,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Can not use 'some content' in a MERGE clause"#
	));
	//
	Ok(())
}

#[tokio::test]
async fn upsert_simple_with_input() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD name ON TABLE person
			ASSERT
				IF $input THEN
					$input = /^[A-Z]{1}[a-z]+$/
				ELSE
					true
				END
			VALUE
				IF $input THEN
					'Name: ' + $input
				ELSE
					$value
				END
		;
		UPSERT person:test;
		UPSERT person:test CONTENT { name: 'Tobie' };
		UPSERT person:test REPLACE { name: 'jaime' };
		UPSERT person:test MERGE { name: 'Jaime' };
		UPSERT person:test SET name = 'tobie';
		UPSERT person:test SET name = 'Tobie';
		SELECT * FROM person:test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: jaime' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Jaime',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: tobie' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn update_complex_with_input() -> Result<(), Error> {
	let sql = "
		DEFINE FIELD images ON product
			TYPE array
			ASSERT array::len($value) > 0
		;
		DEFINE FIELD images.* ON product TYPE string
			VALUE string::trim($input)
			ASSERT $input AND string::len($value) > 0
		;
		CREATE product:test SET images = [' test.png '];
	";
	let mut t = Test::new(sql).await?;
	t.skip_ok(2)?;
	t.expect_val(
		"[
			{
				id: product:test,
				images: ['test.png'],
			}
		]",
	)?;
	Ok(())
}

#[tokio::test]
async fn upsert_with_return_clause() -> Result<(), Error> {
	let sql = "
		CREATE person:test SET age = 18, name = 'John';
		UPSERT person:test SET age = 25 RETURN VALUE $before;
		UPSERT person:test SET age = 30 RETURN VALUE { old_age: $before.age, new_age: $after.age };
		UPSERT person:test SET age = 35 RETURN age, name;
		DELETE person:test RETURN VALUE $before;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 18,
				id: person:test,
				name: 'John'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 18,
				id: person:test,
				name: 'John'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				new_age: 30,
				old_age: 25
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 35,
				name: 'John'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 35,
				id: person:test,
				name: 'John'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn upsert_new_record_with_table() -> Result<(), Error> {
	let sql = "
		-- This will return the created record
		UPSERT person SET one = 'one', two = 'two', three = 'three';
		-- Select all created records
		SELECT count() FROM person GROUP ALL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.len() == 1));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				count: 1,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn upsert_new_records_with_thing_and_where_clause() -> Result<(), Error> {
	let sql = "
		-- This will return the created record as no matching records exist
		UPSERT person:test SET name = 'Jaime' WHERE name = 'Jaime';
		-- This will return the updated record, because a matching record exists, and the WHERE clause matches
		UPSERT person:test SET name = 'Tobie' WHERE name = 'Jaime';
		-- This will return a newly created record, because the WHERE clause does not match
		UPSERT person:test SET name = 'Tobie' WHERE name = 'Jaime';
		-- Select all created records
		SELECT count() FROM person GROUP ALL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.len() == 1));
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.len() == 1));
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.is_empty()));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				count: 1,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn upsert_new_records_with_table_and_where_clause() -> Result<(), Error> {
	let sql = "
		-- This will return the created record as no matching records exist
		UPSERT person SET name = 'Jaime' WHERE name = 'Jaime';
		-- This will return the updated record, because a matching record exists, and the WHERE clause matches
		UPSERT person SET name = 'Tobie' WHERE name = 'Jaime';
		-- This will return a newly created record, because the WHERE clause does not match
		UPSERT person SET name = 'Tobie' WHERE name = 'Jaime';
		-- Select all created records
		SELECT count() FROM person GROUP ALL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.len() == 1));
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.len() == 1));
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.len() == 1));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				count: 2,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn upsert_new_records_with_table_and_unique_index() -> Result<(), Error> {
	let sql = "
		-- This will define a unique index on the table
		DEFINE INDEX OVERWRITE testing ON person FIELDS one, two, three UNIQUE;
		-- This will create a record, and populate the unique index with this record id
		UPSERT person SET one = 'something', two = 'something', three = 'something';
		-- This will update the record, returning the same record id created in the statement above
		UPSERT person SET one = 'something', two = 'something', three = 'something';
		-- This will throw an error, because the unique index already has a record with a different record id
		UPSERT person:test SET one = 'something', two = 'something', three = 'something';
		-- Select all created records
		SELECT count() FROM person GROUP ALL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.len() == 1));
	//
	let tmp = res.remove(0).result?;
	assert!(matches!(tmp, Value::Array(v) if v.len() == 1));
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_err());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				count: 1,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn upsert_new_and_update_records_with_content_and_merge() -> Result<(), Error> {
	let sql = "
		-- Setup the schemaful table
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD age ON person TYPE number;
		DEFINE FIELD metadata ON person FLEXIBLE TYPE any;
		-- This record will be created successfully
		CREATE person:test CONTENT { age: 18 };
		-- This will succeed, because we are updating an already existing record
		UPSERT person:test SET metadata = true, something = 'some';
		-- This will succeed, because we are updating an already existing record
		UPDATE person:test MERGE { metadata: false, something: 'thing' };
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
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
	let val = Value::parse(
		"[
			{
				age: 18,
				id: person:test
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 18,
				id: person:test,
				metadata: true
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 18,
				id: person:test,
				metadata: false
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn upsert_new_and_update_records_with_content_and_merge_with_readonly_fields(
) -> Result<(), Error> {
	let sql = "
		-- Setup the schemaful table
		DEFINE TABLE person SCHEMALESS;
		DEFINE FIELD created ON person TYPE datetime READONLY DEFAULT d'2024-01-01';
		DEFINE FIELD age ON person TYPE number;
		DEFINE FIELD data ON person FLEXIBLE TYPE object;
		-- This record will be created successfully
		UPSERT person:test CONTENT { age: 1, data: { some: true, other: false } };
		-- This record will be updated successfully, with the readonly field untouched
		UPSERT person:test CONTENT { age: 2, data: { nothing: true } };
		-- This record will be updated successfully, with the readonly and flexible fields untouched
		UPSERT person:test MERGE { age: 3 };
		-- This record will be updated successfully, with the readonly and flexible fields untouched
		UPSERT person:test SET age = 4, data.nothing = false;
		-- This will return an error, as the readonly field is modified
		UPSERT person:test REPLACE { age: 5, data: { other: true } };
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 1,
				created: d'2024-01-01T00:00:00Z',
				data: {
					other: false,
					some: true
				},
				id: person:test
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 2,
				created: d'2024-01-01T00:00:00Z',
				data: {
					nothing: true
				},
				id: person:test
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 3,
				created: d'2024-01-01T00:00:00Z',
				data: {
					nothing: true
				},
				id: person:test
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 4,
				created: d'2024-01-01T00:00:00Z',
				data: {
					nothing: false
				},
				id: person:test
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found changed value for field `created`, with record `person:test`, but field is readonly"#
	));
	//
	Ok(())
}

//
// Permissions
//

async fn common_permissions_checks(auth_enabled: bool) {
	let tests = vec![
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to update a record"),
		((().into(), Role::Editor), ("NS", "DB"), true, "editor at root level should be able to update a record"),
		((().into(), Role::Viewer), ("NS", "DB"), false, "viewer at root level should not be able to update a record"),

		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to update a record on its namespace"),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to update a record on another namespace"),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true, "editor at namespace level should be able to update a record on its namespace"),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to update a record on another namespace"),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), false, "viewer at namespace level should not be able to update a record on its namespace"),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to update a record on another namespace"),

		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true, "owner at database level should be able to update a record on its database"),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to update a record on another database"),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to update a record on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true, "editor at database level should be able to update a record on its database"),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to update a record on another database"),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to update a record on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), false, "viewer at database level should not be able to update a record on its database"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to update a record on another database"),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to update a record on another namespace even if the database name matches"),
	];
	let statement = "UPSERT person:test CONTENT { name: 'Name' };";

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		// Test the statement when the table has to be created

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

		// Test the statement when the table already exists
		{
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

			// Prepare datastore
			let mut resp = ds
				.execute("CREATE person:test", &Session::owner().with_ns("NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::parse("[]"),
				"unexpected error creating person record"
			);
			let mut resp = ds
				.execute(
					"CREATE person:test",
					&Session::owner().with_ns("OTHER_NS").with_db("DB"),
					None,
				)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != Value::parse("[]"),
				"unexpected error creating person record"
			);
			let mut resp = ds
				.execute(
					"CREATE person:test",
					&Session::owner().with_ns("NS").with_db("OTHER_DB"),
					None,
				)
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

			// Select always succeeds, but the result may be empty
			assert!(res.is_ok());

			if should_succeed {
				assert!(res.unwrap() != Value::parse("[]"), "{}", msg);

				// Verify the update was persisted
				let mut resp = ds
					.execute(
						"SELECT name FROM person:test",
						&Session::owner().with_ns("NS").with_db("DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				let res = res.unwrap().to_string();
				assert!(res.contains("Name"), "{}: {:?}", msg, res);
			} else {
				assert!(res.unwrap() == Value::parse("[]"), "{}", msg);

				// Verify the update was not persisted
				let mut resp = ds
					.execute(
						"SELECT name FROM person:test",
						&Session::owner().with_ns("NS").with_db("DB"),
						None,
					)
					.await
					.unwrap();
				let res = resp.remove(0).output();
				let res = res.unwrap().to_string();
				assert!(!res.contains("Name"), "{}: {:?}", msg, res);
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

	let statement = "UPSERT person:test CONTENT { name: 'Name' };";

	// When the table doesn't exist
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
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

	// When the table grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE; CREATE person:test;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);
		let res = resp.remove(0).output();
		assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", "failed to create record");

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() == Value::parse("[]"),
			"{}",
			"anonymous user should not be able to select if the table has no permissions"
		);

		// Verify the update was not persisted
		let mut resp = ds
			.execute(
				"SELECT name FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		let res = res.unwrap().to_string();
		assert!(
			!res.contains("Name"),
			"{}: {:?}",
			"anonymous user should not be able to update a record if the table has no permissions",
			res
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL; CREATE person:test;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);
		let res = resp.remove(0).output();
		assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", "failed to create record");

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::parse("[]"),
			"{}",
			"anonymous user should be able to select if the table has full permissions"
		);

		// Verify the update was persisted
		let mut resp = ds
			.execute(
				"SELECT name FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		let res = res.unwrap().to_string();
		assert!(
			res.contains("Name"),
			"{}: {:?}",
			"anonymous user should be able to update a record if the table has full permissions",
			res
		);
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

	let statement = "UPSERT person:test CONTENT { name: 'Name' };";

	// When the table doesn't exist
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::parse("[]"),
			"{}",
			"anonymous user should be able to create the table"
		);
	}

	// When the table grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE; CREATE person:test;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);
		let res = resp.remove(0).output();
		assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", "failed to create record");

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::parse("[]"),
			"{}",
			"anonymous user should be able to update a record if the table has no permissions"
		);

		// Verify the update was persisted
		let mut resp = ds
			.execute(
				"SELECT name FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		let res = res.unwrap().to_string();
		assert!(
			res.contains("Name"),
			"{}: {:?}",
			"anonymous user should be able to update a record if the table has no permissions",
			res
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL; CREATE person:test;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);
		let res = resp.remove(0).output();
		assert!(res.is_ok() && res.unwrap() != Value::parse("[]"), "{}", "failed to create record");

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Value::parse("[]"),
			"{}",
			"anonymous user should be able to select if the table has full permissions"
		);

		// Verify the update was persisted
		let mut resp = ds
			.execute(
				"SELECT name FROM person:test",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		let res = res.unwrap().to_string();
		assert!(
			res.contains("Name"),
			"{}: {:?}",
			"anonymous user should be able to update a record if the table has full permissions",
			res
		);
	}
}
