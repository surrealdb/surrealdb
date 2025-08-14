use surrealdb_core::iam::Level;
use surrealdb_core::syn;
use surrealdb_core::val::Array;
mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::Role;

use crate::helpers::Test;

#[tokio::test]
async fn update_merge_and_content() -> Result<()> {
	let sql = "
		CREATE person:test CONTENT { name: 'Tobie' };
		UPDATE person:test CONTENT { name: 'Jaime' };
		UPDATE person:test CONTENT 'some content';
		UPDATE person:test REPLACE 'some content';
		UPDATE person:test MERGE { age: 50 };
		UPDATE person:test MERGE 'some content';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Tobie',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Jaime',
			}
		]",
	)
	.unwrap();
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
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Jaime',
				age: 50,
			}
		]",
	)
	.unwrap();
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
async fn update_simple_with_input() -> Result<()> {
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
		CREATE person:test;
		UPDATE person:test CONTENT { name: 'Tobie' };
		UPDATE person:test REPLACE { name: 'jaime' };
		UPDATE person:test MERGE { name: 'Jaime' };
		UPDATE person:test SET name = 'tobie';
		UPDATE person:test SET name = 'Tobie';
		SELECT * FROM person:test;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: jaime' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Name: Jaime',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: tobie' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn update_complex_with_input() -> Result<()> {
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
async fn update_with_return_clause() -> Result<()> {
	let sql = "
		CREATE person:test SET age = 18, name = 'John';
		UPDATE person:test SET age = 25 RETURN VALUE $before;
		UPDATE person:test SET age = 30 RETURN VALUE { old_age: $before.age, new_age: $after.age };
		UPDATE person:test SET age = 35 RETURN age, name;
		DELETE person:test RETURN VALUE $before;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				age: 18,
				id: person:test,
				name: 'John'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				age: 18,
				id: person:test,
				name: 'John'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				new_age: 30,
				old_age: 25
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				age: 35,
				name: 'John'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				age: 35,
				id: person:test,
				name: 'John'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn update_with_object_array_string_field_names() -> Result<()> {
	let sql = "
		UPSERT person:one SET field.key = 'value';
		UPSERT person:two SET field['key'] = 'value';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				field: {
					key: 'value'
				},
				id: person:one
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				field: {
					key: 'value'
				},
				id: person:two
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn update_records_and_arrays_with_json_patch() -> Result<()> {
	let sql = "
		UPSERT person:test CONTENT {
			username: 'parsley',
			bugs: [],
			biscuits: [
				{ name: 'Digestive' },
				{ name: 'Choco Leibniz' }
			]
		};
		UPDATE person:test PATCH [
			{
				op: 'add',
				path: '/bugs',
				value: 'rfc6902'
			},
			{
				op: 'add',
				path: '/biscuits/0',
				value: { name: 'Ginger Nut' }
			},
			{
				op: 'add',
				path: '/test',
				value: true,
			}
		];
		UPSERT person:test PATCH [
			{
				op: 'add',
				path: '/bugs/-',
				value: 'rfc6903'
			}
		];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				biscuits: [
					{
						name: 'Digestive'
					},
					{
						name: 'Choco Leibniz'
					}
				],
				bugs: [],
				id: person:test,
				username: 'parsley'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				biscuits: [
					{
						name: 'Ginger Nut'
					},
					{
						name: 'Digestive'
					},
					{
						name: 'Choco Leibniz'
					}
				],
				bugs: [
					'rfc6902'
				],
				id: person:test,
				test: true,
				username: 'parsley'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				biscuits: [
					{
						name: 'Ginger Nut'
					},
					{
						name: 'Digestive'
					},
					{
						name: 'Choco Leibniz'
					}
				],
				bugs: [
					'rfc6902',
					'rfc6903'
				],
				id: person:test,
				test: true,
				username: 'parsley'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
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

async fn common_permissions_checks(auth_enabled: bool) {
	let tests = vec![
		// Root level
		(
			(level_root(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at root level should be able to update a record",
		),
		(
			(level_root(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at root level should be able to update a record",
		),
		(
			(level_root(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at root level should not be able to update a record",
		),
		// Namespace level
		(
			(level_ns(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at namespace level should be able to update a record on its namespace",
		),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at namespace level should not be able to update a record on another namespace",
		),
		(
			(level_ns(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at namespace level should be able to update a record on its namespace",
		),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at namespace level should not be able to update a record on another namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at namespace level should not be able to update a record on its namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at namespace level should not be able to update a record on another namespace",
		),
		// Database level
		(
			(level_db(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at database level should be able to update a record on its database",
		),
		(
			(level_db(), Role::Owner),
			("NS", "OTHER_DB"),
			false,
			"owner at database level should not be able to update a record on another database",
		),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at database level should not be able to update a record on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at database level should be able to update a record on its database",
		),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			"editor at database level should not be able to update a record on another database",
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at database level should not be able to update a record on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "DB"),
			false,
			"viewer at database level should not be able to update a record on its database",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			"viewer at database level should not be able to update a record on another database",
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at database level should not be able to update a record on another namespace even if the database name matches",
		),
	];
	let statement = "UPDATE person:test CONTENT { name: 'Name' };";

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

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
				res.is_ok() && res.unwrap() != Array::new().into(),
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
				res.is_ok() && res.unwrap() != Array::new().into(),
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
				res.is_ok() && res.unwrap() != Array::new().into(),
				"unexpected error creating person record"
			);

			// Run the test
			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			// Select always succeeds, but the result may be empty

			if should_succeed {
				assert!(res.unwrap() != Array::new().into(), "{}", msg);

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
				assert!(res.unwrap() == Array::new().into(), "{}", msg);

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

	let statement = "UPDATE person:test CONTENT { name: 'Name' };";

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
		assert!(
			res.is_ok() && res.unwrap() != Array::new().into(),
			"{}",
			"failed to create record"
		);

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() == Array::new().into(),
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
		assert!(
			res.is_ok() && res.unwrap() != Array::new().into(),
			"{}",
			"failed to create record"
		);

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Array::new().into(),
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

	let statement = "UPDATE person:test CONTENT { name: 'Name' };";

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
		assert!(
			res.is_ok() && res.unwrap() != Array::new().into(),
			"{}",
			"failed to create record"
		);

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Array::new().into(),
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
		assert!(
			res.is_ok() && res.unwrap() != Array::new().into(),
			"{}",
			"failed to create record"
		);

		let mut resp = ds
			.execute(statement, &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Array::new().into(),
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
async fn update_field_permissions() -> Result<()> {
	let dbs = new_ds().await?;

	let sql = r#"
		DEFINE TABLE data PERMISSIONS FULL;
		DEFINE FIELD private ON data TYPE string PERMISSIONS FOR UPDATE FULL, FOR SELECT NONE;
		CREATE data:1 SET public = "public", private = "private";

		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE user PERMISSIONS FULL;
		CREATE user:1;
	"#;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: data:1,
				public: 'public',
				private: 'private'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: user:1
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	let sql = r#"
		UPDATE data:1 SET public = private;
	"#;
	let ses = Session::for_record("test", "test", "user", syn::value("user:1").unwrap());
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: data:1
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	Ok(())
}
