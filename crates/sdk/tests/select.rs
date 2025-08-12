mod helpers;
use helpers::{Test, new_ds};
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::{Level, Role};
use surrealdb_core::syn;
use surrealdb_core::val::{Array, Number, Value};

#[tokio::test]
async fn select_field_value() -> Result<()> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie';
		CREATE person:jaime SET name = 'Jaime';
		SELECT VALUE name FROM person;
		SELECT name FROM person;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:tobie,
				name: 'Tobie'
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
				id: person:jaime,
				name: 'Jaime'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			'Jaime',
			'Tobie',
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				name: 'Jaime'
			},
			{
				name: 'Tobie'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_field_and_omit() -> Result<()> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie', password = '123456', opts.security = 'secure';
		CREATE person:jaime SET name = 'Jaime', password = 'asdfgh', opts.security = 'secure';
		SELECT * OMIT password, opts.security FROM person;
		SELECT * FROM person;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:tobie,
				name: 'Tobie',
				password: '123456',
				opts: {
					security: 'secure',
				},
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
				id: person:jaime,
				name: 'Jaime',
				password: 'asdfgh',
				opts: {
					security: 'secure',
				},
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
				id: person:jaime,
				name: 'Jaime',
				opts: {},
			},
			{
				id: person:tobie,
				name: 'Tobie',
				opts: {},
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
				id: person:jaime,
				name: 'Jaime',
				password: 'asdfgh',
				opts: {
					security: 'secure',
				},
			},
			{
				id: person:tobie,
				name: 'Tobie',
				password: '123456',
				opts: {
					security: 'secure',
				},
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_expression_value() -> Result<()> {
	let sql = "
		CREATE thing:a SET number = 5, boolean = true;
		CREATE thing:b SET number = -5, boolean = false;
		SELECT VALUE -number FROM thing;
		SELECT VALUE !boolean FROM thing;
		SELECT VALUE !boolean FROM thing EXPLAIN FULL;
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
				boolean: true,
				id: thing:a,
				number: 5
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
				boolean: false,
				id: thing:b,
				number: -5
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			-5,
			5,
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			false,
			true
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					detail: {
                        direction: 'forward',
						table: 'thing',
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				},
				{
					detail: {
						type: 'KeysAndValues'
					},
					operation: 'RecordStrategy'
				},
				{
					detail: {
						count: 2,
					},
					operation: 'Fetch'
				}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_dynamic_array_keys_and_object_keys() -> Result<()> {
	let sql = "
		LET $lang = 'en';
		UPSERT documentation:test CONTENT {
			primarylang: 'en',
			languages: {
				'en': 'this is english',
				'es': 'esto es español',
				'de': 'das ist Englisch',
			},
			tags: [
				{ type: 'library', value: 'client-side' },
				{ type: 'library', value: 'server-side' },
				{ type: 'environment', value: 'frontend' },
			]
		};
		-- An array filter, followed by an array index operation
		SELECT tags[WHERE type = 'library'][0].value FROM documentation:test;
		-- Selecting an object value or array index using a string as a key
		SELECT languages['en'] AS content FROM documentation:test;
		-- Updating an object value or array index using a string as a key
		UPSERT documentation:test SET languages['en'] = 'my primary text';
		-- Selecting an object value or array index using a parameter as a key
		SELECT languages[$lang] AS content FROM documentation:test;
		-- Updating an object value or array index using a parameter as a key
		UPSERT documentation:test SET languages[$lang] = 'my secondary text';
		-- Selecting an object or array index value using the value of another document field as a key
		SELECT languages[primarylang] AS content FROM documentation;
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
				id: documentation:test,
				languages: {
					de: 'das ist Englisch',
					en: 'this is english',
					es: 'esto es español',
				},
				primarylang: 'en',
				tags: [
					{
						type: 'library',
						value: 'client-side',
					},
					{
						type: 'library',
						value: 'server-side',
					},
					{
						type: 'environment',
						value: 'frontend',
					}
				]
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
				tags: {
					value: 'client-side'
				}
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
				content: 'this is english'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				content: 'my primary text'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				content: 'my secondary text'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_writeable_subqueries() -> Result<()> {
	let sql = "
		LET $id = (UPSERT tester:test);
		RETURN $id;
		LET $id = (UPSERT tester:test).id;
		RETURN $id;
		LET $id = (SELECT VALUE id FROM (UPSERT tester:test))[0];
		RETURN $id;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: tester:test
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[tester:test]").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("tester:test").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_where_field_is_bool() -> Result<()> {
	let sql = "
		CREATE test:1 SET active = false;
		CREATE test:2 SET active = false;
		CREATE test:3 SET active = true;
		SELECT * FROM test WHERE active = false;
		SELECT * FROM test WHERE active != true;
		SELECT * FROM test WHERE active = true;
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
				id: test:1,
				active: false
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
				id: test:2,
				active: false
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
				id: test:3,
				active: true
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
				id: test:1,
				active: false
			},
			{
				id: test:2,
				active: false
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
				id: test:1,
				active: false
			},
			{
				id: test:2,
				active: false
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
				id: test:3,
				active: true
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	Ok(())
}

#[tokio::test]
async fn select_where_field_is_thing_and_with_index() -> Result<()> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie';
		DEFINE INDEX author ON TABLE post COLUMNS author;
		CREATE post:1 SET author = person:tobie;
		CREATE post:2 SET author = person:tobie;
		SELECT * FROM post WHERE author = person:tobie EXPLAIN;
		SELECT * FROM post WHERE author = person:tobie EXPLAIN FULL;
		SELECT * FROM post WHERE author = person:tobie;";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					detail: {
						plan: {
							index: 'author',
							operator: '=',
							value: person:tobie
						},
						table: 'post',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
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
					detail: {
						plan: {
							index: 'author',
							operator: '=',
							value: person:tobie
						},
						table: 'post',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				},
				{
					detail: {
						type: 'KeysAndValues'
					},
					operation: 'RecordStrategy'
				},
				{
					detail: {
						count: 2,
					},
					operation: 'Fetch'
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
				author: person:tobie,
				id: post:1
			},
			{
				author: person:tobie,
				id: post:2
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_and_with_index() -> Result<()> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie', genre='m';
		CREATE person:jaime SET name = 'Jaime', genre='m';
		DEFINE INDEX person_name ON TABLE person COLUMNS name;
		SELECT name FROM person WHERE name = 'Tobie' AND genre = 'm' EXPLAIN;
		SELECT name FROM person WHERE name = 'Tobie' AND genre = 'm';";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					detail: {
						plan: {
							index: 'person_name',
							operator: '=',
							value: 'Tobie'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
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
				name: 'Tobie'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_and_with_unique_index() -> Result<()> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie', genre='m';
		CREATE person:jaime SET name = 'Jaime', genre='m';
		DEFINE INDEX person_name ON TABLE person COLUMNS name UNIQUE;
		SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' EXPLAIN;
		SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm';";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					detail: {
						plan: {
							index: 'person_name',
							operator: '=',
							value: 'Jaime'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
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
				name: 'Jaime'
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_explain() -> Result<()> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie';
		CREATE person:jaime SET name = 'Jaime';
		CREATE software:surreal SET name = 'SurrealDB';
		SELECT * FROM person,software EXPLAIN;
		SELECT * FROM person,software EXPLAIN FULL;";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					detail: {
                        direction: 'forward',
						table: 'person',
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
                        direction: 'forward',
						table: 'software',
					},
					operation: 'Iterate Table'
				},
                {
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				},
			]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					detail: {
                        direction: 'forward',
						table: 'person',
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
                        direction: 'forward',
						table: 'software',
					},
					operation: 'Iterate Table'
				},
                {
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				},
				{
					detail: {
						type: 'KeysAndValues'
					},
					operation: 'RecordStrategy'
				},
				{
					detail: {
						count: 3,
					},
					operation: 'Fetch'
				},
			]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_with_function_field() -> Result<()> {
	let sql = "SELECT *, function() { return this.a } AS b FROM [{ a: 1 }];";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ a: 1, b: 1 }]").unwrap();
	assert_eq!(tmp, val);
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
			"owner at root level should be able to select",
		),
		(
			(level_root(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at root level should be able to select",
		),
		(
			(level_root(), Role::Viewer),
			("NS", "DB"),
			true,
			"viewer at root level should not be able to select",
		),
		// Namespace level
		(
			(level_ns(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at namespace level should be able to select on its namespace",
		),
		(
			(level_ns(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at namespace level should not be able to select on another namespace",
		),
		(
			(level_ns(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at namespace level should be able to select on its namespace",
		),
		(
			(level_ns(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at namespace level should not be able to select on another namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("NS", "DB"),
			true,
			"viewer at namespace level should not be able to select on its namespace",
		),
		(
			(level_ns(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at namespace level should not be able to select on another namespace",
		),
		// Database level
		(
			(level_db(), Role::Owner),
			("NS", "DB"),
			true,
			"owner at database level should be able to select on its database",
		),
		(
			(level_db(), Role::Owner),
			("NS", "OTHER_DB"),
			false,
			"owner at database level should not be able to select on another database",
		),
		(
			(level_db(), Role::Owner),
			("OTHER_NS", "DB"),
			false,
			"owner at database level should not be able to select on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Editor),
			("NS", "DB"),
			true,
			"editor at database level should be able to select on its database",
		),
		(
			(level_db(), Role::Editor),
			("NS", "OTHER_DB"),
			false,
			"editor at database level should not be able to select on another database",
		),
		(
			(level_db(), Role::Editor),
			("OTHER_NS", "DB"),
			false,
			"editor at database level should not be able to select on another namespace even if the database name matches",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "DB"),
			true,
			"viewer at database level should not be able to select on its database",
		),
		(
			(level_db(), Role::Viewer),
			("NS", "OTHER_DB"),
			false,
			"viewer at database level should not be able to select on another database",
		),
		(
			(level_db(), Role::Viewer),
			("OTHER_NS", "DB"),
			false,
			"viewer at database level should not be able to select on another namespace even if the database name matches",
		),
	];
	let statement = "SELECT * FROM person";

	let empty_array = syn::value("[]").unwrap();

	for ((level, role), (ns, db), should_succeed, msg) in tests.into_iter() {
		let sess = Session::for_level(level, role).with_ns(ns).with_db(db);

		{
			let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

			// Prepare datastore
			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != empty_array,
				"unexpected error creating person record"
			);
			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("OTHER_NS").with_db("DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != empty_array,
				"unexpected error creating person record"
			);
			let mut resp = ds
				.execute("CREATE person", &Session::owner().with_ns("NS").with_db("OTHER_DB"), None)
				.await
				.unwrap();
			let res = resp.remove(0).output();
			assert!(
				res.is_ok() && res.unwrap() != empty_array,
				"unexpected error creating person record"
			);

			// Run the test
			let mut resp = ds.execute(statement, &sess, None).await.unwrap();
			let res = resp.remove(0).output();

			// Select always succeeds, but the result may be empty
			let res = res.unwrap();

			if should_succeed {
				assert!(res != empty_array, "{}", msg);
			} else {
				assert!(res == empty_array, "{}", msg);
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

	// When the table grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE; CREATE person;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute("SELECT * FROM person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() == Array::new().into(),
			"{}",
			"anonymous user should not be able to select if the table has no permissions"
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL; CREATE person;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute("SELECT * FROM person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Array::new().into(),
			"{}",
			"anonymous user should be able to select if the table has full permissions"
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

	// When the table grants no permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS NONE; CREATE person;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute("SELECT * FROM person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Array::new().into(),
			"{}",
			"anonymous user should be able to select if the table has no permissions"
		);
	}

	// When the table exists and grants full permissions
	{
		let ds = new_ds().await.unwrap().with_auth_enabled(auth_enabled);

		let mut resp = ds
			.execute(
				"DEFINE TABLE person PERMISSIONS FULL; CREATE person;",
				&Session::owner().with_ns("NS").with_db("DB"),
				None,
			)
			.await
			.unwrap();
		let res = resp.remove(0).output();
		assert!(res.is_ok(), "failed to create table: {:?}", res);

		let mut resp = ds
			.execute("SELECT * FROM person", &Session::default().with_ns("NS").with_db("DB"), None)
			.await
			.unwrap();
		let res = resp.remove(0).output();

		assert!(
			res.unwrap() != Array::new().into(),
			"{}",
			"anonymous user should be able to select if the table has full permissions"
		);
	}
}

#[tokio::test]
async fn select_issue_3510() -> Result<()> {
	let sql: &str = "
		CREATE a:1;
		CREATE b:1 SET link = a:1, num = 1;
		SELECT link.* FROM b;
		SELECT link.* FROM b WHERE num = 1;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					link: {
						id: a:1
					}
				}
			]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_destructure() -> Result<()> {
	let sql = "
		CREATE person:1 SET name = 'John', age = 21, obj = { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } };
		SELECT obj.{ a, c.{ e, f } } FROM person;
		SELECT * OMIT obj.c.{ d, f } FROM person;
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
				id: person:1,
				name: 'John',
				age: 21,
				obj: {
                    a: 1,
                    b: 2,
                    c: {
                        d: 3,
                        e: 4,
                        f: 5
                    }
                }
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
				obj: {
                    a: 1,
                    c: {
                        e: 4,
                        f: 5
                    }
                }
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
				id: person:1,
				name: 'John',
				age: 21,
				obj: {
                    a: 1,
                    b: 2,
                    c: { e: 4 }
                }
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_field_from_graph_no_flattening() -> Result<()> {
	let sql = "
        CREATE a:1, a:2;

        RELATE a:1->b:1->a:2 SET list = [1, 2, 3];
        RELATE a:1->b:2->a:2 SET list = [4, 5, 6];

        SELECT VALUE ->b.list FROM a:1;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
    		{ id: a:1 },
    		{ id: a:2 }
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: b:1,
				in: a:1,
				out: a:2,
				list: [1, 2, 3]
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
				id: b:2,
				in: a:1,
				out: a:2,
				list: [4, 5, 6]
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
		    [
    			[
    			    1,
                    2,
                    3
                ],
                [
                    4,
                    5,
                    6
                ]
			]
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_field_value_permissions() -> Result<()> {
	let dbs = new_ds().await?;

	let sql = r#"
		DEFINE TABLE data PERMISSIONS FULL;
		DEFINE FIELD private ON data TYPE string PERMISSIONS FOR SELECT NONE;
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
		SELECT * FROM data WHERE id = data:1;
		SELECT private AS public FROM data WHERE id = data:1;
		SELECT public FROM data WHERE private = "private";
		SELECT VALUE private FROM data WHERE id = data:1;
	"#;
	let ses = Session::for_record("test", "test", "user", syn::value("user:1").unwrap());
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: data:1,
				public: 'public'
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
				public: NONE
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Array::new().into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[NONE]").unwrap();
	assert_eq!(tmp, val);

	Ok(())
}

#[tokio::test]
async fn select_order_by_rand_large() -> Result<()> {
	let dbs = new_ds().await?;

	let sql = r#"
		let $array = <array> 0..1000;
		SELECT * FROM $array ORDER BY RAND()
	"#;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	let _ = res.remove(0).result?;

	let v = res.remove(0).result.unwrap();
	let Value::Array(x) = v else {
		panic!("not the right type");
	};

	let x: Vec<_> = x
		.into_iter()
		.map(|x| {
			let Value::Number(Number::Int(x)) = x else {
				panic!("not the right type");
			};
			x
		})
		.collect();

	// It is technically possible that the array was shuffeled in such a way that it
	// ends up with the original order but, if properly shuffeled, that chance
	// should be so small the it will effectively never happens.
	assert!(
		!x.iter().enumerate().all(|(idx, v)| idx as i64 == *v),
		"array was still in original order"
	);

	for i in 0..1000 {
		assert!(x.contains(&i))
	}

	Ok(())
}

#[tokio::test]
async fn select_from_none() -> Result<()> {
	let sql: &str = "
		SELECT * FROM NONE;
		SELECT * FROM NULL;
		SELECT 'A' FROM NONE;
		SELECT 'A' FROM NULL;
		SELECT * FROM NONE, NONE;
		SELECT * FROM NULL, NULL;
		SELECT 'A' FROM NONE, NONE;
		SELECT 'A' FROM NULL, NULL;
		SELECT * FROM [NONE, NONE];
		SELECT * FROM [NULL, NULL];
		SELECT 'A' FROM [NONE, NONE];
		SELECT 'A' FROM [NULL, NULL];
	";
	let mut t = Test::new(sql).await?;
	for i in 0..12 {
		t.expect_val_info("[]", i)?;
	}
	Ok(())
}
