mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::iam::Role;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_field_value() -> Result<(), Error> {
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
			'Jaime',
			'Tobie',
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Jaime'
			},
			{
				name: 'Tobie'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_field_and_omit() -> Result<(), Error> {
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
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_expression_value() -> Result<(), Error> {
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
	let val = Value::parse(
		"[
			{
				boolean: true,
				id: thing:a,
				number: 5
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				boolean: false,
				id: thing:b,
				number: -5
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			-5,
			5,
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			false,
			true
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
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
						count: 2,
					},
					operation: 'Fetch'
				}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_dynamic_array_keys_and_object_keys() -> Result<(), Error> {
	let sql = "
		LET $lang = 'en';
		UPDATE documentation:test CONTENT {
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
		UPDATE documentation:test SET languages['en'] = 'my primary text';
		-- Selecting an object value or array index using a parameter as a key
		SELECT languages[$lang] AS content FROM documentation:test;
		-- Updating an object value or array index using a parameter as a key
		UPDATE documentation:test SET languages[$lang] = 'my secondary text';
		-- Selecting an object or array index value using the value of another document field as a key
		SELECT languages[primarylang] AS content FROM documentation;
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				tags: {
					value: 'client-side'
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				content: 'this is english'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				content: 'my primary text'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				content: 'my secondary text'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_writeable_subqueries() -> Result<(), Error> {
	let sql = "
		LET $id = (UPDATE tester:test);
		RETURN $id;
		LET $id = (UPDATE tester:test).id;
		RETURN $id;
		LET $id = (SELECT VALUE id FROM (UPDATE tester:test))[0];
		RETURN $id;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: tester:test
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[tester:test]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("tester:test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_where_field_is_bool() -> Result<(), Error> {
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
	let val = Value::parse(
		"[
			{
				id: test:1,
				active: false
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:2,
				active: false
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:3,
				active: true
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:3,
				active: true
			}
		]",
	);
	assert_eq!(tmp, val);

	Ok(())
}

#[tokio::test]
async fn select_where_field_is_thing_and_with_index() -> Result<(), Error> {
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
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
						count: 2,
					},
					operation: 'Fetch'
				}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_and_with_index() -> Result<(), Error> {
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
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Tobie'
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_and_with_unique_index() -> Result<(), Error> {
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
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Jaime'
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_and_with_fulltext_index() -> Result<(), Error> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie', genre='m';
		CREATE person:jaime SET name = 'Jaime', genre='m';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX ft_name ON TABLE person COLUMNS name SEARCH ANALYZER simple BM25(1.2,0.75);
		SELECT name FROM person WHERE name @@ 'Jaime' AND genre = 'm' EXPLAIN;
		SELECT name FROM person WHERE name @@ 'Jaime' AND genre = 'm';";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
						plan: {
							index: 'ft_name',
							operator: '@@',
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Jaime'
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_explain() -> Result<(), Error> {
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
	let val = Value::parse(
		"[
				{
					detail: {
						table: 'person',
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
						table: 'person',
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
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
						count: 3,
					},
					operation: 'Fetch'
				},
			]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_with_function_field() -> Result<(), Error> {
	let sql = "SELECT *, function() { return this.a } AS b FROM [{ a: 1 }];";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ a: 1, b: 1 }]");
	assert_eq!(tmp, val);
	Ok(())
}

//
// Permissions
//

async fn common_permissions_checks(auth_enabled: bool) {
	let tests = vec![
		// Root level
		((().into(), Role::Owner), ("NS", "DB"), true, "owner at root level should be able to select"),
		((().into(), Role::Editor), ("NS", "DB"), true, "editor at root level should be able to select"),
		((().into(), Role::Viewer), ("NS", "DB"), true, "viewer at root level should not be able to select"),

		// Namespace level
		((("NS",).into(), Role::Owner), ("NS", "DB"), true, "owner at namespace level should be able to select on its namespace"),
		((("NS",).into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at namespace level should not be able to select on another namespace"),
		((("NS",).into(), Role::Editor), ("NS", "DB"), true, "editor at namespace level should be able to select on its namespace"),
		((("NS",).into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at namespace level should not be able to select on another namespace"),
		((("NS",).into(), Role::Viewer), ("NS", "DB"), true, "viewer at namespace level should not be able to select on its namespace"),
		((("NS",).into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at namespace level should not be able to select on another namespace"),

		// Database level
		((("NS", "DB").into(), Role::Owner), ("NS", "DB"), true, "owner at database level should be able to select on its database"),
		((("NS", "DB").into(), Role::Owner), ("NS", "OTHER_DB"), false, "owner at database level should not be able to select on another database"),
		((("NS", "DB").into(), Role::Owner), ("OTHER_NS", "DB"), false, "owner at database level should not be able to select on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Editor), ("NS", "DB"), true, "editor at database level should be able to select on its database"),
		((("NS", "DB").into(), Role::Editor), ("NS", "OTHER_DB"), false, "editor at database level should not be able to select on another database"),
		((("NS", "DB").into(), Role::Editor), ("OTHER_NS", "DB"), false, "editor at database level should not be able to select on another namespace even if the database name matches"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "DB"), true, "viewer at database level should not be able to select on its database"),
		((("NS", "DB").into(), Role::Viewer), ("NS", "OTHER_DB"), false, "viewer at database level should not be able to select on another database"),
		((("NS", "DB").into(), Role::Viewer), ("OTHER_NS", "DB"), false, "viewer at database level should not be able to select on another namespace even if the database name matches"),
	];
	let statement = "SELECT * FROM person";

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

			// Select always succeeds, but the result may be empty
			assert!(res.is_ok());

			if should_succeed {
				assert!(res.unwrap() != Value::parse("[]"), "{}", msg);
			} else {
				assert!(res.unwrap() == Value::parse("[]"), "{}", msg);
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
			res.unwrap() == Value::parse("[]"),
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
			res.unwrap() != Value::parse("[]"),
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
			res.unwrap() != Value::parse("[]"),
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
			res.unwrap() != Value::parse("[]"),
			"{}",
			"anonymous user should be able to select if the table has full permissions"
		);
	}
}

#[tokio::test]
async fn select_only() -> Result<(), Error> {
	let sql: &str = "
		SELECT * FROM ONLY 1;
		SELECT * FROM ONLY NONE;
		SELECT * FROM ONLY [];
		SELECT * FROM ONLY [1];
		SELECT * FROM ONLY [1, 2];
		SELECT * FROM ONLY [] LIMIT 1;
		SELECT * FROM ONLY [1] LIMIT 1;
		SELECT * FROM ONLY [1, 2] LIMIT 1;
		SELECT * FROM ONLY 1, 2;
		SELECT * FROM ONLY 1, 2 LIMIT 1;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("NONE");
	assert_eq!(tmp, val);
	//
	match res.remove(0).result {
		Err(surrealdb::error::Db::SingleOnlyOutput) => (),
		_ => panic!("Query should have failed with error: Expected a single result output when using the ONLY keyword")
	}
	//
	match res.remove(0).result {
		Err(surrealdb::error::Db::SingleOnlyOutput) => (),
		_ => panic!("Query should have failed with error: Expected a single result output when using the ONLY keyword")
	}
	//
	match res.remove(0).result {
		Err(surrealdb::error::Db::SingleOnlyOutput) => (),
		_ => panic!("Query should have failed with error: Expected a single result output when using the ONLY keyword")
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("NONE");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1");
	assert_eq!(tmp, val);
	//
	match res.remove(0).result {
		Err(surrealdb::error::Db::SingleOnlyOutput) => (),
		_ => panic!("Query should have failed with error: Expected a single result output when using the ONLY keyword")
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_issue_3510() -> Result<(), Error> {
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
	let val = Value::parse(
		"[
				{
					link: {
						id: a:1
					}
				}
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}
