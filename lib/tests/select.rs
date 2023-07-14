mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_field_value() -> Result<(), Error> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie';
		CREATE person:jaime SET name = 'Jaime';
		SELECT VALUE name FROM person;
		SELECT name FROM person;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
async fn select_expression_value() -> Result<(), Error> {
	let sql = "
		CREATE thing:a SET number = 5, boolean = true;
		CREATE thing:b SET number = -5, boolean = false;
		SELECT VALUE -number FROM thing;
		SELECT VALUE !boolean FROM thing;
		SELECT VALUE !boolean FROM thing EXPLAIN FULL;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			id: tester:test
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("tester:test");
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

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
