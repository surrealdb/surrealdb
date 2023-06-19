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
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
async fn select_where_and_with_index() -> Result<(), Error> {
	let sql = "
		CREATE person:tobie SET name = 'Tobie', genre='m';
		CREATE person:jaime SET name = 'Jaime', genre='m';
		DEFINE INDEX person_name ON TABLE person COLUMNS name;
		SELECT name FROM person WHERE name = 'Tobie' AND genre = 'm' EXPLAIN;";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Tobie'
			},
			{
				explain:
				[
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
				]
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
		SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' EXPLAIN;";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Jaime'
			},
			{
				explain:
				[
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
				]
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
		SELECT name FROM person WHERE name @@ 'Jaime' AND genre = 'm' EXPLAIN;";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 5);
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
				name: 'Jaime'
			},
			{
				explain:
				[
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
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}
