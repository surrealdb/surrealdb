mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ something: 'other' }]");
	assert_eq!(tmp, val);
	//
	Ok(())
}
