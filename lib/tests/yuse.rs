mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn use_statement_set_ns() -> Result<(), Error> {
	let sql = "
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
		USE NS my_ns;
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['test', 'test', 'test', 'test']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['my_ns', 'my_ns', 'test', 'test']");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn use_statement_set_db() -> Result<(), Error> {
	let sql = "
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
		USE DB my_db;
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['test', 'test', 'test', 'test']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['test', 'test', 'my_db', 'my_db']");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn use_statement_set_both() -> Result<(), Error> {
	let sql = "
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
		USE NS my_ns DB my_db;
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['test', 'test', 'test', 'test']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['my_ns', 'my_ns', 'my_db', 'my_db']");
	assert_eq!(tmp, val);
	//
	Ok(())
}
