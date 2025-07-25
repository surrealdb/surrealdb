mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb::dbs::Session;
use surrealdb_core::syn;

#[tokio::test]
async fn use_statement_set_ns() -> Result<()> {
	let sql = "
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
		USE NS my_ns;
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("['test', 'test', 'test', 'test']").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("['my_ns', 'my_ns', 'test', 'test']").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn use_statement_set_db() -> Result<()> {
	let sql = "
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
		USE DB my_db;
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("['test', 'test', 'test', 'test']").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("['test', 'test', 'my_db', 'my_db']").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn use_statement_set_both() -> Result<()> {
	let sql = "
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
		USE NS my_ns DB my_db;
		SELECT * FROM $session.ns, session::ns(), $session.db, session::db();
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("['test', 'test', 'test', 'test']").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("['my_ns', 'my_ns', 'my_db', 'my_db']").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}
