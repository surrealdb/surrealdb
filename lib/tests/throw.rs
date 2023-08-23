mod parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;

#[tokio::test]
async fn throw_basic() -> Result<(), Error> {
	let sql = "
		THROW 'there was an error';
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"An error occurred: there was an error"#
	));
	//
	Ok(())
}
