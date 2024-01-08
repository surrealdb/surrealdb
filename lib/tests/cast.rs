mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::sql::Value;
use surrealdb_sql::dbs::Session;
use surrealdb_sql::err::Error;

#[tokio::test]
async fn cast_string_to_record() -> Result<(), Error> {
	let sql = r#"
		<record> <string> a:1
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("a:1");
	assert_eq!(tmp, val);
	//
	Ok(())
}
