mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_matches() -> Result<(), Error> {
	let sql = r#"
		CREATE blog:1 SET title = 'Hello World!';
		DEFINE ANALYZER english TOKENIZERS space,case FILTERS lowercase,snowball(english);
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH english BM25(1.2,0.75,100);
		SELECT * FROM blog WHERE title @@ 'Hello';
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: 1,
				title: 'Hello World!'
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}
