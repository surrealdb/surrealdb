mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_matches_using_index() -> Result<(), Error> {
	let sql = r"
		CREATE blog:1 SET title = 'Hello World!';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		SELECT id, search::highlight('<em>', '</em>', 1) AS title FROM blog WHERE title @1@ 'Hello' EXPLAIN;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				title: '<em>Hello</em> World!'
			},
			{
				explain:
				[
					{
						detail: {
							plan: {
								index: 'blog_title',
								operator: '@1@',
								value: 'Hello'
							},
							table: 'blog',
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
async fn select_where_matches_without_using_index_iterator() -> Result<(), Error> {
	let sql = r"
		CREATE blog:1 SET title = 'Hello World!';
		CREATE blog:2 SET title = 'Foo Bar!';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		SELECT id,search::highlight('<em>', '</em>', 1) AS title FROM blog WHERE (title @0@ 'hello' AND id>0) OR (title @1@ 'world' AND id<99) EXPLAIN;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None, false).await?;
	assert_eq!(res.len(), 5);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				title: 'Hello <em>World</em>!'
			},
			{
				explain:
				[
					{
						detail: {
							table: 'blog',
						},
						operation: 'Iterate Table'
					}
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_matches_using_index_and_arrays() -> Result<(), Error> {
	let sql = r"
		CREATE blog:1 SET content = ['Hello World!', 'Be Bop', 'Foo Bar'];
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_content ON blog FIELDS content SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'Hello Bar';
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				content: [
					'<em>Hello</em> World!',
					'Be Bop',
					'Foo <em>Bar</em>'
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_matches_using_index_and_arrays_and_offsets() -> Result<(), Error> {
	let sql = r"
		CREATE blog:1 SET content = ['Hello World!', 'Be Bop', 'Foo Bar'];
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_content ON blog FIELDS content SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		SELECT id, search::offsets(1) AS offsets FROM blog WHERE content @1@ 'Hello Bar';
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				offsets: {
					0: [{s:0, e:5}],
					2: [{s:4, e:7}]
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	Ok(())
}
