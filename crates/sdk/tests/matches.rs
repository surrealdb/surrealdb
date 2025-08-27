mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_core::val::{Array, Value};

use crate::helpers::{Test, skip_ok};

#[tokio::test]
async fn select_where_matches_partial_highlight() -> Result<()> {
	let sql = r"
		CREATE blog:1 SET content = 'Hello World!';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,edgengram(2,100);
		DEFINE INDEX blog_content ON blog FIELDS content SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::highlight('<em>', '</em>', 1, false) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::highlight('<em>', '</em>', 1, true) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::offsets(1) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::offsets(1, false) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::offsets(1, true) AS content FROM blog WHERE content @1@ 'he';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	skip_ok(res, 3)?;
	//
	for i in 0..2 {
		let tmp = res.remove(0).result?;
		let val = syn::value(
			"[
			{
				id: blog:1,
				content: '<em>Hello</em> World!'
			}
		]",
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: blog:1,
				content: '<em>He</em>llo World!'
			}
		]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	for i in 0..2 {
		let tmp = res.remove(0).result?;
		let val = syn::value(
			"[
					{
						content: {
							0: [
								{
									e: 5,
									s: 0
								}
							]
						},
						id: blog:1
					}
				]",
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
					{
						content: {
							0: [
								{
									e: 2,
									s: 0
								}
							]
						},
						id: blog:1
					}
				]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_partial_highlight_ngram() -> Result<()> {
	let sql = r"
		CREATE blog:1 SET content = 'Hello World!';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,ngram(1,32);
		DEFINE INDEX blog_content ON blog FIELDS content SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'Hello';
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::highlight('<em>', '</em>', 1, false) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::highlight('<em>', '</em>', 1, true) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::offsets(1) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::offsets(1, false) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::offsets(1, true) AS content FROM blog WHERE content @1@ 'el';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	//
	skip_ok(res, 3)?;
	//
	for i in 0..3 {
		let tmp = res.remove(0).result?;
		let val = syn::value(
			"[
			{
				id: blog:1,
				content: '<em>Hello</em> World!'
			}
		]",
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: blog:1,
				content: 'H<em>el</em>lo World!'
			}
		]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	for i in 0..2 {
		let tmp = res.remove(0).result?;
		let val = syn::value(
			"[
					{
						content: {
							0: [
								{
									e: 5,
									s: 0
								}
							]
						},
						id: blog:1
					}
				]",
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
					{
						content: {
							0: [
								{
									e: 3,
									s: 1
								}
							]
						},
						id: blog:1
					}
				]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_using_index_offsets() -> Result<()> {
	let sql = r"
		CREATE blog:1 SET title = 'Blog title!', content = ['Hello World!', 'Be Bop', 'Foo Bãr'];
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		DEFINE INDEX blog_content ON blog FIELDS content SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id, search::offsets(0) AS title, search::offsets(1) AS content FROM blog WHERE title @0@ 'title' AND content @1@ 'Hello Bãr';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	skip_ok(res, 4)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: blog:1,
				title: {
					0: [{s:5, e:10}],
				},
				content: {
					0: [{s:0, e:5}],
					2: [{s:4, e:7}]
				}
			}
		]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_using_index_and_score() -> Result<()> {
	let sql = r"
		CREATE blog:1 SET title = 'the quick brown fox jumped over the lazy dog';
		CREATE blog:2 SET title = 'the fast fox jumped over the lazy dog';
		CREATE blog:3 SET title = 'the other animals sat there watching';
		CREATE blog:4 SET title = 'the dog sat there and did nothing';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		SELECT id,search::score(1) AS score FROM blog WHERE title @1@ 'animals';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	skip_ok(res, 6)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: blog:3,
				score: 0.9227996468544006
			}
		]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_without_using_index_and_score() -> Result<()> {
	let sql = r"
		CREATE blog:1 SET title = 'the quick brown fox jumped over the lazy dog', label = 'test';
		CREATE blog:2 SET title = 'the fast fox jumped over the lazy dog', label = 'test';
		CREATE blog:3 SET title = 'the other animals sat there watching', label = 'test';
		CREATE blog:4 SET title = 'the dog sat there and did nothing', label = 'test';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		LET $keywords = 'animals';
 		SELECT id,search::score(1) AS score FROM blog
 			WHERE (title @1@ $keywords AND label = 'test')
 			OR (title @1@ $keywords AND label = 'test');
		SELECT id,search::score(1) + search::score(2) AS score FROM blog
			WHERE (title @1@ 'dummy1' AND label = 'test')
			OR (title @2@ 'dummy2' AND label = 'test');
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	skip_ok(res, 7)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: blog:3,
				score: 0.9227996468544006
			}
		]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));

	// This result should be empty, as we are looking for non-existing terms (dummy1
	// and dummy2).
	let tmp = res.remove(0).result?;
	let val: Value = Array::new().into();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_analyser_without_tokenizer() -> Result<()> {
	let sql = r"
		DEFINE ANALYZER az FILTERS lowercase,ngram(1,5);
		CREATE t:1 SET text = 'ab';
		DEFINE INDEX search_idx ON TABLE t COLUMNS text SEARCH ANALYZER az BM25 HIGHLIGHTS;
		SELECT * FROM t WHERE text @@ 'a';";
	let mut t = Test::new(sql).await?;
	t.expect_size(4)?;
	t.skip_ok(3)?;
	t.expect_val("[{ id: t:1, text: 'ab' }]")?;
	Ok(())
}

#[tokio::test]
async fn select_where_matches_analyser_with_mapper() -> Result<()> {
	let sql = r"
		DEFINE ANALYZER mapper TOKENIZERS blank,class FILTERS lowercase,mapper('../../tests/data/lemmatization-en.txt');
		CREATE t:1 SET text = 'He drives to work every day, taking the scenic route through town';
		DEFINE INDEX search_idx ON TABLE t COLUMNS text SEARCH ANALYZER mapper BM25;
		SELECT * FROM t WHERE text @@ 'driven'";
	let mut t = Test::new(sql).await?;
	t.expect_size(4)?;
	t.skip_ok(3)?;
	t.expect_val(
		"[{ id: t:1, text: 'He drives to work every day, taking the scenic route through town' }]",
	)?;
	// Reload the database
	let mut t = t
		.restart(
			r"
		SELECT * FROM t WHERE text @@ 'driven';
		REMOVE INDEX search_idx ON TABLE t;
		REMOVE ANALYZER mapper",
		)
		.await?;
	t.expect_size(3)?;
	t.expect_val(
		"[{ id: t:1, text: 'He drives to work every day, taking the scenic route through town' }]",
	)?;
	t.skip_ok(2)?;
	Ok(())
}
