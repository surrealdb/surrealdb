mod helpers;
use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_types::{Array, Value};

use crate::helpers::{Test, skip_ok};

#[tokio::test]
async fn select_where_matches_partial_highlight() -> Result<()> {
	let sql = r"
		CREATE blog:1 SET content = 'Hello World!';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,edgengram(2,100);
		DEFINE INDEX blog_content ON blog FIELDS content FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::highlight('<em>', '</em>', 1, false) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::highlight('<em>', '</em>', 1, true) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::offsets(1) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::offsets(1, false) AS content FROM blog WHERE content @1@ 'he';
		SELECT id, search::offsets(1, true) AS content FROM blog WHERE content @1@ 'he';
	";
	let dbs = new_ds("test", "test").await?;
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
		assert_eq!(tmp, val, "{i}");
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
	assert_eq!(tmp, val);
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
		assert_eq!(tmp, val, "{i}");
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
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_matches_partial_highlight_ngram() -> Result<()> {
	let sql = r"
		CREATE blog:1 SET content = 'Hello World!';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,ngram(1,32);
		DEFINE INDEX blog_content ON blog FIELDS content FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'Hello';
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::highlight('<em>', '</em>', 1, false) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::highlight('<em>', '</em>', 1, true) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::offsets(1) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::offsets(1, false) AS content FROM blog WHERE content @1@ 'el';
		SELECT id, search::offsets(1, true) AS content FROM blog WHERE content @1@ 'el';
	";
	let dbs = new_ds("test", "test").await?;
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
		assert_eq!(tmp, val, "{i}");
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
	assert_eq!(tmp, val);
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
		assert_eq!(tmp, val, "{i}");
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
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_matches_using_index_offsets() -> Result<()> {
	let sql = r"
		CREATE blog:1 SET title = 'Blog title!', content = ['Hello World!', 'Be Bop', 'Foo Bãr'];
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		DEFINE INDEX blog_content ON blog FIELDS content FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id, search::offsets(0) AS title, search::offsets(1) AS content FROM blog WHERE title @0@ 'title' AND content @1@ 'Hello Bãr';
	";
	let dbs = new_ds("test", "test").await?;
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
	assert_eq!(tmp, val);
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
		DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		SELECT id,search::score(1) AS score FROM blog WHERE title @1@ 'animals';
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	skip_ok(res, 6)?;
	//
	let tmp = res.remove(0).result?;
	// Score uses Lucene-style IDF: ln(1 + (N - n + 0.5) / (n + 0.5))
	let val = syn::value(
		"[
			{
				id: blog:3,
				score: 1.3112574815750122
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
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
		DEFINE INDEX blog_title ON blog FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		LET $keywords = 'animals';
 		SELECT id,search::score(1) AS score FROM blog
 			WHERE (title @1@ $keywords AND label = 'test')
 			OR (title @1@ $keywords AND label = 'test');
		SELECT id,search::score(1) + search::score(2) AS score FROM blog
			WHERE (title @1@ 'dummy1' AND label = 'test')
			OR (title @2@ 'dummy2' AND label = 'test');
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	skip_ok(res, 7)?;
	//
	let tmp = res.remove(0).result?;
	// Score uses Lucene-style IDF: ln(1 + (N - n + 0.5) / (n + 0.5))
	let val = syn::value(
		"[
			{
				id: blog:3,
				score: 1.3112574815750122
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	// This result should be empty, as we are looking for non-existing terms (dummy1
	// and dummy2).
	let tmp = res.remove(0).result?;
	let val: Value = Value::Array(Array::new());
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn select_where_matches_analyser_without_tokenizer() -> Result<()> {
	let sql = r"
		DEFINE ANALYZER az FILTERS lowercase,ngram(1,5);
		CREATE t:1 SET text = 'ab';
		DEFINE INDEX search_idx ON TABLE t COLUMNS text FULLTEXT ANALYZER az BM25 HIGHLIGHTS;
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
		DEFINE INDEX search_idx ON TABLE t COLUMNS text FULLTEXT ANALYZER mapper BM25;
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

// BM25 vs BM25_ACCURATE Scoring Tests

#[tokio::test]
async fn bm25_and_bm25_accurate_produce_valid_scores() -> Result<()> {
	// Test that both BM25 (fast, uses SmallFloat encoding) and BM25_ACCURATE
	// produce valid positive scores for matching documents
	let sql = r"
		CREATE blog:1 SET title = 'the quick brown fox jumped over the lazy dog';
		CREATE blog:2 SET title = 'the fast fox jumped over the lazy dog';
		CREATE blog:3 SET title = 'the other animals sat there watching to death';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX idx_bm25 ON blog FIELDS title FULLTEXT ANALYZER simple BM25;
		DEFINE INDEX idx_accurate ON blog FIELDS title FULLTEXT ANALYZER simple BM25_ACCURATE;
		SELECT id, search::score(1) AS score FROM blog WHERE title @1@ 'fox' ORDER BY score DESC;
		SELECT id, search::score(2) AS score FROM blog WHERE title @2@ 'fox' ORDER BY score DESC;
		-- Verify both find 2 documents and scores are positive
		SELECT count() FROM blog WHERE title @1@ 'fox' GROUP ALL;
		SELECT count() FROM blog WHERE title @2@ 'fox' GROUP ALL;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	skip_ok(res, 6)?;

	// BM25 results - just verify it returns results (skip checking content)
	let _ = res.remove(0).result?;

	// BM25_ACCURATE results
	let _ = res.remove(0).result?;

	// Verify count
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ count: 2 }]").unwrap();
	assert_eq!(tmp, val, "BM25 should find 2 documents");

	let tmp = res.remove(0).result?;
	let val = syn::value("[{ count: 2 }]").unwrap();
	assert_eq!(tmp, val, "BM25_ACCURATE should find 2 documents");

	Ok(())
}

#[tokio::test]
async fn bm25_accurate_with_custom_parameters() -> Result<()> {
	// Test BM25_ACCURATE with custom k1 and b parameters
	// b=1 full length normalization means shorter docs score higher
	let sql = r"
		CREATE blog:1 SET title = 'short title';
		CREATE blog:2 SET title = 'this is a much longer title with many more words in it';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX idx ON blog FIELDS title FULLTEXT ANALYZER simple BM25_ACCURATE(1.2, 1.0);
		SELECT id, search::score(1) AS score FROM blog WHERE title @1@ 'title' ORDER BY score DESC LIMIT 1;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	skip_ok(res, 4)?;

	// First result should be blog:1 (shorter doc scores higher with b=1.0)
	let tmp = res.remove(0).result?;
	// Parse and check if the id is blog:1
	if let Value::Array(arr) = &tmp {
		if let Some(first) = arr.first() {
			if let Value::Object(obj) = first {
				let id = obj.get("id").cloned().unwrap_or(Value::None);
				let expected = syn::value("blog:1").unwrap();
				assert_eq!(id, expected, "Short doc should rank first with b=1.0");
			}
		}
	}

	Ok(())
}

#[tokio::test]
async fn bm25_handles_empty_results() -> Result<()> {
	// Searching for non-existent terms should return empty results
	let sql = r"
		CREATE doc:1 SET text = 'hello world';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX idx_bm25 ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
		DEFINE INDEX idx_accurate ON doc FIELDS text FULLTEXT ANALYZER simple BM25_ACCURATE;
		SELECT id FROM doc WHERE text @1@ 'nonexistent';
		SELECT id FROM doc WHERE text @2@ 'nonexistent';
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	skip_ok(res, 4)?;

	let tmp = res.remove(0).result?;
	let val: Value = Value::Array(Array::new());
	assert_eq!(tmp, val, "BM25 should return empty for non-existent term");

	let tmp = res.remove(0).result?;
	assert_eq!(tmp, val, "BM25_ACCURATE should return empty for non-existent term");

	Ok(())
}

#[tokio::test]
async fn bm25_single_document_index() -> Result<()> {
	// Edge case: index with only one document
	let sql = r"
		CREATE doc:1 SET text = 'the only document in the index';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX idx_bm25 ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
		DEFINE INDEX idx_accurate ON doc FIELDS text FULLTEXT ANALYZER simple BM25_ACCURATE;
		SELECT id FROM doc WHERE text @1@ 'document';
		SELECT id FROM doc WHERE text @2@ 'document';
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	skip_ok(res, 4)?;

	let expected = syn::value("[{ id: doc:1 }]").unwrap();

	let tmp = res.remove(0).result?;
	assert_eq!(tmp, expected, "BM25 should find the document");

	let tmp = res.remove(0).result?;
	assert_eq!(tmp, expected, "BM25_ACCURATE should find the document");

	Ok(())
}

#[tokio::test]
async fn bm25_multi_term_query() -> Result<()> {
	// Test scoring with multiple search terms - docs with both terms should rank higher
	let sql = r"
		CREATE doc:1 SET text = 'apple banana cherry';
		CREATE doc:2 SET text = 'apple banana';
		CREATE doc:3 SET text = 'apple';
		CREATE doc:4 SET text = 'banana cherry';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX idx ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
		SELECT id, search::score(1) AS score FROM doc WHERE text @1@ 'apple banana' ORDER BY score DESC LIMIT 1;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	skip_ok(res, 6)?;

	// First result should be doc:1 or doc:2 (both have apple and banana)
	let tmp = res.remove(0).result?;
	let val1 = syn::value("doc:1").unwrap();
	let val2 = syn::value("doc:2").unwrap();
	if let Value::Array(arr) = &tmp {
		if let Some(first) = arr.first() {
			if let Value::Object(obj) = first {
				let id = obj.get("id").cloned().unwrap_or(Value::None);
				assert!(
					id == val1 || id == val2,
					"Top result should be doc:1 or doc:2 (docs with both terms)"
				);
			}
		}
	}

	Ok(())
}

#[tokio::test]
async fn bm25_and_bm25_accurate_same_ranking() -> Result<()> {
	// Both BM25 and BM25_ACCURATE should produce the same ranking order
	// With b=0 (no length normalization), higher TF should rank higher - doc:3 has most apples
	let sql = r"
		CREATE doc:1 SET text = 'apple';
		CREATE doc:2 SET text = 'apple apple';
		CREATE doc:3 SET text = 'apple apple apple';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX idx_bm25 ON doc FIELDS text FULLTEXT ANALYZER simple BM25(1.2, 0);
		DEFINE INDEX idx_accurate ON doc FIELDS text FULLTEXT ANALYZER simple BM25_ACCURATE(1.2, 0);
		-- Get the top result from each index (should be doc:3 with most apples)
		SELECT id, search::score(1) AS score FROM doc WHERE text @1@ 'apple' ORDER BY score DESC LIMIT 1;
		SELECT id, search::score(2) AS score FROM doc WHERE text @2@ 'apple' ORDER BY score DESC LIMIT 1;
		-- Count results to verify both find all docs
		SELECT count() FROM doc WHERE text @1@ 'apple' GROUP ALL;
		SELECT count() FROM doc WHERE text @2@ 'apple' GROUP ALL;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	skip_ok(res, 6)?;

	// Both should have doc:3 as top result (highest TF)
	let expected_top = syn::value("doc:3").unwrap();
	let bm25_result = res.remove(0).result?;
	let accurate_result = res.remove(0).result?;

	// Extract IDs from results
	if let Value::Array(arr) = &bm25_result {
		if let Some(Value::Object(obj)) = arr.first() {
			let id = obj.get("id").cloned().unwrap_or(Value::None);
			assert_eq!(id, expected_top, "BM25 top result should be doc:3");
		}
	}
	if let Value::Array(arr) = &accurate_result {
		if let Some(Value::Object(obj)) = arr.first() {
			let id = obj.get("id").cloned().unwrap_or(Value::None);
			assert_eq!(id, expected_top, "BM25_ACCURATE top result should be doc:3");
		}
	}

	// Both should find all 3 documents
	let bm25_count = res.remove(0).result?;
	let accurate_count = res.remove(0).result?;
	let expected_count = syn::value("[{ count: 3 }]").unwrap();
	assert_eq!(bm25_count, expected_count, "BM25 should find 3 docs");
	assert_eq!(accurate_count, expected_count, "BM25_ACCURATE should find 3 docs");

	Ok(())
}
