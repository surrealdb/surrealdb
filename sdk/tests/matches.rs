mod parse;
use parse::Parse;
mod helpers;
use crate::helpers::{skip_ok, Test};
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_matches_using_index() -> Result<(), Error> {
	let sql = r"
		CREATE blog:1 SET title = 'Hello World!';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id FROM blog WHERE title @1@ 'Hello' EXPLAIN;
		SELECT id, search::highlight('<em>', '</em>', 1) AS title FROM blog WHERE title @1@ 'Hello';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	skip_ok(res, 3)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
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
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				title: '<em>Hello</em> World!'
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_without_using_index_iterator() -> Result<(), Error> {
	let sql = r"
		CREATE blog:1 SET title = 'Hello World!';
		CREATE blog:2 SET title = 'Foo Bar!';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX blog_title ON blog FIELDS title SEARCH ANALYZER simple BM25(1.2,0.75) HIGHLIGHTS;
		SELECT id FROM blog WHERE (title @0@ 'hello' AND identifier > 0) OR (title @1@ 'world' AND identifier < 99) EXPLAIN FULL;
		SELECT id,search::highlight('<em>', '</em>', 1) AS title FROM blog WHERE (title @0@ 'hello' AND identifier > 0) OR (title @1@ 'world' AND identifier < 99);
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	//
	skip_ok(res, 4)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
						table: 'blog',
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				},
				{
					detail: {
						count: 1,
					},
					operation: 'Fetch'
				},
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				title: 'Hello <em>World</em>!'
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

async fn select_where_matches_using_index_and_arrays(parallel: bool) -> Result<(), Error> {
	let p = if parallel {
		"PARALLEL"
	} else {
		""
	};
	let sql = format!(
		r"
		CREATE blog:1 SET content = ['Hello World!', 'Be Bop', 'Foo Bãr'];
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_content ON blog FIELDS content SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id FROM blog WHERE content @1@ 'Hello Bãr' {p} EXPLAIN;
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'Hello Bãr' {p};
	"
	);
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	skip_ok(res, 3)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
					{
						detail: {
							plan: {
								index: 'blog_content',
								operator: '@1@',
								value: 'Hello Bãr'
							},
							table: 'blog',
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				content: [
					'<em>Hello</em> World!',
					'Be Bop',
					'Foo <em>Bãr</em>'
				]
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_using_index_and_arrays_non_parallel() -> Result<(), Error> {
	select_where_matches_using_index_and_arrays(false).await
}

#[tokio::test]
async fn select_where_matches_using_index_and_arrays_with_parallel() -> Result<(), Error> {
	select_where_matches_using_index_and_arrays(true).await
}

#[tokio::test]
async fn select_where_matches_partial_highlight() -> Result<(), Error> {
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
		let val = Value::parse(
			"[
			{
				id: blog:1,
				content: '<em>Hello</em> World!'
			}
		]",
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				content: '<em>He</em>llo World!'
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	for i in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
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
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_partial_highlight_ngram() -> Result<(), Error> {
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
		let val = Value::parse(
			"[
			{
				id: blog:1,
				content: '<em>Hello</em> World!'
			}
		]",
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				content: 'H<em>el</em>lo World!'
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	for i in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
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
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

async fn select_where_matches_using_index_and_objects(parallel: bool) -> Result<(), Error> {
	let p = if parallel {
		"PARALLEL"
	} else {
		""
	};
	let sql = format!(
		r"
		CREATE blog:1 SET content = {{ 'title':'Hello World!', 'content':'Be Bop', 'tags': ['Foo', 'Bãr'] }};
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX blog_content ON blog FIELDS content SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		SELECT id FROM blog WHERE content @1@ 'Hello Bãr' {p} EXPLAIN;
		SELECT id, search::highlight('<em>', '</em>', 1) AS content FROM blog WHERE content @1@ 'Hello Bãr' {p};
	"
	);
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	skip_ok(res, 3)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
					{
						detail: {
							plan: {
								index: 'blog_content',
								operator: '@1@',
								value: 'Hello Bãr'
							},
							table: 'blog',
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: blog:1,
				content: [
					'Be Bop',
					'Foo',
					'<em>Bãr</em>',
					'<em>Hello</em> World!'
				]
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_using_index_and_objects_non_parallel() -> Result<(), Error> {
	select_where_matches_using_index_and_objects(false).await
}

#[tokio::test]
async fn select_where_matches_using_index_and_objects_with_parallel() -> Result<(), Error> {
	select_where_matches_using_index_and_objects(true).await
}

#[tokio::test]
async fn select_where_matches_using_index_offsets() -> Result<(), Error> {
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
	let val = Value::parse(
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
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_using_index_and_score() -> Result<(), Error> {
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
	let val = Value::parse(
		"[
			{
				id: blog:3,
				score: 0.9227996468544006
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_without_using_index_and_score() -> Result<(), Error> {
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
	let val = Value::parse(
		"[
			{
				id: blog:3,
				score: 0.9227996468544006
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));

	// This result should be empty, as we are looking for non-existing terms (dummy1 and dummy2).
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_without_complex_query() -> Result<(), Error> {
	let sql = r"
		CREATE page:1 SET title = 'the quick brown', content = 'fox jumped over the lazy dog', host = 'test';
		CREATE page:2 SET title = 'the fast fox', content = 'jumped over the lazy dog', host = 'test';
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX page_title ON page FIELDS title SEARCH ANALYZER simple BM25;
		DEFINE INDEX page_content ON page FIELDS content SEARCH ANALYZER simple BM25;
		DEFINE INDEX page_host ON page FIELDS host;
		SELECT id, search::score(1) as sc1, search::score(2) as sc2
		FROM page WHERE (title @1@ 'dog' OR content @2@ 'dog');
		SELECT id, search::score(1) as sc1, search::score(2) as sc2
			FROM page WHERE host = 'test'
			AND (title @1@ 'dog' OR content @2@ 'dog') explain;
 		SELECT id, search::score(1) as sc1, search::score(2) as sc2
    		FROM page WHERE
    		host = 'test'
    		AND (title @1@ 'dog' OR content @2@ 'dog');
    	SELECT id, search::score(1) as sc1, search::score(2) as sc2
    		FROM page WHERE
    		host = 'test'
    		AND (title @1@ 'dog' OR content @2@ 'dog') PARALLEL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	//
	skip_ok(res, 6)?;
	//
	let tmp = res.remove(0).result?;
	let val_docs = Value::parse(
		"[
				{
					id: page:1,
					sc1: 0f,
					sc2: -1.5517289638519287f
				},
				{
					id: page:2,
					sc1: 0f,
					sc2: -1.6716052293777466f
				}
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val_docs));

	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				detail: {
					plan: {
						index: 'page_host',
						operator: '=',
						value: 'test'
					},
					table: 'page'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					plan: {
						index: 'page_title',
						operator: '@1@',
						value: 'dog'
					},
					table: 'page'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					plan: {
						index: 'page_content',
						operator: '@2@',
						value: 'dog'
					},
					table: 'page'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));

	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val_docs));

	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val_docs));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_mixing_indexes() -> Result<(), Error> {
	let sql = r"
		DEFINE ANALYZER edgeNgram TOKENIZERS class FILTERS lowercase,ascii,edgeNgram(1,10);
		DEFINE INDEX idxPersonName ON TABLE person COLUMNS name SEARCH ANALYZER edgeNgram BM25;
		DEFINE INDEX idxSecurityNumber ON TABLE person COLUMNS securityNumber UNIQUE;
		CREATE person:1 content {name: 'Tobie', securityNumber: '123456'};
		CREATE person:2 content {name: 'Jaime', securityNumber: 'ABCDEF'};
		SELECT * FROM person WHERE name @@ 'Tobie' || securityNumber == '123456' EXPLAIN;
		SELECT * FROM person WHERE name @@ 'Tobie' || securityNumber == '123456';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	skip_ok(res, 5)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
						plan: {
							index: 'idxPersonName',
							operator: '@@',
							value: 'Tobie'
						},
						table: 'person'
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						plan: {
							index: 'idxSecurityNumber',
							operator: '==',
							value: '123456'
						},
						table: 'person'
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					id: person:1,
					name: 'Tobie',
					securityNumber: '123456'
				}
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_matches_analyser_without_tokenizer() -> Result<(), Error> {
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
async fn select_where_matches_analyser_with_mapper() -> Result<(), Error> {
	let sql = r"
		DEFINE ANALYZER mapper TOKENIZERS blank,class FILTERS lowercase,mapper('../tests/data/lemmatization-en.txt');
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
