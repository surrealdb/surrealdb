//! A build without one of the optional function families still parses and
//! resolves its functions, and each call fails with an error naming the
//! missing cargo feature. Each test runs only in a build that lacks its
//! feature.

#![cfg_attr(
	all(
		feature = "scram",
		feature = "text-diff",
		feature = "url",
		feature = "maintenance-tasks",
		feature = "api",
		feature = "encoding",
		feature = "files",
		feature = "search",
		feature = "collate",
		feature = "geo",
		feature = "regex-unicode",
		feature = "crypto-digest",
		feature = "crypto-password",
		feature = "html-sanitize",
		feature = "cjk-tokenizer",
		feature = "vector-index",
		feature = "fulltext"
	),
	expect(dead_code)
)]

use crate::dbs::Session;
use crate::kvs::Datastore;

/// Runs `sql` statement by statement and asserts each one fails with an error
/// that names `feature`.
async fn assert_each_names_feature(feature: &str, sql: &[&str]) {
	let ds = Datastore::new("memory").await.unwrap();
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute(
		"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;",
		&Session::owner(),
		None,
	)
	.await
	.unwrap();
	for statement in sql {
		let mut res = ds.execute(statement, &session, None).await.unwrap();
		let err = res.pop().unwrap().result.expect_err(statement).to_string();
		assert!(err.contains(feature), "{statement}: `{err}` does not name `{feature}`");
	}
}

#[cfg(not(feature = "crypto-digest"))]
#[tokio::test]
async fn digest_functions_name_the_missing_feature() {
	assert_each_names_feature(
		"crypto-digest",
		&[
			"RETURN crypto::md5('a')",
			"RETURN crypto::sha1('a')",
			"RETURN crypto::sha256('a')",
			"RETURN crypto::sha512('a')",
			"RETURN crypto::blake3('a')",
		],
	)
	.await;
}

#[cfg(not(feature = "crypto-password"))]
#[tokio::test]
async fn password_hash_functions_name_the_missing_feature() {
	let mut sql = Vec::new();
	for family in ["argon2", "bcrypt", "pbkdf2", "scrypt"] {
		sql.push(format!("RETURN crypto::{family}::generate('a')"));
		sql.push(format!("RETURN crypto::{family}::compare('h', 'a')"));
	}
	let sql: Vec<&str> = sql.iter().map(String::as_str).collect();
	assert_each_names_feature("crypto-password", &sql).await;
}

#[cfg(not(feature = "html-sanitize"))]
#[tokio::test]
async fn html_sanitize_names_the_missing_feature() {
	assert_each_names_feature("html-sanitize", &["RETURN string::html::sanitize('<b>a</b>')"])
		.await;
	let ds = Datastore::new("memory").await.unwrap();
	let session = Session::owner().with_ns("test").with_db("test");
	let mut res = ds.execute("RETURN string::html::encode('<b>')", &session, None).await.unwrap();
	assert_eq!(
		res.pop().unwrap().result.unwrap(),
		surrealdb_types::Value::String("&lt;b&gt;".into())
	);
}

// Without `fulltext` no analyzer can be defined at all, and the error names
// that feature instead.
#[cfg(all(not(feature = "cjk-tokenizer"), feature = "fulltext"))]
#[tokio::test]
async fn segment_tokenizer_names_the_missing_feature() {
	assert_each_names_feature(
		"cjk-tokenizer",
		&["DEFINE ANALYZER cjk TOKENIZERS SEGMENT(japanese)"],
	)
	.await;
}

#[cfg(not(feature = "vector-index"))]
#[tokio::test]
async fn hnsw_index_names_the_missing_feature() {
	assert_each_names_feature(
		"vector-index",
		&["DEFINE INDEX ix ON pts FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN"],
	)
	.await;
}

#[cfg(not(feature = "fulltext"))]
#[tokio::test]
async fn fulltext_names_the_missing_feature() {
	assert_each_names_feature(
		"fulltext",
		&[
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase",
			"DEFINE INDEX ix ON doc FIELDS body FULLTEXT ANALYZER like",
		],
	)
	.await;
}

#[cfg(not(feature = "collate"))]
#[tokio::test]
async fn collation_names_the_missing_feature() {
	assert_each_names_feature(
		"collate",
		&[
			"RETURN array::sort_lexical(['b', 'a'])",
			"RETURN array::sort_natural_lexical(['b', 'a'])",
			"RETURN string::slug('a b')",
			"RETURN 'a b'.slug()",
			"SELECT * FROM t ORDER BY a COLLATE",
			"SELECT * FROM t ORDER BY a COLLATE NUMERIC",
		],
	)
	.await;
	// Natural ordering needs no transliteration.
	let ds = Datastore::new("memory").await.unwrap();
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute(
		"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;",
		&Session::owner(),
		None,
	)
	.await
	.unwrap();
	let sql = "RETURN array::sort_natural(['10', '9']); \
	           SELECT VALUE a FROM [{ a: '10' }, { a: '9' }] ORDER BY a NUMERIC";
	let res = ds.execute(sql, &session, None).await.unwrap();
	for r in res {
		assert_eq!(surrealdb_types::ToSql::to_sql(&r.result.unwrap()), "['9', '10']");
	}
}

#[cfg(not(feature = "regex-unicode"))]
#[tokio::test]
async fn unicode_regex_names_the_missing_feature() {
	let ds = Datastore::new("memory").await.unwrap();
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute(
		"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;",
		&Session::owner(),
		None,
	)
	.await
	.unwrap();
	// A regex literal fails as the query parses, with the reason.
	for sql in [
		r"RETURN string::matches('\u{663}', /\d/)",
		r"RETURN string::matches('x', /\p{Greek}/)",
		r"RETURN string::matches('\u{c9}', /(?i)\u{e9}/)",
	] {
		let err = match ds.execute(sql, &session, None).await {
			Err(e) => e.to_string(),
			Ok(mut res) => res.pop().unwrap().result.expect_err(sql).to_string(),
		};
		assert!(err.contains("regex-unicode"), "{sql}: `{err}` does not name `regex-unicode`");
	}
	// A string cast to a regex fails as any uncompilable pattern does.
	let sql = r"RETURN <regex> '\\w+'";
	let mut res = ds.execute(sql, &session, None).await.unwrap();
	res.pop().unwrap().result.expect_err(sql);
	// A pattern that matches alike in both engines compiles.
	let sql = "RETURN string::matches('CAF\u{e9} 42', /(?i)^caf(?-i:\u{e9}) [0-9]+$/)";
	let mut res = ds.execute(sql, &session, None).await.unwrap();
	assert_eq!(res.pop().unwrap().result.unwrap(), surrealdb_types::Value::Bool(true));
}

#[cfg(not(feature = "geo"))]
#[tokio::test]
async fn geometry_operations_name_the_missing_feature() {
	assert_each_names_feature(
		"geo",
		&[
			"RETURN geo::area({ type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] })",
			"RETURN geo::bearing((0, 0), (1, 1))",
			"RETURN geo::centroid((0, 0))",
			"RETURN geo::distance((0, 0), (1, 1))",
			"RETURN (0, 0) INSIDE { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] }",
			"RETURN { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] } CONTAINS (0, 0)",
			"RETURN (0, 0) INTERSECTS (0, 0)",
			"RETURN (0, 0) OUTSIDE (1, 1)",
			"RETURN { type: 'Polygon', coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] } CONTAINSALL [(0, 0)]",
		],
	)
	.await;
	// Geometry values, hashing and validity need no geometry algorithms.
	let ds = Datastore::new("memory").await.unwrap();
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute(
		"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;",
		&Session::owner(),
		None,
	)
	.await
	.unwrap();
	let sql = "CREATE ONLY place:1 SET at = (51.5, -0.1) RETURN VALUE at; \
	           RETURN geo::hash::encode((51.5, -0.1), 5); \
	           RETURN geo::is_valid((51.5, -0.1)); \
	           RETURN [(0, 0)] CONTAINS (0, 0);";
	let res = ds.execute(sql, &session, None).await.unwrap();
	let got: Vec<String> =
		res.into_iter().map(|r| surrealdb_types::ToSql::to_sql(&r.result.unwrap())).collect();
	assert_eq!(got, ["(51.5f, -0.1f)", "'mpux7'", "true", "true"]);
}

#[cfg(not(feature = "api"))]
#[tokio::test]
async fn api_functions_name_the_missing_feature() {
	assert_each_names_feature(
		"api",
		&["RETURN api::invoke('/ping')", "RETURN api::res::status({}, || {}, 200)"],
	)
	.await;
}

#[cfg(not(feature = "files"))]
#[tokio::test]
async fn file_functions_name_the_missing_feature() {
	assert_each_names_feature("files", &["RETURN file::get(NONE)", "RETURN file::key(NONE)"]).await;
}

#[cfg(not(feature = "search"))]
#[tokio::test]
async fn search_functions_name_the_missing_feature() {
	assert_each_names_feature("search", &["RETURN search::rrf([], 10)"]).await;
}

#[cfg(not(feature = "encoding"))]
#[tokio::test]
async fn encoding_functions_name_the_missing_feature() {
	assert_each_names_feature(
		"encoding",
		&["RETURN encoding::base64::encode(<bytes>'a')", "RETURN encoding::json::decode('1')"],
	)
	.await;
}

#[cfg(not(feature = "maintenance-tasks"))]
#[tokio::test]
async fn a_datastore_starts_no_maintenance_tasks() {
	let ds = std::sync::Arc::new(Datastore::new("memory").await.unwrap());
	assert!(!ds.maintenance_tasks_running());
	ds.start_maintenance_tasks().unwrap();
	assert!(!ds.maintenance_tasks_running());
}

#[cfg(not(feature = "url"))]
#[tokio::test]
async fn url_functions_name_the_missing_feature() {
	assert_each_names_feature(
		"url",
		&["RETURN parse::url::host('http://a.b/c')", "RETURN string::is_url('http://a.b')"],
	)
	.await;
}

#[cfg(not(feature = "text-diff"))]
#[tokio::test]
async fn text_patches_name_the_missing_feature() {
	assert_each_names_feature(
		"text-diff",
		&[
			"RETURN value::patch('a', [{ op: 'change', path: '/', value: '@@ -1 +1 @@\\n-a\\n+b\\n' }])",
		],
	)
	.await;
	// A diff replaces a changed string whole, which applies to the same value.
	let ds = Datastore::new("memory").await.unwrap();
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute(
		"DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test;",
		&Session::owner(),
		None,
	)
	.await
	.unwrap();
	let sql = "RETURN value::diff({ a: 'x' }, { a: 'y' }); \
	           RETURN value::patch({ a: 'x' }, value::diff({ a: 'x' }, { a: 'y' }));";
	let res = ds.execute(sql, &session, None).await.unwrap();
	let got: Vec<String> =
		res.into_iter().map(|r| surrealdb_types::ToSql::to_sql(&r.result.unwrap())).collect();
	assert_eq!(got, ["[{ op: 'replace', path: '/a', value: 'y' }]", "{ a: 'y' }"]);
}

#[cfg(not(feature = "scram"))]
#[tokio::test]
async fn a_password_stores_no_scram_verifier() {
	let ds = Datastore::new("memory").await.unwrap();
	let sql = "DEFINE USER edge ON ROOT PASSWORD 'secret' ROLES OWNER; INFO FOR USER edge ON ROOT;";
	let mut res = ds.execute(sql, &Session::owner(), None).await.unwrap();
	let info = surrealdb_types::ToSql::to_sql(&res.pop().unwrap().result.unwrap());
	assert!(!info.to_lowercase().contains("scram"), "{info}");
}
