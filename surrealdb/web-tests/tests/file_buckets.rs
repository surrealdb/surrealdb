//! File buckets backed by OPFS.

#![cfg(all(target_family = "wasm", target_vendor = "unknown", target_os = "unknown"))]
#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

use surrealdb_core::kvs::Datastore;
use surrealdb_web_tests::{DIR, datastore, run};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

/// Runs a script that must succeed and returns its results.
async fn ok(ds: &Datastore, sql: &str) -> Vec<String> {
	run(ds, sql).await.into_iter().map(|r| r.unwrap()).collect()
}

/// Defines bucket `b` on a fresh root, bound to `$root`.
fn with_bucket(sql: &str) -> String {
	format!(
		"LET $root = '/{DIR}/' + <string> rand::uuid();
		DEFINE BUCKET b BACKEND 'file://' + $root;
		{sql}"
	)
}

#[wasm_bindgen_test]
async fn put_get_head_delete() {
	let ds = datastore().await;
	let res = ok(
		&ds,
		&with_bucket(
			"file::put(f\"b:/dir/a.txt\", 'hello');
			file::get(f\"b:/dir/a.txt\").to_string();
			file::head(f\"b:/dir/a.txt\").size;
			time::now() - file::head(f\"b:/dir/a.txt\").updated < 1m;
			file::exists(f\"b:/dir/a.txt\");
			file::delete(f\"b:/dir/a.txt\");
			file::exists(f\"b:/dir/a.txt\");
			file::get(f\"b:/dir/a.txt\");
			file::head(f\"b:/dir/a.txt\");",
		),
	)
	.await;
	assert_eq!(res[2..], ["NONE", "'hello'", "5", "true", "true", "NONE", "false", "NONE", "NONE"]);
}

#[wasm_bindgen_test]
async fn put_if_not_exists_keeps_existing_data() {
	let ds = datastore().await;
	let res = ok(
		&ds,
		&with_bucket(
			"file::put_if_not_exists(f\"b:/a.txt\", 'first');
			file::put_if_not_exists(f\"b:/a.txt\", 'second');
			file::get(f\"b:/a.txt\").to_string();",
		),
	)
	.await;
	assert_eq!(res[4], "'first'");
}

#[wasm_bindgen_test]
async fn copy_and_rename() {
	let ds = datastore().await;
	let res = run(
		&ds,
		&with_bucket(
			"file::put(f\"b:/src.txt\", 'src');
			file::put(f\"b:/taken.txt\", 'taken');
			file::copy(f\"b:/src.txt\", '/nested/copy.txt');
			file::get(f\"b:/nested/copy.txt\").to_string();
			file::exists(f\"b:/src.txt\");
			file::copy(f\"b:/missing.txt\", '/x.txt');
			file::copy_if_not_exists(f\"b:/src.txt\", '/taken.txt');
			file::get(f\"b:/taken.txt\").to_string();
			file::rename(f\"b:/nested/copy.txt\", '/moved/renamed.txt');
			file::get(f\"b:/moved/renamed.txt\").to_string();
			file::exists(f\"b:/nested/copy.txt\");
			file::rename(f\"b:/missing.txt\", '/x.txt');
			file::rename_if_not_exists(f\"b:/src.txt\", '/taken.txt');
			file::get(f\"b:/taken.txt\").to_string();
			file::exists(f\"b:/src.txt\");",
		),
	)
	.await;
	let res: Vec<_> = res[4..].iter().map(|r| r.as_deref().map_err(|_| "error")).collect();
	assert_eq!(
		res,
		[
			Ok("NONE"),
			Ok("'src'"),
			Ok("true"),
			Err("error"),
			Ok("NONE"),
			Ok("'taken'"),
			Ok("NONE"),
			Ok("'src'"),
			Ok("false"),
			Err("error"),
			Ok("NONE"),
			Ok("'taken'"),
			Ok("true"),
		]
	);
}

#[wasm_bindgen_test]
async fn rename_onto_itself_keeps_the_file() {
	let ds = datastore().await;
	let res = ok(
		&ds,
		&with_bucket(
			"file::put(f\"b:/a.txt\", 'a');
			file::rename(f\"b:/a.txt\", '/a.txt');
			file::get(f\"b:/a.txt\").to_string();",
		),
	)
	.await;
	assert_eq!(res[4], "'a'");
}

#[wasm_bindgen_test]
async fn list_filters_and_paginates() {
	let ds = datastore().await;
	let res = ok(
		&ds,
		&with_bucket(
			"file::put(f\"b:/c.txt\", 'x');
			file::put(f\"b:/a.txt\", 'x');
			file::put(f\"b:/b.txt\", 'x');
			file::put(f\"b:/dir/d.txt\", 'x');
			file::list('b').map(|$v| $v.file.key());
			file::list('b', { limit: 2 }).map(|$v| $v.file.key());
			file::list('b', { start: '/a.txt' }).map(|$v| $v.file.key());
			file::list('b', { prefix: '/dir' }).map(|$v| $v.file.key());
			file::list('b', { prefix: '/nope' });",
		),
	)
	.await;
	assert_eq!(
		res[6..],
		[
			"['/a.txt', '/b.txt', '/c.txt']",
			"['/a.txt', '/b.txt']",
			"['/b.txt', '/c.txt']",
			"['/dir/d.txt']",
			"[]",
		]
	);
}

/// Files outlive the datastore, so they are really in OPFS.
#[wasm_bindgen_test]
async fn files_persist_across_datastores() {
	let first = datastore().await;
	let res = ok(&first, &with_bucket("file::put(f\"b:/a.txt\", 'kept'); $root;")).await;
	let root = &res[3];

	let second = datastore().await;
	let res = ok(
		&second,
		&format!(
			"DEFINE BUCKET b BACKEND 'file://' + {root}; file::get(f\"b:/a.txt\").to_string();"
		),
	)
	.await;
	assert_eq!(res[1], "'kept'");
}

#[wasm_bindgen_test]
async fn rejects_keys_escaping_the_root() {
	let ds = datastore().await;
	let res = run(
		&ds,
		&with_bucket(&format!(
			"LET $other = '/{DIR}/' + <string> rand::uuid();
			DEFINE BUCKET o BACKEND 'file://' + $other;
			file::put(f\"o:/secret.txt\", 'secret');
			LET $escape = '/../' + $other.split('/').last() + '/secret.txt';
			file::get(type::file('b', $escape));
			file::put(type::file('b', $escape), 'overwritten');
			file::get(f\"o:/secret.txt\").to_string();"
		)),
	)
	.await;
	for r in &res[6..8] {
		assert!(r.as_ref().unwrap_err().contains("escapes the bucket root"), "{r:?}");
	}
	assert_eq!(res[8].as_deref(), Ok("'secret'"));
}

#[wasm_bindgen_test]
async fn denies_roots_outside_the_allowlist() {
	let ds = datastore().await;
	let res = run(&ds, "DEFINE BUCKET c BACKEND 'file:///elsewhere';").await;
	assert!(res[0].as_ref().unwrap_err().contains("File access denied"), "{res:?}");
}

#[wasm_bindgen_test]
async fn rejects_a_file_as_the_root() {
	let ds = datastore().await;
	let res = run(
		&ds,
		&with_bucket(
			"file::put(f\"b:/file.txt\", 'x');
			DEFINE BUCKET f BACKEND 'file://' + $root + '/file.txt';",
		),
	)
	.await;
	assert!(res[3].as_ref().unwrap_err().contains("is not a directory"), "{res:?}");
}
