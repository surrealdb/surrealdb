//! File buckets whose files another context holds open.
//!
//! Only a dedicated worker can take a sync access handle, the lock that keeps
//! a file from being removed, so these tests run in one.

#![cfg(all(target_family = "wasm", target_vendor = "unknown", target_os = "unknown"))]
#![recursion_limit = "256"]
#![allow(clippy::unwrap_used)]

use surrealdb_core::kvs::Datastore;
use surrealdb_web_tests::{DIR, datastore, run};
use wasm_bindgen_futures::wasm_bindgen::JsCast;
use wasm_bindgen_futures::{JsFuture, js_sys};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{
	FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemSyncAccessHandle, WorkerGlobalScope,
};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Defines bucket `b` on the fresh root `/{DIR}/{name}`, then runs `sql`.
async fn with_bucket(ds: &Datastore, name: &str, sql: &str) -> Vec<Result<String, String>> {
	run(ds, &format!("DEFINE BUCKET b BACKEND 'file:///{DIR}/{name}'; {sql}")).await
}

/// Holds `/{DIR}/{name}/{file}` open, so it cannot be removed until the
/// handle is closed.
async fn lock(name: &str, file: &str) -> FileSystemSyncAccessHandle {
	let scope: WorkerGlobalScope = js_sys::global().unchecked_into();
	let root: FileSystemDirectoryHandle =
		JsFuture::from(scope.navigator().storage().get_directory()).await.unwrap().unchecked_into();
	let mut dir = root;
	for part in [DIR, name] {
		dir = JsFuture::from(dir.get_directory_handle(part)).await.unwrap().unchecked_into();
	}
	let handle: FileSystemFileHandle =
		JsFuture::from(dir.get_file_handle(file)).await.unwrap().unchecked_into();
	JsFuture::from(handle.create_sync_access_handle()).await.unwrap().unchecked_into()
}

/// A directory name no other test uses.
fn fresh_name() -> String {
	format!("{:x}", js_sys::Math::random().to_bits())
}

#[wasm_bindgen_test]
async fn failed_rename_restores_the_replaced_target() {
	let ds = datastore().await;
	let name = fresh_name();
	with_bucket(
		&ds,
		&name,
		"file::put(f\"b:/src.txt\", 'src'); file::put(f\"b:/dst.txt\", 'old');",
	)
	.await;

	let held = lock(&name, "src.txt").await;
	let res = with_bucket(&ds, &name, "file::rename(f\"b:/src.txt\", '/dst.txt');").await;
	held.close();

	assert!(res[1].is_err(), "{res:?}");
	let res = with_bucket(
		&ds,
		&name,
		"file::get(f\"b:/src.txt\").to_string(); file::get(f\"b:/dst.txt\").to_string();",
	)
	.await;
	assert_eq!(res[1..], [Ok("'src'".to_string()), Ok("'old'".to_string())]);
}

#[wasm_bindgen_test]
async fn failed_rename_removes_the_new_target() {
	let ds = datastore().await;
	let name = fresh_name();
	with_bucket(&ds, &name, "file::put(f\"b:/src.txt\", 'src');").await;

	let held = lock(&name, "src.txt").await;
	let res =
		with_bucket(&ds, &name, "file::rename_if_not_exists(f\"b:/src.txt\", '/dst.txt');").await;
	held.close();

	assert!(res[1].is_err(), "{res:?}");
	let res = with_bucket(
		&ds,
		&name,
		"file::get(f\"b:/src.txt\").to_string(); file::exists(f\"b:/dst.txt\");",
	)
	.await;
	assert_eq!(res[1..], [Ok("'src'".to_string()), Ok("false".to_string())]);
}
